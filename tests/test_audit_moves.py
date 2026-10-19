import asyncio
import hashlib
import time
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import httpx
import pytest
import pytest_asyncio

from app import audit_moves
from app.audit_moves import AuditMoveManager, category, copy_verified, move_direct, publish_copied
from app.domain import now_iso
from app.main import create_app
from app.remote import RemoteClient
from app.upload_audit import scan_directory


@pytest_asyncio.fixture
async def local_app(tmp_path):
    source = RemoteClient('source', httpx.MockTransport(lambda r: httpx.Response(500)))
    target = RemoteClient('target', httpx.MockTransport(lambda r: httpx.Response(500)))
    app = create_app(source, target, tmp_path / 'database')
    yield app
    await app.state.audit_moves.shutdown()
    await app.state.videos.shutdown()
    await app.state.upload_audit.shutdown()
    await source.close(); await target.close(); app.state.store.close()


def report_for(app, root, statuses=None, recursive=False):
    files, ignored = scan_directory(str(root), recursive)
    for file in files:
        first, second = (statuses or {}).get(file['name'], ('uploaded', 'uploaded'))
        file['targets'] = {'27912': {'status': first}, '27944': {'status': second}}
    report = {'id': uuid4().hex, 'directory': str(root), 'files': files, 'datasets': [],
              'policyVersion': 2,
              'createdAt': now_iso(), 'localCount': len(files), 'ignoredCount': ignored, 'recursive': recursive}
    app.state.upload_audit.saved['report'] = report
    app.state.upload_audit.save()
    return report


async def preview(app, root, report):
    return await app.state.audit_moves.preview({'reportId': report['id'],
        'uploadedDirectory': str(root / '已上传'), 'magnetMissingDirectory': str(root / '磁盘吊未上传'),
        'bedMissingDirectory': str(root / '车斗未上传')})


def registered(app, path):
    row = {'id': 'a' * 24, 'jobId': 'bound-batch', 'name': '鲁A12345', 'plate': '鲁A12345',
           'qualityStart': '2026-10-01 01:00:00', 'qualityEnd': '2026-10-01 01:20:00',
           'activeRevision': 2, 'acceptedRevision': 2, 'revision': 3,
           'requests': {'user-correction': {'kind': 'correction', 'version': 2}},
           'versions': [{'revision': 2, 'state': 'saved', 'filePath': str(path),
                         'startTime': '2026-10-01 01:07:42', 'endTime': '2026-10-01 02:07:42',
                         'durationSeconds': 3600, 'sha256': hashlib.sha256(path.read_bytes()).hexdigest(),
                         'totalBytes': path.stat().st_size}]}
    app.state.videos.store.save(row)
    return row


@pytest.mark.parametrize('statuses,expected', [
    (('uploaded','uploaded'),'uploaded'), (('uploaded','missing'),'missing_magnet'),
    (('missing','uploaded'),'missing_bed'), (('missing','missing'),'both_missing'), (('review','uploaded'),'held'),
    (('in_progress','missing'),'held'), ((None,'uploaded'),'held'),
])
def test_move_classification_requires_two_confirmed_dataset_results(statuses, expected):
    assert category({'targets': {'27912': {'status': statuses[0]}, '27944': {'status': statuses[1]}}}) == expected


@pytest.mark.asyncio
async def test_preview_is_read_only_and_confirm_cuts_files_with_idempotent_replay(local_app, tmp_path):
    root = tmp_path / 'videos'; root.mkdir()
    for name in ('done.mp4','one-site.mp4','bed-missing.mp4','missing.mp4','unknown.mp4','running.mp4'):
        (root / name).write_bytes(('video:' + name).encode())
    report = report_for(local_app, root, {'one-site.mp4': ('uploaded','missing'), 'bed-missing.mp4': ('missing','uploaded'),
        'missing.mp4': ('missing','missing'),
        'unknown.mp4': ('review','missing'), 'running.mp4': ('uploaded','in_progress')})
    manager = local_app.state.audit_moves
    plan = await preview(local_app, root, report)
    assert plan['ruleVersion'] == 4
    assert plan['counts']['planned'] == 3 and plan['counts']['held'] == 3
    assert all(not (root / name).exists() for name in ('已上传','磁盘吊未上传','车斗未上传'))
    assert len(list(root.glob('*.mp4'))) == 6
    missing = next(item for item in plan['items'] if item['relativePath'] == 'missing.mp4')
    assert missing['category'] == 'both_missing' and '两处均未上传' in missing['message']
    assert 'destination' not in missing
    manager.start(plan['id'], 'confirm-once'); await manager.task
    result = manager.get(plan['id'])
    assert result['status'] == 'complete' and result['counts']['moved'] == 3
    assert (root / '已上传/done.mp4').read_bytes() == b'video:done.mp4'
    assert (root / '磁盘吊未上传/one-site.mp4').read_bytes() == b'video:one-site.mp4'
    assert (root / '车斗未上传/bed-missing.mp4').read_bytes() == b'video:bed-missing.mp4'
    assert (root / 'missing.mp4').read_bytes() == b'video:missing.mp4'
    assert {p.name for p in root.glob('*.mp4')} == {'missing.mp4','unknown.mp4','running.mp4'}
    assert not list(root.rglob('*.moving'))
    replay = manager.start(plan['id'], 'confirm-once')
    assert replay['status'] == 'complete' and len(list(root.rglob('*.mp4'))) == 6


@pytest.mark.asyncio
async def test_both_missing_files_and_bound_manual_correction_stay_at_original_location(local_app, tmp_path):
    root = tmp_path / 'videos'; root.mkdir()
    source = root / 'truck.mp4'; source.write_bytes(b'final-video')
    old = registered(local_app, source)
    before = source.stat()
    report = report_for(local_app, root, {'truck.mp4': ('missing','missing')})
    plan = await preview(local_app, root, report)
    manager = local_app.state.audit_moves
    manager.start(plan['id'], 'keep-both-missing'); await manager.task
    assert manager.get(plan['id'])['counts']['moved'] == 0
    assert source.stat().st_ino == before.st_ino and source.read_bytes() == b'final-video'
    saved = local_app.state.videos.store.get(old['id'])
    assert saved['requests'] == old['requests'] and saved['versions'] == old['versions']
    assert len(list(root.iterdir())) == 1 and 'localMove' not in report['files'][0]


@pytest.mark.asyncio
@pytest.mark.parametrize('status', ['preview','interrupted','attention'])
@pytest.mark.parametrize('rule_version', [None, 3])
async def test_legacy_unfinished_plan_requires_new_preview_without_moving_or_losing_progress(local_app, tmp_path, status, rule_version):
    root = tmp_path / 'videos'; root.mkdir()
    source = root / 'truck.mp4'; source.write_bytes(b'final-video')
    old = registered(local_app, source)
    report = report_for(local_app, root, {'truck.mp4': ('missing','missing')})
    plan = await preview(local_app, root, report)
    plan.pop('ruleVersion')
    if rule_version is not None:
        plan['ruleVersion'] = rule_version
    plan.update(status=status, unuploadedDirectory=str(root / '未上传'))
    plan['items'][0].update(category='unuploaded', state='planned', destination=str(root / '未上传/truck.mp4'))
    manager = local_app.state.audit_moves
    manager.save(plan)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=local_app),base_url='http://localhost') as client:
        response = await client.post(f"/api/upload-audit/moves/{plan['id']}/confirm",json={'requestId':'legacy-confirm'})
        assert response.status_code == 409 and '分类规则已更新' in response.json()['message']
    assert manager.get(plan['id'])['status'] == status
    assert source.read_bytes() == b'final-video' and not (root / '未上传').exists()
    assert local_app.state.videos.store.get(old['id'])['versions'] == old['versions']
    updated = await preview(local_app, root, report)
    assert updated['ruleVersion'] == 4 and updated['counts']['planned'] == 0
    assert manager.get(plan['id'])['items'][0]['category'] == 'unuploaded'


@pytest.mark.asyncio
async def test_completed_legacy_moves_replay_without_relocating_files_or_rewriting_history(local_app, tmp_path):
    root = tmp_path / 'videos'; root.mkdir()
    source = root / 'truck.mp4'; source.write_bytes(b'final-video')
    report = report_for(local_app, root)
    plan = await preview(local_app, root, report)
    manager = local_app.state.audit_moves
    manager.start(plan['id'], 'old-confirm'); await manager.task
    completed = manager.get(plan['id']); completed.pop('ruleVersion')
    completed['unuploadedDirectory'] = str(root / '未上传')
    manager.save(completed)
    persisted = manager.get(plan['id'])
    assert manager.start(plan['id'], 'old-confirm') == persisted
    assert manager.get(plan['id']) == persisted
    assert (root / '已上传/truck.mp4').read_bytes() == b'final-video' and not source.exists()


@pytest.mark.asyncio
async def test_same_disk_cut_of_37gb_video_never_copies_or_hashes_contents(local_app, tmp_path, monkeypatch):
    root = tmp_path / 'videos'; root.mkdir()
    source = root / 'large.mp4'
    with source.open('wb') as stream:
        stream.truncate(3_700_000_000)  # Sparse fixture: exercises metadata-only cut without allocating 3.7 GB.
    original_inode = source.stat().st_ino
    report = report_for(local_app, root); plan = await preview(local_app, root, report)
    def forbidden(*args):
        raise AssertionError('Same-volume cuts must not copy or hash a multi-GB video')
    monkeypatch.setattr(audit_moves, 'copy_verified', forbidden)
    monkeypatch.setattr(audit_moves, 'file_digest', forbidden)
    manager = local_app.state.audit_moves
    manager.start(plan['id'], 'large-cut'); await manager.task
    destination = root / '已上传/large.mp4'
    assert manager.get(plan['id'])['status'] == 'complete'
    assert not source.exists() and destination.stat().st_size == 3_700_000_000
    assert destination.stat().st_ino == original_inode


@pytest.mark.asyncio
async def test_changed_report_after_preview_requires_fresh_user_confirmation(local_app, tmp_path):
    root = tmp_path / 'videos'; root.mkdir(); (root/'truck.mp4').write_bytes(b'video')
    report = report_for(local_app, root); plan = await preview(local_app, root, report)
    report_for(local_app, root, {'truck.mp4': ('missing','missing')})
    with pytest.raises(ValueError, match='核对报告已更新'):
        local_app.state.audit_moves.start(plan['id'], 'stale-confirm')
    assert (root/'truck.mp4').read_bytes() == b'video'
    assert not (root/'已上传').exists()


@pytest.mark.asyncio
async def test_recursive_cut_preserves_subfolders_and_updates_only_bound_saved_path(local_app, tmp_path):
    root = tmp_path / 'videos'; (root / 'day1').mkdir(parents=True)
    source = root / 'day1/truck.mp4'; source.write_bytes(b'final-video')
    old = registered(local_app, source)
    report = report_for(local_app, root, recursive=True)
    plan = await preview(local_app, root, report)
    manager = local_app.state.audit_moves
    manager.start(plan['id'], 'move-bound'); await manager.task
    saved = local_app.state.videos.store.get(old['id'])
    destination = root / '已上传/day1/truck.mp4'
    assert destination.is_file() and not source.exists()
    assert saved['versions'][0]['filePath'] == str(destination)
    assert saved['requests'] == old['requests'] and saved['revision'] == old['revision']
    assert saved['activeRevision'] == saved['acceptedRevision'] == 2
    assert saved['versions'][0]['startTime'] == old['versions'][0]['startTime']
    assert local_app.state.upload_audit.saved['report']['files'][0]['localMove']['path'] == str(destination)
    duplicate = await preview(local_app, root, local_app.state.upload_audit.saved['report'])
    assert duplicate['counts']['planned'] == 0 and duplicate['counts']['unchanged'] == 1


@pytest.mark.asyncio
async def test_completed_uploads_are_reclassified_from_both_missing_folders_without_nesting(local_app, tmp_path):
    root = tmp_path / 'videos'
    (root / '磁盘吊未上传/day1').mkdir(parents=True)
    (root / '车斗未上传').mkdir()
    (root / '已上传').mkdir()
    source = root / '磁盘吊未上传/day1/truck.mp4'; source.write_bytes(b'final-video')
    (root / '车斗未上传/trailer.mp4').write_bytes(b'other-video')
    (root / '已上传/correct.mp4').write_bytes(b'already-correct')
    (root / 'new.mp4').write_bytes(b'not-uploaded')
    original = registered(local_app, source)
    uploads = local_app.state.uploads.store
    for dataset in ('27912', '27944'):
        uploads.save('items', {'id': dataset, 'dataset': dataset, 'path': str(source), 'state': 'complete',
            'fileVersion': audit_moves.source_fingerprint(source), 'jobId': 'import-' + dataset,
            'object': {'etag': 'verified-' + dataset}})
    report = report_for(local_app, root, {'new.mp4': ('missing', 'missing')}, recursive=True)
    manager = local_app.state.audit_moves
    plan = await preview(local_app, root, report)
    assert plan['counts']['planned'] == 2 and plan['counts']['unchanged'] == plan['counts']['held'] == 1
    manager.start(plan['id'], 'reclassify-uploaded'); await manager.task
    destination = root / '已上传/day1/truck.mp4'
    result = manager.get(plan['id'])
    assert result['status'] == 'complete' and result['counts']['moved'] == 2
    assert destination.read_bytes() == b'final-video' and not source.exists()
    assert (root / '已上传/trailer.mp4').read_bytes() == b'other-video'
    assert not (root / '已上传/磁盘吊未上传').exists() and not (root / '已上传/车斗未上传').exists()
    assert (root / 'new.mp4').read_bytes() == b'not-uploaded'
    row = local_app.state.videos.store.get(original['id'])
    assert row['requests'] == original['requests'] and row['revision'] == original['revision']
    assert row['versions'][0]['filePath'] == str(destination)
    assert row['versions'][0]['startTime'] == original['versions'][0]['startTime']
    for dataset in ('27912', '27944'):
        item = uploads.get('items', dataset)
        assert item['state'] == 'complete' and item['jobId'] == 'import-' + dataset
        assert item['object'] == {'etag': 'verified-' + dataset}
        assert item['path'] == str(destination) and item['fileVersion'] == audit_moves.source_fingerprint(destination)
    # A fresh recursive scan includes these folders, but never moves correct files again.
    fresh = report_for(local_app, root, {'new.mp4': ('missing', 'missing')}, recursive=True)
    again = await preview(local_app, root, fresh)
    assert again['counts']['planned'] == 0 and again['counts']['unchanged'] == 3
    assert again['counts']['held'] == 1


@pytest.mark.asyncio
async def test_reclassification_uses_last_verified_location_and_never_overwrites(local_app, tmp_path):
    root = tmp_path / 'videos'; root.mkdir()
    (root / 'truck.mp4').write_bytes(b'final-video')
    report = report_for(local_app, root, {'truck.mp4': ('uploaded', 'missing')})
    manager = local_app.state.audit_moves
    first = await preview(local_app, root, report)
    manager.start(first['id'], 'first-category'); await manager.task
    current = local_app.state.upload_audit.saved['report']
    current['files'][0]['targets']['27944']['status'] = 'uploaded'
    second = await preview(local_app, root, current)
    assert second['items'][0]['source'] == str(root / '磁盘吊未上传/truck.mp4')
    assert second['items'][0]['destination'] == str(root / '已上传/truck.mp4')
    manager.start(second['id'], 'second-category'); await manager.task
    assert (root / '已上传/truck.mp4').read_bytes() == b'final-video'
    assert not (root / '磁盘吊未上传/truck.mp4').exists()
    assert (await preview(local_app, root, local_app.state.upload_audit.saved['report']))['counts']['unchanged'] == 1
    (root / '磁盘吊未上传/truck.mp4').write_bytes(b'different-file')
    fresh = report_for(local_app, root, recursive=True)
    collision = await preview(local_app, root, fresh)
    item = next(i for i in collision['items'] if i['relativePath'] == '磁盘吊未上传/truck.mp4')
    assert item['state'] == 'held' and '同名文件' in item['message']
    assert (root / '已上传/truck.mp4').read_bytes() == b'final-video'
    assert (root / '磁盘吊未上传/truck.mp4').read_bytes() == b'different-file'


@pytest.mark.asyncio
async def test_preview_refresh_reads_latest_import_logs_and_all_category_folders(local_app, tmp_path):
    root = tmp_path / 'videos'; (root / '磁盘吊未上传').mkdir(parents=True)
    (root / 'truck.mp4').write_bytes(b'video')
    (root / '磁盘吊未上传/trailer.mp4').write_bytes(b'video')
    old = report_for(local_app, root, {'truck.mp4': ('missing', 'missing')})
    paths = []
    def handle(request):
        paths.append(request.url.path)
        path = request.url.path
        if path.endswith('/dataset-jobs/'):
            dataset = request.url.params['dataset']
            return httpx.Response(200, json={'data': [{'id': int(dataset), 'dataset': int(dataset), 'type': 1,
                'status': 3, 'utc_create': datetime.now(timezone.utc).isoformat()}]},
                headers={'x-total': '1', 'x-next-page': ''})
        if path.endswith('/logs/'):
            return httpx.Response(200, json={'data': {'result': [{'values': [
                [str(time.time_ns()), '视频无后缀名称:truck'],
                [str(time.time_ns()), '视频无后缀名称:trailer']]}], 'has_more': False, 'index': 99}})
        dataset = path.strip('/').split('/')[-1]
        return httpx.Response(200, json={'data': {'id': int(dataset), 'name': dataset}})
    audit = local_app.state.upload_audit
    await audit.client.client.aclose()
    audit.client.client = httpx.AsyncClient(base_url='https://mock.test/api/', transport=httpx.MockTransport(handle))
    audit.client.access = 'test-only-access'
    body = {'reportId': old['id'], 'uploadedDirectory': str(root / '已上传'),
        'magnetMissingDirectory': str(root / '磁盘吊未上传'), 'bedMissingDirectory': str(root / '车斗未上传'),
        'refreshReport': True}
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=local_app), base_url='http://localhost') as client:
            response = await client.post('/api/upload-audit/moves/preview', json=body)
        assert response.status_code == 200
        plan = response.json()
        assert plan['reportId'] != old['id'] and plan['reportId'] == audit.run['id']
        assert audit.saved['report']['recursive'] and audit.saved['report']['localCount'] == 2
        assert plan['counts']['planned'] == 2
        assert len([p for p in paths if p.endswith('/logs/')]) == 2
        assert {i['destination'] for i in plan['items']} == {str(root / '已上传' / n) for n in ('truck.mp4','trailer.mp4')}
        assert (root / 'truck.mp4').exists() and not (root / '已上传').exists()
    finally:
        await audit.client.close()


@pytest.mark.asyncio
async def test_failed_refresh_does_not_fall_back_to_stale_classification(local_app, tmp_path, monkeypatch):
    root = tmp_path / 'videos'; root.mkdir(); (root / 'truck.mp4').write_bytes(b'video')
    report = report_for(local_app, root)
    audit = local_app.state.upload_audit
    def start(directory, recursive):
        async def failed():
            audit.run.update(status='failed')
        audit.run = {'id': 'failed-refresh', 'status': 'running'}
        audit.task = asyncio.create_task(failed())
    monkeypatch.setattr(audit, 'start', start)
    with pytest.raises(ValueError, match='最新上传状态未能核对完成'):
        await local_app.state.audit_moves.preview({'reportId': report['id'], 'uploadedDirectory': str(root/'已上传'),
            'magnetMissingDirectory': str(root/'磁盘吊未上传'), 'bedMissingDirectory': str(root/'车斗未上传'),
            'refreshReport': True})
    assert local_app.state.audit_moves.latest() is None and (root / 'truck.mp4').read_bytes() == b'video'


@pytest.mark.asyncio
async def test_concurrent_preview_is_rejected_until_latest_log_check_finishes(local_app, tmp_path, monkeypatch):
    root = tmp_path / 'videos'; root.mkdir(); (root / 'truck.mp4').write_bytes(b'video')
    report = report_for(local_app, root)
    audit = local_app.state.upload_audit
    entered, release = asyncio.Event(), asyncio.Event()
    def start(directory, recursive):
        async def refresh():
            entered.set(); await release.wait()
            fresh = report_for(local_app, root)
            fresh['id'] = audit.run['id']
            audit.run.update(status='complete')
            audit.save()
        audit.run = {'id': 'refreshing', 'status': 'running'}
        audit.task = asyncio.create_task(refresh())
    monkeypatch.setattr(audit, 'start', start)
    body = {'reportId': report['id'], 'uploadedDirectory': str(root/'已上传'),
        'magnetMissingDirectory': str(root/'磁盘吊未上传'), 'bedMissingDirectory': str(root/'车斗未上传'),
        'refreshReport': True}
    pending = asyncio.create_task(local_app.state.audit_moves.preview(body))
    await entered.wait()
    try:
        assert local_app.state.audit_moves.busy
        with pytest.raises(ValueError, match='核对或移动正在进行'):
            await local_app.state.audit_moves.preview(body)
    finally:
        release.set()
    result = await pending
    assert result['reportId'] == 'refreshing' and result['counts']['planned'] == 1
    assert not local_app.state.audit_moves.busy and not (root / '已上传').exists()


@pytest.mark.asyncio
async def test_collision_after_preview_and_changed_source_never_overwrite_or_remove(local_app, tmp_path):
    root = tmp_path / 'videos'; root.mkdir()
    source = root / 'truck.mp4'; source.write_bytes(b'original')
    report = report_for(local_app, root); plan = await preview(local_app, root, report)
    destination = root / '已上传/truck.mp4'; destination.parent.mkdir()
    destination.write_bytes(b'someone-elses-video')
    manager = local_app.state.audit_moves
    manager.start(plan['id'], 'collision'); await manager.task
    assert manager.get(plan['id'])['counts']['failed'] == 1
    assert source.read_bytes() == b'original' and destination.read_bytes() == b'someone-elses-video'
    destination.unlink()
    source.write_bytes(b'replaced-after-preview')
    manager.start(plan['id'], 'collision'); await manager.task
    assert source.read_bytes() == b'replaced-after-preview' and not destination.exists()


@pytest.mark.asyncio
async def test_copy_between_disks_is_verified_and_removes_source_only_after_publication(local_app, tmp_path, monkeypatch):
    root = tmp_path / 'videos'; root.mkdir()
    source = root / 'truck.mp4'; source.write_bytes(b'video' * 1000)
    report = report_for(local_app, root); plan = await preview(local_app, root, report)
    monkeypatch.setattr(audit_moves, 'same_device', lambda *args: False)
    original = audit_moves.finish_source
    observed = []
    def finish(item):
        observed.append((source.exists(), Path(item['destination']).is_file(), item.get('sha256')))
        return original(item)
    monkeypatch.setattr(audit_moves, 'finish_source', finish)
    manager = local_app.state.audit_moves
    manager.start(plan['id'], 'cross-device'); await manager.task
    assert manager.get(plan['id'])['status'] == 'complete'
    assert observed[0] == (True, True, hashlib.sha256(b'video' * 1000).hexdigest())
    assert not source.exists() and (root / '已上传/truck.mp4').read_bytes() == b'video' * 1000
    assert not list(root.rglob('*.moving'))


@pytest.mark.asyncio
@pytest.mark.parametrize('cross_device', [False, True])
async def test_lost_move_response_recovers_by_recorded_file_version_without_repeating(local_app, tmp_path, cross_device):
    root = tmp_path / 'videos'; root.mkdir()
    source = root / 'truck.mp4'; source.write_bytes(b'final-video')
    old = registered(local_app, source)
    report = report_for(local_app, root); plan = await preview(local_app, root, report)
    item = plan['items'][0]; plan.update(status='running',requestId='lost-response'); item['state'] = 'moving'
    manager = local_app.state.audit_moves
    if cross_device:
        destination = Path(item['destination'])
        item['temporary'] = str(destination.parent / f".{destination.name}.{item['reservation']['token']}.moving")
        item.update(copy_verified(item)); manager.save(plan)
        publish_copied(item)  # Publication succeeded, but its response was lost.
        assert source.exists()
    else:
        manager.save(plan); move_direct(item)  # Rename succeeded before journal completion.
        assert not source.exists()
    restarted = AuditMoveManager(local_app.state.store, local_app.state.upload_audit, local_app.state.videos)
    assert restarted.latest()['status'] == 'interrupted'
    restarted.start(plan['id'], 'lost-response'); await restarted.task
    assert restarted.get(plan['id'])['status'] == 'complete' and not source.exists()
    assert local_app.state.videos.store.get(old['id'])['versions'][0]['filePath'] == item['destination']
    assert Path(item['destination']).read_bytes() == b'final-video'


@pytest.mark.asyncio
async def test_disk_full_and_hash_mismatch_keep_original_and_cleanup_private_temporary(local_app, tmp_path, monkeypatch):
    root = tmp_path / 'videos'; root.mkdir()
    source = root / 'truck.mp4'; source.write_bytes(b'final-video')
    report = report_for(local_app, root); plan = await preview(local_app, root, report)
    monkeypatch.setattr(audit_moves, 'same_device', lambda *args: False)
    real_usage = audit_moves.shutil.disk_usage
    monkeypatch.setattr(audit_moves.shutil, 'disk_usage', lambda path: type('Disk', (), {'free': 0})())
    manager = local_app.state.audit_moves
    manager.start(plan['id'], 'disk-retry'); await manager.task
    assert manager.get(plan['id'])['status'] == 'attention' and source.exists()
    assert not (root / '已上传/truck.mp4').exists()
    monkeypatch.setattr(audit_moves.shutil, 'disk_usage', real_usage)
    real_digest = audit_moves.file_digest
    monkeypatch.setattr(audit_moves, 'file_digest', lambda path: 'wrong-digest')
    manager.start(plan['id'], 'disk-retry'); await manager.task
    assert source.read_bytes() == b'final-video' and not list(root.rglob('*.moving'))
    monkeypatch.setattr(audit_moves, 'file_digest', real_digest)
    manager.start(plan['id'], 'disk-retry'); await manager.task
    assert manager.get(plan['id'])['status'] == 'complete' and not source.exists()


@pytest.mark.asyncio
async def test_paths_symlinks_protected_folder_and_old_reports_are_rejected(local_app, tmp_path, monkeypatch):
    root = tmp_path / 'videos'; root.mkdir(); (root / 'truck.mp4').write_bytes(b'video')
    report = report_for(local_app, root)
    manager = local_app.state.audit_moves
    for uploaded,magnet,bed in ((root,root/'magnet',root/'bed'), (root/'out',root/'out',root/'bed'),
            (root/'out',root/'out/sub',root/'bed'), (root/'out',root/'magnet',root/'magnet'),
            (root/'out',root/'magnet/sub',root/'magnet'), (root/'out',root/'magnet',root/'out/sub')):
        with pytest.raises(ValueError):
            await manager.preview({'reportId':report['id'],'uploadedDirectory':str(uploaded),
                'magnetMissingDirectory':str(magnet),'bedMissingDirectory':str(bed)})
    link = tmp_path / 'linked'; link.symlink_to(root, target_is_directory=True)
    with pytest.raises(ValueError, match='符号链接'):
        await manager.preview({'reportId':report['id'],'uploadedDirectory':str(link/'done'),
            'magnetMissingDirectory':str(root/'magnet'),'bedMissingDirectory':str(root/'bed')})
    report['files'][0].pop('fileVersion')
    with pytest.raises(ValueError, match='重新读取目录'):
        await preview(local_app,root,report)
    monkeypatch.setattr(Path,'home',classmethod(lambda cls:tmp_path))
    protected = tmp_path / 'Desktop/视频抽帧'; protected.mkdir(parents=True); (protected/'truck.mp4').write_bytes(b'protected')
    report = report_for(local_app, protected)
    with pytest.raises(ValueError, match='受保护'):
        await preview(local_app, protected, report)
    assert (protected/'truck.mp4').read_bytes() == b'protected'


@pytest.mark.asyncio
async def test_api_preview_conflict_and_confirmation_serialize_with_audits(local_app, tmp_path):
    root = tmp_path / 'videos'; root.mkdir(); (root / 'truck.mp4').write_bytes(b'video')
    report = report_for(local_app, root)
    body = {'reportId': report['id'], 'uploadedDirectory': str(root/'done'),
        'magnetMissingDirectory': str(root/'magnet'), 'bedMissingDirectory': str(root/'bed')}
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=local_app),base_url='http://localhost') as client:
        old_body = {'reportId': report['id'], 'uploadedDirectory': str(root/'done'), 'unuploadedDirectory': str(root/'pending')}
        assert (await client.post('/api/upload-audit/moves/preview',json=old_body)).status_code == 422
        assert (await client.post('/api/upload-audit/moves/preview',json={**body,'reportId':'old-report'})).status_code == 409
        plan = (await client.post('/api/upload-audit/moves/preview',json=body)).json()
        assert not (root/'done').exists()
        started = await client.post(f"/api/upload-audit/moves/{plan['id']}/confirm",json={'requestId':'api-confirm'})
        assert started.status_code == 202
        local_app.state.upload_audit.client.access = 'test-only-token'
        with pytest.raises(ValueError):
            local_app.state.upload_audit.start(str(root))
        await local_app.state.audit_moves.task
        state = (await client.get('/api/upload-audit/moves')).json()
        assert state['plan']['status'] == 'complete'
        replay = await client.post(f"/api/upload-audit/moves/{plan['id']}/confirm",json={'requestId':'api-confirm'})
        assert replay.status_code == 202
        assert not (root/'truck.mp4').exists() and (root/'done/truck.mp4').is_file()
