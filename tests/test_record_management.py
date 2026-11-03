"""Record controls use isolated databases and never touch real media/platforms."""
import copy

import httpx
import pytest

from app.main import create_app
from app.submissions import SubmissionLog
from tests.test_slicing_performance import close_app


async def delete_records(app, client, url, **kwargs):
    response = await client.post(url, **kwargs)
    if response.status_code == 202:
        await app.state.record_deletion.tick()
    return response


def seed_slice(app, path, identity='bed', revision=3):
    category = 'bed' if identity != 'magnet' else 'magnet'
    item = {'id': identity, 'name': path.name, 'path': str(path), 'runId': 'run', 'revision': revision,
            'fileVersion': {'size': path.stat().st_size}, 'sourceVideoId': 'source-' + identity,
            'sourceDatasetId': '27912' if category == 'bed' else '27944', 'state': 'complete',
            'categories': {category: {'state': 'complete', 'taskId': 'task-' + identity, 'actualImages': 150,
                'intervals': [{'start': 30, 'end': 900}], 'intervalSeconds': 6, 'targetDatasetId': '27892',
                'preSubmitImageIds': ['private-evidence'], 'outputImageIds': ['private-output']},
                ('magnet' if category == 'bed' else 'bed'): {'state': 'not_applicable'}}}
    app.state.slicing.store.save('items', item)
    return item


@pytest.mark.asyncio
async def test_collection_dates_open_paged_details_from_exact_job(tmp_path):
    app = create_app(data_dir=tmp_path / 'db')
    items = [{'videoName': f'truck-{i}', 'startTime': '2026-10-21 23:55:00',
              'endTime': '2026-10-22 00:55:00', 'status': 'submitted', 'taskId': f't-{i}'} for i in range(53)]
    job = {'id': 'job', 'createdAt': '2026-10-21T12:00:00Z', 'status': 'complete', 'events': [], 'items': items}
    app.state.store.save(job)
    row = SubmissionLog(app.state.store.db).add('collection', '采集提交', {'jobId': 'job'})
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://127.0.0.1') as client:
        summary = (await client.get('/api/submissions?kind=collection')).json()[0]
        assert summary['total'] == 53 and 'items' not in summary['detail']
        detail = (await client.get('/api/submissions/' + row['id'], params={'page': 2})).json()['detail']
        assert len(detail['items']) == 20 and detail['items'][0]['videoName'] == 'truck-20'
        assert detail['items'][0]['endTime'] == '2026-10-22 00:55:00'
        assert detail['pages'] == 3 and detail['total'] == 53
        assert (await client.get('/api/submissions/' + row['id'], params={'page': 999})).json()['detail']['page'] == 3
        assert (await client.get('/api/submissions/' + row['id'], params={'page_size': 1000})).status_code == 422
        assert (await client.get('/api/submissions/missing')).status_code == 404
    assert app.state.store.get('job') == {**job, 'updatedAt': app.state.store.get('job')['updatedAt']}
    await close_app(app)


@pytest.mark.asyncio
async def test_slicing_receipt_exact_scope_survives_later_run_and_hiding(tmp_path):
    app = create_app(data_dir=tmp_path / 'db')
    path = tmp_path / 'source.mp4'; path.write_bytes(b'keep-original')
    item = seed_slice(app, path)
    seed_slice(app, path, 'magnet')
    manager = app.state.slicing
    manager.store.save('requests', {'id': 'click', 'kind': 'execute-all', 'receiptRunId': 'run', 'receiptItemIds': ['bed']})
    manager.store.save('runs', {'id': 'run', 'requestId': 'later-click', 'items': ['magnet']})
    row = SubmissionLog(app.state.store.db).add('slicing', '全部切片', {'requestId': 'click'})
    manager.visibility.hide(manager.store, 'remove', [{'id': 'bed', 'expectedRevision': 3}])
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://127.0.0.1') as client:
        detail = (await client.get('/api/submissions/' + row['id'])).json()
        assert detail['total'] == 1 and detail['completed'] == 1
        assert [i['id'] for i in detail['detail']['items']] == ['bed']
        visible = detail['detail']['items'][0]
        assert visible['intervals'] == item['categories']['bed']['intervals'] and visible['actualImages'] == 150
        assert 'private-evidence' not in str(detail) and 'private-output' not in str(detail)
        assert [i['id'] for i in (await client.get('/api/slicing')).json()['items']] == ['magnet']
    await close_app(app)


@pytest.mark.asyncio
async def test_old_receipt_never_claims_current_unrelated_batch(tmp_path):
    app = create_app(data_dir=tmp_path / 'db')
    path = tmp_path / 'source.mp4'; path.write_bytes(b'keep')
    seed_slice(app, path)
    app.state.slicing.store.save('runs', {'id': 'run', 'requestId': 'current', 'items': ['bed']})
    log = SubmissionLog(app.state.store.db)
    old = log.add('slicing', '全部切片', {'requestId': 'old', 'message': '当时的启动信息'})
    current = log.add('slicing', '全部切片', {'requestId': 'current'})
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://127.0.0.1') as client:
        first = (await client.get('/api/submissions/' + old['id'])).json()
        assert first['detail']['items'] == [] and first['detail']['notice']
        assert first['detail']['message'] == '当时的启动信息' and first['status'] == 'recorded'
        second = (await client.get('/api/submissions/' + current['id'])).json()
        assert second['detail']['items'][0]['id'] == 'bed'
    await close_app(app)


@pytest.mark.asyncio
async def test_remove_card_is_atomic_idempotent_and_keeps_all_processing_evidence(tmp_path):
    app = create_app(data_dir=tmp_path / 'db')
    path = tmp_path / 'source.mp4'; path.write_bytes(b'keep-video')
    for category in ('bed', 'magnet'): seed_slice(app, path, category)
    store = app.state.slicing.store
    store.save('observations', {'id': 'observation', 'label': 'target', 'time': 30})
    app.state.media_control.refresh()
    before = copy.deepcopy(store.list('items'))
    payload = {'requestId': 'remove-pair', 'selections': [{'id': r['id'], 'expectedRevision': r['revision']} for r in before]}
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://127.0.0.1') as client:
        stale = copy.deepcopy(payload); stale['selections'][1]['expectedRevision'] = 2
        assert (await delete_records(app, client, '/api/slicing/records/remove', json=stale)).status_code == 409
        assert len((await client.get('/api/slicing')).json()['items']) == 2
        response = await delete_records(app, client, '/api/slicing/records/remove', json=payload)
        assert response.status_code == 202 and (await client.get('/api/slicing')).json()['items'] == []
        assert (await client.get('/api/slicing')).json()['hiddenRecordCount'] == 2
        assert (await delete_records(app, client, '/api/slicing/records/remove', json=payload)).status_code == 202
        different = {**payload, 'selections': payload['selections'][:1]}
        assert (await delete_records(app, client, '/api/slicing/records/remove', json=different)).status_code == 409
        assert (await delete_records(app, client, '/api/slicing/records/remove', json={**payload, 'selections': []})).status_code == 422
    assert store.list('items') == before and path.read_bytes() == b'keep-video'
    assert store.get('observations', 'observation')['label'] == 'target'
    await close_app(app)


@pytest.mark.asyncio
async def test_hidden_items_survive_worker_save_restart_and_restore_without_recreation(tmp_path):
    app = create_app(data_dir=tmp_path / 'db')
    path = tmp_path / 'source.mp4'; path.write_bytes(b'keep')
    item = seed_slice(app, path)
    manager = app.state.slicing
    manager.visibility.hide(manager.store, 'hide', [{'id': 'bed', 'expectedRevision': 3}])
    # An old background snapshot cannot overwrite the independent visibility ledger.
    item.update(message='后台仍继续', revision=4)
    manager.store.save('items', item)
    assert manager.state()['items'] == []
    await close_app(app)
    app = create_app(data_dir=tmp_path / 'db')
    manager = app.state.slicing
    assert manager.state()['items'] == [] and len(manager.store.list('items')) == 1
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://127.0.0.1') as client:
        body = {'requestId': 'undo', 'removalId': 'hide'}
        response = await client.post('/api/slicing/records/restore', json=body)
        assert response.status_code == 410 and manager.state()['items'] == []
        assert (await client.post('/api/slicing/records/restore', json=body)).status_code == 410
        assert manager.store.get('items', 'bed')['sourceVideoId'] == 'source-bed'
        assert path.exists()
    await close_app(app)


@pytest.mark.asyncio
async def test_stale_undo_cannot_restore_a_more_recent_deletion(tmp_path):
    app = create_app(data_dir=tmp_path / 'db')
    path = tmp_path / 'source.mp4'; path.write_bytes(b'keep')
    seed_slice(app, path)
    manager = app.state.slicing
    choices = [{'id': 'bed', 'expectedRevision': 3}]
    manager.visibility.hide(manager.store, 'first-hide', choices)
    manager.visibility.restore('first-undo', 'first-hide')
    manager.visibility.hide(manager.store, 'second-hide', choices)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://127.0.0.1') as client:
        response = await client.post('/api/slicing/records/restore', json={'requestId': 'late-undo', 'removalId': 'first-hide'})
        assert response.status_code == 410 and manager.state()['items'] == []
        assert (await client.post('/api/slicing/records/restore', json={'requestId': 'missing', 'removalId': 'absent'})).status_code == 410
    await close_app(app)


@pytest.mark.asyncio
async def test_new_slice_submission_records_single_selection_and_request_replay(tmp_path, monkeypatch):
    app = create_app(data_dir=tmp_path / 'db')
    path = tmp_path / 'source.mp4'; path.write_bytes(b'keep')
    seed_slice(app, path)
    seed_slice(app, path, 'magnet')
    async def start(*args):
        return {'run': {'id': 'run', 'items': ['bed', 'magnet'], 'message': '单条已启动'}}
    monkeypatch.setattr(app.state.slicing, 'start', start)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://127.0.0.1') as client:
        group=(await client.get('/api/video-processing')).json()['groups']
        # Finished local-only history stays in the slicing records. Its durable
        # control identity still exists for a versioned, explicit command.
        app.state.media_control.refresh()
        item=app.state.slicing.store.get('items','bed')
        payload = {'requestId': 'single-click', 'expectedRevision': 3, 'generation':0, 'action': 'start', 'itemId': 'bed'}
        url='/api/video-processing/'+item['mediaId']+'/actions'
        assert (await client.post(url, json=payload)).status_code == 200
        assert (await client.post(url, json=payload)).status_code == 200
        receipts = (await client.get('/api/submissions?kind=slicing')).json()
        assert len(receipts) == 1 and receipts[0]['title'] == '单条切片'
        detail = (await client.get('/api/submissions/' + receipts[0]['id'])).json()
        assert [i['id'] for i in detail['detail']['items']] == ['bed']
    await close_app(app)


@pytest.mark.asyncio
async def test_collection_record_remove_and_undo_work_offline_without_touching_corrections_or_files(tmp_path, monkeypatch):
    app = create_app(data_dir=tmp_path / 'db')
    manager = app.state.videos
    path = tmp_path / 'final.mp4'; path.write_bytes(b'preserve-uploaded-final')
    row = {'id': 'c' * 24, 'jobId': 'job', 'name': 'truck', 'plate': 'truck', 'revision': 9,
           'qualityStart': '2026-09-30 00:00:00', 'qualityEnd': '2026-09-30 01:00:00',
           'requests': {'manual': {'kind': 'correction'}}, 'activeRevision': 2, 'acceptedRevision': 2,
           'versions': [{'revision': 2, 'state': 'saved', 'filePath': str(path),
                         'startTime': '2026-09-30 00:15:21', 'durationSeconds': 3601}]}
    manager.store.save(row)
    before = copy.deepcopy(manager.store.get(row['id']))
    cache = manager.work / row['id']; cache.mkdir(); (cache / 'v2.part').write_bytes(b'keep-checkpoint')
    app.state.workspace.config.update(enabled=True, root=str(tmp_path / 'unplugged'))
    app.state.workspace.online = False
    async def forbid_io(*args):
        raise AssertionError('record-only removal must not mutate files')
    monkeypatch.setattr(manager, 'file_operation', forbid_io)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://127.0.0.1') as client:
        body = {'requestId': 'hide-collection', 'expectedRevision': 9}
        response = await delete_records(app, client, f'/api/videos/{row["id"]}/hide-record', json=body)
        assert response.status_code == 202
        assert (await client.get('/api/videos')).json()['totalCount'] == 0
        assert (await delete_records(app, client, f'/api/videos/{row["id"]}/hide-record', json=body)).status_code == 202
        assert (await client.post('/api/videos/records/restore', json={
            'requestId': 'undo-collection', 'removalId': 'hide-collection'})).status_code == 410
        assert (await client.get('/api/videos')).json()['totalCount'] == 0
    assert manager.store.get(row['id']) == before
    assert path.read_bytes() == b'preserve-uploaded-final' and (cache / 'v2.part').read_bytes() == b'keep-checkpoint'
    await close_app(app)


@pytest.mark.asyncio
async def test_entry_page_revalidates_after_ui_update(tmp_path, monkeypatch):
    monkeypatch.setattr('app.main.ROOT', tmp_path)
    dist = tmp_path / 'dist'; dist.mkdir()
    index = dist / 'index.html'; index.write_text('<html>first</html>')
    app = create_app(data_dir=tmp_path / 'db')
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://127.0.0.1') as client:
        old = await client.get('/')
        assert old.status_code == 200 and old.headers['cache-control'] == 'no-cache, must-revalidate'
        index.write_text('<html>updated entry</html>')
        new = await client.get('/', headers={'If-None-Match': old.headers['etag']})
        assert new.status_code == 200 and 'updated entry' in new.text
    await close_app(app)


@pytest.mark.asyncio
async def test_date_delete_receipts_and_children_is_soft_exact_and_survives_restart(tmp_path):
    app = create_app(data_dir=tmp_path / 'db')
    store = app.state.store
    job = {'id': 'job-date', 'createdAt': '2026-10-25T16:00:00Z', 'status': 'complete', 'events': [],
           'items': [{'key': 'a', 'recordId': 'quality-a', 'plate': 'car', 'videoName': 'car',
               'startTime': '2026-09-29 08:05:00', 'endTime': '2026-09-29 09:05:00',
               'checkStartTime': '2026-09-29 08:00:00', 'checkCompleteTime': '2026-09-29 09:00:00',
               'photoCount': 1, 'inspectionDurationSeconds': 3600,
               'status': 'submitted', 'taskId': 'real-task'}]}
    store.save(job)
    log = SubmissionLog(store.db)
    row = log.add('collection', '采集提交', {'jobId': job['id']}, submitted_at=job['createdAt'])
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://127.0.0.1') as client:
        item = (await client.get('/api/submissions/' + row['id'])).json()['detail']['items'][0]
        removal = {'requestId': 'child-remove', 'selections': [{'id': item['deletionKey'], 'expectedRevision': 0}]}
        assert (await delete_records(app, client, '/api/submissions/items/remove', json=removal)).status_code == 202
        assert (await delete_records(app, client, '/api/submissions/items/remove', json=removal)).status_code == 202
        assert (await client.get('/api/submissions/' + row['id'])).json()['total'] == 0
        assert store.get(job['id'])['items'][0]['taskId'] == 'real-task'
        date = {'requestId': 'date-remove', 'selections': [{'id': row['id'], 'expectedRevision': 0}]}
        assert (await delete_records(app, client, '/api/submissions/records/remove', json=date)).status_code == 202
        newer = log.add('collection', '新到记录', {}, submitted_at=job['createdAt'])
        assert (await delete_records(app, client, '/api/submissions/records/remove', json=date)).status_code == 202
        assert [r['id'] for r in (await client.get('/api/submissions')).json()] == [newer['id']]
        assert (await client.get('/api/submissions/' + row['id'])).status_code == 404
        restore = {'requestId': 'date-restore', 'removalId': 'date-remove'}
        assert (await client.post('/api/submissions/records/restore', json=restore)).status_code == 410
        assert (await client.get('/api/submissions/' + row['id'])).status_code == 404
        assert (await client.post('/api/submissions/items/restore', json={'requestId': 'child-restore', 'removalId': 'child-remove'})).status_code == 410
        assert (await client.get('/api/submissions/' + row['id'])).status_code == 404
        videos = (await client.get('/api/videos')).json()['videos']
        assert videos[0]['submittedAt'] == job['createdAt']  # not capture or sync time
    await close_app(app)
    app = create_app(data_dir=tmp_path / 'db')
    assert len(SubmissionLog(app.state.store.db).list('collection')) == 2
    assert len(SubmissionLog(app.state.store.db).visibility.hidden()) == 1
    await close_app(app)


@pytest.mark.asyncio
async def test_merged_slice_date_removal_and_restore_are_atomic_without_changing_work(tmp_path):
    app = create_app(data_dir=tmp_path / 'db')
    path = tmp_path / 'video.mp4'; path.write_bytes(b'original')
    item = seed_slice(app, path)
    log = SubmissionLog(app.state.store.db)
    row = log.add('slicing', '全部切片', {'itemIds': [item['id']]}, submitted_at='2026-10-25T16:00:00Z')
    app.state.media_control.refresh()
    before = copy.deepcopy(app.state.slicing.store.list('items'))
    body = {'requestId': 'merged-remove', 'selections': [{'id': item['id'], 'expectedRevision': 3}],
            'receipts': [{'id': row['id'], 'expectedRevision': 0}]}
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://127.0.0.1') as client:
        state = (await client.get('/api/slicing')).json()
        assert state['items'][0]['submittedAt'] == row['submittedAt']
        assert state['submissionReceipts'][0]['itemIds'] == [item['id']]
        stale = {**body, 'receipts': [{'id': row['id'], 'expectedRevision': 1}]}
        assert (await delete_records(app, client, '/api/slicing/records/remove-group', json=stale)).status_code == 409
        assert len((await client.get('/api/slicing')).json()['items']) == 1
        result = await delete_records(app, client, '/api/slicing/records/remove-group', json=body)
        assert result.status_code == 202
        assert (await client.get('/api/slicing')).json()['items'] == []
        assert (await client.get('/api/slicing')).json()['submissionReceipts'] == []
        assert (await delete_records(app, client, '/api/slicing/records/remove-group', json=body)).status_code == 202
        restore = {'requestId': 'merged-restore', 'removalId': body['requestId']}
        assert (await client.post('/api/slicing/records/restore-group', json=restore)).status_code == 410
        assert (await client.post('/api/slicing/records/restore-group', json=restore)).status_code == 410
        assert (await client.get('/api/slicing')).json()['items'] == []
    assert app.state.slicing.store.list('items') == before and path.read_bytes() == b'original'
    await close_app(app)


@pytest.mark.asyncio
async def test_video_date_snapshot_delete_and_exact_submit_dates_do_not_change_corrections(tmp_path):
    app = create_app(data_dir=tmp_path / 'db')
    manager = app.state.videos
    rows = []
    for identity in ('first', 'second'):
        row = {'id': identity, 'jobId': 'unknown', 'name': identity, 'revision': 2, 'activeRevision': 1, 'requests': {},
               'plate': identity, 'qualityStart': '2026-09-30 00:00:00', 'qualityEnd': '2026-09-30 01:00:00',
               'acceptedRevision': 0, 'versions': [{'revision': 0, 'state': 'saved', 'startTime': '2026-09-30 00:00:00'},
                   {'revision': 1, 'state': 'waiting', 'startTime': '2026-10-01 03:23:45', 'taskId': 'corrected-' + identity}]}
        manager.store.save(row); rows.append(row)
    original = copy.deepcopy(manager.store.list())
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://127.0.0.1') as client:
        assert all(not row['submittedAt'] for row in (await client.get('/api/videos')).json()['videos'])
        selections = [{'id': r['id'], 'expectedRevision': 2} for r in rows]
        stale = [selections[0], {**selections[1], 'expectedRevision': 1}]
        assert (await delete_records(app, client, '/api/videos/records/remove', json={'requestId': 'date', 'selections': stale})).status_code == 409
        assert len(manager.state()['videos']) == 2
        body = {'requestId': 'date', 'selections': selections}
        assert (await delete_records(app, client, '/api/videos/records/remove', json=body)).status_code == 202
        assert manager.state()['videos'] == []
        assert manager.store.list() == original
        # Independent visibility ledger allows a legitimate worker progress update.
        row = manager.store.get('second'); row['revision'] = 3; manager.store.save(row)
        assert (await client.post('/api/videos/records/restore', json={'requestId': 'undo-date', 'removalId': 'date'})).status_code == 410
        assert manager.store.get('second')['versions'][1]['startTime'] == '2026-10-01 03:23:45'
    await close_app(app)


@pytest.mark.asyncio
async def test_submit_dates_use_exact_operations_not_capture_last_update_or_later_rescan(tmp_path):
    app = create_app(data_dir=tmp_path / 'db')
    path = tmp_path / 'capture_20260101000000.mp4'; path.write_bytes(b'keep')
    item = seed_slice(app, path)
    item.update(createdAt='2026-01-01T00:00:00Z', updatedAt='2026-10-26T00:00:00Z')
    item['categories']['bed']['operationId'] = 'confirmed-operation'
    app.state.slicing.store.save('items', item)
    app.state.slicing.store.save('operations', {'id': 'confirmed-operation', 'createdAt': '2026-10-20T16:00:00Z'})
    log = SubmissionLog(app.state.store.db)
    log.add('slicing', '后续扫描', {'itemIds': [item['id']]}, submitted_at='2026-10-25T16:00:00Z')
    unknown = seed_slice(app, path, 'no-submit'); unknown['name'] = 'filename_20261026000000.mp4'
    app.state.slicing.store.save('items', unknown)
    public = {row['id']: row for row in app.state.slicing.state()['items']}
    assert public['bed']['submittedAt'] == '2026-10-20T16:00:00Z'
    assert public['bed']['categories']['bed']['submittedAt'] == '2026-10-20T16:00:00Z'
    assert public['no-submit']['submittedAt'] == ''
    assert app.state.slicing.store.get('items', 'bed')['fileVersion'] == item['fileVersion']
    await close_app(app)


@pytest.mark.asyncio
async def test_mixed_collection_record_batch_is_atomic_replayable_and_reversible(tmp_path):
    app = create_app(data_dir=tmp_path / 'db')
    store = app.state.store
    job = {'id': 'batch-job', 'createdAt': '2026-10-25T16:00:00Z', 'status': 'complete', 'events': [],
           'items': [{'key': 'car-a', 'videoName': 'car-a', 'startTime': '2026-10-25 08:00:00',
                      'endTime': '2026-10-25 09:00:00', 'status': 'submitted', 'taskId': 'task-a'},
                     {'key': 'car-b', 'videoName': 'car-b', 'startTime': '2026-10-25 09:00:00',
                      'endTime': '2026-10-25 10:00:00', 'status': 'submitted', 'taskId': 'task-b'}]}
    store.save(job)
    log = SubmissionLog(store.db)
    first = log.add('collection', '采集一', {'jobId': job['id']})
    second = log.add('collection', '采集二', {'items': []})
    original = copy.deepcopy(store.get(job['id']))
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://127.0.0.1') as client:
        item = (await client.get('/api/submissions/' + first['id'])).json()['detail']['items'][0]
        body = {'requestId': 'mixed-delete',
                'selections': [{'id': item['deletionKey'], 'expectedRevision': item['revision']}],
                'receipts': [{'id': second['id'], 'expectedRevision': 0}]}
        stale = {**body, 'receipts': [{'id': second['id'], 'expectedRevision': 1}]}
        assert (await delete_records(app, client, '/api/submissions/remove-group', json=stale)).status_code == 409
        assert (await client.get('/api/submissions/' + first['id'])).json()['total'] == 2
        assert (await delete_records(app, client, '/api/submissions/remove-group', json=body)).status_code == 202
        assert (await delete_records(app, client, '/api/submissions/remove-group', json=body)).status_code == 202
        assert (await client.get('/api/submissions/' + first['id'])).json()['total'] == 1
        assert (await client.get('/api/submissions/' + second['id'])).status_code == 404
        undo = {'requestId': 'mixed-undo', 'removalId': 'mixed-delete'}
        assert (await client.post('/api/submissions/restore-group', json=undo)).status_code == 410
        assert (await client.post('/api/submissions/restore-group', json=undo)).status_code == 410
        assert (await client.get('/api/submissions/' + first['id'])).json()['total'] == 1
        assert (await client.get('/api/submissions/' + second['id'])).status_code == 404
    assert store.get(job['id']) == original
    await close_app(app)
