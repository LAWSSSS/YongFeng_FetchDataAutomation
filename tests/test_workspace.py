"""All media and platform receipts here are isolated; no real service calls."""
import asyncio
import copy
import json
from pathlib import Path

import httpx
import pytest

from app.main import create_app
from app.audit_moves import source_fingerprint
from app.slicing import source_version
from app.videos import Conflict
from app.workspace_files import BRANCHES, choose_root, create_root, scan_managed
from tests.test_slicing_performance import close_app


@pytest.fixture
def env(tmp_path):
    app = create_app(data_dir=tmp_path / 'db')
    w = app.state.workspace
    return app, w


def pair(w, folder, states=('pending', 'pending'), name='truck.mp4'):
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / name
    path.write_bytes(b'video-version-fixture')
    version = source_version(path)
    for category, dataset in (('bed', '27912'), ('magnet', '27944')):
        state = states[0 if category == 'bed' else 1]
        item = {'id': category + str(folder), 'path': str(path), 'name': name,
                'fileVersion': version, 'sourceDatasetId': dataset, 'sourceVideoId': dataset + '001',
                'state': state, 'revision': 3, 'categories': {category: {'state': state, 'actualImages': 1},
                ('magnet' if category == 'bed' else 'bed'): {'state': 'not_applicable'}}}
        w.slicing.store.save('items', item)
    return path


def proofs(w, path, tmp_path):
    version = source_fingerprint(path)
    for dataset in ('27912', '27944'):
        identity = 'upload-' + dataset
        item = {'id': identity, 'dataset': dataset, 'path': str(path), 'name': path.name,
                'size': version['size'], 'fileVersion': version, 'state': 'complete',
                'remoteVideoId': dataset + '-result', 'jobId': dataset + '-job',
                'batchId': dataset + '-batch', 'object': {'key': dataset + '-object'}}
        w.uploads.store.save('items', item)
        w.uploads.store.save('batches', {'id': item['batchId'], 'dataset': dataset, 'items': [identity],
                'keys': [item['object']['key']], 'jobId': item['jobId'], 'state': 'complete'})
    for item in w.slicing.store.list('items'):
        category = 'bed' if item['sourceDatasetId'] == '27912' else 'magnet'
        folder = tmp_path / 'outputs' / category
        folder.mkdir(parents=True, exist_ok=True)
        archive, photo = folder / 'old.zip', folder / 'one.jpg'
        archive.write_bytes(b'zip-fixture')
        photo.write_bytes(b'photo-fixture')
        package = {'id': 'p-' + category, 'itemId': item['id'], 'category': category,
                   'dataset': category, 'state': 'complete', 'verifiedJobId': 'image-job-' + category, 'jobId': 'image-job-' + category,
                   'path': str(archive), 'fileVersion': source_fingerprint(archive),
                   'imageDir': str(folder), 'images': [photo.name],
                   'imageVersions': {photo.name: source_fingerprint(photo)}, 'outputImageIds': [category + '-image'],
                   'imageCount': 1, 'actualImages': 1, 'videoPath': str(path), 'videoFileVersion': version}
        w.slicing.packages.store.save('items', package)
    w.audit.client.access = 'test-only'
    async def inventory(dataset):
        return [{'id': dataset + '-result', 'size': version['size']}]
    w.uploads.remote.videos = inventory


def downloaded_original(w, path, identity='cleanup-download'):
    """Exact isolated capture receipt; not authority for an arbitrary user source."""
    w.videos.store.save({'id': identity, 'name': path.name, 'revision': 0,
        'activeRevision': 0, 'acceptedRevision': 0, 'versions': [{
            'revision': 0, 'state': 'saved', 'filePath': str(path), 'fileVersion': source_version(path),
            'taskId': identity + '-task', 'resultId': identity + '-result'}]})


async def enable(w, parent, pilot=False):
    p = await w.preview(str(parent), w.config['revision'], 'preview-' + str(w.config['revision']))
    await w.apply(p['id'], w.config['revision'], 'apply-' + str(w.config['revision']), pilot)
    await w.task
    assert w.enabled, w.state()['migration']
    return p


@pytest.mark.asyncio
async def test_preserve_uploaded_invalidates_preview_and_keeps_queues(env, tmp_path):
    app, w = env
    path = pair(w, tmp_path / 'old', ('complete', 'complete'))
    proofs(w, path, tmp_path)
    before = (w.slicing.store.list('items'), w.uploads.store.list('items'), w.slicing.packages.store.list('items'))
    plan = await w.preview(str(tmp_path), 0, 'preview-before-retention')
    result = await w.preserve_uploaded(0, 'keep-uploaded')
    assert result['revision'] == 1 and result['counts']['preserved'] == 1
    assert w.get('plans', plan['id'])['state'] == 'superseded'
    assert Path(w.get('requests', 'keep-uploaded')['backup']).exists()
    assert path.exists() and not (tmp_path / '视频工作区').exists()
    assert before == (w.slicing.store.list('items'), w.uploads.store.list('items'), w.slicing.packages.store.list('items'))
    with pytest.raises(Conflict):
        await w.apply(plan['id'], 0, 'apply-old-preview')
    fresh = await w.preview(str(tmp_path), 1, 'new-preview')
    assert fresh['totalItems'] == 0 and fresh['preserved'] == 1
    await close_app(app)


@pytest.mark.asyncio
async def test_retention_blocks_direct_move_and_cleanup_even_with_all_receipts(env, tmp_path):
    app, w = env
    path = pair(w, tmp_path / 'old', ('complete', 'complete'))
    proofs(w, path, tmp_path)
    await w.discover()
    asset = w.rows('assets')[0]
    entry = w.plan_assets(tmp_path / 'future')[0]
    photos = [p['path'] for p in w.slicing.packages.store.list('items')]
    await w.preserve_uploaded(0, 'preserve')
    asset = w.get('assets', asset['id'])
    assert not w.cleanup_eligibility(asset)[0]
    await w.process_move(entry)
    await w.cleanup(asset)
    assert entry['state'] == 'preserved' and path.exists()
    assert all(Path(p).exists() for p in photos)
    assert not Path(entry['destination']).exists()
    await close_app(app)


@pytest.mark.asyncio
async def test_retention_survives_disconnect_restart_and_late_discovery(env, tmp_path):
    app, w = env
    path = pair(w, tmp_path / 'old')
    (tmp_path / 'old').rename(tmp_path / 'offline')
    await w.preserve_uploaded(0, 'keep-offline')
    assert not w.rows('assets')
    await close_app(app)
    (tmp_path / 'offline').rename(tmp_path / 'old')
    app = create_app(data_dir=tmp_path / 'db')
    w = app.state.workspace
    await w.discover()
    assert w.rows('assets')[0]['status'] == 'preserved'
    await enable(w, tmp_path)
    await w.reconcile()
    assert path.exists() and not list(w.root.rglob('*.mp4'))
    await close_app(app)


@pytest.mark.asyncio
async def test_retention_snapshot_does_not_capture_future_uploads_or_name_matches(env, tmp_path):
    app, w = env
    old = pair(w, tmp_path / 'old')
    await w.discover()
    await w.preserve_uploaded(0, 'keep-existing')
    new = pair(w, tmp_path / 'new')  # same filename, distinct version
    await w.discover()
    assert w.asset_for(old)['status'] == 'preserved'
    assert not w.is_preserved(w.asset_for(new))
    # Replaying the original action cannot silently broaden the saved snapshot.
    await w.preserve_uploaded(0, 'keep-existing')
    assert not w.is_preserved(w.asset_for(new))
    await enable(w, tmp_path)
    await w.reconcile(cleanup=False)
    assert old.exists() and not new.exists()
    assert len(list(w.root.rglob('*.mp4'))) == 1
    await close_app(app)


@pytest.mark.asyncio
async def test_preserve_uploaded_uses_completed_upload_not_filename(env, tmp_path):
    app, w = env
    path = tmp_path / 'already.mp4'
    path.write_bytes(b'uploaded')
    pending = tmp_path / 'pending.mp4'
    pending.write_bytes(b'pending')
    for state, file in [('complete', path), ('importing', pending)]:
        w.uploads.store.save('items', {'id': state, 'path': str(file), 'name': file.name,
                                     'dataset': '27912', 'fileVersion': source_fingerprint(file), 'state': state})
    await w.discover()
    await w.preserve_uploaded(0, 'keep-done')
    assert w.is_preserved(w.asset_for(path))
    assert not w.is_preserved(w.asset_for(pending))
    await close_app(app)


@pytest.mark.asyncio
async def test_workspace_cancel_is_durable_and_protected_settings_are_versioned(env, tmp_path):
    app, w = env
    pair(w, tmp_path / 'old')
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://127.0.0.1') as client:
        preview = await client.post('/api/workspace/preview', json={
            'parent': str(tmp_path), 'expectedRevision': 0, 'requestId': 'preview'})
        identity = preview.json()['id']
        response = await client.post(f'/api/workspace/{identity}/cancel', json={'expectedRevision': 0, 'requestId': 'cancel'})
        assert response.status_code == 200 and response.json()['migration']['state'] == 'cancelled'
        assert (await client.get('/api/workspace')).json()['migration']['state'] == 'cancelled'
        assert (await client.post(f'/api/workspace/{identity}/apply', json={
            'expectedRevision': 0, 'requestId': 'cannot-apply'})).status_code == 409
        assert (await client.post('/api/workspace/preserve-uploaded', json={
            'expectedRevision': 0, 'requestId': 'preserve'})).status_code == 200
        assert (await client.post('/api/workspace/preserve-uploaded', json={
            'expectedRevision': 0, 'requestId': 'stale'})).status_code == 409
        assert (await client.post('/api/workspace/preserve-uploaded', json={
            'expectedRevision': 1, 'requestId': 'cancel'})).status_code == 409
    await close_app(app)


def test_root_collision_and_no_nested_workspaces(tmp_path):
    (tmp_path / '视频工作区').mkdir()
    root = choose_root(str(tmp_path), 'id')
    assert root.name == '视频工作区_2'
    create_root(root, 'id')
    assert choose_root(str(tmp_path), 'id') == root
    assert choose_root(str(root), 'id') == root
    assert all((root / b).is_dir() for b in BRANCHES)


@pytest.mark.asyncio
@pytest.mark.parametrize('states,stage', [
    (('pending', 'pending'), '未切片'), (('complete', 'pending'), '磁盘吊未切片'),
    (('pending', 'complete'), '车斗未切片'), (('complete', 'complete'), '已切片'),
    (('no_target', 'no_target'), '已切片')])
async def test_single_copy_stage_and_identity_survive(env, tmp_path, states, stage):
    app, w = env
    path = pair(w, tmp_path / 'old', states)
    # A completed legacy platform task needs its actual output receipt.
    for item in w.slicing.store.list('items'):
        name = 'bed' if item['sourceDatasetId'] == '27912' else 'magnet'
        category = item['categories'][name]
        if category['state'] == 'complete':
            category.update(taskId='verified-task', targetDatasetId='verified-dataset', outputImageIds=['verified-photo'])
            w.slicing.store.save('items', item)
    old = w.slicing.store.list('items')
    p = await enable(w, tmp_path)
    assert len(p['items']) == 1
    asset = w.rows('assets')[0]
    target = w.root / stage / 'truck' / path.name
    assert target.read_bytes() == b'video-version-fixture' and not path.exists()
    assert asset['path'] == str(target)
    assert {r['id'] for r in old} == {r['id'] for r in w.slicing.store.list('items')}
    for item in w.slicing.store.list('items'):
        assert item['fileVersion'] == source_version(target)
        assert item['observationVersion'] == old[0]['fileVersion']
        assert item['sourceVideoId'] in {'27912001', '27944001'}
    assert w.state()['migration']['backup']
    await w.reconcile(cleanup=False)
    assert len(list(w.root.rglob('*.mp4'))) == 1
    await close_app(app)


@pytest.mark.asyncio
async def test_preview_is_non_destructive_and_requests_are_revisioned(env, tmp_path):
    app, w = env
    path = pair(w, tmp_path / 'old')
    p = await w.preview(str(tmp_path), 0, 'preview')
    assert path.exists() and not Path(p['root']).exists()
    assert (await w.preview(str(tmp_path), 0, 'preview'))['id'] == p['id']
    with pytest.raises(Conflict):
        await w.preview(str(tmp_path), 1, 'stale')
    await w.apply(p['id'], 0, 'apply')
    await w.task
    await w.apply(p['id'], 0, 'apply')
    with pytest.raises(Conflict):
        await w.apply(p['id'], 0, 'another')
    assert len(w.rows('moves')) == 1
    await close_app(app)


@pytest.mark.asyncio
async def test_missing_cloud_or_image_proof_never_deletes(env, tmp_path):
    app, w = env
    path = pair(w, tmp_path / 'old', ('complete', 'complete'))
    proofs(w, path, tmp_path)
    await enable(w, tmp_path)
    asset = w.rows('assets')[0]
    upload = w.uploads.store.get('items', 'upload-27944')
    upload['object'] = {}  # a same-name complete row is insufficient
    w.uploads.store.save('items', upload)
    await w.reconcile()
    assert Path(asset['path']).exists()
    assert w.rows('assets')[0]['cleanup']['state'] == 'waiting'
    upload['object'] = {'key': '27944-object'}
    w.uploads.store.save('items', upload)
    package = w.slicing.packages.store.get('items', 'p-bed')
    package['outputImageIds'] = []
    w.slicing.packages.store.save('items', package)
    await w.reconcile()
    assert Path(asset['path']).exists()
    await close_app(app)


@pytest.mark.asyncio
async def test_verified_cleanup_exact_manifest_and_tombstones(env, tmp_path):
    app, w = env
    path = pair(w, tmp_path / 'old', ('complete', 'complete'))
    proofs(w, path, tmp_path)
    downloaded_original(w, path)
    await enable(w, tmp_path)
    asset = w.rows('assets')[0]
    await w.request_cleanup_completed(w.config['revision'], 'manual-cleanup')
    extra = Path(asset['path']).parent / '用户说明.txt'
    extra.write_text('keep')
    await w.reconcile()
    assert not Path(asset['path']).exists()
    assert extra.exists()
    assert w.rows('assets')[0]['cleanup']['state'] == 'complete'
    assert all(i.get('localCleanupState') == 'complete' for i in w.slicing.store.list('items'))
    assert all(p.get('localCleanupState') == 'complete' for p in w.slicing.packages.store.list('items'))
    await w.reconcile()
    assert len(w.rows('assets')) == 1
    assert not list(w.root.rglob('*.jpg')) and not list(w.root.rglob('*.zip'))
    await close_app(app)


@pytest.mark.asyncio
async def test_lease_delays_move_and_cleanup(env, tmp_path):
    app, w = env
    path = pair(w, tmp_path / 'old', ('complete', 'complete'))
    proofs(w, path, tmp_path)
    downloaded_original(w, path)
    await enable(w, tmp_path)
    asset = w.rows('assets')[0]
    await w.request_cleanup_completed(w.config['revision'], 'manual-cleanup')
    with w.leases.read(asset['path']):
        await w.reconcile()
        assert Path(asset['path']).exists()
    await w.reconcile()
    assert not Path(asset['path']).exists()
    await close_app(app)


@pytest.mark.asyncio
async def test_creation_time_preserved_in_audit_scan(env, tmp_path):
    app, w = env
    pair(w, tmp_path / 'old')
    await w.discover()
    a = w.rows('assets')[0]
    a['originalCreatedNs'] = '1600000000000000000'
    w.save('assets', a)
    await enable(w, tmp_path)
    rows, _ = await asyncio.to_thread(w.scan_directory, str(w.root), True)
    assert rows[0]['createdNs'] == '1600000000000000000'
    # Outputs and hidden branches may never enter original-video inventories.
    (w.root / '车斗已切片' / 'unknown.mp4').write_bytes(b'ignore')
    (w.root / '.缓存' / 'part.mp4').write_bytes(b'ignore')
    assert len(scan_managed(w.root)[0]) == 1
    await close_app(app)


@pytest.mark.asyncio
async def test_same_filename_keeps_distinct_folder_and_original_filename(env, tmp_path):
    app, w = env
    pair(w, tmp_path / 'first')
    pair(w, tmp_path / 'second')
    await enable(w, tmp_path)
    assets = w.rows('assets')
    assert len({a['folder'] for a in assets}) == 2
    assert {Path(a['path']).name for a in assets} == {'truck.mp4'}
    await close_app(app)


@pytest.mark.asyncio
async def test_cross_disk_move_retains_upload_identity(env, tmp_path, monkeypatch):
    app, w = env
    path = pair(w, tmp_path / 'old')
    version = source_fingerprint(path)
    w.uploads.store.save('items', {'id':'upload', 'path':str(path), 'fileVersion':version,
        'originalFileVersion':version, 'dataset':'27912','name':path.name,'state':'queued'})
    monkeypatch.setattr('app.workspace.same_device', lambda *args: False)
    await enable(w, tmp_path)
    item = w.uploads.store.get('items', 'upload')
    assert item['uploadFileVersion'] == version and item['originalFileVersion'] == version
    assert Path(item['path']).exists() and not path.exists()
    assert w.rows('moves')[0]['sha256']
    await close_app(app)


@pytest.mark.asyncio
@pytest.mark.parametrize('code,partial,terminal', [('no_decodable_samples',True,True),
    ('no_cuttable_fragments',True,True),('no_decodable_samples',False,False),
    ('media_incomplete',True,False),('model_load_failed',True,False),('network',True,False)])
async def test_decoder_terminal_is_distinct_from_temporary_failure(env, tmp_path, code, partial, terminal):
    app, w = env
    pair(w, tmp_path / 'old', ('attention','no_target'))
    item = w.slicing.store.list('items')[0]
    item.update(errorCode=code, partialRecovery=partial)
    item['categories']['bed'].update(errorCode=code)
    w.slicing.store.save('items',item)
    await w.discover()
    assert (w.outcomes(w.rows('assets')[0])['bed'] == 'uncuttable') is terminal
    await close_app(app)


@pytest.mark.asyncio
async def test_legacy_paths_return_conflicts_and_history_works_unplugged(env, tmp_path, monkeypatch):
    app, w = env
    pair(w, tmp_path / 'old')
    await enable(w, tmp_path)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://127.0.0.1') as client:
        assert (await client.post('/api/settings/directory',json={'directory':str(tmp_path)})).status_code == 409
        assert (await client.post('/api/slicing/classify/preview',json={'directory':str(tmp_path)})).status_code == 409
        assert (await client.get('/api/workspace')).json()['root'] == str(w.root)
        monkeypatch.setattr('app.workspace.read_marker',lambda _:None)
        with pytest.raises(ValueError):w.require_root()
        state=(await client.get('/api/workspace')).json()
        assert not state['online'] and len(state['assets']) == 1
    await close_app(app)


@pytest.mark.asyncio
async def test_active_leases_drain_and_mutations_conflict_during_migration(env, tmp_path):
    app, w = env
    path = pair(w, tmp_path / 'old')
    p = await w.preview(str(tmp_path),0,'preview')
    with w.leases.read(path):
        await w.apply(p['id'],0,'apply')
        await asyncio.sleep(0)
        assert w.maintenance and path.exists()
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url='http://127.0.0.1') as client:
            response = await client.post('/api/settings/directory',json={'directory':str(tmp_path)})
            assert response.status_code == 409
    await w.task
    assert not path.exists() and not w.maintenance
    await close_app(app)


@pytest.mark.asyncio
async def test_conflict_after_preview_preserves_both_files(env, tmp_path):
    app, w = env
    path = pair(w, tmp_path / 'old')
    p = await w.preview(str(tmp_path),0,'preview')
    # A user-created conflict after preview cannot be adopted or overwritten.
    from app.workspace_files import create_root
    create_root(p['root'],w.config['workspaceId'])
    destination = Path(p['items'][0]['destination'])
    destination.parent.mkdir(parents=True,exist_ok=True)
    destination.write_bytes(b'unknown-user-file')
    await w.apply(p['id'],0,'apply');await w.task
    assert path.exists() and destination.read_bytes() == b'unknown-user-file'
    assert w.state()['migration']['counts']['attention'] == 1
    await close_app(app)


@pytest.mark.asyncio
async def test_checkpoint_and_manual_versions_keep_progress(env, tmp_path):
    app, w = env
    original = pair(w,tmp_path/'old')
    row = {'id':'collection', 'jobId':'batch', 'name':'truck','plate':'real', 'qualityStart':'2026-09-29 00:00:00',
        'qualityEnd':'2026-09-29 00:30:00', 'activeRevision':2,'acceptedRevision':1,'revision':7,
        'versions':[{'revision':1,'state':'saved','filePath':str(original)},
                    {'revision':2,'state':'ready','downloadRequested':True,'etag':'strong-object','receivedBytes':4,
                     'taskId':'kept-task','resultId':'kept-result','startTime':'2026-10-02 00:20:12',
                     'durationSeconds':3600,'requestId':'manual-second-correction'}]}
    w.videos.store.save(row)
    part=w.videos.work/'collection'/'v2.part';part.parent.mkdir(parents=True,exist_ok=True);part.write_bytes(b'part')
    await enable(w,tmp_path)
    new=w.videos.store.get('collection')
    assert new['activeRevision']==2 and new['acceptedRevision']==1 and new['revision']==7
    assert new['versions'][1] == row['versions'][1]
    assert (w.videos.work/'collection'/'v2.part').read_bytes()==b'part'
    assert not part.exists()
    await close_app(app)


@pytest.mark.asyncio
async def test_restart_after_publication_replays_bindings_not_bytes(env,tmp_path,monkeypatch):
    app,w=env
    path=pair(w,tmp_path/'old')
    p=await w.preview(str(tmp_path),0,'preview')
    def lost_callback(*args):raise RuntimeError('simulated crash after publication')
    monkeypatch.setattr(w.moves,'update_bindings',lost_callback)
    await w.apply(p['id'],0,'apply');await w.task
    assert not path.exists()
    destination=Path(p['items'][0]['destination']);assert destination.exists()
    await close_app(app)
    app=create_app(data_dir=tmp_path/'db');w=app.state.workspace
    await w.retry(p['id'],w.config['revision'],'retry');await w.task
    await w.reconcile(cleanup=False)
    assert all(i['path']==str(destination) for i in w.slicing.store.list('items'))
    assert len(list(w.root.rglob('*.mp4')))==1
    await close_app(app)


@pytest.mark.asyncio
@pytest.mark.parametrize('interrupt_at',[1,3,5])
async def test_cleanup_restarts_after_unlink_before_acknowledgement(env,tmp_path,monkeypatch,interrupt_at):
    app,w=env
    path=pair(w,tmp_path/'old',('complete','complete'));proofs(w,path,tmp_path)
    downloaded_original(w, path)
    await enable(w,tmp_path)
    await w.request_cleanup_completed(w.config['revision'], 'interrupted-cleanup')
    original=Path(w.rows('assets')[0]['path'])
    saved=w.videos.file_operation
    deletions=0
    async def interrupt(function,*args):
        nonlocal deletions
        value=await saved(function,*args)
        if getattr(function,'__name__','')=='unlink_workspace_file':
            deletions+=1
            if deletions==interrupt_at:raise asyncio.CancelledError()
        return value
    monkeypatch.setattr(w.videos,'file_operation',interrupt)
    with pytest.raises(asyncio.CancelledError):await w.reconcile()
    assert not original.exists()
    await close_app(app)
    app=create_app(data_dir=tmp_path/'db');w=app.state.workspace
    w.audit.client.access='test-only'
    async def inventory(dataset):return [{'id':dataset+'-result','size':len(b'video-version-fixture')}]
    w.uploads.remote.videos=inventory
    await w.reconcile()
    assert w.rows('assets')[0]['cleanup']['state']=='complete'
    await close_app(app)


@pytest.mark.asyncio
async def test_53_record_snapshot_preserves_corrections_observations_and_bindings(env,tmp_path):
    app,w=env
    before={}
    for index in range(53):
        path=pair(w,tmp_path/'old'/str(index),('no_target','no_target'),f'truck_{index:03d}.mp4')
        row={'id':f'collection-{index}', 'jobId':'batch-53','name':path.stem,'qualityStart':'2026-09-29 01:00:00',
            'qualityEnd':'2026-09-29 01:30:00','revision':9,'activeRevision':2,'acceptedRevision':2,
            'requests':{'manual-revision-2':{'taskId':f'task-{index}'}},
            'versions':[{'revision':2,'state':'saved','filePath':str(path),'startTime':'2026-09-29 01:05:21',
                         'durationSeconds':3601,'taskId':f'task-{index}','resultId':f'result-{index}'}]}
        w.videos.store.save(row);before[row['id']]=copy.deepcopy(row)
        w.slicing.store.save('observations',{'id':f'observation-{index}','time':30,'label':'other','score':0.75})
    observations=w.slicing.store.list('observations')
    source_ids={i['id']:i['sourceVideoId'] for i in w.slicing.store.list('items')}
    await enable(w,tmp_path)
    for row in w.videos.store.list():
        assert row['versions'][0]['filePath']!=before[row['id']]['versions'][0]['filePath']
        from app.audit_moves import source_fingerprint
        assert row['versions'][0].pop('fileVersion') == source_fingerprint(Path(row['versions'][0]['filePath']))
        row['versions'][0]['filePath']=before[row['id']]['versions'][0]['filePath']
        row['versions'][0].pop('classifiedAt',None)
        row.pop('updatedAt',None)
        expected=before[row['id']];expected.pop('updatedAt',None)
        assert row==expected
    assert {i['id']:i['sourceVideoId'] for i in w.slicing.store.list('items')}==source_ids
    assert w.slicing.store.list('observations')==observations
    assert len(list(w.root.rglob('*.mp4')))==53
    await close_app(app)


@pytest.mark.asyncio
async def test_unverified_photos_stay_hidden_then_flow_to_category_branch(env,tmp_path):
    app,w=env
    path=pair(w,tmp_path/'old',('platform_processing','complete'));proofs(w,path,tmp_path)
    package=w.slicing.packages.store.get('items','p-bed')
    package.update(state='importing')
    w.slicing.packages.store.save('items',package)
    await enable(w,tmp_path)
    assert (w.root/'.处理中'/'truck'/'bed'/'图片'/'one.jpg').is_file()
    assert (w.root/'.处理中'/'truck'/'magnet'/'压缩包.zip').is_file()
    assert not (w.root/'磁盘吊已切片'/'truck'/'压缩包.zip').exists()
    item=w.slicing.store.list('items')[0];item['categories']['bed']['state']='complete';item['state']='complete'
    w.slicing.store.save('items',item)
    package=w.slicing.packages.store.get('items','p-bed');package['state']='complete'
    w.slicing.packages.store.save('items',package)
    await w.reconcile(cleanup=False)
    assert (w.root/'车斗已切片'/'truck'/'图片'/'one.jpg').is_file()
    assert (w.root/'磁盘吊已切片'/'truck'/'压缩包.zip').is_file()
    assert not (w.root/'.处理中'/'truck'/'bed'/'图片'/'one.jpg').exists()
    assert len(list(w.root.rglob('*.mp4')))==1
    await close_app(app)


@pytest.mark.asyncio
async def test_old_restore_never_redownloads_intentionally_cleaned_video(env,tmp_path):
    app,w=env
    path=pair(w,tmp_path/'old',('no_target','no_target'))
    for item in w.slicing.store.list('items'):
        item['localCleanupState']='complete';w.slicing.store.save('items',item)
    manager=app.state.original_restore
    row={'name':path.name,'state':'queued'}
    manager.job={'id':'current','state':'running','items':[row]}
    await manager._restore_one(row)
    assert row['state']=='cleaned'
    for item in w.slicing.store.list('items'):
        with pytest.raises(Conflict):w.slicing._revision(item,item['revision'])
    await close_app(app)


@pytest.mark.asyncio
async def test_statistics_never_reload_all_platform_snapshots_per_branch(env,tmp_path,monkeypatch):
    app,w=env
    pair(w,tmp_path/'old')
    await w.discover()
    def disallow_full_table(kind):
        raise AssertionError('Workspace statistics must use indexed, per-file lookups')
    monkeypatch.setattr(w.slicing.store,'list',disallow_full_table)
    monkeypatch.setattr(w.slicing.packages.store,'list',disallow_full_table)
    queries=[];w.db.set_trace_callback(queries.append)
    assert len(w.state()['assets'])==1
    source_queries=[q for q in queries if 'SELECT payload FROM simple_slicing_items' in q]
    assert len(source_queries)==1 and 'WHERE' in source_queries[0]
    w.db.set_trace_callback(None)
    # Restore methods before closing managers that inspect their own queues.
    monkeypatch.undo()
    await close_app(app)


@pytest.mark.asyncio
async def test_disk_disconnect_before_download_preserves_ready_version_and_intent(env,tmp_path,monkeypatch):
    app,w=env
    pair(w,tmp_path/'old');await enable(w,tmp_path)
    row={'id':'download','name':'download','jobId':'batch','revision':1,'activeRevision':0,'acceptedRevision':None,
         'versions':[{'revision':0,'state':'ready','downloadRequested':True,'etag':'strong','receivedBytes':123,
                      'outputDirectory':str(w.root/'采集视频')}]}
    w.videos.store.save(row)
    monkeypatch.setattr('app.workspace.read_marker',lambda path:None)
    await w.videos.publish('download',0)
    v=w.videos.store.get('download')['versions'][0]
    assert v['state']=='ready' and v['downloadRequested'] and v['receivedBytes']==123 and v['etag']=='strong'
    assert not w.online
    await close_app(app)


@pytest.mark.asyncio
async def test_download_publication_uses_one_video_folder_and_registers_its_version(env,tmp_path,monkeypatch):
    import hashlib
    app,w=env
    pair(w,tmp_path/'old');await enable(w,tmp_path)
    row={'id':'download','name':'VIRTUAL_001','jobId':'batch','revision':1,'activeRevision':0,'acceptedRevision':None,
         'versions':[{'revision':0,'state':'ready','downloadRequested':True,'startTime':'2026-10-21 00:00:00',
                      'outputDirectory':str(w.root/'采集视频')}]}
    w.videos.store.save(row)
    media=b'isolated downloaded content'
    part=w.videos.work/'download'/'v0.part';part.parent.mkdir(parents=True);part.write_bytes(media)
    async def downloaded(*args):
        return part,{'id':'test-result','url':'http://example.invalid/fixture.mp4'},hashlib.sha256(media).hexdigest(),len(media)
    monkeypatch.setattr(w.videos,'download_bound',downloaded)
    await w.videos.publish('download',0)
    version=w.videos.store.get('download')['versions'][0]
    assert version['state']=='saved',version
    output=Path(version['filePath'])
    assert output.parent.parent==w.root/'采集视频' and output.parent.name==output.stem
    assert output.read_bytes()==media and not part.exists()
    assert version['fileVersion']==source_version(output)
    await close_app(app)
