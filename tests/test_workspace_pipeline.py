"""Local work is independent of source transfer; all platforms/files are isolated."""
import asyncio
import time
from pathlib import Path

import pytest
import pytest_asyncio

from app.main import create_app
from app.audit_moves import source_fingerprint
from app.slice_export import PACKAGE_DATASETS
from app.workspace_progress import snapshot
from tests.test_media_control import collection
from tests.test_workspace import enable
from tests.test_slicing_performance import close_app


@pytest_asyncio.fixture
async def local_app(tmp_path, monkeypatch):
    app = create_app(data_dir=tmp_path / 'db')
    w = app.state.workspace
    # These tests advance individual stages explicitly. Do not race a real
    # decoder against the folder-move fixture as filesystem checks yield.
    monkeypatch.setattr(w.slicing, '_wake_scheduler', lambda: None)
    path = tmp_path / 'source.mp4'
    path.write_bytes(b'local-video-version-fixture')
    collection(app, path, 'accepted-video')
    w.audit.client.access = 'test-only'
    w.slicing.store.settings.update(localExport=True, targetDatasetIds={'27912': '101', '27944': '102'})
    w.slicing.store.save_settings()
    await enable(w, tmp_path)
    app.state.pipeline.change('start', {'requestId': 'fixture-continuous-start', 'expectedRevision': 0})

    yield app
    await w.shutdown()
    await close_app(app)


@pytest.mark.asyncio
async def test_local_analysis_does_not_scan_or_queue_original_uploads(local_app, monkeypatch):
    w = local_app.state.workspace
    w.slicing.store.settings['targetDatasetIds'] = {'27912': None, '27944': None}
    w.slicing.auth.access = ''
    await w._auto_start_pipeline()
    await w._advance_pipeline()
    items = w.slicing.store.list('items')
    assert len(items) == 1 and items[0]['sourceType'] == 'local_workspace'
    assert items[0]['sourceVideoId'] == '' and not items[0].get('collectionRecovery')
    assert items[0]['collectionVideoId'] == 'accepted-video'
    assert set(items[0]['categories']) == {'bed', 'magnet'}
    assert {name: value['targetDatasetId'] for name, value in items[0]['categories'].items()} == PACKAGE_DATASETS
    assert not w.uploads.store.list('items')
    entered = asyncio.Event()
    async def process(item):
        entered.set()
        item['state'] = 'no_target'
        for category in item['categories'].values():
            category['state'] = 'no_target'
        w.slicing.store.save('items', item)
    async def reconcile(**_):
        pass
    monkeypatch.setattr(w.slicing, '_process_item', process)
    monkeypatch.setattr(w.slicing, '_reconcile_waiting', reconcile)
    await w.slicing._tick()
    await asyncio.wait_for(entered.wait(), 1)
    assert not w.uploads.store.list('items')
    w.slicing.queue_workspace_sources([], 'empty-repeat')
    assert len(w.slicing.store.list('items')) == 1


@pytest.mark.asyncio
async def test_auto_queue_reopens_request_blocked_by_legacy_target_setting(local_app):
    w = local_app.state.workspace
    await w._auto_start_pipeline()
    request = w._pipeline_request()
    w._pipeline_update(request, 'attention', '旧流程阻塞', 'attention',
        pipelineBlockers=[{'name': '', 'reason': '等待车斗和磁盘吊图片目标配置完成。'}])
    await w._auto_start_pipeline()
    assert w._pipeline_request()['id'] == request['id']
    await w._advance_pipeline()
    assert len(w.slicing.store.list('items')) == 1
    assert w._pipeline_request()['pipelineBlockers'] == []


@pytest.mark.asyncio
async def test_verified_folder_move_keeps_the_same_pipeline_and_item(local_app):
    w = local_app.state.workspace
    await w._auto_start_pipeline()
    await w._advance_pipeline()
    request = w._pipeline_request()
    original = request['pipelineSources'][0]['path']
    item_id = w.slicing.store.list('items')[0]['id']
    await w.reconcile(cleanup=False)
    current = w.rows('assets')[0]['path']
    assert current != original and '/未切片/' in current
    sources, excluded = w._pipeline_snapshot(w.get('requests', request['id']))
    assert not excluded and sources[0]['path'] == current
    assert w.slicing.store.get('items', item_id)['path'] == current
    assert all(item['path'] == current for item in w.uploads.store.list('items'))


@pytest.mark.asyncio
async def test_live_progress_reports_photo_packages_not_original_uploads(local_app):
    w = local_app.state.workspace
    await w._auto_start_pipeline()
    await w._advance_pipeline()
    item = w.slicing.store.list('items')[0]
    package = {'id': 'photo-1', 'itemId': item['id'], 'category': 'bed', 'state': 'uploading',
               'name': 'photo.zip', 'size': 20, 'bytes': 4, 'imageCount': 2}
    w.slicing.packages.store.save('items', package)
    w.slicing.packages.runtime[package['id']] = {'bytes': 11, 'speed': 3}
    progress = snapshot(w, w._pipeline_request(), w.rows('assets'))
    assert progress['uploads']['complete'] == 0
    assert progress['uploads']['bytes'] == 11
    assert progress['uploads']['targets'][0]['label'] == '车斗照片'
    assert progress['uploads']['targets'][0]['speed'] == 3
    assert progress['activeTasks'][0]['bytes'] == 11
    assert not w.uploads.store.list('items')


@pytest.mark.asyncio
async def test_unaccepted_or_superseded_collection_is_not_automatically_analyzed(local_app):
    w = local_app.state.workspace
    row = w.videos.store.get('accepted-video')
    row['activeRevision'] = 9
    w.videos.store.save(row)
    sources = [{'id': a['id'], 'name': a['name'], 'path': a['path'], 'fileVersion': a['fileVersion']}
               for a in w.rows('assets')]
    blockers = w._queue_local_pipeline_sources({'id': 'do-not-overwrite'}, sources)
    assert blockers and not w.slicing.store.list('items')


@pytest.mark.asyncio
async def test_local_queue_respects_pause_and_does_not_restart_on_each_poll(local_app):
    w = local_app.state.workspace
    await w._auto_start_pipeline()
    await w._advance_pipeline()
    request = w._pipeline_request()
    run_id = w.slicing.run['id']
    revision = w.slicing.store.get('items', w.slicing.run['items'][0])['revision']
    await w._advance_pipeline()
    assert w.slicing.run['id'] == run_id
    assert w.slicing.store.get('items', w.slicing.run['items'][0])['revision'] == revision
    w.slicing.pause()
    await w._advance_pipeline()
    assert not w.slicing.store.settings['enabled']
    assert any('暂停' in b['reason'] for b in w.get('requests', request['id'])['pipelineBlockers'])


@pytest.mark.asyncio
async def test_local_source_does_not_reuse_another_empty_platform_video_id(local_app,monkeypatch):
    w = local_app.state.workspace
    await w._auto_start_pipeline(); await w._advance_pipeline()
    item = w.slicing.store.list('items')[0]
    item['categories']['bed'].update(state='ready', intervals=[{'start': 0, 'end': 20}], intervalSeconds=1)
    w.slicing.store.save('items', item)
    def ambiguous(*_):
        raise AssertionError('A local source cannot match a package by an empty platform ID')
    monkeypatch.setattr(w.slicing.packages.store, 'find', ambiguous)
    monkeypatch.setattr(w.slicing.packages.store, 'find_video', lambda *_: {
        'id': 'exact-local-package', 'state': 'complete', 'imageCount': 1, 'actualImages': 1})
    await w.slicing._export_category_owned(item, 'bed')
    assert w.slicing.store.get('items', item['id'])['categories']['bed']['packageId'] == 'exact-local-package'


@pytest.mark.asyncio
async def test_routine_inventory_does_not_rehash_unchanged_registered_videos(local_app,monkeypatch):
    from app import workspace as module
    w = local_app.state.workspace
    calls = []
    original = module.file_version
    def record(path, edge=False):
        calls.append(edge)
        return original(path, edge)
    monkeypatch.setattr(module, 'file_version', record)
    await w.discover(); await w.discover()
    assert calls and not any(calls)


@pytest.mark.asyncio
async def test_resume_local_workspace_does_not_wait_for_a_new_platform_scan(local_app):
    w = local_app.state.workspace
    await w._auto_start_pipeline(); await w._advance_pipeline()
    w.slicing.pause()
    if w.slicing.scheduler_task:
        await asyncio.gather(w.slicing.scheduler_task, return_exceptions=True)
    w.slicing.store.settings['workflow'] = {'enabled': False, 'phase': 'scan'}
    w.slicing.store.save_settings()
    w.slicing.resume('resume-known-local-work')
    assert w.slicing.store.settings['enabled']
    assert w.slicing.store.settings['workflow'] == {'enabled': False, 'phase': 'local_processing'}


@pytest.mark.asyncio
async def test_local_queue_reconnects_saved_session_without_a_remote_scan(local_app,monkeypatch):
    w=local_app.state.workspace
    await w._auto_start_pipeline();await w._advance_pipeline()
    w.slicing.auth.access=''
    called=[]
    async def restore():
        called.append(True);w.slicing.auth.access='restored-test-session'
    monkeypatch.setattr(w.slicing.auth,'restore_saved_connection',restore)
    monkeypatch.setattr(w.slicing,'_scope_run_items',lambda _:[])
    await w.slicing._tick()
    assert called and w.slicing.auth.connected


@pytest.mark.asyncio
async def test_read_lease_released_between_discovery_and_arrangement_is_not_attention(local_app):
    w=local_app.state.workspace
    await w._auto_start_pipeline()
    asset=w.rows('assets')[0]
    with w.leases.read(asset['path']):
        await w.discover()
        assert asset['path'] not in w.discovery_errors
    await w.reconcile(cleanup=False)
    assert w.get('assets',asset['id'])['status']=='managed'


@pytest.mark.asyncio
async def test_photo_only_flow_never_starts_a_source_upload_or_remote_inventory(local_app, monkeypatch):
    w = local_app.state.workspace
    def forbidden(*args, **kwargs):
        raise AssertionError('Photo-only processing must not scan or upload original videos')
    monkeypatch.setattr(w.uploads, 'scan', forbidden)
    w.uploads.audit.client.access = ''
    await w._auto_start_pipeline()
    await w._advance_pipeline()
    assert len(w.slicing.store.list('items')) == 1
    assert not w.uploads.store.list('items')
    assert w._pipeline_request()['pipelineStage'] == 'processing'
