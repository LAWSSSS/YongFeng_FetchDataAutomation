"""Offline scheduling, projection and recovery; no real platform or media IO."""
import asyncio
import json

import pytest

from app.main import create_app
from tests.test_media_control import sliced
from tests.test_slicing_performance import close_app


@pytest.mark.asyncio
async def test_status_index_reuses_unchanged_inputs_but_updates_after_new_record(tmp_path, monkeypatch):
    app = create_app(data_dir=tmp_path / 'db')
    control = app.state.media_control
    control.refresh()
    reads = []
    original = app.state.videos.store.list
    monkeypatch.setattr(app.state.videos.store, 'list', lambda **kwargs: (reads.append(1), original(**kwargs))[1])
    control.refresh(); control.refresh()
    assert not reads
    path = tmp_path / 'source.mp4'; path.write_bytes(b'isolated')
    sliced(app, path)
    control.refresh()
    assert len(reads) == 1
    assert app.state.slicing.store.get('items', 'bed')['mediaId'].startswith('local:')
    control.refresh()
    assert len(reads) == 1
    await close_app(app)


@pytest.mark.asyncio
async def test_progress_omits_photo_manifest_but_gallery_receipt_is_preserved(tmp_path):
    app = create_app(data_dir=tmp_path / 'db')
    packages = app.state.slicing.packages
    names = ['frame-' + str(index) + '.jpg' for index in range(200)]
    packages.store.save('items', {'id': 'p', 'dataset': '27912', 'category': 'bed', 'state': 'complete',
                                  'images': names, 'imageCount': len(names), 'jobId': 'exact-job'})
    public = packages.state()
    assert 'images' not in public['items'][0]
    assert public['items'][0]['imageCount'] == 200
    assert packages.store.get('items', 'p')['images'] == names
    assert len(json.dumps(public)) < len(json.dumps(names))
    await close_app(app)


@pytest.mark.asyncio
async def test_offline_login_and_slow_remote_poll_do_not_block_local_analysis(tmp_path, monkeypatch):
    app = create_app(data_dir=tmp_path / 'db')
    manager = app.state.slicing
    path = tmp_path / 'source.mp4'; path.write_bytes(b'isolated')
    sliced(app, path)
    manager.run = {'id': 'run', 'items': ['bed'], 'state': 'running'}
    manager.store.settings.update(enabled=True, latestRunId='run')
    manager.files = None
    manager.store.save('runs', manager.run)
    entered, release = asyncio.Event(), asyncio.Event()
    async def login():
        await release.wait()
    async def analyze(item):
        entered.set()
        await release.wait()
    monkeypatch.setattr(manager.auth, 'restore_saved_connection', login)
    monkeypatch.setattr(manager, 'process_with_recovery', analyze)
    await asyncio.wait_for(manager._tick(), .5)
    await asyncio.wait_for(entered.wait(), .5)
    assert manager.phase(manager.run) == 'analyzing'
    assert manager.store.settings['enabled']
    release.set()
    await asyncio.gather(*manager.analysis_tasks.values())
    await close_app(app)


@pytest.mark.asyncio
async def test_secondary_model_retry_preserves_ready_and_uncertain_categories(tmp_path):
    app = create_app(data_dir=tmp_path / 'db')
    manager = app.state.slicing
    item = {'id': 'source', 'state': 'attention', 'sourceDatasetId': '27912', 'errorCode': '',
            'categories': {'bed': {'state': 'ready', 'intervals': [{'start': 1, 'end': 9}]},
                           'magnet': {'state': 'attention', 'errorCode': 'frame_timeout'}}}
    manager.store.save('items', item)
    result = manager._recover_analysis_outcome(item)
    assert result['state'] == 'pending' and result['automaticRetries'] == 1
    assert result['categories']['bed'] == {'state': 'ready', 'intervals': [{'start': 1, 'end': 9}]}
    assert result['categories']['magnet']['state'] == 'pending'
    result['categories']['magnet'].update(state='attention', errorCode='identity')
    result.update(state='attention', errorCode='identity')
    assert manager._recover_analysis_outcome(result)['categories']['magnet']['state'] == 'attention'
    await close_app(app)


@pytest.mark.asyncio
async def test_photo_upload_starts_while_remote_import_list_is_still_loading(tmp_path, monkeypatch):
    app = create_app(data_dir=tmp_path / 'db')
    packages = app.state.slicing.packages
    packages.store.save('items', {'id': 'p', 'dataset': '900', 'state': 'queued'})
    entered, release = asyncio.Event(), asyncio.Event()
    async def slow_jobs(dataset):
        await release.wait()
        return []
    async def upload(identity):
        entered.set()
        await release.wait()
    monkeypatch.setattr(packages.remote, 'jobs', slow_jobs)
    monkeypatch.setattr(packages, 'upload_one', upload)
    polling = asyncio.create_task(packages.tick_target('900'))
    await asyncio.wait_for(entered.wait(), .5)
    assert not polling.done() and len(packages.running) == 1
    polling.cancel()
    await asyncio.gather(polling, return_exceptions=True)
    await close_app(app)


@pytest.mark.asyncio
async def test_restoring_local_file_does_not_require_platform_login(tmp_path):
    app = create_app(data_dir=tmp_path / 'db')
    manager = app.state.slicing
    path = tmp_path / 'source.mp4'; path.write_bytes(b'isolated')
    item = sliced(app, path, state='waiting_connection')
    item['waitCode'] = 'disk'
    item['categories']['bed']['state'] = 'pending'
    manager.store.save('items', item)
    await manager._restore_waiting_files()
    assert manager.store.get('items', 'bed')['state'] == 'pending'
    assert not manager.auth.connected
    await close_app(app)
