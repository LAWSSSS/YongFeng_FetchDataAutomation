"""Slow platform reads cannot revoke live scope or stop submitted reconciliation."""
import asyncio
import copy

import pytest

from app.main import create_app
from app.slicing import source_version
from tests.test_media_control import sliced
from tests.test_slicing import FakeSlicingRemote
from tests.test_slicing_performance import close_app


@pytest.mark.asyncio
async def test_scan_preserves_recovery_enrolled_while_platform_inventory_waits(tmp_path, monkeypatch):
    app = create_app(data_dir=tmp_path / 'db'); manager = app.state.slicing
    manager.auth.access = 'isolated'
    manager.store.settings.update(enabled=True, latestRunId='run', directories=[str(tmp_path)],
                                  targetDatasetIds={'27912': '900', '27944': '901'})
    path = tmp_path / 'local.mp4'; path.write_bytes(b'isolated-local-video')
    initial = sliced(app, path, 'initial')
    initial.update(localWorkspace=True, runId='run')
    manager.store.save('items', initial)
    run = {'id': 'run', 'requestId': 'scan', 'state': 'scanning', 'items': ['initial'], 'localRecoveryOnly': True}
    manager.run = run; manager.store.save('runs', run)
    remote = FakeSlicingRemote(); manager.remote = remote
    entered, release = asyncio.Event(), asyncio.Event()
    original = remote.list_datasets
    async def inventory():
        entered.set(); await release.wait(); return await original()
    monkeypatch.setattr(remote, 'list_datasets', inventory)
    monkeypatch.setattr('app.slicing.scan_local_files', lambda directories: ([], directories, []))
    monkeypatch.setattr(manager, '_wake_scheduler', lambda: None)
    task = asyncio.create_task(manager._scan('run'))
    await entered.wait()
    newly_queued = manager.queue_collection_recovery(video_id='video-two', revision=0, path=path,
        file_version=source_version(path), metadata={'duration': 30, 'frameRate': 25}, name='two.mp4', request_id='recover-two')
    release.set(); await task
    stored = manager.store.get('runs', 'run')
    assert set(stored['items']) == {'initial', newly_queued['id']}
    assert manager.store.get('items', newly_queued['id'])['state'] == 'pending'
    await close_app(app)


@pytest.mark.asyncio
async def test_scan_does_not_rewrite_a_result_changed_during_platform_wait(tmp_path, monkeypatch):
    app = create_app(data_dir=tmp_path / 'db'); manager = app.state.slicing
    path = tmp_path / 'platform.mp4'; path.write_bytes(b'isolated-video')
    item = sliced(app, path, 'bed')
    item['sourceVideoId'] = '101'; manager.store.save('items', item)
    manager.store.save('runs', {'id': 'run', 'requestId': 'scan', 'state': 'scanning', 'items': ['bed']})
    manager.store.settings.update(directories=[str(tmp_path)], latestRunId='run')
    remote = FakeSlicingRemote(); manager.remote = remote
    remote.video_rows['27912'] = [{'id': 101, 'name': path.name, 'cutStatus': 0}]
    entered, release = asyncio.Event(), asyncio.Event()
    original = remote.list_datasets
    async def inventory():
        entered.set(); await release.wait(); return await original()
    monkeypatch.setattr(remote, 'list_datasets', inventory)
    monkeypatch.setattr('app.slicing.scan_local_files', lambda directories: ([], directories, []))
    task = asyncio.create_task(manager._scan('run')); await entered.wait()
    completed = manager.store.get('items', 'bed')
    completed.update(state='platform_processing', revision=completed['revision'] + 1)
    completed['categories']['bed'].update(state='ready', intervals=[{'start': 5, 'end': 20}], intervalSeconds=1)
    manager.store.save('items', completed); before = copy.deepcopy(completed)
    release.set(); await task
    assert manager.store.get('items', 'bed') == before
    await close_app(app)


@pytest.mark.asyncio
async def test_pause_keeps_submitted_import_reconciliation_and_blocks_new_uploads(tmp_path, monkeypatch):
    app = create_app(data_dir=tmp_path / 'db'); packages = app.state.slicing.packages
    packages.store.save('items', {'id': 'submitted', 'dataset': '27892', 'state': 'importing', 'jobId': 'j'})
    packages.store.save('items', {'id': 'queued', 'dataset': '27893', 'state': 'queued'})
    entered, release = asyncio.Event(), asyncio.Event(); calls = []
    async def jobs(dataset):
        calls.append(('jobs', dataset)); entered.set(); await release.wait()
        return [{'id': 'j', 'status': '3'}]
    async def verify(item, job):
        calls.append(('verified', item['id']))
        item['state'] = 'complete'; packages._save(item)
    monkeypatch.setattr(packages.remote, 'jobs', jobs)
    monkeypatch.setattr(packages, 'verify', verify)
    monkeypatch.setattr(packages, 'launch', lambda item: calls.append(('launched', item['id'])))
    target = asyncio.create_task(packages.tick_target('27892'))
    packages.target_tasks['27892'] = target
    await entered.wait(); packages.pause(); release.set(); await target
    await packages.tick()
    assert ('verified', 'submitted') in calls
    assert not any(kind == 'launched' for kind, _ in calls)
    assert ('jobs', '27893') not in calls
    assert packages.store.get('items', 'queued')['state'] == 'queued'
    await close_app(app)


@pytest.mark.asyncio
async def test_paused_package_manager_never_invokes_legacy_cleanup(tmp_path, monkeypatch):
    app = create_app(data_dir=tmp_path / 'db'); packages = app.state.slicing.packages
    packages.pause()
    def forbidden(*args):
        raise AssertionError('paused cleanup must not inspect or delete local media')
    monkeypatch.setattr(packages.store, 'list', forbidden)
    packages.delete_if_both_complete({'id': 'legacy', 'state': 'complete'})
    monkeypatch.undo()
    await close_app(app)
