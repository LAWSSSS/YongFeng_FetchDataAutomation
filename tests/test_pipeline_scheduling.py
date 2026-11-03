"""Isolated coverage for local routing, bounded recovery and independent stages."""
import asyncio
import copy

import pytest

from app.main import create_app
from app.slicing import source_version
from tests.test_media_control import sliced
from tests.test_slicing_performance import close_app


@pytest.mark.asyncio
async def test_single_retry_mode_never_changes_local_photo_export_route(tmp_path, monkeypatch):
    app = create_app(data_dir=tmp_path / 'db'); manager = app.state.slicing
    manager.auth.access = 'isolated'
    manager.store.settings.update(enabled=True, localExport=True)
    path = tmp_path / 'video.mp4'; path.write_bytes(b'fixture')
    item = sliced(app, path, state='platform_processing')
    item['categories']['bed'].update(state='ready', intervals=[{'start': 0, 'end': 5}], intervalSeconds=1)
    manager.store.save('items', item)
    manager.run = {'id': 'single', 'state': 'running', 'singleMode': True, 'singleItemId': item['id']}
    manager.store.save('runs', manager.run)
    exported = []
    monkeypatch.setattr(manager, '_schedule_export', lambda item: exported.append(item['id']))
    async def forbidden(_): raise AssertionError('local photo mode must never create platform video cuts')
    monkeypatch.setattr(manager, '_submit_ready', forbidden)
    await manager._tick()
    assert exported == ['bed'] and not manager.platform_task
    await close_app(app)


@pytest.mark.asyncio
async def test_one_empty_category_cannot_hide_another_completed_result(tmp_path, monkeypatch):
    app = create_app(data_dir=tmp_path / 'db'); manager = app.state.slicing
    path = tmp_path / 'video.mp4'; path.write_bytes(b'fixture')
    item = sliced(app, path)
    item['categories'] = {'bed': {'state': 'no_target'}, 'magnet': {'state': 'complete', 'packageId': 'bound'}}
    manager.store.save('items', item)
    async def unchanged(_): return {'duration': 10}
    monkeypatch.setattr(manager, '_refresh_local_binding', unchanged)
    monkeypatch.setattr(manager, '_analyze_item', unchanged)
    await manager._process_item(item)
    assert manager.store.get('items', 'bed')['state'] == 'complete'
    await close_app(app)


@pytest.mark.asyncio
async def test_category_retries_have_independent_delays_and_exhaustion(tmp_path):
    app = create_app(data_dir=tmp_path / 'db'); manager = app.state.slicing
    clock = [1000.]; manager.clock = lambda: clock[0]
    item = {'id': 'x', 'state': 'attention', 'sourceDatasetId': '27912', 'fileVersion': {'size': 10},
            'categories': {'bed': {'state': 'attention', 'errorCode': 'frame_timeout'}, 'magnet': {'state': 'complete'}}}
    manager.store.save('items', item)
    result = manager._recover_analysis_outcome(item)
    assert result['categories']['bed']['automaticRetries'] == 1
    assert result['categories']['bed']['retryNotBefore'] == 1005
    assert manager._poll_timeout() == 5
    result['categories']['magnet'].update(state='attention', errorCode='frame_timeout')
    result['state'] = 'attention'
    result = manager._recover_analysis_outcome(result)
    assert result['categories']['magnet']['automaticRetries'] == 1
    assert result['categories']['bed']['automaticRetries'] == 1
    result['categories']['bed'].update(state='attention', errorCode='frame_timeout')
    result['state'] = 'attention'; clock[0] = 1005
    result = manager._recover_analysis_outcome(result)
    assert result['categories']['bed']['retryNotBefore'] == 1020
    result['categories']['bed'].update(state='attention', errorCode='frame_timeout')
    result['state'] = 'attention'
    result = manager._recover_analysis_outcome(result)
    assert result['categories']['bed']['recoveryDisposition'] == 'manual'
    assert result['categories']['bed']['automaticRetries'] == 2
    await close_app(app)


@pytest.mark.asyncio
async def test_slow_remote_poll_cannot_hold_up_refilling_the_local_pool(tmp_path, monkeypatch):
    app = create_app(data_dir=tmp_path / 'db'); manager = app.state.slicing
    manager.auth.access = 'isolated'; manager.files = None
    manager.store.settings.update(enabled=True, maxLocalSlicingProcesses=1)
    for identity in ['first', 'second']:
        path = tmp_path / (identity + '.mp4'); path.write_bytes(identity.encode())
        sliced(app, path, identity)
    manager.store.save('items', {'id': 'remote', 'state': 'platform_processing', 'sourceDatasetId': '27912',
                                'categories': {'bed': {'state': 'waiting', 'taskId': 'known'}}})
    manager.run = {'id': 'run', 'state': 'running'}; manager.store.save('runs', manager.run)
    entered, release_remote, release_first = asyncio.Event(), asyncio.Event(), asyncio.Event()
    started = []
    async def remote(**kwargs):
        entered.set(); await release_remote.wait()
    async def analyze(item):
        started.append(item['id'])
        if item['id'] == 'first': await release_first.wait()
        item.update(state='complete'); item['categories']['bed']['state'] = 'no_target'
        manager.store.save('items', item)
    monkeypatch.setattr(manager, '_reconcile_waiting', remote)
    monkeypatch.setattr(manager, 'process_with_recovery', analyze)
    await manager._tick(); await entered.wait()
    release_first.set(); await manager.analysis_tasks['first']; await asyncio.sleep(0)
    await asyncio.wait_for(manager._tick(), .5); await asyncio.sleep(0)
    assert started == ['first', 'second'] and not manager.platform_task.done()
    release_remote.set()
    await close_app(app)


@pytest.mark.asyncio
async def test_workspace_existing_single_category_adds_only_the_missing_model(tmp_path, monkeypatch):
    app = create_app(data_dir=tmp_path / 'db'); manager = app.state.slicing
    path = tmp_path / 'video.mp4'; path.write_bytes(b'fixture')
    old = sliced(app, path, state='complete')
    old['categories']['bed'].update(packageId='original-package', taskId='original-task')
    manager.store.save('items', old)
    manager.store.settings['targetDatasetIds'] = {'27912': '27892', '27944': '27893'}
    monkeypatch.setattr(manager, '_wake_scheduler', lambda: None)
    source = {'id': 'asset', 'name': path.name, 'path': str(path), 'fileVersion': source_version(path), 'existingItemIds': ['bed']}
    manager.queue_workspace_sources([source], 'append-missing')
    result = manager.store.get('items', 'bed')
    assert result['categories']['bed'] == old['categories']['bed']
    assert result['categories']['magnet']['state'] == 'pending'
    manager.queue_workspace_sources([source], 'append-again')
    assert len(manager.store.list('items')) == 1
    assert manager.store.get('items', 'bed')['categories']['bed']['packageId'] == 'original-package'
    await close_app(app)


@pytest.mark.asyncio
async def test_late_platform_result_cannot_overwrite_a_manual_interval_change(tmp_path):
    app = create_app(data_dir=tmp_path / 'db'); manager = app.state.slicing
    original = {'id': 'x', 'state': 'platform_processing', 'sourceDatasetId': '27912', 'revision': 1,
                'categories': {'bed': {'state': 'waiting', 'taskId': 'old'}, 'magnet': {'state': 'complete'}}}
    manager.store.save('items', original)
    manual = copy.deepcopy(original); manual['revision'] = 2
    manual['categories']['bed'] = {'state': 'ready', 'intervals': [{'start': 5, 'end': 9}], 'intervalSeconds': 1}
    manager.store.save('items', manual)
    late = copy.deepcopy(original); late['categories']['bed'].update(state='complete', actualImages=150)
    manager._save_category_progress(late, original)
    assert manager.store.get('items', 'x')['categories']['bed'] == manual['categories']['bed']
    await close_app(app)


@pytest.mark.asyncio
async def test_slow_photo_target_never_blocks_the_other_target_next_cycle(tmp_path, monkeypatch):
    app = create_app(data_dir=tmp_path / 'db'); packages = app.state.slicing.packages
    clock = [1000.]; packages.clock = lambda: clock[0]
    for dataset in ['27892', '27893']:
        packages.store.save('items', {'id': dataset, 'dataset': dataset, 'state': 'queued'})
    monkeypatch.setattr(packages, '_launch_next', lambda *args: None)
    slow_entered, slow_release = asyncio.Event(), asyncio.Event(); calls = []
    async def target(dataset):
        calls.append(dataset)
        if dataset == '27892': slow_entered.set(); await slow_release.wait()
    monkeypatch.setattr(packages, 'tick_target', target)
    await asyncio.wait_for(packages.tick(), .5); await slow_entered.wait()
    clock[0] += 31
    await asyncio.wait_for(packages.tick(), .5)
    assert calls.count('27892') == 1 and calls.count('27893') == 2
    assert not packages.tick_lock.locked()
    packages.pause(); await asyncio.sleep(0)
    assert all(task.done() for task in packages.target_tasks.values())
    await close_app(app)
