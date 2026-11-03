"""Offline actions register durably without revoking approvals or checkpoints."""
import asyncio
import copy
import sqlite3
import json
import time

import pytest

from app.remote import RemoteError
from app.file_leases import FileEnvironmentError
from app.videos import Conflict
from tests.test_final_processing import env, payload
from tests.test_final_download_controls import approve, stored
from tests.test_collection_runs import make, CollectionRunManager


async def settled(manager, result):
    task = manager.operation_tasks.get(result['operationId'])
    if task:
        await task
    return manager.store.get_operation(result['operationId'])


def checkpoint(app, row):
    current, version = stored(app, row['id'])
    version.update(state='failed', downloadRequested=True, errorCode='range_invalid', etag='version-old',
                   receivedBytes=7, totalBytes=70)
    app.state.videos.store.save(current)
    part = app.state.videos.work / row['id'] / 'v0.part'
    part.parent.mkdir(parents=True, exist_ok=True); part.write_bytes(b'partial')
    return part


@pytest.mark.asyncio
async def test_offline_restart_is_fast_preserves_intent_and_resumes_after_reconnect(env, monkeypatch):
    app, row = env
    flow, old_id = await approve(app, row)
    manager = app.state.videos
    part = checkpoint(app, row)
    before = manager.store.get(row['id'])
    started = time.monotonic()
    result = await manager.restart_download(row['id'], payload(row, 'restart'))
    assert time.monotonic() - started < 1
    operation = await settled(manager, result)
    assert operation['state'] == 'waiting_connection'
    assert part.read_bytes() == b'partial'
    assert stored(app, row['id'])[1]['finalConfirmation'] == before['versions'][0]['finalConfirmation']
    assert stored(app, row['id'])[1]['downloadRequested'] is True
    assert not app.state.media_control.get('collection:' + row['id'])
    assert not manager.download_candidate(stored(app, row['id'])[0])
    assert (await manager.restart_download(row['id'], payload(row, 'other-click')))['operationId'] == result['operationId']
    manager.target.connected = True
    async def bound(_): return {'id': 'exact-result'}
    monkeypatch.setattr(manager, 'bound_result', bound)
    await manager._run_operation(result['operationId'])
    updated = manager.store.get_operation(result['operationId'])
    assert updated['state'] == 'complete' and not part.exists()
    current, version = stored(app, row['id'])
    assert version['downloadRequested'] and version['state'] == 'ready'
    assert flow.current_operation(current, version)['priorOperationId'] == old_id
    part.write_bytes(b'new-progress')
    replay = await manager.restart_download(row['id'], payload(row, 'restart'))
    assert replay['operationId'] == result['operationId'] and part.read_bytes() == b'new-progress'
    alias = await manager.restart_download(row['id'], payload(row, 'other-click'))
    assert alias['operationId'] == result['operationId'] and part.read_bytes() == b'new-progress'


@pytest.mark.asyncio
async def test_disk_offline_or_preflight_network_failure_does_not_change_generation(env, monkeypatch):
    app, row = env
    await approve(app, row)
    manager = app.state.videos; part = checkpoint(app, row)
    manager.target.connected = True
    async def offline(_): raise RemoteError('unreachable', 'NETWORK')
    monkeypatch.setattr(manager, 'bound_result', offline)
    result = await manager.restart_download(row['id'], payload(row, 'restart'))
    assert (await settled(manager, result))['state'] == 'waiting_connection'
    assert part.read_bytes() == b'partial'
    assert not app.state.media_control.get('collection:' + row['id'])
    async def missing(): raise FileEnvironmentError('missing disk', 'disk_offline')
    monkeypatch.setattr(app.state.workspace, 'check_root', missing)
    await manager._run_operation(result['operationId'])
    assert manager.store.get_operation(result['operationId'])['stage'] == 'waiting_disk'
    assert not app.state.media_control.get('collection:' + row['id'])
    assert part.exists()


@pytest.mark.asyncio
async def test_restart_revalidates_version_after_wait_and_does_not_remove_new_checkpoint(env):
    app, row = env
    await approve(app, row)
    manager = app.state.videos; part = checkpoint(app, row)
    result = await manager.restart_download(row['id'], payload(row, 'restart'))
    await settled(manager, result)
    current, version = stored(app, row['id']); version['resultId'] = 'different-result'
    manager.store.save(current)
    await manager._run_operation(result['operationId'])
    assert manager.store.get_operation(result['operationId'])['state'] == 'conflict'
    assert part.read_bytes() == b'partial'
    assert not app.state.media_control.get('collection:' + row['id'])


@pytest.mark.asyncio
@pytest.mark.parametrize('after_unlink', [False, True])
async def test_checkpoint_clear_interruption_restarts_one_durable_operation(env, monkeypatch, after_unlink):
    app, row = env
    await approve(app, row)
    manager = app.state.videos; part = checkpoint(app, row)
    manager.target.connected = True
    async def bound(_): return {'id': 'exact-result'}
    monkeypatch.setattr(manager, 'bound_result', bound)
    original_io = manager.file_operation
    interrupted = False
    async def io(function, *args, **kwargs):
        nonlocal interrupted
        if getattr(function, '__name__', '') == 'unlink' and not interrupted:
            interrupted = True
            if after_unlink:
                await original_io(function, *args, **kwargs)
            raise asyncio.CancelledError()
        return await original_io(function, *args, **kwargs)
    monkeypatch.setattr(manager, 'file_operation', io)
    result = await manager.restart_download(row['id'], payload(row, 'restart'))
    task = manager.operation_tasks.get(result['operationId'])
    assert task
    await asyncio.gather(task, return_exceptions=True)
    assert manager.store.get_operation(result['operationId'])['state'] == 'queued'
    assert part.exists() is not after_unlink
    assert app.state.final_processing.current_operation(*stored(app, row['id'])) is not None
    generation = app.state.media_control.get('collection:' + row['id'])['generation']
    await manager._run_operation(result['operationId'])
    assert manager.store.get_operation(result['operationId'])['state'] == 'complete'
    assert app.state.media_control.get('collection:' + row['id'])['generation'] == generation
    assert not part.exists()


@pytest.mark.asyncio
async def test_first_confirmation_activation_is_atomic_and_repair_preserves_pause(env):
    app, row = env
    flow = app.state.final_processing
    app.state.store.db.execute("CREATE TEMP TRIGGER reject_first_enable BEFORE INSERT ON simple_pipeline_control BEGIN SELECT RAISE(ABORT, 'injected'); END")
    with pytest.raises(sqlite3.IntegrityError):
        flow.confirm(row['id'], payload(row))
    assert not flow.rows() and not stored(app, row['id'])[1].get('finalConfirmation')
    assert not app.state.pipeline.config['enabled']
    app.state.store.db.execute('DROP TRIGGER reject_first_enable')
    result = flow.confirm(row['id'], payload(row))
    assert app.state.pipeline.get('control', 'control')['enabled']
    app.state.pipeline.config.update(enabled=False, paused=True)
    app.state.pipeline.save('control', app.state.pipeline.config)
    app.state.slicing.packages.pause()
    replay = flow.confirm(row['id'], payload(row))
    assert replay['operationId'] == result['operationId']
    assert app.state.pipeline.config['enabled'] and app.state.pipeline.paused and app.state.slicing.packages.paused


@pytest.mark.asyncio
async def test_original_issue_survives_disk_loss_and_capabilities_are_backend_owned(env):
    app, row = env
    flow = app.state.final_processing
    assert flow.capabilities(row)['canConfirmFinal']
    result = flow.confirm(row['id'], payload(row))
    current, version = stored(app, row['id'])
    assert not flow.capabilities(current)['canConfirmFinal']
    version.update(state='failed', errorCode='range_invalid', message='断点Range不符', downloadRequested=True)
    app.state.videos.store.save(current)
    app.state.workspace.online = False
    await flow.advance(flow.get(result['operationId']))
    state = flow.operation(result['operationId'])
    assert state['state'] == 'attention' and state['issue']['code'] == 'range_invalid'
    assert state['environmentBlocker']['code'] == 'disk'
    assert state['message'] == '断点Range不符'
    shown = app.state.videos.state()['videos'][0]
    assert shown['capabilities']['canRestartDownload']
    assert not shown['capabilities']['canConfirmFinal']


@pytest.mark.asyncio
async def test_query_resumes_only_failed_detail_after_manager_restart(env):
    app, _ = env
    service, source = make(app)
    source.count = 100; source.fail_detail = RemoteError('failed-one', 'NETWORK')
    run = service.start({'dateFrom': '2026-09-30', 'dateTo': '2026-09-30', 'requestId': 'details'})
    await service.tick()
    assert len(service.get(run['id'])['detailCheckpoint']['records']) == 99
    resumed = CollectionRunManager(service.store, source, service.jobs, service.videos,
                                   service.admission_settings, service.target_config)
    await resumed.tick()
    assert sum(path.endswith('getCheckDetail') for path, _ in source.calls) == 101
    assert len(resumed.get(run['id'])['records']) == 100
    assert 'detailCheckpoint' not in resumed.get(run['id'])


@pytest.mark.asyncio
async def test_changed_page_does_not_mix_cached_details(env):
    app, _ = env
    service, source = make(app); source.count = 100; source.fail_detail = RemoteError('failed-one', 'NETWORK')
    run = service.start({'dateFrom': '2026-09-30', 'dateTo': '2026-09-30', 'requestId': 'details'})
    await service.tick(); source.shift = True
    await service.tick()
    assert service.get(run['id'])['status'] == 'attention'
    assert service.get(run['id'])['records'] == []
    assert sum(path.endswith('getCheckDetail') for path, _ in source.calls) == 100


@pytest.mark.asyncio
async def test_legacy_completed_restart_request_does_not_erase_new_download_progress(env):
    app, row = env
    manager = app.state.videos; part = checkpoint(app, row)
    current, _ = stored(app, row['id'])
    current['requests']['old-restart'] = {'kind': 'restart-download', 'version': 0, 'expectedRevision': 0, 'state': 'complete'}
    manager.store.save(current)
    result = await manager.restart_download(row['id'], payload(row, 'old-restart'))
    assert result['state'] == 'complete' and part.read_bytes() == b'partial'
    assert result['operationId'] not in manager.operation_tasks
    assert not app.state.media_control.get('collection:' + row['id'])


@pytest.mark.asyncio
async def test_pause_during_preflight_prevents_takeover_and_deletion(env, monkeypatch):
    app, row = env
    flow, operation_id = await approve(app, row)
    manager = app.state.videos; part = checkpoint(app, row); manager.target.connected = True
    entered, release = asyncio.Event(), asyncio.Event()
    async def bound(_):
        entered.set(); await release.wait(); return {'id': 'exact-result'}
    monkeypatch.setattr(manager, 'bound_result', bound)
    result = await manager.restart_download(row['id'], payload(row, 'restart'))
    await entered.wait()
    await manager.cancel_download(row['id'], payload(row, 'pause'))
    release.set(); state = await settled(manager, result)
    assert state['state'] == 'waiting_connection' and state['stage'] == 'paused'
    assert flow.get(operation_id)['manualPaused']
    assert not app.state.media_control.get('collection:' + row['id'])
    assert part.read_bytes() == b'partial'


@pytest.mark.asyncio
async def test_repeated_capture_download_generation_enrolls_new_exact_file_without_old_event_reuse(env):
    app, row = env
    pipeline = app.state.pipeline
    current, version = stored(app, row['id'])
    current['acceptedRevision'] = 0; version.update(state='saved', downloadRequested=False)
    app.state.videos.store.save(current)
    asset = {'id': 'old-asset', 'path': version['filePath'], 'fileVersion': copy.deepcopy(version['fileVersion']), 'status': 'managed'}
    app.state.workspace.save('assets', asset)
    pipeline.notify_saved(row['id'], 0); pipeline._enroll_downloads()
    old = pipeline.rows('events')[0]
    assert old['state'] == 'enrolled'
    app.state.media_control.save({'id': 'collection:' + row['id'], 'generation': 1})
    version['fileVersion'] = {**version['fileVersion'], 'inode': version['fileVersion']['inode'] + 1}
    app.state.videos.store.save(current)
    pipeline.notify_saved(row['id'], 0)
    new = next(event for event in pipeline.rows('events') if event['id'] != old['id'])
    assert new['state'] == 'pending' and new['mediaGeneration'] == 1
    pipeline._enroll_downloads()
    assert pipeline.get('events', new['id'])['state'] == 'pending', 'old physical file cannot satisfy new event'
    asset.update(id='new-asset', fileVersion=copy.deepcopy(version['fileVersion']))
    app.state.workspace.save('assets', asset)
    pipeline._enroll_downloads()
    assert pipeline.get('events', new['id'])['assetId'] == 'new-asset'
    assert pipeline.get('events', old['id']) == old
    pipeline.notify_saved(row['id'], 0)
    assert len(pipeline.rows('events')) == 2


@pytest.mark.asyncio
async def test_pending_download_event_cannot_admit_changed_generation_or_file(env):
    app, row = env
    current, version = stored(app, row['id'])
    current['acceptedRevision'] = 0; version.update(state='saved')
    app.state.videos.store.save(current)
    app.state.pipeline.notify_saved(row['id'], 0)
    event = app.state.pipeline.rows('events')[0]
    version['fileVersion'] = {**version['fileVersion'], 'size': version['fileVersion']['size'] + 1}
    app.state.videos.store.save(current)
    app.state.pipeline._enroll_downloads()
    assert app.state.pipeline.get('events', event['id'])['state'] == 'superseded'
    assert not app.state.pipeline.config['assetIds']


@pytest.mark.asyncio
async def test_restart_workspace_identity_error_is_attention_without_takeover(env):
    app, row = env
    await approve(app, row)
    manager = app.state.videos; part = checkpoint(app, row)
    from app.workspace_files import MARKER, FORMAT
    (app.state.workspace.root / MARKER).write_text(json.dumps({'id': 'another-workspace', 'format': FORMAT}))
    result = await manager.restart_download(row['id'], payload(row, 'restart'))
    operation = await settled(manager, result)
    assert operation['state'] == 'attention' and '身份' in operation['message']
    assert not app.state.media_control.get('collection:' + row['id'])
    assert part.read_bytes() == b'partial'


@pytest.mark.asyncio
async def test_stalled_read_probe_does_not_block_final_tick_or_spawn_replacement_threads(env, monkeypatch):
    import threading
    app, row = env
    flow, operation_id = await approve(app, row)
    checkpoint(app, row)
    current, version = stored(app, row['id']); version['errorCode'] = 'network'
    app.state.videos.store.save(current)
    app.state.videos.target.connected = True
    moments = [1000.]; flow.clock = lambda: moments[0]
    await flow.advance(flow.get(operation_id))
    release, entered = threading.Event(), threading.Event()
    calls = []
    def blocked(*args):
        calls.append(True); entered.set(); release.wait(2); return True, ''
    monkeypatch.setattr('app.final_downloads.environment_ready', blocked)
    app.state.workspace.file_checks.timeout = .02
    try:
        for _ in range(2):
            moments[0] += 30
            start = time.monotonic()
            await flow.advance(flow.get(operation_id))
            assert time.monotonic() - start < .2
        assert entered.is_set() and len(calls) == 1
        assert flow.get(operation_id)['state'] == 'waiting'
        assert flow.get(operation_id)['downloadRecovery']['attempts'] == 0
    finally:
        release.set()
        if app.state.workspace.file_checks.pending:
            await asyncio.gather(*list(app.state.workspace.file_checks.pending.values()), return_exceptions=True)
