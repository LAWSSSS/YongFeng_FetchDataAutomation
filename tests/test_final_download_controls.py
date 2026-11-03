"""Explicit pause and same-version approvals survive without repeating identity."""
import asyncio
import copy

import pytest

from app.final_processing import FinalProcessing
from app.videos import Conflict
from tests.test_final_processing import env, payload


def stored(app, identity):
    row = app.state.videos.store.get(identity)
    return row, row['versions'][0]


async def approve(app, row):
    manager = app.state.final_processing
    result = manager.confirm(row['id'], payload(row))
    await manager.advance(manager.get(result['operationId']))
    return manager, result['operationId']


@pytest.mark.asyncio
async def test_manual_pause_survives_replay_restart_and_explicit_resume(env):
    app, row = env
    manager, operation_id = await approve(app, row)
    await app.state.videos.cancel_download(row['id'], payload(row, 'cancel'))
    assert manager.operation(operation_id)['manualPaused'] is True
    manager.confirm(row['id'], payload(row, 'repeat-confirm'))
    await manager.tick()
    current, version = stored(app, row['id'])
    assert not version['downloadRequested']
    assert not manager.download_allowed(current)
    restored = FinalProcessing(app.state.store, app.state.videos, app.state.pipeline,
                              app.state.workspace, app.state.media_control, app.state.slicing)
    assert restored.operation(operation_id)['manualPaused'] is True
    restored.resume(row['id'], payload(row, 'resume'))
    restored.resume(row['id'], payload(row, 'resume'))
    assert restored.get(operation_id)['resumeRevision'] == 1
    await restored.advance(restored.get(operation_id))
    current, version = stored(app, row['id'])
    assert version['downloadRequested']
    assert restored.download_allowed(current)
    assert app.state.media_control.get('collection:' + row['id']) is None or app.state.media_control.get('collection:' + row['id'])['generation'] == 0


@pytest.mark.asyncio
async def test_resume_does_not_override_global_pause_or_new_revision(env):
    app, row = env
    manager, operation_id = await approve(app, row)
    await app.state.videos.cancel_download(row['id'], payload(row, 'cancel'))
    app.state.pipeline.config['paused'] = True
    manager.resume(row['id'], payload(row, 'resume'))
    await manager.advance(manager.get(operation_id))
    current, version = stored(app, row['id'])
    assert not version['downloadRequested']
    assert not manager.download_allowed(current)
    current['revision'] += 1
    app.state.videos.store.save(current)
    with pytest.raises(Conflict):
        manager.resume(row['id'], payload(row, 'outdated'))


@pytest.mark.asyncio
async def test_explicit_same_version_retry_rebinds_approval_and_archives_old_receipts(env):
    app, row = env
    manager, old_id = await approve(app, row)
    original = manager.get(old_id)
    current, version = stored(app, row['id'])
    version.update(state='failed', errorCode='network', receivedBytes=7, etag='old-etag',
                   automaticRecoveryEvent={'eventId': 'old-event'}, recovery={'state': 'slicing', 'operationId': 'old-recovery'})
    app.state.videos.store.save(current)
    await app.state.videos.retry_download(row['id'])
    current, version = stored(app, row['id'])
    renewed = manager.current_operation(current, version)
    assert renewed['id'] != old_id
    assert renewed['confirmedAt'] == original['confirmedAt']
    assert renewed['priorOperationId'] == old_id
    assert renewed['mediaGeneration'] == 1
    assert version['receivedBytes'] == 7 and version['etag'] == 'old-etag'
    assert not version.get('recovery') and not version.get('automaticRecoveryEvent')
    history = version['processingHistory'][-1]
    assert history['automaticRecoveryEvent']['eventId'] == 'old-event'
    assert history['recovery']['operationId'] == 'old-recovery'
    assert manager.get(old_id)['state'] == 'superseded'
    await manager.advance(original)
    assert manager.current_operation(*stored(app, row['id']))['id'] == renewed['id']
    assert manager.download_allowed(current)


@pytest.mark.asyncio
async def test_restart_download_rebind_is_atomic_with_completed_restart_receipt(env, monkeypatch):
    app, row = env
    manager, old_id = await approve(app, row)
    current, version = stored(app, row['id'])
    version.update(state='failed', errorCode='network', receivedBytes=7, etag='old-etag')
    app.state.videos.store.save(current)
    part = app.state.videos.work / row['id'] / 'v0.part'
    part.parent.mkdir(parents=True, exist_ok=True); part.write_bytes(b'partial')
    async def bound(_version):
        return {'id': 'exact-result'}
    monkeypatch.setattr(app.state.videos, 'bound_result', bound)
    app.state.videos.target.connected = True
    result = await app.state.videos.restart_download(row['id'], payload(row, 'restart'))
    await app.state.videos.operation_tasks[result['operationId']]
    current, version = stored(app, row['id'])
    renewed = manager.current_operation(current, version)
    assert renewed['id'] != old_id and renewed['priorOperationId'] == old_id
    assert current['requests']['restart']['state'] == 'complete'
    assert not part.exists() and 'etag' not in version
    await app.state.videos.restart_download(row['id'], payload(row, 'restart'))
    assert len(manager.rows()) == 2


@pytest.mark.asyncio
async def test_network_wait_retries_with_same_version_and_bounded_backoff(env, monkeypatch):
    app, row = env
    manager, operation_id = await approve(app, row)
    moment = [1000.0]
    manager.clock = lambda: moment[0]
    app.state.videos.target.connected = True
    monkeypatch.setattr('app.final_downloads.environment_ready', lambda *args: (True, ''))
    calls = []
    monkeypatch.setattr(app.state.videos, 'schedule', lambda item: calls.append(item['id']))
    current, version = stored(app, row['id'])
    version.update(state='failed', errorCode='network', receivedBytes=7, etag='retained-etag')
    app.state.videos.store.save(current)
    await manager.advance(manager.get(operation_id))
    assert manager.operation(operation_id)['stage'] == 'download_wait'
    assert calls == []
    for retry, seconds in enumerate((30, 60, 120, 300), 1):
        moment[0] += seconds
        await manager.advance(manager.get(operation_id))
        current, version = stored(app, row['id'])
        assert version['state'] == 'ready'
        assert version['receivedBytes'] == 7 and version['etag'] == 'retained-etag'
        assert version['finalConfirmation']['operationId'] == operation_id
        assert manager.operation(operation_id)['downloadRetryCount'] == retry
        version.update(state='failed', errorCode='network')
        app.state.videos.store.save(current)
    assert len(calls) == 4


@pytest.mark.asyncio
async def test_disconnected_disk_unready_and_manual_pause_do_not_consume_rounds(env, monkeypatch):
    app, row = env
    manager, operation_id = await approve(app, row)
    moment = [1000.0]; manager.clock = lambda: moment[0]
    current, version = stored(app, row['id'])
    version.update(state='failed', errorCode='disk_full')
    app.state.videos.store.save(current)
    monkeypatch.setattr('app.final_downloads.environment_ready', lambda *args: (False, '等待磁盘空间'))
    await manager.advance(manager.get(operation_id))
    moment[0] += 30
    app.state.videos.target.connected = False
    await manager.advance(manager.get(operation_id))
    assert manager.get(operation_id)['downloadRecovery']['attempts'] == 0
    moment[0] += 30
    app.state.videos.target.connected = True
    await manager.advance(manager.get(operation_id))
    assert manager.get(operation_id)['downloadRecovery']['attempts'] == 0
    await app.state.videos.cancel_download(row['id'], payload(row, 'cancel'))
    moment[0] += 30
    monkeypatch.setattr('app.final_downloads.environment_ready', lambda *args: (True, ''))
    await manager.advance(manager.get(operation_id))
    assert stored(app, row['id'])[1]['state'] == 'failed'
    assert manager.get(operation_id)['manualPaused'] is True


@pytest.mark.asyncio
async def test_pause_during_environment_probe_is_not_overwritten(env, monkeypatch):
    import threading
    app, row = env
    manager, operation_id = await approve(app, row)
    moment = [1000.0]; manager.clock = lambda: moment[0]
    app.state.videos.target.connected = True
    current, version = stored(app, row['id'])
    version.update(state='failed', errorCode='network')
    app.state.videos.store.save(current)
    await manager.advance(manager.get(operation_id))
    moment[0] += 30
    entered, release = threading.Event(), threading.Event()
    def check(*args):
        entered.set(); release.wait(3); return True, ''
    monkeypatch.setattr('app.final_downloads.environment_ready', check)
    task = asyncio.create_task(manager.advance(manager.get(operation_id)))
    while not entered.is_set():
        await asyncio.sleep(.001)
    await app.state.videos.cancel_download(row['id'], payload(row, 'cancel'))
    release.set(); await task
    assert manager.get(operation_id)['manualPaused'] is True
    assert not stored(app, row['id'])[1]['downloadRequested']
    assert manager.get(operation_id)['stage'] == 'paused'


@pytest.mark.asyncio
@pytest.mark.parametrize('code', ['object_changed', 'result_identity_changed', 'range_mismatch', 'content_invalid'])
async def test_identity_and_content_failures_never_auto_retry(env, monkeypatch, code):
    app, row = env
    manager, operation_id = await approve(app, row)
    current, version = stored(app, row['id'])
    version.update(state='failed', errorCode=code, message='需核对')
    app.state.videos.store.save(current)
    monkeypatch.setattr('app.final_downloads.environment_ready', lambda *args: pytest.fail('not environmental'))
    await manager.advance(manager.get(operation_id))
    assert manager.operation(operation_id)['state'] == 'attention'
    assert 'downloadRecovery' not in manager.get(operation_id)
    assert stored(app, row['id'])[1]['state'] == 'failed'


@pytest.mark.asyncio
async def test_environment_probe_cannot_walk_out_of_disconnected_workspace(env, monkeypatch):
    from app.final_downloads import environment_ready
    app, row = env
    manager = app.state.final_processing
    app.state.videos.storage_ready = True  # Cached before unplugging.
    original = app.state.workspace.root
    hidden = original.with_name('unplugged-workspace')
    original.rename(hidden)
    monkeypatch.setattr('app.final_downloads.shutil.disk_usage',
                        lambda *_: pytest.fail('must not probe the parent/system disk'))
    ready, reason = environment_ready(manager, row['versions'][0], 'network')
    assert not ready and '原工作区' in reason
    assert not original.exists()
    hidden.rename(original)


@pytest.mark.asyncio
async def test_environment_probe_allows_missing_child_only_with_verified_root(env):
    from app.final_downloads import environment_ready
    app, row = env
    app.state.videos.storage_ready = True
    app.state.videos.work = app.state.workspace.root / '.缓存' / 'new-not-created'
    ready, reason = environment_ready(app.state.final_processing, row['versions'][0], 'network')
    assert ready, reason
    assert not app.state.videos.work.exists()


@pytest.mark.asyncio
async def test_environment_probe_rejects_rebound_root_and_outside_workdir(env):
    from app.final_downloads import environment_ready
    app, row = env
    app.state.videos.storage_ready = True
    app.state.videos.work = app.state.workspace.root.parent / 'unexpected-downloads'
    assert not environment_ready(app.state.final_processing, row['versions'][0], 'network')[0]
    app.state.videos.work = app.state.workspace.root / '.缓存' / '下载'
    app.state.workspace.config['workspaceId'] = 'other-workspace'
    assert not environment_ready(app.state.final_processing, row['versions'][0], 'network')[0]


@pytest.mark.asyncio
async def test_final_confirmation_and_local_resume_preserve_explicit_photo_pause(env):
    app, row = env
    packages = app.state.slicing.packages
    packages.pause()
    manager, operation_id = await approve(app, row)
    assert packages.paused
    await app.state.videos.cancel_download(row['id'], payload(row, 'cancel'))
    manager.resume(row['id'], payload(row, 'resume'))
    await manager.advance(manager.get(operation_id))
    current, version = stored(app, row['id'])
    current['acceptedRevision'] = version['revision']
    version.update(state='saved', downloadRequested=False)
    app.state.videos.store.save(current)
    await manager.advance(manager.get(operation_id))
    await app.state.pipeline.tick()
    assert packages.paused and packages.store.get('batches', 'control')['paused']
    assert not app.state.uploads.store.settings.get('enabled')
