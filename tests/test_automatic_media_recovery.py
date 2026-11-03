"""New full-download decode failures enter the existing conservative recovery."""
import asyncio
import copy

import pytest
import pytest_asyncio

from app.main import create_app
from app.pipeline import PipelineManager
from app.videos import DownloadFailure, VideoManager
from tests.test_media_control import collection
from tests.test_slicing_performance import close_app


@pytest_asyncio.fixture
async def app(tmp_path, monkeypatch):
    app = create_app(data_dir=tmp_path / 'db')
    pipeline = app.state.pipeline
    app.state.workspace.config.update(enabled=True, root=str(tmp_path / 'workspace'))
    pipeline.config.update(enabled=True, paused=False)
    pipeline.save('control', pipeline.config)
    monkeypatch.setattr(app.state.videos, 'launch_operation', lambda _identity: None)
    yield app
    await pipeline.shutdown()
    await close_app(app)


def failed_download(app, tmp_path):
    path = tmp_path / 'original.mp4'
    path.write_bytes(b'complete-downloaded-bytes')
    row = collection(app, path, accepted=False)
    version = row['versions'][0]
    version.update(state='failed', errorCode='invalid_video', downloadRequested=True,
                   taskId='bound-task', resultId='bound-result', etag='"strong-object-version"',
                   totalBytes=path.stat().st_size, receivedBytes=path.stat().st_size)
    app.state.videos.store.save(row)
    return row, version


def persist_failure(app, row, version, notify=True):
    marker = app.state.pipeline.prepare_recovery_event(row, version)
    if marker:
        version['automaticRecoveryEvent'] = marker
    app.state.videos.store.save(row)
    if marker and notify:
        app.state.pipeline.notify_media_failure(row['id'], version['revision'])
    return marker


@pytest.mark.asyncio
async def test_new_failure_is_persisted_and_enrolled_once_without_replacing_old_file(app, tmp_path):
    row, version = failed_download(app, tmp_path)
    original = copy.deepcopy(version)
    marker = persist_failure(app, row, version)
    pipeline = app.state.pipeline
    pipeline.notify_media_failure(row['id'], version['revision'])
    assert len(pipeline.rows('events')) == 1
    assert not app.state.videos.store.list_operations()
    pipeline._enroll_recoveries()
    pipeline._enroll_recoveries()
    operations = app.state.videos.store.list_operations()
    assert len(operations) == 1 and operations[0]['kind'] == 'recover-decodable'
    assert operations[0]['version'] == 0 and operations[0]['expectedRevision'] == row['revision']
    assert pipeline.get('events', marker['id'])['state'] == 'enrolled'
    stored = app.state.videos.store.get(row['id'])['versions'][0]
    assert stored['state'] == 'failed' and stored['filePath'] == original['filePath']
    assert (tmp_path / 'original.mp4').read_bytes() == b'complete-downloaded-bytes'


@pytest.mark.asyncio
@pytest.mark.parametrize('problem', ['disabled', 'paused', 'unapproved', 'short_bytes', 'weak_etag',
    'missing_etag', 'missing_task', 'missing_result', 'object_changed', 'range_invalid', 'timeout',
    'wrong_version', 'manual_pause', 'correction', 'existing_attention'])
async def test_noneligible_or_manual_cases_never_create_automatic_intent(app, tmp_path, problem):
    row, version = failed_download(app, tmp_path)
    pipeline = app.state.pipeline
    if problem == 'disabled': pipeline.config['enabled'] = False
    elif problem == 'paused': pipeline.config['paused'] = True
    elif problem == 'unapproved': version['downloadRequested'] = False
    elif problem == 'short_bytes': version['receivedBytes'] -= 1
    elif problem == 'weak_etag': version['etag'] = 'W/"weak"'
    elif problem == 'missing_etag': version['etag'] = ''
    elif problem == 'missing_task': version['taskId'] = ''
    elif problem == 'missing_result': version['resultId'] = ''
    elif problem in {'object_changed', 'range_invalid'}: version['errorCode'] = problem
    elif problem == 'timeout': version['errorCode'] = 'media_check_timeout'
    elif problem == 'wrong_version': row['activeRevision'] = 1
    elif problem in {'manual_pause', 'correction'}:
        app.state.media_control.save({'id': 'collection:collection', 'generation': 0,
            'paused': problem == 'manual_pause', 'blocked': problem == 'correction'})
    else:
        app.state.videos.store.save_operation({'id': 'previous-manual-recovery', 'videoId': row['id'],
            'kind': 'recover-decodable', 'version': 0, 'state': 'attention'})
    assert persist_failure(app, row, version) is None
    assert pipeline.rows('events') == []


@pytest.mark.asyncio
@pytest.mark.parametrize('change', ['revision', 'generation', 'source', 'cancel_download'])
async def test_newer_manual_decision_supersedes_pending_automatic_event(app, tmp_path, change):
    row, version = failed_download(app, tmp_path)
    marker = persist_failure(app, row, version)
    if change == 'revision': row['revision'] += 1
    elif change == 'source': version['etag'] = '"different-object"'
    elif change == 'cancel_download': version['downloadRequested'] = False
    else: app.state.media_control.save({'id': 'collection:collection', 'generation': 1})
    app.state.videos.store.save(row)
    app.state.pipeline._enroll_recoveries()
    assert not app.state.videos.store.list_operations()
    assert app.state.pipeline.get('events', marker['id'])['state'] == 'superseded'


@pytest.mark.asyncio
async def test_pause_holds_event_and_only_owned_operations_are_gated(app, tmp_path):
    row, version = failed_download(app, tmp_path)
    marker = persist_failure(app, row, version)
    pipeline = app.state.pipeline
    pipeline.config['paused'] = True
    pipeline._enroll_recoveries()
    assert pipeline.get('events', marker['id'])['state'] == 'pending'
    assert not app.state.videos.store.list_operations()
    pipeline.config['paused'] = False
    pipeline._enroll_recoveries()
    operation = app.state.videos.store.list_operations()[0]
    assert pipeline.automatic_operation_allowed(operation)
    owned = asyncio.create_task(asyncio.Event().wait())
    manual = asyncio.create_task(asyncio.Event().wait())
    app.state.videos.operation_tasks.update({operation['id']: owned, 'manual': manual})
    pipeline.change('pause', {'requestId': 'pause-auto-recovery', 'expectedRevision': pipeline.config['revision']})
    await asyncio.sleep(0)
    assert owned.cancelled() and not manual.done()
    assert not pipeline.automatic_operation_allowed(operation)
    assert pipeline.automatic_operation_allowed({'id': 'manual', 'kind': 'recover-decodable'})
    manual.cancel()
    await asyncio.gather(owned, manual, return_exceptions=True)


@pytest.mark.asyncio
async def test_marker_recovers_after_restart_but_historical_attention_is_not_enrolled(app, tmp_path, monkeypatch):
    row, version = failed_download(app, tmp_path)
    marker = persist_failure(app, row, version, notify=False)
    historical = copy.deepcopy(row)
    historical['id'] = 'historical'
    historical['versions'][0].pop('automaticRecoveryEvent')
    app.state.videos.store.save(historical)
    original = app.state.pipeline
    restored = PipelineManager(app.state.store, app.state.workspace, app.state.media_control,
                               app.state.videos, app.state.slicing)
    async def no_tick(): pass
    monkeypatch.setattr(restored, 'tick', no_tick)
    try:
        await restored.startup()
        events = restored.rows('events')
        assert len(events) == 1 and events[0]['id'] == marker['id']
        restored._enroll_recoveries()
        assert [op['videoId'] for op in app.state.videos.store.list_operations()] == ['collection']
        assert 'automaticRecoveryEvent' not in app.state.videos.store.get('historical')['versions'][0]
    finally:
        await restored.shutdown()
        app.state.workspace.pipeline = original


@pytest.mark.asyncio
async def test_failure_to_register_is_terminal_and_does_not_loop(app, tmp_path, monkeypatch):
    row, version = failed_download(app, tmp_path)
    marker = persist_failure(app, row, version)
    calls = []
    def failure(*args, **kwargs):
        calls.append(True)
        raise ValueError('模拟登记失败')
    monkeypatch.setattr(app.state.videos, 'queue_video_operation', failure)
    app.state.pipeline._enroll_recoveries()
    app.state.pipeline._enroll_recoveries()
    assert calls == [True]
    assert app.state.pipeline.get('events', marker['id'])['state'] == 'attention'


@pytest.mark.asyncio
@pytest.mark.parametrize('state', ['attention', 'paused'])
async def test_manual_recovery_started_after_failure_is_kept_without_retry(app, tmp_path, state):
    row, version = failed_download(app, tmp_path)
    marker = persist_failure(app, row, version)
    app.state.videos.store.save_operation({'id': 'manual-attention', 'videoId': row['id'],
        'kind': 'recover-decodable', 'version': 0, 'state': state})
    app.state.pipeline._enroll_recoveries()
    assert len(app.state.videos.store.list_operations()) == 1
    assert app.state.pipeline.get('events', marker['id'])['state'] == 'handled'
    assert app.state.videos.store.get_operation('manual-attention')['state'] == state
    assert not app.state.pipeline.get('events', marker['id']).get('operationId')


@pytest.mark.asyncio
async def test_publish_failure_hook_persists_intent_before_background_enrollment(app, tmp_path, monkeypatch):
    row, version = failed_download(app, tmp_path)
    version.update(state='ready', outputDirectory=str(tmp_path / 'output'))
    app.state.videos.store.save(row)
    monkeypatch.setattr(app.state.workspace, 'require_root', lambda: None)
    async def fail_decode(*args):
        raise DownloadFailure('模拟完整下载后的解码异常。', 'invalid_video')
    monkeypatch.setattr(app.state.videos, 'download_bound', fail_decode)
    await app.state.videos.publish(row['id'], 0)
    stored = app.state.videos.store.get(row['id'])['versions'][0]
    assert stored['state'] == 'failed' and stored['errorCode'] == 'invalid_video'
    marker = stored['automaticRecoveryEvent']
    assert app.state.pipeline.get('events', marker['id'])['state'] == 'pending'
    assert not app.state.videos.store.list_operations()
    app.state.pipeline._enroll_recoveries()
    assert len(app.state.videos.store.list_operations()) == 1


@pytest.mark.asyncio
async def test_video_operation_launcher_honors_automatic_pause_on_restart(app, tmp_path, monkeypatch):
    row, version = failed_download(app, tmp_path)
    persist_failure(app, row, version)
    pipeline = app.state.pipeline
    pipeline._enroll_recoveries()
    operation = app.state.videos.store.list_operations()[0]
    calls = []
    async def execute(identity):
        calls.append(identity)
    monkeypatch.setattr(app.state.videos, '_run_operation', execute)
    pipeline.config['paused'] = True
    VideoManager.launch_operation(app.state.videos, operation['id'])
    assert operation['id'] not in app.state.videos.operation_tasks
    pipeline.config['paused'] = False
    VideoManager.launch_operation(app.state.videos, operation['id'])
    await asyncio.gather(*app.state.videos.operation_tasks.values())
    assert calls == [operation['id']]
