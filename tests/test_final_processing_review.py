"""Final-approval boundaries, with no platform requests or media processing."""
import asyncio
import copy
import time

import pytest
import pytest_asyncio

from app.final_processing import FinalProcessing
from app.main import create_app
from app.videos import Conflict
from tests.test_media_control import collection, sliced
from tests.test_slicing_performance import close_app
from tests.test_workspace import enable


@pytest_asyncio.fixture
async def final_app(tmp_path, monkeypatch):
    app = create_app(data_dir=tmp_path / 'db')
    monkeypatch.setattr(app.state.slicing, '_wake_scheduler', lambda: None)
    await enable(app.state.workspace, tmp_path)
    path = app.state.workspace.root / '采集视频' / 'example' / 'example.mp4'
    path.parent.mkdir()
    path.write_bytes(b'local-test-only')
    row = collection(app, path, identity='confirmed', accepted=False)
    row['versions'][0].update(taskId='task-exact', resultId='result-exact')
    app.state.videos.store.save(row)
    yield app, path
    await app.state.final_processing.shutdown()
    await app.state.pipeline.shutdown()
    await app.state.workspace.shutdown()
    await close_app(app)


def request(identity='approve'):
    return {'version': 0, 'expectedRevision': 0, 'requestId': identity}


@pytest.mark.asyncio
async def test_approval_is_fast_persistent_and_does_not_wait_for_download(final_app, monkeypatch):
    app, _ = final_app
    manager = app.state.final_processing
    calls = []

    async def capture(identity, payload):
        calls.append((identity, payload))

    monkeypatch.setattr(app.state.videos, 'confirm', capture)
    started = time.monotonic()
    first = manager.confirm('confirmed', request())
    assert time.monotonic() - started < 1
    assert calls == []
    assert app.state.pipeline.active()
    assert app.state.videos.store.get('confirmed')['acceptedRevision'] is None
    assert manager.confirm('confirmed', request())['operationId'] == first['operationId']
    await manager.advance(manager.get(first['operationId']))
    assert len(calls) == 1
    assert calls[0][1]['version'] == 0


@pytest.mark.asyncio
async def test_different_request_same_approval_reuses_operation_across_manager_restart(final_app):
    app, _ = final_app
    first = app.state.final_processing.confirm('confirmed', request())
    restored = FinalProcessing(app.state.store, app.state.videos, app.state.pipeline,
                              app.state.workspace, app.state.media_control, app.state.slicing)
    second = restored.confirm('confirmed', request('another-click'))
    assert second['operationId'] == first['operationId']
    assert len(restored.rows()) == 1
    with pytest.raises(Conflict):
        restored.confirm('confirmed', {**request(), 'version': 1, 'expectedRevision': 1})


@pytest.mark.asyncio
async def test_confirmation_preserves_explicit_pause_and_does_not_download(final_app, monkeypatch):
    app, _ = final_app
    app.state.pipeline.config.update(paused=True, enabled=True)
    app.state.pipeline.save('control', app.state.pipeline.config)
    calls = []

    async def capture(*args):
        calls.append(args)

    monkeypatch.setattr(app.state.videos, 'confirm', capture)
    approval = app.state.final_processing.confirm('confirmed', request())
    await app.state.final_processing.advance(app.state.final_processing.get(approval['operationId']))
    assert calls == []
    state = app.state.final_processing.operation(approval['operationId'])
    assert state['stage'] == 'paused'
    assert app.state.pipeline.paused


@pytest.mark.asyncio
async def test_new_version_prevents_old_approval_callbacks_and_download(final_app, monkeypatch):
    app, _ = final_app
    approval = app.state.final_processing.confirm('confirmed', request())
    row = app.state.videos.store.get('confirmed')
    row.update(revision=1, activeRevision=1)
    new_version = copy.deepcopy(row['versions'][0])
    new_version.update(revision=1, taskId='new-task', resultId='new-result')
    new_version.pop('finalConfirmation')
    row['versions'].append(new_version)
    app.state.videos.store.save(row)
    calls = []

    async def capture(*args):
        calls.append(args)

    monkeypatch.setattr(app.state.videos, 'confirm', capture)
    await app.state.final_processing.advance(app.state.final_processing.get(approval['operationId']))
    assert not calls
    assert app.state.final_processing.operation(approval['operationId'])['state'] == 'superseded'
    assert 'finalConfirmation' not in app.state.videos.store.get('confirmed')['versions'][1]


@pytest.mark.asyncio
async def test_result_identity_change_blocks_approved_processing(final_app, monkeypatch):
    app, _ = final_app
    approval = app.state.final_processing.confirm('confirmed', request())
    row = app.state.videos.store.get('confirmed')
    row['versions'][0]['resultId'] = 'different-result'
    app.state.videos.store.save(row)
    calls = []

    async def capture(*args):
        calls.append(args)

    monkeypatch.setattr(app.state.videos, 'confirm', capture)
    await app.state.final_processing.advance(app.state.final_processing.get(approval['operationId']))
    assert not calls
    assert app.state.final_processing.operation(approval['operationId'])['state'] == 'superseded'


@pytest.mark.asyncio
async def test_partial_observation_counts_do_not_subtract_skips_twice(final_app):
    app, path = final_app
    approval = app.state.final_processing.confirm('confirmed', request())
    row = app.state.videos.store.get('confirmed')
    row['acceptedRevision'] = 0
    row['versions'][0]['state'] = 'saved'
    app.state.videos.store.save(row)
    item = sliced(app, path, state='analyzing')
    item['categories']['bed'].update(observations=2, skippedSamples=5)
    app.state.slicing.store.save('items', item)
    app.state.media_control.refresh()
    await app.state.final_processing.advance(app.state.final_processing.get(approval['operationId']))
    result = app.state.final_processing.operation(approval['operationId'])
    assert result['successfulSamples'] == 2
    assert result['skippedSamples'] == 5


@pytest.mark.asyncio
async def test_blocked_download_does_not_claim_to_be_actively_downloading(final_app):
    app, _ = final_app
    approval = app.state.final_processing.confirm('confirmed', request())
    row = app.state.videos.store.get('confirmed')
    row['versions'][0].update(state='attention', message='结果ID需要人工核对。',
                              review={'code': 'result_identity_changed'}, downloadRequested=False)
    app.state.videos.store.save(row)
    await app.state.final_processing.advance(app.state.final_processing.get(approval['operationId']))
    result = app.state.final_processing.operation(approval['operationId'])
    assert result['state'] == 'attention'
    assert '结果ID' in result['message']


@pytest.mark.asyncio
async def test_same_collection_id_wrong_file_version_cannot_finish(final_app):
    app, path = final_app
    approval = app.state.final_processing.confirm('confirmed', request())
    row = app.state.videos.store.get('confirmed')
    row.update(acceptedRevision=0)
    row['versions'][0]['state'] = 'saved'
    app.state.videos.store.save(row)
    item = sliced(app, path, state='no_target')
    item.update(collectionVideoId='confirmed', collectionRevision=0,
                mediaId='collection:confirmed', mediaGeneration=0)
    item['fileVersion']['size'] += 1
    item['categories'] = {name: {'state': 'no_target', 'observations': 5} for name in ('bed', 'magnet')}
    app.state.slicing.store.save('items', item)
    await app.state.final_processing.advance(app.state.final_processing.get(approval['operationId']))
    result = app.state.final_processing.operation(approval['operationId'])
    assert result['state'] == 'attention'
    assert result['stage'] == 'identity'
    assert path.exists()


@pytest.mark.asyncio
async def test_old_no_target_finishes_retained_without_new_cleanup_authority(final_app):
    app, path = final_app
    row = app.state.videos.store.get('confirmed')
    row.update(acceptedRevision=0)
    row['versions'][0]['state'] = 'saved'
    app.state.videos.store.save(row)
    approval = app.state.final_processing.confirm('confirmed', request(), legacy=True)
    item = sliced(app, path, state='no_target')
    item['categories'] = {name: {'state': 'no_target', 'observations': 5} for name in ('bed', 'magnet')}
    app.state.slicing.store.save('items', item)
    app.state.media_control.refresh()
    await app.state.workspace.discover()
    await app.state.final_processing.advance(app.state.final_processing.get(approval['operationId']))
    result = app.state.final_processing.operation(approval['operationId'])
    assert result['state'] == 'complete'
    assert result['stage'] == 'retained'
    assert result['endReason'] == 'no_target'
    assert result['cleanupBytes'] == 0
    assert app.state.workspace.cleanup_summary()['eligibleCount'] == 0
    assert path.exists()


@pytest.mark.asyncio
async def test_local_export_does_not_display_legacy_platform_video_slice_message(final_app):
    app, path = final_app
    approval = app.state.final_processing.confirm('confirmed', request())
    row = app.state.videos.store.get('confirmed')
    row.update(acceptedRevision=0)
    row['versions'][0]['state'] = 'saved'
    app.state.videos.store.save(row)
    item = sliced(app, path, state='platform_processing')
    item['message'] = '平台切片处理中，原视频保留。'
    item['categories'] = {name: {'state': 'ready'} for name in ('bed', 'magnet')}
    app.state.slicing.store.save('items', item)
    app.state.media_control.refresh()
    await app.state.final_processing.advance(app.state.final_processing.get(approval['operationId']))
    public = app.state.final_processing.operation(approval['operationId'])
    assert public['stage'] == 'photo_export'
    assert '导出照片' in public['message']
    assert '平台切片' not in public['message']


@pytest.mark.asyncio
async def test_new_empty_result_without_conclusion_does_not_finish(final_app):
    app, path = final_app
    approval = app.state.final_processing.confirm('confirmed', request())
    row = app.state.videos.store.get('confirmed')
    row.update(acceptedRevision=0)
    row['versions'][0]['state'] = 'saved'
    app.state.videos.store.save(row)
    item = sliced(app, path, state='no_target')
    item['categories'] = {name: {'state': 'no_target', 'observations': 5} for name in ('bed', 'magnet')}
    app.state.slicing.store.save('items', item)
    app.state.media_control.refresh()
    await app.state.final_processing.advance(app.state.final_processing.get(approval['operationId']))
    assert app.state.final_processing.operation(approval['operationId'])['state'] == 'attention'
    assert app.state.final_processing.operation(approval['operationId'])['stage'] == 'verification'


@pytest.mark.asyncio
async def test_migration_keeps_intent_for_paused_approved_version(final_app):
    app, _ = final_app
    row = app.state.videos.store.get('confirmed')
    row['versions'][0]['downloadRequested'] = True
    app.state.videos.store.save(row)
    app.state.pipeline.config.update(enabled=True, paused=True)
    app.state.pipeline.save('control', app.state.pipeline.config)
    await app.state.final_processing.adopt_existing()
    operations = app.state.final_processing.rows()
    assert len(operations) == 1
    await app.state.final_processing.advance(operations[0])
    assert app.state.final_processing.operation(operations[0]['id'])['stage'] == 'paused'
    assert app.state.pipeline.paused


@pytest.mark.asyncio
async def test_recovery_file_version_is_bound_even_when_top_level_download_failed(final_app):
    app, path = final_app
    row = app.state.videos.store.get('confirmed')
    version = row['versions'][0]
    version.update(state='failed', downloadRequested=True, errorCode='media_incomplete', recovery={
        'state': 'slicing', 'filePath': str(path), 'fileVersion': version.pop('fileVersion'),
        'slicingItemId': 'recovered'})
    version.pop('filePath')
    app.state.videos.store.save(row)
    approval = app.state.final_processing.confirm('confirmed', request(), legacy=True)
    item = sliced(app, path, identity='recovered', state='platform_processing')
    item.update(collectionRecovery=True, partialRecovery=True, collectionVideoId='confirmed', collectionRevision=0,
                mediaId='collection:confirmed', mediaGeneration=0)
    app.state.slicing.store.save('items', item)
    await app.state.final_processing.advance(app.state.final_processing.get(approval['operationId']))
    result = app.state.final_processing.operation(approval['operationId'])
    assert result['stage'] == 'photo_export'
    assert result['state'] == 'running'
