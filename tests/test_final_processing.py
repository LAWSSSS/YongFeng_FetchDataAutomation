import asyncio
import time

import httpx
import pytest
import pytest_asyncio

from app.final_processing import FinalProcessing
from app.main import create_app
from app.videos import Conflict
from tests.test_media_control import collection
from tests.test_slicing_performance import close_app
from tests.test_workspace import enable


@pytest_asyncio.fixture
async def env(tmp_path, monkeypatch):
    app = create_app(data_dir=tmp_path / 'data')
    (tmp_path / 'disk').mkdir()
    await enable(app.state.workspace, tmp_path / 'disk')
    app.state.workspace.online = True
    await app.state.workspace.shutdown()
    monkeypatch.setattr(app.state.videos, 'schedule', lambda row: None)
    path = tmp_path / 'fixture.mp4'
    path.write_bytes(b'isolated-media')
    row = collection(app, path, 'a' * 24, accepted=False)
    row['versions'][0].update(taskId='exact-task', resultId='exact-result')
    app.state.videos.store.save(row)
    yield app, row
    await app.state.final_processing.shutdown()
    await app.state.collection_runs.shutdown()
    await app.state.pipeline.shutdown()
    await close_app(app)


def payload(row, request='final'):
    return {'version': row['activeRevision'], 'expectedRevision': row['revision'], 'requestId': request}


@pytest.mark.asyncio
async def test_final_approval_is_fast_durable_and_replayed_without_download(env):
    app, row = env
    manager = app.state.final_processing
    started = time.monotonic()
    result = manager.confirm(row['id'], payload(row))
    assert time.monotonic() - started < 1
    assert result['state'] == 'queued'
    current = app.state.videos.store.get(row['id'])
    assert current['acceptedRevision'] is None
    assert not current['versions'][0].get('downloadRequested')
    assert current['versions'][0]['finalConfirmation']['operationId'] == result['operationId']
    assert manager.confirm(row['id'], payload(row))['operationId'] == result['operationId']
    assert manager.confirm(row['id'], payload(row, 'another-click'))['operationId'] == result['operationId']
    assert len(manager.rows()) == 1
    restored = FinalProcessing(app.state.store, app.state.videos, app.state.pipeline,
                              app.state.workspace, app.state.media_control, app.state.slicing)
    assert restored.operation(result['operationId'])['version'] == 0
    await restored.tick()
    assert app.state.videos.store.get(row['id'])['versions'][0]['downloadRequested']
    assert app.state.pipeline.active()
    assert not app.state.slicing.packages.paused
    assert not app.state.uploads.store.settings.get('enabled')


@pytest.mark.asyncio
async def test_saved_video_waits_for_recognition_before_a_worker_exists(env):
    app, row = env
    manager = app.state.final_processing
    result = manager.confirm(row['id'], payload(row))
    current = app.state.videos.store.get(row['id'])
    current['acceptedRevision'] = 0
    current['versions'][0]['state'] = 'saved'
    app.state.videos.store.save(current)
    await manager.tick()
    status = manager.operation(result['operationId'])
    assert (status['state'], status['stage'], status['progress']) == ('queued', 'recognition_queue', None)


@pytest.mark.asyncio
async def test_pause_wins_and_newer_version_cannot_be_downloaded_by_old_intent(env):
    app, row = env
    manager = app.state.final_processing
    result = manager.confirm(row['id'], payload(row))
    app.state.pipeline.change('pause', {'requestId': 'pause', 'expectedRevision': app.state.pipeline.config['revision']})
    app.state.slicing.packages.pause()
    await manager.tick()
    assert manager.operation(result['operationId'])['stage'] == 'paused'
    assert not app.state.videos.store.get(row['id'])['versions'][0].get('downloadRequested')
    current = app.state.videos.store.get(row['id'])
    current.update(revision=1, activeRevision=1)
    current['versions'].append({**current['versions'][0], 'revision': 1, 'taskId': 'new-task'})
    current['versions'][1].pop('finalConfirmation', None)
    app.state.videos.store.save(current)
    await manager.tick()
    assert manager.operation(result['operationId'])['state'] == 'superseded'
    assert app.state.slicing.packages.paused
    assert not app.state.videos.store.get(row['id'])['versions'][1].get('downloadRequested')


@pytest.mark.asyncio
async def test_confirmation_rejects_unavailable_or_changed_capture_and_batch_isolated(env):
    app, row = env
    manager = app.state.final_processing
    with pytest.raises(Conflict):
        manager.confirm(row['id'], {**payload(row), 'expectedRevision': 99})
    current = app.state.videos.store.get(row['id'])
    current['versions'][0]['state'] = 'waiting'
    app.state.videos.store.save(current)
    with pytest.raises(ValueError):
        manager.confirm(row['id'], payload(row))
    assert not manager.rows()
    current['versions'][0]['state'] = 'ready'
    app.state.videos.store.save(current)
    result = manager.batch({'requestId': 'batch', 'selections': [
        {'id': row['id'], 'version': 0, 'expectedRevision': 0},
        {'id': 'b' * 24, 'version': 0, 'expectedRevision': 0}]})
    assert [entry['ok'] for entry in result['outcomes']] == [True, False]


@pytest.mark.asyncio
async def test_public_api_and_video_state_expose_current_approval(env):
    app, row = env
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://127.0.0.1') as client:
        response = await client.post(f'/api/videos/{row["id"]}/confirm-and-process', json=payload(row))
        assert response.status_code == 202, response.text
        result = response.json()
        assert (await client.get('/api/final-processing/' + result['operationId'])).status_code == 200
        videos = (await client.get('/api/videos?compact=true')).json()['videos']
        assert videos[0]['finalProcessing']['operationId'] == result['operationId']
        conflict = await client.post(f'/api/videos/{row["id"]}/confirm-and-process', json={**payload(row), 'version': 1})
        assert conflict.status_code == 409


@pytest.mark.asyncio
async def test_upgrade_adopts_approved_current_only_not_unconfirmed_or_completed(env):
    app, row = env
    manager = app.state.final_processing
    await manager.adopt_existing()
    assert not manager.rows()  # Ready does not mean finally confirmed.
    app.state.pipeline.config.pop('finalFlowMigration')
    current = app.state.videos.store.get(row['id'])
    current['versions'][0]['downloadRequested'] = True
    app.state.videos.store.save(current)
    await manager.adopt_existing()
    assert len(manager.rows()) == 1
    await manager.adopt_existing()
    assert len(manager.rows()) == 1


@pytest.mark.asyncio
async def test_disk_disconnection_retains_confirmation_without_scheduling(env):
    app, row = env
    manager = app.state.final_processing
    result = manager.confirm(row['id'], payload(row))
    app.state.workspace.online = False
    assert manager.operation(result['operationId'])['stage'] == 'waiting_disk'
    await manager.tick()
    assert manager.operation(result['operationId'])['stage'] == 'waiting_disk'
    assert not app.state.videos.store.get(row['id'])['versions'][0].get('downloadRequested')
    app.state.workspace.online = True
    await manager.tick()
    assert app.state.videos.store.get(row['id'])['versions'][0]['downloadRequested']


@pytest.mark.asyncio
async def test_busy_video_does_not_delay_another_confirmed_download(env, tmp_path):
    app, row = env
    manager = app.state.final_processing
    path = tmp_path / 'other.mp4'
    path.write_bytes(b'other-media')
    other = collection(app, path, 'b' * 24, accepted=False)
    other['versions'][0].update(taskId='other-task', resultId='other-result')
    app.state.videos.store.save(other)
    manager.confirm(row['id'], payload(row, 'busy'))
    manager.confirm(other['id'], payload(other, 'other'))
    async with app.state.videos.row_lock(row['id']):
        await asyncio.wait_for(manager.tick(), timeout=1)
        assert not app.state.videos.store.get(row['id'])['versions'][0].get('downloadRequested')
        assert app.state.videos.store.get(other['id'])['versions'][0]['downloadRequested']
    await manager.tick()
    assert app.state.videos.store.get(row['id'])['versions'][0]['downloadRequested']
