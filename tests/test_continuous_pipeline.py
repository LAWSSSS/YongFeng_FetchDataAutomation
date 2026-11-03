"""Continuous enrollment and deferred control, using only temporary records."""
import asyncio
import time

import httpx
import pytest
import pytest_asyncio

from app.main import create_app
from app.pipeline import PipelineManager
from app.videos import Conflict
from tests.test_media_control import collection, sliced
from tests.test_slicing_continue import failed, setup_run
from tests.test_slicing_performance import close_app
from tests.test_workspace import enable


@pytest_asyncio.fixture
async def app(tmp_path, monkeypatch):
    value = create_app(data_dir=tmp_path / 'db')
    monkeypatch.setattr(value.state.slicing, '_wake_scheduler', lambda: None)
    yield value
    await value.state.pipeline.shutdown()
    await value.state.workspace.shutdown()
    await close_app(value)


@pytest.mark.asyncio
async def test_default_off_and_requests_are_revision_checked_and_idempotent(app, tmp_path):
    pipeline = app.state.pipeline
    assert not pipeline.active()
    await pipeline.tick()
    assert not app.state.slicing.store.list('runs')
    await enable(app.state.workspace, tmp_path)
    value = pipeline.change('start', {'requestId': 'start', 'expectedRevision': 0})
    assert value['enabled'] and not value['paused'] and value['revision'] == 1
    assert pipeline.change('start', {'requestId': 'start', 'expectedRevision': 0})['revision'] == 1
    with pytest.raises(Conflict):
        pipeline.change('pause', {'requestId': 'start', 'expectedRevision': 0})
    with pytest.raises(Conflict):
        pipeline.change('pause', {'requestId': 'stale', 'expectedRevision': 0})
    app.state.slicing.packages.pause()
    pipeline.change('pause', {'requestId': 'pause', 'expectedRevision': 1})
    pipeline.change('resume', {'requestId': 'resume', 'expectedRevision': 2})
    assert app.state.slicing.packages.paused  # A prior manual pause is preserved.
    restored = PipelineManager(app.state.store, app.state.workspace, app.state.media_control,
                               app.state.videos, app.state.slicing)
    assert restored.active() and restored.config['revision'] == 3
    assert not app.state.uploads.store.settings.get('enabled')


@pytest.mark.asyncio
async def test_new_accepted_download_enrolls_but_unknown_file_and_new_draft_do_not(app, tmp_path):
    workspace, pipeline = app.state.workspace, app.state.pipeline
    await enable(workspace, tmp_path)
    pipeline.change('start', {'requestId': 'start', 'expectedRevision': 0})
    path = workspace.root / '采集视频' / 'new' / 'new.mp4'
    path.parent.mkdir(); path.write_bytes(b'accepted-new-file')
    collection(app, path, 'new-video')
    workspace.online = True
    await workspace.discover()
    asset = workspace.asset_for(str(path))
    assert asset and not pipeline.allowed_asset(asset)
    pipeline.notify_saved('new-video', 0)
    pipeline._enroll_downloads()
    assert pipeline.allowed_asset(asset)
    # Correcting the version does not inherit the previous confirmation.
    row = app.state.videos.store.get('new-video')
    row.update(activeRevision=1, revision=1)
    row['versions'].append({**row['versions'][0], 'revision': 1, 'state': 'ready'})
    app.state.videos.store.save(row)
    pipeline.notify_saved('new-video', 1)
    assert len(pipeline.rows('events')) == 1
    assert not app.state.uploads.store.list('items')


@pytest.mark.asyncio
async def test_batch_ack_is_immediate_and_replay_does_not_repeat_slow_work(app, monkeypatch):
    pipeline = app.state.pipeline
    entered, release = asyncio.Event(), asyncio.Event()
    calls = []
    async def slow(identity, body):
        calls.append((identity, body['requestId']))
        entered.set()
        await release.wait()
        return {'outcomes': [{'ok': True}]}
    monkeypatch.setattr(pipeline.control, 'continue_safe', slow)
    data = {'requestId': 'batch', 'selections': [{'mediaId': 'local:1', 'generation': 0,
            'selections': [{'id': 'one', 'expectedRevision': 4}], 'packages': []}]}
    started = time.monotonic()
    result = pipeline.enqueue(data)
    assert time.monotonic() - started < .25
    await entered.wait()
    assert pipeline.operation(result['operationId'])['state'] == 'running'
    assert pipeline.enqueue(data)['operationId'] == result['operationId']
    release.set()
    await pipeline.operation_tasks[result['operationId']]
    assert len(calls) == 1
    assert pipeline.operation(result['operationId'])['state'] == 'complete'
    with pytest.raises(Conflict):
        pipeline.enqueue({**data, 'selections': [{**data['selections'][0], 'generation': 1}]})


@pytest.mark.asyncio
async def test_restart_replays_same_subrequest_and_retains_completed_outcomes(app, monkeypatch):
    pipeline = app.state.pipeline
    release = asyncio.Event()
    calls = []
    async def handler(identity, body):
        calls.append((identity, body['requestId']))
        if identity == 'two':
            await release.wait()
        return {'outcomes': [{'ok': True}]}
    monkeypatch.setattr(pipeline.control, 'continue_safe', handler)
    result = pipeline.enqueue({'requestId': 'recover-batch', 'selections': [
        {'mediaId': name, 'generation': 0, 'selections': [{'id': name, 'expectedRevision': 0}], 'packages': []}
        for name in ['one', 'two']]})
    for _ in range(20):
        if len(calls) == 2:
            break
        await asyncio.sleep(0)
    await pipeline.shutdown()
    assert pipeline.operation(result['operationId'])['finished'] == 1
    pipeline.stopping = False
    release.set()
    pipeline.launch_operation(result['operationId'])
    await pipeline.operation_tasks[result['operationId']]
    assert [identity for identity, _ in calls].count('one') == 1
    assert calls[1][1] == calls[2][1]
    assert pipeline.operation(result['operationId'])['state'] == 'complete'


@pytest.mark.asyncio
async def test_pipeline_api_paginates_effective_groups_and_rejects_invalid_actions(app, tmp_path):
    for index in range(25):
        path = tmp_path / f'{index}.mp4'; path.write_bytes(f'video{index}'.encode())
        sliced(app, path, identity=str(index), state='pending')
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://127.0.0.1') as client:
        first = (await client.get('/api/pipeline')).json()
        assert first['total'] == 25 and first['pages'] == 2 and len(first['items']) == 20
        second = (await client.get('/api/pipeline?page=2')).json()
        assert len(second['items']) == 5
        bad = await client.post('/api/pipeline/continue-batch', json={
            'requestId': 'invalid', 'selections': [{'mediaId': 'one', 'generation': 0, 'action': 'archive'}]})
        assert bad.status_code == 422
        assert (await client.get('/api/pipeline?filter=unknown')).status_code == 422


@pytest.mark.asyncio
async def test_batch_isolation_handles_a_stale_video_without_interrupting_another(app, tmp_path):
    manager = setup_run(app)
    path = tmp_path / 'one.mp4'; path.write_bytes(b'one')
    first = failed(app, path, 'one', '27912')
    path2 = tmp_path / 'two.mp4'; path2.write_bytes(b'two')
    second = failed(app, path2, 'two', '27912')
    app.state.media_control.refresh()
    rows = [manager.store.get('items', item['id']) for item in [first, second]]
    data = {'requestId': 'isolated-batch', 'selections': [
        {'mediaId': item['mediaId'], 'generation': item['mediaGeneration'],
         'selections': [{'id': item['id'], 'expectedRevision': 999 if index == 0 else item['revision']}], 'packages': []}
        for index, item in enumerate(rows)]}
    result = app.state.pipeline.enqueue(data)
    await app.state.pipeline.operation_tasks[result['operationId']]
    outcomes = app.state.pipeline.operation(result['operationId'])['outcomes']
    assert [row['ok'] for row in outcomes] == [False, True]
    assert manager.store.get('items', first['id'])['state'] == 'attention'
    assert manager.store.get('items', second['id'])['state'] == 'pending'
