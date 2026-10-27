"""Live download admission, persistence and revision safety on isolated storage."""
import asyncio
import hashlib
import httpx
import pytest

from tests.test_simple_flow import app_factory, platform, wait_until


def seed_videos(manager, directory, count):
    manager.configure_directory(str(directory))
    rows = []
    for index in range(count):
        identity = f'{index + 1:024x}'
        version = {'revision': 0, 'state': 'ready', 'taskId': 'task-' + identity,
                   'resultId': 'result-' + identity, 'name': 'TEST_' + str(index),
                   'startTime': '2026-10-01 08:00:00', 'endTime': '2026-10-01 09:00:00',
                   'durationSeconds': 3600, 'downloadRequested': True, 'outputDirectory': str(directory)}
        row = {'id': identity, 'jobId': 'isolated', 'plate': 'TEST', 'name': version['name'],
               'qualityStart': version['startTime'], 'qualityEnd': version['endTime'],
               'revision': 0, 'activeRevision': 0, 'acceptedRevision': None,
               'requests': {}, 'versions': [version]}
        manager.store.save(row)
        rows.append(row)
    return rows


def controlled_transfers(manager, monkeypatch):
    started, cancelled, gates = [], [], {}

    async def download(row, version):
        identity = row['id']
        started.append(identity)
        gates.setdefault(identity, asyncio.Event())
        part = manager.work / identity / 'v0.part'
        part.parent.mkdir(parents=True, exist_ok=True)
        if not part.exists():
            part.write_bytes(b'checkpoint-' + identity.encode())
        version.update(receivedBytes=part.stat().st_size, totalBytes=part.stat().st_size)
        manager.store.save(row)
        try:
            await gates[identity].wait()
        except asyncio.CancelledError:
            cancelled.append(identity)
            raise
        return part, {'id': version['resultId'], 'url': 'https://storage.test/video.mp4'}, hashlib.sha256(part.read_bytes()).hexdigest(), part.stat().st_size

    async def copy(source, destination, *_args, **_kwargs):
        destination.write_bytes(source.read_bytes())
        source.unlink()

    monkeypatch.setattr(manager, 'download_bound', download)
    monkeypatch.setattr(manager, 'copy_to_final', copy)
    return started, cancelled, gates


@pytest.mark.asyncio
async def test_live_limit_raise_and_lower_preserve_running_downloads_and_fill_queue(app_factory, tmp_path, monkeypatch):
    app = app_factory()
    manager = app.state.videos
    rows = seed_videos(manager, tmp_path / 'output', 5)
    started, cancelled, gates = controlled_transfers(manager, monkeypatch)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://127.0.0.1') as client:
        try:
            response = await client.post('/api/videos/download-limit', json={
                'maxConcurrentDownloads': 1, 'expectedRevision': 0, 'requestId': 'one'})
            assert response.status_code == 200
            await wait_until(lambda: len(started) == 1)
            assert started == [rows[0]['id']]
            assert manager.download_workers() == {'active': 1, 'queued': 4, 'max': 1, 'revision': 1}
            assert not manager.row_lock(rows[-1]['id']).locked()
            raised = {'maxConcurrentDownloads': 3, 'expectedRevision': 1, 'requestId': 'raise'}
            assert (await client.post('/api/videos/download-limit', json=raised)).status_code == 200
            await wait_until(lambda: len(started) == 3)
            replay = await client.post('/api/videos/download-limit', json=raised)
            assert replay.status_code == 200 and replay.json()['active'] == 3
            lower = await client.post('/api/videos/download-limit', json={
                'maxConcurrentDownloads': 1, 'expectedRevision': 2, 'requestId': 'lower'})
            assert lower.json() == {'active': 3, 'queued': 2, 'max': 1, 'revision': 3}
            for expected, identity in zip((2, 1), started[:2]):
                gates[identity].set()
                await wait_until(lambda: manager.download_workers()['active'] == expected)
                assert len(started) == 3
            gates[started[2]].set()
            await wait_until(lambda: len(started) == 4)
            gates[started[3]].set()
            await wait_until(lambda: len(started) == 5)
            gates[started[4]].set()
            await wait_until(lambda: manager.download_workers()['active'] == 0)
            assert not cancelled
            assert all(manager.store.get(row['id'])['versions'][0]['state'] == 'saved' for row in rows)
            assert len(list((tmp_path / 'output').glob('*.mp4'))) == 5
            assert len(started) == len(set(started))
        finally:
            await manager.shutdown()
            app.state.store.close()


@pytest.mark.asyncio
async def test_eight_downloads_can_run_without_the_old_five_slot_ceiling(app_factory, tmp_path, monkeypatch):
    app = app_factory()
    manager = app.state.videos
    rows = seed_videos(manager, tmp_path / 'output', 10)
    started, cancelled, gates = controlled_transfers(manager, monkeypatch)
    try:
        manager.update_download_limit({'maxConcurrentDownloads': 8, 'expectedRevision': 0, 'requestId': 'eight'})
        # Repeated scheduling cannot reserve more slots or submit duplicate work.
        for _ in range(3):
            manager.schedule_downloads()
        await wait_until(lambda: len(started) == 8)
        assert manager.download_workers() == {'active': 8, 'queued': 2, 'max': 8, 'revision': 1}
        for row in rows:
            gates.setdefault(row['id'], asyncio.Event()).set()
        await wait_until(lambda: len(started) == 10 and not manager.active_downloads())
        assert not cancelled and len(set(started)) == 10
    finally:
        await manager.shutdown()
        app.state.store.close()


@pytest.mark.asyncio
async def test_limit_is_persisted_and_stale_or_reused_requests_cannot_override_it(app_factory):
    app = app_factory()
    manager = app.state.videos
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://127.0.0.1') as client:
        initial = (await client.get('/api/videos?summary=true')).json()['downloadWorkers']
        assert initial == {'active': 0, 'queued': 0, 'max': 5, 'revision': 0}
        payload = {'maxConcurrentDownloads': 2, 'expectedRevision': 0, 'requestId': 'save-limit'}
        assert (await client.post('/api/videos/download-limit', json=payload)).json()['revision'] == 1
        assert (await client.post('/api/videos/download-limit', json=payload)).status_code == 200
        assert (await client.post('/api/videos/download-limit', json={**payload, 'maxConcurrentDownloads': 7})).status_code == 409
        assert (await client.post('/api/videos/download-limit', json={**payload, 'requestId': 'stale'})).status_code == 409
    app.state.store.close()
    restarted = app_factory()
    try:
        assert restarted.state.videos.download_workers() == {'active': 0, 'queued': 0, 'max': 2, 'revision': 1}
        # Idempotent responses also survive a backend restart.
        assert restarted.state.videos.update_download_limit(payload)['max'] == 2
    finally:
        restarted.state.store.close()


@pytest.mark.asyncio
@pytest.mark.parametrize('invalid', [0, 9, -1, True, 1.5, '3', None])
async def test_limit_rejects_non_integer_or_out_of_range_values(app_factory, invalid):
    app = app_factory()
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://127.0.0.1') as client:
            response = await client.post('/api/videos/download-limit', json={
                'maxConcurrentDownloads': invalid, 'expectedRevision': 0, 'requestId': 'invalid'})
            assert response.status_code == 422
            assert app.state.videos.download_workers()['max'] == 5
    finally:
        app.state.store.close()


@pytest.mark.asyncio
async def test_stopping_keeps_breakpoints_and_limit_does_not_restart_manual_review(app_factory, tmp_path, monkeypatch):
    app = app_factory()
    manager = app.state.videos
    rows = seed_videos(manager, tmp_path / 'output', 4)
    rows[-1]['versions'][0].update(state='failed', errorCode='media_incomplete')
    manager.store.save(rows[-1])
    started, _, _ = controlled_transfers(manager, monkeypatch)
    manager.update_download_limit({'maxConcurrentDownloads': 2, 'expectedRevision': 0, 'requestId': 'two'})
    await wait_until(lambda: len(started) == 2)
    await manager.shutdown()
    assert len(started) == 2 and manager.download_workers()['queued'] == 3
    for identity in started:
        assert (manager.work / identity / 'v0.part').read_bytes() == b'checkpoint-' + identity.encode()
        assert manager.store.get(identity)['versions'][0]['state'] == 'ready'
    saved = manager.store.get(rows[-1]['id'])
    assert saved['versions'][0]['errorCode'] == 'media_incomplete'
    assert saved['versions'][0]['taskId'] == rows[-1]['versions'][0]['taskId']
    app.state.store.close()


@pytest.mark.asyncio
async def test_summary_counts_remain_global_when_record_filters_hide_queued_videos(app_factory, tmp_path):
    app = app_factory()
    manager = app.state.videos
    rows = seed_videos(manager, tmp_path / 'output', 3)
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://127.0.0.1') as client:
            response = await client.get('/api/videos?plate=NO_MATCH&compact=true')
            assert response.status_code == 200
            assert response.json()['videos'] == []
            assert response.json()['downloadWorkers'] == {'active': 0, 'queued': 3, 'max': 5, 'revision': 0}
            assert all(manager.store.get(row['id'])['versions'][0]['state'] == 'ready' for row in rows)
    finally:
        app.state.store.close()
