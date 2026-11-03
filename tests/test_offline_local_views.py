"""Offline playback and status reads cannot depend on a live platform."""
import asyncio
import json
import time
import threading

import httpx
import pytest

from app.availability import connection_state
from app.connection_restore import ConnectionRestorer
from app.main import create_app
from app.remote import RemoteClient, RemoteError
from app.upload_audit import AthenaClient, AuditError
from tests.test_media_control import collection, sliced
from tests.test_slicing_performance import close_app


@pytest.mark.asyncio
async def test_local_playback_uses_exact_history_version_range_and_read_lease(tmp_path, monkeypatch):
    app = create_app(data_dir=tmp_path / 'db')
    old, new = tmp_path / 'old.mp4', tmp_path / 'new.mp4'
    old.write_bytes(b'old-version-contents'); new.write_bytes(b'new-version-contents')
    row = collection(app, old)
    current = collection(app, new, identity='other', revision=1)['versions'][0]
    row.update(activeRevision=1, revision=1, versions=[row['versions'][0], current])
    app.state.videos.store.save(row)
    async def no_platform(_):
        pytest.fail('local preview must not query the platform')
    monkeypatch.setattr(app.state.videos, 'bound_result', no_platform)
    from app.main import DownloadStreamResponse
    original = DownloadStreamResponse.__call__
    async def leased(response, *args, **kwargs):
        assert app.state.slicing.leases.busy(old) or app.state.slicing.leases.busy(new)
        await original(response, *args, **kwargs)
    monkeypatch.setattr(DownloadStreamResponse, '__call__', leased)
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://127.0.0.1') as client:
            url = '/api/videos/collection/versions/'
            historical = await client.get(url + '0/stream', headers={'Range': 'bytes=0-2'})
            assert historical.status_code == 206 and historical.content == b'old'
            latest = await client.get(url + '1/stream', headers={'Range': 'bytes=0-2'})
            assert latest.status_code == 206 and latest.content == b'new'
            assert (await client.head(url + '0/stream')).headers['content-length'] == str(old.stat().st_size)
            assert (await client.get(url + '0/stream', headers={'Range': 'bytes=999-'})).status_code == 416
            assert (await client.get(url + '0/download')).content == old.read_bytes()
            assert not app.state.slicing.leases.busy(old)
            old.write_bytes(b'changed-version')
            assert (await client.get(url + '0/stream')).status_code == 400
            assert not app.state.slicing.leases.busy(old)
    finally:
        await close_app(app)


@pytest.mark.asyncio
async def test_external_path_replacement_after_validation_does_not_play_wrong_file(tmp_path, monkeypatch):
    from app.local_playback import LocalPlayback
    app = create_app(data_dir=tmp_path / 'db')
    path = tmp_path / 'video.mp4'; path.write_bytes(b'original-verified-video')
    collection(app, path)
    prepare = LocalPlayback.prepare
    async def replacing(self, version):
        result = await prepare(self, version)
        path.rename(tmp_path / 'retained.mp4')
        path.write_bytes(b'wrong-unverified-video')
        return result
    monkeypatch.setattr(LocalPlayback, 'prepare', replacing)
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://127.0.0.1') as client:
            result = await client.get('/api/videos/collection/versions/0/stream')
            assert result.status_code == 200 and result.content == b'original-verified-video'
            assert not app.state.slicing.leases.busy(path)
    finally:
        await close_app(app)


@pytest.mark.asyncio
async def test_playback_checks_do_not_block_health_and_legacy_digest_is_reused(tmp_path, monkeypatch):
    app = create_app(data_dir=tmp_path / 'db')
    path = tmp_path / 'old.mp4'; path.write_bytes(b'legacy-verified-video')
    row = collection(app, path)
    row['versions'][0].pop('fileVersion')
    app.state.videos.store.save(row)
    from app import local_playback
    digest = local_playback.file_digest
    calls = []
    def slow(value):
        calls.append(1); time.sleep(.3)
        return digest(value)
    monkeypatch.setattr(local_playback, 'file_digest', slow)
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://127.0.0.1') as client:
            pending = asyncio.create_task(client.get('/api/videos/collection/versions/0/stream'))
            while not calls:
                await asyncio.sleep(.001)
            start = time.monotonic()
            assert (await client.get('/api/health')).status_code == 200
            assert time.monotonic() - start < .15
            assert (await pending).status_code == 200
            assert (await client.get('/api/videos/collection/versions/0/stream')).status_code == 200
            assert len(calls) == 1
    finally:
        await close_app(app)


@pytest.mark.asyncio
async def test_network_failure_keeps_auth_but_never_claims_reachable():
    def offline(request):
        raise httpx.ConnectError('network-only-test', request=request)
    client = RemoteClient('target', transport=httpx.MockTransport(offline))
    client.connected = True
    try:
        assert connection_state(client)['reachability'] == 'unknown'
        with pytest.raises(RemoteError):
            await client.validate()
        assert connection_state(client) | {'checkedAt': None} == {
            'authenticated': True, 'reachability': 'offline', 'checkedAt': None, 'lastFailure': 'network'}
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_refresh_network_failure_does_not_discard_runtime_credentials():
    def handler(request):
        if request.method == 'POST':
            raise httpx.ConnectError('offline refresh', request=request)
        return httpx.Response(401)
    auth = AthenaClient(transport=httpx.MockTransport(handler))
    auth.access, auth.refresh, auth.user = 'fixture-access', 'fixture-refresh', 'fixture-user'
    try:
        with pytest.raises(AuditError) as error:
            await auth.get_slicing('devel/datasets/')
        assert error.value.code == 'network'
        assert auth.connected and auth.refresh == 'fixture-refresh'
        assert connection_state(auth)['reachability'] == 'offline'
    finally:
        await auth.client.aclose()


@pytest.mark.asyncio
async def test_summary_does_not_hydrate_operation_snapshots_and_details_are_paged(tmp_path, monkeypatch):
    app = create_app(data_dir=tmp_path / 'db')
    manager = app.state.slicing
    try:
        for index in range(25):
            path = tmp_path / f'{index}.mp4'; path.write_bytes(str(index).encode())
            item = sliced(app, path, identity=f'item-{index}', state='complete')
            item['categories']['bed'].update(operationId=f'op-{index}', intervals=[{'start': 0, 'end': 10}])
            manager.store.save('items', item)
            manager.store.save('operations', {'id': f'op-{index}', 'createdAt': '2026-11-01T08:00:00Z',
                'preSubmitImageIds': ['snapshot'] * 1000})
        original_get = manager.store.get
        def get(kind, identity):
            assert kind != 'operations', 'summary dates must not hydrate task snapshots'
            return original_get(kind, identity)
        monkeypatch.setattr(manager.store, 'get', get)
        summary = manager.state(summary=True)
        assert summary['recordDays'][0]['total'] == 25
        assert 'intervals' not in summary['items'][0]['categories']['bed']
        assert 'snapshot' not in json.dumps(summary)
        page = manager.state(records_day='2026-11-01', page=999)
        assert page['recordPagination']['page'] == 2 and len(page['items']) == 5
        assert page['items'][0]['categories']['bed']['intervals'] == [{'start': 0, 'end': 10}]
    finally:
        await close_app(app)


@pytest.mark.asyncio
async def test_connection_restore_is_independent_and_explicit_disconnect_cancels_retry():
    manager = ConnectionRestorer(delay=.01)
    blocked = asyncio.Event()
    other_done = asyncio.Event()
    calls = []
    async def waiting():
        calls.append(1)
        await blocked.wait()
        return True
    async def other():
        other_done.set()
        return False
    manager.start('source', waiting)
    manager.start('visual', other)
    await asyncio.wait_for(other_done.wait(), .1)
    assert len(calls) == 1
    await manager.stop('source')
    blocked.set()
    await asyncio.sleep(.03)
    assert len(calls) == 1
    await manager.shutdown()


@pytest.mark.asyncio
async def test_cleaned_saved_history_cannot_be_enrolled_by_startup(tmp_path):
    app = create_app(data_dir=tmp_path / 'db')
    path = tmp_path / 'already-cleaned.mp4'; path.write_bytes(b'fixture')
    row = collection(app, path)
    row['versions'][0]['localCleanupState'] = 'complete'
    app.state.videos.store.save(row)
    path.unlink()
    try:
        app.state.pipeline.notify_saved(row['id'], 0)
        assert not app.state.pipeline.rows('events')
    finally:
        await close_app(app)


@pytest.mark.asyncio
async def test_repeated_stop_keeps_file_lease_until_disk_thread_really_finishes(tmp_path):
    from app.file_leases import FileLeases, finish_io
    leases = FileLeases()
    entered, released = threading.Event(), threading.Event()
    path = tmp_path / 'file'
    def disk_operation():
        entered.set()
        assert released.wait(2)
    async def work():
        with leases.write(path):
            await finish_io(disk_operation)
    task = asyncio.create_task(work())
    try:
        while not entered.is_set():
            await asyncio.sleep(.001)
        task.cancel(); await asyncio.sleep(.01)
        task.cancel(); await asyncio.sleep(.01)
        assert leases.busy(path) and not task.done()
    finally:
        released.set()
        await asyncio.gather(task, return_exceptions=True)
    assert not leases.busy(path)
