"""Version-specific recovery and read-only historical downloads on a fake platform."""
import asyncio
import hashlib
import json
from pathlib import Path
from urllib.parse import unquote

import httpx
import pytest
from starlette.requests import Request

from app.media import DownloadFailure
from tests.test_simple_flow import app_factory, platform, new_video


@pytest.mark.asyncio
async def test_restart_only_discards_selected_checkpoint_and_replay_keeps_new_progress(app_factory, tmp_path):
    app = app_factory()
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://127.0.0.1') as client:
            row = await new_video(client, app)
            manager = app.state.videos
            manager.configure_directory(str(tmp_path / 'out'))
            manager.schedule = lambda row: None
            version = row['versions'][0]
            version.update(state='failed', errorCode='range_invalid', downloadRequested=True, etag='"old"',
                           receivedBytes=100, totalBytes=200, outputDirectory=str(tmp_path / 'out'))
            manager.store.save(row)
            part = manager.work / row['id'] / 'v0.part'
            part.parent.mkdir(parents=True, exist_ok=True)
            part.write_bytes(b'bad-checkpoint')
            other = manager.work / 'another-record' / 'v2.part'
            other.parent.mkdir(parents=True); other.write_bytes(b'keep-other-progress')
            payload = {'version': 0, 'expectedRevision': 0, 'requestId': 'restart-one'}
            result = await client.post(f'/api/videos/{row["id"]}/restart-download', json=payload)
            assert result.status_code == 202, result.text
            operation = manager.operation_tasks.get(result.json()['operationId'])
            if operation:
                await operation
            saved = manager.store.get(row['id'])
            assert saved['activeRevision'] == 0 and saved['revision'] == 0
            assert saved['versions'][0]['taskId'] == version['taskId']
            assert saved['versions'][0]['resultId'] == version['resultId']
            assert saved['versions'][0]['startTime'] == version['startTime']
            assert saved['versions'][0]['downloadRequested']
            assert 'etag' not in saved['versions'][0] and not part.exists()
            assert other.read_bytes() == b'keep-other-progress'
            part.write_bytes(b'new-progress')
            assert (await client.post(f'/api/videos/{row["id"]}/restart-download', json=payload)).status_code == 202
            assert part.read_bytes() == b'new-progress'
            async with manager.row_lock(row['id']):
                replay = await asyncio.wait_for(client.post(f'/api/videos/{row["id"]}/restart-download', json=payload), 1)
                assert replay.status_code == 202 and part.read_bytes() == b'new-progress'
            await client.post(f'/api/videos/{row["id"]}/cancel-download', json={**payload, 'requestId':'stop'})
            corrected = await client.post(f'/api/videos/{row["id"]}/corrections', json={
                'startTime':'2026-09-30 02:40:00','durationSeconds':1800,'expectedRevision':0,'requestId':'new-correction'})
            assert corrected.status_code == 202
            stale = await client.post(f'/api/videos/{row["id"]}/restart-download', json={**payload,'requestId':'stale-reset'})
            assert stale.status_code == 409 and part.read_bytes() == b'new-progress'
            assert manager.store.get(row['id'])['activeRevision'] == 1


@pytest.mark.asyncio
async def test_restart_never_removes_a_symlink_or_its_target(app_factory, tmp_path):
    app = app_factory()
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://127.0.0.1') as client:
            row = await new_video(client, app)
            manager = app.state.videos; manager.configure_directory(str(tmp_path / 'out'))
            row['versions'][0].update(state='failed', errorCode='range_invalid')
            manager.store.save(row)
            target = tmp_path / 'user-video'; target.write_bytes(b'preserve-user-file')
            part = manager.work / row['id'] / 'v0.part'
            part.parent.mkdir(parents=True, exist_ok=True); part.symlink_to(target)
            result = await client.post(f'/api/videos/{row["id"]}/restart-download', json={
                'version':0,'expectedRevision':0,'requestId':'unsafe-path'})
            assert result.status_code == 202
            operation = manager.operation_tasks.get(result.json()['operationId'])
            if operation:
                await operation
            assert manager.store.get_operation(result.json()['operationId'])['state'] == 'attention'
            assert target.read_bytes() == b'preserve-user-file' and part.is_symlink()


@pytest.mark.asyncio
@pytest.mark.parametrize('changed', [False, True])
async def test_full_checkpoint_with_headerless_416_requires_exact_probe_version(app_factory, platform, monkeypatch, changed):
    original = platform.handle
    def handle(request):
        result = original(request)
        if result.status_code == 416:
            return httpx.Response(416)
        if changed and request.headers.get('range') == 'bytes=0-0' and result.status_code == 206:
            result.headers['etag'] = '"new-object"'
        return result
    monkeypatch.setattr(platform, 'handle', handle)
    app = app_factory()
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://127.0.0.1') as client:
            row = await new_video(client, app)
            manager = app.state.videos; version = row['versions'][0]
            version.update(etag='"v1"', totalBytes=len(platform.media))
            manager.store.save(row)
            part = manager.work / row['id'] / 'v0.part'
            part.parent.mkdir(parents=True, exist_ok=True); part.write_bytes(platform.media)
            manager.ffprobe = lambda path: asyncio.sleep(0, result=60.0)
            if changed:
                with pytest.raises(DownloadFailure, match='断点'):
                    await manager.download_bound(row, version)
            else:
                result = await manager.download_bound(row, version)
                assert result[0] == part and result[3] == len(platform.media)
            assert part.read_bytes() == platform.media


@pytest.mark.asyncio
async def test_historical_download_ranges_do_not_change_active_or_accepted_version(app_factory, platform):
    app = app_factory()
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://127.0.0.1') as client:
            row = await new_video(client, app)
            corrected = await client.post(f'/api/videos/{row["id"]}/corrections', json={
                'startTime':'2026-09-30 02:40:00','durationSeconds':1800,'expectedRevision':0,'requestId':'correct'})
            assert corrected.status_code == 202
            before = app.state.videos.store.get(row['id'])
            result = await client.get(f'/api/videos/{row["id"]}/versions/0/download', headers={'Range':'bytes=0-15'})
            assert result.status_code == 206 and result.content == platform.media[:16]
            assert result.headers['content-range'] == f'bytes 0-15/{len(platform.media)}'
            filename = unquote(result.headers['content-disposition'])
            assert 'attachment' in filename and '_v1.mp4' in filename and row['name'] in filename
            assert app.state.videos.store.get(row['id']) == before
            assert len(platform.parents) == 2 and not app.state.videos.file_leases.readers


@pytest.mark.asyncio
async def test_local_saved_history_is_hash_verified_and_supports_head(app_factory, tmp_path):
    app = app_factory()
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://127.0.0.1') as client:
            row = await new_video(client, app)
            path = tmp_path / 'verified.mp4'; path.write_bytes(b'known-local-version')
            version = row['versions'][0]
            version.update(state='saved', filePath=str(path), sha256=hashlib.sha256(path.read_bytes()).hexdigest())
            app.state.videos.store.save(row)
            url = f'/api/videos/{row["id"]}/versions/0/download'
            result = await client.head(url)
            assert result.status_code == 200 and not result.content
            assert (await client.get(url)).content == b'known-local-version'
            path.write_bytes(b'changed-file')
            assert (await client.get(url)).status_code == 400
            assert not app.state.videos.file_leases.readers


@pytest.mark.asyncio
async def test_cloud_cleanup_waits_for_historical_download_and_releases_after_response(app_factory):
    app = app_factory()
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://127.0.0.1') as client:
            row = await new_video(client, app)
            endpoint = next(route.endpoint for route in app.routes if getattr(route, 'path', '') ==
                            '/api/videos/{identity}/versions/{revision}/download')
            scope = {'type':'http','method':'GET','headers':[], 'asgi':{'version':'3.0','spec_version':'2.4'}}
            response = await endpoint(row['id'], 0, Request(scope))
            key = app.state.videos.cloud_lease_key(row['versions'][0]['taskId'])
            assert app.state.videos.file_leases.busy(key)
            with pytest.raises(ValueError):
                await app.state.videos.cleanup_cloud(row, row['versions'][0], protected=set())
            async def receive(): return {'type':'http.request','body':b'','more_body':False}
            async def send(message): pass
            await response(scope, receive, send)
            assert not app.state.videos.file_leases.busy(key)


@pytest.mark.asyncio
async def test_cancelled_historical_response_releases_cleanup_guard(app_factory):
    app = app_factory()
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://127.0.0.1') as client:
            row = await new_video(client, app)
            endpoint = next(route.endpoint for route in app.routes if getattr(route, 'path', '') ==
                            '/api/videos/{identity}/versions/{revision}/download')
            scope = {'type':'http','method':'GET','headers':[], 'asgi':{'version':'3.0','spec_version':'2.4'}}
            response = await endpoint(row['id'], 0, Request(scope))
            async def receive(): return {'type':'http.request','body':b'','more_body':False}
            async def send(message): raise asyncio.CancelledError()
            with pytest.raises(asyncio.CancelledError): await response(scope, receive, send)
            assert not app.state.videos.file_leases.readers


@pytest.mark.asyncio
async def test_deleted_history_is_not_recreated_or_downloaded(app_factory, platform):
    app = app_factory()
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://127.0.0.1') as client:
            row = await new_video(client, app)
            row['versions'][0]['state'] = 'cloud_deleted'; app.state.videos.store.save(row)
            response = await client.get(f'/api/videos/{row["id"]}/versions/0/download')
            assert response.status_code == 404 and len(platform.parents) == 1
