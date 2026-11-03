"""Small contract checks for the isolated real-media rehearsal transport."""
import hashlib
import shutil
import subprocess
from datetime import timedelta

import httpx
import pytest

from real_video_capture import CapturePlatform, FileStream, source_handler
from app.domain import DEFAULT_TARGET
from app.main import create_app
from app.media import DownloadFailure
from app.remote import RemoteClient


@pytest.mark.asyncio
async def test_file_stream_is_bounded_and_honors_interrupt(tmp_path):
    source = tmp_path / "media.mp4"
    source.write_bytes(b"0123456789" * 200_000)
    blocks = [block async for block in FileStream(source, 123, 1_200_000)]
    assert max(map(len, blocks)) <= 1024 * 1024
    assert hashlib.sha256(b"".join(blocks)).digest() == hashlib.sha256(source.read_bytes()[123:1_200_123]).digest()
    seen = 0
    with pytest.raises(httpx.ReadError):
        async for block in FileStream(source, 0, 2_000_000, fail_after=100_000):
            seen += len(block)
    assert seen == 100_000


@pytest.mark.asyncio
async def test_private_mock_range_and_delete_bindings(tmp_path):
    source = tmp_path / "source.mp4"
    source.write_bytes(bytes(range(256)) * 16)
    platform = CapturePlatform(tmp_path / "clips", source, "not-executed", "private-test")
    platform.objects["/one.mp4"] = source
    async with httpx.AsyncClient(transport=httpx.MockTransport(platform.handle)) as client:
        response = await client.get("http://storage.test/one.mp4", headers={"Range": "bytes=21-31"})
        assert response.status_code == 206
        assert response.headers["content-range"] == "bytes 21-31/4096"
        assert response.content == bytes(range(21, 32))
        response = await client.delete("http://storage.test/one.mp4", headers={"If-Match": '"other-version"'})
        assert response.status_code == 412 and "/one.mp4" in platform.objects
        assert source.is_file()
        assert (await client.get("http://unconfigured.invalid/never-egress")).status_code == 404
        assert platform.metrics["unexpectedRequests"] == [{"method": "GET", "path": "/never-egress"}]


@pytest.mark.asyncio
async def test_accept_then_lose_response_creates_one_exact_task(tmp_path):
    source = tmp_path / "source.mp4"
    source.write_bytes(b"real bytes for the transport unit test")
    platform = CapturePlatform(tmp_path / "clips", source, "not-executed", "private-test")
    platform.clip_cache[(0, 3600)] = source
    platform.fail_after_write = True
    task = {"startTime": "2026-09-13 09:54:00", "endTime": "2026-09-13 10:54:00", "videoName": "鲁NH1113"}
    async with httpx.AsyncClient(transport=httpx.MockTransport(platform.handle)) as client:
        with pytest.raises(httpx.ReadError):
            await client.post("http://fixture.invalid/data-acquisition/batchSave", json=[task])
        assert len(platform.parents) == 1 and platform.metrics["createdTasks"] == 1
        response = await client.post("http://fixture.invalid/data-acquisition/condition-query", json={})
        assert response.json()["data"]["total"] == 1
        assert source.read_bytes() == b"real bytes for the transport unit test"


def test_source_fixture_has_strict_boundary_and_identity_cases():
    request = httpx.Request("GET", "http://fixture.invalid/intelligence/intelliTaskInfo/page")
    rows = source_handler(request).json()["data"]["records"]
    assert {row["flowCode"] for row in rows} == {"R-GOOD", "R-SHORT", "R-IDENTITY"}
    request = httpx.Request("GET", "http://fixture.invalid/intelligence/intelliTaskInfo/getCheckDetail?flowCode=R-SHORT")
    detail = source_handler(request).json()["data"]
    assert detail["stationResults"][0]["checkCompleteTime"] == "2026-09-13 09:59:00"


def _download_app(tmp_path, source, handler=None):
    platform = CapturePlatform(tmp_path / "clips", source, "unused", "private-test")
    task_id, result_id = "exact-local-task", "exact-local-result"
    object_path = f"/{task_id}/original.mp4"
    platform.objects[object_path] = source
    start, end = platform.start.isoformat(sep=" "), (platform.start + timedelta(seconds=4)).isoformat(sep=" ")
    platform.parents[task_id] = {"id": task_id, "cameraId": DEFAULT_TARGET["cameraId"], "videoName": "REHEARSAL",
                               "startTime": start, "endTime": end, "taskStatus": "FIXED"}
    platform.results[task_id] = [{"id": result_id, "taskId": task_id, "status": "FINISH",
                                 "url": "http://storage.test" + object_path, "beginTime": start}]
    source_client = RemoteClient("source", transport=httpx.MockTransport(source_handler))
    target_client = RemoteClient("target", transport=httpx.MockTransport(handler(platform) if handler else platform.handle))
    source_client.connected = target_client.connected = True
    app = create_app(source_client, target_client, tmp_path / "database", staging_directory=tmp_path / "staging")
    row = {"id": "private-download", "jobId": "private-job", "name": "REHEARSAL", "plate": "REHEARSAL",
           "revision": 0, "activeRevision": 0, "acceptedRevision": None, "requests": {}, "versions": [{
               "revision": 0, "name": "REHEARSAL", "startTime": start, "endTime": end, "durationSeconds": 4,
               "taskId": task_id, "resultId": result_id, "state": "ready"}]}
    app.state.videos.store.save(row)
    return app, platform, row, source_client, target_client


@pytest.mark.asyncio
@pytest.mark.skipif(not shutil.which("ffmpeg") or not shutil.which("ffprobe"), reason="real media tools required")
async def test_expired_media_link_refreshes_without_losing_platform_session(tmp_path):
    source = tmp_path / "source.mp4"
    subprocess.run([shutil.which("ffmpeg"), "-nostdin", "-v", "error", "-f", "lavfi", "-i",
                    "color=size=64x64:rate=10", "-t", "4", "-c:v", "libx264", str(source)], check=True,
                   capture_output=True, timeout=30)
    observed = {"results": 0, "media": 0, "paths": []}

    def handler(platform):
        async def handle(request):
            if request.url.path.endswith("/data-acquisition/task-result"):
                observed["results"] += 1
                if observed["results"] >= 2:
                    refreshed = "/exact-local-task/refreshed.mp4"
                    platform.objects[refreshed] = source
                    platform.results["exact-local-task"][0]["url"] = "http://storage.test" + refreshed
            if request.url.path.startswith("/platform/oss/"):
                observed["media"] += 1
                observed["paths"].append(request.url.path)
                if observed["media"] == 1:
                    return httpx.Response(403)
            return await platform.handle(request)
        return handle

    app, platform, row, source_client, target_client = _download_app(tmp_path, source, handler)
    try:
        path, result, digest, size = await app.state.videos.download_bound(row, row["versions"][0])
        assert path.is_file() and size == source.stat().st_size
        assert digest == hashlib.sha256(source.read_bytes()).hexdigest()
        assert row["versions"][0]["attempts"] == 2
        assert target_client.connected and observed["results"] >= 2
        assert observed["paths"][0].endswith("/original.mp4") and observed["paths"][1].endswith("/refreshed.mp4")
        assert result["id"] == "exact-local-result" and platform.metrics["createdTasks"] == 0
    finally:
        await source_client.close()
        await target_client.close()


@pytest.mark.asyncio
async def test_download_disk_full_stops_before_writing_or_publishing(tmp_path, monkeypatch):
    source = tmp_path / "source.mp4"
    source.write_bytes(b"\x00\x00\x00\x18ftypisom" + b"not-downloaded" * 100)
    app, platform, row, source_client, target_client = _download_app(tmp_path, source)
    monkeypatch.setattr("app.videos.shutil.disk_usage", lambda _: shutil._ntuple_diskusage(100, 100, 0))
    try:
        with pytest.raises(DownloadFailure) as raised:
            await app.state.videos.download_bound(row, row["versions"][0])
        assert raised.value.code == "disk_full"
        assert row["versions"][0]["attempts"] == 1
        assert not (app.state.videos.work / row["id"] / "v0.part").exists()
        assert source.exists() and platform.metrics["createdTasks"] == 0
    finally:
        await source_client.close()
        await target_client.close()
