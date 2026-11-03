"""Slow removable-volume writes must not block UI reads or race cancellation."""
import asyncio
import hashlib
import os
from pathlib import Path
import threading
import time

import httpx
import pytest

from app.main import create_app
from app.remote import RemoteClient
from tests.test_slicing_performance import close_app


class TrackedFile:
    def __init__(self, raw, stage, entered, released, finished, *, timed):
        self.raw, self.stage = raw, stage
        self.entered, self.released, self.finished = entered, released, finished
        self.timed = timed
        self.working = False
        self.closed_while_working = False
        self.worker_thread = None

    def block(self):
        self.worker_thread = threading.get_ident()
        self.working = True
        self.entered.set()
        if self.timed:
            time.sleep(.25)
        else:
            assert self.released.wait(3), 'test must release its simulated disk operation'

    def write(self, value):
        if self.stage != 'write':
            return self.raw.write(value)
        try:
            self.block()
            return self.raw.write(value)
        finally:
            self.working = False
            self.finished.set()

    def flush(self):
        return self.raw.flush()

    def fileno(self):
        return self.raw.fileno()

    def close(self):
        self.closed_while_working |= self.working
        return self.raw.close()


def download_environment(tmp_path, monkeypatch, stage, *, timed):
    media = b'\0\0\0\x18ftypisom' + b'actual-downloaded-bytes' * 100
    target = RemoteClient('target', transport=httpx.MockTransport(lambda _: httpx.Response(
        200, content=media, headers={'content-type': 'video/mp4', 'etag': '"fixed-version"'})))
    app = create_app(target=target, data_dir=tmp_path / 'db')
    manager = app.state.videos
    row = {'id': 'isolated-download', 'revision': 0, 'activeRevision': 0, 'acceptedRevision': None,
           'requests': {}, 'versions': [{'revision': 0, 'state': 'ready', 'resultId': 'bound-result'}]}
    version = row['versions'][0]
    manager.store.save(row)
    async def result(_):
        return {'id': 'bound-result', 'url': 'https://storage.test/video.mp4'}
    monkeypatch.setattr(manager, 'bound_result', result)
    monkeypatch.setattr(manager, 'ffprobe', lambda _: asyncio.sleep(0))
    entered, released, finished = (threading.Event() for _ in range(3))
    original_open, original_fsync = Path.open, os.fsync
    tracked = []
    def opened(path, mode='r', *args, **kwargs):
        raw = original_open(path, mode, *args, **kwargs)
        if path.name == 'v0.part' and mode in {'wb', 'ab'}:
            raw = TrackedFile(raw, stage, entered, released, finished, timed=timed)
            tracked.append(raw)
        return raw
    def synced(fd):
        if stage == 'fsync' and tracked and fd == tracked[0].fileno():
            try:
                tracked[0].block()
                return original_fsync(fd)
            finally:
                tracked[0].working = False
                finished.set()
        return original_fsync(fd)
    monkeypatch.setattr(Path, 'open', opened)
    monkeypatch.setattr('app.videos.os.fsync', synced)
    return app, target, manager, row, version, media, tracked, entered, released, finished


@pytest.mark.asyncio
@pytest.mark.parametrize('stage', ['write', 'fsync'])
async def test_slow_download_disk_io_keeps_health_responsive(tmp_path, monkeypatch, stage):
    app, target, manager, row, version, media, tracked, entered, released, finished = download_environment(
        tmp_path, monkeypatch, stage, timed=True)
    task = asyncio.create_task(manager.download_bound(row, version))
    try:
        assert await asyncio.to_thread(entered.wait, 2)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://127.0.0.1') as client:
            start = time.monotonic()
            response = await client.get('/api/health')
            assert response.status_code == 200
            assert time.monotonic() - start < .15
            assert not finished.is_set(), 'health must respond while the disk operation is still running'
        path, _, digest, size = await task
        assert size == len(media) and path.read_bytes() == media
        assert digest == hashlib.sha256(media).hexdigest()
        assert tracked[0].worker_thread != threading.get_ident()
        assert tracked[0].raw.closed and not tracked[0].closed_while_working
    finally:
        released.set()
        await asyncio.gather(task, return_exceptions=True)
        await close_app(app)
        await target.close()


@pytest.mark.asyncio
@pytest.mark.parametrize('stage', ['write', 'fsync'])
async def test_cancellation_waits_for_inflight_disk_io_before_closing(tmp_path, monkeypatch, stage):
    app, target, manager, row, version, media, tracked, entered, released, finished = download_environment(
        tmp_path, monkeypatch, stage, timed=False)
    task = asyncio.create_task(manager.download_bound(row, version))
    try:
        assert await asyncio.to_thread(entered.wait, 2)
        task.cancel()
        await asyncio.sleep(.03)
        assert not task.done() and not tracked[0].raw.closed
        assert not finished.is_set()
        released.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert finished.is_set() and tracked[0].raw.closed
        assert not tracked[0].closed_while_working
        part = manager.work / row['id'] / 'v0.part'
        assert part.read_bytes() == media, 'the safely closed partial remains available for Range recovery'
    finally:
        released.set()
        await asyncio.gather(task, return_exceptions=True)
        await close_app(app)
        await target.close()
