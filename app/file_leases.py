"""In-process read leases shared by uploads, moves and registered-file cleanup."""
from collections import Counter
from contextlib import contextmanager
from pathlib import Path
import asyncio
from threading import RLock


async def finish_io(function, *args, **kwargs):
    task = asyncio.create_task(asyncio.to_thread(function, *args, **kwargs))
    try:
        return await asyncio.shield(task)
    except asyncio.CancelledError:
        await asyncio.gather(task, return_exceptions=True)
        raise


class FileLeases:
    def __init__(self):
        self.readers = Counter()
        self.writers = set()
        self.on_move = lambda source, destination, version: None
        self.move_listeners = []
        self.release_listeners = []
        self.lock = RLock()

    def subscribe_moves(self, callback):
        self.move_listeners.append(callback)

    def subscribe_release(self, callback):
        self.release_listeners.append(callback)

    def released(self, key):
        for callback in self.release_listeners:
            callback(key)

    def notify_move(self, source, destination, version):
        self.on_move(source, destination, version)
        for callback in self.move_listeners:
            callback(source, destination, version)

    @staticmethod
    def key(value):
        return str(Path(value).absolute())

    def busy(self, value):
        with self.lock:
            return self.key(value) in self.writers or self.readers[self.key(value)] > 0

    @contextmanager
    def read(self, value):
        key = self.key(value)
        with self.lock:
            if key in self.writers:
                raise ValueError("此视频正在移动或清理，上传稍后继续。")
            self.readers[key] += 1
        try:
            yield
        finally:
            released = False
            with self.lock:
                self.readers[key] -= 1
                if not self.readers[key]:
                    del self.readers[key]
                    released = True
            if released:
                self.released(key)

    @contextmanager
    def write(self, value):
        key = self.key(value)
        with self.lock:
            if self.busy(value):
                raise ValueError("文件正在使用，释放后自动继续移动或清理。")
            self.writers.add(key)
        try:
            yield
        finally:
            with self.lock:
                self.writers.discard(key)
            self.released(key)
