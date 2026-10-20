"""In-process read leases shared by uploads, moves and registered-file cleanup."""
from collections import Counter
from contextlib import contextmanager
from pathlib import Path


class FileLeases:
    def __init__(self):
        self.readers = Counter()
        self.writers = set()
        self.on_move = lambda source, destination, version: None
        self.move_listeners = []

    def subscribe_moves(self, callback):
        self.move_listeners.append(callback)

    def notify_move(self, source, destination, version):
        self.on_move(source, destination, version)
        for callback in self.move_listeners:
            callback(source, destination, version)

    @staticmethod
    def key(value):
        return str(Path(value).absolute())

    def busy(self, value):
        return self.key(value) in self.writers or self.readers[self.key(value)] > 0

    @contextmanager
    def read(self, value):
        key = self.key(value)
        if key in self.writers:
            raise ValueError("此视频正在移动或清理，上传稍后继续。")
        self.readers[key] += 1
        try:
            yield
        finally:
            self.readers[key] -= 1
            if not self.readers[key]:
                del self.readers[key]

    @contextmanager
    def write(self, value):
        key = self.key(value)
        if self.busy(value):
            raise ValueError("此视频正在上传，保留原文件；上传结束后可重试移动或清理。")
        self.writers.add(key)
        try:
            yield
        finally:
            self.writers.discard(key)
