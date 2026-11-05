"""In-process read leases shared by uploads, moves and registered-file cleanup."""
from collections import Counter
from contextlib import contextmanager
from pathlib import Path
import asyncio
import errno
from threading import RLock
import logging


logger = logging.getLogger(__name__)


class FileEnvironmentError(ValueError):
    """Temporary filesystem conditions, never evidence of corrupt media."""

    def __init__(self, message, code='disk_io'):
        super().__init__(message)
        self.code = code


def filesystem_wait_code(error):
    if isinstance(error, FileEnvironmentError):
        return error.code
    if isinstance(error, OSError) and error.errno in {
            errno.EIO, errno.ENOSPC, errno.EDQUOT, errno.EBUSY, errno.ENODEV,
            errno.ENXIO, errno.ETIMEDOUT, errno.EACCES, errno.EPERM, errno.EROFS}:
        return 'disk_io'
    return ''


class ReadProbes:
    """Bound read-only checks without creating replacement threads on timeout.

    A timed-out check keeps its lease until the actual filesystem read ends.
    Mutating file operations must continue to use finish_io instead.
    """

    def __init__(self, wake=None, limit=2, timeout=3):
        self.pending = {}
        self.waiting = set()
        self.wake = wake
        self.limit = limit
        self.timeout = timeout

    async def run(self, key, function, *args):
        task = self.pending.get(key)
        if task is None:
            if len(self.pending) >= self.limit:
                raise FileEnvironmentError('磁盘检查尚未返回，保留进度并自动等待。')
            task = asyncio.create_task(asyncio.to_thread(function, *args))
            self.pending[key] = task

            def finished(done):
                if self.pending.get(key) is done:
                    self.pending.pop(key, None)
                if not done.cancelled():
                    done.exception()  # Observe errors even after a caller timed out.
                notify = key in self.waiting
                self.waiting.discard(key)
                if notify and self.wake:
                    self.wake()

            task.add_done_callback(finished)
        try:
            return await asyncio.wait_for(asyncio.shield(task), self.timeout)
        except TimeoutError:
            self.waiting.add(key)
            raise FileEnvironmentError('磁盘检查超时，保留进度并自动等待。') from None
        except asyncio.CancelledError:
            self.waiting.add(key)
            raise


async def finish_io(function, *args, **kwargs):
    task = asyncio.create_task(asyncio.to_thread(function, *args, **kwargs))
    cancelled = False
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            cancelled = True
        except Exception:
            break
    try:
        result = task.result()
    except Exception:
        if cancelled:
            raise asyncio.CancelledError() from None
        raise
    if cancelled:
        raise asyncio.CancelledError()
    return result


class FileLeases:
    def __init__(self):
        self.readers = Counter()
        self.writers = set()
        self.on_move = lambda source, destination, version: None
        self.move_listeners = []
        self.release_listeners = []
        self.notification_failures = []
        self.lock = RLock()

    def subscribe_moves(self, callback):
        self.move_listeners.append(callback)

    def subscribe_release(self, callback):
        self.release_listeners.append(callback)

    def released(self, key):
        # Wakeups are advisory: a failed listener must not hide the original
        # I/O result or stop other managers from resuming their pending work.
        self._notify(tuple(self.release_listeners), (key,), 'release', required=False)

    def notify_move(self, source, destination, version):
        # Every store receives a verified location even if another subscriber
        # fails. Still fail the operation so its durable move journal is replayed
        # until all subscribers have accepted the same receipt.
        self._notify((self.on_move, *tuple(self.move_listeners)),
                     (source, destination, version), 'move', required=True)

    def _notify(self, callbacks, args, event, *, required):
        failures = []
        for callback in callbacks:
            try:
                callback(*args)
            except Exception as error:
                failures.append(error)
                # Exception messages can contain credentials or media URLs.
                failure = {'event': event, 'errorType': type(error).__name__}
                with self.lock:
                    self.notification_failures.append(failure)
                    self.notification_failures = self.notification_failures[-32:]
                logger.warning('File %s notification failed (%s)', event, type(error).__name__)
        if required and failures:
            raise failures[0]

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
                raise FileEnvironmentError("此视频正在移动或清理，释放后自动继续。", 'file_busy')
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
                raise FileEnvironmentError("文件正在使用，释放后自动继续移动或清理。", 'file_busy')
            self.writers.add(key)
        try:
            yield
        finally:
            with self.lock:
                self.writers.discard(key)
            self.released(key)
