"""Version-bound local playback, without a platform round trip or media copy."""
from contextlib import ExitStack
import os
from pathlib import Path

from .file_leases import finish_io
from .media import file_digest


class LocalPlayback:
    def __init__(self, leases):
        self.leases = leases
        self.verified = {}

    def _prepare(self, version):
        from .slicing import source_version
        recovery = version.get('recovery') or {}
        value = version.get('filePath') or recovery.get('filePath')
        if not value:
            return None
        path = Path(value)
        guards = ExitStack()
        try:
            guards.enter_context(self.leases.read(path))
            if path.is_symlink():
                raise ValueError('本地视频路径已变化，请先核对文件。')
            if not path.exists():
                guards.close()
                return None
            actual = source_version(path)
            expected = version.get('fileVersion') if version.get('filePath') else recovery.get('fileVersion')
            digest = version.get('sha256') if version.get('filePath') else recovery.get('sha256')
            if expected and expected.get('edgeHash') and expected.get('size') is not None:
                if any(actual.get(key) != expected[key] for key in expected if key in actual):
                    raise ValueError('登记的本地视频版本已变化，请先核对；未播放其他版本。')
            elif digest:
                key = (str(path), digest, tuple(sorted(actual.items())))
                if key not in self.verified:
                    if file_digest(path) != digest or source_version(path) != actual:
                        raise ValueError('登记的本地视频已变化，请先核对；未播放其他版本。')
                    if len(self.verified) >= 64:
                        self.verified.pop(next(iter(self.verified)))
                    self.verified[key] = True
            else:
                raise ValueError('本地视频缺少版本凭证，请先核对文件。')
            descriptor = os.open(path, os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0))
            stream = os.fdopen(descriptor, 'rb')
            guards.callback(stream.close)
            info = os.fstat(stream.fileno())
            pinned = {'device': info.st_dev, 'inode': info.st_ino, 'size': info.st_size,
                      'modifiedNs': info.st_mtime_ns}
            if any(actual[key] != value for key, value in pinned.items()):
                raise ValueError('本地视频在核验期间发生变化，请重新核对。')
            # Serve this verified inode, not a pathname that another app could
            # replace between this check and the response opening its body.
            return path, guards.close, stream, pinned
        except BaseException:
            guards.close()
            raise

    async def prepare(self, version):
        # Cancellation must not release a read lease while its disk check runs.
        result = None
        def check():
            nonlocal result
            result = self._prepare(version)
            return result
        try:
            return await finish_io(check)
        except BaseException:
            if result:
                result[1]()
            raise
