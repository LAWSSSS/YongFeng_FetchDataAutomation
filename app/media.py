import ctypes
import errno
import hashlib
import os
import re
import shutil
import stat
import sys
import tempfile
import time
from contextlib import contextmanager
from uuid import uuid4
from pathlib import Path
from urllib.parse import urlsplit

from .domain import ORIGIN


class DownloadFailure(ValueError):
    """Only controlled messages and categories may enter persisted diagnostics."""
    def __init__(self, message: str, code: str, *, retryable: bool = False):
        super().__init__(message)
        self.code, self.retryable = code, retryable


def download_url(value: str) -> str:
    """与原站下载按钮的 proxyImage 一致，origin 参数不能百分号转义。"""
    parsed = urlsplit(value)
    if (parsed.scheme not in ("http", "https") or not parsed.hostname or parsed.username
            or parsed.password or any(part == ".." for part in parsed.path.split("/"))):
        raise ValueError("原站视频下载地址无效。")
    return f"{ORIGIN}/platform/oss/{parsed.path.lstrip('/')}?origin={parsed.scheme}://{parsed.netloc}"


def video_filename(item: dict, result: dict) -> str:
    name = re.sub(r'[\\/:*?"<>|\x00-\x1f]', "_", item["videoName"]).strip(" .")[:80] or "video"
    stamp = re.sub(r"[^0-9]", "", str(result.get("beginTime") or item.get("startTime") or ""))[:14]
    identity = hashlib.sha256(str(result["id"]).encode()).hexdigest()[:12]
    suffix = Path(urlsplit(result["url"]).path).suffix.lower()
    if suffix not in (".mp4", ".mov", ".mkv", ".webm", ".avi", ".ts"):
        suffix = ".mp4"
    return f"{name}_{stamp}_{identity}{suffix}"


def valid_video_header(data: bytes) -> bool:
    return (b"ftyp" in data[:32] or data.startswith(b"\x1aE\xdf\xa3")
            or (data.startswith(b"RIFF") and data[8:12] == b"AVI ")
            or (len(data) > 188 and data[0] == data[188] == 0x47))


def file_digest(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(4 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def edge_digest(stream, size: int) -> str:
    digest = hashlib.sha256(stream.read(65536))
    if size > 65536:
        stream.seek(size - 65536)
        digest.update(stream.read(65536))
    return digest.hexdigest()


def fingerprint(info):
    return {"device": info.st_dev, "inode": info.st_ino, "size": info.st_size, "modifiedNs": info.st_mtime_ns}


@contextmanager
def _bound_media(path):
    flags = os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0) | getattr(os, 'O_NONBLOCK', 0)
    with os.fdopen(os.open(path, flags), 'rb') as stream:
        info = os.fstat(stream.fileno())
        if not stat.S_ISREG(info.st_mode):
            raise OSError(errno.EINVAL, '视频路径不是普通文件。')
        version = fingerprint(info)
        yield stream, version
        after = os.fstat(stream.fileno())
        if (fingerprint(after) != version or after.st_ctime_ns != info.st_ctime_ns
                or fingerprint(path.lstat()) != version):
            raise OSError(errno.ESTALE, '验证期间视频文件发生变化。')


def media_version(path):
    with _bound_media(Path(path)) as (stream, version):
        return {**version, 'edgeHash': edge_digest(stream, version['size'])}


def checked_media_digest(path, synchronize=False):
    with _bound_media(Path(path)) as (stream, version):
        if synchronize:
            os.fsync(stream.fileno())
        observed = {**version, 'edgeHash': edge_digest(stream, version['size'])}
        stream.seek(0)
        return hashlib.file_digest(stream, 'sha256').hexdigest(), observed


def verify_publication(path, prepared, receipt_version=None):
    observed = media_version(path)
    if observed['size'] != prepared.get('bytes') or (receipt_version and observed != receipt_version):
        raise OSError(errno.ESTALE, '保存的视频与发布凭证不一致。')
    if receipt_version or observed == prepared.get('sourceVersion'):
        return observed
    # Legacy or cross-volume interruption has no destination inode receipt.
    digest, verified = checked_media_digest(path)
    if verified != observed or digest != prepared.get('sha256'):
        raise OSError(errno.ESTALE, '无法确认目标文件是本次发布的视频。')
    return verified


def copy_exclusive(source: Path, destination: Path, expected_digest: str | None = None):
    """Portable publication for filesystems without exclusive rename or hard links."""
    created = None
    try:
        with os.fdopen(os.open(source, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)), "rb") as incoming:
            original = os.fstat(incoming.fileno())
            if not stat.S_ISREG(original.st_mode):
                raise OSError(errno.EINVAL, "发布源文件不是普通文件。")
            if shutil.disk_usage(destination.parent).free < original.st_size + 16 * 1024 * 1024:
                raise OSError(errno.ENOSPC, "最终文件夹空间不足，已保留原视频。")
            flags = os.O_RDWR | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
            with os.fdopen(os.open(destination, flags, 0o600), "w+b") as output:
                created = os.fstat(output.fileno())
                try:
                    digest = hashlib.sha256()
                    for block in iter(lambda: incoming.read(4 * 1024 * 1024), b""):
                        output.write(block)
                        digest.update(block)
                    output.flush()
                    os.fsync(output.fileno())
                    after = os.fstat(incoming.fileno())
                    if (after.st_size, after.st_mtime_ns) != (original.st_size, original.st_mtime_ns):
                        raise OSError(errno.ESTALE, "发布源文件在复制期间发生变化。")
                    copied_digest = digest.hexdigest()
                    if expected_digest and copied_digest != expected_digest:
                        raise OSError(errno.EIO, "发布文件摘要不一致。")
                    output.seek(0)
                    verified = hashlib.sha256()
                    for block in iter(lambda: output.read(4 * 1024 * 1024), b""):
                        verified.update(block)
                    held = os.fstat(output.fileno())
                    if verified.hexdigest() != copied_digest or held.st_size != original.st_size:
                        raise OSError(errno.EIO, "发布文件复制校验失败。")
                    current = destination.lstat()
                    # ExFAT replaces its synthetic empty-file ID when it allocates data clusters.
                    # Compare the path with the currently open handle, never its empty-file ID.
                    if not stat.S_ISREG(current.st_mode) or (current.st_dev, current.st_ino) != (held.st_dev, held.st_ino):
                        raise OSError(errno.ESTALE, "发布路径被其他文件替换，已保留外部文件。")
                finally:
                    try:
                        created = os.fstat(output.fileno())
                    except OSError:
                        pass
        current_source = source.lstat()
        if not stat.S_ISREG(current_source.st_mode) or (current_source.st_dev, current_source.st_ino,
                current_source.st_size, current_source.st_mtime_ns) != (original.st_dev, original.st_ino,
                original.st_size, original.st_mtime_ns):
            raise OSError(errno.ESTALE, "发布源路径发生变化，已保留外部文件。")
        source.unlink()
        return copied_digest  # The final path was checked through its open handle after fsync and re-read.
    except BaseException:
        if created:
            try:
                current = destination.lstat()
                if stat.S_ISREG(current.st_mode) and (current.st_dev, current.st_ino) == (created.st_dev, created.st_ino):
                    destination.unlink()
            except OSError:
                pass
        raise


_RENAME_UNSUPPORTED = {errno.ENOTSUP, errno.EOPNOTSUPP, errno.ENOSYS}


def _rename_exclusive(source, destination):
    if sys.platform == "darwin":
        # macOS SDK sys/stdio.h：RENAME_EXCL=4；无需文件系统支持硬链接。
        library = ctypes.CDLL(None, use_errno=True)
        rename = library.renamex_np
        rename.argtypes = [ctypes.c_char_p, ctypes.c_char_p, ctypes.c_uint]
        rename.restype = ctypes.c_int
        if rename(os.fsencode(source), os.fsencode(destination), 4):
            code = ctypes.get_errno()
            raise OSError(code, os.strerror(code))
    elif sys.platform == "win32":
        os.rename(source, destination)  # Windows 的 rename 不覆盖已有目标。
    else:
        os.link(source, destination)
        source.unlink()


def publish_file(source: Path, destination: Path, expected_digest: str | None = None):
    try:
        _rename_exclusive(source, destination)
    except OSError as error:
        if error.errno not in _RENAME_UNSUPPORTED | {errno.EXDEV}:
            raise
        return copy_exclusive(source, destination, expected_digest)
    return expected_digest  # The source was hashed before the same-filesystem atomic move.


def transfer_reservation_identity(destination: Path, reservation: dict):
    """Recognize only our small ownership marker, never read back a copied video."""
    token = reservation.get("token", "")
    if not re.fullmatch(r"[0-9a-f]{32}", token):
        raise FileExistsError(errno.EEXIST, "目标文件名已存在。")
    marker = b"simple-video-transfer-reservation:" + token.encode("ascii")
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0)
    with os.fdopen(os.open(destination, flags), "rb") as held:
        identity = os.fstat(held.fileno())
        if (not stat.S_ISREG(identity.st_mode) or identity.st_size != len(marker)
                or held.read(len(marker)) != marker):
            raise FileExistsError(errno.EEXIST, "目标文件名已存在。")
        return identity


def publish_with_reservation(temporary: Path, destination: Path, report=None, reservation=None):
    """ExFAT lacks RENAME_EXCL: reserve the final name before its same-folder rename.

    A recorded marker allows retry after a crash. An existing video is never
    claimed as a reservation or removed to make room for this transfer.
    """
    reservation = reservation or {"token": uuid4().hex}
    token = reservation.get("token", "")
    if not re.fullmatch(r"[0-9a-f]{32}", token):
        raise OSError(errno.EINVAL, "转移占位记录无效。")
    marker = b"simple-video-transfer-reservation:" + token.encode("ascii")
    if report:
        report(reservation=reservation)
    try:
        fd = os.open(destination, os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0), 0o600)
    except FileExistsError:
        held = transfer_reservation_identity(destination, reservation)
    else:
        with os.fdopen(fd, "wb") as reserved:
            reserved.write(marker)
            reserved.flush()
            os.fsync(reserved.fileno())
            held = os.fstat(reserved.fileno())
    current = destination.lstat()
    if (not stat.S_ISREG(current.st_mode) or current.st_size != len(marker)
            or (current.st_dev, current.st_ino) != (held.st_dev, held.st_ino)):
        raise OSError(errno.ESTALE, "保存路径被其他文件替换。")
    os.replace(temporary, destination)


def copy_to_destination(source: Path, destination: Path, report=None, partial_path=None, keep_partial=False,
                        reservation=None, verified_source=None, expected_digest=None):
    """Publish a verified download by an exclusive move when both paths share a volume.

    The destination only becomes visible after the write is flushed and atomically
    published. The local source is removed only after that publication succeeds.
    """
    temporary = None
    stage = "source_open"
    def progress(**details):
        if report:
            report({"stage": stage, **details})
    try:
        progress()
        if verified_source:
            if media_version(source) != verified_source:
                raise OSError(errno.ESTALE, '已验证的下载文件发生变化。')
            if not partial_path and not reservation and destination.parent.stat().st_dev == verified_source['device']:
                stage = 'publish'
                progress(total=verified_source['size'])
                try:
                    _rename_exclusive(source, destination)
                except OSError as error:
                    if error.errno not in _RENAME_UNSUPPORTED | {errno.EXDEV}:
                        raise
                else:
                    version = verify_publication(destination, {'bytes': verified_source['size'],
                        'sha256': expected_digest, 'sourceVersion': verified_source})
                    stage = 'source_remove'
                    progress(bytes=version['size'], total=version['size'], fileVersion=version)
                    return True
        with os.fdopen(os.open(source, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)), "rb") as incoming:
            original = os.fstat(incoming.fileno())
            if not stat.S_ISREG(original.st_mode):
                raise OSError(errno.EINVAL, "下载源不是普通文件。")
            source_identity = fingerprint(original)
            if verified_source and any(verified_source.get(key) != value for key, value in source_identity.items()):
                raise OSError(errno.ESTALE, '已验证的下载文件发生变化。')
            stage = "temporary_open" if partial_path else "temporary_create"
            candidate = Path(partial_path) if partial_path else destination.with_name(f".{destination.name}.{uuid4().hex}.uploading")
            if (candidate.parent != destination.parent or not candidate.name.startswith("." + destination.name + ".")
                    or not candidate.name.endswith(".uploading")):
                raise OSError(errno.EINVAL, "转移断点不属于当前视频。")
            progress(total=original.st_size, temporary=str(candidate))
            flags = os.O_WRONLY | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0)
            flags |= os.O_APPEND if partial_path else os.O_EXCL
            fd = os.open(candidate, flags, 0o600)
            temporary = candidate
            with os.fdopen(fd, "ab" if partial_path else "wb") as outgoing:
                offset = os.fstat(outgoing.fileno()).st_size if partial_path else 0
                if offset > original.st_size:
                    raise OSError(errno.ESTALE, "转移断点超出本机文件长度。")
                incoming.seek(offset)
                stage = "copy"
                copied, last_report = offset, 0.0
                progress(bytes=copied, total=original.st_size)
                for block in iter(lambda: incoming.read(4 * 1024 * 1024), b""):
                    outgoing.write(block)
                    copied += len(block)
                    if time.monotonic() - last_report >= 1:
                        progress(bytes=copied, total=original.st_size)
                        last_report = time.monotonic()
                stage = "flush"
                progress(bytes=copied, total=original.st_size)
                outgoing.flush()
                os.fsync(outgoing.fileno())
                if copied != original.st_size or fingerprint(os.fstat(incoming.fileno())) != source_identity:
                    raise OSError(errno.ESTALE, '视频在转移期间发生变化。')

        stage = "publish"
        progress()
        if fingerprint(source.lstat()) != source_identity:
            raise OSError(errno.ESTALE, '发布源路径发生变化。')
        if offset and verified_source and checked_media_digest(temporary)[0] != expected_digest:
            raise OSError(errno.ESTALE, '转移断点的内容与已校验视频不一致。')
        if reservation:
            publish_with_reservation(temporary, destination, progress, reservation)
        else:
            try:
                _rename_exclusive(temporary, destination)
            except OSError as error:
                if error.errno not in _RENAME_UNSUPPORTED:
                    raise
                publish_with_reservation(temporary, destination, progress, reservation)
        temporary = None
        stage = "source_remove"
        details = {}
        if verified_source:
            version = media_version(destination)
            if any(version[key] != verified_source[key] for key in ('size', 'edgeHash')):
                raise OSError(errno.ESTALE, '转移后的内容与已校验视频不一致。')
            details['fileVersion'] = version
        if fingerprint(source.lstat()) != source_identity:
            raise OSError(errno.ESTALE, '发布后源路径发生变化，源文件保留。')
        progress(**details)
        source.unlink()
    except BaseException as error:
        if isinstance(error, OSError):
            error.transfer_stage = stage
        if temporary is not None and not keep_partial:
            try:
                stage = "temporary_cleanup"
                progress()
                temporary.unlink()
            except OSError:
                pass
        raise
    return True


def prepare_directory(value: str) -> str:
    value = value.strip()
    if not value:
        return ""
    directory = Path(value).expanduser()
    if not directory.is_absolute():
        raise ValueError("请填写本机文件夹的完整路径，例如 /Users/你的用户名/Movies/废钢采集。")
    try:
        directory = directory.resolve()
        if directory.is_relative_to((Path.home() / "Desktop" / "视频抽帧").resolve()):
            raise ValueError("桌面的“视频抽帧”源目录受保护，请另选最终视频保存目录。")
        directory.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryFile(dir=directory):
            pass
    except OSError:
        raise ValueError("无法写入指定文件夹，请检查路径和文件夹权限。") from None
    return str(directory)
