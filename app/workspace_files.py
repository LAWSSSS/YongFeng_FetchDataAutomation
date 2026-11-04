"""Filesystem primitives for the managed workspace. No platform operations."""
import errno
import hashlib
import json
import os
import re
import stat
from contextlib import ExitStack
from pathlib import Path

from .audit_moves import checked_path, fingerprint, source_fingerprint
from .media import edge_digest

VIDEO_BRANCHES = ('采集视频', '未切片', '车斗未切片', '磁盘吊未切片', '已切片')
OUTPUT_BRANCHES = {'bed': '车斗已切片', 'magnet': '磁盘吊已切片'}
BRANCHES = (*VIDEO_BRANCHES, *OUTPUT_BRANCHES.values(), '.处理中', '.缓存', '.处理记录')
MARKER = '.workspace.json'
FORMAT = 'inspection-video-workspace-v1'


def read_marker(root):
    try:
        path = checked_path(root) / MARKER
        if path.is_symlink() or not path.is_file():
            return None
        if path.stat().st_size > 4096:
            return None
        data = json.loads(path.read_text())
        return data if isinstance(data, dict) and data.get('format') == FORMAT and data.get('id') else None
    except (ValueError, OSError, TypeError):
        return None


def choose_root(parent, workspace_id, current=None):
    parent = checked_path(parent)
    if not parent.is_dir():
        raise ValueError('父目录不可用，请连接磁盘后重试。')
    # Selecting a managed root itself must never create another nested root.
    marker = read_marker(parent)
    if marker:
        if marker['id'] != workspace_id:
            raise ValueError('这个工作区属于另一份程序数据，请选择它的父目录。')
        return parent
    if current and Path(current).parent == parent:
        marker = read_marker(current)
        if marker and marker['id'] == workspace_id:
            return checked_path(current)
    for suffix in range(10000):
        root = parent / ('视频工作区' + (f'_{suffix + 1}' if suffix else ''))
        if not os.path.lexists(root):
            return root
        marker = read_marker(root)
        if marker and marker['id'] == workspace_id:
            return root
    raise ValueError('工作区同名目录过多，请选择其他父目录。')


def create_root(root, workspace_id):
    root = checked_path(root)
    if root.exists():
        marker = read_marker(root)
        if not marker or marker['id'] != workspace_id:
            raise ValueError('预览后目标目录被占用，请重新预览。')
    else:
        root.mkdir(exist_ok=False)
        # Exclusive marker publication. Unknown folders are never adopted.
        with (root / MARKER).open('x') as stream:
            json.dump({'format': FORMAT, 'id': workspace_id}, stream)
            stream.flush()
            os.fsync(stream.fileno())
    for branch in BRANCHES:
        path = checked_path(root / branch)
        path.mkdir(exist_ok=True)
    return root


def folder_name(filename):
    name = re.sub(r'[\x00-\x1f/\\:]', '_', Path(filename).stem).strip(' .')
    # Leave room for stable disambiguation and platform file suffixes.
    return name[:150] or '视频'


def compatible(actual, expected):
    """A receipt may omit edge hashes, but cannot disagree on saved fields."""
    return bool(expected) and all(actual.get(k) == v for k, v in expected.items()
                                  if k in {'device', 'inode', 'size', 'modifiedNs', 'edgeHash'})


def reconnected_version(actual, expected):
    """A remount may change st_dev, never inode, bytes, time or content hash."""
    keys = {'device', 'inode', 'size', 'modifiedNs', 'edgeHash'}
    return (keys.issubset(actual or {}) and keys.issubset(expected or {})
            and bool(expected['edgeHash']) and actual['device'] != expected['device']
            and all(actual[k] == expected[k] for k in keys - {'device'}))


def file_version(path, edge=False):
    if edge:
        from .slicing import source_version
        return source_version(path)
    return source_fingerprint(Path(path))


def unlink_workspace_file(root, workspace_id, path, expected, expected_ctime=None):
    """Recheck the bound file using directory handles at the deletion boundary."""
    root = checked_path(root)
    path = checked_path(path)
    if not path.is_relative_to(root) or len(path.relative_to(root).parts) < 3:
        raise ValueError('待清理文件不属于已登记工作区。')
    flags = os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK
    try:
        with ExitStack() as handles:
            def open_entry(name, parent=None, *, directory=False):
                fd = os.open(name, flags | (os.O_DIRECTORY if directory else 0), dir_fd=parent)
                handles.callback(os.close, fd)
                return fd

            root_fd = open_entry(root.anchor, directory=True)
            for part in root.parts[1:]:
                root_fd = open_entry(part, root_fd, directory=True)
            marker_fd = open_entry(MARKER, root_fd)
            info = os.fstat(marker_fd)
            try:
                data = json.loads(os.read(marker_fd, 4096)) if stat.S_ISREG(info.st_mode) and info.st_size <= 4096 else None
            except ValueError:
                data = None
            if not isinstance(data, dict) or data.get('id') != workspace_id or data.get('format') != FORMAT:
                raise ValueError('工作区身份无法确认，文件保留。')

            parent_fd = root_fd
            directories = []
            for part in path.relative_to(root).parts[:-1]:
                child_fd = open_entry(part, parent_fd, directory=True)
                directories.append((parent_fd, part, child_fd))
                parent_fd = child_fd

            file_fd = open_entry(path.name, parent_fd)
            before = os.fstat(file_fd)
            actual = fingerprint(before)
            if not stat.S_ISREG(before.st_mode) or any(expected.get(key) != value for key, value in actual.items()):
                raise ValueError('文件在清理前发生变化，剩余文件保留。')
            if 'edgeHash' in expected:
                with os.fdopen(file_fd, 'rb', closefd=False) as stream:
                    if edge_digest(stream, before.st_size) != expected['edgeHash']:
                        raise ValueError('文件内容在清理前发生变化，剩余文件保留。')
            current = os.stat(path.name, dir_fd=parent_fd, follow_symlinks=False)
            if (not stat.S_ISREG(current.st_mode) or fingerprint(current) != actual
                    or current.st_ctime_ns != before.st_ctime_ns
                    or (expected_ctime is not None and current.st_ctime_ns != expected_ctime)):
                raise ValueError('文件在清理前发生变化，剩余文件保留。')
            os.unlink(path.name, dir_fd=parent_fd)

            # Only prune empty folders below the named workspace branches.
            for directory_fd, name, child_fd in reversed(directories[1:][-2:]):
                try:
                    current = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
                    if os.path.samestat(current, os.fstat(child_fd)):
                        os.rmdir(name, dir_fd=directory_fd)
                except OSError:
                    pass
    except OSError as error:
        if error.errno in {errno.ELOOP, errno.ENOTDIR}:
            raise ValueError('清理路径已变化或经过符号链接，剩余文件保留。') from error
        raise


def birth_ns(path):
    info = Path(path).stat()
    value = getattr(info, 'st_birthtime_ns', None)
    if value is None and getattr(info, 'st_birthtime', None) is not None:
        value = round(info.st_birthtime * 1_000_000_000)
    return str(value) if value is not None else ''


def content_id(filename, version):
    payload = [filename, version.get('size'), version.get('edgeHash'),
               version.get('modifiedNs'), version.get('inode'), version.get('device')]
    return hashlib.sha256(json.dumps(payload, ensure_ascii=False).encode()).hexdigest()[:24]


def scan_managed(root, created=None):
    """Audit adapter: only original-video branches, retaining pre-move birthtime."""
    from .upload_audit import scan_directory, ns_iso
    root = checked_path(root)
    rows, ignored = [], 0
    for branch in VIDEO_BRANCHES:
        found, skipped = scan_directory(str(root / branch), True)
        ignored += skipped
        for row in found:
            row['relativePath'] = str(Path(branch) / row['relativePath'])
            original = (created or {}).get(str(root / row['relativePath']))
            if original:
                row.update(createdNs=original, createdAt=ns_iso(int(original)))
            rows.append(row)
    return rows, ignored
