"""Filesystem primitives for the managed workspace. No platform operations."""
import hashlib
import json
import os
import re
from pathlib import Path

from .audit_moves import checked_path, source_fingerprint

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
        return data if data.get('format') == FORMAT and data.get('id') else None
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
