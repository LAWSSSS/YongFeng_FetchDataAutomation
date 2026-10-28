"""Refresh photo metadata only when exact bytes match the verified bound ZIP."""
import hashlib
import os
from pathlib import Path
import zipfile

from .audit_moves import fingerprint, source_fingerprint


def refresh_photo_versions(package):
    mismatches = []
    for name in package.get('images', []):
        if not isinstance(name, str) or Path(name).name != name or '\\' in name:
            raise ValueError('照片清单无效。')
        path = Path(package.get('imagePaths', {}).get(name, str(Path(package['imageDir']) / name)))
        before = source_fingerprint(path)
        if before != package.get('imageVersions', {}).get(name):
            mismatches.append((name, path, before))
    if not mismatches:
        return {}
    archive = Path(package['path'])
    if source_fingerprint(archive) != package.get('fileVersion'):
        raise ValueError('压缩包版本已变化，保留原文件。')
    changes = {}
    with os.fdopen(os.open(archive, os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0)), 'rb') as raw:
        if fingerprint(os.fstat(raw.fileno())) != package['fileVersion']:
            raise ValueError('压缩包版本已变化，保留原文件。')
        with zipfile.ZipFile(raw) as opened:
            for name, path, before in mismatches:
                members = [i for i in opened.infolist() if i.filename in {name, 'images/' + name, 'image/' + name}]
                if (len(members) != 1 or before['size'] > 32 * 1024 * 1024 or members[0].flag_bits & 1
                        or members[0].file_size != before['size']):
                    raise ValueError('照片与已核验压缩包不符，保留原文件。')
                with os.fdopen(os.open(path, os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0)), 'rb') as photo:
                    if fingerprint(os.fstat(photo.fileno())) != before:
                        raise ValueError('照片版本正在变化，请稍后重试。')
                    digest = hashlib.sha256(photo.read(32 * 1024 * 1024 + 1)).digest()
                if digest != hashlib.sha256(opened.read(members[0])).digest() or source_fingerprint(path) != before:
                    raise ValueError('照片内容与已核验压缩包不同，保留原文件。')
                changes[name] = before
    if source_fingerprint(archive) != package['fileVersion']:
        raise ValueError('压缩包版本正在变化，请稍后重试。')
    return changes
