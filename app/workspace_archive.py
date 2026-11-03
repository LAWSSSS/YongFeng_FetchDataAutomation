"""Refresh photo metadata only when exact bytes match the verified bound ZIP."""
import hashlib
import os
import re
from pathlib import Path
import zipfile

from .audit_moves import fingerprint, source_fingerprint


def _verify_remounted_archive(package, archive, current):
    """Rebind only the same uploaded bytes after an external disk remount."""
    expected = package.get('fileVersion') or {}
    if (not all(key in expected and key in current for key in ('device', 'inode', 'size', 'modifiedNs'))
            or expected['device'] == current['device']
            or any(expected[key] != current[key] for key in ('inode', 'size', 'modifiedNs'))):
        raise ValueError('压缩包版本已变化，保留原文件。')
    parts = package.get('parts')
    if not isinstance(parts, dict) or not parts:
        raise ValueError('压缩包缺少上传校验凭证，保留原文件。')
    try:
        ordered = sorted(parts.values(), key=lambda part: part['number'])
        if ([part['number'] for part in ordered] != list(range(1, len(ordered) + 1))
                or any(type(part['size']) is not int or part['size'] <= 0 for part in ordered)
                or sum(part['size'] for part in ordered) != current['size']):
            raise ValueError()
        part_hashes = [str(part['etag']).strip('"').lower() for part in ordered]
        if any(not re.fullmatch(r'[0-9a-f]{32}', value) for value in part_hashes):
            raise ValueError()
        composite = hashlib.md5(b''.join(bytes.fromhex(value) for value in part_hashes),
                                usedforsecurity=False).hexdigest() + '-' + str(len(ordered))
        if str(package.get('etag') or '').strip('"').lower() != composite:
            raise ValueError()
    except (KeyError, TypeError, ValueError):
        raise ValueError('压缩包上传校验凭证不完整，保留原文件。') from None
    with os.fdopen(os.open(archive, os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0)), 'rb') as raw:
        if fingerprint(os.fstat(raw.fileno())) != current:
            raise ValueError('压缩包正在变化，保留原文件。')
        for part, expected_hash in zip(ordered, part_hashes):
            remaining = part['size']
            digest = hashlib.md5(usedforsecurity=False)
            while remaining:
                chunk = raw.read(min(remaining, 1024 * 1024))
                if not chunk:
                    raise ValueError('压缩包长度不符，保留原文件。')
                digest.update(chunk)
                remaining -= len(chunk)
            if digest.hexdigest() != expected_hash:
                raise ValueError('压缩包内容与上传文件不符，保留原文件。')
        if raw.read(1) or fingerprint(os.fstat(raw.fileno())) != current:
            raise ValueError('压缩包正在变化，保留原文件。')
    if source_fingerprint(archive) != current:
        raise ValueError('压缩包正在变化，保留原文件。')


def refresh_photo_versions(package, allow_remount=False):
    mismatches = []
    for name in package.get('images', []):
        if not isinstance(name, str) or Path(name).name != name or '\\' in name:
            raise ValueError('照片清单无效。')
        path = Path(package.get('imagePaths', {}).get(name, str(Path(package['imageDir']) / name)))
        before = source_fingerprint(path)
        if before != package.get('imageVersions', {}).get(name):
            mismatches.append((name, path, before))
    archive = Path(package['path'])
    archive_version = source_fingerprint(archive)
    if archive_version != package.get('fileVersion'):
        if not allow_remount:
            raise ValueError('压缩包版本已变化，保留原文件。')
        _verify_remounted_archive(package, archive, archive_version)
    if not mismatches:
        package['fileVersion'] = archive_version
        return {}
    changes = {}
    with os.fdopen(os.open(archive, os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0)), 'rb') as raw:
        if fingerprint(os.fstat(raw.fileno())) != archive_version:
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
    if source_fingerprint(archive) != archive_version:
        raise ValueError('压缩包版本正在变化，请稍后重试。')
    package['fileVersion'] = archive_version
    return changes
