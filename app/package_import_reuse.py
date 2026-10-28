"""Reuse an older import only for byte-identical photos of the same collection.

This module never submits, uploads, deletes or marks a package complete. It
rebinds an import for the ordinary platform verifier to check again.
"""
import copy
import hashlib
import os
from pathlib import Path, PurePosixPath
import re
import stat
import zipfile

from .audit_moves import checked_path, fingerprint, source_fingerprint
from .domain import now_iso
from .upload_audit import name_key

MAX_IMAGE_BYTES = 32 * 1024 * 1024
MAX_ARCHIVE_BYTES = 2 * 1024 ** 3


def collection_version(package):
    match = re.match(r'^collection:([a-f0-9]{24}):v(\d+):', package.get('sourceIdentity') or '')
    return (match[1], int(match[2])) if match else None


def photo_names(package):
    names = package.get('images') or []
    if (not isinstance(names, list) or not names or len(names) != package.get('imageCount') or len(names) > 2000
            or any(not isinstance(n, str) or PurePosixPath(n).name != n or '\\' in n
                   or not n.lower().endswith(('.jpg', '.jpeg')) for n in names)
            or len({name_key(n) for n in names}) != len(names)):
        raise ValueError('照片清单无效，不能复用导入成果。')
    return names


def _valid_hashes(hashes, names):
    return (isinstance(hashes, dict) and set(hashes) == set(names)
            and all(isinstance(value, str) and re.fullmatch(r'[a-f0-9]{64}', value)
                    for value in hashes.values()))


def _archive_version(value):
    return (isinstance(value, dict)
            and all(type(value.get(key)) is int and value[key] >= 0
                    for key in ('device', 'inode', 'size', 'modifiedNs')))


def reuse_image_ids(package, resolve_archive=None):
    """Return an exact image-ID receipt; a present but invalid receipt fails closed."""
    if 'reusedImport' not in package:
        return None
    receipt = package['reusedImport']
    names = photo_names(package)
    current = collection_version(package)
    if not isinstance(receipt, dict):
        raise ValueError('照片复用凭证不完整，保留文件待核对。')
    source = collection_version({'sourceIdentity': receipt.get('sourceIdentity')})
    ids = receipt.get('imageIdsByName')
    if (receipt.get('algorithm') != 'sha256' or not _valid_hashes(receipt.get('imageHashes'), names)
            or not current or not source or current[0] != source[0] or source[1] >= current[1]
            or not isinstance(receipt.get('sourcePackageId'), str) or not receipt['sourcePackageId']
            or receipt['sourcePackageId'] == package.get('id')
            or type(receipt.get('sourcePackageRevision')) is not int or receipt['sourcePackageRevision'] < 1
            or not isinstance(receipt.get('sourceJobId'), str) or not receipt['sourceJobId']
            or receipt['sourceJobId'] != str(package.get('jobId'))
            or not _archive_version(receipt.get('currentArchiveVersion'))
            or not _archive_version(receipt.get('sourceArchiveVersion'))
            or not isinstance(ids, dict) or set(ids) != {name_key(name) for name in names}
            or any(not isinstance(value, str) or not value.strip() for value in ids.values())
            or len(set(ids.values())) != len(names)):
        raise ValueError('照片复用凭证不完整，保留文件待核对。')
    path = receipt.get('currentArchivePath', package.get('path'))
    version = receipt['currentArchiveVersion']
    if resolve_archive:
        path, version = resolve_archive(path, version)
    if path != package.get('path') or version != package.get('fileVersion'):
        raise ValueError('照片复用凭证与压缩包版本不同，保留文件待核对。')
    return ids


def reuse_candidates(current, packages):
    version = collection_version(current)
    if not version or 'reusedImport' in current:
        return []
    names = set(photo_names(current))
    result = []
    for peer in packages:
        older = collection_version(peer)
        if (peer.get('id') == current['id'] or not older or older[0] != version[0] or older[1] >= version[1]
                or peer.get('dataset') != current.get('dataset') or peer.get('category') != current.get('category')
                or peer.get('state') != 'complete' or not peer.get('verifiedJobId')
                or str(peer.get('jobId')) != str(peer['verifiedJobId'])
                or peer.get('actualImages') != current.get('imageCount')
                or len(peer.get('outputImageIds') or []) != current['imageCount']
                or len(set(map(str, peer.get('outputImageIds') or []))) != current['imageCount']
                or not peer.get('keys') or not isinstance(peer.get('beforeImageIds'), list)):
            continue
        try:
            if set(photo_names(peer)) == names:
                result.append(peer)
        except (TypeError, ValueError):
            continue
    return result


def archive_hashes(package):
    names = set(photo_names(package))
    path = checked_path(package['path'])
    version = package.get('fileVersion')
    if not version or source_fingerprint(path) != version:
        raise ValueError('压缩包版本变化，不能复用旧凭证。')
    result, total = {}, 0
    with os.fdopen(os.open(path, os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0)), 'rb') as raw:
        if fingerprint(os.fstat(raw.fileno())) != version:
            raise ValueError('压缩包版本正在变化。')
        with zipfile.ZipFile(raw) as archive:
            for entry in archive.infolist():
                member = PurePosixPath(entry.filename)
                if (member.is_absolute() or '..' in member.parts or '\\' in entry.filename
                        or stat.S_ISLNK(entry.external_attr >> 16) or entry.flag_bits & 1):
                    raise ValueError('压缩包包含异常条目。')
                if entry.is_dir():
                    if entry.filename not in {'images/', 'image/'}:
                        raise ValueError('压缩包目录与清单不同。')
                    continue
                name = member.name
                total += entry.file_size
                if (name not in names or name in result
                        or entry.filename not in {name, 'images/' + name, 'image/' + name}
                        or entry.file_size > MAX_IMAGE_BYTES or total > MAX_ARCHIVE_BYTES
                        or entry.file_size > max(1, entry.compress_size) * 250):
                    raise ValueError('压缩包内容与照片清单不同。')
                digest, size = hashlib.sha256(), 0
                with archive.open(entry) as photo:
                    while block := photo.read(1024 * 1024):
                        size += len(block)
                        if size > MAX_IMAGE_BYTES:
                            raise ValueError('照片超过允许大小。')
                        digest.update(block)
                result[name] = digest.hexdigest()
        if fingerprint(os.fstat(raw.fileno())) != version:
            raise ValueError('核对期间压缩包发生变化。')
    if set(result) != names or source_fingerprint(path) != version:
        raise ValueError('压缩包不完整或版本发生变化。')
    return result


def identical_photos(current, peer):
    left, right = archive_hashes(current), archive_hashes(peer)
    if left != right:
        return None
    return {'algorithm': 'sha256', 'imageHashes': left,
            'currentArchivePath': current['path'], 'sourceArchivePath': peer['path'],
            'currentArchiveVersion': copy.deepcopy(current['fileVersion']),
            'sourceArchiveVersion': copy.deepcopy(peer['fileVersion'])}


def prepare_import_reuse(current, peer, equality):
    if peer['id'] not in {p['id'] for p in reuse_candidates(current, [peer])}:
        raise ValueError('历史导入凭证不属于同一采集的已完成旧版。')
    if (not equality or equality.get('algorithm') != 'sha256'
            or not _valid_hashes(equality.get('imageHashes'), photo_names(current))
            or equality.get('currentArchivePath', current['path']) != current['path']
            or equality.get('sourceArchivePath', peer['path']) != peer['path']
            or equality.get('currentArchiveVersion') != current.get('fileVersion')
            or equality.get('sourceArchiveVersion') != peer.get('fileVersion')):
        raise ValueError('缺少逐张内容一致的凭证。')
    result = copy.deepcopy(current)
    result['reusedImport'] = {**copy.deepcopy(equality), 'sourcePackageId': peer['id'],
        'sourcePackageRevision': peer['revision'], 'sourceIdentity': peer.get('sourceIdentity'),
        'sourceJobId': str(peer['verifiedJobId']), 'preparedAt': now_iso(),
        'imageIdsByName': dict(zip(sorted(name_key(n) for n in peer['images']), map(str, peer['outputImageIds']))),
        'originalAttempt': {k:copy.deepcopy(current.get(k)) for k in ('jobId', 'keys', 'beforeJobs', 'beforeImageIds')}}
    # Keep the new upload object and all local/video identities unchanged. The
    # existing job and its original pre-import snapshot are the reused receipt.
    result.update(jobId=str(peer['verifiedJobId']), keys=copy.deepcopy(peer['keys']),
                  beforeJobs=copy.deepcopy(peer.get('beforeJobs') or []),
                  beforeImageIds=copy.deepcopy(peer['beforeImageIds']), state='importing',
                  actualImages=0, errorCode='', message='照片逐张相同，正在重新核验已有导入成果。')
    reuse_image_ids(result)
    return result
