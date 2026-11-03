"""Resume a fixed deletion scope after a proven same-volume remount.

Original consent and per-file receipts stay immutable. A separate, content-
checked mapping bridges only st_dev; a missing ZIP never licenses its photos.
"""
import copy
import hashlib
import json
from pathlib import Path

from .audit_moves import checked_path, fingerprint
from .domain import now_iso
from .workspace_archive import refresh_photo_versions
from .workspace_files import file_version, read_marker, reconnected_version


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def scope_digest(saved):
    return digest([{key: entry.get(key) for key in ('path', 'expected', 'kind', 'packageId')}
                   for entry in saved.get('manifest', [])])


def same_except_device(actual, expected):
    fields = {'device', 'inode', 'size', 'modifiedNs'}
    return (fields.issubset(actual or {}) and fields.issubset(expected or {})
            and actual['device'] != expected['device']
            and all(actual.get(key) == value for key, value in expected.items() if key != 'device'))


def candidate(asset, saved, request_id):
    """Pure scheduling hint. This is not deletion permission."""
    journal = asset.get('cleanup') or {}
    if (saved.get('state') != 'attention' or saved.get('reasonCode') not in {'receipt_changed', 'file_changed'}
            or journal.get('requestId') != request_id or asset.get('path') != saved.get('path')):
        return False
    if reconnected_version(asset.get('fileVersion'), saved.get('fileVersion')):
        return True
    # The original may already be gone with an exact deletion receipt. Its
    # remaining photos still need their own content proof below.
    return (asset.get('fileVersion') == saved.get('fileVersion') and any(
        entry.get('path') == saved.get('path') and entry.get('state') in {'deleting', 'deleted'}
        and entry.get('expected') == saved.get('fileVersion')
        for entry in journal.get('manifest', [])))


def mapping(saved, journal):
    proof = journal.get('remountProof') or {}
    if (proof.get('version') != 2 or proof.get('sourceSignature') != saved.get('receiptSignature')
            or proof.get('scopeSignature') != scope_digest(saved)
            or not isinstance(proof.get('files'), dict)
            or any(type(row.get('ctimeNs')) is not int or row['ctimeNs'] <= 0
                   for row in proof['files'].values())):
        return None
    return proof


def effective_snapshot(saved, journal):
    proof = mapping(saved, journal)
    if not proof:
        return saved
    return {**saved, 'fileVersion': proof['fileVersion'], 'receiptSignature': proof['receiptSignature']}


def expected_version(entry, saved, journal):
    proof = mapping(saved, journal)
    receipt = (proof or {}).get('files', {}).get(entry['path']) or {}
    return (receipt['toVersion'] if receipt.get('fromVersion') == entry['expected']
            else entry['expected'])


def verify_current(path, entry, saved, journal):
    """A persisted content check cannot survive an in-place file rewrite."""
    proof = mapping(saved, journal)
    if not proof:
        return
    receipt = proof['files'].get(entry['path']) or {}
    if receipt.get('fromVersion') != entry['expected']:
        raise ValueError('缺少该文件的重挂载内容凭证，保留文件。')
    current = Path(path).stat(follow_symlinks=False)
    if (fingerprint(current) != {k: receipt['toVersion'].get(k)
                                for k in ('device', 'inode', 'size', 'modifiedNs')}
            or current.st_ctime_ns != receipt['ctimeNs']):
        raise ValueError('文件在内容核验后发生变化，剩余文件保留。')


def _old_receipt(payload, expected):
    result = copy.deepcopy(payload)

    def restore(version):
        if same_except_device(version or {}, {k: expected[k] for k in version or {} if k in expected}):
            version['device'] = expected['device']

    restore(result.get('version'))
    for item in result.get('items', []):
        restore(item.get('fileVersion'))
    for package in result.get('packages', []):
        restore(package.get('videoFileVersion'))
    return result


def receipt_matches(payload, saved):
    original = _old_receipt(payload, saved['fileVersion'])
    if digest(original) == saved['receiptSignature']:
        return True
    # Verified location receipts can add the original's edge hash to an old
    # metadata-only binding. Recognize that additive proof only when the exact
    # old consent digest matches; never omit an unrelated or changed hash.
    for groups in (('packages',), ('items',), ('items', 'packages')):
        candidate_payload = copy.deepcopy(original)
        for group in groups:
            key = 'videoFileVersion' if group == 'packages' else 'fileVersion'
            for row in candidate_payload.get(group, []):
                version = row.get(key) or {}
                expected = saved['fileVersion']
                if (expected.get('edgeHash') and version.get('edgeHash') == expected['edgeHash']
                        and all(version.get(k) == expected.get(k) for k in ('device', 'inode', 'size', 'modifiedNs'))):
                    row[key] = {k: v for k, v in version.items() if k != 'edgeHash'}
        if digest(candidate_payload) == saved['receiptSignature']:
            return True
    return False


def validate(root, workspace_id, asset, saved, journal, packages, payload):
    """Blocking, read-only filesystem work; caller holds all file read leases."""
    root = checked_path(root)
    if (read_marker(root) or {}).get('id') != workspace_id:
        raise ValueError('工作区身份无法确认，原清理范围保留。')
    if asset['path'] != saved['path'] or not receipt_matches(payload, saved):
        raise ValueError('照片成果或原片身份已变化，原清理范围保留。')
    entries = saved['manifest']
    if len({entry['path'] for entry in entries}) != len(entries):
        raise ValueError('原清理清单包含重复路径，保留文件。')
    receipts = {entry['path']: entry for entry in journal.get('manifest', [])}
    if set(receipts) != {entry['path'] for entry in entries} or any(
            receipts[entry['path']].get('expected') != entry['expected'] for entry in entries):
        raise ValueError('原清理日志与固定范围不符，保留文件。')
    files, pending = {}, []
    root_device = root.stat().st_dev
    for entry in entries:
        path = checked_path(entry['path'])
        if not path.is_relative_to(root) or len(path.relative_to(root).parts) < 3:
            raise ValueError('文件不属于原工作区，原位保留。')
        receipt = receipts[entry['path']]
        if receipt.get('state') == 'deleted':
            continue
        try:
            actual = file_version(path, 'edgeHash' in entry['expected'])
        except FileNotFoundError:
            if receipt.get('state') == 'deleting':
                continue
            raise ValueError('缺少文件或精确删除凭证，剩余文件保留。') from None
        expected = entry['expected']
        if actual != expected and not same_except_device(actual, expected):
            raise ValueError('文件内容或版本已变化，剩余文件保留。')
        if actual['device'] != root_device:
            raise ValueError('清理文件不在原工作区磁盘，保留文件。')
        if entry['kind'] == 'video' and actual != expected and not reconnected_version(actual, expected):
            raise ValueError('原视频缺少重挂载内容凭证，保留文件。')
        current_info = path.stat(follow_symlinks=False)
        if fingerprint(current_info) != {k: actual.get(k) for k in ('device', 'inode', 'size', 'modifiedNs')}:
            raise ValueError('文件在内容核验期间发生变化，保留文件。')
        files[entry['path']] = {'fromVersion': copy.deepcopy(expected), 'toVersion': actual,
                               'ctimeNs': current_info.st_ctime_ns}
        pending.append(entry)
    changed = any(row['fromVersion'] != row['toVersion'] for row in files.values())
    if not changed:
        raise ValueError('未确认原磁盘重新挂载，保留原清理异常。')
    by_package = {package['id']: package for package in packages}
    for identity in {entry.get('packageId') for entry in pending if entry['kind'] in {'archive', 'image'}}:
        package = copy.deepcopy(by_package.get(identity))
        if not package:
            raise ValueError('缺少原照片包凭证，保留文件。')
        members = [entry for entry in pending if entry.get('packageId') == identity]
        archive = next((entry for entry in entries if entry.get('packageId') == identity and entry['kind'] == 'archive'), None)
        if not archive or archive['path'] not in files:
            raise ValueError('原压缩包已清理，剩余照片缺少内容校验依据，保留待核对。')
        if package.get('path') != archive['path']:
            raise ValueError('照片包路径已改变，保留原清理范围。')
        selected, image_paths, versions = [], {}, {}
        names = {package.get('imagePaths', {}).get(name, str(Path(package['imageDir']) / name)): name
                 for name in package.get('images', [])}
        for entry in members:
            if entry['kind'] != 'image':
                continue
            name = names.get(entry['path'])
            if not name:
                raise ValueError('照片不属于原包清单，保留文件。')
            selected.append(name)
            image_paths[name] = entry['path']
            versions[name] = entry['expected']
        package.update(path=archive['path'], fileVersion=archive['expected'],
                       images=selected, imagePaths=image_paths, imageVersions=versions)
        # Checks complete multipart-upload hashes, then each retained photo
        # against the exact ZIP member. Does not mutate package records.
        refresh_photo_versions(package, allow_remount=True)
    if (read_marker(root) or {}).get('id') != workspace_id:
        raise ValueError('校验期间工作区已变化，保留文件。')
    proof = {'version': 2, 'sourceSignature': saved['receiptSignature'],
            'scopeSignature': scope_digest(saved), 'receiptSignature': digest(payload),
            'fileVersion': copy.deepcopy(asset['fileVersion']), 'workspaceId': workspace_id,
            'files': files, 'validatedAt': now_iso()}
    # A photo can be rewritten while another ZIP is being checked. Fence the
    # entire proof, including preserved mtimes, before persisting it.
    for entry in pending:
        verify_current(entry['path'], entry, saved, {'remountProof': proof})
    return proof
