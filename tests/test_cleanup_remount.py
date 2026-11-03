import copy
import hashlib
import os
from pathlib import Path
import zipfile

import pytest

from app.cleanup_remount import (candidate, digest, validate, receipt_matches,
                                mapping, expected_version, verify_current)
from app.workspace_cleanup import receipt_payload
from app.workspace_files import create_root, file_version
from tests.test_manual_completed_cleanup import prepared
from tests.test_slicing_performance import close_app


def uploaded_zip(path, photos):
    with zipfile.ZipFile(path, 'w') as archive:
        for name, content in photos.items():
            archive.writestr('images/' + name, content)
    content = path.read_bytes()
    part = hashlib.md5(content, usedforsecurity=False).hexdigest()
    return {'parts': {'1': {'number': 1, 'size': len(content), 'etag': part}},
            'etag': hashlib.md5(bytes.fromhex(part), usedforsecurity=False).hexdigest() + '-1'}


def versions_old(value):
    if isinstance(value, dict):
        result = {key: versions_old(item) for key, item in value.items()}
        if {'device', 'inode', 'size', 'modifiedNs'}.issubset(result):
            result['device'] += 99
        return result
    if isinstance(value, list):
        return [versions_old(item) for item in value]
    return value


def remount_case(tmp_path):
    root = tmp_path / 'workspace'
    create_root(root, 'owned')
    original = root / '已切片' / 'truck' / 'truck.mp4'
    original.parent.mkdir()
    original.write_bytes(b'original-video')
    folder = root / '车斗已切片' / 'truck'
    folder.mkdir()
    photo = folder / 'one.jpg'
    photo.write_bytes(b'actual-photo-content')
    archive = folder / 'images.zip'
    upload = uploaded_zip(archive, {photo.name: photo.read_bytes()})
    version = file_version(original, True)
    package = {'id': 'package', 'itemId': 'slice', 'videoFileVersion': version,
               'path': str(archive), 'fileVersion': file_version(archive),
               'images': ['one.jpg'], 'imageDir': str(folder),
               'imageVersions': {'one.jpg': file_version(photo)}, **upload}
    payload = {'version': version, 'items': [{'id': 'slice', 'fileVersion': version,
               'categories': {'bed': {'state': 'complete', 'packageId': 'package'}}}],
               'packages': [{'id': 'package', 'jobId': 'import', 'videoFileVersion': version,
                             'actualImages': 1, 'outputImageIds': ['image']} ]}
    entries = [{'path': str(original), 'expected': version, 'kind': 'video', 'state': 'planned'},
               {'path': str(archive), 'expected': file_version(archive), 'kind': 'archive',
                'packageId': 'package', 'state': 'planned'},
               {'path': str(photo), 'expected': file_version(photo), 'kind': 'image',
                'packageId': 'package', 'state': 'planned'}]
    saved = {'id': 'asset', 'path': str(original), 'fileVersion': versions_old(version),
             'manifest': versions_old(entries), 'receiptSignature': digest(versions_old(payload)),
             'state': 'attention', 'reasonCode': 'receipt_changed'}
    journal = {'requestId': 'request', 'manifest': copy.deepcopy(saved['manifest'])}
    asset = {'id': 'asset', 'path': str(original), 'fileVersion': version, 'cleanup': journal}
    return root, asset, saved, journal, [package], payload


def test_remount_verifies_uploaded_zip_and_photos_without_rewriting_scope(tmp_path):
    args = remount_case(tmp_path)
    before = copy.deepcopy(args)
    proof = validate(args[0], 'owned', *args[1:])
    assert len(proof['files']) == 3
    assert proof['sourceSignature'] == args[2]['receiptSignature']
    assert candidate(args[1], args[2], 'request')
    assert args == before
    assert all(Path(entry['path']).exists() for entry in args[2]['manifest'])


def test_remount_accepts_only_proven_additive_edge_hash_in_legacy_binding(tmp_path):
    root, asset, saved, journal, packages, payload = remount_case(tmp_path)
    before = versions_old(payload)
    before['packages'][0]['videoFileVersion'].pop('edgeHash')
    saved['receiptSignature'] = digest(before)
    assert receipt_matches(payload, saved)
    assert validate(root, 'owned', asset, saved, journal, packages, payload)['files']
    payload['packages'][0]['videoFileVersion'] = {**payload['packages'][0]['videoFileVersion'], 'edgeHash': 'wrong'}
    assert not receipt_matches(payload, saved)


@pytest.mark.parametrize('fault', ['zip_bytes', 'photo_bytes', 'receipt', 'marker', 'inode'])
def test_remount_never_ignores_real_content_or_identity_change(tmp_path, fault):
    root, asset, saved, journal, packages, payload = remount_case(tmp_path)
    if fault in {'zip_bytes', 'photo_bytes'}:
        path = Path(packages[0]['path'] if fault == 'zip_bytes' else packages[0]['imageDir'] + '/one.jpg')
        old = path.stat()
        data = path.read_bytes()
        path.write_bytes(bytes([data[0] ^ 1]) + data[1:])
        os.utime(path, ns=(old.st_atime_ns, old.st_mtime_ns))
    elif fault == 'receipt':
        payload['packages'][0]['jobId'] = 'another-import'
    elif fault == 'marker':
        (root / '.workspace.json').write_text('{"format":"inspection-video-workspace-v1","id":"foreign"}')
    else:
        saved['manifest'][-1]['expected']['inode'] += 1
        journal['manifest'] = copy.deepcopy(saved['manifest'])
    with pytest.raises(ValueError):
        validate(root, 'owned', asset, saved, journal, packages, payload)
    assert Path(asset['path']).exists()


def test_deleted_original_is_accepted_only_with_exact_prior_deletion_receipt(tmp_path):
    root, asset, saved, journal, packages, payload = remount_case(tmp_path)
    Path(asset['path']).unlink()
    journal['manifest'][0]['state'] = 'deleted'
    proof = validate(root, 'owned', asset, saved, journal, packages, payload)
    assert len(proof['files']) == 2
    journal['manifest'][0]['state'] = 'planned'
    with pytest.raises(ValueError):
        validate(root, 'owned', asset, saved, journal, packages, payload)


def test_missing_deleted_zip_cannot_authorize_its_remaining_photos(tmp_path):
    root, asset, saved, journal, packages, payload = remount_case(tmp_path)
    Path(packages[0]['path']).unlink()
    journal['manifest'][1]['state'] = 'deleted'
    with pytest.raises(ValueError, match='原压缩包已清理'):
        validate(root, 'owned', asset, saved, journal, packages, payload)
    assert Path(packages[0]['imageDir'], 'one.jpg').exists()


def rewrite_preserving_time(path):
    info = path.stat()
    data = path.read_bytes()
    with path.open('r+b') as stream:
        stream.write(bytes([data[0] ^ 1]) + data[1:])
    os.utime(path, ns=(info.st_atime_ns, info.st_mtime_ns))


@pytest.mark.parametrize('kind', ['image', 'archive'])
def test_persisted_remount_proof_rejects_in_place_rewrite_with_same_mtime(tmp_path, kind):
    root, asset, saved, journal, packages, payload = remount_case(tmp_path)
    proof = validate(root, 'owned', asset, saved, journal, packages, payload)
    # A restart reloads only the JSON proof; it cannot treat this as a fresh
    # content check when another application changed bytes under the same name.
    import json
    journal['remountProof'] = json.loads(json.dumps(proof))
    entry = next(row for row in saved['manifest'] if row['kind'] == kind)
    path = Path(entry['path'])
    verify_current(path, entry, saved, journal)
    before = file_version(path)
    rewrite_preserving_time(path)
    assert file_version(path) == before
    with pytest.raises(ValueError, match='内容核验后发生变化'):
        verify_current(path, entry, saved, journal)
    assert path.exists()


def test_legacy_or_incomplete_remount_proof_requires_new_content_check(tmp_path):
    root, asset, saved, journal, packages, payload = remount_case(tmp_path)
    proof = validate(root, 'owned', asset, saved, journal, packages, payload)
    for broken in [dict(proof, version=1), dict(proof, files={path: {k: v for k, v in entry.items() if k != 'ctimeNs'}
                   for path, entry in proof['files'].items()})]:
        journal['remountProof'] = broken
        assert mapping(saved, journal) is None
        assert expected_version(saved['manifest'][-1], saved, journal) == saved['manifest'][-1]['expected']


def test_remount_rejects_rewrite_while_other_content_is_being_checked(tmp_path, monkeypatch):
    root, asset, saved, journal, packages, payload = remount_case(tmp_path)
    import app.cleanup_remount as remount
    original = remount.refresh_photo_versions
    def changed_after_hash(package, allow_remount=False):
        result = original(package, allow_remount)
        rewrite_preserving_time(Path(package['imageDir'], 'one.jpg'))
        return result
    monkeypatch.setattr(remount, 'refresh_photo_versions', changed_after_hash)
    with pytest.raises(ValueError, match='内容核验后发生变化'):
        validate(root, 'owned', asset, saved, journal, packages, payload)
    assert Path(packages[0]['imageDir'], 'one.jpg').exists()


@pytest.mark.asyncio
async def test_existing_attention_request_resumes_same_scope_then_restarts_safely(tmp_path, monkeypatch):
    app, w, asset = await prepared(tmp_path)
    try:
        for p in w.packages(asset):
            p.update(uploaded_zip(Path(p['path']), {name: Path(p['imagePaths'][name]).read_bytes()
                     for name in p['images']}))
            p['fileVersion'] = file_version(p['path'])
            w.slicing.packages.store.save('items', p)
        current_asset = w.get('assets', asset['id'])
        old_asset = versions_old(current_asset)
        w.save('assets', old_asset)
        for row in w.videos.store.list(include_deleted=True):
            w.videos.store.save(versions_old(row))
        for item in w.slicing.store.list('items'):
            w.slicing.store.save('items', versions_old(item))
        for p in w.packages(old_asset):
            w.slicing.packages.store.save('items', versions_old(p))
        operation = await w.request_cleanup_completed(w.config['revision'], 'fixed-remount')
        assert operation['total'] == 1
        request = w.get('requests', 'fixed-remount')
        saved = request['assets'][0]
        original_scope = copy.deepcopy(saved)
        request['state'] = saved['state'] = 'attention'
        saved['reasonCode'] = 'receipt_changed'
        w.save('requests', request)
        # Same precise binding update as a verified workspace remount.
        for item in w.slicing.store.list('items'):
            item['fileVersion']['device'] -= 99
            w.slicing.store.save('items', item)
        for row in w.videos.store.list(include_deleted=True):
            row['versions'][0]['fileVersion']['device'] -= 99
            w.videos.store.save(row)
        for p in w.slicing.packages.store.list('items'):
            p['videoFileVersion']['device'] -= 99
            w.slicing.packages.store.save('items', p)
        current_asset['cleanup'] = {'requestId': request['id'], 'state': 'attention',
                                   'reasonCode': 'receipt_changed', 'manifest': copy.deepcopy(saved['manifest'])}
        w.save('assets', current_asset)
        original_io = w.videos.file_operation
        interrupted = False
        async def stop_after_unlink(function, *args):
            nonlocal interrupted
            result = await original_io(function, *args)
            if getattr(function, '__name__', '') == 'unlink' and not interrupted:
                interrupted = True
                import asyncio
                raise asyncio.CancelledError()
            return result
        monkeypatch.setattr(w.videos, 'file_operation', stop_after_unlink)
        import asyncio
        with pytest.raises(asyncio.CancelledError):
            await w.cleanup(w.get('assets', asset['id']))
        assert interrupted
        persisted = w.get('requests', request['id'])['assets'][0]
        for key in ('manifest', 'fileVersion', 'receiptSignature', 'path'):
            assert persisted[key] == original_scope[key]
    finally:
        await close_app(app)
    from app.main import create_app
    restarted = create_app(data_dir=tmp_path/'db')
    try:
        w = restarted.state.workspace
        await w.cleanup(w.get('assets', asset['id']))
        assert w.cleanup_operation('fixed-remount')['state'] == 'complete'
        assert not Path(asset['path']).exists()
        assert len(w.get('requests', 'fixed-remount')['assets']) == 1
    finally:
        await close_app(restarted)
