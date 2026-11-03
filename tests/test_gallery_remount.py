"""A reconnected volume must not make a verified photo ZIP disappear."""
import hashlib
import os
from pathlib import Path

import pytest

from tests.test_media_control import app, package


def remounted_package(app, tmp_path, *, receipt=True):
    item, image = package(app, tmp_path, zip_only=True)
    raw = Path(item['path']).read_bytes()
    digest = hashlib.md5(raw, usedforsecurity=False).hexdigest()
    item['fileVersion']['device'] += 100
    if receipt:
        item['parts'] = {'1': {'number': 1, 'size': len(raw), 'etag': digest}}
        item['etag'] = hashlib.md5(bytes.fromhex(digest), usedforsecurity=False).hexdigest() + '-1'
    app.state.slicing.packages.store.save('items', item)
    return item, image


@pytest.mark.asyncio
async def test_remount_gallery_verifies_uploaded_zip_without_changing_receipts(app, tmp_path, monkeypatch):
    item, image = remounted_package(app, tmp_path)
    gallery = app.state.package_gallery
    import app.workspace_archive as archive
    calls = []
    original = archive._verify_remounted_archive

    def checked(*args):
        calls.append(True)
        return original(*args)

    monkeypatch.setattr(archive, '_verify_remounted_archive', checked)
    before = app.state.slicing.packages.store.get('items', item['id'])
    view = await gallery.view(item['id'])
    assert view['available'] and view['count'] == 1 and view['canDownload']
    assert gallery.read(item['id'], 'same_001.jpg', view['sessionId']) == image
    path, guard = await gallery.download(item['id'], view['sessionId'])
    assert path == Path(item['path'])
    guard.close()
    gallery.close(view['sessionId'])
    repeated = await gallery.view(item['id'])
    gallery.close(repeated['sessionId'])
    assert len(calls) == 1
    assert app.state.slicing.packages.store.get('items', item['id']) == before


@pytest.mark.asyncio
async def test_remount_without_content_receipt_cannot_accept_stat_match(app, tmp_path):
    item, _ = remounted_package(app, tmp_path, receipt=False)
    assert not (await app.state.package_gallery.view(item['id']))['available']


@pytest.mark.asyncio
async def test_remount_rejects_changed_bytes_even_when_size_and_time_match(app, tmp_path):
    item, _ = remounted_package(app, tmp_path)
    path = Path(item['path'])
    stat = path.stat()
    with path.open('r+b') as stream:
        stream.seek(64)
        old = stream.read(1)
        stream.seek(64)
        stream.write(bytes([old[0] ^ 1]))
    os.utime(path, ns=(stat.st_atime_ns, stat.st_mtime_ns))
    with pytest.raises(ValueError, match='内容与上传文件不符'):
        await app.state.package_gallery.view(item['id'])
    assert not app.state.package_gallery.leases.busy(path)


@pytest.mark.asyncio
async def test_remount_receipt_change_does_not_reuse_cached_approval(app, tmp_path):
    item, _ = remounted_package(app, tmp_path)
    gallery = app.state.package_gallery
    view = await gallery.view(item['id'])
    gallery.close(view['sessionId'])
    item['etag'] = '0' * 32 + '-1'
    app.state.slicing.packages.store.save('items', item)
    with pytest.raises(ValueError, match='校验凭证不完整'):
        await gallery.view(item['id'])


@pytest.mark.asyncio
async def test_cached_remount_detects_equal_length_rewrite_with_restored_mtime(app, tmp_path):
    item, _ = remounted_package(app, tmp_path)
    gallery = app.state.package_gallery
    view = await gallery.view(item['id'])
    gallery.close(view['sessionId'])
    path = Path(item['path'])
    stat = path.stat()
    with path.open('r+b') as stream:
        stream.seek(64)
        old = stream.read(1)
        stream.seek(64)
        stream.write(bytes([old[0] ^ 1]))
    os.utime(path, ns=(stat.st_atime_ns, stat.st_mtime_ns))
    with pytest.raises(ValueError, match='内容与上传文件不符'):
        await gallery.view(item['id'])
