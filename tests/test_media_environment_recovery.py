"""Offline media remains resumable; remote receipts and the UI stay independent."""
import asyncio
import errno
import sqlite3
import threading
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

from app.file_leases import FileEnvironmentError
from app.main import create_app
from app.slice_packages import SlicePackageManager
from app.upload_audit import AuditError
from tests.test_manual_completed_cleanup import prepared
from tests.test_slicing_performance import close_app
from tests.test_workspace import pair


class Remote:
    def __init__(self):
        self.queries = 0
        self.signatures = 0

    async def jobs(self, dataset):
        self.queries += 1
        return [{'id': 'bound-job', 'type': 1, 'status': '3', 'dataset': dataset,
                 'config': {'source': {'files': [{'key': 'bound-object'}]}}}]

    async def images(self, dataset, *, prefix):
        return [{'id': 'image-1', 'name': 'probe_001.jpg', 'taskId': 'bound-job'}]

    async def signature(self, *args):
        self.signatures += 1
        raise AuditError('offline', 'network')


def package(identity='probe'):
    return {'id': identity, 'itemId': 'source-' + identity, 'dataset': '27892', 'category': 'bed',
            'state': 'queued', 'path': '/Volumes/offline-probe/photos.zip', 'name': 'photos.zip', 'size': 5,
            'fileVersion': {'device': 1, 'inode': 1, 'size': 5, 'modifiedNs': 1}}


@pytest.fixture
def packages():
    db = sqlite3.connect(':memory:')
    manager = SlicePackageManager(db, Remote(), clock=lambda: 1000)
    yield manager
    db.close()


@pytest.mark.asyncio
async def test_disconnected_disk_does_not_block_import_receipts_or_watchdog(packages):
    item = package('import')
    item.update(state='importing', jobId='bound-job', keys=['bound-object'], images=['probe_001.jpg'], imageCount=1)
    packages.store.save('items', item)
    packages.store.save('items', package())
    packages.workspace = SimpleNamespace(blocked=True, maintenance=False)
    worker = asyncio.create_task(asyncio.Event().wait())
    packages.running['stalled'] = worker
    runtime = {'lastProgress': 0}
    packages.runtime['stalled'] = runtime
    try:
        await packages.tick()
        await asyncio.gather(*packages.target_tasks.values())
        await asyncio.gather(worker, return_exceptions=True)
        assert worker.cancelled()
        assert runtime['stopCode'] == 'disk'
        imported = packages.store.get('items', 'import')
        assert imported['state'] == 'complete' and imported['verifiedJobId'] == 'bound-job'
        assert imported['outputImageIds'] == ['image-1']
        assert packages.remote.queries and packages.remote.signatures == 0
        assert packages.store.get('items', 'probe')['state'] == 'queued'
    finally:
        worker.cancel()
        await asyncio.gather(worker, return_exceptions=True)


@pytest.mark.asyncio
async def test_unmounted_path_never_uses_upload_retry_budget(packages, monkeypatch):
    packages.store.save('items', package())
    monkeypatch.setattr('app.audit_moves.os.path.ismount', lambda path: False)
    for _ in range(4):
        await packages.upload_one('probe')
    current = packages.store.get('items', 'probe')
    assert current['state'] == 'queued' and current.get('retries', 0) == 0
    assert current['reasonCode'] == 'disk_offline'
    assert packages.remote.signatures == 0


@pytest.mark.asyncio
async def test_lease_conflict_waits_then_release_wakes_exact_package(packages):
    packages.store.save('items', package())
    with packages.leases.write(package()['path']):
        for _ in range(4):
            await packages.upload_one('probe')
        current = packages.store.get('items', 'probe')
        assert current['state'] == 'queued' and current.get('retries', 0) == 0
        assert current['reasonCode'] == 'file_busy' and current['nextAttempt'] > 1000
    await asyncio.sleep(0)
    current = packages.store.get('items', 'probe')
    assert current['nextAttempt'] == 0 and not current['reasonCode']


@pytest.mark.asyncio
async def test_slow_upload_stat_is_bounded_deduplicated_and_keeps_its_lease(packages, monkeypatch):
    packages.store.save('items', package())
    packages.file_checks.timeout = .02
    release = threading.Event()
    entered = threading.Event()
    calls = 0

    def blocked_stat(path):
        nonlocal calls
        calls += 1
        entered.set()
        release.wait(2)
        return package()['fileVersion']

    monkeypatch.setattr('app.slice_packages.source_fingerprint', blocked_stat)
    ticks = 0

    async def responsive():
        nonlocal ticks
        while not release.is_set():
            ticks += 1
            await asyncio.sleep(.005)

    heartbeat = asyncio.create_task(responsive())
    try:
        for _ in range(4):
            await asyncio.wait_for(packages.upload_one('probe'), .2)
        assert entered.is_set() and calls == 1 and ticks >= 8
        assert len(packages.file_checks.pending) == 1
        assert packages.leases.busy(package()['path'])
        current = packages.store.get('items', 'probe')
        assert current['state'] == 'queued' and current.get('retries', 0) == 0
        assert current['reasonCode'] == 'disk_io'
    finally:
        release.set()
        await heartbeat
        await asyncio.gather(*packages.file_checks.pending.values())
    assert not packages.leases.busy(package()['path'])


@pytest.mark.asyncio
async def test_mid_organization_disconnect_recovers_without_manual_retry(tmp_path, monkeypatch):
    app, w, asset = await prepared(tmp_path)
    original = w.refresh_archive_photos
    calls = 0

    async def interrupted(current):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise FileEnvironmentError('测试磁盘暂不可用。', 'disk_offline')
        await original(current)

    monkeypatch.setattr(w, 'refresh_archive_photos', interrupted)
    try:
        await w.reconcile(cleanup=False)
        first = w.get('assets', asset['id'])
        assert first['status'] == 'waiting' and first['organization']['reasonCode'] == 'disk_offline'
        assert not w.online
        await w.reconcile(cleanup=False)
        current = w.get('assets', asset['id'])
        assert calls == 2 and w.online
        assert current['status'] == 'managed' and current['organization']['state'] == 'complete'
        assert Path(current['path']).is_file()
    finally:
        await close_app(app)


@pytest.mark.asyncio
async def test_root_probe_is_bounded_and_status_is_read_only_snapshot(tmp_path, monkeypatch):
    app, w, asset = await prepared(tmp_path)
    original = w._inspect_root
    release = threading.Event()
    calls = 0

    def blocked_root(*args):
        nonlocal calls
        calls += 1
        release.wait(2)
        return original(*args)

    w.file_checks.timeout = .02
    monkeypatch.setattr(w, '_inspect_root', blocked_root)
    try:
        with pytest.raises(FileEnvironmentError):
            await asyncio.wait_for(w.check_root(), .2)
        assert w.availability['reasonCode'] == 'disk_io' and not w.online
        before = time.monotonic()
        for _ in range(3):
            state = w.state()
            assert state['availability']['reasonCode'] == 'disk_io'
        assert time.monotonic() - before < .2 and calls == 1
    finally:
        release.set()
        await asyncio.gather(*w.file_checks.pending.values())
        await w.check_root()
        assert w.online and not w.availability['reasonCode']
        await close_app(app)


@pytest.mark.asyncio
async def test_cleanup_progress_uses_fixed_files_and_keeps_environment_reason(tmp_path):
    app, w, asset = await prepared(tmp_path)
    try:
        operation = await w.request_cleanup_completed(w.config['revision'], 'fixed-progress')
        assert operation['totalFiles'] == 5 and operation['deletedFiles'] == 0
        assert operation['remainingBytes'] == operation['totalBytes'] > 0
        with w.leases.read(asset['path']):
            await w.cleanup(asset)
        waiting = w.cleanup_operation('fixed-progress')
        assert waiting['reasonCode'] == 'file_busy' and waiting['deletedBytes'] == 0
        assert waiting['assets'][0]['remainingBytes'] == operation['remainingBytes']
        await w.cleanup(w.get('assets', asset['id']))
        done = w.cleanup_operation('fixed-progress')
        assert done['state'] == 'complete' and done['deletedFiles'] == done['totalFiles'] == 5
        assert done['remainingBytes'] == 0 and done['deletedBytes'] == operation['totalBytes']
        assert not done['reasonCode'] and not Path(asset['path']).exists()
        assert w.cleanup_summary()['eligibleCount'] == 0
    finally:
        await close_app(app)


@pytest.mark.asyncio
async def test_interrupted_cleanup_keeps_real_byte_progress_and_resumes_same_files(tmp_path, monkeypatch):
    app, w, asset = await prepared(tmp_path)
    original = w.videos.file_operation
    deletes = 0

    async def io(function, *args, **kwargs):
        nonlocal deletes
        if getattr(function, '__name__', '') == 'unlink_workspace_file':
            deletes += 1
            if deletes == 2:
                raise OSError(errno.EIO, 'injected read/write interruption')
        return await original(function, *args, **kwargs)

    try:
        operation = await w.request_cleanup_completed(w.config['revision'], 'progress-restart')
        monkeypatch.setattr(w.videos, 'file_operation', io)
        await w.cleanup(asset)
        partial = w.cleanup_operation('progress-restart')
        assert partial['state'] == 'waiting' and partial['reasonCode'] == 'disk_io'
        assert partial['deletedFiles'] == 1 and 0 < partial['deletedBytes'] < operation['totalBytes']
        assert partial['deletedBytes'] + partial['remainingBytes'] == operation['totalBytes']
        manifest = w.get('requests', 'progress-restart')['assets'][0]['manifest']
        monkeypatch.setattr(w.videos, 'file_operation', original)
        await w.cleanup(w.get('assets', asset['id']))
        done = w.cleanup_operation('progress-restart')
        assert done['state'] == 'complete' and done['deletedFiles'] == operation['totalFiles']
        assert done['deletedBytes'] == operation['totalBytes'] and done['remainingBytes'] == 0
        assert w.get('requests', 'progress-restart')['assets'][0]['manifest'] == manifest
    finally:
        await close_app(app)


@pytest.mark.asyncio
async def test_migration_disconnect_resumes_the_persisted_plan_automatically(tmp_path, monkeypatch):
    app = create_app(data_dir=tmp_path/'db')
    w = app.state.workspace
    pair(w, tmp_path/'original-one', name='one.mp4')
    pair(w, tmp_path/'original-two', name='two.mp4')
    original = w.process_move
    calls = 0

    async def interrupted(entry):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise FileEnvironmentError('测试断盘。', 'disk_offline')
        return await original(entry)

    monkeypatch.setattr(w, 'process_move', interrupted)
    try:
        await w.startup()
        preview = await w.preview(str(tmp_path), w.config['revision'], 'preview')
        await w.apply(preview['id'], w.config['revision'], 'apply')
        await w.task
        waiting = w.get('plans', preview['id'])
        assert waiting['state'] == 'waiting' and waiting['reasonCode'] == 'disk_offline'
        assert len(w.rows('plans')) == 1
        waiting['retryNotBefore'] = 0
        w.save('plans', waiting)
        w.wake.set()

        async def completed():
            while w.get('plans', preview['id'])['state'] != 'complete':
                await asyncio.sleep(.01)

        await asyncio.wait_for(completed(), 3)
        assert len(w.rows('plans')) == 1 and len(list(w.root.rglob('*.mp4'))) == 2
        assert all(move['state'] == 'complete' for move in w.rows('moves'))
    finally:
        await w.shutdown()
        await close_app(app)


def test_package_summary_omits_large_receipts_and_detail_filter_keeps_global_counts(packages, monkeypatch):
    first, second = package(), package('second')
    first.update(images=['never-serialize'] * 1000, object={'key': 'private-object'}, state='queued')
    second.update(dataset='27893', category='magnet', state='importing')
    for item in (first, second):
        packages.store.save('items', item)
    monkeypatch.setattr(packages.store, 'list', lambda *a: pytest.fail('Summary/detail filter loaded all payloads'))
    state = packages.state(summary=True)
    assert len(state['items']) == 2 and all(set(item) == {'id', 'itemId', 'category', 'state'} for item in state['items'])
    assert all('items' not in target for target in state['targets'])
    detail = packages.state(item_ids=['source-probe'])
    assert [item['id'] for item in detail['items']] == ['probe']
    assert detail['targets'][1]['importing'] == 1
    assert not detail['targets'][1]['items']
    assert 'images' not in detail['items'][0] and 'object' not in detail['items'][0]
    assert packages.state(item_ids=[])['items'] == []
