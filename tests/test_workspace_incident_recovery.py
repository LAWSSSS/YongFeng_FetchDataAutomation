"""Faults at the last safe boundary: receipts, user commands and disk writes."""
import errno
from contextlib import ExitStack
from pathlib import Path

import pytest
import pytest_asyncio

from app.main import create_app
from app.workspace_files import file_version
from tests.test_slicing_performance import close_app
from tests.test_workspace import enable, pair, proofs, downloaded_original
from tests.test_media_control import collection
from app.slicing import source_version


@pytest_asyncio.fixture
async def env(tmp_path, monkeypatch):
    app = create_app(data_dir=tmp_path / 'db')
    monkeypatch.setattr(app.state.slicing, '_wake_scheduler', lambda: None)
    yield app, app.state.workspace
    await app.state.pipeline.shutdown()
    await app.state.workspace.shutdown()
    await close_app(app)


@pytest.mark.asyncio
async def test_final_same_disk_move_rechecks_receipts_after_async_disk_probe(env, tmp_path, monkeypatch):
    app, w = env
    original = pair(w, tmp_path / 'old', ('complete', 'complete'))
    proofs(w, original, tmp_path)
    await w.discover()
    entry = next(e for e in w.plan_assets(tmp_path / 'future') if e['kind'] == 'video')
    io = w.videos.file_operation

    async def invalidate_after_probe(function, *args):
        value = await io(function, *args)
        if function.__name__ == 'same_device':
            package = w.slicing.packages.store.get('items', 'p-magnet')
            package['state'] = 'importing'
            w.slicing.packages.store.save('items', package)
        return value

    monkeypatch.setattr(w.videos, 'file_operation', invalidate_after_probe)
    with pytest.raises(ValueError, match='尚未全部'):
        await w.process_move(entry)
    assert original.exists() and not Path(entry['destination']).exists()


@pytest.mark.asyncio
@pytest.mark.parametrize('command', ['pause', 'takeover', 'photo_receipt_changed'])
async def test_cleanup_rechecks_latest_command_after_file_hash_before_unlink(env, tmp_path, monkeypatch, command):
    app, w = env
    source = pair(w, tmp_path / 'old', ('complete', 'complete'))
    proofs(w, source, tmp_path)
    downloaded_original(w, source)
    await enable(w, tmp_path)
    app.state.media_control.refresh()
    asset = w.rows('assets')[0]
    await w.request_cleanup_completed(w.config['revision'], 'manual-safe-cleanup')
    path = Path(asset['path'])
    io = w.videos.file_operation
    injected = False

    async def intervene_during_hash(function, *args):
        nonlocal injected
        value = await io(function, *args)
        if not injected and function is file_version and str(args[0]) in w.leases.writers:
            injected = True
            if command == 'photo_receipt_changed':
                package = w.slicing.packages.store.get('items', 'p-magnet')
                package['state'] = 'importing'
                w.slicing.packages.store.save('items', package)
            else:
                item = w.slicing.store.list('items')[0]
                row = app.state.media_control.get(item['mediaId'])
                if command == 'pause':
                    row['paused'] = True
                else:
                    row.update(blocked=True, generation=row['generation'] + 1)
                app.state.media_control.save(row)
        return value

    monkeypatch.setattr(w.videos, 'file_operation', intervene_during_hash)
    await w.cleanup(asset)
    assert injected and path.exists(), 'A newer instruction or invalid photo receipt must prevent this unlink.'
    assert all(Path(p['path']).exists() for p in w.slicing.packages.store.list('items'))
    assert w.get('assets', asset['id'])['cleanup']['state'] != 'complete'


@pytest.mark.asyncio
async def test_transient_move_io_error_waits_then_recovers_without_manual_retry(env, tmp_path, monkeypatch):
    app, w = env
    pair(w, tmp_path / 'old')
    await enable(w, tmp_path)
    for item in w.slicing.store.list('items'):
        for slot in item['categories'].values():
            if slot['state'] != 'not_applicable':
                slot.update(state='no_target')
        item['state'] = 'no_target'
        w.slicing.store.save('items', item)
    asset = w.rows('assets')[0]
    original = Path(asset['path'])
    clock = [100.0]
    w.slicing.clock = lambda: clock[0]
    io = w.videos.file_operation
    failures = []

    async def disk_blip(function, *args):
        if function.__name__ == 'move_direct' and not failures:
            failures.append(function.__name__)
            raise OSError(errno.EIO, 'controlled disk fault')
        return await io(function, *args)

    monkeypatch.setattr(w.videos, 'file_operation', disk_blip)
    w.request_archive_retry(asset['id'], 'recover-disk-blip')
    await w.reconcile(cleanup=False)
    assert original.exists() and w.get('assets', asset['id'])['status'] == 'waiting'
    assert w.get('requests', 'recover-disk-blip')['state'] == 'queued'
    clock[0] += 10
    await w.reconcile(cleanup=False)
    current = w.get('assets', asset['id'])
    assert current['status'] == 'managed' and current['stage'] == '已切片'
    assert Path(current['path']).exists() and not original.exists()
    assert w.get('requests', 'recover-disk-blip')['state'] == 'complete'


@pytest.mark.asyncio
async def test_one_unexpected_cleanup_failure_does_not_drop_other_assets(env, tmp_path, monkeypatch):
    app, w = env
    for name in ('one', 'two'):
        pair(w, tmp_path / name)
    await enable(w, tmp_path)
    assets = w.rows('assets')
    calls = []

    async def cleanup_one(asset):
        calls.append(asset['id'])
        if asset['id'] == assets[0]['id']:
            raise RuntimeError('controlled isolated worker failure')
        asset['cleanup'] = {'state': 'complete'}
        w.save('assets', asset)

    monkeypatch.setattr(w, 'cleanup', cleanup_one)
    await w._cleanup_ids([a['id'] for a in assets])
    assert calls == [a['id'] for a in assets]
    failed = w.get('assets', assets[0]['id'])
    assert failed['cleanup']['state'] == 'waiting' and failed['cleanup']['failureType'] == 'RuntimeError'
    assert w.get('assets', assets[1]['id'])['cleanup']['state'] == 'complete'


@pytest.mark.asyncio
async def test_reader_starting_before_cleanup_wakes_cleanup_on_release(env, tmp_path):
    app, w = env
    source = pair(w, tmp_path / 'old', ('complete', 'complete'))
    proofs(w, source, tmp_path)
    downloaded_original(w, source)
    await enable(w, tmp_path)
    asset = w.rows('assets')[0]
    await w.request_cleanup_completed(w.config['revision'], 'manual-safe-cleanup')
    stack = ExitStack()
    acquired = False

    stack.enter_context(w.leases.read(asset['path']))
    try:
        await w.cleanup(asset)
        assert Path(asset['path']).exists()
        assert asset['path'] in w.waiting_paths
        w.wake.clear()
    finally:
        stack.close()
    assert w.wake.is_set(), 'Releasing the late reader must not wait for the next 30-second inventory.'
    await w.cleanup(w.get('assets', asset['id']))
    assert w.get('assets', asset['id'])['cleanup']['state'] == 'complete'


@pytest.mark.asyncio
async def test_move_listener_failure_restarts_from_exact_published_receipt(tmp_path):
    database = tmp_path / 'db'
    app = create_app(data_dir=database)
    app.state.slicing._wake_scheduler = lambda: None
    w = app.state.workspace
    try:
        pair(w, tmp_path / 'old')
        await enable(w, tmp_path)
        asset = w.rows('assets')[0]
        old_path = asset['path']
        for item in w.slicing.store.list('items'):
            for category in item['categories'].values():
                if category['state'] != 'not_applicable':
                    category['state'] = 'no_target'
            item['state'] = 'no_target'
            w.slicing.store.save('items', item)

        def broken_listener(*_):
            raise RuntimeError('controlled listener interruption after publication')

        w.leases.move_listeners.insert(0, broken_listener)
        await w.reconcile(cleanup=False)
        receipt = next(e for e in reversed(w.rows('moves')) if e['source'] == old_path)
        assert receipt['state'] == 'moving'
        destination = Path(receipt['destination'])
        assert destination.exists() and not Path(old_path).exists()
        # Later callbacks must already know the new location even though the
        # first subscriber failed and left a replayable publication journal.
        assert all(item['path'] == str(destination) for item in w.slicing.store.list('items'))
        assert w.get('assets', asset['id'])['path'] == str(destination)
    finally:
        await close_app(app)
    app = create_app(data_dir=database)
    app.state.slicing._wake_scheduler = lambda: None
    w = app.state.workspace
    try:
        w.request_archive_retry(asset['id'], 'replay-published-location')
        await w.reconcile(cleanup=False)
        assert w.get('moves', receipt['id'])['state'] == 'complete'
        assert all(item['path'] == str(destination) for item in w.slicing.store.list('items'))
        assert w.get('requests', 'replay-published-location')['state'] == 'complete'
        assert len(list(w.root.rglob('*.mp4'))) == 1
    finally:
        await close_app(app)


@pytest.mark.asyncio
async def test_new_generation_after_atomic_rename_keeps_exact_location_and_restarts(tmp_path, monkeypatch):
    database = tmp_path / 'db'
    app = create_app(data_dir=database)
    app.state.slicing._wake_scheduler = lambda: None
    w = app.state.workspace
    try:
        path = pair(w, tmp_path / 'old')
        collection(app, path)
        await enable(w, tmp_path)
        for item in w.slicing.store.list('items'):
            for category in item['categories'].values():
                if category['state'] != 'not_applicable':
                    category['state'] = 'no_target'
            item['state'] = 'no_target'
            w.slicing.store.save('items', item)
        app.state.media_control.refresh()
        asset = w.rows('assets')[0]
        source = Path(asset['path'])
        io = w.videos.file_operation
        mutations = []
        command = {}

        async def take_over_after_commit(function, *args):
            value = await io(function, *args)
            if function.__name__ == 'move_direct':
                mutations.append(function.__name__)
                item = w.slicing.store.list('items')[0]
                latest = app.state.media_control.get(item['mediaId'])
                latest.update(generation=latest['generation'] + 1, blocked=False, owner='review')
                app.state.media_control.save(latest)
                command.update(latest)
                newer = w.videos.store.get('collection')
                newer.update(revision=1, activeRevision=1)
                newer['versions'].append({'revision': 1, 'state': 'ready', 'taskId': 'new-task-kept'})
                w.videos.store.save(newer)
            return value

        monkeypatch.setattr(w.videos, 'file_operation', take_over_after_commit)
        await w.reconcile(cleanup=False)
        entry = next(e for e in reversed(w.rows('moves')) if e['source'] == str(source))
        destination = Path(entry['destination'])
        assert not source.exists() and destination.exists()
        assert w.get('assets', asset['id'])['path'] == str(destination)
        assert all(item['path'] == str(destination) and item['state'] == 'no_target'
                   for item in w.slicing.store.list('items'))
        assert entry['state'] == 'complete'
        newer = w.videos.store.get('collection')
        assert newer['activeRevision'] == 1 and newer['acceptedRevision'] == 0
        assert newer['versions'][1] == {'revision': 1, 'state': 'ready', 'taskId': 'new-task-kept'}
        assert newer['versions'][0]['filePath'] == str(destination)
        assert newer['versions'][0]['fileVersion'] == source_version(destination)
        assert app.state.media_control.get(command['id'])['generation'] == command['generation']
        await w.reconcile(cleanup=False)
        assert mutations == ['move_direct']
    finally:
        await close_app(app)
    app = create_app(data_dir=database)
    app.state.slicing._wake_scheduler = lambda: None
    try:
        w = app.state.workspace
        await w.reconcile(cleanup=False)
        assert w.get('assets', asset['id'])['path'] == str(destination)
        assert w.get('moves', entry['id'])['state'] == 'complete'
        assert all(item['path'] == str(destination) and item['state'] == 'no_target'
                   for item in w.slicing.store.list('items'))
        assert app.state.media_control.get(command['id'])['generation'] == command['generation']
        assert len(list(w.root.rglob('*.mp4'))) == 1
        newer = w.videos.store.get('collection')
        assert newer['activeRevision'] == 1 and newer['versions'][1]['taskId'] == 'new-task-kept'
        assert newer['versions'][0]['filePath'] == str(destination)
        assert newer['versions'][0]['fileVersion'] == source_version(destination)
    finally:
        await close_app(app)


@pytest.mark.asyncio
async def test_new_generation_after_copy_publication_keeps_source_and_records_target(env, tmp_path, monkeypatch):
    app, w = env
    pair(w, tmp_path / 'old')
    await enable(w, tmp_path)
    for item in w.slicing.store.list('items'):
        for category in item['categories'].values():
            if category['state'] != 'not_applicable':
                category['state'] = 'no_target'
        item['state'] = 'no_target'
        w.slicing.store.save('items', item)
    app.state.media_control.refresh()
    asset = w.rows('assets')[0]
    source = Path(asset['path'])
    io = w.videos.file_operation

    async def take_over_after_publication(function, *args):
        if function.__name__ == 'same_device':
            return False  # Same test disk, actual copy/hash/publish workflow.
        value = await io(function, *args)
        if function.__name__ == 'publish_copied':
            item = w.slicing.store.list('items')[0]
            latest = app.state.media_control.get(item['mediaId'])
            latest.update(generation=latest['generation'] + 1, blocked=False, owner='review')
            app.state.media_control.save(latest)
        return value

    monkeypatch.setattr(w.videos, 'file_operation', take_over_after_publication)
    await w.reconcile(cleanup=False)
    entry = next(e for e in reversed(w.rows('moves')) if e['source'] == str(source))
    destination = Path(entry['destination'])
    assert source.exists() and destination.exists(), 'The new command must prevent removing the source copy.'
    assert source.read_bytes() == destination.read_bytes()
    assert w.get('assets', asset['id'])['path'] == str(destination)
    assert all(item['path'] == str(destination) and item['state'] == 'no_target'
               for item in w.slicing.store.list('items'))
    assert entry['state'] != 'complete'


@pytest.mark.asyncio
async def test_copy_listener_failure_keeps_collection_path_and_version_atomic_across_restart(tmp_path, monkeypatch):
    database = tmp_path / 'db'
    app = create_app(data_dir=database)
    app.state.slicing._wake_scheduler = lambda: None
    w = app.state.workspace
    try:
        path = pair(w, tmp_path / 'old')
        collection(app, path)
        await enable(w, tmp_path)
        for item in w.slicing.store.list('items'):
            for category in item['categories'].values():
                if category['state'] != 'not_applicable':
                    category['state'] = 'no_target'
            item['state'] = 'no_target'
            w.slicing.store.save('items', item)
        asset = w.rows('assets')[0]
        source = Path(asset['path'])

        def broken_listener(*_):
            raise RuntimeError('controlled copy notification failure')

        w.leases.move_listeners.insert(0, broken_listener)
        monkeypatch.setattr('app.workspace.same_device', lambda *args: False)
        await w.reconcile(cleanup=False)
        entry = next(e for e in reversed(w.rows('moves')) if e['source'] == str(source))
        destination = Path(entry['destination'])
        assert source.exists() and destination.exists()
        assert entry['state'] == 'moving'
        assert entry['destinationVersion']['inode'] != entry['fileVersion']['inode']
        version = w.videos.store.get('collection')['versions'][0]
        assert version['filePath'] == str(destination) and version['fileVersion'] == source_version(destination)
    finally:
        await close_app(app)
    app = create_app(data_dir=database)
    app.state.slicing._wake_scheduler = lambda: None
    w = app.state.workspace
    try:
        w.request_archive_retry(asset['id'], 'resume-copy-notification')
        await w.reconcile(cleanup=False)
        assert not source.exists() and destination.exists()
        assert w.get('moves', entry['id'])['state'] == 'complete'
        version = w.videos.store.get('collection')['versions'][0]
        assert version['filePath'] == str(destination) and version['fileVersion'] == source_version(destination)
        assert len(list(w.root.rglob('*.mp4'))) == 1
    finally:
        await close_app(app)
