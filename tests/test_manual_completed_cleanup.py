"""Cleanup is an explicit fixed-scope operation; recognition never grants consent."""
import asyncio
from pathlib import Path

import pytest

from app.main import create_app
from app.videos import Conflict
from app.workspace_files import file_version
from tests.test_media_control import collection
from tests.test_slicing_performance import close_app
from tests.test_workspace import enable, pair, proofs, downloaded_original


async def prepared(tmp_path, name='truck.mp4'):
    app = create_app(data_dir=tmp_path/'db')
    w = app.state.workspace
    app.state.slicing._wake_scheduler = lambda: None
    path = pair(w, tmp_path/'old', ('complete', 'complete'), name)
    proofs(w, path, tmp_path)
    row = collection(app, path)
    row['versions'][0].update(taskId='capture-task', resultId='capture-result')
    w.videos.store.save(row)
    # Photo-only workflow needs no original-video uploads or platform connection.
    w.db.execute('DELETE FROM simple_upload_items')
    w.db.execute('DELETE FROM simple_upload_batches')
    w.db.commit()
    await enable(w, tmp_path)
    return app, w, w.rows('assets')[0]


@pytest.mark.asyncio
async def test_finished_assets_wait_for_manual_fixed_scope_and_replay(tmp_path):
    app, w, asset = await prepared(tmp_path)
    try:
        await w.reconcile()
        assert Path(asset['path']).is_file()
        assert w.get('assets', asset['id'])['cleanup']['state'] == 'pending_manual'
        summary = w.cleanup_summary()
        assert summary['eligibleCount'] == 1 and summary['eligibleBytes'] > 0
        operation = await w.request_cleanup_completed(w.config['revision'], 'cleanup-one')
        assert operation['total'] == 1
        # A later completed video is deliberately outside the accepted scope.
        later = pair(w, tmp_path/'later', ('no_target', 'no_target'), 'later.mp4')
        downloaded_original(w, later, 'later')
        for item in w.slicing.store.list('items'):
            if item['path'] != str(later):
                continue
            name = 'bed' if item['sourceDatasetId'] == '27912' else 'magnet'
            item['categories'][name]['processingConclusion'] = {'version': 1, 'fileVersion': item['fileVersion'],
                'outcome': 'no_target', 'requestedSamples': 3, 'decodedSamples': 3, 'skippedSamples': 0,
                'completed': True}
            w.slicing.store.save('items', item)
        await w.reconcile(cleanup=False)
        later_asset = next(row for row in w.rows('assets') if row['name'] == later.name)
        later = Path(later_asset['path'])
        assert w.cleanup_eligibility(later_asset)[0]
        assert (await w.request_cleanup_completed(-1, 'cleanup-one'))['total'] == 1
        await w.cleanup(w.get('assets', asset['id']))
        assert not Path(asset['path']).exists()
        assert later.exists()
        assert w.cleanup_operation('cleanup-one')['state'] == 'complete'
        assert w.slicing.store.list('items')
        assert w.cleanup_summary()['eligibleCount'] == 1
    finally:
        await close_app(app)


@pytest.mark.asyncio
async def test_no_manual_request_never_deletes_even_direct_call(tmp_path):
    app, w, asset = await prepared(tmp_path)
    try:
        await w.cleanup(asset)
        assert Path(asset['path']).exists()
        assert w.cleanup_summary()['eligibleCount'] == 1
        with pytest.raises(Conflict):
            await w.request_cleanup_completed(-1, 'stale')
    finally:
        await close_app(app)


@pytest.mark.asyncio
async def test_lease_wait_and_restart_keep_exact_manifest(tmp_path):
    app, w, asset = await prepared(tmp_path)
    try:
        await w.request_cleanup_completed(w.config['revision'], 'fixed-request')
        with w.leases.read(asset['path']):
            await w.cleanup(asset)
        assert Path(asset['path']).exists()
        stored = w.get('requests', 'fixed-request')
        assert stored['assets'][0]['state'] == 'waiting'
        manifest = stored['assets'][0]['manifest']
    finally:
        await close_app(app)
    restarted = create_app(data_dir=tmp_path/'db')
    try:
        w = restarted.state.workspace
        current = w.get('assets', asset['id'])
        assert w.get('requests', 'fixed-request')['assets'][0]['manifest'] == manifest
        await w.cleanup(current)
        assert w.cleanup_operation('fixed-request')['state'] == 'complete'
    finally:
        await close_app(restarted)


@pytest.mark.asyncio
@pytest.mark.parametrize('fault', ['file_changed', 'receipt_changed', 'new_version'])
async def test_changed_snapshot_or_receipt_is_retained(tmp_path, fault):
    app, w, asset = await prepared(tmp_path)
    try:
        await w.request_cleanup_completed(w.config['revision'], 'safe')
        if fault == 'file_changed':
            Path(asset['path']).write_bytes(b'changed-original')
        elif fault == 'receipt_changed':
            package = w.slicing.packages.store.get('items', 'p-bed')
            package['verifiedJobId'] = 'wrong-job'
            w.slicing.packages.store.save('items', package)
        else:
            row = w.videos.store.get('collection')
            row['activeRevision'] = 1
            w.videos.store.save(row)
        await w.cleanup(asset)
        assert Path(asset['path']).exists()
        assert w.get('assets', asset['id'])['cleanup']['state'] != 'complete'
    finally:
        await close_app(app)


@pytest.mark.asyncio
async def test_delete_crash_after_unlink_resumes_same_journal(tmp_path, monkeypatch):
    app, w, asset = await prepared(tmp_path)
    await w.request_cleanup_completed(w.config['revision'], 'crash')
    original = w.videos.file_operation
    crashed = False
    async def crash_after_delete(function, *args):
        nonlocal crashed
        value = await original(function, *args)
        if getattr(function, '__name__', '') == 'unlink' and not crashed:
            crashed = True
            raise asyncio.CancelledError()
        return value
    monkeypatch.setattr(w.videos, 'file_operation', crash_after_delete)
    with pytest.raises(asyncio.CancelledError):
        await w.cleanup(asset)
    assert crashed
    await close_app(app)
    restarted = create_app(data_dir=tmp_path/'db')
    try:
        w = restarted.state.workspace
        await w.cleanup(w.get('assets', asset['id']))
        assert w.cleanup_operation('crash')['state'] == 'complete'
        assert not Path(asset['path']).exists()
    finally:
        await close_app(restarted)


@pytest.mark.asyncio
async def test_empty_outcome_requires_explicit_current_sample_conclusion(tmp_path):
    app, w, asset = await prepared(tmp_path)
    try:
        w.db.execute('DELETE FROM simple_slice_package_items'); w.db.commit()
        for item in w.slicing.store.list('items'):
            name = 'bed' if item['sourceDatasetId'] == '27912' else 'magnet'
            item['categories'][name] = {'state': 'uncuttable'}
            w.slicing.store.save('items', item)
        assert not w.cleanup_eligibility(asset)[0]
        for item in w.slicing.store.list('items'):
            name = 'bed' if item['sourceDatasetId'] == '27912' else 'magnet'
            item['categories'][name]['processingConclusion'] = {'version': 1, 'fileVersion': item['fileVersion'],
                'outcome': 'no_usable_frames', 'requestedSamples': 3, 'decodedSamples': 0,
                'skippedSamples': 3, 'completed': True, 'exclusionIntervals': [[0, 60]]}
            w.slicing.store.save('items', item)
        assert w.cleanup_eligibility(asset)[0]
        assert Path(asset['path']).exists()
    finally:
        await close_app(app)


@pytest.mark.asyncio
async def test_cleanup_registration_does_not_wait_for_slow_organizer(tmp_path):
    app, w, asset = await prepared(tmp_path)
    try:
        async with w.lock:
            result = await asyncio.wait_for(w.request_cleanup_completed(w.config['revision'], 'fast-click'), 0.5)
        assert result['total'] == 1 and Path(asset['path']).exists()
    finally:
        await close_app(app)


@pytest.mark.asyncio
async def test_cleanup_counts_are_cached_and_do_not_probe_media(tmp_path, monkeypatch):
    app, w, asset = await prepared(tmp_path)
    try:
        first = w.cleanup_summary()
        def forbidden(*_args, **_kwargs):
            pytest.fail('An unchanged summary must not reread records or media.')
        monkeypatch.setattr(w, 'linked', forbidden)
        monkeypatch.setattr(w, 'packages', forbidden)
        monkeypatch.setattr(w.videos.store, 'list', forbidden)
        monkeypatch.setattr('app.workspace_cleanup.checked_path', forbidden)
        assert w.cleanup_summary() == first
        assert Path(asset['path']).exists()
    finally:
        await close_app(app)


@pytest.mark.asyncio
async def test_verified_move_carries_empty_conclusion_but_unbound_name_does_not(tmp_path):
    from app import file_locations
    app = create_app(data_dir=tmp_path/'db')
    try:
        db = app.state.store.db
        old = {'device': 1, 'inode': 2, 'size': 3, 'modifiedNs': 4, 'edgeHash': 'exact-content'}
        new = {**old, 'device': 11, 'inode': 22}
        item = {'path': '/managed/old.mp4', 'fileVersion': old, 'categories': {
            'bed': {'state': 'no_target', 'processingConclusion': {'fileVersion': dict(old)}},
            'magnet': {'state': 'no_target', 'processingConclusion': {'fileVersion': {**old, 'inode': 99}}}}}
        file_locations.remember(db, {'source': item['path'], 'destination': '/managed/new.mp4',
                                     'expected': old, 'destinationVersion': new})
        assert file_locations.field(db, item, 'path', 'fileVersion')
        proof = item['categories']['bed']['processingConclusion']
        assert proof['fileVersion'] == new and proof['originalFileVersion'] == old
        assert item['categories']['magnet']['processingConclusion']['fileVersion']['inode'] == 99
    finally:
        await close_app(app)


@pytest.mark.asyncio
async def test_verified_recovery_original_is_bound_and_cleanup_marks_both_histories(tmp_path):
    app, w, asset = await prepared(tmp_path)
    try:
        links = w.linked(asset)
        for item in links:
            item.update(collectionRecovery=True, collectionVideoId='collection', collectionRevision=0)
            w.slicing.store.save('items', item)
        row = w.videos.store.get('collection')
        version = row['versions'][0]
        version.update(state='failed', finalConfirmation={'version': 0, 'operationId': 'approved'},
            recovery={'state': 'partial_complete', 'resultVerified': True, 'filePath': version.pop('filePath'),
                      'fileVersion': version.pop('fileVersion'), 'slicingItemId': links[0]['id']})
        w.videos.store.save(row)
        assert w.cleanup_eligibility(asset)[0]
        version['recovery']['slicingItemId'] = 'different-result'
        w.videos.store.save(row)
        assert not w.cleanup_eligibility(asset)[0]
        version['recovery']['slicingItemId'] = links[0]['id']
        w.videos.store.save(row)
        await w.request_cleanup_completed(w.config['revision'], 'recover-cleanup')
        await w.cleanup(asset)
        current = w.videos.store.get('collection')['versions'][0]
        assert current['localCleanupState'] == current['recovery']['localCleanupState'] == 'complete'
        assert not Path(asset['path']).exists()
    finally:
        await close_app(app)


@pytest.mark.asyncio
async def test_import_count_must_match_unique_local_photo_manifest(tmp_path):
    app, w, asset = await prepared(tmp_path)
    try:
        package = w.slicing.packages.store.get('items', 'p-bed')
        package.update(imageCount=2, actualImages=2, outputImageIds=['one', 'two'])
        w.slicing.packages.store.save('items', package)
        item = w.slicing.store.get('items', package['itemId'])
        item['categories']['bed']['actualImages'] = 2
        w.slicing.store.save('items', item)
        assert not w.cleanup_eligibility(asset)[0]
        result = await w.request_cleanup_completed(w.config['revision'], 'wrong-manifest')
        assert result['total'] == 0 and Path(asset['path']).exists()
    finally:
        await close_app(app)


@pytest.mark.asyncio
@pytest.mark.parametrize('fault', ['wrong_job', 'invalid_image_id', 'weak_version', 'wrong_dataset'])
async def test_partial_completion_uses_same_strict_import_proof_as_cleanup(tmp_path, fault):
    from tests.test_archive_completion import seed
    app=create_app(data_dir=tmp_path/'db')
    try:
        original=seed(app,tmp_path)
        packages=app.state.slicing.packages.store
        package=packages.get('items','magnet')
        if fault=='wrong_job':
            package['jobId']='different-job'
        elif fault=='invalid_image_id':
            package['outputImageIds']=[None]
        elif fault=='weak_version':
            package['videoFileVersion']={'size':original.stat().st_size}
        else:
            item=app.state.slicing.store.get('items','recovery')
            item['categories']['magnet']['targetDatasetId']='expected-target'
            app.state.slicing.store.save('items',item)
            package['dataset']='wrong-target'
        packages.save('items',package)
        app.state.videos.reconcile_recovery_results()
        row=app.state.videos.store.get('collection')
        assert app.state.videos.recovery_completion_proof(row,row['versions'][0]) is None
        assert not row['versions'][0]['recovery'].get('resultVerified')
        assert row['versions'][0]['downloadRequested']
        assert original.is_file()
    finally:
        await close_app(app)


@pytest.mark.asyncio
async def test_cleanup_disk_disconnect_waits_but_unlogged_missing_file_needs_review(tmp_path):
    app,w,asset=await prepared(tmp_path)
    try:
        await w.request_cleanup_completed(w.config['revision'],'disk-loss')
        original=w.root;disconnected=original.with_name('detached-test-disk')
        original.rename(disconnected)
        try:
            await w.cleanup(asset)
            assert w.cleanup_operation('disk-loss')['state']=='waiting'
            assert not any(e['state']=='deleted' for e in w.get('assets',asset['id'])['cleanup']['manifest'])
        finally:
            disconnected.rename(original)
        assert Path(asset['path']).is_file()
        # The disk is back, but a separately removed planned photo has no
        # deletion journal: do not pretend a network retry can restore it.
        w.require_root()
        entries=w.media_entries(asset);missing=Path(entries[-1]['path']);missing.unlink()
        await w.cleanup(w.get('assets',asset['id']))
        current=w.get('assets',asset['id'])
        assert current['cleanup']['state']=='attention' and '外部移除' in current['cleanup']['message']
        assert Path(asset['path']).is_file()
        assert not any(e['state']=='deleted' for e in current['cleanup']['manifest'])
    finally:
        await close_app(app)
