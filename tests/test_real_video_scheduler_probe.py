"""Fixed-behavior regressions from the isolated real-video failure scene."""
import asyncio
import copy

import pytest

from app.main import create_app
from app.slicing import source_version
from app.videos import Conflict
from tests.test_slicing_continue import failed, setup_run
from tests.test_slicing_performance import close_app


@pytest.mark.asyncio
@pytest.mark.parametrize('other_state', ['ready', 'importing', 'complete'])
@pytest.mark.parametrize('callback', [False, True], ids=['persisted-legacy-state', 'category-callback'])
async def test_due_category_continues_after_another_category_callback(tmp_path, monkeypatch, other_state, callback):
    app = create_app(data_dir=tmp_path / 'private-db')
    manager = app.state.slicing
    manager.control = None
    manager.clock = lambda: 100.0
    manager.auth.access = 'isolated-probe'
    path = tmp_path / 'readonly-probe.mp4'; path.write_bytes(b'not-decoded-by-this-state-probe')
    item = {'id': 'pending-bed', 'sourceDatasetId': '27912', 'sourceVideoId': 'probe-only',
            'name': path.name, 'path': str(path), 'fileVersion': source_version(path),
            'revision': 1, 'state': 'platform_processing', 'localWorkspace': True,
            'categories': {'bed': {'state': 'pending', 'retryNotBefore': 99.0,
                                    'recoveryDisposition': 'waiting', 'observations': 0},
                           'magnet': {'state': other_state, 'intervals': [{'start': 0, 'end': 2}],
                                      'intervalSeconds': 1, 'observations': 2}}}
    manager.store.settings.update(enabled=True, localExport=True)
    manager.store.save('items', item)
    manager.run = {'id': 'probe-run', 'state': 'running', 'items': [item['id']], 'localRecoveryOnly': True}
    spawned, release = [], asyncio.Event()
    async def process(row):
        spawned.append(row['id'])
        await release.wait()
    monkeypatch.setattr(manager, 'process_with_recovery', process)
    monkeypatch.setattr(manager, '_schedule_export', lambda row: None)
    monkeypatch.setattr(manager, '_schedule_platform_work', lambda: None)
    try:
        if callback:
            manager._save_category_progress(manager.store.get('items', item['id']))
            assert manager.store.get('items', item['id'])['state'] == 'pending'
        await manager._tick(); await asyncio.sleep(0)
        assert spawned == [item['id']]
        await manager._tick(); await asyncio.sleep(0)
        assert spawned == [item['id']]  # an upload callback cannot duplicate analysis
        assert manager.store.get('items', item['id'])['categories']['magnet'] == item['categories']['magnet']
    finally:
        release.set()
        await close_app(app)


@pytest.mark.asyncio
async def test_writer_conflict_waits_without_a_model_failure_or_retry_budget(tmp_path, monkeypatch):
    app = create_app(data_dir=tmp_path / 'private-db')
    manager = app.state.slicing
    manager.control = None
    manager.clock = lambda: 100.0
    path = tmp_path / 'occupied.mp4'; path.write_bytes(b'state-probe-only')
    row = failed(app, path, 'bed', '27912', 'worker_failed')
    row.update(state='pending', errorCode='')
    row['categories']['bed'].update(state='pending', errorCode='', automaticRetries=0)
    manager.store.save('items', row)
    async def unchanged(_): return False
    async def forbidden(_): raise AssertionError('writer conflict must not reach inference')
    monkeypatch.setattr(manager, '_refresh_local_binding', unchanged)
    monkeypatch.setattr(manager, '_analyze_item', forbidden)
    try:
        with manager.leases.write(str(path)):
            recovered = await manager.process_with_recovery(row)
        bed = recovered['categories']['bed']
        assert recovered['state'] == 'pending' and recovered['errorCode'] == ''
        assert bed['recoveryDisposition'] == 'waiting' and bed['waitCode'] == 'file_busy'
        assert bed['retryNotBefore'] == 100.5 and bed['automaticRetries'] == 0
        assert bed['errorCode'] == ''
    finally:
        await close_app(app)


@pytest.mark.asyncio
async def test_continue_resumes_only_stranded_pending_category_and_replays_once(tmp_path, monkeypatch):
    app = create_app(data_dir=tmp_path / 'private-db')
    path = tmp_path / 'stranded.mp4'; path.write_bytes(b'state-probe-only')
    manager = setup_run(app, enabled=True)
    monkeypatch.setattr(manager, '_wake_scheduler', lambda: None)
    row = failed(app, path, 'bed', '27912')
    row.update(state='platform_processing', errorCode='')
    row['categories']['bed'].update(state='pending', errorCode='', retryNotBefore=200, automaticRetries=1)
    row['categories']['magnet'] = {'state': 'complete', 'packageId': 'already-verified-probe', 'actualImages': 150}
    manager.store.save('items', row)
    control = app.state.media_control
    control.refresh(); row = manager.store.get('items', row['id'])
    body = {'requestId': 'resume-stranded', 'generation': row['mediaGeneration'],
            'selections': [{'id': row['id'], 'expectedRevision': row['revision']}], 'packages': []}
    try:
        result = await control.continue_safe(row['mediaId'], body)
        current = manager.store.get('items', row['id'])
        assert result['outcomes'][0]['ok'] and current['state'] == 'pending'
        assert current['categories'] == row['categories']
        revision = current['revision']
        replay = await control.continue_safe(row['mediaId'], body)
        assert replay['outcomes'] == result['outcomes']
        assert manager.store.get('items', row['id'])['revision'] == revision
        with pytest.raises(Conflict):
            await control.continue_safe(row['mediaId'], {**body, 'requestId': 'stale-pending'})
    finally:
        await close_app(app)


@pytest.mark.asyncio
@pytest.mark.parametrize('flag', ['pipelinePaused', 'collectionRecoveryPaused', 'superseded'])
async def test_pending_category_cannot_override_user_pause_or_supersession(tmp_path, monkeypatch, flag):
    app = create_app(data_dir=tmp_path / 'private-db'); manager = setup_run(app, enabled=True)
    monkeypatch.setattr(manager, '_wake_scheduler', lambda: None)
    path = tmp_path / 'paused.mp4'; path.write_bytes(b'state-probe-only')
    row = failed(app, path, 'paused', '27912'); row.update(state='platform_processing', errorCode='', **{flag: True})
    row['categories']['bed'].update(state='pending', errorCode='')
    manager.store.save('items', row)
    try:
        assert not manager.pending_analysis(manager.store.get('items', row['id']), due=False)
        with pytest.raises(Conflict):
            manager.continue_pending(row['id'], row['revision'], 'paused-request')
        assert manager.store.get('items', row['id']).get(flag)
    finally:
        await close_app(app)


@pytest.mark.asyncio
async def test_identity_failure_does_not_become_automatic_just_because_another_slot_is_pending(tmp_path):
    app = create_app(data_dir=tmp_path / 'private-db'); manager = app.state.slicing
    try:
        item = {'state': 'attention', 'errorCode': 'source_changed', 'categories': {'bed': {'state': 'attention', 'errorCode': 'source_changed'}, 'magnet': {'state': 'pending'}}}
        assert not manager.pending_analysis(item)
        item['errorCode'] = 'bed_resolution'
        item['categories']['bed']['errorCode'] = 'bed_resolution'
        assert manager.pending_analysis(item) == ['magnet']
    finally:
        await close_app(app)


@pytest.mark.asyncio
async def test_analysis_progress_never_rolls_back_another_category_import_callback(tmp_path, monkeypatch):
    app = create_app(data_dir=tmp_path / 'private-db'); manager = app.state.slicing
    manager.control = None
    path = tmp_path / 'parallel.mp4'; path.write_bytes(b'private-state-fixture')
    row = failed(app, path, 'parallel', '27912')
    row.update(state='pending', errorCode='', localWorkspace=True)
    row['categories'] = {'bed': {'state': 'pending'}, 'magnet': {'state': 'importing', 'packageId': 'verified-package'}}
    manager.store.save('items', row)
    async def unchanged(_): return False
    async def analyze(work):
        # A real package callback saves a fresh record while FFmpeg is awaited.
        fresh = manager.store.get('items', row['id'])
        fresh['categories']['magnet'].update(state='complete', actualImages=101, importJobId='exact-job')
        manager.store.save('items', fresh)
        work.update(analysisDone=1, analysisTotal=2, analysisStage='sampling')
        manager._save_analysis_item(work)
        assert work['categories']['magnet']['state'] == 'complete'
        work['categories']['bed'].update(state='no_target', observations=2)
        manager._save_analysis_item(work)
        return {'duration': 30}
    monkeypatch.setattr(manager, '_refresh_local_binding', unchanged)
    monkeypatch.setattr(manager, '_analyze_item', analyze)
    try:
        await manager.process_with_recovery(manager.store.get('items', row['id']))
        current = manager.store.get('items', row['id'])
        assert current['state'] == 'complete' and current['categories']['bed']['state'] == 'no_target'
        assert current['categories']['magnet'] == {'state': 'complete', 'packageId': 'verified-package',
                                                   'actualImages': 101, 'importJobId': 'exact-job'}
        assert not any(key.startswith('_analysis') for key in current)
    finally:
        await close_app(app)


@pytest.mark.asyncio
@pytest.mark.parametrize('takeover', ['generation', 'file-version', 'manual-category'])
async def test_late_analysis_cannot_override_a_newer_version_or_manual_outcome(tmp_path, monkeypatch, takeover):
    app = create_app(data_dir=tmp_path / 'private-db'); manager = app.state.slicing
    manager.control = None
    path = tmp_path / 'takeover.mp4'; path.write_bytes(b'private-state-fixture')
    row = failed(app, path, 'takeover', '27912')
    row.update(state='pending', errorCode='', localWorkspace=True, mediaGeneration=0)
    row['categories'] = {'bed': {'state': 'pending'}, 'magnet': {'state': 'complete', 'packageId': 'old-result'}}
    manager.store.save('items', row)
    expected = []
    async def unchanged(_): return False
    async def analyze(work):
        fresh = manager.store.get('items', row['id']); fresh['revision'] += 1
        if takeover == 'generation':
            fresh['mediaGeneration'] = 1
        elif takeover == 'file-version':
            fresh['fileVersion']['size'] += 1
        fresh['categories']['bed'] = {'state': 'ready', 'intervals': [{'start': 5, 'end': 9}], 'intervalSeconds': 1}
        manager.store.save('items', fresh); expected.append(copy.deepcopy(fresh))
        work['categories']['bed'].update(state='no_target', observations=2)
        manager._save_analysis_item(work)
        return {'duration': 30}
    monkeypatch.setattr(manager, '_refresh_local_binding', unchanged)
    monkeypatch.setattr(manager, '_analyze_item', analyze)
    try:
        await manager.process_with_recovery(manager.store.get('items', row['id']))
        assert manager.store.get('items', row['id']) == expected[0]
    finally:
        await close_app(app)


@pytest.mark.asyncio
@pytest.mark.parametrize('during', ['binding-refresh', 'inference-exception'])
async def test_takeover_fences_early_return_and_exception_recovery(tmp_path, monkeypatch, during):
    app = create_app(data_dir=tmp_path / 'private-db'); manager = app.state.slicing
    manager.control = None
    path = tmp_path / 'takeover-fence.mp4'; path.write_bytes(b'private-state-fixture')
    row = failed(app, path, 'takeover-fence', '27912')
    row.update(state='pending', errorCode='', localWorkspace=True, mediaGeneration=0)
    row['categories']['bed'].update(state='pending', errorCode='')
    manager.store.save('items', row); expected = []
    def takeover():
        fresh = manager.store.get('items', row['id'])
        fresh.update(mediaGeneration=1, revision=fresh['revision'] + 1)
        fresh['categories']['bed'] = {'state': 'ready', 'intervals': [{'start': 5, 'end': 9}], 'intervalSeconds': 1}
        manager.store.save('items', fresh); expected.append(copy.deepcopy(fresh))
    async def refresh(_):
        if during == 'binding-refresh': takeover()
        return False
    async def analyze(_):
        if during == 'inference-exception':
            takeover(); raise RuntimeError('injected old-worker failure after newer command')
        raise AssertionError('old generation must stop before inference')
    monkeypatch.setattr(manager, '_refresh_local_binding', refresh)
    monkeypatch.setattr(manager, '_analyze_item', analyze)
    try:
        await manager.process_with_recovery(manager.store.get('items', row['id']))
        assert manager.store.get('items', row['id']) == expected[0]
    finally:
        await close_app(app)


@pytest.mark.asyncio
async def test_other_pending_model_can_run_while_source_model_needs_manual_review(tmp_path, monkeypatch):
    app = create_app(data_dir=tmp_path / 'private-db'); manager = app.state.slicing
    manager.control = None; manager.auth.access = 'isolated-probe'
    path = tmp_path / 'model-isolation.mp4'; path.write_bytes(b'private-state-fixture')
    row = failed(app, path, 'bed', '27912', 'bed_resolution')
    row['categories']['magnet'] = {'state': 'pending'}
    manager.store.save('items', row); manager.store.settings.update(enabled=True, sharedSampling=True)
    manager.run = {'id': 'models', 'state': 'running', 'items': [row['id']]}
    started, release = [], asyncio.Event()
    async def analyze(work):
        started.append(work['id']); await release.wait()
    async def forbidden(*args): raise AssertionError('source category is not the pending work')
    monkeypatch.setattr(manager, 'process_with_recovery', analyze)
    monkeypatch.setattr(manager, '_process_shared', forbidden)
    monkeypatch.setattr(manager, '_schedule_platform_work', lambda: None)
    try:
        await manager._tick(); await asyncio.sleep(0)
        assert started == ['bed']
        assert manager.store.get('items', row['id'])['categories']['bed']['errorCode'] == 'bed_resolution'
    finally:
        release.set(); await close_app(app)
