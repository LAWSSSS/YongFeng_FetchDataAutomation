"""A few rejected bed frames never discard valid work or replay photo imports."""
import asyncio
import copy

import pytest

from app.main import create_app
from app.photo_completion import verified_empty_conclusion
from app.slicing_core import (build_category_result, category_exclusions,
    partition_category_observations, position_excluded, sample_points)
from tests.test_slicing_recovery import item_fixture, prepare_run
from tests.test_slicing_performance import close_app
from tests.test_final_processing import env
from tests.test_processing_live_projection import confirmed_photos
from tests.test_media_control import sliced
from app.video_filters import record_status


def samples(duration=120, failures=(30,), code='bed_alignment_uncertain', label='target'):
    return [{'time': point, 'label': 'uncertain' if point in failures else label,
             'code': code if point in failures else '', 'actualTime': point,
             'quality': {'quality': 'excessive_shift' if point in failures else 'ok'}}
            for point in sample_points(duration, 30)]


@pytest.mark.parametrize('failures', [(30,), (30, 60), (0,), (119.96,)])
def test_alignment_exclusions_are_category_local_and_cannot_be_bridged(failures):
    item = {'fileVersion': {'size': 123}, 'categories': {'bed': {}, 'magnet': {}}}
    rows = samples(failures=failures)
    before = copy.deepcopy(rows)
    valid, unavailable, rejected, excluded = partition_category_observations(item, 'bed', rows, 120, 'bed-v1')
    item['categories']['bed']['modelFingerprint'] = 'bed-v1'
    assert not unavailable and len(rejected) == len(failures) and rows == before
    assert not item.get('decodeCoverage'), 'alignment is not a shared decoding failure'
    assert category_exclusions(item, 'magnet') == []
    assert all(position_excluded(point, excluded) for point in failures)
    result = build_category_result(valid, interval=30, duration=120, positive_labels={'target'}, exclusion_intervals=excluded)
    assert result['intervals']
    assert all(not (row['start'] <= end and row['end'] >= start)
               for row in result['intervals'] for start, end in excluded)
    item['categories']['bed']['modelFingerprint'] = 'changed'
    with pytest.raises(ValueError, match='model_changed'):
        category_exclusions(item, 'bed')
    item['categories']['bed']['modelFingerprint'] = 'bed-v1'
    item['fileVersion'] = {'size': 124}
    with pytest.raises(ValueError, match='source_changed'):
        category_exclusions(item, 'bed')


def test_normal_windows_are_unchanged_and_file_moves_keep_provenance():
    version = {'size': 123, 'inode': 1}
    item = {'fileVersion': version, 'observationVersion': version, 'categories': {'bed': {}}}
    rows = samples(failures=())
    before = build_category_result(rows, interval=30, duration=120, positive_labels={'target'})
    valid, _, _, exclusions = partition_category_observations(item, 'bed', rows, 120, 'model')
    assert before == build_category_result(valid, interval=30, duration=120,
        positive_labels={'target'}, exclusion_intervals=exclusions)
    assert not item.get('partialRecovery')
    partition_category_observations(item, 'bed', samples(), 120, 'model')
    item['categories']['bed']['modelFingerprint'] = 'model'
    item['fileVersion'] = {'size': 123, 'inode': 2}  # verified move retains observationVersion
    assert category_exclusions(item, 'bed')


@pytest.mark.asyncio
async def test_shared_physical_file_only_excludes_bed_regions(tmp_path, monkeypatch):
    app = create_app(data_dir=tmp_path / 'db'); manager = app.state.slicing
    path = tmp_path / 'shared.mp4'; path.write_bytes(b'shared-media')
    bed = sliced(app, path, 'bed', '27912')
    magnet = sliced(app, path, 'magnet', '27944')
    for item in [bed, magnet]:
        item['metadata'] = {'duration': 120, 'frameRate': 25}
        manager.store.save('items', item)
    manager.store.settings['targetDatasetIds'] = {'27912': '27892', '27944': '27893'}
    manager.control.refresh()
    monkeypatch.setattr(manager, '_model_fingerprint', lambda name: name)
    for name in ['bed', 'magnet']:
        for row in samples(failures=(30,) if name == 'bed' else ()):
            key = manager._observation_key(bed, name, row['time'], name)
            manager.store.save('observations', {'id': key, **row})
    try:
        await manager._process_shared(manager.store.get('items', 'bed'), manager.store.list('items'))
        a, b = (manager.store.get('items', name) for name in ['bed', 'magnet'])
        assert a['categories']['bed']['state'] == 'ready'
        assert a['categories']['bed']['skippedAlignmentSamples'] == 1
        assert b['categories']['magnet']['state'] == 'ready'
        assert category_exclusions(b, 'magnet') == []
        assert not (b.get('decodeCoverage') or {}).get('exclusionIntervals')
    finally:
        await close_app(app)


@pytest.mark.asyncio
@pytest.mark.parametrize('code', ['bed_alignment_uncertain', 'bed_resolution', 'model_load_failed'])
async def test_cached_retry_reuses_completed_category_and_never_redownloads(tmp_path, monkeypatch, code):
    app = create_app(data_dir=tmp_path / 'db'); manager = app.state.slicing
    prepare_run(manager, monkeypatch)
    manager.control = None
    item = item_fixture(tmp_path)
    item.update(collectionRecovery=True, errorCode=code)
    item['metadata']['duration'] = 120
    item['categories']['bed'].update(errorCode=code)
    item['categories']['magnet'] = {'state': 'complete', 'packageId': 'already-imported',
                                   'actualImages': 150, 'importJobId': 'verified-job'}
    manager.store.save('items', item)
    before = copy.deepcopy(item['categories']['magnet'])
    monkeypatch.setattr(manager, '_model_fingerprint', lambda name: 'model-' + name)
    rows = samples(code=code)
    for row in rows:
        key = manager._observation_key(item, 'bed', row['time'], 'model-bed')
        manager.store.save('observations', {'id': key, **row})
    async def unexpected(*args, **kwargs):
        pytest.fail('All observations exist: no decoding or model work is needed')
    monkeypatch.setattr(manager, '_run_worker', unexpected)
    try:
        manager.retry(item['id'], item['revision'], 'resume-bed')
        selected = manager.store.get('items', item['id'])
        await manager._analyze_item(selected)
        bed = selected['categories']['bed']
        assert selected['categories']['magnet'] == before
        assert manager.remote.created == 0
        assert len(manager.store.list('observations')) == len(rows)
        if code == 'bed_alignment_uncertain':
            assert bed['state'] == 'ready' and bed['errorCode'] == ''
            assert bed['skippedAlignmentSamples'] == bed['skippedSamples'] == 1
            assert bed['observations'] == len(rows) - 1
            seen = []
            manager.slice_reader = lambda path, tool, stamp: seen.append(stamp) or b'\xff\xd8photo\xff\xd9'
            manager.store.settings['sliceOutputDirectory'] = str(tmp_path / 'photos')
            # Deliberately expand the saved window: export must still enforce
            # the rejected region independently of interval generation.
            selected['categories']['bed'].update(intervals=[{'start': 0, 'end': 120}], intervalSeconds=10)
            manager.store.save('items', selected)
            await manager._export_category_owned(selected, 'bed')
            assert seen and all(not position_excluded(t, category_exclusions(selected, 'bed')) for t in seen)
            package = manager.packages.store.get('items', selected['categories']['bed']['packageId'])
            assert package['imageCount'] == len(seen)
            await manager._export_category_owned(selected, 'bed')
            assert len(manager.packages.store.list('items')) == 1
            assert selected['categories']['magnet'] == before
        else:
            assert bed['state'] == 'attention' and bed['errorCode'] == code
    finally:
        await close_app(app)


@pytest.mark.asyncio
@pytest.mark.parametrize('all_rejected', [False, True])
async def test_no_target_requires_reliable_frames_not_only_alignment_failures(tmp_path, monkeypatch, all_rejected):
    app = create_app(data_dir=tmp_path / 'db'); manager = app.state.slicing
    item = item_fixture(tmp_path, state='pending'); item['metadata']['duration'] = 120
    item['categories']['bed']['errorCode'] = ''
    manager.store.settings['sharedSampling'] = False
    monkeypatch.setattr(manager, '_model_fingerprint', lambda name: 'model-' + name)
    failures = sample_points(120, 30) if all_rejected else (30,)
    for row in samples(failures=failures, label='clear'):
        key = manager._observation_key(item, 'bed', row['time'], 'model-bed')
        manager.store.save('observations', {'id': key, **row})
    manager.store.save('items', item)
    try:
        await manager._analyze_item(item)
        if all_rejected:
            assert item['categories']['bed']['state'] == 'attention'
            assert not verified_empty_conclusion(item, 'bed')
        else:
            assert item['categories']['bed']['state'] == 'no_target'
            assert verified_empty_conclusion(item, 'bed')
            assert item['categories']['bed']['processingConclusion']['skippedAlignmentSamples'] == 1
    finally:
        await close_app(app)


@pytest.mark.asyncio
async def test_current_recognition_issue_replaces_old_decode_warning_then_clears(env):
    app, row = env
    current, item, operation = confirmed_photos(app, row, state='complete')
    item['categories']['bed']['state'] = 'complete'
    item['categories']['magnet'].update(state='attention', errorCode='model_load_failed', message='模型加载失败。')
    item.update(state='attention', errorCode='model_load_failed', message='模型加载失败。')
    app.state.slicing.store.save('items', item)
    operation.update(state='attention', stage='recognition_queue',
                     issue={'code': 'invalid_video', 'message': '原片末段损坏'}, message='旧错误')
    app.state.final_processing.save(operation)
    shown = app.state.final_processing.presentations()[0]
    assert shown['issue']['code'] == 'model_load_failed'
    assert shown['stageLabel'] == '识别需核对' and shown['message'] == '模型加载失败。'
    await app.state.final_processing.advance(operation)
    assert app.state.final_processing.get(operation['id'])['issue']['code'] == 'model_load_failed'
    # A previous alignment warning must disappear as soon as that exact
    # category is resumed, without waiting for another long platform poll.
    operation.update(state='attention', stage='recognition_queue',
                     issue={'code': 'bed_alignment_uncertain', 'message': '旧定位错误'})
    app.state.final_processing.save(operation)
    item['categories']['magnet'].update(state='ready', errorCode='', message='正常片段准备导出。')
    item.update(state='platform_processing', errorCode='', message='准备导出')
    app.state.slicing.store.save('items', item)
    shown = app.state.final_processing.presentations()[0]
    assert shown['issue'] is None and shown['stageLabel'] == '照片导出'
    assert not shown['terminal']


@pytest.mark.asyncio
@pytest.mark.parametrize('case', ['mixed', 'all_rejected', 'user_paused', 'existing_bed_package'])
async def test_approved_legacy_alignment_recovery_reuses_only_safe_current_category(env, monkeypatch, case):
    app, row = env
    manager = app.state.slicing
    flow = app.state.final_processing
    _, item, operation = confirmed_photos(app, row, state='complete')
    item.update(state='attention', errorCode='bed_alignment_uncertain', localWorkspace=True, sourceVideoId='',
                settingsRevision=manager.store.settings['revision'], finalProcessingIntent=operation['id'],
                metadata={'duration': 120, 'frameRate': 25})
    item['categories'] = {
        'bed': {'state': 'attention', 'errorCode': 'bed_alignment_uncertain', 'intervals': [],
                'samplingIntervalSeconds': 30, 'modelFingerprint': 'cached-bed'},
        'magnet': {'state': 'complete', 'packageId': 'exact-package', 'actualImages': 61},
    }
    if case == 'user_paused':
        item['pipelinePaused'] = True
    manager.store.save('items', item)
    packages = manager.packages.store
    magnet = packages.get('items', 'exact-package')
    magnet.update(category='magnet', jobId='magnet-job', verifiedJobId='magnet-job', actualImages=61,
                  outputImageIds=[str(i+1) for i in range(61)])
    packages.save('items', magnet)
    magnet = packages.get('items', 'exact-package')
    if case == 'existing_bed_package':
        packages.save('items', {'id': 'old-bed', 'itemId': item['id'], 'category': 'bed', 'state': 'reconciling'})
    for observation in samples(failures=sample_points(120, 30) if case == 'all_rejected' else (30,)):
        key = manager._observation_key(item, 'bed', observation['time'], 'cached-bed')
        manager.store.save('observations', {'id': key, **observation})
    monkeypatch.setattr(manager, '_wake_scheduler', lambda: None)
    monkeypatch.setattr(manager, '_model_fingerprint', lambda name: 'cached-bed'
                        if name == 'bed' else pytest.fail('The verified magnet must not run again.'))
    async def unexpected_worker(*args):
        pytest.fail('The complete observation grid should be reused.')
    monkeypatch.setattr(manager, '_run_worker', unexpected_worker)

    await flow.advance(operation)
    queued = manager.store.get('items', item['id'])
    if case != 'mixed':
        assert queued['state'] == 'attention' and not queued.get('legacyAlignmentRepair')
        assert app.state.videos.state({'status': 'review'})['videos'][0]['id'] == row['id']
        return
    assert queued['categories']['bed']['state'] == 'pending'
    assert flow.presentations()[0]['issue'] is None
    assert item['id'] in manager.run['items']
    assert app.state.videos.state({'status': 'processing'})['videos'][0]['id'] == row['id']
    # The durable pending item survives a stop between recording and scheduling.
    manager.store.settings.update(enabled=False, latestRunId='')
    manager.store.save_settings()
    manager.run = None
    await flow.advance(operation)
    assert item['id'] in manager.run['items']
    await manager._analyze_item(manager.store.get('items', item['id']))
    resumed = manager.store.get('items', item['id'])
    assert resumed['categories']['bed']['state'] == 'ready'
    assert resumed['categories']['bed']['skippedAlignmentSamples'] == 1
    assert resumed['categories']['magnet'] == item['categories']['magnet']
    assert packages.get('items', 'exact-package') == magnet
    await flow.advance(operation)
    assert manager.store.get('items', item['id'])['revision'] == resumed['revision']


@pytest.mark.asyncio
@pytest.mark.parametrize('already_queued', [False, True])
async def test_single_retry_enters_worker_even_when_last_local_batch_excludes_it(tmp_path, monkeypatch, already_queued):
    app = create_app(data_dir=tmp_path / 'db'); manager = app.state.slicing
    prepare_run(manager, monkeypatch)
    manager.control = None
    manager.files = None
    selected = item_fixture(tmp_path, state='pending' if already_queued else 'attention')
    selected['collectionRecovery'] = True
    other = item_fixture(tmp_path, 'other', state='pending')
    for row in (selected, other):
        manager.store.save('items', row)
    manager.run.update(localRecoveryOnly=True, items=['other'], singleItemId=selected['id'])
    manager.store.save('runs', manager.run)
    started = []
    release = asyncio.Event()
    async def process(row):
        started.append(row['id'])
        await release.wait()
    monkeypatch.setattr(manager, 'process_with_recovery', process)
    try:
        if already_queued:
            # The old code could persist a retry then disable the scheduler
            # because the latest local run did not include this selected video.
            manager.resume('resume-existing-request')
        else:
            manager.retry(selected['id'], selected['revision'], 'retry-current')
            assert selected['id'] in manager.run['items']
        await manager._tick()
        await asyncio.sleep(0)
        assert started == [selected['id']]
        assert manager.store.settings['enabled']
        assert manager.run['state'] == 'running'
        assert manager.store.get('items', 'other')['state'] == 'pending'
        assert manager.remote.created == 0
    finally:
        release.set()
        await asyncio.gather(*list(manager.analysis_tasks.values()), return_exceptions=True)
        await close_app(app)


def test_category_retry_status_uses_current_final_processing_not_old_media_failure():
    row = {'activeRevision': 2, 'revision': 4,
           'versions': [{'revision': 2, 'state': 'failed',
                         'recovery': {'state': 'attention', 'slicingItemId': 'bound'}}],
           'finalProcessing': {'state': 'queued', 'version': 2, 'expectedRevision': 4}}
    for state in ['queued', 'running', 'waiting']:
        row['finalProcessing']['state'] = state
        assert record_status(row) == 'processing'
    assert row['versions'][0]['state'] == 'failed'
    for change in [{'state': 'attention'}, {'state': 'paused'}, {'version': 1},
                   {'expectedRevision': 3}, {'manualPaused': True}]:
        changed = copy.deepcopy(row)
        changed['finalProcessing'].update(change)
        assert record_status(changed) == 'review'
    row['versions'][0]['recovery'].pop('slicingItemId')
    assert record_status(row) == 'review'
