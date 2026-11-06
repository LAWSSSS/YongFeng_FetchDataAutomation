"""A duplicate is a receipt to reconcile, never a permanently pending video."""
import asyncio
import copy

import pytest

from app.domain import gather_payload
from tests.test_collection_review import queried
from tests.test_simple_flow import app_factory, platform, wait_until
from tests.test_slicing_performance import close_app


def seed_capture(platform, task, target, identity='existing', status='FIXED', **changes):
    platform.parents[identity] = {**gather_payload(task, target), 'id': identity,
                                  'taskStatus': status, **changes}
    if status == 'FIXED':
        platform.results[identity] = [{'id': 'result-' + identity, 'taskId': identity, 'status': 'FINISH',
                                      'url': 'http://storage.test/video/fixture.mp4', 'beginTime': task['startTime']}]


async def submit_review(app, manager, identity):
    plan = manager.review.get(identity)
    manager.review.submit(identity, {'recordIds': plan['readyIds'],
        'expectedPlanRevision': plan['planRevision'], 'requestId': 'approve'})
    await manager.tick()
    await asyncio.gather(*tuple(app.state.jobs.running.values()))
    await app.state.videos.check_once()
    await manager.tick()
    return manager.get(identity)


@pytest.mark.asyncio
@pytest.mark.parametrize('waiting', [False, True])
async def test_existing_capture_becomes_viewable_without_submit_or_download(app_factory, platform, waiting):
    app = app_factory()
    try:
        manager, identity = await queried(app)
        run = manager.get(identity)
        seed_capture(platform, run['reviewTasks'][0], run['target'], status='RUNNING' if waiting else 'FIXED')
        result = await submit_review(app, manager, identity)
        video = app.state.videos.state()['videos'][0]
        assert video['versions'][0]['taskId'] == 'existing'
        assert video['versions'][0]['state'] == ('waiting' if waiting else 'ready')
        assert result['counts']['reused'] == 1
        assert result['counts']['waiting' if waiting else 'ready'] == 1
        assert result['status'] == ('waiting' if waiting else 'complete')
        assert not platform.batch_sizes and not platform.download_calls
        assert video['acceptedRevision'] is None and not video['versions'][0].get('downloadRequested')
        assert video['capabilities']['canForceRecapture']
        old_run = manager.get(identity)
        old_run.update(status='complete', stage='confirm', pendingSubmissionId='', message='0 条采集已可查看')
        old_run['counts'].update(ready=0, waiting=0)
        presented = manager.public(old_run)
        assert presented['status'] == ('waiting' if waiting else 'complete')
        assert presented['counts']['waiting' if waiting else 'ready'] == 1
        assert old_run['counts']['ready'] == old_run['counts']['waiting'] == 0
        if waiting:
            seed_capture(platform, run['reviewTasks'][0], run['target'])
            app.state.videos.inventory_cached_at = 0
            await app.state.videos.check_once()
            await manager.tick()
            assert manager.get(identity)['status'] == 'complete'
        next_run = manager.start({'dateFrom': '2026-09-30', 'dateTo': '2026-10-01', 'requestId': 'again'})
        await manager.tick()
        assert not manager.review.get(next_run['id'])['readyIds']
    finally:
        await close_app(app)


@pytest.mark.asyncio
@pytest.mark.parametrize('problem', ['ambiguous', 'name', 'embed', 'repeat'])
async def test_duplicate_conflicts_are_visible_and_never_arbitrarily_adopted(app_factory, platform, problem):
    app = app_factory()
    try:
        manager, identity = await queried(app)
        run = manager.get(identity)
        task = run['reviewTasks'][0]
        changes = {'name': {'videoName': 'OTHER'}, 'embed': {'embedId': 'OTHER'},
                   'repeat': {'isRepeat': True}}.get(problem, {})
        seed_capture(platform, task, run['target'], **changes)
        if problem == 'ambiguous':
            seed_capture(platform, task, run['target'], identity='another')
        result = await submit_review(app, manager, identity)
        video = app.state.videos.state()['videos'][0]
        assert result['status'] == 'attention' and result['counts']['attention'] == 1
        assert video['versions'][0]['state'] == 'attention'
        assert not video['versions'][0]['taskId']
        assert video['name'] == task['videoName']
        assert not platform.batch_sizes and not platform.download_calls
    finally:
        await close_app(app)


@pytest.mark.asyncio
@pytest.mark.parametrize('corrected', [False, True])
async def test_legacy_pending_duplicate_repaired_without_losing_correction(app_factory, platform, corrected):
    app = app_factory()
    try:
        manager, identity = await queried(app)
        await submit_review(app, manager, identity)
        videos = app.state.videos
        row = videos.store.list()[0]
        job = app.state.jobs.store.get(row['jobId'])
        original = copy.deepcopy(row['versions'][0])
        job['items'][0].update(status='duplicate', duplicateSource='remote', bindingVerified=False)
        job['items'][0].pop('submission', None)
        job['items'][0].pop('bindingSource', None)
        app.state.jobs.store.save(job)
        row['versions'][0].update(state='pending', taskId='', resultId='')
        if corrected:
            row.update(activeRevision=1, revision=1)
            row['versions'].append({**original, 'revision': 1})
            row['requests']['manual-edit'] = {'kind': 'correction', 'version': 1}
        videos.store.save(row)
        await videos.check_once()
        restored = videos.store.get(row['id'])
        assert restored['versions'][0]['taskId'] == original['taskId']
        assert restored['activeRevision'] == int(corrected)
        if corrected:
            assert restored['requests']['manual-edit']['version'] == 1
            assert restored['versions'][1]['resultId'] == original['resultId']
        else:
            assert restored['versions'][0]['state'] == 'ready'
        assert platform.batch_sizes == [1]
    finally:
        await close_app(app)


@pytest.mark.asyncio
async def test_duplicate_uses_one_control_record_and_preserves_hidden_state(app_factory, platform):
    app = app_factory()
    try:
        manager, identity = await queried(app)
        await submit_review(app, manager, identity)
        videos = app.state.videos
        original = videos.store.list()[0]
        # Same-interval corrections legitimately leave several platform tasks;
        # the already registered local video remains their sole control record.
        old_parent = next(iter(platform.parents.values()))
        platform.parents['same-window-history'] = {**old_parent, 'id': 'same-window-history'}
        job = copy.deepcopy(app.state.jobs.store.get(original['jobId']))
        job.update(id='duplicate-race', planId='duplicate-race', status='running')
        item = job['items'][0]
        for key in ('submission', 'remoteId', 'bindingVerified', 'bindingSource'):
            item.pop(key, None)
        item['status'] = 'pending'
        app.state.jobs.store.save(job)
        videos.sync_jobs()  # The old UI could already have created a pending placeholder.
        placeholder = videos.job_video_id(job, item)
        await app.state.jobs.execute(job)
        videos.sync_jobs()
        assert videos.store.get(placeholder)['duplicateOfVideoId'] == original['id']
        assert [row['id'] for row in videos.state()['videos']] == [original['id']]
        assert videos.state(identity=placeholder)['videos'][0]['id'] == original['id']
        assert manager.review.get(identity)['tasks'][0]['existingVideoId'] == original['id']
        videos.record_visibility.hide(videos.store, 'hide-original', [{'id': original['id'], 'expectedRevision': original['revision']}])
        videos.sync_jobs()
        assert not videos.state()['videos']
        assert not videos.state(identity=placeholder)['videos']
        assert not videos.state(identity=original['id'])['videos']
        assert original['id'] in videos.record_visibility.hidden()
        assert platform.batch_sizes == [1]
    finally:
        await close_app(app)


@pytest.mark.asyncio
async def test_uncertain_write_cannot_adopt_preexisting_duplicate(app_factory, platform):
    app = app_factory()
    try:
        manager, identity = await queried(app)
        run = manager.get(identity)
        item = copy.deepcopy(run['reviewTasks'][0])
        seed_capture(platform, item, run['target'])
        item.update(status='duplicate', submission={'snapshotVersion': 1, 'beforeIds': ['existing'],
            'cameraId': run['target']['cameraId'], 'startTime': item['startTime'], 'endTime': item['endTime'],
            'videoName': item['videoName'], 'submittedAt': '2026-11-04T00:00:00+00:00'})
        result, _ = app.state.jobs.bind_existing_item(run['target'], item, platform.parents)
        assert result is None and item['bindingIssue'] == 'submission_missing'
        assert not item.get('remoteId') and not platform.batch_sizes
    finally:
        await close_app(app)


@pytest.mark.asyncio
async def test_legacy_duplicate_missing_bound_id_never_switches_to_other_match(app_factory, platform):
    app = app_factory()
    try:
        manager, identity = await queried(app)
        run = manager.get(identity)
        item = copy.deepcopy(run['reviewTasks'][0])
        seed_capture(platform, item, run['target'])
        item.update(status='duplicate', duplicateSource='remote', remoteId='missing')
        result, _ = app.state.jobs.bind_existing_item(run['target'], item, platform.parents)
        assert result is None and item['bindingIssue'] == 'bound_identity_missing'
        assert item['remoteId'] == 'missing'
    finally:
        await close_app(app)


@pytest.mark.asyncio
async def test_single_check_repairs_only_its_duplicate_without_resubmitting(app_factory, platform):
    app = app_factory()
    try:
        manager, identity = await queried(app)
        run = manager.get(identity)
        seed_capture(platform, run['reviewTasks'][0], run['target'])
        seed_capture(platform, run['reviewTasks'][0], run['target'], identity='another')
        await submit_review(app, manager, identity)
        videos = app.state.videos
        row = videos.store.list()[0]
        assert row['versions'][0]['state'] == 'attention'
        del platform.parents['another']
        operation = videos.queue_video_operation(row['id'], 'check', {'expectedRevision': 0, 'requestId': 'check-one'})
        await wait_until(lambda: videos.store.get_operation(operation['operationId'])['state'] in {'complete', 'attention', 'conflict'})
        assert videos.store.get_operation(operation['operationId'])['state'] == 'complete'
        assert videos.store.get(row['id'])['versions'][0]['state'] == 'ready'
        assert not platform.batch_sizes
    finally:
        await close_app(app)


@pytest.mark.asyncio
async def test_verified_existing_capture_can_be_explicitly_recaptured_once(app_factory, platform):
    app = app_factory()
    try:
        manager, identity = await queried(app)
        run = manager.get(identity)
        seed_capture(platform, run['reviewTasks'][0], run['target'])
        await submit_review(app, manager, identity)
        videos = app.state.videos
        row = videos.store.list()[0]
        request = {'version': 0, 'expectedRevision': 0, 'requestId': 'force', 'mode': 'force'}
        operation = await videos.recapture_unavailable(row['id'], request)
        replay = await videos.recapture_unavailable(row['id'], request)
        assert operation['operationId'] == replay['operationId']
        await wait_until(lambda: videos.store.get_operation(operation['operationId'])['state'] == 'complete')
        updated = videos.store.get(row['id'])
        assert updated['activeRevision'] == 1 and len(updated['versions']) == 2
        assert updated['versions'][0]['taskId'] == 'existing'
        assert 'existing' in platform.parents and platform.batch_sizes == [1]
    finally:
        await close_app(app)
