import asyncio
import copy
import sqlite3

import httpx
import pytest

from app.domain import gather_payload
from app.models import AdmissionRule
from app.videos import Conflict
from tests.test_collection_runs import make
from tests.test_simple_flow import app_factory, platform
from tests.test_slicing_performance import close_app


async def queried(app, count=1):
    service, source = make(app)
    source.count = count
    run=service.start({'dateFrom':'2026-09-30','dateTo':'2026-09-30','requestId':'query'})
    await service.tick()
    assert not app.state.jobs.store.list()
    assert service.get(run['id'])['status']=='awaiting_review'
    return service, run['id']


@pytest.mark.asyncio
async def test_query_edit_and_confirm_only_selected_items(app_factory, platform):
    app=app_factory()
    try:
        manager, identity=await queried(app,25)
        before=manager.get(identity)['records'][0].copy()
        plan=manager.review.get(identity)
        assert len(plan['tasks'])==20 and plan['pages']==2 and len(plan['readyIds'])==25
        task=plan['tasks'][0]
        updated=manager.review.edit(identity,{'recordId':task['recordId'],'startTime':'2026-09-30 03:00:01',
            'durationSeconds':123,'expectedPlanRevision':plan['planRevision'],'requestId':'edit','page':1})
        assert manager.get(identity)['records'][0]==before
        assert updated['tasks'][0]['endTime']=='2026-09-30 03:02:04'
        assert updated['tasks'][1]['startTime']==plan['tasks'][1]['startTime']
        payload={'recordIds':[task['recordId']],'expectedPlanRevision':updated['planRevision'],'requestId':'approve'}
        operation=manager.review.submit(identity,payload)
        assert operation==manager.review.submit(identity,payload)
        await manager.tick()
        await asyncio.gather(*tuple(app.state.jobs.running.values()))
        await app.state.videos.check_once()
        await manager.tick()
        assert platform.batch_sizes==[1]
        assert manager.review.operation(operation['operationId'])['state']=='complete'
        assert manager.get(identity)['status']=='awaiting_review'
        assert len(manager.review.get(identity)['readyIds'])==24
        row=app.state.videos.store.list()[0]
        assert row['versions'][0]['startTime']=='2026-09-30 03:00:01'
        assert row['versions'][0]['durationSeconds']==123
        assert row['acceptedRevision'] is None
        remaining=manager.review.get(identity, category='ready')
        next_task = remaining['tasks'][0]
        remaining = manager.review.edit(identity, {'recordId': next_task['recordId'],
            'startTime': next_task['startTime'], 'durationSeconds': 100,
            'expectedPlanRevision': remaining['planRevision'], 'requestId': 'filtered-edit'}, category='ready')
        assert remaining['total'] == 24 and all(task['status'] == 'ready' for task in remaining['tasks'])
        manager.review.submit(identity,{'recordIds':[remaining['readyIds'][0]],'expectedPlanRevision':remaining['planRevision'],'requestId':'second-selection'})
        await manager.tick();await asyncio.gather(*tuple(app.state.jobs.running.values()))
        await app.state.videos.check_once();await manager.tick()
        assert manager.get(identity)['counts']['submitted']==2 and manager.get(identity)['counts']['ready']==2
    finally:await close_app(app)


@pytest.mark.asyncio
async def test_plan_conflicts_invalid_selection_and_rule_change_never_submit(app_factory,platform):
    app=app_factory()
    try:
        manager,identity=await queried(app,2)
        plan=manager.review.get(identity)
        base={'recordIds':plan['readyIds'],'expectedPlanRevision':plan['planRevision'],'requestId':'approve'}
        with pytest.raises(Conflict):manager.review.submit(identity,{**base,'expectedPlanRevision':999})
        with pytest.raises(Conflict):manager.review.submit(identity,{**base,'recordIds':['unknown']})
        manager.admission_settings.save(AdmissionRule(minInspectionMinutes=20),0)
        with pytest.raises(Conflict,match='准入'):manager.review.submit(identity,base)
        assert not platform.batch_sizes and not app.state.jobs.store.list()
    finally:await close_app(app)


@pytest.mark.asyncio
async def test_confirm_intent_and_plan_are_atomic(app_factory,monkeypatch):
    app=app_factory()
    try:
        manager,identity=await queried(app)
        plan=manager.review.get(identity)
        payload={'recordIds':plan['readyIds'],'expectedPlanRevision':plan['planRevision'],'requestId':'atomic'}
        original=manager._save
        def fail(*args,**kwargs):raise sqlite3.OperationalError('injected write interruption')
        monkeypatch.setattr(manager,'_save',fail)
        with pytest.raises(sqlite3.OperationalError):manager.review.submit(identity,payload)
        assert not manager.db.execute('select count(*) from simple_collection_submissions').fetchone()[0]
        assert manager.get(identity)['planRevision']==plan['planRevision']
        monkeypatch.setattr(manager,'_save',original)
        assert manager.review.submit(identity,payload)['state']=='queued'
    finally:await close_app(app)


@pytest.mark.asyncio
async def test_two_previews_of_same_fact_do_not_create_two_captures(app_factory,platform):
    app=app_factory()
    try:
        manager,identity=await queried(app)
        second=manager.start({'dateFrom':'2026-09-30','dateTo':'2026-10-01','requestId':'overlap'})['id']
        await manager.tick()
        for key in (identity,second):
            plan=manager.review.get(key)
            manager.review.submit(key,{'recordIds':plan['readyIds'],'expectedPlanRevision':plan['planRevision'],'requestId':'approve-'+key})
        for _ in range(5):
            await manager.tick()
            await asyncio.gather(*tuple(app.state.jobs.running.values()))
            await app.state.videos.check_once()
        assert platform.batch_sizes==[1]
        assert manager.get(second)['status']=='complete'
    finally:await close_app(app)


@pytest.mark.asyncio
async def test_next_batch_reuses_capture_added_on_platform_after_first_write(app_factory, platform):
    app = app_factory()
    try:
        manager, identity = await queried(app, 21)
        run = manager.get(identity)
        plan = manager.review.get(identity)
        last = run['reviewTasks'][-1]

        def add_manual_capture(request):
            response = platform.handle(request)
            if request.url.path.endswith('/data-acquisition/batchSave') and len(platform.batch_sizes) == 1:
                platform.parents['manual'] = {**gather_payload(last, run['target']), 'id': 'manual', 'taskStatus': 'RUNNING'}
            return response

        app.state.videos.target.client._transport = httpx.MockTransport(add_manual_capture)
        manager.review.submit(identity, {'recordIds': plan['selectAllIds'], 'expectedPlanRevision': 0,
                                         'requestId': 'two-batches'})
        await manager.tick()
        await asyncio.gather(*tuple(app.state.jobs.running.values()))
        job = manager.store.get(manager.get(identity)['jobId'])
        assert platform.batch_sizes == [20]
        assert job['items'][-1]['status'] == 'duplicate' and job['items'][-1]['remoteId'] == 'manual'
    finally:
        await close_app(app)


@pytest.mark.asyncio
@pytest.mark.parametrize('scenario, submissions', [('ended', 2), ('active', 1), ('unbound', 1), ('unconfirmed', 2)])
async def test_reviewed_existing_capture_can_be_selected_without_blind_duplicates(app_factory, platform, scenario, submissions):
    app = app_factory()
    platform.new_tasks_waiting = scenario == 'active'
    try:
        manager, first = await queried(app)
        plan = manager.review.get(first)
        manager.review.submit(first, {'recordIds': plan['readyIds'], 'expectedPlanRevision': 0, 'requestId': 'original'})
        await manager.tick()
        await asyncio.gather(*tuple(app.state.jobs.running.values()))
        await app.state.videos.check_once()
        await manager.tick()
        original_id, = platform.parents
        if scenario == 'unbound':
            platform.parents['unbound'] = {**platform.parents[original_id], 'id': 'unbound'}
        elif scenario == 'unconfirmed':
            videos = app.state.videos
            video, = videos.store.list()

            def delayed_inventory(request):
                if request.url.path.endswith('/data-acquisition/condition-query'):
                    return platform.answer({'records': [platform.parents[original_id]], 'total': 1})
                return platform.handle(request)

            videos.target.client._transport = httpx.MockTransport(delayed_inventory)
            platform.fail_after_write = True
            repeat = await videos.recapture_unavailable(video['id'], {
                'version': 0, 'expectedRevision': 0, 'requestId': 'correction-in-flight', 'mode': 'force'})
            await asyncio.gather(*tuple(videos.operation_tasks.values()))
            assert videos.store.get_operation(repeat['operationId'])['state'] == 'reconciling'
            assert len(platform.parents) == 2 and not videos.store.get(video['id'])['versions'][1]['taskId']

        second = manager.start({'dateFrom': '2026-09-30', 'dateTo': '2026-09-30', 'requestId': 'repeat-query'})['id']
        await manager.tick()
        plan = manager.review.get(second, category='ready')
        record, = plan['tasks']
        assert record['status'] == 'duplicate'
        assert plan['selectableIds'] == plan['selectAllIds'] == [record['recordId']]
        data = {'recordIds': [record['recordId']], 'expectedPlanRevision': plan['planRevision'], 'requestId': 'confirmed-repeat'}
        operation = manager.review.submit(second, data)
        assert manager.review.submit(second, data) == operation
        await manager.tick()
        await asyncio.gather(*tuple(app.state.jobs.running.values()))
        await app.state.videos.check_once()
        await manager.tick()
        assert platform.batch_sizes == [1] * submissions
        assert original_id in platform.parents
        job = manager.store.get(manager.get(second)['jobId'])
        item, = job['items']
        if scenario == 'active':
            assert item['status'] == 'duplicate' and item['remoteId'] == original_id
            assert len(app.state.videos.store.list()) == 1
        elif scenario == 'unbound':
            assert item['status'] == 'failed' and '未核实归属' in item['message']
        elif scenario == 'unconfirmed':
            assert item['status'] == 'failed' and '提交结果未确认' in item['message']
            assert not item.get('submission')
            plan = manager.review.get(second, category='ready')
            assert plan['selectableIds'] == [record['recordId']]
            assert plan['tasks'][0]['status'] == 'duplicate' and '提交结果未确认' in plan['tasks'][0]['reason']
            videos.target.client._transport = httpx.MockTransport(platform.handle)
            await videos.resume_operations()
            await asyncio.gather(*tuple(videos.operation_tasks.values()))
            assert videos.store.get_operation(repeat['operationId'])['state'] == 'complete'
            manager.review.submit(second, {**data, 'expectedPlanRevision': plan['planRevision'], 'requestId': 'retry-repeat'})
            await manager.tick()
            await asyncio.gather(*tuple(app.state.jobs.running.values()))
            assert platform.batch_sizes == [1, 1, 1]
        else:
            assert item['status'] == 'submitted' and item['remoteId'] != original_id
            videos = app.state.videos.store.list()
            assert len(videos) == 2
            assert manager.review.get(second)['tasks'][0]['existingVideoId'] == next(
                video['id'] for video in videos if video['jobId'] == job['id'])
    finally:
        await close_app(app)


@pytest.mark.asyncio
async def test_awaiting_review_survives_restart_with_draft(app_factory):
    app=app_factory()
    try:
        manager,identity=await queried(app)
        plan=manager.review.get(identity)
        manager.review.edit(identity,{'recordId':plan['readyIds'][0],'startTime':'2026-10-01 00:00:00',
            'durationSeconds':60,'expectedPlanRevision':0,'requestId':'keep'})
        from app.collection_runs import CollectionRunManager
        reopened=CollectionRunManager(manager.store,manager.source,manager.jobs,manager.videos,manager.admission_settings,manager.target_config)
        await reopened.tick()
        assert reopened.review.get(identity)['tasks'][0]['durationSeconds']==60
        assert reopened.get(identity)['status']=='awaiting_review'
        assert not app.state.jobs.store.list()
    finally:await close_app(app)


@pytest.mark.asyncio
async def test_submission_operation_reports_connection_wait(app_factory):
    app=app_factory()
    try:
        manager,identity=await queried(app)
        plan=manager.review.get(identity)
        operation=manager.review.submit(identity,{'recordIds':plan['readyIds'],'expectedPlanRevision':0,'requestId':'offline-submit'})
        manager.jobs.client.connected=False
        await manager.tick()
        state=manager.review.operation(operation['operationId'])
        assert state['state']=='waiting_connection' and not app.state.jobs.store.list()
    finally:await close_app(app)


@pytest.mark.asyncio
@pytest.mark.parametrize('submitted_first',[False,True])
async def test_upgrade_preserves_emitted_items_and_requires_approval_for_unsent(app_factory,platform,submitted_first):
    app=app_factory(); reopened=None
    try:
        manager,identity=await queried(app,2)
        old=manager.get(identity)
        initial=copy.deepcopy(old['plan'])
        if submitted_first:initial['tasks']=initial['tasks'][:1]
        job=app.state.jobs.start(initial)
        if submitted_first:
            await asyncio.gather(*tuple(app.state.jobs.running.values()))
            job=app.state.jobs.store.get(job['id'])
            job['items'].append({**copy.deepcopy(old['plan']['tasks'][1]),'status':'pending'})
            job['status']='running';app.state.jobs.store.save(job)
        else:
            for task in tuple(app.state.jobs.running.values()):task.cancel()
            await asyncio.gather(*tuple(app.state.jobs.running.values()),return_exceptions=True)
        old.update(jobId=job['id'],status='submitting')
        for key in ('reviewVersion','reviewTasks','planRevision','jobIds'):old.pop(key,None)
        manager._save(old)
        previous=set(platform.parents)
        from app.collection_runs import CollectionRunManager
        reopened=CollectionRunManager(manager.store,manager.source,manager.jobs,manager.videos,manager.admission_settings,manager.target_config)
        await reopened.startup();await reopened.tick()
        plan=reopened.review.get(identity)
        assert len(plan['readyIds'])==(1 if submitted_first else 2)
        assert set(platform.parents)==previous
        saved=app.state.jobs.store.get(job['id'])
        assert saved['items'][-1]['status']=='awaiting_confirmation'
        if submitted_first:assert saved['items'][0]['remoteId'] in previous
    finally:
        if reopened:await reopened.shutdown()
        await close_app(app)
