import asyncio
import copy
import sqlite3

import pytest

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
        remaining=manager.review.get(identity)
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
