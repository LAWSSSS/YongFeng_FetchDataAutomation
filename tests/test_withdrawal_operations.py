import asyncio
import copy
import time
import sqlite3

import httpx
import pytest

from app.video_withdrawal import cleanup_checkpoint
from scripts.user_rehearsal import create_rehearsal
from tests.test_simple_flow import app_factory, platform, new_video, wait_video_operation
from tests.test_slicing_performance import close_app


async def correction(app,client):
    row=await new_video(client,app)
    response=await client.post(f'/api/videos/{row["id"]}/corrections',json={'startTime':'2026-09-30 02:30:00',
        'durationSeconds':120,'expectedRevision':0,'requestId':'correct'})
    assert response.status_code==202,response.text
    assert (await wait_video_operation(app,response))['state']=='complete'
    await app.state.videos.check_once()
    return app.state.videos.store.get(row['id'])


@pytest.mark.asyncio
@pytest.mark.parametrize('lost',['','acInfo','object','task'])
async def test_withdrawal_reconciles_exact_cleanup_and_preserves_previous(app_factory,platform,lost):
    app=app_factory()
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url='http://127.0.0.1') as client:
            row=await correction(app,client)
            previous=row['versions'][0]['taskId']; removed=row['versions'][1]['taskId']
            platform.lose_delete_response=lost
            data={'version':1,'expectedRevision':1,'requestId':'withdraw'}
            response=await client.post(f'/api/videos/{row["id"]}/withdraw',json=data)
            assert response.status_code==202 and response.json().get('operationId')
            await asyncio.gather(*tuple(app.state.videos.operation_tasks.values()))
            op=app.state.videos.store.get_operation(response.json()['operationId'])
            if op['state'] in {'reconciling','waiting_connection'}:
                op['nextAttemptAt']=0;app.state.videos.store.save_operation(op)
                await app.state.videos.resume_operations()
                await asyncio.gather(*tuple(app.state.videos.operation_tasks.values()))
            op=app.state.videos.store.get_operation(op['id'])
            assert op['state']=='complete',op
            assert previous in platform.parents and removed not in platform.parents
            assert app.state.videos.store.get(row['id'])['activeRevision']==0
            replay=await client.post(f'/api/videos/{row["id"]}/withdraw',json=data)
            assert replay.json()['operationId']==op['id']
    finally:await close_app(app)


@pytest.mark.asyncio
async def test_withdraw_registration_does_not_wait_for_platform_lock(app_factory):
    app=app_factory()
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url='http://127.0.0.1') as client:
            row=await correction(app,client)
            async with app.state.jobs.lock:
                started=time.monotonic()
                response=await client.post(f'/api/videos/{row["id"]}/withdraw',json={'version':1,'expectedRevision':1,'requestId':'fast'})
                assert response.status_code==202 and time.monotonic()-started<1
            assert (await wait_video_operation(app,response))['state']=='complete'
    finally:await close_app(app)


@pytest.mark.asyncio
async def test_restart_resumes_the_same_withdrawal_without_new_capture(app_factory,platform):
    app=app_factory()
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url='http://127.0.0.1') as client:
        row=await correction(app,client)
        previous=row['versions'][0]['taskId']
        await app.state.jobs.lock.acquire()
        response=await client.post(f'/api/videos/{row["id"]}/withdraw',json={'version':1,'expectedRevision':1,'requestId':'restart-withdraw'})
        await asyncio.sleep(0)
        await app.state.videos.shutdown()
        app.state.jobs.lock.release()
    operation_id=response.json()['operationId']
    assert app.state.videos.store.get_operation(operation_id)['state']=='queued'
    await close_app(app)
    reopened=app_factory()
    async with reopened.router.lifespan_context(reopened):
        from tests.test_simple_flow import wait_until
        await wait_until(lambda:reopened.state.videos.store.get_operation(operation_id)['state']=='complete')
        assert reopened.state.videos.store.get(row['id'])['activeRevision']==0
        assert previous in platform.parents and platform.batch_sizes==[1,1]


@pytest.mark.asyncio
async def test_final_confirmed_version_cannot_be_withdrawn_or_preempted(app_factory):
    app=app_factory()
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url='http://127.0.0.1') as client:
            row=await correction(app,client)
            row['versions'][-1]['finalConfirmation']={'operationId':'confirmed'}
            app.state.videos.store.save(row)
            generation=(app.state.media_control.get('collection:'+row['id']) or {})['generation']
            response=await client.post(f'/api/videos/{row["id"]}/withdraw',json={'version':1,'expectedRevision':1,'requestId':'no'})
            assert response.status_code==409
            assert app.state.media_control.get('collection:'+row['id'])['generation']==generation
    finally:await close_app(app)


@pytest.mark.asyncio
async def test_withdraw_intent_and_generation_commit_together(app_factory,monkeypatch):
    app=app_factory()
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url='http://127.0.0.1') as client:
            row=await correction(app,client)
            manager=app.state.videos
            before=app.state.media_control.get('collection:'+row['id'])['generation']
            original=manager.store.save_operation
            def interrupt(*args,**kwargs):raise sqlite3.OperationalError('injected interruption')
            monkeypatch.setattr(manager.store,'save_operation',interrupt)
            with pytest.raises(sqlite3.OperationalError):
                await manager.withdraw(row['id'],{'version':1,'expectedRevision':1,'requestId':'atomic-withdraw'})
            assert app.state.media_control.get('collection:'+row['id'])['generation']==before
            monkeypatch.setattr(manager.store,'save_operation',original)
            response=await client.post(f'/api/videos/{row["id"]}/withdraw',json={'version':1,'expectedRevision':1,'requestId':'atomic-withdraw'})
            assert (await wait_video_operation(app,response))['state']=='complete'
    finally:await close_app(app)


@pytest.mark.asyncio
async def test_known_rejection_can_withdraw_but_unknown_write_waits(app_factory):
    app=app_factory()
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url='http://127.0.0.1') as client:
            row=await new_video(client,app)
            row['versions'].append({**copy.deepcopy(row['versions'][0]),'revision':1,'taskId':'','resultId':'',
                'state':'uncertain','beforeIds':[row['versions'][0]['taskId']],'writeAttempted':True,'previousActiveRevision':0})
            row.update(revision=1,activeRevision=1);app.state.videos.store.save(row)
            response=await client.post(f'/api/videos/{row["id"]}/withdraw',json={'version':1,'expectedRevision':1,'requestId':'wait'})
            await asyncio.gather(*tuple(app.state.videos.operation_tasks.values()))
            op=app.state.videos.store.get_operation(response.json()['operationId'])
            assert op['state']=='reconciling' and op['stage']=='waiting_binding'
            assert app.state.videos.store.get(row['id'])['activeRevision']==1
            # An explicit, durable rejection is different from an absent task.
            row=app.state.videos.store.get(row['id']);row['versions'][1]['submissionOutcome']='rejected';app.state.videos.store.save(row)
            op['nextAttemptAt']=0;app.state.videos.store.save_operation(op)
            await app.state.videos.resume_operations();await asyncio.gather(*tuple(app.state.videos.operation_tasks.values()))
            assert app.state.videos.store.get_operation(op['id'])['state']=='complete'
    finally:await close_app(app)


@pytest.mark.asyncio
async def test_explicit_platform_rejection_stays_distinct_from_lost_response(app_factory,platform):
    app=app_factory()
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url='http://127.0.0.1') as client:
            row=await new_video(client,app)
            original=platform.handle
            def reject(request):
                if request.url.path.endswith('/data-acquisition/batchSave'):
                    return httpx.Response(200,json={'code':'INTERVAL_INVALID','message':'Unsupported interval'})
                return original(request)
            app.state.videos.target.client._transport=httpx.MockTransport(reject)
            response=await client.post(f'/api/videos/{row["id"]}/corrections',json={'startTime':'2026-09-30 03:00:00','durationSeconds':120,'expectedRevision':0,'requestId':'rejected'})
            op=await wait_video_operation(app,response)
            assert op['state']=='attention'
            current=app.state.videos.store.get(row['id'])
            assert current['versions'][-1]['submissionOutcome']=='rejected'
            assert current['versions'][-1]['errorCode']=='capture_rejected'
            removed=await client.post(f'/api/videos/{row["id"]}/withdraw',json={'version':1,'expectedRevision':1,'requestId':'withdraw-rejected'})
            assert (await wait_video_operation(app,removed))['state']=='complete'
            assert len(platform.parents)==1 and app.state.videos.store.get(row['id'])['activeRevision']==0
    finally:await close_app(app)


@pytest.mark.asyncio
async def test_late_cleanup_receipt_updates_old_history_only(app_factory):
    app=app_factory()
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url='http://127.0.0.1') as client:
            row=await correction(app,client)
            snapshot=copy.deepcopy(row);snapshot['_withdrawCheckpoint']=1
            old=snapshot['versions'][1];old['cleanup']={'started':True,'complete':True}
            latest=copy.deepcopy(row);latest['versions'].append({**copy.deepcopy(old),'revision':2,'taskId':'new-task','cleanup':{}})
            latest.update(revision=2,activeRevision=2);app.state.videos.store.save(latest)
            cleanup_checkpoint(app.state.videos,snapshot,old)
            current=app.state.videos.store.get(row['id'])
            assert current['activeRevision']==2 and current['versions'][1]['cleanup']['complete']
            assert current['versions'][2]['taskId']=='new-task' and current['versions'][2]['cleanup']=={}
    finally:await close_app(app)


@pytest.mark.asyncio
async def test_sample_90_minutes_rejected_before_any_version_or_generation_change(tmp_path):
    source=tmp_path/'source.mp4';source.write_bytes(b'isolated-no-decode')
    app=create_rehearsal(tmp_path/'test',source)
    try:
        row={'id':'sample','jobId':'sample','name':'鲁NH1113','plate':'鲁NH1113','revision':0,'activeRevision':0,'acceptedRevision':None,'requests':{},
             'versions':[{'revision':0,'state':'ready','startTime':'2026-09-13 09:54:00','endTime':'2026-09-13 10:54:00','durationSeconds':3600}]}
        app.state.videos.store.save(row)
        before=app.state.media_control.get('collection:sample')
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url='http://127.0.0.1') as client:
            response=await client.post('/api/videos/sample/corrections',json={'startTime':'2026-09-13 09:54:00','durationSeconds':5400,'expectedRevision':0,'requestId':'invalid'})
            assert response.status_code==400 and '测试原片' in response.text
        assert app.state.videos.store.get('sample')['revision']==0
        assert app.state.media_control.get('collection:sample')==before
        assert not app.state.videos.store.list_operations()
    finally:await close_app(app)
