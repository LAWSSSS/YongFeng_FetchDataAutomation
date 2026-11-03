"""Date-run records can disappear from the UI while their jobs remain intact."""
import copy

import httpx
import pytest

from app.main import create_app
from tests.test_slicing_performance import close_app


async def delete_records(app, client, url, **kwargs):
    response = await client.post(url, **kwargs)
    if response.status_code == 202:
        await app.state.record_deletion.tick()
    return response


def seed(manager, identity, status='running'):
    row={'id':identity,'requestId':'request-'+identity,'revision':0,'status':status,'stage':'query',
         'message':'正在查询','filters':{'dateFrom':'2026-10-30','dateTo':'2026-10-31'},
         'counts':{},'scanned':0,'total':1,'createdAt':'2026-11-03T00:00:00Z','records':[]}
    manager._save(row)
    return manager.public(manager.get(identity))


@pytest.mark.asyncio
async def test_date_run_batch_hide_and_restore_never_change_background_ledger(tmp_path):
    app=create_app(data_dir=tmp_path/'db')
    try:
        manager=app.state.collection_runs
        rows=[seed(manager,'one'),seed(manager,'two','complete')]
        baseline=copy.deepcopy(manager.list())
        body={'requestId':'hide-both','selections':[{'id':r['id'],'expectedRevision':r['revision']} for r in rows]}
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url='http://127.0.0.1') as client:
            response=await delete_records(app, client, '/api/collection-runs/records/remove',json=body)
            assert response.status_code==202
            assert (await client.get('/api/collection-runs')).json()['runs']==[]
            assert (await client.get('/api/collection-runs/one')).status_code==404
            assert manager.list()==baseline and manager.get('one')['status']=='running'
            assert (await delete_records(app, client, '/api/collection-runs/records/remove',json=body)).json()['operationId']==response.json()['operationId']
            restore={'requestId':'undo-both','removalId':response.json()['recordRemoval']['removalId']}
            assert (await client.post('/api/collection-runs/records/restore',json=restore)).status_code==410
            assert (await client.get('/api/collection-runs')).json()['runs']==[]
            assert manager.list()==baseline
    finally:
        await close_app(app)


@pytest.mark.asyncio
async def test_date_run_progress_conflict_is_atomic_and_hidden_work_keeps_progressing(tmp_path):
    app=create_app(data_dir=tmp_path/'db')
    try:
        manager=app.state.collection_runs
        one,two=seed(manager,'one'),seed(manager,'two')
        changed=manager.get('two');changed['scanned']=1;manager._save(changed)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url='http://127.0.0.1') as client:
            body={'requestId':'stale-selection','selections':[{'id':r['id'],'expectedRevision':r['revision']} for r in [one,two]]}
            assert (await delete_records(app, client, '/api/collection-runs/records/remove',json=body)).status_code==409
            assert not manager.visibility.hidden()
            assert (await delete_records(app, client, '/api/collection-runs/records/remove',json={'requestId':'hide-one',
                'selections':[{'id':'one','expectedRevision':one['revision']}]})).status_code==202
            row=manager.get('one');row.update(status='complete',message='后台完成');manager._save(row)
            assert manager.get('one')['status']=='complete'
            assert [r['id'] for r in (await client.get('/api/collection-runs')).json()['runs']]==['two']
    finally:
        await close_app(app)


@pytest.mark.asyncio
async def test_date_run_page_clamps_after_last_page_is_hidden(tmp_path):
    app=create_app(data_dir=tmp_path/'db')
    try:
        manager=app.state.collection_runs
        for index in range(21):seed(manager,str(index),'complete')
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url='http://127.0.0.1') as client:
            last=(await client.get('/api/collection-runs?page=2')).json()
            assert last['page']==2 and last['pages']==2 and len(last['runs'])==1
            row=last['runs'][0]
            await delete_records(app, client, '/api/collection-runs/records/remove',json={'requestId':'hide-last-page',
                'selections':[{'id':row['id'],'expectedRevision':row['revision']}]})
            clamped=(await client.get('/api/collection-runs?page=2')).json()
            assert clamped['page']==clamped['pages']==1 and len(clamped['runs'])==20
    finally:
        await close_app(app)
