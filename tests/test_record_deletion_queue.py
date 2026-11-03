import asyncio
import copy
from pathlib import Path
import httpx
import pytest
from app.main import create_app
from tests.test_slicing_performance import close_app
from tests.test_record_management import seed_slice
from tests.test_manual_completed_cleanup import prepared


@pytest.mark.asyncio
async def test_registration_fast_idempotent_and_no_undo_or_cloud_mutations(tmp_path):
    app=create_app(data_dir=tmp_path/'db');p=tmp_path/'video.mp4';p.write_bytes(b'keep')
    item=seed_slice(app,p);item['state']='analyzing';app.state.slicing.store.save('items',item)
    payload={'requestId':'delete-one','selections':[{'kind':'slicing','id':'bed','expectedRevision':item['revision']}]}
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url='http://127.0.0.1') as client:
        result=await asyncio.wait_for(client.post('/api/records/delete',json=payload),1)
        assert result.status_code==202
        assert (await client.post('/api/records/delete',json=payload)).json()['operationId']==result.json()['operationId']
        await app.state.record_deletion.tick()
        assert not (await client.get('/api/slicing')).json()['items']
        assert p.exists() and app.state.slicing.store.get('items','bed')['state']=='analyzing'
        assert (await client.post('/api/slicing/records/restore',json={'requestId':'undo','removalId':'delete-one'})).status_code==410
    await close_app(app)


@pytest.mark.asyncio
async def test_completed_record_cleanup_honors_lease_then_hides_after_verification(tmp_path):
    app,w,asset=await prepared(tmp_path)
    row=app.state.videos.store.list()[0]
    controller=app.state.record_deletion
    op=controller.register('delete-finished',[('collection',[{'id':row['id'],'expectedRevision':row['revision']}])])
    await controller.tick()
    assert controller.get(op['operationId'])['state']=='waiting'
    with w.leases.read(asset['path']):
        await w.cleanup(w.get('assets',asset['id']))
    assert Path(asset['path']).exists()
    assert row['id'] not in app.state.videos.record_visibility.hidden()
    await w.cleanup(w.get('assets',asset['id']))
    await controller.tick()
    assert not Path(asset['path']).exists()
    assert controller.get(op['operationId'])['state']=='complete'
    assert row['id'] in app.state.videos.record_visibility.hidden()
    await close_app(app)


@pytest.mark.asyncio
async def test_late_user_change_isolated_from_other_selection(tmp_path):
    app=create_app(data_dir=tmp_path/'db');p=tmp_path/'v.mp4';p.write_bytes(b'keep')
    a=seed_slice(app,p,'bed');b=seed_slice(app,p,'magnet')
    controller=app.state.record_deletion
    controller.register('two',[('slicing',[{'id':v['id'],'expectedRevision':v['revision']} for v in (a,b)])])
    a['state']='pending';app.state.slicing.store.save('items',a)
    await controller.tick()
    states={v['id']:v['state'] for v in controller.get('two')['selections']}
    assert states=={'bed':'attention','magnet':'complete'}
    assert app.state.slicing.visibility.hidden()=={'magnet'}
    await close_app(app)


@pytest.mark.asyncio
async def test_other_visible_video_reference_preserves_shared_media(tmp_path):
    app,w,asset=await prepared(tmp_path)
    row=app.state.videos.store.list()[0]
    other=copy.deepcopy(row);other['id']='other-reference';app.state.videos.store.save(other)
    app.state.record_deletion.register('remove-one',[('collection',[{'id':row['id'],'expectedRevision':row['revision']}])])
    await app.state.record_deletion.tick()
    assert app.state.record_deletion.get('remove-one')['state']=='complete'
    assert Path(asset['path']).exists() and not w.get('assets',asset['id']).get('cleanup',{}).get('requestId')
    assert other['id'] not in app.state.videos.record_visibility.hidden()
    await close_app(app)
