import io
import zipfile
from pathlib import Path

import httpx
from PIL import Image
import pytest
import pytest_asyncio

from app.audit_moves import source_fingerprint
from app.main import create_app
from app.slicing import source_version
from app.video_filters import record_status
from tests.test_media_control import collection
from tests.test_slicing_performance import close_app


@pytest_asyncio.fixture
async def app(tmp_path):
    value = create_app(data_dir=tmp_path/'data')
    yield value
    await close_app(value)


def seed(app, tmp_path, identity='collection'):
    path = tmp_path/'source.mp4'; path.write_bytes(b'original-video')
    row = collection(app, path, identity)
    row['versions'][0].update(state='failed', errorCode='media_incomplete', downloadRequested=True,
        recovery={'state':'attention','slicingItemId':'recovery','errorCode':'old_failure'})
    app.state.videos.store.save(row)
    item = {'id':'recovery','name':path.name,'path':str(path),'fileVersion':source_version(path),'revision':0,
        'state':'complete','collectionRecovery':True,'collectionVideoId':row['id'],'collectionRevision':0,
        'sourceDatasetId':'27912','sourceVideoId':'','categories':{}}
    for name in ('bed','magnet'):
        item['categories'][name] = {'state':'complete','packageId':name,'actualImages':1}
        folder=tmp_path/name; folder.mkdir()
        picture=folder/'image.jpg'
        data=io.BytesIO();Image.new('RGB',(32,18),name=='bed' and 'green' or 'blue').save(data,format='JPEG')
        picture.write_bytes(data.getvalue());archive=folder/'images.zip'
        with zipfile.ZipFile(archive,'w') as zipped:zipped.writestr('images/image.jpg',data.getvalue())
        app.state.slicing.packages.store.save('items', {'id':name,'itemId':item['id'],'category':name,'state':'complete',
            'name':archive.name,'path':str(archive),'fileVersion':source_fingerprint(archive),'imageDir':str(folder),
            'images':[picture.name],'imageVersions':{picture.name:source_fingerprint(picture)},'imageCount':1,
            'actualImages':1,'outputImageIds':['image-'+name],'verifiedJobId':'job-'+name,
            'videoPath':str(path),'videoFileVersion':item['fileVersion']})
    app.state.slicing.store.save('items',item)
    app.state.media_control.refresh()
    return path


@pytest.mark.asyncio
async def test_verified_partial_result_automatically_leaves_review_without_changing_media_fact(app,tmp_path):
    seed(app,tmp_path)
    manager=app.state.videos
    manager.store.save_operation({'id':'old-attention','videoId':'collection','kind':'recover-decodable',
        'version':0,'expectedRevision':0,'state':'attention'})
    manager.reconcile_recovery_results()
    row=manager.store.get('collection');version=row['versions'][0]
    assert record_status(row)=='saved' and version['state']=='failed' and version['errorCode']=='media_incomplete'
    assert version['recovery']['resultVerified'] and not version['downloadRequested']
    assert version['recovery']['errorCode']=='' and len(version['recovery']['results'])==2
    assert manager.store.get_operation('old-attention')['state']=='complete'
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url='http://127.0.0.1') as c:
        state=(await c.get('/api/videos?status=complete')).json()
        assert len(state['videos'])==1 and state['counts']['complete']==1
        assert state['counts']['saved']==1
        assert len((await c.get('/api/videos?status=saved')).json()['videos'])==1
        assert (await c.get('/api/videos?status=review')).json()['videos']==[]


@pytest.mark.asyncio
@pytest.mark.parametrize('problem',['importing','missing_receipt','wrong_version','superseded'])
async def test_incomplete_or_wrong_package_never_completes_partial_video(app,tmp_path,problem):
    seed(app,tmp_path)
    store=app.state.slicing.packages.store;p=store.get('items','magnet')
    if problem=='importing':p['state']='importing'
    elif problem=='missing_receipt':p['outputImageIds']=[]
    elif problem=='wrong_version':p['videoFileVersion']={'size':9999}
    else:p['superseded']=True
    store.save('items',p)
    app.state.videos.reconcile_recovery_results()
    assert record_status(app.state.videos.store.get('collection'))=='review'


@pytest.mark.asyncio
async def test_bulk_classification_checks_receipts_and_preserves_progress(app,tmp_path):
    identity='a'*24
    path=seed(app,tmp_path,identity)
    # Another file has an unfinished upload; one failure must not block the good row.
    other=collection(app,path,'b'*24)
    other['versions'][0].update(state='failed',errorCode='media_incomplete',downloadRequested=True,
                               recovery={'state':'slicing','slicingItemId':'not-complete'})
    app.state.videos.store.save(other)
    body={'requestId':'manual-classification','selections':[{'id':key,'version':0,'expectedRevision':0}
          for key in (identity,other['id'])]}
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url='http://127.0.0.1') as c:
        response=await c.post('/api/videos/bulk-classify-saved',json=body)
        assert response.status_code==200
        assert [o['ok'] for o in response.json()['outcomes']]==[True,False]
        assert '核验' in response.json()['outcomes'][1]['message']
        assert (await c.post('/api/videos/bulk-classify-saved',json=body)).json()==response.json()
    saved=app.state.videos.store.get(identity)
    assert saved['revision']==0 and saved['activeRevision']==0
    assert saved['versions'][0]['state']=='failed' and record_status(saved)=='saved'
    assert saved['versions'][0]['recovery']['resultVerified']
    assert path.read_bytes()==b'original-video'
    assert app.state.videos.store.get(other['id'])['versions'][0]['downloadRequested']
    assert record_status(app.state.videos.store.get(other['id']))=='review'
    assert len(app.state.slicing.store.list('items'))==1


@pytest.mark.asyncio
async def test_manual_classification_cannot_overwrite_a_new_revision_or_a_reused_request(app,tmp_path):
    seed(app,tmp_path)
    manager=app.state.videos
    data={'version':0,'expectedRevision':0,'requestId':'classify'}
    await manager.classify_partial_saved('collection',data)
    from app.videos import Conflict
    with pytest.raises(Conflict):
        await manager.classify_partial_saved('collection',{**data,'expectedRevision':1})
    row=manager.store.get('collection');row['revision']=1
    row['versions'].append({**row['versions'][0],'revision':1,'state':'ready','recovery':{}})
    row['activeRevision']=1;manager.store.save(row)
    with pytest.raises(Conflict):
        await manager.classify_partial_saved('collection',data)
    assert manager.store.get('collection')['activeRevision']==1
    assert record_status(manager.store.get('collection'))=='ready'


@pytest.mark.asyncio
async def test_archive_retry_uses_workspace_and_repairs_identical_photos_only(app,tmp_path):
    seed(app,tmp_path);w=app.state.workspace
    preview=await w.preview(str(tmp_path),0,'enable-preview')
    await w.apply(preview['id'],0,'enable');await w.task
    asset=w.rows('assets')[0]
    assert w.outcomes(asset)=={'bed':'complete','magnet':'complete'}
    assert asset['stage']=='已切片'
    p=app.state.slicing.packages.store.get('items','magnet')
    name=p['images'][0];picture=Path(p.get('imagePaths',{}).get(name,str(Path(p['imageDir'])/name)))
    contents=picture.read_bytes();replacement=picture.with_suffix('.replaced');replacement.write_bytes(contents);replacement.replace(picture)
    p['photoMetadataVerifiedAt']='old'
    app.state.slicing.packages.store.save('items',p)
    asset.update(status='attention',message='old conflict');w.save('assets',asset)
    app.state.slicing.files.refresh()
    file=next(f for f in app.state.slicing.files.rows('files') if f['path']==asset['path'])
    result=await app.state.slicing.files.retry(file['id'],'retry-one')
    assert next(r for r in result['items'] if r['id']==file['id'])['archiveState']=='waiting'
    await w.reconcile(only=asset['id'],cleanup=False)
    current=w.get('assets',asset['id']);assert current['status']=='managed' and current['archiveRetry']['state']=='complete'
    p=app.state.slicing.packages.store.get('items','magnet')
    assert p['imageVersions'][name]==source_fingerprint(picture)
    assert current['path']==asset['path'] and Path(current['path']).is_file()


@pytest.mark.asyncio
async def test_mismatching_photo_is_not_rebound_to_verified_zip(app,tmp_path):
    seed(app,tmp_path);w=app.state.workspace
    await w.discover();asset=w.rows('assets')[0]
    p=app.state.slicing.packages.store.get('items','magnet');photo=Path(p['imageDir'])/'image.jpg'
    photo.write_bytes(b'different-photo')
    with pytest.raises(ValueError,match='不符|不同'):
        await w.refresh_archive_photos(asset)
    assert app.state.slicing.packages.store.get('items','magnet')['imageVersions']==p['imageVersions']


@pytest.mark.asyncio
async def test_archive_button_and_recheck_register_once_without_waiting_for_disk_work(app,tmp_path,monkeypatch):
    seed(app,tmp_path);w=app.state.workspace
    preview=await w.preview(str(tmp_path),0,'preview')
    await w.apply(preview['id'],0,'apply');await w.task
    asset=w.rows('assets')[0];asset.update(status='attention',message='retry me');w.save('assets',asset)
    async def slow(*_a,**_k):
        pytest.fail('request registration must not wait for reconciliation')
    monkeypatch.setattr(w,'reconcile',slow)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url='http://127.0.0.1') as c:
        group=(await c.get('/api/video-processing')).json()['groups'][0]
        body={'action':'retry-archive','itemId':group['items'][0]['id'],'fileId':group['files'][0]['id'],
              'generation':group['generation'],'expectedRevision':group['items'][0]['revision'],'requestId':'button'}
        url='/api/video-processing/'+group['id']+'/actions'
        response=await c.post(url,json=body)
        assert response.status_code==200 and response.json()['groups'][0]['files'][0]['archiveState']=='waiting'
        assert (await c.post(url,json=body)).status_code==200
        assert sum(r.get('kind')=='archive-retry' for r in w.rows('requests'))==1
        assert (await c.post(url,json={**body,'fileId':'wrong','requestId':'wrong'})).status_code==409
        request={'expectedRevision':w.config['revision'],'requestId':'recheck'}
        assert (await c.post('/api/workspace/reconcile',json=request)).json()['lastAction']['state']=='queued'
        assert (await c.post('/api/workspace/reconcile',json=request)).status_code==200


@pytest.mark.asyncio
async def test_cleanup_button_reports_missing_receipts_instead_of_silent_success(app,tmp_path):
    seed(app,tmp_path);w=app.state.workspace
    p=await w.preview(str(tmp_path),0,'preview');await w.apply(p['id'],0,'apply');await w.task
    app.state.slicing.files.refresh()
    with pytest.raises(ValueError,match='凭证|更正|下载'):
        await app.state.slicing.files.cleanup_confirmed(app.state.slicing.files.rows('files')[0]['id'],'cleanup')


@pytest.mark.asyncio
async def test_legacy_waiting_flag_cannot_keep_finished_workspace_running(app,tmp_path):
    seed(app,tmp_path);w=app.state.workspace
    p=await w.preview(str(tmp_path),0,'preview');await w.apply(p['id'],0,'apply');await w.task
    files=app.state.slicing.files;files.refresh();legacy=files.rows('files')[0]
    legacy['archiveState']='waiting';files.save('files',legacy)
    assert files.archive_count('waiting')==0
    asset=w.rows('assets')[0];w.request_archive_retry(asset['id'],'retry')
    assert files.archive_count('waiting')==1
    await w.reconcile(only=asset['id'],cleanup=False)
    assert files.archive_count('waiting')==0
