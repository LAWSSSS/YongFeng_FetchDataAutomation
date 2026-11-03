"""Targeted continuation and photo archive proofs use isolated local data only."""
import copy

import httpx
import pytest

from app.main import create_app
from app.slicing import source_version
from tests.test_media_control import sliced, collection
from tests.test_slicing_performance import close_app


def failed(app, path, identity, dataset, code='worker_stalled'):
    sliced(app, path, identity, dataset, 'attention')
    item = app.state.slicing.store.get('items', identity)
    name = 'bed' if dataset == '27912' else 'magnet'
    item.update(errorCode=code, message='模拟临时失败')
    item['categories'][name].update(errorCode=code, message='模拟临时失败')
    app.state.slicing.store.save('items', item)
    return item


def setup_run(app, *, enabled=True):
    manager = app.state.slicing
    manager.auth.access = 'isolated-test'
    run = {'id': 'previous-run', 'state': 'running' if enabled else 'paused', 'items': ['unrelated'],
           'settingsRevision': manager.store.settings['revision'], 'localRecoveryOnly': True, 'singleMode': False}
    manager.store.save('runs', run)
    manager.store.settings.update(enabled=enabled, latestRunId=run['id'], targetDatasetIds={'27912': '101', '27944': '102'})
    manager.store.save_settings()
    manager.run = run
    return manager


@pytest.mark.asyncio
@pytest.mark.parametrize('enabled', [True, False])
async def test_continue_requeues_both_categories_without_restarting_other_work(tmp_path, monkeypatch, enabled):
    app = create_app(data_dir=tmp_path / 'db'); path = tmp_path / 'video.mp4'; path.write_bytes(b'video')
    failed(app, path, 'bed', '27912'); failed(app, path, 'magnet', '27944')
    manager = setup_run(app, enabled=enabled)
    monkeypatch.setattr(manager, '_wake_scheduler', lambda: None)
    app.state.media_control.refresh()
    bed = manager.store.get('items', 'bed'); magnet = manager.store.get('items', 'magnet')
    assert bed['mediaId'] == magnet['mediaId']
    body = {'requestId': 'continue-both', 'generation': bed['mediaGeneration'],
            'selections': [{'id': i['id'], 'expectedRevision': i['revision']} for i in (bed, magnet)], 'packages': []}
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://127.0.0.1') as client:
        url = '/api/video-processing/' + bed['mediaId'] + '/continue'
        response = await client.post(url, json=body)
        assert response.status_code == 202, response.text
        assert [x['ok'] for x in response.json()['outcomes']] == [True, True]
        revisions = {x:manager.store.get('items', x)['revision'] for x in ('bed', 'magnet')}
        assert all(manager.store.get('items', x)['state'] == 'pending' for x in revisions)
        assert set(manager.run['items']) >= {'bed', 'magnet'}
        if not enabled:
            assert 'unrelated' not in manager.run['items']
        assert (await client.post(url, json=body)).status_code == 202
        assert revisions == {x:manager.store.get('items', x)['revision'] for x in revisions}
    await close_app(app)


@pytest.mark.asyncio
async def test_stale_and_unconfirmed_versions_never_start(tmp_path, monkeypatch):
    app = create_app(data_dir=tmp_path / 'db'); path = tmp_path / 'video.mp4'; path.write_bytes(b'video')
    collection(app, path, accepted=False)
    failed(app, path, 'bed', '27912'); manager = setup_run(app)
    monkeypatch.setattr(manager, '_wake_scheduler', lambda: None)
    app.state.media_control.refresh(); item = manager.store.get('items', 'bed')
    body = {'requestId': 'blocked', 'generation': item['mediaGeneration'],
            'selections': [{'id': item['id'], 'expectedRevision': item['revision']}], 'packages': []}
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://127.0.0.1') as client:
        url = '/api/video-processing/' + item['mediaId'] + '/continue'
        result = await client.post(url, json=body)
        assert result.status_code == 409
        assert '确认' in result.text and manager.store.get('items', 'bed')['state'] == 'attention'
    await close_app(app)


@pytest.mark.asyncio
async def test_retry_local_dual_model_retains_completed_other_category(tmp_path, monkeypatch):
    app = create_app(data_dir=tmp_path / 'db'); path = tmp_path / 'video.mp4'; path.write_bytes(b'video')
    item = failed(app, path, 'dual', '27912')
    item.update(localWorkspace=True, sourceType='local_workspace')
    item['categories']['magnet'] = {'state': 'complete', 'packageId': 'already-imported', 'actualImages': 150}
    app.state.slicing.store.save('items', item)
    manager = setup_run(app); monkeypatch.setattr(manager, '_wake_scheduler', lambda: None)
    before = copy.deepcopy(item['categories']['magnet'])
    manager.retry('dual', item['revision'], 'retry-dual')
    current = manager.store.get('items', 'dual')
    assert current['categories']['bed']['state'] == 'pending'
    assert current['categories']['magnet'] == before
    assert 'dual' in manager.run['items']
    await close_app(app)


@pytest.mark.asyncio
async def test_reverify_existing_photo_import_does_not_reupload(tmp_path, monkeypatch):
    app = create_app(data_dir=tmp_path / 'db'); path = tmp_path / 'video.mp4'; path.write_bytes(b'video')
    item = failed(app, path, 'bed', '27912', 'verification')
    item['categories']['bed']['packageId'] = 'photo-package'
    app.state.slicing.store.save('items', item)
    manager = setup_run(app); monkeypatch.setattr(manager, '_wake_scheduler', lambda: None)
    manager.packages.store.save('items', {'id':'photo-package', 'itemId':'bed', 'category':'bed', 'dataset':'101',
        'revision':0,'state':'attention','errorCode':'verification','jobId':'existing-job','name':'photo.zip'})
    app.state.media_control.refresh(); item = manager.store.get('items', 'bed')
    body = {'requestId':'reverify','generation':item['mediaGeneration'],'selections':[],
            'packages':[{'id':'photo-package','expectedRevision':manager.packages.store.get('items','photo-package')['revision']}]}
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url='http://127.0.0.1') as client:
        url = '/api/video-processing/' + item['mediaId'] + '/continue'
        response = await client.post(url,json=body)
        assert response.status_code == 202, response.text
        package=manager.packages.store.get('items','photo-package')
        assert package['state']=='importing' and package['jobId']=='existing-job'
        assert (await client.post(url,json=body)).status_code == 202
        assert manager.packages.store.get('items','photo-package')['jobId']=='existing-job'
    await close_app(app)


@pytest.mark.asyncio
async def test_unknown_or_stale_errors_are_not_approved(tmp_path, monkeypatch):
    app = create_app(data_dir=tmp_path / 'db'); path = tmp_path / 'video.mp4'; path.write_bytes(b'video')
    item = failed(app, path, 'bed', '27912', 'bed_alignment_uncertain')
    manager = setup_run(app); monkeypatch.setattr(manager, '_wake_scheduler', lambda: None)
    app.state.media_control.refresh(); item = manager.store.get('items', 'bed')
    body={'requestId':'unknown','generation':item['mediaGeneration'],
          'selections':[{'id':item['id'],'expectedRevision':item['revision']}],'packages':[]}
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url='http://127.0.0.1') as client:
        url='/api/video-processing/'+item['mediaId']+'/continue'
        response=await client.post(url,json=body)
        assert response.status_code==409 and '人工核对' in response.text
        assert manager.store.get('items','bed')['state']=='attention'
        item['errorCode']='worker_stalled';item['categories']['bed']['errorCode']='worker_stalled'
        item['revision']+=1;manager.store.save('items',item)
        assert (await client.post(url,json=body)).status_code==409
        assert manager.store.get('items','bed')['state']=='attention'
    await close_app(app)


@pytest.mark.asyncio
async def test_lost_response_after_first_retry_reconciles_without_duplicate_work(tmp_path, monkeypatch):
    app=create_app(data_dir=tmp_path/'db');path=tmp_path/'video.mp4';path.write_bytes(b'video')
    failed(app,path,'bed','27912');failed(app,path,'magnet','27944')
    manager=setup_run(app);monkeypatch.setattr(manager,'_wake_scheduler',lambda:None)
    app.state.media_control.refresh();bed=manager.store.get('items','bed');magnet=manager.store.get('items','magnet')
    body={'requestId':'lost-response','generation':bed['mediaGeneration'],
          'selections':[{'id':x['id'],'expectedRevision':x['revision']} for x in (bed,magnet)],'packages':[]}
    real_retry=manager.retry;calls=[]
    def interrupted(*args,**kwargs):
        result=real_retry(*args,**kwargs);calls.append(args[0])
        if len(calls)==1:raise RuntimeError('simulated lost response')
        return result
    monkeypatch.setattr(manager,'retry',interrupted)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app,raise_app_exceptions=False),base_url='http://127.0.0.1') as client:
        url='/api/video-processing/'+bed['mediaId']+'/continue'
        assert (await client.post(url,json=body)).status_code==500
        first_revision=manager.store.get('items','bed')['revision']
        result=await client.post(url,json=body)
        assert result.status_code==202,result.text
        assert all(outcome['ok'] for outcome in result.json()['outcomes'])
        assert manager.store.get('items','bed')['revision']==first_revision
        assert manager.store.get('items','magnet')['state']=='pending'
    await close_app(app)


def test_timed_out_observation_is_not_reused_as_an_image_decision(tmp_path):
    app=create_app(data_dir=tmp_path/'db');manager=app.state.slicing
    manager.store.save('observations',{'id':'timed','label':'uncertain','code':'frame_timeout'})
    manager.store.save('observations',{'id':'valid','label':'target','code':''})
    assert manager._get_observation('timed') is None
    assert manager._get_observation('valid')['label']=='target'
    app.state.store.close()


def test_automatic_import_recheck_is_bounded_and_never_creates_an_upload(tmp_path):
    app=create_app(data_dir=tmp_path/'db');manager=app.state.slicing.packages
    manager.clock=lambda:1000
    record={'id':'one','state':'attention','errorCode':'verification','jobId':'existing-job',
            'keys':['existing-object'],'itemId':'missing-item','revision':0}
    manager.store.save('items',record)
    item=manager.store.get('items','one')
    assert manager.queue_verification_retry(item)
    saved=manager.store.get('items','one')
    assert saved['state']=='importing' and saved['jobId']=='existing-job'
    assert saved['verificationNotBefore']==1030 and saved['verificationAutoRetryForJob']=='existing-job'
    assert not saved.get('uploadId') and not manager.queue_verification_retry(saved)
    app.state.store.close()
