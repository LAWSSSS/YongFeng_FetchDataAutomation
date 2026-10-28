"""Isolated queue acceptance tests: no real platform, database, volume or video writes."""
import asyncio
import copy
import json
import time
from pathlib import Path
from uuid import uuid4

import httpx
import pytest
import pytest_asyncio

from app.audit_moves import fingerprint
from app.main import create_app
from app.remote import RemoteClient
from app.store import JobStore
from app.upload_audit import AthenaClient, AuditError, UploadAuditManager, scan_directory
from app.upload_remote import UploadRemote, source_keys
from app.uploads import MAX_BATCH_BYTES, MAX_FILE_BYTES, STALL_SECONDS, UploadManager


class Platform:
    """Uses the actual Athena adapter and real FastAPI endpoints."""
    def __init__(self):
        self.rows = {'27912': [], '27944': []}
        self.videos = {'27912': [], '27944': []}
        self.logs = {}
        self.posts = []
        self.next_job, self.next_video = 1000, 2000
        self.lose_response = False
        self.network = False
        self.incomplete_config = False
        self.scoped_grants = False
        self.grants_issued = 0

    @staticmethod
    def response(data, headers=None):
        return httpx.Response(200, json={'data': data}, headers=headers)

    def handle(self, request):
        path, params = request.url.path, request.url.params
        if self.network:
            raise httpx.ConnectError('test network', request=request)
        if path == '/api/auth/login/':
            return self.response({'access':'test-access','refresh':'test-refresh'})
        assert request.headers.get('authorization') == 'Bearer test-access'
        if path == '/api/devel/dataset-jobs/' and request.method == 'POST':
            body = json.loads(request.content)
            assert body['type'] == 1 and body['namespace'] == 1
            assert body['config']['options'] == {'mode':1}
            assert set(body['config']) == {'source','options'}, 'Do not delete annotations or import labels'
            assert 1 <= len(body['config']['source']['files']) <= 4
            self.posts.append(copy.deepcopy(body))
            dataset = str(body['dataset'])
            assert not any(str(j['status']) in ('1','2') for j in self.rows[dataset]), 'Only one import per target'
            self.next_job += 1
            row = {**body, 'id':self.next_job, 'status':1}
            self.rows[dataset].append(row)
            if self.lose_response:
                raise httpx.ReadError('lost after server accepted', request=request)
            return self.response(row)
        if path == '/api/devel/dataset-jobs/':
            assert params['type'] == '1'
            rows = self.rows[params['dataset']]
            return self.response(rows, {'x-total':str(len(rows)), 'x-next-page':''})
        if path.endswith('/logs/'):
            identity = path.split('/')[-3]
            return self.response({'result':[{'values':[[str(time.time_ns()), line] for line in self.logs.get(identity,[])]}],
                'has_more':False, 'index':999})
        if '/datasets/' in path:
            parts = path.strip('/').split('/')
            dataset = parts[3]
            if path.endswith('/upload/'):
                self.grants_issued += 1
                # Real Django wire format, before Athena's browser camel-case interceptor.
                return self.response({'endpoint':'//s3.test', 'region':'us-east-1', 'access_key':'test-key',
                    'secret_key':'test-secret', 'session_token':'test-session', 'bucket':'uploads',
                    'prefix':dataset+f'/grant-{self.grants_issued}/' if self.scoped_grants else dataset+'/'})
            if path.endswith('/videos/'):
                rows = self.videos[dataset]
                return self.response(rows, {'x-total':str(len(rows)), 'x-next-page':''})
            return self.response({'id':int(dataset),'name':dataset})
        raise AssertionError((request.method,path))

    def finish(self, dataset, index=-1, failed=()):
        job = self.rows[dataset][index]
        job['status'] = 4 if failed else 3
        lines = []
        for file in job['config']['source']['files']:
            name = file['key'].rsplit('/',1)[-1]
            if name in failed:
                lines.append('[ERROR] /data/' + name + ' failed')
            else:
                self.next_video += 1
                self.videos[dataset].append({'id':self.next_video,'name':Path(name).stem})
                lines.append('视频无后缀名称:' + Path(name).stem)
        self.logs[str(job['id'])] = lines


class Worker:
    def __init__(self):
        self.calls, self.cleanups = [], []
        self.gates, self.codes = {}, {}
        self.active = {'27912':0,'27944':0}
        self.max_active = dict(self.active)
        self.parts = {}

    async def run(self, item, credentials, event, operation='upload'):
        assert credentials['secretKey'] == 'test-secret'
        if operation == 'cleanup':
            self.cleanups.append(item['id'])
            await event({'event':'cleaned'})
            return
        self.calls.append(copy.deepcopy(item))
        dataset = item['dataset']
        self.active[dataset] += 1
        self.max_active[dataset] = max(self.active[dataset],self.max_active[dataset])
        try:
            await event({'event':'creating'})
            await event({'event':'created','uploadId':item.get('uploadId') or 'multipart-'+item['id']})
            if item['name'] in self.gates:
                await self.gates[item['name']].wait()
            code = self.codes.get(item['id']) or self.codes.get(item['name'])
            if code:
                await event({'event':'error','code':code})
                return
            await event({'event':'progress','bytes':item['size']})
            await event({'event':'part','number':1,'size':item['size'],'etag':'part-etag'})
            await event({'event':'completing'})
            await event({'event':'complete','size':item['size'],'etag':'object-etag'})
        finally:
            self.active[dataset] -= 1


@pytest_asyncio.fixture
async def harness(tmp_path):
    platform, worker = Platform(), Worker()
    auth = AthenaClient(httpx.MockTransport(platform.handle))
    await auth.login('tester','test-only')
    unused = lambda request:httpx.Response(500)
    app = create_app(source=RemoteClient('source',httpx.MockTransport(unused)),
        target=RemoteClient('target',httpx.MockTransport(unused)), data_dir=tmp_path/'db', audit_client=auth,
        upload_worker=worker, upload_file_check=lambda path,version:None)
    manager = app.state.uploads
    now = [time.time()]
    manager.clock = lambda:now[0]
    manager.store.settings.update(enabled=True, validated={'27912':True,'27944':True})
    manager.store.save_settings()
    yield app, manager, platform, worker, now
    await manager.shutdown()
    await app.state.audit_moves.shutdown()
    await app.state.videos.shutdown()
    await app.state.jobs.shutdown()
    await app.state.upload_audit.shutdown()
    app.state.store.close()


def add(manager, root, count=1, dataset='27912', size=8, prefix='truck'):
    root.mkdir(exist_ok=True)
    result = []
    for number in range(count):
        path = root/f'{prefix}_{number}.mp4'
        path.write_bytes(b'video123')
        item = {'id':uuid4().hex,'dataset':dataset,'name':path.name,'path':str(path),'size':size,
            'fileVersion':fingerprint(path.stat()),'originalFileVersion':fingerprint(path.stat()),
            'state':'queued','phase':'queued','bytes':0,'speed':0,'retries':0,'parts':{},'batchId':'','jobId':'',
            'message':'queued','createdAt':'2026-10-15T00:00:00+08:00','errorCode':''}
        manager.store.save('items',item); result.append(item)
    return result


async def settle(manager):
    # Let scheduled file processes start and finish without advancing the fake clock.
    for _ in range(20):
        await asyncio.sleep(0.001)
        if not manager.running:
            return


@pytest.mark.asyncio
async def test_two_targets_four_uploads_each_one_import_and_one_staged_batch(harness,tmp_path):
    _,manager,platform,worker,_ = harness
    for dataset in ('27912','27944'):
        add(manager,tmp_path/dataset,12,dataset)
    gate = asyncio.Event()
    worker.gates = {f'truck_{i}.mp4':gate for i in range(4)}
    await manager.tick(); await settle(manager)
    assert worker.active == {'27912':4,'27944':4}
    assert worker.max_active == {'27912':4,'27944':4}
    gate.set(); await settle(manager)
    await manager.tick()
    assert len(platform.posts) == 2
    assert all(len(p['config']['source']['files'])==4 for p in platform.posts)
    await manager.tick(); await settle(manager)
    await manager.tick(); await settle(manager)
    assert len(platform.posts) == 2, 'No overlapping import'
    assert len(worker.calls) == 16, 'Only one staged batch per target'
    await manager.tick(); assert len(worker.calls) == 16
    platform.finish('27912'); platform.finish('27944')
    await manager.tick()
    assert len(platform.posts) == 4
    assert sum(i['state']=='complete' for i in manager.store.list('items')) == 8


@pytest.mark.asyncio
async def test_human_import_blocks_only_its_target_but_allows_one_preupload(harness,tmp_path):
    _,manager,platform,worker,_ = harness
    platform.rows['27912'] = [{'id':55,'dataset':27912,'type':1,'status':2,'config':{'source':{'files':[{'key':'human'}]}}}]
    for dataset in ('27912','27944'):add(manager,tmp_path/dataset,8,dataset)
    await manager.tick(); await settle(manager); await manager.tick()
    assert [p['dataset'] for p in platform.posts] == [27944]
    await manager.tick(); await settle(manager); await manager.tick()
    assert len([i for i in worker.calls if i['dataset']=='27912']) == 4
    assert manager.state()['targets'][0]['activeImport'] == '55'
    platform.rows['27912'][0]['status'] = 3
    await manager.tick()
    assert len([p for p in platform.posts if p['dataset']==27912]) == 1


@pytest.mark.asyncio
async def test_stalled_one_is_stopped_and_cleaned_without_losing_three_good_files(harness,tmp_path):
    _,manager,platform,worker,now = harness
    items=add(manager,tmp_path/'v',4)
    worker.gates['truck_0.mp4'] = asyncio.Event()
    await manager.tick(); await settle(manager)
    assert len(manager.running) == 1
    good=[manager.store.get('items',i['id']) for i in items[1:]]
    assert all(i['state']=='uploaded' for i in good)
    now[0] += STALL_SECONDS-1; await manager.tick()
    assert len(manager.running)==1 and not worker.cleanups
    now[0] += 1; await manager.tick(); await settle(manager)
    stalled=manager.store.get('items',items[0]['id'])
    assert stalled['state']=='cleanup' and stalled['retries']==1
    await manager.tick()
    assert len(platform.posts)==1 and len(platform.posts[0]['config']['source']['files'])==3
    now[0] += 10; await manager.tick(); await settle(manager)
    assert worker.cleanups == [items[0]['id']]
    assert all(manager.store.get('items',i['id'])['state']=='importing' for i in items[1:])
    assert all(Path(i['path']).is_file() for i in items)


@pytest.mark.asyncio
@pytest.mark.parametrize('code,state',[('network','waiting_connection'),('authorization','waiting_auth'),('disk','waiting_disk')])
async def test_transient_unavailability_keeps_bound_parts_and_does_not_consume_retry(harness,tmp_path,code,state):
    _,manager,_,worker,now=harness
    item=add(manager,tmp_path/'v')[0]; worker.codes[item['name']]=code
    await manager.tick(); await settle(manager)
    paused=manager.store.get('items',item['id'])
    assert paused['state']==state and paused['retries']==0 and paused['uploadId']
    original_id=paused['uploadId']; worker.codes.clear(); now[0]+=30
    await manager.tick(); await settle(manager)
    assert manager.store.get('items',item['id'])['state']=='uploaded'
    assert worker.calls[-1]['uploadId']==original_id and not worker.cleanups


@pytest.mark.asyncio
async def test_only_three_automatic_retries_then_manual_attention(harness,tmp_path):
    _,manager,_,worker,now=harness
    item=add(manager,tmp_path/'v')[0]; worker.codes[item['name']]='upload_failed'
    for failure in range(4):
        await manager.tick(); await settle(manager)
        current=manager.store.get('items',item['id'])
        assert current['retries']==failure+1
        now[0]+=70; await manager.tick(); await settle(manager)
        current=manager.store.get('items',item['id'])
        if failure<3:assert current['state']=='retry_wait'
        else:assert current['state']=='attention'
    assert len(worker.calls)==4 and len(worker.cleanups)==4
    assert manager.state()['targets'][0]['items'][0]['retries']==3


@pytest.mark.asyncio
async def test_lost_import_response_reconciles_exact_manifest_never_reposts(harness,tmp_path):
    _,manager,platform,_,_=harness
    add(manager,tmp_path/'v',2)
    platform.lose_response=True
    await manager.tick(); await settle(manager); await manager.tick()
    assert len(platform.posts)==1
    batch=manager.store.list('batches')[0]
    assert batch['state']=='uncertain' and batch['beforeJobs']==[]
    await manager.tick(); await manager.tick()
    assert len(platform.posts)==1 and manager.store.get('batches',batch['id'])['state']=='importing'
    platform.finish('27912'); await manager.tick()
    assert all(i['state']=='complete' for i in manager.store.list('items'))


@pytest.mark.asyncio
async def test_uncertain_manifest_blocks_target_not_other_target_and_retry_cannot_resend(harness,tmp_path):
    _,manager,platform,_,_=harness
    item=add(manager,tmp_path/'v')[0]
    platform.lose_response=True
    await manager.tick(); await settle(manager); await manager.tick()
    platform.rows['27912'].clear()  # Absence is not proof of rejection.
    add(manager,tmp_path/'other',1,'27944')
    await manager.tick(); await settle(manager); await manager.tick()
    assert len([p for p in platform.posts if p['dataset']==27912])==1
    assert len([p for p in platform.posts if p['dataset']==27944])==1
    manager.retry(item['id']); await manager.tick()
    assert len([p for p in platform.posts if p['dataset']==27912])==1
    assert manager.state()['targets'][0]['blocked']


@pytest.mark.asyncio
async def test_pause_resume_only_keeps_upload_receipts_and_does_not_cancel_import(harness,tmp_path):
    _,manager,platform,worker,_=harness
    items=add(manager,tmp_path/'v',4)
    worker.gates={i['name']:asyncio.Event() for i in items}
    await manager.tick(); await settle(manager)
    assert len(manager.running)==4
    await manager.pause()
    assert not manager.running and all(manager.store.get('items',i['id']).get('uploadId') for i in items)
    await manager.tick(); assert len(worker.calls)==4
    for gate in worker.gates.values():gate.set()
    manager.resume(); await manager.tick(); await settle(manager); await manager.tick()
    assert len(platform.posts)==1
    await manager.pause(); platform.finish('27912'); await manager.tick()
    assert all(i['state']=='complete' for i in manager.store.list('items'))


@pytest.mark.asyncio
async def test_partial_import_retries_only_explicitly_failed_file(harness,tmp_path):
    _,manager,platform,_,_=harness
    items=add(manager,tmp_path/'v',4)
    await manager.tick(); await settle(manager); await manager.tick()
    platform.finish('27912',failed=('truck_1.mp4',))
    await manager.tick()
    states={i['name']:i for i in manager.store.list('items')}
    assert states['truck_1.mp4']['state']=='retry_wait'
    assert all(states[f'truck_{n}.mp4']['state']=='complete' for n in (0,2,3))
    assert states['truck_1.mp4']['object'] and states['truck_1.mp4']['reuseObject']
    assert all(Path(i['path']).exists() for i in items)


@pytest.mark.asyncio
async def test_validation_samples_gate_full_concurrency_until_both_targets_succeed(harness,tmp_path):
    _,manager,platform,worker,_=harness
    manager.store.settings['validated']={}
    for dataset in ('27912','27944'):add(manager,tmp_path/dataset,8,dataset)
    await manager.tick(); await settle(manager); await manager.tick()
    assert [len(p['config']['source']['files']) for p in platform.posts]==[1,1]
    platform.finish('27912'); await manager.tick()
    assert manager.state()['validationMode']
    await manager.tick(); await settle(manager)
    assert len([i for i in worker.calls if i['dataset']=='27912'])==1
    platform.finish('27944'); await manager.tick(); await settle(manager); await manager.tick()
    assert not manager.state()['validationMode']
    assert all(len(p['config']['source']['files'])==4 for p in platform.posts[2:])


@pytest.mark.asyncio
async def test_limit_10gib_per_file_and_strictly_less_than_20gib_per_batch(harness,tmp_path):
    _,manager,platform,worker,_=harness
    add(manager,tmp_path/'v',2,size=MAX_FILE_BYTES)
    over=add(manager,tmp_path/'v2',1,size=MAX_FILE_BYTES+1,prefix='too-big')[0]
    await manager.tick(); await settle(manager); await manager.tick()
    assert len(platform.posts[0]['config']['source']['files'])==1
    assert manager.store.get('items',over['id'])['state']=='attention'
    assert sum(i['size'] for i in worker.calls)<MAX_BATCH_BYTES


@pytest.mark.asyncio
async def test_scan_is_explicit_recursive_idempotent_and_keeps_uncertain_separate(harness,tmp_path):
    app,manager,platform,worker,_=harness
    folder=tmp_path/'videos'; folder.mkdir()
    for sub in ('车斗未上传','磁盘吊未上传','已上传'): (folder/sub).mkdir()
    for path in ('new.mp4','车斗未上传/bed.mp4','磁盘吊未上传/magnet.mp4','已上传/done.mp4'):
        (folder/path).write_bytes(b'video123')
    (folder/'incomplete.part').write_bytes(b'cache')
    platform.rows['27912']=[{'id':71,'type':1,'dataset':27912,'status':3}]
    platform.rows['27944']=[{'id':72,'type':1,'dataset':27944,'status':3}]
    platform.logs={'71':['/data/magnet.mp4','/data/done.mp4'],'72':['/data/bed.mp4','/data/done.mp4']}
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url='http://localhost') as http:
        data={'directory':str(folder),'requestId':'scan-one'}
        assert (await http.post('/api/uploads/scan',json=data)).status_code==202
        await manager.scan_task
        assert len(manager.store.list('items'))==4
        assert (await http.post('/api/uploads/scan',json=data)).status_code==202
        assert len(manager.store.list('items'))==4
        assert (await http.post('/api/uploads/scan',json={**data,'directory':str(tmp_path)})).status_code==409
        (folder/'later.mp4').write_bytes(b'video123')
        await manager.tick(); await settle(manager)
        assert len(manager.store.list('items'))==4, 'No automatic scan for new files'
        assert (await http.post('/api/uploads/scan',json={**data,'requestId':'scan-two'})).status_code==202
        await manager.scan_task
        assert len(manager.store.list('items'))==6
        public=(await http.get('/api/uploads')).text
        for secret in ('test-secret','test-key','test-access','test-session','multipart-'):
            assert secret not in public
        dump=''.join(r[0] for r in manager.store.db.execute('SELECT payload FROM simple_upload_items'))
        for secret in ('test-secret','test-key','test-access','test-session'):
            assert secret not in dump


@pytest.mark.asyncio
async def test_move_and_registered_delete_protect_only_actively_read_file(harness,tmp_path):
    _,manager,_,worker,_=harness
    items=add(manager,tmp_path/'v',2)
    worker.gates[items[0]['name']]=asyncio.Event()
    await manager.tick(); await settle(manager)
    with pytest.raises(ValueError,match='正在使用'):
        with manager.leases.write(items[0]['path']):pass
    with manager.leases.write(items[1]['path']):pass
    destination=tmp_path/'moved.mp4'
    Path(items[1]['path']).rename(destination)
    manager.moved(items[1]['path'],str(destination),fingerprint(destination.stat()))
    updated=manager.store.get('items',items[1]['id'])
    assert updated['path']==str(destination) and updated['fileVersion']==fingerprint(destination.stat())
    assert updated['object']==manager.store.get('items',items[1]['id'])['object']


@pytest.mark.asyncio
async def test_restart_preserves_manual_data_parts_and_uncertain_submission(harness,tmp_path):
    app,manager,platform,worker,_=harness
    item=add(manager,tmp_path/'v')[0]
    worker.gates[item['name']]=asyncio.Event()
    await manager.tick(); await settle(manager)
    before=manager.store.get('items',item['id'])
    assert before['uploadId']
    original_videos=app.state.store.db.execute('SELECT * FROM simple_videos').fetchall()
    await manager.shutdown()
    new=UploadManager(app.state.store,app.state.upload_audit,remote=manager.remote,worker=worker,file_check=lambda *args:None)
    after=new.store.get('items',item['id'])
    assert after['object']==before['object'] and after['uploadId']==before['uploadId']
    assert original_videos==app.state.store.db.execute('SELECT * FROM simple_videos').fetchall()
    worker.gates[item['name']].set()
    await new.tick(); await settle(new); await new.tick()
    assert len(platform.posts)==1
    # A restarted submitted import is polled by its bound ID and exact source manifest.
    restarted=UploadManager(app.state.store,app.state.upload_audit,remote=manager.remote,worker=worker,file_check=lambda *args:None)
    await restarted.tick(); assert len(platform.posts)==1
    platform.finish('27912'); await restarted.tick()
    assert restarted.store.get('items',item['id'])['state']=='complete'
    await new.shutdown(); await restarted.shutdown()


@pytest.mark.asyncio
async def test_repeated_bytes_or_polling_are_not_progress_but_new_bytes_reset_stall(harness,tmp_path):
    _,manager,_,worker,now=harness
    item=add(manager,tmp_path/'v')[0]
    worker.gates[item['name']]=asyncio.Event()
    await manager.tick();await settle(manager)
    now[0]+=599
    await manager.worker_event(item['id'],{'event':'progress','bytes':1})
    now[0]+=599
    await manager.worker_event(item['id'],{'event':'progress','bytes':1})
    await manager.tick();assert item['id'] in manager.running
    now[0]+=1
    await manager.tick();await settle(manager)
    assert manager.store.get('items',item['id'])['errorCode']=='stalled'


@pytest.mark.asyncio
async def test_unknown_import_outcome_not_marked_complete_or_silently_retried(harness,tmp_path):
    _,manager,platform,_,_=harness
    item=add(manager,tmp_path/'v')[0]
    await manager.tick();await settle(manager);await manager.tick()
    job=platform.rows['27912'][0];job['status']=4
    platform.logs[str(job['id'])]=['global exception without a per-file outcome']
    await manager.tick()
    current=manager.store.get('items',item['id'])
    assert current['state']=='attention' and current['errorCode']=='verification'
    manager.retry(item['id']);await manager.tick()
    assert len(platform.posts)==1 and manager.store.get('items',item['id'])['state']=='attention'


@pytest.mark.asyncio
async def test_source_change_or_missing_disk_never_starts_upload(harness,tmp_path):
    _,manager,_,worker,_=harness
    items=add(manager,tmp_path/'v',2)
    def check(path,expected):
        if fingerprint(Path(path).stat())!=expected:raise ValueError('file_changed')
    manager.file_check=check
    Path(items[0]['path']).write_bytes(b'changed')
    Path(items[1]['path']).unlink()
    await manager.tick();await settle(manager)
    assert not worker.calls
    assert manager.store.get('items',items[0]['id'])['state']=='attention'
    missing=manager.store.get('items',items[1]['id'])
    assert missing['state']=='waiting_disk' and missing['retries']==0


@pytest.mark.asyncio
async def test_registered_record_delete_is_deferred_before_cloud_or_file_mutation(harness,tmp_path):
    app,manager,_,worker,_=harness
    from test_audit_moves import registered
    item=add(manager,tmp_path/'v')[0]
    row=registered(app,Path(item['path']))
    worker.gates[item['name']]=asyncio.Event()
    await manager.tick();await settle(manager)
    request={'requestId':'delete-during-upload','expectedRevision':row['revision'],'deleteCloud':False,'deleteLocalFiles':True}
    await app.state.videos.remove_record(row['id'],request)
    await asyncio.gather(*app.state.videos.running.values())
    stored=app.state.videos.store.get(row['id'])
    assert stored['deletion']['state']=='error' and '正在使用' in stored['deletion']['message']
    assert not stored.get('deletedAt') and Path(item['path']).is_file()


@pytest.mark.asyncio
async def test_duplicate_queue_entry_can_be_removed_and_restored_without_touching_video(harness,tmp_path):
    _,manager,_,_,_=harness
    duplicate=add(manager,tmp_path/'duplicates',prefix='copy')[0]
    duplicate.update(state='attention',phase='review',errorCode='scan_review',message='同名文件已有任务。')
    manager.store.save('items',duplicate)
    active=add(manager,tmp_path/'duplicates',prefix='active')[0]
    active.update(state='uploading',phase='uploading',bytes=4,uploadId='bound-multipart',batchId='batch-bound')
    manager.store.save('items',active)
    original=Path(duplicate['path'])

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=harness[0]),base_url='http://localhost') as http:
        removed=await http.post(f'/api/uploads/{duplicate["id"]}/remove',json={})
        assert removed.status_code==200
        target=next(row for row in removed.json()['targets'] if row['id']==duplicate['dataset'])
        assert all(row['id']!=duplicate['id'] for row in target['items'])
        assert any(row['id']==duplicate['id'] for row in target['removedItems'])
        assert manager.store.get('items',duplicate['id'])['state']=='dismissed'
        assert original.is_file()

        restored=await http.post(f'/api/uploads/{duplicate["id"]}/restore',json={})
        assert restored.status_code==200
        target=next(row for row in restored.json()['targets'] if row['id']==duplicate['dataset'])
        visible=next(row for row in target['items'] if row['id']==duplicate['id'])
        assert visible['state']=='attention' and visible['removable']
        assert original.is_file()

    with pytest.raises(Exception,match='不能从队列移除'):
        await manager.remove(active['id'])
    assert manager.store.get('items',active['id'])['state']=='uploading'


@pytest.mark.asyncio
async def test_new_scan_lists_unknown_log_evidence_as_review_without_uploading(harness,tmp_path):
    _,manager,platform,worker,_=harness
    folder=tmp_path/'videos';folder.mkdir();(folder/'unresolved.mp4').write_bytes(b'video')
    platform.rows['27912']=[{'id':99,'dataset':27912,'type':1,'status':4}]
    platform.logs['99']=['[ERROR] /data/unresolved.mp4 failed']
    manager.scan(str(folder),'unknown-scan');await manager.scan_task
    first=next(i for i in manager.store.list('items') if i['dataset']=='27912')
    assert first['state']=='attention'
    await manager.tick();await settle(manager)
    assert all(i['dataset']!='27912' for i in worker.calls)


def test_upload_receipt_manifest_does_not_ignore_duplicate_keys():
    assert source_keys({'config':{'source':{'files':[{'key':'x'},{'key':'x'}]}}}) is None


@pytest.mark.asyncio
async def test_scheme_relative_athena_minio_endpoint_and_safe_diagnostics(harness):
    app,manager,platform,_,_=harness
    handle=platform.handle
    def relative(request):
        answer=handle(request)
        if request.url.path.endswith('/upload/'):
            body=answer.json();body['data']['endpoint']='//s3.test'
            return platform.response(body['data'])
        return answer
    original=manager.audit.client.client
    manager.audit.client.client=httpx.AsyncClient(base_url='http://athena.test/api/',transport=httpx.MockTransport(relative))
    try:
        credentials=await manager.remote.signature('27912')
        assert credentials['endpoint']=='http://s3.test'
        assert credentials['accessKey']=='test-key' and credentials['secretKey']=='test-secret'
        assert credentials['sessionToken']=='test-session' and 'secret_key' not in credentials
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url='http://localhost') as http:
            response=await http.post('/api/uploads/check-connection',json={})
            assert response.status_code==200 and all(t['ok'] for t in response.json()['targets'])
            for secret in ('test-secret','test-key','test-access','s3.test'):
                assert secret not in response.text
    finally:
        await manager.audit.client.client.aclose();manager.audit.client.client=original


@pytest.mark.asyncio
@pytest.mark.parametrize('shape',['camel','snake','conflicting'])
async def test_upload_authorization_normalizes_only_credential_field_names(harness,shape):
    _,manager,platform,_,_=harness
    async def request(*args,**kwargs):
        data={'endpoint':'http://s3.test','bucket':'uploads','prefix':'owned/'}
        if shape=='camel':data.update(accessKey='test-key',secretKey='test-secret',sessionToken='test-session')
        else:data.update(access_key='test-key',secret_key='test-secret',session_token='test-session')
        if shape=='conflicting':data['secretKey']='another-secret'
        return data,{}
    manager.remote.request=request
    if shape=='conflicting':
        with pytest.raises(AuditError) as error:await manager.remote.signature('27912')
        assert error.value.code=='authorization_fields' and 'secret' not in str(error.value)
    else:
        data=await manager.remote.signature('27912')
        assert data['accessKey']=='test-key' and data['sessionToken']=='test-session'


@pytest.mark.asyncio
async def test_network_resume_keeps_runtime_grant_bound_to_original_prefix(harness,tmp_path):
    _,manager,platform,worker,now=harness
    platform.scoped_grants=True
    item=add(manager,tmp_path/'v')[0];worker.codes[item['name']]='network'
    run=worker.run
    async def scoped(item,credentials,event,operation='upload'):
        assert item['object']['key'].startswith(credentials['prefix'])
        return await run(item,credentials,event,operation)
    worker.run=scoped
    await manager.tick();await settle(manager)
    assert platform.grants_issued==1 and manager.grants[item['id']]
    worker.codes.clear();now[0]+=30
    await manager.tick();await settle(manager)
    assert platform.grants_issued==1 and manager.store.get('items',item['id'])['state']=='uploaded'
    assert 'test-secret' not in json.dumps(manager.state())
    assert 'test-secret' not in manager.store.db.execute('SELECT payload FROM simple_upload_items').fetchone()[0]


@pytest.mark.asyncio
async def test_unwritten_old_namespace_can_rebind_after_new_scoped_authorization(harness,tmp_path):
    _,manager,platform,worker,_=harness
    platform.scoped_grants=True
    item=add(manager,tmp_path/'v')[0]
    item['object']={'bucket':'uploads','endpoint':'http://s3.test','region':'us-east-1',
        'key':f"old-grant/simple-{item['id']}/{item['name']}"}
    manager.store.save('items',item)
    run=worker.run
    async def scoped(item,credentials,event,operation='upload'):
        assert item['object']['key'].startswith(credentials['prefix'])
        return await run(item,credentials,event,operation)
    worker.run=scoped
    await manager.tick();await settle(manager)
    saved=manager.store.get('items',item['id'])
    assert saved['state']=='uploaded' and '/grant-1/' in saved['object']['key']
    assert saved['id']==item['id'] and not worker.cleanups


@pytest.mark.asyncio
async def test_explicit_rejected_create_can_refresh_namespace_without_unknown_write(harness,tmp_path):
    _,manager,platform,worker,now=harness
    platform.scoped_grants=True;item=add(manager,tmp_path/'v')[0]
    run=worker.run;calls=[]
    async def reject_once(item,credentials,event,operation='upload'):
        calls.append(item['object']['key'])
        if len(calls)==1:
            await event({'event':'creating'})
            await event({'event':'error','code':'authorization','stage':'create_multipart_upload','httpStatus':403,'keyInIssuedPrefix':False})
        else:
            assert item['object']['key'].startswith(credentials['prefix'])
            await run(item,credentials,event,operation)
    worker.run=reject_once
    await manager.tick();await settle(manager)
    saved=manager.store.get('items',item['id'])
    assert not saved['createAttempted'] and saved['retries']==0 and item['id'] not in manager.grants
    now[0]+=30;await manager.tick();await settle(manager)
    assert manager.store.get('items',item['id'])['state']=='uploaded'
    assert calls[0]!=calls[1] and not worker.cleanups


@pytest.mark.asyncio
async def test_pause_drains_slow_selector_before_returning_and_never_starts_late_worker(harness,tmp_path):
    _,manager,_,worker,_=harness
    add(manager,tmp_path/'v',4)
    entered,release=asyncio.Event(),asyncio.Event()
    videos=manager.remote.videos
    async def slow(dataset):
        entered.set();await release.wait();return await videos(dataset)
    manager.remote.videos=slow
    tick=asyncio.create_task(manager.tick());await entered.wait()
    pause=asyncio.create_task(manager.pause());await asyncio.sleep(0)
    assert not manager.store.settings['enabled'] and not pause.done()
    release.set();await tick;await pause;await settle(manager)
    assert not manager.running and not worker.calls
    manager.resume();await manager.tick();await settle(manager)
    assert len(worker.calls)==4


@pytest.mark.asyncio
async def test_pause_during_submit_inventory_keeps_uploaded_objects_without_new_import(harness,tmp_path):
    _,manager,platform,worker,_=harness
    add(manager,tmp_path/'v',2)
    await manager.tick();await settle(manager)
    entered,release=asyncio.Event(),asyncio.Event()
    videos=manager.remote.videos
    async def slow(dataset):
        entered.set();await release.wait();return await videos(dataset)
    manager.remote.videos=slow
    tick=asyncio.create_task(manager.tick());await entered.wait()
    pause=asyncio.create_task(manager.pause());await asyncio.sleep(0);release.set()
    await tick;await pause
    assert not platform.posts and all(i['state']=='uploaded' for i in manager.store.list('items'))
    assert len(worker.calls)==2


@pytest.mark.asyncio
async def test_cleanup_that_discovers_lost_upload_success_imports_without_reupload(harness,tmp_path):
    _,manager,platform,worker,now=harness
    item=add(manager,tmp_path/'v')[0]
    worker.codes[item['name']]='upload_failed'
    await manager.tick();await settle(manager)
    async def reconcile(item,credentials,event,operation='upload'):
        assert operation=='cleanup'
        await event({'event':'complete','size':item['size'],'etag':'confirmed-after-lost-response'})
    worker.run=reconcile
    now[0]+=10;await manager.tick();await settle(manager)
    assert manager.store.get('items',item['id'])['state']=='uploaded'
    await manager.tick()
    assert len(platform.posts)==1 and len(worker.calls)==1


@pytest.mark.asyncio
async def test_interrupted_scan_request_can_resume_with_same_request_id(harness,tmp_path):
    app,manager,_,_,_=harness
    root=tmp_path/'v';root.mkdir();(root/'new.mp4').write_bytes(b'video')
    request={'id':'interrupted','directory':str(root),'state':'scanning','createdAt':'2026-10-15','added':0,'review':0}
    manager.store.settings['scan']=request
    manager.store.save('requests',request);manager.store.save_settings()
    new=UploadManager(app.state.store,app.state.upload_audit,worker=manager.worker,file_check=lambda *args:None)
    assert new.store.get('requests','interrupted')['state']=='interrupted'
    new.scan(str(root),'interrupted');await new.scan_task
    assert new.store.get('requests','interrupted')['state']=='complete' and len(new.store.list('items'))==2
    await new.shutdown()
