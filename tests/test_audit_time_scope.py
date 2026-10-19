from types import SimpleNamespace

import httpx
import pytest

from app import upload_audit
from app.main import create_app
from app.remote import RemoteClient
from app.store import JobStore
from app.upload_audit import (AthenaClient, compare_files, creation_time, job_latest_ns,
                              ns_iso, timestamp_ns)


def local(name, created):
    return {'name':name, 'relativePath':name, 'size':10, 'issue':'', 'createdNs':str(created)}


def test_birthtime_is_distinct_from_modified_changed_and_filename_dates():
    expected = timestamp_ns('2026-10-13T08:09:10.123456Z')
    assert creation_time(SimpleNamespace(st_birthtime_ns=expected,st_birthtime=0,st_ctime=0,st_mtime=0))==expected
    assert creation_time(SimpleNamespace(st_birthtime=1791878950.123456,st_ctime=2,st_mtime=3))==1791878950123456000
    assert creation_time(SimpleNamespace(st_ctime=1791878950,st_mtime=1791878950)) is None
    assert timestamp_ns('2026-10-13T16:09:10.123456+08:00')==expected
    assert timestamp_ns(str(expected))==expected
    assert timestamp_ns(str(expected//1000000))==expected//1000000*1000000
    assert timestamp_ns('10') is None and timestamp_ns(True) is None


def test_finished_import_uses_last_update_and_running_or_undated_import_is_not_excluded():
    old='2026-10-10T00:00:00Z'; newer='2026-10-14T00:00:00Z'
    assert job_latest_ns({'status':3,'utc_create':old,'utc_modify':newer})==timestamp_ns(newer)
    assert job_latest_ns({'status':3,'utcCreate':old})==timestamp_ns(old)
    assert job_latest_ns({'status':2,'utc_create':old,'utc_modify':old}) is None
    assert job_latest_ns({'status':3,'utc_create':'unknown'}) is None


def test_missing_log_only_blocks_files_created_during_its_possible_window():
    early=timestamp_ns('2026-10-13T00:00:00Z'); middle=timestamp_ns('2026-10-14T00:00:00Z')
    late=timestamp_ns('2026-10-15T00:00:00Z')
    target={'id':'27912','complete':False,'runningJobs':0,'timeScoped':True,'evidence':{},
            'issues':[{'jobId':'old-missing','latestNs':str(middle)}]}
    rows=compare_files([local('early.mp4',early),local('late.mp4',late)],[target])
    assert [row['targets']['27912']['status'] for row in rows]==['review','missing']
    target['issues'][0]['latestNs']=''
    assert all(row['targets']['27912']['status']=='review' for row in compare_files([local('late.mp4',late)],[target]))


def test_each_file_ignores_earlier_matches_and_errors_but_retains_later_evidence():
    birth=timestamp_ns('2026-10-13T00:00:00Z')
    def proof(when, problems=()):
        return {'status':'uploaded','jobId':'1','observedNs':str(when),'problemNs':list(problems)}
    target={'id':'27944','complete':True,'runningJobs':0,'timeScoped':True,'issues':[],
            'evidence':{('truck','stem'):[proof(birth-1)]}}
    assert compare_files([local('truck.mp4',birth)],[target])[0]['targets']['27944']['status']=='missing'
    target['evidence'][('truck','stem')].append(proof(birth, [str(birth-1)]))
    assert compare_files([local('truck.mp4',birth)],[target])[0]['targets']['27944']['status']=='uploaded'
    target['evidence'][('truck','stem')][1]['problemNs'].append(str(birth+1))
    assert compare_files([local('truck.mp4',birth)],[target])[0]['targets']['27944']['status']=='review'
    target['evidence']={('truck','stem'):[{'status':'uploaded','jobId':'1','observedNs':''}]}
    assert compare_files([local('truck.mp4',birth)],[target])[0]['targets']['27944']['status']=='review'


@pytest.mark.asyncio
async def test_old_log_lines_are_excluded_without_turning_a_complete_old_page_into_an_error():
    birth=timestamp_ns('2026-10-13T00:00:00Z')
    values=[[str(birth-1),'视频无后缀名称:truck'],[str(birth),'视频无后缀名称:new']]
    client=AthenaClient(httpx.MockTransport(lambda request:httpx.Response(200,json={'data':{
        'result':[{'values':values}],'has_more':False,'index':0}})))
    client.access='test-only'
    try:
        result=await client.job_evidence('27912',{'id':1,'type':1,'status':3},since_ns=birth)
        assert not result['reason'] and ('truck','stem') not in result['evidence']
        assert result['evidence'][('new','stem')][0]['observedNs']==str(birth)
        values.pop()
        result=await client.job_evidence('27912',{'id':1,'type':1,'status':3},since_ns=birth)
        assert not result['reason'] and not result['evidence']
    finally:
        await client.close()


class DatedPlatform:
    def __init__(self):
        self.logs=[]

    def handle(self, request):
        assert request.method=='GET'
        path=request.url.path
        if '/datasets/' in path:
            return httpx.Response(200,json={'data':{'id':int(path.split('/')[-2])}})
        dataset=int(request.url.params['dataset'])
        if path.endswith('/dataset-jobs/'):
            assert request.url.params['type']=='1'
            return httpx.Response(200,json={'data':[
                {'id':dataset*10+1,'dataset':dataset,'type':1,'status':3,
                 'utc_create':'2026-10-10T00:00:00Z','utc_modify':'2026-10-10T01:00:00Z'},
                {'id':dataset*10+2,'dataset':dataset,'type':1,'status':3,
                 'utc_create':'2026-10-14T00:00:00Z','utc_modify':'2026-10-14T01:00:00Z'},
            ]},headers={'x-total':'2','x-next-page':''})
        identity=int(path.split('/')[-3]); self.logs.append(identity)
        assert identity%10==2, 'Expired logs before file creation must not be fetched'
        names=['both','bed-only'] if dataset==27912 else ['both','magnet-only']
        return httpx.Response(200,json={'data':{'result':[{'values':[
            [str(timestamp_ns('2026-10-14T00:01:00Z')),'视频无后缀名称:'+name] for name in names]}],
            'has_more':False,'index':0}})


@pytest.mark.asyncio
async def test_real_backend_time_filtered_logs_to_two_missing_folders_and_both_missing_stays(tmp_path, monkeypatch):
    birth=timestamp_ns('2026-10-13T08:00:00Z')
    monkeypatch.setattr(upload_audit,'creation_time',lambda info:birth)
    platform=DatedPlatform(); client=AthenaClient(httpx.MockTransport(platform.handle)); client.access='test-only'
    offline=httpx.MockTransport(lambda request:httpx.Response(503))
    source=RemoteClient('source',offline); capture=RemoteClient('target',offline)
    app=create_app(source,capture,tmp_path/'database',audit_client=client)
    root=tmp_path/'videos'; root.mkdir()
    for name in ('both.mp4','bed-only.mp4','magnet-only.mp4','neither.mp4'):
        (root/name).write_bytes(('synthetic-video:'+name).encode())
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url='http://localhost') as http:
            assert (await http.post('/api/upload-audit/run',json={'directory':str(root)})).status_code==202
            await app.state.upload_audit.task
            state=(await http.get('/api/upload-audit')).json()
            assert state['run']['status']=='complete'
            assert state['run']['totalLogs']==2 and state['run']['skippedBeforeCreation']==2
            report=state['report']
            assert report['scope']=={'basis':'local_birthtime','since':ns_iso(birth),'unknownTimes':0}
            assert all(d['complete'] and d['skippedBeforeCreation']==1 for d in report['datasets'])
            assert platform.logs==[279122,279442]
            body={'reportId':report['id'],'uploadedDirectory':str(root/'done'),
                  'magnetMissingDirectory':str(root/'磁盘吊'),'bedMissingDirectory':str(root/'车斗')}
            plan=(await http.post('/api/upload-audit/moves/preview',json=body)).json()
            assert plan['counts']['planned']==3 and plan['counts']['held']==1
            assert (await http.post(f"/api/upload-audit/moves/{plan['id']}/confirm",json={'requestId':'dated-cut'})).status_code==202
            await app.state.audit_moves.task
            assert (root/'done/both.mp4').is_file()
            assert (root/'磁盘吊/bed-only.mp4').is_file()
            assert (root/'车斗/magnet-only.mp4').is_file()
            assert {p.name for p in root.glob('*.mp4')}=={'neither.mp4'}
    finally:
        await app.state.audit_moves.shutdown(); await app.state.videos.shutdown()
        await app.state.upload_audit.shutdown(); await source.close(); await capture.close(); app.state.store.close()


@pytest.mark.asyncio
async def test_old_audit_report_cannot_be_used_to_move_with_new_time_policy(tmp_path):
    offline=httpx.MockTransport(lambda request:httpx.Response(503))
    source=RemoteClient('source',offline); target=RemoteClient('target',offline)
    app=create_app(source,target,tmp_path/'database')
    app.state.upload_audit.saved['report']={'id':'old','files':[]}
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url='http://localhost') as http:
            reply=await http.post('/api/upload-audit/moves/preview',json={'reportId':'old',
                'uploadedDirectory':str(tmp_path/'done'),'magnetMissingDirectory':str(tmp_path/'magnet'),
                'bedMissingDirectory':str(tmp_path/'bed')})
            assert reply.status_code==409 and '重新读取目录' in reply.json()['message']
    finally:
        await app.state.upload_audit.shutdown(); await app.state.videos.shutdown()
        await source.close(); await target.close(); app.state.store.close()


@pytest.mark.asyncio
async def test_an_expired_in_window_task_does_not_block_a_later_file_from_classification(tmp_path, monkeypatch):
    early=timestamp_ns('2026-10-13T00:00:00Z'); late=timestamp_ns('2026-10-15T00:00:00Z')
    monkeypatch.setattr(upload_audit,'creation_time',lambda info:early if info.st_size==10 else late)
    def handle(request):
        path=request.url.path
        if '/datasets/' in path:
            return httpx.Response(200,json={'data':{'id':int(path.split('/')[-2])}})
        dataset=int(request.url.params['dataset'])
        if path.endswith('/dataset-jobs/'):
            return httpx.Response(200,json={'data':[
                {'id':dataset*10+1,'type':1,'status':3,'utc_create':'2026-10-14T00:00:00Z'},
                {'id':dataset*10+2,'type':1,'status':3,'utc_create':'2026-10-16T00:00:00Z'},
            ]},headers={'x-total':'2','x-next-page':''})
        identity=int(path.split('/')[-3])
        values=[] if identity%10==1 else [[str(timestamp_ns('2026-10-16T00:01:00Z')),
            '视频无后缀名称:'+('late' if dataset==27912 else 'another-video')]]
        return httpx.Response(200,json={'data':{'result':[{'values':values}],'has_more':False,'index':0}})
    store=JobStore(tmp_path/'audit.sqlite3')
    client=AthenaClient(httpx.MockTransport(handle)); client.access='test-only'
    manager=upload_audit.UploadAuditManager(store,client)
    root=tmp_path/'videos'; root.mkdir(); (root/'early.mp4').write_bytes(b'a'*10); (root/'late.mp4').write_bytes(b'b'*20)
    try:
        manager.start(str(root)); await manager.task
        report=manager.state()['report']; files={file['name']:file for file in report['files']}
        assert not any(d['complete'] for d in report['datasets'])
        assert all(value['status']=='review' for value in files['early.mp4']['targets'].values())
        assert files['late.mp4']['targets']['27912']['status']=='uploaded'
        assert files['late.mp4']['targets']['27944']['status']=='missing'
    finally:
        await manager.shutdown(); store.close()


@pytest.mark.asyncio
@pytest.mark.parametrize('running',[False,True])
async def test_import_started_before_file_creation_is_read_if_still_running_or_updated_later(tmp_path, monkeypatch, running):
    birth=timestamp_ns('2026-10-13T00:00:00Z')
    monkeypatch.setattr(upload_audit,'creation_time',lambda info:birth)
    log_ids=[]
    def handle(request):
        path=request.url.path
        if '/datasets/' in path:
            return httpx.Response(200,json={'data':{'id':int(path.split('/')[-2])}})
        dataset=int(request.url.params['dataset'])
        if path.endswith('/dataset-jobs/'):
            return httpx.Response(200,json={'data':[{'id':dataset,'type':1,'status':2 if running else 3,
                'utc_create':'2026-10-10T00:00:00Z','utc_modify':'2026-10-14T00:00:00Z'}]},
                headers={'x-total':'1','x-next-page':''})
        log_ids.append(dataset)
        return httpx.Response(200,json={'data':{'result':[{'values':[
            [str(timestamp_ns('2026-10-14T00:00:00Z')),'视频无后缀名称:truck']]}],'has_more':False,'index':0}})
    store=JobStore(tmp_path/'audit.sqlite3'); client=AthenaClient(httpx.MockTransport(handle)); client.access='test-only'
    manager=upload_audit.UploadAuditManager(store,client)
    root=tmp_path/'videos'; root.mkdir(); (root/'truck.mp4').write_bytes(b'video')
    try:
        manager.start(str(root)); await manager.task
        state=manager.state()
        assert state['run']['skippedBeforeCreation']==0 and state['run']['totalLogs']==2
        assert log_ids==[27912,27944]
        assert all(value['status']==('in_progress' if running else 'uploaded')
                   for value in state['report']['files'][0]['targets'].values())
    finally:
        await manager.shutdown(); store.close()
