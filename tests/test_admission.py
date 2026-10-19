"""Admission preferences are tested against isolated source/capture platforms."""
import asyncio
import copy
from datetime import datetime, timedelta

import httpx
import pytest
import pytest_asyncio

from app.domain import apply_admission, approval_issue, normalize_record
from app.main import create_app
from app.models import AdmissionRule, SearchFilters
from app.remote import RemoteClient
from test_simple_flow import FakePlatform, wait_until


DEFAULT = AdmissionRule().model_dump()
FILTERS = {'dateFrom': '2026-09-30', 'dateTo': '2026-09-30'}


@pytest_asyncio.fixture
async def admission_app(tmp_path):
    platform = FakePlatform()
    facts = {'plate': '鲁A12345', 'seconds': 720, 'photos': [], 'details': 0}
    def source_response(request):
        if request.url.path.endswith('/intelligence/intelliTaskInfo/page'):
            return httpx.Response(200, json={'success': True, 'data': {'total': 1,
                'records': [{'flowCode': 'F001', 'carNumber': facts['plate'], 'createTime': '2026-09-30 01:00:00'}]}})
        if request.url.path.endswith('/intelligence/intelliTaskInfo/getCheckDetail'):
            facts['details'] += 1
            end = datetime(2026,9,30,1,50) + timedelta(seconds=facts['seconds'])
            return httpx.Response(200, json={'success': True, 'data': {
                'stationResults': [{'checkStartTime': '2026-09-30 01:50:00',
                    'checkCompleteTime': end.strftime('%Y-%m-%d %H:%M:%S')}],
                'totalCheckResult': {'allOriginImageUrls': facts['photos']}}})
        return httpx.Response(404)
    applications = []
    def reopen():
        source = RemoteClient('source', httpx.MockTransport(source_response))
        target = RemoteClient('target', httpx.MockTransport(platform.handle))
        source.connected = target.connected = True
        app = create_app(source, target, tmp_path / 'isolated-data')
        applications.append((app,source,target))
        return app
    app = reopen()
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://localhost') as client:
        yield app,client,platform,facts,reopen
    for app,source,target in reversed(applications):
        await app.state.uploads.shutdown()
        await app.state.audit_moves.shutdown()
        await app.state.videos.shutdown()
        await app.state.jobs.shutdown()
        await app.state.upload_audit.shutdown()
        await source.close();await target.close();app.state.store.close()


async def query(app, client):
    response = await client.post('/api/search', json={'filters': FILTERS})
    assert response.status_code == 202
    identity = response.json()['id']
    for _ in range(50):
        result = (await client.get(f'/api/search/{identity}')).json()
        if result['status'] != 'running':
            break
        await asyncio.sleep(.01)
    assert result['status'] == 'complete'
    return result


async def save(client, rule, search_id=None):
    before = (await client.get('/api/settings/admission')).json()
    response = await client.post('/api/settings/admission', json={'rule': {**DEFAULT, **rule},
        'expectedRevision': before['revision'], 'searchId': search_id})
    assert response.status_code == 200, response.text
    return response.json()


@pytest.mark.parametrize('real,photos,options,allowed', [
    (True,0,{},True), (False,1,{},True), (False,0,{},False),
    (True,0,{'matchMode':'all'},False), (False,2,{'matchMode':'all'},False),
    (True,2,{'matchMode':'all','minPhotoCount':2},True),
    (False,2,{'minPhotoCount':3},False), (True,0,{'minPhotoCount':3},True),
    (True,9,{'requireRealPlate':False,'minPhotoCount':10},False),
    (False,3,{'requireRealPlate':False,'minPhotoCount':3},True),
    (False,9,{'requireIntelligentPhotos':False},False),
    (True,0,{'requireIntelligentPhotos':False},True),
    (False,0,{'requireRealPlate':False,'requireIntelligentPhotos':False},True),
    (True,0,{'requireRealPlate':False,'requireIntelligentPhotos':False,'matchMode':'all'},True),
])
def test_selected_identity_conditions_and_minimum_photo_count(real, photos, options, allowed):
    record = {'plate':'鲁A12345' if real else 'VIRTUAL', 'photoCount':photos, 'inspectionDurationSeconds':720}
    assert bool(not approval_issue(record, AdmissionRule(**options))) == allowed


@pytest.mark.parametrize('seconds,threshold,allowed', [
    (599.9,10,False), (600,10,False), (600.1,10,True),
    (690,11.5,False), (690.1,11.5,True), (0,0,False), (1,0,True),
    (None,0,False), (True,0,False), (float('inf'),0,False), (float('nan'),0,False),
])
def test_duration_is_always_required_and_strict_even_when_other_conditions_are_disabled(seconds, threshold, allowed):
    rule = AdmissionRule(requireRealPlate=False,requireIntelligentPhotos=False,minInspectionMinutes=threshold)
    assert (not approval_issue({'inspectionDurationSeconds':seconds}, rule)) == allowed


def test_reclassification_preserves_source_failures_and_counts_only_valid_unique_intelligent_photos():
    raw = {'flowCode':'F1','carNumber':'VIRTUAL','checkStartTime':'2026-09-30 01:00:00',
        'checkCompleteTime':'2026-09-30 01:11:00'}
    details = {'totalCheckResult':{'allOriginImageUrls':['a.jpg',' a.jpg ','b.jpg','','null',None]},
        'manualPhotos':['other.jpg']}
    record = normalize_record(raw, details, SearchFilters(**FILTERS))
    assert record['photoCount'] == 2 and 'issue' not in record
    refused = apply_admission(record, AdmissionRule(requireRealPlate=False,minPhotoCount=3))
    assert '不足3张' in refused['issue']
    assert 'issue' not in apply_admission(refused, AdmissionRule(requireRealPlate=False,minPhotoCount=2))
    failed = normalize_record(raw, details, SearchFilters(**FILTERS), issue='详情读取失败：缺少完整信息')
    relaxed = apply_admission(failed, AdmissionRule(requireRealPlate=False,requireIntelligentPhotos=False,minInspectionMinutes=0))
    assert relaxed['issue'] == failed['issue']


@pytest.mark.asyncio
async def test_settings_default_persistence_conflict_and_no_history_rewrites(admission_app):
    app,client,platform,facts,reopen = admission_app
    initial = (await client.get('/api/settings/admission')).json()
    assert initial['rule'] == DEFAULT and initial['revision'] == 0
    saved = await save(client, {'matchMode':'all','minInspectionMinutes':11.5,'minPhotoCount':3})
    assert saved['revision'] == 1 and saved['search'] is None
    assert '严格超过 11.5 分钟' in saved['summary']
    stale = await client.post('/api/settings/admission',json={'rule':DEFAULT,'expectedRevision':0})
    assert stale.status_code == 409
    again = await save(client, saved['rule'])
    assert again['revision'] == 1
    second = reopen()
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=second),base_url='http://localhost') as other:
        assert (await other.get('/api/settings/admission')).json()['rule'] == saved['rule']
    assert not platform.parents and not app.state.jobs.store.list() and not app.state.videos.store.list()


@pytest.mark.asyncio
async def test_explicitly_saved_defaults_are_persisted_once(admission_app):
    app,client,_,_,_ = admission_app
    first = await save(client,DEFAULT)
    assert first['revision']==1
    assert app.state.store.db.execute('SELECT COUNT(*) FROM simple_admission_settings').fetchone()[0]==1
    assert (await save(client,DEFAULT))['revision']==first['revision']


@pytest.mark.asyncio
@pytest.mark.parametrize('patch', [
    {'minInspectionMinutes':-1}, {'minInspectionMinutes':'10'}, {'minInspectionMinutes':True},
    {'minPhotoCount':0}, {'minPhotoCount':1.5}, {'minPhotoCount':True}, {'requireRealPlate':'no'},
    {'matchMode':'unexpected'}, {'otherCondition':True},
])
async def test_invalid_settings_do_not_change_rule_or_create_tasks(admission_app, patch):
    app,client,platform,_,_ = admission_app
    response = await client.post('/api/settings/admission',json={'rule':{**DEFAULT,**patch},'expectedRevision':0})
    assert response.status_code == 422
    assert (await client.get('/api/settings/admission')).json()['rule'] == DEFAULT
    assert not platform.parents and not app.state.jobs.store.list()


@pytest.mark.asyncio
async def test_apply_reclassifies_cached_facts_without_reading_remote_details_again(admission_app):
    app,client,_,facts,_ = admission_app
    result = await query(app,client)
    assert not result['records'][0].get('issue')
    reads = facts['details']
    strict = await save(client, {'matchMode':'all','minPhotoCount':3},result['id'])
    assert '不足3张' in strict['search']['records'][0]['issue']
    relaxed = await save(client, {'requireIntelligentPhotos':False,'minInspectionMinutes':11.5},result['id'])
    assert not relaxed['search']['records'][0].get('issue')
    assert facts['details'] == reads
    plan = (await client.post('/api/preview',json={'searchId':result['id'],'recordIds':['F001']})).json()
    assert plan['tasks'][0]['status'] == 'ready' and plan['admissionRule'] == relaxed['rule']
    assert plan['tasks'][0]['admissionRule'] == relaxed['rule']


@pytest.mark.asyncio
async def test_changed_rule_blocks_stale_search_and_unsubmitted_preview(admission_app):
    app,client,platform,_,_ = admission_app
    result = await query(app,client)
    body = {'searchId':result['id'],'recordIds':['F001']}
    plan = (await client.post('/api/preview',json=body)).json()
    await save(client,{'matchMode':'all'})
    assert (await client.post('/api/preview',json=body)).status_code == 409
    response = await client.post('/api/jobs',json={'planId':plan['id']})
    assert response.status_code == 409 and not platform.parents
    assert not app.state.jobs.store.list()
    invalid_search = await client.post('/api/settings/admission',json={'rule':DEFAULT,'expectedRevision':1,'searchId':'expired'})
    assert invalid_search.status_code == 409
    assert (await client.get('/api/settings/admission')).json()['rule']['matchMode'] == 'all'


@pytest.mark.asyncio
async def test_lowered_rule_is_used_for_submission_and_later_manual_corrections(admission_app):
    app,client,platform,facts,_ = admission_app
    facts.update(plate='未识别车牌',seconds=420,photos=[])
    relaxed = await save(client,{'requireRealPlate':False,'requireIntelligentPhotos':False,'minInspectionMinutes':5})
    result = await query(app,client)
    assert not result['records'][0].get('issue')
    plan = (await client.post('/api/preview',json={'searchId':result['id'],'recordIds':['F001']})).json()
    assert plan['tasks'][0]['status'] == 'ready' and plan['tasks'][0]['videoName']=='VIRTUAL_001'
    response = await client.post('/api/jobs',json={'planId':plan['id']})
    assert response.status_code == 202
    job_id = response.json()['id']
    await wait_until(lambda:not app.state.jobs.running)
    app.state.videos.sync_jobs()
    await app.state.videos.check_once()
    row = app.state.videos.store.list()[0]
    assert len(platform.parents)==1 and row['admissionRule']==relaxed['rule']
    original = copy.deepcopy(row)
    await save(client,DEFAULT)
    # Existing batches and replayed submissions keep their original approved snapshot.
    assert app.state.videos.store.get(row['id'])==original
    replay = await client.post('/api/jobs',json={'planId':plan['id']})
    assert replay.status_code==202 and replay.json()['id']==job_id and len(platform.parents)==1
    correction = await client.post(f"/api/videos/{row['id']}/corrections",json={
        'startTime':'2026-09-30 02:10:00','durationSeconds':1800,'expectedRevision':0,'requestId':'new-manual-correction'})
    assert correction.status_code==202, correction.text
    assert len(platform.parents)==2
    changed = app.state.videos.store.get(row['id'])
    assert changed['admissionRule']==relaxed['rule'] and changed['revision']==1
    assert changed['versions'][0]==original['versions'][0]
