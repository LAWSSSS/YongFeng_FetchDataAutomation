"""Isolated workflow and file-management acceptance; never uses live services."""
import asyncio
import copy
from pathlib import Path

import httpx
import pytest

from app.main import create_app
from app.slicing import source_version
from app.upload_audit import AuditError
from app.videos import Conflict
from tests.test_slicing import FakeSlicingRemote
from tests.test_slicing_performance import close_app
from tests.test_slicing_recovery import item_fixture


@pytest.fixture
def environment(tmp_path, monkeypatch):
    app = create_app(data_dir=tmp_path / 'db')
    manager = app.state.slicing
    manager.auth.access = 'isolated'
    manager.remote = FakeSlicingRemote()
    manager.store.settings.update(directories=[str(tmp_path)], targetDatasetIds={'27912':'900','27944':'901'})
    manager.store.save_settings()
    monkeypatch.setattr(manager, '_wake_scheduler', lambda: None)
    return app, manager


def pair(manager, tmp_path, states=('pending', 'pending')):
    bed = item_fixture(tmp_path, 'bed', states[0])
    bed.update(errorCode='', sourceVideoId='101')
    bed['categories']['bed'].update(errorCode='', state=states[0], actualImages=151 if states[0] == 'complete' else None)
    magnet = copy.deepcopy(bed)
    magnet.update(id='magnet', sourceDatasetId='27944', sourceVideoId='102', state=states[1])
    magnet['categories']['bed'] = {'state':'not_applicable'}
    magnet['categories']['magnet'] = {'state':states[1], 'intervals':[], 'actualImages':150 if states[1] == 'complete' else None}
    manager.store.save('items', bed)
    manager.store.save('items', magnet)
    return bed, magnet


@pytest.mark.asyncio
async def test_execute_all_replays_and_keeps_batch_identity(environment, tmp_path, monkeypatch):
    app, manager = environment
    monkeypatch.setattr('app.slicing.ffprobe_file', lambda *args: {'duration':60, 'frameRate':25, 'width':3840,'height':2160})
    bed, magnet = pair(manager, tmp_path)
    run = {'id':'original-run', 'requestId':'old', 'state':'complete','items':['bed','magnet'], 'settingsRevision':0}
    manager.store.save('runs',run)
    manager.store.settings['latestRunId'] = run['id']
    manager.remote.video_rows = {'27912':[{'id':101,'name':bed['name'],'cutStatus':0}],
                                  '27944':[{'id':102,'name':bed['name'],'cutStatus':0}]}
    await manager.execute_all('one-click',0)
    await manager.execute_all('one-click',0)
    assert len(manager.store.list('runs')) == 1
    assert await manager._workflow_scan()
    assert manager.run['id'] == 'original-run'
    assert manager.run['executeAll'] and not manager.run['pilotMode']
    assert {item['id'] for item in manager.store.list('items')} == {'bed','magnet'}
    assert manager.remote.created == 0
    await manager.execute_all('another-click',0)
    assert await manager._workflow_scan()
    assert len(manager.store.list('items')) == 2
    with pytest.raises(Conflict):
        await manager.execute_all('stale',4)
    await close_app(app)


@pytest.mark.asyncio
async def test_execute_all_waits_without_login_and_recovers_scan(environment):
    app, manager = environment
    manager.auth.access = ''
    await manager.execute_all('offline',0)
    assert manager.store.settings['workflow']['phase'] == 'scan'
    assert manager.store.settings['enabled']
    manager.auth.access = 'isolated'
    assert await manager._workflow_scan()
    assert manager.store.settings['workflow']['phase'] == 'processing'
    await close_app(app)


@pytest.mark.asyncio
async def test_refresh_scan_waits_for_analysis_and_does_not_overwrite_it(environment):
    app, manager = environment
    blocker = asyncio.Event()
    manager.analysis_task = asyncio.create_task(blocker.wait())
    await manager.execute_all('queued-scan',0)
    assert not await manager._workflow_scan()
    assert not manager.store.list('runs')
    blocker.set()
    await manager.analysis_task
    assert await manager._workflow_scan()
    await close_app(app)


@pytest.mark.asyncio
async def test_shared_worker_receives_both_models_and_refines_only_missing_points(environment, tmp_path, monkeypatch):
    app, manager = environment
    bed, magnet = pair(manager, tmp_path)
    monkeypatch.setattr(manager, '_model_fingerprint', lambda name: name)
    calls = []
    async def worker(work, metadata, points, keys, phase):
        calls.append((copy.deepcopy(points),phase))
        for name, values in points.items():
            for value in values:
                manager.store.save('observations', {'id':keys[name][value], 'time':value, 'label':'target','score':.9})
    monkeypatch.setattr(manager, '_run_worker', worker)
    await manager._process_shared(bed,[bed,magnet])
    assert set(calls[0][0]) == {'bed','magnet'}
    # Short positive video is refined to 10s. 0,30,tail already cached.
    assert set(calls[1][0]) == {'magnet'}
    assert not set(calls[0][0]['magnet']).intersection(calls[1][0]['magnet'])
    assert manager.store.get('items','bed')['categories']['magnet']['state'] == 'not_applicable'
    assert manager.store.get('items','magnet')['categories']['bed']['state'] == 'not_applicable'
    assert manager.store.get('items','bed')['state'] == 'platform_processing'
    assert manager.store.get('items','magnet')['state'] == 'platform_processing'
    await close_app(app)


@pytest.mark.asyncio
async def test_single_model_failure_does_not_reject_other_category(environment,tmp_path,monkeypatch):
    app, manager = environment
    bed, magnet = pair(manager,tmp_path)
    monkeypatch.setattr(manager,'_model_fingerprint',lambda name:name)
    async def worker(work, metadata, points, keys, phase):
        work['categories']['bed'].update(state='attention',errorCode='bed_alignment_uncertain')
        for value in points.get('magnet',[]):
            manager.store.save('observations',{'id':keys['magnet'][value],'time':value,'label':'clear'})
    monkeypatch.setattr(manager,'_run_worker',worker)
    await manager._process_shared(bed,[bed,magnet])
    assert manager.store.get('items','bed')['state'] == 'attention'
    assert manager.store.get('items','magnet')['state'] == 'no_target'
    await app.state.files.tick()
    assert Path(bed['path']).exists()
    assert not (tmp_path/'切片完成').exists()
    await close_app(app)


@pytest.mark.asyncio
@pytest.mark.parametrize('states,folder',[(('complete','complete'),'切片完成'),(('complete','no_target'),'切片完成'),(('no_target','no_target'),'未检出目标')])
async def test_archive_after_both_categories_updates_all_bindings(environment,tmp_path,states,folder):
    app, manager = environment
    root=tmp_path/'视频';original=root/'已上传'/'day';original.mkdir(parents=True)
    bed,magnet=pair(manager,original,states)
    manager.store.settings['directories']=[str(root)]
    files=app.state.files
    await files.tick()
    destination=root/'已上传'/folder/'day'/'bed.mp4'
    assert destination.exists() and not Path(bed['path']).exists()
    for identity in ('bed','magnet'):
        item=manager.store.get('items',identity)
        assert item['path']==str(destination)
        assert item['fileVersion']==source_version(destination)
        assert item['observationVersion']==bed['fileVersion']
    assert files.state()['counts']['complete']==1
    await files.tick()
    assert len(files.rows('file_moves'))==1
    await close_app(app)


@pytest.mark.asyncio
async def test_archive_conflict_never_overwrites_and_lease_defers(environment,tmp_path):
    app,manager=environment
    bed,magnet=pair(manager,tmp_path,('complete','complete'))
    files=app.state.files
    with manager.leases.read(bed['path']):
        await files.tick()
        assert files.rows('files')[0]['archiveState']=='waiting'
    destination=tmp_path/'切片完成'/'bed.mp4';destination.parent.mkdir();destination.write_bytes(b'other')
    await files.tick()
    assert files.rows('files')[0]['archiveState']=='attention'
    assert Path(bed['path']).exists() and destination.read_bytes()==b'other'
    destination.unlink()
    await files.retry(files.rows('files')[0]['id'])
    assert destination.exists() and files.state()['counts']['complete']==1
    await close_app(app)


@pytest.mark.asyncio
async def test_classification_move_updates_slicing_and_preserves_observation_keys(environment,tmp_path):
    app,manager=environment
    bed,magnet=pair(manager,tmp_path)
    old=manager._observation_key(bed,'bed',30,'model')
    destination=tmp_path/'new.mp4'
    Path(bed['path']).rename(destination)
    manager.leases.notify_move(bed['path'],str(destination),{})
    new=manager.store.get('items','bed')
    assert new['path']==str(destination)
    assert manager._observation_key(new,'bed',30,'model')==old
    await close_app(app)


@pytest.mark.asyncio
async def test_archive_restart_after_publish_reconciles_without_second_move(environment,tmp_path,monkeypatch):
    app,manager=environment
    bed,magnet=pair(manager,tmp_path,('complete','complete'))
    files=app.state.files
    original=app.state.audit_moves.update_bindings
    def interrupt(journal):
        raise asyncio.CancelledError('restart after publish')
    monkeypatch.setattr(app.state.audit_moves,'update_bindings',interrupt)
    with pytest.raises(asyncio.CancelledError):
        await files.tick()
    journal=files.rows('file_moves')[0]
    assert Path(journal['destination']).exists()
    monkeypatch.setattr(app.state.audit_moves,'update_bindings',original)
    await files.tick()
    assert files.rows('file_moves')[0]['state']=='complete'
    assert files.state()['counts']['complete']==1
    await close_app(app)


@pytest.mark.asyncio
async def test_real_identity_errors_are_never_retried(environment,tmp_path,monkeypatch):
    app,manager=environment
    bed,_=pair(manager,tmp_path)
    async def failure(item):
        item.update(state='attention',errorCode='media_incomplete')
        manager.store.save('items',item)
    monkeypatch.setattr(manager,'_process_item',failure)
    current=await manager.process_with_recovery(bed)
    assert current['state']=='attention' and not current.get('automaticRetries')
    await close_app(app)


@pytest.mark.asyncio
async def test_transient_failures_stop_after_two_retries(environment,tmp_path,monkeypatch):
    app,manager=environment
    bed,_=pair(manager,tmp_path)
    async def failure(item):
        item.update(state='attention',errorCode='worker_stalled')
        manager.store.save('items',item)
    monkeypatch.setattr(manager,'_process_item',failure)
    for _ in range(3):
        bed=await manager.process_with_recovery(bed)
    assert bed['automaticRetries']==2 and bed['state']=='attention'
    await close_app(app)


@pytest.mark.asyncio
async def test_execute_all_http_endpoint_validates_revision_and_request(environment):
    app,manager=environment
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url='http://127.0.0.1') as client:
        response=await client.post('/api/slicing/execute-all',json={'requestId':'http','expectedRevision':0})
        assert response.status_code==202
        replay=await client.post('/api/slicing/execute-all',json={'requestId':'http','expectedRevision':0})
        assert replay.status_code==202
        stale=await client.post('/api/slicing/execute-all',json={'requestId':'stale-http','expectedRevision':1})
        assert stale.status_code==409
        assert (await client.get('/api/files')).status_code==200
    await close_app(app)


@pytest.mark.asyncio
async def test_full_backend_workflow_two_targets_unique_tasks_lost_response_and_archive(environment,tmp_path,monkeypatch):
    """Drive real orchestration + SQLite + filesystem through an isolated platform."""
    app,manager=environment
    remote=manager.remote
    root=tmp_path/'videos';root.mkdir()
    for number in range(3):
        path=root/f'video-{number}.mp4';path.write_bytes(f'video-{number}'.encode())
        for offset,dataset in enumerate(('27912','27944')):
            remote.video_rows[dataset].append({'id':100+number*2+offset,'name':path.name,'cutStatus':0})
    manager.store.settings['directories']=[str(root)]
    monkeypatch.setattr('app.slicing.ffprobe_file',lambda *args:{'duration':180.0,'frameRate':25,'width':3840,'height':2160})
    monkeypatch.setattr(manager,'_model_fingerprint',lambda name:name)
    calls=[]
    async def worker(work,metadata,points,keys,phase):
        calls.append(set(points))
        for name,values in points.items():
            for value in values:
                manager.store.save('observations',{'id':keys[name][value],'time':value,'label':'target','score':.9})
    monkeypatch.setattr(manager,'_run_worker',worker)
    receipt=[]
    async def create(source,video,target,intervals,seconds,prefix,request):
        # Assert both locally and externally active tasks respect target slots.
        assert not any(task['status']==1 for dataset in remote.job_rows.values() for task in dataset['21']
                       if str(task['config']['target']['dataset']['id'])==str(target))
        remote.created+=1
        task={'id':str(9000+remote.created),'type':21,'status':1,
              'config':{'source':{'datasets':[{'videos':{'ids':[int(video)]}}]},'target':{'dataset':{'id':int(target)}},
                        'options':{'slice_periods':intervals,'image':{'interval':seconds,'prefix':prefix},'sliceImage':True,'sliceVideo':False}}}
        remote.job_rows[source]['21'].append(task);receipt.append((video,target,prefix))
        if remote.created==1:
            raise AuditError('accepted but response lost','uncertain')
        return task
    remote.create_cut=create
    async def images(dataset,**kwargs):
        prefix=kwargs.get('prefix')
        match=next((entry for entry in receipt if str(entry[1])==str(dataset) and entry[2]==prefix),None)
        if not match:return []
        task=next(task for source in remote.job_rows.values() for task in source['21']
                  if task['config']['options']['image']['prefix']==prefix)
        return [{'id':f'{task["id"]}-{index}','name':prefix+f'_{index}.jpg','taskId':task['id']} for index in range(150)] if task['status']==3 else []
    remote.image_rows=images
    clock=[1000.0];manager.clock=lambda:clock[0]
    await manager.execute_all('end-to-end',0)
    assert await manager._workflow_scan()
    first_run=manager.run['id']
    for cycle in range(20):
        await manager._tick()
        if manager.analysis_task:
            await manager.analysis_task
        if app.state.files.task:
            await app.state.files.task
        # Leave the first tasks waiting while the next file is analyzed.
        if cycle>2:
            for source in remote.job_rows.values():
                for task in source['21']:task['status']=3
        clock[0]+=31
        if not manager.store.settings['enabled']:break
    assert remote.created==6 and len(set(entry[:2] for entry in receipt))==6
    assert all(item['state']=='complete' for item in manager.store.list('items'))
    assert manager.run['id']==first_run
    assert all(call=={'bed','magnet'} for call in calls)
    assert len(list((root/'切片完成').glob('*.mp4')))==3
    assert app.state.files.state()['counts']['complete']==3
    # Refresh after completion: same task and file IDs, no second submission.
    await manager.execute_all('end-to-end-again',0)
    assert await manager._workflow_scan()
    await manager._tick()
    assert remote.created==6 and manager.run['id']==first_run
    await close_app(app)


@pytest.mark.asyncio
async def test_snapshot_migration_retains_all_127_receipts_and_observations(tmp_path):
    import sqlite3
    data=tmp_path/'snapshot';data.mkdir()
    db=sqlite3.connect(data/'collection.sqlite3')
    for table in ('items','observations'):
        db.execute(f'CREATE TABLE simple_slicing_{table} (id TEXT PRIMARY KEY,payload TEXT NOT NULL)')
    import json
    rows=[]
    for number in range(127):
        row={'id':str(number),'revision':4,'sourceDatasetId':'27912','sourceVideoId':str(1000+number),
             'state':'complete' if number<2 else 'pending','path':'','fileVersion':{'size':number},
             'categories':{'bed':{'state':'complete' if number<2 else 'pending','taskId':str(5000+number) if number<2 else ''},
                           'magnet':{'state':'not_applicable'}}}
        rows.append(row)
        db.execute('INSERT INTO simple_slicing_items VALUES(?,?)',(row['id'],json.dumps(row)))
    observation={'id':'valid','label':'target','time':30,'score':.98}
    db.execute('INSERT INTO simple_slicing_observations VALUES(?,?)',('valid',json.dumps(observation)))
    db.commit();db.close()
    app=create_app(data_dir=data)
    assert app.state.slicing.store.list('items')==rows
    assert app.state.slicing.store.get('observations','valid')==observation
    assert not app.state.slicing.store.settings['enabled']
    await close_app(app)


def test_overlapping_directories_do_not_create_false_filename_ambiguity(tmp_path):
    from app.slicing import scan_local_files
    child=tmp_path/'child';child.mkdir();(child/'one.mp4').write_bytes(b'video')
    files,roots,issues=scan_local_files([str(tmp_path),str(child)])
    assert len(files)==1 and not issues


@pytest.mark.asyncio
async def test_unavailable_disk_scan_waits_and_resumes_without_new_batch(environment,monkeypatch):
    app,manager=environment
    monkeypatch.setattr('app.slicing.scan_local_files',lambda roots:([],[],[{'path':roots[0],'message':'disconnected'}]))
    await manager.execute_all('disk-offline',0)
    assert not await manager._workflow_scan()
    identity=manager.run['id']
    assert manager.run['state']=='waiting_connection' and manager.store.settings['enabled']
    monkeypatch.setattr('app.slicing.scan_local_files',lambda roots:([],roots,[]))
    assert await manager._workflow_scan()
    assert manager.run['id']==identity
    await close_app(app)


@pytest.mark.asyncio
async def test_internal_failure_is_journaled_for_one_video_not_the_queue(environment,tmp_path,monkeypatch):
    app,manager=environment
    bed,magnet=pair(manager,tmp_path)
    async def broken(item):
        raise RuntimeError('not-for-user-or-logs')
    monkeypatch.setattr(manager,'_process_item',broken)
    result=await manager.process_with_recovery(bed)
    assert result['state']=='attention' and result['errorCode']=='internal_error'
    assert manager.store.get('items','magnet')['state']=='pending'
    await close_app(app)


@pytest.mark.asyncio
async def test_refinement_failure_never_approves_coarse_magnet_result(environment,tmp_path,monkeypatch):
    app,manager=environment
    bed,magnet=pair(manager,tmp_path)
    monkeypatch.setattr(manager,'_model_fingerprint',lambda name:name)
    async def worker(work,metadata,points,keys,phase):
        if phase=='refine':raise OSError('temporary worker start failure')
        for name,values in points.items():
            for pos in values:manager.store.save('observations',{'id':keys[name][pos],'time':pos,'label':'clear' if name=='magnet' else 'target'})
    monkeypatch.setattr(manager,'_run_worker',worker)
    await manager._process_shared(bed,[bed,magnet])
    assert manager.store.get('items','bed')['state']=='platform_processing'
    failed=manager.store.get('items','magnet')
    assert failed['state']=='attention' and failed['categories']['magnet']['state']=='attention'
    await close_app(app)


@pytest.mark.asyncio
async def test_manual_reclassification_rearchives_from_empty_to_completed(environment,tmp_path):
    app,manager=environment
    pair(manager,tmp_path,('no_target','no_target'))
    files=app.state.files
    await files.tick()
    assert (tmp_path/'未检出目标'/'bed.mp4').exists()
    bed=manager.store.get('items','bed');bed['categories']['bed'].update(state='complete',actualImages=150)
    bed['state']='complete';manager.store.save('items',bed)
    await files.tick()
    assert not (tmp_path/'未检出目标'/'bed.mp4').exists()
    assert (tmp_path/'切片完成'/'bed.mp4').exists()
    assert len(files.rows('file_moves'))==2
    assert files.rows('files')[0]['archiveKind']=='complete'
    await close_app(app)


@pytest.mark.asyncio
async def test_click_during_inventory_schedules_one_follow_up_scan(environment,monkeypatch):
    app,manager=environment
    entered=asyncio.Event();release=asyncio.Event();scans=[]
    original=manager._scan
    async def delayed(identity):
        scans.append(identity)
        entered.set();await release.wait();await original(identity)
    monkeypatch.setattr(manager,'_scan',delayed)
    await manager.execute_all('first-scan',0)
    running=asyncio.create_task(manager._workflow_scan())
    await entered.wait()
    await manager.execute_all('second-scan',0)
    await manager.execute_all('third-scan',0)
    release.set()
    assert not await running
    assert manager.store.settings['workflow']['phase']=='scan'
    assert await manager._workflow_scan()
    assert len(scans)==2 and scans[0]==scans[1]
    assert manager.store.settings['workflow']['phase']=='processing'
    await close_app(app)


@pytest.mark.asyncio
async def test_cross_volume_archive_updates_collection_upload_and_slicing_together(environment,tmp_path,monkeypatch):
    from app.audit_moves import fingerprint
    from tests.test_audit_moves import registered
    app,manager=environment
    bed,magnet=pair(manager,tmp_path,('complete','complete'))
    path=Path(bed['path'])
    collected=registered(app,path)
    uploaded={'id':'linked-upload','dataset':'27912','name':path.name,'path':str(path),
              'fileVersion':fingerprint(path.stat()),'state':'complete'}
    app.state.uploads.store.save('items',uploaded)
    monkeypatch.setattr('app.file_index.same_device',lambda *args:False)
    await app.state.files.tick()
    destination=tmp_path/'切片完成'/path.name
    assert destination.read_bytes()==b'immutable file fixture' and not path.exists()
    current=app.state.videos.store.get(collected['id'])
    for field in ('revision','acceptedRevision','activeRevision','requests'):
        assert current[field]==collected[field]
    assert current['versions'][0]['filePath']==str(destination)
    assert current['versions'][0]['startTime']==collected['versions'][0]['startTime']
    upload=app.state.uploads.store.get('items','linked-upload')
    assert upload['path']==str(destination) and upload['state']=='complete'
    assert upload['uploadFileVersion']==uploaded['fileVersion']
    indexed=app.state.files.state()['items'][0]
    assert indexed['links']['slicing']==['bed','magnet']
    assert indexed['links']['uploads']==['linked-upload'] and len(indexed['links']['collection'])==1
    assert not list(destination.parent.glob('*.moving'))
    await close_app(app)


@pytest.mark.asyncio
async def test_isolated_database_never_reads_or_saves_machine_connections(tmp_path,monkeypatch):
    def forbidden(*args,**kwargs):
        pytest.fail('isolated app touched real machine credentials')
    monkeypatch.setattr('app.main.credentials.load',forbidden)
    monkeypatch.setattr('app.main.credentials.save',forbidden)
    monkeypatch.setattr('app.secure_store.load_connection',forbidden)
    app=create_app(data_dir=tmp_path/'offline')
    assert not app.state.upload_audit.client.persist_connection
    async with app.router.lifespan_context(app):
        assert not app.state.upload_audit.client.connected
        assert not app.state.slicing.store.settings['enabled']


@pytest.mark.asyncio
async def test_archive_scheduler_has_no_idle_wakeup_or_busy_file_spin(environment,tmp_path):
    app,manager=environment
    pair(manager,tmp_path)
    files=app.state.files;wakeups=[]
    files.schedule(lambda:wakeups.append(True))
    assert files.task is None and not wakeups
    for item in manager.store.list('items'):
        name='bed' if item['sourceDatasetId']=='27912' else 'magnet'
        item['categories'][name].update(state='complete',actualImages=150);item['state']='complete'
        manager.store.save('items',item)
    with manager.leases.read(str(tmp_path/'bed.mp4')):
        files.schedule(lambda:wakeups.append(True))
        first=files.task;await first;await asyncio.sleep(0)
        assert len(wakeups)==1
        files.schedule(lambda:wakeups.append(True))
        assert files.task is first
    await close_app(app)


@pytest.mark.asyncio
async def test_single_archive_never_moves_unrelated_files(environment,tmp_path):
    app,manager=environment
    first_dir=tmp_path/'one';second_dir=tmp_path/'two';first_dir.mkdir();second_dir.mkdir()
    pair(manager,first_dir,('complete','complete'))
    initial=manager.store.list('items')
    for item in initial:
        item['id']='first-'+item['id'];manager.store.save('items',item)
    # Replace the two fixture IDs with a second independently completed file.
    pair(manager,second_dir,('complete','complete'))
    files=app.state.files
    files.schedule(lambda:None,item_id='first-bed')
    await files.task
    assert (tmp_path/'切片完成'/'one'/'bed.mp4').exists()
    assert (second_dir/'bed.mp4').exists()
    assert not (tmp_path/'切片完成'/'two').exists()
    await close_app(app)


@pytest.mark.asyncio
async def test_rate_limited_archive_keeps_single_run_enabled_until_move_can_start(environment,tmp_path):
    app,manager=environment
    pair(manager,tmp_path,('complete','complete'))
    manager.run={'id':'single','state':'running','singleMode':True,'singleItemId':'bed','settingsRevision':0}
    manager.store.settings['enabled']=True
    manager.store.settings['latestRunId']='single'
    manager.store.save('runs',manager.run)
    app.state.files.next_check=manager.clock()+30
    await manager._tick()
    assert app.state.files.task is None
    assert manager.store.settings['enabled']
    assert app.state.files.state()['counts']['waiting']==1
    await close_app(app)
