"""Organizer reliability: isolated files, exact move receipts, no live platforms."""
import asyncio
import copy
from contextlib import ExitStack
from pathlib import Path

import pytest
import pytest_asyncio

from app.main import create_app
from app import file_locations
from app.slicing import source_version
from tests.test_workspace import pair, enable, proofs
from tests.test_media_control import collection
from tests.test_archive_completion import seed
from tests.test_slicing_performance import close_app


@pytest_asyncio.fixture
async def app(tmp_path):
    value = create_app(data_dir=tmp_path/'db')
    yield value
    await value.state.workspace.shutdown()
    await close_app(value)


async def prepared(app, tmp_path, count=2):
    w=app.state.workspace
    for index in range(count):
        path=pair(w,tmp_path/f'old-{index}',name=f'truck-{index}.mp4')
        collection(app,path,f'video-{index}')
    await enable(w,tmp_path)
    for item in w.slicing.store.list('items'):
        item['state']='no_target'
        for category in item['categories'].values():
            if category['state']!='not_applicable':category['state']='no_target'
        w.slicing.store.save('items',item)
    return w


async def eventually(predicate):
    async def wait():
        while not predicate():await asyncio.sleep(.01)
    await asyncio.wait_for(wait(),2)


@pytest.mark.asyncio
async def test_one_row_and_all_platform_queries_cannot_block_other_video(app,tmp_path):
    w=await prepared(app,tmp_path)
    row_lock=w.videos.row_lock('video-0')
    locks=[row_lock,w.uploads.tick_lock,w.slicing.tick_lock,w.slicing.packages.tick_lock]
    for lock in locks:await lock.acquire()
    try:
        w.request_reconcile(w.config['revision'],'organize')
        await asyncio.wait_for(w.reconcile(cleanup=False),2)
        assets={a['name']:a for a in w.rows('assets')}
        assert assets['truck-0.mp4']['status']=='waiting'
        assert '/已切片/' in assets['truck-1.mp4']['path']
        request=w.get('requests','organize')
        assert request['state']=='waiting' and request['finished']==1 and request['waiting']==1
    finally:
        for lock in reversed(locks):lock.release()
    await w.reconcile(cleanup=False)
    assert w.get('requests','organize')['state']=='complete'


@pytest.mark.asyncio
async def test_release_wakes_only_the_waiting_video_without_another_click(app,tmp_path):
    w=await prepared(app,tmp_path,1)
    source=w.rows('assets')[0]['path']
    guard=ExitStack();guard.enter_context(w.leases.read(source))
    w.request_reconcile(w.config['revision'],'automatic')
    await w.startup()
    try:
        await eventually(lambda:w.get('requests','automatic')['state']=='waiting')
        assert not w.get('requests','automatic').get('completedAt')
        guard.close()
        await eventually(lambda:w.get('requests','automatic')['state']=='complete')
        assert '/已切片/' in w.rows('assets')[0]['path']
    finally:guard.close()


@pytest.mark.asyncio
async def test_request_arriving_during_pass_is_not_lost(app,tmp_path,monkeypatch):
    w=await prepared(app,tmp_path,1)
    entered,release=asyncio.Event(),asyncio.Event()
    original=w.reconcile;calls=0
    async def gated(*args,**kwargs):
        nonlocal calls
        calls+=1
        if calls==1:
            entered.set();await release.wait();return w.state()
        return await original(*args,**kwargs)
    monkeypatch.setattr(w,'reconcile',gated)
    await w.startup();await entered.wait()
    w.request_reconcile(w.config['revision'],'during-pass');release.set()
    await eventually(lambda:w.get('requests','during-pass')['state']=='complete')
    assert calls>=2


@pytest.mark.asyncio
async def test_duplicate_requests_share_active_work_and_restart_resumes_scope(tmp_path):
    app=create_app(data_dir=tmp_path/'db');w=await prepared(app,tmp_path,1)
    w.request_reconcile(w.config['revision'],'original')
    w.request_reconcile(w.config['revision'],'another-click')
    assert w.get('requests','another-click')['canonicalId']=='original'
    await close_app(app)
    app=create_app(data_dir=tmp_path/'db');w=app.state.workspace
    try:
        await w.reconcile(cleanup=False)
        assert w.state()['lastAction']['state']=='complete'
        assert w.get('requests','original')['finished']==1
        assert len(w.rows('assets'))==1 and len(list(w.root.rglob('*.mp4')))==1
    finally:await close_app(app)


@pytest.mark.asyncio
async def test_bad_one_does_not_abort_remaining_files(app,tmp_path,monkeypatch):
    w=await prepared(app,tmp_path)
    original=w.process_move
    async def fail_one(entry):
        if entry['source'].endswith('truck-0.mp4'):raise OSError('fixture error')
        await original(entry)
    monkeypatch.setattr(w,'process_move',fail_one)
    w.request_reconcile(w.config['revision'],'mixed')
    await w.reconcile(cleanup=False)
    result=w.get('requests','mixed')
    assert result['state']=='attention' and result['finished']==1 and result['failed']==1
    assert any('/已切片/' in a['path'] for a in w.rows('assets'))


@pytest.mark.asyncio
async def test_all_late_callbacks_keep_verified_locations_and_new_progress(app,tmp_path):
    seed(app,tmp_path);w=app.state.workspace
    path=tmp_path/'source.mp4'
    upload={'id':'upload','dataset':'27912','name':path.name,'path':str(path),'fileVersion':source_version(path),'state':'importing'}
    w.uploads.store.save('items',upload)
    old_video=copy.deepcopy(w.videos.store.get('collection'))
    old_slice=copy.deepcopy(w.slicing.store.get('items','recovery'))
    old_pack=copy.deepcopy(w.slicing.packages.store.get('items','bed'))
    old_upload=copy.deepcopy(upload)
    await enable(w,tmp_path)
    destination=w.rows('assets')[0]['path']
    assert destination!=str(path)
    old_video['note']='new platform status';w.videos.store.save(old_video)
    old_slice['progress']=99;w.slicing.store.save('items',old_slice)
    old_pack['message']='late import receipt';w.slicing.packages.store.save('items',old_pack)
    old_upload['message']='late remote status';w.uploads.store.save('items',old_upload)
    assert w.videos.store.get('collection')['versions'][0]['filePath']==destination
    assert w.slicing.store.get('items','recovery')['path']==destination
    assert w.slicing.store.get('items','recovery')['progress']==99
    assert w.uploads.store.get('items','upload')['path']==destination
    pack=w.slicing.packages.store.get('items','bed')
    assert str(w.root) in pack['path'] and str(w.root) in pack['imageDir'] and pack['videoPath']==destination
    assert pack['message']=='late import receipt'
    await w.discover();assert len(w.rows('assets'))==1


@pytest.mark.asyncio
async def test_move_chains_and_reused_names_require_full_file_identity(app,tmp_path):
    db=app.state.store.db
    a,b,c=map(str,[tmp_path/'a.mp4',tmp_path/'b.mp4',tmp_path/'c.mp4'])
    version={'device':1,'inode':2,'size':3,'modifiedNs':4,'edgeHash':'content'}
    def move(source,dest):
        file_locations.remember(db,{'source':source,'destination':dest,'expected':version,'destinationVersion':version})
    move(a,b);move(b,a)
    assert file_locations.resolve(db,a,version)[0]==a
    move(a,c)
    assert file_locations.resolve(db,b,version)[0]==c
    assert file_locations.resolve(db,a,{'size':3})[0]==a
    assert file_locations.resolve(db,a,{**version,'inode':99})[0]==a


@pytest.mark.asyncio
async def test_late_callbacks_do_not_clear_cleanup_tombstones(app,tmp_path):
    seed(app,tmp_path);w=app.state.workspace
    old_video=w.videos.store.get('collection');old_item=w.slicing.store.get('items','recovery')
    old_package=w.slicing.packages.store.get('items','bed')
    current=copy.deepcopy(old_video);current['versions'][0]['localCleanupState']='complete';w.videos.store.save(current)
    current=copy.deepcopy(old_item);current['localCleanupState']='complete';w.slicing.store.save('items',current)
    current=copy.deepcopy(old_package);current['localCleanupState']='complete';w.slicing.packages.store.save('items',current)
    w.videos.store.save(old_video);w.slicing.store.save('items',old_item);w.slicing.packages.store.save('items',old_package)
    assert w.videos.store.get('collection')['versions'][0]['localCleanupState']=='complete'
    assert w.slicing.store.get('items','recovery')['localCleanupState']=='complete'
    assert w.slicing.packages.store.get('items','bed')['localCleanupState']=='complete'


@pytest.mark.asyncio
async def test_correction_during_cleanup_verification_retains_original(app,tmp_path):
    w=app.state.workspace
    path=pair(w,tmp_path/'old',('complete','complete'))
    proofs(w,path,tmp_path)
    await enable(w,tmp_path)
    asset=w.rows('assets')[0]
    entered,release=asyncio.Event(),asyncio.Event()
    async def remote(dataset):
        entered.set();await release.wait()
        return [{'id':dataset+'-result','size':asset['fileVersion']['size']}]
    w.uploads.remote.videos=remote
    task=asyncio.create_task(w.cleanup(asset));await entered.wait()
    for item in w.slicing.store.list('items'):
        item['superseded']=True;w.slicing.store.save('items',item)
    release.set();await task
    assert Path(asset['path']).exists()
    assert w.get('assets',asset['id'])['cleanup']['state']=='waiting'


@pytest.mark.asyncio
async def test_already_organized_is_reported_as_no_move_not_silent_success(app,tmp_path):
    w=await prepared(app,tmp_path,1)
    await w.reconcile(cleanup=False)
    before=len(w.rows('moves'))
    w.request_reconcile(w.config['revision'],'again')
    await w.reconcile(cleanup=False)
    request=w.get('requests','again')
    assert request['state']=='complete' and request['movedFiles']==0 and '无需移动' in request['message']
    assert len(w.rows('moves'))==before


@pytest.mark.asyncio
async def test_first_discovery_while_playing_remains_pending_until_release(app,tmp_path):
    w=app.state.workspace
    await enable(w,tmp_path)
    path=tmp_path/'new.mp4';path.write_bytes(b'complete-source')
    collection(app,path,'new')
    with w.leases.read(path):
        w.request_reconcile(w.config['revision'],'new-during-play')
        await w.reconcile(cleanup=False)
        request=w.get('requests','new-during-play')
        assert request['state']=='waiting' and request['waiting']==1
    await w.reconcile(cleanup=False)
    assert w.get('requests','new-during-play')['state']=='complete'
    assert len(w.rows('assets'))==1 and '/采集视频/' in w.rows('assets')[0]['path']


@pytest.mark.asyncio
async def test_disk_reconnect_resumes_the_same_request(app,tmp_path):
    w=await prepared(app,tmp_path,1)
    marker=w.root/'.workspace.json';hidden=w.root/'.marker-off';marker.rename(hidden)
    w.request_reconcile(w.config['revision'],'reconnect')
    await w.startup()
    try:
        await eventually(lambda:w.get('requests','reconnect')['state']=='waiting')
        hidden.rename(marker);w.wake.set()
        await eventually(lambda:w.get('requests','reconnect')['state']=='complete')
        assert len(list(w.root.rglob('*.mp4')))==1
    finally:
        if hidden.exists():hidden.rename(marker)


@pytest.mark.asyncio
async def test_duplicate_file_index_rows_do_not_make_dead_archive_buttons(app,tmp_path):
    seed(app,tmp_path);w=app.state.workspace
    await enable(w,tmp_path)
    files=w.files;files.refresh();original=files.rows('files')[0]
    duplicate=copy.deepcopy(original);duplicate.update(id='duplicate',archiveState='waiting')
    files.save('files',duplicate)
    group=app.state.media_control.state()['groups'][0]
    assert len(group['files'])==1 and group['files'][0]['archiveState']=='complete'
    assert next(r for r in files.rows('files') if r['id']=='duplicate')['aliasOf']==original['id']


@pytest.mark.asyncio
async def test_preserved_history_is_not_read_or_moved_again(app,tmp_path,monkeypatch):
    w=await prepared(app,tmp_path,1)
    await w.preserve_uploaded(w.config['revision'],'retain')
    original=w.rows('assets')[0]['path']
    def no_read(*_a,**_k):raise AssertionError('preserved history must not be read')
    monkeypatch.setattr('app.workspace.file_version',no_read)
    await w.discover()
    assert w.rows('assets')[0]['path']==original and w.is_preserved(w.rows('assets')[0])


@pytest.mark.asyncio
async def test_waiting_remote_cleanup_does_not_block_new_file_arrangement(app,tmp_path,monkeypatch):
    w=await prepared(app,tmp_path,1)
    entered,release=asyncio.Event(),asyncio.Event()
    monkeypatch.setattr(w,'cleanup_eligibility',lambda asset:(True,''))
    async def slow_cleanup(asset):
        entered.set()
        await release.wait()
    monkeypatch.setattr(w,'cleanup',slow_cleanup)
    await w.reconcile(background_cleanup=True)
    await entered.wait()
    path=pair(w,tmp_path/'new-file',name='new-truck.mp4')
    collection(app,path,'new-collection')
    try:
        w.request_reconcile(w.config['revision'],'while-cleaning')
        await asyncio.wait_for(w.reconcile(cleanup=False),2)
        added=next(a for a in w.rows('assets') if a['name']=='new-truck.mp4')
        assert added['status']=='managed' and str(w.root) in added['path']
        assert w.get('requests','while-cleaning')['finished']==1
    finally:
        release.set()
        await w.cleanup_task


@pytest.mark.asyncio
async def test_remounted_disk_updates_exact_versions_without_accepting_content_change(app,tmp_path):
    w=await prepared(app,tmp_path,1)
    await w.reconcile(cleanup=False)
    asset=w.rows('assets')[0]
    old=copy.deepcopy(asset['fileVersion']);old['device']+=500
    asset['fileVersion']=old;w.save('assets',asset)
    for item in w.slicing.store.list('items'):
        item['fileVersion']={**item['fileVersion'],'device':old['device']}
        w.slicing.store.save('items',item)
    await w.discover()
    actual=source_version(Path(asset['path']))
    assert w.get('assets',asset['id'])['fileVersion']==actual
    assert all(i['fileVersion']['device']==actual['device'] for i in w.slicing.store.list('items'))
    # A weak collection receipt (length only) must not hide changed contents.
    for item in w.slicing.store.list('items'):
        item['localCleanupState']='complete';w.slicing.store.save('items',item)
    video=w.videos.store.get('video-0');video['versions'][0]['fileVersion']={'size':actual['size']}
    w.videos.store.save(video)
    Path(asset['path']).write_bytes(b'X'*actual['size'])
    await w.discover()
    changed=w.get('assets',asset['id'])
    assert changed['status']=='attention' and '版本' in changed['message']
    assert changed['fileVersion']==actual
    w.request_reconcile(w.config['revision'],'changed-content')
    await w.reconcile(cleanup=False)
    assert w.get('assets',asset['id'])['status']=='attention'
    assert w.get('requests','changed-content')['state']=='attention'
