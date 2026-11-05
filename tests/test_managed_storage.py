import json
import shutil
import sqlite3
from pathlib import Path

import pytest
from app.managed_storage import ManagedConnection, paths, prepare, PREFIX
from app.store import JobStore
from scripts.reset_managed_data import preserve_sample, retire_database, delete_manifest, collect_manifest
from app.workspace_files import file_version


def test_relative_journal_moves_progress_without_rewriting_unknown_paths(tmp_path):
    root = tmp_path/'程序 中文'; root.mkdir()
    db = sqlite3.connect(root/'test.sqlite3', factory=ManagedConnection); db.initialize(root)
    db.execute('CREATE TABLE things (id TEXT PRIMARY KEY,payload TEXT)')
    with db:
        db.execute('INSERT INTO things VALUES(?,?)', ('a',json.dumps({'path':str(root/'媒体/a.mp4'),'unrelated':'/original/a.mp4','counter':1})))
    stored=json.loads(db.execute("SELECT payload FROM managed_paths WHERE table_name='things'").fetchone()[0])
    assert stored['path']==PREFIX+'媒体/a.mp4'
    with db:
        db.execute('UPDATE things SET payload=?', (json.dumps({'path':str(root/'媒体/a.mp4'),'unrelated':'/original/a.mp4','counter':2}),))
    db.close()
    moved = tmp_path/'moved 文件夹'; root.rename(moved)
    db=sqlite3.connect(moved/'test.sqlite3',factory=ManagedConnection);db.initialize(moved)
    row=json.loads(db.execute('SELECT payload FROM things').fetchone()[0])
    assert row == {'path':str(moved/'媒体/a.mp4'),'unrelated':'/original/a.mp4','counter':2}
    with pytest.raises(RuntimeError):
        with db:
            db.execute('DELETE FROM things');raise RuntimeError('rollback')
    assert db.execute('SELECT count(*) FROM things').fetchone()[0]==1
    db.close()


def test_relative_traversal_and_unknown_media_root_are_rejected(tmp_path):
    with pytest.raises(ValueError):paths(PREFIX+'../escape',tmp_path,False)
    (tmp_path/'媒体').mkdir();(tmp_path/'媒体/unknown.txt').write_text('keep')
    with pytest.raises(ValueError):prepare(tmp_path)
    assert (tmp_path/'媒体/unknown.txt').read_text()=='keep'


def test_external_commit_is_preserved_before_whole_folder_moves(tmp_path):
    root=tmp_path/'old';root.mkdir()
    db=sqlite3.connect(root/'db',factory=ManagedConnection);db.initialize(root)
    db.execute('CREATE TABLE things(id TEXT PRIMARY KEY,payload TEXT)')
    db.execute('INSERT INTO things VALUES(?,?)',('a',json.dumps({'path':str(root/'a'),'progress':1})));db.commit();db.close()
    with sqlite3.connect(root/'db') as other:
        other.execute('UPDATE things SET payload=?',(json.dumps({'path':str(root/'b'),'progress':2}),))
    moved=tmp_path/'新目录 空格';root.rename(moved)
    db=sqlite3.connect(moved/'db',factory=ManagedConnection);db.initialize(moved)
    row=json.loads(db.execute('SELECT payload FROM things').fetchone()[0]);db.close()
    assert row=={'path':str(moved/'b'),'progress':2}


@pytest.mark.asyncio
async def test_managed_storage_rejects_separate_paths_and_legacy_cloud_deletion(tmp_path,monkeypatch):
    import httpx
    from app import runtime
    from app.main import create_app
    from tests.test_slicing_performance import close_app
    root=tmp_path/'程序 中文/数据'
    monkeypatch.setattr(runtime,'data_directory',lambda:root)
    monkeypatch.setattr(runtime,'profile_directory',lambda:root/'系统')
    app=create_app()
    try:
        row={'id':'test','name':'test','revision':0,'activeRevision':0,'versions':[{'revision':0,'state':'waiting'}]}
        app.state.videos.store.save(row)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url='http://127.0.0.1') as client:
            response=await client.post('/api/videos/test/remove-record',json={'expectedRevision':0,'requestId':'new-remove','deleteCloud':True,'deleteLocalFiles':True})
            assert response.status_code==202 and response.json()['operationKind']=='record-delete'
            await app.state.record_deletion.tick()
            assert not app.state.videos.store.get('test').get('deletion')
            assert (await client.post('/api/workspace/preview',json={'parent':str(tmp_path/'missing'),
                'expectedRevision':0,'requestId':'elsewhere'})).status_code==400
            assert (await client.post('/api/settings/directory',json={'path':'/elsewhere'})).status_code==409
            assert (await client.post('/api/videos/test/cancel-clear',json={'requestId':'undo','expectedRevision':0})).status_code==410
        assert Path(app.state.videos.store.directory()).is_relative_to(root)
        assert not (root.parent/'credentials.json').exists()
    finally:
        await close_app(app)


@pytest.fixture
def managed_profile(tmp_path, monkeypatch):
    from app import runtime
    root = tmp_path / '程序 中文' / '数据'
    monkeypatch.setattr(runtime, 'data_directory', lambda: root)
    monkeypatch.setattr(runtime, 'profile_directory', lambda: root / '系统')
    monkeypatch.setenv('IVG_SKIP_SAVED_CONNECTIONS', '1')
    return root


@pytest.mark.asyncio
async def test_choose_media_location_migrates_bound_files_and_resumes_after_remount(
        managed_profile, tmp_path, monkeypatch):
    import httpx
    from app.main import create_app
    from app.file_leases import FileEnvironmentError
    from tests.test_slicing_performance import close_app
    from tests.test_workspace import downloaded_original, pair

    drive = tmp_path / '测试盘 Media'
    drive.mkdir()
    # A folder of the same name is not adopted or overwritten.
    unrelated = drive / '视频工作区' / '保留.txt'
    unrelated.parent.mkdir()
    unrelated.write_text('unrelated')
    app = create_app()
    w, videos, history = app.state.workspace, app.state.videos, app.state.history_downloads
    default = w.root
    original = pair(w, default / '采集视频' / '测试视频', name='测试视频.mp4')
    downloaded_original(w, original, identity='video')
    old = videos.store.get('video')
    old.update(activeRevision=1)
    old['versions'].append({'revision': 1, 'state': 'ready', 'taskId': 'new-task',
                            'resultId': 'new-result', 'downloadRequested': True})
    videos.store.save(old)
    output = default / '.处理中' / '测试视频' / 'bed'
    image, archive = output / '图片' / 'one.jpg', output / '压缩包.zip'
    image.parent.mkdir(parents=True)
    image.write_bytes(b'photo')
    archive.write_bytes(b'photo package')
    bed = next(item for item in w.slicing.store.list('items') if item['sourceDatasetId'] == '27912')
    w.slicing.packages.store.save('items', {'id': 'package', 'itemId': bed['id'], 'category': 'bed',
        'state': 'queued', 'path': str(archive), 'fileVersion': file_version(archive),
        'imageDir': str(image.parent), 'images': [image.name], 'imageVersions': {image.name: file_version(image)}})
    history_file = default / '采集视频' / '测试视频_历史_old-copy' / '测试视频_v1.mp4'
    history_file.parent.mkdir()
    history_file.write_bytes(b'older video')
    history.save({'id': 'old-copy', 'state': 'complete', 'filePath': str(history_file),
                  'fileVersion': file_version(history_file, True)})
    history.save({'id': 'old-resume', 'videoId': 'video', 'state': 'queued', 'version': 0})
    old_work = videos.work
    part = old_work / 'video' / 'v1.part'
    history_part = default / '.缓存' / '历史下载' / 'old-resume' / 'v0.part'
    unknown = default / '采集视频' / '未登记.mp4'
    unknown.write_bytes(b'keep')
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://127.0.0.1') as client:
            preview = await client.post('/api/workspace/preview', json={
                'parent': str(drive), 'expectedRevision': 0, 'requestId': 'external-preview'})
            assert preview.status_code == 200, preview.text
            plan = preview.json()
            destination = drive / '视频工作区_2'
            assert plan['root'] == str(destination) and plan['totalItems'] == 4
            assert not destination.exists()
            # Downloads can finish or receive more bytes between preview and
            # quiescing. The final move plan must include those checkpoints.
            for checkpoint in (part, history_part):
                checkpoint.parent.mkdir(parents=True)
                checkpoint.write_bytes(b'checkpoint')
            with monkeypatch.context() as patch:
                patch.setattr('app.workspace.same_device', lambda *_: False)
                body = {'expectedRevision': 0, 'requestId': 'external-apply'}
                route = f"/api/workspace/{plan['id']}/apply"
                assert (await client.post(route, json=body)).status_code == 202
                await w.task
                replay = await client.post(route, json=body)
                state = replay.json()
            assert state['root'] == str(destination) and not state['fixedLocation']
            assert state['migration']['counts']['complete'] == 6, state['migration']
            assert state['revision'] == 1
            opened = []
            with monkeypatch.context() as patch:
                patch.setattr('subprocess.run', lambda command, **_: opened.append(command))
                assert (await client.post('/api/workspace/open', json={})).status_code == 200
            assert opened == [['/usr/bin/open', str(destination)]]
        saved = Path(videos.store.get('video')['versions'][0]['filePath'])
        assert saved.is_relative_to(destination) and saved.read_bytes() == b'video-version-fixture'
        assert videos.work == destination / '.缓存' / '下载'
        assert (videos.work / 'video' / 'v1.part').read_bytes() == b'checkpoint'
        assert (destination / '.缓存' / '历史下载' / 'old-resume' / 'v0.part').read_bytes() == b'checkpoint'
        assert not any(p.exists() for p in (original, image, archive, history_file, part, history_part))
        photo_package = w.slicing.packages.store.get('items', 'package')
        assert Path(photo_package['path']).is_relative_to(destination)
        assert Path(photo_package['path']).read_bytes() == b'photo package'
        assert (Path(photo_package['imageDir']) / image.name).read_bytes() == b'photo'
        saved_history = next(row for row in history.rows() if row['id'] == 'old-copy')
        assert Path(saved_history['filePath']).is_relative_to(destination)
        assert Path(saved_history['filePath']).read_bytes() == b'older video'
        assert unknown.read_bytes() == b'keep' and unrelated.read_text() == 'unrelated'
        assert (managed_profile / '系统' / 'collection.sqlite3').is_file()
    finally:
        await close_app(app)

    detached = drive.with_name('盘已拔出')
    drive.rename(detached)
    restarted = create_app()
    w, videos = restarted.state.workspace, restarted.state.videos
    try:
        assert w.root == destination and videos.work.is_relative_to(destination)
        with pytest.raises(FileEnvironmentError):
            await w.check_root()
        await videos.publish('video', 1)
        assert videos.store.get('video')['versions'][1]['downloadRequested']
        assert not (old_work / 'video' / 'v1.part').exists()
        detached.rename(drive)
        await w.check_root()
        assert w.download_destination('new.mp4').is_relative_to(destination)
        assert w.output_directory({'path': str(saved), 'id': 'bed', 'name': saved.name}, 'bed').is_relative_to(destination)
        restore = await w.preview('', 1, 'preview-default', use_default=True)
        assert restore['root'] == str(default)
        await w.apply(restore['id'], 1, 'apply-default')
        await w.task
        assert w.config['fixedLocation'] and w.root == default
        assert Path(videos.store.get('video')['versions'][0]['filePath']).is_relative_to(default)
        assert (old_work / 'video' / 'v1.part').read_bytes() == b'checkpoint'
    finally:
        await close_app(restarted)


@pytest.mark.asyncio
async def test_media_location_refuses_replaced_destination_and_keeps_original(managed_profile, tmp_path):
    from app.main import create_app
    from tests.test_slicing_performance import close_app
    from tests.test_workspace import downloaded_original

    app = create_app()
    w = app.state.workspace
    original = w.root / '采集视频' / 'test.mp4'
    original.write_bytes(b'original')
    downloaded_original(w, original)
    selected = tmp_path / 'external'
    selected.mkdir()
    try:
        preview = await w.preview(str(selected), 0, 'preview')
        selected.rename(tmp_path / 'unplugged')
        selected.mkdir()
        await w.apply(preview['id'], 0, 'apply')
        await w.task
        migration = w.state()['migration']
        assert migration['state'] == 'attention' and '更换' in migration['message']
        assert original.read_bytes() == b'original'
        assert not (selected / '视频工作区').exists() and w.config['revision'] == 0
    finally:
        await close_app(app)


@pytest.mark.asyncio
async def test_interrupted_location_change_resumes_same_plan_after_restart(managed_profile, tmp_path):
    from app.main import create_app
    from tests.test_slicing_performance import close_app
    from tests.test_workspace import downloaded_original

    selected = tmp_path / 'external'
    selected.mkdir()
    detached = tmp_path / 'detached'
    app = create_app()
    w = app.state.workspace
    original = w.root / '采集视频' / 'test.mp4'
    original.write_bytes(b'original')
    downloaded_original(w, original)
    try:
        plan = await w.preview(str(selected), 0, 'preview')
        selected.rename(detached)
        await w.apply(plan['id'], 0, 'apply')
        await w.task
        assert w.maintenance and w.config['revision'] == 0
        assert w.state()['migration']['reasonCode'] == 'disk_offline'
        assert original.is_file()
    finally:
        await close_app(app)

    detached.rename(selected)
    restarted = create_app()
    w = restarted.state.workspace
    try:
        await w.startup()
        await w.task
        assert not w.maintenance
        assert w.state()['migration']['id'] == plan['id']
        assert w.state()['migration']['state'] == 'complete'
        assert not original.exists()
        assert (w.root / '采集视频' / 'test.mp4').read_bytes() == b'original'
    finally:
        await w.shutdown()
        await close_app(restarted)


def test_unmounted_volume_is_rejected_before_preparing_target(managed_profile, monkeypatch):
    from app.file_leases import FileEnvironmentError
    from app.workspace_files import location_parent

    monkeypatch.setattr('os.path.ismount', lambda _: False)
    with pytest.raises(FileEnvironmentError) as unavailable:
        location_parent('/Volumes/测试盘/视频工作区')
    assert unavailable.value.code == 'disk_offline'


@pytest.mark.asyncio
@pytest.mark.parametrize(('history', 'has_source'), [(False, True), (False, False), (True, True)])
async def test_foreign_checkpoint_blocks_switch_before_moving_anything(managed_profile, tmp_path, history, has_source):
    from app.main import create_app
    from app.workspace_files import create_root
    from tests.test_slicing_performance import close_app
    from tests.test_workspace import downloaded_original

    app = create_app()
    w = app.state.workspace
    original_root = w.root
    saved_video = original_root / '采集视频' / 'fixture.mp4'
    saved_video.write_bytes(b'saved original')
    downloaded_original(w, saved_video, identity='video')
    if history:
        app.state.history_downloads.save({'id': 'fixture', 'state': 'waiting', 'version': 1})
        relative = Path('.缓存/历史下载/fixture/v1.part')
    else:
        relative = Path('.缓存/下载/video/v0.part')
    if has_source:
        part = original_root / relative
        part.parent.mkdir(parents=True)
        part.write_bytes(b'authorized checkpoint')

    drive = tmp_path / 'external'
    drive.mkdir()
    target = create_root(drive / '视频工作区', w.config['workspaceId'])
    foreign = target / relative
    foreign.parent.mkdir(parents=True)
    foreign.write_bytes(b'foreign checkpoint')
    try:
        preview = await w.preview(str(drive), 0, 'preview')
        await w.apply(preview['id'], 0, 'apply')
        await w.task
        migration = w.state()['migration']
        assert migration['state'] == 'attention' and '下载断点' in migration['message']
        assert w.root == original_root and not w.maintenance
        assert saved_video.read_bytes() == b'saved original'
        assert foreign.read_bytes() == b'foreign checkpoint'
        if has_source:
            assert part.read_bytes() == b'authorized checkpoint'

        foreign.unlink()
        await w.retry(preview['id'], 0, 'try-again')
        await w.task
        assert w.root == target and w.state()['migration']['state'] == 'complete'
        if has_source:
            assert foreign.read_bytes() == b'authorized checkpoint' and not part.exists()
        else:
            assert not foreign.exists()
    finally:
        await close_app(app)


@pytest.mark.asyncio
async def test_checkpoint_is_kept_until_switch_and_can_be_copied_again(managed_profile, tmp_path, monkeypatch):
    from app.main import create_app
    from tests.test_slicing_performance import close_app
    from tests.test_workspace import downloaded_original

    app = create_app()
    w = app.state.workspace
    original_root = w.root
    original = original_root / '采集视频' / 'test.mp4'
    original.write_bytes(b'original')
    downloaded_original(w, original, identity='video')
    part = app.state.videos.work / 'video' / 'v0.part'
    part.parent.mkdir(parents=True)
    part.write_bytes(b'checkpoint')
    drive = tmp_path / 'external'
    drive.mkdir()
    migrate = w.process_move

    async def lose_published_checkpoint(entry):
        await migrate(entry)
        if entry['kind'] == 'checkpoint':
            assert part.read_bytes() == b'checkpoint'
            Path(entry['destination']).unlink()

    try:
        first = await w.preview(str(drive), 0, 'preview')
        with monkeypatch.context() as patch:
            patch.setattr(w, 'process_move', lose_published_checkpoint)
            await w.apply(first['id'], 0, 'apply')
            await w.task
        assert w.root == original_root and w.state()['migration']['state'] == 'attention'
        assert part.read_bytes() == b'checkpoint'
        await w.retry(first['id'], 0, 'retry')
        await w.task
        assert w.state()['migration']['state'] == 'complete'
        assert (app.state.videos.work / 'video' / 'v0.part').read_bytes() == b'checkpoint'
        assert not part.exists()
    finally:
        await close_app(app)


@pytest.mark.asyncio
async def test_waiting_location_can_be_cancelled_before_writing(managed_profile, tmp_path):
    from app.main import create_app
    from tests.test_slicing_performance import close_app

    app = create_app()
    w = app.state.workspace
    original = w.root
    selected = tmp_path / 'external'
    selected.mkdir()
    try:
        plan = await w.preview(str(selected), 0, 'preview')
        selected.rename(tmp_path / 'detached')
        await w.apply(plan['id'], 0, 'apply')
        await w.task
        assert w.state()['migration']['canCancel'] and w.maintenance
        cancelled = await w.cancel_preview(plan['id'], 0, 'cancel')
        assert cancelled['migration']['state'] == 'cancelled'
        assert w.root == original and not w.maintenance
    finally:
        await close_app(app)


def test_samples_keep_original_and_reject_different_existing_copy(tmp_path):
    source=tmp_path/'original.mp4';source.write_bytes(b'original')
    before=file_version(source,True)
    dest=tmp_path/'数据/测试样本/正常/original.mp4'
    proof=preserve_sample(source,dest)
    assert proof['size']==8 and file_version(source,True)==before
    assert preserve_sample(source,dest)['sha256']==proof['sha256']
    dest.write_bytes(b'changed')
    with pytest.raises(ValueError):preserve_sample(source,dest)
    assert source.read_bytes()==b'original'


def test_reset_retires_queue_preserves_keys_and_settings_without_resurrection(tmp_path):
    old=tmp_path/'old.sqlite3';store=JobStore(old)
    store.save({'id':'job','createdAt':'2026-01-01','status':'complete','items':[{'key':'identity','status':'submitted'}]})
    db=store.db
    db.execute('CREATE TABLE simple_slicing_items (id TEXT PRIMARY KEY,payload TEXT)')
    db.execute('INSERT INTO simple_slicing_items VALUES(?,?)',('old',json.dumps({'id':'old','state':'pending'})))
    db.execute('CREATE TABLE simple_slicing_settings (id INTEGER PRIMARY KEY,payload TEXT)')
    db.execute('INSERT INTO simple_slicing_settings VALUES(1,?)',(json.dumps({'enabled':True,'magnetConfidence':.25}),));db.commit();db.close()
    new=tmp_path/'new.sqlite3';retire_database(old,new,'reset')
    store=JobStore(new)
    assert store.list()==[] and store.known_keys()=={'identity'}
    assert store.db.execute('SELECT count(*) FROM simple_slicing_items').fetchone()[0]==0
    settings=json.loads(store.db.execute('SELECT payload FROM simple_slicing_settings').fetchone()[0])
    assert not settings['enabled'] and settings['magnetConfidence']==.25
    assert store.db.execute('SELECT count(*) FROM retired_records').fetchone()[0]>=2
    store.close();retire_database(old,new,'reset')


def test_reset_deletes_exact_manifest_only_and_keeps_changed_or_unknown_files(tmp_path):
    path=tmp_path/'known.mp4';path.write_bytes(b'delete')
    changed=tmp_path/'changed.mp4';changed.write_bytes(b'old');version=file_version(changed,True);changed.write_bytes(b'new')
    unknown=tmp_path/'unknown.mp4';unknown.write_bytes(b'keep')
    manifest={'files':[{'path':str(path),'expected':file_version(path,True),'state':'planned'},
                       {'path':str(changed),'expected':version,'state':'planned'}]}
    journal=tmp_path/'journal.json';delete_manifest(manifest,journal,[])
    assert not path.exists() and changed.exists() and unknown.exists()
    assert [r['state'] for r in manifest['files']]==['deleted','retained']
    delete_manifest(manifest,journal,[])
    assert changed.exists()
