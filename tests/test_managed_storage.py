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
async def test_fixed_storage_rejects_path_settings_and_legacy_cloud_deletion(tmp_path,monkeypatch):
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
            assert (await client.post('/api/workspace/preview',json={'parent':'/elsewhere','expectedRevision':0,'requestId':'elsewhere'})).status_code==409
            assert (await client.post('/api/settings/directory',json={'path':'/elsewhere'})).status_code==409
            assert (await client.post('/api/videos/test/cancel-clear',json={'requestId':'undo','expectedRevision':0})).status_code==410
        assert Path(app.state.videos.store.directory()).is_relative_to(root)
        assert not (root.parent/'credentials.json').exists()
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
