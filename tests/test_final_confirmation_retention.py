"""Final confirmation does not inherit the legacy automatic replacement deletion."""
import hashlib
import shutil
from pathlib import Path

import pytest

from app.main import create_app
from tests.test_slicing_performance import close_app


def retained_row(tmp_path, revisions=2):
    versions=[]
    for revision in range(revisions):
        path=tmp_path/f'version-{revision}.mp4'
        data=f'original-version-{revision}'.encode()
        path.write_bytes(data)
        versions.append({'revision':revision,'state':'saved','filePath':str(path),
            'sha256':hashlib.sha256(data).hexdigest(),'totalBytes':len(data),
            'taskId':f'task-{revision}','resultId':f'result-{revision}',
            'startTime':'2026-09-30 02:10:00','endTime':'2026-09-30 03:10:00','durationSeconds':3600})
    return {'id':'a'*24,'revision':revisions-1,'activeRevision':revisions-1,'acceptedRevision':revisions-1,
            'name':'保留旧版','versions':versions,'requests':{},'cleanupPending':True}


@pytest.mark.asyncio
@pytest.mark.parametrize('final_confirmation',[True,False])
async def test_new_publication_retains_history_only_for_final_flow(tmp_path,monkeypatch,final_confirmation):
    app=create_app(data_dir=tmp_path/'db');manager=app.state.videos
    row=retained_row(tmp_path)
    old=Path(row['versions'][0]['filePath'])
    current=row['versions'][1]
    Path(current.pop('filePath')).unlink()
    row['acceptedRevision']=0
    current.update(state='ready',downloadRequested=True,outputDirectory=str(tmp_path/'published'))
    if final_confirmation:
        current['finalConfirmation']={'operationId':'final-intent','version':1,'expectedRevision':1}
    manager.store.save(row)
    part=tmp_path/'download.part';part.write_bytes(b'new-complete-media')
    calls=[];notifications=[]
    async def download(_row,_version):
        return part,{'id':'result-1','url':'http://mock.invalid/video.mp4'},hashlib.sha256(part.read_bytes()).hexdigest(),part.stat().st_size
    async def copy(source,destination,*args,**kwargs):
        destination.parent.mkdir(parents=True,exist_ok=True)
        shutil.copyfile(source,destination);source.unlink()
    async def cloud(_row,version,protected,**kwargs):
        calls.append(version['taskId'])
    monkeypatch.setattr(manager,'download_bound',download)
    monkeypatch.setattr(manager,'copy_to_final',copy)
    monkeypatch.setattr(manager,'cleanup_cloud',cloud)
    monkeypatch.setattr(manager,'on_saved',lambda identity,revision:notifications.append((identity,revision)))
    try:
        await manager.publish(row['id'],1)
        saved=manager.store.get(row['id'])
        assert saved['acceptedRevision']==1 and saved['versions'][1]['state']=='saved'
        assert Path(saved['versions'][1]['filePath']).read_bytes()==b'new-complete-media'
        assert not saved['cleanupPending'] and notifications==[(row['id'],1)]
        if final_confirmation:
            assert old.read_bytes()==b'original-version-0' and calls==[]
            assert saved['versions'][0]['filePath']==str(old)
            assert saved['replacementRetention']['versions']==[0]
            assert saved['versions'][0]['state']=='saved'
        else:
            assert not old.exists() and calls==['task-0']
            assert saved['versions'][0]['state']=='cloud_deleted'
    finally:
        await close_app(app)


@pytest.mark.asyncio
async def test_prior_dispatched_deletion_reconciles_exact_journal_without_new_local_or_cloud_deletes(tmp_path,monkeypatch):
    app=create_app(data_dir=tmp_path/'db');manager=app.state.videos
    row=retained_row(tmp_path,3)
    row['versions'][2]['finalConfirmation']={'operationId':'final-current','version':2}
    row['versions'][0]['cleanup']={'started':True,'objects':{'result-0':'bound-existing-object'},'checks':{}}
    paths=[Path(version['filePath']) for version in row['versions']]
    manager.store.save(row)
    calls=[]
    async def reconcile(_row,version,protected,**kwargs):
        assert version['cleanup']['started'] and version['cleanup']['objects']=={'result-0':'bound-existing-object'}
        assert protected=={'task-2'}
        calls.append(version['taskId']);version['cleanup']['complete']=True
    monkeypatch.setattr(manager,'cleanup_cloud',reconcile)
    try:
        await manager.finish_cleanup(row['id'])
        saved=manager.store.get(row['id'])
        assert calls==['task-0'] and not saved['cleanupPending']
        assert all(path.is_file() for path in paths)
        assert saved['versions'][0]['state']=='cloud_deleted'
        assert saved['versions'][1]['state']=='saved'
        assert 'cleanup' not in saved['versions'][1]
        await manager.finish_cleanup(row['id'])
        assert calls==['task-0']
    finally:
        await close_app(app)
