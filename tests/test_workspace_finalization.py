"""Status-only recovery: completed cleanup journals never restart media work."""
from types import SimpleNamespace

import pytest

from app.workspace_scheduler import WorkspaceScheduler
from app.workspace_progress import snapshot
from app.main import create_app
from tests.test_slicing_performance import close_app
from tests.test_attention_presentation import recovery_pair


def finish_fixture(cleanup, *, verified=True):
    asset={'id':'one','name':'one.mp4','path':'/isolated/one.mp4',
           'status':'cleaned' if cleanup=='complete' else 'managed',
           'organization':{'state':'waiting'},'cleanup':{'state':cleanup}}
    request={'pipelineStage':'organizing','pipelineSources':[{'id':'one'}], 'pipelineBlockers':[]}
    manager=SimpleNamespace(_pipeline_request=lambda:request,get=lambda *_:asset,
        _asset_manually_paused=lambda _:False,outcomes=lambda _:{'bed':'complete','magnet':'complete'},
        photo_results_verified=lambda _:verified,cleanup_eligibility=lambda _:(False,'原片保留'))
    manager._pipeline_update=lambda target,stage,message,state,**extra:target.update(
        pipelineStage=stage,pipelineState=state,pipelineMessage=message,**extra)
    return manager,request


def test_cleaned_source_ignores_stale_arrangement_wait_and_does_not_probe_removed_media():
    manager,request=finish_fixture('complete')
    def forbidden(*_):
        pytest.fail('A completed cleanup journal must not reread or restart removed media.')
    manager.photo_results_verified=manager.outcomes=manager.cleanup_eligibility=forbidden
    WorkspaceScheduler._finish_pipeline(manager)
    assert request['pipelineState']=='complete'
    assert request['finished']==1 and request['waiting']==0


def test_inflight_cleanup_is_still_pending_and_unverified_photos_stay_blocked():
    manager,request=finish_fixture('running')
    WorkspaceScheduler._finish_pipeline(manager)
    assert request['pipelineState']=='waiting' and request['waiting']==1
    manager,request=finish_fixture('waiting',verified=False)
    WorkspaceScheduler._finish_pipeline(manager)
    assert request['pipelineState']=='attention'
    assert request['finished']==0 and request['failed']==1


@pytest.mark.asyncio
async def test_cleaned_source_counts_complete_when_its_removed_inventory_is_omitted(tmp_path):
    app=create_app(data_dir=tmp_path/'db')
    workspace=app.state.workspace
    asset={'id':'cleaned','path':str(tmp_path/'missing.mp4'),'fileVersion':{'size':1},
           'cleanup':{'state':'complete'},'organization':{'state':'waiting'}}
    try:
        progress=snapshot(workspace,{'pipelineSources':[{'id':'cleaned'}]},[asset],slices=[],packages=[])
        assert progress['slicing']['total']==progress['slicing']['complete']==1
        assert progress['slicing']['queued']==progress['slicing']['attention']==0
        assert progress['activeTaskCount']==0
        assert not (tmp_path/'missing.mp4').exists()
    finally:
        await close_app(app)


@pytest.mark.asyncio
async def test_covered_attempt_does_not_create_phantom_waiting_or_attention(tmp_path):
    app=create_app(data_dir=tmp_path/'db')
    path=tmp_path/'original.mp4';path.write_bytes(b'isolated-source')
    try:
        old=recovery_pair(app,path)
        workspace=app.state.workspace
        await workspace.discover()
        asset=workspace.rows('assets')[0]
        before=app.state.slicing.store.get('items',old['id'])
        result=snapshot(workspace,{'pipelineSources':[{'id':asset['id']}]},[asset])
        assert result['slicing']['complete']==1
        assert result['slicing']['queued']==result['slicing']['attention']==0
        assert app.state.slicing.store.get('items',old['id'])==before
        pack=app.state.slicing.packages.store.get('items','pack-bed')
        pack['verifiedJobId']='wrong-job'
        app.state.slicing.packages.store.save('items',pack)
        result=snapshot(workspace,{'pipelineSources':[{'id':asset['id']}]},[asset])
        assert result['slicing']['complete']==0
        assert result['slicing']['attention']==1
        assert path.exists()
    finally:
        await close_app(app)


@pytest.mark.asyncio
@pytest.mark.parametrize('problem', ['none', 'orphan_operation', 'orphan_package', 'wrong_job',
                                    'new_version', 'generation', 'missing_owner', 'submitted'])
async def test_scoped_coverage_matches_global_proof_without_full_history_reads(tmp_path, monkeypatch, problem):
    app=create_app(data_dir=tmp_path/'db')
    path=tmp_path/'original.mp4';path.write_bytes(b'isolated-source')
    try:
        old=recovery_pair(app,path)
        store=app.state.slicing.store
        if problem=='orphan_operation':
            store.save('operations',{'id':'uncertain','itemId':'old','state':'submitting'})
        elif problem=='orphan_package':
            app.state.slicing.packages.store.save('items',{'id':'uncertain','itemId':'old','state':'uploading'})
        elif problem=='wrong_job':
            package=app.state.slicing.packages.store.get('items','pack-bed')
            package['verifiedJobId']='another-job'
            app.state.slicing.packages.store.save('items',package)
        elif problem=='new_version':
            row=app.state.videos.store.get('collection');row['activeRevision']=1
            app.state.videos.store.save(row)
        elif problem in {'generation','missing_owner'}:
            result=store.get('items','recovered')
            if problem=='generation': result['mediaGeneration']+=1
            else: result['superseded']=True
            store.save('items',result)
        elif problem=='submitted':
            old['categories']['bed']['taskId']='bound-task'
            store.save('items',old)
        control=app.state.media_control
        expected=control.effective_items([old])
        monkeypatch.setattr(store,'list',lambda *_:pytest.fail('Scoped status must not read full history.'))
        monkeypatch.setattr(app.state.slicing.packages.store,'list',lambda *_:pytest.fail('Scoped status must not read all packages.'))
        assert control.scoped_effective_items([old])==expected
        assert bool(expected)==(problem!='none')
        assert path.is_file()
    finally:
        await close_app(app)
