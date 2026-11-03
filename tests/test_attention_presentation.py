"""Previously completed recovery is never offered as an unfinished attempt."""
import copy

import pytest

from app.main import create_app
from app.recovery_results import verified_result
from tests.test_media_control import collection, sliced
from tests.test_slicing_performance import close_app


def recovery_pair(app, path):
    row=collection(app,path,accepted=False)
    old=sliced(app,path,'old','27912','attention')
    old['errorCode']='worker_failed'
    old['categories']['bed']['errorCode']='worker_failed'
    app.state.slicing.store.save('items',old)
    result=sliced(app,path,'recovered','27912','complete')
    result.update(collectionRecovery=True,partialRecovery=True)
    result['categories']={name:{'state':'complete','packageId':f'pack-{name}',
        'targetDatasetId':dataset,'actualImages':2} for name,dataset in [('bed','101'),('magnet','102')]}
    app.state.slicing.store.save('items',result)
    for name,dataset in [('bed','101'),('magnet','102')]:
        app.state.slicing.packages.store.save('items',{'id':f'pack-{name}','itemId':'recovered',
            'category':name,'dataset':dataset,'state':'complete','jobId':f'job-{name}',
            'verifiedJobId':f'job-{name}','imageCount':2,'actualImages':2,
            'outputImageIds':[f'{name}-1',f'{name}-2'],'videoFileVersion':result['fileVersion']})
    proof=verified_result(result,lambda key:app.state.slicing.packages.store.get('items',key))
    row['versions'][0].update(state='failed',recovery={'state':'partial_complete','resultVerified':True,
        'slicingItemId':'recovered','filePath':str(path),'fileVersion':result['fileVersion'],'results':proof})
    app.state.videos.store.save(row)
    app.state.media_control.refresh()
    return app.state.slicing.store.get('items','old')


@pytest.mark.asyncio
async def test_only_verified_recovery_covers_an_unsubmitted_old_attempt(tmp_path):
    app=create_app(data_dir=tmp_path/'db');path=tmp_path/'video.mp4';path.write_bytes(b'original')
    old=recovery_pair(app,path)
    before=copy.deepcopy(old)
    public={item['id']:item for item in app.state.slicing.state()['items']}
    assert public['old']['coveredByRecovery']=='recovered'
    assert app.state.slicing.store.get('items','old')==before
    assert app.state.media_control.state()['groups'][0]['items']
    await close_app(app)


@pytest.mark.asyncio
@pytest.mark.parametrize('change',['photo_unverified','wrong_job','file_changed','old_submitted','orphan_package','orphan_operation','new_version','missing_receipt'])
async def test_no_attempt_is_covered_if_identity_or_photo_proof_is_incomplete(tmp_path,change):
    app=create_app(data_dir=tmp_path/'db');path=tmp_path/'video.mp4';path.write_bytes(b'original')
    old=recovery_pair(app,path)
    if change in {'photo_unverified','wrong_job'}:
        pack=app.state.slicing.packages.store.get('items','pack-bed')
        pack['state']='importing' if change=='photo_unverified' else 'complete'
        if change=='wrong_job': pack['verifiedJobId']='different'
        app.state.slicing.packages.store.save('items',pack)
    elif change=='file_changed':
        old['fileVersion']['inode']+=1;app.state.slicing.store.save('items',old)
    elif change=='old_submitted':
        old['categories']['bed']['operationId']='already-submitted';app.state.slicing.store.save('items',old)
    elif change=='orphan_package':
        app.state.slicing.packages.store.save('items',{'id':'old-pack','itemId':'old','state':'importing'})
    elif change=='orphan_operation':
        app.state.slicing.store.save('operations',{'id':'old-operation','itemId':'old','state':'submitting'})
    else:
        row=app.state.videos.store.get('collection')
        if change=='new_version':row['activeRevision']=1
        else:row['versions'][0]['recovery']['results']={}
        app.state.videos.store.save(row)
    public={item['id']:item for item in app.state.slicing.state()['items']}
    assert 'coveredByRecovery' not in public['old']
    await close_app(app)


@pytest.mark.asyncio
async def test_covered_attempt_does_not_block_archive_but_photo_proof_is_still_required(tmp_path):
    app=create_app(data_dir=tmp_path/'db');path=tmp_path/'video.mp4';path.write_bytes(b'original')
    recovery_pair(app,path)
    workspace=app.state.workspace
    await workspace.discover();asset=workspace.rows('assets')[0]
    assert workspace.outcomes(asset)=={'bed':'complete','magnet':'complete'}
    assert workspace.photo_results_verified(asset) and workspace.stage(asset)=='已切片'
    package=app.state.slicing.packages.store.get('items','pack-bed');package['state']='importing'
    app.state.slicing.packages.store.save('items',package)
    assert not workspace.photo_results_verified(asset) and workspace.stage(asset)!='已切片'
    assert not workspace.cleanup_eligibility(asset)[0] and path.exists()
    await close_app(app)
