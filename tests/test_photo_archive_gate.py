"""Every final archive is gated by exact photo import receipts for the same video."""
import pytest
from pathlib import Path

from app.main import create_app
from tests.test_slicing_performance import close_app
from tests.test_workspace import pair, proofs, enable


@pytest.mark.asyncio
@pytest.mark.parametrize('problem', ['importing', 'dismissed', 'missing_job', 'wrong_job', 'missing_photo', 'wrong_version', 'wrong_dataset'])
async def test_unverified_photo_never_archives_or_cleans_any_result(tmp_path, problem):
    app=create_app(data_dir=tmp_path/'db');w=app.state.workspace
    path=pair(w,tmp_path/'old',('complete','complete'));proofs(w,path,tmp_path)
    package=w.slicing.packages.store.get('items','p-magnet')
    if problem in {'importing','dismissed'}:package['state']=problem
    elif problem=='missing_job':package.pop('verifiedJobId')
    elif problem=='wrong_job':package['verifiedJobId']='different-job'
    elif problem=='missing_photo':package['outputImageIds']=[]
    elif problem=='wrong_version':package['videoFileVersion']={'size':999}
    else:
        item=w.slicing.store.get('items',package['itemId'])
        item['categories']['magnet']['targetDatasetId']='real-target'
        w.slicing.store.save('items',item)
    w.slicing.packages.store.save('items',package)
    await w.discover();asset=w.rows('assets')[0]
    assert not w.photo_results_verified(asset)
    assert w.stage(asset)!='已切片'
    assert not w.cleanup_eligibility(asset)[0]
    preview=w.plan_assets(tmp_path/'future')
    assert preview and all('/已切片/' not in p['destination'] for p in preview)
    assert all('/车斗已切片/' not in p['destination'] and '/磁盘吊已切片/' not in p['destination'] for p in preview)
    await w.cleanup(asset)
    assert path.is_file() and all(Path(p['path']).is_file() for p in w.slicing.packages.store.list('items'))
    await close_app(app)


@pytest.mark.asyncio
async def test_final_archive_waits_until_both_imports_are_verified(tmp_path):
    app=create_app(data_dir=tmp_path/'db');w=app.state.workspace
    path=pair(w,tmp_path/'old',('complete','complete'));proofs(w,path,tmp_path)
    pending=w.slicing.packages.store.get('items','p-magnet');pending['state']='importing'
    w.slicing.packages.store.save('items',pending)
    await enable(w,tmp_path)
    asset=w.rows('assets')[0]
    assert w.stage(asset)!='已切片' and Path(asset['path']).exists()
    assert not (w.root/'车斗已切片'/'truck'/'压缩包.zip').exists()
    assert (w.root/'.处理中'/'truck'/'bed'/'压缩包.zip').is_file()
    pending=w.slicing.packages.store.get('items','p-magnet');pending['state']='complete'
    w.slicing.packages.store.save('items',pending)
    await w.reconcile(cleanup=False)
    asset=w.get('assets',asset['id'])
    assert w.stage(asset)=='已切片' and Path(asset['path']).exists()
    assert (w.root/'车斗已切片'/'truck'/'压缩包.zip').is_file()
    assert (w.root/'磁盘吊已切片'/'truck'/'压缩包.zip').is_file()
    await close_app(app)


@pytest.mark.asyncio
async def test_old_move_plan_is_rechecked_if_photo_status_changes(tmp_path):
    app=create_app(data_dir=tmp_path/'db');w=app.state.workspace
    original=pair(w,tmp_path/'old',('complete','complete'));proofs(w,original,tmp_path)
    await w.discover();asset=w.rows('assets')[0]
    entries=w.plan_assets(tmp_path/'future')
    video=next(entry for entry in entries if entry['kind']=='video')
    assert w.is_final_move(video)
    photo=w.slicing.packages.store.get('items','p-magnet');photo['state']='importing'
    w.slicing.packages.store.save('items',photo)
    with pytest.raises(ValueError,match='尚未全部'):
        await w.process_move(video)
    assert original.is_file() and not Path(video['destination']).exists()
    await close_app(app)
