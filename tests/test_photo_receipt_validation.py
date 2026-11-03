"""Old complete markers cannot authorize cleanup with malformed image receipts."""
import pytest

from app.photo_completion import category_verified, verified_package
from app.main import create_app
from tests.test_slicing_performance import close_app
from tests.test_workspace import pair, proofs


@pytest.mark.parametrize('ids', [[True], [None], ['  '], {'image': 'id'}, 'image-id'])
@pytest.mark.asyncio
async def test_bad_historical_complete_receipt_cannot_archive_or_delete(tmp_path, ids):
    app = create_app(data_dir=tmp_path / 'private-db')
    workspace = app.state.workspace
    path = pair(workspace, tmp_path / 'originals', ('complete', 'complete'))
    proofs(workspace, path, tmp_path)
    package = workspace.slicing.packages.store.get('items', 'p-bed')
    package['outputImageIds'] = ids
    workspace.slicing.packages.store.save('items', package)
    try:
        await workspace.discover()
        asset = workspace.rows('assets')[0]
        assert not workspace.photo_results_verified(asset)
        assert not workspace.cleanup_eligibility(asset)[0]
        assert workspace.stage(asset) != '已切片'
        await workspace.cleanup(asset)
        assert path.exists()
    finally:
        await close_app(app)


@pytest.mark.parametrize('ids', [['id', ' id '], ['same', 'same'], ['one', False]])
def test_canonical_image_id_collision_never_passes_either_completion_route(ids):
    version = dict(device=1, inode=2, size=3, modifiedNs=4)
    item = {'id': 'slice', 'fileVersion': version, 'categories': {'bed': {
        'state': 'complete', 'actualImages': 2, 'taskId': 'job', 'targetDatasetId': 'photos',
        'outputImageIds': ids}}}
    package = dict(itemId='slice', category='bed', state='complete', imageCount=2, actualImages=2,
                   jobId='job', verifiedJobId='job', outputImageIds=ids,
                   videoFileVersion=version, dataset='photos')
    assert not verified_package(package, item, 'bed')
    assert not category_verified(item, 'bed', [])
