"""A shared package belongs to one owner, even with several video bindings."""
import copy

import pytest

from app.main import create_app
from app.photo_completion import category_verified, photo_results_verified, verified_package
from tests.test_slicing_performance import close_app
from tests.test_workspace import pair, proofs, enable


def shared_receipts():
    version = dict(device=1, inode=2, size=3, modifiedNs=4, edgeHash='exact-bytes')
    owner = dict(id='owner', mediaId='collection:video', mediaGeneration=0,
                 collectionVideoId='video', collectionRevision=2, path='/managed/video.mp4',
                 fileVersion=version, state='complete', categories={})
    packages = []
    for name in ('bed', 'magnet'):
        owner['categories'][name] = dict(state='complete', packageId='package-' + name,
            targetDatasetId='photos-' + name, actualImages=1, importJobId='job-' + name)
        packages.append(dict(id='package-' + name, itemId='owner', category=name,
            dataset='photos-' + name, state='complete', jobId='job-' + name,
            verifiedJobId='job-' + name, imageCount=1, actualImages=1,
            outputImageIds=['photo-' + name], videoFileVersion=version))
    mirror = copy.deepcopy(owner)
    mirror['id'] = 'platform-binding'
    return owner, mirror, packages


def test_exact_shared_receipts_do_not_leave_a_completed_video_waiting():
    owner, mirror, packages = shared_receipts()
    assert not verified_package(packages[0], mirror, 'bed'), 'The actual owner remains immutable.'
    assert not category_verified(mirror, 'bed', packages), 'No guessing without the original owner.'
    assert photo_results_verified([owner, mirror], packages)
    assert category_verified(mirror, 'bed', packages, [owner, mirror])


@pytest.mark.parametrize('problem', ['media', 'missing_media', 'generation', 'revision', 'collection',
    'version', 'path', 'target', 'count', 'boolean_count', 'job', 'task', 'operation',
    'owner_superseded', 'owner_missing_reference', 'package_superseded', 'duplicate_package', 'extra_import'])
def test_shared_reference_cannot_bypass_identity_or_import_proofs(problem):
    owner, mirror, packages = shared_receipts()
    slot = mirror['categories']['bed']
    if problem == 'media': mirror['mediaId'] = 'another-video'
    elif problem == 'missing_media': mirror.pop('mediaId')
    elif problem == 'generation': mirror['mediaGeneration'] += 1
    elif problem == 'revision': mirror['collectionRevision'] += 1
    elif problem == 'collection': mirror['collectionVideoId'] = 'another'
    elif problem == 'version': mirror['fileVersion']['edgeHash'] = 'changed'
    elif problem == 'path': mirror['path'] = '/other/same-name.mp4'
    elif problem == 'target': slot['targetDatasetId'] = 'other'
    elif problem == 'count': slot['actualImages'] = 2
    elif problem == 'boolean_count': slot['actualImages'] = True
    elif problem == 'job': slot['importJobId'] = 'other-job'
    elif problem == 'task': slot['taskId'] = 'independent-task'
    elif problem == 'operation': slot['operationId'] = 'independent-submit'
    elif problem == 'owner_superseded': owner['superseded'] = True
    elif problem == 'owner_missing_reference': owner['categories']['bed'].pop('packageId')
    elif problem == 'package_superseded': packages[0]['superseded'] = True
    elif problem == 'duplicate_package': packages.append(copy.deepcopy(packages[0]))
    else:
        additional = copy.deepcopy(packages[0])
        additional.update(id='unexpected-other-package', itemId=mirror['id'])
        packages.append(additional)
    assert not photo_results_verified([owner, mirror], packages)


@pytest.mark.asyncio
async def test_shared_results_automatically_arrange_without_reuploading(tmp_path):
    app = create_app(data_dir=tmp_path / 'db')
    w = app.state.workspace
    try:
        path = pair(w, tmp_path / 'originals', ('complete', 'complete'))
        proofs(w, path, tmp_path)
        w.slicing.control.refresh()
        owner = w.slicing.store.list('items')[0]
        mirror = copy.deepcopy(owner)
        mirror['id'] = 'same-version-platform-binding'
        mirror['categories']['bed'].update(packageId='p-bed', targetDatasetId='bed', importJobId='image-job-bed')
        owner['categories']['bed'].update(packageId='p-bed', targetDatasetId='bed')
        w.slicing.store.save('items', owner)
        w.slicing.store.save('items', mirror)
        # The other model has its own owner and package.
        w.slicing.packages.pause()
        before = w.slicing.packages.store.list('items')
        await enable(w, tmp_path)
        await w.reconcile(cleanup=False)
        asset = w.rows('assets')[0]
        assert asset['stage'] == '已切片' and w.photo_results_verified(asset)
        assert '/已切片/' in asset['path']
        assert w.slicing.packages.paused
        assert {p['id'] for p in w.slicing.packages.store.list('items')} == {p['id'] for p in before}
        assert all(p['state'] == 'complete' for p in w.slicing.packages.store.list('items'))
    finally:
        await close_app(app)
