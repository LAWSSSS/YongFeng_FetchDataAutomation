import copy
import asyncio
import sqlite3
import zipfile
from pathlib import Path

import pytest

from app.audit_moves import source_fingerprint
from app.package_import_reuse import identical_photos, prepare_import_reuse, reuse_candidates, reuse_image_ids
from app.slice_packages import SlicePackageManager


def packages(tmp_path):
    result=[]
    for revision in (4,5):
        path=tmp_path/f'v{revision}.zip'
        with zipfile.ZipFile(path,'w') as archive:
            for name,data in [('photo_001.jpg',b'first-photo'),('photo_002.jpg',b'second-photo')]:
                archive.writestr('images/'+name,data)
        result.append({'id':f'p{revision}','revision':1,'dataset':'27893','category':'magnet',
            'sourceIdentity':f"collection:{'a'*24}:v{revision}:file-hash", 'itemId':f'i{revision}',
            'path':str(path),'fileVersion':source_fingerprint(path),'imageCount':2,
            'images':['photo_001.jpg','photo_002.jpg'],'state':'complete' if revision==4 else 'importing',
            'jobId':'100' if revision==4 else '200','verifiedJobId':'100' if revision==4 else '',
            'actualImages':2 if revision==4 else 0,'outputImageIds':['10','11'] if revision==4 else [],
            'keys':['old-key'] if revision==4 else ['new-key'],
            'beforeJobs':[] if revision==4 else ['100'], 'beforeImageIds':[] if revision==4 else ['10','11'],
            'object':{'key':'new-key'},'superseded':revision==4,'videoPath':str(tmp_path/f'v{revision}.mp4')})
    return result


class Remote:
    def __init__(self):
        self.rows=[{'id':'10','name':'photo_001.jpg','taskId':'100'},
                   {'id':'11','name':'photo_002.jpg','taskId':'100'}]
    async def images(self,*args,**kwargs):return copy.deepcopy(self.rows)


def job(identity,key):
    return {'id':identity,'type':1,'status':'3','dataset':'27893',
            'config':{'source':{'files':[{'key':key}]}}}


@pytest.mark.asyncio
async def test_identical_older_photos_rebind_then_require_fresh_platform_verification(tmp_path):
    old,new=packages(tmp_path)
    db=sqlite3.connect(':memory:');manager=SlicePackageManager(db,Remote())
    for row in (old,new):manager.store.save('items',row)
    await manager.verify(manager.store.get('items','p5'),job('200','new-key'))
    pending=manager.store.get('items','p5')
    assert pending['state']=='importing' and pending['jobId']=='100' and pending['actualImages']==0
    assert pending['reusedImport']['originalAttempt']['jobId']=='200'
    assert pending['object']==new['object'] and pending['videoPath']==new['videoPath']
    await manager.verify(pending,job('100','old-key'))
    done=manager.store.get('items','p5')
    assert done['state']=='complete' and done['actualImages']==2
    assert done['verifiedJobId']=='100' and done['outputImageIds']==['10','11']
    assert manager.store.get('items','p4')['superseded']
    assert Path(new['path']).exists() and Path(old['path']).exists()
    db.close()


@pytest.mark.asyncio
async def test_reused_result_does_not_accept_same_names_with_different_cloud_ids(tmp_path):
    old,new=packages(tmp_path);remote=Remote()
    db=sqlite3.connect(':memory:');manager=SlicePackageManager(db,remote)
    for row in (old,new):manager.store.save('items',row)
    await manager.verify(manager.store.get('items','p5'),job('200','new-key'))
    remote.rows[0]['id']='99'
    await manager.verify(manager.store.get('items','p5'),job('100','old-key'))
    saved=manager.store.get('items','p5')
    assert saved['state']=='importing' and saved['actualImages']==1
    assert saved['jobId']=='100' and saved['verificationAutoRetryForJob']=='100'
    await manager.verify(saved,job('100','old-key'))
    saved=manager.store.get('items','p5')
    assert saved['state']=='attention' and saved['actualImages']==1
    db.close()


@pytest.mark.parametrize('problem',['different_photo','changed_archive','different_collection','missing_receipt','wrong_category'])
def test_name_matches_never_replace_content_or_identity_proof(tmp_path,problem):
    old,new=packages(tmp_path)
    if problem=='different_photo':
        with zipfile.ZipFile(new['path'],'w') as archive:
            archive.writestr('images/photo_001.jpg',b'different-photo')
            archive.writestr('images/photo_002.jpg',b'second-photo')
        new['fileVersion']=source_fingerprint(Path(new['path']))
        assert identical_photos(new,old) is None
    elif problem=='changed_archive':
        Path(old['path']).write_bytes(b'changed')
        with pytest.raises(ValueError,match='版本'):
            identical_photos(new,old)
    else:
        if problem=='different_collection':old['sourceIdentity']=old['sourceIdentity'].replace('a'*24,'b'*24)
        elif problem=='missing_receipt':old['outputImageIds']=[]
        else:old['category']='bed'
        assert not reuse_candidates(new,[old])


def test_reuse_preserves_original_submission_and_does_not_mark_complete(tmp_path):
    old,new=packages(tmp_path);before=copy.deepcopy(new)
    proof=identical_photos(new,old);result=prepare_import_reuse(new,old,proof)
    assert new==before and result['state']=='importing'
    assert result['fileVersion']==new['fileVersion'] and result['object']==new['object']
    assert result['reusedImport']['originalAttempt']['keys']==['new-key']
    assert len(result['reusedImport']['imageHashes'])==2
    result['reusedImport']['sourceJobId']='not-the-source'
    assert old['jobId']=='100'


def test_unsafe_zip_members_are_rejected(tmp_path):
    old,new=packages(tmp_path)
    with zipfile.ZipFile(new['path'],'w') as archive:
        archive.writestr('../photo_001.jpg',b'first-photo')
        archive.writestr('images/photo_002.jpg',b'second-photo')
    new['fileVersion']=source_fingerprint(Path(new['path']))
    with pytest.raises(ValueError,match='异常'):
        identical_photos(new,old)


@pytest.mark.asyncio
@pytest.mark.parametrize('change', ['new_job', 'new_path', 'superseded', 'deleted'])
async def test_late_verification_never_overwrites_a_newer_package(tmp_path, change):
    _, item = packages(tmp_path)
    entered, release = asyncio.Event(), asyncio.Event()
    remote = Remote()
    remote.rows = [{'id': str(index + 20), 'name': name, 'taskId': '200'}
                   for index, name in enumerate(item['images'])]

    async def delayed_images(*args, **kwargs):
        entered.set()
        await release.wait()
        return remote.rows

    remote.images = delayed_images
    with sqlite3.connect(':memory:') as db:
        manager = SlicePackageManager(db, remote)
        manager.store.save('items', item)
        changes, cleanups = [], []
        manager.on_change = changes.append
        manager.delete_if_both_complete = cleanups.append
        task = asyncio.create_task(manager.verify(copy.deepcopy(item), job('200', 'new-key')))
        await asyncio.wait_for(entered.wait(), 2)
        current = manager.store.get('items', item['id'])
        if change == 'deleted':
            db.execute('DELETE FROM simple_slice_package_items WHERE id=?', (item['id'],))
            db.commit()
            expected = None
        else:
            if change == 'new_job':
                current.update(jobId='300', keys=['corrected-key'], message='new correction')
            elif change == 'new_path':
                current.update(path=str(tmp_path / 'moved.zip'), message='moved')
            else:
                current.update(superseded=True, message='historical version')
            manager.store.save('items', current)
            expected = manager.store.get('items', item['id'])
        release.set()
        await asyncio.wait_for(task, 2)
        assert manager.store.get('items', item['id']) == expected
        assert changes == [] and cleanups == []


@pytest.mark.asyncio
@pytest.mark.parametrize('status', ['3', '4'])
async def test_already_stale_snapshot_never_writes_or_queries(tmp_path, status):
    _, item = packages(tmp_path)
    with sqlite3.connect(':memory:') as db:
        manager = SlicePackageManager(db, Remote())
        manager.store.save('items', item)
        stale = copy.deepcopy(item)
        item.update(jobId='300', keys=['new-key-300'])
        manager.store.save('items', item)
        expected = manager.store.get('items', item['id'])

        async def must_not_query(*args, **kwargs):
            pytest.fail('a stale snapshot must not query images')

        manager.remote.images = must_not_query
        old_job = job('200', 'new-key') | {'status': status}
        await manager.verify(stale, old_job)
        assert manager.store.get('items', item['id']) == expected


@pytest.mark.asyncio
async def test_failed_verification_after_reuse_check_preserves_a_newer_correction(tmp_path):
    _, item = packages(tmp_path)
    remote = Remote()
    remote.rows = []
    with sqlite3.connect(':memory:') as db:
        manager = SlicePackageManager(db, remote)
        manager.store.save('items', item)
        expected = None

        async def deferred_reuse(_snapshot):
            nonlocal expected
            current = manager.store.get('items', item['id'])
            current.update(jobId='300', keys=['corrected-key'], message='new correction')
            manager.store.save('items', current)
            expected = manager.store.get('items', item['id'])
            return False

        manager.reuse_identical_import = deferred_reuse
        await manager.verify(item, job('200', 'new-key'))
        assert manager.store.get('items', item['id']) == expected


@pytest.mark.asyncio
async def test_fresh_old_generation_can_record_its_own_verified_history(tmp_path):
    old, _ = packages(tmp_path)
    old.update(state='importing', actualImages=0, verifiedJobId='', outputImageIds=[])
    with sqlite3.connect(':memory:') as db:
        manager = SlicePackageManager(db, Remote())
        manager.store.save('items', old)
        await manager.verify(old, job('100', 'old-key'))
        saved = manager.store.get('items', old['id'])
        assert saved['superseded'] and saved['state'] == 'complete'
        assert saved['outputImageIds'] == ['10', '11']
        assert Path(old['path']).exists()


@pytest.mark.asyncio
@pytest.mark.parametrize('problem', [
    'null', 'empty', 'empty_ids', 'missing_id', 'duplicate_ids', 'null_id',
    'missing_hash', 'invalid_hash', 'wrong_job', 'wrong_source', 'newer_source',
    'same_package', 'missing_revision', 'changed_archive', 'missing_archive_version',
])
async def test_incomplete_reuse_receipt_stops_before_remote_query(tmp_path, problem):
    old, item = packages(tmp_path)
    item = prepare_import_reuse(item, old, identical_photos(item, old))
    receipt = item['reusedImport']
    if problem == 'null':
        item['reusedImport'] = None
    elif problem == 'empty':
        item['reusedImport'] = {}
    elif problem == 'empty_ids':
        receipt['imageIdsByName'] = {}
    elif problem == 'missing_id':
        receipt['imageIdsByName'].pop('photo_001.jpg')
    elif problem == 'duplicate_ids':
        receipt['imageIdsByName']['photo_001.jpg'] = '11'
    elif problem == 'null_id':
        receipt['imageIdsByName']['photo_001.jpg'] = None
    elif problem == 'missing_hash':
        receipt['imageHashes'].pop('photo_001.jpg')
    elif problem == 'invalid_hash':
        receipt['imageHashes']['photo_001.jpg'] = 'not-a-sha256'
    elif problem == 'wrong_job':
        receipt['sourceJobId'] = 'other-job'
    elif problem == 'wrong_source':
        receipt['sourceIdentity'] = receipt['sourceIdentity'].replace('a' * 24, 'b' * 24)
    elif problem == 'newer_source':
        receipt['sourceIdentity'] = receipt['sourceIdentity'].replace(':v4:', ':v6:')
    elif problem == 'same_package':
        receipt['sourcePackageId'] = item['id']
    elif problem == 'missing_revision':
        receipt.pop('sourcePackageRevision')
    elif problem == 'changed_archive':
        item['fileVersion']['size'] += 1
    else:
        receipt.pop('currentArchiveVersion')
    with sqlite3.connect(':memory:') as db:
        manager = SlicePackageManager(db, Remote())
        manager.store.save('items', item)

        async def must_not_query(*args, **kwargs):
            pytest.fail('an incomplete reuse receipt must not query images')

        manager.remote.images = must_not_query
        await manager.verify(item, job('100', 'old-key'))
        saved = manager.store.get('items', item['id'])
        assert saved['state'] == 'attention' and saved['errorCode'] == 'reuse_receipt_invalid'
        assert saved['actualImages'] == 0 and Path(item['path']).exists()


@pytest.mark.asyncio
async def test_verified_archive_move_preserves_the_exact_reuse_receipt(tmp_path):
    from app import file_locations
    old, item = packages(tmp_path)
    item = prepare_import_reuse(item, old, identical_photos(item, old))
    source, version = item['path'], copy.deepcopy(item['fileVersion'])
    target = tmp_path / 'archive' / 'moved.zip'
    target.parent.mkdir()
    target.write_bytes(Path(source).read_bytes())
    destination_version = source_fingerprint(target)
    with sqlite3.connect(':memory:') as db:
        manager = SlicePackageManager(db, Remote())
        file_locations.remember(db, {'source': source, 'expected': version,
            'destination': str(target), 'destinationVersion': destination_version})
        manager.store.save('items', item)
        assert item['path'] == str(target) and item['fileVersion'] == destination_version
        await manager.verify(item, job('100', 'old-key'))
        assert manager.store.get('items', item['id'])['state'] == 'complete'


def test_reuse_requires_real_sha256_proof_and_well_formed_photo_names(tmp_path):
    old, item = packages(tmp_path)
    proof = identical_photos(item, old)
    proof['imageHashes']['photo_001.jpg'] = ''
    with pytest.raises(ValueError):
        prepare_import_reuse(item, old, proof)
    item['images'][0] = 1
    with pytest.raises(ValueError):
        reuse_candidates(item, [old])


def test_ordinary_package_does_not_require_a_reuse_receipt(tmp_path):
    _, item = packages(tmp_path)
    assert reuse_image_ids(item) is None
