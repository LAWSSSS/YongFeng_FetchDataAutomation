"""Photo queue faults use private stores and never contact a real platform."""
import asyncio
import copy
import sqlite3

import httpx
import pytest

from app.audit_moves import source_fingerprint
from app.slice_packages import SlicePackageManager
from app.upload_audit import AuditError
from tests.test_slice_packages import Remote


def package(tmp_path, *, state='importing'):
    archive = tmp_path / 'photos.zip'
    archive.write_bytes(b'archive-only-state-fixture')
    return {'id': 'package-one', 'dataset': '27892', 'category': 'bed',
            'path': str(archive), 'name': archive.name, 'fileVersion': source_fingerprint(archive),
            'size': archive.stat().st_size, 'state': state, 'images': ['sample_001.jpg', 'sample_002.jpg'],
            'imageCount': 2, 'keys': ['one-object'], 'object': {'key': 'one-object'},
            'jobId': '701', 'beforeJobs': [], 'beforeImageIds': []}


def bound_job():
    return {'id': '701', 'type': 1, 'status': '3', 'dataset': '27892',
            'config': {'source': {'files': [{'key': 'one-object'}]}}}


def valid_rows():
    return [{'id': '1', 'name': 'sample_001.jpg', 'taskId': '701'},
            {'id': '2', 'name': 'sample_002.jpg', 'taskId': '701'}]


@pytest.mark.asyncio
async def test_expired_refresh_keeps_photo_upload_queued_without_using_retry_budget(tmp_path):
    from app.upload_audit import AthenaClient
    from app.upload_remote import UploadRemote

    calls = []
    def handler(request):
        calls.append((request.method, request.url.path))
        if request.url.path.endswith('/token/refresh/'):
            return httpx.Response(400, json={'code': 10002, 'message': 'Token is invalid', 'data': None})
        return httpx.Response(401)

    client = AthenaClient(httpx.MockTransport(handler), persist_connection=False)
    client.access, client.refresh = 'expired-access', 'expired-refresh'
    with sqlite3.connect(':memory:') as db:
        manager = SlicePackageManager(db, UploadRemote(client))
        item = package(tmp_path, state='queued')
        for key in ('jobId', 'keys', 'object'):
            item.pop(key)
        item['retries'] = 2
        manager.store.save('items', item)
        try:
            await manager.upload_one(item['id'])
            current = manager.store.get('items', item['id'])
            assert current['state'] == 'queued' and current['errorCode'] == 'auth'
            assert current['retries'] == 2 and not current.get('object')
            assert not client.connected
            assert [method for method, _ in calls] == ['GET', 'GET', 'POST']
        finally:
            await manager.shutdown()
            await client.close()


def test_photo_reconnect_wakes_only_auth_waiters_without_overriding_pause(tmp_path):
    with sqlite3.connect(':memory:') as db:
        manager = SlicePackageManager(db, Remote(), clock=lambda: 100)
        waiting = package(tmp_path, state='queued')
        waiting.update(errorCode='auth', nextAttempt=130, retries=2)
        manager.store.save('items', waiting)
        other = {**waiting, 'id': 'other', 'errorCode': 'network', 'nextAttempt': 130}
        manager.store.save('items', other)
        attention = {**waiting, 'id': 'attention', 'state': 'attention', 'errorCode': 'file_changed'}
        manager.store.save('items', attention)
        manager.pause()
        manager.wake.clear()
        manager.target_next_check['27892'] = 130
        manager.connection_restored()
        current = manager.store.get('items', waiting['id'])
        assert current['state'] == 'queued' and current['nextAttempt'] == 0 and current['retries'] == 2
        assert not current['errorCode'] and manager.wake.is_set() and manager.paused
        assert manager.store.get('items', other['id'])['nextAttempt'] == 130
        assert manager.store.get('items', attention['id'])['state'] == 'attention'
        assert not manager.target_next_check


@pytest.mark.asyncio
@pytest.mark.parametrize('problem', ['duplicate_id', 'blank', 'null', 'missing', 'bool', 'repeat_row'])
async def test_invalid_image_identity_cannot_complete_or_cleanup_and_retry_is_bounded(tmp_path, problem):
    remote = Remote()
    remote.rows = valid_rows()
    if problem == 'duplicate_id':
        remote.rows[1]['id'] = ' 1 '
    elif problem == 'missing':
        remote.rows[1].pop('id')
    elif problem == 'repeat_row':
        remote.rows.append(copy.deepcopy(remote.rows[1]))
    else:
        remote.rows[1]['id'] = {'blank': '', 'null': None, 'bool': True}[problem]
    with sqlite3.connect(':memory:') as db:
        manager = SlicePackageManager(db, remote, clock=lambda: 100)
        item = package(tmp_path)
        manager.store.save('items', item)
        cleaned = []
        manager.delete_if_both_complete = cleaned.append
        await manager.verify(item, bound_job())
        current = manager.store.get('items', item['id'])
        assert current['state'] == 'importing'
        assert current['errorCode'] == 'verification_pending'
        assert current['actualImages'] == (0 if problem == 'duplicate_id' else 1)
        assert current['verificationNotBefore'] == 130
        await manager.verify(current, bound_job())
        current = manager.store.get('items', item['id'])
        assert current['state'] == 'attention' and current['errorCode'] == 'verification'
        assert current['verificationFailures'] == 2 and not current.get('outputImageIds')
        assert not cleaned and not remote.submitted
        assert (tmp_path / 'photos.zip').exists()


@pytest.mark.asyncio
async def test_missing_image_appears_on_bound_recheck_without_reimport(tmp_path):
    remote = Remote(); remote.rows = valid_rows()[:1]
    remote.jobs_by_dataset['27892'] = [bound_job()]
    clock = [100.0]
    with sqlite3.connect(':memory:') as db:
        manager = SlicePackageManager(db, remote, clock=lambda: clock[0])
        item = package(tmp_path); manager.store.save('items', item)
        await manager.verify(item, bound_job())
        remote.rows = valid_rows()
        await manager.tick_target('27892')
        assert manager.store.get('items', item['id'])['state'] == 'importing'
        clock[0] = 130
        await manager.tick_target('27892')
        current = manager.store.get('items', item['id'])
        assert current['state'] == 'complete' and current['outputImageIds'] == ['1', '2']
        assert not remote.submitted


@pytest.mark.asyncio
@pytest.mark.parametrize('superseded', [False, True])
async def test_pause_after_upload_ack_preserves_submitted_import_and_generation(tmp_path, superseded):
    remote = Remote()
    acknowledged = asyncio.Event()

    class Worker:
        async def run(self, item, credentials, callback, operation='upload'):
            await callback({'event': 'complete', 'size': item['size'], 'etag': 'bound-etag'})
            acknowledged.set()
            await asyncio.Event().wait()  # process has not yet acknowledged its exit

    with sqlite3.connect(':memory:') as db:
        manager = SlicePackageManager(db, remote, worker=Worker())
        item = package(tmp_path, state='queued')
        item.pop('jobId'); item.pop('keys'); item.pop('object')
        item['mediaGeneration'] = 1
        manager.store.save('items', item)
        manager.launch(item)
        await asyncio.wait_for(acknowledged.wait(), 2)
        await manager.submit_one(manager.store.get('items', item['id']), [])
        current = manager.store.get('items', item['id'])
        assert current['state'] == 'importing' and len(remote.submitted) == 1
        if superseded:
            current.update(superseded=True, mediaGeneration=1)
            manager.store.save('items', current)
            newer = {**copy.deepcopy(current), 'id': 'new-correction', 'superseded': False,
                     'mediaGeneration': 2, 'state': 'review', 'jobId': ''}
            manager.store.save('items', newer)
            new_snapshot = manager.store.get('items', newer['id'])
        manager.pause()
        await asyncio.gather(*manager.running.values(), return_exceptions=True)
        current = manager.store.get('items', item['id'])
        assert current['state'] == 'importing' and current['jobId'] == '701'
        # Delayed worker/error events cannot regress an existing import either.
        await manager.worker_event(item['id'], {'event': 'error', 'code': 'upload_failed'})
        manager.fail(manager.store.get('items', item['id']), 'stalled')
        assert manager.store.get('items', item['id']) == current
        if superseded:
            assert manager.store.get('items', newer['id']) == new_snapshot
        await manager.shutdown()


@pytest.mark.asyncio
async def test_new_version_during_presubmit_read_does_not_receive_stale_submit(tmp_path):
    remote = Remote(); entered, release = asyncio.Event(), asyncio.Event()

    async def delayed(*args, **kwargs):
        entered.set(); await release.wait(); return []

    remote.images = delayed
    with sqlite3.connect(':memory:') as db:
        manager = SlicePackageManager(db, remote)
        item = package(tmp_path, state='uploaded'); item.pop('jobId')
        manager.store.save('items', item)
        task = asyncio.create_task(manager.submit_one(item, []))
        await entered.wait()
        newer = manager.store.get('items', item['id'])
        newer.update(state='review', object={'key': 'new-version-object'}, message='new draft')
        manager.store.save('items', newer)
        expected = manager.store.get('items', item['id'])
        release.set(); await task
        assert not remote.submitted
        assert manager.store.get('items', item['id']) == expected


@pytest.mark.asyncio
@pytest.mark.parametrize('bad_id', [None, '', ' ', True])
async def test_invalid_presubmit_snapshot_is_actionable_and_never_sends_import(tmp_path, bad_id):
    remote = Remote(); remote.rows = [{'id': bad_id, 'name': 'sample_001.jpg'}]
    with sqlite3.connect(':memory:') as db:
        manager = SlicePackageManager(db, remote)
        item = package(tmp_path, state='uploaded'); item.pop('jobId')
        manager.store.save('items', item)
        await manager.submit_one(item, [])
        current = manager.store.get('items', item['id'])
        assert current['state'] == 'attention' and current['errorCode'] == 'image_identity'
        assert not remote.submitted and not current.get('jobId')


@pytest.mark.asyncio
async def test_old_generation_upload_events_and_errors_do_not_change_the_record(tmp_path):
    class Control:
        def package_allowed(self, package, allow_paused=False):
            return False
    with sqlite3.connect(':memory:') as db:
        manager = SlicePackageManager(db, Remote())
        manager.control = Control()
        item = package(tmp_path, state='uploading')
        manager.store.save('items', item)
        before = manager.store.get('items', item['id'])
        await manager.worker_event(item['id'], {'event': 'complete', 'size': item['size'], 'etag': 'late'})
        manager.fail(manager.store.get('items', item['id']), 'upload_failed')
        assert manager.store.get('items', item['id']) == before


@pytest.mark.asyncio
@pytest.mark.parametrize('code', ['identity', 'pagination', 'image_filter', 'response'])
async def test_real_adapter_rejection_has_bounded_result_checks_not_infinite_importing(tmp_path, code):
    remote = Remote()
    async def rejected(*args, **kwargs):
        raise AuditError('isolated protocol fault', code)
    remote.images = rejected
    with sqlite3.connect(':memory:') as db:
        manager = SlicePackageManager(db, remote, clock=lambda: 100)
        item = package(tmp_path); manager.store.save('items', item)
        cleanups = []; manager.delete_if_both_complete = cleanups.append
        await manager.verify(item, bound_job())
        current = manager.store.get('items', item['id'])
        assert current['state'] == 'importing' and current['verificationFailures'] == 1
        assert current['verificationFailureType'] == code and current['jobId'] == '701'
        await manager.verify(current, bound_job())
        current = manager.store.get('items', item['id'])
        assert current['state'] == 'attention' and current['errorCode'] == 'verification'
        assert current['verificationFailures'] == 2 and not cleanups and not remote.submitted


@pytest.mark.asyncio
@pytest.mark.parametrize('code', ['auth', 'network', 'disk'])
async def test_connection_failures_preserve_binding_without_using_verification_budget(tmp_path, code):
    remote = Remote()
    async def offline(*args, **kwargs):
        raise AuditError('isolated waiting fault', code)
    remote.images = offline
    with sqlite3.connect(':memory:') as db:
        manager = SlicePackageManager(db, remote)
        item = package(tmp_path); manager.store.save('items', item)
        before = manager.store.get('items', item['id'])
        for _ in range(3):
            with pytest.raises(AuditError) as caught:
                await manager.verify(item, bound_job())
            assert caught.value.code == code
        assert manager.store.get('items', item['id']) == before
        assert not remote.submitted


@pytest.mark.asyncio
async def test_presubmit_adapter_identity_error_stops_only_this_item_without_write(tmp_path):
    remote = Remote()
    async def rejected(*args, **kwargs):
        raise AuditError('isolated duplicate IDs in API response', 'identity')
    remote.images = rejected
    with sqlite3.connect(':memory:') as db:
        manager = SlicePackageManager(db, remote)
        item = package(tmp_path, state='uploaded'); item.pop('jobId')
        manager.store.save('items', item)
        await manager.submit_one(item, [])
        current = manager.store.get('items', item['id'])
        assert current['state'] == 'attention' and current['errorCode'] == 'image_identity'
        assert current['verificationFailureType'] == 'identity' and not remote.submitted


@pytest.mark.asyncio
async def test_paused_video_keeps_one_recheck_of_its_already_submitted_import(tmp_path):
    class PausedControl:
        def package_allowed(self, item, allow_paused=False):
            return allow_paused
    remote = Remote(); remote.rows = valid_rows()[:1]
    with sqlite3.connect(':memory:') as db:
        manager = SlicePackageManager(db, remote)
        manager.control = PausedControl()
        item = package(tmp_path); manager.store.save('items', item)
        await manager.verify(item, bound_job())
        current = manager.store.get('items', item['id'])
        assert current['state'] == 'importing' and current['verificationAutoRetryForJob'] == '701'
        remote.rows = valid_rows()
        await manager.verify(current, bound_job())
        assert manager.store.get('items', item['id'])['state'] == 'complete'
        assert not remote.submitted


@pytest.mark.parametrize('ids', [[True], [' '], [None], ['one', 'one']])
def test_legacy_cleanup_rejects_malformed_or_duplicate_image_ids(tmp_path, ids):
    video = tmp_path / 'original.mp4'; video.write_bytes(b'preserve-video')
    archive = tmp_path / 'photos.zip'; archive.write_bytes(b'preserve-archive')
    photo = tmp_path / 'one.jpg'; photo.write_bytes(b'preserve-photo')
    with sqlite3.connect(':memory:') as db:
        manager = SlicePackageManager(db, Remote())
        for name in ('bed', 'magnet'):
            manager.store.save('items', {'id': name, 'category': name, 'state': 'complete',
                'videoPath': str(video), 'videoFileVersion': source_fingerprint(video),
                'path': str(archive), 'fileVersion': source_fingerprint(archive),
                'imageDir': str(tmp_path), 'images': [photo.name],
                'imageVersions': {photo.name: source_fingerprint(photo)},
                'imageCount': 1, 'actualImages': 1, 'jobId': '701', 'verifiedJobId': '701',
                'outputImageIds': ids})
        manager.delete_if_both_complete(manager.store.get('items', 'magnet'))
        assert video.exists() and archive.exists() and photo.exists()


@pytest.mark.asyncio
async def test_restart_requeues_only_orphan_upload_and_preserves_multipart_receipts(tmp_path):
    remote = Remote()
    with sqlite3.connect(':memory:') as db:
        before = SlicePackageManager(db, remote)
        for identity, state in [('interrupted', 'uploading'), ('uploaded', 'uploaded'),
                                ('submitted', 'submitting'), ('imported', 'importing')]:
            item = package(tmp_path, state=state)
            item.update(id=identity, uploadId='multipart-one', parts={'1': {'etag': 'part-one', 'size': 7}},
                        completionAttempted=True, phase='completing', bytes=7)
            before.store.save('items', item)
        before.pause()
        restored = SlicePackageManager(db, remote)
        await restored.start()
        try:
            row = restored.store.get('items', 'interrupted')
            assert row['state'] == 'queued' and row['phase'] == 'reconciling'
            assert row['uploadId'] == 'multipart-one' and row['parts']['1']['etag'] == 'part-one'
            assert row['completionAttempted'] and row['bytes'] == 7 and row['object']['key'] == 'one-object'
            assert restored.paused and not restored.running
            for identity, state in [('uploaded', 'uploaded'), ('submitted', 'submitting'), ('imported', 'importing')]:
                assert restored.store.get('items', identity)['state'] == state
        finally:
            await restored.shutdown()
