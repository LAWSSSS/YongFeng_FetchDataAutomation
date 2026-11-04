"""Archived capture recovery and deliberate recapture; isolated HTTP/SQLite only."""
import asyncio
import copy
import json
import sqlite3
from datetime import date

import httpx
import pytest

from app.capture_history import keep_receipts, matching_receipts
from app.remote import RemoteError
from app.submissions import SubmissionLog
from app.videos import Conflict
from app.workspace_files import file_version
from scripts.retain_capture_history import (TABLES, apply_records, collect_files,
                                            retention_plan, submission_day)
from scripts.reset_managed_data import delete_manifest
from tests.test_collection_review import queried
from tests.test_simple_flow import app_factory, new_video, platform, wait_until, wait_video_operation
from tests.test_slicing_performance import close_app
from tests.test_workspace import enable


def archive(db, table, row):
    db.execute('CREATE TABLE IF NOT EXISTS retired_records '
               '(table_name TEXT, record_id TEXT, payload TEXT, PRIMARY KEY(table_name,record_id))')
    db.execute('INSERT OR REPLACE INTO retired_records VALUES(?,?,?)',
               (table, row['id'], json.dumps(row)))
    db.execute(f'DELETE FROM {table} WHERE id=?', (row['id'],))
    db.commit()


@pytest.mark.asyncio
@pytest.mark.parametrize('lost_response', [False, True])
async def test_explicit_recapture_keeps_playable_previous_version_and_replays_once(app_factory, platform, lost_response):
    app = app_factory()
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://127.0.0.1') as client:
            row = await new_video(client, app)
            manager = app.state.videos
            original = copy.deepcopy(manager.store.get(row['id'])['versions'][0])
            assert row['capabilities']['canForceRecapture']
            platform.fail_after_write = lost_response
            body = {'version': 0, 'expectedRevision': 0, 'requestId': 'force-once', 'mode': 'force'}
            response = await client.post(f'/api/videos/{row["id"]}/recapture', json=body)
            assert response.status_code == 202, response.text
            operation = await wait_video_operation(app, response)
            assert operation['state'] == 'complete', operation
            replay = await client.post(f'/api/videos/{row["id"]}/recapture', json=body)
            assert replay.json()['operationId'] == response.json()['operationId']
            await manager.check_once()
            saved = manager.store.get(row['id'])
            assert len(saved['versions']) == 2 and saved['activeRevision'] == 1
            assert saved['versions'][0] == original
            assert saved['versions'][1]['state'] == 'ready'
            assert saved['versions'][1]['keepPreviousCapture']
            assert manager._manual_media_policy(saved['versions'][1])
            assert not saved['versions'][1].get('downloadRequested')
            assert platform.batch_sizes == [1, 1]
            preview = await client.get(f'/api/videos/{row["id"]}/versions/0/stream', headers={'Range': 'bytes=0-31'})
            assert preview.status_code == 206 and preview.content == platform.media[:32]
            assert original['taskId'] in platform.parents
    finally:
        await close_app(app)


@pytest.mark.asyncio
async def test_recapture_reconciles_after_inventory_outage_without_creating_again(app_factory, platform, monkeypatch):
    app = app_factory()
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://127.0.0.1') as client:
            row = await new_video(client, app)
            manager = app.state.videos
            original = app.state.jobs.remote_parents
            async def inventory(target):
                if len(platform.parents) > 1:
                    raise RemoteError('offline', 'NETWORK')
                return await original(target)
            monkeypatch.setattr(app.state.jobs, 'remote_parents', inventory)
            platform.fail_after_write = True
            result = await manager.recapture_unavailable(row['id'], {'version': 0, 'expectedRevision': 0,
                'requestId': 'unknown-submit', 'mode': 'force'})
            await wait_until(lambda: manager.store.get_operation(result['operationId'])['state'] == 'reconciling')
            assert len(manager.store.get(row['id'])['versions']) == 2
            monkeypatch.setattr(app.state.jobs, 'remote_parents', original)
            await manager._run_operation(result['operationId'])
            assert manager.store.get_operation(result['operationId'])['state'] == 'complete'
            assert platform.batch_sizes == [1, 1]
    finally:
        await close_app(app)


@pytest.mark.asyncio
async def test_recapture_registration_failure_does_not_leave_processing_blocked(app_factory, monkeypatch):
    app = app_factory()
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://127.0.0.1') as client:
            row = await new_video(client, app)
            control = app.state.media_control
            control.refresh()
            previous = control.get('collection:' + row['id'])
            def fail(*args, **kwargs):
                raise sqlite3.OperationalError('injected operation save failure')
            monkeypatch.setattr(app.state.videos.store, 'save_operation', fail)
            with pytest.raises(sqlite3.OperationalError):
                await app.state.videos.recapture_unavailable(row['id'], {
                    'version': 0, 'expectedRevision': 0, 'requestId': 'atomic', 'mode': 'force'})
            assert control.get('collection:' + row['id']) == previous
            assert not app.state.videos.operation_tasks
    finally:
        await close_app(app)


@pytest.mark.asyncio
@pytest.mark.parametrize('collision', ['foreign', 'identity_changed', 'running', 'target_changed', 'rejected'])
async def test_force_recapture_does_not_bypass_foreign_tasks_or_platform_rejection(app_factory, platform, monkeypatch, collision):
    app = app_factory()
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://127.0.0.1') as client:
            row = await new_video(client, app)
            manager = app.state.videos
            task_id = row['versions'][0]['taskId']
            if collision == 'foreign':
                platform.parents['foreign'] = {**platform.parents[task_id], 'id': 'foreign'}
            elif collision == 'identity_changed':
                platform.parents[task_id]['videoName'] = 'someone-else'
            elif collision == 'running':
                platform.parents[task_id]['taskStatus'] = 'RUNNING'
            elif collision == 'target_changed':
                job = app.state.jobs.store.get(row['jobId'])
                job['target']['embedId'] = 'other-node'
                app.state.jobs.store.save(job)
            else:
                original = manager.target.request
                async def reject(path, **kwargs):
                    if path.endswith('/batchSave'):
                        raise RemoteError('重复区间', 'DUPLICATE_INTERVAL')
                    return await original(path, **kwargs)
                monkeypatch.setattr(manager.target, 'request', reject)
            response = await client.post(f'/api/videos/{row["id"]}/recapture', json={
                'version': 0, 'expectedRevision': 0, 'requestId': 'blocked', 'mode': 'force'})
            assert response.status_code == 202
            operation = await wait_video_operation(app, response)
            assert operation['state'] in {'attention', 'conflict'}
            saved = manager.store.get(row['id'])
            assert saved['activeRevision'] == 0
            assert saved['versions'][0]['taskId'] == task_id
            assert saved['versions'][0]['state'] == 'ready'
            assert platform.batch_sizes == [1]
            if collision == 'rejected':
                assert saved['revision'] == 1
                assert saved['versions'][1]['submissionOutcome'] == 'rejected'
    finally:
        await close_app(app)


@pytest.mark.asyncio
async def test_retired_key_is_only_a_hint_and_archived_plan_recapture_is_explicit(app_factory, platform):
    app = app_factory()
    try:
        manager, identity = await queried(app)
        preview = manager.review.get(identity)
        manager.review.submit(identity, {'recordIds': preview['readyIds'], 'expectedPlanRevision': 0, 'requestId': 'first'})
        await manager.tick()
        await asyncio.gather(*tuple(app.state.jobs.running.values()))
        await app.state.videos.check_once()
        await manager.tick()
        video = app.state.videos.store.list()[0]
        job = app.state.jobs.store.get(video['jobId'])
        db = manager.db
        keep_receipts(db, video, job)
        db.execute('CREATE TABLE retired_task_keys(key TEXT PRIMARY KEY)')
        db.execute('INSERT INTO retired_task_keys VALUES(?)', (video['taskKey'],))
        archive(db, 'simple_videos', video)
        archive(db, 'jobs', job)
        second = manager.start({'dateFrom': '2026-09-30', 'dateTo': '2026-10-01', 'requestId': 'new-query'})
        await manager.tick()
        plan = manager.review.get(second['id'])
        assert len(plan['readyIds']) == 1 and plan['tasks'][0]['canRecaptureArchived']
        payload = {'recordId': video['recordId'], 'expectedPlanRevision': plan['planRevision'], 'requestId': 'force-archived'}
        operation = await manager.review.recapture(second['id'], payload)
        await wait_until(lambda: app.state.videos.store.get_operation(operation['operationId'])['state'] == 'complete')
        assert (await manager.review.recapture(second['id'], payload))['operationId'] == operation['operationId']
        with pytest.raises(Conflict, match='请求编号'):
            await manager.review.recapture(second['id'], {**payload, 'expectedPlanRevision': 99})
        restored = app.state.videos.store.get(video['id'])
        assert restored['activeRevision'] == 1 and len(restored['versions']) == 2
        assert restored['versions'][0]['taskId'] == video['versions'][0]['taskId']
        assert platform.batch_sizes == [1, 1]
    finally:
        await close_app(app)


@pytest.mark.asyncio
@pytest.mark.parametrize('remote_present', [False, True])
async def test_stale_key_normal_capture_queries_remote_inventory(app_factory, platform, remote_present):
    app = app_factory()
    try:
        manager, identity = await queried(app)
        row = manager.get(identity)
        task = row['reviewTasks'][0]
        manager.db.execute('CREATE TABLE retired_task_keys(key TEXT PRIMARY KEY)')
        manager.db.execute('INSERT INTO retired_task_keys VALUES(?)', (task['key'],))
        task.update(status='duplicate', duplicateSource='local', reason='旧键')
        row['status'] = 'complete'
        manager._save(row)
        if remote_present:
            from app.domain import gather_payload
            platform.parents['old'] = {**gather_payload(task, row['target']), 'id': 'old', 'taskStatus': 'FIXED'}
        plan = manager.review.get(identity)
        assert plan['readyIds'] == [task['recordId']] and plan['planRevision'] == 1
        manager.review.submit(identity, {'recordIds': plan['readyIds'], 'expectedPlanRevision': 1, 'requestId': 'new'})
        await manager.tick()
        await asyncio.gather(*tuple(app.state.jobs.running.values()))
        await app.state.videos.check_once()
        await manager.tick()
        final = manager.review.get(identity)['tasks'][0]
        assert final['status'] == ('duplicate' if remote_present else 'submitted')
        assert platform.batch_sizes == ([] if remote_present else [1])
        if remote_present:
            assert final['existingTaskId'] == 'old'
    finally:
        await close_app(app)


@pytest.mark.parametrize('value,expected', [
    ('2026-11-02T15:59:59+00:00', date(2026, 11, 2)),
    ('2026-11-02T16:00:00Z', date(2026, 11, 3)),
    ('2026-11-04 23:59:59', date(2026, 11, 4)),
    ('2026-11-05 00:00:00', date(2026, 11, 5)),
    ('invalid', None), (None, None),
])
def test_retention_uses_beijing_submission_day(value, expected):
    assert submission_day(value) == expected


@pytest.mark.asyncio
@pytest.mark.parametrize('remote_state', ['ready', 'waiting', 'missing_result', 'no_result_binding', 'wrong_identity', 'missing_task'])
async def test_archived_video_restores_original_ids_and_preview_without_download(app_factory, platform, remote_state, tmp_path):
    app = app_factory()
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://127.0.0.1') as client:
            row = await new_video(client, app)
            video = app.state.videos.store.get(row['id'])
            if remote_state == 'no_result_binding':
                video['versions'][0]['resultId'] = ''
            job = app.state.jobs.store.get(row['jobId'])
            job['createdAt'] = '2026-11-02T16:00:00Z'
            db = app.state.store.db
            log = SubmissionLog(db)
            log.import_jobs([job], lambda job: {'jobId': job['id']})
            receipt = log.list('collection')[0]
            receipt['submittedAt'] = job['createdAt']
            archive(db, 'simple_submissions', receipt)
            archive(db, 'simple_videos', video)
            archive(db, 'jobs', job)
            plan = retention_plan(db, tmp_path, date(2026, 11, 3))
            assert plan[4] == {video['id']} and not plan[3]
            apply_records(db, *plan[:5], date(2026, 11, 3))
            current = app.state.videos.store.get(video['id'])
            assert current['revision'] == video['revision'] and current['acceptedRevision'] is None
            assert current['versions'][0]['taskId'] == video['versions'][0]['taskId']
            assert current['versions'][0]['state'] == 'submitted'
            assert log.get(receipt['id'])
            task = current['versions'][0]['taskId']
            if remote_state == 'waiting':
                platform.parents[task]['taskStatus'] = 'RUNNING'
                platform.results[task] = []
            elif remote_state in {'missing_result', 'no_result_binding'}:
                platform.results[task] = []
            elif remote_state == 'wrong_identity':
                platform.parents[task]['videoName'] = 'wrong'
            elif remote_state == 'missing_task':
                del platform.parents[task]
            app.state.videos.inventory_cached_at = 0
            await app.state.videos.check_once()
            result = app.state.videos.store.get(video['id'])['versions'][0]
            assert result['state'] == {'ready': 'ready', 'waiting': 'waiting'}.get(remote_state, 'attention')
            if remote_state == 'ready':
                stream = await client.get(f'/api/videos/{video["id"]}/versions/0/stream', headers={'Range': 'bytes=0-7'})
                assert stream.status_code == 206
                (tmp_path / 'disk').mkdir()
                await enable(app.state.workspace, tmp_path / 'disk')
                await app.state.workspace.shutdown()
                public = (await client.get('/api/videos?compact=true')).json()['videos'][0]
                assert public['capabilities']['canConfirmFinal']
            assert not result.get('downloadRequested') and platform.batch_sizes == [1]
    finally:
        await close_app(app)


def test_cleanup_manifest_only_deletes_owned_matching_files_and_resumes(tmp_path):
    tables = {table: {} for table in TABLES}
    files = {}
    for name in ('exclusive', 'shared', 'changed', 'missing', 'unknown'):
        path = tmp_path / (name + '.mp4')
        path.write_bytes(name.encode())
        files[name] = {'filePath': str(path), 'fileVersion': file_version(path, True)}
    tables['simple_videos']['old'] = {'versions': list(files.values())[:4]}
    tables['simple_videos']['keep'] = {'versions': [files['shared']]}
    tables['simple_files']['unknown'] = {'path': files['unknown']['filePath'], 'links': {}}
    (tmp_path / 'changed.mp4').write_bytes(b'changed-version')
    (tmp_path / 'missing.mp4').unlink()
    manifest = {'files': collect_files((tables,), {'old'}, ())}
    journal = tmp_path / 'journal.json'
    delete_manifest(manifest, journal, [])
    state = {entry['path']: entry['state'] for entry in manifest['files']}
    assert state[files['exclusive']['filePath']] == 'deleted'
    assert state[files['shared']['filePath']] == 'retained'
    assert state[files['changed']['filePath']] == 'retained'
    assert state[files['missing']['filePath']] == 'missing'
    assert state[files['unknown']['filePath']] == 'retained'
    delete_manifest(manifest, journal, [])
    assert state == {entry['path']: entry['state'] for entry in manifest['files']}
    assert (tmp_path / 'shared.mp4').exists() and (tmp_path / 'unknown.mp4').exists()


@pytest.mark.asyncio
async def test_retention_prunes_old_capture_but_keeps_receipts_new_records_and_frozen_scope(app_factory, tmp_path):
    app = app_factory()
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://127.0.0.1') as client:
            row = await new_video(client, app)
            video = app.state.videos.store.get(row['id'])
            job = app.state.jobs.store.get(row['jobId'])
            job['createdAt'] = '2026-11-02T15:59:59Z'
            db = app.state.store.db
            archive(db, 'jobs', job)
            archive(db, 'simple_videos', video)
            initial = retention_plan(db, tmp_path, date(2026, 11, 3))
            assert initial[2] == {job['id']} and initial[3] == {video['id']}
            frozen = {'excludedVideos': list(initial[3]), 'excludedJobs': list(initial[2]), 'restoredVideos': []}
            newer_job = {**job, 'id': 'later-job', 'createdAt': '2026-11-05T00:00:00+08:00'}
            newer_video = {**video, 'id': 'later-video', 'jobId': newer_job['id']}
            app.state.jobs.store.save(newer_job)
            app.state.videos.store.save(newer_video)
            # A later import with an old timestamp is outside this frozen operation.
            late_job = {**job, 'id': 'later-import'}
            app.state.jobs.store.save(late_job)
            refreshed = retention_plan(db, tmp_path, date(2026, 11, 3), frozen)
            assert refreshed[2:5] == initial[2:5]
            apply_records(db, *refreshed[:5], date(2026, 11, 3))
            assert not app.state.videos.store.get(video['id'])
            assert not db.execute("SELECT 1 FROM retired_records WHERE table_name='simple_videos' AND record_id=?", (video['id'],)).fetchone()
            assert app.state.videos.store.get('later-video') == newer_video
            assert app.state.jobs.store.get('later-import')
            receipts = matching_receipts(db, video['recordId'], job['target'],
                video['versions'][0]['startTime'], video['versions'][0]['endTime'])
            assert len(receipts) == 1 and receipts[0]['version']['taskId'] == video['versions'][0]['taskId']
    finally:
        await close_app(app)
