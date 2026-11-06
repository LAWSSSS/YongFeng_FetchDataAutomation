import asyncio
import json
from datetime import datetime, timedelta
from pathlib import Path

import httpx
import pytest

from app.domain import CollectionRule, DEFAULT_TARGET, SearchFilters, approval_issue, build_tasks, normalize_record
from app.main import create_app
from app.media import DownloadFailure
from app.remote import RemoteClient
from app.videos import external_video_directory


class FakePlatform:
    def __init__(self):
        self.parents = {}
        self.results = {}
        self.objects = {}
        self.batch_sizes = []
        self.next_id = 0
        self.media = b"\x00\x00\x00\x18ftypisom" + b"video-content" * 400
        self.break_first_download = False
        self.download_calls = 0
        self.fail_after_write = False
        self.deny_object_delete = False
        self.change_on_resume = False
        self.new_tasks_waiting = False
        self.lose_delete_response = ""
        self.require_stopped_delete = False
        self.restart_parent_after_result_delete = False
        self.stop_calls = []
        self.lose_stop_response = False

    @staticmethod
    def answer(data):
        return httpx.Response(200, json={"code": "0", "data": data})

    def result(self, parent_id):
        return self.results.get(parent_id, [])

    def handle(self, request):
        path = request.url.path
        if path.endswith("/record/doLogin") or path.endswith("/account/current-user"):
            return self.answer({"id": "tester"})
        if path.startswith("/platform/oss/") or request.url.host == "storage.test":
            direct = path.removeprefix("/platform/oss") if path.startswith("/platform/oss/") else path
            if request.method == "DELETE":
                if direct not in self.objects:
                    return httpx.Response(404)
                if self.deny_object_delete:
                    return httpx.Response(403)
                if request.headers.get("if-match") != '"v1"':
                    return httpx.Response(412)
                del self.objects[direct]
                if self.lose_delete_response == "object":
                    self.lose_delete_response = ""
                    return httpx.Response(503)
                return httpx.Response(204)
            if direct not in self.objects:
                return httpx.Response(404)
            data = self.objects[direct]
            headers = {"content-type": "video/mp4", "etag": '"v1"', "accept-ranges": "bytes"}
            requested = request.headers.get("range", "")
            if requested:
                first, last = requested[6:].split("-", 1)
                offset = int(first)
                if offset >= len(data):
                    return httpx.Response(416, headers={**headers, "content-range": f"bytes */{len(data)}"})
                end = min(int(last), len(data) - 1) if last else len(data) - 1
                output = data[offset:end + 1]
                headers["content-range"] = f"bytes {offset}-{end}/{len(data)}"
                headers["content-length"] = str(len(output))
                if self.change_on_resume and offset:
                    headers["etag"] = '"v2"'
                return httpx.Response(206, content=output, headers=headers)
            self.download_calls += 1
            headers["content-length"] = str(len(data))
            if self.break_first_download and self.download_calls == 1:
                return httpx.Response(200, content=data[:1024], headers=headers)
            return httpx.Response(200, content=data, headers=headers)
        if path.endswith("/camera/40d496692fd8d3a55822b199be3289b7"):
            return self.answer({"id": "40d496692fd8d3a55822b199be3289b7", "embedId": "ef259982807aa9de45f668892174bf09", "channelId": "1"})
        if path.endswith("/embed/ef259982807aa9de45f668892174bf09"):
            return self.answer({"id": "ef259982807aa9de45f668892174bf09"})
        if path.endswith("/data-acquisition/condition-query"):
            return self.answer({"records": list(self.parents.values()), "total": len(self.parents)})
        if path.endswith("/data-acquisition/task-result"):
            body = json.loads(request.content)
            return self.answer({"records": self.result(body["taskId"]), "total": len(self.result(body["taskId"]))})
        if path.endswith("/data-acquisition/batchSave"):
            payload = json.loads(request.content)
            self.batch_sizes.append(len(payload))
            for task in payload:
                self.next_id += 1
                identity = f"task-{self.next_id}"
                result_id = f"result-{self.next_id}"
                object_path = f"/video/{identity}/{result_id}.mp4"
                self.parents[identity] = {**task, "id": identity, "taskStatus": "RUNNING" if self.new_tasks_waiting else "FIXED"}
                self.results[identity] = [] if self.new_tasks_waiting else [{"id": result_id, "taskId": identity, "status": "FINISH",
                                           "url": f"http://storage.test{object_path}?signed=do-not-save",
                                           "beginTime": task["startTime"]}]
                self.objects[object_path] = self.media
            if self.fail_after_write:
                self.fail_after_write = False
                return httpx.Response(503)
            return self.answer(True)
        if path.endswith('/data-acquisition/end-task'):
            identity = json.loads(request.content)['taskId']
            self.stop_calls.append(identity)
            self.parents[identity]['taskStatus'] = 'ENDED'
            if self.lose_stop_response:
                self.lose_stop_response = False
                return httpx.Response(503)
            return self.answer(True)
        if path.endswith("/data-acquisition/delete"):
            payload = json.loads(request.content)
            task_id = payload['id']
            if payload['type'] == 'acInfo':
                task_id = next((task for task, results in self.results.items()
                                if any(result['id'] == payload['id'] for result in results)), '')
            if (self.require_stopped_delete and task_id in self.parents
                    and self.parents[task_id]['taskStatus'] not in {'FIXED', 'ENDED'}):
                return httpx.Response(200, json={'code': 'DWB55-1002', 'message': 'Stop the task before deletion'})
            if payload["type"] == "acInfo":
                for values in self.results.values():
                    values[:] = [row for row in values if row["id"] != payload["id"]]
                if self.restart_parent_after_result_delete and task_id in self.parents:
                    self.parents[task_id]['taskStatus'] = 'RUNNING'
            else:
                self.parents.pop(payload["id"], None)
            if self.lose_delete_response == payload["type"]:
                self.lose_delete_response = ""
                return httpx.Response(503)
            return self.answer(True)
        return httpx.Response(404)


def source_handler(request):
    if request.url.path.endswith("/auth/login"):
        return httpx.Response(200, json={"success": True, "data": {"tokenInfo": {"tokenValue": "fixture-token"}}})
    if request.url.path.endswith("/auth/personal_ccenter/selfInfo"):
        return httpx.Response(200, json={"success": True, "data": {"id": "tester"}})
    if request.url.path.endswith("/intelligence/intelliTaskInfo/page"):
        return httpx.Response(200, json={"success": True, "data": {"records": [
            {"flowCode": "F001", "carNumber": "鲁A12345", "createTime": "2026-09-30 01:00:00"}], "total": 1}})
    if request.url.path.endswith("/intelligence/intelliTaskInfo/getCheckDetail"):
        return httpx.Response(200, json={"success": True, "data": {"stationResults": [{
            "checkStartTime": "2026-09-30 01:50:00", "checkCompleteTime": "2026-09-30 02:02:00"}],
            "totalCheckResult": {"allOriginImageUrls": []}}})
    return httpx.Response(404)


@pytest.fixture
def platform():
    return FakePlatform()


@pytest.fixture
def app_factory(tmp_path, platform):
    def make(video_directory=None, staging_directory=None):
        source = RemoteClient("source", transport=httpx.MockTransport(source_handler))
        target = RemoteClient("target", transport=httpx.MockTransport(platform.handle))
        source.connected = target.connected = True
        return create_app(source, target, tmp_path / "simple-data", video_directory=video_directory,
                          staging_directory=staging_directory)
    return make


async def wait_until(predicate, seconds=3):
    for _ in range(int(seconds / .02)):
        if predicate():
            return
        await asyncio.sleep(.02)
    raise AssertionError("operation did not finish")


async def wait_video_operation(app, response):
    operation_id = response.json().get("operationId")
    assert operation_id, response.text
    await wait_until(lambda: (app.state.videos.store.get_operation(operation_id) or {}).get("state")
                     in {"complete", "attention", "conflict"}, seconds=5)
    return app.state.videos.store.get_operation(operation_id)


@pytest.mark.asyncio
async def test_fixed_external_download_storage_migrates_resumable_parts(app_factory, tmp_path):
    external = tmp_path / "external" / "视频"
    app = app_factory(video_directory=external)
    videos = app.state.videos
    identity = "a" * 24
    videos.store.save({"id": identity, "jobId": "test", "name": "TEST", "plate": "TEST",
                       "qualityStart": "2026-09-30 00:00:00", "revision": 0, "acceptedRevision": None,
                       "activeRevision": 0, "requests": {}, "versions": [{"revision": 0,
                       "state": "ready", "startTime": "2026-09-30 00:00:00"}]})
    legacy_part = videos.legacy_work / identity / "v0.part"
    legacy_part.parent.mkdir(parents=True)
    legacy_part.write_bytes(b"resumable-partial-video")

    assert videos.download_workers() == {"active": 0, "queued": 0, "max": 5, "revision": 0}
    async with app.router.lifespan_context(app):
        migrated = videos.work / identity / "v0.part"
        assert migrated.read_bytes() == b"resumable-partial-video"
        assert not legacy_part.exists()
        assert videos.store.directory() == str(external.resolve())
        selected = tmp_path / "custom-output"
        videos.configure_directory(str(selected))
        assert videos.store.directory() == str(selected.resolve())
    assert videos.work == external.resolve() / ".download-cache"


def test_external_video_default_does_not_probe_mount(monkeypatch):
    monkeypatch.setattr(Path, "is_dir", lambda self: pytest.fail("external volume checked during import"))
    monkeypatch.setattr("app.videos.os.path.ismount",
                        lambda path: pytest.fail("external mount checked during import"))
    assert external_video_directory() == Path("/Volumes/out/视频")


@pytest.mark.asyncio
async def test_downloads_continue_to_local_staging_while_external_storage_is_unavailable(app_factory, platform, tmp_path):
    app = app_factory(video_directory=tmp_path / "external" / "视频",
                      staging_directory=tmp_path / "local-work")
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1") as client:
            app.state.videos.ffprobe = lambda path: asyncio.sleep(0, result=60.0)
            row = await new_video(client, app)
            app.state.videos.prepare_fixed_storage = lambda: (_ for _ in ()).throw(OSError("drive unavailable"))
            app.state.videos.storage_ready = False
            response = await client.post(f'/api/videos/{row["id"]}/confirm', json={
                "version": 0, "expectedRevision": 0, "requestId": "confirm-local-first"})
            assert response.status_code == 202
            part = app.state.videos.work / row["id"] / "v0.part"
            await wait_until(lambda: app.state.videos.version(app.state.videos.store.get(row["id"]), 0)["state"] == "publishing" and part.is_file())
            saved = app.state.videos.store.get(row["id"])["versions"][0]
            assert saved["downloadRequested"] and saved["state"] == "publishing"
            assert part.read_bytes() == platform.media
            assert not saved.get("filePath")


@pytest.mark.asyncio
async def test_direct_external_transfer_skips_startup_probe(app_factory, monkeypatch, tmp_path):
    app = app_factory(video_directory=tmp_path / "external" / "视频",
                      staging_directory=tmp_path / "local-work")
    manager = app.state.videos
    manager.require_mounted_external = True
    manager.storage_ready = False
    manager.target.connected = False
    monkeypatch.setattr(manager, "prepare_fixed_storage",
                        lambda: pytest.fail("startup must attempt direct transfer without probing storage"))
    monkeypatch.setattr(manager, "migrate_legacy_partials",
                        lambda: pytest.fail("startup must leave local staging in place"))

    await manager.start()
    try:
        assert manager.storage_ready
        assert manager.storage_check_task is None
    finally:
        await manager.shutdown()


@pytest.mark.asyncio
async def test_local_staging_migrates_and_verifies_external_resumable_parts(app_factory, tmp_path):
    external = tmp_path / "external" / "视频"
    staging = tmp_path / "local-work"
    app = app_factory(video_directory=external, staging_directory=staging)
    videos = app.state.videos
    identity = "b" * 24
    videos.store.save({"id": identity, "jobId": "test", "name": "TEST", "plate": "TEST",
                       "qualityStart": "2026-09-30 00:00:00", "revision": 0, "acceptedRevision": None,
                       "activeRevision": 0, "requests": {}, "versions": [{"revision": 0,
                       "state": "ready", "startTime": "2026-09-30 00:00:00"}]})
    legacy_part = external / ".download-cache" / identity / "v0.part"
    legacy_part.parent.mkdir(parents=True)
    legacy_part.write_bytes(b"verified-external-part")

    async with app.router.lifespan_context(app):
        staged_part = staging / identity / "v0.part"
        assert staged_part.read_bytes() == b"verified-external-part"
        assert not legacy_part.exists()
        assert videos.work == staging.resolve()
        assert videos.store.directory() == str(external.resolve())


async def preview_video(client):
    query = await client.post('/api/search', json={"filters": {
        "dateFrom": "2026-09-30", "dateTo": "2026-09-30",
        "timeFrom": "00:00:00", "timeTo": "23:59:59", "plates": ""}})
    assert query.status_code == 202
    search_id = query.json()["id"]
    result = None
    for _ in range(50):
        result = (await client.get(f"/api/search/{search_id}")).json()
        if result["status"] == "complete":
            break
        await asyncio.sleep(.01)
    assert result["status"] == "complete"
    plan = (await client.post('/api/preview', json={"searchId": search_id, "recordIds": ["F001"]})).json()
    assert plan["rule"] == {"offsetMinutes": 5, "durationMinutes": 60}
    assert plan["tasks"][0]["startTime"] == "2026-09-30 01:55:00"
    assert plan["tasks"][0]["endTime"] == "2026-09-30 02:55:00"
    return plan


async def new_video(client, app, download_after_collection=False):
    plan = await preview_video(client)
    submit = await client.post('/api/jobs', json={"confirmed": True, "planId": plan["id"],
                                               "downloadAfterCollection": download_after_collection})
    assert submit.status_code == 202
    jobs = app.state.jobs
    await wait_until(lambda: not jobs.running)
    await app.state.videos.check_once()
    rows = (await client.get('/api/videos')).json()["videos"]
    assert len(rows) == 1
    return rows[0]


def test_eligibility_and_cross_midnight():
    filters = SearchFilters(dateFrom="2026-09-30", dateTo="2026-09-30")
    raw = {"flowCode": "F1", "carNumber": "未识别车牌", "createTime": "2026-09-30 23:00:00"}
    detail = {"stationResults": [{"checkStartTime": "2026-09-30 23:55:00", "checkCompleteTime": "2026-10-01 00:06:00"}],
              "totalCheckResult": {"allOriginImageUrls": ["photo.jpg"]}}
    row = normalize_record(raw, detail, filters)
    assert row["plate"] == "VIRTUAL" and not approval_issue(row)
    task = build_tasks([row], CollectionRule(), {"origin": "x", "cameraId": "c", "embedId": "e"})[0]
    assert task["videoName"] == "VIRTUAL_001"
    assert task["startTime"] == "2026-10-01 00:00:00"
    assert task["endTime"] == "2026-10-01 01:00:00"
    row["photoCount"] = 0
    assert approval_issue(row)
    row["plate"] = "鲁A12345"
    row["inspectionDurationSeconds"] = 600
    assert approval_issue(row)


@pytest.mark.asyncio
async def test_collect_and_download_requires_directory_then_publishes_only_opted_in_batch(app_factory, platform, tmp_path):
    app = app_factory()
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1:8766") as client:
            app.state.videos.ffprobe = lambda path: asyncio.sleep(0, result=3600.0)
            plan = await preview_video(client)
            body = {"confirmed": True, "planId": plan["id"], "downloadAfterCollection": True}
            missing = await client.post('/api/jobs', json=body)
            assert missing.status_code == 400
            assert not app.state.jobs.store.list() and not platform.batch_sizes
            directory = tmp_path / 'approved-downloads'
            await client.post('/api/settings/directory', json={"directory": str(directory)})
            first = (await client.post('/api/jobs', json=body)).json()
            await wait_until(lambda: not app.state.jobs.running)
            await app.state.videos.check_once()
            await wait_until(lambda: not app.state.videos.running)
            row = app.state.videos.store.list()[0]
            assert row['versions'][0]['state'] == 'saved'
            assert Path(row['versions'][0]['filePath']).parent == directory
            assert Path(row['versions'][0]['filePath']).read_bytes() == platform.media
            assert row['activeRevision'] == 0 and row['batchDownloadApplied']
            replay = (await client.post('/api/jobs', json=body)).json()
            assert replay['id'] == first['id'] and len(platform.batch_sizes) == 1
            assert len(list(directory.glob('*.mp4'))) == 1


@pytest.mark.asyncio
async def test_batch_download_waits_and_cancellation_and_correction_survive_sync_and_restart(app_factory, platform, tmp_path):
    platform.new_tasks_waiting = True
    app = app_factory()
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1:8766") as client:
            directory = tmp_path / 'first-directory'
            await client.post('/api/settings/directory', json={"directory": str(directory)})
            row = await new_video(client, app, download_after_collection=True)
            assert row['versions'][0]['downloadRequested'] and not platform.download_calls
            await client.post('/api/settings/directory', json={"directory": str(tmp_path / 'later-directory')})
            assert app.state.videos.store.get(row['id'])['versions'][0]['outputDirectory'] == str(directory)
            response = await client.post(f"/api/videos/{row['id']}/cancel-download", json={
                "version": 0, "expectedRevision": 0, "requestId": "stop-batch-download"})
            assert response.status_code == 200
            app.state.videos.sync_jobs()
            assert not app.state.videos.store.get(row['id'])['versions'][0]['downloadRequested']
            corrected = await client.post(f"/api/videos/{row['id']}/corrections", json={
                "startTime": "2026-09-30 02:12:34", "durationSeconds": 1800,
                "expectedRevision": 0, "requestId": "manual-after-cancel"})
            assert corrected.status_code == 202
            app.state.videos.sync_jobs()
            assert app.state.videos.store.get(row['id'])['activeRevision'] == 1
    restarted = app_factory()
    async with restarted.router.lifespan_context(restarted):
        restarted.state.videos.sync_jobs()
        saved = restarted.state.videos.store.get(row['id'])
        assert saved['activeRevision'] == 1 and saved['revision'] == 1
        assert saved['versions'][1]['startTime'] == '2026-09-30 02:12:34'
        assert not saved['versions'][0]['downloadRequested']
        assert not saved['versions'][1].get('downloadRequested')
        assert len(platform.batch_sizes) == 2  # Original and the user's correction, never a replay.
        assert platform.download_calls == 0


@pytest.mark.asyncio
async def test_waiting_batch_download_resumes_after_restart_with_original_bound_directory(app_factory, platform, tmp_path):
    platform.new_tasks_waiting = True
    app = app_factory()
    directory = tmp_path / 'bound-output'
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1:8766") as client:
            await client.post('/api/settings/directory', json={"directory": str(directory)})
            row = await new_video(client, app, download_after_collection=True)
            parent_id = row['versions'][0]['taskId']
            await client.post('/api/settings/directory', json={"directory": str(tmp_path / 'changed-setting')})
    # Complete the existing task, with no new submission during recovery.
    platform.parents[parent_id]['taskStatus'] = 'FIXED'
    platform.results[parent_id] = [{"id": "result-1", "taskId": parent_id, "status": "FINISH",
        "url": f"http://storage.test/video/{parent_id}/result-1.mp4?signed=do-not-save",
        "beginTime": row['versions'][0]['startTime']}]
    restarted = app_factory()
    restarted.state.videos.ffprobe = lambda path: asyncio.sleep(0, result=3600.0)
    async with restarted.router.lifespan_context(restarted):
        await restarted.state.videos.check_once()
        await wait_until(lambda: not restarted.state.videos.running)
        saved = restarted.state.videos.store.get(row['id'])
        assert saved['versions'][0]['state'] == 'saved'
        assert Path(saved['versions'][0]['filePath']).parent == directory
        assert Path(saved['versions'][0]['filePath']).read_bytes() == platform.media
        assert len(platform.batch_sizes) == 1


@pytest.mark.asyncio
async def test_existing_batch_replay_cannot_opt_into_download_and_remote_duplicate_never_downloads(app_factory, tmp_path):
    app = app_factory()
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1:8766") as client:
            row = await new_video(client, app)
            await client.post('/api/settings/directory', json={"directory": str(tmp_path / 'final')})
            first = app.state.jobs.store.list()[0]
            replay = await client.post('/api/jobs', json={"confirmed": True, "planId": first['planId'], "downloadAfterCollection": True})
            assert replay.json()['id'] == first['id'] and not replay.json()['downloadAfterCollection']
            app.state.videos.sync_jobs()
            assert not app.state.videos.store.get(row['id'])['versions'][0].get('downloadRequested')
            # A new preview of this time range must be deduplicated, not approved for download.
            plan = await preview_video(client)
            assert plan['tasks'][0]['status'] == 'duplicate'
            duplicate = await client.post('/api/jobs', json={"confirmed": True, "planId": plan['id'], "downloadAfterCollection": True})
            assert duplicate.status_code == 400
            assert len(app.state.videos.store.list()) == 1


@pytest.mark.asyncio
async def test_capture_preview_correction_withdraw_and_final_replacement(app_factory, platform, tmp_path):
    app = app_factory()
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1:8766") as client:
            app.state.videos.ffprobe = lambda path: asyncio.sleep(0, result=60.0)
            original = await new_video(client, app)
            original_id = original["versions"][0]["taskId"]
            assert original["versions"][0]["state"] == "ready"
            assert not (app.state.videos.work / original["id"]).exists()
            streamed = await client.get(f'/api/videos/{original["id"]}/versions/0/stream', headers={"Range": "bytes=0-15"})
            assert streamed.status_code == 206 and streamed.content == platform.media[:16]
            directory = tmp_path / "final"
            assert (await client.post('/api/settings/directory', json={"directory": str(directory)})).status_code == 200
            first = await client.post(f'/api/videos/{original["id"]}/confirm', json={
                "version": 0, "expectedRevision": 0, "requestId": "confirm-original"})
            assert first.status_code == 202
            await wait_until(lambda: not app.state.videos.running)
            original_saved = app.state.videos.store.get(original["id"])
            old_file = Path(original_saved["versions"][0]["filePath"])
            assert old_file.is_file()
            payload = {"startTime": "2026-09-30 02:10:00", "durationSeconds": 2400,
                       "expectedRevision": 0, "requestId": "correction-1"}
            correction = await client.post(f'/api/videos/{original["id"]}/corrections', json=payload)
            assert correction.status_code == 202
            await wait_video_operation(app, correction)
            replay = await client.post(f'/api/videos/{original["id"]}/corrections', json=payload)
            assert replay.status_code == 202 and replay.json()["operationId"] == correction.json()["operationId"]
            assert app.state.videos.store.get(original["id"])["revision"] == 1
            assert (await client.post(f'/api/videos/{original["id"]}/corrections', json={**payload,
                "requestId": "stale"})).status_code == 409
            await app.state.videos.check_once()
            draft = app.state.videos.store.get(original["id"])
            candidate_id = draft["versions"][-1]["taskId"]
            assert draft["versions"][-1]["state"] == "ready", draft["versions"][-1]
            assert old_file.is_file() and original_id in platform.parents
            withdrawn = await client.post(f'/api/videos/{original["id"]}/withdraw', json={
                "version": 1, "expectedRevision": 1, "requestId": "withdraw-1"})
            assert withdrawn.status_code == 202, withdrawn.text
            await wait_video_operation(app, withdrawn)
            assert candidate_id not in platform.parents
            assert old_file.is_file() and original_id in platform.parents
            second = await client.post(f'/api/videos/{original["id"]}/corrections', json={
                **payload, "expectedRevision": 2, "requestId": "correction-2"})
            assert second.status_code == 202
            await wait_video_operation(app, second)
            await app.state.videos.check_once()
            row = app.state.videos.store.get(original["id"])
            assert row["activeRevision"] == 3
            accepted = await client.post(f'/api/videos/{original["id"]}/confirm', json={
                "version": 3, "expectedRevision": 3, "requestId": "confirm-new"})
            assert accepted.status_code == 202
            await wait_until(lambda: not app.state.videos.running)
            final = app.state.videos.store.get(original["id"])
            assert final["acceptedRevision"] == 3 and not final["cleanupPending"]
            assert final["versions"][-1]["state"] == "saved"
            assert Path(final["versions"][-1]["filePath"]).is_file()
            assert not old_file.exists() and original_id not in platform.parents
            assert platform.objects == {f'/video/{final["versions"][-1]["taskId"]}/{final["versions"][-1]["resultId"]}.mp4': platform.media}
            assert len(list(directory.glob('*.mp4'))) == 1


@pytest.mark.asyncio
async def test_accepted_write_lost_response_and_range_resume(app_factory, platform, tmp_path):
    platform.fail_after_write = True
    platform.break_first_download = True
    app = app_factory()
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1:8766") as client:
            app.state.videos.ffprobe = lambda path: asyncio.sleep(0, result=60.0)
            row = await new_video(client, app)
            assert len(platform.parents) == 1 and platform.batch_sizes == [1]
            folder = tmp_path / "output"
            await client.post('/api/settings/directory', json={"directory": str(folder)})
            response = await client.post(f'/api/videos/{row["id"]}/confirm', json={
                "version": 0, "expectedRevision": 0, "requestId": "confirm"})
            assert response.status_code == 202
            await wait_until(lambda: not app.state.videos.running, seconds=6)
            saved = app.state.videos.store.get(row["id"])
            assert saved["versions"][0]["state"] == "saved"
            assert saved["versions"][0]["totalBytes"] == len(platform.media)
            assert platform.download_calls == 1  # second request used Range
            assert len(platform.parents) == 1 and list(folder.glob('*.mp4'))


@pytest.mark.asyncio
async def test_batches_of_twenty_are_submitted_once(app_factory, platform):
    app = app_factory()
    async with app.router.lifespan_context(app):
        records = []
        for index in range(21):
            start = datetime(2026, 9, 30, 1, 0) + timedelta(minutes=index)
            records.append({"id": f"r{index}", "flowCode": f"f{index}", "plate": f"鲁A{index:05d}",
                            "sourceTime": start.strftime("%Y-%m-%d %H:%M:%S"),
                            "checkStartTime": start.strftime("%Y-%m-%d %H:%M:%S"),
                            "checkCompleteTime": (start + timedelta(minutes=11)).strftime("%Y-%m-%d %H:%M:%S"),
                            "photoCount": 0, "inspectionDurationSeconds": 660})
        tasks = build_tasks(records, CollectionRule(), dict(DEFAULT_TARGET))
        job = app.state.jobs.start({"id": "batch-21", "demo": False, "target": dict(DEFAULT_TARGET), "tasks": tasks})
        await wait_until(lambda: not app.state.jobs.running)
        assert app.state.jobs.store.get(job["id"])["status"] == "complete"
        assert platform.batch_sizes == [20, 1] and len(platform.parents) == 21


@pytest.mark.asyncio
async def test_restart_preserves_binding_without_new_submission(app_factory, platform):
    app = app_factory()
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1:8766") as client:
            row = await new_video(client, app)
            identity, parent = row["id"], row["versions"][0]["taskId"]
    reopened = app_factory()
    async with reopened.router.lifespan_context(reopened):
        await reopened.state.videos.check_once()
        persisted = reopened.state.videos.store.get(identity)
        assert persisted["versions"][0]["taskId"] == parent
        assert persisted["versions"][0]["state"] == "ready"
        assert platform.batch_sizes == [1]


@pytest.mark.asyncio
async def test_failed_cloud_cleanup_keeps_bound_old_task_and_new_final(app_factory, platform, tmp_path):
    app = app_factory()
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1:8766") as client:
            app.state.videos.ffprobe = lambda path: asyncio.sleep(0, result=60.0)
            row = await new_video(client, app)
            original_id = row["versions"][0]["taskId"]
            await client.post('/api/settings/directory', json={"directory": str(tmp_path / "final")})
            changed = await client.post(f'/api/videos/{row["id"]}/corrections', json={
                "startTime": "2026-09-30 02:10:00", "durationSeconds": 3000,
                "expectedRevision": 0, "requestId": "change"})
            assert changed.status_code == 202
            await wait_video_operation(app, changed)
            await app.state.videos.check_once()
            platform.deny_object_delete = True
            response = await client.post(f'/api/videos/{row["id"]}/confirm', json={
                "version": 1, "expectedRevision": 1, "requestId": "confirm"})
            assert response.status_code == 202, response.text
            await wait_until(lambda: not app.state.videos.running)
            pending = app.state.videos.store.get(row["id"])
            assert pending["acceptedRevision"] == 1 and pending["cleanupPending"]
            assert Path(pending["versions"][1]["filePath"]).is_file()
            assert original_id in platform.parents
            platform.deny_object_delete = False
            await app.state.videos.finish_cleanup(row["id"])
            done = app.state.videos.store.get(row["id"])
            assert not done["cleanupPending"] and original_id not in platform.parents


@pytest.mark.asyncio
async def test_changed_media_during_range_resume_stops_without_publishing(app_factory, platform, tmp_path):
    platform.break_first_download = True
    platform.change_on_resume = True
    app = app_factory()
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1:8766") as client:
            app.state.videos.ffprobe = lambda path: asyncio.sleep(0, result=60.0)
            row = await new_video(client, app)
            await client.post('/api/settings/directory', json={"directory": str(tmp_path / "final")})
            await client.post(f'/api/videos/{row["id"]}/confirm', json={
                "version": 0, "expectedRevision": 0, "requestId": "confirm"})
            await wait_until(lambda: not app.state.videos.running, seconds=6)
            failed = app.state.videos.store.get(row["id"])
            assert failed["versions"][0]["state"] == "failed"
            assert failed["acceptedRevision"] is None
            assert list((tmp_path / "final").glob('*.mp4')) == []


@pytest.mark.asyncio
async def test_correction_write_response_lost_reconciles_once(app_factory, platform):
    app = app_factory()
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1:8766") as client:
            row = await new_video(client, app)
            platform.fail_after_write = True
            body = {"startTime": "2026-09-30 02:15:00", "durationSeconds": 1800,
                    "expectedRevision": 0, "requestId": "same-request"}
            result = await client.post(f'/api/videos/{row["id"]}/corrections', json=body)
            assert result.status_code == 202
            await wait_video_operation(app, result)
            current = app.state.videos.store.get(row["id"])
            version = current["versions"][-1]
            assert version["taskId"] in platform.parents
            replay = await client.post(f'/api/videos/{row["id"]}/corrections', json=body)
            assert replay.status_code == 202 and replay.json()["operationId"] == result.json()["operationId"]
            assert current["revision"] == 1
            assert platform.batch_sizes == [1, 1] and len(platform.parents) == 2


@pytest.mark.asyncio
async def test_restart_resumes_existing_partial_without_resubmitting(app_factory, platform, tmp_path):
    app = app_factory()
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1:8766") as client:
            row = await new_video(client, app)
            destination = tmp_path / "final"
            await client.post('/api/settings/directory', json={"directory": str(destination)})
            version = app.state.videos.store.get(row["id"])
            first = version["versions"][0]
            first.update(state="downloading", downloadRequested=True, outputDirectory=str(destination),
                         etag='"v1"', totalBytes=len(platform.media), receivedBytes=1024)
            app.state.videos.store.save(version)
            part_dir = app.state.videos.work / row["id"]
            part_dir.mkdir(parents=True)
            (part_dir / "v0.part").write_bytes(platform.media[:1024])
    reopened = app_factory()
    async with reopened.router.lifespan_context(reopened):
        reopened.state.videos.ffprobe = lambda path: asyncio.sleep(0, result=60.0)
        await reopened.state.videos.check_once()
        await wait_until(lambda: not reopened.state.videos.running)
        saved = reopened.state.videos.store.get(row["id"])
        assert saved["versions"][0]["state"] == "saved"
        assert Path(saved["versions"][0]["filePath"]).is_file()
        assert platform.batch_sizes == [1]
        assert platform.download_calls == 0  # only Range was fetched


@pytest.mark.asyncio
async def test_bad_new_media_preserves_previous_local_and_cloud(app_factory, platform, tmp_path):
    app = app_factory()
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1:8766") as client:
            app.state.videos.ffprobe = lambda path: asyncio.sleep(0, result=60.0)
            row = await new_video(client, app)
            old_id = row["versions"][0]["taskId"]
            await client.post('/api/settings/directory', json={"directory": str(tmp_path / "final")})
            await client.post(f'/api/videos/{row["id"]}/confirm', json={
                "version": 0, "expectedRevision": 0, "requestId": "old-confirm"})
            await wait_until(lambda: not app.state.videos.running)
            old = app.state.videos.store.get(row["id"])
            old_path = Path(old["versions"][0]["filePath"])
            await client.post(f'/api/videos/{row["id"]}/corrections', json={
                "startTime": "2026-09-30 02:20:00", "durationSeconds": 1800,
                "expectedRevision": 0, "requestId": "correction"})
            correction_op = app.state.videos.store.list_operations(row["id"])[0]
            await wait_until(lambda: (app.state.videos.store.get_operation(correction_op["id"]) or {}).get("state")
                             in {"complete", "attention", "conflict"}, seconds=5)
            await app.state.videos.check_once()
            async def invalid(_path):
                raise DownloadFailure("文件无法播放。", "invalid_video")
            app.state.videos.ffprobe = invalid
            await client.post(f'/api/videos/{row["id"]}/confirm', json={
                "version": 1, "expectedRevision": 1, "requestId": "new-confirm"})
            await wait_until(lambda: not app.state.videos.running)
            failed = app.state.videos.store.get(row["id"])
            assert failed["acceptedRevision"] == 0
            assert failed["versions"][1]["state"] == "failed", failed["versions"][1]
            assert old_path.is_file() and old_id in platform.parents


@pytest.mark.asyncio
async def test_local_login_keeps_separate_sessions(tmp_path, platform, monkeypatch):
    monkeypatch.setattr("app.main.credentials.load", lambda service: None)
    source = RemoteClient("source", transport=httpx.MockTransport(source_handler))
    target = RemoteClient("target", transport=httpx.MockTransport(platform.handle))
    app = create_app(source, target, tmp_path / "auth")
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1:8766") as client:
            start = (await client.get('/api/session')).json()
            assert not start["source"]["connected"] and not start["target"]["connected"]
            assert (await client.post('/api/session/source/login', json={"account": "s", "password": "fixture"})).status_code == 200
            middle = (await client.get('/api/session')).json()
            assert middle["source"]["connected"] and not middle["target"]["connected"]
            assert (await client.post('/api/session/target/login', json={"account": "t", "password": "fixture"})).status_code == 200
            complete = (await client.get('/api/session')).json()
            assert complete["source"]["connected"] and complete["target"]["connected"]
            assert "fixture" not in str(complete)
            worker = asyncio.create_task(asyncio.sleep(1))
            app.state.videos.running["other-video"] = worker
            try:
                assert (await client.post('/api/session/source/logout', json={})).status_code == 200
                assert (await client.post('/api/session/source/login', json={"account": "s", "password": "fixture"})).status_code == 200
                assert (await client.post('/api/session/target/login', json={"account": "t", "password": "fixture"})).status_code == 400
            finally:
                worker.cancel(); await asyncio.gather(worker, return_exceptions=True)
                app.state.videos.running.pop("other-video", None)


@pytest.mark.asyncio
async def test_withdraw_collecting_correction_keeps_original(app_factory, platform):
    app = app_factory()
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1:8766") as client:
            row = await new_video(client, app)
            original_id = row["versions"][0]["taskId"]
            platform.new_tasks_waiting = True
            reply = await client.post(f'/api/videos/{row["id"]}/corrections', json={
                "startTime": "2026-09-30 02:30:00", "durationSeconds": 1200,
                "expectedRevision": 0, "requestId": "correction"})
            assert reply.status_code == 202
            await wait_video_operation(app, reply)
            candidate = app.state.videos.store.get(row["id"])["versions"][-1]["taskId"]
            assert platform.parents[candidate]["taskStatus"] == "RUNNING"
            # This candidate has no finished result and may be removed by its exact task ID.
            withdrawn = await client.post(f'/api/videos/{row["id"]}/withdraw', json={
                "version": 1, "expectedRevision": 1, "requestId": "withdraw"})
            assert withdrawn.status_code == 202, withdrawn.text
            await wait_video_operation(app, withdrawn)
            assert candidate not in platform.parents and original_id in platform.parents
            assert app.state.videos.store.get(row["id"])["activeRevision"] == 0


@pytest.mark.asyncio
async def test_continuous_corrections_and_withdraw_restore_previous_version(app_factory, platform):
    app = app_factory()
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1") as client:
            row = await new_video(client, app)
            platform.new_tasks_waiting = True
            for revision in range(3):
                payload = {"startTime": f"2026-09-30 02:{10 + revision * 10}:00", "durationSeconds": 1800,
                           "expectedRevision": revision, "requestId": f"continuous-{revision}"}
                changed = await client.post(f'/api/videos/{row["id"]}/corrections', json=payload)
                assert changed.status_code == 202, changed.text
                await wait_video_operation(app, changed)
                current = app.state.videos.store.get(row["id"])
                assert current["activeRevision"] == revision + 1
                assert current["versions"][-1]["previousActiveRevision"] == revision
                replay = await client.post(f'/api/videos/{row["id"]}/corrections', json=payload)
                assert replay.status_code == 202 and replay.json()["operationId"] == changed.json()["operationId"]
            assert len(platform.parents) == 4 and platform.batch_sizes == [1, 1, 1, 1]
            before = app.state.videos.store.get(row["id"])
            previous_id = before["versions"][2]["taskId"]
            withdrawn = await client.post(f'/api/videos/{row["id"]}/withdraw', json={
                "version": 3, "expectedRevision": 3, "requestId": "withdraw-latest"})
            assert withdrawn.status_code == 202, withdrawn.text
            await wait_video_operation(app, withdrawn)
            assert app.state.videos.store.get(row["id"])["activeRevision"] == 2 and previous_id in platform.parents
            stale = await client.post(f'/api/videos/{row["id"]}/corrections', json={
                "startTime": "2026-09-30 02:40:00", "durationSeconds": 1800,
                "expectedRevision": 3, "requestId": "stale-draft"})
            assert stale.status_code == 409 and len(platform.parents) == 3


@pytest.mark.asyncio
async def test_cleanup_of_accepted_version_does_not_delete_later_correction(app_factory, platform, tmp_path):
    app = app_factory()
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1") as client:
            app.state.videos.ffprobe = lambda path: asyncio.sleep(0, result=60.0)
            row = await new_video(client, app)
            original_id = row["versions"][0]["taskId"]
            await client.post('/api/settings/directory', json={"directory": str(tmp_path / 'out')})
            await client.post(f'/api/videos/{row["id"]}/corrections', json={
                "startTime": "2026-09-30 02:10:00", "durationSeconds": 1800,
                "expectedRevision": 0, "requestId": "first"})
            first_operation = app.state.videos.store.list_operations(row["id"])[0]
            await wait_until(lambda: (app.state.videos.store.get_operation(first_operation["id"]) or {}).get("state")
                             in {"complete", "attention", "conflict"}, seconds=5)
            await app.state.videos.check_once()
            platform.deny_object_delete = True
            await client.post(f'/api/videos/{row["id"]}/confirm', json={
                "version": 1, "expectedRevision": 1, "requestId": "save-first"})
            await wait_until(lambda: not app.state.videos.running)
            assert app.state.videos.store.get(row["id"])["cleanupPending"]
            later = await client.post(f'/api/videos/{row["id"]}/corrections', json={
                "startTime": "2026-09-30 02:20:00", "durationSeconds": 1800,
                "expectedRevision": 1, "requestId": "second"})
            assert later.status_code == 202
            await wait_video_operation(app, later)
            new_id = app.state.videos.store.get(row["id"])["versions"][-1]["taskId"]
            platform.deny_object_delete = False
            await app.state.videos.finish_cleanup(row["id"])
            latest = app.state.videos.store.get(row["id"])
            assert new_id in platform.parents and original_id not in platform.parents
            assert latest["activeRevision"] == 2 and latest["acceptedRevision"] == 1
            assert latest["versions"][-1]["state"] != "cloud_deleted"


@pytest.mark.asyncio
async def test_review_recovers_exact_result_and_missing_result_can_be_recaptured(app_factory, platform):
    app = app_factory()
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1") as client:
            row = await new_video(client, app)
            version = row["versions"][0]
            snapshot = {key: version[key] for key in ("taskId", "resultId", "startTime", "endTime")}
            version.update(state="attention", message="绑定任务不存在或身份改变，请在平台核对。")
            app.state.videos.store.save(row)
            payload = {"expectedRevision": 0, "requestId": "review-exact-result"}
            recovered = await client.post(f'/api/videos/{row["id"]}/check', json=payload)
            assert recovered.status_code == 202
            await wait_video_operation(app, recovered)
            current = app.state.videos.store.get(row["id"])
            assert current["versions"][0]["state"] == "ready"
            assert {key: current["versions"][0][key] for key in snapshot} == snapshot
            platform.results[version["taskId"]] = []
            missing = await client.post(f'/api/videos/{row["id"]}/check', json={
                "expectedRevision": 0, "requestId": "review-missing-result"})
            await wait_video_operation(app, missing)
            assert app.state.videos.store.get(row["id"])["versions"][0]["review"]["code"] == "result_missing"
            payload = {"version": 0, "expectedRevision": 0, "requestId": "recapture-missing"}
            recaptured = await client.post(f'/api/videos/{row["id"]}/recapture', json=payload)
            assert recaptured.status_code == 202, recaptured.text
            await wait_video_operation(app, recaptured)
            current = app.state.videos.store.get(row["id"])
            assert current["versions"][-1]["startTime"] == snapshot["startTime"]
            assert current["versions"][0]["resultId"] == snapshot["resultId"]
            assert len(platform.parents) == 2
            assert (await client.post(f'/api/videos/{row["id"]}/recapture', json=payload)).status_code == 202
            assert len(platform.parents) == 2 and platform.batch_sizes == [1, 1]


@pytest.mark.asyncio
@pytest.mark.parametrize('error_code', ['invalid_video', 'range_invalid', 'object_changed', 'invalid_length'])
async def test_invalid_video_recapture_keeps_versions_and_only_creates_once(app_factory, platform, tmp_path, error_code):
    app = app_factory()
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1") as client:
            row = await new_video(client, app)
            version = row["versions"][0]
            version.update(state="failed", errorCode=error_code, downloadRequested=True,
                           message="Video tail cannot be decoded.")
            app.state.videos.store.save(row)
            cached = app.state.videos.work / row["id"] / "v0.part"
            cached.parent.mkdir(parents=True, exist_ok=True)
            cached.write_bytes(b"keep-unverified-original")
            payload = {"version": 0, "expectedRevision": 0, "requestId": "repair-incomplete-video"}
            result = await client.post(f'/api/videos/{row["id"]}/recapture', json=payload)
            assert result.status_code == 202, result.text
            await wait_video_operation(app, result)
            current = app.state.videos.store.get(row["id"])
            assert current["activeRevision"] == 1
            assert current["versions"][1]["startTime"] == version["startTime"]
            assert current["versions"][1]["durationSeconds"] == version["durationSeconds"]
            assert current["versions"][0]["resultId"] == version["resultId"]
            assert current["versions"][0]["state"] == "failed"
            assert cached.read_bytes() == b"keep-unverified-original"
            assert version["taskId"] in platform.parents
            assert version["resultId"] == platform.results[version["taskId"]][0]["id"]
            replay = await client.post(f'/api/videos/{row["id"]}/recapture', json=payload)
            assert replay.status_code == 202 and len(platform.parents) == 2
            stale = await client.post(f'/api/videos/{row["id"]}/recapture', json={**payload, "requestId": "stale-repair"})
            assert stale.status_code == 409 and len(platform.parents) == 2


@pytest.mark.asyncio
async def test_recapture_does_not_turn_disk_failure_into_new_cloud_task(app_factory, platform):
    app = app_factory()
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1") as client:
            row = await new_video(client, app)
            row["versions"][0].update(state="failed", errorCode="disk_full", downloadRequested=True)
            app.state.videos.store.save(row)
            result = await client.post(f'/api/videos/{row["id"]}/recapture', json={
                "version": 0, "expectedRevision": 0, "requestId": "do-not-recapture-disk"})
            assert result.status_code == 400
            assert len(platform.parents) == 1


@pytest.mark.asyncio
async def test_remove_uncorrected_only_clears_own_cache_and_does_not_reappear(app_factory, platform):
    app = app_factory()
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1") as client:
            row = await new_video(client, app)
            folder = app.state.videos.work / row["id"]
            folder.mkdir()
            (folder / 'v0.part').write_bytes(b'cached-video')
            other = app.state.videos.work / 'other-video'
            other.mkdir()
            (other / 'v1.part').write_bytes(b'keep-progress')
            payload = {"expectedRevision": 0, "requestId": "delete-original-history"}
            deleted = await client.post(f'/api/videos/{row["id"]}/remove-uncorrected', json=payload)
            assert deleted.status_code == 200 and deleted.json()["cacheBytes"] == 12
            assert not folder.exists() and (other / 'v1.part').read_bytes() == b'keep-progress'
            assert (await client.post(f'/api/videos/{row["id"]}/remove-uncorrected', json=payload)).status_code == 200
            assert (await client.get('/api/videos')).json()["videos"] == []
            assert row["versions"][0]["taskId"] in platform.parents and platform.objects
    reopened = app_factory()
    async with reopened.router.lifespan_context(reopened):
        assert reopened.state.videos.state()["videos"] == []
        assert platform.batch_sizes == [1]


@pytest.mark.asyncio
async def test_remove_protects_corrections_including_withdrawn_and_stale_requests(app_factory, platform):
    app = app_factory()
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1") as client:
            row = await new_video(client, app)
            await client.post(f'/api/videos/{row["id"]}/corrections', json={
                "startTime": "2026-09-30 02:10:00", "durationSeconds": 1800,
                "expectedRevision": 0, "requestId": "corrected"})
            correction_operation = app.state.videos.store.list_operations(row["id"])[0]
            await wait_until(lambda: (app.state.videos.store.get_operation(correction_operation["id"]) or {}).get("state")
                             in {"complete", "attention", "conflict"}, seconds=5)
            stale = await client.post(f'/api/videos/{row["id"]}/remove-uncorrected', json={
                "expectedRevision": 0, "requestId": "stale-delete"})
            assert stale.status_code == 409
            await app.state.videos.check_once()
            await client.post(f'/api/videos/{row["id"]}/withdraw', json={
                "version": 1, "expectedRevision": 1, "requestId": "withdrawn"})
            await asyncio.gather(*tuple(app.state.videos.operation_tasks.values()))
            refused = await client.post(f'/api/videos/{row["id"]}/remove-uncorrected', json={
                "expectedRevision": 2, "requestId": "refuse-withdrawn-history"})
            assert refused.status_code == 400 and app.state.videos.store.get(row["id"])["activeRevision"] == 0
            assert not app.state.videos.store.get(row["id"]).get("deletedAt")


@pytest.mark.asyncio
async def test_clear_withdrawn_record_verifies_cloud_files_and_never_resurrects(app_factory, platform, tmp_path):
    from app.media import file_digest
    app = app_factory()
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1") as client:
            row = await new_video(client, app)
            await client.post(f'/api/videos/{row["id"]}/corrections', json={
                "startTime": "2026-09-30 02:10:00", "durationSeconds": 2400,
                "expectedRevision": 0, "requestId": "correction"})
            correction_operation = app.state.videos.store.list_operations(row["id"])[0]
            await wait_until(lambda: (app.state.videos.store.get_operation(correction_operation["id"]) or {}).get("state")
                             in {"complete", "attention", "conflict"}, seconds=5)
            await app.state.videos.check_once()
            await client.post(f'/api/videos/{row["id"]}/withdraw', json={
                "version": 1, "expectedRevision": 1, "requestId": "withdraw"})
            await asyncio.gather(*tuple(app.state.videos.operation_tasks.values()))
            row = app.state.videos.store.get(row["id"])
            final = tmp_path / "own-final.mp4"
            final.write_bytes(platform.media)
            row["versions"][0].update(filePath=str(final), sha256=file_digest(final), totalBytes=len(platform.media))
            app.state.videos.store.save(row)
            cache = app.state.videos.work / row["id"]
            cache.mkdir(); (cache / "v0.part").write_bytes(b"partial")
            other = tmp_path / "leave-other-file.mp4"; other.write_bytes(b"keep")
            payload = {"expectedRevision": 2, "requestId": "remove-withdrawn", "deleteCloud": True, "deleteLocalFiles": True}
            response = await client.post(f'/api/videos/{row["id"]}/remove-record', json=payload)
            assert response.status_code == 202, response.text
            await wait_until(lambda: not app.state.videos.running)
            deleted = app.state.videos.store.get(row["id"])
            assert deleted["deletion"]["state"] == "complete" and deleted["deletedAt"]
            assert not platform.parents and not any(platform.results.values()) and not platform.objects
            assert not final.exists() and not cache.exists() and other.read_bytes() == b"keep"
            assert (await client.post(f'/api/videos/{row["id"]}/remove-record', json=payload)).status_code == 202
            await app.state.videos.check_once()
            assert (await client.get('/api/videos')).json()["totalCount"] == 0
    restarted = app_factory()
    async with restarted.router.lifespan_context(restarted):
        assert restarted.state.videos.state()["videos"] == []


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["acInfo", "object", "task"])
async def test_clear_response_lost_resumes_exact_ids(app_factory, platform, phase):
    app = app_factory()
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1") as client:
            row = await new_video(client, app)
            platform.lose_delete_response = phase
            payload = {"expectedRevision": 0, "requestId": "clear-uncertain", "deleteCloud": True, "deleteLocalFiles": True}
            assert (await client.post(f'/api/videos/{row["id"]}/remove-record', json=payload)).status_code == 202
            await wait_until(lambda: not app.state.videos.running)
            first = app.state.videos.store.get(row["id"])
            assert first["deletion"]["state"] == "error" and not first.get("deletedAt")
            assert first["versions"][0]["cleanup"]["started"]
            assert (await client.post(f'/api/videos/{row["id"]}/remove-record', json=payload)).status_code == 202
            await wait_until(lambda: not app.state.videos.running)
            assert app.state.videos.store.get(row["id"])["deletion"]["state"] == "complete"
            assert not platform.parents and not platform.objects and not any(platform.results.values())
            assert platform.batch_sizes == [1]


@pytest.mark.asyncio
async def test_clear_collecting_initial_records_intent_before_lost_response(app_factory, platform):
    platform.new_tasks_waiting = True
    app = app_factory()
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1") as client:
            row = await new_video(client, app)
            # A collecting fixture should not have a storage object until its result exists.
            platform.objects.clear()
            platform.lose_delete_response = "task"
            payload = {"expectedRevision": 0, "requestId": "clear-pending", "deleteCloud": True, "deleteLocalFiles": True}
            await client.post(f'/api/videos/{row["id"]}/remove-record', json=payload)
            await wait_until(lambda: not app.state.videos.running)
            assert app.state.videos.store.get(row["id"])["versions"][0]["cleanup"]["pendingTaskOnly"]
            await client.post(f'/api/videos/{row["id"]}/remove-record', json=payload)
            await wait_until(lambda: not app.state.videos.running)
            assert app.state.videos.store.get(row["id"])["deletedAt"]


@pytest.mark.asyncio
async def test_clear_local_scope_and_stale_revision_preserve_corrections(app_factory, platform):
    app = app_factory()
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1") as client:
            row = await new_video(client, app)
            await client.post(f'/api/videos/{row["id"]}/corrections', json={
                "startTime": "2026-09-30 02:11:11", "durationSeconds": 2100,
                "expectedRevision": 0, "requestId": "preserve"})
            correction_operation = app.state.videos.store.list_operations(row["id"])[0]
            await wait_until(lambda: (app.state.videos.store.get_operation(correction_operation["id"]) or {}).get("state")
                             in {"complete", "attention", "conflict"}, seconds=5)
            stale = await client.post(f'/api/videos/{row["id"]}/remove-record', json={
                "expectedRevision": 0, "requestId": "stale-clear", "deleteCloud": True, "deleteLocalFiles": True})
            assert stale.status_code == 409 and len(platform.parents) == 2
            kept = app.state.videos.store.get(row["id"])
            assert kept["versions"][-1]["startTime"] == "2026-09-30 02:11:11"
            payload = {"expectedRevision": 1, "requestId": "clear-only-local", "deleteCloud": False, "deleteLocalFiles": False}
            await client.post(f'/api/videos/{row["id"]}/remove-record', json=payload)
            await wait_until(lambda: not app.state.videos.running)
            assert app.state.videos.store.get(row["id"])["deletedAt"]
            assert len(platform.parents) == 2 and len(platform.objects) == 2


@pytest.mark.asyncio
async def test_clear_shared_binding_identity_and_modified_file_are_protected(app_factory, platform, tmp_path):
    import copy
    from app.media import file_digest
    app = app_factory()
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1") as client:
            row = await new_video(client, app)
            shared = copy.deepcopy(row); shared["id"] = "a" * 24
            app.state.videos.store.save(shared)
            payload = {"expectedRevision": 0, "requestId": "guard-shared", "deleteCloud": True, "deleteLocalFiles": True}
            await client.post(f'/api/videos/{row["id"]}/remove-record', json=payload)
            await wait_until(lambda: not app.state.videos.running)
            refused = app.state.videos.store.get(row["id"])
            assert refused["deletion"]["state"] == "error" and "其他记录" in refused["deletion"]["message"]
            assert len(platform.parents) == 1 and len(platform.objects) == 1
            await client.post(f'/api/videos/{row["id"]}/cancel-clear', json={"expectedRevision": 1,"requestId": "stop"})
            row = app.state.videos.store.get(row["id"])
            final = tmp_path / "changed.mp4"; final.write_bytes(b"original")
            row["versions"][0].update(filePath=str(final),sha256=file_digest(final),totalBytes=8)
            final.write_bytes(b"modified")
            app.state.videos.store.save(row)
            await client.post(f'/api/videos/{row["id"]}/remove-record', json={
                "expectedRevision": 2,"requestId": "guard-file","deleteCloud": False,"deleteLocalFiles": True})
            await wait_until(lambda: not app.state.videos.running)
            assert app.state.videos.store.get(row["id"])["deletion"]["state"] == "error"
            assert final.read_bytes() == b"modified" and len(platform.parents) == 1


@pytest.mark.asyncio
async def test_record_filters_counts_date_basis_and_no_mutation(app_factory):
    import copy
    app = app_factory()
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1") as client:
            row = await new_video(client, app)
            second = copy.deepcopy(row);second.update(id="b" * 24,name="VIRTUAL_002",plate="VIRTUAL",qualityStart="2026-10-01 01:00:00")
            second["versions"][0].update(startTime="2026-10-02 02:00:00",state="attention")
            app.state.videos.store.save(second)
            filtered = (await client.get('/api/videos',params={"plate":"未识别","status":"review"})).json()
            assert [v["id"] for v in filtered["videos"]] == [second["id"]]
            assert filtered["totalCount"] == 2 and filtered["filteredCount"] == 1
            assert filtered["counts"]["review"] == 1 and filtered["counts"]["ready"] == 1
            for basis,day in (("quality","2026-10-01"),("capture","2026-10-02")):
                assert (await client.get('/api/videos',params={"timeBasis":basis,"dateFrom":day,"dateTo":day})).json()["filteredCount"] == 1
            assert (await client.get('/api/videos',params={"dateFrom":"2026-02-30"})).status_code == 400
            assert app.state.videos.store.get(second["id"])["versions"][0]["startTime"] == "2026-10-02 02:00:00"


@pytest.mark.asyncio
async def test_polling_skips_long_download_lock_without_blocking_other_rows(app_factory):
    app = app_factory()
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1") as client:
            row = await new_video(client, app)
            async with app.state.videos.row_lock(row["id"]):
                await asyncio.wait_for(app.state.videos.check_once(), timeout=.5)
                response = await asyncio.wait_for(client.post(f'/api/videos/{row["id"]}/check', json={
                    "expectedRevision": 0, "requestId": "quick-single-check"}), timeout=.5)
                assert response.status_code == 202
                operation_id = response.json()["operationId"]
                assert app.state.videos.store.get_operation(operation_id)["state"] in {"queued", "running"}
            await wait_until(lambda: (app.state.videos.store.get_operation(operation_id) or {}).get("state")
                             == "complete", seconds=5)


@pytest.mark.asyncio
async def test_queued_video_operations_are_idempotent_and_reject_reused_ids_with_new_content(app_factory, platform):
    app = app_factory()
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1") as client:
            row = await new_video(client, app)
            body = {"startTime": "2026-09-30 02:10:00", "durationSeconds": 1800,
                    "expectedRevision": 0, "requestId": "same-operation"}
            first = await client.post(f'/api/videos/{row["id"]}/corrections', json=body)
            replay = await client.post(f'/api/videos/{row["id"]}/corrections', json=body)
            assert first.status_code == replay.status_code == 202
            assert replay.json()["operationId"] == first.json()["operationId"]
            changed = await client.post(f'/api/videos/{row["id"]}/corrections', json={
                **body, "startTime": "2026-09-30 02:20:00"})
            assert changed.status_code == 409
            operation = await wait_video_operation(app, first)
            assert operation["state"] == "complete"
            current = app.state.videos.store.get(row["id"])
            assert current["revision"] == 1 and len(current["versions"]) == 2
            assert len(platform.parents) == 2 and platform.batch_sizes == [1, 1]


@pytest.mark.asyncio
async def test_partial_recovery_reuses_verified_cache_without_creating_a_platform_video(
        app_factory, platform, tmp_path, monkeypatch):
    from types import SimpleNamespace

    class RecoveryStore:
        def __init__(self, frame_tool):
            self.settings = {"frameToolPath": str(frame_tool)}
            self.items = {}

        def get(self, table, identity):
            return self.items.get(identity) if table == "items" else None

    class RecoverySlicing:
        def __init__(self, frame_tool):
            self.auth = SimpleNamespace(connected=True)
            self.store = RecoveryStore(frame_tool)
            self.packages = SimpleNamespace(store=RecoveryStore(frame_tool))

        def queue_collection_recovery(self, **values):
            identity = "local-recovery-item"
            self.store.items[identity] = {"id": identity, "state": "complete", "progress": 100,
                "collectionRecovery": True, "sourceVideoId": "", "sourceType": "local_collection_recovery",
                "collectionVideoId": values['video_id'], "collectionRevision": values['revision'],
                "fileVersion": values['file_version'],
                "categories": {name: {"state": "complete", "packageId": name, "observations": 8, "skippedSamples": 1}
                               for name in ('bed', 'magnet')}}
            for name in ('bed', 'magnet'):
                self.packages.store.items[name] = {'id': name, 'itemId': identity, 'category': name, 'state': 'complete',
                    'verifiedJobId': 'job-'+name, 'jobId': 'job-'+name,
                    'imageCount': 1, 'actualImages': 1, 'outputImageIds': ['image-'+name],
                    'videoFileVersion': values['file_version']}
            self.values = values
            return self.store.items[identity]

    app = app_factory()
    frame_tool = tmp_path / "ffmpeg"
    frame_tool.write_text("test stub")
    frame_tool.chmod(0o755)
    monkeypatch.setattr("app.slicing.ffprobe_file", lambda *_args: {"duration": 60, "frameRate": 25})
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1") as client:
            row = await new_video(client, app)
            manager = app.state.videos
            slicing = RecoverySlicing(frame_tool)
            manager.slicing = slicing
            saved_row = manager.store.get(row["id"])
            version = saved_row["versions"][0]
            version.update(state="failed", errorCode="invalid_video", downloadRequested=True,
                etag='"v1"', totalBytes=len(platform.media), receivedBytes=len(platform.media))
            manager.store.save(saved_row)
            cache = manager.work / row["id"] / "v0.part"
            cache.parent.mkdir(parents=True)
            cache.write_bytes(platform.media)

            response = await client.post(f'/api/videos/{row["id"]}/recover-decodable', json={
                "expectedRevision": 0, "requestId": "partial-recovery-1"})
            assert response.status_code == 202
            operation = await wait_video_operation(app, response)
            assert operation["state"] == "complete"
            current = manager.store.get(row["id"])
            recovery = current["versions"][0]["recovery"]
            assert recovery["state"] == "partial_complete"
            assert recovery["successfulSamples"] == 16 and recovery["skippedSamples"] == 2
            assert current["versions"][0]["taskId"] == row["versions"][0]["taskId"]
            assert current["versions"][0]["resultId"] == row["versions"][0]["resultId"]
            assert not cache.exists() and Path(recovery["filePath"]).is_file()
            assert slicing.values["video_id"] == row["id"]
            assert slicing.values["path"] == Path(recovery["filePath"])
            assert len(platform.parents) == 1 and platform.batch_sizes == [1]


@pytest.mark.asyncio
async def test_bulk_per_item_conflict_does_not_approve_review_or_stop_others(app_factory, tmp_path):
    import copy
    app = app_factory()
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1") as client:
            row = await new_video(client, app)
            review = copy.deepcopy(row);review.update(id="c" * 24,name="needs-review")
            review["versions"][0]["state"] = "attention";app.state.videos.store.save(review)
            await client.post('/api/settings/directory',json={"directory":str(tmp_path / "out")})
            response = await client.post('/api/videos/bulk-confirm',json={"requestId":"bulk-test","selections":[
                {"id":review["id"],"version":0,"expectedRevision":0},
                {"id":row["id"],"version":0,"expectedRevision":1},
            ]})
            assert response.status_code == 202 and not any(o["ok"] for o in response.json()["outcomes"])
            assert not app.state.videos.store.get(review["id"])["versions"][0].get("downloadRequested")
            response = await client.post('/api/videos/bulk-remove',json={"requestId":"bulk-local","selections":[
                {"id":review["id"],"version":0,"expectedRevision":0},
                {"id":row["id"],"version":0,"expectedRevision":1}]})
            assert [o["ok"] for o in response.json()["outcomes"]] == [True,False]
            await wait_until(lambda: not app.state.videos.running)
            assert app.state.videos.store.get(review["id"])["deletedAt"]
            assert not app.state.videos.store.get(row["id"]).get("deletedAt")


def test_cross_disk_publication_copies_exclusively_and_verifies(tmp_path, monkeypatch):
    import errno
    import app.media as media
    source, destination = tmp_path / "complete.part", tmp_path / "published.mp4"
    source.write_bytes(b"verified-video")
    expected = media.file_digest(source)
    def different_device(*args):
        raise OSError(errno.EXDEV,"different devices")
    monkeypatch.setattr(media.sys,"platform","linux")
    monkeypatch.setattr(media.os,"link",different_device)
    media.publish_file(source,destination,expected)
    assert destination.read_bytes() == b"verified-video" and not source.exists()
    source.write_bytes(b"new-video")
    with pytest.raises(FileExistsError):
        media.publish_file(source,destination,media.file_digest(source))
    assert destination.read_bytes() == b"verified-video" and source.exists()


def test_local_to_external_copy_skips_post_copy_validation_and_removes_source(tmp_path, monkeypatch):
    import app.media as media
    source, destination = tmp_path / "local.part", tmp_path / "external" / "published.mp4"
    destination.parent.mkdir()
    source.write_bytes(b"video-data")
    monkeypatch.setattr(media, "file_digest", lambda *_: (_ for _ in ()).throw(AssertionError("unexpected hash")))
    monkeypatch.setattr(media.shutil, "disk_usage",
                        lambda *_: (_ for _ in ()).throw(AssertionError("unexpected storage preflight")))
    assert media.copy_to_destination(source, destination)
    assert destination.read_bytes() == b"video-data"
    assert not source.exists()


def test_recovery_cache_publication_trusts_verified_transfer_without_second_full_read(tmp_path, monkeypatch):
    import os
    import app.videos as videos

    root = tmp_path / "cache"
    root.mkdir()
    source = root / "complete.part"
    destination = tmp_path / "managed" / "published.mp4"
    destination.parent.mkdir()
    source.write_bytes(b"\x00\x00\x00\x18ftypmp42" + b"video-payload")
    real_digest = videos.file_digest
    digest_calls = []

    def count_digest(path):
        digest_calls.append(Path(path))
        return real_digest(path)

    def same_volume_move(src, dst, expected_digest):
        os.replace(src, dst)
        return expected_digest

    monkeypatch.setattr(videos, "file_digest", count_digest)
    monkeypatch.setattr(videos, "publish_file", same_volume_move)
    identity, digest = videos.inspect_recovery_cache(
        source, root, source.stat().st_size, source.stat().st_size)
    videos.publish_recovery_cache(source, destination, identity, digest)

    assert destination.read_bytes().startswith(b"\x00\x00\x00\x18ftypmp42")
    assert not source.exists()
    assert digest_calls == [source]


def test_local_to_external_copy_conflict_keeps_local_source(tmp_path):
    import app.media as media
    source, destination = tmp_path / "local.part", tmp_path / "published.mp4"
    source.write_bytes(b"new-video")
    destination.write_bytes(b"existing-video")
    with pytest.raises(FileExistsError):
        media.copy_to_destination(source, destination)
    assert source.read_bytes() == b"new-video"
    assert destination.read_bytes() == b"existing-video"
    assert not list(tmp_path.glob("*.uploading"))


def test_transfer_write_failure_keeps_source_and_records_stage(tmp_path, monkeypatch):
    import errno
    import app.media as media
    source, destination = tmp_path / "source.part", tmp_path / "video.mp4"
    source.write_bytes(b"complete-video")
    original_open = media.os.open
    def disk_full(path, flags, *args, **kwargs):
        if flags & media.os.O_CREAT:
            raise OSError(errno.ENOSPC, "not enough space")
        return original_open(path, flags, *args, **kwargs)
    monkeypatch.setattr(media.os, "open", disk_full)
    with pytest.raises(OSError) as raised:
        media.copy_to_destination(source, destination)
    assert raised.value.errno == errno.ENOSPC
    assert raised.value.transfer_stage == "temporary_create"
    assert source.read_bytes() == b"complete-video"
    assert not destination.exists()


@pytest.mark.asyncio
@pytest.mark.parametrize("limit", [5, 8])
async def test_external_transfers_respect_configured_limit(app_factory, tmp_path, monkeypatch, limit):
    manager = app_factory().state.videos
    if limit != 5:
        manager.update_download_limit({"maxConcurrentDownloads": limit, "expectedRevision": 0, "requestId": "transfer-limit"})
    release = asyncio.Event()
    workers = []
    active = 0
    peak = 0

    class TransferWorker:
        returncode = None
        first = True

        @property
        def stdout(self):
            return self

        async def readline(self):
            if self.first:
                self.first = False
                return b'{"stage":"copy"}\n'
            await release.wait()
            return b'{"event":"complete"}\n'

        async def wait(self):
            nonlocal active
            if self.returncode is None:
                self.returncode = 0
                active -= 1
            return self.returncode

        def kill(self):
            nonlocal active
            if self.returncode is None:
                self.returncode = -9
                active -= 1

    async def start_worker(*args, **kwargs):
        nonlocal active, peak
        worker = TransferWorker()
        workers.append(worker)
        active += 1
        peak = max(peak, active)
        return worker

    monkeypatch.setattr(asyncio, "create_subprocess_exec", start_worker)
    tasks = [asyncio.create_task(manager.copy_to_final(tmp_path / f"source-{i}.part", tmp_path / f"video-{i}.mp4"))
             for i in range(limit + 1)]
    try:
        await wait_until(lambda: len(workers) == limit)
        assert active == limit and len(workers) == limit
        assert all(not task.done() for task in tasks)
        release.set()
        await asyncio.wait_for(asyncio.gather(*tasks), timeout=3)
        assert len(workers) == limit + 1 and peak == limit and active == 0
    finally:
        release.set()
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


@pytest.mark.asyncio
async def test_transfer_timeout_stops_worker_and_preserves_local_file(app_factory, tmp_path, monkeypatch):
    import errno
    app = app_factory()
    manager = app.state.videos
    manager.transfer_timeout = .02
    source, destination = tmp_path / "source.part", tmp_path / "final.mp4"
    source.write_bytes(b"keep-this-video")
    class StalledWorker:
        returncode = None
        killed = False
        first = True
        @property
        def stdout(self): return self
        async def readline(self):
            if self.first:
                self.first = False
                return b'{"stage":"temporary_create"}\n'
            await asyncio.sleep(10)
        def kill(self): self.killed = True; self.returncode = -9
        async def wait(self): return self.returncode
    worker = StalledWorker()
    async def start_worker(*args, **kwargs): return worker
    monkeypatch.setattr(asyncio, "create_subprocess_exec", start_worker)
    with pytest.raises(OSError) as raised:
        await manager.copy_to_final(source, destination)
    assert raised.value.errno == errno.ETIMEDOUT
    assert raised.value.transfer_stage == "temporary_create"
    assert worker.killed
    assert source.read_bytes() == b"keep-this-video"
    assert not destination.exists()


@pytest.mark.asyncio
async def test_transfer_error_details_persist_without_changing_revision(app_factory, tmp_path, monkeypatch):
    import errno
    app = app_factory()
    manager = app.state.videos
    identity = "d" * 24
    part = manager.work / identity / "v2.part"
    part.parent.mkdir(parents=True)
    part.write_bytes(b"complete-video")
    row = {"id": identity, "jobId": "test", "revision": 7, "activeRevision": 2,
           "acceptedRevision": None, "requests": {}, "versions": [{"revision": 2, "state": "publishing",
           "downloadRequested": True, "outputDirectory": str(tmp_path),
           "publishing": {"path": str(tmp_path / "final.mp4"), "bytes": 14, "sha256": "known"}}]}
    manager.store.save(row)
    async def no_space(*args, **kwargs):
        error = OSError(errno.ENOSPC, "raw path must not be persisted")
        error.transfer_stage = "copy"
        raise error
    monkeypatch.setattr(manager, "copy_to_final", no_space)
    await manager.publish(identity, 2)
    saved = manager.store.get(identity)
    assert saved["revision"] == 7 and saved["activeRevision"] == 2
    version = saved["versions"][0]
    assert version["state"] == "publishing" and version["downloadRequested"]
    assert version["transferError"]["errno"] == errno.ENOSPC
    assert version["transferError"]["stage"] == "copy"
    assert "raw path" not in version["message"]
    assert part.exists()


@pytest.mark.asyncio
async def test_transfer_receipt_recovers_publication_without_recopy(app_factory, tmp_path, monkeypatch):
    from app.media import file_digest
    app = app_factory()
    manager = app.state.videos
    identity = "e" * 24
    part = manager.work / identity / "v0.part"
    part.parent.mkdir(parents=True)
    part.write_bytes(b"completed-video")
    destination = tmp_path / "published.mp4"
    destination.write_bytes(b"completed-video")
    row = {"id": identity, "jobId": "test", "revision": 0, "activeRevision": 0,
           "acceptedRevision": None, "requests": {}, "versions": [{"revision": 0, "state": "publishing",
           "downloadRequested": True, "outputDirectory": str(tmp_path),
           "publishing": {"path": str(destination), "bytes": destination.stat().st_size, "sha256": file_digest(destination)},
           "transferReceipt": {"path": str(destination), "publishedAt": "confirmed-before-restart"}}]}
    manager.store.save(row)
    async def must_not_recopy(*args, **kwargs): pytest.fail("acknowledged publication must not copy again")
    monkeypatch.setattr(manager, "copy_to_final", must_not_recopy)
    await manager.publish(identity, 0)
    saved = manager.store.get(identity)["versions"][0]
    assert saved["state"] == "saved" and not saved["downloadRequested"]
    assert destination.read_bytes() == b"completed-video" and not part.exists()


@pytest.mark.asyncio
@pytest.mark.parametrize('replaced', [False, True])
async def test_interrupted_fast_publication_checks_the_destination_content(app_factory, tmp_path, replaced):
    from app.media import checked_media_digest
    from tests.test_slicing_performance import close_app

    app = app_factory()
    manager = app.state.videos
    identity = 'a' * 24
    part = manager.work / identity / 'v0.part'
    part.parent.mkdir(parents=True)
    original = b'verified-video-data'
    part.write_bytes(original)
    digest, proof = checked_media_digest(part)
    destination = tmp_path / 'finished.mp4'
    part.rename(destination)  # The process exited after rename and before saving a receipt.
    if replaced:
        other = tmp_path / 'other.mp4'
        other.write_bytes(b'Z' * len(original))
        other.replace(destination)
    manager.store.save({'id': identity, 'jobId': 'test', 'revision': 0, 'activeRevision': 0,
        'acceptedRevision': None, 'requests': {}, 'versions': [{'revision': 0, 'state': 'publishing',
        'downloadRequested': True, 'outputDirectory': str(tmp_path), 'publishing': {
            'path': str(destination), 'bytes': len(original), 'sha256': digest, 'sourceVersion': proof}}]})
    try:
        await manager.publish(identity, 0)
        row = manager.store.get(identity)
        version = row['versions'][0]
        if replaced:
            assert version['state'] == 'failed' and version['errorCode'] == 'transfer_conflict'
            assert row['acceptedRevision'] is None and destination.read_bytes() == b'Z' * len(original)
        else:
            assert version['state'] == 'saved' and row['acceptedRevision'] == 0
            assert version['transferReceipt']['fileVersion'] == proof and destination.read_bytes() == original
    finally:
        await close_app(app)


def test_verified_same_volume_transfer_keeps_the_original_inode(tmp_path):
    from app.media import checked_media_digest, copy_to_destination, media_version

    source, destination = tmp_path / 'verified.part', tmp_path / 'final.mp4'
    source.write_bytes(b'verified-video-data')
    digest, proof = checked_media_digest(source)
    events = []
    copy_to_destination(source, destination, events.append, verified_source=proof, expected_digest=digest)
    assert not source.exists() and media_version(destination) == proof
    assert 'copy' not in [event['stage'] for event in events]
    assert events[-1]['fileVersion'] == proof


def test_transfer_resumes_owned_partial_without_hash_or_readback(tmp_path, monkeypatch):
    import app.media as media
    source, destination = tmp_path / "source.part", tmp_path / "final.mp4"
    partial = tmp_path / ".final.mp4.recorded.uploading"
    data = b"the-full-video-content"
    source.write_bytes(data)
    partial.write_bytes(data[:8])
    monkeypatch.setattr(media, "file_digest", lambda *_: pytest.fail("transfer must not hash the video"))
    monkeypatch.setattr(media.shutil, "disk_usage", lambda *_: pytest.fail("transfer must not probe the drive"))
    events = []
    media.copy_to_destination(source, destination, events.append, partial, keep_partial=True)
    assert next(e["bytes"] for e in events if e["stage"] == "copy") == 8
    assert destination.read_bytes() == data
    assert not source.exists() and not partial.exists()


def test_failed_flush_retains_complete_partial_for_retry(tmp_path, monkeypatch):
    import errno
    import app.media as media
    source, destination = tmp_path / "source.part", tmp_path / "final.mp4"
    partial = tmp_path / ".final.mp4.recorded.uploading"
    data = b"the-full-video-content"
    source.write_bytes(data)
    original_sync = media.os.fsync
    def failed_sync(*args): raise OSError(errno.EIO, "temporary sync failure")
    monkeypatch.setattr(media.os, "fsync", failed_sync)
    with pytest.raises(OSError) as raised:
        media.copy_to_destination(source, destination, partial_path=partial, keep_partial=True)
    assert raised.value.transfer_stage == "flush"
    assert source.read_bytes() == data and partial.read_bytes() == data
    assert not destination.exists()
    monkeypatch.setattr(media.os, "fsync", original_sync)
    events = []
    media.copy_to_destination(source, destination, events.append, partial, keep_partial=True)
    assert next(e["bytes"] for e in events if e["stage"] == "copy") == len(data)
    assert destination.read_bytes() == data and not source.exists()


def test_exfat_transfer_publishes_without_recopy_or_validation(tmp_path, monkeypatch):
    import errno
    import app.media as media
    source, destination = tmp_path / "source.part", tmp_path / "final.mp4"
    source.write_bytes(b"completed-video")
    def unsupported(*args): raise OSError(errno.ENOTSUP, "exclusive rename unsupported")
    monkeypatch.setattr(media.sys, "platform", "linux")
    monkeypatch.setattr(media.os, "link", unsupported)
    monkeypatch.setattr(media, "file_digest", lambda *_: pytest.fail("must not hash transfer"))
    monkeypatch.setattr(media.shutil, "disk_usage", lambda *_: pytest.fail("must not probe volume"))
    events = []
    media.copy_to_destination(source, destination, events.append)
    assert destination.read_bytes() == b"completed-video" and not source.exists()
    assert next(e["reservation"] for e in events if e.get("reservation"))["token"]
    assert not list(tmp_path.glob("*.uploading"))


def test_exfat_reservation_never_replaces_an_existing_video(tmp_path, monkeypatch):
    import errno
    import app.media as media
    source, destination = tmp_path / "source.part", tmp_path / "final.mp4"
    source.write_bytes(b"new-video")
    destination.write_bytes(b"unrelated-video")
    def unsupported(*args): raise OSError(errno.ENOTSUP, "unsupported")
    monkeypatch.setattr(media.sys, "platform", "linux")
    monkeypatch.setattr(media.os, "link", unsupported)
    with pytest.raises(FileExistsError):
        media.copy_to_destination(source, destination)
    assert source.read_bytes() == b"new-video"
    assert destination.read_bytes() == b"unrelated-video"


def test_exfat_publish_retry_reuses_owned_reservation_and_full_partial(tmp_path, monkeypatch):
    import errno
    import app.media as media
    source, destination = tmp_path / "source.part", tmp_path / "final.mp4"
    partial = tmp_path / ".final.mp4.recorded.uploading"
    data = b"completed-video"
    source.write_bytes(data)
    partial.write_bytes(data)
    def unsupported(*args): raise OSError(errno.ENOTSUP, "unsupported")
    def interrupted(*args): raise OSError(errno.EIO, "publish interrupted")
    monkeypatch.setattr(media.sys, "platform", "linux")
    monkeypatch.setattr(media.os, "link", unsupported)
    original_replace = media.os.replace
    monkeypatch.setattr(media.os, "replace", interrupted)
    events = []
    with pytest.raises(OSError):
        media.copy_to_destination(source, destination, events.append, partial, keep_partial=True)
    reservation = next(e["reservation"] for e in events if e.get("reservation"))
    assert media.transfer_reservation_identity(destination, reservation)
    assert source.read_bytes() == data and partial.read_bytes() == data
    monkeypatch.setattr(media.os, "replace", original_replace)
    resumed = []
    media.copy_to_destination(source, destination, resumed.append, partial, True, reservation=reservation)
    assert next(e["bytes"] for e in resumed if e["stage"] == "copy") == len(data)
    assert destination.read_bytes() == data and not source.exists() and not partial.exists()


def test_exfat_reservation_rejects_other_operations_and_replaced_files(tmp_path):
    import app.media as media
    destination = tmp_path / "final.mp4"
    reservation = {"token": "a" * 32}
    destination.write_bytes(b"simple-video-transfer-reservation:" + b"b" * 32)
    with pytest.raises(FileExistsError):
        media.transfer_reservation_identity(destination, reservation)
    destination.write_bytes(b"another-video")
    with pytest.raises(FileExistsError):
        media.transfer_reservation_identity(destination, reservation)


@pytest.mark.asyncio
async def test_live_prior_worker_blocks_second_writer_without_losing_source(app_factory, tmp_path, monkeypatch):
    manager = app_factory().state.videos
    identity = "f" * 24
    part = manager.work / identity / "v0.part"
    part.parent.mkdir(parents=True)
    part.write_bytes(b"keep-this-video")
    row = {"id": identity, "jobId": "test", "revision": 0, "activeRevision": 0,
           "acceptedRevision": None, "requests": {}, "versions": [{"revision": 0, "state": "publishing",
           "downloadRequested": True, "outputDirectory": str(tmp_path), "transferWorkerPid": 123,
           "publishing": {"path": str(tmp_path / "final.mp4"), "bytes": 15, "sha256": "known"}}]}
    manager.store.save(row)
    async def prior_alive(*args): return True
    async def must_not_copy(*args, **kwargs): pytest.fail("must wait for the original writer")
    monkeypatch.setattr(manager, "transfer_worker_running", prior_alive)
    monkeypatch.setattr(manager, "copy_to_final", must_not_copy)
    await manager.publish(identity, 0)
    assert part.read_bytes() == b"keep-this-video"
    assert manager.store.get(identity)["versions"][0]["transferWorkerPid"] == 123


@pytest.mark.asyncio
async def test_flush_has_longer_wait_than_copy_progress(app_factory, tmp_path, monkeypatch):
    manager = app_factory().state.videos
    manager.transfer_timeout = .01
    manager.transfer_flush_timeout = .1
    class FlushingWorker:
        returncode = None
        first = True
        @property
        def stdout(self): return self
        async def readline(self):
            if self.first:
                self.first = False
                return b'{"stage":"flush"}\n'
            await asyncio.sleep(.04)
            return b'{"event":"complete"}\n'
        async def wait(self): self.returncode = 0; return 0
        def kill(self): pytest.fail("a valid slow flush must not be killed")
    async def start_worker(*args, **kwargs): return FlushingWorker()
    monkeypatch.setattr(asyncio, "create_subprocess_exec", start_worker)
    await manager.copy_to_final(tmp_path / "source.part", tmp_path / "final.mp4")


@pytest.mark.asyncio
async def test_publication_retry_reuses_complete_local_part(app_factory, platform, tmp_path):
    from app.media import file_digest
    app = app_factory()
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1") as client:
            row = await new_video(client, app)
            directory = tmp_path / "final";directory.mkdir()
            await client.post('/api/settings/directory',json={"directory":str(directory)})
            part = app.state.videos.work / row["id"] / "v0.part";part.parent.mkdir();part.write_bytes(platform.media)
            version = row["versions"][0]
            version.update(state="failed",downloadRequested=True,outputDirectory=str(directory),
                publishing={"path":str(directory / "finished.mp4"),"sha256":file_digest(part),"bytes":len(platform.media)})
            app.state.videos.store.save(row)
            app.state.videos.ffprobe = lambda path: asyncio.sleep(0,result=60.0)
            response = await client.post(f'/api/videos/{row["id"]}/retry-download',json={})
            assert response.status_code == 202
            await wait_until(lambda: not app.state.videos.running)
            assert app.state.videos.store.get(row["id"])["versions"][0]["state"] == "saved"
            assert (directory / "finished.mp4").read_bytes() == platform.media
            assert platform.download_calls == 0 and not part.exists()


@pytest.mark.asyncio
async def test_remove_does_not_follow_cache_symlinks(app_factory, tmp_path):
    app = app_factory()
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1") as client:
            row = await new_video(client, app)
            foreign = tmp_path / 'foreign'
            foreign.mkdir()
            (foreign / 'v0.part').write_bytes(b'keep-this-file')
            (app.state.videos.work / row["id"]).symlink_to(foreign, target_is_directory=True)
            refused = await client.post(f'/api/videos/{row["id"]}/remove-uncorrected', json={
                "expectedRevision": 0, "requestId": "do-not-follow-symlink"})
            assert refused.status_code == 400
            assert (foreign / 'v0.part').read_bytes() == b'keep-this-file'
            assert not app.state.videos.store.get(row["id"]).get("deletedAt")
