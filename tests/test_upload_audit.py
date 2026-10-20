import asyncio
import json
import time
from datetime import datetime, timezone
from pathlib import Path

import httpx
import pytest

from app.main import create_app
from app.remote import RemoteClient
from app.store import JobStore
from app.upload_audit import (AthenaClient, AuditError, UploadAuditManager, compare_files,
                              log_references, name_key, scan_directory)


def local(name, path=None, size=10):
    return {"name": name, "relativePath": path or name, "size": size, "issue": ""}


def target(evidence=None, **values):
    return {"id": "27912", "complete": True, "runningJobs": 0, "evidence": evidence or {}, **values}


def proof(status="uploaded", job="100"):
    return {"jobId": job, "at": "2026-10-15T08:00:00+08:00", "status": status}


def test_matching_keeps_time_and_unique_suffix_and_matches_extensionless_logs():
    files = [local("鲁A12345_20261001080000_a1.MP4"), local("鲁A12345_20261001080000_a2.mp4")]
    refs = log_references("[INFO] 视频无后缀名称:鲁A12345_20261001080000_a1")
    rows = compare_files(files, [target({key: [proof()] for key in refs})])
    assert [r["targets"]["27912"]["status"] for r in rows] == ["uploaded", "missing"]


def test_exact_extensions_and_ambiguous_stems_do_not_approve_another_format():
    files = [local("车辆_a1.mp4"), local("车辆_a1.mov")]
    refs = log_references("/tmp/datasets/dataset_jobs/100/data/车辆_a1.mp4")
    rows = compare_files(files, [target({key: [proof()] for key in refs})])
    assert [r["targets"]["27912"]["status"] for r in rows] == ["uploaded", "missing"]
    rows = compare_files(files, [target({("车辆_a1", "stem"): [proof()]})])
    assert all(r["targets"]["27912"]["status"] == "review" for r in rows)


def test_incomplete_failed_and_running_logs_are_not_missing_or_success():
    files = [local("a.mp4"), local("b.mp4")]
    evidence = {("a", "stem"): [proof("in_progress")], ("b", "stem"): [proof("review")]}
    rows = compare_files(files, [target(evidence)])
    assert [r["targets"]["27912"]["status"] for r in rows] == ["in_progress", "review"]
    for options in ({"complete": False}, {"runningJobs": 1}):
        assert compare_files([local("missing.mp4")], [target(**options)])[0]["targets"]["27912"]["status"] == "review"
    evidence[("b.mp4", "full")] = [proof()]
    assert compare_files(files, [target(evidence)])[1]["targets"]["27912"]["status"] == "uploaded"


def test_scan_is_read_only_and_excludes_caches_symlinks_and_partial_files(tmp_path):
    (tmp_path / "one.mp4").write_bytes(b"video")
    (tmp_path / "two.part").write_bytes(b"partial")
    (tmp_path / "._resource.mp4").write_bytes(b"resource")
    (tmp_path / "empty.mov").touch()
    (tmp_path / ".download-cache").mkdir()
    (tmp_path / ".download-cache" / "cached.mp4").write_bytes(b"cache")
    (tmp_path / "sub").mkdir()
    (tmp_path / "sub" / "inner.mp4").write_bytes(b"inner")
    (tmp_path / "link.mp4").symlink_to(tmp_path / "one.mp4")
    before = sorted(str(p.relative_to(tmp_path)) for p in tmp_path.rglob("*"))
    files, _ = scan_directory(str(tmp_path))
    assert [f["name"] for f in files] == ["empty.mov", "one.mp4"]
    assert files[0]["issue"]
    files, _ = scan_directory(str(tmp_path), True)
    assert {f["relativePath"] for f in files} == {"one.mp4", "empty.mov", "sub/inner.mp4"}
    assert sorted(str(p.relative_to(tmp_path)) for p in tmp_path.rglob("*")) == before
    with pytest.raises(ValueError, match="不存在"):
        scan_directory(str(tmp_path / "absent"))
    assert not (tmp_path / "absent").exists()


def test_duplicate_basenames_need_review_and_unicode_names_are_normalized():
    files = [local("same.mp4", "a/same.mp4"), local("same.mp4", "b/same.mp4")]
    result = compare_files(files, [target({("same", "stem"): [proof()]})])
    assert all(r["targets"]["27912"]["status"] == "review" for r in result)
    assert name_key("e\u0301.mp4") == name_key("é.MP4")


class Platform:
    def __init__(self):
        self.requests = []
        self.bad_log = False
        self.stalled_cursor = False

    @staticmethod
    def answer(data, headers=None):
        return httpx.Response(200, json={"code": "0", "data": data}, headers=headers)

    def handle(self, request):
        self.requests.append(request)
        path, params = request.url.path, request.url.params
        if path == "/api/auth/login/":
            assert request.method == "POST"
            assert json.loads(request.content) == {"username": "tester", "password": "test-only-password"}
            return self.answer({"access": "test-access", "refresh": "test-refresh"})
        assert request.method == "GET", "Audit must never mutate platform datasets"
        assert request.headers["authorization"] == "Bearer test-access"
        if path.startswith("/api/devel/datasets/"):
            identity = path.split("/")[-2]
            return self.answer({"id": int(identity), "name": "车斗" if identity == "27912" else "磁盘吊"})
        if path == "/api/devel/dataset-jobs/":
            assert params["type"] == "1", "Only dataset-import tasks may be queried"
            identity, page = params["dataset"], int(params["page"])
            jobs = ([{"id": 101, "type": 1, "status": 3, "dataset": 27912},
                     {"id": 102, "type": 21, "status": 3, "dataset": 27912}] if page == 1 else
                    [{"id": 103, "type": 1, "status": 4, "dataset": 27912}]) if identity == "27912" else [
                    {"id": 201, "type": 1, "status": 3, "dataset": 27944}]
            for job in jobs:
                job['utc_create'] = datetime.now(timezone.utc).isoformat()
            return self.answer(jobs, {"x-total": "3" if identity == "27912" else "1",
                "x-next-page": "2" if identity == "27912" and page == 1 else ""})
        if path.endswith("/logs/"):
            job = path.split("/")[-3]
            assert job != "102", "Video slicing logs are not import evidence"
            if self.bad_log and job == "201":
                return self.answer({"result": None})
            if job == "101":
                if params["index"] == "0":
                    return self.answer({"result": [{"values": [[str(time.time_ns()), "视频无后缀名称:one"]]}],
                        "index": 0 if self.stalled_cursor else 99, "has_more": True})
                return self.answer({"result": [{"values": [[str(time.time_ns()), "/tmp/data/two.mp4"]]}], "index": 100, "has_more": False})
            line = "视频无后缀名称:two" if job == "201" else "[ERROR] /tmp/data/failed.mp4"
            return self.answer({"result": [{"values": [[str(time.time_ns()), line]]}], "index": 100, "has_more": False})
        raise AssertionError(path)


@pytest.mark.asyncio
async def test_backend_reads_all_task_and_log_pages_and_reports_separately(tmp_path):
    platform = Platform()
    client = AthenaClient(httpx.MockTransport(platform.handle))
    source = RemoteClient("source", httpx.MockTransport(lambda request: httpx.Response(500)))
    capture = RemoteClient("target", httpx.MockTransport(lambda request: httpx.Response(500)))
    app = create_app(source=source, target=capture, data_dir=tmp_path / "data", audit_client=client)
    folder = tmp_path / "videos"; folder.mkdir()
    for name in ("one.mp4", "two.mp4", "missing.mp4", "failed.mp4"):
        (folder / name).write_bytes(b"video")
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://localhost") as http:
        denied = await http.post("/api/upload-audit/run", json={"directory": str(folder)})
        assert denied.status_code == 401
        login = await http.post("/api/upload-audit/login", json={"account": "tester", "password": "test-only-password"})
        assert login.status_code == 200
        assert "test-access" not in login.text and "test-only-password" not in login.text
        started = await http.post("/api/upload-audit/run", json={"directory": str(folder)})
        assert started.status_code == 202
        await app.state.upload_audit.task
        data = (await http.get("/api/upload-audit")).json()
        assert data["run"]["status"] == "complete"
        assert data["run"]["totalLogs"] == 3
        assert [dataset["importCount"] for dataset in data["report"]["datasets"]] == [2, 1]
        assert {request.url.path.split('/')[-3] for request in platform.requests
                if request.url.path.endswith('/logs/')} == {'101', '103', '201'}
        rows = {r["name"]: r for r in data["report"]["files"]}
        assert rows["one.mp4"]["targets"]["27912"]["status"] == "uploaded"
        assert rows["one.mp4"]["targets"]["27944"]["status"] == "missing"
        assert rows["two.mp4"]["targets"]["27912"]["status"] == "uploaded"
        assert rows["two.mp4"]["targets"]["27944"]["status"] == "uploaded"
        assert rows["failed.mp4"]["targets"]["27912"]["status"] == "review"
        assert all(v["status"] == "missing" for v in rows["missing.mp4"]["targets"].values())
        platform.bad_log = True
        await http.post("/api/upload-audit/run", json={"directory": str(folder)})
        await app.state.upload_audit.task
        partial = (await http.get("/api/upload-audit")).json()["report"]
        assert not partial["datasets"][1]["complete"]
        assert partial["files"][0]["targets"]["27944"]["status"] == "review"
        previous_id = partial["id"]
        await http.post("/api/upload-audit/run", json={"directory": str(folder / "absent")})
        await app.state.upload_audit.task
        failure = (await http.get("/api/upload-audit")).json()
        assert failure["run"]["status"] == "failed" and failure["report"]["id"] == previous_id
    await app.state.upload_audit.shutdown()
    await source.close(); await capture.close()
    app.state.store.close()


@pytest.mark.asyncio
async def test_stalled_log_cursor_is_incomplete_not_missing(tmp_path):
    platform = Platform(); platform.stalled_cursor = True
    client = AthenaClient(httpx.MockTransport(platform.handle))
    await client.login("tester", "test-only-password")
    result = await client.job_evidence("27912", {"id": 101, "type": 1, "status": 3})
    assert result["reason"]
    assert result["evidence"][("one", "stem")][0]["status"] == "review"
    await client.close()


@pytest.mark.asyncio
async def test_saved_platform_session_uses_refresh_token_only_and_restores(monkeypatch):
    from app import secure_store

    saved = {}
    monkeypatch.setattr(secure_store, "save_connection",
                        lambda refresh, user: saved.update(refresh=refresh, user=user))
    monkeypatch.setattr(secure_store, "load_connection", lambda: dict(saved) if saved else None)
    monkeypatch.setattr(secure_store, "delete_connection", lambda: saved.clear())
    requests = []

    def handle(request):
        requests.append(request)
        if request.url.path == "/api/auth/login/":
            assert json.loads(request.content) == {"username": "tester", "password": "test-only-password"}
            return httpx.Response(200, json={"code": "0", "data": {"access": "access-one", "refresh": "refresh-one"}})
        if request.url.path == "/api/auth/token/refresh/":
            assert json.loads(request.content) == {"refresh": "refresh-one"}
            return httpx.Response(200, json={"code": "0", "data": {"access": "access-two", "refresh": "refresh-two"}})
        raise AssertionError(request.url.path)

    client = AthenaClient(httpx.MockTransport(handle), persist_connection=True)
    await client.login("tester", "test-only-password")
    assert client.saved_connection
    assert saved == {"refresh": "refresh-one", "user": "tester"}
    state = UploadAuditManager.__new__(UploadAuditManager)
    state.client, state.saved, state.run = client, {}, None
    response_state = UploadAuditManager.state(state)
    assert response_state["savedConnection"] is True
    assert "refresh-one" not in json.dumps(response_state)
    assert "access-one" not in json.dumps(response_state)
    await client.close()

    restored = AthenaClient(httpx.MockTransport(handle), persist_connection=True)
    assert await restored.restore_saved_connection()
    assert restored.connected and restored.user == "tester" and restored.saved_connection
    assert saved == {"refresh": "refresh-two", "user": "tester"}
    assert [request.url.path for request in requests] == [
        "/api/auth/login/", "/api/auth/token/refresh/",
    ]
    await restored.forget_saved_connection()
    assert saved == {}
    await restored.close()


@pytest.mark.asyncio
async def test_missing_pagination_headers_does_not_claim_complete_history():
    def handle(request):
        return Platform.answer([{"id": 1, "type": 1, "status": 3}])
    client = AthenaClient(httpx.MockTransport(handle)); client.access = "test-access"
    with pytest.raises(AuditError, match="分页信息"):
        await client.jobs("27912")
    await client.close()


@pytest.mark.asyncio
@pytest.mark.parametrize('task_type', [21, '21', 22, 2, 0, None, True])
async def test_non_import_log_calls_are_rejected_before_any_request(task_type):
    requests = []
    def handle(request):
        requests.append(request)
        return Platform.answer({'result': [], 'has_more': False})
    client = AthenaClient(httpx.MockTransport(handle)); client.access = 'test-access'
    try:
        with pytest.raises(AuditError, match='仅允许读取数据集导入'):
            await client.job_evidence('27912', {'id': 901, 'type': task_type, 'status': 3})
        assert not requests
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_import_only_query_handles_server_ignoring_filter_without_breaking_pagination():
    pages = []
    def handle(request):
        page = int(request.url.params['page']); pages.append(page)
        assert request.url.params['type'] == '1'
        types = [1, 21, 22] if page == 1 else [2, '1']
        return Platform.answer([{'id': page * 100 + i, 'type': value, 'status': 3} for i, value in enumerate(types)],
                               {'x-total': '5', 'x-next-page': '2' if page == 1 else ''})
    client = AthenaClient(httpx.MockTransport(handle)); client.access = 'test-access'
    try:
        assert [row['id'] for row in await client.jobs('27912')] == [100, 201]
        assert pages == [1, 2]
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_slice_names_and_running_slice_do_not_change_upload_status(tmp_path):
    requests = []
    def handle(request):
        requests.append(request)
        if '/datasets/' in request.url.path:
            return Platform.answer({'id': int(request.url.path.split('/')[-2])})
        dataset = int(request.url.params['dataset'])
        if request.url.path.endswith('/dataset-jobs/'):
            return Platform.answer([
                {'id': dataset * 10 + 1, 'dataset': dataset, 'type': 1, 'status': 3},
                {'id': dataset * 10 + 2, 'dataset': dataset, 'type': 21, 'status': 3},
                {'id': dataset * 10 + 3, 'dataset': dataset, 'type': 22, 'status': 2},
            ], {'x-total': '3', 'x-next-page': ''})
        job_id = int(request.url.path.split('/')[-3])
        # A slicing log could mention the exact local filename. It must never
        # be fetched or serve as evidence; a running slice must not mark it pending.
        line = '视频无后缀名称:imported' if job_id % 10 == 1 else '视频无后缀名称:slice-only'
        return Platform.answer({'result': [{'values': [[str(time.time_ns()), line]]}], 'has_more': False, 'index': 0})
    store = JobStore(tmp_path / 'audit.sqlite3')
    client = AthenaClient(httpx.MockTransport(handle)); client.access = 'test-access'
    manager = UploadAuditManager(store, client)
    folder = tmp_path / 'videos'; folder.mkdir()
    for name in ('imported.mp4', 'slice-only.mp4'):
        (folder / name).write_bytes(b'video')
    try:
        manager.start(str(folder)); await manager.task
        state = manager.state()
        assert state['run']['status'] == 'complete' and state['run']['totalLogs'] == 2
        report = state['report']
        assert all(d['importCount'] == 1 and d['runningJobs'] == 0 for d in report['datasets'])
        files = {row['name']: row for row in report['files']}
        assert all(value['status'] == 'uploaded' for value in files['imported.mp4']['targets'].values())
        assert all(value['status'] == 'missing' for value in files['slice-only.mp4']['targets'].values())
        log_ids = {int(r.url.path.split('/')[-3]) for r in requests if r.url.path.endswith('/logs/')}
        assert log_ids == {279121, 279441}
    finally:
        await manager.shutdown(); store.close()


@pytest.mark.asyncio
@pytest.mark.parametrize('terminal', ['', '  ', '0', '-1'])
@pytest.mark.parametrize('total', [None, '1'])
async def test_terminal_page_headers_match_athena_client(terminal, total):
    calls = []
    def handle(request):
        calls.append(request)
        headers = {'X-Next-Page': terminal}
        if total is not None:
            headers['X-Total'] = total
        return Platform.answer([{'id': 1, 'type': 1, 'status': 3, 'dataset': 27912}], headers)
    client = AthenaClient(httpx.MockTransport(handle)); client.access = 'test-access'
    try:
        assert [row['id'] for row in await client.jobs('27912')] == [1]
        assert len(calls) == 1
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_multiple_task_pages_end_with_empty_next_page_and_keep_all_identities():
    pages = []
    def handle(request):
        page = int(request.url.params['page']); pages.append(page)
        assert request.url.params['dataset'] == '27912'
        assert request.url.params['page_size'] == '100'
        ids = range(1, 101) if page == 1 else [101]
        return Platform.answer([{'id': i, 'type': 1, 'status': 3, 'dataset': 27912} for i in ids],
                               {'x-total': '101', 'x-next-page': '2' if page == 1 else ''})
    client = AthenaClient(httpx.MockTransport(handle)); client.access = 'test-access'
    try:
        assert [row['id'] for row in await client.jobs('27912')] == list(range(1, 102))
        assert pages == [1, 2]
    finally:
        await client.close()


@pytest.mark.asyncio
@pytest.mark.parametrize('headers,data,reason', [
    ({'x-total': '2', 'x-next-page': ''}, [{'id': 1, 'type': 1, 'status': 3}], '总数不符'),
    ({'x-total': '0', 'x-next-page': ''}, [{'id': 1, 'type': 1, 'status': 3}], '总数不符'),
    ({'x-total': '-1', 'x-next-page': ''}, [], '总数无效'),
    ({'x-total': '0', 'x-next-page': '3'}, [], '不连续'),
    ({'x-total': '0', 'x-next-page': 'invalid'}, [], '分页信息无效'),
])
async def test_pagination_fix_still_rejects_unproven_history(headers, data, reason):
    client = AthenaClient(httpx.MockTransport(lambda request: Platform.answer(data, headers)))
    client.access = 'test-access'
    try:
        with pytest.raises(AuditError, match=reason):
            await client.jobs('27912')
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_empty_dataset_is_complete_with_explicit_terminal_header():
    client = AthenaClient(httpx.MockTransport(lambda request: Platform.answer([], {'x-total': '0', 'x-next-page': ''})))
    client.access = 'test-access'
    try:
        assert await client.jobs('27912') == []
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_existing_53_video_payloads_and_progress_survive_audit_restart(tmp_path):
    store = JobStore(tmp_path / "database.sqlite3")
    store.db.execute("CREATE TABLE simple_videos (id TEXT PRIMARY KEY, payload TEXT NOT NULL)")
    payloads = {str(i): json.dumps({"id": str(i), "offset": i * 1024, "corrections": [1, 2]}) for i in range(53)}
    store.db.executemany("INSERT INTO simple_videos VALUES (?, ?)", payloads.items()); store.db.commit()
    platform = Platform(); client = AthenaClient(httpx.MockTransport(platform.handle))
    manager = UploadAuditManager(store, client)
    folder = tmp_path / "videos"; folder.mkdir(); (folder / "one.mp4").write_bytes(b"video")
    await client.login("tester", "test-only-password")
    manager.start(str(folder)); await manager.task
    report_id = manager.state()["report"]["id"]
    assert dict(store.db.execute("SELECT id,payload FROM simple_videos")) == payloads
    await manager.shutdown()
    restarted = UploadAuditManager(store, AthenaClient(httpx.MockTransport(platform.handle)))
    assert restarted.state()["report"]["id"] == report_id
    assert not restarted.client.connected
    assert dict(store.db.execute("SELECT id,payload FROM simple_videos")) == payloads
    await restarted.shutdown();store.close()


@pytest.mark.asyncio
async def test_refreshes_expired_session_without_storing_secrets_in_results():
    calls = []
    def handle(request):
        calls.append(request)
        if request.method == "POST":
            assert request.url.path == "/api/auth/token/refresh/"
            assert json.loads(request.content) == {"refresh": "test-refresh"}
            return Platform.answer({"access": "new-test-access"})
        if request.headers["authorization"] == "Bearer old-test-access":
            return httpx.Response(401)
        return Platform.answer({"id": 27912, "name": "test"})
    client = AthenaClient(httpx.MockTransport(handle))
    client.access, client.refresh = "old-test-access", "test-refresh"
    data, _ = await client.get("devel/datasets/27912/")
    assert data == {"id": 27912, "name": "test"}
    assert len(calls) == 3 and client.access == "new-test-access"
    await client.close()


@pytest.mark.asyncio
async def test_successful_task_with_errors_or_expired_logs_needs_review():
    def handle(request):
        if request.url.params["dataset"] == "27912":
            values = [["1", "视频无后缀名称:one"], ["2", "[ERROR] /tmp/data/one.mp4 解码失败"]]
        else:
            values = []
        return Platform.answer({"result": [{"values": values}], "has_more": False, "index": 0})
    client = AthenaClient(httpx.MockTransport(handle));client.access = "test-access"
    found = await client.job_evidence("27912", {"id": 1, "type": 1, "status": 3, "utc_create": "2026-10-14T23:59:59Z"})
    assert found["evidence"][("one", "stem")][0]["problemNs"]
    assert found["evidence"][("one.mp4", "full")][0]["status"] == "review"
    assert found["evidence"][("one", "stem")][0]["at"] == "2026-10-14T23:59:59Z"
    assert compare_files([local('one.mp4')], [target(found['evidence'])])[0]['targets']['27912']['status']=='review'
    missing = await client.job_evidence("27944", {"id": 2, "type": 1, "status": 3})
    assert missing["reason"] and not missing["evidence"]
    await client.close()
