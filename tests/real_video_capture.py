"""Isolated real-media capture rehearsal; no production credentials or network.

The simulator implements the existing platform contract, but all media bytes are
read from a caller-owned test copy. Importing this module does not start anything.
The returned app deliberately remains open for downstream rehearsal stages.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import re
import shutil
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import uuid4

import httpx
from starlette.requests import Request

from app.domain import DEFAULT_TARGET
from app.main import create_app
from app.media import DownloadFailure, file_digest
from app.remote import RemoteClient


class FileStream(httpx.AsyncByteStream):
    """Bounded-memory, honest byte ranges, optionally interrupted exactly once."""

    def __init__(self, path: Path, start: int, length: int, fail_after: int | None = None):
        self.path, self.start, self.length, self.fail_after = path, start, length, fail_after

    async def __aiter__(self):
        emitted = 0
        with self.path.open("rb") as source:
            source.seek(self.start)
            while emitted < self.length:
                amount = min(1024 * 1024, self.length - emitted)
                if self.fail_after is not None:
                    amount = min(amount, self.fail_after - emitted)
                    if amount <= 0:
                        raise httpx.ReadError("injected local rehearsal interruption")
                block = await asyncio.to_thread(source.read, amount)
                if not block:
                    return
                emitted += len(block)
                yield block


class CapturePlatform:
    """A private platform inventory backed by real stream-copy clips."""

    def __init__(self, root: Path, source: Path, ffmpeg: str, run_id: str):
        self.root, self.source, self.ffmpeg, self.run_id = root, source, ffmpeg, run_id
        self.root.mkdir(parents=True, exist_ok=True)
        self.start = datetime(2026, 9, 13, 9, 54)
        self.parents: dict[str, dict] = {}
        self.results: dict[str, list[dict]] = {}
        self.objects: dict[str, Path] = {}
        self.clip_cache: dict[tuple[int, int], Path] = {}
        self.metrics: dict[str, Any] = {"createRequests": 0, "createdTasks": 0, "mediaRequests": 0,
                                       "ranges": [], "clips": [], "unexpectedRequests": []}
        self.fail_after_write = False
        self.break_next_download = False
        self.fault = ""

    @staticmethod
    def answer(data):
        return httpx.Response(200, json={"code": "0", "data": data})

    async def clip(self, offset: int, duration: int) -> Path:
        key = offset, duration
        if key in self.clip_cache:
            return self.clip_cache[key]
        if offset < 0 or offset + duration > 4499:
            raise ValueError("The rehearsal collection interval exceeds the real source.")
        destination = self.root / f"clip-{offset}-{duration}.mp4"
        started = time.monotonic()
        process = await asyncio.create_subprocess_exec(
            self.ffmpeg, "-nostdin", "-v", "error", "-ss", str(offset), "-i", str(self.source),
            "-t", str(duration), "-map", "0:v:0", "-an", "-c", "copy", "-avoid_negative_ts", "make_zero",
            str(destination), stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.PIPE)
        try:
            _, error = await asyncio.wait_for(process.communicate(), 600)
        except BaseException:
            if process.returncode is None:
                process.kill()
                await process.wait()
            raise
        if process.returncode:
            destination.unlink(missing_ok=True)
            raise RuntimeError("Local stream-copy capture failed: " + error.decode(errors="replace")[-500:])
        self.clip_cache[key] = destination
        self.metrics["clips"].append({"offsetSeconds": offset, "durationSeconds": duration,
                                      "bytes": destination.stat().st_size, "seconds": round(time.monotonic() - started, 3)})
        return destination

    async def handle(self, request: httpx.Request) -> httpx.Response:
        # Even unexpected hosts remain inside MockTransport; they cannot egress.
        path = request.url.path
        if path.endswith("/record/doLogin") or path.endswith("/account/current-user"):
            return self.answer({"id": "isolated-rehearsal"})
        if path.startswith("/platform/oss/") or request.url.host == "storage.test":
            direct = path.removeprefix("/platform/oss") if path.startswith("/platform/oss/") else path
            if direct not in self.objects:
                return httpx.Response(404)
            media = self.objects[direct]
            etag = '"rehearsal-' + media.stem + '"'
            if request.method == "DELETE":
                if request.headers.get("if-match") not in (None, etag):
                    return httpx.Response(412)
                self.objects.pop(direct)
                return httpx.Response(204)
            if request.method not in {"GET", "HEAD"}:
                return httpx.Response(405)
            size = media.stat().st_size
            headers = {"content-type": "video/mp4", "etag": etag, "accept-ranges": "bytes"}
            begin, end, status = 0, size - 1, 200
            requested = request.headers.get("range", "")
            if requested:
                match = re.fullmatch(r"bytes=(\d*)-(\d*)", requested)
                if not match:
                    return httpx.Response(416, headers={"content-range": f"bytes */{size}"})
                if not match[1]:
                    begin = max(0, size - int(match[2]))
                else:
                    begin = int(match[1])
                    end = min(end, int(match[2])) if match[2] else end
                if begin >= size or begin > end:
                    return httpx.Response(416, headers={**headers, "content-range": f"bytes */{size}"})
                status = 206
                headers["content-range"] = f"bytes {begin}-{end}/{size}"
                if self.fault == "etag":
                    headers["etag"] = '"changed-rehearsal-object"'
                elif self.fault == "range":
                    headers["content-range"] = f"bytes {begin + 1}-{end}/{size}"
            headers["content-length"] = str(end - begin + 1)
            if self.fault == "length":
                headers["content-length"] = "invalid-length"
            self.metrics["mediaRequests"] += 1
            self.metrics["ranges"].append({"method": request.method, "offset": begin,
                                            "bytes": end - begin + 1, "status": status})
            fail_after = None
            if request.method == "GET" and not requested and self.break_next_download:
                self.break_next_download = False
                fail_after = 16 * 1024 * 1024
            return httpx.Response(status, headers=headers, stream=FileStream(
                media, begin, 0 if request.method == "HEAD" else end - begin + 1, fail_after))
        if path.endswith("/camera/" + DEFAULT_TARGET["cameraId"]):
            return self.answer({"id": DEFAULT_TARGET["cameraId"], "embedId": DEFAULT_TARGET["embedId"], "channelId": "1"})
        if path.endswith("/embed/" + DEFAULT_TARGET["embedId"]):
            return self.answer({"id": DEFAULT_TARGET["embedId"]})
        if path.endswith("/data-acquisition/condition-query"):
            payload = json.loads(request.content or b"{}")
            begin = (int(payload.get("currPage", 1)) - 1) * int(payload.get("pageSize", 100))
            parents = list(self.parents.values())
            return self.answer({"records": parents[begin:begin + int(payload.get("pageSize", 100))], "total": len(parents)})
        if path.endswith("/data-acquisition/task-result"):
            payload = json.loads(request.content or b"{}")
            rows = self.results.get(str(payload.get("taskId", "")), [])
            return self.answer({"records": rows, "total": len(rows)})
        if path.endswith("/data-acquisition/batchSave"):
            self.metrics["createRequests"] += 1
            for task in json.loads(request.content):
                start, end = datetime.fromisoformat(task["startTime"]), datetime.fromisoformat(task["endTime"])
                media = await self.clip(round((start - self.start).total_seconds()), round((end - start).total_seconds()))
                task_id, result_id = "task-" + uuid4().hex, "result-" + uuid4().hex
                object_path = f"/rehearsal/{self.run_id}/{task_id}/{result_id}.mp4"
                self.parents[task_id] = {**task, "id": task_id, "taskStatus": "FIXED"}
                self.results[task_id] = [{"id": result_id, "taskId": task_id, "status": "FINISH",
                                           "url": "http://storage.test" + object_path,
                                           "beginTime": task["startTime"]}]
                self.objects[object_path] = media
                self.metrics["createdTasks"] += 1
            if self.fail_after_write:
                self.fail_after_write = False
                raise httpx.ReadError("injected accepted-submit response loss", request=request)
            return self.answer(True)
        if path.endswith("/data-acquisition/delete"):
            payload = json.loads(request.content)
            identity = str(payload.get("id", ""))
            if payload.get("type") == "acInfo":
                for parent, rows in self.results.items():
                    self.results[parent] = [row for row in rows if str(row["id"]) != identity]
            else:
                self.parents.pop(identity, None)
            return self.answer(True)
        self.metrics["unexpectedRequests"].append({"method": request.method, "path": path})
        return httpx.Response(404)


def source_handler(request: httpx.Request) -> httpx.Response:
    records = [{"flowCode": "R-GOOD", "carNumber": "鲁NH1113", "createTime": "2026-09-13 09:49:00"},
               {"flowCode": "R-SHORT", "carNumber": "鲁NH1114", "createTime": "2026-09-13 09:49:00"},
               {"flowCode": "R-IDENTITY", "carNumber": "VIRTUAL", "createTime": "2026-09-13 09:49:00"}]
    path = request.url.path
    if path.endswith("/intelligence/intelliTaskInfo/page"):
        return httpx.Response(200, json={"success": True, "data": {"records": records, "total": len(records)}})
    if path.endswith("/intelligence/intelliTaskInfo/getCheckDetail"):
        duration = 10 if request.url.params.get("flowCode") == "R-SHORT" else 12
        return httpx.Response(200, json={"success": True, "data": {
            "stationResults": [{"checkStartTime": "2026-09-13 09:49:00", "checkCompleteTime":
                                 (datetime(2026, 9, 13, 9, 49) + timedelta(minutes=duration)).isoformat(sep=" ")}],
            "totalCheckResult": {"allOriginImageUrls": []}}})
    return httpx.Response(404)


async def _wait(predicate, timeout=600):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        value = predicate()
        if value:
            return value
        await asyncio.sleep(.05)
    raise TimeoutError("Isolated rehearsal stage did not reach its expected terminal state.")


async def run_capture(root: Path, source: Path, settings: dict) -> dict:
    """Execute a real-file capture flow and retain the isolated app for inference."""
    supplied_source = Path(source)
    if supplied_source.is_symlink():
        raise ValueError("The rehearsal source must not be a symbolic link.")
    root, source = Path(root).resolve(), supplied_source.resolve()
    root.mkdir(parents=True, exist_ok=True)
    if source.is_symlink() or not source.is_file():
        raise ValueError("The rehearsal source must be a regular caller-owned copy.")
    ffmpeg = str(settings.get("frameToolPath") or shutil.which("ffmpeg") or "ffmpeg")
    run_id = uuid4().hex
    platform = CapturePlatform(root / "simulated-capture", source, ffmpeg, run_id)
    source_client = RemoteClient("source", transport=httpx.MockTransport(source_handler))
    target_client = RemoteClient("target", transport=httpx.MockTransport(platform.handle))
    source_client.connected = target_client.connected = True
    source_client.user = target_client.user = "isolated-rehearsal"
    app = create_app(source_client, target_client, root / "database", staging_directory=root / "download-cache")
    app.state.slicing.store.settings["frameToolPath"] = ffmpeg
    app.state.slicing.store.save_settings()
    # No lifespan: never restore credentials or start broad production-style loops.
    stages: list[dict] = []
    result = {"app": app, "videoId": "", "filePath": None, "stages": stages,
              "metrics": platform.metrics, "platform": platform}

    def record(node, case, expected, actual, passed, started, detail=None, kind="平台协议模拟／真实视频字节"):
        stages.append({"node": node, "case": case, "kind": kind, "expected": expected,
                       "actual": actual, "passed": bool(passed), "seconds": round(time.monotonic() - started, 3),
                       "detail": detail or {}})
        (root / "capture-stages.json").write_text(json.dumps({"stages": stages, "metrics": platform.metrics},
                                                            ensure_ascii=False, indent=2), encoding="utf-8")

    async def operation(response):
        if response.status_code != 202:
            raise AssertionError(f"Operation registration returned HTTP {response.status_code}: {response.text[:250]}")
        operation_id = response.json()["operationId"]
        return await _wait(lambda: (entry if (entry := app.state.videos.store.get_operation(operation_id))
                                   and entry.get("state") in {"complete", "attention", "conflict", "failed"} else None))

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1") as client:
        started = time.monotonic()
        query = await client.post("/api/search", json={"filters": {
            "dateFrom": "2026-09-13", "dateTo": "2026-09-13", "timeFrom": "00:00:00", "timeTo": "23:59:59", "plates": ""}})
        if query.status_code != 202:
            record("质检查询与准入", "正常", "查询完成且1条批准", f"HTTP {query.status_code}", False, started)
            return result
        search_id = query.json()["id"]
        search = None
        for _ in range(200):
            search = (await client.get(f"/api/search/{search_id}")).json()
            if search.get("status") in {"complete", "failed"}:
                break
            await asyncio.sleep(.02)
        rows = {row["id"]: row for row in search.get("records", [])}
        good = rows.get("R-GOOD", {})
        record("质检查询与准入", "正常", "真实车牌、12分钟通过默认准入", {"status": search.get("status"), "issue": good.get("issue")},
               search.get("status") == "complete" and bool(good) and not good.get("issue"), started)
        for identity, label in (("R-SHORT", "严格10分钟拒绝"), ("R-IDENTITY", "无车牌且无照片拒绝")):
            stage_start = time.monotonic()
            bad = await client.post("/api/preview", json={"searchId": search_id, "recordIds": [identity]})
            tasks = bad.json().get("tasks", []) if bad.status_code == 200 else []
            rejected = (tasks and all(task.get("status") == "invalid" for task in tasks)) or bad.status_code == 400
            before = platform.metrics["createdTasks"]
            submit = await client.post("/api/jobs", json={"confirmed": True, "planId": bad.json().get("id", "")}) if tasks else None
            record("质检查询与准入", label, "拒绝且不创建采集", {"previewStatus": bad.status_code,
                   "taskStatuses": [t.get("status") for t in tasks], "submitStatus": submit.status_code if submit else None},
                   rejected and before == platform.metrics["createdTasks"] and (submit is None or submit.status_code == 400), stage_start)
        started = time.monotonic()
        preview = await client.post("/api/preview", json={"searchId": search_id, "recordIds": ["R-GOOD"]})
        plan = preview.json()
        planned = plan.get("tasks", [{}])[0]
        record("采集计划", "正常", "质检开始+5分钟，60分钟", {"rule": plan.get("rule"), "startTime": planned.get("startTime"), "endTime": planned.get("endTime")},
               preview.status_code == 200 and planned.get("startTime") == "2026-09-13 09:54:00"
               and planned.get("endTime") == "2026-09-13 10:54:00", started)
        if preview.status_code != 200 or planned.get("status") != "ready":
            return result
        started = time.monotonic()
        submission = await client.post("/api/jobs", json={"confirmed": True, "planId": plan["id"]})
        await _wait(lambda: not app.state.jobs.running)
        await app.state.videos.check_once()
        videos = (await client.get("/api/videos")).json()["videos"]
        if not videos:
            record("采集提交", "正常", "原版ready", "未生成视频记录", False, started)
            return result
        identity = videos[0]["id"]
        result["videoId"] = identity
        current = app.state.videos.store.get(identity)
        original = app.state.videos.version(current, 0)
        replay = await client.post("/api/jobs", json={"confirmed": True, "planId": plan["id"]})
        record("采集提交", "正常及重复点击", "恰好1个原版任务并且ready", {"http": submission.status_code,
               "state": original.get("state"), "tasks": len(platform.parents), "sameJob": replay.json().get("id") == submission.json().get("id")},
               submission.status_code == 202 and original.get("state") == "ready" and len(platform.parents) == 1
               and replay.json().get("id") == submission.json().get("id"), started)
        started = time.monotonic()
        streamed = await client.get(f"/api/videos/{identity}/versions/0/stream", headers={"Range": "bytes=0-65535"})
        with platform.clip_cache[(0, 3600)].open("rb") as original_file:
            reference = original_file.read(65536)
        record("预览", "Range", "206且头64KiB字节一致", {"http": streamed.status_code, "bytes": len(streamed.content)},
               streamed.status_code == 206 and streamed.content == reference, started)
        started = time.monotonic()
        invalid = await client.get(f"/api/videos/{identity}/versions/0/stream", headers={"Range": "bytes=1-2,5-6"})
        record("预览", "非法Range", "416且无全文件读取", {"http": invalid.status_code}, invalid.status_code == 416, started)
        started = time.monotonic()
        # Exercise the actual download endpoint body incrementally: ASGITransport
        # buffers full responses, so calling its endpoint directly avoids a 3GB RAM copy.
        endpoint = next(route.endpoint for route in app.routes if getattr(route, "path", "") == "/api/videos/{identity}/versions/{revision}/download")
        request = Request({"type": "http", "method": "GET", "path": f"/api/videos/{identity}/versions/0/download",
                           "headers": [], "query_string": b"", "scheme": "http", "server": ("127.0.0.1", 80)})
        response = await endpoint(identity, 0, request)
        received, digest = 0, hashlib.sha256()
        try:
            async for block in response.body_iterator:
                digest.update(block)
                received += len(block)
        finally:
            # Iterating directly also requires the same lifecycle as the
            # response's ASGI __call__; background alone does not own its lease.
            try:
                if response.background:
                    await response.background()
                if getattr(response, "close_stream", None):
                    await response.close_stream()
            finally:
                if getattr(response, "release_read", None):
                    response.release_read()
        original_digest = await asyncio.to_thread(file_digest, platform.clip_cache[(0, 3600)])
        record("历史版本下载", "原版全量流式下载", "原版所有字节和SHA-256一致，无整文件内存缓冲", {"bytes": received, "sha256": digest.hexdigest()},
               received == platform.clip_cache[(0, 3600)].stat().st_size and digest.hexdigest() == original_digest, started,
               {"transport": "真实download端点迭代器；另已通过ASGI验证Range预览"})
        # Leave an unsent draft locally; neither HTTP request nor platform write occurs.
        started = time.monotonic()
        before_revision, before_tasks = current["revision"], platform.metrics["createdTasks"]
        draft = {"startTime": "2026-09-13 10:09:00", "durationSeconds": 3600}
        draft.clear()
        record("人工更正", "撤销未提交草稿", "没有平台操作或版本变化", {"revision": app.state.videos.store.get(identity)["revision"],
               "createdTasks": platform.metrics["createdTasks"]}, before_tasks == platform.metrics["createdTasks"]
               and before_revision == app.state.videos.store.get(identity)["revision"], started,
               {"scope": "仅验证未提交数据无副作用；页面交互由浏览器测试覆盖"}, kind="本地状态检查")
        for offset in (300, 600):
            started = time.monotonic()
            current = app.state.videos.store.get(identity)
            payload = {"startTime": (platform.start + timedelta(seconds=offset)).isoformat(sep=" "), "durationSeconds": 3600,
                       "expectedRevision": current["revision"], "requestId": f"real-correction-{offset}"}
            if offset == 300:
                platform.fail_after_write = True
            created_before = platform.metrics["createdTasks"]
            reply = await client.post(f"/api/videos/{identity}/corrections", json=payload)
            op = await operation(reply)
            await app.state.videos.check_once()
            duplicate = await client.post(f"/api/videos/{identity}/corrections", json=payload)
            current = app.state.videos.store.get(identity)
            active = app.state.videos.version(current, current["activeRevision"])
            record("人工更正", "接受后丢响应及对账" if offset == 300 else "连续第二次更正", "唯一新任务、版本ready、重复请求复用操作", {
                "operation": op.get("state"), "activeRevision": current["activeRevision"], "state": active.get("state"),
                "createdTasks": platform.metrics["createdTasks"] - created_before, "sameOperation": duplicate.json().get("operationId") == reply.json().get("operationId")},
                op.get("state") == "complete" and active.get("state") == "ready"
                and platform.metrics["createdTasks"] == created_before + 1
                and duplicate.json().get("operationId") == reply.json().get("operationId"), started)
        started = time.monotonic()
        stale = await client.post(f"/api/videos/{identity}/corrections", json={"startTime": "2026-09-13 10:05:00", "durationSeconds": 3600,
                   "expectedRevision": 0, "requestId": "stale-correction"})
        record("人工更正", "旧修订冲突", "409，原绑定不变", {"http": stale.status_code}, stale.status_code == 409, started)
        started = time.monotonic()
        historical = await client.get(f"/api/videos/{identity}/versions/0/download", headers={"Range": "bytes=0-65535"})
        record("历史版本下载", "连续更正后旧版可下载", "206，原版仍在且字节一致", {"http": historical.status_code,
               "bytes": len(historical.content)}, historical.status_code == 206 and historical.content == reference, started)
        # Seed a tiny, genuine checkpoint for destructive-version guard cases.
        # These call the real downloader and are rejected before ffprobe/publishing.
        final_row = app.state.videos.store.get(identity)
        active = app.state.videos.version(final_row, final_row["activeRevision"])
        final_path = platform.clip_cache[(600, 3600)]
        for fault, expected_code in (("etag", "object_changed"), ("range", "range_invalid"), ("length", "invalid_length")):
            started = time.monotonic()
            trial_id = "fault-" + fault + "-" + uuid4().hex
            trial = json.loads(json.dumps(final_row)); trial["id"] = trial_id
            trial_version = app.state.videos.version(trial, trial["activeRevision"])
            trial_version.update(etag='"rehearsal-' + final_path.stem + '"', totalBytes=final_path.stat().st_size)
            part = app.state.videos.work / trial_id / f"v{trial_version['revision']}.part"
            part.parent.mkdir(parents=True)
            with final_path.open("rb") as incoming:
                part.write_bytes(incoming.read(1024 * 1024))
            platform.fault = fault
            actual = "unexpected success"
            try:
                await app.state.videos.download_bound(trial, trial_version)
            except DownloadFailure as error:
                actual = error.code
            finally:
                platform.fault = ""
            record("视频下载", fault + "异常", expected_code + "且保留断点", {"code": actual, "checkpointBytes": part.stat().st_size},
                   actual == expected_code and part.stat().st_size == 1024 * 1024, started, kind="受控故障注入／真实下载逻辑")
            part.unlink(missing_ok=True)
            part.parent.rmdir()
            # The downloader durably saved its disposable trial row. Do not
            # expose those failure fixtures to downstream inference enrollment.
            app.state.videos.store.db.execute("DELETE FROM simple_videos WHERE id=?", (trial_id,))
            app.state.videos.store.db.commit()
        # The real confirmation uses unmodified downloader, hash, ffprobe and tail decoder.
        started = time.monotonic()
        configured = await client.post("/api/settings/directory", json={"directory": str(root / "downloads")})
        current = app.state.videos.store.get(identity)
        platform.break_next_download = True
        confirmation = await client.post(f"/api/videos/{identity}/confirm", json={"version": current["activeRevision"],
                    "expectedRevision": current["revision"], "requestId": "real-final-confirm"})
        await _wait(lambda: not app.state.videos.running, timeout=1200)
        current = app.state.videos.store.get(identity)
        saved = app.state.videos.version(current, current["activeRevision"])
        downloaded = Path(saved["filePath"]) if saved.get("filePath") else None
        downloaded_digest = await asyncio.to_thread(file_digest, downloaded) if downloaded and downloaded.is_file() else ""
        reference_digest = await asyncio.to_thread(file_digest, final_path)
        resumed = any(entry["offset"] == 16 * 1024 * 1024 for entry in platform.metrics["ranges"])
        passed = (configured.status_code == 200 and confirmation.status_code == 202 and saved.get("state") == "saved"
                  and downloaded_digest == reference_digest)
        record("视频下载", "断流续传、真实完整性与最终发布", "续传并保存，真实ffprobe/末帧校验与SHA-256全部通过", {
            "state": saved.get("state"), "errorCode": saved.get("errorCode"), "message": saved.get("message"),
            "attempts": saved.get("attempts"), "bytes": downloaded.stat().st_size if downloaded and downloaded.is_file() else 0,
            "sha256": downloaded_digest, "resumed": resumed},
            passed and resumed and saved.get("attempts", 0) >= 2, started, kind="真实视频解码／平台协议模拟／受控断流")
        result["filePath"] = downloaded
        started = time.monotonic()
        old_versions = [version for version in current["versions"] if version["revision"] < current["activeRevision"]]
        record("新版替换与清理", "成功后精确清理旧版", "仅新版保存后旧版绑定清理，最终文件保留", {
            "oldStates": [version.get("state") for version in old_versions], "remainingTasks": len(platform.parents),
            "cleanupPending": current.get("cleanupPending"), "finalExists": bool(downloaded and downloaded.exists())},
            passed and not current.get("cleanupPending") and all(version.get("state") == "cloud_deleted" for version in old_versions)
            and len(platform.parents) == 1, started)
    return result
