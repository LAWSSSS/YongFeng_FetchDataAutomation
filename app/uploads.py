"""Durable, target-independent upload and import queues for the manual app."""
import asyncio
import base64
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import time
from collections import Counter
from pathlib import Path
from uuid import uuid4

from .audit_moves import checked_path, source_fingerprint
from .domain import now_iso
from .file_leases import FileLeases
from .upload_audit import AuditError, DATASETS, VIDEO_EXTENSIONS, name_key, scan_directory
from .upload_remote import UploadRemote, source_keys, video_names
from .videos import Conflict

CHECK_SECONDS = 30
STALL_SECONDS = 600
RETRY_DELAYS = (10, 30, 60)
MAX_FILE_BYTES = 10 * 1024 ** 3
MAX_BATCH_BYTES = 20 * 1024 ** 3  # Strictly smaller than this bound.
DEFAULT_DIRECTORY = "/Volumes/out/视频"
PAUSE_CODES = {"auth", "network", "disk", "authorization"}
TERMINAL = {"complete", "attention"}
MESSAGES = {
    "auth": "视觉AI会话已失效，请重新连接；断点已保留。",
    "authorization": "上传授权需刷新，分片已保留，正在重新授权。",
    "network": "网络暂不可达，保留断点并等待连接恢复。",
    "disk": "原视频或外置盘暂不可用，连接原磁盘后继续。",
    "file_changed": "原视频版本发生变化，未继续上传；请核对文件后重新扫描。",
    "object_changed": "上传对象身份或长度发生变化，已停止该条，请核对。",
    "multipart_unknown": "分片上传身份无法唯一确认，未重复创建或清理。",
    "parts_invalid": "平台分片长度或身份异常，请核对。",
    "stalled": "连续10分钟没有真实上传进展，正在清理本条未完成分片。",
    "upload_failed": "本条上传失败，其他视频继续处理。",
    "worker_unavailable": "上传工作进程未能启动，请检查依赖后重试。",
    "completion_unknown": "上传完成响应尚未确认，先核对对象与分片。",
    "cleanup_pending": "未完成分片的清理尚未确认，将继续对账。",
    "multipart_missing": "原分片上传已不存在，正在核对完整对象。",
    "import_uncertain": "导入提交结果无法唯一确认，此目标暂停新导入，请到平台核对。",
    "import_failed": "平台明确报告此文件导入失败，将单独重试。",
    "import_identity": "导入任务或对象清单不一致，请核对，未重复提交。",
    "verification": "导入结束，但无法确认此视频的结果，保留待核对。",
    "authorization_binding": "原上传位置的授权无法确认，已保留分片及原视频；需要平台重新授权后再单条核对。",
}


def safe_message(code):
    return MESSAGES.get(code, "操作未完成，进度已保留，请核对后重试。")


def inspect_video(path, expected):
    """Read metadata only; reject growing/unfinished containers and unsafe paths."""
    if source_fingerprint(Path(path)) != expected:
        raise ValueError("file_changed")
    executable = shutil.which("ffprobe")
    if not executable:
        raise ValueError("worker_unavailable")
    try:
        result = subprocess.run([executable, "-v", "error", "-show_entries", "format=duration,size",
            "-of", "json", path], capture_output=True, timeout=20, check=False)
        data = json.loads(result.stdout) if result.returncode == 0 else {}
        info = data.get("format", {})
        if float(info.get("duration", 0)) <= 0 or int(info.get("size", 0)) != expected["size"]:
            raise ValueError("incomplete_video")
    except (subprocess.TimeoutExpired, json.JSONDecodeError, TypeError, OverflowError):
        raise ValueError("incomplete_video") from None
    if source_fingerprint(Path(path)) != expected:
        raise ValueError("file_changed")


class UploadStore:
    def __init__(self, db):
        self.db = db
        for table in ("simple_upload_items", "simple_upload_batches", "simple_upload_requests"):
            db.execute(f"CREATE TABLE IF NOT EXISTS {table} (id TEXT PRIMARY KEY, payload TEXT NOT NULL)")
        db.execute("CREATE TABLE IF NOT EXISTS simple_upload_settings (id INTEGER PRIMARY KEY CHECK(id=1), payload TEXT NOT NULL)")
        db.commit()
        row = db.execute("SELECT payload FROM simple_upload_settings WHERE id=1").fetchone()
        self.settings = json.loads(row[0]) if row else {"enabled": False, "directory": DEFAULT_DIRECTORY,
            "validated": {}, "targets": {}, "scan": None}

    def save_settings(self):
        self.db.execute("INSERT INTO simple_upload_settings VALUES(1,?) ON CONFLICT(id) DO UPDATE SET payload=excluded.payload",
            (json.dumps(self.settings, ensure_ascii=False),))
        self.db.commit()

    def save(self, kind, value):
        assert kind in ("items", "batches", "requests")
        value["updatedAt"] = now_iso()
        self.db.execute(f"INSERT INTO simple_upload_{kind} VALUES(?,?) ON CONFLICT(id) DO UPDATE SET payload=excluded.payload",
            (value["id"], json.dumps(value, ensure_ascii=False)))
        self.db.commit()

    def get(self, kind, identity):
        assert kind in ("items", "batches", "requests")
        row = self.db.execute(f"SELECT payload FROM simple_upload_{kind} WHERE id=?", (identity,)).fetchone()
        return json.loads(row[0]) if row else None

    def list(self, kind):
        assert kind in ("items", "batches", "requests")
        return [json.loads(r[0]) for r in self.db.execute(f"SELECT payload FROM simple_upload_{kind} ORDER BY rowid")]


class ProcessWorker:
    """Credentials never enter argv, environment, a file, queue payload or logs."""
    def __init__(self):
        self.processes = {}

    async def run(self, item, credentials, callback, operation="upload"):
        process = await asyncio.create_subprocess_exec(sys.executable, "-m", "app.upload_worker",
            stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL,
            cwd=str(Path(__file__).resolve().parents[1]), limit=128 * 1024)
        self.processes[item["id"]] = process
        try:
            process.stdin.write((json.dumps({"item": item, "credentials": credentials, "operation": operation}) + "\n").encode())
            await process.stdin.drain()
            terminal = False
            while True:
                line = await process.stdout.readline()
                if not line:
                    break
                try:
                    event = json.loads(line)
                except ValueError:
                    raise ValueError("worker_unavailable") from None
                if not isinstance(event, dict):
                    raise ValueError("worker_unavailable")
                await callback(event)
                terminal |= event.get("event") in ("complete", "cleaned", "error")
                if event.get("event") in ("creating", "created", "part", "completing", "complete", "cleaned"):
                    process.stdin.write(b"ack\n")
                    await process.stdin.drain()
            await process.wait()
            if not terminal:
                await callback({"event": "error", "code": "completion_unknown" if item.get("uploadId") else "upload_failed"})
        finally:
            if process.returncode is None:
                process.kill()
                await process.wait()
            self.processes.pop(item["id"], None)


class UploadManager:
    def __init__(self, store, audit, leases=None, remote=None, worker=None, file_check=None, clock=None):
        self.store = UploadStore(store.db)
        self.audit, self.remote = audit, remote or UploadRemote(audit.client)
        self.leases = leases or FileLeases()
        self.leases.on_move = self.moved
        self.worker = worker or ProcessWorker()
        self.file_check = file_check or inspect_video
        self.clock = clock or time.time
        self.running = {}
        self.runtime = {}
        self.grants = {}  # Runtime-only STS grants; never part of state(), logs or SQLite.
        self.wake = asyncio.Event()
        self.loop_task = self.scan_task = None
        self.stopping = False
        self.tick_lock = asyncio.Lock()
        self.scan_lock = asyncio.Lock()
        # Additive migration only: never touch collection/video tables.
        for item in self.store.list("items"):
            if item["state"] == "uploading":
                item.update(state="queued", phase="reconciling", message="程序重启，先核对已完成对象及分片。")
                self.store.save("items", item)
        scan = self.store.settings.get("scan")
        if scan and scan["state"] == "scanning":
            scan.update(state="interrupted", message="扫描被中断，请用同一请求继续；已入队记录保留。")
            request = self.store.get("requests", scan["id"])
            if request and request["state"] == "scanning":
                request.update(state="interrupted", message=scan["message"])
                self.store.save("requests", request)
            self.store.save_settings()

    @property
    def busy(self):
        return bool(self.running) or bool(self.scan_task and not self.scan_task.done())

    def target_info(self, dataset):
        return self.store.settings.setdefault("targets", {}).setdefault(dataset, {})

    def moved(self, source, destination, file_version):
        for item in self.store.list("items"):
            if item["path"] == source:
                # A verified cross-volume move changes inode/device, but not bytes.
                item.setdefault("uploadFileVersion", item["fileVersion"])
                item.update(path=destination, fileVersion=file_version, movedAt=now_iso())
                if item.get("object"):
                    # S3 metadata continues to refer to the original upload version.
                    item.setdefault("uploadFileVersion", item.get("originalFileVersion", item["fileVersion"]))
                self.store.save("items", item)

    def state(self):
        items, batches = self.store.list("items"), self.store.list("batches")
        targets = []
        validated = all(self.store.settings.get("validated", {}).get(d["id"]) for d in DATASETS)
        for dataset in DATASETS:
            selected = [i for i in items if i["dataset"] == dataset["id"]]
            public = []
            for item in selected:
                # Object keys and multipart IDs are receipts, not browser data.
                row = {k: item.get(k) for k in ("id", "name", "path", "size", "state", "phase", "bytes", "speed",
                    "retries", "message", "errorCode", "jobId", "nextAttempt", "createdAt", "updatedAt", "completedAt")}
                row["bytes"] = max(row.get("bytes") or 0, self.runtime.get(item["id"], {}).get("bytes", 0))
                row["speed"] = self.runtime.get(item["id"], {}).get("speed", 0)
                row["retries"] = min(row.get("retries") or 0, 3)
                public.append(row)
            info = self.target_info(dataset["id"])
            targets.append({**dataset, "items": public, "counts": dict(Counter(i["state"] for i in selected)),
                "activeUploads": sum(i["id"] in self.running for i in selected),
                "activeImport": info.get("activeImport", ""), "blocked": bool(info.get("blocked")),
                "message": info.get("message", ""), "lastCheckedAt": info.get("lastCheckedAt", ""),
                "batches": [{k: b.get(k) for k in ("id", "state", "jobId", "message", "createdAt")}
                    for b in batches if b["dataset"] == dataset["id"] and b["state"] not in ("complete", "empty")]})
        return {"enabled": self.store.settings["enabled"], "connected": self.audit.client.connected,
            "directory": self.store.settings.get("directory", DEFAULT_DIRECTORY), "scan": self.store.settings.get("scan"),
            "validationMode": not validated, "parallelFilesPerTarget": 4 if validated else 1,
            "checkSeconds": CHECK_SECONDS, "stallSeconds": STALL_SECONDS, "targets": targets}

    async def start(self):
        if not self.loop_task or self.loop_task.done():
            self.stopping = False
            self.loop_task = asyncio.create_task(self.loop(), name="automatic-uploads")

    async def loop(self):
        while not self.stopping:
            self.wake.clear()
            try:
                await self.tick()
            except asyncio.CancelledError:
                raise
            except Exception:
                # No exception text: SDK/HTTP errors may contain credentials.
                for dataset in DATASETS:
                    self.target_info(dataset["id"])["message"] = "上传调度暂未完成，进度已保留，下一轮重新核对。"
                self.store.save_settings()
            try:
                await asyncio.wait_for(self.wake.wait(), CHECK_SECONDS)
            except TimeoutError:
                pass

    def scan(self, directory, request_id):
        directory = str(Path(directory or DEFAULT_DIRECTORY).expanduser())
        old = self.store.get("requests", request_id)
        if old and old["directory"] != directory:
            raise Conflict("请求ID已用于另一个目录，请使用新的请求。")
        if old and old["state"] not in ("interrupted", "failed"):
            return self.state()
        if self.scan_task and not self.scan_task.done():
            raise Conflict("扫描并补传正在进行，请等待本次完成。")
        if self.audit.busy or self.audit.file_move_busy:
            raise Conflict("日志核对或分类移动正在进行，请完成后扫描。")
        if not self.audit.client.connected:
            raise AuditError("请先连接视觉AI平台。", "auth", 401)
        value = {"id": request_id, "directory": directory, "state": "scanning", "createdAt": now_iso(),
                 "message": "正在核对完整文件及两个目标的缺失情况…", "added": 0, "review": 0}
        self.store.save("requests", value)
        self.store.settings.update(directory=directory, enabled=True, scan=value)
        self.store.save_settings()
        self.scan_task = asyncio.create_task(self.scan_run(value))
        return self.state()

    async def scan_run(self, run):
        try:
            async with self.scan_lock:
                checked_path(run["directory"])
                self.audit.start(run["directory"], True)
                audit_id = self.audit.run["id"]
                await self.audit.task
                report = self.audit.saved.get("report")
                if self.audit.run.get("status") != "complete" or not report or report["id"] != audit_id:
                    raise ValueError("日志核对未完成，未追加未知视频；请检查连接后重试。")
                existing = self.store.list("items")
                for file in report["files"]:
                    relative = Path(file["relativePath"])
                    if any(p.casefold() in ("cache", "tmp", "temp", "work", "node_modules") for p in relative.parts[:-1]):
                        continue
                    if re.search(r"\.(?:part|partial|download|tmp|moving)(?:\.|$)", file["name"], re.I):
                        continue
                    path = str(Path(run["directory"]) / relative)
                    for dataset in DATASETS:
                        previous = [i for i in existing if i["dataset"] == dataset["id"] and
                            ((i["path"] == path and i["fileVersion"] == file["fileVersion"]) or
                             i.get("originalFileVersion") == file["fileVersion"])]
                        if previous:
                            continue
                        result = file["targets"][dataset["id"]]
                        if result["status"] == "uploaded":
                            continue
                        issue = file.get("issue", "")
                        if file["size"] > MAX_FILE_BYTES:
                            issue = "单个视频超过平台10 GiB限制，请人工分片后重新扫描。"
                        if not re.fullmatch(r"[a-zA-Z0-9_~!.#@&+=$%\-()（）+\u4e00-\u9fa5]{1,100}", file["name"]):
                            issue = "文件名超出平台允许的字符或100字符限制，请核对后重命名。"
                        if any(i["dataset"] == dataset["id"] and name_key(i["name"]) == name_key(file["name"])
                               and i["fileVersion"] != file["fileVersion"] for i in existing):
                            issue = "同名视频已有其他文件版本，不能自动重复提交。"
                        review = bool(issue) or result["status"] not in ("missing",)
                        item = {"id": uuid4().hex, "dataset": dataset["id"], "name": file["name"], "path": path,
                            "size": file["size"], "fileVersion": file["fileVersion"], "originalFileVersion": file["fileVersion"],
                            "state": "attention" if review else "queued", "phase": "review" if review else "queued",
                            "bytes": 0, "speed": 0, "retries": 0, "parts": {}, "batchId": "", "jobId": "",
                            "message": issue or result["message"] if review else "已排队，等待上传。",
                            "errorCode": "scan_review" if review else "", "createdAt": now_iso(), "scanId": run["id"]}
                        self.store.save("items", item)
                        existing.append(item)
                        run["review" if review else "added"] += 1
                run.update(state="complete", message=f"扫描完成：新增 {run['added']} 条目标上传，待核对 {run['review']} 条。")
        except asyncio.CancelledError:
            run.update(state="interrupted", message="扫描中断，已入队视频保留，可重试同一请求。")
            raise
        except (AuditError, ValueError) as error:
            run.update(state="failed", message=str(error))
        except Exception:
            run.update(state="failed", message="扫描未能完成，已入队进度保留。")
        finally:
            self.store.save("requests", run)
            self.store.settings["scan"] = run
            self.store.save_settings()
            self.wake.set()

    async def pause(self):
        self.store.settings["enabled"] = False
        self.store.save_settings()
        # Drain selectors before cancelling workers: an in-flight inventory read
        # must not launch a new process after this endpoint has returned.
        async with self.tick_lock:
            tasks = list(self.running.values())
            for task in tasks:
                task.cancel()
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
        self.wake.set()
        return self.state()

    def resume(self):
        self.store.settings["enabled"] = True
        self.store.save_settings()
        self.wake.set()
        return self.state()

    def retry(self, identity):
        item = self.store.get("items", identity)
        if not item:
            raise ValueError("上传记录不存在。")
        if item["state"] == "complete" or identity in self.running:
            return self.state()
        batch = self.store.get("batches", item["batchId"]) if item.get("batchId") else None
        if batch and batch["state"] in ("submitting", "uncertain"):
            item.update(message="已安排按原对象清单重新对账；不会重新提交导入。")
            self.store.save("items", item)
            self.wake.set()
            return self.state()
        if batch and batch["state"] in ("importing", "verifying"):
            raise Conflict("此条已有提交或导入凭证，先核对原任务，不能重复上传。")
        if item.get("errorCode") in ("object_changed", "file_changed", "import_identity"):
            raise Conflict("对象或文件身份变化，需要人工核对原记录，不能按重试重新发送。")
        if item.get("errorCode") == "verification" and batch and batch.get("jobId"):
            batch.update(state="verifying", message="正在重新核对原导入任务的逐文件结果。")
            self.store.save("batches", batch)
            item.update(state="importing", phase="reconciling", message="正在核对原导入结果，不重新发送。")
            self.store.save("items", item)
            self.wake.set()
            return self.state()
        # A retry never turns an uncertain scan into missing. Re-read its evidence.
        item.update(state="checking" if item.get("errorCode") in ("scan_review", "verification") else
            "cleanup" if item.get("cleanupNeeded") else "queued", message="已安排单条重新核对。", retries=0, nextAttempt=0,
            batchId="", errorCode="")
        self.store.save("items", item)
        self.store.settings["enabled"] = True
        self.store.save_settings()
        self.wake.set()
        return self.state()

    async def tick(self):
        async with self.tick_lock:
            now = self.clock()
            for identity, task in list(self.running.items()):
                runtime = self.runtime.get(identity, {})
                if not task.done() and now - runtime.get("lastProgress", now) >= STALL_SECONDS:
                    runtime["stopCode"] = "stalled"
                    task.cancel()
            if not self.audit.client.connected:
                for dataset in DATASETS:
                    self.target_info(dataset["id"])["message"] = safe_message("auth")
                self.store.save_settings()
                return
            await asyncio.gather(*(self.tick_target(d["id"]) for d in DATASETS))

    async def tick_target(self, dataset):
        items = [i for i in self.store.list("items") if i["dataset"] == dataset]
        batches = [b for b in self.store.list("batches") if b["dataset"] == dataset]
        if not items and not batches:
            return
        info = self.target_info(dataset)
        try:
            jobs = await self.remote.jobs(dataset)
            info.update(lastCheckedAt=now_iso(), message="", activeImport="")
            for batch in batches:
                if batch["state"] in ("submitting", "uncertain"):
                    await self.reconcile_submit(batch, jobs)
                if batch["state"] in ("importing", "verifying"):
                    job = next((j for j in jobs if str(j["id"]) == batch.get("jobId")), None)
                    if not job:
                        self.block_batch(batch, "import_uncertain")
                    elif source_keys(job) != frozenset(batch["keys"]):
                        self.block_batch(batch, "import_identity")
                    elif str(job["status"]) in ("1", "2"):
                        info["activeImport"] = str(job["id"])
                    elif str(job["status"]) in ("3", "4"):
                        await self.verify_batch(batch, job)
                    else:
                        self.block_batch(batch, "import_identity")
            active = [str(j["id"]) for j in jobs if str(j["status"]) in ("1", "2")]
            info["blocked"] = any(b["dataset"] == dataset and b["state"] in ("submitting", "uncertain", "attention")
                and b.get("blocksImport") for b in self.store.list("batches"))
            if info["blocked"]:
                info["message"] = safe_message("import_uncertain")
            if active:
                info.update(activeImport=", ".join(active), message="等待已有导入结束；允许预上传下一批。")
            elif not info.get("blocked"):
                info["activeImport"] = ""
            if not self.store.settings["enabled"] or self.stopping:
                self.store.save_settings()
                return
            if info.get("pauseUntil", 0) > self.clock():
                return
            now = self.clock()
            for item in items:
                if item["id"] in self.running or item["state"] in TERMINAL or item["state"] in ("uploaded", "importing"):
                    continue
                if item.get("nextAttempt", 0) > now:
                    continue
                if item["state"] == "checking":
                    await self.check_one(item, jobs)
                elif item["state"] in ("waiting_connection", "waiting_auth", "waiting_disk"):
                    item.update(state="cleanup" if item.get("cleanupNeeded") else "queued", phase="reconciling")
                    self.store.save("items", item)
                elif item["state"] == "retry_wait":
                    item.update(state="queued", phase="queued")
                    self.store.save("items", item)
                if item["state"] == "cleanup" and item["id"] not in self.running:
                    if sum(self.store.get("items", i)["dataset"] == dataset for i in self.running) < 4:
                        self.launch(item, "cleanup")

            # Only one preparing/uploaded batch may exist, even during a manual import.
            batches = [b for b in self.store.list("batches") if b["dataset"] == dataset]
            preparing = next((b for b in batches if b["state"] == "preparing"), None)
            if preparing and not any(self.store.get("items", identity).get("batchId") == preparing["id"]
                    and self.store.get("items", identity)["state"] != "attention" for identity in preparing["items"]):
                preparing.update(state="empty", message="本批各条已独立恢复，准备下一批。")
                self.store.save("batches", preparing)
                preparing = None
            if not preparing and not info.get("blocked"):
                queued = [i for i in self.store.list("items") if i["dataset"] == dataset and i["state"] in ("queued", "uploaded")
                    and not i.get("batchId") and i.get("nextAttempt", 0) <= now and i["id"] not in self.running]
                if queued:
                    inventory = await self.remote.videos(dataset)
                    if not self.store.settings["enabled"] or self.stopping:
                        return
                    chosen, size = [], 0
                    validated = all(self.store.settings.get("validated", {}).get(d["id"]) for d in DATASETS)
                    if not validated and any(b["state"] in ("submitting", "uncertain", "importing", "verifying") for b in batches):
                        self.store.save_settings()
                        return
                    # Each target performs exactly one successful real validation sample before the full queue.
                    if not validated and self.store.settings.get("validated", {}).get(dataset):
                        info["message"] = "本目标单条验收已通过，等待另一目标验收后开启每批4条。"
                        self.store.save_settings()
                        return
                    limit = 4 if validated else 1
                    for item in queued:
                        if await self.existing_video(item, inventory):
                            continue
                        if item["size"] > MAX_FILE_BYTES or item["size"] <= 0:
                            self.attention(item, "file_changed", "文件大小无效或超过10 GiB限制，请核对。")
                            continue
                        if len(chosen) >= limit or size + item["size"] >= MAX_BATCH_BYTES:
                            continue
                        chosen.append(item); size += item["size"]
                    if chosen:
                        preparing = {"id": uuid4().hex, "dataset": dataset, "state": "preparing",
                            "items": [i["id"] for i in chosen], "createdAt": now_iso(), "bytes": size,
                            "message": "正在上传本批视频。"}
                        self.store.save("batches", preparing)
                        for item in chosen:
                            item["batchId"] = preparing["id"]
                            self.store.save("items", item)
            if preparing:
                for identity in preparing["items"]:
                    item = self.store.get("items", identity)
                    if (item["batchId"] == preparing["id"] and item["state"] == "queued"
                            and identity not in self.running and self.leases.key(item["path"]) not in self.leases.writers):
                        if sum(self.store.get("items", i)["dataset"] == dataset for i in self.running) < 4:
                            self.launch(item)
                current = [self.store.get("items", i) for i in preparing["items"]]
                waiting = [i for i in current if i["batchId"] == preparing["id"] and i["state"] not in ("uploaded", "attention")]
                uploaded = [i for i in current if i["batchId"] == preparing["id"] and i["state"] == "uploaded"]
                if not waiting:
                    if not uploaded:
                        preparing.update(state="empty", message="本批没有已完成上传的文件，失败项单独恢复。")
                        self.store.save("batches", preparing)
                    elif not active and not info.get("blocked"):
                        await self.submit_batch(preparing, uploaded)
            self.store.save_settings()
        except AuditError as error:
            info.update(message=safe_message(error.code), errorCode=error.code)
            self.store.save_settings()
        except Exception:
            info.update(message="本目标暂未完成核对，进度保留，另一目标继续处理。", errorCode="scheduler")
            self.store.save_settings()

    def attention(self, item, code, message=None):
        item.update(state="attention", phase="review", errorCode=code, message=message or safe_message(code), speed=0)
        self.store.save("items", item)

    async def existing_video(self, item, inventory):
        keys = {name_key(item["name"]), name_key(Path(item["name"]).stem)}
        matches = [v for v in inventory if video_names(v) & keys]
        if not matches:
            return False
        if len(matches) != 1:
            self.attention(item, "verification", "平台存在同名的多个视频，请人工核对，未重复上传。")
        else:
            video = matches[0]
            remote_size = video.get("file_size", video.get("size"))
            if isinstance(remote_size, (int, float)) and remote_size != item["size"]:
                self.attention(item, "verification", "平台同名视频的大小不同，未覆盖或重复上传。")
            else:
                item.update(state="complete", phase="complete", message="平台视频清单已存在该完整名称，无需重复上传。",
                    remoteVideoId=str(video["id"]), completedAt=now_iso(), existing=True)
                self.store.save("items", item)
        return True

    async def check_one(self, item, jobs):
        inventory = await self.remote.videos(item["dataset"])
        if await self.existing_video(item, inventory):
            return
        # Re-check full, successful import logs. Missing evidence is never approved while any log is unavailable.
        full, stem = name_key(item["name"]), name_key(Path(item["name"]).stem)
        for job in jobs:
            found = await self.remote.evidence(item["dataset"], job)
            matches = found["evidence"].get((full, "full"), []) + found["evidence"].get((stem, "stem"), [])
            if matches or found.get("reason") or str(job["status"]) in ("1", "2"):
                self.attention(item, "scan_review", "平台仍有相关记录、未结束导入或不完整日志，未重复提交。")
                return
        if source_fingerprint(Path(item["path"])) != item["fileVersion"]:
            self.attention(item, "file_changed")
            return
        item.update(state="queued", phase="queued", message="单条重新核对完成，已确认缺失。", errorCode="")
        self.store.save("items", item)

    def launch(self, item, operation="upload"):
        identity = item["id"]
        if identity in self.running or not self.store.settings["enabled"] or self.stopping:
            return
        self.runtime[identity] = {"lastProgress": self.clock(), "bytes": item.get("bytes", 0), "speed": 0,
                                  "sampleAt": self.clock(), "sampleBytes": item.get("bytes", 0)}
        task = asyncio.create_task(self.upload_one(identity, operation), name="upload-" + identity)
        self.running[identity] = task
        task.add_done_callback(lambda done: (self.running.pop(identity, None), self.runtime.pop(identity, None), self.wake.set()))

    async def upload_one(self, identity, operation):
        item = self.store.get("items", identity)
        try:
            with self.leases.read(item["path"]):
                if operation != "cleanup":
                    try:
                        await asyncio.wait_for(asyncio.to_thread(self.file_check, item["path"], item["fileVersion"]), 25)
                    except OSError:
                        raise ValueError("disk") from None
                    except TimeoutError:
                        raise ValueError("disk") from None
                credentials = await self.authorization(item)
                item = self.store.get("items", identity)
                prefix = credentials.get("prefix")
                unwritten = not item.get("createAttempted") and not item.get("uploadId") and not item.get("parts")
                replace_unwritten = (item.get("object") and unwritten and isinstance(prefix, str)
                    and not item["object"]["key"].startswith(prefix))
                if not item.get("object") or replace_unwritten:
                    prefix = credentials.get("prefix")
                    key = prefix + "simple-" + identity + "/" + item["name"] if prefix is not None else credentials["key"]
                    item["object"] = {"key": key, "bucket": credentials["bucket"],
                        "endpoint": credentials["endpoint"], "region": credentials.get("region") or "us-east-1"}
                    item["uploadFileVersion"] = item["fileVersion"]
                elif (item["object"]["bucket"] != credentials["bucket"] or
                      item["object"]["endpoint"] != credentials["endpoint"]):
                    raise ValueError("object_changed")
                item.update(state="cleanup" if operation == "cleanup" else "uploading", phase="cleanup" if operation == "cleanup" else "reconciling",
                    message="正在核对并清理本条未完成分片。" if operation == "cleanup" else "正在核对对象与分片，随后续传。", errorCode="")
                self.store.save("items", item)
                payload = {**item, "fileVersion": item.get("uploadFileVersion", item["fileVersion"])}
                # File moves change filesystem identity, while S3 retains the version marker used at creation.
                payload["sourceFileVersion"] = item["fileVersion"]
                await self.worker.run(payload, credentials, lambda event: self.worker_event(identity, event), operation)
        except asyncio.CancelledError:
            item = self.store.get("items", identity)
            code = self.runtime.get(identity, {}).get("stopCode")
            if code == "stalled":
                # An unavailable volume is a pause, not a consumed upload retry.
                try:
                    await asyncio.wait_for(asyncio.to_thread(source_fingerprint, Path(item["path"])), 3)
                except (ValueError, OSError, TimeoutError):
                    self.pause_item(item, "disk")
                else:
                    await self.fail_item(item, "stalled")
            elif item["state"] not in ("uploaded", "complete"):
                item.update(state="cleanup" if item.get("cleanupNeeded") else "queued", phase="paused", speed=0,
                    message="已暂停，已确认分片和原视频保留。")
                self.store.save("items", item)
            raise
        except AuditError as error:
            item = self.store.get("items", identity)
            if error.code in PAUSE_CODES:
                self.pause_item(item, error.code)
            else:
                self.attention(item, error.code)
        except ValueError as error:
            code = str(error) if str(error) in MESSAGES or str(error) in ("incomplete_video",) else "disk"
            item = self.store.get("items", identity)
            if code in PAUSE_CODES:
                self.pause_item(item, code)
            else:
                self.attention(item, code, "视频容器不完整或尚未写入完成，请完成下载后重新扫描。" if code == "incomplete_video" else None)
        except Exception:
            await self.fail_item(self.store.get("items", identity), "upload_failed")

    async def worker_event(self, identity, event):
        kind = event.get("event")
        item = self.store.get("items", identity)
        runtime = self.runtime.get(identity, {})
        now = self.clock()
        if kind == "progress":
            count = event.get("bytes")
            if not isinstance(count, int) or count < 0 or count > item["size"]:
                raise ValueError("parts_invalid")
            if count > runtime.get("bytes", 0):
                elapsed = max(now - runtime.get("sampleAt", now), 0.001)
                runtime.update(lastProgress=now, bytes=count,
                    speed=max(0, count - runtime.get("sampleBytes", 0)) / elapsed, sampleAt=now, sampleBytes=count)
            return
        old_phase = item["phase"]
        if kind == "creating":
            item.update(createAttempted=True, phase="creating")
        elif kind == "created":
            value = event.get("uploadId")
            if not isinstance(value, str) or not value or len(value) > 2000:
                raise ValueError("parts_invalid")
            if item.get("uploadId") and item["uploadId"] != value:
                raise ValueError("object_changed")
            item.update(uploadId=value, phase="uploading")
        elif kind == "part":
            number, size, etag = event.get("number"), event.get("size"), event.get("etag")
            if not isinstance(number, int) or not 1 <= number <= 10000 or not isinstance(size, int) or size <= 0 or not isinstance(etag, str):
                raise ValueError("parts_invalid")
            previous = item["parts"].get(str(number))
            item["parts"][str(number)] = {"number": number, "size": size, "etag": etag}
            item.update(bytes=sum(p["size"] for p in item["parts"].values()), phase="uploading")
            if not previous:
                runtime["lastProgress"] = now
        elif kind == "completing":
            item["phase"] = "completing"
            item["completionAttempted"] = True
        elif kind == "complete":
            if event.get("size") != item["size"] or not event.get("etag"):
                raise ValueError("object_changed")
            item.update(state="uploaded", phase="uploaded", bytes=item["size"], etag=event["etag"],
                objectVersion=event.get("versionId", ""), message="上传已核验，等待本目标导入。", errorCode="",
                cleanupNeeded=False, uploadId="", nextAttempt=0)
        elif kind == "cleaned":
            item.update(cleanupNeeded=False, uploadId="", createAttempted=False, completionAttempted=False, parts={}, bytes=0, phase="queued")
            if item.get("retries", 0) > len(RETRY_DELAYS):
                item.update(state="attention", message="本条已重试3次，失败分片已清理，请人工检查后单条重试。")
            else:
                item.update(state="retry_wait", message="本条分片清理已确认，等待单独重试。")
        elif kind == "error":
            code = event.get("code") if event.get("code") in MESSAGES else "upload_failed"
            if event.get("stage") == "create_multipart_upload" and event.get("httpStatus") == 403 and not item.get("uploadId"):
                # An explicit S3 AccessDenied is a rejection, not an uncertain create.
                item["createAttempted"] = False
            if code == "authorization":
                self.grants.pop(identity, None)
                if event.get("keyInIssuedPrefix") is False and (item.get("uploadId") or item.get("createAttempted")):
                    self.attention(item, "authorization_binding")
                    return
            if code in PAUSE_CODES:
                self.pause_item(item, code)
            elif code in ("object_changed", "file_changed", "multipart_unknown", "parts_invalid"):
                self.attention(item, code)
            elif item.get("cleanupNeeded"):
                item.update(state="cleanup", errorCode=code, message=safe_message(code), nextAttempt=now + CHECK_SECONDS)
                self.store.save("items", item)
            else:
                await self.fail_item(item, code)
            return
        else:
            raise ValueError("worker_unavailable")
        if item["phase"] != old_phase:
            # Each monotonic phase transition is genuine progress, never a UI heartbeat.
            runtime["lastProgress"] = now
        item["lastProgressAt"] = runtime.get("lastProgress", now)
        self.store.save("items", item)

    def pause_item(self, item, code):
        state = "waiting_disk" if code == "disk" else "waiting_auth" if code in ("auth", "authorization") else "waiting_connection"
        item.update(state=state, phase="paused", message=safe_message(code), errorCode=code, speed=0,
                    nextAttempt=self.clock() + CHECK_SECONDS)
        self.detach(item)
        if code in ("auth", "authorization", "network"):
            self.target_info(item["dataset"])["pauseUntil"] = self.clock() + CHECK_SECONDS
            self.store.save_settings()
        self.store.save("items", item)

    async def authorization(self, item):
        now = self.clock()
        cached = self.grants.get(item["id"])
        owner = self.audit.client.user
        if cached and cached["owner"] == owner and cached["until"] > now + 30:
            return cached["credentials"]
        binding = item.get("object") if item.get("createAttempted") or item.get("uploadId") else None
        credentials = await self.remote.signature(item["dataset"], binding=binding) if binding else await self.remote.signature(item["dataset"])
        until = now + 900
        token = credentials.get("sessionToken") or ""
        try:
            part = token.split('.')[1]
            claims = json.loads(base64.urlsafe_b64decode(part + '=' * (-len(part) % 4)))
            if isinstance(claims.get("exp"), (int, float)):
                until = min(float(claims["exp"]), now + 3600)
        except (ValueError, IndexError, TypeError):
            pass
        self.grants[item["id"]] = {"owner": owner, "until": until, "credentials": credentials}
        return credentials

    def detach(self, item):
        if item.get("batchId"):
            item.setdefault("batchHistory", []).append(item["batchId"])
        item["batchId"] = ""

    async def fail_item(self, item, code):
        self.detach(item)
        item["retries"] = item.get("retries", 0) + 1
        item.update(errorCode=code, message=safe_message(code), speed=0,
            cleanupNeeded=bool(item.get("uploadId") or item.get("createAttempted")),
            nextAttempt=self.clock() + RETRY_DELAYS[min(item["retries"] - 1, 2)])
        if item["cleanupNeeded"]:
            item.update(state="cleanup", phase="cleanup")
        elif item["retries"] > len(RETRY_DELAYS):
            item.update(state="attention", phase="review", message="本条已重试3次仍失败，请检查后单条重试。")
        else:
            item.update(state="retry_wait", phase="queued")
        self.store.save("items", item)

    async def submit_batch(self, batch, items):
        # Fresh pre-submit reads, after uploads: a human import may have started meanwhile.
        jobs = await self.remote.jobs(batch["dataset"])
        if any(str(j["status"]) in ("1", "2") for j in jobs):
            return
        inventory = await self.remote.videos(batch["dataset"])
        if not self.store.settings["enabled"] or self.stopping:
            return
        retained = []
        for item in items:
            # An external import of the same named file also suppresses our pending submission.
            if not await self.existing_video(item, inventory):
                retained.append(item)
        if not retained:
            batch.update(state="complete", message="视频已由其他导入确认存在，未重复启动。")
            self.store.save("batches", batch)
            return
        keys = [i["object"]["key"] for i in retained]
        if sum(i["size"] for i in retained) >= MAX_BATCH_BYTES:
            self.block_batch(batch, "import_identity")
            return
        batch.update(state="submitting", items=[i["id"] for i in retained], keys=keys,
            beforeJobs=[str(j["id"]) for j in jobs], beforeVideos=[str(v["id"]) for v in inventory],
            submitAt=now_iso(), message="已保存提交前快照，正在提交增量导入。", blocksImport=True)
        self.store.save("batches", batch)
        for item in retained:
            item.update(state="importing", phase="submitting", message="正在提交本批导入。")
            self.store.save("items", item)
        try:
            identity = await self.remote.submit(batch["dataset"], keys)
        except AuditError as error:
            # All uncertain responses go through snapshot+manifest reconciliation.
            batch.update(state="uncertain", message=safe_message("import_uncertain"), errorCode=error.code)
            self.store.save("batches", batch)
            for item in retained:
                self.attention(item, "import_uncertain")
            self.target_info(batch["dataset"]).update(blocked=True, message=safe_message("import_uncertain"))
            return
        self.bind_import(batch, identity)

    def bind_import(self, batch, identity):
        batch.update(state="importing", jobId=identity, blocksImport=False, message="等待平台导入完成。", errorCode="")
        self.store.save("batches", batch)
        self.target_info(batch["dataset"]).update(activeImport=identity, blocked=False, message="等待平台导入完成。")
        for identity in batch["items"]:
            item = self.store.get("items", identity)
            item.update(state="importing", phase="importing", jobId=batch["jobId"], message="平台正在导入；原视频保留。")
            self.store.save("items", item)

    async def reconcile_submit(self, batch, jobs):
        if "beforeJobs" not in batch or not batch.get("keys"):
            self.block_batch(batch, "import_uncertain")
            return
        candidates = [j for j in jobs if str(j["id"]) not in batch["beforeJobs"]
            and source_keys(j) == frozenset(batch["keys"])]
        if len(candidates) == 1:
            self.bind_import(batch, str(candidates[0]["id"]))
        else:
            # No candidate is not proof that the server rejected the write.
            batch.update(state="uncertain", blocksImport=True, message=safe_message("import_uncertain"))
            self.store.save("batches", batch)
            for identity in batch["items"]:
                item = self.store.get("items", identity)
                if item and item["state"] != "complete":
                    self.attention(item, "import_uncertain")
            self.target_info(batch["dataset"]).update(blocked=True, message=safe_message("import_uncertain"))

    def block_batch(self, batch, code):
        batch.update(state="attention", blocksImport=True, message=safe_message(code), errorCode=code)
        self.store.save("batches", batch)
        self.target_info(batch["dataset"]).update(blocked=True, message=safe_message(code))
        for identity in batch["items"]:
            item = self.store.get("items", identity)
            if item and item["state"] != "complete":
                self.attention(item, code)

    async def verify_batch(self, batch, job):
        batch.update(state="verifying", message="导入已结束，正在逐文件核验结果。")
        self.store.save("batches", batch)
        inventory = await self.remote.videos(batch["dataset"])
        evidence = await self.remote.evidence(batch["dataset"], job)
        before = set(batch.get("beforeVideos", []))
        complete = 0
        for identity in batch["items"]:
            item = self.store.get("items", identity)
            item["jobId"] = batch["jobId"]
            if item["state"] == "complete":
                complete += 1
                continue
            full, stem = name_key(item["name"]), name_key(Path(item["name"]).stem)
            matches = [v for v in inventory if str(v["id"]) not in before and video_names(v) & {full, stem}]
            proofs = evidence["evidence"].get((full, "full"), []) + evidence["evidence"].get((stem, "stem"), [])
            good_log = any(p["status"] == "uploaded" and not p.get("problemNs") for p in proofs)
            # A successful job plus its exact manifest and a new uniquely named video is sufficient even if logs expire.
            good_video = len(matches) == 1 and (str(job["status"]) == "3" or bool(proofs))
            if len(matches) > 1:
                self.attention(item, "verification", "本次导入新增多个同名结果，保留待核对。")
            elif good_video or (good_log and str(job["status"]) == "3" and not evidence.get("reason")):
                item.update(state="complete", phase="complete", message="导入结果已逐文件核验，原视频保留。",
                    completedAt=now_iso(), errorCode="", remoteVideoId=str(matches[0]["id"]) if matches else "")
                self.store.save("items", item)
                complete += 1
            elif str(job["status"]) == "4" and not matches and not evidence.get("reason") and proofs and any(p.get("fileProblem") for p in proofs):
                self.detach(item)
                item["retries"] = item.get("retries", 0) + 1
                item.update(state="attention" if item["retries"] > 3 else "retry_wait", phase="queued",
                    nextAttempt=self.clock() + RETRY_DELAYS[min(item["retries"] - 1, 2)], errorCode="import_failed",
                    message=safe_message("import_failed"), reuseObject=True)
                self.store.save("items", item)
            else:
                self.attention(item, "verification")
        batch.update(state="complete", blocksImport=False, completedAt=now_iso(),
            message=f"本批核验结束：{complete}/{len(batch['items'])} 个成功，其他项单独处理。")
        self.store.save("batches", batch)
        if complete:
            self.store.settings.setdefault("validated", {})[batch["dataset"]] = True
        self.store.save_settings()

    async def shutdown(self):
        self.stopping = True
        if self.scan_task and not self.scan_task.done():
            self.scan_task.cancel()
            await asyncio.gather(self.scan_task, return_exceptions=True)
        if self.loop_task and not self.loop_task.done():
            self.loop_task.cancel()
            await asyncio.gather(self.loop_task, return_exceptions=True)
        tasks = list(self.running.values())
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self.grants.clear()
