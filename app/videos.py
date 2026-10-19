"""Manual video review, versioned recapture, verified publication and bound cleanup."""
import asyncio
import errno
import hashlib
import json
import math
import os
import re
import shutil
import stat
import sys
import tempfile
import time
from contextlib import ExitStack, nullcontext
from datetime import timedelta
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

import httpx

from .domain import DEFAULT_TARGET, approval_issue, format_time, gather_payload, now_iso, parse_time
from .jobs import JobManager
from .media import (DownloadFailure, download_url, file_digest,
                    prepare_directory, valid_video_header, video_filename)
from .remote import RemoteClient, RemoteError
from .source import page_records
from .video_filters import filter_records, record_status

RETRY_DELAYS = (2, 5, 10, 20, 30, 60, 60, 60)
TERMINAL = {"withdrawn", "cloud_deleted"}


def external_video_directory() -> Path:
    # Do not touch removable-volume metadata during module import. Some filesystems
    # block even stat/is_mount while recovering; startup must keep serving locally.
    return Path("/Volumes/out/视频")


EXTERNAL_VIDEO_DIR = external_video_directory()


class Conflict(ValueError):
    pass


class VideoStore:
    def __init__(self, job_store):
        self.db = job_store.db
        self.db.execute("CREATE TABLE IF NOT EXISTS simple_videos (id TEXT PRIMARY KEY, payload TEXT NOT NULL)")
        self.db.execute("CREATE TABLE IF NOT EXISTS simple_settings (id INTEGER PRIMARY KEY, directory TEXT NOT NULL)")
        self.db.commit()
        for row in self.list():
            for version in row["versions"]:
                if version["state"] == "submitting":
                    version["state"] = "uncertain"
                if version["state"] == "downloading":
                    version["state"] = "ready" if version.get("resultId") else "waiting"
            self.save(row)

    def save(self, row):
        row["updatedAt"] = now_iso()
        self.db.execute("INSERT INTO simple_videos VALUES (?, ?) ON CONFLICT(id) DO UPDATE SET payload=excluded.payload",
                        (row["id"], json.dumps(row, ensure_ascii=False)))
        self.db.commit()

    def get(self, identity):
        value = self.db.execute("SELECT payload FROM simple_videos WHERE id=?", (identity,)).fetchone()
        return json.loads(value[0]) if value else None

    def list(self, include_deleted=False):
        rows = [json.loads(row[0]) for row in self.db.execute("SELECT payload FROM simple_videos ORDER BY rowid DESC")]
        return rows if include_deleted else [row for row in rows if not row.get("deletedAt")]

    def directory(self):
        value = self.db.execute("SELECT directory FROM simple_settings WHERE id=1").fetchone()
        return value[0] if value else ""

    def set_directory(self, value):
        self.db.execute("INSERT INTO simple_settings VALUES (1, ?) ON CONFLICT(id) DO UPDATE SET directory=excluded.directory", (value,))
        self.db.commit()


class VideoManager:
    def __init__(self, jobs: JobManager, target: RemoteClient, data_dir: Path,
                 fixed_directory: Path | None = None, require_mounted_external: bool = False,
                 staging_directory: Path | None = None):
        self.jobs, self.target = jobs, target
        self.store = VideoStore(jobs.store)
        self.fixed_directory = Path(os.path.normpath(os.fspath(fixed_directory))) if fixed_directory else None
        self.require_mounted_external = require_mounted_external
        # Direct-transfer mode intentionally avoids probing a removable volume at startup.
        # The user requested that completed local files be copied straight to the chosen path.
        if not self.require_mounted_external:
            self.rebase_mounted_external_paths()
        if staging_directory:
            self.work = Path(staging_directory).resolve()
            self.legacy_work = self.fixed_directory / ".download-cache" if self.fixed_directory else None
        else:
            self.legacy_work = data_dir / "work" if self.fixed_directory else None
            self.work = (self.fixed_directory / ".download-cache") if self.fixed_directory else data_dir / "work"
        if not self.fixed_directory:
            self.work.mkdir(parents=True, exist_ok=True)
        elif not self.store.directory():
            self.store.set_directory(str(self.fixed_directory))
        self.storage_ready = not bool(self.fixed_directory) or self.require_mounted_external
        self.storage_error = ""
        self.transfer_timeout = 180
        self.transfer_flush_timeout = 600
        self.target_config = dict(DEFAULT_TARGET)
        self.check_lock = asyncio.Lock()
        self.row_locks: dict[str, asyncio.Lock] = {}
        self.download_slots = asyncio.Semaphore(5)
        self.publish_slots = asyncio.Semaphore(5)
        self.running: dict[str, asyncio.Task] = {}
        self.loop_task: asyncio.Task | None = None
        self.storage_check_task: asyncio.Task | None = None
        self.wake = asyncio.Event()
        self.last_check = ""
        self.check_error = ""

    def rebase_mounted_external_paths(self):
        if not self.fixed_directory or not self.require_mounted_external:
            return
        old_value = self.store.directory()
        new_root = self.fixed_directory
        known = {Path("/Volumes/out 1/视频"), Path("/Volumes/out/视频")}
        if not old_value or old_value == str(new_root):
            return
        old_root = Path(old_value)
        if old_root not in known or new_root not in known or old_root.exists():
            return
        if not new_root.is_dir() or not os.path.ismount(str(new_root.parent)):
            return

        rows = self.store.list()
        saved_files = [
            (version.get("filePath"), version.get("totalBytes"))
            for row in rows for version in row.get("versions", [])
            if version.get("state") == "saved" and version.get("filePath")
            and version["filePath"].startswith(str(old_root) + os.sep)
        ]
        if not saved_files or any(
                not (mapped := Path(str(new_root) + path[len(str(old_root)):])).is_file()
                or (size is not None and mapped.stat().st_size != size)
                for path, size in saved_files):
            return

        old_prefix = str(old_root) + os.sep
        for row in rows:
            changed = False
            for version in row.get("versions", []):
                for key in ("filePath", "outputDirectory"):
                    value = version.get(key)
                    if isinstance(value, str) and (value == str(old_root) or value.startswith(old_prefix)):
                        mapped = str(new_root) + value[len(str(old_root)):]
                        if key != "filePath" or (Path(mapped).is_file() and
                                                  (version.get("totalBytes") is None or
                                                   Path(mapped).stat().st_size == version["totalBytes"])):
                            version[key] = mapped
                            changed = True
                publishing = version.get("publishing")
                value = publishing.get("path") if isinstance(publishing, dict) else None
                if isinstance(value, str) and value.startswith(old_prefix):
                    publishing["path"] = str(new_root) + value[len(str(old_root)):]
                    changed = True
            if changed:
                self.store.save(row)
        self.store.set_directory(str(new_root))

    def row_lock(self, identity):
        return self.row_locks.setdefault(identity, asyncio.Lock())

    async def start(self):
        if self.fixed_directory:
            if self.require_mounted_external:
                # Do not probe the mount or free space: try the real copy directly.
                # A failed write leaves the staged local source intact for retry.
                self.storage_ready = True
                self.storage_error = ""
            else:
                try:
                    self.prepare_fixed_storage()
                    self.migrate_legacy_partials()
                    self.storage_ready = True
                    self.storage_error = ""
                except (OSError, ValueError):
                    self.storage_ready = False
                    self.storage_error = "保存目录不可用，下载已暂停。"
        self.sync_jobs()
        self.loop_task = asyncio.create_task(self.loop())

    async def shutdown(self):
        if self.loop_task:
            self.loop_task.cancel()
            await asyncio.gather(self.loop_task, return_exceptions=True)
        if self.storage_check_task and not self.storage_check_task.done():
            self.storage_check_task.cancel()
            await asyncio.gather(self.storage_check_task, return_exceptions=True)
        for task in self.running.values():
            task.cancel()
        if self.running:
            await asyncio.gather(*self.running.values(), return_exceptions=True)

    def state(self, query=None):
        self.sync_jobs()
        rows = self.store.list()
        found = filter_records(rows, query)
        return {"videos": found, "directory": self.store.directory(), "totalCount": len(rows),
                "filteredCount": len(found), "byJob": {job_id: sum(r["jobId"] == job_id for r in rows)
                    for job_id in {r["jobId"] for r in rows}},
                "counts": {kind: sum(record_status(row) == kind for row in rows)
                           for kind in ("ready", "waiting", "review", "download", "saved", "deleting")},
                "lastCheck": self.last_check, "checkError": self.storage_error or self.check_error}

    def configure_directory(self, value):
        if self.fixed_directory:
            directory = prepare_directory(value)
            self.store.set_directory(directory)
            return self.state()
        directory = prepare_directory(value)
        self.store.set_directory(directory)
        return self.state()

    def prepare_fixed_storage(self):
        if self.require_mounted_external and not os.path.ismount(str(self.fixed_directory.parent)):
            raise OSError("外置盘未挂载。")
        self.fixed_directory.mkdir(parents=True, exist_ok=True)
        if self.require_mounted_external and self.fixed_directory.stat().st_dev != self.fixed_directory.parent.stat().st_dev:
            raise OSError("保存目录不在外置盘上。")
        with tempfile.TemporaryFile(dir=self.fixed_directory) as probe:
            probe.write(b"ok")
            probe.flush()
            os.fsync(probe.fileno())
        self.work.mkdir(parents=True, exist_ok=True)

    def migrate_legacy_partials(self):
        """Move resumable parts onto the external disk after a safe app restart."""
        if not self.legacy_work or not self.legacy_work.exists() or self.legacy_work == self.work:
            return
        for folder in self.legacy_work.iterdir():
            if folder.is_symlink() or not folder.is_dir() or not re.fullmatch(r"[a-f0-9]{24}", folder.name):
                raise ValueError("旧断点目录含未知内容，已停止迁移并保留原文件。")
            row = self.store.get(folder.name)
            known = {f"v{v['revision']}.part" for v in (row or {}).get("versions", [])}
            for source in folder.iterdir():
                if source.is_symlink() or not source.is_file() or source.name not in known:
                    raise ValueError("旧断点目录含未登记文件，已停止迁移并保留原文件。")
                destination_folder = self.work / folder.name
                destination_folder.mkdir(parents=True, exist_ok=True)
                destination = destination_folder / source.name
                if destination.exists():
                    if (destination.stat().st_size != source.stat().st_size
                            or file_digest(destination) != file_digest(source)):
                        raise ValueError("外置盘已有不同断点，原断点已保留供核对。")
                    source.unlink()
                    continue
                temporary = destination.with_name(destination.name + ".migrating")
                if temporary.exists():
                    if temporary.is_symlink() or not temporary.is_file():
                        raise ValueError("迁移临时文件异常，原断点已保留。")
                    temporary.unlink()
                source_digest = file_digest(source)
                with source.open("rb") as incoming, temporary.open("xb") as outgoing:
                    shutil.copyfileobj(incoming, outgoing, 4 * 1024 * 1024)
                    outgoing.flush()
                    os.fsync(outgoing.fileno())
                if (temporary.stat().st_size != source.stat().st_size
                        or file_digest(temporary) != source_digest):
                    temporary.unlink(missing_ok=True)
                    raise ValueError("断点迁移校验失败，原文件已保留。")
                os.replace(temporary, destination)
                source.unlink()
            folder.rmdir()
        self.legacy_work.rmdir()

    def sync_jobs(self):
        for job in self.jobs.store.list():
            for item in job["items"]:
                if item["status"] in ("duplicate", "invalid", "removed", "failed"):
                    continue
                identity = hashlib.sha256((job["id"] + "|" + item["key"]).encode()).hexdigest()[:24]
                row = self.store.get(identity)
                if row and row.get("deletedAt"):
                    continue
                if not row:
                    row = {"id": identity, "jobId": job["id"], "taskKey": item["key"],
                           "recordId": item["recordId"], "plate": item["plate"], "name": item["videoName"],
                           "qualityStart": item["checkStartTime"], "qualityEnd": item["checkCompleteTime"],
                           "photoCount": item["photoCount"], "inspectionDurationSeconds": item["inspectionDurationSeconds"],
                           "admissionRule": item.get("admissionRule") or job.get("admissionRule"),
                           "revision": 0, "acceptedRevision": None, "activeRevision": 0,
                           "versions": [{"revision": 0, "startTime": item["startTime"], "endTime": item["endTime"],
                                         "durationSeconds": 3600, "name": item["videoName"], "taskId": "", "resultId": "",
                                         "state": "pending", "createdAt": now_iso()}],
                           "requests": {}, "createdAt": now_iso(), "error": ""}
                original = row["versions"][0]
                changed = False
                if item.get("remoteId") and not original.get("taskId") and item.get("bindingVerified"):
                    original["taskId"] = str(item["remoteId"])
                    changed = True
                if original["state"] in ("pending", "submitting", "uncertain", "submitted", "waiting"):
                    next_state = "uncertain" if item["status"] == "uncertain" else "submitted" if item["status"] == "submitted" else item["status"]
                    if next_state != original["state"]:
                        original["state"] = next_state
                        changed = True
                # Apply explicit batch download consent once, only after a new task
                # has a verified binding. Replays never re-enable cancelled downloads
                # or replace a user's correction. Legacy batches have no such intent.
                if (job.get("downloadAfterCollection") and job.get("downloadDirectory")
                        and not row.get("batchDownloadApplied")
                        and item["status"] == "submitted" and item.get("bindingVerified")
                        and original.get("taskId") and original["state"] in ("submitted", "waiting", "ready")):
                    row["batchDownloadApplied"] = True
                    if row["activeRevision"] == 0 and row["revision"] == 0 and not row["requests"]:
                        original.update(downloadRequested=True, outputDirectory=job["downloadDirectory"])
                        row["requests"]["batch-download:" + job["id"]] = {"kind": "confirm", "version": 0}
                    changed = True
                if changed or not self.store.get(identity):
                    self.store.save(row)

    async def loop(self):
        while True:
            self.wake.clear()
            for row in self.store.list():
                active = self.version(row, row.get("activeRevision"))
                if active and active.get("downloadRequested") and active.get("publishing"):
                    self.schedule(row)  # Completed local files do not require a platform connection.
            if self.fixed_directory and not self.storage_ready and not self.require_mounted_external:
                if not self.storage_check_task or self.storage_check_task.done():
                    self.storage_error = self.storage_error or "外置盘不可用；视频会先下载到本机暂存。"
                    self.storage_check_task = asyncio.create_task(self.refresh_storage())
            if self.target.connected:
                try:
                    await self.check_once()
                    self.check_error = ""
                except (RemoteError, ValueError, OSError):
                    self.check_error = "平台检查暂时失败；已有任务和视频记录已保留。"
                except Exception:
                    self.check_error = "检查视频时出现异常，请稍后重试。"
            try:
                await asyncio.wait_for(self.wake.wait(), timeout=30)
            except asyncio.TimeoutError:
                pass

    async def refresh_storage(self):
        try:
            await asyncio.to_thread(self.prepare_fixed_storage)
            await asyncio.to_thread(self.migrate_legacy_partials)
            self.storage_ready = True
            self.storage_error = ""
        except (OSError, ValueError):
            self.storage_ready = False
            self.storage_error = "外置盘不可用；视频会先下载到本机暂存，连接恢复后自动保存。"
        finally:
            self.wake.set()

    async def results(self, parent_id):
        seen, output = set(), []
        for page in range(1, 101):
            data = await self.target.request("/data-acquisition/task-result", method="POST", body={
                "taskId": parent_id, "cameraId": self.target_config["cameraId"],
                "currPage": page, "pageSize": 100,
                "orderList": [{"asc": False, "column": "gmt_modify"}],
            })
            rows, total = page_records(data)
            if total > 10000 or any(not r.get("id") or str(r["id"]) in seen
                                    or str(r.get("taskId")) != parent_id for r in rows):
                raise ValueError("视频结果身份或分页异常，请在采集平台核对。")
            for result in rows:
                seen.add(str(result["id"]))
                output.append(result)
            if len(seen) >= total:
                return output
            if not rows:
                raise ValueError("视频结果分页不完整，请稍后重试。")
        raise ValueError("视频结果超过核对范围。")

    @staticmethod
    def matches(parent, version, camera_id):
        return (str(parent.get("cameraId", camera_id)) == camera_id
                and str(parent.get("taskType", "VIDEO")).upper() == "VIDEO"
                and not parent.get("isRepeat")
                and format_time(parent.get("startTime")) == version["startTime"]
                and format_time(parent.get("endTime")) == version["endTime"]
                and str(parent.get("videoName") or parent.get("taskName") or "") == version["name"])

    async def check_once(self):
        if not self.target.connected:
            raise RemoteError("请先连接采集平台。", "401")
        async with self.check_lock:
            for job in self.jobs.store.list():
                if job["id"] not in self.jobs.running and any(item["status"] == "uncertain" for item in job["items"]):
                    try:
                        await self.jobs.reconcile(job["id"])
                    except (RemoteError, ValueError):
                        pass
            self.sync_jobs()
            parents = await self.jobs.remote_parents(self.target_config)
            for item in self.store.list():
                lock = self.row_lock(item["id"])
                if item.get("deletion", {}).get("state") in ("pending", "error"):
                    self.schedule(item)
                    continue
                if lock.locked():
                    continue  # Long downloads must not block other videos' collection checks.
                async with lock:
                    row = self.store.get(item["id"])
                    if not row or row.get("deletedAt"):
                        continue
                    try:
                        await self._check_row(row, parents)
                        row["error"] = "" if not row.get("cleanupPending") else row.get("error", "")
                    except (RemoteError, ValueError, httpx.HTTPError):
                        row["error"] = "平台暂时无法核对本条视频；其他视频继续处理。"
                    self.store.save(row)
                    self.schedule(row)
            self.last_check = now_iso()
        return self.state()

    async def _check_row(self, row, parents):
        for version in row["versions"]:
            if version["state"] in TERMINAL:
                continue
            if not version.get("taskId") and version.get("beforeIds") and version.get("writeAttempted"):
                candidates = [parent for key, parent in parents.items()
                              if key not in version["beforeIds"]
                              and self.matches(parent, version, self.target_config["cameraId"])]
                if len(candidates) == 1:
                    version["taskId"] = str(candidates[0]["id"])
                    version["state"] = "submitted"
                elif len(candidates) > 1:
                    self.review_issue(version, "submission_ambiguous", "提交后出现多个相同任务，须人工核对确切任务ID。")
            parent_id = version.get("taskId")
            if not parent_id:
                if version.get("writeAttempted"):
                    version.update(state="uncertain", message="提交已发送，但尚未确认新任务ID；先核对平台，不能重复提交或删除。")
                continue
            parent = parents.get(parent_id)
            if not parent or not self.matches(parent, version, self.target_config["cameraId"]):
                if not version.get("cleanup", {}).get("started"):
                    actual = None if not parent else {key: parent.get(key) for key in
                        ("id", "cameraId", "videoName", "startTime", "endTime", "taskStatus")}
                    self.review_issue(version, "task_missing" if not parent else "task_identity_changed",
                                      "绑定的平台任务暂时不存在。" if not parent else "平台任务的名称、相机或采集时段与已提交记录不同。",
                                      actual=actual)
                continue
            version["platformStatus"] = str(parent.get("taskStatus") or parent.get("status") or "")
            if version["platformStatus"].upper() in ("ENDED", "FAIL", "ABNORMAL_END"):
                self.review_issue(version, "platform_failed", "平台任务失败或已终止，需要核对或重新采集。")
                continue
            if version["platformStatus"].upper() != "FIXED":
                if version["state"] not in ("saved", "downloading", "publishing", "failed", "attention"):
                    version["state"] = "waiting"
                continue
            results = await self.results(parent_id)
            ready = [result for result in results if str(result.get("status", "")).upper() == "FINISH" and result.get("url")]
            if len(results) == 1 and len(ready) == 1:
                result_id = str(ready[0]["id"])
                if version.get("resultId") and version["resultId"] != result_id:
                    self.review_issue(version, "result_identity_changed", "视频结果ID发生变化，已保留原绑定，不能自动认领新结果。",
                                      actualResultId=result_id)
                else:
                    version["resultId"] = result_id
                    if version["state"] not in ("saved", "downloading", "publishing", "failed"):
                        version["state"] = "ready"
                        version.pop("message", None)
                        version.pop("review", None)
            elif len(results) > 1:
                self.review_issue(version, "multiple_results", "同一任务出现多个结果，需要人工核对。",
                                  resultIds=[str(result["id"]) for result in results])
            elif version.get("resultId"):
                self.review_issue(version, "result_missing", "平台任务已完成，但此前绑定的视频结果目前不存在或不能播放。")
            elif version["state"] not in ("saved", "downloading", "publishing", "failed", "attention"):
                version["state"] = "waiting"

    @staticmethod
    def review_issue(version, code, message, **details):
        version.update(state="attention", message=message,
                       review={"code": code, "message": message, "checkedAt": now_iso(), **details})

    async def check_video(self, identity):
        if not self.target.connected:
            raise RemoteError("请先连接采集平台。", "401")
        if self.row_lock(identity).locked():
            raise Conflict("本条正在下载或清理，其他视频仍会继续核对。")
        async with self.check_lock:
            parents = await self.jobs.remote_parents(self.target_config)
            async with self.row_lock(identity):
                row = self.store.get(identity)
                if not row or row.get("deletedAt"):
                    raise ValueError("视频记录不存在。")
                self.require_editable(row)
                await self._check_row(row, parents)
                row["error"] = "" if not row.get("cleanupPending") else row.get("error", "")
                self.store.save(row)
                self.schedule(row)
                return row

    def schedule(self, row):
        if row.get("deletedAt"):
            return
        identity = row["id"]
        if identity in self.running and not self.running[identity].done():
            return
        if row.get("deletion", {}).get("state") == "pending":
            if not self.storage_ready:
                return
            task = asyncio.create_task(self.clear_record_worker(identity), name="clear-record")
            self.register_task(identity, task)
            return
        if row.get("deletion", {}).get("state") == "error":
            return
        active = next((v for v in row["versions"] if v["revision"] == row["activeRevision"]), None)
        if not active:
            return
        if active.get("downloadRequested") and active["state"] in ("ready", "publishing") and self.store.directory():
            task = asyncio.create_task(self.publish(identity, active["revision"]), name="publish-video")
        elif row.get("cleanupPending") and self.target.connected and self.storage_ready:
            task = asyncio.create_task(self.finish_cleanup(identity))
        else:
            return
        self.register_task(identity, task)

    def register_task(self, identity, task):
        self.running[identity] = task
        def finished(done, key=identity):
            if self.running.get(key) is done:
                self.running.pop(key, None)
        task.add_done_callback(finished)

    async def bound_result(self, version):
        if not version.get("taskId") or not version.get("resultId"):
            raise ValueError("视频尚未完成，不能播放或下载。")
        rows = await self.results(version["taskId"])
        if len(rows) != 1 or str(rows[0].get("id")) != version["resultId"] or str(rows[0].get("status", "")).upper() != "FINISH" or not rows[0].get("url"):
            raise ValueError("绑定的视频结果已改变，请先核对平台。")
        return rows[0]

    def version(self, row, revision):
        return next((v for v in row["versions"] if v["revision"] == revision), None)

    @staticmethod
    def require_editable(row):
        if row.get("deletion", {}).get("state") in ("pending", "error"):
            raise Conflict("本条正在清理；请先完成或停止继续清理，不能覆盖当前操作。")

    async def correct(self, identity, payload):
        async with self.row_lock(identity):
            row = self.store.get(identity)
            if not row or row.get("deletedAt"):
                raise ValueError("视频记录不存在。")
            request_id = payload["requestId"]
            signature = {key: payload[key] for key in ("startTime", "durationSeconds", "expectedRevision")}
            if payload.get("repair"):
                signature["repair"] = True
            existing = row["requests"].get(request_id)
            if existing:
                if existing["kind"] == "correction" and existing["signature"] == signature:
                    return row
                raise Conflict("请求ID已用于其他操作。")
            if payload["expectedRevision"] != row["revision"]:
                raise Conflict("记录已变化，请刷新后重新核对时间。")
            self.require_editable(row)
            current = self.version(row, row["activeRevision"])
            if not current or current["state"] not in ("ready", "saved", "waiting", "submitted", "attention", "failed"):
                raise ValueError("当前版本正在提交、下载或保存；请完成核对后再更正。")
            if current.get("writeAttempted") and not current.get("taskId"):
                raise ValueError("上次提交的任务ID尚未确认，请先核对平台，避免重复提交。")
            if current.get("downloadRequested") and current["state"] != "failed":
                raise ValueError("当前版本已确认并正在等待保存，完成保存后可以再次更正。")
            start = parse_time(payload["startTime"])
            if not start or format_time(start) != format_time(payload["startTime"]):
                raise ValueError("采集开始时间无效。")
            end = start + timedelta(seconds=payload["durationSeconds"])
            start_text, end_text = format_time(start), format_time(end)
            if start_text == current["startTime"] and end_text == current["endTime"] and not payload.get("repair"):
                raise ValueError("采集区间未变化，可直接确认当前视频。")
            source = {"plate": row["plate"], "photoCount": row["photoCount"],
                      "inspectionDurationSeconds": row["inspectionDurationSeconds"],
                      "admissionRule": row.get("admissionRule")}
            if issue := approval_issue(source):
                raise ValueError("质检实绩未通过采集准入：" + issue)
            async with self.jobs.lock:
                await self.jobs.camera(self.target_config)
                parents = await self.jobs.remote_parents(self.target_config)
                if payload.get("repair"):
                    await self._check_row(row, parents)
                    self.store.save(row)
                    if current["state"] == "ready":
                        return row  # A verified result recovered; no new capture is necessary.
                    if current.get("review", {}).get("code") not in ("task_missing", "task_identity_changed", "result_missing", "platform_failed"):
                        raise ValueError("该异常需要先人工核对任务或结果身份，不能直接重新采集。")
                version = {"revision": row["revision"] + 1, "startTime": start_text, "endTime": end_text,
                           "durationSeconds": payload["durationSeconds"], "name": row["name"],
                           "taskId": "", "resultId": "", "state": "submitting", "createdAt": now_iso(),
                           "beforeIds": sorted(parents), "writeAttempted": False,
                           "previousActiveRevision": row["activeRevision"]}
                if any(self.matches(parent, version, self.target_config["cameraId"])
                       and not (payload.get("repair") and str(parent["id"]) == current.get("taskId"))
                       for parent in parents.values()):
                    raise ValueError("采集平台已有相同名称和时段的视频任务，请先核对，避免重复采集。")
                row["revision"] += 1
                row["activeRevision"] = version["revision"]
                row["versions"].append(version)
                row["requests"][request_id] = {"kind": "correction", "signature": signature,
                                                "version": version["revision"]}
                self.store.save(row)
                task = {**source, "videoName": row["name"], "taskName": row["name"],
                        "startTime": start_text, "endTime": end_text}
                version["writeAttempted"] = True
                self.store.save(row)
                try:
                    await self.target.request("/data-acquisition/batchSave", method="POST",
                                              body=[gather_payload(task, self.target_config)], write=True)
                except RemoteError:
                    version["message"] = "提交响应不明，正在按提交前任务ID核对；不会自动重发。"
                version["state"] = "uncertain"
                self.store.save(row)
                try:
                    after = await self.jobs.remote_parents(self.target_config)
                    candidates = [parent for key, parent in after.items() if key not in version["beforeIds"]
                                  and self.matches(parent, version, self.target_config["cameraId"])]
                    if len(candidates) == 1:
                        version["taskId"] = str(candidates[0]["id"])
                        version["state"] = "submitted"
                        version.pop("message", None)
                    elif len(candidates) > 1:
                        version["state"] = "attention"
                        version["message"] = "多个新任务匹配本次更正，请人工核对。"
                except RemoteError:
                    pass
                self.store.save(row)
            self.wake.set()
            return row

    async def recapture_unavailable(self, identity, payload):
        row = self.store.get(identity)
        version = self.version(row, payload["version"]) if row and not row.get("deletedAt") else None
        if not version or (row["activeRevision"] != version["revision"] and payload["requestId"] not in row["requests"]):
            raise Conflict("当前版本已变化，请刷新后重试。")
        return await self.correct(identity, {"startTime": version["startTime"],
            "durationSeconds": version["durationSeconds"], "expectedRevision": payload["expectedRevision"],
            "requestId": payload["requestId"], "repair": True})

    async def withdraw(self, identity, payload):
        async with self.row_lock(identity):
            row = self.store.get(identity)
            if not row or row.get("deletedAt"):
                raise ValueError("视频记录不存在。")
            request_id = payload["requestId"]
            existing = row["requests"].get(request_id)
            if existing:
                if existing["kind"] == "withdraw" and existing["version"] == payload["version"]:
                    return row
                raise Conflict("请求ID已用于其他操作。")
            if payload["expectedRevision"] != row["revision"]:
                raise Conflict("记录已变化，请刷新后重试。")
            self.require_editable(row)
            version = self.version(row, payload["version"])
            if not version or payload["version"] == 0 or row["activeRevision"] != payload["version"]:
                raise ValueError("只能撤回当前更正版本。")
            if row["acceptedRevision"] == payload["version"] or version["state"] in ("saved", "downloading", "publishing"):
                raise ValueError("新版已确认或正在下载，不能撤回；可以再次更正。")
            if version.get("writeAttempted") and not version.get("taskId"):
                raise ValueError("提交结果尚未确定，请先核对平台任务，暂不能安全撤回。")
            if version.get("taskId"):
                await self.cleanup_cloud(row, version, protected={v.get("taskId") for v in row["versions"] if v is not version})
            version["state"] = "withdrawn"
            previous = self.version(row, version.get("previousActiveRevision"))
            if not previous or previous["state"] in TERMINAL:
                previous = next((item for item in reversed(row["versions"])
                                 if item["revision"] < version["revision"] and item["state"] not in TERMINAL), None)
            if not previous:
                raise ValueError("没有可恢复的上一版本，请人工核对。")
            row["activeRevision"] = previous["revision"]
            row["revision"] += 1
            row["requests"][request_id] = {"kind": "withdraw", "version": payload["version"]}
            self.store.save(row)
            return row

    async def confirm(self, identity, payload):
        async with self.row_lock(identity):
            row = self.store.get(identity)
            if not row or row.get("deletedAt"):
                raise ValueError("视频记录不存在。")
            request_id = payload["requestId"]
            existing = row["requests"].get(request_id)
            if existing:
                if existing["kind"] == "confirm" and existing["version"] == payload["version"]:
                    return row
                raise Conflict("请求ID已用于其他操作。")
            if payload["expectedRevision"] != row["revision"]:
                raise Conflict("记录已变化，请刷新后再确认。")
            self.require_editable(row)
            if not self.store.directory():
                raise ValueError("请先设置最终视频保存文件夹。")
            version = self.version(row, payload["version"])
            allowed = ("ready", "waiting", "submitted") if payload.get("waitForCompletion") else ("ready",)
            if not version or row["activeRevision"] != payload["version"] or version["state"] not in allowed or not version.get("taskId"):
                raise ValueError("当前版本尚未完成或已发生变化，请刷新后核对。")
            version["downloadRequested"] = True
            version["outputDirectory"] = self.store.directory()
            row["requests"][request_id] = {"kind": "confirm", "version": payload["version"]}
            self.store.save(row)
            self.schedule(row)
            return row

    async def retry_download(self, identity):
        async with self.row_lock(identity):
            row = self.store.get(identity)
            if not row or row.get("deletedAt"):
                raise ValueError("视频记录不存在。")
            self.require_editable(row)
            version = self.version(row, row["activeRevision"])
            if not version or not version.get("downloadRequested") or version["state"] not in ("failed", "ready"):
                raise ValueError("当前视频没有可重试的下载。")
            version["state"] = "ready"
            version.pop("message", None)
            self.store.save(row)
            self.schedule(row)
            return row

    async def cancel_download(self, identity, payload):
        row = self.store.get(identity)
        if not row or row.get("deletedAt"):
            raise ValueError("视频记录不存在。")
        existing = row["requests"].get(payload["requestId"])
        if existing:
            if existing == {"kind": "cancel-download", "version": payload["version"]}:
                return row
            raise Conflict("请求ID已用于其他操作。")
        self.require_editable(row)
        if row["revision"] != payload["expectedRevision"] or row["activeRevision"] != payload["version"]:
            raise Conflict("当前版本已变化，未停止其他版本。")
        task = self.running.get(identity)
        if task and not task.done() and task.get_name() == "publish-video":
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        async with self.row_lock(identity):
            row = self.store.get(identity)
            if row["revision"] != payload["expectedRevision"] or row["activeRevision"] != payload["version"]:
                raise Conflict("当前版本已变化，进度已保留。")
            self.require_editable(row)
            version = self.version(row, payload["version"])
            version["downloadRequested"] = False
            row["requests"][payload["requestId"]] = {"kind": "cancel-download", "version": payload["version"]}
            self.store.save(row)
            return row

    async def bulk_action(self, action, payload):
        selections = payload["selections"]
        if len({v["id"] for v in selections}) != len(selections):
            raise ValueError("同一视频只能选择一次。")
        outcomes = []
        for selected in selections:
            data = {**selected, "requestId": payload["requestId"] + "-" + selected["id"]}
            try:
                if action == "confirm":
                    result = await self.confirm(selected["id"], {**data, "waitForCompletion": True})
                else:
                    current = self.store.get(selected["id"])
                    if not current or current.get("deletedAt") or current["activeRevision"] != selected["version"]:
                        raise Conflict("选中后视频版本有变化，已保留，请重新选择。")
                    result = await self.remove_record(selected["id"], {**data,
                        "deleteCloud": payload["deleteCloud"], "deleteLocalFiles": payload["deleteLocalFiles"]})
                outcomes.append({"id": selected["id"], "ok": True, "revision": result["revision"]})
            except (ValueError, RemoteError) as error:
                outcomes.append({"id": selected["id"], "ok": False,
                                 "message": str(error), "conflict": isinstance(error, Conflict)})
        return {"outcomes": outcomes}

    async def remove_record(self, identity, payload):
        """Persist an explicit deletion scope before any network/file mutation."""
        row = self.store.get(identity)
        if not row:
            raise ValueError("视频记录不存在。")
        signature = {k: payload[k] for k in ("expectedRevision", "deleteCloud", "deleteLocalFiles")}
        request_id = payload["requestId"]
        existing = row["requests"].get(request_id)
        if existing and (existing.get("kind") != "remove-record" or existing.get("signature") != signature):
            raise Conflict("请求ID已用于其他操作。")
        if not existing and (row.get("deletedAt") or row["revision"] != payload["expectedRevision"]):
            raise Conflict("记录已变化，未清除最新更正。请刷新后重新选择。")
        task = self.running.get(identity)
        if task and not task.done() and task.get_name() != "clear-record":
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        async with self.row_lock(identity):
            row = self.store.get(identity)
            existing = row["requests"].get(request_id)
            if existing:
                if existing.get("signature") != signature or existing.get("kind") != "remove-record":
                    raise Conflict("请求ID已用于其他操作。")
                if row.get("deletedAt"):
                    return row
                if row.get("deletion", {}).get("requestId") != request_id:
                    raise Conflict("此清理已被停止或替换，请刷新后重新操作。")
                if row["deletion"]["state"] == "cancelled":
                    raise Conflict("此清理已停止，请使用新的清理请求。")
            else:
                if row.get("deletedAt") or row["revision"] != payload["expectedRevision"]:
                    raise Conflict("此条刚有新的更正，已保留最新进度。")
                self.require_editable(row)
                row["revision"] += 1
                row["requests"][request_id] = {"kind": "remove-record", "signature": signature}
                row["deletion"] = {**signature, "requestId": request_id, "createdAt": now_iso()}
                for version in row["versions"]:
                    version["downloadRequested"] = False
            row["deletion"].update(state="pending", message="正在核对并清除所选范围。")
            self.store.save(row)
        self.schedule(row)
        return row

    async def cancel_clear(self, identity, payload):
        row = self.store.get(identity)
        if not row or row.get("deletedAt"):
            raise ValueError("视频记录不存在。")
        if row["revision"] != payload["expectedRevision"]:
            raise Conflict("记录已变化，请刷新后重试。")
        task = self.running.get(identity)
        if task and not task.done() and task.get_name() == "clear-record":
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        async with self.row_lock(identity):
            row = self.store.get(identity)
            if row.get("deletedAt") or row["revision"] != payload["expectedRevision"]:
                raise Conflict("清理进度已变化，请刷新后核对。")
            if row.get("deletion", {}).get("state") not in ("pending", "error"):
                raise ValueError("当前没有待停止的清理。")
            row["deletion"].update(state="cancelled", message="已停止继续清理；已删除的云端文件不能恢复。")
            row["revision"] += 1
            self.store.save(row)
            return row

    @staticmethod
    def clear_registered_file(value, digest, size):
        path = Path(value)
        if not path.is_absolute() or not digest or not size:
            raise ValueError("本地文件缺少绑定或校验凭证，已保留。")
        if path.resolve().is_relative_to((Path.home() / "Desktop" / "视频抽帧").resolve()):
            raise ValueError("桌面视频抽帧源目录受保护。")
        flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
        folder_fd = os.open(path.parent, flags | os.O_DIRECTORY)
        try:
            try:
                fd = os.open(path.name, flags, dir_fd=folder_fd)
            except FileNotFoundError:
                return
            with os.fdopen(fd, "rb") as incoming:
                before = os.fstat(incoming.fileno())
                if not stat.S_ISREG(before.st_mode) or before.st_size != size:
                    raise ValueError("本地文件类型或长度已变化，已保留。")
                checksum = hashlib.sha256()
                for block in iter(lambda: incoming.read(4 * 1024 * 1024), b""):
                    checksum.update(block)
                current = os.stat(path.name, dir_fd=folder_fd, follow_symlinks=False)
                if (checksum.hexdigest() != digest or not stat.S_ISREG(current.st_mode)
                        or (current.st_dev, current.st_ino, current.st_size, current.st_mtime_ns)
                        != (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)):
                    raise ValueError("本地文件在校验前后发生变化，已保留。")
                os.unlink(path.name, dir_fd=folder_fd)
        finally:
            os.close(folder_fd)

    async def clear_record_worker(self, identity):
        async with self.row_lock(identity):
            row = self.store.get(identity)
            if not row or row.get("deletedAt") or row.get("deletion", {}).get("state") != "pending":
                return
            deletion = row["deletion"]
            file_locks = ExitStack()
            try:
                if deletion["deleteLocalFiles"] and getattr(self, "file_leases", None):
                    paths = {v.get("filePath") or (v.get("publishing") or {}).get("path") for v in row["versions"]}
                    for value in sorted(p for p in paths if p):
                        file_locks.enter_context(self.file_leases.write(value))
                if deletion["deleteCloud"]:
                    if not self.target.connected:
                        raise ValueError("请连接采集平台后重试清理；记录和文件已保留。")
                    protected = {v.get("taskId") for other in self.store.list(include_deleted=True)
                                 if other["id"] != identity for v in other["versions"] if v.get("taskId")}
                    for version in row["versions"]:
                        if version.get("writeAttempted") and not version.get("taskId"):
                            raise ValueError("有提交尚未确认任务ID，先对账才能安全清理。")
                        if version.get("taskId") in protected:
                            raise ValueError("任务ID同时绑定其他记录，已阻止清理。")
                    for version in row["versions"]:
                        if version.get("taskId"):
                            await self.cleanup_cloud(row, version, protected, allow_incomplete=True)
                            version["state"] = "cloud_deleted"
                            self.store.save(row)
                if deletion["deleteLocalFiles"]:
                    for version in row["versions"]:
                        prepared = version.get("publishing") or {}
                        value = version.get("filePath") or prepared.get("path")
                        if value:
                            await self.file_operation(self.clear_registered_file, value,
                                version.get("sha256") or prepared.get("sha256"),
                                version.get("totalBytes") or prepared.get("bytes"))
                            version["filePath"] = ""
                            version.pop("publishing", None)
                            self.store.save(row)
                deletion["cacheBytes"] = await self.file_operation(self.clear_owned_cache, row)
                deletion.update(state="complete", completedAt=now_iso(), message="所选范围已核验清理。")
                row.update(deletedAt=now_iso(), deletionReason="user-record-clear", cleanupPending=False, error="")
                self.store.save(row)
            except asyncio.CancelledError:
                self.store.save(row)
                raise
            except (ValueError, RemoteError, OSError, httpx.HTTPError) as error:
                deletion.update(state="error", message=str(error) if isinstance(error, ValueError)
                                else "平台或磁盘未确认清理结果，已保留记录，可重试核对。")
                self.store.save(row)
            finally:
                file_locks.close()

    def clear_owned_cache(self, row):
        """Only unlink registered .part files through directory FDs; never follow a symlink."""
        if not re.fullmatch(r"[a-f0-9]{24}", row["id"]):
            raise ValueError("缓存目录身份无效。")
        flags = os.O_RDONLY | os.O_DIRECTORY | getattr(os, "O_NOFOLLOW", 0)
        root_fd = os.open(self.work, flags)
        folder_fd = None
        try:
            try:
                folder_fd = os.open(row["id"], flags, dir_fd=root_fd)
            except FileNotFoundError:
                return 0
            names = os.listdir(folder_fd)
            known = {f"v{v['revision']}.part" for v in row["versions"]}
            if any(name not in known for name in names):
                raise ValueError("缓存目录中有未登记文件，已保留，请人工核对。")
            details = [os.stat(name, dir_fd=folder_fd, follow_symlinks=False) for name in names]
            if any(not stat.S_ISREG(item.st_mode) for item in details):
                raise ValueError("缓存目录含非程序视频缓存，已保留。")
            for name in names:
                os.unlink(name, dir_fd=folder_fd)
            os.close(folder_fd)
            folder_fd = None
            os.rmdir(row["id"], dir_fd=root_fd)
            return sum(item.st_size for item in details)
        finally:
            if folder_fd is not None:
                os.close(folder_fd)
            os.close(root_fd)

    async def remove_uncorrected(self, identity, payload):
        async with self.row_lock(identity):
            row = self.store.get(identity)
            if not row:
                raise ValueError("视频记录不存在。")
            existing = row["requests"].get(payload["requestId"])
            if existing:
                if existing.get("kind") == "remove-uncorrected" and existing.get("revision") == payload["expectedRevision"]:
                    return {"id": identity, "deleted": True, "cacheBytes": existing["cacheBytes"]}
                raise Conflict("请求ID已用于其他操作。")
            if row.get("deletedAt"):
                raise ValueError("记录已删除。")
            if row["revision"] != payload["expectedRevision"]:
                raise Conflict("此条刚有新的操作，已保留，请刷新后核对。")
            if any(v["revision"] > 0 for v in row["versions"]) or any(
                    r.get("kind") == "correction" for r in row["requests"].values()):
                raise ValueError("本条有更正历史，禁止作为未更正记录清理。")
            if identity in self.running and not self.running[identity].done():
                raise ValueError("本条正在保存，请完成后再清理记录。")
            try:
                cleared = await asyncio.to_thread(self.clear_owned_cache, row)
            except OSError:
                raise ValueError("缓存目录安全检查未通过，记录和文件已保留。") from None
            row.update(deletedAt=now_iso(), deletionReason="uncorrected-history")
            row["requests"][payload["requestId"]] = {"kind": "remove-uncorrected",
                "revision": payload["expectedRevision"], "cacheBytes": cleared}
            self.store.save(row)
            return {"id": identity, "deleted": True, "cacheBytes": cleared}

    async def ffprobe(self, path):
        try:
            process = await asyncio.create_subprocess_exec("ffprobe", "-v", "error", "-show_entries", "format=duration",
                                                             "-of", "default=noprint_wrappers=1:nokey=1", str(path),
                                                             stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL)
            output, _ = await process.communicate()
            duration = float(output.strip())
            if process.returncode or not math.isfinite(duration) or duration <= 0:
                raise ValueError()
            return duration
        except (OSError, ValueError):
            raise DownloadFailure("文件无法作为完整视频播放，旧版已保留。", "invalid_video") from None

    @staticmethod
    async def file_operation(function, *args):
        # Cancellation must wait for filesystem mutation to finish before releasing the row lock.
        task = asyncio.create_task(asyncio.to_thread(function, *args))
        try:
            return await asyncio.shield(task)
        except asyncio.CancelledError:
            await asyncio.gather(task, return_exceptions=True)
            raise

    async def copy_to_final(self, source, destination, report=None, previous_temporary=None, recover=False,
                            reservation=None):
        async with self.publish_slots:
            command = [sys.executable, "-m", "app.transfer", str(source), str(destination)]
            if recover:
                command.append("--recover")
            elif previous_temporary:
                command.append(previous_temporary)
            if reservation:
                if len(command) == 5:
                    command.append("")
                command.append(json.dumps(reservation))
            process = await asyncio.create_subprocess_exec(*command, cwd=Path(__file__).resolve().parents[1],
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL)
            stage = "transfer_start"
            if report:
                report({"stage": stage, "workerPid": getattr(process, "pid", None)})
            try:
                while True:
                    try:
                        timeout = self.transfer_timeout if stage == "copy" else min(self.transfer_timeout, 60)
                        if stage == "flush":
                            timeout = max(self.transfer_timeout, self.transfer_flush_timeout)
                        line = await asyncio.wait_for(process.stdout.readline(), timeout=timeout)
                    except asyncio.TimeoutError:
                        error = OSError(errno.ETIMEDOUT, "目标文件操作长时间无响应。")
                        error.transfer_stage = stage
                        raise error from None
                    if not line:
                        error = OSError(errno.EIO, "转移进程未返回完成状态。")
                        error.transfer_stage = stage
                        raise error
                    event = json.loads(line)
                    stage = event.get("stage", stage)
                    if event.get("event") == "error":
                        error = OSError(event.get("errno") or errno.EIO, "文件转移失败。")
                        error.transfer_stage = stage
                        raise error
                    if event.get("event") == "complete":
                        await process.wait()
                        return
                    if report:
                        report(event)
            finally:
                if process.returncode is None:
                    try:
                        process.kill()
                    except ProcessLookupError:
                        pass
                try:
                    await asyncio.wait_for(process.wait(), timeout=5)
                    if report:
                        report({"event": "worker_stopped", "stage": stage, "workerPid": getattr(process, "pid", None)})
                except asyncio.TimeoutError:
                    error = OSError(errno.ETIMEDOUT, "文件转移进程尚未退出。")
                    error.transfer_stage = stage
                    raise error from None

    def transfer_report(self, row, version, destination):
        def transferred(event):
            if event.get("event") == "worker_stopped":
                if version.get("transferWorkerPid") == event.get("workerPid"):
                    version.pop("transferWorkerPid", None)
                self.store.save(row)
                return
            version["transferStage"] = event.get("stage")
            if event.get("workerPid"):
                version["transferWorkerPid"] = event["workerPid"]
            if "bytes" in event:
                version["transferredBytes"] = event["bytes"]
            if "temporary" in event:
                version["transferTemporaryPath"] = event["temporary"]
            if "reservation" in event:
                version["transferReservation"] = event["reservation"]
            if event.get("stage") == "source_remove":
                version["transferReceipt"] = {"path": str(destination), "publishedAt": now_iso()}
            self.store.save(row)
        return transferred

    async def transfer_worker_running(self, identity, version):
        pid = version.get("transferWorkerPid")
        if not isinstance(pid, int) or pid <= 0:
            return False
        process = await asyncio.create_subprocess_exec("ps", "-p", str(pid), "-o", "command=",
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL)
        try:
            output, _ = await asyncio.wait_for(process.communicate(), timeout=2)
        except asyncio.TimeoutError:
            process.kill()
            await process.wait()
            return True
        command = output.decode(errors="replace")
        source = str(self.work / identity / f"v{version['revision']}.part")
        destination = version.get("publishing", {}).get("path", "")
        return "-m app.transfer " in command and source in command and bool(destination) and destination in command

    async def download_bound(self, row, version):
        folder = self.work / row["id"]
        folder.mkdir(parents=True, exist_ok=True)
        part = folder / f"v{version['revision']}.part"
        progress_before, stalled = -1, 0
        for attempt, delay in enumerate(RETRY_DELAYS, 1):
            version["attempts"] = attempt
            self.store.save(row)
            try:
                result = await self.bound_result(version)  # refresh media link for every attempt
                url = download_url(result["url"])
                offset = part.stat().st_size if part.is_file() else 0
                if offset and (not version.get("etag") or version["etag"].startswith("W/")):
                    offset = 0  # without a strong version, restart instead of combining two objects
                headers = {"Accept-Encoding": "identity", **({"Range": f"bytes={offset}-"} if offset else {})}
                if offset and version.get("etag"):
                    headers["If-Range"] = version["etag"]
                async with self.target.client.stream("GET", url, headers=headers, timeout=httpx.Timeout(60, connect=15)) as response:
                    if response.status_code in (401, 403, 429, 500, 502, 503, 504):
                        raise DownloadFailure("媒体链接或服务器暂时不可用，稍后续传。", "temporary_media", retryable=True)
                    if response.status_code == 416:
                        match = re.fullmatch(r"bytes \*/(\d+)", response.headers.get("content-range", ""))
                        if (offset and match and int(match[1]) == offset
                                and version.get("totalBytes") == offset
                                and (not version.get("etag") or response.headers.get("etag") in (None, version["etag"]))):
                            await self.ffprobe(part)
                            return part, result, await asyncio.to_thread(file_digest, part), offset
                        raise DownloadFailure("断点位置与平台文件不一致，已保留断点。", "range_invalid")
                    if response.status_code not in (200, 206):
                        raise DownloadFailure("媒体响应异常，已保留断点。", "media_response")
                    kind = response.headers.get("content-type", "").lower()
                    if kind.startswith("text/") or "json" in kind:
                        raise DownloadFailure("服务器返回的不是视频。", "invalid_content")
                    etag = response.headers.get("etag", "")
                    length_text = response.headers.get("content-length", "")
                    if length_text and not length_text.isdigit():
                        raise DownloadFailure("媒体长度异常，已停止下载。", "invalid_length")
                    length = int(length_text or 0)
                    if offset and version.get("etag") and etag != version["etag"]:
                        raise DownloadFailure("媒体文件版本发生变化，已保留断点。", "object_changed")
                    if response.status_code == 206:
                        match = re.fullmatch(r"bytes (\d+)-(\d+)/(\d+)", response.headers.get("content-range", ""))
                        if not match or int(match[1]) != offset or int(match[2]) < offset or int(match[3]) <= int(match[2]):
                            raise DownloadFailure("断点范围异常，已停止下载。", "range_invalid")
                        total = int(match[3])
                        expected = int(match[2]) - offset + 1
                        if length and length != expected:
                            raise DownloadFailure("断点长度异常，已停止下载。", "range_invalid")
                    else:
                        total, expected = length, length
                        if offset and version.get("totalBytes") and length != version["totalBytes"]:
                            raise DownloadFailure("媒体长度发生变化，已停止下载。", "object_changed")
                        offset = 0  # server ignored Range; restart only after version checks
                    if offset and version.get("totalBytes") and total != version["totalBytes"]:
                        raise DownloadFailure("媒体长度发生变化，已停止下载。", "object_changed")
                    if not total:
                        raise DownloadFailure("媒体未提供完整长度，无法校验下载。", "invalid_length")
                    if shutil.disk_usage(folder).free < total - offset + 16 * 1024 * 1024:
                        raise DownloadFailure("工作磁盘空间不足，旧版已保留。", "disk_full")
                    version.update(etag=etag, totalBytes=total, receivedBytes=offset)
                    self.store.save(row)
                    received, saved_at = offset, time.monotonic()
                    with part.open("ab" if offset else "wb") as output:
                        async for block in response.aiter_bytes(chunk_size=1024 * 1024):
                            if received == 0 and not valid_video_header(block[:512]):
                                raise DownloadFailure("下载内容不是视频。", "invalid_content")
                            output.write(block)
                            received += len(block)
                            version["receivedBytes"] = received
                            if time.monotonic() - saved_at >= 1:
                                self.store.save(row)
                                saved_at = time.monotonic()
                        output.flush()
                        os.fsync(output.fileno())
                    if expected and received - offset != expected:
                        raise DownloadFailure("下载连接中断，将续传。", "incomplete", retryable=True)
                    if received != total:
                        raise DownloadFailure("视频长度不完整，将续传。", "incomplete", retryable=True)
                await self.ffprobe(part)
                return part, result, await asyncio.to_thread(file_digest, part), total
            except (httpx.HTTPError, DownloadFailure, RemoteError) as error:
                retryable = isinstance(error, httpx.HTTPError) or isinstance(error, RemoteError) or getattr(error, "retryable", False)
                size = part.stat().st_size if part.is_file() else 0
                stalled = stalled + 1 if size == progress_before else 1
                progress_before = size
                if not retryable or stalled >= 3 or attempt >= len(RETRY_DELAYS):
                    if isinstance(error, DownloadFailure):
                        raise
                    raise DownloadFailure("网络中断或多次下载无进展，旧版已保留。", "network") from None
                await asyncio.sleep(delay)
        raise DownloadFailure("下载次数已用尽。", "network")

    async def publish(self, identity, revision):
        async with self.row_lock(identity):
            row = self.store.get(identity)
            version = self.version(row, revision) if row else None
            if not version or row.get("deletedAt") or row["activeRevision"] != revision or not version.get("downloadRequested"):
                return
            try:
                if not version.get("outputDirectory"):
                    raise DownloadFailure("请先指定保存文件夹。", "directory_missing")
                if version.get("transferWorkerPid"):
                    if await self.transfer_worker_running(identity, version):
                        version["message"] = "正在等待上一转移进程退出；本机视频和断点已保留。"
                        self.store.save(row)
                        return
                    version.pop("transferWorkerPid", None)
                if version.get("publishing"):
                    if not self.storage_ready and not self.require_mounted_external:
                        version["state"] = "publishing"
                        version["message"] = "本机下载已完成，等待外置盘可用后保存。"
                        version["errorCode"] = "external_unavailable"
                        self.store.save(row)
                        return
                    prepared = version["publishing"]
                    destination = Path(prepared["path"])
                    part = self.work / identity / f"v{revision}.part"
                    version["message"] = "正在转移到保存目录。"
                    version["transferAttempts"] = version.get("transferAttempts", 0) + 1
                    self.store.save(row)
                    transferred = self.transfer_report(row, version, destination)
                    if version.get("transferReceipt", {}).get("path") == str(destination):
                        # The worker acknowledged publication before removing the local source.
                        part.unlink(missing_ok=True)
                    elif part.is_file():
                        # Attempt the transfer directly; the exclusive atomic publish handles
                        # name conflicts without a separate removable-volume probe.
                        await self.copy_to_final(part, destination, transferred, version.get("transferTemporaryPath"),
                                                 reservation=version.get("transferReservation"))
                    else:
                        # Reconcile legacy publication in a bounded worker, never in the API thread.
                        await self.copy_to_final(part, destination, transferred, recover=True,
                                                 reservation=version.get("transferReservation"))
                    digest, size = prepared["sha256"], prepared["bytes"]
                else:
                    async with self.download_slots:
                        version["state"] = "downloading"
                        self.store.save(row)
                        part, result, digest, size = await self.download_bound(row, version)
                    destination = Path(version["outputDirectory"]) / video_filename({"videoName": row["name"],
                        "startTime": version["startTime"]}, result)
                    version["publishing"] = {"path": str(destination), "sha256": digest, "bytes": size}
                    version["state"] = "publishing"
                    version["message"] = "正在转移到保存目录。"
                    version["transferAttempts"] = version.get("transferAttempts", 0) + 1
                    self.store.save(row)
                    if not self.storage_ready and not self.require_mounted_external:
                        version["message"] = "本机下载已完成，等待外置盘可用后保存。"
                        version["errorCode"] = "external_unavailable"
                        self.store.save(row)
                        return
                    await self.copy_to_final(part, destination, self.transfer_report(row, version, destination))
                version.update(state="saved", filePath=str(destination), sha256=digest, totalBytes=size,
                               downloadRequested=False, savedAt=now_iso())
                version.pop("message", None)
                version.pop("errorCode", None)
                version.pop("transferError", None)
                version.pop("transferTemporaryPath", None)
                version.pop("transferReservation", None)
                previous = row["acceptedRevision"]
                row["acceptedRevision"] = revision
                row["cleanupPending"] = revision > 0 and previous != revision
                self.store.save(row)
                if row.get("cleanupPending"):
                    try:
                        await self._finish_cleanup_locked(row)
                    except (RemoteError, ValueError, OSError):
                        row["error"] = "新版已保存；旧版清理尚未完成，将继续核对。"
                        self.store.save(row)
            except asyncio.CancelledError:
                if version["state"] == "downloading":
                    version["state"] = "ready"
                self.store.save(row)
                raise
            except (DownloadFailure, ValueError, OSError, RemoteError, httpx.HTTPError) as error:
                if isinstance(error, OSError) and version.get("publishing"):
                    version["state"] = "publishing"
                    stage = getattr(error, "transfer_stage", "transfer")
                    error_name = errno.errorcode.get(error.errno, "UNKNOWN")
                    version["transferError"] = {"type": type(error).__name__, "errno": error.errno,
                                                "code": error_name, "stage": stage,
                                                "occurredAt": now_iso()}
                    reasons = {errno.ENOENT: "目标文件夹或源文件不存在", errno.EACCES: "文件访问被拒绝",
                               errno.EPERM: "系统拒绝文件操作", errno.ENOSPC: "目标空间不足",
                               errno.EROFS: "目标文件系统只读", errno.EIO: "文件系统读写错误",
                               errno.EEXIST: "目标文件名已存在", errno.ENOTSUP: "文件系统不支持当前发布操作",
                               errno.ETIMEDOUT: "目标文件操作长时间无响应"}
                    reason = reasons.get(error.errno, "文件操作失败")
                    version["message"] = f"转移失败：{reason}；本机视频已保留，稍后自动重试。"
                    version["errorCode"] = "transfer_failed"
                    self.store.save(row)
                    return
                version["state"] = "failed"
                version["message"] = str(error) if isinstance(error, DownloadFailure) else "视频保存暂时失败，旧版已保留。"
                version["errorCode"] = error.code if isinstance(error, DownloadFailure) else "filesystem" if isinstance(error, OSError) else "media_unavailable"
                self.store.save(row)

    async def finish_cleanup(self, identity):
        async with self.row_lock(identity):
            row = self.store.get(identity)
            if row and not row.get("deletedAt") and row.get("cleanupPending"):
                try:
                    await self._finish_cleanup_locked(row)
                except (RemoteError, ValueError, OSError):
                    row["error"] = "旧版云端或本地清理尚未完成，可重新核对。"
                    self.store.save(row)

    async def _finish_cleanup_locked(self, row):
        latest = self.version(row, row["acceptedRevision"])
        if not latest or latest["state"] != "saved" or not latest.get("filePath"):
            raise ValueError("新版文件尚未发布，不能清理旧版。")
        for version in row["versions"]:
            if version["revision"] >= latest["revision"] or version["state"] in TERMINAL:
                continue
            old_path = version.get("filePath")
            if old_path:
                guard = self.file_leases.write(old_path) if getattr(self, "file_leases", None) else nullcontext()
                with guard:
                    path = Path(old_path)
                    if path.is_symlink() or (path.is_file() and await asyncio.to_thread(file_digest, path) != version.get("sha256")):
                        raise ValueError("旧版本地文件已变化，未自动删除。")
                    if path.is_file():
                        path.unlink()
                    version["filePath"] = ""
                    self.store.save(row)
            if version.get("taskId"):
                await self.cleanup_cloud(row, version, protected={v.get("taskId") for v in row["versions"]
                    if v["revision"] >= latest["revision"]})
            version["state"] = "cloud_deleted"
            self.store.save(row)
        row["cleanupPending"] = False
        row["error"] = ""
        self.store.save(row)

    async def cleanup_cloud(self, row, version, protected, allow_incomplete=False):
        parent_id = version.get("taskId")
        if not parent_id or parent_id in protected:
            raise ValueError("旧任务ID无效或指向保留版本，已阻止删除。")
        async with self.jobs.lock:
            parents = await self.jobs.remote_parents(self.target_config)
            parent = parents.get(parent_id)
            cleanup = version.setdefault("cleanup", {"started": False, "objects": {}, "checks": {}})
            if not parent:
                if not cleanup["started"]:
                    raise ValueError("旧任务已在外部消失，无法确认云端对象。")
                results = await self.results(parent_id)
                if any(str(result["id"]) not in cleanup["objects"] for result in results):
                    raise ValueError("删除任务后出现未绑定结果，清理尚未完成。")
                for result in results:
                    await self.target.request("/data-acquisition/delete", method="POST",
                                              body={"id": str(result["id"]), "type": "acInfo"}, write=True)
                await self._delete_objects(cleanup, parent_id, lambda: self.store.save(row))
                await self._verify_objects(cleanup)
                if await self.results(parent_id):
                    raise ValueError("旧结果记录仍存在，清理尚未完成。")
                cleanup["complete"] = True
                self.store.save(row)
                return
            if not self.matches(parent, version, self.target_config["cameraId"]):
                raise ValueError("旧任务身份已变化，已阻止删除。")
            results = await self.results(parent_id)
            if version.get("resultId") and str(parent.get("taskStatus", "")).upper() != "FIXED":
                raise ValueError("旧任务完成状态已变化，暂不删除。")
            if not results and not version.get("resultId") and (version["revision"] > 0 or allow_incomplete):
                # A withdrawn correction can still be collecting. Recheck the result after removing its exact task.
                cleanup.update(started=True, pendingTaskOnly=True)
                self.store.save(row)
                await self.target.request("/data-acquisition/delete", method="POST",
                                          body={"id": parent_id, "type": "task"}, write=True)
                if parent_id in await self.jobs.remote_parents(self.target_config) or await self.results(parent_id):
                    raise ValueError("更正任务仍存在，暂不能确认撤回完成。")
                cleanup.update(started=True, complete=True)
                self.store.save(row)
                return
            if not version.get("resultId") and len(results) == 1 and (allow_incomplete or cleanup.get("pendingTaskOnly")):
                version["resultId"] = str(results[0]["id"])
                cleanup["started"] = False  # Bind an exact-task result discovered during explicit full cleanup.
            if not cleanup["started"]:
                expected = {version.get("resultId")} if version.get("resultId") else set()
                if not expected or {str(result["id"]) for result in results} != expected:
                    raise ValueError("旧任务结果ID无法唯一核对，已阻止删除。")
                for result in results:
                    parsed = urlsplit(result.get("url", ""))
                    if parsed.scheme not in ("http", "https") or not parsed.hostname or parsed.username or parsed.password:
                        raise ValueError("旧视频对象地址无效，已阻止删除。")
                    cleanup["objects"][str(result["id"])] = urlunsplit((parsed.scheme, parsed.netloc, parsed.path, "", ""))
                if version.get("etag") and version.get("totalBytes"):
                    cleanup["checks"][version["resultId"]] = {"etag": version["etag"],
                                                                    "length": version["totalBytes"]}
                cleanup["started"] = True
                self.store.save(row)
            if set(cleanup["objects"]) != {version["resultId"]} or any(str(r["id"]) != version["resultId"] for r in results):
                raise ValueError("旧任务出现未绑定的视频结果，已阻止删除。")
            for result in results:
                await self.target.request("/data-acquisition/delete", method="POST",
                                          body={"id": str(result["id"]), "type": "acInfo"}, write=True)
            await self._delete_objects(cleanup, parent_id, lambda: self.store.save(row))
            await self._verify_objects(cleanup)
            if await self.results(parent_id):
                raise ValueError("旧结果记录仍存在，稍后继续核对。")
            await self.target.request("/data-acquisition/delete", method="POST",
                                      body={"id": parent_id, "type": "task"}, write=True)
            if parent_id in await self.jobs.remote_parents(self.target_config):
                raise ValueError("旧任务仍存在，稍后继续核对。")
            cleanup["complete"] = True
            self.store.save(row)

    async def _delete_objects(self, cleanup, parent_id, persist=None):
        for result_id, reference in cleanup["objects"].items():
            parsed = urlsplit(reference)
            marker = r"(?<![A-Za-z0-9])" + re.escape(parent_id) + r"(?![A-Za-z0-9])"
            if not parent_id or not re.search(marker, parsed.path) or parsed.query or parsed.fragment:
                raise ValueError("存储路径无法确认属于旧任务，已阻止删除。")
            async with self.target.client.stream("GET", reference, headers={"Range": "bytes=0-0"}, timeout=15) as response:
                if response.status_code in (404, 410):
                    continue
                if response.status_code not in (200, 206):
                    raise ValueError("无法核对旧文件版本，清理尚未完成。")
                etag = response.headers.get("etag", "")
                match = re.fullmatch(r"bytes 0-0/(\d+)", response.headers.get("content-range", ""))
                length = int(match[1]) if match else int(response.headers.get("content-length", "0"))
                if not etag or etag.startswith("W/") or not length:
                    raise ValueError("旧文件缺少强版本标识或长度，已阻止删除。")
            previous = cleanup["checks"].get(result_id)
            if previous and previous != {"etag": etag, "length": length}:
                raise ValueError("旧文件版本发生变化，已阻止删除。")
            cleanup["checks"][result_id] = {"etag": etag, "length": length}
            if persist:
                persist()  # Save the strong version before a possibly ambiguous storage DELETE.
            response = await self.target.client.request("DELETE", reference, headers={"If-Match": etag}, timeout=15)
            if response.status_code == 412:
                raise ValueError("旧文件版本在删除前发生变化。")
            if response.status_code not in (200, 202, 204, 404, 410):
                raise ValueError("存储端未确认删除旧文件，稍后继续核对。")

    async def _verify_objects(self, cleanup):
        for reference in cleanup["objects"].values():
            for url in (download_url(reference), reference):
                async with self.target.client.stream("GET", url, headers={"Range": "bytes=0-0"}, timeout=15) as response:
                    if response.status_code not in (404, 410):
                        raise ValueError("旧视频对象仍可访问，云端清理尚未完成。")
