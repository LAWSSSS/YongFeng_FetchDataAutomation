"""Upload one slice archive per category, then import one package at a time."""
import asyncio
import json
import os
import shutil
from pathlib import Path

from .audit_moves import source_fingerprint
from .domain import now_iso
from .slice_export import PACKAGE_DATASETS, package_directory
from .upload_audit import AuditError, name_key
from .upload_remote import source_keys
from .uploads import CHECK_SECONDS, ProcessWorker, RETRY_DELAYS, STALL_SECONDS, safe_message

PACKAGE_TARGETS = (
    {"id": "27892", "category": "bed", "name": "车斗切片照片"},
    {"id": "27893", "category": "magnet", "name": "磁盘吊切片照片"},
)


def _place_file(source, destination):
    """Move a file onto another volume without copying macOS-only flags."""
    if not source.is_file():
        return destination.is_file()
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists():
        if destination.stat().st_size == source.stat().st_size:
            source.unlink()
            return True
        return False
    try:
        os.replace(source, destination)
    except OSError:
        with source.open("rb") as incoming, destination.open("wb") as outgoing:
            shutil.copyfileobj(incoming, outgoing, 1024 * 1024)
        if destination.stat().st_size != source.stat().st_size:
            destination.unlink(missing_ok=True)
            raise OSError("copy_incomplete")
        source.unlink()
    return True


class SlicePackageStore:
    def __init__(self, db):
        self.db = db
        for name in ("items", "batches"):
            db.execute(f"CREATE TABLE IF NOT EXISTS simple_slice_package_{name} (id TEXT PRIMARY KEY, payload TEXT NOT NULL)")
        db.commit()

    def save(self, kind, value):
        value["updatedAt"] = now_iso()
        self.db.execute(
            f"INSERT INTO simple_slice_package_{kind} VALUES(?,?) ON CONFLICT(id) DO UPDATE SET payload=excluded.payload",
            (value["id"], json.dumps(value, ensure_ascii=False)))
        self.db.commit()

    def get(self, kind, identity):
        row = self.db.execute(f"SELECT payload FROM simple_slice_package_{kind} WHERE id=?", (identity,)).fetchone()
        return json.loads(row[0]) if row else None

    def list(self, kind):
        return [json.loads(row[0]) for row in self.db.execute(f"SELECT payload FROM simple_slice_package_{kind} ORDER BY rowid")]

    def find(self, source_dataset, video_id, category):
        for item in self.list("items"):
            if (item.get("sourceDatasetId") == source_dataset and str(item.get("sourceVideoId")) == str(video_id)
                    and item.get("category") == category and item.get("state") != "dismissed"):
                return item
        return None


class SlicePackageManager:
    def __init__(self, db, remote, worker=None, clock=None):
        self.store = SlicePackageStore(db)
        self.remote = remote
        self.worker = worker or ProcessWorker()
        self.clock = clock or __import__("time").time
        self.running = {}
        self.runtime = {}
        self.wake = asyncio.Event()
        self.stopping = False
        self.loop_task = None
        self.grants = {}
        for item in self.store.list("items"):
            try:
                changed = self._move_beside_video(item) or self._requeue_failed_upload(item)
            except OSError:
                continue
            if changed:
                self.store.save("items", item)

    def _move_beside_video(self, item):
        video = item.get("videoPath") or ""
        if not video or not item.get("path") or item.get("uploadId"):
            return False
        dest = package_directory(video, item.get("category") or "bed")
        archive = Path(item["path"])
        if archive.parent == dest:
            return False
        protected = (Path.home() / "Desktop" / "视频抽帧").resolve()
        if dest.resolve().is_relative_to(protected) or archive.resolve().is_relative_to(protected):
            return False
        dest.mkdir(parents=True, exist_ok=True)
        moved = False
        target = dest / archive.name
        if _place_file(archive, target):
            moved = True
        images = Path(item["imageDir"]) if item.get("imageDir") else None
        image_dest = dest / "image"
        if images and images.is_dir() and images.resolve() != image_dest.resolve():
            image_dest.mkdir(parents=True, exist_ok=True)
            for child in list(images.iterdir()):
                if _place_file(child, image_dest / child.name):
                    moved = True
        if target.is_file():
            item["path"] = str(target)
            item["fileVersion"] = source_fingerprint(target)
            moved = True
        if image_dest.is_dir():
            item["imageDir"] = str(image_dest)
        return moved

    def _requeue_failed_upload(self, item):
        key = (item.get("object") or {}).get("key") or ""
        if item.get("uploadId") or item.get("state") not in {"attention", "queued"}:
            return False
        if "/simple-" + str(item.get("id")) + "/" in "/" + key:
            return False
        item.pop("object", None)
        item.pop("createAttempted", None)
        if item.get("state") == "attention" and item.get("errorCode") == "upload_failed":
            item.update(retries=0, state="queued", errorCode="", nextAttempt=0,
                        message="正在排队上传切片压缩包。")
        return True

    def state(self):
        items = self.store.list("items")
        targets = []
        for dataset in PACKAGE_TARGETS:
            rows = [item for item in items if item.get("dataset") == dataset["id"]]
            targets.append({**dataset, "items": rows,
                            "importing": sum(item.get("state") == "importing" for item in rows),
                            "uploading": sum(item.get("state") == "uploading" for item in rows),
                            "queued": sum(item.get("state") in {"queued", "uploaded", "review"} for item in rows)})
        return {"targets": targets, "items": items}

    def remember(self, record):
        existing = self.store.find(record["sourceDatasetId"], record["sourceVideoId"], record["category"])
        if existing:
            return existing
        self.store.save("items", record)
        return record

    def confirm(self, identity):
        item = self.store.get("items", identity)
        if not item:
            raise ValueError("切片压缩包不存在。")
        if item["state"] == "review":
            item.update(state="queued", message="已确认照片，正在排队上传。")
            self.store.save("items", item)
        self.wake.set()
        return item

    async def start(self):
        if not self.loop_task or self.loop_task.done():
            self.stopping = False
            self.loop_task = asyncio.create_task(self.loop(), name="slice-packages")

    async def shutdown(self):
        self.stopping = True
        self.wake.set()
        if self.loop_task:
            self.loop_task.cancel()
            await asyncio.gather(self.loop_task, return_exceptions=True)
        for task in list(self.running.values()):
            task.cancel()
        if self.running:
            await asyncio.gather(*self.running.values(), return_exceptions=True)

    async def loop(self):
        while not self.stopping:
            self.wake.clear()
            try:
                await self.tick()
            except asyncio.CancelledError:
                raise
            except Exception:
                pass
            try:
                await asyncio.wait_for(self.wake.wait(), CHECK_SECONDS)
            except TimeoutError:
                pass

    async def tick(self):
        now = self.clock()
        for identity, task in list(self.running.items()):
            runtime = self.runtime.get(identity, {})
            if not task.done() and now - runtime.get("lastProgress", now) >= STALL_SECONDS:
                runtime["stopCode"] = "stalled"
                task.cancel()
        if not any(item.get("state") in {"queued", "uploaded", "uploading", "importing", "submitting"}
                   for item in self.store.list("items")):
            return
        await asyncio.gather(*(self.tick_target(dataset["id"]) for dataset in PACKAGE_TARGETS))

    async def tick_target(self, dataset):
        items = [item for item in self.store.list("items") if item.get("dataset") == dataset]
        if not items:
            return
        jobs = await self.remote.jobs(dataset)
        active = [job for job in jobs if str(job.get("status")) in {"1", "2"}]
        for item in items:
            if item["state"] in {"submitting", "importing"} and not item.get("jobId"):
                await self.reconcile(item, jobs)
                continue
            if item["state"] != "importing" or not item.get("jobId"):
                continue
            job = next((row for row in jobs if str(row.get("id")) == str(item["jobId"])), None)
            if job and str(job.get("status")) in {"3", "4"}:
                await self.verify(item, job)
        busy = bool(active) or any(item.get("state") in {"submitting", "importing"}
                                    for item in self.store.list("items") if item.get("dataset") == dataset)
        if not busy:
            ready = next((item for item in self.store.list("items")
                          if item.get("dataset") == dataset and item.get("state") == "uploaded"), None)
            if ready:
                await self.submit_one(ready, jobs)
        now = self.clock()
        for item in self.store.list("items"):
            if item.get("dataset") != dataset or item.get("state") != "queued" or item["id"] in self.running:
                continue
            if item.get("nextAttempt", 0) > now:
                continue
            if any(other.get("dataset") == dataset and other["id"] in self.running for other in self.store.list("items")):
                break
            self.launch(item)
            break

    def launch(self, item):
        identity = item["id"]
        self.runtime[identity] = {"lastProgress": self.clock(), "bytes": item.get("bytes", 0)}
        task = asyncio.create_task(self.upload_one(identity), name="slice-package-" + identity)
        self.running[identity] = task
        task.add_done_callback(lambda _done: (self.running.pop(identity, None), self.runtime.pop(identity, None), self.wake.set()))

    async def upload_one(self, identity):
        item = self.store.get("items", identity)
        try:
            current = source_fingerprint(Path(item["path"]))
            if current != item["fileVersion"]:
                raise ValueError("file_changed")
            credentials = await self.remote.signature(item["dataset"])
            prefix = credentials.get("prefix")
            key = (prefix or "") + "simple-" + identity + "/" + item["name"]
            current = (item.get("object") or {}).get("key") or ""
            if not item.get("uploadId") and ("/simple-" + identity + "/") not in ("/" + current):
                item["object"] = {"key": key, "bucket": credentials["bucket"], "endpoint": credentials["endpoint"]}
            item.update(state="uploading", message="正在上传切片压缩包。")
            self.store.save("items", item)
            await self.worker.run(item, credentials, lambda event: self.worker_event(identity, event))
        except asyncio.CancelledError:
            item = self.store.get("items", identity)
            if self.runtime.get(identity, {}).get("stopCode") == "stalled":
                self.fail(item, "stalled")
            elif item and item["state"] not in {"uploaded", "complete"}:
                item.update(state="queued", message="上传已暂停，压缩包保留。")
                self.store.save("items", item)
            raise
        except AuditError as error:
            self.fail(self.store.get("items", identity), error.code if error.code in {"auth", "network", "disk"} else "upload_failed")
        except ValueError as error:
            self.fail(self.store.get("items", identity), str(error) if str(error) in {"file_changed", "stalled"} else "upload_failed")
        except Exception:
            self.fail(self.store.get("items", identity), "upload_failed")

    async def worker_event(self, identity, event):
        item = self.store.get("items", identity)
        runtime = self.runtime.setdefault(identity, {"lastProgress": self.clock(), "bytes": 0})
        kind = event.get("event")
        if kind == "progress":
            runtime["lastProgress"] = self.clock()
            return
        if kind in {"creating", "created", "part", "completing"}:
            runtime["lastProgress"] = self.clock()
            if kind == "created" and isinstance(event.get("uploadId"), str):
                item["uploadId"] = event["uploadId"]
            self.store.save("items", item)
            return
        if kind == "complete":
            if event.get("size") not in (None, item["size"]):
                raise ValueError("object_changed")
            item.update(state="uploaded", bytes=item["size"], etag=event.get("etag", ""),
                        message="压缩包已上传，等待该目标的导入空位。")
            self.store.save("items", item)
            return
        if kind == "error":
            self.fail(item, event.get("code") if event.get("code") in {"auth", "network", "disk", "stalled"} else "upload_failed")

    def fail(self, item, code):
        if not item:
            return
        item["retries"] = item.get("retries", 0) + 1
        if code in {"auth", "network", "disk"}:
            item.update(state="queued", errorCode=code, message=safe_message(code), nextAttempt=self.clock() + CHECK_SECONDS)
        elif item["retries"] > len(RETRY_DELAYS):
            item.update(state="attention", errorCode=code, message="压缩包已重试3次仍失败，请核对后重试。")
        else:
            item.update(state="queued", errorCode=code, message=safe_message(code),
                        nextAttempt=self.clock() + RETRY_DELAYS[min(item["retries"] - 1, 2)])
        self.store.save("items", item)

    async def submit_one(self, item, jobs):
        item = self.store.get("items", item["id"])
        if item["state"] != "uploaded" or not item.get("object"):
            return
        keys = [item["object"]["key"]]
        item.update(state="submitting", beforeJobs=[str(job.get("id")) for job in jobs], keys=keys,
                    message="已保存导入前任务清单，正在提交这一个压缩包。")
        self.store.save("items", item)
        try:
            identity = await self.remote.submit(item["dataset"], keys)
        except AuditError as error:
            item.update(state="importing", errorCode=error.code or "uncertain", message="导入响应不确定，只按原压缩包对账。")
            self.store.save("items", item)
            return
        self.bind(item, identity)

    def bind(self, item, identity):
        item.update(state="importing", jobId=str(identity), errorCode="", message="平台正在导入这一个切片压缩包。")
        self.store.save("items", item)

    async def reconcile(self, item, jobs):
        candidates = [job for job in jobs if str(job.get("id")) not in set(item.get("beforeJobs") or [])
                      and source_keys(job) == frozenset(item.get("keys") or [])]
        if len(candidates) == 1:
            self.bind(item, candidates[0]["id"])
        else:
            item.update(state="attention", errorCode="import_uncertain", message="无法唯一确认这个压缩包的导入任务。")
            self.store.save("items", item)

    async def verify(self, item, job):
        evidence = await self.remote.evidence(item["dataset"], job)
        images = await self.remote.images(item["dataset"]) if hasattr(self.remote, "images") else []
        stem = name_key(Path(item["name"]).stem)
        full = name_key(item["name"])
        proofs = evidence.get("evidence", {}).get((full, "full"), []) + evidence.get("evidence", {}).get((stem, "stem"), [])
        named = [row for row in images if stem and stem in name_key(str(row.get("name", row.get("fileName", ""))))]
        if str(job.get("status")) == "3" and (proofs or named) and not evidence.get("reason"):
            item.update(state="complete", message="切片压缩包已导入并核对。", errorCode="")
            self.store.save("items", item)
            self.delete_if_both_complete(item)
            return
        item.update(state="attention", errorCode="verification", message="导入结束，但没有核对到这个压缩包。")
        self.store.save("items", item)

    def delete_if_both_complete(self, item):
        video_id = str(item.get("sourceVideoId") or "")
        video_path = item.get("videoPath") or ""
        related = [row for row in self.store.list("items")
                   if str(row.get("sourceVideoId") or "") == video_id and (row.get("videoPath") or "") == video_path]
        states = {row.get("category"): row.get("state") for row in related}
        if states.get("bed") != "complete" or states.get("magnet") != "complete":
            return
        removed = []
        try:
            protected = (Path.home() / "Desktop" / "视频抽帧").resolve()
            for row in related:
                for raw in (row.get("path"), row.get("imageDir")):
                    if not raw:
                        continue
                    path = Path(raw)
                    if path.resolve().is_relative_to(protected):
                        raise ValueError("protected")
                    if not path.exists():
                        continue
                    if path.is_file():
                        path.unlink()
                        removed.append(str(path))
                    elif path.is_dir():
                        for child in path.glob("*.jpg"):
                            child.unlink()
                        if not any(path.iterdir()):
                            path.rmdir()
                        removed.append(str(path))
            source = Path(video_path) if video_path else None
            if source and source.is_file():
                if source.resolve().is_relative_to(protected):
                    raise ValueError("protected")
                source.unlink()
                removed.append(str(source))
        except (OSError, ValueError) as error:
            for row in related:
                row.update(cleanupState="attention", cleanupMessage="照片或原片尚未删除，请核对后重试。",
                           cleanupError=type(error).__name__)
                self.store.save("items", row)
            return
        for row in related:
            row.update(cleanupState="complete", cleanupMessage="车斗和磁盘吊都已导入，照片和原片已删除。",
                       deletedPaths=removed)
            self.store.save("items", row)
