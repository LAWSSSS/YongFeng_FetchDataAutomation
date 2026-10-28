"""Upload one slice archive per category, then import one package at a time."""
import asyncio
import json
import zipfile
from contextlib import ExitStack
from pathlib import Path

from .audit_moves import checked_path, source_fingerprint
from .domain import now_iso
from .file_leases import FileLeases, finish_io
from .package_archive import legacy_layout, repair_archive
from .upload_audit import AuditError, name_key, is_import_job
from .upload_remote import source_keys
from .uploads import CHECK_SECONDS, MESSAGES, PAUSE_CODES, ProcessWorker, RETRY_DELAYS, STALL_SECONDS, safe_message
from .videos import Conflict
from . import file_locations

PACKAGE_TARGETS = (
    {"id": "27892", "category": "bed", "name": "车斗切片照片"},
    {"id": "27893", "category": "magnet", "name": "磁盘吊切片照片"},
)


def image_prefix(item):
    names = item.get("images") or []
    return Path(names[0]).stem.rsplit("_", 1)[0] if names else Path(item["name"]).stem


class SlicePackageStore:
    def __init__(self, db):
        self.db = db
        file_locations.initialize(db)
        for name in ("items", "batches"):
            db.execute(f"CREATE TABLE IF NOT EXISTS simple_slice_package_{name} (id TEXT PRIMARY KEY, payload TEXT NOT NULL)")
        db.commit()

    def save(self, kind, value):
        if kind not in {"items", "batches"}:
            raise ValueError("记录类型无效。")
        if kind == 'items':
            file_locations.package(self.db, value)
        previous = self.get(kind, value["id"])
        file_locations.keep_cleanup(previous, value)
        if previous and previous.get("superseded"):
            value["superseded"] = True
            for key in ("jobId", "verifiedJobId", "outputImageIds"):
                if previous.get(key) and not value.get(key):
                    value[key] = previous[key]
            if not value.get("jobId") and value.get("state") not in {"submitting", "importing", "complete"}:
                value["state"] = "superseded"
        value["revision"] = int((previous or {}).get("revision", 0)) + 1
        value["updatedAt"] = now_iso()
        self.db.execute(
            f"INSERT INTO simple_slice_package_{kind} VALUES(?,?) ON CONFLICT(id) DO UPDATE SET payload=excluded.payload",
            (value["id"], json.dumps(value, ensure_ascii=False)))
        self.db.commit()

    def get(self, kind, identity):
        if kind not in {"items", "batches"}:
            raise ValueError("记录类型无效。")
        row = self.db.execute(f"SELECT payload FROM simple_slice_package_{kind} WHERE id=?", (identity,)).fetchone()
        return json.loads(row[0]) if row else None

    def list(self, kind):
        if kind not in {"items", "batches"}:
            raise ValueError("记录类型无效。")
        return [json.loads(row[0]) for row in self.db.execute(f"SELECT payload FROM simple_slice_package_{kind} ORDER BY rowid")]

    def find(self, source_dataset, video_id, category):
        for item in self.list("items"):
            if (item.get("sourceDatasetId") == source_dataset and str(item.get("sourceVideoId")) == str(video_id)
                    and item.get("category") == category and item.get("state") != "dismissed" and not item.get("superseded")):
                return item
        return None

    def find_video(self, path, version, category):
        matches = [item for item in self.list("items") if item.get("videoPath") == path
                   and item.get("videoFileVersion") == version and item.get("category") == category
                   and item.get("state") != "dismissed" and not item.get("superseded")]
        if len(matches) > 1:
            raise ValueError("同一视频存在多个切片包，请先核对已有记录。")
        return matches[0] if matches else None


class SlicePackageManager:
    def __init__(self, db, remote, worker=None, clock=None, leases=None):
        self.store = SlicePackageStore(db)
        self.remote = remote
        self.worker = worker or ProcessWorker()
        self.clock = clock or __import__("time").time
        self.running = {}
        self.runtime = {}
        self.wake = asyncio.Event()
        self.stopping = False
        self.loop_task = None
        self.leases = leases or FileLeases()
        self.tick_lock = asyncio.Lock()
        self.paused = bool((self.store.get("batches", "control") or {}).get("paused"))
        self.on_change = lambda item: None
        self.leases.subscribe_moves(self.moved)

    def moved(self, source, destination, version):
        for item in self.store.list("items"):
            changed = False
            if item.get("path") == str(source):
                if item.get("object"):
                    item.setdefault("uploadFileVersion", item["fileVersion"])
                item.update(path=str(destination), fileVersion=version)
                changed = True
            if item.get("videoPath") == str(source):
                item.update(videoPath=str(destination))
                if item.get("videoFileVersion"):
                    item["videoFileVersion"] = version
                changed = True
            if changed:
                self._save(item)

    def _save(self, item):
        self.store.save("items", item)
        self.on_change(item)

    def pause(self):
        self.paused = True
        self.store.save("batches", {"id": "control", "paused": True})
        for task in self.running.values():
            task.cancel()

    def resume(self):
        if self.paused:
            self.paused = False
            self.store.save("batches", {"id": "control", "paused": False})
        self.wake.set()

    def state(self):
        items = self.store.list("items")
        targets = []
        for dataset in PACKAGE_TARGETS:
            rows = [item for item in items if item.get("dataset") == dataset["id"]]
            targets.append({**dataset, "items": rows,
                            "importing": sum(item.get("state") == "importing" for item in rows),
                            "uploading": sum(item.get("state") == "uploading" for item in rows),
                            "queued": sum(item.get("state") in {"queued", "uploaded", "review"} for item in rows)})
        # Object addresses and submit snapshots are internal recovery receipts.
        fields = {"id", "revision", "dataset", "category", "sourceDatasetId", "sourceVideoId", "itemId",
                  "videoPath", "path", "imageDir", "name", "state", "message", "errorCode", "images",
                  "imageCount", "actualImages", "bytes", "size", "retries", "jobId", "cleanupState",
                  "cleanupMessage", "createdAt", "updatedAt", "superseded", "localCleanupState"}
        public = [{key: value for key, value in item.items() if key in fields} for item in items]
        for target in targets:
            target["items"] = [item for item in public if item.get("dataset") == target["id"]]
        return {"targets": targets, "items": public, "paused": self.paused,
                "busy": bool(self.running) or self.tick_lock.locked()}

    def remember(self, record):
        if getattr(self, "control", None) and not self.control.package_allowed(record):
            record.update(superseded=True, state="superseded", message="旧版照片已保留，不再上传。")
        if record.get("sourceIdentity"):
            existing = next((item for item in self.store.list("items")
                if item.get("sourceIdentity") == record["sourceIdentity"]
                and item.get("category") == record["category"] and item.get("state") != "dismissed"), None)
        else:
            existing = self.store.find_video(record.get("videoPath"), record.get("videoFileVersion"), record["category"])
            if not existing:
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
            self._save(item)
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
        if self.paused or self.tick_lock.locked():
            return
        async with self.tick_lock:
            await self._tick()

    async def _tick(self):
        if getattr(self, 'workspace', None) and self.workspace.blocked:
            return
        now = self.clock()
        for identity, task in list(self.running.items()):
            runtime = self.runtime.get(identity, {})
            if not task.done() and now - runtime.get("lastProgress", now) >= STALL_SECONDS:
                runtime["stopCode"] = "stalled"
                task.cancel()
        if not any(item.get("state") in {"queued", "uploaded", "uploading", "importing", "submitting"}
                   for item in self.store.list("items")):
            return
        # One target's outage must not interrupt or leak the other target's work.
        results = await asyncio.gather(*(self.tick_target(dataset["id"]) for dataset in PACKAGE_TARGETS),
                                       return_exceptions=True)
        for target, result in zip(PACKAGE_TARGETS, results):
            if isinstance(result, Exception):
                code = result.code if isinstance(result, AuditError) else "network"
                for item in self.store.list("items"):
                    if item.get("dataset") == target["id"] and item.get("state") in {"queued", "uploaded", "submitting", "importing"}:
                        item.update(errorCode=code, message=safe_message(code))
                        self._save(item)

    async def tick_target(self, dataset):
        items = [item for item in self.store.list("items") if item.get("dataset") == dataset]
        if not items:
            return
        jobs = await self.remote.jobs(dataset)
        if self.paused:
            return
        active = [job for job in jobs if str(job.get("status")) in {"1", "2"}]
        for item in items:
            if item["state"] in {"submitting", "importing"} and not item.get("jobId"):
                await self.reconcile(item, jobs)
                continue
            if item["state"] != "importing" or not item.get("jobId"):
                continue
            job = next((row for row in jobs if str(row.get("id")) == str(item["jobId"])), None)
            if job and str(job.get("status")) in {"3", "4", "5"}:
                await self.verify(item, job)
        busy = bool(active) or any(item.get("state") in {"submitting", "importing"}
                                    or item.get("errorCode") in {"import_uncertain", "import_identity"}
                                    for item in self.store.list("items") if item.get("dataset") == dataset)
        if not busy:
            ready = next((item for item in self.store.list("items")
                          if item.get("dataset") == dataset and item.get("state") == "uploaded"
                          and (not getattr(self, "control", None) or self.control.package_allowed(item))), None)
            if ready:
                await self.submit_one(ready, jobs)
        now = self.clock()
        for item in self.store.list("items"):
            if self.paused:
                return
            if item.get("dataset") != dataset or item.get("state") != "queued" or item["id"] in self.running:
                continue
            if getattr(self, "control", None) and not self.control.package_allowed(item):
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
        if getattr(self, "control", None) and not self.control.package_allowed(item):
            return
        try:
            operation = "cleanup" if item.get("cleanupNeeded") else "upload"
            with self.leases.read(item["path"]):
                if operation == "upload":
                    try:
                        current = source_fingerprint(Path(item["path"]))
                    except OSError:
                        raise AuditError("外置盘或压缩包暂不可用。", "disk") from None
                    if current != item["fileVersion"]:
                        raise ValueError("file_changed")
                credentials = await self.remote.signature(item["dataset"], item.get("object"))
                if not item.get("object"):
                    prefix = credentials.get("prefix")
                    key = prefix + "simple-" + identity + "/" + item["name"] if prefix is not None else credentials["key"]
                    item["object"] = {"key": key, "bucket": credentials["bucket"], "endpoint": credentials["endpoint"]}
                elif any(item["object"][key] != credentials[key] for key in ("bucket", "endpoint")):
                    raise ValueError("object_changed")
                item.update(state="uploading", message="正在核对本条分片。" if operation == "cleanup" else "正在上传切片压缩包。")
                self._save(item)
                payload = {**item, "fileVersion": item.get("uploadFileVersion", item["fileVersion"]),
                           "sourceFileVersion": item["fileVersion"]}
                await self.worker.run(payload, credentials, lambda event: self.worker_event(identity, event), operation)
        except asyncio.CancelledError:
            item = self.store.get("items", identity)
            if self.runtime.get(identity, {}).get("stopCode") == "stalled":
                self.fail(item, "stalled")
            elif item and item["state"] not in {"uploaded", "complete"}:
                item.update(state="queued", message="上传已暂停，压缩包保留。")
                self._save(item)
            raise
        except AuditError as error:
            self.fail(self.store.get("items", identity), error.code if error.code in {"auth", "network", "disk"} else "upload_failed")
        except ValueError as error:
            self.fail(self.store.get("items", identity), str(error) if str(error) in {
                "file_changed", "object_changed", "parts_invalid", "stalled"} else "upload_failed")
        except Exception:
            self.fail(self.store.get("items", identity), "upload_failed")

    async def worker_event(self, identity, event):
        item = self.store.get("items", identity)
        runtime = self.runtime.setdefault(identity, {"lastProgress": self.clock(), "bytes": 0})
        kind, now = event.get("event"), self.clock()
        before = (item.get("phase"), item.get("uploadId"), len(item.get("parts") or {}))
        if kind == "progress":
            count = event.get("bytes")
            if type(count) is not int or not 0 <= count <= item["size"]:
                raise ValueError("parts_invalid")
            if count > runtime.get("bytes", 0):
                runtime.update(lastProgress=now, bytes=count)
            return
        if kind == "creating":
            item.update(createAttempted=True, phase="creating")
            item.setdefault("uploadFileVersion", item["fileVersion"])
        elif kind == "created":
            value = event.get("uploadId")
            if not isinstance(value, str) or not value or len(value) > 2000:
                raise ValueError("parts_invalid")
            if item.get("uploadId") and item["uploadId"] != value:
                raise ValueError("object_changed")
            item.update(uploadId=value, phase="uploading")
        elif kind == "part":
            number, size, etag = event.get("number"), event.get("size"), event.get("etag")
            if (type(number) is not int or not 1 <= number <= 10000 or type(size) is not int
                    or size <= 0 or not isinstance(etag, str) or not etag):
                raise ValueError("parts_invalid")
            item.setdefault("parts", {})[str(number)] = {"number": number, "size": size, "etag": etag}
            count = sum(p["size"] for p in item["parts"].values())
            if count > item["size"]:
                raise ValueError("parts_invalid")
            item.update(bytes=count, phase="uploading")
        elif kind == "completing":
            item.update(phase="completing", completionAttempted=True)
        elif kind == "complete":
            if event.get("size") != item["size"] or not event.get("etag"):
                raise ValueError("object_changed")
            item.update(state="uploaded", phase="uploaded", bytes=item["size"], etag=event["etag"],
                        objectVersion=event.get("versionId", ""), uploadId="", cleanupNeeded=False,
                        errorCode="", nextAttempt=0, message="压缩包已上传，等待导入。")
        elif kind == "cleaned":
            item.update(uploadId="", parts={}, bytes=0, createAttempted=False, completionAttempted=False,
                        cleanupNeeded=False, phase="queued")
            item.update(state="attention" if item.get("retries", 0) > len(RETRY_DELAYS) else "queued",
                        message="本条分片已清理，等待单独重试。")
        elif kind == "error":
            if event.get("stage") == "create_multipart_upload" and event.get("httpStatus") == 403 and not item.get("uploadId"):
                item["createAttempted"] = False
            self.fail(item, event.get("code") if event.get("code") in MESSAGES else "upload_failed")
            return
        else:
            raise ValueError("parts_invalid")
        if before != (item.get("phase"), item.get("uploadId"), len(item.get("parts") or {})):
            runtime["lastProgress"] = now
        self._save(item)

    def fail(self, item, code):
        if not item:
            return
        if code in PAUSE_CODES:
            item.update(state="queued", errorCode=code, message=safe_message(code), nextAttempt=self.clock() + CHECK_SECONDS)
        elif code in {"file_changed", "object_changed", "multipart_unknown", "parts_invalid", "authorization_binding"}:
            item.update(state="attention", errorCode=code, message=safe_message(code))
        else:
            item["retries"] = item.get("retries", 0) + 1
            cleanup = bool(item.get("uploadId") or item.get("createAttempted")) and code == "stalled"
            exhausted = item["retries"] > len(RETRY_DELAYS)
            item.update(state="queued" if cleanup or not exhausted else "attention", cleanupNeeded=cleanup,
                        errorCode=code, message=safe_message(code),
                        nextAttempt=self.clock() + RETRY_DELAYS[min(item["retries"] - 1, 2)])
        self._save(item)

    async def retry(self, identity, request_id, expected_revision):
        marker = "retry:" + request_id
        existing = self.store.get("batches", marker)
        if existing:
            if existing.get("itemId") != identity:
                raise Conflict("此请求编号已用于其他操作。")
            return self.store.get("items", identity)
        item = self.store.get("items", identity)
        if not item:
            raise ValueError("切片压缩包不存在。")
        if int(item.get("revision", 0)) != expected_revision:
            raise Conflict("记录已更新，请刷新后重试。")
        if identity in self.running or self.tick_lock.locked():
            raise Conflict("本条正在核对，请稍后重试。")
        if item["state"] == "attention":
            if item.get("errorCode") == "archive_layout":
                with self.leases.write(item["path"]):
                    destination, version, revision = await asyncio.to_thread(repair_archive, item)
                item.setdefault("archiveHistory", []).append({key:item.get(key) for key in (
                    "path", "fileVersion", "name", "object", "keys", "jobId", "etag", "beforeJobs", "beforeImageIds")})
                for key in ("object", "keys", "jobId", "etag", "beforeJobs", "beforeImageIds", "uploadFileVersion", "verifiedJobId"):
                    item.pop(key, None)
                item.update(path=str(destination), name=destination.name, size=version["size"], fileVersion=version,
                            archiveRevision=revision, parts={}, uploadId="", createAttempted=False,
                            completionAttempted=False, bytes=0, state="queued")
            elif item.get("jobId") or item.get("errorCode") == "import_uncertain":
                item["state"] = "importing" if item.get("jobId") else "submitting"
            elif item.get("etag") and item.get("object"):
                item["state"] = "uploaded"
            else:
                item["state"] = "queued"
            item.update(retries=0, nextAttempt=0, errorCode="", message="先核对已有任务和断点，再继续。")
            self._save(item)
        self.store.save("batches", {"id": marker, "itemId": identity})
        self.wake.set()
        return item

    async def submit_one(self, item, jobs):
        item = self.store.get("items", item["id"])
        if item["state"] != "uploaded" or not item.get("object"):
            return
        keys = [item["object"]["key"]]
        # Old images with the same names are not proof of this import. Keep a
        # pre-submit inventory when possible; network failures happen pre-write.
        images = await self.remote.images(item["dataset"], prefix=image_prefix(item))
        current = self.store.get("items", item["id"])
        if getattr(self, "control", None) and not self.control.package_allowed(current):
            return
        if self.paused:
            return
        item.update(state="submitting", beforeJobs=[str(job.get("id")) for job in jobs], keys=keys,
                    beforeImageIds=[str(image["id"]) for image in images],
                    message="已保存导入前任务清单，正在提交这一个压缩包。")
        self._save(item)
        try:
            identity = await self.remote.submit(item["dataset"], keys)
        except AuditError as error:
            item.update(state="importing", errorCode=error.code or "uncertain", message="导入响应不确定，只按原压缩包对账。")
            self._save(item)
            return
        self.bind(item, identity)

    def bind(self, item, identity):
        item.update(state="importing", jobId=str(identity), errorCode="", message="平台正在导入这一个切片压缩包。")
        self._save(item)

    async def reconcile(self, item, jobs):
        if not isinstance(item.get("beforeJobs"), list) or not item.get("keys"):
            item.update(state="attention", errorCode="import_uncertain", message="缺少提交前凭证，保留待核对，未重复提交。")
            self._save(item)
            return
        candidates = [job for job in jobs if str(job.get("id")) not in set(item.get("beforeJobs") or [])
                      and is_import_job(job) and source_keys(job) == frozenset(item.get("keys") or [])]
        if len(candidates) == 1:
            self.bind(item, candidates[0]["id"])
        else:
            item.update(state="attention", errorCode="import_uncertain", message="无法唯一确认这个压缩包的导入任务。")
            self._save(item)

    def _verification_current(self, item):
        # Every stored edit increments revision. An old network response may
        # reconcile its own history, but must never overwrite a newer edit.
        current = self.store.get('items', item['id'])
        return bool(current and current['revision'] == item.get('revision'))

    async def verify(self, item, job):
        if not self._verification_current(item):
            return
        from .package_import_reuse import reuse_image_ids
        try:
            reused_ids = reuse_image_ids(item, lambda path, version: file_locations.resolve(self.store.db, path, version))
        except (TypeError, ValueError):
            item.update(state='attention', errorCode='reuse_receipt_invalid', actualImages=0,
                        message='照片复用凭证不完整或版本不同，保留文件待核对。')
            self._save(item)
            return
        expected_keys = frozenset(item.get("keys") or [(item.get("object") or {}).get("key")])
        bound = job.get("dataset")
        bound = bound.get("id") if isinstance(bound, dict) else bound
        if (str(job.get("id")) != str(item.get("jobId")) or not is_import_job(job)
                or source_keys(job) != expected_keys or (bound is not None and str(bound) != item["dataset"])):
            item.update(state="attention", errorCode="import_identity", message="导入任务与绑定对象不符，未接受结果。")
            self._save(item)
            return
        if str(job.get("status")) != "3":
            item.update(state="attention", errorCode="import_failed", message="平台导入失败，压缩包和原片保留。")
            self._save(item)
            return
        images = await self.remote.images(item["dataset"], prefix=image_prefix(item))
        if not self._verification_current(item):
            return
        expected = {name_key(name) for name in item.get("images") or []}
        found = {}
        for image in images:
            name = str(image.get("name", image.get("fileName", ""))).replace("\\", "/").rsplit("/", 1)[-1]
            key = name_key(name)
            if key not in expected:
                continue
            job_id = image.get("taskId", image.get("task_id", image.get("jobId", image.get("job_id"))))
            # Exact job association or an exact new-image delta is required.
            if job_id is not None:
                owned = str(job_id) == str(item["jobId"])
            else:
                owned = isinstance(item.get("beforeImageIds"), list) and str(image["id"]) not in item["beforeImageIds"]
            if owned:
                if reused_ids is not None and str(image['id']) != reused_ids.get(key):
                    continue
                found.setdefault(key, []).append(str(image["id"]))
        if expected and len(expected) == item.get("imageCount") and all(len(found.get(name, [])) == 1 for name in expected):
            item.update(state="complete", message="图片已逐张核验。", errorCode="", actualImages=len(expected),
                        outputImageIds=[found[name][0] for name in sorted(expected)], verifiedJobId=str(item["jobId"]))
            self._save(item)
            self.delete_if_both_complete(item)
            return
        if await self.reuse_identical_import(item):
            return
        current = self.store.get('items', item['id'])
        if (not self._verification_current(item) or current.get('superseded')
                or (getattr(self, 'control', None) and not self.control.package_allowed(current))):
            return
        wrong_layout = legacy_layout(item)
        item.update(state="attention", errorCode="archive_layout" if wrong_layout else "verification", actualImages=sum(len(v) == 1 for v in found.values()),
                    message="压缩包目录格式有误，原片保留；可修复格式后重传。" if wrong_layout else
                    f"导入已结束，明确关联 {sum(len(v) == 1 for v in found.values())}/{len(expected)} 张；原片保留，可重新核对。")
        self._save(item)

    async def reuse_identical_import(self, item):
        from .package_import_reuse import reuse_candidates, identical_photos, prepare_import_reuse
        try:
            candidates = reuse_candidates(item, self.store.list('items'))
            if len(candidates) != 1:
                return False
            peer = candidates[0]
            with ExitStack() as guards:
                for path in sorted({item['path'], peer['path']}):
                    guards.enter_context(self.leases.read(path))
                equality = await finish_io(identical_photos, item, peer)
            current = self.store.get('items', item['id'])
            source = self.store.get('items', peer['id'])
            if (not equality or not current or not source or current['revision'] != item['revision']
                    or source['revision'] != peer['revision'] or current.get('superseded')
                    or (getattr(self, 'control', None) and not self.control.package_allowed(current))):
                return False
            self._save(prepare_import_reuse(current, source, equality))
            self.wake.set()
            return True
        except (OSError, TypeError, ValueError, zipfile.BadZipFile):
            return False

    def delete_if_both_complete(self, item):
        if item.get("superseded"):
            return
        if getattr(self, 'workspace', None) and self.workspace.enabled:
            self.workspace.wake.set()
            return
        video_path = item.get("videoPath") or ""
        related = [row for row in self.store.list("items") if (row.get("videoPath") or "") == video_path
                   and row.get("videoFileVersion") == item.get("videoFileVersion")]
        states = {row.get("category"): row.get("state") for row in related}
        if states.get("bed") != "complete" or states.get("magnet") != "complete":
            return
        if len(related) != 2:
            return
        if any(not row.get("verifiedJobId") or not row.get("videoFileVersion")
               or row.get("actualImages") != row.get("imageCount") or not row.get("imageVersions")
               for row in related):
            for row in related:
                row.update(cleanupState="attention", cleanupMessage="缺少原片或图片版本凭证，文件保留。")
                self._save(row)
            return
        removed = []
        try:
            # Validate the source video and the entire temporary-output deletion
            # set before removing anything. Source videos are user data and must
            # remain available after their images have been imported.
            expected = {}
            for row in related:
                expected[row["path"]] = row["fileVersion"]
                directory = checked_path(row["imageDir"])
                for name in row["images"]:
                    if Path(name).name != name:
                        raise ValueError("unsafe_image_name")
                    expected[str(directory / name)] = row["imageVersions"][name]
                if row.get("videoPath") != video_path:
                    raise ValueError("file_changed")
                if row.get("videoFileVersion") != related[0].get("videoFileVersion"):
                    raise ValueError("file_changed")
            source_video = checked_path(video_path)
            if not source_video.is_file() or source_fingerprint(source_video) != related[0]["videoFileVersion"]:
                raise ValueError("file_changed")
            for raw, version in expected.items():
                path = checked_path(raw)
                if self.leases.busy(path):
                    raise ValueError("file_busy")
                if path.exists() and source_fingerprint(path) != version:
                    raise ValueError("file_changed")
            # Only exact recorded files; no globbing user-added JPEGs.
            for raw, version in expected.items():
                path = checked_path(raw)
                with self.leases.write(path):
                    if path.exists():
                        if source_fingerprint(path) != version:
                            raise ValueError("file_changed")
                        path.unlink()
                    removed.append(raw)
        except (OSError, ValueError) as error:
            for row in related:
                row.update(cleanupState="attention", cleanupMessage="临时照片尚未清理，原片保留；请核对后重试。",
                           cleanupError=type(error).__name__)
                self._save(row)
            return
        for row in related:
            row.update(cleanupState="complete", cleanupMessage="车斗和磁盘吊都已导入，临时照片已清理，原片保留。",
                       deletedPaths=removed)
            self._save(row)
