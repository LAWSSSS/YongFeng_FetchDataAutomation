"""Upload one slice archive per category, then import one package at a time."""
import asyncio
import json
import zipfile
from collections import Counter
from contextlib import ExitStack
from pathlib import Path

from .audit_moves import checked_path, source_fingerprint
from .domain import now_iso
from .file_leases import FileEnvironmentError, FileLeases, ReadProbes, finish_io
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
        self.target_tasks = {}
        self.target_next_check = {}
        self.runtime = {}
        self.wake = asyncio.Event()
        self.file_checks = ReadProbes(self.wake.set)
        self.waiting_paths = set()
        self.event_loop = None
        self.stopping = False
        self.loop_task = None
        self.leases = leases or FileLeases()
        self.tick_lock = asyncio.Lock()
        self.paused = bool((self.store.get("batches", "control") or {}).get("paused"))
        self.on_change = lambda item: None
        self.leases.subscribe_moves(self.moved)
        self.leases.subscribe_release(self._file_released)

    def _file_released(self, path):
        if self.stopping or path not in self.waiting_paths or not self.event_loop or self.event_loop.is_closed():
            return
        self.event_loop.call_soon_threadsafe(self._resume_released, path)

    def _resume_released(self, path):
        if self.stopping:
            return
        self.waiting_paths.discard(path)
        for item in self.store.list('items'):
            if item.get('path') == path and item.get('state') == 'queued' and item.get('reasonCode') == 'file_busy':
                item.update(nextAttempt=0, reasonCode='', errorCode='', message='文件已释放，继续上传。')
                self._save(item)
        self.wake.set()

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

    def queue_verification_retry(self, item):
        """Recheck a bound import once; this never creates another upload or import."""
        job = str(item.get('jobId') or '')
        keys = item.get('keys') or [(item.get('object') or {}).get('key')]
        if (not job or not all(keys) or item.get('verificationAutoRetryForJob') == job
                or item.get('superseded') or (getattr(self, 'control', None) and not self.control.package_allowed(item, allow_paused=True))):
            return False
        item.update(state='importing', errorCode='verification_pending', verificationAutoRetryForJob=job,
                    verificationNotBefore=self.clock()+30,
                    message='图片结果尚未齐全，将自动重新核对已有导入任务。')
        self._save(item)
        return True

    def pause(self):
        self.paused = True
        self.store.save("batches", {"id": "control", "paused": True})
        for task in self.running.values():
            task.cancel()
        for dataset, task in self.target_tasks.items():
            if not any(item.get('dataset') == dataset and item.get('state') in {'submitting', 'importing'}
                       for item in self.store.list('items')):
                task.cancel()
        self.wake.set()

    def resume(self):
        if self.paused:
            self.paused = False
            self.store.save("batches", {"id": "control", "paused": False})
        self.wake.set()
        self.target_next_check.clear()

    def state(self, summary=False, item_ids=None):
        # Summary polling never deserializes image inventories or submission
        # snapshots. Detail callers can request only their visible source IDs.
        if summary or item_ids is not None:
            projection = [dict(zip(('id', 'itemId', 'category', 'state', 'dataset'), row))
                          for row in self.store.db.execute("""SELECT id,
                json_extract(payload,'$.itemId'),json_extract(payload,'$.category'),
                json_extract(payload,'$.state'),json_extract(payload,'$.dataset')
                FROM simple_slice_package_items ORDER BY rowid""")]
            selected = None if item_ids is None else set(item_ids)
            if summary:
                items = [row for row in projection if selected is None or row.get('itemId') in selected]
            elif not selected:
                items = []
            else:
                placeholders = ','.join('?' for _ in selected)
                items = [json.loads(row[0]) for row in self.store.db.execute(
                    "SELECT payload FROM simple_slice_package_items WHERE json_extract(payload,'$.itemId') IN ("
                    + placeholders + ') ORDER BY rowid', tuple(selected))]
        else:
            items = self.store.list("items")
            projection = items
        targets = []
        for dataset in PACKAGE_TARGETS:
            rows = [item for item in projection if item.get("dataset") == dataset["id"]]
            targets.append({**dataset,
                            "importing": sum(item.get("state") == "importing" for item in rows),
                            "uploading": sum(item.get("state") == "uploading" for item in rows),
                            "queued": sum(item.get("state") in {"queued", "uploaded", "review"} for item in rows)})
        # Object addresses and submit snapshots are internal recovery receipts.
        fields = {"id", "revision", "dataset", "category", "sourceDatasetId", "sourceVideoId", "itemId",
                  "videoPath", "path", "imageDir", "name", "state", "message", "errorCode",
                  "imageCount", "actualImages", "bytes", "size", "retries", "jobId", "cleanupState",
                  "cleanupMessage", "createdAt", "updatedAt", "superseded", "localCleanupState"}
        fields.add('reasonCode')
        if summary:
            fields = {'id', 'itemId', 'category', 'state'}
        public = [{key: value for key, value in item.items() if key in fields} for item in items]
        if not summary:
            for target in targets:
                target["items"] = [item for item in public if item.get("dataset") == target["id"]]
        return {"targets": targets, "items": public, "paused": self.paused,
                "busy": bool(self.running) or any(not task.done() for task in self.target_tasks.values()) or self.tick_lock.locked()}

    def remember(self, record):
        if getattr(self, "control", None) and not self.control.package_allowed(record, allow_paused=True):
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
            # A process restart loses only the in-memory worker, not its bound
            # object or multipart journal. Let the ordinary worker reconcile
            # HEAD/parts before uploading; submitted imports never go backwards.
            for item in self.store.list('items'):
                active = self.running.get(item['id'])
                if item.get('state') == 'uploading' and (not active or active.done()):
                    item.update(state='queued', phase='reconciling',
                                message='已恢复上传断点，先核对原对象及分片。')
                    self._save(item)
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
        for task in self.target_tasks.values():
            task.cancel()
        if self.target_tasks:
            await asyncio.gather(*self.target_tasks.values(), return_exceptions=True)

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
        if self.tick_lock.locked():
            return
        async with self.tick_lock:
            await self._tick()

    async def _tick(self):
        now = self.clock()
        for identity, task in list(self.running.items()):
            runtime = self.runtime.get(identity, {})
            if not task.done() and now - runtime.get("lastProgress", now) >= STALL_SECONDS:
                runtime["stopCode"] = ("disk" if getattr(self, 'workspace', None)
                                       and self.workspace.blocked else "stalled")
                task.cancel()
        if getattr(self, 'workspace', None) and self.workspace.blocked:
            reason = getattr(self.workspace, 'availability', {}).get('reasonCode') or 'disk_offline'
            for item in self.store.list('items'):
                if item.get('state') == 'queued' and item['id'] not in self.running and item.get('reasonCode') != reason:
                    item.update(errorCode='disk', reasonCode=reason,
                                message='等待原工作区磁盘恢复；照片和上传断点保留。')
                    self._save(item)
        if not any(item.get("state") in {"queued", "uploaded", "uploading", "importing", "submitting"}
                   for item in self.store.list("items")):
            return
        # Each target owns one import/poll task. A slow target cannot hold the
        # other target's next admission or the local upload watchdog lock.
        items = self.store.list('items')
        for target in PACKAGE_TARGETS:
            dataset = target['id']
            if self.paused and not any(item.get('dataset') == dataset and item.get('state') in
                                      {'submitting', 'importing'} for item in items):
                continue
            self._launch_next(dataset, items)
            task = self.target_tasks.get(dataset)
            if task and not task.done():
                continue
            if now < self.target_next_check.get(dataset, 0):
                continue
            if not any(item.get('dataset') == dataset and item.get('state') in
                       {'queued', 'uploading', 'uploaded', 'submitting', 'importing'} for item in items):
                continue
            self.target_tasks[dataset] = asyncio.create_task(self._run_target(dataset), name='photo-import-' + dataset)
        for _ in range(3):
            if all(task.done() for task in self.target_tasks.values()):
                break
            await asyncio.sleep(0)

    async def _run_target(self, dataset):
        try:
            await self.tick_target(dataset)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            code = error.code if isinstance(error, AuditError) else 'network'
            for item in self.store.list('items'):
                if item.get('dataset') == dataset and item.get('state') in {'queued', 'uploaded', 'submitting', 'importing'}:
                    if item.get('state') == 'queued' and getattr(self, 'workspace', None) and self.workspace.blocked:
                        continue
                    item.update(errorCode=code, message=safe_message(code))
                    self._save(item)
        finally:
            self.target_next_check[dataset] = self.clock() + CHECK_SECONDS

    async def tick_target(self, dataset):
        items = [item for item in self.store.list("items") if item.get("dataset") == dataset]
        if not items:
            return
        # Uploading local photos does not depend on a remote import list.
        # Start its independent worker before a possibly slow platform poll.
        self._launch_next(dataset, items)
        jobs = await self.remote.jobs(dataset)
        active = [job for job in jobs if str(job.get("status")) in {"1", "2"}]
        for item in items:
            if item["state"] in {"submitting", "importing"} and not item.get("jobId"):
                await self.reconcile(item, jobs)
                continue
            if item["state"] != "importing" or not item.get("jobId"):
                continue
            if item.get('verificationNotBefore', 0) > self.clock():
                continue
            job = next((row for row in jobs if str(row.get("id")) == str(item["jobId"])), None)
            if job and str(job.get("status")) in {"3", "4", "5"}:
                await self.verify(item, job)
        if self.paused:
            return
        busy = bool(active) or any(item.get("state") in {"submitting", "importing"}
                                    or item.get("errorCode") in {"import_uncertain", "import_identity"}
                                    for item in self.store.list("items") if item.get("dataset") == dataset)
        if not busy:
            ready = next((item for item in self.store.list("items")
                          if item.get("dataset") == dataset and item.get("state") == "uploaded"
                          and (not getattr(self, "control", None) or self.control.package_allowed(item))), None)
            if ready:
                await self.submit_one(ready, jobs)
        self._launch_next(dataset, self.store.list('items'))

    def _launch_next(self, dataset, items):
        # Task completion callbacks run on a later loop turn. A caller can
        # already observe a finished task before that callback releases it.
        for identity, task in list(self.running.items()):
            if task.done():
                row = self.store.get('items', identity) or {}
                self._release_upload_task(identity, task, row.get('dataset'))
        # Local media requires the disk. Already-submitted imports and watchdogs
        # continue independently, using their persisted remote receipts.
        if getattr(self, 'workspace', None) and self.workspace.blocked:
            return
        if self.paused or any(item.get('dataset') == dataset and item['id'] in self.running for item in items):
            return
        now = self.clock()
        for item in items:
            if self.paused:
                return
            if item.get("dataset") != dataset or item.get("state") != "queued" or item["id"] in self.running:
                continue
            if getattr(self, "control", None) and not self.control.package_allowed(item):
                continue
            if item.get("nextAttempt", 0) > now:
                continue
            self.launch(item)
            break

    def launch(self, item):
        identity = item["id"]
        self.runtime[identity] = {"lastProgress": self.clock(), "bytes": item.get("bytes", 0)}
        task = asyncio.create_task(self.upload_one(identity), name="slice-package-" + identity)
        self.running[identity] = task
        def finished(_done):
            self._release_upload_task(identity, _done, item['dataset'])
        task.add_done_callback(finished)

    def _release_upload_task(self, identity, task, dataset):
        # A delayed callback from an old attempt cannot release a newer one.
        if self.running.get(identity) is not task:
            return
        self.running.pop(identity, None)
        self.runtime.pop(identity, None)
        if dataset is not None:
            self.target_next_check[dataset] = 0
        self.wake.set()

    async def upload_one(self, identity):
        self.event_loop = asyncio.get_running_loop()
        item = self.store.get("items", identity)
        if getattr(self, "control", None) and not self.control.package_allowed(item):
            return
        try:
            operation = "cleanup" if item.get("cleanupNeeded") else "upload"
            with self.leases.read(item["path"]):
                if operation == "upload":
                    try:
                        def inspect():
                            # A timed-out stat may still be running. Its own
                            # lease prevents cleanup until that read finishes.
                            with self.leases.read(item['path']):
                                return source_fingerprint(Path(item['path']))
                        current = await self.file_checks.run(item['path'], inspect)
                    except OSError:
                        raise AuditError("外置盘或压缩包暂不可用。", "disk") from None
                    if current != item["fileVersion"]:
                        raise ValueError("file_changed")
                credentials = await self.remote.signature(item["dataset"], item.get("object"))
                if (not self._verification_current(item) or item.get('superseded')
                        or (getattr(self, 'control', None) and not self.control.package_allowed(item))):
                    return
                if not item.get("object"):
                    prefix = credentials.get("prefix")
                    key = prefix + "simple-" + identity + "/" + item["name"] if prefix is not None else credentials["key"]
                    item["object"] = {"key": key, "bucket": credentials["bucket"], "endpoint": credentials["endpoint"]}
                elif any(item["object"][key] != credentials[key] for key in ("bucket", "endpoint")):
                    raise ValueError("object_changed")
                item.update(state="uploading", message="正在核对本条分片。" if operation == "cleanup" else "正在上传切片压缩包。")
                item['reasonCode'] = ''
                self._save(item)
                payload = {**item, "fileVersion": item.get("uploadFileVersion", item["fileVersion"]),
                           "sourceFileVersion": item["fileVersion"]}
                await self.worker.run(payload, credentials, lambda event: self.worker_event(identity, event), operation)
        except asyncio.CancelledError:
            item = self.store.get("items", identity)
            stop_code = self.runtime.get(identity, {}).get("stopCode")
            if stop_code in {"stalled", "disk"}:
                self.fail(item, stop_code)
            elif item and item["state"] in {"queued", "uploading"}:
                item.update(state="queued", message="上传已暂停，压缩包保留。")
                self._save(item)
            raise
        except AuditError as error:
            self.fail(self.store.get("items", identity), error.code if error.code in {"auth", "network", "disk"} else "upload_failed")
        except FileEnvironmentError as error:
            if error.code == 'file_busy':
                self.waiting_paths.add(item['path'])
            self.fail(self.store.get("items", identity), "disk", reason_code=error.code)
        except ValueError as error:
            self.fail(self.store.get("items", identity), str(error) if str(error) in {
                "file_changed", "object_changed", "parts_invalid", "stalled"} else "upload_failed")
        except Exception:
            self.fail(self.store.get("items", identity), "upload_failed")

    async def worker_event(self, identity, event):
        item = self.store.get("items", identity)
        if not item:
            return
        # Completion acknowledgement and process exit are separate awaits. An
        # import may already have started, or correction may own this history.
        # No late upload callback can revert those facts or revive an old file.
        if (item.get('superseded') or item.get('state') in {'uploaded', 'submitting', 'importing', 'complete', 'superseded', 'dismissed'}
                or (getattr(self, 'control', None) and not self.control.package_allowed(item, allow_paused=True))):
            return
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
                        errorCode="", reasonCode="", nextAttempt=0, message="压缩包已上传，等待导入。")
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

    def fail(self, item, code, *, reason_code=None):
        if not item:
            return
        if (item.get('superseded') or item.get('state') in {'uploaded', 'submitting', 'importing', 'complete', 'superseded', 'dismissed'}
                or (getattr(self, 'control', None) and not self.control.package_allowed(item, allow_paused=True))):
            return
        if code in PAUSE_CODES:
            item.update(state="queued", errorCode=code, message=safe_message(code), nextAttempt=self.clock() + CHECK_SECONDS)
            item['reasonCode'] = reason_code or ('disk_io' if code == 'disk' else code)
            if reason_code == 'file_busy':
                item['message'] = '文件正在使用，释放后自动继续；上传断点保留。'
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
            item.update(retries=0, nextAttempt=0, verificationNotBefore=0, errorCode="", message="先核对已有任务和断点，再继续。")
            self._save(item)
        self.store.save("batches", {"id": marker, "itemId": identity})
        self.wake.set()
        return item

    async def submit_one(self, item, jobs):
        if self.paused or (getattr(self, 'workspace', None) and getattr(self.workspace, 'maintenance', False)):
            return
        item = self.store.get("items", item["id"])
        if item["state"] != "uploaded" or not item.get("object"):
            return
        keys = [item["object"]["key"]]
        # Old images with the same names are not proof of this import. Keep a
        # pre-submit inventory when possible; network failures happen pre-write.
        try:
            images = await self.remote.images(item["dataset"], prefix=image_prefix(item))
        except AuditError as error:
            if error.code in {'auth', 'network', 'disk'}:
                raise
            if self._verification_current(item):
                item.update(state='attention', errorCode='image_identity',
                            verificationFailureType=error.code,
                            message='平台图片清单暂不能确认身份，未提交导入；请核对后继续。')
                self._save(item)
            return
        current = self.store.get("items", item["id"])
        if (not self._verification_current(item) or current.get('state') != 'uploaded'
                or current.get('superseded')):
            return
        if getattr(self, "control", None) and not self.control.package_allowed(current):
            return
        if self.paused:
            return
        from .package_import_reuse import image_identity
        before_ids = [image_identity(image.get('id')) for image in images]
        if any(identity is None for identity in before_ids) or len(set(before_ids)) != len(before_ids):
            item.update(state='attention', errorCode='image_identity',
                        message='平台图片ID缺失或重复，未提交导入；压缩包和原片保留，请核对后继续。')
            self._save(item)
            return
        item.update(state="submitting", beforeJobs=[str(job.get("id")) for job in jobs], keys=keys,
                    beforeImageIds=before_ids,
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
        from .package_import_reuse import image_identity, reuse_image_ids
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
        try:
            images = await self.remote.images(item["dataset"], prefix=image_prefix(item))
        except AuditError as error:
            if error.code in {'auth', 'network', 'disk'}:
                raise
            item['verificationFailureType'] = error.code
            self._record_verification_failure(item, item.get('imageCount', 0), 0)
            return
        if not self._verification_current(item):
            return
        expected = {name_key(name) for name in item.get("images") or []}
        found = {}
        for image in images:
            name = str(image.get("name", image.get("fileName", ""))).replace("\\", "/").rsplit("/", 1)[-1]
            key = name_key(name)
            if key not in expected:
                continue
            identity = image_identity(image.get('id'))
            if identity is None:
                continue
            job_id = image.get("taskId", image.get("task_id", image.get("jobId", image.get("job_id"))))
            # Exact job association or an exact new-image delta is required.
            if job_id is not None:
                owned = str(job_id) == str(item["jobId"])
            else:
                owned = isinstance(item.get("beforeImageIds"), list) and identity not in item["beforeImageIds"]
            if owned:
                if reused_ids is not None and identity != reused_ids.get(key):
                    continue
                found.setdefault(key, []).append(identity)
        identity_counts = Counter(identity for identities in found.values() for identity in identities)
        verified = {name: identities[0] for name, identities in found.items()
                    if len(identities) == 1 and identity_counts[identities[0]] == 1}
        if expected and len(expected) == item.get("imageCount") and set(verified) == expected:
            item.update(state="complete", message="图片已逐张核验。", errorCode="", actualImages=len(expected),
                        outputImageIds=[verified[name] for name in sorted(expected)], verifiedJobId=str(item["jobId"]))
            self._save(item)
            self.delete_if_both_complete(item)
            return
        if await self.reuse_identical_import(item):
            return
        self._record_verification_failure(item, len(expected), len(verified), legacy_layout(item))

    def _record_verification_failure(self, item, expected_count, actual_count, wrong_layout=False):
        current = self.store.get('items', item['id'])
        if (not self._verification_current(item) or current.get('superseded')
                or (getattr(self, 'control', None) and not self.control.package_allowed(current, allow_paused=True))):
            return
        item['actualImages'] = actual_count
        job = str(item.get('jobId') or '')
        item['verificationFailures'] = (int(item.get('verificationFailures') or 0)
            if item.get('verificationFailureJob') == job else 0) + 1
        item['verificationFailureJob'] = job
        if not wrong_layout and self.queue_verification_retry(item):
            return
        item.update(state="attention", errorCode="archive_layout" if wrong_layout else "verification", actualImages=actual_count,
                    message="压缩包目录格式有误，原片保留；可修复格式后重传。" if wrong_layout else
                    f"导入已结束，唯一关联 {actual_count}/{expected_count} 张；图片ID、数量或任务归属不符，原片保留，可重新核对。")
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
                    or (getattr(self, 'control', None) and not self.control.package_allowed(current, allow_paused=True))):
                return False
            self._save(prepare_import_reuse(current, source, equality))
            self.wake.set()
            return True
        except (OSError, TypeError, ValueError, zipfile.BadZipFile):
            return False

    def delete_if_both_complete(self, item):
        """Import completion never authorizes local deletion.

        Retain the legacy callback name for callers; only the explicit workspace
        cleanup operation may remove originals, photographs or their archives.
        """
        if item.get('state') != 'complete' or item.get('superseded') or item.get('localCleanupState') == 'complete':
            return
        if item.get('cleanupState') != 'complete':
            item.update(cleanupState='pending_manual', cleanupMessage='照片已导入，等待手动清理。')
            self._save(item)
        workspace = getattr(self, 'workspace', None)
        if workspace and workspace.enabled:
            workspace.wake.set()
