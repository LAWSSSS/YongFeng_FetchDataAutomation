"""Disk-backed, loopback-only photo platform for isolated real-media exercises.

No production sessions are imported. Boto3's ordinary subprocess worker talks
to the HTTP S3 subset below; imports then read the bytes actually assembled by
that service. Fault flags affect only this instance and its private directory.
"""
from __future__ import annotations

import asyncio
import copy
import hashlib
import json
import shutil
import socket
import sqlite3
import threading
import time
import zipfile
from collections import defaultdict
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path, PurePosixPath
from urllib.parse import parse_qs, unquote, urlsplit
from uuid import uuid4
from xml.etree import ElementTree
from xml.sax.saxutils import escape

from app.audit_moves import source_fingerprint
from app.upload_audit import AuditError


class PhotoPlatform:
    """One disposable object store plus two independent image datasets."""

    def __init__(self, root: Path):
        self.root = Path(root).resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        (self.root / "parts").mkdir(exist_ok=True)
        (self.root / "objects").mkdir(exist_ok=True)
        self.bucket = "isolated-photos"
        self.remote = self
        self.endpoint = ""
        self.uploads: dict = {}
        self.objects: dict = {}
        self.jobs_by_dataset = defaultdict(list)
        self.image_rows = defaultdict(list)
        self.faults: dict = {}
        self.metrics = defaultdict(int)
        self.lock = threading.RLock()
        self.server = None
        self.thread = None

    def start(self):
        if not self.server:
            self.server = ThreadingHTTPServer(("127.0.0.1", 0), self._handler())
            self.server.daemon_threads = True
            self.endpoint = f"http://127.0.0.1:{self.server.server_port}"
            self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
            self.thread.start()
        return self

    def close(self):
        if self.server:
            self.server.shutdown()
            self.server.server_close()
            self.thread.join(timeout=3)
            self.server = None
        # Explicit artifact ownership, never a permission to delete neighbors.
        paths = [obj['path'] for obj in self.objects.values()]
        paths.extend(part['path'] for upload in self.uploads.values() for part in upload['parts'].values())
        registry = self.root / 'owned-media.json'
        prior = json.loads(registry.read_text()).get('paths', []) if registry.exists() else []
        registry.write_text(json.dumps({'paths': sorted(set([*prior, *paths]))}, indent=2))

    def __enter__(self):
        return self.start()

    def __exit__(self, *_):
        self.close()

    def _once(self, name):
        with self.lock:
            if self.faults.get(name):
                self.faults[name] = False
                self.metrics[f"fault:{name}"] += 1
                return True
        return False

    def _handler(self):
        owner = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_):
                pass

            def reply(self, status, body=b"", headers=None):
                if isinstance(body, str):
                    body = body.encode()
                self.send_response(status)
                self.send_header("Content-Length", str(len(body)))
                for name, value in (headers or {}).items():
                    self.send_header(name, str(value))
                self.end_headers()
                if self.command != "HEAD" and body:
                    self.wfile.write(body)

            def disconnect(self):
                self.close_connection = True
                try:
                    self.connection.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                self.connection.close()

            def dispatch(self):
                parsed = urlsplit(self.path)
                query = parse_qs(parsed.query, keep_blank_values=True)
                parts = unquote(parsed.path).lstrip("/").split("/", 1)
                bucket, key = parts[0], parts[1] if len(parts) == 2 else ""
                if bucket != owner.bucket:
                    return self.reply(404)
                upload_id = query.get("uploadId", [""])[0]
                with owner.lock:
                    owner.metrics["httpRequests"] += 1
                    obj = owner.objects.get(key)
                    upload = owner.uploads.get(upload_id)
                if self.command in {"HEAD", "GET"} and not query:
                    if not obj:
                        return self.reply(404)
                    headers = {"ETag": obj["etag"], "Accept-Ranges": "bytes",
                               **obj["metadata"]}
                    length = obj["size"]
                    start, end, status = 0, length - 1, 200
                    if self.headers.get("Range"):
                        bounds = self.headers["Range"].removeprefix("bytes=").split("-")
                        start = int(bounds[0])
                        end = min(int(bounds[1]) if bounds[1] else end, end)
                        if start > end or start < 0:
                            return self.reply(416)
                        status = 206
                        headers["Content-Range"] = f"bytes {start}-{end}/{length}"
                    self.send_response(status)
                    self.send_header("Content-Length", str(end - start + 1))
                    for name, value in headers.items():
                        self.send_header(name, value)
                    self.end_headers()
                    if self.command == "GET":
                        with Path(obj["path"]).open("rb") as source:
                            source.seek(start)
                            left = end - start + 1
                            while left:
                                data = source.read(min(left, 1024 * 1024))
                                if not data:
                                    break
                                self.wfile.write(data)
                                left -= len(data)
                    return
                if "uploads" in query:
                    if self.command == "POST":
                        identity = uuid4().hex
                        directory = owner.root / "parts" / identity
                        directory.mkdir()
                        with owner.lock:
                            owner.uploads[identity] = {"id": identity, "key": key,
                                "parts": {}, "directory": str(directory),
                                "metadata": {k.lower(): v for k, v in self.headers.items()
                                             if k.lower().startswith("x-amz-meta-")}}
                            owner.metrics["uploadsStarted"] += 1
                        return self.reply(200, f"<InitiateMultipartUploadResult><Bucket>{bucket}</Bucket>"
                            f"<Key>{escape(key)}</Key><UploadId>{identity}</UploadId></InitiateMultipartUploadResult>")
                    prefix = query.get("prefix", [""])[0]
                    with owner.lock:
                        rows = [u for u in owner.uploads.values() if u["key"].startswith(prefix)]
                    listing = "".join(f"<Upload><Key>{escape(u['key'])}</Key><UploadId>{u['id']}</UploadId></Upload>" for u in rows)
                    return self.reply(200, f"<ListMultipartUploadsResult><Bucket>{bucket}</Bucket>"
                        f"<IsTruncated>false</IsTruncated>{listing}</ListMultipartUploadsResult>")
                if not upload or upload["key"] != key:
                    return self.reply(404, "<Error><Code>NoSuchUpload</Code></Error>")
                if self.command == "GET":
                    listing = "".join(f"<Part><PartNumber>{number}</PartNumber><ETag>{escape(part['etag'])}</ETag>"
                        f"<Size>{part['size']}</Size></Part>" for number, part in sorted(upload["parts"].items()))
                    return self.reply(200, f"<ListPartsResult><Bucket>{bucket}</Bucket><Key>{escape(key)}</Key>"
                        f"<UploadId>{upload_id}</UploadId><IsTruncated>false</IsTruncated>{listing}</ListPartsResult>")
                if self.command == "DELETE":
                    with owner.lock:
                        owner.uploads.pop(upload_id, None)
                    shutil.rmtree(upload["directory"])
                    return self.reply(204)
                if self.command == "PUT":
                    number = int(query.get("partNumber", ["0"])[0])
                    if not 1 <= number <= 10000:
                        return self.reply(400)
                    length = int(self.headers.get("Content-Length", 0))
                    path = Path(upload["directory"]) / f"{number:05d}.part"
                    digest = hashlib.md5()
                    left = length
                    dropped = owner._once("drop_part_once")
                    with path.open("wb") as destination:
                        while left:
                            data = self.rfile.read(min(left, 1024 * 1024))
                            if not data:
                                break
                            destination.write(data)
                            digest.update(data)
                            left -= len(data)
                            with owner.lock:
                                owner.metrics["wireBytes"] += len(data)
                            if dropped:
                                break
                    with owner.lock:
                        owner.metrics["partRequests"] += 1
                    if dropped or left:
                        path.unlink(missing_ok=True)
                        return self.disconnect()
                    etag = '"' + digest.hexdigest() + '"'
                    with owner.lock:
                        upload["parts"][number] = {"path": str(path), "size": length, "etag": etag}
                    return self.reply(200, headers={"ETag": etag})
                if self.command == "POST":
                    manifest = ElementTree.fromstring(self.rfile.read(int(self.headers.get("Content-Length", 0))))
                    selected = []
                    for part in manifest:
                        values = {entry.tag.rsplit("}", 1)[-1]: entry.text for entry in part}
                        bound = upload["parts"].get(int(values["PartNumber"]))
                        if not bound or bound["etag"] != values["ETag"]:
                            return self.reply(400, "<Error><Code>InvalidPart</Code></Error>")
                        selected.append(bound)
                    path = owner.root / "objects" / (uuid4().hex + ".zip")
                    digest, size = hashlib.md5(), 0
                    with path.open("wb") as destination:
                        for part in selected:
                            with Path(part["path"]).open("rb") as source:
                                while data := source.read(1024 * 1024):
                                    destination.write(data)
                                    digest.update(data)
                                    size += len(data)
                    etag = '"' + digest.hexdigest() + '"'
                    with owner.lock:
                        owner.objects[key] = {"key": key, "path": str(path), "size": size,
                            "etag": etag, "metadata": upload["metadata"]}
                        owner.uploads.pop(upload_id)
                        owner.metrics["logicalBytes"] += size
                        owner.metrics["completedObjects"] += 1
                    shutil.rmtree(upload["directory"])
                    if owner._once("drop_complete_once"):
                        return self.disconnect()
                    return self.reply(200, f"<CompleteMultipartUploadResult><Bucket>{bucket}</Bucket>"
                        f"<Key>{escape(key)}</Key><ETag>{escape(etag)}</ETag></CompleteMultipartUploadResult>")
                self.reply(405)

            def handle_request(self):
                try:
                    self.dispatch()
                except (BrokenPipeError, ConnectionResetError):
                    pass

            do_GET = do_HEAD = do_POST = do_PUT = do_DELETE = handle_request

        return Handler

    async def signature(self, dataset, binding=None):
        if not self.endpoint:
            raise RuntimeError("isolated platform has not started")
        return {"endpoint": self.endpoint, "bucket": self.bucket,
                "prefix": f"exercise/{dataset}/", "accessKey": "local-exercise-only",
                "secretKey": "local-exercise-only-not-a-real-secret"}

    async def jobs(self, dataset):
        self.metrics["jobQueries"] += 1
        return copy.deepcopy(self.jobs_by_dataset[str(dataset)])

    def _read_archive(self, key, dataset, task_id):
        from PIL import Image
        obj = self.objects.get(key)
        if not obj:
            raise AuditError("未完成对象不能导入。", "missing_object")
        rows, seen = [], set()
        with zipfile.ZipFile(obj["path"]) as archive:
            for info in archive.infolist():
                if info.is_dir():
                    continue
                relative = PurePosixPath(info.filename)
                mode = (info.external_attr >> 16) & 0o170000
                if relative.is_absolute() or ".." in relative.parts or mode == 0o120000:
                    raise AuditError("压缩包包含不安全路径。", "archive_invalid")
                if relative.suffix.lower() not in {".jpg", ".jpeg", ".png", ".webp"}:
                    continue
                if relative.name in seen:
                    raise AuditError("压缩包有重名图片。", "archive_invalid")
                seen.add(relative.name)
                digest = hashlib.sha256()
                with archive.open(info) as stream:
                    while data := stream.read(1024 * 1024):
                        digest.update(data)
                        self.metrics["importBytesRead"] += len(data)
                with archive.open(info) as stream:
                    with Image.open(stream) as image:
                        width, height = image.size
                        image.verify()
                rows.append({"id": uuid4().hex, "name": relative.name, "taskId": task_id,
                    "dataset": str(dataset), "objectKey": key, "sha256": digest.hexdigest(),
                    "width": width, "height": height})
        if not rows:
            raise AuditError("压缩包没有可导入图片。", "archive_empty")
        return rows

    async def submit(self, dataset, keys):
        dataset = str(dataset)
        identity = str(uuid4().int)
        rows = []
        # ZIP parsing is genuine I/O and must not stall the async scheduler.
        for key in keys:
            rows.extend(await asyncio.to_thread(self._read_archive, key, dataset, identity))
        job = {"id": identity, "type": 1, "status": "3", "dataset": dataset,
               "config": {"source": {"files": [{"key": key} for key in keys]}}}
        self.jobs_by_dataset[dataset].append(job)
        self.image_rows[dataset].extend(rows)
        self.metrics["imports"] += 1
        self.metrics[f"imports:{dataset}"] += 1
        self.metrics["importedImages"] += len(rows)
        if self._once("lose_submit_once"):
            raise AuditError("本地模拟：接受导入后连接中断。", "network")
        return identity

    async def images(self, dataset, *, prefix=None):
        rows = copy.deepcopy([row for row in self.image_rows[str(dataset)]
                              if not prefix or row["name"].startswith(prefix)])
        self.metrics["verificationCalls"] += 1
        self.metrics["verificationRowsRead"] += len(rows)
        if self.faults.get("missing_image") and rows:
            rows.pop()
        if self.faults.get("wrong_task"):
            for row in rows:
                row["taskId"] = "wrong-task-" + row["taskId"]
        if self.faults.get("duplicate_image") and rows:
            rows.append(copy.deepcopy(rows[-1]))
        if self.faults.get("duplicate_id") and len(rows) > 1:
            for row in rows:
                row["id"] = rows[0]["id"]
        if self.faults.get('null_id'):
            for row in rows:
                row['id'] = None
        if self.faults.get('empty_id'):
            for row in rows:
                row['id'] = ''
        if self.faults.get('missing_id'):
            for row in rows:
                row.pop('id', None)
        return rows

    async def evidence(self, dataset, job):
        return {"evidence": {}, "reason": ""}


async def exercise_transfer_faults(platform: PhotoPlatform, actual_zip: Path, root: Path):
    """Run real subprocess/Boto3 transfers and production import verification.

    Returns measured report rows, including failed assertions rather than hiding
    a business defect. All object/operation/database identities are disposable.
    """
    from app.slice_packages import SlicePackageManager
    from app.uploads import ProcessWorker

    root = Path(root)
    root.mkdir(parents=True, exist_ok=True)
    actual_zip = Path(actual_zip).resolve()
    with zipfile.ZipFile(actual_zip) as archive:
        names = [Path(info.filename).name for info in archive.infolist()
                 if not info.is_dir() and Path(info.filename).suffix.lower() in {".jpg", ".jpeg", ".png", ".webp"}]
    rows = []
    worker = ProcessWorker()

    async def safe_case(node, case, action):
        started = time.monotonic()
        try:
            return await action()
        except Exception as error:
            # Exception messages can contain a signed request. Retain the type
            # only and continue with independent cases/checkpoints.
            rows.append({"node": node, "case": case, "kind": "injected_failure",
                "expected": "节点独立完成或按预期拒绝", "actual": {"exceptionType": type(error).__name__},
                "passed": False, "seconds": round(time.monotonic() - started, 3)})
            return None

    async def upload(case, fault=None):
        identity = uuid4().hex
        credentials = await platform.signature("27892")
        item = {"id": identity, "name": actual_zip.name, "path": str(actual_zip),
                "size": actual_zip.stat().st_size, "fileVersion": source_fingerprint(actual_zip),
                "object": {"bucket": platform.bucket, "key": f"exercise/simple-{identity}/{actual_zip.name}"},
                "parts": {}}
        events = []

        async def journal(event):
            events.append(event)
            kind = event["event"]
            if kind == "creating":
                item["createAttempted"] = True
            elif kind == "created":
                item["uploadId"] = event["uploadId"]
            elif kind == "part":
                item["parts"][str(event["number"])] = {key: event[key] for key in ("number", "size", "etag")}
            elif kind == "completing":
                item["completionAttempted"] = True

        if fault:
            platform.faults[fault] = True
        started = time.monotonic()
        await asyncio.wait_for(worker.run(item, credentials, journal), 180)
        first = events[-1]
        if fault == "drop_part_once" and first["event"] == "error":
            await asyncio.wait_for(worker.run(item, credentials, journal), 180)
        complete = events[-1]["event"] == "complete"
        writes = platform.metrics["partRequests"]
        await asyncio.wait_for(worker.run(item, credentials, journal), 180)
        replay_ok = events[-1]["event"] == "complete" and writes == platform.metrics["partRequests"]
        obj = platform.objects.get(item["object"]["key"])
        same = False
        if obj:
            def digest(path):
                with Path(path).open("rb") as stream:
                    return hashlib.file_digest(stream, "sha256").hexdigest()
            same = await asyncio.to_thread(digest, obj["path"]) == await asyncio.to_thread(digest, actual_zip)
        rows.append({"node": "multipart-upload", "case": case,
            "kind": "protocol_simulation" if not fault else "injected_failure",
            "expected": "真实ZIP一致；响应丢失先对账；完成后不重传",
            "actual": {"complete": complete, "sha256Equal": same, "replayNoPut": replay_ok,
                       "firstEvent": first["event"], "firstCode": first.get("code", "")},
            "passed": complete and same and replay_ok, "seconds": round(time.monotonic() - started, 3)})
        return item, credentials, journal

    result = await safe_case("multipart-upload", "positive", lambda: upload("positive"))
    await safe_case("multipart-upload", "part-disconnect", lambda: upload("part-disconnect", "drop_part_once"))
    await safe_case("multipart-upload", "complete-response-lost", lambda: upload("complete-response-lost", "drop_complete_once"))
    if not result or result[0]["object"]["key"] not in platform.objects:
        rows.append({"node": "photo-import-verification", "case": "checkpoint-unavailable",
            "kind": "protocol_simulation", "expected": "上游真实上传检查点可用",
            "actual": {"reason": "真实ZIP尚未完成上传，未伪造导入对象。"}, "passed": False, "seconds": 0})
        platform.faults.clear()
        return rows
    baseline, credentials, journal = result

    # Confirm an existing key with changed identity is never silently reused.
    obj = platform.objects[baseline["object"]["key"]]
    original_metadata = dict(obj["metadata"])
    obj["metadata"]["x-amz-meta-file-version"] = "changed-object-version"
    captured = []
    async def capture(event):
        captured.append(event)
    try:
        await safe_case("multipart-upload", "object-changed", lambda: worker.run(baseline, credentials, capture))
    finally:
        obj["metadata"] = original_metadata
    rows.append({"node": "multipart-upload", "case": "object-changed", "kind": "injected_failure",
        "expected": "object_changed，不覆盖已有对象", "actual": {key: captured[-1].get(key)
            for key in ("event", "code")} if captured else {"event": "missing"},
        "passed": bool(captured and captured[-1].get("code") == "object_changed"), "seconds": 0})

    async def import_case(fault):
        started = time.monotonic()
        db = sqlite3.connect(root / (f"import-{uuid4().hex}.sqlite3"))
        manager = SlicePackageManager(db, platform)
        try:
            await perform_import_case(fault, db, manager, started)
        finally:
            await manager.shutdown()
            db.close()

    async def perform_import_case(fault, db, manager, started):
        identity = uuid4().hex
        item = {**copy.deepcopy(baseline), "id": identity, "state": "uploaded", "dataset": "27892",
            "category": "bed", "sourceDatasetId": "27912", "sourceVideoId": identity,
            "videoPath": str(root / (identity + ".mp4")), "videoFileVersion": {"size": 1},
            "images": names, "imageCount": len(names), "etag": obj["etag"]}
        manager.store.save("items", item)
        platform.faults.clear()
        if fault == 'lose_submit_once':
            platform.faults[fault] = True
        before = platform.metrics["imports"]
        await manager.submit_one(item, await platform.jobs("27892"))
        if fault and fault != 'lose_submit_once':
            platform.faults[fault] = True
        current = manager.store.get("items", identity)
        jobs = await platform.jobs("27892")
        if not current.get("jobId"):
            await manager.reconcile(current, jobs)
            current = manager.store.get("items", identity)
        job = next((job for job in jobs if job["id"] == current.get("jobId")), None)
        if job:
            await manager.verify(current, job)
            current = manager.store.get("items", identity)
            if current["state"] == "importing":
                await manager.verify(current, job)
                current = manager.store.get("items", identity)
        valid = fault in (None, "lose_submit_once")
        accepted = current["state"] == "complete"
        unique = len(set(current.get("outputImageIds") or [])) == len(names)
        passed = (accepted and unique if valid else not accepted)
        rows.append({"node": "photo-import-verification", "case": fault or "positive",
            "kind": "protocol_simulation" if valid and not fault else "injected_failure",
            "expected": "唯一精确任务/图片核验完成" if valid else "拒绝错误照片凭证，不误报完成",
            "actual": {"state": current["state"], "errorCode": current.get("errorCode", ""),
                       "actualImages": current.get("actualImages", 0), "expectedImages": len(names),
                       "uniqueOutputIds": unique, "importsCreated": platform.metrics["imports"] - before},
            "passed": passed, "seconds": round(time.monotonic() - started, 3)})
    for fault in (None, "lose_submit_once", "missing_image", "wrong_task", "duplicate_image", "duplicate_id", 'null_id', 'empty_id', 'missing_id'):
        await safe_case("photo-import-verification", fault or "positive", lambda fault=fault: import_case(fault))

    async def two_target_case(manual_busy):
        started = time.monotonic()
        platform.faults.clear()
        db = sqlite3.connect(root / (f"dual-target-{uuid4().hex}.sqlite3"))
        manager = SlicePackageManager(db, platform)
        identities = {}
        manual = None
        try:
            for dataset, category in (("27892", "bed"), ("27893", "magnet")):
                identity = uuid4().hex
                identities[dataset] = identity
                manager.store.save("items", {**copy.deepcopy(baseline), "id": identity,
                    "state": "uploaded", "dataset": dataset, "category": category,
                    "sourceDatasetId": "27912" if category == "bed" else "27944",
                    "sourceVideoId": identity, "videoPath": str(root / (identity + ".mp4")),
                    "videoFileVersion": {"size": 1}, "images": names, "imageCount": len(names),
                    "etag": obj["etag"]})
            if manual_busy:
                manual = {"id": str(uuid4().int), "type": 1, "status": "1", "dataset": "27892",
                          "config": {"source": {"files": [{"key": "manual-only-" + uuid4().hex}]}}}
                platform.jobs_by_dataset["27892"].append(manual)
            before = platform.metrics["imports"]
            for _ in range(2):
                await asyncio.gather(manager.tick_target("27892"), manager.tick_target("27893"))
            first_states = {dataset: manager.store.get("items", identity)["state"]
                            for dataset, identity in identities.items()}
            isolated = (first_states == {"27892": "uploaded", "27893": "complete"}
                        if manual_busy else all(value == "complete" for value in first_states.values()))
            blocked_imports = platform.metrics["imports"] - before
            if manual:
                manual["status"] = "3"
                for _ in range(2):
                    await manager.tick_target("27892")
            final_states = {dataset: manager.store.get("items", identity)["state"]
                            for dataset, identity in identities.items()}
            passed = (isolated and all(value == "complete" for value in final_states.values())
                      and platform.metrics["imports"] - before == 2
                      and (not manual_busy or blocked_imports == 1))
            rows.append({"node": "photo-import-scheduling",
                "case": "manual-import-busy" if manual_busy else "two-target-independent",
                "kind": "protocol_contract",
                "expected": "人工导入只阻止同目标；另一目标完成；释放后续接且只提交两次" if manual_busy
                            else "两个目标各自提交并核验真实ZIP图片，互不阻塞",
                "actual": {"initialStates": first_states, "finalStates": final_states,
                           "importsBeforeRelease": blocked_imports,
                           "importsCreated": platform.metrics["imports"] - before},
                "passed": passed, "seconds": round(time.monotonic() - started, 3)})
        finally:
            if manual:
                platform.jobs_by_dataset["27892"].remove(manual)
            await manager.shutdown()
            db.close()

    for manual_busy in (False, True):
        await safe_case("photo-import-scheduling", "manual-import-busy" if manual_busy else "two-target-independent",
                        lambda manual_busy=manual_busy: two_target_case(manual_busy))

    async def restart_completed_upload():
        # Simulate a crash after the object completed, before the parent saved
        # the final upload event. The next worker must HEAD the same version.
        started = time.monotonic()
        platform.faults.clear()
        db = sqlite3.connect(root / (f'restart-object-{uuid4().hex}.sqlite3'))
        manager = SlicePackageManager(db, platform)
        # Object metadata binds the upload's original ID as well as bytes.
        # A restart keeps that identity; a new UUID must be rejected.
        identity = baseline['id']
        credentials = await platform.signature('27892')
        item = {**copy.deepcopy(baseline), 'id': identity, 'dataset': '27892', 'category': 'bed',
                'state': 'uploading', 'phase': 'completing', 'completionAttempted': True,
                'object': {**baseline['object'], 'endpoint': credentials['endpoint']},
                'sourceDatasetId': '27912', 'sourceVideoId': identity,
                'videoPath': str(root / (identity + '.mp4')), 'videoFileVersion': {'size': 1},
                'images': names, 'imageCount': len(names)}
        manager.store.save('items', item)
        manager.pause()  # Recovery also preserves a prior explicit pause.
        before = platform.metrics['partRequests']
        try:
            await manager.start()
            restored = manager.store.get('items', identity)
            restored_ok = (restored['state'] == 'queued' and restored['phase'] == 'reconciling'
                           and restored['object']['key'] == baseline['object']['key']
                           and restored.get('parts') == baseline.get('parts') and manager.paused)
            manager.resume()
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                current = manager.store.get('items', identity)
                if current['state'] in {'uploaded', 'submitting', 'importing', 'complete', 'attention'}:
                    break
                await asyncio.sleep(.02)
            current = manager.store.get('items', identity)
            reused = current['state'] in {'uploaded', 'submitting', 'importing', 'complete'}
            rows.append({'node': 'multipart-upload', 'case': 'restart-after-object-complete',
                'kind': 'real_worker_protocol_simulation',
                'expected': '重启恢复uploading，同一对象HEAD对账成功，无新增分片，保留暂停意图',
                'actual': {'state': current['state'], 'restoredReceipts': restored_ok,
                           'newPartRequests': platform.metrics['partRequests'] - before},
                'passed': restored_ok and reused and platform.metrics['partRequests'] == before,
                'seconds': round(time.monotonic() - started, 3)})
        finally:
            await manager.shutdown()
            db.close()

    await safe_case('multipart-upload', 'restart-after-object-complete', restart_completed_upload)
    platform.faults.clear()
    return rows
