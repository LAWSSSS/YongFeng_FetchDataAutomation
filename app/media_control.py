"""One durable authority for collection versions and their local slicing work."""
import asyncio
import hashlib
import json
from pathlib import Path
from urllib.parse import urlencode

from .domain import now_iso
from .videos import Conflict


TERMINAL = {"complete", "no_target", "excluded", "superseded"}


class MediaControl:
    def __init__(self, db, videos, slicing, leases):
        self.db, self.videos, self.slicing, self.leases = db, videos, slicing, leases
        self.db.execute("CREATE TABLE IF NOT EXISTS simple_media_controls (id TEXT PRIMARY KEY, payload TEXT NOT NULL)")
        self.db.execute("CREATE TABLE IF NOT EXISTS simple_media_actions (id TEXT PRIMARY KEY, payload TEXT NOT NULL)")
        self.db.commit()
        self.locks = {}
        leases.subscribe_moves(self.moved)
        self.reconcile_finished_controls()
        self.refresh()

    def reconcile_finished_controls(self):
        """Recover a crash between the operation receipt and releasing its gate."""
        for (raw,) in self.db.execute("SELECT payload FROM simple_media_controls").fetchall():
            row = json.loads(raw)
            if not row.get("blocked") or not row.get("requestId"):
                continue
            operation = self.videos.store.get_operation(row["requestId"])
            if (operation and operation.get("videoId") == row.get("videoId")
                    and operation.get("state") in {"complete", "attention", "failed", "conflict", "cancelled"}):
                row.update(blocked=False, owner="review")
                self.save(row)

    async def verify_collection_file(self, item, path):
        """A path change must not silently change the accepted collection source."""
        from .file_leases import finish_io
        from .media import file_digest
        from .slicing import source_version
        video_id = item.get("collectionVideoId")
        if not video_id:
            return
        row = self.videos.store.get(video_id)
        version = self.videos.version(row, item.get("collectionRevision")) if row else None
        if not version or row.get("activeRevision") != item.get("collectionRevision"):
            raise Conflict("对应采集版本已变化，请刷新后重新绑定。")
        with self.leases.read(path):
            before = await finish_io(source_version, path)
            expected = version.get("fileVersion") or version.get("recovery", {}).get("fileVersion") or {}
            if not self.same_version(expected, before):
                digest = version.get("sha256") or version.get("recovery", {}).get("sha256")
                if not digest or await finish_io(file_digest, Path(path)) != digest:
                    raise Conflict("原片与采集版本不一致，未修改绑定。")
            if await finish_io(source_version, path) != before:
                raise Conflict("原片在核对时发生变化，未修改绑定。")
        latest = self.videos.store.get(video_id)
        if not latest or latest.get("revision") != row.get("revision") or not self.allowed(item):
            raise Conflict("采集指令已变化，请刷新后重新绑定。")

    def get(self, identity):
        row = self.db.execute("SELECT payload FROM simple_media_controls WHERE id=?", (identity,)).fetchone()
        return json.loads(row[0]) if row else None

    def save(self, row):
        row["updatedAt"] = now_iso()
        self.db.execute("INSERT INTO simple_media_controls VALUES(?,?) ON CONFLICT(id) DO UPDATE SET payload=excluded.payload",
                        (row["id"], json.dumps(row, ensure_ascii=False)))
        self.db.commit()

    @staticmethod
    def same_version(left, right):
        keys = {"device", "inode", "size", "modifiedNs", "edgeHash"} & left.keys() & right.keys()
        return "size" in keys and len(keys) >= 3 and all(left[k] == right[k] for k in keys)

    def refresh(self):
        index = {}
        for row in self.videos.store.list(include_deleted=True):
            for version in row.get("versions", []):
                recovery = version.get("recovery") or {}
                for path, fingerprint in ((version.get("filePath"), version.get("fileVersion") or {}),
                                          (recovery.get("filePath") or recovery.get("destination"), recovery.get("fileVersion") or {})):
                    if path:
                        index.setdefault(path, []).append((row["id"], version["revision"], fingerprint))
        items = self.slicing.store.list("items")
        physical = {}
        for item in items:
            if item.get("mediaId") and not item.get("superseded"):
                physical.setdefault(self.physical_key(item), []).append(item)
        for item in items:
            original = json.dumps(item, sort_keys=True)
            if not item.get("mediaId") or (item["mediaId"].startswith("local:") and not item.get("superseded")):
                matches = {(identity, revision) for identity, revision, version in index.get(item.get("path"), [])
                           if self.same_version(version, item.get("fileVersion") or {})}
                if item.get("collectionVideoId"):
                    matches = {(item["collectionVideoId"], item["collectionRevision"])}
                if len(matches) > 1:
                    item.update(identityConflict=True, processingReason="原片对应多个采集版本，请人工绑定。")
                    matches = set()
                if matches:
                    identity, revision = next(iter(matches))
                    item.update(collectionVideoId=identity, collectionRevision=revision, mediaId="collection:" + identity)
                else:
                    version = item.get("fileVersion") or {"unboundPath": item.get("path") or item["id"]}
                    content = {"version": version}
                    if not {"device", "inode", "size", "modifiedNs"}.issubset(version):
                        content["path"] = item.get("path") or item["id"]
                    digest = hashlib.sha256(json.dumps(content, sort_keys=True).encode()).hexdigest()[:32]
                    peers = physical.get(self.physical_key(item), [])
                    identities = {peer["mediaId"] for peer in peers}
                    if len(identities) == 1:
                        peer = peers[0]
                        for key in ("mediaId", "mediaGeneration", "collectionVideoId", "collectionRevision"):
                            if key in peer:
                                item[key] = peer[key]
                    else:
                        item["mediaId"] = "local:" + digest
            control = self.get(item["mediaId"])
            if not control:
                control = {"id": item["mediaId"], "videoId": item.get("collectionVideoId"), "generation": 0,
                           "owner": "slicing", "blocked": False, "createdAt": now_iso(), "paths": []}
            if item.get("path") and item["path"] not in control.setdefault("paths", []):
                control["paths"].append(item["path"])
                self.save(control)
            elif not self.get(control["id"]):
                self.save(control)
            item.setdefault("mediaGeneration", control["generation"])
            if original != json.dumps(item, sort_keys=True):
                self.slicing.store.save("items", item)
            physical.setdefault(self.physical_key(item), []).append(item)

    @staticmethod
    def physical_key(item):
        version = item.get("fileVersion") or {}
        key = json.dumps(version, sort_keys=True)
        if not {"device", "inode", "size", "modifiedNs"}.issubset(version):
            key += ":" + str(item.get("path") or item["id"])
        return key

    def moved(self, source, destination, version):
        for (raw,) in self.db.execute("SELECT payload FROM simple_media_controls").fetchall():
            row = json.loads(raw)
            if source in row.get("paths", []) and destination not in row["paths"]:
                row["paths"].append(destination)
                self.save(row)

    def reason(self, item):
        if item.get("identityConflict"):
            return item.get("processingReason") or "原片身份需要核对。"
        control = self.get(item.get("mediaId", ""))
        if item.get("superseded") or (control and item.get("mediaGeneration", 0) != control["generation"]):
            return "旧版处理已停止，按视频查看与更正中的指令执行。"
        if control and control.get("blocked"):
            return "视频查看与更正正在处理此视频。"
        video_id = item.get("collectionVideoId")
        if video_id:
            row = self.videos.store.get(video_id)
            revision = item.get("collectionRevision")
            if not row or row.get("deletedAt") or row.get("activeRevision") != revision:
                return "此采集版本已撤回或被更正，停止旧版切片。"
            version = self.videos.version(row, revision) or {}
            if not item.get("collectionRecovery") and (row.get("acceptedRevision") != revision or version.get("state") != "saved"):
                return "等待当前版本经人工确认并下载完成。"
        return ""

    def allowed(self, item):
        return not self.reason(item)

    def package_allowed(self, package):
        item = self.slicing.store.get("items", package.get("itemId"))
        return not package.get("superseded") and (not item or self.allowed(item))

    def claim(self, item):
        if not self.allowed(item):
            raise Conflict(self.reason(item))
        row = self.get(item.get("mediaId", ""))
        if row and row.get("owner") != "slicing":
            row["owner"] = "slicing"
            self.save(row)

    def reserve(self, video_id, request_id):
        self.refresh()
        identity = "collection:" + video_id
        row = self.get(identity) or {"id": identity, "videoId": video_id, "generation": 0, "paths": []}
        if row.get("requestId") != request_id:
            row.update(generation=row["generation"] + 1, requestId=request_id, owner="review", blocked=True)
            self.save(row)
        return row["generation"]

    async def take_over(self, video_id, request_id, expected_generation=None):
        if expected_generation is None:
            self.reserve(video_id, request_id)
        self.refresh()
        identity = "collection:" + video_id
        row = self.get(identity)
        if (not row or (expected_generation is not None and
                (row["generation"] != expected_generation or row.get("requestId") != request_id))):
            raise Conflict("已有更新的视频指令，本次旧操作已停止。")
        tasks = []
        affected = []
        for item in self.slicing.store.list("items"):
            if item.get("mediaId") != identity or item.get("mediaGeneration", 0) >= row["generation"]:
                continue
            affected.append(item["id"])
            item.update(superseded=True, state=item["state"] if item.get("state") in TERMINAL else "superseded",
                        message="旧版处理已停止，等待更正版确认。")
            self.slicing.store.save("items", item)
            for task in (self.slicing.analysis_tasks.get(item["id"]), self.slicing.export_tasks.get(item["id"])):
                if task and not task.done():
                    task.cancel(); tasks.append(task)
        for package in self.slicing.packages.store.list("items"):
            if package.get("itemId") not in affected:
                continue
            package["superseded"] = True
            submitted = package.get("jobId") or package.get("state") in {"submitting", "importing", "complete"}
            if not submitted:
                package.update(state="superseded", message="旧版照片包保留，不再上传或导入。")
                task = self.slicing.packages.running.get(package["id"])
                if task and not task.done():
                    task.cancel(); tasks.append(task)
            self.slicing.packages._save(package)
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self.require_command(identity, request_id, row["generation"])
        download = self.videos.running.get(video_id)
        if download and download is not asyncio.current_task() and not download.done() and download.get_name() != "clear-record":
            download.cancel()
            await asyncio.gather(download, return_exceptions=True)
        self.require_command(identity, request_id, row["generation"])
        recovery_tasks = []
        for operation in self.videos.store.list_operations(video_id):
            if operation.get("kind") == "recover-decodable" and operation.get("state") in {"queued", "running", "waiting_connection", "paused", "reconciling"}:
                operation.update(state="conflict", stage="superseded", message="恢复已停止，按最新视频指令执行。")
                self.videos.store.save_operation(operation)
                task = self.videos.operation_tasks.get(operation["id"])
                if task and task is not asyncio.current_task() and not task.done():
                    task.cancel(); recovery_tasks.append(task)
        if recovery_tasks:
            await asyncio.gather(*recovery_tasks, return_exceptions=True)
        self.require_command(identity, request_id, row["generation"])
        video = self.videos.store.get(video_id)
        if video:
            for version in video.get("versions", []):
                if version.get("downloadRequested"):
                    version["downloadRequested"] = False
                if version.get("state") in {"downloading", "publishing"}:
                    version.update(state="attention", message="旧版保存已停止，文件和断点保留。")
                recovery = version.get("recovery") or {}
                if recovery.get("state") in {"queued", "moving", "slicing", "paused"}:
                    recovery.update(state="superseded", message="旧版恢复已停止。")
            self.videos.store.save(video)
        self.slicing.wake.set()

    def require_command(self, identity, request_id, generation):
        current = self.get(identity)
        if not current or current.get("requestId") != request_id or current["generation"] != generation:
            raise Conflict("已有更新的视频指令，本次旧操作已停止。")

    def release_review(self, video_id, request_id=None):
        row = self.get("collection:" + video_id)
        if row and (request_id is None or row.get("requestId") == request_id):
            row.update(blocked=False, owner="review")
            self.save(row)

    async def link(self, item, data):
        from .media import file_digest
        from .slicing import source_version
        from .file_leases import finish_io
        if item.get("superseded"):
            raise Conflict("旧版任务不能重新关联为当前版本。")
        row = self.videos.store.get(data.get("videoId"))
        revision = data.get("collectionRevision")
        if (not row or row.get("deletedAt") or row.get("revision") != data.get("collectionExpectedRevision")
                or row.get("activeRevision") != revision):
            raise Conflict("所选采集版本已变化，请重新选择。")
        version = self.videos.version(row, revision) or {}
        expected = version.get("fileVersion") or version.get("recovery", {}).get("fileVersion") or {}
        if not self.same_version(expected, item.get("fileVersion") or {}):
            digest = version.get("sha256") or version.get("recovery", {}).get("sha256")
            if not digest or not item.get("path"):
                raise Conflict("原片与所选采集版本不一致，未建立关联。")
            with self.leases.read(item["path"]):
                before = await finish_io(source_version, item["path"])
                if (before != item.get("fileVersion") or await finish_io(file_digest, Path(item["path"])) != digest
                        or await finish_io(source_version, item["path"]) != before):
                    raise Conflict("原片与所选采集版本不一致，未建立关联。")
        latest = self.videos.store.get(row["id"])
        if not latest or latest.get("revision") != row["revision"]:
            raise Conflict("核对期间采集记录已变化。")
        target_id = "collection:" + row["id"]
        target = self.get(target_id) or {"id": target_id, "videoId": row["id"], "generation": 0,
                                        "owner": "review", "blocked": False, "paths": []}
        if target.get("blocked"):
            raise Conflict("所选视频正在执行更正，请稍后关联。")
        for peer in self.slicing.store.list("items"):
            if peer.get("mediaId") != item["mediaId"]:
                continue
            if peer["id"] in self.slicing.analysis_tasks or peer["id"] in self.slicing.export_tasks:
                raise Conflict("请先暂停本地处理，再关联采集版本。")
        for peer in self.slicing.store.list("items"):
            if peer.get("mediaId") == item["mediaId"]:
                peer.update(mediaId=target_id, mediaGeneration=target["generation"], collectionVideoId=row["id"],
                            collectionRevision=revision, identityConflict=False, processingReason="")
                peer["revision"] = int(peer.get("revision", 0)) + 1
                self.slicing.store.save("items", peer)
                if peer.get("path") not in target["paths"]:
                    target["paths"].append(peer["path"])
        self.save(target)

    @staticmethod
    def processing_entry(item=None):
        item = item or {}
        query = {"tab": "videos"}
        if item.get("collectionVideoId"):
            query["videoId"] = item["collectionVideoId"]
        if item.get("mediaId"):
            query["mediaId"] = item["mediaId"]
        return "/?" + urlencode(query)

    def state(self):
        public_items = {item["id"]: item for item in self.slicing.state()["items"]}
        groups = {}
        for item in self.slicing.store.list("items"):
            if item.get("superseded") or item["id"] not in public_items:
                continue
            identity = item.get("mediaId") or "unbound:" + item["id"]
            group = groups.setdefault(identity, {"id": identity, "videoId": item.get("collectionVideoId"),
                "name": item.get("name"), "createdAt": item.get("createdAt") or item.get("updatedAt"), "items": []})
            reason = self.reason(item)
            group["items"].append({**public_items[item["id"]], "blockedReason": reason})
            group.update(generation=(self.get(identity) or {}).get("generation", 0),
                         owner=(self.get(identity) or {}).get("owner", "slicing"), blockedReason=reason,
                         processingEntry=self.processing_entry(item))
        packages = self.slicing.packages.state()["items"]
        files = getattr(self.slicing, 'files', None)
        if files:
            files.refresh()
        public_files = {row['id']: row for row in files.state()['items']} if files else {}
        for group in groups.values():
            ids = {item["id"] for item in group["items"]}
            group["packages"] = [p for p in packages if p.get("itemId") in ids]
            group["files"] = [{k: public_files.get(f['id'], f).get(k) for k in ("id", "archiveState", "archiveMessage", "cleanup")}
                              for f in files.rows("files") if ids.intersection(f.get("links", {}).get("slicing", []))] if files else []
            if files and getattr(files, 'workspace', None) and files.workspace.enabled:
                for file in group['files']:
                    asset = files.workspace.asset_for(public_files.get(file['id'], {}).get('path', ''))
                    if asset and file['archiveState'] == 'complete' and file.get('cleanup', {}).get('state') != 'complete':
                        file['cleanupEligible'], file['cleanupReason'] = files.workspace.cleanup_eligibility(asset)
        return {"groups": [g for g in groups.values() if g.get("videoId") or
                           any(i.get("state") not in TERMINAL for i in g["items"])]}

    async def action(self, identity, body):
        lock = self.locks.setdefault(identity, asyncio.Lock())
        async with lock:
            self.refresh()
            control = self.get(identity)
            if not control:
                raise ValueError("视频控制记录不存在。")
            signature = json.dumps(body, sort_keys=True)
            key = hashlib.sha256((identity + ":" + str(body.get("requestId"))).encode()).hexdigest()
            old = self.db.execute("SELECT payload FROM simple_media_actions WHERE id=?", (key,)).fetchone()
            if old:
                receipt = json.loads(old[0])
                if receipt["signature"] != signature:
                    raise Conflict("请求编号已用于其他操作。")
                if receipt["state"] == "complete":
                    return self.state()
            if body.get("generation") != control["generation"]:
                raise Conflict("视频指令已更新，请刷新后重试。")
            item = self.slicing.store.get("items", body.get("itemId"))
            if not item or item.get("mediaId") != identity or (body.get("action") != "link" and not self.allowed(item)):
                raise Conflict(self.reason(item or {}) or "原片身份不匹配。")
            data = {**body, "expectedRevision": body.get("expectedRevision")}
            action = body.get("action")
            if action in {"bind", "review", "link", "start", "retry", "recover"}:
                ids = {row["id"] for row in self.slicing.store.list("items") if row.get("mediaId") == identity}
                if any(key in self.slicing.analysis_tasks or key in self.slicing.export_tasks for key in ids):
                    raise Conflict("这个视频仍在本地处理，完成或暂停后再调整；其他视频继续。")
                if action == "bind" and any(p.get("itemId") in ids and p["id"] in self.slicing.packages.running
                                            for p in self.slicing.packages.store.list("items")):
                    raise Conflict("此视频的照片包正在上传，请完成或暂停后重新绑定。")
            if action in {"retry", "recover", "bind", "review", "start", "link"} and item.get("revision") != data["expectedRevision"]:
                raise Conflict("切片记录已变化，请刷新后重试。")
            receipt = {"id": key, "signature": signature, "state": "running"}
            self.db.execute("INSERT INTO simple_media_actions VALUES(?,?) ON CONFLICT(id) DO UPDATE SET payload=excluded.payload", (key,json.dumps(receipt)))
            self.db.commit()
            if action == "link":
                await self.link(item, data)
            elif action in {"retry", "recover"}:
                self.slicing.retry(item["id"], data["expectedRevision"], data["requestId"], recover_decodable=action == "recover")
            elif action == "bind":
                from .models import SlicingBindInput
                validated = SlicingBindInput.model_validate(data).model_dump()
                await self.verify_collection_file(item, validated["path"])
                await self.slicing.bind(item["id"], validated)
                bound = self.slicing.store.get("items", item["id"])
                if bound.get("fileVersion") != item.get("fileVersion"):
                    control["generation"] += 1
                    self.save(control)
                    bound["mediaGeneration"] = control["generation"]
                    self.slicing.store.save("items", bound)
                    old_ids = set()
                    for peer in self.slicing.store.list("items"):
                        if peer.get("mediaId") == identity and peer["id"] != item["id"]:
                            old_ids.add(peer["id"])
                            peer.update(superseded=True, state=peer["state"] if peer.get("state") in TERMINAL else "superseded")
                            self.slicing.store.save("items", peer)
                    for package in self.slicing.packages.store.list("items"):
                        if package.get("itemId") in old_ids:
                            package["superseded"] = True
                            if not package.get("jobId") and package.get("state") not in {"submitting", "importing"}:
                                package["state"] = "superseded"
                            self.slicing.packages._save(package)
            elif action == "review":
                from .models import SlicingReviewInput
                await self.slicing.review(item["id"], SlicingReviewInput.model_validate(data).model_dump())
            elif action == "start":
                result = await self.slicing.start(data["requestId"], self.slicing.store.settings["revision"], "single", item["id"])
                if getattr(self, "submissions", None):
                    self.submissions.add("slicing", "单条切片", {"requestId": data["requestId"],
                        "runId": (result.get("run") or {}).get("id"), "itemIds": [item["id"]],
                        "mode": "single", "message": "单条切片已启动。"}, request_id=data["requestId"])
            elif action in {"confirm-package", "retry-package"}:
                package = self.slicing.packages.store.get("items", data.get("packageId"))
                if not package or package.get("itemId") != item["id"]:
                    raise Conflict("照片包不属于此视频。")
                if action == "confirm-package":
                    self.slicing.confirm_package(package["id"])
                else:
                    await self.slicing.retry_package(package["id"], data)
            elif action in {"cleanup", "retry-archive"}:
                file = next((f for f in self.slicing.files.rows("files") if f["id"] == data.get("fileId")
                             and item["id"] in f.get("links", {}).get("slicing", [])), None)
                if not file:
                    raise Conflict("文件清理记录不属于此视频。")
                if action == 'retry-archive':
                    await self.slicing.files.retry(file['id'], data['requestId'])
                else:
                    await self.slicing.files.cleanup_confirmed(file["id"], data["requestId"])
            else:
                raise ValueError("操作无效。")
            receipt["state"] = "complete"
            self.db.execute("UPDATE simple_media_actions SET payload=? WHERE id=?", (json.dumps(receipt),key)); self.db.commit()
            return self.state()
