"""Explicit, journaled cuts of audited local videos; no platform writes."""
import asyncio
import errno
import hashlib
import json
import os
import shutil
import stat
from contextlib import AsyncExitStack
from itertools import combinations
from pathlib import Path
from uuid import uuid4

from .domain import now_iso
from .media import file_digest, publish_with_reservation, transfer_reservation_identity
from .upload_audit import AUDIT_POLICY_VERSION
from .videos import Conflict

MOVE_RULE_VERSION = 4


def fingerprint(info):
    return {"device": info.st_dev, "inode": info.st_ino, "size": info.st_size, "modifiedNs": info.st_mtime_ns}


def checked_path(value):
    path = Path(value).expanduser()
    if not path.is_absolute() or ".." in path.parts:
        raise ValueError("请填写不含上级跳转的文件夹完整路径。")
    path = Path(os.path.normpath(path))
    for part in (path, *path.parents):
        if part.is_symlink():
            raise ValueError("移动路径不能经过符号链接，请填写实际文件夹路径。")
    protected = (Path.home() / "Desktop" / "视频抽帧").resolve()
    if path.is_relative_to(protected):
        raise ValueError("桌面的“视频抽帧”源目录受保护，不能用于分类移动。")
    if path.is_relative_to(Path("/Volumes")) and len(path.parts) > 2:
        if not os.path.ismount(str(Path(*path.parts[:3]))):
            raise ValueError("外置盘尚未挂载，请连接原磁盘后重试。")
    return path


def source_fingerprint(path):
    checked_path(str(path))
    info = path.lstat()
    if not stat.S_ISREG(info.st_mode):
        raise ValueError("视频已不是普通文件，原位保留。")
    return fingerprint(info)


def category(file):
    targets = file.get("targets", {})
    bed = targets.get("27912", {}).get("status")
    magnet = targets.get("27944", {}).get("status")
    if any(value not in ("uploaded", "missing") for value in (bed, magnet)) or file.get("issue"):
        return "held"
    if bed == magnet:
        return "uploaded" if bed == "uploaded" else "both_missing"
    return "missing_magnet" if magnet == "missing" else "missing_bed"


def inspect_plan(report, uploaded, magnet_missing, bed_missing):
    root = checked_path(report["directory"])
    if not root.is_dir():
        raise ValueError("原视频文件夹不存在，请连接磁盘后重新核对。")
    destinations = {"uploaded": checked_path(uploaded), "missing_magnet": checked_path(magnet_missing),
                    "missing_bed": checked_path(bed_missing)}
    if any(first.is_relative_to(second) or second.is_relative_to(first)
           for first, second in combinations(destinations.values(), 2)):
        raise ValueError("三个分类文件夹必须互相独立，不能相同或互相包含。")
    if any(root.is_relative_to(path) for path in destinations.values()):
        raise ValueError("目标文件夹不能等于原文件夹或包含原文件夹。")
    if any(not file.get("fileVersion") for file in report["files"]):
        raise ValueError("此报告未记录文件版本，请先重新读取目录并核对，再生成移动预览。")
    items = []
    for file in report["files"]:
        relative = Path(file["relativePath"])
        if relative.is_absolute() or not relative.parts or ".." in relative.parts:
            raise ValueError("报告中的文件路径无效，请重新核对。")
        kind = category(file)
        source = root / relative
        moved = file.get("localMove")
        expected = file["fileVersion"]
        if moved and moved.get("fileVersion"):
            source = checked_path(moved["path"])
            expected = moved["fileVersion"]
        item = {"relativePath": str(relative), "source": str(source), "size": file["size"],
                "fileVersion": expected, "category": kind, "state": "planned",
                "reservation": {"token": uuid4().hex}}
        try:
            if moved and not moved.get("fileVersion"):
                raise ValueError("旧报告中的移动缺少目标文件版本，请重新读取目录并核对。")
            if kind == "held":
                raise ValueError("日志待核对或仍在导入，保留原位。")
            if kind == "both_missing":
                raise ValueError("磁盘吊和车斗两处均未上传，保留原位。")
            if source_fingerprint(source) != expected:
                raise ValueError("文件在核对后已变化，请重新核对。")
            # Reclassify files after the missing target has been uploaded. Strip
            # only the configured category root, preserving genuine subfolders.
            current_root = next((path for path in destinations.values() if source.is_relative_to(path)), None)
            if current_root == destinations[kind]:
                item.update(state="unchanged", destination=str(source), message="已在正确分类文件夹，无需移动。")
                items.append(item)
                continue
            target_relative = source.relative_to(current_root) if current_root else relative
            destination = destinations[kind] / target_relative
            checked_path(str(destination))
            item["destination"] = str(destination)
            if os.path.lexists(destination):
                raise ValueError("目标已有同名文件，原文件保留；不会覆盖。")
            if destinations[kind].exists() and not destinations[kind].is_dir():
                raise ValueError("目标路径不是文件夹。")
        except (ValueError, OSError) as error:
            item.update(state="held", message=str(error) if isinstance(error, ValueError) else "文件或目录暂时无法读取，保留原位。")
        items.append(item)
    return {"sourceDirectory": str(root), "uploadedDirectory": str(destinations["uploaded"]),
            "magnetMissingDirectory": str(destinations["missing_magnet"]),
            "bedMissingDirectory": str(destinations["missing_bed"]), "items": items}


def prepare_paths(item):
    source, destination = Path(item["source"]), Path(item["destination"])
    checked_path(str(source)); checked_path(str(destination))
    destination.parent.mkdir(parents=True, exist_ok=True)
    checked_path(str(destination))
    return source, destination


def recover_published(item):
    """Only a previously recorded inode/version can prove a lost move response."""
    destination = Path(item["destination"])
    if not destination.exists():
        return False
    expected = item.get("destinationVersion") or item.get("temporaryVersion") or item["fileVersion"]
    return source_fingerprint(destination) == expected


def same_device(source, destination):
    return source.stat().st_dev == destination.parent.stat().st_dev


def move_direct(item):
    source, destination = prepare_paths(item)
    if source_fingerprint(source) != item["fileVersion"]:
        raise ValueError("原文件已变化，未移动。请重新核对。")
    publish_with_reservation(source, destination, reservation=item["reservation"])
    observed = source_fingerprint(destination)
    # ExFAT may give the renamed directory entry a new synthetic inode.
    # Record its actual identity after a successful rename, without copying.
    if any(observed[key] != item["fileVersion"][key] for key in ("size", "modifiedNs")):
        raise ValueError("移动后的文件版本需要核对，请保留目标文件。")
    return observed


def copy_verified(item):
    source, destination = prepare_paths(item)
    if source_fingerprint(source) != item["fileVersion"]:
        raise ValueError("原文件已变化，保留原位。")
    if destination.exists():
        transfer_reservation_identity(destination, item["reservation"])
    temporary = Path(item["temporary"])
    if temporary.parent != destination.parent or temporary.name != f".{destination.name}.{item['reservation']['token']}.moving":
        raise ValueError("转移临时文件身份不符。")
    if temporary.is_symlink():
        raise ValueError("临时文件已被替换，保留原文件。")
    # Only this plan's private temporary name can be retried after interruption.
    temporary.unlink(missing_ok=True)
    if shutil.disk_usage(destination.parent).free < item["size"] + 16 * 1024 * 1024:
        raise OSError(errno.ENOSPC, "目标磁盘空间不足，原文件已保留。")
    try:
        flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
        digest = hashlib.sha256()
        with os.fdopen(os.open(source, flags), "rb") as incoming:
            if fingerprint(os.fstat(incoming.fileno())) != item["fileVersion"]:
                raise ValueError("原文件版本已变化。")
            with temporary.open("xb") as outgoing:
                for block in iter(lambda: incoming.read(4 * 1024 * 1024), b""):
                    outgoing.write(block); digest.update(block)
                outgoing.flush(); os.fsync(outgoing.fileno())
        if source_fingerprint(source) != item["fileVersion"] or file_digest(temporary) != digest.hexdigest():
            raise ValueError("跨磁盘转移校验失败，原文件保留。")
        return {"sha256": digest.hexdigest(), "temporaryVersion": source_fingerprint(temporary)}
    except BaseException:
        if not temporary.is_symlink():
            temporary.unlink(missing_ok=True)
        raise


def publish_copied(item):
    _, destination = prepare_paths(item)
    temporary = Path(item["temporary"])
    if source_fingerprint(temporary) != item["temporaryVersion"]:
        raise ValueError("转移临时文件已变化，原文件保留。")
    publish_with_reservation(temporary, destination, reservation=item["reservation"])
    return source_fingerprint(destination)


def finish_source(item):
    source = Path(item["source"])
    if not recover_published(item):
        raise ValueError("无法确认目标文件的身份，未删除原文件。")
    if os.path.lexists(source):
        if source_fingerprint(source) != item["fileVersion"]:
            raise ValueError("原路径已被其他文件替换，两个文件均保留，请人工核对。")
        source.unlink()


class AuditMoveManager:
    def __init__(self, store, audit, videos):
        self.db, self.audit, self.videos = store.db, audit, videos
        self.db.execute("CREATE TABLE IF NOT EXISTS simple_audit_moves (id TEXT PRIMARY KEY, created_at TEXT, payload TEXT NOT NULL)")
        self.db.commit()
        self.task = None
        self.preview_lock = asyncio.Lock()
        self.stopping = False
        previous = self.latest()
        if previous and previous["status"] == "running":
            previous.update(status="interrupted", message="上次移动已中断；点击继续可先核对已移动文件，再处理剩余文件。")
            self.save(previous)

    @property
    def busy(self):
        return self.preview_lock.locked() or (self.task is not None and not self.task.done())

    def get(self, identity):
        row = self.db.execute("SELECT payload FROM simple_audit_moves WHERE id=?", (identity,)).fetchone()
        return json.loads(row[0]) if row else None

    def latest(self):
        row = self.db.execute("SELECT payload FROM simple_audit_moves ORDER BY rowid DESC LIMIT 1").fetchone()
        return json.loads(row[0]) if row else None

    def save(self, plan):
        plan["updatedAt"] = now_iso()
        plan["counts"] = {state: sum(i["state"] == state for i in plan["items"])
                          for state in ("planned", "moving", "moved", "unchanged", "held", "failed")}
        self.db.execute("INSERT INTO simple_audit_moves VALUES (?,?,?) ON CONFLICT(id) DO UPDATE SET payload=excluded.payload",
                        (plan["id"], plan["createdAt"], json.dumps(plan, ensure_ascii=False)))
        self.db.commit()

    async def preview(self, payload):
        if self.busy or self.audit.busy:
            raise Conflict("核对或移动正在进行，请完成后再生成预览。")
        async with self.preview_lock:
            return await self.prepare_preview(payload)

    async def prepare_preview(self, payload):
        report = self.audit.saved.get("report")
        if not report or report["id"] != payload["reportId"]:
            raise Conflict("核对报告已更新，请刷新后重新生成移动预览。")
        if report.get("policyVersion") != AUDIT_POLICY_VERSION:
            raise Conflict("日志核对规则已更新，请先重新读取目录并核对，再生成分类预览。")
        if payload.get("refreshReport"):
            # Preview only: re-read current logs and all category folders before
            # deciding which files to cut. Confirm still binds this exact report.
            self.audit.start(report["directory"], True)
            audit_id = self.audit.run["id"]
            await self.audit.task
            report = self.audit.saved.get("report")
            if self.audit.run.get("status") != "complete" or not report or report["id"] != audit_id:
                raise Conflict("最新上传状态未能核对完成，未生成移动计划；请检查连接后重试。")
        details = await asyncio.to_thread(inspect_plan, report, payload["uploadedDirectory"],
                                         payload["magnetMissingDirectory"], payload["bedMissingDirectory"])
        if (self.audit.saved.get("report", {}).get("id") != report["id"] or self.audit.busy
                or (self.task is not None and not self.task.done())):
            raise Conflict("生成预览期间报告已变化，请重试。")
        plan = {"id": uuid4().hex, "reportId": report["id"], "status": "preview", "createdAt": now_iso(),
                "ruleVersion": MOVE_RULE_VERSION,
                "message": "请核对目标文件夹及清单；明确确认后才剪切文件。", **details}
        self.save(plan)
        return plan

    def start(self, identity, request_id):
        plan = self.get(identity)
        if not plan:
            raise ValueError("移动预览不存在，请重新生成。")
        if plan.get("requestId") and plan["requestId"] != request_id:
            raise Conflict("此预览已有确认请求，请使用原请求继续或重新生成预览。")
        if plan["status"] == "complete":
            return plan
        if plan.get("ruleVersion") != MOVE_RULE_VERSION:
            raise Conflict("分类规则已更新，请重新生成移动预览；已完成的移动记录会保留。")
        if self.busy and plan["status"] == "running":
            return plan
        if self.busy or self.audit.busy:
            raise Conflict("核对或移动正在进行，请稍后重试。")
        if (self.audit.saved.get("report") or {}).get("id") != plan["reportId"]:
            raise Conflict("核对报告已更新，请按最新报告重新生成预览。")
        plan.update(status="running", requestId=request_id, message="正在逐条剪切视频…")
        self.audit.file_move_busy = True
        self.stopping = False
        self.save(plan)
        self.task = asyncio.create_task(self.run(plan))
        return plan

    async def io(self, fn, *args):
        return await self.videos.file_operation(fn, *args)

    def bindings(self, source):
        return [row for row in self.videos.store.list(include_deleted=True)
                if any(v.get("filePath") == source for v in row["versions"])]

    def update_bindings(self, item):
        if getattr(self, "file_leases", None):
            self.file_leases.on_move(item["source"], item["destination"], item["destinationVersion"])
        for row in self.bindings(item["source"]):
            for version in row["versions"]:
                if version.get("filePath") == item["source"]:
                    version.update(filePath=item["destination"], classifiedAt=now_iso())
            self.videos.store.save(row)
        report = self.audit.saved.get("report")
        if report:
            for file in report["files"]:
                if (file["relativePath"] == item["relativePath"] and
                        (str(Path(report["directory"]) / file["relativePath"]) == item["source"]
                         or file.get("localMove", {}).get("path") == item["source"])):
                    file["localMove"] = {"path": item["destination"], "category": item["category"], "at": now_iso(),
                                         "fileVersion": item["destinationVersion"]}
            self.audit.save()

    async def process(self, plan, item):
        async with AsyncExitStack() as locks:
            if getattr(self, "file_leases", None):
                locks.enter_context(self.file_leases.write(item["source"]))
            for row in sorted(self.bindings(item["source"]), key=lambda r: r["id"]):
                await locks.enter_async_context(self.videos.row_lock(row["id"]))
                current = self.videos.store.get(row["id"])
                if current.get("deletion", {}).get("state") in ("pending", "error"):
                    raise ValueError("对应采集记录正在清理，本次保留原文件。")
                if any(v.get("filePath") == item["source"] and v["state"] != "saved" for v in current["versions"]):
                    raise ValueError("对应视频尚在处理，本次保留原文件。")
            item["state"] = "moving"; self.save(plan)
            if await self.io(recover_published, item):
                item["destinationVersion"] = await self.io(source_fingerprint, Path(item["destination"]))
                self.save(plan)
                await self.io(finish_source, item)
            else:
                source, destination = await self.io(prepare_paths, item)
                if not item.get("temporaryVersion"):
                    if await self.io(same_device, source, destination):
                        item["destinationVersion"] = await self.io(move_direct, item)
                    else:
                        item["temporary"] = str(destination.parent / f".{destination.name}.{item['reservation']['token']}.moving")
                        self.save(plan)
                        item.update(await self.io(copy_verified, item)); self.save(plan)
                if item.get("temporaryVersion"):
                    item["destinationVersion"] = await self.io(publish_copied, item)
                self.save(plan)
                await self.io(finish_source, item)
            self.update_bindings(item)
            item.update(state="moved", message="已剪切到目标文件夹。", completedAt=now_iso())
            self.save(plan)

    async def run(self, plan):
        try:
            for item in plan["items"]:
                if self.stopping:
                    plan.update(status="interrupted", message="移动已暂停，已完成文件保留，剩余文件可继续。")
                    return
                if item["state"] in ("held", "moved", "unchanged"):
                    continue
                try:
                    await self.process(plan, item)
                except (ValueError, OSError) as error:
                    message = str(error) if isinstance(error, ValueError) else {
                        errno.ENOSPC: "目标磁盘空间不足，原文件保留。", errno.EEXIST: "目标已有同名文件，未覆盖；原文件保留。",
                        errno.ENOENT: "原文件或磁盘暂时不可用，请核对路径后继续。",
                    }.get(error.errno, "文件移动失败，原文件或已发布文件均已保留，请检查磁盘和权限后重试。")
                    item.update(state="failed", message=message); self.save(plan)
            failed = any(item["state"] == "failed" for item in plan["items"])
            plan.update(status="attention" if failed else "complete",
                        message="移动已结束；失败项已保留，可核对原因后继续。" if failed else
                        f"分类完成：已剪切 {sum(i['state'] == 'moved' for i in plan['items'])} 个，"
                        f"已在正确目录 {sum(i['state'] == 'unchanged' for i in plan['items'])} 个，"
                        f"保留原位 {sum(i['state'] == 'held' for i in plan['items'])} 个。")
        except asyncio.CancelledError:
            plan.update(status="interrupted", message="移动已中断，继续前将核对文件身份，避免重复移动。")
            raise
        except Exception:
            plan.update(status="attention", message="移动暂未完成，已记录进度；请核对后继续。")
        finally:
            self.audit.file_move_busy = False
            self.save(plan)

    async def shutdown(self):
        if self.task is not None and not self.task.done():
            self.stopping = True
            await asyncio.gather(self.task, return_exceptions=True)
