"""Snapshot-based, journaled classification of program-owned slice archives."""
import asyncio
import hashlib
import json
import os
from contextlib import ExitStack
from pathlib import Path
from uuid import uuid4

from .audit_moves import (checked_path, copy_verified, finish_source, move_direct,
                          prepare_paths, publish_copied, recover_published, same_device,
                          source_fingerprint)
from .videos import Conflict

HELD = {"importing", "submitting", "uploading", "uploaded", "attention", "review"}


def side_status(state):
    return "uploaded" if state == "complete" else "held" if state in HELD else "missing"


def classify_pair(bed_state, magnet_state):
    bed, magnet = side_status(bed_state), side_status(magnet_state)
    if "held" in (bed, magnet):
        return "held"
    if bed == magnet:
        return "uploaded" if bed == "uploaded" else "both_missing"
    return "missing_magnet" if magnet == "missing" else "missing_bed"


def group_packages(items):
    groups = {}
    for item in items:
        if item.get("state") == "dismissed":
            continue
        binding = [item.get("videoPath"), item.get("videoFileVersion")]
        if not binding[0]:
            binding = [item.get("sourceDatasetId"), item.get("sourceVideoId") or item.get("id")]
        key = hashlib.sha256(json.dumps(binding, sort_keys=True).encode()).hexdigest()[:24]
        group = groups.setdefault(key, {"name": item.get("name", key), "packages": {}, "ambiguous": False})
        category = item.get("category")
        if category in group["packages"]:
            group["ambiguous"] = True
        group["packages"][category] = item
    return groups


def plan_moves(items, root):
    root = checked_path(str(root))
    folders = {"uploaded": root / "已上传", "missing_magnet": root / "磁盘吊未上传", "missing_bed": root / "车斗未上传"}
    rows = []
    for key, group in group_packages(items).items():
        packages = group["packages"]
        kind = "held" if group["ambiguous"] else classify_pair(packages.get("bed", {}).get("state"), packages.get("magnet", {}).get("state"))
        files = []
        for package in packages.values():
            if not package.get("path"):
                continue
            source = checked_path(package["path"])
            video = Path(package.get("videoPath") or "")
            relative = Path(video.stem) / ("车斗" if package.get("category") == "bed" else "磁盘吊") / source.name if video.stem else Path(source.name)
            destination = folders[kind] / relative if kind in folders else None
            version = package.get("fileVersion")
            if version is None and source.is_file():
                version = source_fingerprint(source)
            files.append({"packageId": package.get("id"), "packageRevision": package.get("revision", 0),
                          "category": package.get("category"), "source": str(source), "size": (version or {}).get("size", 0),
                          "destination": str(destination) if destination else "", "name": source.name,
                          "fileVersion": version, "state": "planned", "reservation": {"token": uuid4().hex}})
        rows.append({"id": key, "name": group["name"], "kind": kind, "files": files,
                     "message": {"uploaded": "两处已导入。", "missing_magnet": "磁盘吊未导入。", "missing_bed": "车斗未导入。",
                                 "both_missing": "两处未导入，保留原位。", "held": "正在处理或身份待核对，保留原位。"}[kind]})
    return {"root": str(root), "folders": {key: str(value) for key, value in folders.items()}, "items": rows}


async def execute_moves(plan, leases, save, moved):
    """Every destructive step follows a saved receipt; replay only that receipt."""
    result = {"moved": [], "skipped": [], "errors": [], "bindings": []}
    for row in plan["items"]:
        for file in row["files"]:
            if row["kind"] not in {"uploaded", "missing_magnet", "missing_bed"}:
                result["skipped"].append(file["name"])
                continue
            try:
                source, destination = checked_path(file["source"]), checked_path(file["destination"])
                if source == destination:
                    result["skipped"].append(file["name"])
                    continue
                with ExitStack() as guard:
                    guard.enter_context(leases.write(source))
                    guard.enter_context(leases.write(destination))
                    if file["state"] == "planned":
                        if not file.get("fileVersion") or source_fingerprint(source) != file["fileVersion"]:
                            raise ValueError("文件已变化，请重新预览。")
                        if os.path.lexists(destination):
                            raise ValueError("目标已有同名文件，原文件保留。")
                        prepare_paths(file)
                        file["state"] = "moving" if same_device(source, destination) else "copying"
                        file["temporary"] = str(destination.parent / f".{destination.name}.{file['reservation']['token']}.moving")
                        save(plan)
                    if file["state"] == "moving":
                        if destination.exists():
                            if not recover_published(file):
                                raise ValueError("移动结果身份待核对，未覆盖文件。")
                            file["destinationVersion"] = source_fingerprint(destination)
                        else:
                            file["destinationVersion"] = await asyncio.to_thread(move_direct, file)
                        file["state"] = "published"
                        save(plan)
                    if file["state"] == "copying":
                        file.update(await asyncio.to_thread(copy_verified, file))
                        file["state"] = "copied"
                        save(plan)
                    if file["state"] == "copied":
                        if destination.exists() and recover_published(file):
                            file["destinationVersion"] = source_fingerprint(destination)
                        else:
                            file["destinationVersion"] = await asyncio.to_thread(publish_copied, file)
                        file["state"] = "published"
                        save(plan)
                    if file["state"] in {"published", "complete"}:
                        await asyncio.to_thread(finish_source, file)
                        moved(source, destination, file["destinationVersion"])
                        file["state"] = "complete"
                        save(plan)
                        result["moved"].append(str(destination))
                        result["bindings"].append({"packageId": file.get("packageId"), "source": str(source), "destination": str(destination)})
            except (OSError, ValueError) as error:
                result["skipped"].append(file["name"])
                result["errors"].append({"name": file["name"], "message": str(error) if isinstance(error, ValueError) else "文件暂不可用，保留进度。"})
    return result


def apply_moves(plan):
    from .file_leases import FileLeases
    return asyncio.run(execute_moves(plan, FileLeases(), lambda value: None, lambda *args: None))


class SliceClassification:
    def __init__(self, packages):
        self.packages = packages

    def preview(self, root):
        plan = {**plan_moves(self.packages.store.list("items"), root), "id": "classify:" + uuid4().hex, "state": "preview"}
        self.packages.store.save("batches", plan)
        return plan

    async def confirm(self, identity, request_id, expected_revision):
        async with self.packages.tick_lock:
            plan = self.packages.store.get("batches", identity)
            if not plan or not identity.startswith("classify:"):
                raise ValueError("分类预览不存在，请重新预览。")
            if plan.get("requestId") and plan["requestId"] != request_id:
                raise Conflict("此预览已有确认操作，请重新预览。")
            if plan.get("state") == "complete":
                return plan["result"]
            if not plan.get("requestId"):
                if plan.get("revision") != expected_revision:
                    raise Conflict("预览已更新，请重新预览。")
                for row in plan["items"]:
                    for file in row["files"]:
                        package = self.packages.store.get("items", file.get("packageId"))
                        if not package or package.get("revision", 0) != file["packageRevision"]:
                            raise Conflict("上传状态已变化，请重新预览。")
            plan.update(requestId=request_id, state="moving")
            self.packages.store.save("batches", plan)
            result = await execute_moves(plan, self.packages.leases,
                                         lambda value: self.packages.store.save("batches", value), self.packages.leases.notify_move)
            result["items"] = plan["items"]
            plan.update(state="complete" if not result["errors"] else "moving", result=result)
            self.packages.store.save("batches", plan)
            return result
