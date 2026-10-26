"""Journaled migration of verified slice output into a central folder.

Only complete, program-owned result folders are eligible. Source videos and
recognition decisions are never changed by this organizer.
"""
import asyncio
import os
from contextlib import ExitStack
from pathlib import Path
from uuid import uuid4

from .audit_moves import checked_path, source_fingerprint
from .domain import now_iso
from .slice_export import package_directory
from .videos import Conflict


class SliceOutputOrganizer:
    def __init__(self, slicing):
        self.slicing = slicing
        self.packages = slicing.packages
        self.db = self.packages.store.db
        self.db.execute("CREATE TABLE IF NOT EXISTS simple_slice_output_organization "
                        "(id TEXT PRIMARY KEY, payload TEXT NOT NULL)")
        self.db.commit()

    def _all(self):
        import json
        return [json.loads(row[0]) for row in self.db.execute(
            "SELECT payload FROM simple_slice_output_organization ORDER BY rowid")]

    def _get(self, identity):
        import json
        row = self.db.execute("SELECT payload FROM simple_slice_output_organization WHERE id=?", (identity,)).fetchone()
        return json.loads(row[0]) if row else None

    def _save(self, plan):
        import json
        previous = self._get(plan["id"])
        plan["revision"] = int((previous or {}).get("revision", 0)) + 1
        plan["updatedAt"] = now_iso()
        self.db.execute("INSERT INTO simple_slice_output_organization VALUES(?,?) "
                        "ON CONFLICT(id) DO UPDATE SET payload=excluded.payload",
                        (plan["id"], json.dumps(plan, ensure_ascii=False)))
        self.db.commit()

    @staticmethod
    def _counts(plan):
        states = ("planned", "moving", "complete", "unchanged", "cleaned", "held", "failed")
        return {state: sum(row.get("state") == state for row in plan.get("items", [])) for state in states}

    @staticmethod
    def _public(plan):
        if not plan:
            return None
        return {key: plan.get(key) for key in ("id", "revision", "state", "root", "createdAt", "updatedAt",
                                                "previewRequestId", "confirmRequestId", "counts", "message")} | {
            "items": [{key: row.get(key) for key in ("packageId", "name", "category", "state", "message",
                                                       "size", "fileCount", "source", "destination")}
                      for row in plan.get("items", [])]
        }

    def state(self):
        plans = self._all()
        return self._public(plans[-1]) if plans else None

    def preview(self, payload):
        requests = [plan for plan in self._all() if plan.get("previewRequestId") == payload["requestId"]]
        if requests:
            return self._public(requests[-1])
        if payload["expectedRevision"] != self.slicing.store.settings["revision"]:
            raise Conflict("目录设置已变化，请刷新后重新生成整理预览。")

        root = self.slicing._checked_output_directory(self.slicing.slice_output_directory())
        rows = []
        videos = {item["id"]: item for item in self.slicing.store.list("items")}
        for package in self.packages.store.list("items"):
            if package.get("state") == "dismissed":
                continue
            row = {"packageId": package["id"], "packageRevision": package.get("revision", 0),
                   "name": package.get("name") or package["id"], "category": package.get("category"),
                   "state": "held", "message": "结果身份或本地文件尚未核实。", "size": 0, "fileCount": 0,
                   "source": "", "destination": "", "fileVersions": {}}
            if package.get("state") != "complete":
                row.update(state="held", message="图片包仍在处理，暂不移动。")
                rows.append(row)
                continue
            if package.get("localCleanupState") == "complete" or package.get("cleanupState") == "complete":
                row.update(state="cleaned", message="本地文件已由程序核验清理。")
                rows.append(row)
                continue

            item = videos.get(package.get("itemId")) or {}
            video_path = package.get("videoPath") or item.get("path")
            image_dir = package.get("imageDir")
            archive = package.get("path")
            names = package.get("images") or []
            versions = package.get("imageVersions") or {}
            try:
                if (not video_path or package.get("category") not in {"bed", "magnet"}
                        or not image_dir or not archive or not names
                        or any(Path(name).name != name or name in {".", ".."} for name in names)):
                    raise ValueError("缺少视频、类别或图片清单绑定。")
                source = checked_path(str(Path(image_dir).parent))
                image_directory = checked_path(str(image_dir))
                archive_path = checked_path(str(archive))
                destination = checked_path(str(package_directory(
                    video_path, package["category"], output_root=root,
                    video_id=package.get("sourceVideoId"))))
                row.update(source=str(source), destination=str(destination))
                if package.get("localCleanupState") == "complete":
                    row.update(state="cleaned", message="本地文件已由程序核验清理。")
                elif source == destination:
                    row.update(state="unchanged", message="已经位于分类后的目录。")
                elif not source.is_dir() or source.is_symlink():
                    recovered = self._verify_folder(destination, archive_path.name, names,
                                                    package.get("fileVersion"), versions)
                    if recovered:
                        row.update(state="moving", message="检测到上次移动已发布的目标，可安全续接。")
                    else:
                        raise ValueError("原结果文件夹缺失，且新位置无法验证；保留记录待核对。")
                else:
                    if archive_path.parent != source or image_directory != source / "image":
                        raise ValueError("压缩包与照片目录不在同一结果文件夹。")
                    expected = {archive_path.name: package.get("fileVersion")}
                    expected.update({f"image/{name}": versions.get(name) for name in names})
                    if any(not value for value in expected.values()):
                        raise ValueError("压缩包或照片缺少版本凭证。")
                    for relative, version in expected.items():
                        path = source / relative
                        if path.is_symlink() or not path.is_file() or source_fingerprint(path) != version:
                            raise ValueError("照片或压缩包已缺失或变化，原位置保留。")
                        row["size"] += path.stat().st_size
                        row["fileCount"] += 1
                        row["fileVersions"][relative] = version
                    unknown = self._unknown_entries(source, set(expected))
                    if unknown:
                        raise ValueError("文件夹含有本次任务之外的文件，避免连带移动，原位保留。")
                    if os.path.lexists(destination):
                        raise ValueError("目标目录已存在，避免覆盖，原位保留。")
                    if self._device(source) != self._device(destination.parent):
                        raise ValueError("整理目标位于另一块磁盘；为避免占用双倍空间，暂不自动复制。")
                    row.update(state="planned", message="压缩包和全部照片均核验通过，可整理。")
            except (OSError, ValueError) as error:
                row.update(state="held", message=str(error) or "本地路径暂不可用，原位保留。")
            rows.append(row)

        plan = {"id": "slice-organize:" + uuid4().hex, "previewRequestId": payload["requestId"],
                "state": "preview", "createdAt": now_iso(), "root": str(root),
                "settingsRevision": self.slicing.store.settings["revision"], "items": rows,
                "message": "仅整理平台状态已完成且本地文件版本核验通过的结果；不会移动原视频或删除文件。"}
        plan["counts"] = self._counts(plan)
        self._save(plan)
        return self._public(plan)

    @staticmethod
    def _device(path):
        candidate = Path(path)
        while not candidate.exists():
            candidate = candidate.parent
        return candidate.stat().st_dev

    @staticmethod
    def _unknown_entries(root, expected):
        for current, directories, files in os.walk(root, followlinks=False):
            base = Path(current)
            relative = base.relative_to(root)
            if relative.parts not in {(), ("image",)}:
                return True
            if any((base / name).is_symlink() for name in directories):
                return True
            for name in files:
                path = base / name
                if path.is_symlink():
                    return True
                rel = path.relative_to(root).as_posix()
                if rel in expected or name.startswith("._") or name == ".DS_Store":
                    continue
                return True
        return False

    @staticmethod
    def _matches(path, expected, *, moved=False):
        try:
            actual = source_fingerprint(path)
        except (OSError, ValueError):
            return False
        keys = ("size", "modifiedNs") if moved else tuple(expected)
        return all(actual.get(key) == expected.get(key) for key in keys)

    def _verify_folder(self, folder, archive_name, names, archive_version, image_versions):
        if not archive_version or not folder.is_dir() or folder.is_symlink():
            return False
        if not self._matches(folder / archive_name, archive_version, moved=True):
            return False
        return all(image_versions.get(name) and self._matches(folder / "image" / name,
                                                               image_versions[name], moved=True)
                   for name in names)

    async def confirm(self, identity, payload):
        async with self.packages.tick_lock:
            plan = self._get(identity)
            if not plan or not identity.startswith("slice-organize:"):
                raise ValueError("整理预览不存在，请重新预览。")
            if plan.get("state") == "complete":
                return self._public(plan)
            if plan.get("settingsRevision") != self.slicing.store.settings["revision"]:
                raise Conflict("目录设置已变化，请重新生成整理预览。")
            if plan.get("confirmRequestId") != payload["requestId"]:
                if plan.get("confirmRequestId"):
                    raise Conflict("此整理预览已绑定其他确认操作，请重新预览。")
                if plan.get("revision") != payload["expectedRevision"]:
                    raise Conflict("整理预览已变化，请重新预览后再确认。")
            if self.slicing.store.settings.get("enabled") or self.slicing.busy or self.slicing._active_analysis_tasks():
                raise Conflict("切片队列仍在运行；队列结束后才能整理历史结果，当前文件保持不动。")

            for row in plan.get("items", []):
                if row.get("state") not in {"planned", "moving"}:
                    continue
                package = self.packages.store.get("items", row["packageId"])
                if not package or package.get("state") != "complete":
                    raise Conflict("图片包状态已变化，请重新预览。")
                if not plan.get("confirmRequestId") and package.get("revision", 0) != row.get("packageRevision", 0):
                    raise Conflict("图片包记录已更新，请重新预览。")

            plan.update(state="moving", confirmRequestId=payload["requestId"], message="正在整理已核验的历史切片结果。")
            self._save(plan)
            for row in plan.get("items", []):
                if row.get("state") not in {"planned", "moving"}:
                    continue
                package = self.packages.store.get("items", row["packageId"])
                source, destination = Path(row["source"]), Path(row["destination"])
                archive_name = Path(package["path"]).name
                names = package.get("images") or []
                versions = package.get("imageVersions") or {}
                if not source.exists() and self._verify_folder(destination, archive_name, names,
                                                               package.get("fileVersion"), versions):
                    await self._update_package(package, source, destination, archive_name, names)
                    row.update(state="complete", message="已核对并续接上次移动。")
                    plan["counts"] = self._counts(plan); self._save(plan)
                    continue
                if source.exists() and destination.exists():
                    row.update(state="held", message="原目录和目标目录同时存在，保留两份并暂停核对。")
                    plan["counts"] = self._counts(plan); self._save(plan)
                    continue
                try:
                    paths = [source, Path(package["path"]), Path(package["imageDir"])]
                    paths.extend(Path(package["imageDir"]) / name for name in names)
                    with ExitStack() as stack:
                        for path in paths:
                            stack.enter_context(self.slicing.leases.write(path))
                        if not source.is_dir() or source.is_symlink():
                            raise ValueError("原结果文件夹已变化，原位保留并等待核对。")
                        for relative, expected in row.get("fileVersions", {}).items():
                            current = source / relative
                            if not current.is_file() or current.is_symlink() or source_fingerprint(current) != expected:
                                raise ValueError("结果文件版本已变化，原位保留并等待核对。")
                        if os.path.lexists(destination):
                            raise ValueError("目标目录已有内容，原位保留，不覆盖。")
                        row.update(state="moving", message="正在移动整个结果目录。")
                        plan["counts"] = self._counts(plan); self._save(plan)
                        await asyncio.to_thread(destination.parent.mkdir, parents=True, exist_ok=True)
                        await asyncio.to_thread(os.rename, source, destination)
                        if not self._verify_folder(destination, archive_name, names,
                                                   package.get("fileVersion"), versions):
                            raise ValueError("移动后版本核验不符，目标保留并等待核对。")
                        await self._update_package(package, source, destination, archive_name, names)
                    row.update(state="complete", message="已移动并核验压缩包、照片及文件版本。")
                except (OSError, ValueError) as error:
                    row.update(state="failed", message=str(error) or "移动失败，原文件保留。")
                plan["counts"] = self._counts(plan)
                self._save(plan)
            failures = [row for row in plan.get("items", []) if row.get("state") in {"failed", "moving"}]
            plan.update(state="attention" if failures else "complete",
                        message=("部分结果保留原位；修复原因后可继续此整理计划。" if failures
                                 else "历史切片结果整理完成；原视频、云端任务和算法判断未改变。"))
            plan["counts"] = self._counts(plan)
            self._save(plan)
            return self._public(plan)

    async def _update_package(self, package, source, destination, archive_name, names):
        archive = destination / archive_name
        image_dir = destination / "image"
        if package.get("path") != str(archive):
            self.slicing.leases.notify_move(str(package["path"]), str(archive), source_fingerprint(archive))
            package = self.packages.store.get("items", package["id"]) or package
        package.update(path=str(archive), imageDir=str(image_dir),
                       fileVersion=source_fingerprint(archive),
                       imageVersions={name: source_fingerprint(image_dir / name) for name in names})
        self.packages._save(package)
