"""Manual video review, versioned recapture, verified publication and bound cleanup."""
import asyncio
import copy
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
from uuid import uuid4

import httpx

from .domain import DEFAULT_TARGET, approval_issue, format_time, gather_payload, now_iso, parse_time
from .download_slots import DownloadSlots
from .file_leases import FileEnvironmentError
from .jobs import JobManager
from .media import (DownloadFailure, download_url, file_digest, publish_file,
                    prepare_directory, valid_video_header, video_filename,
                    publish_with_reservation, transfer_reservation_identity)
from .remote import RemoteClient, RemoteError
from .media_validation import MediaValidationError, verify_video_tail
from .source import page_records
from .video_filters import filter_records, matches_status, record_status
from . import file_locations
from .recovery_results import partial_complete, verified_result

RETRY_DELAYS = (2, 5, 10, 20, 30, 60, 60, 60)
IDLE_POLL_SECONDS = 30
ACTIVE_POLL_SECONDS = 8
TERMINAL = {"withdrawn", "cloud_deleted"}
DEFAULT_DOWNLOAD_LIMIT = 5
MAX_DOWNLOAD_LIMIT = 8


def inspect_recovery_cache(source: Path, root: Path, total_bytes: int, received_bytes: int,
                           expected_digest: str | None = None):
    """Validate a recovery cache without performing removable-volume I/O on the event loop."""
    root = root.resolve()
    if source.is_symlink() or source.parent.is_symlink() or not source.resolve().is_relative_to(root):
        raise ValueError("下载缓存路径异常；原文件保留。")
    info = source.lstat()
    if not stat.S_ISREG(info.st_mode) or info.st_size != int(total_bytes):
        raise ValueError("下载缓存大小或文件类型与已记录长度不符；原缓存保留。")
    if int(received_bytes or 0) != info.st_size:
        raise ValueError("下载字节数未达到平台文件长度；不能恢复不完整下载。")
    with source.open("rb") as stream:
        if not valid_video_header(stream.read(512)):
            raise ValueError("文件头不是有效视频；原缓存保留。")
    digest = file_digest(source)
    after = source.lstat()
    if (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns) != (
            info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns):
        raise Conflict("验证期间下载缓存发生变化；原文件保留。")
    if expected_digest and expected_digest != digest:
        raise Conflict("恢复缓存内容与上次记录不符；原文件保留。")
    return (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns), digest


def recovery_destination(destination: Path, total_bytes: int, digest: str, reservation: dict):
    """Keep an unowned/incomplete old copy intact; use a stable separate name."""
    if destination.is_symlink() or any(parent.is_symlink() for parent in destination.parents):
        raise Conflict('恢复文件路径是符号链接，文件保留，不能自动替换。')
    if not destination.exists():
        return destination, None
    try:
        transfer_reservation_identity(destination, reservation)
        return destination, None
    except OSError:
        pass
    info = destination.lstat()
    if not stat.S_ISREG(info.st_mode):
        raise Conflict('恢复位置不是普通文件，原文件保留。')
    if info.st_size == total_bytes:
        return destination, None  # Full content is checked before reuse below.
    conflict = {'path': str(destination), 'size': info.st_size, 'device': info.st_dev,
                'inode': info.st_ino, 'modifiedNs': info.st_mtime_ns}
    stem = destination.stem
    suffix = '__恢复_' + digest[:12] + destination.suffix
    while len(os.fsencode(stem + suffix)) > 240:
        stem = stem[:-1]
    return destination.with_name(stem + suffix), conflict


def publish_recovery_cache(source: Path, destination: Path, expected_identity, expected_digest: str,
                           reservation: dict | None = None):
    """Move a validated recovery cache and validate the published file once."""
    if destination.is_symlink() or any(parent.is_symlink() for parent in destination.parents):
        raise Conflict('恢复文件路径是符号链接，文件保留，不能自动替换。')
    current = source.lstat()
    if (current.st_dev, current.st_ino, current.st_size, current.st_mtime_ns) != expected_identity:
        raise Conflict("转移前缓存版本发生变化；原文件保留。")
    owned_marker = False
    if reservation and destination.exists():
        try:
            transfer_reservation_identity(destination, reservation)
            owned_marker = True
        except OSError:
            pass
    if destination.exists() and not owned_marker:
        if destination.is_symlink() or file_digest(destination) != expected_digest:
            raise FileExistsError("恢复目录存在不同内容的同名文件；原文件保留。")
        source.unlink()
    elif reservation and current.st_dev == destination.parent.stat().st_dev:
        # ExFAT does not support RENAME_EXCL. A persisted exclusive marker lets
        # the existing guarded publisher rename on this volume without a copy.
        publish_with_reservation(source, destination, reservation=reservation)
    else:
        published_digest = publish_file(source, destination, expected_digest)
        if published_digest != expected_digest:
            raise OSError("恢复文件移动后校验失败；原文件保留。")
    info = destination.lstat()
    if not stat.S_ISREG(info.st_mode) or info.st_size != expected_identity[2]:
        raise OSError("恢复文件移动后校验失败；原文件保留。")
    return expected_digest


def verify_existing_recovery_destination(destination: Path, expected_digest: str):
    info = destination.lstat()
    if not stat.S_ISREG(info.st_mode) or file_digest(destination) != expected_digest:
        raise Conflict("断点恢复目录中的文件与本次操作凭证不符；文件保留。")
    return info.st_size


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
        file_locations.initialize(self.db)
        self.db.execute("CREATE TABLE IF NOT EXISTS simple_videos (id TEXT PRIMARY KEY, payload TEXT NOT NULL)")
        self.db.execute("CREATE TABLE IF NOT EXISTS simple_settings (id INTEGER PRIMARY KEY, directory TEXT NOT NULL)")
        self.db.execute("CREATE TABLE IF NOT EXISTS simple_download_settings (id INTEGER PRIMARY KEY, payload TEXT NOT NULL)")
        self.db.execute("CREATE TABLE IF NOT EXISTS simple_video_operations (id TEXT PRIMARY KEY, video_id TEXT NOT NULL, payload TEXT NOT NULL)")
        self.db.execute("CREATE INDEX IF NOT EXISTS simple_video_operations_video ON simple_video_operations(video_id)")
        self.db.commit()
        for row in self.list():
            for version in row["versions"]:
                if version["state"] == "submitting":
                    version["state"] = "uncertain"
                if version["state"] == "downloading":
                    version["state"] = "ready" if version.get("resultId") else "waiting"
            self.save(row)

    def save(self, row):
        file_locations.video(self.db, row, self.get(row['id']))
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

    def download_settings(self):
        row = self.db.execute("SELECT payload FROM simple_download_settings WHERE id=1").fetchone()
        return json.loads(row[0]) if row else {"maxConcurrentDownloads": DEFAULT_DOWNLOAD_LIMIT,
                                              "revision": 0, "requests": {}}

    def save_download_settings(self, settings):
        self.db.execute("INSERT INTO simple_download_settings VALUES (1, ?) ON CONFLICT(id) DO UPDATE SET payload=excluded.payload",
                        (json.dumps(settings),))
        self.db.commit()

    def save_operation(self, operation, *, commit=True):
        operation["updatedAt"] = now_iso()
        self.db.execute("INSERT INTO simple_video_operations VALUES (?, ?, ?) ON CONFLICT(id) DO UPDATE SET payload=excluded.payload",
                        (operation["id"], operation["videoId"], json.dumps(operation, ensure_ascii=False)))
        if commit:
            self.db.commit()

    def get_operation(self, identity):
        row = self.db.execute("SELECT payload FROM simple_video_operations WHERE id=?", (identity,)).fetchone()
        return json.loads(row[0]) if row else None

    def list_operations(self, video_id=None):
        if video_id:
            rows = self.db.execute("SELECT payload FROM simple_video_operations WHERE video_id=? ORDER BY rowid DESC", (video_id,))
        else:
            rows = self.db.execute("SELECT payload FROM simple_video_operations ORDER BY rowid")
        return [json.loads(row[0]) for row in rows]


class VideoManager:
    def __init__(self, jobs: JobManager, target: RemoteClient, data_dir: Path,
                 fixed_directory: Path | None = None, require_mounted_external: bool = False,
                 staging_directory: Path | None = None):
        self.jobs, self.target = jobs, target
        self.cloud_lease_root = data_dir / '.leases' / 'cloud'
        self.store = VideoStore(jobs.store)
        from .record_visibility import RecordVisibility
        self.record_visibility = RecordVisibility(self.store.db, 'collection')
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
        self.inventory_lock = asyncio.Lock()
        self.inventory_slots = asyncio.Semaphore(3)
        self.result_slots = asyncio.Semaphore(3)
        self.inventory_cache: dict[str, dict] = {}
        self.inventory_cached_at = 0.0
        self.row_locks: dict[str, asyncio.Lock] = {}
        self.download_settings = self.store.download_settings()
        # Both stages share the scheduler's configured pipeline limit. Resizing
        # the stage gates keeps already admitted transfers and their checkpoints.
        self.download_slots = DownloadSlots(self.download_settings["maxConcurrentDownloads"])
        self.publish_slots = DownloadSlots(self.download_settings["maxConcurrentDownloads"])
        self.running: dict[str, asyncio.Task] = {}
        self.operation_tasks: dict[str, asyncio.Task] = {}
        self.loop_task: asyncio.Task | None = None
        self.storage_check_task: asyncio.Task | None = None
        self.wake = asyncio.Event()
        self.last_check = ""
        self.check_error = ""
        self.stopping = False

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
        self.stopping = False
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
        # Resume explicitly requested downloads as soon as the service starts.
        # The regular poll remains a fallback, but shouldn't be the mechanism
        # that restores a persisted checkpoint after a restart.
        self.schedule_downloads()
        self.loop_task = asyncio.create_task(self.loop())
        for operation in self.store.list_operations():
            if operation.get("state") in {"queued", "running", "waiting_connection", "reconciling"}:
                if operation.get("state") in {"running", "reconciling"}:
                    operation.update(state="queued", message="程序已恢复，正在核对操作进度。")
                    self.store.save_operation(operation)
                self.launch_operation(operation["id"])

    async def shutdown(self):
        self.stopping = True
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
        for task in self.operation_tasks.values():
            task.cancel()
        if self.operation_tasks:
            await asyncio.gather(*self.operation_tasks.values(), return_exceptions=True)

    def state(self, query=None, *, identity=None):
        if identity is None:
            self.sync_jobs()
        final_processing = getattr(self, 'final_processing', None)
        final_operations = {(entry['videoId'], entry['version'], entry['expectedRevision']): entry
                            for entry in final_processing.presentations()} if final_processing else {}
        hidden = self.record_visibility.hidden()
        histories = {(r['videoId'], r['version']): {k:r.get(k) for k in ('state','message','receivedBytes','totalBytes')}
                     for r in self.history_downloads.rows()} if hasattr(self, 'history_downloads') else {}
        active_operations = {}
        for operation in self.store.list_operations():
            if operation.get('state') in {'queued', 'running', 'waiting_connection', 'reconciling', 'paused'} or (operation.get('kind') == 'withdraw' and operation.get('state') == 'attention'):
                active_operations[(operation.get('videoId'), operation.get('version'),
                                   operation.get('expectedRevision'))] = operation
        rows = []
        source_rows = self.store.list() if identity is None else [self.store.get(identity)]
        for row in source_rows:
            if not row or row.get('deletedAt') or (row['id'] in hidden and identity is None):
                continue
            active_operation = active_operations.get((row['id'], row.get('activeRevision'), row.get('revision')))
            visible_operation = ({key: active_operation.get(key) for key in
                ('id', 'kind', 'state', 'stage', 'message', 'errorCode', 'progress',
                 'successfulSamples', 'skippedSamples', 'createdAt', 'updatedAt', 'slicingItemId',
                 'version', 'expectedRevision')}
                if active_operation else None)
            recovery = None
            active = self.version(row, row.get('activeRevision'))
            if active and active.get('recovery'):
                recovery = copy.deepcopy(active['recovery'])
                slicing = getattr(self, 'slicing', None)
                if slicing and recovery.get('slicingItemId'):
                    item = slicing.store.get('items', recovery['slicingItemId'])
                    if (item and item.get('collectionVideoId') == row['id']
                            and item.get('collectionRevision') == row['activeRevision']
                            and not item.get('superseded')):
                        recovery.update(slicingState=item.get('state'), slicingMessage=item.get('message'),
                                       errorCode=item.get('errorCode'), progress=item.get('progress'))
                        recovery['categories'] = {name: {key: value.get(key) for key in
                            ('state', 'message', 'errorCode', 'observations', 'skippedSamples', 'imageCount', 'actualImages', 'packageId')}
                            for name, value in (item.get('categories') or {}).items()}
                        # Surface the current stage, not a stale recovered error.
                        recovery['message'] = item.get('message') or recovery.get('message', '')
                        if item.get('state') == 'platform_processing':
                            pending = [slicing.packages.store.get('items', value.get('packageId'))
                                       for value in (item.get('categories') or {}).values() if value.get('packageId')]
                            if slicing.packages.paused and any(p and p.get('state') in {'queued', 'uploaded', 'uploading'} for p in pending):
                                recovery.update(stage='photos_paused', message='照片上传已暂停；继续照片上传后会自动导入、核验并整理。')
                            else:
                                recovery.update(stage='photos_processing', message='正在上传或导入照片，核验完成后自动归入已保存。')
                        elif item.get('state') in {'pending', 'analyzing'}:
                            recovery['stage'] = 'analyzing'
            row = {**row, 'versions': [{**v, 'historyDownload': histories.get((row['id'],v['revision']))} for v in row['versions']],
                   'operation': visible_operation, 'recovery': recovery,
                   'finalProcessing': final_operations.get((row['id'], row.get('activeRevision'), row.get('revision'))),
                   'capabilities': {**(final_processing.capabilities(row) if final_processing else {}),
                                    **self.withdraw_capabilities(row)},
                   'submittedAt': (self.jobs.store.get(row.get('jobId', '')) or {}).get('createdAt') or ''}
            rows.append(row)
        found = filter_records(rows, query)
        return {"videos": found, "directory": self.store.directory(), "workspaceManaged": bool(getattr(self, 'workspace', None) and self.workspace.enabled), "totalCount": len(rows),
                "filteredCount": len(found), "byJob": {job_id: sum(r["jobId"] == job_id for r in rows)
                    for job_id in {r["jobId"] for r in rows}},
                "counts": {kind: sum(matches_status(row, kind) for row in rows)
                           for kind in ("ready", "waiting", "review", "download", "saved", "complete", "deleting", "processing")},
                "lastCheck": self.last_check, "checkError": self.storage_error or self.check_error,
                "hiddenRecordCount": len(hidden), "downloadWorkers": self.download_workers()}

    def download_candidate(self, row):
        if any(operation.get('kind') == 'restart-download' and operation.get('version') == row.get('activeRevision')
               and operation.get('expectedRevision') == row.get('revision')
               and operation.get('state') in {'queued', 'running', 'waiting_connection', 'reconciling'}
               for operation in self.store.list_operations(row['id'])):
            return False
        flow = getattr(self, 'final_processing', None)
        if flow and not flow.download_allowed(row):
            return False
        if row.get("deletedAt") or row.get("deletion", {}).get("state") in ("pending", "error"):
            return False
        active = self.version(row, row.get("activeRevision"))
        return bool(active and active.get("downloadRequested") and active["state"] in ("ready", "publishing")
                    and self.store.directory())

    def active_downloads(self):
        return {identity for identity, task in self.running.items()
                if not task.done() and task.get_name() == "publish-video"}

    def download_workers(self):
        active = self.active_downloads()
        queued = sum(self.download_candidate(row) and row["id"] not in active for row in self.store.list())
        return {"active": len(active), "queued": queued,
                "max": self.download_settings["maxConcurrentDownloads"],
                "revision": self.download_settings["revision"]}

    def update_download_limit(self, data):
        settings = self.download_settings
        limit = data["maxConcurrentDownloads"]
        if type(limit) is not int or not 1 <= limit <= MAX_DOWNLOAD_LIMIT:
            raise ValueError("同时下载数量须为1到8的整数。")
        requests = settings["requests"]
        if data["requestId"] in requests:
            if requests[data["requestId"]] != limit:
                raise Conflict("这个请求编号已用于其他下载设置。")
            return self.download_workers()
        if data["expectedRevision"] != settings["revision"]:
            raise Conflict("下载设置已在其他页面修改，请刷新后重试。")
        updated = {"maxConcurrentDownloads": limit,
                   "revision": settings["revision"] + (limit != settings["maxConcurrentDownloads"]),
                   "requests": dict(list((requests | {data["requestId"]: limit}).items())[-1000:])}
        self.store.save_download_settings(updated)
        self.download_settings = updated
        self.download_slots.set_limit(limit)
        self.publish_slots.set_limit(limit)
        self.schedule_downloads()
        self.wake.set()
        return self.download_workers()

    def schedule_downloads(self, *, ready_only=False, exclude=None):
        if self.stopping:
            return
        # Admit in record creation order; queued rows keep their current revision
        # and checkpoint and do not acquire a row lock until a slot is available.
        for row in reversed(self.store.list()):
            if row["id"] == exclude or not self.download_candidate(row):
                continue
            active = self.version(row, row["activeRevision"])
            if ready_only and (active["state"] != "ready" or active.get("publishing")):
                continue
            if self.target.connected or active.get("publishing"):
                self.schedule(row)

    def configure_directory(self, value):
        if getattr(self, 'workspace', None) and self.workspace.enabled:
            self.workspace.require_directory(value, download=True)
            return self.state()
        if self.fixed_directory:
            directory = prepare_directory(value)
            self.store.set_directory(directory)
            return self.state()
        directory = prepare_directory(value)
        self.store.set_directory(directory)
        return self.state()

    def prepare_fixed_storage(self):
        if getattr(self, 'workspace', None) and self.workspace.enabled:
            self.workspace.require_root()
            self.fixed_directory.mkdir(parents=True, exist_ok=True)
            self.work.mkdir(parents=True, exist_ok=True)
            return
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

    @staticmethod
    def initial_duration(item):
        start, end = parse_time(item.get('startTime')), parse_time(item.get('endTime'))
        if start and end and end > start:
            return round((end-start).total_seconds())
        return int(item.get('durationSeconds') or round(item.get('durationMinutes', 60)*60))

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
                                         "durationSeconds": self.initial_duration(item), "name": item["videoName"], "taskId": "", "resultId": "",
                                         "state": "pending", "createdAt": now_iso()}],
                           "requests": {}, "createdAt": now_iso(), "error": ""}
                original = row["versions"][0]
                changed = False
                if item.get("remoteId") and not original.get("taskId") and item.get("bindingVerified"):
                    original["taskId"] = str(item["remoteId"])
                    # A capture submitted after a cached inventory must not be
                    # reported missing from that older ten-second snapshot.
                    if original["taskId"] not in self.inventory_cache:
                        self.inventory_cached_at = 0
                        self.inventory_generation = getattr(self, "inventory_generation", 0) + 1
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
            self.schedule_downloads()  # Local publication can continue without a platform connection.
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
            await self.resume_operations()
            try:
                await asyncio.wait_for(self.wake.wait(), timeout=self._poll_timeout())
            except asyncio.TimeoutError:
                pass

    def _poll_timeout(self):
        """Idle checks stay at 30s. An open platform capture is checked again in 8s."""
        for row in self.store.list():
            for version in row.get("versions") or []:
                if version.get("state") == "waiting":
                    return ACTIVE_POLL_SECONDS
        return IDLE_POLL_SECONDS

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
        async with self.result_slots:
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

    async def remote_parents_cached(self, *, fresh=False):
        """Coalesce complete inventories for ten seconds; mutations always bypass this cache."""
        if not self.target.connected:
            raise RemoteError("请先连接采集平台。", "401")
        if not fresh and self.inventory_cache and time.monotonic() - self.inventory_cached_at < 10:
            return copy.deepcopy(self.inventory_cache)
        async with self.inventory_slots:
            async with self.inventory_lock:
                if not fresh and self.inventory_cache and time.monotonic() - self.inventory_cached_at < 10:
                    return copy.deepcopy(self.inventory_cache)
                for _ in range(3):
                    generation = getattr(self, "inventory_generation", 0)
                    parents = await self.jobs.remote_parents(self.target_config)
                    if generation != getattr(self, "inventory_generation", 0):
                        continue
                    self.inventory_cache = copy.deepcopy(parents)
                    self.inventory_cached_at = time.monotonic()
                    return copy.deepcopy(parents)
                raise RemoteError("新采集任务仍在登记，等待下一次完整核对。", "NETWORK")

    def remember_remote_parents(self, parents):
        """Refresh the shared view immediately after a confirmed platform write."""
        self.inventory_cache = copy.deepcopy(parents)
        self.inventory_cached_at = time.monotonic()

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
            candidates = []
            for item in self.store.list():
                if item.get("deletion", {}).get("state") in ("pending", "error"):
                    self.schedule(item)
                    continue
                active = self.version(item, item.get("activeRevision"))
                if item.get("cleanupPending") or (active and active.get("state") in {"submitted", "waiting", "uncertain"}):
                    candidates.append(item["id"])
            if not candidates:
                self.last_check = now_iso()
                return self.state()
            parents = await self.remote_parents_cached()
            semaphore = asyncio.Semaphore(3)

            async def check_identity(identity):
                async with semaphore:
                    snapshot = self.store.get(identity)
                    if not snapshot or snapshot.get("deletedAt"):
                        return
                    active = self.version(snapshot, snapshot.get("activeRevision"))
                    if not active or active.get("state") not in {"submitted", "waiting", "uncertain"}:
                        if snapshot.get("cleanupPending"):
                            self.schedule(snapshot)
                        return
                    before_revision = snapshot.get("revision")
                    before_active = snapshot.get("activeRevision")
                    try:
                        await self._check_row(snapshot, parents, active_only=True)
                        issue = ""
                    except (RemoteError, ValueError, httpx.HTTPError):
                        issue = "平台暂时无法核对本条视频；其他视频继续处理。"
                    async with self.row_lock(identity):
                        current = self.store.get(identity)
                        if (not current or current.get("deletedAt") or current.get("revision") != before_revision
                                or current.get("activeRevision") != before_active):
                            return
                        current_version = self.version(current, before_active)
                        checked_version = self.version(snapshot, before_active)
                        if current_version and checked_version:
                            self._merge_check_result(current_version, checked_version)
                        if issue:
                            current["error"] = issue
                        elif not current.get("cleanupPending"):
                            current["error"] = ""
                        self.store.save(current)
                        self.schedule(current)

            await asyncio.gather(*(check_identity(identity) for identity in candidates))
            self.last_check = now_iso()
        return self.state()

    @staticmethod
    def _merge_check_result(current, checked):
        # Keep concurrent download/correction progress; only merge remote facts.
        if current.get("taskId") and checked.get("taskId") and current["taskId"] != checked["taskId"]:
            return
        if current.get("resultId") and checked.get("resultId") and current["resultId"] != checked["resultId"]:
            return
        protected = {"saved", "downloading", "publishing", "failed"}
        for field in ("taskId", "resultId", "platformStatus", "review", "message"):
            value = checked.get(field)
            if field in {"taskId", "resultId"} and current.get(field) and not value:
                continue
            if field == "message" and current.get("state") in protected:
                continue
            if value is not None:
                current[field] = copy.deepcopy(value)
        if current.get("state") not in protected:
            current["state"] = checked.get("state", current.get("state"))

    async def _check_row(self, row, parents, *, active_only=False):
        versions = ([self.version(row, row.get("activeRevision"))] if active_only else row["versions"])
        for version in (item for item in versions if item):
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

    @staticmethod
    def operation_identity(video_id, kind, request_id):
        return hashlib.sha256(f"{video_id}:{kind}:{request_id}".encode()).hexdigest()[:32]

    @staticmethod
    def public_operation(operation):
        if not operation:
            return None
        return {key: operation.get(key) for key in (
            "id", "videoId", "kind", "state", "stage", "message", "errorCode",
            "failedStage", "failureType", "failureErrno",
            "expectedRevision", "version", "slicingItemId", "progress", "successfulSamples",
            "skippedSamples", "resultRevision", "createdAt", "updatedAt")}

    def launch_operation(self, operation_id):
        if self.stopping:
            return
        allowed = getattr(self, 'automatic_operation_allowed', None)
        if allowed and not allowed(self.store.get_operation(operation_id) or {}):
            return
        task = self.operation_tasks.get(operation_id)
        if task and not task.done():
            return
        task = asyncio.create_task(self._run_operation(operation_id), name="video-operation-" + operation_id)
        self.operation_tasks[operation_id] = task
        task.add_done_callback(lambda done, identity=operation_id:
                               self.operation_tasks.pop(identity, None) if self.operation_tasks.get(identity) is done else None)

    def queue_video_operation(self, identity, kind, payload):
        row = self.store.get(identity)
        if not row or row.get("deletedAt"):
            raise ValueError("视频记录不存在。")
        request_id = str(payload.get("requestId") or hashlib.sha256(str(time.time_ns()).encode()).hexdigest()[:24])
        expected_revision = payload.get("expectedRevision", row.get("revision"))
        operation_payload = {key: value for key, value in payload.items()
                             if key in {"startTime", "durationSeconds", "repair"}}
        operation_id = self.operation_identity(identity, kind, request_id)
        existing = self.store.get_operation(operation_id)
        if kind == 'restart-download' and not existing:
            existing = next((saved for saved in self.store.list_operations(identity)
                if saved.get('kind') == kind and request_id in saved.get('requestAliases', [])), None)
        version = payload.get("version", existing.get("version") if existing else row.get("activeRevision"))
        if existing:
            same_request = (
                existing.get("expectedRevision") == expected_revision
                and existing.get("version") == version
                and existing.get("payload", {}) == operation_payload
            )
            if not same_request:
                raise Conflict("请求编号已用于不同的操作内容；请刷新后使用新的请求编号。")
            return {"operationId": existing['id'], "state": existing["state"], "videoId": identity}
        legacy_restart = row.get('requests', {}).get(request_id)
        if kind == 'restart-download' and legacy_restart:
            if (legacy_restart.get('kind') != kind or legacy_restart.get('version') != version
                    or legacy_restart.get('expectedRevision') != expected_revision):
                raise Conflict('请求编号已用于其他版本或操作。')
            if legacy_restart.get('state') == 'complete':
                operation = {'id': operation_id, 'videoId': identity, 'requestId': request_id,
                    'kind': kind, 'state': 'complete', 'stage': 'download_queued',
                    'expectedRevision': expected_revision, 'version': version, 'payload': operation_payload,
                    'message': '这次完整重下已登记，保留后续下载进度。', 'createdAt': now_iso()}
                self.store.save_operation(operation)
                return {'operationId': operation_id, 'state': 'complete', 'videoId': identity}
        if type(expected_revision) is not int or expected_revision != row.get("revision"):
            raise Conflict("记录已变化，请刷新后重新核对。")
        if version != row.get("activeRevision"):
            raise Conflict("当前采集版本已变化，请刷新后重试。")
        self.require_editable(row)
        if kind == 'withdraw':
            from .video_withdrawal import capability
            eligibility = capability(self, row)
            if not eligibility['canWithdraw']:
                raise Conflict(eligibility['withdrawReason'])
            pending = next((entry for entry in self.store.list_operations(identity) if entry.get('kind') == 'withdraw'
                and entry.get('version') == version and entry.get('expectedRevision') == expected_revision
                and entry.get('state') in {'queued','running','waiting_connection','reconciling'}), None)
            if pending:
                return {'operationId':pending['id'], 'state':pending['state'], 'videoId':identity}
        if kind in {'correction', 'recapture'}:
            current = self.version(row, version)
            if kind == 'correction':
                start = parse_time(payload.get('startTime'))
                seconds = payload.get('durationSeconds')
                if not start or type(seconds) is not int or not 60 <= seconds <= 7200:
                    raise ValueError('请填写有效的采集开始时间和 1–120 分钟时长。')
                validator = getattr(self, 'validate_capture_interval', None)
                if validator:
                    validator(format_time(start), seconds)
                if (format_time(start) == current.get('startTime') and
                        format_time(start + timedelta(seconds=seconds)) == current.get('endTime')):
                    raise ValueError('采集区间未变化，可直接确认当前视频。')
            if ((current or {}).get('recovery', {}).get('state') in {'queued', 'moving', 'slicing'}
                    and not (current or {}).get('finalConfirmation')):
                # Reject before reserving a new media generation: an invalid
                # correction must not supersede/cancel the working recovery.
                raise Conflict('当前版本正在处理可解码部分，请先暂停此条再更正。')
        if kind == 'restart-download':
            current = self.version(row, version)
            if (not current or current.get('state') not in {'failed', 'ready'}
                    or current.get('publishing') or current.get('transferWorkerPid')
                    or not current.get('taskId') or not current.get('resultId')):
                raise ValueError('当前版本不能完整重下，已有成果和保存进度保留。')
        if kind in {'recover-decodable', 'restart-download'}:
            active = next((old for old in self.store.list_operations(identity)
                           if old.get('kind') == kind and old.get('version') == version
                           and old.get('expectedRevision') == expected_revision
                           and old.get('state') in {'queued', 'running', 'waiting_connection', 'reconciling', 'paused'}), None)
            if active:
                if active.get('payload', {}) != operation_payload:
                    raise Conflict('此视频已有不同的恢复操作，请等待或暂停原操作。')
                if kind == 'restart-download':
                    active['requestAliases'] = list(dict.fromkeys([*active.get('requestAliases', []), request_id]))
                    self.store.save_operation(active)
                return {'operationId': active['id'], 'state': active['state'], 'videoId': identity}
        operation = {"id": operation_id, "videoId": identity, "requestId": request_id,
                     "kind": kind, "state": "queued", "stage": "queued",
                     "expectedRevision": expected_revision, "version": version,
                     "payload": operation_payload,
                     "message": "已登记，后台马上处理。", "createdAt": now_iso()}
        if kind in {"correction", "recapture"} and getattr(self, "control", None):
            operation["mediaGeneration"] = self.control.reserve(identity, operation_id)
        if kind == 'restart-download':
            current = self.version(row, version)
            operation['source'] = {key: current.get(key) for key in ('taskId', 'resultId')}
            control = self.control.get('collection:' + identity) if getattr(self, 'control', None) else {}
            operation['sourceGeneration'] = int((control or {}).get('generation') or 0)
        if kind == 'withdraw' and getattr(self, 'control', None):
            self.control.refresh()
            with self.store.db:
                operation['mediaGeneration'] = self.control.reserve(identity, operation_id, commit=False, refresh=False)
                self.store.save_operation(operation, commit=False)
        else:
            self.store.save_operation(operation)
        self.launch_operation(operation_id)
        return {"operationId": operation_id, "state": "queued", "videoId": identity}

    def operation_state(self, operation_id):
        operation = self.store.get_operation(operation_id)
        if not operation:
            raise ValueError("后台操作不存在。")
        return self.public_operation(operation)

    async def _run_operation(self, operation_id):
        operation = self.store.get_operation(operation_id)
        if not operation or operation.get("state") in {"complete", "attention", "conflict"}:
            return
        try:
            operation.update(state="running", stage="starting", message="正在处理。")
            self.store.save_operation(operation)
            if operation["kind"] == "check":
                await self._execute_check_operation(operation)
            elif operation["kind"] == "recover-decodable":
                await self._execute_recovery_operation(operation)
            elif operation["kind"] == "restart-download":
                from .video_restart import execute
                await execute(self, operation)
            elif operation['kind'] == 'withdraw':
                from .video_withdrawal import execute
                await execute(self, operation)
            elif operation["kind"] in {"correction", "recapture"}:
                await self._execute_correction_operation(operation)
        except asyncio.CancelledError:
            latest = self.store.get_operation(operation_id)
            if latest and latest.get("state") not in {"complete", "attention", "conflict", "paused"}:
                latest.update(state="queued", message="程序正在停止；操作和进度已保存，重启后继续。")
                self.store.save_operation(latest)
            raise
        except (RemoteError, httpx.HTTPError) as error:
            latest = self.store.get_operation(operation_id)
            if latest:
                code = getattr(error, "code", "network")
                latest.update(state="waiting_connection" if not self.target.connected or str(code).upper() in {"401", "403", "NETWORK", "408", "429", "500", "502", "503", "504"} else "attention",
                              stage="waiting_connection" if not self.target.connected or str(code).upper() in {"401", "403", "NETWORK", "408", "429", "500", "502", "503", "504"} else "attention",
                              errorCode=code, message="平台暂时不可达，操作和媒体均保留；连接恢复后继续。")
                if latest.get('kind') == 'restart-download' and latest['state'] == 'waiting_connection':
                    latest['nextAttemptAt'] = time.time() + 30
                self.store.save_operation(latest)
        except (OSError, ValueError, Conflict) as error:
            latest = self.store.get_operation(operation_id)
            if latest:
                failure_stage = latest.get('stage')
                code = 'recovery_file_conflict' if isinstance(error, FileExistsError) else (
                    'disk_full' if getattr(error, 'errno', None) == errno.ENOSPC else 'operation_failed')
                message = ('恢复位置已有不同内容的文件，完整缓存保留；请核对恢复位置。'
                           if isinstance(error, FileExistsError) else
                           '磁盘空间不足，完整缓存和恢复进度保留。' if code == 'disk_full' else
                           str(error) if isinstance(error, (ValueError, Conflict)) else
                           '本地文件或工具暂不可用，原文件与进度已保留。')
                latest.update(state="conflict" if isinstance(error, Conflict) else "attention",
                              failedStage=failure_stage, failureType=type(error).__name__,
                              failureErrno=getattr(error, 'errno', None),
                              stage="attention", errorCode="conflict" if isinstance(error, Conflict) else code,
                              message=message)
                self.store.save_operation(latest)
                if (latest.get('kind') == 'restart-download'
                        and isinstance(error, (FileEnvironmentError, OSError)) and not isinstance(error, FileExistsError)):
                    latest.update(state='waiting_connection', stage='waiting_disk', nextAttemptAt=time.time() + 30,
                                  errorCode=getattr(error, 'code', code),
                                  message='等待文件目录恢复，原文件与断点保留。')
                    self.store.save_operation(latest)
                if latest.get("kind") == "recover-decodable":
                    try:
                        await self._update_recovery_version(
                            latest["videoId"], latest["version"], latest["expectedRevision"],
                            {"state": "attention", "errorCode": latest["errorCode"],
                             "message": latest["message"]})
                    except Conflict:
                        # A newer user edit owns the row; never put the old
                        # recovery error onto that newer version.
                        pass
        except Exception as error:
            latest = self.store.get_operation(operation_id)
            if latest:
                code = getattr(error, "code", "operation_failed")
                if code in {"auth", "network"}:
                    latest.update(state="waiting_connection", stage="waiting_connection", errorCode=code,
                                  message="平台暂不可用；视频、任务绑定和已保存进度均保留。")
                else:
                    latest.update(state="attention", stage="attention", errorCode="operation_failed",
                                  message="本条后台操作未完成，原视频和已保存进度保留。")
                self.store.save_operation(latest)
        finally:
            latest = self.store.get_operation(operation_id)
            if (getattr(self, "control", None) and latest and latest.get("kind") in {"correction", "recapture", "restart-download", "withdraw"}
                    and latest.get("state") in {"complete", "attention", "conflict"}):
                self.control.release_review(latest["videoId"], operation_id)

    async def _execute_check_operation(self, operation):
        identity = operation["videoId"]
        row = self.store.get(identity)
        if not row or row.get("deletedAt"):
            raise ValueError("视频记录不存在。")
        if row.get("revision") != operation["expectedRevision"] or row.get("activeRevision") != operation["version"]:
            raise Conflict("记录或采集版本已变化；没有覆盖更新。")
        if not self.target.connected:
            operation.update(state="waiting_connection", stage="waiting_connection", message="等待采集平台连接。")
            self.store.save_operation(operation)
            return
        operation.update(stage="reading_platform", message="正在读取当前版本对应的平台任务。")
        self.store.save_operation(operation)
        parents = await self.remote_parents_cached()
        snapshot = copy.deepcopy(row)
        await self._check_row(snapshot, parents, active_only=True)
        async with self.row_lock(identity):
            current = self.store.get(identity)
            if (not current or current.get("deletedAt") or current.get("revision") != operation["expectedRevision"]
                    or current.get("activeRevision") != operation["version"]):
                raise Conflict("核对期间记录已变化；最新人工修改已保留。")
            current_version = self.version(current, operation["version"])
            checked_version = self.version(snapshot, operation["version"])
            if current_version and checked_version:
                self._merge_check_result(current_version, checked_version)
            current["error"] = "" if not current.get("cleanupPending") else current.get("error", "")
            self.store.save(current)
        operation.update(state="complete", stage="complete", message="本条核对完成。")
        self.store.save_operation(operation)
        self.schedule(current)

    async def _execute_recovery_operation(self, operation):
        identity = operation["videoId"]
        row = self.store.get(identity)
        if not row or row.get("deletedAt"):
            raise ValueError("视频记录不存在。")
        if row.get("revision") != operation["expectedRevision"] or row.get("activeRevision") != operation["version"]:
            raise Conflict("记录或采集版本已变化；没有覆盖新版本。")
        version = self.version(row, operation["version"])
        if not version or not self.repairable_media_failure(version) or version.get("errorCode") not in {
                "invalid_video", "media_incomplete"}:
            raise ValueError("只有完整下载后出现媒体解码异常的当前版本可以恢复；其他异常按原状态保留。")
        if not self.target.connected:
            operation.update(state="waiting_connection", stage="waiting_connection", message="等待采集平台连接。")
            self.store.save_operation(operation)
            return
        if not getattr(self, "slicing", None):
            raise ValueError("本地切片服务尚未启动。")
        if not self.slicing.auth.connected:
            operation.update(state="waiting_connection", stage="waiting_connection", message="等待视觉AI平台连接。")
            self.store.save_operation(operation)
            return

        recovery = version.get("recovery") or {}
        if recovery.get("slicingItemId"):
            existing_item = self.slicing.store.get("items", recovery["slicingItemId"])
            if existing_item and existing_item.get("state") not in {"attention", "excluded"}:
                await self._monitor_recovery(operation, recovery["slicingItemId"])
                return
            # An earlier attempt may have stopped on a repairable local/model
            # error. Revalidate the exact file and let queue_collection_recovery
            # resume that same item; never create a second item for the source.
        operation.update(stage="validating_cache", message="正在验证已下载文件、平台绑定和文件版本。")
        self.store.save_operation(operation)
        if not version.get("taskId") or not version.get("resultId"):
            raise ValueError("当前版本没有明确的平台任务和结果ID，原绑定保留，不能开始恢复。")
        if not version.get("downloadRequested") or not version.get("totalBytes"):
            raise ValueError("没有完整下载凭证；请先核对下载状态，不能把部分下载当作原片。")
        etag = str(version.get("etag") or "")
        if not etag or etag.startswith("W/"):
            raise ValueError("下载文件缺少强版本校验标记；原缓存保留，转人工核对。")
        result = await self.bound_result(version)
        if str(result.get("id")) != str(version["resultId"]):
            raise Conflict("平台视频结果ID已变化；原绑定保留。")
        await self._verify_cached_media_object(version, result)

        source = self.work / identity / f"v{version['revision']}.part"
        prior_destination = recovery.get("filePath") or recovery.get("destination") or operation.get("destination")
        if prior_destination:
            destination = Path(prior_destination)
        elif getattr(self, "workspace", None) and self.workspace.enabled:
            filename = video_filename({"videoName": row["name"], "startTime": version["startTime"]},
                                      {"id": version["resultId"], "url": ".mp4"})
            destination = self.workspace.download_destination(filename)
        else:
            filename = video_filename({"videoName": row["name"], "startTime": version["startTime"]},
                                      {"id": version["resultId"], "url": ".mp4"})
            destination = self.work / "partial-recovery" / identity / filename
        await asyncio.to_thread(destination.parent.mkdir, parents=True, exist_ok=True)
        source_exists = await asyncio.to_thread(source.exists)
        if source_exists:
            source_identity, digest = await asyncio.to_thread(
                inspect_recovery_cache, source, self.work, version["totalBytes"],
                version.get("receivedBytes"), recovery.get("sha256"))
            reservation = operation.get('recoveryReservation') or {'token': uuid4().hex}
            operation['recoveryReservation'] = reservation
            self.store.save_operation(operation)
            destination, conflict = await self.file_operation(
                recovery_destination, destination, version['totalBytes'], digest, reservation)
            if conflict:
                operation['preservedConflict'] = conflict
            await self._update_recovery_version(identity, version["revision"], operation["expectedRevision"],
                {"sha256": digest, "destination": str(destination), "state": "moving",
                 **({'preservedConflict': conflict} if conflict else {})})
            operation.update(stage="moving_cache", message="文件完整性已核验，正在移入受管理目录。",
                             destination=str(destination), sourceSha256=digest)
            self.store.save_operation(operation)
            with ExitStack() as guards:
                if getattr(self, "file_leases", None):
                    guards.enter_context(self.file_leases.write(source))
                    guards.enter_context(self.file_leases.write(destination))
                await self.file_operation(publish_recovery_cache, source, destination, source_identity, digest, reservation)
        elif await asyncio.to_thread(destination.is_file) and not await asyncio.to_thread(destination.is_symlink):
            digest = recovery.get("sha256") or operation.get("sourceSha256")
            if not digest:
                raise Conflict("断点恢复目录中的文件与本次操作凭证不符；文件保留。")
            await asyncio.to_thread(verify_existing_recovery_destination, destination, digest)
        else:
            raise ValueError("已下载缓存和恢复文件均不存在；不会重新下载或重新生成视频。")

        from .slicing import ffprobe_file, source_version
        operation.update(stage="checking_media", message="正在读取可用视频元数据；末段完整性异常不会阻止抽取其他位置。")
        self.store.save_operation(operation)
        metadata = await asyncio.to_thread(ffprobe_file, destination, self.slicing.store.settings["frameToolPath"])
        file_version = await asyncio.to_thread(source_version, destination)
        if int(file_version["size"]) != int(version["totalBytes"]):
            raise Conflict("恢复文件长度与平台下载长度不符；文件保留。")
        await self._update_recovery_version(identity, version["revision"], operation["expectedRevision"],
            {"state": "queued", "destination": str(destination), "filePath": str(destination),
             "fileVersion": file_version, "sha256": digest, "slicingItemId": "",
             "successfulSamples": 0, "skippedSamples": 0, "partialOnly": True})
        operation.update(stage="queueing_slicing", message="缓存已验证；仅排队处理可解码部分。",
                         destination=str(destination), sourceSha256=digest)
        self.store.save_operation(operation)
        item = self.slicing.queue_collection_recovery(video_id=identity, revision=version["revision"],
            path=destination, file_version=file_version, metadata=metadata, name=destination.name,
            request_id=operation["requestId"])
        current = self.store.get(identity)
        approved = self.version(current, version['revision']) if current else None
        if (current and current.get('revision') == operation['expectedRevision']
                and current.get('activeRevision') == version['revision']
                and (approved or {}).get('finalConfirmation')):
            item['finalProcessingIntent'] = approved['finalConfirmation']['operationId']
            self.slicing.store.save('items', item)
        await self._update_recovery_version(identity, version["revision"], operation["expectedRevision"],
                                            {"state": "slicing", "slicingItemId": item["id"]})
        operation.update(state="running", stage="partial_slicing", slicingItemId=item["id"],
                         message="正在识别并处理可解码部分；跳过位置不会生成替代视频。")
        self.store.save_operation(operation)
        await self._monitor_recovery(operation, item["id"])

    async def _monitor_recovery(self, operation, slicing_item_id):
        identity = operation["videoId"]
        while not self.stopping:
            saved_operation = self.store.get_operation(operation["id"])
            if saved_operation and saved_operation.get("state") == "paused":
                await asyncio.sleep(2)
                continue
            item = self.slicing.store.get("items", slicing_item_id) if getattr(self, "slicing", None) else None
            if not item:
                raise ValueError("本地切片记录不存在；已移动的原片保留。")
            if item.get("superseded"):
                operation.update(state="conflict", stage="superseded", message="旧版恢复已停止，按最新视频指令执行。")
                self.store.save_operation(operation)
                return
            row = self.store.get(identity)
            version = self.version(row, operation["version"]) if row else None
            if row and (row.get("revision") != operation["expectedRevision"]
                        or row.get("activeRevision") != operation["version"]):
                operation.update(state="conflict", stage="conflict", errorCode="revision_changed",
                                 message="视频采集版本已更正；原有更正保留，恢复结果没有写入新版。")
                self.store.save_operation(operation)
                return
            if version:
                category_rows = item.get("categories") or {}
                values = dict(
                    successfulSamples=sum(int(category.get("observations") or 0) for category in category_rows.values()),
                    skippedSamples=sum(int(category.get("skippedSamples") or 0) + int(category.get("skippedExportFrames") or 0)
                                       for category in category_rows.values()),
                    slicingState=item.get("state"), slicingItemId=slicing_item_id)
                await self._update_recovery_version(identity, operation["version"], operation["expectedRevision"], values)
            if item.get("state") in {"complete", "no_target", "uncuttable"}:
                self.reconcile_recovery_results(slicing_item_id)
                current = self.store.get(identity)
                current_version = self.version(current, operation['version']) if current else None
                if not (current_version or {}).get('recovery', {}).get('resultVerified'):
                    raise ValueError('照片尚未全部导入核验，原片与已有成果保留。')
                if version:
                    await self._update_recovery_version(identity, operation["version"], operation["expectedRevision"],
                        {"state": "partial_complete", "finishedAt": now_iso(), "errorCode": "",
                         "message": current_version['recovery']['message']})
                operation.update(state="complete", stage="partial_complete", message="可解码部分已处理；未生成替代视频。",
                                 successfulSamples=(version or {}).get("recovery", {}).get("successfulSamples", 0),
                                 skippedSamples=(version or {}).get("recovery", {}).get("skippedSamples", 0))
                self.store.save_operation(operation)
                return
            if item.get("state") in {"attention", "excluded"}:
                if version:
                    await self._update_recovery_version(identity, operation["version"], operation["expectedRevision"],
                        {"state": "attention", "errorCode": item.get("errorCode", "processing_error"),
                         "message": item.get("message", "可解码部分处理需要核对。")})
                operation.update(state="attention", stage="attention", errorCode=item.get("errorCode", "processing_error"),
                                 message=item.get("message", "可解码部分处理需要核对；原视频保留。"))
                self.store.save_operation(operation)
                return
            if not self.slicing.auth.connected:
                operation.update(state="waiting_connection", stage="waiting_connection", message="等待视觉AI平台连接；恢复文件和抽样进度均保留。")
                self.store.save_operation(operation)
                return
            operation.update(state="running", stage="partial_slicing", progress=item.get("progress", 0),
                             message=item.get("message") or "正在处理可解码部分。")
            self.store.save_operation(operation)
            await asyncio.sleep(10)
        if not self.stopping:
            return
        operation.update(state="queued", stage="partial_slicing", message="服务正在停止；恢复进度已保存，重启后继续。")
        self.store.save_operation(operation)

    async def _verify_cached_media_object(self, version, result):
        url = download_url(result["url"])
        try:
            async with self.target.client.stream("GET", url, headers={
                    "Accept-Encoding": "identity", "Range": "bytes=0-0", "If-Range": version["etag"]},
                    timeout=httpx.Timeout(30, connect=15)) as response:
                content_range = response.headers.get("content-range", "")
                match = re.fullmatch(r"bytes 0-0/(\d+)", content_range)
                if (response.status_code != 206 or response.headers.get("etag") != version["etag"]
                        or not match or int(match[1]) != int(version["totalBytes"])):
                    raise ValueError("平台媒体对象的ETag或长度已变化；缓存文件保留，未开始处理。")
                first = await response.aread()
                if len(first) != 1:
                    raise ValueError("平台媒体对象范围校验未通过；缓存文件保留。")
        except httpx.HTTPError:
            raise RemoteError("暂时无法核对平台媒体对象版本；缓存文件保留，稍后可重试。", "network") from None

    async def resume_operations(self):
        if self.stopping:
            return
        self.reconcile_recovery_results()
        for operation in self.store.list_operations():
            if operation.get("state") in {"queued", "waiting_connection", "reconciling"}:
                if operation.get('nextAttemptAt', 0) > time.time():
                    continue
                if operation.get("state") == "waiting_connection":
                    if not self.target.connected:
                        continue
                    if operation.get("kind") == "recover-decodable" and not getattr(
                            getattr(self, "slicing", None), "auth", self.target).connected:
                        continue
                self.launch_operation(operation["id"])

    def reconcile_recovery_results(self, item_id=None):
        slicing = getattr(self, 'slicing', None)
        if not slicing or not getattr(slicing, 'packages', None):
            return
        for row in self.store.list():
            if self.row_lock(row['id']).locked():
                continue
            version = self.version(row, row.get('activeRevision'))
            recovery = (version or {}).get('recovery') or {}
            identity = recovery.get('slicingItemId')
            if not identity or (item_id and identity != item_id):
                continue
            proof = self.recovery_completion_proof(row, version)
            if proof is None:
                continue
            self.finish_recovery_result(row, version, proof)

    def recovery_completion_proof(self, row, version):
        slicing = getattr(self, 'slicing', None)
        identity = (version or {}).get('recovery', {}).get('slicingItemId')
        if not identity or not slicing or not getattr(slicing, 'packages', None):
            return None
        item = slicing.store.get('items', identity)
        if (not item or str(item.get('collectionVideoId')) != str(row['id'])
                or item.get('collectionRevision') != version['revision']
                or (getattr(self, 'control', None) and not self.control.allowed(item))):
            return None
        return verified_result(item, lambda key: slicing.packages.store.get('items', key))

    def finish_recovery_result(self, row, version, proof):
        recovery = version.get('recovery') or {}
        if not partial_complete(version) or recovery.get('results') != proof:
            has_images = any(value.get('images', 0) > 0 for value in proof.values())
            no_frames = all(value.get('state') == 'uncuttable' for value in proof.values())
            end_reason = 'partial_complete' if has_images else 'no_usable_frames' if no_frames else 'no_target'
            message = ('可用部分处理完成，照片已导入核验，坏位置已跳过。' if has_images else
                       '按配置抽样未找到可用画面，处理已结束；未上传空照片包。' if no_frames else
                       '可用画面未检出目标，处理已结束；未上传空照片包。')
            recovery.update(state='partial_complete', resultVerified=True, results=proof, errorCode='',
                            endReason=end_reason, finishedAt=now_iso(), message=message)
            version.update(recovery=recovery, downloadRequested=False, processingState='complete')
            self.store.save(row)
        for operation in self.store.list_operations(row['id']):
            if (operation.get('kind') == 'recover-decodable' and operation.get('version') == version['revision']
                    and operation.get('expectedRevision') == row['revision']
                    and operation.get('state') not in {'complete', 'conflict'}):
                operation.update(state='complete', stage='partial_complete', errorCode='',
                                 message=recovery['message'], results=proof)
                self.store.save_operation(operation)

    async def classify_partial_saved(self, identity, payload):
        """Reconcile selected receipts only; never approve media or launch work."""
        async with self.row_lock(identity):
            row = self.store.get(identity)
            if not row or row.get('deletedAt'):
                raise ValueError('视频记录不存在。')
            signature = {k:payload[k] for k in ('version', 'expectedRevision')}
            if row['revision'] != payload['expectedRevision'] or row['activeRevision'] != payload['version']:
                raise Conflict('视频已有新版本或更正，未覆盖，请重新选择。')
            previous = row['requests'].get(payload['requestId'])
            if previous:
                if previous.get('kind') != 'classify-saved' or previous.get('signature') != signature:
                    raise Conflict('请求编号已用于其他操作。')
                return row
            if row.get('deletion', {}).get('state') in {'pending', 'error'} or row.get('cleanupPending'):
                raise Conflict('此条正在清理或替换旧版，稍后再归类。')
            version = self.version(row, payload['version'])
            if version['state'] != 'saved':
                proof = self.recovery_completion_proof(row, version)
                if proof is None:
                    raise ValueError('可用部分尚未完成切片、图片上传和导入核验，保留当前状态。')
                self.finish_recovery_result(row, version, proof)
            row['requests'][payload['requestId']] = {'kind':'classify-saved', 'signature':signature,
                                                    'classifiedAt':now_iso()}
            self.store.save(row)
            return row

    async def check_video(self, identity, payload=None):
        return self.queue_video_operation(identity, "check", payload or {})

    async def recover_decodable(self, identity, payload):
        return self.queue_video_operation(identity, "recover-decodable", payload)

    def _recovery_operation(self, identity, revision):
        return next((operation for operation in reversed(self.store.list_operations(identity))
                     if operation.get("kind") == "recover-decodable" and operation.get("version") == revision), None)

    async def pause_recovery(self, identity, payload):
        row = self.store.get(identity)
        if not row or row.get("revision") != payload["expectedRevision"] or row.get("activeRevision") != payload.get("version"):
            raise Conflict("采集记录或版本已变化，请刷新后重试。")
        operation = self._recovery_operation(identity, payload["version"])
        if not operation or operation.get("state") not in {"queued", "running", "waiting_connection", "reconciling", "paused"}:
            raise ValueError("当前没有可暂停的部分恢复操作。")
        if operation.get("state") == "paused":
            return self.public_operation(operation)
        operation.update(state="paused", stage="paused", message="此条恢复已暂停；其他视频和平台任务继续运行。")
        self.store.save_operation(operation)
        item_id = operation.get("slicingItemId") or (self.version(row, payload["version"]) or {}).get("recovery", {}).get("slicingItemId")
        if item_id and getattr(self, "slicing", None):
            await self.slicing.pause_collection_recovery(item_id)
        await self._update_recovery_version(identity, payload["version"], payload["expectedRevision"],
                                            {"state": "paused", "slicingItemId": item_id or ""})
        return self.public_operation(operation)

    async def resume_recovery(self, identity, payload):
        row = self.store.get(identity)
        if not row or row.get("revision") != payload["expectedRevision"] or row.get("activeRevision") != payload.get("version"):
            raise Conflict("采集记录或版本已变化，请刷新后重试。")
        operation = self._recovery_operation(identity, payload["version"])
        if not operation or operation.get("state") != "paused":
            raise ValueError("当前没有已暂停的部分恢复操作。")
        item_id = operation.get("slicingItemId") or (self.version(row, payload["version"]) or {}).get("recovery", {}).get("slicingItemId")
        if item_id:
            self.slicing.resume_collection_recovery(item_id)
            await self._update_recovery_version(identity, payload["version"], payload["expectedRevision"],
                                                {"state": "slicing", "slicingItemId": item_id})
        operation.update(state="running" if item_id else "queued",
                         stage="partial_slicing" if item_id else "queued", message="继续处理可解码部分。")
        self.store.save_operation(operation)
        self.launch_operation(operation["id"])
        return self.public_operation(operation)

    async def _update_recovery_version(self, identity, revision, expected_revision, values):
        async with self.row_lock(identity):
            latest = self.store.get(identity)
            if (not latest or latest.get("deletedAt") or latest.get("revision") != expected_revision
                    or latest.get("activeRevision") != revision):
                raise Conflict("记录或当前采集版本已变化；新版本和恢复文件均保留。")
            current = self.version(latest, revision)
            if not current:
                raise Conflict("恢复对应的采集版本不存在；文件保留。")
            current.setdefault("recovery", {}).update(copy.deepcopy(values))
            self.store.save(latest)
            return latest, current

    def schedule(self, row):
        if self.stopping:
            return
        if getattr(self, 'workspace', None) and self.workspace.blocked:
            return
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
        if self.download_candidate(row):
            if len(self.active_downloads()) >= self.download_settings["maxConcurrentDownloads"]:
                return
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
            if done.get_name() == "publish-video" and not self.stopping:
                # Refill real queued downloads immediately. Interrupted transfers
                # retry on the regular poll, avoiding a tight retry loop offline.
                row = self.store.get(key)
                active = self.version(row, row.get("activeRevision")) if row else None
                if not active or not active.get("downloadRequested") or active["state"] in ("saved", "failed"):
                    self.schedule_downloads(ready_only=True, exclude=key)
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

    def cloud_lease_key(self, task_id):
        return self.cloud_lease_root / hashlib.sha256(str(task_id).encode()).hexdigest()

    @staticmethod
    def require_editable(row):
        if row.get("deletion", {}).get("state") in ("pending", "error"):
            raise Conflict("本条正在清理；请先完成或停止继续清理，不能覆盖当前操作。")

    @staticmethod
    def repairable_media_failure(version):
        return (version.get("state") == "failed" and version.get("errorCode") in (
                    "invalid_video", "media_incomplete", "range_invalid", "object_changed", "invalid_length")
                and bool(version.get("taskId")) and bool(version.get("resultId")))

    async def correct(self, identity, payload):
        return self.queue_video_operation(identity, "correction", payload)

    async def recapture_unavailable(self, identity, payload):
        operation_id = self.operation_identity(identity, "recapture", payload.get("requestId", ""))
        if payload.get("requestId") and self.store.get_operation(operation_id):
            return self.queue_video_operation(identity, "recapture", payload)
        row = self.store.get(identity)
        if not row or row.get("deletedAt"):
            raise ValueError("视频记录不存在。")
        if row.get("revision") != payload.get("expectedRevision"):
            raise Conflict("记录已变化，请刷新后重新核对。")
        version = self.version(row, payload.get("version"))
        if not version or row.get("activeRevision") != payload.get("version"):
            raise Conflict("当前采集版本已变化，请刷新后重试。")
        review_code = (version.get("review") or {}).get("code")
        if not self.repairable_media_failure(version) and review_code not in {
                "task_missing", "result_missing", "platform_failed"}:
            raise ValueError("当前异常不能通过重新采集修复；原任务和版本保持不变。")
        return self.queue_video_operation(identity, "recapture", payload)

    async def _execute_correction_operation(self, operation):
        identity = operation["videoId"]
        current = self.store.get(identity)
        submitted = (current or {}).get("requests", {}).get(operation["requestId"], {})
        if not current or (current.get("revision") != operation["expectedRevision"] and
                           not (submitted.get("kind") == "correction" and submitted.get("version") == current.get("activeRevision"))):
            raise Conflict("记录已变化，请重新核对更正。")
        if getattr(self, "control", None):
            operation.update(stage="stopping_old_processing", message="正在停止旧版本地处理，等待文件安全释放。")
            self.store.save_operation(operation)
            await self.control.take_over(identity, operation["id"], operation.get("mediaGeneration"))
        payload = dict(operation.get("payload") or {})
        payload.update(requestId=operation["requestId"], expectedRevision=operation["expectedRevision"])
        payload["mediaGeneration"] = operation.get("mediaGeneration")
        if operation["kind"] == "recapture":
            row = self.store.get(identity)
            version = self.version(row, operation["version"]) if row else None
            if not row or row.get("revision") != operation["expectedRevision"] or not version:
                raise Conflict("记录或版本已变化；没有覆盖更新。")
            payload.update(startTime=version["startTime"], durationSeconds=version["durationSeconds"], repair=True)
        operation.update(stage="preparing", message="正在核对采集区间和平台现有任务。")
        self.store.save_operation(operation)
        try:
            result = await self._perform_correction(identity, payload)
        finally:
            if getattr(self, "control", None):
                self.control.release_review(identity, operation["id"])
        if result.get("reconcilePending"):
            operation.update(state="reconciling", stage="reconciling",
                             message="提交结果尚未出现在平台任务列表；继续核对，不会重复提交。")
            self.store.save_operation(operation)
            return
        operation.update(state="complete", stage="complete", resultRevision=result.get("revision"),
                         message="操作已登记到平台；后台继续核对任务结果。")
        self.store.save_operation(operation)

    async def _perform_correction(self, identity, payload):
        identity = str(identity)
        request_id = payload["requestId"]
        signature = {key: payload[key] for key in ("startTime", "durationSeconds", "expectedRevision")}
        if payload.get("repair"):
            signature["repair"] = True

        async with self.row_lock(identity):
            row = self.store.get(identity)
            if not row or row.get("deletedAt"):
                raise ValueError("视频记录不存在。")
            existing = row["requests"].get(request_id)
            if existing:
                if existing.get("kind") != "correction" or existing.get("signature") != signature:
                    raise Conflict("请求ID已用于其他操作。")
                existing_version = self.version(row, existing.get("version"))
                if not existing_version:
                    raise Conflict("已登记的更正版本缺失，请人工核对；不会重复提交。")
                if existing_version.get("taskId"):
                    return row
                reconcile_version = copy.deepcopy(existing_version)
                snapshot = copy.deepcopy(row)
            else:
                if payload["expectedRevision"] != row["revision"]:
                    raise Conflict("记录已变化，请刷新后重新核对时间。")
                self.require_editable(row)
                if row.get("activeRevision") != payload.get("expectedRevision") and payload.get("repair"):
                    # The original version may be zero while the row revision is newer.
                    pass
                current = self.version(row, row["activeRevision"])
                if not current or current["state"] not in ("ready", "saved", "waiting", "submitted", "attention", "failed"):
                    raise ValueError("当前版本正在提交、下载或保存；请完成核对后再更正。")
                if current.get("writeAttempted") and not current.get("taskId") and current.get('submissionOutcome') != 'rejected':
                    raise ValueError("上次提交的任务ID尚未确认，请先核对平台，避免重复提交。")
                if current.get("recovery", {}).get("state") in {"queued", "moving", "slicing"}:
                    raise Conflict("当前版本正在处理可解码部分，请等待该操作结束后再更正。")
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
                current_snapshot = copy.deepcopy(current)
                snapshot = copy.deepcopy(row)
                reconcile_version = None

        if reconcile_version:
            if not self.target.connected:
                raise RemoteError("等待采集平台连接。", "401")
            parents = await self.remote_parents_cached(fresh=True)
            self.remember_remote_parents(parents)
            before_ids = set(reconcile_version.get("beforeIds") or [])
            candidates = [parent for key, parent in parents.items() if key not in before_ids
                          and self.matches(parent, reconcile_version, self.target_config["cameraId"])]
            async with self.row_lock(identity):
                latest = self.store.get(identity)
                found = self.version(latest, reconcile_version["revision"]) if latest else None
                if not found or (found.get("taskId") and candidates and found["taskId"] != str(candidates[0]["id"])):
                    raise Conflict("平台核对期间版本已变化；现有绑定保留。")
                if len(candidates) == 1:
                    found.update(taskId=str(candidates[0]["id"]), state="submitted")
                    found.pop("message", None)
                    self.store.save(latest)
                    self.wake.set()
                    return latest
                if len(candidates) > 1:
                    found.update(state="attention", message="多个新任务匹配本次更正，请人工核对；不会再次提交。")
                    self.store.save(latest)
                    return latest
                found.update(state="uncertain", message="提交结果尚未出现在平台任务列表；继续核对，不会重复提交。")
                self.store.save(latest)
                return {**latest, "reconcilePending": True}

        async with self.jobs.lock:
            await self.jobs.camera(self.target_config)
            parents = await self.jobs.remote_parents(self.target_config)
            current = self.version(snapshot, snapshot["activeRevision"])
            if payload.get("repair"):
                await self._check_row(snapshot, parents, active_only=True)
                checked = self.version(snapshot, snapshot["activeRevision"])
                if checked and checked.get("state") == "ready":
                    async with self.row_lock(identity):
                        latest = self.store.get(identity)
                        if latest and latest.get("revision") == payload["expectedRevision"]:
                            self._merge_check_result(self.version(latest, snapshot["activeRevision"]), checked)
                            self.store.save(latest)
                            return latest
                    raise Conflict("核对期间记录已变化；新版本保留。")
                if (not self.repairable_media_failure(checked or current) and (checked or current).get("review", {}).get("code")
                        not in ("task_missing", "task_identity_changed", "result_missing", "platform_failed")):
                    raise ValueError("该异常需要先人工核对任务或结果身份，不能直接重新采集。")
            version = {"revision": snapshot["revision"] + 1, "startTime": start_text, "endTime": end_text,
                       "durationSeconds": payload["durationSeconds"], "name": snapshot["name"],
                       "taskId": "", "resultId": "", "state": "submitting", "createdAt": now_iso(),
                       "beforeIds": sorted(parents), "writeAttempted": False,
                       "previousActiveRevision": snapshot["activeRevision"]}
            if any(self.matches(parent, version, self.target_config["cameraId"])
                   and not (payload.get("repair") and str(parent["id"]) == current.get("taskId"))
                   for parent in parents.values()):
                raise ValueError("采集平台已有相同名称和时段的视频任务，请先核对，避免重复采集。")
            source = {"plate": snapshot["plate"], "photoCount": snapshot["photoCount"],
                      "inspectionDurationSeconds": snapshot["inspectionDurationSeconds"],
                      "admissionRule": snapshot.get("admissionRule")}
            task = {**source, "videoName": snapshot["name"], "taskName": snapshot["name"],
                    "startTime": start_text, "endTime": end_text}
            async with self.row_lock(identity):
                latest = self.store.get(identity)
                if (not latest or latest.get("deletedAt") or latest.get("revision") != payload["expectedRevision"]
                        or latest.get("activeRevision") != snapshot.get("activeRevision")):
                    raise Conflict("采集区间核对期间记录已变化；未提交，也未覆盖人工修改。")
                if getattr(self, "control", None) and payload.get("mediaGeneration") is not None:
                    control = self.control.get("collection:" + identity)
                    if not control or control["generation"] != payload["mediaGeneration"]:
                        raise Conflict("已有更新的视频指令，未提交旧操作。")
                if request_id in latest["requests"]:
                    raise Conflict("此更正已登记，正在核对平台结果；不会重复提交。")
                latest["revision"] += 1
                latest["activeRevision"] = version["revision"]
                latest["versions"].append(version)
                latest["requests"][request_id] = {"kind": "correction", "signature": signature,
                                                    "version": version["revision"]}
                version["writeAttempted"] = True
                version["state"] = "uncertain"
                version["message"] = "提交结果正在核对；不会重复发送。"
                self.store.save(latest)

            try:
                await self.target.request("/data-acquisition/batchSave", method="POST",
                                          body=[gather_payload(task, self.target_config)], write=True)
            except (RemoteError, httpx.HTTPError) as error:
                # The pre-submit snapshot and request ID are durable. Reconciliation
                # below is the only safe next action; never replay this write blindly.
                if isinstance(error, RemoteError) and not error.ambiguous and error.code not in {'NETWORK','RESPONSE','401','403','11015'}:
                    async with self.row_lock(identity):
                        latest = self.store.get(identity)
                        rejected = self.version(latest, version['revision']) if latest else None
                        if rejected and not rejected.get('taskId'):
                            rejected.update(state='attention', submissionOutcome='rejected', errorCode='capture_rejected',
                                            message='平台明确拒绝本次采集，可修改区间或撤回更正。')
                            self.store.save(latest)
                    raise ValueError('平台明确拒绝本次采集，可修改区间或撤回更正。') from None
                pass
            try:
                after = await self.jobs.remote_parents(self.target_config)
            except (RemoteError, httpx.HTTPError):
                after = None
        if after is None:
            return {**(self.store.get(identity) or {}), "reconcilePending": True}
        self.remember_remote_parents(after)
        candidates = [parent for key, parent in after.items() if key not in version["beforeIds"]
                      and self.matches(parent, version, self.target_config["cameraId"])]
        async with self.row_lock(identity):
            latest = self.store.get(identity)
            found = self.version(latest, version["revision"]) if latest else None
            if not found:
                raise Conflict("已提交的版本记录不见了；请人工核对，程序不会重发。")
            if len(candidates) == 1:
                found.update(taskId=str(candidates[0]["id"]), state="submitted")
                found.pop("message", None)
            elif len(candidates) > 1:
                found.update(state="attention", message="多个新任务匹配本次更正，请人工核对；不会再次提交。")
            else:
                found.update(state="uncertain", message="提交结果尚未出现在平台任务列表；继续核对，不会重复提交。")
            self.store.save(latest)
            if not candidates:
                return {**latest, "reconcilePending": True}
        self.wake.set()
        return latest

    async def withdraw(self, identity, payload):
        return self.queue_video_operation(identity, 'withdraw', payload)

    def withdraw_capabilities(self, row):
        from .video_withdrawal import capability
        return capability(self, row)

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
        current = self.store.get(identity)
        current_version = self.version(current, current["activeRevision"]) if current else None
        if not current or current.get("deletedAt"):
            raise ValueError("视频记录不存在。")
        self.require_editable(current)
        if not current_version or not current_version.get("downloadRequested") or current_version["state"] not in ("failed", "ready"):
            raise ValueError("当前视频没有可重试的下载。")
        takeover_request = "retry-download-" + uuid4().hex
        if getattr(self, "control", None):
            await self.control.take_over(identity, takeover_request)
            control = self.control.get("collection:" + identity)
            if control.get("requestId") != takeover_request:
                raise Conflict("已有更新的视频指令，下载未重试。")
            self.control.release_review(identity, takeover_request)
        async with self.row_lock(identity):
            row = self.store.get(identity)
            if not row or row.get("deletedAt"):
                raise ValueError("视频记录不存在。")
            self.require_editable(row)
            version = self.version(row, row["activeRevision"])
            if (not version or row["revision"] != current["revision"] or version["revision"] != current_version["revision"]
                    or version["state"] not in ("failed", "ready")):
                raise Conflict("视频版本已变化，未重试其他版本。")
            version.update(state="ready", downloadRequested=True)
            version.pop("message", None)
            flow = getattr(self, 'final_processing', None)
            if flow and version.get('finalConfirmation'):
                flow.rebind_confirmed_retry(row, version, takeover_request)
            else:
                self.store.save(row)
            self.schedule(row)
            return row

    async def restart_download(self, identity, payload):
        """Register first; offline checks never change approval or checkpoints."""
        return self.queue_video_operation(identity, 'restart-download', payload)

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
            flow = getattr(self, 'final_processing', None)
            if flow and version.get('finalConfirmation'):
                flow.pause_download(row, version)
            row["requests"][payload["requestId"]] = {"kind": "cancel-download", "version": payload["version"]}
            self.store.save(row)
            return row

    async def bulk_action(self, action, payload):
        if action not in {'confirm', 'remove', 'classify-saved'}:
            raise ValueError('批量操作无效。')
        selections = payload["selections"]
        if len({v["id"] for v in selections}) != len(selections):
            raise ValueError("同一视频只能选择一次。")
        outcomes = []
        for selected in selections:
            data = {**selected, "requestId": payload["requestId"] + "-" + selected["id"]}
            try:
                if action == "confirm":
                    result = await self.confirm(selected["id"], {**data, "waitForCompletion": True})
                elif action == 'classify-saved':
                    result = await self.classify_partial_saved(selected['id'], data)
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
        if getattr(self, "control", None) and not existing:
            await self.control.take_over(identity, request_id)
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
                    paths.update(v.get("recovery", {}).get("filePath") or v.get("recovery", {}).get("destination") for v in row["versions"])
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
                        recovery = version.get("recovery") or {}
                        recovery_path = recovery.get("filePath") or recovery.get("destination")
                        if recovery_path:
                            await self.file_operation(self.clear_registered_file, recovery_path,
                                recovery.get("sha256") or version.get("sha256"),
                                recovery.get("fileVersion", {}).get("size") or version.get("totalBytes"))
                            recovery.update(filePath="", destination="", localCleanupState="complete")
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
        configured = getattr(getattr(self, 'slicing', None), 'store', None)
        frame_tool = (configured.settings.get('frameToolPath') if configured else None) or shutil.which('ffmpeg')
        probe_tool = str(Path(frame_tool).with_name('ffprobe')) if frame_tool else None
        if not frame_tool or not probe_tool or not all(
                Path(tool).is_file() and os.access(tool, os.X_OK) for tool in (frame_tool, probe_tool)):
            raise DownloadFailure('视频检查工具不可用，请检查已配置的抽帧工具；缓存保留，未判定视频损坏。', 'media_tool_missing')
        process = None
        try:
            process = await asyncio.create_subprocess_exec(probe_tool, "-v", "error", "-show_entries", "format=duration",
                                                             "-of", "default=noprint_wrappers=1:nokey=1", str(path),
                                                             stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL)
            output, _ = await asyncio.wait_for(process.communicate(), 40)
            duration = float(output.strip())
            if process.returncode or not math.isfinite(duration) or duration <= 0:
                raise ValueError()
            await self.file_operation(verify_video_tail, path, frame_tool, duration)
            return duration
        except MediaValidationError as error:
            if str(error) == 'media_check_timeout':
                raise DownloadFailure('视频检查超时，缓存保留；未判定视频损坏。', 'media_check_timeout') from None
            raise DownloadFailure("视频末段缺失或无法解码，尚未替换旧版。请使用完整视频重新采集或下载。", "invalid_video") from None
        except TimeoutError:
            raise DownloadFailure('视频元数据检查超时，缓存保留；未判定视频损坏。', 'media_check_timeout') from None
        except OSError:
            raise DownloadFailure('视频检查工具无法启动或访问，缓存保留；未判定视频损坏。', 'media_tool_missing') from None
        except ValueError:
            raise DownloadFailure("文件无法作为完整视频播放，旧版已保留。", "invalid_video") from None
        finally:
            if process and process.returncode is None:
                try:
                    process.kill()
                except ProcessLookupError:
                    pass
                await process.wait()

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
                        complete = bool(offset and match and int(match[1]) == offset
                                        and version.get("totalBytes") == offset
                                        and response.headers.get("etag") == version.get("etag"))
                        # Some proxies omit Content-Range/ETag on 416. Confirm the
                        # length and strong file version using a single-byte probe.
                        if not complete and offset and version.get("totalBytes") == offset and version.get("etag"):
                            async with self.target.client.stream("GET", url, headers={
                                    "Accept-Encoding": "identity", "Range": "bytes=0-0",
                                    "If-Range": version["etag"]}, timeout=httpx.Timeout(30, connect=15)) as probe:
                                bounds = re.fullmatch(r"bytes 0-0/(\d+)", probe.headers.get("content-range", ""))
                                complete = bool(probe.status_code == 206 and bounds and int(bounds[1]) == offset
                                                and probe.headers.get("etag") == version["etag"])
                        if complete:
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
                    output = part.open("ab" if offset else "wb")
                    try:
                        async for block in response.aiter_bytes(chunk_size=1024 * 1024):
                            if received == 0 and not valid_video_header(block[:512]):
                                raise DownloadFailure("下载内容不是视频。", "invalid_content")
                            # Removable-volume writes and fsync may stall for
                            # seconds. Keep API handling off that filesystem
                            # wait, and finish an in-flight write before close
                            # when the user pauses this download.
                            await self.file_operation(output.write, block)
                            received += len(block)
                            version["receivedBytes"] = received
                            if time.monotonic() - saved_at >= 1:
                                self.store.save(row)
                                saved_at = time.monotonic()
                        await self.file_operation(output.flush)
                        await self.file_operation(os.fsync, output.fileno())
                    finally:
                        await self.file_operation(output.close)
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
            if getattr(self, 'workspace', None) and self.workspace.enabled:
                try:
                    self.workspace.require_root()
                except (OSError, ValueError):
                    self.storage_ready = False
                    self.storage_error = '等待原工作区磁盘连接；下载意图和断点保留。'
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
                    if getattr(self, 'workspace', None) and self.workspace.enabled:
                        destination = self.workspace.download_destination(destination.name)
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
                if getattr(self, 'workspace', None) and self.workspace.enabled:
                    from .workspace_files import file_version
                    version['fileVersion'] = await self.file_operation(file_version, destination, True)
                version.pop("message", None)
                version.pop("errorCode", None)
                version.pop("transferError", None)
                version.pop("transferTemporaryPath", None)
                version.pop("transferReservation", None)
                previous = row["acceptedRevision"]
                row["acceptedRevision"] = revision
                row["cleanupPending"] = revision > 0 and previous != revision
                if self._manual_media_policy(version):
                    self._retain_replacement_history(row, version)
                self.store.save(row)
                on_saved = getattr(self, 'on_saved', None)
                if on_saved:
                    try:
                        on_saved(identity, revision)
                    except Exception:
                        # Publication already succeeded. Startup's accepted-file
                        # reconciliation repairs a lost advisory notification.
                        pass
                if row.get("cleanupPending") and not self._manual_media_policy(version):
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
                marker = None
                prepare = getattr(self, 'prepare_recovery_event', None)
                if prepare:
                    try:
                        marker = prepare(row, version)
                    except Exception:
                        pass  # Keep the original download failure intact.
                if marker:
                    version['automaticRecoveryEvent'] = marker
                self.store.save(row)
                notify = getattr(self, 'on_media_failure', None)
                if marker and notify:
                    try:
                        notify(identity, revision)
                    except Exception:
                        pass  # The durable marker is reconciled on restart.

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
        if self._manual_media_policy(latest):
            self._retain_replacement_history(row, latest)
            self.store.save(row)
            for version in row['versions']:
                journal = version.get('cleanup') or {}
                if (version['revision'] >= latest['revision'] or not journal.get('started')
                        or journal.get('complete')):
                    continue
                # A previously dispatched deletion cannot be revoked. Reconcile
                # only its exact persisted cloud journal; never add local files.
                await self.cleanup_cloud(row, version, protected={v.get('taskId') for v in row['versions']
                    if v['revision'] >= latest['revision']})
                version['state'] = 'cloud_deleted'
                version['cloudCleanupReconciledAt'] = now_iso()
                self.store.save(row)
            row.update(cleanupPending=False, error='')
            self.store.save(row)
            return
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
            recovery = version.get("recovery") or {}
            recovery_path = recovery.get("filePath") or recovery.get("destination")
            if recovery_path:
                guard = self.file_leases.write(recovery_path) if getattr(self, "file_leases", None) else nullcontext()
                with guard:
                    await self.file_operation(self.clear_registered_file, recovery_path,
                        recovery.get("sha256") or version.get("sha256"),
                        recovery.get("fileVersion", {}).get("size") or version.get("totalBytes"))
                    recovery.update(filePath="", destination="", localCleanupState="complete")
                    self.store.save(row)
            if version.get("taskId"):
                await self.cleanup_cloud(row, version, protected={v.get("taskId") for v in row["versions"]
                    if v["revision"] >= latest["revision"]})
            version["state"] = "cloud_deleted"
            self.store.save(row)
        row["cleanupPending"] = False
        row["error"] = ""
        self.store.save(row)

    @staticmethod
    def _manual_media_policy(version):
        confirmation = version.get('finalConfirmation') or {}
        return bool(confirmation.get('operationId') and confirmation.get('version') == version.get('revision'))

    @staticmethod
    def _retain_replacement_history(row, latest):
        retained, reconciling = [], False
        for version in row.get('versions', []):
            if version['revision'] >= latest['revision']:
                continue
            cleanup = version.get('cleanup') or {}
            reconciling = reconciling or bool(cleanup.get('started') and not cleanup.get('complete'))
            if version['state'] not in TERMINAL:
                retained.append(version['revision'])
                version['replacementRetention'] = {
                    'policy': 'manual-local-keep-cloud', 'acceptedRevision': latest['revision'],
                    'message': '旧版文件与云端任务保留；本次确认不自动删除。'}
        row['cleanupPending'] = reconciling
        row['replacementRetention'] = {'policy': 'manual-local-keep-cloud', 'versions': retained,
                                       'acceptedRevision': latest['revision']}

    async def cleanup_cloud(self, row, version, protected, allow_incomplete=False):
        guard = (self.file_leases.write(self.cloud_lease_key(version.get("taskId")))
                 if getattr(self, "file_leases", None) else nullcontext())
        with guard:
            return await self._cleanup_cloud(row, version, protected, allow_incomplete)

    async def _cleanup_cloud(self, row, version, protected, allow_incomplete=False):
        from .video_withdrawal import cleanup_checkpoint
        checkpoint = lambda: cleanup_checkpoint(self, row, version)
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
                await self._delete_objects(cleanup, parent_id, lambda: checkpoint())
                await self._verify_objects(cleanup)
                if await self.results(parent_id):
                    raise ValueError("旧结果记录仍存在，清理尚未完成。")
                cleanup["complete"] = True
                checkpoint()
                return
            if not self.matches(parent, version, self.target_config["cameraId"]):
                raise ValueError("旧任务身份已变化，已阻止删除。")
            results = await self.results(parent_id)
            if version.get("resultId") and str(parent.get("taskStatus", "")).upper() != "FIXED":
                raise ValueError("旧任务完成状态已变化，暂不删除。")
            if not results and not version.get("resultId") and (version["revision"] > 0 or allow_incomplete):
                # A withdrawn correction can still be collecting. Recheck the result after removing its exact task.
                cleanup.update(started=True, pendingTaskOnly=True)
                checkpoint()
                await self.target.request("/data-acquisition/delete", method="POST",
                                          body={"id": parent_id, "type": "task"}, write=True)
                if parent_id in await self.jobs.remote_parents(self.target_config) or await self.results(parent_id):
                    raise ValueError("更正任务仍存在，暂不能确认撤回完成。")
                cleanup.update(started=True, complete=True)
                checkpoint()
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
                checkpoint()
            if set(cleanup["objects"]) != {version["resultId"]} or any(str(r["id"]) != version["resultId"] for r in results):
                raise ValueError("旧任务出现未绑定的视频结果，已阻止删除。")
            for result in results:
                await self.target.request("/data-acquisition/delete", method="POST",
                                          body={"id": str(result["id"]), "type": "acInfo"}, write=True)
            await self._delete_objects(cleanup, parent_id, lambda: checkpoint())
            await self._verify_objects(cleanup)
            if await self.results(parent_id):
                raise ValueError("旧结果记录仍存在，稍后继续核对。")
            await self.target.request("/data-acquisition/delete", method="POST",
                                      body={"id": parent_id, "type": "task"}, write=True)
            if parent_id in await self.jobs.remote_parents(self.target_config):
                raise ValueError("旧任务仍存在，稍后继续核对。")
            cleanup["complete"] = True
            checkpoint()

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
