"""Durable model-assisted slicing queue, isolated from collection and import logs."""
from __future__ import annotations

import asyncio
import copy
import hashlib
import json
import math
import os
import shutil
import signal
import stat
import subprocess
import sys
import time
import unicodedata
from contextlib import ExitStack, contextmanager
from pathlib import Path
from uuid import uuid4

from .slicing_workflow import SlicingWorkflow
from .audit_moves import source_fingerprint
from .slicing_analysis import SharedSlicingAnalysis, outcome_changed, revision_after_outcome
from .slice_export import (package_identity, photo_names, read_jpeg, slice_timestamps, write_image_archive,
                           PACKAGE_DATASETS)
from .slice_packages import SlicePackageManager
from .upload_remote import UploadRemote
from .domain import now_iso
from .file_leases import FileLeases
from .slicing_core import build_category_result, category_image_target, sample_points
from .slicing_remote import (ACTIVE_STATUSES, CUT_TYPES, FAILED_STATUSES, SUCCESS_STATUSES,
                             SlicingRemote, log_matches_video, parse_config, source_video_ids,
                             target_dataset_id, task_state)
from .upload_audit import AuditError, VIDEO_EXTENSIONS
from .videos import Conflict

ROOT = Path(__file__).resolve().parents[1]
POLICY_VERSION = 1
SAMPLING_VERSION = 2
TICK_SECONDS = 30
ACTIVE_TICK_SECONDS = 8
PLATFORM_SLOT_LIMIT = 2
SOURCE = {
    "27912": {"category": "bed", "name": "车斗", "targetName": "永锋车斗检测"},
    "27944": {"category": "magnet", "name": "磁盘吊", "targetName": "永锋-磁盘吊检测"},
}
ERROR_TEXT = {
    "auth": "视觉AI平台未连接或登录已失效。重新连接后可继续，已保存进度不会清除。",
    "network": "平台暂时不可达，保留当前进度，连接恢复后继续。",
    "disk": "视频目录或外置盘暂不可用。连接原磁盘后可继续。",
    "local_video_missing": "未在配置目录中找到这个视频；重新扫描或连接原视频所在磁盘后再试。",
    "local_video_ambiguous": "有多个同名文件，无法判断对应关系；请手动绑定。",
    "local_video_mismatch": "本地视频无法读取或身份不明确，请检查文件后重试。",
    "local_video_metadata_missing": "平台未提供可比较的文件信息；已按唯一文件名匹配本地视频。",
    "target_dataset_unresolved": "图片目标数据集不能唯一确认，请在设置里选择正确数据集。",
    "worker_missing": "独立推理环境尚未安装。运行项目中的切片环境安装脚本后可重试。",
    "inference_environment_missing": "推理环境缺少模型依赖。运行切片环境安装脚本后可重试。",
    "magnet_model_missing": "磁盘吊模型路径无效或外置盘未连接。",
    "bed_model_missing": "车斗模型或定位文件不可用。",
    "model_load_failed": "模型未能加载，请核对模型文件和部署目录。",
    "video_probe_failed": "视频元数据无法读取，原文件保留，请核对视频。",
    "frame_extract_failed": "视频抽帧失败，原文件保留，可单条重试。",
    "media_incomplete": "视频末段缺失或无法解码，不能完成切片。请使用完整版本；原文件和有效判断已保留。",
    "media_check_timeout": "读取视频末段超时。请检查外置盘连接后单条重试。",
    "observations_incomplete": "部分抽样位置尚未识别，已保留有效判断，重试只补齐缺失位置。",
    "worker_stalled": "识别进程长时间未返回进度，已单独停止本条，可重试继续。",
    "frame_tool_missing": "找不到抽帧工具，请检查 FFmpeg 路径设置。",
    "video_resolution": "视频分辨率不符合车斗模型要求，车斗类别待核对；磁盘吊任务可独立继续。",
    "bed_resolution": "视频分辨率不符合车斗模型要求，车斗类别待核对；磁盘吊任务可独立继续。",
    "bed_alignment_uncertain": "车斗画面对齐质量未通过模型检查，等待人工核对。",
    "inference_failed": "模型推理未能完成，仅暂停对应类别。",
    "output_unverified": "平台任务完成，但尚未关联核验到目标图片，暂不标记完成。",
    "submit_uncertain": "提交响应不确定，正在查询平台任务；不会盲目重复提交。",
    "slice_limit": "保留全部区间时无法满足图片数量上限，请人工调整区间。",
    "source_changed": "本地视频版本已变化，旧判断作废，请重新扫描。",
    "internal_error": "程序内部处理异常，本条和队列进度均已保留；请修复后重试。",
}
_RETRYABLE_LOCAL_ERRORS = {"frame_extract_failed", "local_video_missing", "local_video_ambiguous",
                           "video_probe_failed", "source_changed"}
_PLATFORM_STARTED_STATES = {"ready", "waiting_slot", "submitting", "waiting", "complete"}


def task_matches_submission(job, item, category):
    """Require the new task to match the submitted source, target and cut settings."""
    config = parse_config(job.get("config"))
    if source_video_ids(job) != {str(item.get("sourceVideoId"))}:
        return False
    if target_dataset_id(job) != str(category.get("targetDatasetId") or ""):
        return False
    options = config.get("options") if isinstance(config.get("options"), dict) else {}
    image = options.get("image") if isinstance(options.get("image"), dict) else {}
    if not image or str(image.get("interval")) != str(category.get("intervalSeconds")):
        return False
    if str(image.get("prefix") or "") != str(category.get("prefix") or ""):
        return False
    if options.get("sliceVideo") is not False or options.get("sliceImage") is not True:
        return False
    expected = category.get("intervals") or []
    actual = options.get("slice_periods")
    if not isinstance(actual, list) or len(actual) != len(expected):
        return False
    try:
        return all(abs(float(actual[index]["start"]) - float(window["start"])) <= 0.001 and
                   abs(float(actual[index]["end"]) - float(window["end"])) <= 0.001
                   for index, window in enumerate(expected))
    except (KeyError, TypeError, ValueError):
        return False


def safe_path(value, *, must_exist=False, kind=None, allow_symlink=False, preserve_symlink=False):
    path = Path(value).expanduser()
    if not path.is_absolute() or ".." in path.parts:
        raise ValueError("请填写绝对路径，且不要包含上级目录跳转。")
    path = Path(os.path.normpath(path))
    if allow_symlink and not preserve_symlink:
        path = path.resolve(strict=False)
    elif not allow_symlink:
        for current in (path, *path.parents):
            if current.is_symlink():
                raise ValueError("路径不能经过符号链接。")
    if must_exist and not path.exists():
        raise ValueError("文件或目录不存在，请连接对应磁盘并重试。")
    if must_exist and kind == "directory" and not path.is_dir():
        raise ValueError("配置路径不是文件夹。")
    if must_exist and kind == "file" and not path.is_file():
        raise ValueError("配置路径不是普通文件。")
    if path.is_relative_to(Path("/Volumes")) and len(path.parts) > 2:
        mount = Path(*path.parts[:3])
        if not os.path.ismount(str(mount)):
            raise ValueError("外置盘尚未挂载，请先连接原磁盘。")
    return path


def _hash_file(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(4 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def source_version(path):
    path = safe_path(path, must_exist=True, kind="file")
    info = path.lstat()
    if not stat.S_ISREG(info.st_mode):
        raise ValueError("绑定文件不是普通视频文件。")
    edge = hashlib.sha256()
    with path.open("rb") as stream:
        edge.update(stream.read(65536))
        if info.st_size > 65536:
            stream.seek(max(0, info.st_size - 65536))
            edge.update(stream.read(65536))
    return {"device": info.st_dev, "inode": info.st_ino, "size": info.st_size,
            "modifiedNs": info.st_mtime_ns, "edgeHash": edge.hexdigest()}


def ffprobe_file(path, ffmpeg_path):
    ffprobe = Path(ffmpeg_path).with_name("ffprobe")
    if not ffprobe.is_file():
        ffprobe = shutil.which("ffprobe")
    if not ffprobe:
        raise ValueError("找不到 ffprobe，请检查抽帧工具路径。")
    result = subprocess.run([str(ffprobe), "-v", "error", "-select_streams", "v:0", "-show_entries",
        "format=duration,size:stream=width,height,avg_frame_rate,r_frame_rate", "-of", "json", str(path)],
        capture_output=True, timeout=40, check=False)
    if result.returncode:
        raise ValueError("视频元数据读取失败。")
    data = json.loads(result.stdout)
    streams = data.get("streams") or []
    fmt = data.get("format") or {}
    if not streams or float(fmt.get("duration", 0)) <= 0:
        raise ValueError("视频元数据不完整。")
    rate = streams[0].get("avg_frame_rate") or "25/1"
    try:
        numerator, denominator = rate.split("/", 1)
        frame_rate = float(numerator) / float(denominator)
    except (ValueError, ZeroDivisionError):
        frame_rate = 25.0
    return {"duration": float(fmt["duration"]), "size": int(fmt.get("size", 0)),
            "width": int(streams[0].get("width", 0)), "height": int(streams[0].get("height", 0)),
            "frameRate": frame_rate if math.isfinite(frame_rate) and frame_rate > 0 else 25.0}


def scan_local_files(directories):
    found, roots, issues = [], [], []
    seen_paths = set()
    for raw in directories:
        try:
            root = safe_path(raw, must_exist=True, kind="directory")
        except (OSError, ValueError):
            issues.append({"path": str(raw), "message": "目录当前不可读取，检查路径或连接外置盘。"})
            continue
        roots.append(str(root))
        try:
            for current, dirs, files in os.walk(root, followlinks=False):
                dirs[:] = sorted(name for name in dirs if not name.startswith(".") and not (Path(current) / name).is_symlink())
                for filename in files:
                    path = Path(current) / filename
                    if str(path) in seen_paths or filename.startswith(".") or path.suffix.lower() not in VIDEO_EXTENSIONS:
                        continue
                    try:
                        version = source_version(path)
                        seen_paths.add(str(path))
                        found.append({"name": path.name, "path": str(path), "size": version["size"],
                                      "version": version, "relativePath": str(path.relative_to(root)), "root": str(root)})
                    except (OSError, ValueError):
                        continue
                    if len(found) > 20000:
                        raise ValueError("视频目录超过20000个文件，请拆分后分次扫描。")
        except OSError:
            issues.append({"path": str(root), "message": "外置盘读取中断，检查磁盘连接后重新扫描。"})
    found.sort(key=lambda row: (row["name"].casefold(), row["path"].casefold()))
    return found, roots, issues


def video_name(row):
    for key in ("name", "fileName", "filename", "videoName", "video_name"):
        if isinstance(row.get(key), str) and row[key].strip():
            return Path(row[key].replace("\\", "/")).name
    for key in ("video", "file", "resource"):
        child = row.get(key)
        if isinstance(child, dict):
            result = video_name(child)
            if result:
                return result
    return ""


def video_properties(row):
    data = dict(row)
    for key in ("video", "file", "resource"):
        if isinstance(row.get(key), dict):
            data = {**row[key], **data}
    size = data.get("size", data.get("fileSize", data.get("file_size")))
    duration = data.get("duration", data.get("durationSeconds", data.get("duration_seconds")))
    for key in ("width", "height"):
        if data.get(key) is None and isinstance(data.get("metadata"), dict):
            data[key] = data["metadata"].get(key)
    try:
        size = int(size) if size is not None else None
    except (ValueError, TypeError):
        size = None
    try:
        duration = float(duration) if duration is not None else None
    except (ValueError, TypeError):
        duration = None
    return {"size": size, "duration": duration, "width": data.get("width"), "height": data.get("height")}


def data_category(row):
    value = row.get("dataCategory", row.get("data_category", row.get("dataType", row.get("data_type",
                 row.get("datasetType", row.get("dataset_type", row.get("category", row.get("type"))))))))
    if isinstance(value, dict):
        value = value.get("value", value.get("name", value.get("id")))
    label = str(value or "").casefold()
    if "image" in label or "图片" in label or label in {"1", "img"}:
        return "image"
    if "video" in label or "视频" in label or label in {"2", "video"}:
        return "video"
    return "unknown"


def dataset_namespace(row):
    value = row.get("namespace", row.get("namespaceId", row.get("namespace_id")))
    if isinstance(value, dict):
        value = value.get("id", value.get("namespace"))
    return str(value) if value is not None else ""


def normalize_dataset_name(value):
    """Compare dataset names while ignoring harmless whitespace and punctuation differences."""
    normalized = unicodedata.normalize("NFKC", str(value or "")).casefold()
    return "".join(char for char in normalized if not char.isspace() and not unicodedata.category(char).startswith("P"))


def target_dataset_identity_matches(row, descriptor):
    # These reads explicitly pass namespace=1. Some Athena list/detail responses
    # omit the namespace field, so absence means the query's scoped namespace.
    namespace = dataset_namespace(row)
    return (normalize_dataset_name(row.get("name")) == normalize_dataset_name(descriptor["targetName"])
            and namespace in ("", "1"))


async def target_dataset_verified(remote, identity, row, descriptor):
    # Athena can mislabel a fixed input collection as an image set in generic
    # metadata. Its dedicated /videos/ endpoint is authoritative; never reuse
    # either input collection as an output target.
    if str(identity) in SOURCE:
        return False
    if not target_dataset_identity_matches(row, descriptor):
        return False
    if data_category(row) == "image":
        return True
    probe = getattr(remote, "is_image_dataset", None)
    if not probe:
        return False
    try:
        return bool(await probe(str(identity)))
    except AuditError as error:
        if error.status in (400, 404):
            return False
        raise


def applicable_category(source_dataset_id):
    descriptor = SOURCE.get(str(source_dataset_id))
    if not descriptor:
        raise ValueError("视频来源数据集不在允许范围内。")
    return descriptor["category"]


def normalize_item_categories(item):
    """Repair legacy aggregate flags without changing bound platform operations."""
    descriptor = SOURCE.get(str(item.get("sourceDatasetId")))
    if not descriptor:
        return False
    before = json.dumps(item, sort_keys=True)
    active = descriptor["category"]
    for name, category in item.get("categories", {}).items():
        if name != active and not category.get("taskId") and category.get("state") != "not_applicable":
            if category.get("intervals") or category.get("modelFingerprint") or category.get("packageId"):
                continue
            category.update(state="not_applicable", errorCode="", message="此视频集不使用该类别模型。")
    category = item.get("categories", {}).get(active, {})
    if item.get("state") == "complete" and category.get("state") == "no_target":
        item.update(state="no_target", message="未检出目标，未提交切片任务。")
    if item.get("state") == "attention" and not item.get("errorCode"):
        if category.get("state") == "pending":
            item.update(state="pending", message="待处理，可单独启动识别与切片。")
        elif category.get("state") == "attention":
            item.update(errorCode=category.get("errorCode", ""), message=category.get("message") or "本条需要核对。")
    return json.dumps(item, sort_keys=True) != before


def archive_failed_attempt(category):
    if not category.get("taskId") or category.get("errorCode") != "platform_failed":
        return
    history = category.setdefault("previousAttempts", [])
    history.append({key: category.get(key) for key in ("taskId", "operationId", "prefix", "intervals",
        "intervalSeconds", "targetDatasetId", "platformStatus", "errorCode")})
    category["submissionRevision"] = int(category.get("submissionRevision", 0)) + 1
    for key in ("taskId", "operationId", "prefix", "preSubmitTaskIds", "preSubmitImageIds", "outputImageIds",
                "platformStatus", "imageQueryProgress"):
        category.pop(key, None)


class SlicingStore:
    def __init__(self, db, python_path=None, frame_tool=None):
        self.db = db
        for name in ("runs", "items", "observations", "operations", "requests"):
            db.execute(f"CREATE TABLE IF NOT EXISTS simple_slicing_{name} (id TEXT PRIMARY KEY, payload TEXT NOT NULL)")
        db.execute("CREATE TABLE IF NOT EXISTS simple_slicing_settings (id INTEGER PRIMARY KEY CHECK(id=1), payload TEXT NOT NULL)")
        db.commit()
        row = db.execute("SELECT payload FROM simple_slicing_settings WHERE id=1").fetchone()
        self.settings = json.loads(row[0]) if row else self.defaults(python_path, frame_tool)
        for key, value in self.defaults(python_path, frame_tool).items():
            self.settings.setdefault(key, value)
        self.save_settings()

    @staticmethod
    def defaults(python_path=None, frame_tool=None):
        return {"revision": 0, "directories": ["/Volumes/out/视频"],
                "magnetDeployDirectory": "/Volumes/out/yongfeng_cipandiao_deploy",
                "magnetModelPath": "/Volumes/out/yongfeng_cipandiao_deploy/best.onnx",
                "bedDeployDirectory": "/Volumes/out/yongfeng_wall_presence_deploy_20261018_v1",
                "bedModelPath": "/Volumes/out/yongfeng_wall_presence_deploy_20261018_v1/runs/supervised_wall_20261017_v5/model.pt",
                "bedGeometryPath": "/Volumes/out/yongfeng_wall_presence_deploy_20261018_v1/runs/supervised_wall_20261017_v5/geometry.npz",
                "frameToolPath": frame_tool or shutil.which("ffmpeg") or "/opt/homebrew/bin/ffmpeg",
                "workerPythonPath": python_path or str(ROOT / ".venv-slicing" / "bin" / "python"),
                "sharedSampling": True, "sampleIntervalSeconds": 30, "magnetIntervalSeconds": 30,
                "bedIntervalSeconds": 30, "magnetConfidence": 0.25, "targetImages": 150,
                "bedTargetImages": 180,
                "targetDatasetIds": {"27912": None, "27944": None}, "enabled": False,
                "latestRunId": "", "settingsRequestIds": {}, "autoArchive": True,
                "archiveCompletedDirectory": "切片完成", "archiveEmptyDirectory": "未检出目标",
                "reviewBeforeUpload": True, "localExport": True}

    def save_settings(self):
        self.db.execute("INSERT INTO simple_slicing_settings VALUES(1,?) ON CONFLICT(id) DO UPDATE SET payload=excluded.payload",
                        (json.dumps(self.settings, ensure_ascii=False),))
        self.db.commit()

    def save(self, kind, value):
        assert kind in ("runs", "items", "observations", "operations", "requests")
        value["updatedAt"] = now_iso()
        self.db.execute(f"INSERT INTO simple_slicing_{kind} VALUES(?,?) ON CONFLICT(id) DO UPDATE SET payload=excluded.payload",
                        (value["id"], json.dumps(value, ensure_ascii=False)))
        self.db.commit()

    def get(self, kind, identity):
        assert kind in ("runs", "items", "observations", "operations", "requests")
        row = self.db.execute(f"SELECT payload FROM simple_slicing_{kind} WHERE id=?", (identity,)).fetchone()
        return json.loads(row[0]) if row else None

    def list(self, kind):
        assert kind in ("runs", "items", "observations", "operations", "requests")
        return [json.loads(row[0]) for row in self.db.execute(f"SELECT payload FROM simple_slicing_{kind} ORDER BY rowid")]


class SlicingManager(SlicingWorkflow, SharedSlicingAnalysis):
    def __init__(self, store, auth, data_dir, leases=None, remote=None, worker_python=None, frame_tool=None, clock=None):
        self.store = SlicingStore(store.db, worker_python, frame_tool)
        self.auth = auth
        self.remote = remote or SlicingRemote(auth)
        self.data_dir = Path(data_dir)
        self.leases = leases or FileLeases()
        self.clock = clock or time.time
        self.scheduler_task = None
        self.scan_task = None
        self.analysis_task = None
        self.worker_process = None
        self.wake = asyncio.Event()
        self.stopping = False
        self.tick_lock = asyncio.Lock()
        self.start_lock = asyncio.Lock()
        self.fingerprint_cache = {}
        self.files = None
        self.packages = SlicePackageManager(store.db, UploadRemote(auth), clock=self.clock)
        self.slice_reader = read_jpeg
        self.leases.subscribe_moves(self.moved)
        self.run = self.store.get("runs", self.store.settings.get("latestRunId"))
        if self.run and self.run.get("state") in {"scanning", "running"}:
            self.run.update(state="interrupted", message="程序重启，任务凭证与抽样观察已保留；请连接平台后继续。")
            self.store.save("runs", self.run)
        for item in self.store.list("items"):
            if normalize_item_categories(item):
                item["revision"] = int(item.get("revision", 0)) + 1
                self.store.save("items", item)
            if item.get("state") == "analyzing":
                item.update(state="pending", message="程序重启，继续使用已保存的有效抽样观察。")
                self.store.save("items", item)
            legacy_local_gate = item.get("errorCode") in {"local_video_metadata_missing", "local_video_mismatch"}
            if legacy_local_gate and item.get("path") and item.get("fileVersion"):
                try:
                    unchanged = source_version(item["path"]) == item["fileVersion"]
                except (OSError, ValueError):
                    unchanged = False
                if unchanged:
                    item.update(state="pending", errorCode="", message="本地原片按唯一文件名自动匹配，已加入识别队列。",
                                revision=int(item.get("revision", 0)) + 1)
                    for category in item.get("categories", {}).values():
                        if category.get("errorCode") in {"local_video_metadata_missing", "local_video_mismatch"}:
                            category.update(state="pending", errorCode="", message="本地原片已自动匹配，可继续识别。")
                    self.store.save("items", item)

    @property
    def busy(self):
        return bool(any(task and not task.done() for task in (self.scan_task, self.scheduler_task, self.analysis_task)) or (self.files and self.files.busy))

    def _safe_settings(self):
        return {key: value for key, value in self.store.settings.items()
                if key not in {"settingsRequestIds", "enabled", "latestRunId"}}

    def phase(self, run):
        if not self.store.settings.get("enabled"):
            return "idle"
        if not self.auth.connected or (run and run.get("state") == "waiting_connection"):
            return "waiting_connection"
        if self.store.settings.get("workflow", {}).get("phase") == "scan":
            return "scanning"
        if self.analysis_task and not self.analysis_task.done():
            return "analyzing"
        if self.files and self.files.busy:
            return "archiving"
        return "platform_processing"

    def state(self):
        items = self.store.list("items")
        run = self.store.get("runs", self.store.settings.get("latestRunId")) or self.run
        if run:
            self.run = run
        counts = {}
        for item in items:
            counts[item.get("state", "attention")] = counts.get(item.get("state", "attention"), 0) + 1
        public_items = []
        for item in items:
            public = {**item, "categories": {}}
            for name, category in item.get("categories", {}).items():
                public["categories"][name] = {key: value for key, value in category.items()
                    if key not in {"preSubmitImageIds", "preSubmitTaskIds", "outputImageIds", "fileVersion"}}
            public_items.append(public)
        return {"workflow": self.store.settings.get("workflow"),
                "archive": self.files.state() if self.files else {"items": [], "counts": {}},
                "phase": self.phase(run),
                "connected": bool(self.auth.connected), "user": self.auth.user if self.auth.connected else "",
                "settings": self._safe_settings(), "settingsRevision": self.store.settings["revision"],
                "run": run, "items": public_items, "counts": counts, "busy": self.busy,
                "enabled": bool(self.store.settings.get("enabled")),
                "packages": self.packages.state()}

    def update_settings(self, data):
        request = self.store.settings.get("settingsRequestIds", {})
        if data["requestId"] in request:
            return self.state()
        if data["expectedRevision"] != self.store.settings["revision"]:
            raise Conflict("模型切片设置已被其他页面修改，请刷新后重试。")
        if self.busy or self.store.settings.get("enabled") or self.start_lock.locked():
            raise Conflict("切片正在运行或停止中，请暂停并等待结束后再修改设置，已保存的进度会保留。")
        updates = {key: value for key, value in data.items() if key not in {"expectedRevision", "requestId"}}
        for key in ("archiveCompletedDirectory", "archiveEmptyDirectory"):
            value = updates.get(key, self.store.settings[key])
            if value in {".", ".."} or "/" in value or "\\" in value:
                raise ValueError("归档名称只能填写单个文件夹名称。")
            updates[key] = value
        if updates["archiveCompletedDirectory"] == updates["archiveEmptyDirectory"]:
            raise ValueError("两类归档文件夹名称不能相同。")
        directories = []
        for value in updates["directories"]:
            path = safe_path(value)
            if str(path) not in directories:
                directories.append(str(path))
        if not directories:
            raise ValueError("至少填写一个视频目录。")
        for key in ("magnetDeployDirectory", "bedDeployDirectory"):
            updates[key] = str(safe_path(updates[key]))
        for key in ("magnetModelPath", "bedModelPath", "bedGeometryPath", "frameToolPath", "workerPythonPath"):
            updates[key] = str(safe_path(updates[key], allow_symlink=True,
                                          preserve_symlink=key == "workerPythonPath"))
        target_ids = updates.get("targetDatasetIds") or {}
        for key in ("27912", "27944"):
            value = target_ids.get(key)
            if value not in (None, "") and not str(value).isdigit():
                raise ValueError("图片目标数据集编号必须是数字。")
        updates["targetDatasetIds"] = {"27912": str(target_ids.get("27912") or "") or None,
                                        "27944": str(target_ids.get("27944") or "") or None}
        self.store.settings.update(updates)
        self.store.settings["revision"] += 1
        request[data["requestId"]] = self.store.settings["revision"]
        self.store.settings["settingsRequestIds"] = dict(list(request.items())[-1000:])
        self.store.save_settings()
        return self.state()

    def _revision(self, item, expected):
        if not item:
            raise ValueError("切片记录不存在，请重新扫描。")
        if int(item.get("revision", 0)) != expected:
            raise Conflict("这条视频已在其他页面更新，请刷新后重新核对。")

    def scan(self, request_id):
        existing = self.store.get("requests", request_id)
        if existing:
            return self.state()
        if self.scan_task and not self.scan_task.done():
            raise ValueError("已有扫描正在进行，请等待完成。")
        if self.store.settings.get("enabled") or (self.analysis_task and not self.analysis_task.done()):
            raise Conflict("队列正在处理，请使用“全部切片”补充扫描。")
        if not self.auth.connected:
            raise AuditError("请先在本机程序连接视觉AI平台。", "auth", 401)
        run = {"id": uuid4().hex, "requestId": request_id, "state": "scanning",
               "message": "正在读取两个视频数据集、21/22类切片日志和本地目录…", "startedAt": now_iso(),
               "settingsRevision": self.store.settings["revision"], "items": [],
               "excluded": {"cut": 0, "active": 0, "complete": 0, "submitted": 0}, "issues": []}
        self.run = run
        self.store.settings["latestRunId"] = run["id"]
        self.store.settings["settingsRequestIds"] = self.store.settings.get("settingsRequestIds", {})
        self.store.save_settings()
        self.store.save("runs", run)
        self.store.save("requests", {"id": request_id, "kind": "scan", "runId": run["id"], "state": "scanning"})
        self.scan_task = asyncio.create_task(self._scan(run["id"]))
        return self.state()

    async def _scan(self, run_id):
        run = self.store.get("runs", run_id)
        try:
            local, roots, issues = await asyncio.to_thread(scan_local_files, self.store.settings["directories"])
            if not roots and issues:
                raise AuditError("视频目录暂不可用，等待原磁盘恢复。", "disk")
            run.update(localCount=len(local), directories=roots, issues=issues,
                       message="正在读取平台视频、切片任务及目标图片集…")
            self.store.save("runs", run)
            dataset_rows = await self.remote.list_datasets()
            candidate_targets = {}
            for source_id, descriptor in SOURCE.items():
                matches = []
                for row in dataset_rows:
                    if str(row.get("id", "")) in SOURCE:
                        continue
                    namespace = dataset_namespace(row)
                    if (normalize_dataset_name(row.get("name")) != normalize_dataset_name(descriptor["targetName"])
                            or namespace not in ("", "1")):
                        continue
                    category = data_category(row)
                    verified = await target_dataset_verified(self.remote, row["id"], row, descriptor)
                    if verified:
                        category = "image"
                    matches.append({"id": str(row["id"]), "name": row.get("name"), "category": category,
                                    "namespace": namespace or "1", "namespaceInferred": not bool(namespace),
                                    "verified": verified, "selected": False})
                chosen = str(self.store.settings.get("targetDatasetIds", {}).get(source_id) or "")
                if chosen:
                    selected = next((row for row in matches if row["id"] == chosen), None)
                    if selected:
                        selected["selected"] = True
                verified = [row for row in matches if row["verified"]]
                if not chosen and len(verified) == 1:
                    verified[0]["selected"] = True
                    self.store.settings["targetDatasetIds"][source_id] = verified[0]["id"]
                candidate_targets[source_id] = {"name": descriptor["targetName"], "candidates": matches,
                    "selectedId": (chosen or (verified[0]["id"] if len(verified) == 1 else None)),
                    "resolved": bool((chosen and any(row["id"] == chosen and row["verified"] for row in matches))
                                     or (not chosen and len(verified) == 1))}
            self.store.save_settings()

            all_remote, all_jobs = {}, {}
            async def read_source(source_id):
                # The platform's generic dataset detail can classify these
                # fixed media collections as "image" even though their
                # authoritative content endpoint is /videos/. The IDs are
                # allowlisted by SlicingRemote; validate identity there, then
                # use its paginated video endpoint rather than the ambiguous
                # generic category field to decide whether they are videos.
                _, videos, manual, automatic = await asyncio.gather(self.remote.dataset(source_id),
                    self.remote.videos(source_id), self.remote.jobs(source_id, "21"), self.remote.jobs(source_id, "22"))
                all_remote[source_id], all_jobs[source_id] = videos, manual + automatic
            await asyncio.gather(*(read_source(source_id) for source_id in SOURCE))
            # Task logs are read independently by job type. Cache only bounded
            # metadata for the scan record; the upload-audit code is untouched.
            task_log_rows = 0
            log_cache = {}
            for previous in self.store.list("runs"):
                for entry in previous.get("taskLogIndex", []):
                    if entry.get("state") == "complete" and entry.get("jobVersion") and not entry.get("issue"):
                        log_cache[(str(entry.get("datasetId")), str(entry.get("jobId")))] = entry
            log_slots = asyncio.Semaphore(4)
            async def read_log(dataset, job):
                nonlocal task_log_rows
                version = hashlib.sha256(json.dumps({key: job.get(key) for key in
                    ("id", "status", "config", "utc_modify", "utcModify", "updatedAt", "updated_at")},
                    sort_keys=True, default=str).encode()).hexdigest()
                cached = log_cache.get((str(dataset), str(job["id"])))
                if cached and cached.get("jobVersion") == version and task_state(job) == "complete":
                    return {**cached, "cacheHit": True}
                try:
                    async with log_slots:
                        logs = await self.remote.logs(dataset, str(job["id"]))
                    task_log_rows += len(logs)
                    ids = set(source_video_ids(job))
                    for remote_video in all_remote.get(dataset, []):
                        identity, name = str(remote_video["id"]), video_name(remote_video)
                        if log_matches_video(logs, video_id=identity, name=name):
                            ids.add(identity)
                    return {"jobId": str(job["id"]), "datasetId": dataset, "type": str(job.get("type", "")),
                            "state": task_state(job), "sourceVideoIds": sorted(ids),
                            "jobVersion": version,
                            "targetDatasetId": target_dataset_id(job), "logLineCount": len(logs),
                            "logDigest": hashlib.sha256("\n".join(row["message"] for row in logs).encode()).hexdigest()}
                except AuditError as error:
                    return {"jobId": str(job.get("id", "")), "datasetId": dataset,
                            "type": str(job.get("type", "")), "state": task_state(job), "issue": error.code}
            relevant_jobs = []
            uncut_ids = {str(row["id"]) for rows in all_remote.values() for row in rows}
            bound_tasks = {str(category.get("taskId")) for item in self.store.list("items")
                           for category in item.get("categories", {}).values() if category.get("taskId")}
            for dataset, jobs in all_jobs.items():
                for job in jobs:
                    ids = source_video_ids(job)
                    if task_state(job) == "active" or ids.intersection(uncut_ids) or not ids or str(job.get("id")) in bound_tasks:
                        relevant_jobs.append((dataset, job))
            log_index = await asyncio.gather(*(read_log(dataset, job) for dataset, job in relevant_jobs))
            known_task_by_source = {}
            unresolved_log_datasets = set()
            for task in log_index:
                if task.get("issue") and not task.get("sourceVideoIds") and task.get("state") in {"active", "complete", "failed"}:
                    unresolved_log_datasets.add(str(task["datasetId"]))
                for source_identity in task.get("sourceVideoIds", []):
                    known_task_by_source.setdefault((task["datasetId"], source_identity), []).append(task)
            local_by_name = {}
            for row in local:
                local_by_name.setdefault(row["name"].casefold(), []).append(row)
            existing_items = {(str(item["sourceDatasetId"]), str(item["sourceVideoId"])): item for item in self.store.list("items")}
            candidates = []
            run["excluded"] = {"cut": 0, "active": 0, "complete": 0, "submitted": 0}
            for source_id, rows in all_remote.items():
                jobs = all_jobs[source_id]
                for remote_video in rows:
                    if str(remote_video.get("cutStatus", remote_video.get("cut_status", 0))) != "0":
                        run["excluded"]["cut"] += 1
                        continue
                    identity = str(remote_video["id"])
                    prior = existing_items.get((source_id, identity))
                    if prior:
                        prior = self.store.get("items", prior["id"])
                        active_category = applicable_category(source_id)
                        for category_name, category in prior.get("categories", {}).items():
                            if category_name != active_category and not category.get("taskId") and category.get("state") not in {"complete", "waiting", "submitting"}:
                                category.update(state="not_applicable", errorCode="", message="此视频集不使用该类别模型。")
                            elif category_name == active_category and category.get("errorCode") == "frame_extract_failed":
                                category.update(state="pending", errorCode="", message="抽帧修复后重新识别。")
                        if prior.get("errorCode") == "frame_extract_failed" and prior.get("state") == "attention":
                            prior.update(state="pending", errorCode="", message="抽帧修复后重新识别。")
                        if source_id in unresolved_log_datasets and prior.get("state") not in {"complete", "no_target"}:
                            prior.update(state="attention", errorCode="task_log_unavailable",
                                         message="平台有切片任务日志无法读取，不能排除重复提交；核对平台任务后再处理。")
                            for category_name, category in prior.get("categories", {}).items():
                                if category_name == active_category and category.get("state") not in {"complete", "no_target", "waiting", "submitting"}:
                                    category.update(state="attention", errorCode="task_log_unavailable",
                                                    message="平台切片任务日志无法核对，暂不自动提交。")
                            self.store.save("items", prior)
                        elif prior.get("errorCode") == "task_log_unavailable":
                            for category_name, category in prior.get("categories", {}).items():
                                if category_name == active_category and category.get("errorCode") == "task_log_unavailable":
                                    category.update(state="pending", errorCode="", message="任务日志核对恢复；等待重新识别。")
                                elif category_name != active_category and not category.get("taskId"):
                                    category.update(state="not_applicable", errorCode="", message="此视频集不使用该类别模型。")
                            if prior.get("path"):
                                prior.update(state="pending", errorCode="", message="任务日志核对恢复；重新识别本条视频。")
                            else:
                                prior.update(state="attention", errorCode="local_video_missing",
                                             message=ERROR_TEXT["local_video_missing"])
                            prior["runId"] = run_id
                            prior["revision"] += 1
                            self.store.save("items", prior)
                        if prior.get("state") not in {"complete", "attention", "no_target"}:
                            run["excluded"]["submitted"] += 1
                        prior["runId"] = run_id
                        prior["settingsRevision"] = self.store.settings["revision"]
                        prior["revision"] = int(prior.get("revision", 0)) + 1
                        self.store.save("items", prior)
                        candidates.append((source_id, remote_video, prior, ""))
                        continue
                    matched_job = None
                    for job in jobs:
                        task = next((candidate for candidate in known_task_by_source.get((source_id, identity), [])
                                     if candidate["jobId"] == str(job["id"])), None)
                        if identity not in source_video_ids(job) and not task:
                            continue
                        matched_job = job
                        status = task_state(job)
                        if status == "active":
                            break
                        if status == "complete":
                            run["excluded"]["complete"] += 1
                            break
                    if matched_job and task_state(matched_job) == "complete":
                        continue
                    name = video_name(remote_video)
                    matches = local_by_name.get(name.casefold(), []) if name else []
                    item = await self._make_item(source_id, remote_video, name, matches, candidate_targets, run_id)
                    if source_id in unresolved_log_datasets:
                        item.update(state="attention", errorCode="task_log_unavailable",
                                    message="平台有切片任务日志无法读取，不能排除重复提交；核对平台任务后再处理。")
                        for category_name, category in item["categories"].items():
                            if category_name == applicable_category(source_id):
                                category.update(state="attention", errorCode="task_log_unavailable",
                                                message="平台切片任务日志无法核对，暂不自动提交。")
                    elif matched_job and task_state(matched_job) == "active":
                        self._bind_platform_job(item, matched_job)
                    if matched_job:
                        item["previousTask"] = {"id": str(matched_job["id"]), "state": task_state(matched_job),
                                                 "type": str(matched_job.get("type", ""))}
                        if task_state(matched_job) == "failed":
                            item["message"] = "此前平台切片任务失败，可在确认区间后单条重试。"
                    self.store.save("items", item)
                    candidates.append((source_id, remote_video, item, name))
            run.update(state="complete", message="扫描完成。请核对目标图片集和需要手动绑定的本地视频，再开始自动切片。",
                       finishedAt=now_iso(), items=[item[2]["id"] for item in candidates], itemCount=len(candidates),
                       targetDatasets=candidate_targets, taskLogIndex=log_index, taskLogLines=task_log_rows,
                       sourceCounts={key: len(value) for key, value in all_remote.items()})
            self.store.save("runs", run)
            request = self.store.get("requests", run["requestId"])
            if request:
                request.update(state="complete")
                self.store.save("requests", request)
            self.store.settings["latestRunId"] = run_id
            self.store.save_settings()
        except asyncio.CancelledError:
            run.update(state="interrupted", message="扫描已暂停，之前的任务记录与抽样观察保留。")
            self.store.save("runs", run)
            raise
        except (AuditError, ValueError, OSError, TimeoutError) as error:
            run.update(state="waiting_connection" if isinstance(error, AuditError) and error.code in {"auth", "network", "disk"} else "attention",
                       message=str(error) if isinstance(error, (AuditError, ValueError)) else "扫描未完成，原有进度保留。")
            self.store.save("runs", run)
        except Exception:
            run.update(state="attention", message="平台或本机返回了无法核对的数据，原有进度保留；检查连接后重试扫描。")
            self.store.save("runs", run)

    async def _make_item(self, source_id, remote_video, name, local_matches, targets, run_id):
        identity = str(remote_video["id"])
        props = video_properties(remote_video)
        issue = ""
        local = None
        if not name:
            issue = "local_video_missing"
        elif len(local_matches) == 0:
            issue = "local_video_missing"
        elif len(local_matches) > 1:
            issue = "local_video_ambiguous"
        else:
            local = local_matches[0]
            try:
                metadata = await asyncio.to_thread(ffprobe_file, local["path"], self.store.settings["frameToolPath"])
                # The platform's cut selector is authoritative for source-video
                # identity. Use the unique exact-name local file for inference;
                # do not block on missing or differing remote size/duration fields.
                local["metadata"] = metadata
            except Exception:
                issue = "video_probe_failed"
        active_category = applicable_category(source_id)
        categories = {}
        for category in ("bed", "magnet"):
            categories[category] = {"state": "pending" if category == active_category else "not_applicable",
                                    "intervals": [], "observations": 0,
                                    "estimatedImages": None, "actualImages": None,
                                    "targetDatasetId": None, "taskId": "", "intervalSeconds": None,
                                    "message": "" if category == active_category else "此视频集不使用该类别模型。"}
        if issue:
            categories[active_category].update(state="attention", errorCode=issue,
                                                message=ERROR_TEXT.get(issue, ""))
        return {"id": uuid4().hex, "runId": run_id, "sourceDatasetId": source_id, "sourceVideoId": identity,
                "name": name or f"视频 {identity}", "remote": {"size": props["size"], "duration": props["duration"],
                "width": props["width"], "height": props["height"], "cutStatus": remote_video.get("cutStatus", 0)},
                "path": local["path"] if local else "", "fileVersion": local["version"] if local else None,
                "metadata": local.get("metadata") if local else None, "state": "attention" if issue else "pending",
                "errorCode": issue, "message": ERROR_TEXT.get(issue, ""), "revision": 0,
                "categories": categories, "settingsRevision": self.store.settings["revision"],
                "targetResolved": bool(targets.get(source_id, {}).get("resolved")), "progress": 0,
                "createdAt": now_iso()}

    def _model_fingerprint(self, category):
        settings = self.store.settings
        if category == "magnet":
            paths = [settings["magnetModelPath"], Path(settings["magnetDeployDirectory"]) / "infer.py"]
        else:
            paths = [settings["bedModelPath"], settings["bedGeometryPath"],
                     Path(settings["bedDeployDirectory"]) / "training" / "supervised.py",
                     Path(settings["bedDeployDirectory"]) / "training" / "data.py"]
        parts = []
        for raw in paths:
            path = Path(raw).expanduser()
            if not path.is_file():
                return "missing"
            info = path.stat()
            key = (str(path), info.st_size, info.st_mtime_ns)
            digest = self.fingerprint_cache.get(key)
            if digest is None:
                digest = _hash_file(path)
                self.fingerprint_cache[key] = digest
            parts.append(digest)
        config = {"policy": POLICY_VERSION, "sampling": SAMPLING_VERSION, "files": parts,
                  "confidence": settings.get("magnetConfidence", .25) if category == "magnet" else "checkpoint"}
        return hashlib.sha256(json.dumps(config, sort_keys=True).encode()).hexdigest()

    def _observation_key(self, item, category, timestamp, model_fingerprint):
        payload = {"version": item.get("observationVersion", item.get("fileVersion")), "model": model_fingerprint, "preprocess": SAMPLING_VERSION,
                   "position": round(float(timestamp), 6), "category": category}
        return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()).hexdigest()

    def _get_observation(self, key):
        observation = self.store.get("observations", key)
        if observation and observation.get("code") in {"frame_extract_failed", "worker_stalled", "media_check_timeout", "observations_incomplete"}:
            return None
        return observation

    async def _refresh_local_binding(self, item):
        """Find a moved local source by unique exact filename before extraction.

        Platform-selected video identity remains authoritative. This lookup
        only locates local bytes and deliberately does not compare platform
        size or duration metadata.
        """
        old_path = item.get("path") or ""
        old_version = item.get("fileVersion")
        try:
            current_version = source_version(old_path) if old_path else None
        except (OSError, ValueError):
            current_version = None

        if current_version and current_version == old_version:
            return False
        if current_version:
            matches = [{"name": item.get("name", ""), "path": old_path,
                        "version": current_version, "size": current_version["size"]}]
        else:
            local, _roots, issues = await asyncio.to_thread(scan_local_files, self.store.settings["directories"])
            if issues:
                raise ValueError("disk")
            expected = str(item.get("name", "")).casefold()
            matches = [row for row in local if row["name"].casefold() == expected]
        if not matches:
            raise ValueError("local_video_missing")
        if len(matches) != 1:
            raise ValueError("local_video_ambiguous")

        match = matches[0]
        changed_version = bool(old_version and match["version"] != old_version)
        has_platform_operation = any(
            category.get("taskId") or category.get("state") in _PLATFORM_STARTED_STATES
            for category in item.get("categories", {}).values()
        )
        if changed_version and has_platform_operation:
            raise ValueError("source_changed")

        metadata = await asyncio.to_thread(ffprobe_file, match["path"], self.store.settings["frameToolPath"])
        latest = self.store.get("items", item["id"])
        if latest and latest.get("path") != old_path and latest.get("revision") != item.get("revision"):
            item.clear()
            item.update(latest)
            return True
        if changed_version:
            item.pop("observationVersion", None)
            item.pop("submissionFileVersion", None)
        path_changed = match["path"] != old_path
        item.update(path=match["path"], fileVersion=match["version"], metadata=metadata)
        if path_changed or changed_version:
            active_category = applicable_category(item["sourceDatasetId"])
            for category_name, category in item.get("categories", {}).items():
                if category_name == active_category and category.get("state") == "attention" and category.get("errorCode") in _RETRYABLE_LOCAL_ERRORS:
                    category.update(state="pending", errorCode="", message="已重新定位本地视频，将恢复抽帧识别。")
                elif category_name != active_category and not category.get("taskId"):
                    category.update(state="not_applicable", errorCode="", message="此视频集不使用该类别模型。")
            item["updatedAt"] = now_iso()
            self.store.save("items", item)
        return path_changed or changed_version

    async def _analyze_item(self, item):
        if not item.get("path") or not item.get("fileVersion"):
            raise ValueError("local_video_missing")
        if source_version(item["path"]) != item["fileVersion"]:
            raise ValueError("source_changed")
        metadata = item.get("metadata") or await asyncio.to_thread(
            ffprobe_file, item["path"], self.store.settings["frameToolPath"])
        item["metadata"] = metadata
        self.store.save("items", item)
        settings = self.store.settings
        active_category = applicable_category(item["sourceDatasetId"])
        run_categories = ("bed", "magnet") if settings.get("sharedSampling", True) else (active_category,)
        for name in run_categories:
            slot = item["categories"].setdefault(name, {"state": "pending", "intervals": []})
            if name != active_category and slot.get("state") == "not_applicable" and not slot.get("taskId"):
                slot.update(state="pending", errorCode="", message="与另一模型共用抽帧，同时识别。")
        intervals = {}
        fingerprints = {}
        for category in run_categories:
            slot = item["categories"][category]
            if slot.get("intervalSeconds") and slot.get("state") not in {"pending"}:
                continue
            if slot.get("state") in {"complete", "waiting", "submitting", "review", "queued", "uploading", "uploaded", "importing"}:
                continue
            intervals[category] = int(settings[f"{category}IntervalSeconds"] if not settings.get("sharedSampling", True) else settings["sampleIntervalSeconds"])
            fingerprints[category] = self._model_fingerprint(category)
        results = {}
        phase = "initial"
        for phase in ("initial", "refine"):
            points_by_category = {}
            keys_by_category = {}
            active_categories = tuple(intervals) if phase == "initial" else tuple(name for name in intervals if name == "magnet")
            for category in active_categories:
                sample_interval = 10 if phase == "refine" else intervals[category]
                points = sample_points(float(metadata["duration"]), sample_interval, float(metadata.get("frameRate") or 25))
                keys, missing = {}, []
                for point in points:
                    key = self._observation_key(item, category, point, fingerprints[category])
                    keys[point] = key
                    if not self._get_observation(key):
                        missing.append(point)
                points_by_category[category] = missing
                keys_by_category[category] = keys
            if any(points_by_category.values()):
                await self._run_worker(item, metadata, points_by_category, keys_by_category, phase)
            for category in active_categories:
                sample_interval = 10 if phase == "refine" else intervals[category]
                results[category] = self._category_observations(item, category,
                    sample_points(metadata["duration"], sample_interval, metadata.get("frameRate", 25)),
                    keys_by_category.get(category, {}), fingerprints[category])
            if phase == "initial":
                magnet = results.get("magnet", [])
                if "magnet" not in intervals:
                    break
                initial_result = build_category_result(magnet, interval=intervals["magnet"],
                    duration=metadata["duration"], positive_labels={"target"}, target=category_image_target(settings, "magnet"))
                if not (initial_result["estimatedImages"] < 100 or initial_result["estimatedImages"] > 200):
                    break
        analyzed = {}
        for category, sample_interval in intervals.items():
            observations = results.get(category, [])
            refined = category == "magnet" and phase == "refine"
            expected_points = sample_points(metadata["duration"], 10 if refined else sample_interval,
                                            metadata.get("frameRate", 25))
            uncertain = [row for row in observations if row["label"] == "uncertain"]
            error = next((row.get("code") for row in observations if row.get("code")), "")
            final_interval = 10 if refined else sample_interval
            result = build_category_result(observations, interval=final_interval, duration=metadata["duration"],
                positive_labels={"target"}, target=category_image_target(settings, category))
            categories = item["categories"][category]
            categories.update(intervals=result["intervals"], intervalSeconds=result["intervalSeconds"],
                              estimatedImages=result["estimatedImages"], observations=len(observations),
                              samplingIntervalSeconds=final_interval, modelFingerprint=fingerprints[category])
            if categories.get("state") == "attention":
                pass
            elif len(observations) != len(expected_points):
                categories.update(state="attention", errorCode="observations_incomplete",
                                  message=ERROR_TEXT["observations_incomplete"])
            elif error:
                categories.update(state="attention", errorCode=error, message=ERROR_TEXT.get(error, ERROR_TEXT["inference_failed"]))
            elif uncertain:
                code = uncertain[0].get("code", "bed_alignment_uncertain")
                categories.update(state="attention", errorCode=code, message=ERROR_TEXT.get(code, ERROR_TEXT["bed_alignment_uncertain"]))
            elif not result["intervals"]:
                categories.update(state="no_target", message="未检出目标")
            elif result["tooMany"] or (result["intervalSeconds"] is None and result["estimatedImages"] > 200):
                categories.update(state="attention", errorCode="slice_limit", message=ERROR_TEXT["slice_limit"])
            elif categories.get("state") not in {"complete", "waiting", "submitting", "attention"}:
                categories.update(state="ready", errorCode="", message="识别完成，准备核对平台任务槽位。")
            target_key = "27912" if category == "bed" else "27944"
            categories["targetDatasetId"] = settings.get("targetDatasetIds", {}).get(target_key)
        self.store.save("items", item)
        return metadata

    def _category_observations(self, item, category, points, keys, model_fingerprint):
        rows = []
        for point in points:
            key = keys.get(point) or self._observation_key(item, category, point, model_fingerprint)
            observation = self._get_observation(key)
            if observation:
                rows.append({key_name: observation[key_name] for key_name in ("time", "label", "score", "code", "width", "height", "quality") if key_name in observation})
        return sorted(rows, key=lambda row: row["time"])

    async def _run_worker(self, item, metadata, points, keys, phase):
        python = str(Path(self.store.settings["workerPythonPath"]).expanduser())
        if not Path(python).is_file():
            raise ValueError("worker_missing")
        if not Path(self.store.settings["frameToolPath"]).is_file():
            raise ValueError("worker_missing")
        request = {"filePath": item["path"], "frameToolPath": self.store.settings["frameToolPath"],
                   "settings": {key: self.store.settings[key] for key in (
                       "magnetDeployDirectory", "magnetModelPath", "bedDeployDirectory", "bedModelPath",
                       "bedGeometryPath", "magnetConfidence")}, "points": points}
        with self._read_lease(item["path"]):
            process = await asyncio.create_subprocess_exec(python, "-B", "-m", "app.slicing_worker",
                stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL,
                cwd=str(ROOT), limit=128 * 1024, start_new_session=(os.name == "posix"))
            self.worker_process = process
            observations = 0
            failed = []
            fingerprints = {category: self._model_fingerprint(category) for category in points}
            try:
                process.stdin.write((json.dumps(request, separators=(",", ":")) + "\n").encode())
                await process.stdin.drain()
                process.stdin.close()
                while True:
                    try:
                        line = await asyncio.wait_for(process.stdout.readline(), timeout=120)
                    except TimeoutError:
                        failed.append({"category": "both", "code": "worker_stalled", "stage": "sampling"})
                        break
                    if not line:
                        break
                    try:
                        event = json.loads(line)
                    except ValueError:
                        failed.append({"category": "both", "code": "inference_failed"})
                        continue
                    kind = event.get("event")
                    if kind == "stage":
                        item["analysisStage"] = event.get("stage")
                        item["analysisTotal"] = sum(len(values) for values in points.values())
                        item["analysisDone"] = 0
                        self._save_analysis_item(item)
                    elif kind == "observation":
                        category = event.get("category")
                        timestamp = round(float(event.get("time")), 6)
                        key = keys.get(category, {}).get(timestamp)
                        if not key:
                            continue
                        value = {"id": key, "itemId": item["id"], "sourceVideoId": item["sourceVideoId"],
                                 "category": category, "time": timestamp, "label": event.get("label", "uncertain"),
                                 "score": event.get("score"), "code": event.get("code", ""),
                                 "width": event.get("width"), "height": event.get("height"),
                                 "quality": event.get("quality"), "modelFingerprint": fingerprints[category],
                                 "fileVersion": item["fileVersion"], "phase": phase}
                        self.store.save("observations", value)
                        observations += 1
                        item["progress"] = min(90, int(10 + 80 * observations / max(1, sum(len(v) for v in points.values()))))
                        item.update(analysisDone=observations, analysisPosition=timestamp)
                        self._save_analysis_item(item)
                    elif kind == "error":
                        failed.append({"category": event.get("category", "both"), "code": event.get("code", "inference_failed"),
                                       "stage": event.get("stage", "sampling"), "time": event.get("time")})
                if process.returncode is None and failed and any(f["code"] == "worker_stalled" for f in failed):
                    await self._stop_worker(process)
                await process.wait()
            finally:
                if process.returncode is None:
                    await self._stop_worker(process)
                if self.worker_process is process:
                    self.worker_process = None
            if process.returncode != 0 and not failed:
                failed.append({"category": "both", "code": "inference_failed"})
            for failure in failed:
                cats = tuple(points) if failure["category"] == "both" else (failure["category"],)
                for category in cats:
                    if category in item["categories"] and item["categories"][category].get("state") != "not_applicable":
                        item["categories"][category].update(state="attention", errorCode=failure["code"],
                            message=ERROR_TEXT.get(failure["code"], ERROR_TEXT["inference_failed"]),
                            failure={"code": failure["code"], "stage": failure.get("stage", "sampling"),
                                     "time": failure.get("time"), "at": now_iso()})
            if failed:
                self._save_analysis_item(item)

    async def _stop_worker(self, process):
        if process.returncode is not None:
            return
        self._signal_worker(process)
        try:
            await asyncio.wait_for(process.wait(), timeout=5)
        except TimeoutError:
            if os.name == "posix":
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
            elif process.returncode is None:
                process.kill()
            await process.wait()

    @staticmethod
    def _signal_worker(process):
        if process.returncode is not None:
            return
        try:
            if os.name == "posix":
                os.killpg(process.pid, signal.SIGTERM)
            else:
                process.terminate()
        except ProcessLookupError:
            pass

    @contextmanager
    def _read_lease(self, path):
        with self.leases.read(path):
            yield

    async def start(self, request_id, expected_revision, mode="pilot", item_id=None):
        async with self.start_lock:
            return await self._start(request_id, expected_revision, mode, item_id)

    async def _start(self, request_id, expected_revision, mode, item_id):
        existing = self.store.get("requests", request_id)
        if existing:
            if (existing.get("kind") != "start" or (existing.get("itemId") and existing["itemId"] != item_id)
                    or (existing.get("mode") and existing["mode"] != mode)):
                raise Conflict("此请求编号已用于其他操作。")
            return self.state()
        if expected_revision != self.store.settings["revision"]:
            raise Conflict("模型切片设置已更新，请刷新后重新开始。")
        if mode not in {"pilot", "all", "single"}:
            raise ValueError("切片启动模式无效。")
        if self.busy or self.store.settings.get("enabled"):
            raise Conflict("当前切片仍在运行或停止中，请等待本条结束，或先暂停。")
        run = self.store.get("runs", self.store.settings.get("latestRunId"))
        scan_ready = bool(run and (run.get("state") in {"complete", "pilot_complete"} or
            (run.get("state") in {"attention", "paused", "interrupted"}
             and (run.get("finishedAt") or run.get("singleMode")))))
        if not scan_ready:
            raise ValueError("请先完成“扫描待切片视频”，再开始自动切片。")
        if mode == "single":
            if not item_id:
                raise ValueError("请先完成扫描，并选择一条待处理视频进行单条试切。")
            item = self.store.get("items", item_id)
            if not item or item.get("runId") != run.get("id"):
                raise ValueError("所选视频不属于当前扫描批次，请刷新后重新选择。")
            category_name = applicable_category(item.get("sourceDatasetId"))
            category = item.get("categories", {}).get(category_name, {})
            if (item.get("state") not in {"pending", "platform_processing"}
                    or category.get("state") not in {"pending", "ready", "waiting_slot"} or not item.get("path")):
                raise ValueError("请选择已绑定本地视频的待处理项，或已确认区间、尚未提交的项目。")
            if category.get("taskId") or category.get("state") in {"submitting", "waiting", "complete"}:
                raise ValueError("这条视频已有平台切片操作，不能重复提交。")
            if source_version(item["path"]) != item.get("fileVersion"):
                raise Conflict("本地视频版本已变化，请重新扫描后再试切。")
            target_id = str(self.store.settings.get("targetDatasetIds", {}).get(item["sourceDatasetId"]) or "")
            if not target_id:
                raise ValueError("扫描尚未确认该类别的目标图片集，请重新扫描。")
        elif item_id:
            raise ValueError("只有单条试切模式可以指定视频。")
        continuing_pilot = run.get("state") == "pilot_complete"
        if continuing_pilot and mode == "pilot":
            raise ValueError("试切已完成，请选择“开始剩余批量切片”。")
        if mode == "all":
            continuing_pilot = True
        if run.get("settingsRevision") != self.store.settings["revision"]:
            raise Conflict("模型、目录或目标数据集设置已变化，请重新扫描后再开始。")
        if not self.auth.connected:
            raise AuditError("请先连接视觉AI平台。", "auth", 401)
        targets_to_verify = {item["sourceDatasetId"]: SOURCE[item["sourceDatasetId"]]} if mode == "single" else SOURCE
        for source_id, descriptor in targets_to_verify.items():
            chosen = str(self.store.settings.get("targetDatasetIds", {}).get(source_id) or "")
            if not chosen:
                raise ValueError("请先为车斗和磁盘吊分别选择图片目标数据集。")
            info = await self.remote.dataset(chosen)
            if not await target_dataset_verified(self.remote, chosen, info, descriptor):
                raise ValueError(f"目标数据集 {chosen} 未通过名称、图片类型和命名空间核验。")
        self.store.settings["enabled"] = True
        self.store.settings["saveRequestId"] = request_id
        self.store.save_settings()
        if mode == "single":
            run.update(pilotMode=False, singleMode=True, singleItemId=item_id,
                       singleStartedAt=now_iso())
            message = f"单条试切已启动：{item.get('name')}；其他视频不会自动处理。"
        elif continuing_pilot:
            run["singleMode"] = False
            run.pop("singleItemId", None)
            run["pilotMode"] = False
            for item in self.store.list("items"):
                changed = False
                for category in item.get("categories", {}).values():
                    if category.get("state") == "pilot_deferred":
                        category.update(state="ready", message="已开放批量切片，按原有识别区间继续。")
                        changed = True
                if changed:
                    item["revision"] += 1
                    self.store.save("items", item)
            message = "批量切片已启动，沿用试切通过的设置和观察记录。"
        else:
            run["singleMode"] = False
            run.pop("singleItemId", None)
            run.update(pilotMode=True, pilotDoneTargets=[], pilotStartedAt=now_iso())
            message = "试切已启动：每个图片目标集最多先完成一条并核验输出，核验后再开放剩余批次。"
        run.update(state="running", message=message)
        self.run = run
        self.store.save("runs", run)
        self.store.save("requests", {"id": request_id, "kind": "start", "runId": run["id"],
                                     "itemId": item_id, "mode": mode, "state": "running"})
        self._wake_scheduler()
        return self.state()

    def pause(self):
        self.store.settings["enabled"] = False
        self.store.save_settings()
        if self.run:
            self.run.update(state="paused", message="已暂停。已保存观察、任务编号和提交前快照均保留。")
            self.store.save("runs", self.run)
        workflow = self.store.settings.get("workflow")
        if workflow:
            workflow["enabled"] = False
            self.store.save_settings()
        if self.scan_task and not self.scan_task.done():
            self.scan_task.cancel()
        if self.worker_process and self.worker_process.returncode is None:
            self._signal_worker(self.worker_process)
        if self.scheduler_task and not self.scheduler_task.done():
            self.scheduler_task.cancel()
        if self.analysis_task and not self.analysis_task.done():
            self.analysis_task.cancel()
        return self.state()

    def resume(self, request_id):
        existing = self.store.get("requests", request_id)
        if existing:
            if existing.get("kind") != "resume":
                raise Conflict("此请求编号已用于其他操作。")
            return self.state()
        if self.start_lock.locked():
            raise Conflict("切片启动中，请等待操作完成。")
        if self.busy and not self.store.settings.get("enabled"):
            raise Conflict("上一条处理正在停止，请稍后继续。")
        if not self.auth.connected:
            raise AuditError("请先重新连接视觉AI平台，已保存进度保留。", "auth", 401)
        run = self.store.get("runs", self.store.settings.get("latestRunId"))
        if not run or run.get("state") not in {"paused", "interrupted", "waiting_connection", "running", "attention"}:
            raise ValueError("没有可继续的切片批次。")
        if run.get("settingsRevision", self.store.settings["revision"]) != self.store.settings["revision"]:
            raise Conflict("模型或目录设置已变化，请重新扫描后再继续。")
        if run.get("singleMode"):
            selected = self.store.get("items", run.get("singleItemId")) or {}
            if selected.get("state") in {"attention", "complete"}:
                raise ValueError("上次单条试切已结束。请在视频卡片上单条重试，或选择其他待处理视频。")
        self.store.settings["enabled"] = True
        self.store.save_settings()
        workflow = self.store.settings.get("workflow")
        if workflow and (run.get("executeAll") or workflow.get("phase") == "scan"):
            workflow["enabled"] = True
            self.store.save_settings()
        run.update(state="running", message="正在先对账已有平台任务，再继续队列。")
        self.run = run
        self.store.save("runs", run)
        self.store.save("requests", {"id": request_id, "kind": "resume", "runId": run["id"], "state": "running"})
        self._wake_scheduler()
        return self.state()

    def retry(self, identity, expected_revision, request_id):
        item = self.store.get("items", identity)
        existing = self.store.get("requests", request_id)
        if existing:
            if existing.get("kind") != "retry" or existing.get("itemId") != identity:
                raise Conflict("此请求编号已用于其他操作。")
            return self.state()
        self._revision(item, expected_revision)
        if self.start_lock.locked():
            raise Conflict("切片启动中，请等待操作完成。")
        if self.busy and not self.store.settings.get("enabled"):
            raise Conflict("上一条处理正在停止，请稍后重试。")
        if not self.auth.connected:
            raise AuditError(ERROR_TEXT["auth"], "auth", 401)
        if item.get("state") != "attention":
            raise Conflict("该视频状态已变化，请刷新后查看；不会重复识别或提交。")
        run = self.store.get("runs", self.store.settings.get("latestRunId"))
        if not run or run.get("state") == "scanning":
            raise ValueError("请先完成平台视频扫描。")
        if run.get("settingsRevision", self.store.settings["revision"]) != self.store.settings["revision"]:
            raise Conflict("模型或目录设置已变化，请重新扫描后再重试。")
        if self.store.settings.get("enabled") and run.get("singleMode") and run.get("singleItemId") != identity:
            raise Conflict("另一条试切正在运行，请等待结束后再重试本条。")
        if item.get("errorCode") == "task_log_unavailable":
            raise ValueError("请重新扫描并核对平台切片日志后，再重试这条视频。")
        active_category = applicable_category(item["sourceDatasetId"])
        output_retry = False
        uncertain_retry = False
        for category_name, category in item["categories"].items():
            if category_name == active_category and category.get("state") == "attention":
                output_retry = bool(category.get("taskId") and category.get("errorCode") == "output_unverified")
                uncertain_retry = bool(category.get("operationId") and category.get("errorCode") == "submit_uncertain")
                archive_failed_attempt(category)
                category.update(state="submitting" if uncertain_retry else "waiting" if output_retry else "pending", errorCode="",
                    message="正在对账原提交，不重新发送。" if uncertain_retry else
                            "继续核验已有平台任务的图片。" if output_retry else "已加入单条重试队列。")
            elif category_name != active_category and not category.get("taskId"):
                category.update(state="not_applicable", errorCode="", message="此视频集不使用该类别模型。")
        if not item.get("path") and not (output_retry or uncertain_retry):
            raise ValueError("请先绑定本地视频文件，再重试识别。")
        item.update(state="platform_processing" if (output_retry or uncertain_retry) else "pending", errorCode="",
                    message="正在对账原提交，不重新发送。" if uncertain_retry else
                            "等待核验已有切片结果。" if output_retry else "正在重新识别这条视频；其他视频保持原进度。",
                    revision=item["revision"] + 1)
        self.store.save("items", item)
        self.store.save("requests", {"id": request_id, "kind": "retry", "itemId": identity, "state": "queued"})
        if not self.store.settings.get("enabled"):
            # A single retry authorizes this video only, including when the last
            # selected video failed. Never silently enable the rest of the batch.
            run.update(state="running", pilotMode=False, singleMode=True, singleItemId=identity,
                       singleStartedAt=now_iso(), message=f"正在单条恢复：{item.get('name')}；其他视频保持原进度。")
            self.run = run
            self.store.save("runs", run)
            self.store.settings["enabled"] = True
            self.store.save_settings()
        self._continue_after_manual_action()
        self._wake_scheduler()
        return self.state()

    async def bind(self, identity, data):
        item = self.store.get("items", identity)
        existing = self.store.get("requests", data["requestId"])
        if existing:
            if existing.get("kind") != "bind" or existing.get("itemId") != identity:
                raise Conflict("此请求编号已用于其他操作。")
            return self.state()
        self._revision(item, data["expectedRevision"])
        active = item["categories"][applicable_category(item["sourceDatasetId"])]
        if (item.get("state") == "analyzing" or active.get("taskId") or active.get("errorCode") == "submit_uncertain"
                or active.get("state") in {"submitting", "waiting", "complete"}):
            raise Conflict("本条已开始识别或平台处理，请先等待结束；当前绑定和进度已保留。")
        path = safe_path(data["path"], must_exist=True, kind="file")
        if path.suffix.lower() not in VIDEO_EXTENSIONS or path.name.casefold() != item["name"].casefold():
            raise ValueError("请选择与平台完整文件名相同的视频。")
        version = source_version(path)
        metadata = await asyncio.to_thread(ffprobe_file, path, self.store.settings["frameToolPath"])
        remote = item.get("remote", {})
        if remote.get("size") is not None and int(remote["size"]) != version["size"]:
            raise ValueError("本地文件大小与平台视频不符，不能绑定。")
        if remote.get("duration") is not None and abs(float(remote["duration"]) - metadata["duration"]) > max(1, 0.01 * float(remote["duration"])):
            raise ValueError("本地视频时长与平台视频不符，不能绑定。")
        self._revision(self.store.get("items", identity), data["expectedRevision"])
        if source_version(path) != version:
            raise Conflict("文件在绑定过程中已变化，请重新选择。")
        if version != item.get("fileVersion"):
            item.pop("observationVersion", None)
            item.pop("submissionFileVersion", None)
        blocked = item.get("errorCode") == "task_log_unavailable"
        item.update(path=str(path), fileVersion=version, metadata=metadata,
                    state="attention" if blocked else "pending",
                    errorCode="task_log_unavailable" if blocked else "",
                    message="本地视频已绑定；仍需重新扫描并核对平台切片日志。" if blocked else "本地视频已人工绑定；将按当前文件版本重新识别。",
                    revision=item["revision"] + 1)
        if not blocked:
            active_category = applicable_category(item["sourceDatasetId"])
            for category_name, category in item["categories"].items():
                if category_name == active_category:
                    category.update(state="pending", errorCode="", message="")
                else:
                    category.update(state="not_applicable", errorCode="", message="此视频集不使用该类别模型。")
        self.store.save("items", item)
        self.store.save("requests", {"id": data["requestId"], "kind": "bind", "itemId": identity, "state": "bound"})
        self._continue_after_manual_action()
        self._wake_scheduler()
        return self.state()

    async def review(self, identity, data):
        item = self.store.get("items", identity)
        existing = self.store.get("requests", data["requestId"])
        if existing:
            if existing.get("kind") != "review" or existing.get("itemId") != identity:
                raise Conflict("此请求编号已用于其他操作。")
            return self.state()
        self._revision(item, data["expectedRevision"])
        if item.get("errorCode") == "task_log_unavailable":
            raise ValueError("请先重新扫描并核对平台切片日志，当前不能提交以免重复切片。")
        category = data["category"]
        if category != applicable_category(item["sourceDatasetId"]):
            raise ValueError("这类视频不使用当前模型，不能提交对应类别切片。")
        value = item["categories"][category]
        if (item.get("state") == "analyzing" or value.get("errorCode") == "submit_uncertain"
                or (value.get("taskId") and value.get("errorCode") != "platform_failed")
                or value.get("state") in {"submitting", "waiting", "complete"}):
            raise Conflict("本条已开始识别或平台处理，不能覆盖已经提交的区间。")
        if data.get("confirmedNoTarget"):
            archive_failed_attempt(value)
            value.update(state="no_target", intervals=[], intervalSeconds=None, estimatedImages=0,
                         errorCode="", message="人工确认：未检出目标。")
            item.update(state="no_target", errorCode="", message="人工确认未检出目标，未提交空任务。")
        elif data.get("intervals") is not None:
            from .slicing_core import merge_intervals, choose_slice_interval
            intervals = merge_intervals((row["start"], row["end"]) for row in data["intervals"])
            if not intervals:
                raise ValueError("请填写至少一个有效区间，或明确确认未检出目标。")
            duration = float((item.get("metadata") or {}).get("duration") or item.get("remote", {}).get("duration") or 0)
            if duration <= 0 or any(end > duration for _, end in intervals):
                raise ValueError("人工区间超出视频时长。")
            choice = choose_slice_interval(intervals, target=category_image_target(self.store.settings, category))
            selected = data.get("intervalSeconds") or choice["interval"]
            count = sum(max(1, math.floor((end - start) / selected + 1e-9) + 1) for start, end in intervals)
            if count > 200:
                raise ValueError("该间隔预计超过200张，请加大间隔或调整区间。")
            archive_failed_attempt(value)
            value.update(state="ready", intervals=[{"start": a, "end": b} for a, b in intervals],
                         intervalSeconds=selected, estimatedImages=count, errorCode="",
                         message="人工确认区间，准备提交平台切片任务。", manualReview=True)
            item.update(state="platform_processing", errorCode="", message="人工区间已保存，等待继续处理。")
        else:
            raise ValueError("请选择人工修正区间，或确认未检出目标。")
        item["revision"] += 1
        self.store.save("items", item)
        self.store.save("requests", {"id": data["requestId"], "kind": "review", "itemId": identity, "state": "saved"})
        self._continue_after_manual_action()
        self._wake_scheduler()
        return self.state()

    def _wake_scheduler(self):
        self.wake.set()
        if self.store.settings.get("enabled") and (not self.scheduler_task or self.scheduler_task.done()):
            self.scheduler_task = asyncio.create_task(self._scheduler())

    def _continue_after_manual_action(self):
        self.run = self.store.get("runs", self.store.settings.get("latestRunId")) or self.run
        # A scan or manual review is not consent to submit cut tasks. Resume
        # only when the user had already enabled this batch.
        if self.store.settings.get("enabled") and self.run and self.run.get("state") in {"attention", "complete"}:
            self.run.update(state="running", message="已收到人工处理结果，继续核对平台并运行可处理队列。")
            self.store.save("runs", self.run)

    async def startup(self):
        await self._recover_moved_sources()
        if self.files:
            self.files.refresh()
        await self.packages.start()
        if self.store.settings.get("enabled"):
            self._wake_scheduler()

    async def _recover_moved_sources(self):
        """Repair moved paths and give old frame failures one safe automatic retry."""
        for item in self.store.list("items"):
            try:
                active_category = applicable_category(item.get("sourceDatasetId"))
            except ValueError:
                active_category = ""
            if active_category:
                for category_name, category in item.get("categories", {}).items():
                    if (category_name != active_category and not category.get("taskId")
                            and category.get("state") not in {"complete", "waiting", "submitting"}):
                        category.update(state="not_applicable", errorCode="", message="此视频集不使用该类别模型。")
            errors = {item.get("errorCode", "")} | {
                category.get("errorCode", "") for category in item.get("categories", {}).values()
            }
            if not errors.intersection(_RETRYABLE_LOCAL_ERRORS):
                continue
            frame_failure = "frame_extract_failed" in errors
            try:
                moved = await self._refresh_local_binding(item)
            except (OSError, ValueError):
                continue
            if moved and frame_failure:
                item["frameRepairAttemptedVersion"] = item.get("fileVersion")
            if not moved and frame_failure:
                try:
                    current_version = source_version(item.get("path", ""))
                except (OSError, ValueError):
                    continue
                # Requeue a persisted FFmpeg failure once for the exact file
                # version. A repeated failure stays visible for manual review.
                if current_version != item.get("fileVersion") or item.get("frameRepairAttemptedVersion") == current_version:
                    continue
                item["frameRepairAttemptedVersion"] = current_version
            elif not moved:
                continue
            for category_name, category in item.get("categories", {}).items():
                if category_name == active_category and category.get("state") == "attention" and category.get("errorCode") in _RETRYABLE_LOCAL_ERRORS:
                    category.update(state="pending", errorCode="", message="原视频已重新定位，将恢复抽帧识别。")
            if item.get("errorCode") in _RETRYABLE_LOCAL_ERRORS:
                item.update(state="pending", errorCode="", message="原视频已重新定位，可继续抽帧识别。")
            elif item.get("state") == "attention" and not any(
                    value.get("state") == "attention" for value in item.get("categories", {}).values()):
                item.update(state="pending", message="原视频已重新定位，可继续抽帧识别。")
            item["revision"] = int(item.get("revision", 0)) + 1
            self.store.save("items", item)

    async def connection_restored(self):
        """Resume an enabled queue once the shared platform client reconnects."""
        if self.store.settings.get("enabled") and self.auth.connected:
            if self.run and self.run.get("state") in {"waiting_connection", "interrupted"}:
                self.run.update(state="running", message="平台已连接，先核对任务状态再继续切片队列。")
                self.store.save("runs", self.run)
            self._wake_scheduler()

    async def shutdown(self):
        self.stopping = True
        await self.packages.shutdown()
        if self.files:
            await self.files.shutdown()
        for task in (self.scan_task, self.scheduler_task, self.analysis_task):
            if task and not task.done():
                task.cancel()
        if self.worker_process and self.worker_process.returncode is None:
            self._signal_worker(self.worker_process)
        await asyncio.gather(*(task for task in (self.scan_task, self.scheduler_task, self.analysis_task) if task), return_exceptions=True)

    async def _scheduler(self):
        while self.store.settings.get("enabled") and not self.stopping:
            self.wake.clear()
            try:
                await self._tick()
            except asyncio.CancelledError:
                raise
            except (AuditError, OSError, ValueError) as error:
                if self.run:
                    code = error.code if isinstance(error, AuditError) else "worker_failed"
                    self.run.update(state="waiting_connection" if code in {"auth", "network"} else "attention",
                                    message=ERROR_TEXT.get(code, str(error) if isinstance(error, ValueError) else "切片暂时未完成，进度保留。"))
                    self.store.save("runs", self.run)
                if isinstance(error, AuditError) and error.code in {"auth", "network"}:
                    try:
                        await asyncio.wait_for(self.wake.wait(), timeout=TICK_SECONDS)
                    except TimeoutError:
                        pass
                    continue
            except Exception as error:
                self.store.settings["enabled"] = False
                self.store.save_settings()
                if self.run:
                    self.run.update(state="attention",
                        message="切片队列因程序内部异常已暂停；当前视频和其他进度均已保留。",
                        lastFailure={"errorType": type(error).__name__, "at": now_iso()})
                    self.store.save("runs", self.run)
            if not self.store.settings.get("enabled") or self.stopping:
                break
            try:
                await asyncio.wait_for(self.wake.wait(), timeout=self._poll_timeout())
            except TimeoutError:
                pass

    def _poll_timeout(self):
        """Idle queue stays at 30s. Platform cuts and filled slots are checked again in 8s."""
        for item in self.store.list("items"):
            if item.get("state") in {"waiting", "waiting_slot", "platform_processing"}:
                return ACTIVE_TICK_SECONDS
            for category in (item.get("categories") or {}).values():
                if isinstance(category, dict) and category.get("state") in {"waiting", "waiting_slot"}:
                    return ACTIVE_TICK_SECONDS
        return TICK_SECONDS

    async def _tick(self):
        if not self.auth.connected and self.store.settings.get("workflow", {}).get("enabled"):
            await self.auth.restore_saved_connection()
        if not self.auth.connected:
            raise AuditError(ERROR_TEXT["auth"], "auth", 401)
        async with self.tick_lock:
            workflow = self.store.settings.get("workflow") or {}
            if workflow.get("enabled") and workflow.get("phase") == "scan":
                if not await self._workflow_scan():
                    return
            await self._restore_waiting_files()
            if self.analysis_task and self.analysis_task.done():
                finished, self.analysis_task = self.analysis_task, None
                if not finished.cancelled():
                    finished.result()
            single_item_id = self.run.get("singleItemId") if self.run and self.run.get("singleMode") else None
            await self._reconcile_waiting(single_item_id=single_item_id)
            if self.run and self.run.get("pilotMode"):
                target_ids = {str(value) for value in self.store.settings.get("targetDatasetIds", {}).values() if value}
                done = set(self.run.get("pilotDoneTargets", []))
                if target_ids and target_ids.issubset(done):
                    self.run.update(state="pilot_complete", message="车斗和磁盘吊试切均已完成且图片关联核验通过。请先在平台检查结果，再决定是否开放剩余批次。",
                                    pilotCompletedAt=now_iso())
                    self.store.settings["enabled"] = False
                    self.store.save_settings()
                    self.store.save("runs", self.run)
                    return
            submit_items = self.store.list("items")
            if single_item_id:
                submit_items = [item for item in submit_items if item.get("id") == single_item_id]
            for item in submit_items:
                if item.get("state") in {"attention", "excluded", "complete", "no_target", "analyzing"}:
                    continue
                if any(category.get("state") in {"ready", "waiting_slot"} for category in item.get("categories", {}).values()):
                    local = self.store.settings.get("localExport", True) and not (
                        self.run and (self.run.get("pilotMode") or self.run.get("singleMode")))
                    if local:
                        await self._export_ready(item)
                    else:
                        await self._submit_ready(item)
            if self.packages.store.list("items"):
                await self.packages.tick()
            if not self.analysis_task:
                for item in submit_items:
                    if item.get("state") == "pending" and item.get("path"):
                        item.update(state="analyzing", message="正在抽帧识别；平台任务会同时核验。")
                        self.store.save("items", item)
                        self.analysis_task = asyncio.create_task(
                            self._process_shared(item, submit_items) if self.store.settings.get("sharedSampling")
                            else self.process_with_recovery(item))
                        self.analysis_task.add_done_callback(lambda _task: self.wake.set())
                        break
            if self.files and (workflow.get("enabled") or single_item_id):
                self.files.schedule(self.wake.set, item_id=single_item_id)
            items = self.store.list("items")
            if single_item_id:
                items = [item for item in items if item.get("id") == single_item_id]
            actionable = [item for item in items if item.get("state") in {"pending", "analyzing", "platform_processing", "waiting_connection"}
                          or any(category.get("state") in {"submitting", "waiting"} for category in item.get("categories", {}).values())
                          or (item.get("state") != "attention" and any(
                              category.get("state") in {"pending", "ready", "waiting_slot", "submitting", "waiting"}
                              for category in item.get("categories", {}).values()))]
            if self.run:
                unresolved = [item for item in items if item.get("state") == "attention"]
                if actionable:
                    self.run.update(state="running", message=f"队列处理中：{len(actionable)} 条仍可自动继续，{len(unresolved)} 条待核对。")
                else:
                    if single_item_id:
                        selected = items[0] if items else {}
                        selected_complete = selected.get("state") in {"complete", "no_target"}
                        no_target = selected.get("state") == "no_target"
                        self.run.update(state="complete" if selected_complete else "attention",
                            message=(f"单条识别结束：{selected.get('name', single_item_id)} 未检出目标，未提交切片任务。" if no_target else
                                     f"单条试切已结束：{selected.get('name', single_item_id)} 已完成并核验。" if selected_complete
                                     else f"单条试切需人工处理：{selected.get('name', single_item_id)}；其他视频未启动。"))
                        self.run["singleFinishedAt"] = now_iso()
                    else:
                        self.run.update(state="complete" if not unresolved else "attention",
                            message=f"自动队列已结束：{sum(item.get('state') == 'complete' for item in items)} 项类别完成，{len(unresolved)} 项需处理。")
                    archive_pending = self.files and (workflow.get("enabled") or single_item_id) and any(
                        row.get("archiveState") == "waiting" and (not single_item_id or single_item_id in row.get("links", {}).get("slicing", []))
                        for row in self.files.rows("files"))
                    if archive_pending:
                        self.run.update(state="running", message="等待文件释放或磁盘恢复后归档。")
                    else:
                        self.store.settings["enabled"] = False
                        if workflow:
                            workflow.update(phase="complete", enabled=False)
                        self.store.save_settings()
                self.store.save("runs", self.run)

    async def _process_item(self, item):
        guard = ExitStack()
        try:
            await self._refresh_local_binding(item)
            guard.enter_context(self._read_lease(item["path"]))
            item.update(state="analyzing", message="正在按来源视频集运行对应模型并抽帧识别。")
            self.store.save("items", item)
            item["metadata"] = await self._analyze_item(item)
            pending = [c for c in item["categories"].values() if c.get("state") == "pending"]
            if pending:
                item.update(state="pending", errorCode="worker_failed", message=ERROR_TEXT["inference_failed"])
            elif all(c.get("state") in {"no_target", "complete", "not_applicable"} for c in item["categories"].values()):
                no_target = item["categories"][applicable_category(item["sourceDatasetId"])].get("state") == "no_target"
                item.update(state="no_target" if no_target else "complete", errorCode="", progress=100,
                            message="未检出目标，未提交切片任务。" if no_target else "切片任务已完成并核验。")
            elif any(c.get("state") == "attention" for c in item["categories"].values()):
                category = item["categories"][applicable_category(item["sourceDatasetId"])]
                item.update(state="attention", errorCode=category.get("errorCode", ""),
                            message=category.get("message") or "本条需要核对，其他视频可独立继续。")
            else:
                item.update(state="platform_processing", progress=90, message="识别完成，平台切片任务按目标队列继续。")
        except asyncio.CancelledError:
            item.update(state="pending", message="识别已暂停，已保存的有效观察可恢复。")
            raise
        except (AuditError, OSError, ValueError) as error:
            code = error.code if isinstance(error, AuditError) else str(error) if str(error) in ERROR_TEXT else "worker_failed"
            category = applicable_category(item["sourceDatasetId"])
            if item["categories"][category].get("state") == "pending":
                item["categories"][category].update(state="attention", errorCode=code, message=ERROR_TEXT.get(code, "本条未完成，原视频与抽样记录保留。"))
            item.update(state="attention", errorCode=code, message=ERROR_TEXT.get(code, "本条未完成，原视频与抽样记录保留。"))
        except Exception as error:
            code = "internal_error"
            category = applicable_category(item["sourceDatasetId"])
            if item["categories"][category].get("state") == "pending":
                item["categories"][category].update(state="attention", errorCode=code,
                                                     message=ERROR_TEXT[code])
            item.update(state="attention", errorCode=code, message=ERROR_TEXT[code],
                        failureType=type(error).__name__)
            raise
        finally:
            guard.close()
            if item.get("id"):
                stored = self.store.get("items", item["id"])
                revision_after_outcome(stored, item, outcome_changed(stored, item))
                self.store.save("items", item)

    async def _jobs_for_all_sources(self):
        queries = [(source_id, kind) for source_id in SOURCE for kind in ("21", "22")]
        rows = await asyncio.gather(*(self.remote.jobs(source_id, kind) for source_id, kind in queries))
        result = {source_id: [] for source_id in SOURCE}
        for (source_id, _), batch in zip(queries, rows):
            result[source_id].extend(batch)
        return result

    def _image_progress(self, item, category, phase):
        def update(progress):
            category["imageQueryProgress"] = {**progress, "phase": phase}
            total = progress.get("total")
            count = f"{progress['read']} / {total}" if total is not None else str(progress["read"])
            category["message"] = f"{phase}：已读取本视频相关图片 {count} 张。"
            self.store.save("items", item)
        return update

    def _save_category_progress(self, item, baseline=None):
        categories = list(item.get("categories", {}).values())
        if categories and all(c.get("state") in {"complete", "no_target", "not_applicable"} for c in categories):
            no_target = not any(c.get("state") == "complete" for c in categories)
            item.update(state="no_target" if no_target else "complete", errorCode="", progress=100,
                message="未检出目标，未提交切片任务。" if no_target else "切片任务完成，目标图片已核验。")
        elif any(c.get("state") == "attention" for c in categories):
            failure = next(c for c in categories if c.get("state") == "attention")
            item.update(state="attention", errorCode=failure.get("errorCode", ""),
                        message=failure.get("message") or "本条需要核对；其他视频可独立继续。")
        elif categories and all(c.get("state") in {"pilot_deferred", "complete", "no_target", "not_applicable"} for c in categories):
            item.update(state="pilot_deferred", errorCode="", message="该类别已通过试切，等待开放批量切片。")
        elif any(c.get("state") == "review" for c in categories):
            item.update(state="review", errorCode="", message="请查看切片照片后再上传。")
        elif any(c.get("state") in {"queued", "uploading", "uploaded", "importing", "submitting"} for c in categories):
            item.update(state="platform_processing", errorCode="", message="切片压缩包正在上传或等待导入。")
        else:
            item.update(state="platform_processing", errorCode="", message="平台切片处理中，原视频保留。")
        previous = baseline if baseline is not None else (self.store.get("items", item["id"]) if item.get("id") else None)
        revision_after_outcome(previous, item, outcome_changed(previous, item))
        self.store.save("items", item)

    async def _reconcile_waiting(self, single_item_id=None):
        waiting = [item for item in self.store.list("items")
                   if (single_item_id is None or item.get("id") == single_item_id)
                   and any(c.get("state") in {"waiting", "submitting"} for c in item.get("categories", {}).values())]
        if not waiting:
            return
        jobs = None
        for item in waiting:
            for category_name, category in item["categories"].items():
                if category.get("state") not in {"waiting", "submitting"}:
                    continue
                found = None
                if category.get("taskId") and hasattr(self.remote, "job"):
                    try:
                        found = await self.remote.job(category["taskId"])
                    except AuditError as error:
                        if error.code in {"auth", "network", "permission", "identity"}:
                            raise
                if found is None:
                    if jobs is None:
                        jobs = await self._jobs_for_all_sources()
                    found = next((job for rows in jobs.values() for job in rows
                                  if str(job["id"]) == str(category.get("taskId"))), None)
                if not found and category.get("state") == "submitting":
                    if "preSubmitTaskIds" not in category:
                        category.update(state="attention", errorCode="submit_uncertain", message=ERROR_TEXT["submit_uncertain"])
                        continue
                    snapshot = set(category.get("preSubmitTaskIds", []))
                    source_jobs = jobs.get(str(item["sourceDatasetId"]), [])
                    matches = [job for job in source_jobs if str(job.get("id")) not in snapshot
                               and task_matches_submission(job, item, category)]
                    if len(matches) == 1:
                        found = matches[0]
                        category["taskId"] = str(found["id"])
                        category["state"] = "waiting"
                        category["message"] = "已通过平台任务清单对账，继续等待结果。"
                    elif len(matches) > 1:
                        category.update(state="attention", errorCode="submit_uncertain", message=ERROR_TEXT["submit_uncertain"])
                        continue
                    else:
                        category.update(state="attention", errorCode="submit_uncertain", message=ERROR_TEXT["submit_uncertain"])
                        continue
                if not found:
                    category.update(state="attention", errorCode="submit_uncertain", message=ERROR_TEXT["submit_uncertain"])
                    continue
                status = task_state(found)
                category["platformStatus"] = str(found.get("status", ""))
                category["lastPolledAt"] = now_iso()
                if status == "active":
                    category["state"] = "waiting"
                    category["message"] = "平台正在切片；有任务时每8秒核对一次。"
                elif status == "failed":
                    category.update(state="attention", errorCode="platform_failed", message="平台切片任务失败；可核对原视频或调整区间后单条重试。")
                    operation = self.store.get("operations", category.get("operationId"))
                    if operation:
                        operation.update(state="failed", errorCode="platform_failed", message=category["message"])
                        self.store.save("operations", operation)
                elif status == "complete":
                    category.update(message="平台已完成，正在核验本视频生成的图片。")
                    self.store.save("items", item)
                    try:
                        image_rows = await self.remote.image_rows(category["targetDatasetId"],
                            prefix=category.get("prefix") or None,
                            progress=self._image_progress(item, category, "核验切片结果"))
                    except AuditError as error:
                        if error.code in {"auth", "network"}:
                            raise
                        category.update(state="attention", errorCode="output_unverified", message=ERROR_TEXT["output_unverified"])
                    else:
                        baseline = set(category.get("preSubmitImageIds", []))
                        prefix = category.get("prefix", "")
                        associated = [row for row in image_rows if str(row.get("id")) not in baseline and
                            (str(row.get("taskId", row.get("task_id", ""))) == str(category.get("taskId")) or
                             str(row.get("sourceVideoId", row.get("source_video_id", ""))) == item["sourceVideoId"] or
                             (prefix and str(row.get("name", row.get("fileName", ""))).startswith(prefix)))]
                        if associated:
                            category.update(state="complete", actualImages=len(associated), outputImageIds=[str(row["id"]) for row in associated],
                                            message=f"平台任务成功，已关联核验 {len(associated)} 张目标图片。")
                            operation = self.store.get("operations", category.get("operationId"))
                            if operation:
                                operation.update(state="complete", outputImageIds=category["outputImageIds"])
                                self.store.save("operations", operation)
                            if self.run and self.run.get("pilotMode"):
                                done = set(self.run.get("pilotDoneTargets", []))
                                done.add(str(category["targetDatasetId"]))
                                self.run["pilotDoneTargets"] = sorted(done)
                                self.store.save("runs", self.run)
                        else:
                            category.update(state="attention", errorCode="output_unverified", message=ERROR_TEXT["output_unverified"])
            self._save_category_progress(item)

    def _bind_platform_job(self, item, job):
        """Show an in-progress platform cut in the unsliced list without creating another task."""
        category_name = applicable_category(item["sourceDatasetId"])
        category = item.setdefault("categories", {}).setdefault(category_name, {})
        category.update(state="waiting", taskId=str(job["id"]), errorCode="",
                        message="已绑定平台上的切片任务，等待完成。")
        item.update(state="platform_processing", errorCode="", message=category["message"])
        item["revision"] = int(item.get("revision", 0)) + 1

    def _platform_jobs_for_video(self, jobs, source_id, video_id):
        identity = str(video_id)
        found = []
        for job in jobs.get(str(source_id), []):
            if str(job.get("type", "")) not in CUT_TYPES:
                continue
            if identity not in source_video_ids(job):
                continue
            if task_state(job) in {"active", "complete"}:
                found.append(job)
        return found

    def _local_cut_for_video(self, item, category_name):
        identity = str(item.get("sourceVideoId"))
        source = str(item.get("sourceDatasetId"))
        for other in self.store.list("items"):
            if other.get("id") == item.get("id"):
                continue
            if str(other.get("sourceDatasetId")) != source or str(other.get("sourceVideoId")) != identity:
                continue
            category = (other.get("categories") or {}).get(category_name) or {}
            if category.get("taskId") or category.get("state") in {"submitting", "waiting", "complete"}:
                return category
        return None

    def _occupied_slots(self, jobs):
        active_ids = set()
        active_videos = set()
        for source_id, rows in jobs.items():
            for job in rows:
                if task_state(job) != "active" or str(job.get("type", "")) not in CUT_TYPES:
                    continue
                active_ids.add(str(job.get("id")))
                for video_id in source_video_ids(job):
                    active_videos.add((str(source_id), str(video_id)))
        extra = set()
        for stored in self.store.list("items"):
            video_key = (str(stored.get("sourceDatasetId")), str(stored.get("sourceVideoId")))
            for category in (stored.get("categories") or {}).values():
                if not isinstance(category, dict) or category.get("state") not in {"submitting", "waiting"}:
                    continue
                task_id = str(category.get("taskId") or "")
                if task_id and task_id in active_ids:
                    continue
                if not task_id and video_key in active_videos:
                    continue
                extra.add(task_id or f"local:{stored.get('id')}")
        return len(active_ids) + len(extra)

    async def _export_ready(self, item):
        names = [name for name, category in (item.get("categories") or {}).items()
                 if isinstance(category, dict) and category.get("state") in {"ready", "waiting_slot"}
                 and category.get("intervals") and not category.get("packageId")]
        for category_name in names:
            await self._export_category(item, category_name)

    async def _export_category(self, item, category_name):
        category = item["categories"].get(category_name, {})
        if category.get("state") not in {"ready", "waiting_slot"} or category.get("packageId"):
            return
        existing = self.packages.store.find(str(item["sourceDatasetId"]), item.get("sourceVideoId"), category_name)
        if existing:
            category.update(packageId=existing["id"], imageCount=existing.get("imageCount"),
                            state="review" if existing.get("state") == "review" else "queued",
                            message=existing.get("message") or "已有这个视频的切片压缩包，未重复导出。")
            self._save_category_progress(item)
            return
        intervals, seconds = category.get("intervals") or [], category.get("intervalSeconds")
        if not intervals or not seconds:
            category.update(state="no_target", message="未检出目标")
            self._save_category_progress(item)
            return
        if not item.get("path"):
            category.update(state="attention", errorCode="local_video_missing", message=ERROR_TEXT["local_video_missing"])
            self._save_category_progress(item)
            return
        try:
            points = slice_timestamps(intervals, seconds)
            names = photo_names(item.get("name"), len(points))
            frames = []
            for stamp in points:
                frames.append(await asyncio.to_thread(
                    self.slice_reader, item["path"], self.store.settings["frameToolPath"], stamp))
            folder = self.data_dir / "slices" / category_name / str(item["sourceVideoId"])
            image_dir = folder / "image"
            image_dir.mkdir(parents=True, exist_ok=True)
            for name, blob in zip(names, frames):
                (image_dir / name).write_bytes(blob)
            archive = folder / f"{Path(names[0]).stem.rsplit('_', 1)[0]}.zip"
            write_image_archive(archive, names, frames)
        except (OSError, RuntimeError, ValueError):
            category.update(state="attention", errorCode="frame_extract_failed", message=ERROR_TEXT["frame_extract_failed"])
            self._save_category_progress(item)
            return
        # Bed exports are already accepted and can upload immediately. Magnet stays for review.
        review = category_name == "magnet" and bool(self.store.settings.get("reviewBeforeUpload", True))
        record = {"id": package_identity(item["sourceDatasetId"], item["sourceVideoId"], category_name),
                  "dataset": PACKAGE_DATASETS[category_name], "category": category_name,
                  "sourceDatasetId": str(item["sourceDatasetId"]), "sourceVideoId": str(item["sourceVideoId"]),
                  "itemId": item["id"], "name": archive.name, "path": str(archive), "imageDir": str(image_dir),
                  "images": names, "size": archive.stat().st_size, "fileVersion": source_fingerprint(archive),
                  "imageCount": len(names), "bytes": 0, "retries": 0,
                  "state": "review" if review else "queued",
                  "message": "请查看切片照片后再上传。" if review else "正在排队上传切片压缩包。"}
        saved = self.packages.remember(record)
        category.update(packageId=saved["id"], imageCount=saved["imageCount"],
                        state="review" if saved.get("state") == "review" else "queued", message=saved["message"])
        self._save_category_progress(item)
        self.packages.wake.set()

    def confirm_package(self, identity):
        package = self.packages.confirm(identity)
        for item in self.store.list("items"):
            for category in item.get("categories", {}).values():
                if category.get("packageId") == identity and category.get("state") == "review":
                    category.update(state="queued", message="已确认照片，正在排队上传。")
                    self._save_category_progress(item)
        return self.state() | {"package": package}

    def package_image(self, identity, name):
        package = self.packages.store.get("items", identity)
        if not package or name not in set(package.get("images") or []):
            raise ValueError("切片照片不存在。")
        root = Path(package["imageDir"]).resolve()
        path = (root / name).resolve()
        if path.parent != root or not path.is_file():
            raise ValueError("切片照片不存在。")
        return path

    async def _submit_ready(self, item):
        if not self.auth.connected:
            return
        outcome_baseline = copy.deepcopy(self.store.get("items", item["id"]) or item)
        for category in item.get("categories", {}).values():
            if category.get("state") in {"ready", "waiting_slot"}:
                category.update(message="正在核对现有平台切片任务。")
        jobs = await self._jobs_for_all_sources()
        active_category = applicable_category(item["sourceDatasetId"])
        for category_name, category in item["categories"].items():
            if category_name != active_category:
                if not category.get("taskId") and category.get("state") not in {"complete", "waiting", "submitting"}:
                    category.update(state="not_applicable", errorCode="", message="此视频集不使用该类别模型。")
                continue
            if category.get("state") not in {"ready", "waiting_slot"}:
                continue
            source_id = item["sourceDatasetId"]
            target_key = "27912" if category_name == "bed" else "27944"
            target_id = str(self.store.settings.get("targetDatasetIds", {}).get(target_key) or "")
            if not target_id:
                category.update(state="attention", errorCode="target_dataset_unresolved", message=ERROR_TEXT["target_dataset_unresolved"])
                continue
            if self.run and self.run.get("pilotMode") and target_id in set(self.run.get("pilotDoneTargets", [])):
                category.update(state="pilot_deferred", message="此图片目标集已完成一条试切；剩余视频将在批量核验后继续。")
                continue
            related = self._platform_jobs_for_video(jobs, source_id, item.get("sourceVideoId"))
            if len(related) > 1:
                category.update(state="attention", errorCode="submit_uncertain",
                                message="平台存在多条与该视频相关的切片任务，未重复提交。")
                continue
            if len(related) == 1:
                category.update(state="waiting", taskId=str(related[0]["id"]), errorCode="",
                                message="已绑定平台上的切片任务，未重复提交。")
                continue
            local_cut = self._local_cut_for_video(item, category_name)
            if local_cut:
                if local_cut.get("taskId"):
                    category.update(state="waiting", taskId=local_cut["taskId"], errorCode="",
                                    message="已绑定这条视频已有的切片任务，未重复提交。")
                else:
                    category.update(state="attention", errorCode="submit_uncertain",
                                    message="同一视频已有切片操作，未重复提交。")
                continue
            category.update(message="正在核验目标图片集身份。")
            target_info = await self.remote.dataset(target_id)
            if not await target_dataset_verified(self.remote, target_id, target_info, SOURCE[target_key]):
                category.update(state="attention", errorCode="target_dataset_unresolved", message=ERROR_TEXT["target_dataset_unresolved"])
                continue
            if self._occupied_slots(jobs) >= PLATFORM_SLOT_LIMIT:
                category.update(state="waiting_slot", message="已有两条平台切片任务，等待空位后再提交。")
                continue
            if not item.get("path") or source_version(item["path"]) != item.get("fileVersion"):
                category.update(state="attention", errorCode="source_changed", message=ERROR_TEXT["source_changed"])
                continue
            category.update(message="正在确认源视频仍处于未切片状态。")
            current_sources = await self.remote.videos(source_id)
            source_video = next((row for row in current_sources if str(row.get("id")) == item["sourceVideoId"]), None)
            if not source_video or str(source_video.get("cutStatus", source_video.get("cut_status", 0))) != "0":
                category.update(state="attention", errorCode="platform_video_changed",
                                message="平台视频已不再处于未切片状态，请重新扫描并核对。")
                continue
            intervals = category.get("intervals", [])
            if not intervals:
                category.update(state="no_target", message="未检出目标")
                continue
            suffix = "_D" if category_name == "magnet" else "_C"
            prefix = f"{Path(item['name']).stem[:58]}_{item['sourceVideoId'][-10:]}{suffix}"
            if category.get("submissionRevision"):
                prefix += f"_R{category['submissionRevision']}"
            category.update(message="正在核对本视频相关图片，建立提交前快照。")
            image_rows = await self.remote.image_rows(target_id, prefix=prefix,
                progress=self._image_progress(item, category, "核对已有图片"))
            baseline = [str(row["id"]) for row in image_rows]
            payload_identity = {"video": item["sourceVideoId"], "version": item.get("submissionFileVersion", item["fileVersion"]), "category": category_name,
                                "target": target_id, "intervals": intervals, "seconds": category.get("intervalSeconds"),
                                "model": category.get("modelFingerprint")}
            if category.get("submissionRevision"):
                payload_identity["revision"] = category["submissionRevision"]
            operation_id = hashlib.sha256(json.dumps(payload_identity, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
            existing = self.store.get("operations", operation_id)
            if existing and existing.get("state") in {"submitting", "waiting", "complete"}:
                category.update(state="waiting" if existing.get("taskId") else "attention", taskId=existing.get("taskId", ""),
                                operationId=operation_id, errorCode="submit_uncertain" if not existing.get("taskId") else "",
                                message=existing.get("message", ERROR_TEXT["submit_uncertain"]))
                continue
            operation = {"id": operation_id, "state": "submitting", "requestId": uuid4().hex,
                         "sourceDatasetId": source_id, "sourceVideoId": item["sourceVideoId"], "targetDatasetId": target_id,
                         "type": 21, "preSubmitTaskIds": sorted(str(job["id"]) for rows in jobs.values() for job in rows),
                         "preSubmitImageIds": baseline, "imageQueryPrefix": prefix,
                         "intervals": intervals, "intervalSeconds": category.get("intervalSeconds"),
                         "fileVersion": item["fileVersion"], "createdAt": now_iso()}
            self.store.save("operations", operation)
            category.update(state="submitting", operationId=operation_id, targetDatasetId=target_id,
                            preSubmitTaskIds=operation["preSubmitTaskIds"], preSubmitImageIds=baseline, prefix=prefix,
                            message="已保存提交前快照，正在提交唯一的21类手动切片任务。")
            self.store.save("items", item)
            try:
                result = await self.remote.create_cut(source_id, item["sourceVideoId"], target_id, intervals,
                    int(category["intervalSeconds"]), prefix, operation["requestId"])
            except AuditError as error:
                if error.code == "uncertain":
                    operation.update(state="submitting", message=ERROR_TEXT["submit_uncertain"])
                    category.update(state="submitting", errorCode="submit_uncertain", message=ERROR_TEXT["submit_uncertain"])
                else:
                    operation.update(state="attention", errorCode=error.code, message=str(error))
                    category.update(state="attention", errorCode=error.code, message=str(error))
                self.store.save("operations", operation)
                continue
            task_id = str(result["id"])
            operation.update(state="waiting", taskId=task_id, platformStatus=str(result.get("status", "")))
            category.update(state="waiting", taskId=task_id, platformStatus=str(result.get("status", "")),
                            message="平台已接收切片任务，等待完成后关联核验图片。")
            self.store.save("operations", operation)
            self.store.save("items", item)
            # Keep this tick's snapshot so another category of the same item cannot open a second task.
            jobs.setdefault(source_id, []).append({"id": task_id, "type": "21", "status": result.get("status", "1"),
                "config": {"target": {"dataset": {"id": target_id}}, "source": {"datasets": [{"videos": {"ids": [item["sourceVideoId"]]}}]}}})
        self._save_category_progress(item, outcome_baseline)

    async def frame(self, identity, timestamp):
        item = self.store.get("items", identity)
        if not item or not item.get("path"):
            raise ValueError("本地视频未绑定，无法生成关键画面。")
        duration = float((item.get("metadata") or {}).get("duration") or 0)
        if not 0 <= timestamp <= duration:
            raise ValueError("关键画面时间超出视频范围。")
        if source_version(item["path"]) != item.get("fileVersion"):
            raise Conflict("本地视频版本已变化，请重新绑定后查看。")
        ffmpeg = self.store.settings["frameToolPath"]
        with self._read_lease(item["path"]):
            process = await asyncio.create_subprocess_exec(ffmpeg, "-hide_banner", "-loglevel", "error", "-ss",
                f"{timestamp:.6f}", "-i", item["path"], "-frames:v", "1", "-f", "image2pipe", "-vcodec", "mjpeg", "pipe:1",
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL)
            output, _ = await process.communicate()
        if process.returncode or not output.startswith(b"\xff\xd8"):
            raise ValueError("未能读取这个位置的画面，原视频保留。")
        return output
