"""Application-relative tools and a single writable sibling data directory."""
from __future__ import annotations

import os
from pathlib import Path
import shutil

VERSION = "1.3.15"
BUNDLE_ID = "cn.yongfeng.video-gather.standalone"
PROFILE_NAME = "InspectionVideoGatherStandalone"
PORTABLE_PYTHON = Path("runtime/bin/python3.11")


def portable() -> bool:
    return os.environ.get("IVG_PORTABLE") == "1"


def resources() -> Path:
    return Path(os.environ["IVG_RESOURCES"]).resolve()


def profile_directory() -> Path:
    return Path(os.environ.get("IVG_PROFILE_DIR") or data_directory() / "系统").expanduser().resolve()


def portable_package_directory(resource_directory: Path) -> Path:
    if resource_directory.name == "运行组件":
        return resource_directory.parent
    # Older packages put the runtime inside an application bundle.
    folder = resource_directory.parents[2]
    return folder.parent if folder.name == "运行组件" else folder


def application_directory() -> Path:
    return portable_package_directory(resources()) if portable() else Path(__file__).resolve().parents[1]


def data_directory() -> Path:
    override = os.environ.get("IVG_PROFILE_DIR")
    if override:
        return Path(override).expanduser().resolve().parent / "数据" if Path(override).name != "系统" else Path(override).parent
    return application_directory() / "数据"


def credentials_directory() -> Path:
    if portable():
        return profile_directory() / "credentials"
    return Path.home() / "Library" / "Application Support" / "InspectionVideoGatherSimple"


def keychain_service() -> bytes:
    if portable():
        return f'{BUNDLE_ID}.{os.environ.get("IVG_PROFILE_ID", "standalone")}'.encode()
    return b"com.codex.inspection-video-gather-simple"


def slicing_paths() -> dict:
    if not portable():
        return {}
    root = resources()
    return {"directories": [],
            "magnetDeployDirectory": str(root / "models" / "magnet"),
            "magnetModelPath": str(root / "models" / "magnet" / "best.onnx"),
            "bedDeployDirectory": str(root / "models" / "bed"),
            "bedModelPath": str(root / "models" / "bed" / "model.pt"),
            "bedGeometryPath": str(root / "models" / "bed" / "geometry.npz"),
            "frameToolPath": str(root / "tools" / "bin" / "ffmpeg"),
            "workerPythonPath": str(root / PORTABLE_PYTHON)}


def source_model_paths() -> dict:
    """Resolve source-bundle models relative to code, never a developer's disk."""
    root = Path(__file__).resolve().parents[1]
    models = root / "assets" / "models"
    if not (models / "magnet" / "best.onnx").is_file() and (root / "build/models/magnet/best.onnx").is_file():
        models = root / "build" / "models"
    frame_tool = root / "build/ffmpeg-tools/bin/ffmpeg"
    # Source downloads can use installed media tools without compiling FFmpeg.
    # Keep both executables in the same directory for the inference worker.
    for candidate in (frame_tool, shutil.which("ffmpeg"), "/opt/homebrew/bin/ffmpeg", "/usr/local/bin/ffmpeg"):
        if candidate and all(path.is_file() and os.access(path, os.X_OK)
                             for path in (Path(candidate), Path(candidate).with_name("ffprobe"))):
            frame_tool = Path(candidate)
            break
    return {"magnetDeployDirectory": str(models / "magnet"),
            "magnetModelPath": str(models / "magnet/best.onnx"),
            "bedDeployDirectory": str(models / "bed"),
            "bedModelPath": str(models / "bed/model.pt"),
            "bedGeometryPath": str(models / "bed/geometry.npz"),
            "frameToolPath": str(frame_tool),
            "workerPythonPath": str(root / ".venv-slicing/bin/python")}


def identity() -> dict:
    return {"version": VERSION, "bundleId": BUNDLE_ID if portable() else "development",
            "profileId": os.environ.get("IVG_PROFILE_ID", "development"),
            "instanceId": os.environ.get("IVG_INSTANCE_ID", "development"),
            "packageId": os.environ.get("IVG_PACKAGE_ID", "development"), "pid": os.getpid()}


def refresh_bundled_paths(settings: dict) -> bool:
    """Repair only the distribution's own paths after a move/update, never custom paths."""
    if not portable():
        return False
    desired = slicing_paths()
    previous = settings.get("bundledResources")
    changed = False
    for key, value in desired.items():
        if key == "directories":
            continue
        old_defaults = [None] if not previous else []
        if previous:
            relative = Path(value).relative_to(resources())
            old_defaults.append(str(Path(previous) / relative))
            if key == "workerPythonPath":
                old_defaults.append(str(Path(previous) / "runtime/inference/bin/python3.11"))
        if key not in settings or settings[key] in old_defaults:
            if settings.get(key) != value:
                settings[key] = value
                changed = True
    if settings.get("bundledResources") != str(resources()):
        settings["bundledResources"] = str(resources())
        changed = True
    return changed
