"""Paths and identity for the relocatable distribution; development stays unchanged."""
from __future__ import annotations

import os
from pathlib import Path

VERSION = "1.1.0"
BUNDLE_ID = "cn.yongfeng.video-gather.standalone"
PROFILE_NAME = "InspectionVideoGatherStandalone"


def portable() -> bool:
    return os.environ.get("IVG_PORTABLE") == "1"


def resources() -> Path:
    return Path(os.environ["IVG_RESOURCES"]).resolve()


def profile_directory() -> Path:
    return Path(os.environ.get("IVG_PROFILE_DIR") or
                Path.home() / "Library" / "Application Support" / PROFILE_NAME).expanduser().resolve()


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
            "workerPythonPath": str(root / "runtime" / "inference" / "bin" / "python3.11")}


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
        old_default = None
        if previous:
            relative = Path(value).relative_to(resources())
            old_default = str(Path(previous) / relative)
        if key not in settings or settings[key] == old_default:
            if settings.get(key) != value:
                settings[key] = value
                changed = True
    if settings.get("bundledResources") != str(resources()):
        settings["bundledResources"] = str(resources())
        changed = True
    return changed
