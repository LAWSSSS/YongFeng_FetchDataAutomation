"""Portable profile and launcher checks; never touch the running user's profile."""
import importlib.util
import json
import os
from pathlib import Path

import httpx
import pytest

from app import runtime
from app.main import create_app
from app.slicing import SlicingStore

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("portable_launcher", ROOT / "scripts/portable_launcher.py")
launcher = importlib.util.module_from_spec(spec)
spec.loader.exec_module(launcher)


@pytest.fixture
def portable(monkeypatch, tmp_path):
    monkeypatch.setenv("IVG_PORTABLE", "1")
    monkeypatch.setenv("IVG_RESOURCES", str(tmp_path / "中文 application"))
    monkeypatch.setenv("IVG_PROFILE_DIR", str(tmp_path / "independent-profile"))
    monkeypatch.setenv("IVG_PROFILE_ID", "isolated-profile")
    monkeypatch.setenv("IVG_INSTANCE_ID", "isolated-instance")
    monkeypatch.setenv("IVG_PACKAGE_ID", "isolated-package")
    return tmp_path


def test_profile_and_keychain_are_independent(portable):
    assert runtime.profile_directory() == portable / "independent-profile"
    assert runtime.credentials_directory() == portable / "independent-profile/credentials"
    assert runtime.keychain_service() != b"com.codex.inspection-video-gather-simple"
    assert runtime.keychain_service() == (runtime.BUNDLE_ID + ".isolated-profile").encode()


def test_fresh_paths_are_bundled_and_no_external_disk_is_selected(portable):
    defaults = SlicingStore.defaults()
    assert defaults["directories"] == []
    assert defaults["magnetConfidence"] == 0.25
    for key in ("magnetModelPath", "bedModelPath", "bedGeometryPath", "frameToolPath", "workerPythonPath"):
        assert Path(defaults[key]).is_relative_to(runtime.resources())


def test_move_repairs_bundled_paths_but_preserves_custom_paths(portable, monkeypatch):
    settings = SlicingStore.defaults()
    old = settings["bundledResources"]
    settings["bedModelPath"] = str(portable / "custom.pt")
    monkeypatch.setenv("IVG_RESOURCES", str(portable / "moved renamed 中文"))
    assert runtime.refresh_bundled_paths(settings)
    assert settings["bedModelPath"] == str(portable / "custom.pt")
    assert settings["magnetModelPath"] == str(runtime.resources() / "models/magnet/best.onnx")
    assert old != settings["bundledResources"]
    assert not runtime.refresh_bundled_paths(settings)


@pytest.mark.asyncio
async def test_health_identifies_only_own_instance_and_fresh_data(portable):
    # Explicit test data disables all saved-session restore and platform access.
    app = create_app(data_dir=portable / "test-data")
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1") as client:
        data = (await client.get("/api/health")).json()
        assert data["ok"]
        assert data["profileId"] == "isolated-profile"
        assert data["instanceId"] == "isolated-instance"
        assert data["packageId"] == "isolated-package"
        assert data["pid"] == os.getpid()
        videos = (await client.get("/api/videos")).json()
        assert not videos["videos"]
        assert not videos["directory"]
    app.state.store.close()


def test_launcher_never_reuses_another_program_even_on_same_port(monkeypatch):
    monkeypatch.setattr(launcher.urllib.request, "build_opener", lambda *_: pytest.fail("must reject before network"))
    assert not launcher.verified_server({"pid": 8090, "port": 8786}, "isolated-profile")
    assert not launcher.verified_server({"bundleId": runtime.BUNDLE_ID, "profileId": "other",
        "instanceId": "x", "pid": 8090, "port": 8786}, "isolated-profile")


def test_launcher_cleans_python_injection_and_uses_only_bundled_tools(portable, monkeypatch):
    monkeypatch.setenv("PYTHONPATH", "/untrusted")
    monkeypatch.setenv("DYLD_INSERT_LIBRARIES", "/untrusted.dylib")
    env = launcher.environment(portable, {"packageId": "package"}, "profile", "instance")
    assert "PYTHONPATH" not in env
    assert "DYLD_INSERT_LIBRARIES" not in env
    assert "/homebrew" not in env["PATH"]
    assert env["IVG_PROFILE_DIR"] == str(portable)


def test_profile_id_survives_restart_without_reading_legacy_data(portable):
    home, first = launcher.prepare_profile()
    assert home == runtime.profile_directory()
    assert launcher.prepare_profile()[1] == first
    assert json.loads((home / "profile.json").read_text())["bundleId"] == runtime.BUNDLE_ID
    assert (home / "profile.json").stat().st_mode & 0o077 == 0
