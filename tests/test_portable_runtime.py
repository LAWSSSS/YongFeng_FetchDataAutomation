"""Portable profile and launcher checks; never touch the running user's profile."""
import importlib.util
import json
import os
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest

from app import runtime
from app.main import create_app
from app.models import SlicingSettingsInput
from app.slicing import SlicingStore
from tests.test_slicing_performance import close_app

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


@pytest.mark.asyncio
async def test_fresh_portable_workspace_keeps_derived_directories_and_can_save_settings(portable):
    app = create_app()
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://127.0.0.1') as client:
            settings = (await client.get('/api/slicing?summary=true')).json()['settings']
            media = runtime.data_directory() / '媒体'
            assert settings['directories'] and all(Path(path).is_relative_to(media) for path in settings['directories'])
            data = {key: value for key, value in settings.items() if key in SlicingSettingsInput.model_fields}
            response = await client.post('/api/slicing/settings', json={**data,
                'expectedRevision': settings['revision'], 'requestId': 'fresh-portable-settings'})
            assert response.status_code == 200, response.text
    finally:
        await close_app(app)


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


def test_source_launcher_skips_other_checkout_and_finds_its_own_nondefault_port(tmp_path, monkeypatch, capsys):
    from scripts import local_service

    data = tmp_path / "数据" / "系统"
    data.mkdir(parents=True)
    monkeypatch.setattr(local_service, "ROOT", tmp_path)
    monkeypatch.setattr(local_service, "DATA", data)
    owned = set()
    monkeypatch.setattr(local_service, "can_bind", lambda port: port == 8787)

    def launchctl(argv, **_):
        if argv[1] == "bootstrap":
            config = local_service.plistlib.loads(Path(argv[-1]).read_bytes())
            assert config["ProgramArguments"][0] == str(tmp_path / ".venv/bin/python")
            owned.add(config["Label"])
            return SimpleNamespace(returncode=0)
        label = argv[-1].rsplit("/", 1)[-1]
        return SimpleNamespace(returncode=int(label not in owned), stdout="")

    monkeypatch.setattr(local_service.subprocess, "run", launchctl)
    local_service.run("start", 8786, False)
    endpoint = json.loads(capsys.readouterr().out)
    assert endpoint["port"] == 8787
    assert endpoint["instanceId"] == local_service.service_label(8787)
    local_service.run("start", 8786, False)
    assert json.loads(capsys.readouterr().out) == endpoint
    local_service.run("status", 8786, False)
    assert "127.0.0.1:8787/" in capsys.readouterr().out
    assert len(owned) == 1


def test_source_uses_system_media_tools_without_building_them(tmp_path, monkeypatch):
    monkeypatch.setattr(runtime, "__file__", str(tmp_path / "app/runtime.py"))
    tool_dir = tmp_path / "program tools"
    tool_dir.mkdir()
    for tool in ("ffmpeg", "ffprobe"):
        path = tool_dir / tool
        path.write_text("#!/bin/sh\nexit 0\n")
        path.chmod(0o700)
    monkeypatch.setenv("PATH", str(tool_dir))
    assert runtime.source_model_paths()["frameToolPath"] == str(tool_dir / "ffmpeg")
