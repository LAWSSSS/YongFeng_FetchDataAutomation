import importlib.util
import json
from pathlib import Path

import pytest

from scripts.package_safety import inventory_tree, verify_tree, relative_name, source_inventory

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("release_launcher", ROOT / "scripts/portable_launcher.py")
launcher = importlib.util.module_from_spec(spec)
spec.loader.exec_module(launcher)


def test_package_verification_rejects_extra_files_changed_links_and_permissions(tmp_path):
    target = tmp_path / "runtime/python"
    target.parent.mkdir()
    target.write_bytes(b"runtime")
    target.chmod(0o755)
    alias = target.parent / "python3"
    alias.symlink_to("python")
    manifest = {"files": inventory_tree(tmp_path)}
    (tmp_path / "文件校验.json").write_text(json.dumps(manifest))
    verify_tree(tmp_path, manifest)
    private = tmp_path / "credentials.json"
    private.write_text("{}")
    with pytest.raises(ValueError, match="unexpected"):
        verify_tree(tmp_path, manifest)
    private.unlink()
    target.chmod(0o644)
    with pytest.raises(ValueError, match="changed"):
        verify_tree(tmp_path, manifest)
    target.chmod(0o755)
    alias.unlink()
    alias.symlink_to(target.resolve())
    with pytest.raises(ValueError, match="Nonportable"):
        verify_tree(tmp_path, manifest)


@pytest.mark.parametrize("name", ["../secret", "/etc/passwd", "a/../../secret", "a\\secret", "a//b", "./file"])
def test_package_paths_cannot_escape(name):
    with pytest.raises(ValueError):
        relative_name(name)


def test_package_verification_rejects_duplicate_manifest_entries(tmp_path):
    (tmp_path / "file").write_bytes(b"data")
    entries = inventory_tree(tmp_path)
    with pytest.raises(ValueError, match="Duplicate"):
        verify_tree(tmp_path, {"files": entries + entries})


def test_malformed_launcher_json_is_handled(tmp_path):
    file = tmp_path / "server.json"
    for content in ("[]", '"invalid"', "null", "{"):
        file.write_text(content)
        assert launcher.read_json(file) == {}


def setup_launcher(monkeypatch, tmp_path):
    monkeypatch.setattr(launcher, "release_manifest", lambda: {"bundleId": launcher.BUNDLE_ID, "packageId": "new-package"})
    monkeypatch.setattr(launcher, "prepare_profile", lambda: (tmp_path, "profile"))
    monkeypatch.setattr(launcher, "verified_server", lambda *args: False)
    monkeypatch.setattr(launcher, "serve", lambda *a, **k: pytest.fail("must not start another writer"))
    monkeypatch.setattr(launcher.os, "kill", lambda *a: pytest.fail("must not signal an unverified process"))


def test_busy_instance_is_not_mistaken_for_absent_server(monkeypatch, tmp_path):
    setup_launcher(monkeypatch, tmp_path)
    with launcher.lock(tmp_path / "server.lock"):
        with pytest.raises(RuntimeError, match="正在启动或安全停止"):
            launcher.launch(no_browser=True)
        result = launcher.stop()
        assert not result["stopped"]
        assert "无法核实" in result["message"]


def test_old_package_stop_checks_package_identity(monkeypatch, tmp_path):
    setup_launcher(monkeypatch, tmp_path)
    calls = []
    monkeypatch.setattr(launcher, "verified_server", lambda *args: calls.append(args) or False)
    assert not launcher.stop()["stopped"]
    assert calls[0][-1] == "new-package"


def test_launcher_removes_inherited_package_identity(monkeypatch, tmp_path):
    monkeypatch.setenv("IVG_UNKNOWN", "do-not-inherit")
    env = launcher.environment(tmp_path, {"packageId": "new-package"}, "profile", "instance")
    assert "IVG_UNKNOWN" not in env
    assert env["IVG_PACKAGE_ID"] == "new-package"


def test_all_release_versions_agree():
    from app.runtime import VERSION
    for name in ("package.json", "package-lock.json", "packaging.lock.json"):
        assert json.loads((ROOT / name).read_text())["version"] == VERSION


def test_every_runtime_requirement_has_a_locked_wheel():
    from packaging.utils import parse_wheel_filename
    from scripts.package_safety import verify_requirement_wheels
    entries = json.loads((ROOT / "assets/wheels.lock.json").read_text())["files"]
    names = [parse_wheel_filename(entry["wheel"])[0] for entry in entries]
    assert len(set(names)) == len(names)
    for filename in ("requirements.lock.txt", "requirements-slicing.lock.txt"):
        verify_requirement_wheels(ROOT / filename, entries)


def test_missing_or_wrong_version_wheel_fails_before_build(tmp_path):
    from scripts.package_safety import verify_requirement_wheels
    requirements = tmp_path / "requirements.txt"
    requirements.write_text("Pillow==13.0.0\n")
    for entries in ([], [{"wheel": "pillow-12.0.0-cp313-cp313-macosx_11_0_arm64.whl"}]):
        with pytest.raises(ValueError, match="Missing locked wheel: Pillow"):
            verify_requirement_wheels(requirements, entries)


def test_source_model_defaults_use_resources_relative_to_code():
    from app import runtime
    defaults = runtime.source_model_paths()
    for key in ("magnetModelPath", "bedModelPath", "bedGeometryPath", "workerPythonPath"):
        assert Path(defaults[key]).is_relative_to(ROOT)


def test_stale_frontend_build_fails_before_packaging(tmp_path, monkeypatch):
    import os
    from scripts import build_mac
    index = tmp_path / "web/index.html"
    source = tmp_path / "src/main.tsx"
    for path in (index, source):
        path.parent.mkdir()
        path.write_text("fixture")
    monkeypatch.setattr(build_mac, "ROOT", tmp_path)
    os.utime(index, (1, 1))
    with pytest.raises(ValueError, match="older than the source"):
        build_mac.verify_web_build(index.parent, [{"path": "src/main.tsx"}])


def test_mixed_source_and_incomplete_release_are_not_exported(tmp_path, monkeypatch):
    from scripts import release_assets
    folder = tmp_path / "package"
    resources = folder / release_assets.PORTABLE_RESOURCES
    resources.mkdir(parents=True)
    (resources / "release.json").write_text('{"sourceTreeSha256":"different"}')
    manifest = {"version": release_assets.VERSION, "files": inventory_tree(folder)}
    (folder / "文件校验.json").write_text(json.dumps(manifest))
    output = tmp_path / "release"
    with pytest.raises(ValueError, match="different source files"):
        release_assets.export(folder, output)
    assert not output.exists()

    def fail_after_writing(_folder, stage):
        (stage / "incomplete.zip").touch()
        raise ValueError("asset could not be prepared")

    monkeypatch.setattr(release_assets, "build_assets", fail_after_writing)
    with pytest.raises(ValueError, match="asset could not be prepared"):
        release_assets.export(folder, output)
    assert set(tmp_path.iterdir()) == {folder}


def test_source_export_includes_new_modules_but_excludes_runtime_data(tmp_path, monkeypatch):
    for name in ("app/main.py", "app/new_module.py", "pytest.ini", "app/credentials.json", "src/.env",
                 "data/collection.sqlite3", "tests/result.mp4", "build/output.txt"):
        file = tmp_path / name
        file.parent.mkdir(parents=True, exist_ok=True)
        file.write_text("fixture")
    names = [str(p.relative_to(tmp_path)) for p in tmp_path.rglob("*") if p.is_file()]
    monkeypatch.setattr("scripts.package_safety.subprocess.check_output", lambda *a, **k: "\0".join(names).encode())
    assert [entry["path"] for entry in source_inventory(tmp_path)] == ["app/main.py", "app/new_module.py", "pytest.ini"]


def test_bundled_source_zip_is_scanned_without_extracting(tmp_path):
    import zipfile
    from scripts.scan_secrets import archive_issues
    path = tmp_path / "sources.zip"
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("src/credentials.json", '{}')
        archive.writestr("src/config.py", 'key = "' + 'ghp_' + 'a' * 40 + '"')
    assert set(archive_issues(path)) == {("src/credentials.json", "private-runtime-file"),
                                        ("src/config.py", "github-token")}
    assert not (tmp_path / "src").exists()


def test_archive_only_exempts_exact_vendor_documentation(tmp_path):
    import hashlib
    import zipfile
    from scripts.scan_secrets import archive_issues
    package = tmp_path / "package.zip"
    vendor = "lib/python3.11/site-packages/vendor/docs.py"
    example = b"https://user:" + b"example-only@example.invalid"
    known = {vendor: hashlib.sha256(example).hexdigest()}
    with zipfile.ZipFile(package, "w") as archive:
        archive.writestr("bundle/runtime/" + vendor, example)
        archive.writestr("bundle/app/docs.py", example)
    assert archive_issues(package, known) == [("bundle/app/docs.py", "embedded-url-password")]
    assert len(archive_issues(package, {vendor: "changed"})) == 2


def test_model_loader_rejects_duplicate_archive_entries(tmp_path, monkeypatch):
    import hashlib
    import sys
    import zipfile
    from scripts import fetch_models
    manifest = tmp_path / "assets/models/manifest.json"
    manifest.parent.mkdir(parents=True)
    manifest.write_text(json.dumps({"files": [{"path": "model.pt", "bytes": 1,
        "sha256": hashlib.sha256(b"x").hexdigest()}]}))
    archive = tmp_path / "models.zip"
    with zipfile.ZipFile(archive, "w") as bundle:
        bundle.writestr("model.pt", b"x")
        with pytest.warns(UserWarning, match="Duplicate"):
            bundle.writestr("model.pt", b"x")
    monkeypatch.setattr(fetch_models, "ROOT", tmp_path)
    monkeypatch.setattr(sys, "argv", ["fetch_models", "--archive", str(archive), "--destination", str(tmp_path / "output")])
    with pytest.raises(ValueError, match="Unexpected"):
        fetch_models.main()
    assert not (tmp_path / "output").exists()
