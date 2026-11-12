"""Release allowlists and credential scanning must fail closed."""
import hashlib
import importlib.util
import json
from pathlib import Path

from scripts.scan_secrets import issues

ROOT = Path(__file__).resolve().parents[1]


def test_scanner_detects_generated_credentials_without_printing_them():
    value = ("ghp_" + "a" * 40).encode()
    assert issues(value) == ["github-token"]
    assert issues(value, documentation=True) == ["github-token"]
    example = b"https://" + b"example-user:" + b"example-password@host"
    assert issues(example) == ["embedded-url-password"]
    assert issues(example, documentation=True) == []


def test_model_helper_sources_are_exact_originals():
    manifest = json.loads((ROOT / "assets/models/manifest.json").read_text())
    assert len(manifest["files"]) == 7
    for entry in manifest["files"]:
        if entry["path"].endswith(".py"):
            path = ROOT / "assets/models" / entry["path"]
            assert hashlib.sha256(path.read_bytes()).hexdigest() == entry["sha256"]


def test_builder_rejects_a_venv_before_copying(tmp_path):
    spec = importlib.util.spec_from_file_location("mac_builder", ROOT / "scripts/build_mac.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    source = tmp_path / "not-portable"
    source.mkdir()
    (source / "pyvenv.cfg").write_text("home = /some/system/python")
    import pytest
    with pytest.raises(ValueError, match="never a virtualenv"):
        module.make_runtime(source, tmp_path / "output", tmp_path / "wheels")
    assert not (tmp_path / "output").exists()
