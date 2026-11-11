import os
import subprocess
import sys

import pytest

from app import local_instance


def test_production_lock_excludes_another_backend_process(tmp_path, monkeypatch):
    monkeypatch.setenv("IVG_TEST_INSTANCE_DIR", str(tmp_path / "shared"))
    monkeypatch.setenv("IVG_SKIP_SAVED_CONNECTIONS", "1")
    # The second process uses another profile but the same user-level lock.
    env = {**os.environ, "IVG_PROFILE_DIR": str(tmp_path / "other-profile")}
    command = [sys.executable, "-c", "from app.local_instance import check_available; check_available()"]
    with local_instance.backend_instance():
        blocked = subprocess.run(command, env=env, capture_output=True, text=True, timeout=5)
        assert blocked.returncode != 0
        assert "另一份采集助手正在运行或启动" in blocked.stderr
    subprocess.run(command, env=env, check=True, timeout=5)


def test_legacy_listener_requires_matching_pid(monkeypatch):
    monkeypatch.setattr(local_instance, "_legacy_listeners", lambda: iter([(17, 6789), (24, 6790)]))
    monkeypatch.setattr(local_instance, "health_at", lambda port: {"pid": 24 if port == 6790 else 42})
    with pytest.raises(local_instance.InstanceBusy, match=r"127\.0\.0\.1:6790"):
        local_instance._check_legacy()


def test_profile_override_does_not_select_a_different_lock(tmp_path, monkeypatch):
    monkeypatch.delenv("IVG_TEST_INSTANCE_DIR", raising=False)
    original = local_instance.lock_directory()
    monkeypatch.setenv("IVG_PROFILE_DIR", str(tmp_path / "different-install"))
    assert local_instance.lock_directory() == original
