import json
import os

from app import credentials


def test_credentials_round_trip_with_private_local_config(monkeypatch, tmp_path):
    config_dir = tmp_path / "config"
    config_file = config_dir / "credentials.json"
    monkeypatch.setattr(credentials, "CONFIG_DIR", config_dir)
    monkeypatch.setattr(credentials, "CONFIG_FILE", config_file)

    credentials.save("target", "worker", "example-secret")

    assert credentials.load("target") == {"account": "worker", "password": "example-secret"}
    assert json.loads(config_file.read_text(encoding="utf-8"))["target"] == {
        "account": "worker", "password": "example-secret"
    }
    assert os.stat(config_dir).st_mode & 0o777 == 0o700
    assert os.stat(config_file).st_mode & 0o777 == 0o600


def test_credentials_reject_invalid_saved_payload(monkeypatch, tmp_path):
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    config_file = config_dir / "credentials.json"
    config_file.write_text("not-json", encoding="utf-8")
    monkeypatch.setattr(credentials, "CONFIG_DIR", config_dir)
    monkeypatch.setattr(credentials, "CONFIG_FILE", config_file)

    assert credentials.load("target") is None


def test_credentials_only_allow_known_services():
    import pytest

    with pytest.raises(ValueError):
        credentials.load("unknown")
