"""Private local configuration storage for optional platform auto-login."""

import json
import os
import tempfile
from pathlib import Path
from .runtime import credentials_directory


CONFIG_DIR = credentials_directory()
CONFIG_FILE = CONFIG_DIR / "credentials.json"
_SERVICES = {"source", "target"}


def _read_all() -> dict:
    try:
        with CONFIG_FILE.open("r", encoding="utf-8") as file:
            data = json.load(file)
        return data if isinstance(data, dict) else {}
    except FileNotFoundError:
        return {}
    except (OSError, json.JSONDecodeError):
        return {}


def load(service: str) -> dict[str, str] | None:
    if service not in _SERVICES:
        raise ValueError("未知的登录平台。")
    entry = _read_all().get(service)
    if not isinstance(entry, dict):
        return None
    account, password = entry.get("account"), entry.get("password")
    if not isinstance(account, str) or not account or not isinstance(password, str) or not password:
        return None
    return {"account": account, "password": password}


def save(service: str, account: str, password: str) -> None:
    if service not in _SERVICES:
        raise ValueError("未知的登录平台。")
    if not account.strip() or not password:
        raise ValueError("账号和密码不能为空。")

    CONFIG_DIR.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(CONFIG_DIR, 0o700)
    data = _read_all()
    data[service] = {"account": account.strip(), "password": password}

    fd, temporary = tempfile.mkstemp(prefix=".credentials-", suffix=".tmp", dir=CONFIG_DIR)
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as file:
            json.dump(data, file, ensure_ascii=False)
            file.flush()
            os.fsync(file.fileno())
        os.replace(temporary, CONFIG_FILE)
        os.chmod(CONFIG_FILE, 0o600)
    finally:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass
