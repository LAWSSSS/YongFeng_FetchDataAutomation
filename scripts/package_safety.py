"""Allowlisted source export and exact portable-package verification."""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import stat
import subprocess

from packaging.requirements import Requirement
from packaging.utils import canonicalize_name, parse_wheel_filename

ROOT_FILES = {".gitignore", "README.md", "DESIGN.md", "pytest.ini", "index.html", "package.json",
              "package-lock.json", "tsconfig.json", "vite.config.ts", "packaging.lock.json",
              "requirements.txt", "requirements.lock.txt", "requirements-slicing.txt",
              "requirements-slicing.lock.txt", "启动简易版.command", "停止简易版.command"}
SOURCE_SUFFIXES = {
    "app": {".py"}, "src": {".ts", ".tsx", ".css"}, "tests": {".py", ".mjs"},
    "scripts": {".py", ".m", ".mjs", ".cjs", ".sh"}, "docs": {".md"},
    "assets": {".json", ".py"}, "public": {".svg", ".ico"},
}


def digest(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def relative_name(raw):
    if not isinstance(raw, str) or not raw or "\\" in raw or "\x00" in raw:
        raise ValueError("Unsafe package path.")
    path = PurePosixPath(raw)
    if path.is_absolute() or any(part in {"", ".", ".."} for part in raw.split("/")):
        raise ValueError("Unsafe package path.")
    return path


def source_inventory(root):
    root = Path(root).resolve()
    names = subprocess.check_output(["git", "ls-files", "--cached", "--others",
                                     "--exclude-standard", "-z"], cwd=root).decode().split("\0")
    entries = []
    for name in sorted(set(names) - {""}):
        path = relative_name(name)
        if (name not in ROOT_FILES and
                path.suffix not in SOURCE_SUFFIXES.get(path.parts[0], set())):
            continue
        local = root / name
        if local.is_symlink() or not local.resolve().is_relative_to(root):
            raise ValueError(f"Source symlink is not distributable: {name}")
        if not local.is_file():
            continue  # A deletion in the working tree is an intentional deletion.
        if any(part.startswith(".") or part in {"__pycache__", "node_modules"} for part in path.parts):
            if name != ".gitignore":
                continue
        entries.append({"path": name, "bytes": local.stat().st_size, "sha256": digest(local)})
    return entries


def inventory_digest(entries):
    return hashlib.sha256(json.dumps(entries, ensure_ascii=False, sort_keys=True,
                                    separators=(",", ":")).encode()).hexdigest()


def verify_requirement_wheels(requirements, entries):
    available = {}
    for entry in entries:
        name, version, _, _ = parse_wheel_filename(entry["wheel"])
        available.setdefault(canonicalize_name(name), set()).add(version)
    for line in Path(requirements).read_text().splitlines():
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        requirement = Requirement(line)
        if not any(version in requirement.specifier for version in available.get(canonicalize_name(requirement.name), set())):
            raise ValueError(f"Missing locked wheel: {requirement.name}")


def inventory_tree(root):
    root = Path(root).resolve()
    entries = []
    for path in sorted(root.rglob("*")):
        info = path.lstat()
        name = path.relative_to(root).as_posix()
        if stat.S_ISLNK(info.st_mode):
            target = os.readlink(path)
            if Path(target).is_absolute() or not path.resolve().is_relative_to(root) or not path.exists():
                raise ValueError(f"Nonportable symbolic link: {name}")
            entries.append({"path": name, "link": target})
        elif stat.S_ISREG(info.st_mode):
            entries.append({"path": name, "bytes": info.st_size, "sha256": digest(path),
                            "mode": stat.S_IMODE(info.st_mode)})
        elif not stat.S_ISDIR(info.st_mode):
            raise ValueError(f"Unsupported package file: {name}")
    return entries


def verify_tree(root, manifest):
    root = Path(root).resolve()
    entries = manifest.get("files")
    if not isinstance(entries, list) or not entries:
        raise ValueError("Package manifest is empty.")
    expected = {}
    for entry in entries:
        name = str(relative_name(entry.get("path")))
        if name in expected or name == "文件校验.json":
            raise ValueError("Duplicate package path.")
        expected[name] = entry
    actual = {entry["path"]: entry for entry in inventory_tree(root) if entry["path"] != "文件校验.json"}
    if set(actual) != set(expected):
        raise ValueError("Package contains missing or unexpected files.")
    for name, entry in expected.items():
        current = actual[name]
        fields = ("link",) if "link" in entry else ("bytes", "sha256", "mode")
        if any(entry.get(field) != current.get(field) for field in fields):
            raise ValueError(f"Package file changed: {name}")
