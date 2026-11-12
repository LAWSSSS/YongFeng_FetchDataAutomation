"""Assemble an allowlisted, self-contained macOS arm64 command package.

The base interpreters must be python-build-standalone install_only distributions,
not virtual environments. Dependencies are installed anew from locked wheels.
"""
from __future__ import annotations

import argparse
import ctypes
import hashlib
import json
import os
import shutil
import subprocess
import sys
import zipfile
from pathlib import Path
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from app.runtime import BUNDLE_ID, VERSION
from scripts.fetch_models import verify as verify_models
from scripts.macos_signing import find_identity, is_macho, native_files, sign_native
from scripts.package_safety import (
    PORTABLE_COMMAND, PORTABLE_RESOURCES, inventory_digest, inventory_tree, relative_name,
    source_inventory, verify_requirement_wheels,
)
from scripts.scan_secrets import issues

def run(*args, **kwargs):
    subprocess.run([str(value) for value in args], check=True, **kwargs)


def digest(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def make_runtime(source, destination, version, wheels, requirements):
    if (source / "pyvenv.cfg").exists() or not (source / "BUILD").is_file():
        raise ValueError("Use an upstream python-build-standalone distribution, never a virtualenv.")
    if not (source / "bin" / f"python{version}").is_file():
        raise ValueError("Wrong Python distribution version.")
    lock = json.loads((ROOT / "packaging.lock.json").read_text())["python"]
    if (source / "BUILD").read_text().strip() != lock["build"]:
        raise ValueError("Unexpected upstream standalone Python build.")
    expected_python = lock["backend" if version == "3.13" else "inference"]
    if subprocess.check_output([str(source / "bin" / f"python{version}"), "-I", "--version"], text=True).strip() != "Python " + expected_python:
        raise ValueError("Unexpected standalone Python patch version.")
    wheel_lock = json.loads((ROOT / "assets/wheels.lock.json").read_text())["files"]
    entries = [entry for entry in wheel_lock if entry["environment"] == wheels.name]
    verify_requirement_wheels(requirements, entries)
    if {entry["wheel"] for entry in entries} != {path.name for path in wheels.glob("*.whl")}:
        raise ValueError("Wheel directory does not match the locked allowlist.")
    for entry in entries:
        if digest(wheels / entry["wheel"]) != entry["sha256"]:
            raise ValueError(f'Wheel checksum mismatch: {entry["wheel"]}')
    shutil.copytree(source, destination, symlinks=True, ignore=shutil.ignore_patterns(
        "__pycache__", "*.pyc", "site-packages", "EXTERNALLY-MANAGED"))
    for path in destination.rglob("*"):
        if path.is_symlink() and Path(os.readlink(path)).is_absolute():
            relative = Path(os.readlink(path)).relative_to(source)
            path.unlink()
            path.symlink_to(os.path.relpath(destination / relative, path.parent))
    executable = destination / "bin" / f"python{version}"
    env = {key: value for key, value in os.environ.items()
           if not key.startswith(("PYTHON", "DYLD_", "LD_"))}
    run(executable, "-I", "-B", "-m", "ensurepip", "--default-pip", env=env)
    run(executable, "-I", "-B", "-m", "pip", "install", "--no-index", "--find-links", wheels,
        "--no-compile", "--disable-pip-version-check", "-r", requirements, env=env)
    run(executable, "-I", "-m", "pip", "check", env=env)
    # pip console scripts embed build paths. Runtime workers use python -m only;
    # replace optional console entries with relative shell wrappers.
    for path in (destination / "bin").iterdir():
        if path.is_file() and not path.is_symlink() and not is_macho(path):
            data = path.read_bytes()
            if data.startswith(b"#!"):
                if path.name.startswith("pip"):
                    path.write_text('#!/bin/sh\nexec "$(dirname "$0")/python' + version + '" -I -m pip "$@"\n')
                else:
                    path.unlink()
    for path in sorted(destination.rglob("__pycache__"), reverse=True):
        shutil.rmtree(path)
    for path in destination.rglob("direct_url.json"):
        path.unlink()
    for path in destination.rglob("*"):
        if path.is_symlink() and not path.exists():
            path.unlink()
    # Botocore's documentation examples include public dummy keys/certificates;
    # they are not used by the client and are omitted from a credential-free app.
    for path in destination.rglob("examples-1.json"):
        if "botocore" in path.parts:
            path.unlink()
    return {"python": subprocess.check_output([str(executable), "-I", "--version"], text=True).strip(),
            "provider": "astral-sh/python-build-standalone", "build": (source / "BUILD").read_text().strip()}


def collect_licenses(resources, wheels):
    destination = resources / "licenses"
    destination.mkdir()
    inventory = []
    for group in ("backend", "inference"):
        for wheel in sorted((wheels / group).glob("*.whl")):
            inventory.append({"environment": group, "wheel": wheel.name, "sha256": digest(wheel)})
            with zipfile.ZipFile(wheel) as archive:
                for name in archive.namelist():
                    upper = Path(name).name.upper()
                    if name.endswith("/") or not any(token in upper for token in ("LICENSE", "COPYING", "NOTICE")):
                        continue
                    if ".." in Path(name).parts:
                        raise ValueError("Unsafe wheel license path.")
                    target = destination / group / wheel.stem / name
                    target.parent.mkdir(parents=True, exist_ok=True)
                    target.write_bytes(archive.read(name))
    for group, version in (("backend", "3.13"), ("inference", "3.11")):
        shutil.copy2(resources / "runtime" / group / "lib" / f"python{version}" / "LICENSE.txt",
                     destination / f"Python-{version}-LICENSE.txt")
    shutil.copy2(ROOT / "build/ffmpeg-tools/FFmpeg-LICENSE.txt", destination / "FFmpeg-LICENSE.txt")
    (resources / "dependencies.json").write_text(json.dumps(inventory, indent=2) + "\n")


def audit_binaries(resources, identity=None):
    # Strip resource-fork/Finder metadata from newly assembled copies only.
    # Never clear quarantine on a user's downloaded application or alter Gatekeeper.
    libc = ctypes.CDLL("/usr/lib/libSystem.B.dylib")
    libc.removexattr.argtypes = [ctypes.c_char_p, ctypes.c_char_p, ctypes.c_int]
    for path in (resources, *resources.rglob("*")):
        if path.is_symlink():
            continue
        for name in ("com.apple.FinderInfo", "com.apple.ResourceFork"):
            libc.removexattr(os.fsencode(path), name.encode(), 0)
    binaries = native_files(resources)
    for path in binaries:
        if path.suffix == ".dylib":
            identifiers = subprocess.check_output(["/usr/bin/otool", "-arch", "arm64", "-D", str(path)], text=True).splitlines()[1:]
            if any(identifier.strip().startswith("/") for identifier in identifiers):
                run("/usr/bin/install_name_tool", "-id", f"@rpath/{path.name}", path,
                    stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        links = subprocess.check_output(["/usr/bin/otool", "-arch", "arm64", "-L", str(path)], text=True)
        for line in links.splitlines()[1:]:
            link = line.strip().split(" (", 1)[0]
            if link.startswith("/") and not link.startswith(("/usr/lib/", "/System/Library/")):
                # Dylib self identifiers may be absolute in upstream Python wheels;
                # require them to be relocatable too.
                raise ValueError(f"Nonportable binary dependency: {path.relative_to(resources)}")
        commands = subprocess.check_output(["/usr/bin/otool", "-arch", "arm64", "-l", str(path)], text=True)
        lines = commands.splitlines()
        for index, line in enumerate(lines):
            if line.strip().startswith("minos ") or (index and "LC_VERSION_MIN_MACOSX" in lines[index-1] and line.strip().startswith("version ")):
                version = line.split()[-1]
                if tuple(map(int, version.split(".")[:2])) > (14, 0):
                    raise ValueError(f"Requires macOS newer than 14: {path.relative_to(resources)}")
        sign_native(path, identity)
    return len(binaries)


def assemble(args, identity=None):
    sources = source_inventory(ROOT)
    for entry in sources:
        if issues((ROOT / entry["path"]).read_bytes()):
            raise ValueError(f'Credential scan failed: {entry["path"]}')
    if not (args.web / "index.html").is_file():
        raise ValueError("A fresh frontend build is required.")
    inventory_tree(args.web)  # Reject links escaping the fresh web build.
    output = args.output.resolve()
    if output.exists():
        raise ValueError("Output already exists. Choose a fresh staging directory.")
    output.mkdir(parents=True)
    resources = output / PORTABLE_RESOURCES
    application = resources / "application"
    application.mkdir(parents=True)
    # Only source modules, fresh web build and explicit resource lists enter the app.
    for entry in sources:
        if not entry["path"].startswith("app/"):
            continue
        target = application / entry["path"]
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(ROOT / entry["path"], target)
    shutil.copytree(args.web, application / "dist")
    shutil.copy2(ROOT / "scripts/portable_launcher.py", application / "launcher.py")
    verify_models(args.models)
    manifest = json.loads((ROOT / "assets/models/manifest.json").read_text())
    for entry in manifest["files"]:
        relative_name(entry["path"])
        target = resources / "models" / entry["path"]
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(args.models / entry["path"], target)
    shutil.copy2(ROOT / "assets/models/manifest.json", resources / "models/manifest.json")
    (resources / "runtime").mkdir()
    runtimes = {}
    for group, version, source, lock_file in (("backend", "3.13", args.backend, "requirements.lock.txt"),
                                            ("inference", "3.11", args.inference, "requirements-slicing.lock.txt")):
        runtimes[group] = make_runtime(source.resolve(), resources / "runtime" / group,
                                      version, args.wheels / group, ROOT / lock_file)
        shutil.copy2(ROOT / lock_file, resources / lock_file)
    tools = resources / "tools/bin"
    tools.mkdir(parents=True)
    for name in ("ffmpeg", "ffprobe"):
        shutil.copy2(ROOT / "build/ffmpeg-tools/bin" / name, tools / name)
    collect_licenses(resources, args.wheels)
    web_licenses = resources / "licenses/frontend"
    web_licenses.mkdir()
    for name in ("react", "react-dom", "scheduler", "@phosphor-icons/react"):
        shutil.copy2(ROOT / "node_modules" / name / "LICENSE", web_licenses / (name.replace("/", "-") + ".txt"))
    package = {"bundleId": BUNDLE_ID, "version": VERSION, "packageId": uuid4().hex,
               "platform": "macos-arm64", "minimumMacOS": "14.0", "runtimes": runtimes,
               "sourceCommit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
               "sourceDirty": bool(subprocess.check_output(["git", "status", "--porcelain"], cwd=ROOT)),
               "sourceTreeSha256": inventory_digest(sources),
               "signing": {"mode": "developer-id", "team": identity.team} if identity else {"mode": "ad-hoc"},
               "algorithmChanged": False}
    (resources / "release.json").write_text(json.dumps(package, indent=2) + "\n")
    (resources / "source-manifest.json").write_text(json.dumps({"files": sources}, ensure_ascii=False, indent=2) + "\n")
    run(args.pandoc, "--from=gfm", "--to=html5", "--standalone", "--variable=lang:zh-CN",
        "--metadata=pagetitle:采集助手使用说明", ROOT / "README.md", "--output", output / "使用说明.html")
    shutil.copy2(ROOT / "THIRD_PARTY_NOTICES.md", output / "THIRD_PARTY_NOTICES.md")
    command = output / PORTABLE_COMMAND
    shutil.copy2(ROOT / "scripts/portable_command.sh", command)
    command.chmod(0o755)
    binaries = audit_binaries(resources, identity)
    # Export the actual working tree, including reviewed new modules. HEAD alone
    # cannot reproduce an uncommitted pre-release; never label it as that commit.
    if sources != source_inventory(ROOT):
        raise ValueError("Source changed during packaging; rebuild from a stable working tree.")
    with zipfile.ZipFile(output / "源码与模型.zip", "w", compression=zipfile.ZIP_DEFLATED) as archive:
        names = set()
        for entry in sources:
            archive.write(ROOT / entry["path"], "采集助手-源码/" + entry["path"])
            names.add(entry["path"])
        for entry in manifest["files"]:
            name = "assets/models/" + entry["path"]
            if name not in names:
                archive.write(resources / "models" / entry["path"], "采集助手-源码/" + name)
        archive.writestr("采集助手-源码/源码版本.json", json.dumps(package, ensure_ascii=False, indent=2) + "\n")
    entries = inventory_tree(output)
    (output / "文件校验.json").write_text(json.dumps({"version": VERSION, "binaryCount": binaries, "files": entries},
                                                 ensure_ascii=False, indent=2) + "\n")
    print(json.dumps({"output": str(output), "files": len(entries), "binaries": binaries}, ensure_ascii=False))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--backend", type=Path, required=True, help="standalone Python 3.13 root")
    parser.add_argument("--inference", type=Path, required=True, help="standalone Python 3.11 root")
    parser.add_argument("--models", type=Path, default=ROOT / "build/models")
    parser.add_argument("--wheels", type=Path, default=ROOT / "build/wheels")
    parser.add_argument("--web", type=Path, default=ROOT / "build/web")
    parser.add_argument("--pandoc", default=shutil.which("pandoc"), help="Pandoc executable used to generate the offline guide")
    parser.add_argument("--signing-identity", help="Developer ID Application name or certificate hash in the local Keychain")
    parser.add_argument("--output", type=Path, default=Path.home() / "Library/Caches/InspectionVideoGatherBuild" / VERSION / "采集助手-Mac-arm64")
    args = parser.parse_args()
    if sys.platform != "darwin" or os.uname().machine != "arm64":
        parser.error("Build on an Apple Silicon Mac.")
    if not args.pandoc:
        parser.error("Install Pandoc or provide its path with --pandoc.")
    try:
        identity = find_identity(args.signing_identity) if args.signing_identity else None
    except ValueError as error:
        parser.error(str(error))
    assemble(args, identity)


if __name__ == "__main__":
    main()
