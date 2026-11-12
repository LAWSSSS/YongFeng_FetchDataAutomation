"""Assemble an allowlisted, self-contained macOS arm64 command package.

The shared runtime is built from a python-build-standalone install_only
distribution. Dependencies are installed anew from locked wheels.
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
from app.runtime import BUNDLE_ID, PORTABLE_PYTHON, VERSION
from scripts.fetch_models import verify as verify_models
from scripts.package_safety import (
    PORTABLE_COMMAND, PORTABLE_RESOURCES, inventory_digest, inventory_tree, relative_name,
    source_inventory, verify_requirement_wheels,
)
from scripts.scan_secrets import issues

MACH_HEADERS = {b"\xcf\xfa\xed\xfe", b"\xfe\xed\xfa\xcf", b"\xca\xfe\xba\xbe", b"\xbe\xba\xfe\xca"}
RUNTIME_REQUIREMENTS = ("requirements.lock.txt", "requirements-slicing.lock.txt")

def run(*args, **kwargs):
    subprocess.run([str(value) for value in args], check=True, **kwargs)


def digest(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def is_macho(path):
    if not path.is_file() or path.is_symlink():
        return False
    with path.open("rb") as stream:
        return stream.read(4) in MACH_HEADERS


def make_runtime(source, destination, wheels):
    if (source / "pyvenv.cfg").exists() or not (source / "BUILD").is_file():
        raise ValueError("Use an upstream python-build-standalone distribution, never a virtualenv.")
    relative_python = PORTABLE_PYTHON.relative_to("runtime")
    if not (source / relative_python).is_file():
        raise ValueError("Wrong Python distribution version.")
    lock = json.loads((ROOT / "packaging.lock.json").read_text())["python"]
    if (source / "BUILD").read_text().strip() != lock["build"]:
        raise ValueError("Unexpected upstream standalone Python build.")
    expected_python = lock["version"]
    if subprocess.check_output([str(source / relative_python), "-I", "--version"], text=True).strip() != "Python " + expected_python:
        raise ValueError("Unexpected standalone Python patch version.")
    entries = json.loads((ROOT / "assets/wheels.lock.json").read_text())["files"]
    for requirements in RUNTIME_REQUIREMENTS:
        verify_requirement_wheels(ROOT / requirements, entries)
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
    executable = destination / relative_python
    env = {key: value for key, value in os.environ.items()
           if not key.startswith(("PYTHON", "DYLD_", "LD_"))}
    run(executable, "-I", "-B", "-m", "ensurepip", "--default-pip", env=env)
    pins = [value for name in RUNTIME_REQUIREMENTS for value in ("-r", ROOT / name)]
    run(executable, "-I", "-B", "-m", "pip", "install", "--no-index", "--find-links", wheels,
        "--no-compile", "--disable-pip-version-check", *pins, env=env)
    run(executable, "-I", "-m", "pip", "check", env=env)
    # pip console scripts embed build paths. Runtime workers use python -m only;
    # replace optional console entries with relative shell wrappers.
    for path in (destination / "bin").iterdir():
        if path.is_file() and not path.is_symlink() and not is_macho(path):
            data = path.read_bytes()
            if data.startswith(b"#!"):
                if path.name.startswith("pip"):
                    path.write_text('#!/bin/sh\nexec "$(dirname "$0")/' + executable.name + '" -I -m pip "$@"\n')
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


def collect_licenses(resources, wheels, ffmpeg_tools):
    destination = resources / "licenses"
    destination.mkdir()
    inventory = []
    for wheel in sorted(wheels.glob("*.whl")):
        inventory.append({"wheel": wheel.name, "sha256": digest(wheel)})
        with zipfile.ZipFile(wheel) as archive:
            for name in archive.namelist():
                upper = Path(name).name.upper()
                if name.endswith("/") or not any(token in upper for token in ("LICENSE", "COPYING", "NOTICE")):
                    continue
                relative_name(name)
                target = destination / wheel.stem / name
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(archive.read(name))
    shutil.copy2(resources / "runtime/lib" / PORTABLE_PYTHON.name / "LICENSE.txt", destination / "Python-LICENSE.txt")
    shutil.copy2(ffmpeg_tools / "FFmpeg-LICENSE.txt", destination / "FFmpeg-LICENSE.txt")
    (resources / "dependencies.json").write_text(json.dumps(inventory, indent=2) + "\n")


def audit_binaries(resources):
    # Strip resource-fork/Finder metadata from newly assembled copies only.
    # Never clear quarantine on a user's downloaded application or alter Gatekeeper.
    libc = ctypes.CDLL("/usr/lib/libSystem.B.dylib")
    libc.removexattr.argtypes = [ctypes.c_char_p, ctypes.c_char_p, ctypes.c_int]
    for path in (resources, *resources.rglob("*")):
        if path.is_symlink():
            continue
        for name in ("com.apple.FinderInfo", "com.apple.ResourceFork"):
            libc.removexattr(os.fsencode(path), name.encode(), 0)
    binaries = [path for path in resources.rglob("*") if is_macho(path)]
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
        run("/usr/bin/codesign", "--force", "--sign", "-", "--timestamp=none", path,
            stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        run("/usr/bin/codesign", "--verify", "--strict", path)
    return len(binaries)


def assemble(args):
    sources = source_inventory(ROOT)
    for entry in sources:
        if issues((ROOT / entry["path"]).read_bytes()):
            raise ValueError(f'Credential scan failed: {entry["path"]}')
    if not (args.web / "index.html").is_file():
        raise ValueError("A fresh frontend build is required.")
    ffmpeg = args.ffmpeg_tools / "bin/ffmpeg"
    ffprobe = ffmpeg.with_name("ffprobe")
    if not is_macho(ffmpeg) or not ffprobe.is_symlink() or os.readlink(ffprobe) != "ffmpeg":
        raise ValueError("Build the shared FFmpeg tools with scripts/build_ffmpeg.sh first.")
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
    runtime = make_runtime(args.python.resolve(), resources / "runtime", args.wheels)
    for name in RUNTIME_REQUIREMENTS:
        shutil.copy2(ROOT / name, resources / name)
    tools = resources / "tools/bin"
    tools.mkdir(parents=True)
    shutil.copy2(ffmpeg, tools / "ffmpeg")
    (tools / "ffprobe").symlink_to("ffmpeg")
    collect_licenses(resources, args.wheels, args.ffmpeg_tools)
    web_licenses = resources / "licenses/frontend"
    web_licenses.mkdir()
    for name in ("react", "react-dom", "scheduler", "@phosphor-icons/react"):
        shutil.copy2(ROOT / "node_modules" / name / "LICENSE", web_licenses / (name.replace("/", "-") + ".txt"))
    package = {"bundleId": BUNDLE_ID, "version": VERSION, "packageId": uuid4().hex,
               "platform": "macos-arm64", "minimumMacOS": "14.0", "runtime": runtime,
               "sourceCommit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
               "sourceDirty": bool(subprocess.check_output(["git", "status", "--porcelain"], cwd=ROOT)),
               "sourceTreeSha256": inventory_digest(sources),
               "signing": {"mode": "ad-hoc"},
               "algorithmChanged": False}
    (resources / "release.json").write_text(json.dumps(package, indent=2) + "\n")
    (resources / "source-manifest.json").write_text(json.dumps({"files": sources}, ensure_ascii=False, indent=2) + "\n")
    run(args.pandoc, "--from=gfm", "--to=html5", "--standalone", "--variable=lang:zh-CN",
        "--metadata=pagetitle:采集助手使用说明", ROOT / "README.md", "--output", output / "使用说明.html")
    shutil.copy2(ROOT / "THIRD_PARTY_NOTICES.md", output / "THIRD_PARTY_NOTICES.md")
    command = output / PORTABLE_COMMAND
    shutil.copy2(ROOT / "scripts/portable_command.sh", command)
    command.chmod(0o755)
    binaries = audit_binaries(resources)
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
    parser.add_argument("--python", type=Path, required=True, help="standalone Python 3.11 root")
    parser.add_argument("--models", type=Path, default=ROOT / "build/models")
    parser.add_argument("--wheels", type=Path, default=ROOT / "build/wheels")
    parser.add_argument("--web", type=Path, default=ROOT / "build/web")
    parser.add_argument("--ffmpeg-tools", type=Path, default=ROOT / "build/ffmpeg-tools")
    parser.add_argument("--pandoc", default=shutil.which("pandoc"), help="Pandoc executable used to generate the offline guide")
    parser.add_argument("--output", type=Path, default=Path.home() / "Library/Caches/InspectionVideoGatherBuild" / VERSION / "采集助手-Mac-arm64")
    args = parser.parse_args()
    if sys.platform != "darwin" or os.uname().machine != "arm64":
        parser.error("Build on an Apple Silicon Mac.")
    if not args.pandoc:
        parser.error("Install Pandoc or provide its path with --pandoc.")
    assemble(args)


if __name__ == "__main__":
    main()
