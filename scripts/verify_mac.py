"""Exercise an extracted command package with isolated profiles."""
from __future__ import annotations

import argparse
from contextlib import contextmanager
from functools import partial
import hashlib
import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts.package_safety import PORTABLE_COMMAND, PORTABLE_RESOURCES, verify_tree


def read(url):
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    with opener.open(url, timeout=10) as response:
        return json.load(response)


def verify_files(folder):
    if any(folder.rglob("*.app")):
        raise ValueError("The command-only package must not contain macOS application bundles.")
    if not os.access(folder / PORTABLE_COMMAND, os.X_OK):
        raise ValueError("The startup command must be executable after extraction.")
    manifest = json.loads((folder / "文件校验.json").read_text())
    verify_tree(folder, manifest)
    print(f'Verified {len(manifest["files"])} packaged file checksums.')


def command_environment(profile, instance_dir):
    env = {key: value for key, value in os.environ.items() if not key.startswith(("PYTHON", "DYLD_", "LD_", "IVG_"))}
    env.update(IVG_TEST_INSTANCE_DIR=str(instance_dir), IVG_SKIP_SAVED_CONNECTIONS="1", PATH="/usr/bin:/bin")
    if profile is not None:
        env["IVG_PROFILE_DIR"] = str(profile)
    return env


def invoke(folder, profile, *arguments, instance_dir):
    result = subprocess.run([str(folder / PORTABLE_COMMAND), *arguments, "--output", "json"],
                            env=command_environment(profile, instance_dir), cwd=instance_dir.parent,
                            capture_output=True, text=True, timeout=45)
    if result.returncode:
        raise RuntimeError(result.stderr.strip() or "Portable launch failed.")
    return json.loads(result.stdout)


@contextmanager
def foreground(folder, profile, *, instance_dir):
    with tempfile.TemporaryFile(mode="w+") as console:
        process = subprocess.Popen(
            [str(folder / PORTABLE_COMMAND), "start", "--no-browser", "--output", "json"],
            env=command_environment(profile, instance_dir), cwd=instance_dir.parent,
            stdout=console, stderr=console,
        )
        try:
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline and process.poll() is None:
                state = invoke(folder, profile, "status", instance_dir=instance_dir)
                if state.get("pid") == process.pid:
                    yield state, process
                    return
                time.sleep(0.15)
            console.seek(0)
            raise RuntimeError(f"Foreground command did not become ready: {console.read()}")
        finally:
            if process.poll() is None:
                process.send_signal(signal.SIGHUP)
                try:
                    process.wait(timeout=45)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
                    raise


def smoke(folder):
    with tempfile.TemporaryDirectory(prefix="ivg-isolated-") as temporary:
        work = Path(temporary).resolve()
        run = partial(invoke, instance_dir=work / "instance")
        profile = work / "独立数据 中文 空格"
        # An unrelated program already owning 8786 must survive launch and stop.
        blocker = socket.socket()
        owns_blocker = False
        try:
            blocker.bind(("127.0.0.1", 8786))
            blocker.listen()
            owns_blocker = True
        except OSError:
            blocker.close()  # The user's existing server already provides the conflict.
        assert not run(folder, profile, "status")["running"]
        assert not profile.exists()
        start = partial(foreground, instance_dir=work / "instance")
        try:
            with start(folder, profile) as (first, process):
                if owns_blocker:
                    assert first["port"] != 8786
                base = f'http://127.0.0.1:{first["port"]}'
                assert read(base + "/api/health")["instanceId"] == first["instanceId"]
                assert not read(base + "/api/videos")["videos"]
                assert Path(read(base + "/api/videos")["directory"]).is_relative_to(work / "数据")
                for endpoint in ("/api/session", "/api/uploads", "/api/slicing", "/api/workspace"):
                    assert isinstance(read(base + endpoint), dict), endpoint
                second = run(folder, profile, "launch", "--no-browser")
                assert second["pid"] == process.pid == first["pid"]
                other_profile = work / "另一个数据目录"
                try:
                    run(folder, other_profile, "launch", "--no-browser")
                except RuntimeError as error:
                    assert "另一份采集助手" in str(error)
                else:
                    raise AssertionError("A second profile started a duplicate backend.")
                assert read(base + "/api/health")["pid"] == first["pid"]

                # Terminal close delivers SIGHUP to the foreground command.
                process.send_signal(signal.SIGHUP)
                assert process.wait(timeout=45) == 0
                assert not run(folder, profile, "status")["running"]
                assert not (profile / "server.json").exists()
                assert not (profile / "credentials/credentials.json").exists()
            with start(folder, profile) as (restarted, process):
                assert restarted["profileId"] == first["profileId"]
                assert restarted["instanceId"] != first["instanceId"]
                assert run(folder, profile, "stop")["stopped"]
                assert process.wait(timeout=45) == 0
                assert not run(folder, profile, "stop")["stopped"]
            print("Port conflict, all API entries, repeat launch, terminal hangup, relaunch and stop passed.")
        finally:
            if owns_blocker:
                assert blocker.getsockname()[1] == 8786
                blocker.close()

        moved = work / "改名后的程序 空格 中文"
        shutil.copytree(folder, moved, symlinks=True)
        with start(moved, profile) as (state, process):
            base = f'http://127.0.0.1:{state["port"]}'
            settings = read(base + "/api/slicing")["settings"]
            for key in ("magnetModelPath", "bedModelPath", "bedGeometryPath", "workerPythonPath", "frameToolPath"):
                assert Path(settings[key]).is_relative_to(moved), key
                assert Path(settings[key]).is_file(), key
            assert state["profileId"] == first["profileId"]
            assert state["instanceId"] != first["instanceId"]
            process.send_signal(signal.SIGINT)
            assert process.wait(timeout=45) == 0
        assert not run(moved, profile, "status")["running"]
        print("Moved/renamed Chinese and space paths, bundled path repair and Control-C passed.")

        # A move preserves the live profile's lock inode. Start the moved copy
        # while the original command is open and verify the lock is handed over.
        renamed = work / "运行中改名 中文"
        with start(moved, None) as (live, old_process):
            moved.rename(renamed)
            with start(renamed, None) as (reopened, process):
                assert old_process.wait(timeout=45) == 0
                assert reopened["profileId"] == live["profileId"]
                assert reopened["instanceId"] != live["instanceId"]
                assert read(f'http://127.0.0.1:{reopened["port"]}/api/health')["pid"] == process.pid
                assert run(renamed, None, "stop")["stopped"]
        print("Live folder rename restarts the same profile from its new path.")


PARITY_SCRIPT = '''
import json, sys
from pathlib import Path
request=json.load(sys.stdin)
sys.path.insert(0, request['application'])
from app.slicing_worker import Models
models=Models(request['settings'], ['bed','magnet'])
for category in ['bed','magnet']: models.load(category)
outputs=[]
for sample in request['samples']:
    data=Path(sample).read_bytes()
    outputs.append({category: models.classify(data, category) for category in ['bed','magnet']})
print(json.dumps(outputs))
'''


def parity(folder, reference_python, samples, reference_models):
    resources = folder / PORTABLE_RESOURCES
    def settings(models):
        return {"magnetDeployDirectory": str(models / "magnet"), "magnetModelPath": str(models / "magnet/best.onnx"),
                "bedDeployDirectory": str(models / "bed"), "bedModelPath": str(models / "bed/model.pt"),
                "bedGeometryPath": str(models / "bed/geometry.npz"), "magnetConfidence": 0.25}
    outputs = []
    for executable, application, models in ((reference_python, ROOT, reference_models),
            (resources / "runtime/inference/bin/python3.11", resources / "application", resources / "models")):
        payload = {"application": str(application), "settings": settings(models), "samples": [str(path) for path in samples]}
        result = subprocess.run([str(executable.absolute()), "-I", "-B", "-c", PARITY_SCRIPT],
            input=json.dumps(payload), text=True, capture_output=True, timeout=180,
            env={**os.environ, "PATH": "/usr/bin:/bin", "PYTHONNOUSERSITE": "1"})
        if result.returncode:
            raise RuntimeError("Model parity runner failed; inspect only sanitized local diagnostics.")
        outputs.append(json.loads(result.stdout))
    def compare(left, right):
        if isinstance(left, float):
            assert abs(left - right) <= 1e-6
        elif isinstance(left, dict):
            assert left.keys() == right.keys()
            for key in left: compare(left[key], right[key])
        elif isinstance(left, list):
            assert len(left) == len(right)
            for a, b in zip(left, right): compare(a, b)
        else:
            assert left == right
    compare(*outputs)
    print(json.dumps({"modelParity": "passed", "samples": len(samples), "scoresAndLabelsAndGeometry": outputs[1]}, ensure_ascii=False))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("folder", type=Path)
    parser.add_argument("--reference-python", type=Path)
    parser.add_argument("--reference-models", type=Path, default=ROOT / "build/models")
    parser.add_argument("--sample", type=Path, action="append", default=[])
    args = parser.parse_args()
    verify_files(args.folder)
    smoke(args.folder.resolve())
    if args.reference_python and args.sample:
        parity(args.folder.resolve(), args.reference_python, args.sample, args.reference_models.resolve())


if __name__ == "__main__":
    main()
