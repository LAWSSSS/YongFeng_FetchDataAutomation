"""Exercise an extracted app in isolated profiles; no real platform writes."""
from __future__ import annotations

import argparse
from functools import partial
import hashlib
import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts.package_safety import PORTABLE_APP, PORTABLE_COMMAND, verify_tree


def read(url):
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    with opener.open(url, timeout=10) as response:
        return json.load(response)


def verify_files(folder):
    manifest = json.loads((folder / "文件校验.json").read_text())
    verify_tree(folder, manifest)
    print(f'Verified {len(manifest["files"])} packaged file checksums.')


def invoke(folder, profile, *arguments, instance_dir):
    env = {key: value for key, value in os.environ.items() if not key.startswith(("PYTHON", "DYLD_", "LD_", "IVG_"))}
    env.update(IVG_TEST_INSTANCE_DIR=str(instance_dir), IVG_SKIP_SAVED_CONNECTIONS="1", PATH="/usr/bin:/bin")
    if profile is not None:
        env["IVG_PROFILE_DIR"] = str(profile)
    result = subprocess.run(["/bin/zsh", str(folder / PORTABLE_COMMAND), *arguments, "--output", "json"],
                            env=env, cwd=instance_dir.parent, capture_output=True, text=True, timeout=100)
    if result.returncode:
        raise RuntimeError(result.stderr.strip() or "Portable launch failed.")
    return json.loads(result.stdout)


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
        first = run(folder, profile, "start", "--no-browser")
        try:
            assert first["port"] != 8786
            base = f'http://127.0.0.1:{first["port"]}'
            assert read(base + "/api/health")["instanceId"] == first["instanceId"]
            assert not read(base + "/api/videos")["videos"]
            assert Path(read(base + "/api/videos")["directory"]).is_relative_to(work / "数据")
            for endpoint in ("/api/session", "/api/uploads", "/api/slicing", "/api/workspace"):
                assert isinstance(read(base + endpoint), dict), endpoint
            second = run(folder, profile, "launch", "--no-browser")
            assert second["pid"] == first["pid"]
            assert run(folder, profile, "status")["pid"] == first["pid"]
            other_profile = work / "另一个数据目录"
            try:
                run(folder, other_profile, "launch", "--no-browser")
            except RuntimeError as error:
                assert "另一份采集助手" in str(error)
            else:
                run(folder, other_profile, "stop")
                raise AssertionError("A second profile started a duplicate backend.")
            assert read(base + "/api/health")["pid"] == first["pid"]
            assert run(folder, profile, "stop")["stopped"]
            assert not (profile / "server.json").exists()
            assert not run(folder, profile, "stop")["stopped"]
            assert not (profile / "credentials/credentials.json").exists()
            print("Port conflict, empty profile, all API entries, repeat launch and safe stop passed.")
        finally:
            run(folder, profile, "stop")
            if owns_blocker:
                assert blocker.getsockname()[1] == 8786
                blocker.close()
        # Rename and move the physical bundle, not just a symlink to the old path.
        moved = work / "改名后的应用 空格 中文"
        shutil.copytree(folder, moved, symlinks=True)
        moved = moved.resolve()
        state = run(moved, profile, "launch", "--no-browser")
        try:
            base = f'http://127.0.0.1:{state["port"]}'
            data = read(base + "/api/slicing")
            settings = data["settings"]
            for key in ("magnetModelPath", "bedModelPath", "bedGeometryPath", "workerPythonPath", "frameToolPath"):
                assert Path(settings[key]).is_relative_to(moved), key
                assert Path(settings[key]).is_file(), key
            assert state["profileId"] == first["profileId"]
            assert state["instanceId"] != first["instanceId"]
            print("Moved/renamed Chinese and space paths, retained profile and bundled path repair passed.")
        finally:
            run(moved, profile, "stop")

        # Moving the live application and its default profile preserves the
        # writer's lock inode. Reopening the moved bundle must replace that writer.
        live = run(moved, None, 'start', '--no-browser')
        renamed = work / '运行中改名 中文'
        moved.rename(renamed)
        try:
            reopened = run(renamed, None, 'start', '--no-browser')
            assert reopened['profileId'] == live['profileId']
            assert reopened['instanceId'] != live['instanceId']
            assert read(f'http://127.0.0.1:{reopened["port"]}/api/health')['pid'] == reopened['pid']
            print('Live folder rename restarts the same profile from its new path.')
        finally:
            run(renamed, None, 'stop')


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
    resources = folder / PORTABLE_APP / "Contents/Resources"
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
