"""Bundled-Python launcher. Does not use a shell, system Python, Node or Homebrew."""
from __future__ import annotations

import argparse
import contextlib
import errno
import fcntl
import json
import os
import signal
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from uuid import uuid4

BUNDLE_ID = "cn.yongfeng.video-gather.standalone"
PROFILE_NAME = "InspectionVideoGatherStandalone"
APP_ROOT = Path(__file__).resolve().parent
RESOURCES = APP_ROOT.parent


def private_json(path, value):
    temporary = path.with_suffix(".tmp")
    with temporary.open("w", encoding="utf-8") as stream:
        os.chmod(temporary, 0o600)
        json.dump(value, stream, ensure_ascii=False)
        stream.flush()
        os.fsync(stream.fileno())
    temporary.replace(path)


def read_json(path):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}


def profile():
    return Path(os.environ.get("IVG_PROFILE_DIR") or
                Path.home() / "Library" / "Application Support" / PROFILE_NAME).expanduser().resolve()


@contextlib.contextmanager
def lock(path, blocking=True):
    with path.open("a+") as stream:
        os.chmod(path, 0o600)
        fcntl.flock(stream, fcntl.LOCK_EX | (0 if blocking else fcntl.LOCK_NB))
        try:
            yield
        finally:
            fcntl.flock(stream, fcntl.LOCK_UN)


def environment(home, manifest, profile_id, instance_id):
    env = os.environ.copy()
    for key in list(env):
        if key.startswith(("PYTHON", "DYLD_", "LD_")):
            env.pop(key, None)
    env.update(IVG_PORTABLE="1", IVG_RESOURCES=str(RESOURCES), IVG_PROFILE_DIR=str(home),
               IVG_PROFILE_ID=profile_id, IVG_INSTANCE_ID=instance_id,
               IVG_PACKAGE_ID=manifest["packageId"], PYTHONNOUSERSITE="1",
               PYTHONDONTWRITEBYTECODE="1",
               PATH=str(RESOURCES / "tools" / "bin") + ":/usr/bin:/bin:/usr/sbin:/sbin")
    return env


def verified_server(state, profile_id, package_id=None):
    """Never reuse/stop another app just because it owns port 8786."""
    if (state.get("bundleId") != BUNDLE_ID or state.get("profileId") != profile_id
            or not isinstance(state.get("pid"), int) or state["pid"] <= 1
            or not isinstance(state.get("port"), int) or not 1 <= state["port"] <= 65535
            or not state.get("instanceId") or (package_id and state.get("packageId") != package_id)):
        return False
    try:
        # A local request must never follow a proxy or redirect to another host.
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        with opener.open(f'http://127.0.0.1:{state["port"]}/api/health', timeout=1) as response:
            if response.geturl() != f'http://127.0.0.1:{state["port"]}/api/health':
                return False
            actual = json.load(response)
        return actual.get("ok") is True and all(actual.get(key) == state.get(key) for key in
                   ("bundleId", "profileId", "instanceId", "packageId", "pid"))
    except (OSError, ValueError, urllib.error.URLError):
        return False


def open_browser(port, disabled=False):
    if not disabled:
        subprocess.run(["/usr/bin/open", f"http://127.0.0.1:{port}/"], check=False)


def prepare_profile():
    home = profile()
    home.mkdir(parents=True, mode=0o700, exist_ok=True)
    os.chmod(home, 0o700)
    path = home / "profile.json"
    with lock(home / "launch.lock"):
        config = read_json(path)
        if not config.get("id"):
            config = {"id": uuid4().hex, "bundleId": BUNDLE_ID}
            private_json(path, config)
    return home, config["id"]


def launch(no_browser=False):
    manifest = read_json(RESOURCES / "release.json")
    if manifest.get("bundleId") != BUNDLE_ID or not manifest.get("packageId"):
        raise RuntimeError("应用文件不完整，请重新解压完整包。")
    home, profile_id = prepare_profile()
    with lock(home / "launch.lock"):
        state = read_json(home / "server.json")
        if verified_server(state, profile_id):
            # Updates must be explicitly restarted, not started over an old writer.
            if state.get("packageId") != manifest["packageId"]:
                raise RuntimeError("旧版采集助手仍在运行，请先使用旧版的停止入口，再打开新版。进度会保留。")
            open_browser(state["port"], no_browser)
            return state
        instance_id = uuid4().hex
        executable = RESOURCES / "runtime" / "backend" / "bin" / "python3.13"
        logs = home / "logs"
        logs.mkdir(mode=0o700, exist_ok=True)
        log_path = logs / "service.log"
        if log_path.exists() and log_path.stat().st_size > 5 * 1024 ** 2:
            log_path.replace(logs / "service.previous.log")
        with log_path.open("ab") as stream:
            os.chmod(log_path, 0o600)
            child = subprocess.Popen([str(executable), "-I", "-B", str(Path(__file__).resolve()),
                                      "serve", "--instance-id", instance_id],
                                     cwd=APP_ROOT, env=environment(home, manifest, profile_id, instance_id),
                                     stdin=subprocess.DEVNULL, stdout=stream, stderr=stream,
                                     start_new_session=True, close_fds=True)
        for _ in range(240):
            state = read_json(home / "server.json")
            if state.get("instanceId") == instance_id and verified_server(state, profile_id, manifest["packageId"]):
                open_browser(state["port"], no_browser)
                return state
            if child.poll() is not None:
                raise RuntimeError("采集助手启动失败，请查看独立数据目录中的 logs/service.log。")
            time.sleep(0.25)
        raise RuntimeError("采集助手仍在启动，请稍后再次打开。原程序不会被停止。")


def serve(instance_id):
    home = profile()
    # The parent holds launch.lock until health is ready; do not take it here.
    with lock(home / "server.lock", blocking=False):
        sys.path.insert(0, str(APP_ROOT))
        import uvicorn
        from app.main import create_app
        from app.runtime import identity
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.bind(("127.0.0.1", 8786))
        except OSError as error:
            if error.errno != errno.EADDRINUSE:
                raise
            sock.bind(("127.0.0.1", 0))
        sock.listen(128)
        sock.set_inheritable(True)
        state = {**identity(), "port": sock.getsockname()[1]}
        private_json(home / "server.json", state)
        try:
            uvicorn.Server(uvicorn.Config(create_app(), host="127.0.0.1", access_log=False,
                                         timeout_graceful_shutdown=30)).run(sockets=[sock])
        finally:
            sock.close()
            if read_json(home / "server.json").get("instanceId") == instance_id:
                (home / "server.json").unlink(missing_ok=True)


def stop():
    home, profile_id = prepare_profile()
    with lock(home / "launch.lock"):
        state = read_json(home / "server.json")
        if not verified_server(state, profile_id):
            return {"stopped": False, "message": "本包没有正在运行的实例。"}
        os.kill(state["pid"], signal.SIGTERM)
        for _ in range(160):
            try:
                os.kill(state["pid"], 0)
            except ProcessLookupError:
                return {"stopped": True}
            if not verified_server(state, profile_id):
                # Graceful shutdown can continue after the listening socket closes.
                try:
                    with lock(home / "server.lock", blocking=False):
                        return {"stopped": True}
                except BlockingIOError:
                    pass
            time.sleep(0.25)
        return {"stopped": False, "message": "正在等待当前写入安全结束，请稍后重试停止。"}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("action", nargs="?", choices=("launch", "serve", "stop"), default="launch")
    parser.add_argument("--no-browser", action="store_true")
    parser.add_argument("--instance-id", default="")
    args = parser.parse_args()
    try:
        if args.action == "serve":
            serve(args.instance_id)
        else:
            result = stop() if args.action == "stop" else launch(args.no_browser)
            print(json.dumps(result, ensure_ascii=False))
    except (OSError, RuntimeError) as error:
        # No remote URL, credential or platform response is ever part of this error.
        message = str(error) if isinstance(error, RuntimeError) else "无法启动，请检查应用和数据目录权限。"
        print(message, file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
