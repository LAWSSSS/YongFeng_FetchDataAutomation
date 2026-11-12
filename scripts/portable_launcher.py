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
from pathlib import Path
from uuid import uuid4

BUNDLE_ID = "cn.yongfeng.video-gather.standalone"
APP_ROOT = Path(__file__).resolve().parent
RESOURCES = APP_ROOT.parent
sys.path.insert(0, str(APP_ROOT if (APP_ROOT / "app").is_dir() else APP_ROOT.parent))
from app import local_instance
from app.runtime import identity, portable_package_directory


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
        value = json.loads(path.read_text(encoding="utf-8"))
        return value if isinstance(value, dict) else {}
    except (OSError, ValueError):
        return {}


def clear_server_state(home, instance_id):
    path = home / "server.json"
    if read_json(path).get("instanceId") == instance_id:
        path.unlink(missing_ok=True)


def profile():
    folder = portable_package_directory(RESOURCES)
    return Path(os.environ.get("IVG_PROFILE_DIR") or folder / "数据" / "系统").expanduser().resolve()


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
        if key.startswith(("PYTHON", "DYLD_", "LD_", "IVG_")):
            env.pop(key, None)
    env.update(IVG_PORTABLE="1", IVG_RESOURCES=str(RESOURCES), IVG_PROFILE_DIR=str(home),
               IVG_PROFILE_ID=profile_id, IVG_INSTANCE_ID=instance_id,
               IVG_PACKAGE_ID=manifest["packageId"], PYTHONNOUSERSITE="1",
               PYTHONDONTWRITEBYTECODE="1",
               PATH=str(RESOURCES / "tools" / "bin") + ":/usr/bin:/bin:/usr/sbin:/sbin")
    if isolated := local_instance.isolated_directory():
        env.update(IVG_TEST_INSTANCE_DIR=str(isolated), IVG_SKIP_SAVED_CONNECTIONS="1")
    return env


def verified_server(state, profile_id, package_id=None):
    """Never reuse/stop another app just because it owns port 8786."""
    if (state.get("bundleId") != BUNDLE_ID or state.get("profileId") != profile_id
            or not isinstance(state.get("pid"), int) or state["pid"] <= 1
            or not isinstance(state.get("port"), int) or not 1 <= state["port"] <= 65535
            or not state.get("instanceId") or (package_id and state.get("packageId") != package_id)):
        return False
    actual = local_instance.health_at(state["port"])
    return bool(actual) and all(actual.get(key) == state.get(key) for key in
                               ("bundleId", "profileId", "instanceId", "packageId", "pid"))


def open_browser(port):
    address = f"http://127.0.0.1:{port}/"
    try:
        subprocess.run(["/usr/bin/open", address], check=True, timeout=5,
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except (OSError, subprocess.SubprocessError):
        print(f"请在浏览器打开 {address}", file=sys.stderr)


def server_locked(home):
    """A busy/unresponsive writer is still a writer; never spawn over it."""
    try:
        with lock(home / "server.lock", blocking=False):
            return False
    except BlockingIOError:
        return True


def stop_server(home, state):
    with contextlib.suppress(ProcessLookupError):
        os.kill(state["pid"], signal.SIGTERM)
    for _ in range(160):
        if not server_locked(home):
            clear_server_state(home, state["instanceId"])
            return True
        time.sleep(0.25)
    return False


def release_manifest():
    manifest = read_json(RESOURCES / "release.json")
    if manifest.get("bundleId") != BUNDLE_ID or not manifest.get("packageId"):
        raise RuntimeError("应用文件不完整，请重新解压完整包。")
    return manifest


def prepare_profile():
    home = profile()
    try:
        home.mkdir(parents=True, mode=0o700, exist_ok=True)
    except OSError:
        raise RuntimeError("程序所在位置不可写，请将整个程序文件夹移到可写的磁盘后再打开。") from None
    os.chmod(home, 0o700)
    path = home / "profile.json"
    with lock(home / "launch.lock"):
        config = read_json(path)
        if not config.get("id"):
            config = {"id": uuid4().hex, "bundleId": BUNDLE_ID}
            private_json(path, config)
    return home, config["id"]


def launch(no_browser=False, output="json"):
    manifest = release_manifest()
    home, profile_id = prepare_profile()
    with contextlib.ExitStack() as ownership:
        with lock(home / "launch.lock"):
            state = read_json(home / "server.json")
            if verified_server(state, profile_id) and server_locked(home):
                if state.get("packageId") != manifest["packageId"]:
                    raise RuntimeError("旧版采集助手仍在运行，请先停止旧版，再打开新版。")
                previous = state.get("profilePath")
                moved = (isinstance(previous, str) and Path(previous).is_absolute()
                         and Path(previous) != home and not Path(previous).exists())
                if not moved:
                    announce(state, no_browser, output, reused=True)
                    return
                if not stop_server(home, state):
                    raise RuntimeError("正在停止移动前的服务，请稍后再次打开。")
            if server_locked(home):
                raise RuntimeError("采集助手正在启动或安全停止，请稍后再打开。")
            ownership.enter_context(local_instance.backend_instance())
            ownership.enter_context(lock(home / "server.lock", blocking=False))
        instance_id = uuid4().hex
        env = environment(home, manifest, profile_id, instance_id)
        os.environ.clear()
        os.environ.update(env)
        serve(home, no_browser, output)


def announce(state, no_browser, output, *, reused=False):
    print_result("launch", state, output)
    if output == "text":
        print("服务由首次启动的终端窗口控制。关闭该窗口或按 Control-C 可停止。" if reused
              else "关闭此终端窗口或按 Control-C 可停止服务。", flush=True)
    if not no_browser:
        open_browser(state["port"])


def service_logging(home):
    logs = home / "logs"
    logs.mkdir(mode=0o700, exist_ok=True)
    path = logs / "service.log"
    if path.exists() and path.stat().st_size > 5 * 1024 ** 2:
        path.replace(logs / "service.previous.log")
    path.touch(mode=0o600, exist_ok=True)
    return {
        "version": 1, "disable_existing_loggers": False,
        "formatters": {"service": {"format": "%(asctime)s %(levelname)s %(message)s"}},
        "handlers": {"service": {"class": "logging.FileHandler", "filename": str(path),
                                 "encoding": "utf-8", "formatter": "service"}},
        "root": {"level": "WARNING", "handlers": ["service"]},
        "loggers": {"uvicorn": {"level": "INFO"}},
    }


def serve(home, no_browser, output):
    import uvicorn

    class TerminalServer(uvicorn.Server):
        def handle_exit(self, sig, frame):
            # All three terminal exit paths go through Uvicorn's lifespan
            # shutdown, without replaying the signal after cleanup.
            self.should_exit = True

        async def startup(self, sockets=None):
            await super().startup(sockets=sockets)
            if not self.should_exit:
                try:
                    announce(state, no_browser, output)
                except OSError:
                    # The terminal can vanish while printing the ready message.
                    self.should_exit = True

    config = uvicorn.Config("app.main:create_app", factory=True, host="127.0.0.1",
                            log_config=service_logging(home), access_log=False,
                            timeout_graceful_shutdown=30)
    server = TerminalServer(config)
    # Closing a macOS Terminal window sends SIGHUP. Uvicorn handles SIGINT and
    # SIGTERM itself, but a hangup needs an explicit handler.
    with contextlib.ExitStack() as cleanup, socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        for sig in (signal.SIGHUP, signal.SIGINT, signal.SIGTERM):
            original = signal.signal(sig, server.handle_exit)
            cleanup.callback(signal.signal, sig, original)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.bind(("127.0.0.1", 8786))
        except OSError as error:
            if error.errno != errno.EADDRINUSE:
                raise
            sock.bind(("127.0.0.1", 0))
        sock.listen(128)
        state = {**identity(), "port": sock.getsockname()[1], "profilePath": str(home)}
        cleanup.callback(clear_server_state, home, state["instanceId"])
        if not server.should_exit:
            private_json(home / "server.json", state)
            server.run(sockets=[sock])


def stop():
    manifest = release_manifest()
    home, profile_id = prepare_profile()
    with lock(home / "launch.lock"):
        state = read_json(home / "server.json")
        if not verified_server(state, profile_id, manifest["packageId"]):
            if server_locked(home):
                return {"stopped": False, "retry": True,
                        "message": "已有实例运行但无法核实为本包，请稍后重试或使用运行版本的停止入口。"}
            return {"stopped": False, "message": "本包没有正在运行的实例。"}
        if not server_locked(home):
            return {"stopped": False, "message": "这个数据目录没有运行中的后台。"}
        if stop_server(home, state):
            return {"stopped": True}
        return {"stopped": False, "retry": True, "message": "正在等待当前写入安全结束，请稍后重试停止。"}


def status():
    home = profile()
    profile_id = read_json(home / "profile.json").get("id")
    state = read_json(home / "server.json")
    if (profile_id and (home / "server.lock").is_file()
            and verified_server(state, profile_id) and server_locked(home)):
        return {**state, "running": True}
    return {"running": False}


def print_result(action, result, output):
    if output == "json":
        print(json.dumps(result, ensure_ascii=False), flush=True)
    elif action == "stop":
        print("采集助手已停止，数据和进度已保留。" if result["stopped"] else result["message"])
    elif action == "status" and not result["running"]:
        print("这个数据目录的后台未运行。")
    else:
        print(f"采集助手 {result['version']} 正在运行：http://127.0.0.1:{result['port']}/")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("action", nargs="?", choices=("launch", "stop", "status"), default="launch")
    parser.add_argument("--no-browser", action="store_true")
    parser.add_argument("--output", choices=("json", "text"), default="json")
    args = parser.parse_args()
    try:
        if args.action == "launch":
            launch(args.no_browser, args.output)
        else:
            result = stop() if args.action == "stop" else status()
            if result.get("retry"):
                raise RuntimeError(result["message"])
            print_result(args.action, result, args.output)
    except (OSError, RuntimeError) as error:
        # No remote URL, credential or platform response is ever part of this error.
        message = str(error) if isinstance(error, RuntimeError) else "无法启动，请检查应用和数据目录权限。"
        print(message, file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
