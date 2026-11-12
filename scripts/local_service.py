"""Run the source checkout in Terminal and stop its legacy background service."""
import argparse
from contextlib import ExitStack
import hashlib
import os
from pathlib import Path
import re
import subprocess
import sys
import time
from uuid import uuid4


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from app import local_instance
from scripts import portable_launcher as launcher

DATA = Path(os.environ.get("IVG_PROFILE_DIR") or ROOT / "数据" / "系统").expanduser().absolute()


def checkout_id():
    return hashlib.sha256(str(ROOT).encode()).hexdigest()[:12]


def service_label(port):
    return f"cn.local.inspection-video.{checkout_id()}.{port}"


def existing_services(domain, preferred):
    if local_instance.isolated_directory():
        return
    ports = {preferred, launcher.DEFAULT_PORT}
    for path in DATA.glob("local-service-*.plist"):
        match = re.fullmatch(r"local-service-([0-9]{4,5})\.plist", path.name)
        if match and 1024 <= int(match[1]) <= 65535:
            ports.add(int(match[1]))
    for port in sorted(ports, key=lambda port: (port != preferred, port)):
        result = subprocess.run(["/bin/launchctl", "print", f"{domain}/{service_label(port)}"],
                                capture_output=True, text=True, timeout=3)
        if result.returncode == 0:
            yield port, result.stdout


def stop_legacy(domain, port, details):
    target = f"{domain}/{service_label(port)}"
    result = subprocess.run(["/bin/launchctl", "bootout", target], capture_output=True, timeout=45)
    if result.returncode:
        raise launcher.LaunchError("停止旧后台失败，请稍后重试。")
    pid = re.search(r"\bpid = (\d+)", details)
    deadline = time.monotonic() + 45
    while True:
        alive = False
        if pid:
            try:
                os.kill(int(pid.group(1)), 0)
                alive = True
            except ProcessLookupError:
                pass
        registered = subprocess.run(["/bin/launchctl", "print", target], capture_output=True, timeout=3).returncode == 0
        if not registered and not alive:
            return
        if time.monotonic() >= deadline:
            raise launcher.LaunchError("旧后台仍在安全停止，请稍后检查状态。")
        time.sleep(.2)


def run(action, preferred, explicit_port, *, no_browser=False):
    profile_id = checkout_id()
    domain = f"gui/{os.getuid()}"
    with ExitStack() as ownership:
        with launcher.lock(DATA / "launch.lock"):
            if launcher.server_locked(DATA):
                state = launcher.read_json(DATA / "server.json")
                if not launcher.verified_server(state, profile_id, bundle_id="development"):
                    raise launcher.LaunchError("此目录的服务正在启动或安全停止，请稍后重试。")
                if action == "start":
                    launcher.announce(state, no_browser, "text", reused=True)
                elif action == "status":
                    launcher.print_result("status", {**state, "running": True}, "text")
                elif launcher.stop_server(DATA, state):
                    launcher.print_result("stop", {"stopped": True}, "text")
                else:
                    raise launcher.LaunchError("正在等待当前写入安全结束，请稍后检查状态。")
                return
            if legacy := next(existing_services(domain, preferred), None):
                if action == "start":
                    raise launcher.LaunchError("此目录的旧后台仍在运行。请先运行启动简易版.command stop，再重新启动。")
                if action == "stop":
                    stop_legacy(domain, *legacy)
                    launcher.print_result("stop", {"stopped": True}, "text")
                else:
                    print(f"旧后台已加载：http://127.0.0.1:{legacy[0]}/")
                return
            if action != "start":
                print("此目录的服务未运行。")
                return
            ownership.enter_context(local_instance.backend_instance())
            ownership.enter_context(launcher.lock(DATA / "server.lock", blocking=False))
        for key in ("IVG_PORTABLE", "IVG_PACKAGE_ID", "IVG_RESOURCES"):
            os.environ.pop(key, None)
        os.environ.update(IVG_PROFILE_DIR=str(DATA), IVG_PROFILE_ID=profile_id, IVG_INSTANCE_ID=uuid4().hex)
        launcher.serve(DATA, no_browser, "text", port=preferred, fixed_port=explicit_port)


def main():
    if sys.platform != "darwin":
        raise SystemExit("此后台启动入口仅用于 macOS。")
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=("start", "stop", "status"), nargs="?", default="status")
    parser.add_argument("--no-open", "--no-browser", dest="no_browser", action="store_true")
    args = parser.parse_args()
    raw_port = os.environ.get("PORT")
    if raw_port is not None and (not re.fullmatch(r"[0-9]{4,5}", raw_port) or not 1024 <= int(raw_port) <= 65535):
        raise SystemExit("端口必须是 1024 到 65535 之间的整数。")
    for directory in (DATA.parent, DATA):
        if directory.is_symlink():
            raise SystemExit("程序数据目录不能是符号链接。")
    if args.action != "start" and not DATA.is_dir():
        print("此目录的服务未运行。")
        return
    try:
        DATA.mkdir(parents=True, mode=0o700, exist_ok=True)
        run(args.action, int(raw_port) if raw_port else launcher.DEFAULT_PORT,
            raw_port is not None, no_browser=args.no_browser)
    except (launcher.LaunchError, local_instance.InstanceBusy) as error:
        raise SystemExit(str(error)) from None
    except (OSError, subprocess.TimeoutExpired):
        raise SystemExit("无法操作本地服务。请检查数据目录权限，或稍后重试。") from None


if __name__ == "__main__":
    main()
