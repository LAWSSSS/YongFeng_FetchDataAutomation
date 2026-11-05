"""Manage this checkout's macOS backend independently of the starting terminal."""
import fcntl
import hashlib
import json
import os
from pathlib import Path
import plistlib
import re
import socket
import subprocess
import sys
import time


ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "数据" / "系统"
DEFAULT_PORT = 8786


def service_label(port):
    identity = hashlib.sha256(str(ROOT).encode()).hexdigest()[:12]
    return f"cn.local.inspection-video.{identity}.{port}"


def can_bind(port):
    with socket.socket() as probe:
        # Match uvicorn's bind flags when connections are in TIME_WAIT.
        probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            probe.bind(("127.0.0.1", port))
        except OSError:
            return False
        return True


def existing_services(domain, preferred):
    ports = {preferred, DEFAULT_PORT}
    for path in DATA.glob("local-service-*.plist"):
        match = re.fullmatch(r"local-service-([0-9]{4,5})\.plist", path.name)
        if match and 1024 <= int(match[1]) <= 65535:
            ports.add(int(match[1]))
    for port in sorted(ports, key=lambda port: (port != preferred, port)):
        result = subprocess.run(["launchctl", "print", f"{domain}/{service_label(port)}"],
                                capture_output=True, text=True)
        if result.returncode == 0:
            yield port, result.stdout


def run(action, preferred, explicit_port):
    domain = f"gui/{os.getuid()}"
    active = next(existing_services(domain, preferred), None)
    if action == "status":
        print(f"后台服务已加载：http://127.0.0.1:{active[0]}/" if active else "后台服务未启动。")
        return
    if action == "stop" and not active:
        print("此目录的后台服务未启动。")
        return
    if active:
        port, existing_output = active
    else:
        choices = (preferred,) if explicit_port else range(DEFAULT_PORT, DEFAULT_PORT + 20)
        port = next((candidate for candidate in choices if can_bind(candidate)), None)
        if port is None:
            raise SystemExit("本机端口已被占用。请尝试 PORT=8806 zsh ./启动简易版.command。")
    label = service_label(port)
    target = f"{domain}/{label}"
    log = DATA / "local-service.log"
    if action == "stop":
        result = subprocess.run(["launchctl", "bootout", target], capture_output=True)
        if result.returncode:
            raise SystemExit("停止失败，请等待当前文件操作完成后重试。")
        pid = re.search(r"\bpid = (\d+)", existing_output)
        deadline = time.monotonic() + 45
        while True:
            alive = False
            if pid:
                try:
                    os.kill(int(pid.group(1)), 0)
                    alive = True
                except ProcessLookupError:
                    pass
            registered = subprocess.run(["launchctl", "print", target], capture_output=True).returncode == 0
            if not registered and not alive:
                break
            if time.monotonic() >= deadline:
                raise SystemExit("后台仍在安全停止，文件与进度保留；请稍后检查状态。")
            time.sleep(.2)
        print("后台服务已停止，任务进度保留。")
        return

    endpoint = {"port": port, "instanceId": label, "logPath": str(log)}
    if active:
        print(json.dumps(endpoint))
        return
    env = {"PORT": str(port), "IVG_INSTANCE_ID": label,
           "PATH": "/usr/bin:/bin:/usr/sbin:/sbin:/opt/homebrew/bin:/usr/local/bin"}
    if os.environ.get("IVG_SKIP_SAVED_CONNECTIONS") == "1":
        env["IVG_SKIP_SAVED_CONNECTIONS"] = "1"
    configuration = {
        "Label": label,
        "ProgramArguments": [str(ROOT / ".venv/bin/python"), "-u", "-m", "app.main"],
        "WorkingDirectory": str(ROOT),
        "EnvironmentVariables": env,
        "RunAtLoad": True,
        # The browser waits on this local HTTP server. launchd's default
        # Standard class throttles disk I/O even for these foreground requests;
        # XPC-based Adaptive promotion does not apply to HTTP connections.
        "ProcessType": "Interactive",
        "KeepAlive": {"SuccessfulExit": False},
        "ThrottleInterval": 10,
        "StandardOutPath": str(log),
        "StandardErrorPath": str(log),
    }
    path = DATA / f"local-service-{port}.plist"
    path.write_bytes(plistlib.dumps(configuration))
    path.chmod(0o600)
    result = subprocess.run(["launchctl", "bootstrap", domain, str(path)], capture_output=True)
    if result.returncode:
        raise SystemExit(f"无法加载 macOS 后台服务。请在已登录的桌面账户中打开终端重试。日志：{log}")
    print(json.dumps(endpoint))


def main():
    if sys.platform != "darwin":
        raise SystemExit("此后台启动入口仅用于 macOS。")
    action = sys.argv[1] if len(sys.argv) > 1 else "status"
    if action not in {"start", "stop", "status"}:
        raise SystemExit("用法：local_service.py start|stop|status")
    raw_port = os.environ.get("PORT")
    if raw_port is not None and (not re.fullmatch(r"[0-9]{4,5}", raw_port) or not 1024 <= int(raw_port) <= 65535):
        raise SystemExit("端口必须是 1024 到 65535 之间的整数。")
    if action != "start" and not DATA.is_dir():
        print("此目录的后台服务未启动。")
        return
    for directory in (DATA.parent, DATA):
        if directory.is_symlink():
            raise SystemExit("程序数据目录不能是符号链接。")
        directory.mkdir(mode=0o700, exist_ok=True)
    with (DATA / ".local-service.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        run(action, int(raw_port) if raw_port else DEFAULT_PORT, raw_port is not None)


if __name__ == "__main__":
    main()
