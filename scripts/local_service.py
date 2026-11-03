"""Manage this checkout's macOS backend independently of the starting terminal."""
import hashlib
import os
from pathlib import Path
import plistlib
import re
import socket
import subprocess
import sys
import time


ROOT = Path(__file__).resolve().parents[1]


def main():
    if sys.platform != "darwin":
        raise SystemExit("此后台启动入口仅用于 macOS。")
    action = sys.argv[1] if len(sys.argv) > 1 else "status"
    if action not in {"start", "stop", "status"}:
        raise SystemExit("用法：local_service.py start|stop|status")
    port = int(os.environ.get("PORT", "8786"))
    if not 1024 <= port <= 65535:
        raise SystemExit("端口必须在1024到65535之间。")
    identity = hashlib.sha256(str(ROOT).encode()).hexdigest()[:12]
    label = f"cn.local.inspection-video.{identity}.{port}"
    domain = f"gui/{os.getuid()}"
    target = f"{domain}/{label}"
    existing = subprocess.run(["launchctl", "print", target], capture_output=True, text=True)
    loaded = existing.returncode == 0
    if action == "status":
        print("后台服务已加载。" if loaded else "后台服务未启动。")
        return
    if action == "stop":
        if loaded:
            result = subprocess.run(["launchctl", "bootout", target], capture_output=True)
            if result.returncode:
                raise SystemExit("停止失败，请等待当前文件操作完成后重试。")
            pid = re.search(r"\bpid = (\d+)", existing.stdout)
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
    if loaded:
        print(f"后台服务已加载：http://127.0.0.1:{port}/")
        return
    with socket.socket() as probe:
        # Match the server's bind flags: recently closed connections may still
        # be in TIME_WAIT even though no service is listening.
        probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            probe.bind(("127.0.0.1", port))
        except OSError:
            raise SystemExit("端口已被其他程序占用，请使用 PORT 指定其他端口。") from None
    data = ROOT / "数据" / "系统"
    data.mkdir(parents=True, exist_ok=True)
    configuration = {
        "Label": label,
        "ProgramArguments": [str(ROOT / ".venv/bin/python"), "-u", "-m", "app.main"],
        "WorkingDirectory": str(ROOT),
        "EnvironmentVariables": {"PORT": str(port), "IVG_INSTANCE_ID": label},
        "RunAtLoad": True,
        # The browser waits on this local HTTP server. launchd's default
        # Standard class throttles disk I/O even for these foreground requests;
        # XPC-based Adaptive promotion does not apply to HTTP connections.
        "ProcessType": "Interactive",
        "KeepAlive": {"SuccessfulExit": False},
        "ThrottleInterval": 10,
        "StandardOutPath": str(data / "local-service.log"),
        "StandardErrorPath": str(data / "local-service.log"),
    }
    path = data / f"local-service-{port}.plist"
    path.write_bytes(plistlib.dumps(configuration))
    path.chmod(0o600)
    result = subprocess.run(["launchctl", "bootstrap", domain, str(path)], capture_output=True)
    if result.returncode:
        raise SystemExit("后台服务启动失败，请查看 数据/系统/local-service.log。")
    print(f"后台服务启动中：http://127.0.0.1:{port}/")


if __name__ == "__main__":
    main()
