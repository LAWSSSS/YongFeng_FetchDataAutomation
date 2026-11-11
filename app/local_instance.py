"""One production backend per user, regardless of its bundle or data path."""
from contextlib import contextmanager
import errno
from http.client import HTTPException
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import sys
import urllib.request

from .runtime import BUNDLE_ID


class InstanceBusy(RuntimeError):
    pass


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def health_at(port):
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())
    try:
        with opener.open(f"http://127.0.0.1:{port}/api/health", timeout=0.5) as response:
            health = json.loads(response.read(8192))
    except (HTTPException, OSError, ValueError):
        return {}
    if (isinstance(health, dict) and health.get("ok") is True
            and health.get("app") == "inspection-video-gather-simple"
            and health.get("bundleId") in (BUNDLE_ID, "development")):
        return health
    return {}


def isolated_directory():
    # The package smoke test supplies both flags. A different profile alone
    # never isolates a normal installation from the production lock.
    test_root = os.environ.get("IVG_TEST_INSTANCE_DIR")
    if test_root and os.environ.get("IVG_SKIP_SAVED_CONNECTIONS") == "1":
        return Path(test_root).resolve()
    return None


def lock_directory():
    if isolated := isolated_directory():
        return isolated
    if sys.platform == "darwin":
        return Path.home() / "Library/Application Support/InspectionVideoGatherSimple/instance"
    if sys.platform == "win32":
        return Path.home() / "AppData/Local/InspectionVideoGatherSimple/instance"
    return Path.home() / ".local/state/InspectionVideoGatherSimple/instance"


def _legacy_listeners():
    if sys.platform != "darwin" or isolated_directory():
        return
    # Do not scan ports on the network. Only inspect Python listeners owned by
    # this user, then confirm the app and process identities over loopback.
    try:
        result = subprocess.run(
            ["/usr/sbin/lsof", "-nP", "-a", "-u", str(os.getuid()), "-iTCP",
             "-sTCP:LISTEN", "-Fpcn"], capture_output=True, text=True, timeout=3)
    except (OSError, subprocess.TimeoutExpired):
        raise InstanceBusy("暂时无法检查旧版后台，请稍后再试。") from None
    if result.returncode not in (0, 1):
        raise InstanceBusy("暂时无法检查旧版后台，请稍后再试。")
    pid, python = 0, False
    for line in result.stdout.splitlines():
        if line.startswith("p") and line[1:].isdigit():
            pid, python = int(line[1:]), False
        elif line.startswith("c"):
            python = line[1:].lower().startswith("python")
        elif line.startswith("n") and python and pid > 1 and pid != os.getpid():
            address = re.fullmatch(r"n127\.0\.0\.1:(\d+)", line)
            if address and 0 < (port := int(address[1])) <= 65535:
                yield pid, port


def _check_legacy():
    for pid, port in _legacy_listeners():
        if health_at(port).get("pid") == pid:
            raise InstanceBusy(f"采集助手已在 http://127.0.0.1:{port}/ 运行。请先停止该后台，再打开此安装。")


@contextmanager
def backend_instance():
    """Hold the stable lock until the backend and its workers have shut down."""
    root = lock_directory()
    root.mkdir(parents=True, mode=0o700, exist_ok=True)
    directory = root.lstat()
    if not stat.S_ISDIR(directory.st_mode) or (hasattr(os, "getuid") and directory.st_uid != os.getuid()):
        raise InstanceBusy("后台锁目录不可用，请检查当前用户的应用数据目录。")
    fd = os.open(root / "backend.lock", os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0), 0o600)
    try:
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            raise InstanceBusy("后台锁文件不可用。")
        try:
            if sys.platform == "win32":
                import msvcrt
                msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as error:
            if error.errno not in (errno.EACCES, errno.EAGAIN, errno.EDEADLK):
                raise
            raise InstanceBusy("另一份采集助手正在运行或启动。请先停止该后台，再打开此安装。") from None
        _check_legacy()
        yield
    finally:
        os.close(fd)


def check_available():
    with backend_instance():
        pass
