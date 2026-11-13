"""Exercise macOS first-use approval on isolated copies, never real user apps."""
import json
import os
from pathlib import Path
import pty
import select
import shutil
import subprocess
import sys
import time

import pytest

from scripts.build_mac import install_first_use
from scripts.package_safety import PORTABLE_COMMAND, PORTABLE_RESOURCES

pytestmark = pytest.mark.skipif(sys.platform != "darwin", reason="macOS bootstrap")
QUARANTINE = "com.apple.quarantine"
MARKER = "0083;69150e00;Safari;C264A041-6210-448D-8D61-9741876E7066"


def quarantine_files(*paths):
    subprocess.run(["/usr/bin/xattr", "-w", QUARANTINE, MARKER, *map(str, paths)], check=True, timeout=10)


@pytest.fixture
def package(tmp_path):
    folder = (tmp_path / "中文 包 with spaces").resolve()
    resources = folder / PORTABLE_RESOURCES
    native = resources / "runtime/bin/python3.11"
    native.parent.mkdir(parents=True)
    # echo is a real native binary, harmless if invoked as the fake interpreter.
    shutil.copyfile("/bin/echo", native)
    native.chmod(0o755)
    subprocess.run(["/usr/bin/codesign", "--force", "--sign", "-", "--timestamp=none", str(native)],
                   check=True, capture_output=True, timeout=10)
    launcher = resources / "application/launcher.py"
    launcher.parent.mkdir()
    launcher.write_text("# A checksummed fixture, never executed.\n")
    (native.parent / "python").symlink_to("python3.11")
    command = folder / PORTABLE_COMMAND
    install_first_use(resources, command)
    quarantine_files(resources, native, launcher, command)
    return folder, resources, native


def helper(package, mode):
    _, resources, _ = package
    return subprocess.run(["/usr/bin/osascript", "-l", "JavaScript",
                           str(resources / "prepare-first-run.js"), mode, str(resources)],
                          capture_output=True, text=True, timeout=30)


def has_quarantine(path):
    return subprocess.run(["/usr/bin/xattr", "-p", QUARANTINE, str(path)],
                          capture_output=True, timeout=10).returncode == 0


def test_prepare_checks_content_and_only_changes_registered_native_files(package, tmp_path):
    folder, resources, native = package
    neighbor = tmp_path / "unrelated app"
    neighbor.write_text("Do not change me")
    quarantine_files(neighbor)
    data = folder / "数据"
    data.mkdir()
    media = data / "user-video"
    media.write_text("User data")
    quarantine_files(media)
    assert helper(package, "check").stdout.strip() == "approval-required"
    result = helper(package, "approve")
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "ready"
    assert not has_quarantine(native)
    assert helper(package, "check").stdout.strip() == "ready"
    assert helper(package, "approve").stdout.strip() == "ready"
    for path in (neighbor, media, folder / PORTABLE_COMMAND, resources,
                 resources / "application/launcher.py"):
        assert has_quarantine(path)


@pytest.mark.parametrize("failure", ["same_size_change", "missing", "additional_file", "external_link", "parent_link", "hard_link"])
def test_invalid_package_is_never_approved(package, tmp_path, failure):
    _, resources, native = package
    launcher = resources / "application/launcher.py"
    if failure == "same_size_change":
        data = launcher.read_bytes()
        launcher.write_bytes(b"!" + data[1:])
    elif failure == "missing":
        launcher.unlink()
    elif failure == "additional_file":
        (resources / "runtime/extra.so").write_bytes(b"unexpected")
    elif failure == "external_link":
        (native.parent / "python").unlink()
        (native.parent / "python").symlink_to("/bin/echo")
    elif failure == "parent_link":
        moved = tmp_path / "outside"
        launcher.parent.rename(moved)
        (resources / "application").symlink_to(moved, target_is_directory=True)
    else:
        os.link(native, tmp_path / "shared-external-file")
    result = helper(package, "approve")
    assert result.returncode != 0
    assert has_quarantine(native)


def test_finder_metadata_does_not_require_new_package(package):
    (package[1] / ".DS_Store").write_bytes(b"Finder metadata")
    result = helper(package, "approve")
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize("action", ["start", "status", "stop"])
def test_noninteractive_commands_never_assume_consent(package, action):
    folder, _, native = package
    result = subprocess.run(["/bin/zsh", str(folder / PORTABLE_COMMAND), action],
                            input="y\n", capture_output=True, text=True, timeout=15)
    assert result.returncode != 0
    assert "请先在终端中启动一次" in result.stderr
    assert has_quarantine(native)


@pytest.mark.parametrize("name", ["prepare-first-run.js", "first-run.json"])
def test_pinned_bootstrap_inputs_cannot_be_replaced(package, name):
    folder, resources, native = package
    path = resources / name
    path.write_bytes(path.read_bytes() + b"\n")
    result = subprocess.run(["/bin/zsh", str(folder / PORTABLE_COMMAND)],
                            input="y\n", capture_output=True, text=True, timeout=15)
    assert result.returncode != 0
    assert "首次启动文件校验未通过" in result.stderr
    assert has_quarantine(native)


def run_interactive(command, answer):
    master, slave = pty.openpty()
    process = subprocess.Popen(["/bin/zsh", str(command)], stdin=slave, stdout=slave, stderr=slave)
    os.close(slave)
    output = b""
    try:
        deadline = time.monotonic() + 30
        sent = False
        while time.monotonic() < deadline:
            readable, _, _ = select.select([master], [], [], 0.1)
            if readable:
                try:
                    chunk = os.read(master, 8192)
                except OSError:
                    break
                if not chunk:
                    break
                output += chunk
                if b"[y/N]" in output and not sent:
                    os.write(master, (answer + "\n").encode())
                    sent = True
            elif process.poll() is not None:
                break
        assert process.wait(timeout=5) in (0, 1), output.decode()
        return process.returncode, output.decode()
    finally:
        if process.poll() is None:
            process.kill()
            process.wait()
        os.close(master)


def test_user_declines_then_approves_once(package):
    folder, resources, native = package
    command = folder / PORTABLE_COMMAND
    status, output = run_interactive(command, "")
    assert status == 1 and "已取消" in output
    assert has_quarantine(native)
    status, output = run_interactive(command, "y")
    assert status == 0, output
    assert not has_quarantine(native)
    assert "-I -B" in output  # The trusted fake interpreter is only reached now.
    status, output = run_interactive(command, "")
    assert status == 0 and "[y/N]" not in output
    assert has_quarantine(resources / "application/launcher.py")


def test_manifest_and_command_record_all_inputs(package):
    folder, resources, native = package
    manifest = json.loads((resources / "first-run.json").read_text())
    assert manifest["nativeFiles"] == [native.relative_to(resources).as_posix()]
    assert "prepare-first-run.js" in {entry["path"] for entry in manifest["files"]}
    assert "@FIRST_USE_" not in (folder / PORTABLE_COMMAND).read_text()


def test_approved_package_can_move_and_copy_without_another_prompt(package, tmp_path):
    folder, _, _ = package
    assert helper(package, "approve").returncode == 0
    moved = folder.rename(tmp_path / "已改名 程序")
    for target in (moved, tmp_path / "另一个位置"):
        if target != moved:
            shutil.copytree(moved, target, symlinks=True)
        result = subprocess.run(["/bin/zsh", str(target / PORTABLE_COMMAND), "status"],
                                input="", capture_output=True, text=True, timeout=15)
        assert result.returncode == 0, result.stderr
        assert "-I -B" in result.stdout
        assert "[y/N]" not in result.stdout
