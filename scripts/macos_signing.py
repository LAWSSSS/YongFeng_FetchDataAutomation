"""Sign native code and notarize the portable macOS disk image."""
from __future__ import annotations

from dataclasses import dataclass
import json
import os
from pathlib import Path
import re
import subprocess
from uuid import UUID


MACH_HEADERS = {b"\xcf\xfa\xed\xfe", b"\xfe\xed\xfa\xcf", b"\xca\xfe\xba\xbe", b"\xbe\xba\xfe\xca"}
IDENTITY_LINE = re.compile(
    r'^[ \t]*\d+\)[ \t]+([0-9A-F]{40}) "(Developer ID Application: .+\(([A-Z0-9]{10})\))"[ \t]*$',
    re.MULTILINE,
)


@dataclass(frozen=True)
class DeveloperIdentity:
    certificate: str
    team: str


def tool(*args, capture=False, check=True):
    return subprocess.run(
        [str(arg) for arg in args], check=check, capture_output=capture, text=True,
        env={**os.environ, "LC_ALL": "C"},
    )


def find_identity(selector):
    available = tool("/usr/bin/security", "find-identity", "-v", "-p", "codesigning", capture=True)
    matches = [
        DeveloperIdentity(certificate, team)
        for certificate, name, team in IDENTITY_LINE.findall(available.stdout)
        if selector == name or selector.upper() == certificate
    ]
    if len(matches) != 1:
        raise ValueError("钥匙串中没有匹配的 Developer ID Application 签名身份，请检查证书和私钥。")
    return matches[0]


def is_macho(path):
    if path.is_symlink() or not path.is_file():
        return False
    with path.open("rb") as stream:
        return stream.read(4) in MACH_HEADERS


def native_files(folder):
    return sorted(path for path in folder.rglob("*") if is_macho(path))


def verify_developer_code(path, identity, *, native=False):
    requirement = (
        "anchor apple generic and certificate leaf[field.1.2.840.113635.100.6.1.13] exists "
        f'and certificate leaf[subject.OU] = "{identity.team}"'
    )
    tool("/usr/bin/codesign", "--verify", "--strict", "-R=" + requirement, path)
    arch = ("--arch", "arm64") if native else ()
    details = tool("/usr/bin/codesign", "--display", "--verbose=4", *arch, path, capture=True).stderr
    flags = re.search(r"\bflags=0x([0-9a-f]+)", details)
    if native and (not flags or not int(flags[1], 16) & 0x10000):
        raise ValueError(f"缺少 Hardened Runtime：{path.name}")
    if not re.search(r"^Timestamp=", details, re.MULTILINE):
        raise ValueError(f"签名缺少可信时间戳：{path.name}")
    cdhash = re.search(r"^CDHash=([0-9a-f]{40})$", details, re.MULTILINE)
    if not cdhash:
        raise ValueError(f"无法读取签名指纹：{path.name}")
    return cdhash[1]


def sign_native(path, identity=None):
    if identity is None:
        tool("/usr/bin/codesign", "--force", "--sign", "-", "--timestamp=none", path, capture=True)
        tool("/usr/bin/codesign", "--verify", "--strict", path)
        return
    tool("/usr/bin/codesign", "--force", "--sign", identity.certificate, "--timestamp",
         "--options", "runtime", path, capture=True)
    verify_developer_code(path, identity, native=True)


def make_disk_image(payload, output):
    tool("/usr/bin/hdiutil", "create", "-format", "UDZO", "-fs", "HFS+", "-volname", "采集助手",
         "-srcfolder", payload, output)


def verify_notarized_image(path):
    tool("/usr/bin/xcrun", "stapler", "validate", path)
    assessment = tool("/usr/sbin/spctl", "--assess", "--type", "open",
                      "--context", "context:primary-signature", "--verbose=2", path, capture=True)
    if "source=Notarized Developer ID" not in assessment.stdout + assessment.stderr:
        raise ValueError("磁盘映像未通过 macOS 的公证发行检查。")


def notarize_image(path, identity, profile, *, identifier, code_hashes):
    tool("/usr/bin/codesign", "--force", "--sign", identity.certificate, "--timestamp",
         "--identifier", identifier, path)
    image_hash = verify_developer_code(path, identity)
    submission = tool("/usr/bin/xcrun", "notarytool", "submit", path, "--keychain-profile", profile,
                      "--wait", "--timeout", "30m", "--output-format", "json", capture=True, check=False)
    try:
        response = json.loads(submission.stdout)
        job_id = str(UUID(response["id"]))
        status = response["status"]
    except (ValueError, KeyError, TypeError, AttributeError):
        raise RuntimeError("无法完成 Apple 公证请求，请检查钥匙串公证配置和网络。") from None
    if submission.returncode or status != "Accepted":
        raise RuntimeError(f"Apple 公证未通过，请查看提交 {job_id} 的公证日志。")
    log = tool("/usr/bin/xcrun", "notarytool", "log", job_id, "--keychain-profile", profile, capture=True)
    report = json.loads(log.stdout)
    issues = report.get("issues") or []
    if (report.get("status") != "Accepted" or str(report.get("jobId")).lower() != job_id
            or any(issue.get("severity") == "error" for issue in issues)):
        raise ValueError(f"公证日志未通过检查，请查看提交 {job_id} 的公证日志。")
    covered = {str(entry.get("cdhash")).lower() for entry in report.get("ticketContents") or []}
    if not (set(code_hashes) | {image_hash}) <= covered:
        raise ValueError(f"公证票据未覆盖全部本地组件，请查看提交 {job_id} 的公证日志。")
    if issues:
        print(f"公证日志有 {len(issues)} 条提示，提交编号 {job_id}。")
    tool("/usr/bin/xcrun", "stapler", "staple", path)
    verify_notarized_image(path)
