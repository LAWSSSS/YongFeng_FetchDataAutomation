"""High-confidence credential scan; print locations/types, never matched values."""
import argparse
import hashlib
import json
import re
import subprocess
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PATTERNS = {
    "private-key": re.compile(rb"-----BEGIN (?:RSA |OPENSSH |EC |DSA )?PRIVATE KEY-----"),
    "github-token": re.compile(rb"\b(?:gh[pousr]_[A-Za-z0-9]{30,}|github_pat_[A-Za-z0-9_]{50,})\b"),
    "provider-token": re.compile(rb"\bsk-(?:proj-|sp-)?[A-Za-z0-9_.-]{32,}\b"),
    "embedded-url-password": re.compile(rb"https?://[^\s/:\"']{1,100}:[^\s/@\"']{4,100}@"),
    "aws-access-key": re.compile(rb"\b(?:AKIA|ASIA)[A-Z0-9]{16}\b"),
}
PRIVATE_NAMES = {"credentials.json", ".env", "collection.sqlite3", "server.json", "profile.json"}
TEXT_SUFFIXES = {".py", ".md", ".json", ".txt", ".toml", ".ini", ".cfg", ".xml", ".yaml", ".yml",
                 ".sh", ".command", ".mjs", ".tsx", ".ts", ".js", ".css", ".pem", ".key"}


def issues(data, *, documentation=False):
    return [kind for kind, expression in PATTERNS.items() if expression.search(data)
            and not (documentation and kind == "embedded-url-password")]


def archive_issues(path):
    """Inspect bundled source text in place; never extract user archives."""
    findings = []
    with zipfile.ZipFile(path) as archive:
        for info in archive.infolist():
            name = Path(info.filename)
            if name.name in PRIVATE_NAMES or name.suffix.lower() in {".sqlite3", ".db", ".mp4", ".mov", ".jpg", ".jpeg"}:
                findings.append((info.filename, "private-runtime-file"))
            if name.suffix.lower() in TEXT_SUFFIXES:
                if info.file_size > 30 * 1024 ** 2:
                    findings.append((info.filename, "unscanned-large-text"))
                else:
                    findings.extend((info.filename, kind) for kind in issues(archive.read(info)))
    return findings


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--directory", type=Path)
    parser.add_argument("--history", action="store_true")
    args = parser.parse_args()
    failures = []
    count = 0
    if args.directory:
        documentation = json.loads((ROOT / "assets/vendor-documentation.json").read_text())["files"]
        paths = [path for path in args.directory.rglob("*") if path.is_file() and not path.is_symlink()]
        for path in paths:
            relative = str(path.relative_to(args.directory))
            if path.name in PRIVATE_NAMES or path.suffix in {".sqlite3", ".db", ".mp4", ".mov", ".jpg", ".jpeg"}:
                failures.append((relative, "private-runtime-file"))
            if path.suffix.lower() == ".zip":
                failures.extend((relative + "!" + name, kind) for name, kind in archive_issues(path))
            if path.suffix.lower() in TEXT_SUFFIXES and path.stat().st_size < 30 * 1024 ** 2:
                count += 1
                data = path.read_bytes()
                known_documentation = any(relative.endswith("runtime/" + name) and
                    hashlib.sha256(data).hexdigest() == checksum for name, checksum in documentation.items())
                failures.extend((relative, kind) for kind in issues(data, documentation=known_documentation))
    else:
        names = subprocess.check_output(["git", "ls-files", "--cached", "--others", "--exclude-standard", "-z"], cwd=ROOT).split(b"\0")
        for name in set(names):
            if not name:
                continue
            path = ROOT / name.decode()
            if path.is_file():
                count += 1
                failures.extend((str(path.relative_to(ROOT)), kind) for kind in issues(path.read_bytes()))
    if args.history:
        objects = subprocess.check_output(["git", "rev-list", "--objects", "--all"], cwd=ROOT).decode().splitlines()
        for row in objects:
            identity, _, name = row.partition(" ")
            if not name:
                continue
            kind = subprocess.check_output(["git", "cat-file", "-t", identity], cwd=ROOT).strip()
            if kind != b"blob":
                continue
            count += 1
            content = subprocess.check_output(["git", "cat-file", "blob", identity], cwd=ROOT)
            failures.extend((f"history:{identity[:12]}:{name}", finding) for finding in issues(content))
    for path, kind in sorted(set(failures)):
        print(f"{kind}: {path}")
    print(f"Scanned {count} files/blobs; {len(set(failures))} findings.")
    raise SystemExit(1 if failures else 0)


if __name__ == "__main__":
    main()
