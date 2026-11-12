"""Create the complete zip, allowlisted model zip, sources and SHA256SUMS."""
import argparse
import hashlib
import json
import shutil
import subprocess
import sys
import tempfile
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from app.runtime import VERSION
from scripts.package_safety import PORTABLE_RESOURCES, inventory_tree, verify_tree


def sha(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("folder", type=Path)
    parser.add_argument("--output", type=Path, default=ROOT / "releases" / f"v{VERSION}")
    args = parser.parse_args()
    if args.output.exists():
        raise ValueError("Release output exists; choose a new folder.")
    args.output.mkdir(parents=True)
    manifest = json.loads((args.folder / "文件校验.json").read_text())
    verify_tree(args.folder, manifest)
    if manifest.get("version") != VERSION:
        raise ValueError("Package version does not match the current source.")
    destination = args.output / f"gather-assistant-{VERSION}-macos-arm64.zip"
    # Runtime signatures and executable modes survive Archive Utility extraction.
    # Source files have no resource forks; exclude incidental build metadata.
    with tempfile.TemporaryDirectory(prefix=".package-", dir=args.output) as staging:
        package = Path(staging) / args.folder.name
        # Developers get a separate source archive. The user runs the command
        # beside the offline guide directly after extracting the download.
        shutil.copytree(args.folder, package, symlinks=True,
                        ignore=shutil.ignore_patterns("源码与模型.zip", "文件校验.json"))
        delivered = {**manifest, "files": inventory_tree(package)}
        (package / "文件校验.json").write_text(json.dumps(delivered, ensure_ascii=False, indent=2) + "\n")
        subprocess.run(["/usr/bin/ditto", "-c", "-k", "--norsrc", "--keepParent", str(package),
                        str(destination.resolve())], check=True)
    resources = args.folder / PORTABLE_RESOURCES
    shutil.copy2(args.folder / "源码与模型.zip", args.output / f"source-and-models-{VERSION}.zip")
    model_manifest = json.loads((ROOT / "assets/models/manifest.json").read_text())
    with zipfile.ZipFile(args.output / "models-v1.zip", "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for entry in model_manifest["files"]:
            path = resources / "models" / entry["path"]
            if sha(path) != entry["sha256"]:
                raise ValueError(f'Model resource changed: {entry["path"]}')
            archive.write(path, entry["path"])
    lock = json.loads((ROOT / "packaging.lock.json").read_text())
    for section in ("ffmpeg", "opencvEmbeddedFFmpeg", "opencvSource"):
        entry = lock[section]
        source = ROOT / "build/downloads" / entry["archive"]
        if sha(source) != entry["sha256"]:
            raise ValueError(f'Corresponding source checksum mismatch: {entry["archive"]}')
        shutil.copy2(source, args.output / source.name)
    shutil.copy2(ROOT / lock["ffmpeg"]["patch"], args.output)
    shutil.copy2(ROOT / "README.md", args.output / "README.md")
    shutil.copy2(ROOT / "THIRD_PARTY_NOTICES.md", args.output / "THIRD_PARTY_NOTICES.md")
    shutil.copy2(ROOT / "packaging.lock.json", args.output / "packaging.lock.json")
    paths = sorted(path for path in args.output.iterdir() if path.is_file())
    for path in paths:
        if path.stat().st_size >= 2 * 1024**3:
            raise ValueError(f'Release attachment exceeds GitHub limit: {path.name}')
    (args.output / "SHA256SUMS").write_text("".join(f"{sha(path)}  {path.name}\n" for path in paths))
    print(json.dumps({"assets": [{"name": path.name, "bytes": path.stat().st_size} for path in paths]}, indent=2))


if __name__ == "__main__":
    main()
