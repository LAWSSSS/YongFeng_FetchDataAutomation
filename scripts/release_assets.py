"""Create the complete zip, allowlisted model zip, sources and SHA256SUMS."""
import argparse
import hashlib
import json
import shutil
import subprocess
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def sha(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("folder", type=Path)
    parser.add_argument("--output", type=Path, default=ROOT / "releases/v1.1.0")
    args = parser.parse_args()
    if args.output.exists():
        raise ValueError("Release output exists; choose a new folder.")
    args.output.mkdir(parents=True)
    manifest = json.loads((args.folder / "文件校验.json").read_text())
    for entry in manifest["files"]:
        if sha(args.folder / entry["path"]) != entry["sha256"]:
            raise ValueError(f'Package file changed: {entry["path"]}')
    destination = args.output / "gather-assistant-1.1.0-macos-arm64.zip"
    # App signatures and executable modes survive Archive Utility extraction.
    # Source files have no resource forks; exclude incidental build metadata.
    subprocess.run(["/usr/bin/ditto", "-c", "-k", "--norsrc", "--keepParent", str(args.folder.resolve()),
                    str(destination.resolve())], check=True)
    resources = args.folder / "采集助手.app/Contents/Resources"
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
    shutil.copy2(ROOT / "docs/releasing.md", args.output / "BUILD.md")
    shutil.copy2(ROOT / "docs/third-party.md", args.output / "THIRD-PARTY.md")
    shutil.copy2(ROOT / "packaging.lock.json", args.output / "packaging.lock.json")
    paths = sorted(path for path in args.output.iterdir() if path.is_file())
    for path in paths:
        if path.stat().st_size >= 2 * 1024**3:
            raise ValueError(f'Release attachment exceeds GitHub limit: {path.name}')
    (args.output / "SHA256SUMS").write_text("".join(f"{sha(path)}  {path.name}\n" for path in paths))
    print(json.dumps({"assets": [{"name": path.name, "bytes": path.stat().st_size} for path in paths]}, indent=2))


if __name__ == "__main__":
    main()
