"""Fetch exact Release model files with gh (credentials stay in gh's credential store)."""
import argparse
import hashlib
import json
import shutil
import subprocess
import tempfile
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def digest(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def verify(directory):
    manifest = json.loads((ROOT / "assets/models/manifest.json").read_text())
    for entry in manifest["files"]:
        path = directory / entry["path"]
        if (not path.is_file() or path.is_symlink() or not path.resolve().is_relative_to(directory.resolve())
                or path.stat().st_size != entry["bytes"] or digest(path) != entry["sha256"]):
            raise ValueError(f'Model resource checksum mismatch: {entry["path"]}')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--tag", default="v1.1.0")
    parser.add_argument("--destination", type=Path, default=ROOT / "build/models")
    parser.add_argument("--archive", type=Path)
    args = parser.parse_args()
    manifest = json.loads((ROOT / "assets/models/manifest.json").read_text())
    with tempfile.TemporaryDirectory(prefix="ivg-models-") as temporary:
        work = Path(temporary)
        archive = args.archive
        if archive is None:
            subprocess.run(["gh", "release", "download", args.tag, "--repo", manifest["repository"],
                            "--pattern", manifest["releaseAsset"], "--dir", str(work)], check=True)
            archive = work / manifest["releaseAsset"]
        allowed = {item["path"] for item in manifest["files"]}
        with zipfile.ZipFile(archive) as bundle:
            if len(bundle.namelist()) != len(allowed) or set(bundle.namelist()) != allowed:
                raise ValueError("Unexpected model resource archive contents.")
            for info in bundle.infolist():
                entry = next(item for item in manifest["files"] if item["path"] == info.filename)
                if info.file_size > entry.get("bytes", 1024 * 1024) or (info.external_attr >> 16) & 0o170000 == 0o120000:
                    raise ValueError("Unexpected model resource size or symbolic link.")
            bundle.extractall(work / "models")
        verify(work / "models")
        for name in allowed:
            target = args.destination / name
            if (target.is_symlink() or not target.resolve().is_relative_to(args.destination.resolve())):
                raise ValueError(f"Refusing a model resource symlink: {name}")
            if target.exists() and digest(target) != next(
                    entry["sha256"] for entry in manifest["files"] if entry["path"] == name):
                raise ValueError(f"Refusing to overwrite a different model resource: {name}")
        shutil.copytree(work / "models", args.destination, dirs_exist_ok=True)
    print("Model resources verified and ready.")


if __name__ == "__main__":
    main()
