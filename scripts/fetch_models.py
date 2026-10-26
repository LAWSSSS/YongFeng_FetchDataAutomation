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


def verify(directory):
    manifest = json.loads((ROOT / "assets/models/manifest.json").read_text())
    for entry in manifest["files"]:
        path = directory / entry["path"]
        if not path.is_file() or hashlib.file_digest(path.open("rb"), "sha256").hexdigest() != entry["sha256"]:
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
            if set(bundle.namelist()) != allowed:
                raise ValueError("Unexpected model resource archive contents.")
            for info in bundle.infolist():
                entry = next(item for item in manifest["files"] if item["path"] == info.filename)
                if info.file_size > entry.get("bytes", 1024 * 1024) or (info.external_attr >> 16) & 0o170000 == 0o120000:
                    raise ValueError("Unexpected model resource size or symbolic link.")
            bundle.extractall(work / "models")
        verify(work / "models")
        for name in allowed:
            target = args.destination / name
            if target.exists() and hashlib.file_digest(target.open("rb"), "sha256").hexdigest() != next(
                    entry["sha256"] for entry in manifest["files"] if entry["path"] == name):
                raise ValueError(f"Refusing to overwrite a different model resource: {name}")
        shutil.copytree(work / "models", args.destination, dirs_exist_ok=True)
    print("Model resources verified and ready.")


if __name__ == "__main__":
    main()
