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
from scripts.package_safety import (PORTABLE_RESOURCES, inventory_digest, inventory_tree,
                                    source_inventory, verify_tree)


def sha(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def build_assets(folder, output):
    manifest = json.loads((folder / "文件校验.json").read_text())
    verify_tree(folder, manifest)
    if manifest.get("version") != VERSION:
        raise ValueError("Package version does not match the current source.")
    resources = folder / PORTABLE_RESOURCES
    source_hash = json.loads((resources / "release.json").read_text()).get("sourceTreeSha256")
    if source_hash != inventory_digest(source_inventory(ROOT)):
        raise ValueError("Package was built from different source files. Rebuild the package before exporting.")
    destination = output / f"gather-assistant-{VERSION}-macos-arm64.zip"
    # Runtime signatures and executable modes survive Archive Utility extraction.
    # Source files have no resource forks; exclude incidental build metadata.
    with tempfile.TemporaryDirectory(prefix=".package-", dir=output) as staging:
        package = Path(staging) / folder.name
        # Developers get a separate source archive. The user runs the command
        # beside the offline guide directly after extracting the download.
        shutil.copytree(folder, package, symlinks=True,
                        ignore=shutil.ignore_patterns("源码与模型.zip", "文件校验.json"))
        delivered = {**manifest, "files": inventory_tree(package)}
        (package / "文件校验.json").write_text(json.dumps(delivered, ensure_ascii=False, indent=2) + "\n")
        subprocess.run(["/usr/bin/ditto", "-c", "-k", "--norsrc", "--keepParent", str(package),
                        str(destination.resolve())], check=True)
    shutil.copy2(folder / "源码与模型.zip", output / f"source-and-models-{VERSION}.zip")
    model_manifest = json.loads((ROOT / "assets/models/manifest.json").read_text())
    with zipfile.ZipFile(output / "models-v1.zip", "w", compression=zipfile.ZIP_DEFLATED) as archive:
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
        shutil.copy2(source, output / source.name)
    shutil.copy2(ROOT / lock["ffmpeg"]["patch"], output)
    for name in ("README.md", "THIRD_PARTY_NOTICES.md", "packaging.lock.json"):
        shutil.copy2(ROOT / name, output / name)
    if source_hash != inventory_digest(source_inventory(ROOT)):
        raise ValueError("Source changed while exporting release assets. Rebuild before exporting.")
    paths = sorted(path for path in output.iterdir() if path.is_file())
    for path in paths:
        if path.stat().st_size >= 2 * 1024**3:
            raise ValueError(f'Release attachment exceeds GitHub limit: {path.name}')
    (output / "SHA256SUMS").write_text("".join(f"{sha(path)}  {path.name}\n" for path in paths))
    return [{"name": path.name, "bytes": path.stat().st_size} for path in paths]


def export(folder, output):
    if output.exists() or output.is_symlink():
        raise ValueError("Release output exists; choose a new folder.")
    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=".release-", dir=output.parent) as staging:
        ready = Path(staging) / "assets"
        ready.mkdir()
        assets = build_assets(folder, ready)
        if output.exists() or output.is_symlink():
            raise ValueError("Release output appeared during export; choose a new folder.")
        ready.rename(output)
    return assets


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("folder", type=Path)
    parser.add_argument("--output", type=Path, default=ROOT / "releases" / f"v{VERSION}")
    args = parser.parse_args()
    print(json.dumps({"assets": export(args.folder, args.output)}, indent=2))


if __name__ == "__main__":
    main()
