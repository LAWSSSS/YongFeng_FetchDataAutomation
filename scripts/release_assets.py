"""Create the Mac distribution, model archive, sources and SHA256SUMS."""
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
from app.runtime import BUNDLE_ID, VERSION
from scripts.macos_signing import find_identity, make_disk_image, native_files, notarize_image, verify_developer_code
from scripts.package_safety import PORTABLE_RESOURCES, inventory_tree, verify_tree


def sha(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def distribution_identity(package, selector, profile):
    signing = package.get("signing") or {"mode": "ad-hoc"}
    if not selector and not profile and signing.get("mode") == "ad-hoc":
        return None
    if not selector or not profile:
        raise ValueError("公证发行需同时指定 --signing-identity 和 --notary-profile。")
    identity = find_identity(selector)
    if signing != {"mode": "developer-id", "team": identity.team}:
        raise ValueError("请先用同一团队的 Developer ID Application 签名构建运行组件。")
    return identity


def package_distribution(folder, output, manifest, identity, profile):
    extension = "dmg" if identity else "zip"
    destination = output / f"gather-assistant-{VERSION}-macos-arm64.{extension}"
    with tempfile.TemporaryDirectory(prefix=".package-", dir=output) as staging:
        staging = Path(staging)
        package = staging / "payload" / folder.name
        shutil.copytree(folder, package, symlinks=True,
                        ignore=shutil.ignore_patterns("源码与模型.zip", "文件校验.json"))
        delivered = {**manifest, "files": inventory_tree(package)}
        (package / "文件校验.json").write_text(json.dumps(delivered, ensure_ascii=False, indent=2) + "\n")
        if identity:
            binaries = native_files(package / PORTABLE_RESOURCES)
            if not binaries or len(binaries) != manifest["binaryCount"]:
                raise ValueError("运行组件清单与实际本地代码数量不符。")
            code_hashes = {
                verify_developer_code(path, identity, native=True)
                for path in binaries
            }
            image = staging / destination.name
            make_disk_image(package.parent, image)
            notarize_image(image, identity, profile, identifier=BUNDLE_ID + ".disk-image", code_hashes=code_hashes)
            image.replace(destination)
        else:
            # Local builds use a ZIP. Preserve executable modes during extraction.
            subprocess.run(["/usr/bin/ditto", "-c", "-k", "--norsrc", "--keepParent", str(package),
                            str(destination.resolve())], check=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("folder", type=Path)
    parser.add_argument("--output", type=Path, default=ROOT / "releases" / f"v{VERSION}")
    parser.add_argument("--signing-identity", help="Developer ID Application name or certificate hash in the local Keychain")
    parser.add_argument("--notary-profile", help="notarytool Keychain profile name")
    args = parser.parse_args()
    if args.output.exists():
        raise ValueError("Release output exists; choose a new folder.")
    manifest = json.loads((args.folder / "文件校验.json").read_text())
    verify_tree(args.folder, manifest)
    if manifest.get("version") != VERSION:
        raise ValueError("Package version does not match the current source.")
    resources = args.folder / PORTABLE_RESOURCES
    package = json.loads((resources / "release.json").read_text())
    try:
        identity = distribution_identity(package, args.signing_identity, args.notary_profile)
    except ValueError as error:
        parser.error(str(error))
    args.output.mkdir(parents=True)
    package_distribution(args.folder, args.output, manifest, identity, args.notary_profile)
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
