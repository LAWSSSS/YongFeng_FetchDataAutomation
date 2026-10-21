"""Repair the archive container only; JPEG bytes and model decisions never change."""
import tempfile
import zipfile
from pathlib import Path

from .audit_moves import checked_path, source_fingerprint
from .media import publish_file


def legacy_layout(item):
    try:
        source = checked_path(item["path"])
        if source_fingerprint(source) != item.get("fileVersion"):
            return False
        names = item.get("images") or []
        if not names or any(Path(name).name != name for name in names):
            return False
        with zipfile.ZipFile(source) as archive:
            return sorted(archive.namelist()) == sorted("image/" + name for name in names)
    except (KeyError, OSError, ValueError, zipfile.BadZipFile):
        return False


def repair_archive(item):
    source = checked_path(item["path"])
    if not legacy_layout(item):
        raise ValueError("原压缩包版本或目录不符，未改动文件。")
    revision = int(item.get("archiveRevision", 0)) + 1
    destination = source.with_name(f"{source.stem}_r{revision}.zip")
    checked_path(str(destination))
    if destination.exists():
        raise ValueError("修复位置已有同名文件，请先核对。")
    with tempfile.TemporaryDirectory(dir=source.parent, prefix=".simple-repack-") as temporary:
        prepared = Path(temporary) / destination.name
        with zipfile.ZipFile(source) as incoming, zipfile.ZipFile(prepared, "w", compression=zipfile.ZIP_DEFLATED) as outgoing:
            for name in item["images"]:
                info = incoming.getinfo("image/" + name)
                if not 0 < info.file_size <= 30 * 1024 * 1024:
                    raise ValueError("照片大小不符合平台要求，原文件保留。")
                blob = incoming.read(info)
                if not blob.startswith(b"\xff\xd8") or not blob.endswith(b"\xff\xd9"):
                    raise ValueError("照片内容异常，原文件保留。")
                outgoing.writestr("images/" + name, blob)
        if source_fingerprint(source) != item["fileVersion"]:
            raise ValueError("原压缩包已变化，未继续修复。")
        publish_file(prepared, destination)
    return destination, source_fingerprint(destination), revision
