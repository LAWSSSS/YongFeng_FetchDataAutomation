"""Local-only package galleries, with expiring read leases and bounded ZIP reads."""
import asyncio
from collections import OrderedDict
from contextlib import ExitStack
import io
import os
from pathlib import Path, PurePosixPath
import stat
import time
from threading import RLock
from uuid import uuid4
import zipfile

from .audit_moves import source_fingerprint
from .videos import Conflict
from .file_leases import finish_io


MAX_IMAGE = 32 * 1024 * 1024


class PackageGallery:
    def __init__(self, packages, leases):
        self.packages, self.leases = packages, leases
        self.sessions, self.thumbnails = {}, OrderedDict()
        self.thumbnail_lock = RLock()
        self.task = None

    async def start(self):
        async def reap():
            while True:
                await asyncio.sleep(15)
                self.prune()
        self.task = asyncio.create_task(reap())

    async def shutdown(self):
        if self.task:
            self.task.cancel()
            await asyncio.gather(self.task, return_exceptions=True)
        for session in list(self.sessions):
            self.close(session)

    def prune(self):
        for key, value in list(self.sessions.items()):
            if value["expires"] < time.monotonic():
                self.close(key)

    def close(self, session):
        value = self.sessions.pop(session, None)
        if value:
            value["guard"].close()

    def package(self, identity):
        package = self.packages.store.get("items", identity)
        if not package:
            raise ValueError("照片包记录不存在。")
        return package

    @staticmethod
    def names(package):
        names = package.get("images") or []
        if len(names) != len(set(names)) or any(not isinstance(n, str) or PurePosixPath(n).name != n
                or "\\" in n or n in {".", ".."} or not n.lower().endswith((".jpg", ".jpeg")) for n in names):
            raise ValueError("照片包清单异常，不能读取。")
        return names

    @staticmethod
    def valid_file(path, version):
        if not version:
            return False
        try:
            return source_fingerprint(Path(path)) == version
        except FileNotFoundError:
            return False

    def inspect(self, package):
        names = self.names(package)
        paths = {}
        for name in names:
            raw = package.get("imagePaths", {}).get(name) or str(Path(package.get("imageDir") or "/") / name)
            if self.valid_file(raw, package.get("imageVersions", {}).get(name)):
                paths[name] = raw
        archive = package.get("path") or ""
        valid_archive = bool(archive and self.valid_file(archive, package.get("fileVersion")))
        zip_names = {}
        if valid_archive:
            with zipfile.ZipFile(archive) as opened:
                for entry in opened.infolist():
                    if entry.is_dir():
                        continue
                    member = PurePosixPath(entry.filename)
                    if (member.is_absolute() or ".." in member.parts or "\\" in entry.filename
                            or stat.S_ISLNK(entry.external_attr >> 16) or entry.flag_bits & 1
                            or entry.file_size > MAX_IMAGE or entry.file_size > max(1, entry.compress_size) * 250):
                        raise ValueError("压缩包包含异常条目，不能读取。")
                    name = member.name
                    if name not in names or entry.filename not in {name, "images/" + name, "image/" + name}:
                        raise ValueError("压缩包内容与登记清单不符。")
                    if name in zip_names:
                        raise ValueError("压缩包包含重名照片，不能读取。")
                    zip_names[name] = entry.filename
        available = [name for name in names if name in paths or name in zip_names]
        return {"paths": paths, "zipNames": zip_names, "archive": archive if valid_archive else "", "names": available,
                "expectedCount": len(names), "available": bool(available)}

    async def view(self, identity, existing=None):
        self.prune()
        if existing:
            session = self.session(identity, existing)
            session["expires"] = time.monotonic() + 120
            return self.public(identity, existing, session)
        package = self.package(identity)
        guard = ExitStack()
        # Register the exact managed files before probing them, so cleanup
        # cannot pass its busy check during inspection.
        paths = {package.get("path")} | set(package.get("imagePaths", {}).values())
        paths |= {str(Path(package.get("imageDir") or "/") / n) for n in self.names(package)}
        try:
            for path in sorted(p for p in paths if p):
                guard.enter_context(self.leases.read(path))
            inspected = await finish_io(self.inspect, package)
            if not inspected["available"]:
                guard.close()
                return {"available": False, "message": "本地照片已清理或没有可用照片包。", "packageId": identity}
            key = uuid4().hex
            session = {**inspected, "package": package, "guard": guard, "expires": time.monotonic() + 120}
            self.sessions[key] = session
            return self.public(identity, key, session)
        except BaseException:
            guard.close()
            raise

    @staticmethod
    def public(identity, key, session):
        package = session["package"]
        return {"available": True, "packageId": identity, "sessionId": key, "category": package.get("category"),
                "name": Path(package.get("videoPath") or package.get("name") or "照片包").stem,
                "count": len(session["names"]), "expectedCount": session["expectedCount"],
                "canDownload": bool(session["archive"])}

    def session(self, identity, key):
        self.prune()
        session = self.sessions.get(key)
        if not session or session["package"]["id"] != identity:
            raise Conflict("照片查看已结束，请重新打开这个照片包。")
        return session

    def contents(self, identity, key, page=1):
        session = self.session(identity, key)
        names = session["names"]
        pages = max(1, (len(names) + 23) // 24)
        page = max(1, min(page, pages))
        return {"names": names[(page - 1)*24:page*24], "page": page, "pages": pages, "count": len(names)}

    def read(self, identity, name, key=None, thumbnail=False):
        session = self.session(identity, key) if key else None
        package = session["package"] if session else self.package(identity)
        if name not in self.names(package):
            raise ValueError("照片不属于这个照片包。")
        cache_key = (identity, name, str(package.get("fileVersion")), str(package.get("imageVersions", {}).get(name)))
        if thumbnail:
            with self.thumbnail_lock:
                if cache_key in self.thumbnails:
                    self.thumbnails.move_to_end(cache_key)
                    return self.thumbnails[cache_key]
        with ExitStack() as guard:
            raw = package.get("imagePaths", {}).get(name) or str(Path(package.get("imageDir") or "/") / name)
            guard.enter_context(self.leases.read(raw))
            if self.valid_file(raw, package.get("imageVersions", {}).get(name)):
                if Path(raw).stat().st_size > MAX_IMAGE:
                    raise ValueError("照片超过可查看大小。")
                with os.fdopen(os.open(raw, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)), "rb") as source:
                    from .audit_moves import fingerprint
                    if fingerprint(os.fstat(source.fileno())) != package["imageVersions"][name]:
                        raise ValueError("照片版本已变化。")
                    data = source.read(MAX_IMAGE + 1)
            else:
                archive = package.get("path") or ""
                guard.enter_context(self.leases.read(archive))
                if session:
                    info = session if self.valid_file(archive, package.get("fileVersion")) else {"zipNames": {}}
                else:
                    info = self.inspect(package)
                member = info["zipNames"].get(name)
                if not member:
                    raise ValueError("本地照片已清理或文件版本已变化。")
                with os.fdopen(os.open(archive, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)), "rb") as source:
                    from .audit_moves import fingerprint
                    if fingerprint(os.fstat(source.fileno())) != package["fileVersion"]:
                        raise ValueError("压缩包版本已变化。")
                    with zipfile.ZipFile(source) as opened:
                        with opened.open(member) as stream:
                            data = stream.read(MAX_IMAGE + 1)
            if len(data) > MAX_IMAGE or not data.startswith(b"\xff\xd8") or not data.endswith(b"\xff\xd9"):
                raise ValueError("照片内容异常。")
        if thumbnail:
            from PIL import Image
            with Image.open(io.BytesIO(data)) as image:
                if image.width * image.height > 25_000_000:
                    raise ValueError("照片尺寸异常。")
                image.thumbnail((480, 300))
                output = io.BytesIO(); image.convert("RGB").save(output, format="JPEG", quality=80)
                data = output.getvalue()
            with self.thumbnail_lock:
                self.thumbnails[cache_key] = data
                while len(self.thumbnails) > 64:
                    self.thumbnails.popitem(last=False)
        return data

    async def download(self, identity, key):
        session = self.session(identity, key)
        package = session["package"]
        path = session["archive"]
        guard = ExitStack()
        try:
            guard.enter_context(self.leases.read(path))
            if not path or not await asyncio.to_thread(self.valid_file, path, package.get("fileVersion")):
                raise ValueError("本地压缩包已清理或文件版本已变化。")
            return Path(path), guard
        except BaseException:
            guard.close()
            raise
