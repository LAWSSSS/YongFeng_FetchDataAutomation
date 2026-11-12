"""Resume previously registered repairs for originals removed by legacy cleanup."""
from __future__ import annotations

import asyncio
import errno
import fcntl
import hashlib
import json
import os
import re
import stat
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import partial
from pathlib import Path
from uuid import UUID

import httpx

from .audit_moves import checked_path
from .domain import now_iso
from .file_leases import FileEnvironmentError, finish_io
from .upload_storage import runtime_management_credentials

CHUNK_SIZE = 8 * 1024 * 1024
LEGACY_CHUNK_SIZE = 32 * 1024 * 1024
RANGE_WORKERS = 2
RANGE_RETRIES = 3
RANGE_BACKOFF = (1, 2, 5, 10, 20, 30, 60)
MIN_RANGE_SIZE = 1024 * 1024
AUTH_DATASETS = ("27912", "27944")


class RestoreFailure(Exception):
    def __init__(self, code: str, message: str, *, retryable: bool = False):
        super().__init__(message)
        self.code, self.retryable = code, retryable


def edge_hash(first: bytes, last: bytes) -> str:
    return hashlib.sha256(first + last).hexdigest()


def _upload_marker(item: dict) -> str:
    version = item.get("uploadFileVersion") or item.get("originalFileVersion") or item["fileVersion"]
    return hashlib.sha256(json.dumps(version, sort_keys=True).encode()).hexdigest()


def _range_meta(client, item: dict, expected: dict) -> dict:
    obj = item["object"]
    base = {"Bucket": obj["bucket"], "Key": obj["key"]}
    try:
        head = client.head_object(**base)
    except Exception as error:
        response = getattr(error, "response", {}) or {}
        code = str((response.get("Error") or {}).get("Code", ""))
        if code in {"404", "NoSuchKey", "NotFound"}:
            raise RestoreFailure("identity", "绑定的云端对象不存在，未尝试其他文件。") from None
        if code not in {"403", "AccessDenied"}:
            raise RestoreFailure("network", "读取云端原片信息失败，将重试。", retryable=True) from None
        try:
            head = client.get_object(**base, Range="bytes=0-0")
        except Exception as get_error:
            code = str((((getattr(get_error, "response", {}) or {}).get("Error")) or {}).get("Code", ""))
            if code in {"403", "AccessDenied", "ExpiredToken", "InvalidToken", "SignatureDoesNotMatch"}:
                raise RestoreFailure("authorization", "云端读取授权暂不可用，保留进度并稍后重试。", retryable=True) from None
            raise RestoreFailure("network", "读取云端原片信息失败，将重试。", retryable=True) from None
        try:
            match = re.fullmatch(r"bytes 0-0/(\d+)", head.get("ContentRange", ""))
            total = int(match.group(1)) if match else None
            head["ContentLength"] = total
        finally:
            body = head.get("Body")
            if body:
                body.close()

    metadata = {str(k).lower(): str(v) for k, v in (head.get("Metadata") or {}).items()}
    if (head.get("ContentLength") != expected["size"] or item.get("size") != expected["size"]
            or metadata.get("uploader-id") != item["id"]
            or metadata.get("file-version") != _upload_marker(item)):
        raise RestoreFailure("identity", "云端对象身份或文件版本与原记录不符，未发布文件。")
    etag = head.get("ETag")
    if not isinstance(etag, str) or not etag:
        raise RestoreFailure("identity", "云端对象缺少版本校验信息，未发布文件。")
    return {"etag": etag, "versionId": head.get("VersionId") or ""}


def _atomic_json(path: Path, value: dict):
    fd, temporary = tempfile.mkstemp(prefix=path.name + ".", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as output:
            json.dump(value, output, ensure_ascii=False, sort_keys=True)
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, path)
    finally:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass


def _pwrite_all(fd: int, data: bytes, offset: int):
    view = memoryview(data)
    written = 0
    while written < len(view):
        amount = os.pwrite(fd, view[written:], offset + written)
        if amount <= 0:
            raise OSError(errno.EIO, "短写入")
        written += amount


def _hash_fd_range(fd: int, start: int, length: int):
    digest = hashlib.sha256()
    offset, remaining = start, length
    while remaining:
        block = os.pread(fd, min(1024 * 1024, remaining), offset)
        if not block:
            return None
        digest.update(block)
        offset += len(block)
        remaining -= len(block)
    return digest.hexdigest()


def _check_stop(stopping):
    if stopping and stopping():
        raise RestoreFailure("stopping", "程序正在停止，已保留校验通过的分段。")


def _stream_range(client, obj: dict, etag: str, version_id: str, total: int,
                  start: int, end: int, fd: int, *, retries: int, backoff,
                  sleep=time.sleep, stopping=None) -> str:
    length = end - start + 1
    for attempt in range(retries):
        _check_stop(stopping)
        response = None
        try:
            args = {"Bucket": obj["bucket"], "Key": obj["key"],
                    "Range": f"bytes={start}-{end}"}
            if version_id:
                args["VersionId"] = version_id
            else:
                args["IfMatch"] = etag
            response = client.get_object(**args)
            expected_range = f"bytes {start}-{end}/{total}"
            if (response.get("ContentRange") != expected_range
                    or response.get("ContentLength") != length
                    or response.get("ETag") != etag):
                raise RestoreFailure("identity", "云端返回的分段或对象版本不符。")
            body = response.get("Body")
            if body is None:
                raise OSError(errno.EIO, "缺少分段内容")
            digest, received, offset = hashlib.sha256(), 0, start
            while received < length:
                _check_stop(stopping)
                block = body.read(min(1024 * 1024, length - received))
                if not block:
                    break
                _pwrite_all(fd, block, offset)
                digest.update(block)
                received += len(block)
                offset += len(block)
            if received != length or body.read(1):
                raise OSError(errno.EIO, "分段长度异常")
            return digest.hexdigest()
        except RestoreFailure:
            raise
        except Exception as error:
            code = str((((getattr(error, "response", {}) or {}).get("Error")) or {}).get("Code", ""))
            if code in {"403", "AccessDenied", "ExpiredToken", "InvalidToken", "SignatureDoesNotMatch"}:
                raise RestoreFailure("authorization", "云端读取授权暂不可用，保留进度并稍后重试。", retryable=True) from None
            if code in {"412", "PreconditionFailed", "NoSuchVersion", "InvalidVersionId"}:
                raise RestoreFailure("identity", "云端对象版本已变化，未发布文件。") from None
            if attempt + 1 >= retries:
                raise RestoreFailure("network", "云端分段多次中断，保留已验证分段并稍后续传。", retryable=True) from None
            _check_stop(stopping)
            sleep(backoff[min(attempt, len(backoff) - 1)])
        finally:
            body = response.get("Body") if isinstance(response, dict) else None
            if body:
                try:
                    body.close()
                except Exception:
                    pass
    raise RestoreFailure("network", "云端分段未能下载。", retryable=True)


def _stream_range_resilient(client, obj: dict, etag: str, version_id: str, total: int,
                            start: int, end: int, fd: int, *, retries: int, backoff,
                            sleep=time.sleep, stopping=None):
    """Retry a large range, then split only that range if streaming stays unstable."""
    try:
        return _stream_range(client, obj, etag, version_id, total, start, end, fd,
                             retries=retries, backoff=backoff, sleep=sleep, stopping=stopping)
    except RestoreFailure as error:
        length = end - start + 1
        if error.code != "network" or length <= MIN_RANGE_SIZE:
            raise
        middle = start + length // 2 - 1
        _stream_range_resilient(client, obj, etag, version_id, total, start, middle, fd,
                                retries=retries, backoff=backoff, sleep=sleep, stopping=stopping)
        _stream_range_resilient(client, obj, etag, version_id, total, middle + 1, end, fd,
                                retries=retries, backoff=backoff, sleep=sleep, stopping=stopping)
        digest = _hash_fd_range(fd, start, length)
        if digest is None:
            raise RestoreFailure("network", "拆分分段未完整落盘，保留进度并稍后续传。", retryable=True)
        return digest


def _file_edge(path: Path) -> str:
    with path.open("rb") as stream:
        first = stream.read(65536)
        stream.seek(max(0, path.stat().st_size - 65536))
        last = stream.read(65536)
    return edge_hash(first, last)


def _publish_exclusive(part: Path, final: Path, expected_size: int, expected_edge: str):
    try:
        os.link(part, final)
        part.unlink()
        return
    except FileExistsError:
        if final.stat().st_size == expected_size and _file_edge(final) == expected_edge:
            part.unlink(missing_ok=True)
            return
        raise RestoreFailure("conflict", "目标文件已存在且内容不一致，未覆盖。") from None
    except OSError as error:
        if error.errno not in {errno.EPERM, errno.EOPNOTSUPP, errno.ENOTSUP, errno.EMLINK}:
            raise RestoreFailure("disk", "外置盘无法安全发布已校验文件，原目标未覆盖。", retryable=True) from None

    created = False
    try:
        source_digest = hashlib.sha256()
        target_digest = hashlib.sha256()
        with part.open("rb") as source:
            source_stat = os.fstat(source.fileno())
            if source_stat.st_size != expected_size:
                raise RestoreFailure("identity", "临时原片长度不符，未发布文件。")
            fd = os.open(final, os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0), 0o600)
            created = True
            with os.fdopen(fd, "wb") as target:
                for block in iter(lambda: source.read(4 * 1024 * 1024), b""):
                    target.write(block)
                    source_digest.update(block)
                    target_digest.update(block)
                target.flush()
                os.fsync(target.fileno())
        if (final.stat().st_size != expected_size or source_digest.digest() != target_digest.digest()
                or _file_edge(final) != expected_edge):
            raise RestoreFailure("identity", "最终文件校验失败，未确认恢复。")
        part.unlink()
    except FileExistsError:
        if final.stat().st_size == expected_size and _file_edge(final) == expected_edge:
            part.unlink(missing_ok=True)
            return
        raise RestoreFailure("conflict", "目标文件已存在且内容不一致，未覆盖。") from None
    except RestoreFailure:
        if created:
            final.unlink(missing_ok=True)
        raise
    except OSError as error:
        if created:
            final.unlink(missing_ok=True)
        raise RestoreFailure("disk", "外置盘写入失败，原片仍保留在临时文件中。", retryable=True) from None


def download_bound_object(client, item: dict, expected: dict, final_path: Path, *,
                          chunk_size: int = CHUNK_SIZE, workers: int = RANGE_WORKERS,
                          retries: int = RANGE_RETRIES, backoff=RANGE_BACKOFF, sleep=time.sleep,
                          progress=None, stopping=None):
    """Download one exact S3 object with independently retried, version-bound ranges."""
    final_path = Path(final_path)
    _check_stop(stopping)
    if final_path.exists():
        if (final_path.is_file() and final_path.stat().st_size == expected["size"]
                and _file_edge(final_path) == expected["edgeHash"]):
            return {"path": str(final_path), "size": expected["size"], "existing": True}
        raise RestoreFailure("conflict", "目标路径已有不同内容，未覆盖。")
    if not final_path.parent.is_dir():
        raise RestoreFailure("disk", "外置盘目标目录当前不可用。", retryable=True)

    metadata = _range_meta(client, item, expected)
    obj = item["object"]
    suffix = hashlib.sha256((item["id"] + metadata["etag"]).encode()).hexdigest()[:16]
    part = final_path.parent / f".{final_path.name}.restore-{suffix}.part"
    state_path = final_path.parent / f".{final_path.name}.restore-{suffix}.json"
    count = (expected["size"] + chunk_size - 1) // chunk_size
    expected_state = {"name": final_path.name, "size": expected["size"], "etag": metadata["etag"],
                      "chunkSize": chunk_size,
                      "versionId": metadata["versionId"], "chunks": {}}

    flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(part, flags, 0o600)
    try:
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            raise RestoreFailure("identity", "恢复临时文件不是普通文件。")
        # A process restart can reopen the same temporary file and continue. The
        # advisory lock prevents two web workers from writing the same ranges.
        fcntl.flock(fd, fcntl.LOCK_EX)
        if os.fstat(fd).st_size != expected["size"]:
            os.ftruncate(fd, expected["size"])
        saved = {}
        migrated = False
        try:
            old = json.loads(state_path.read_text(encoding="utf-8"))
            if all(old.get(key) == expected_state[key] for key in ("name", "size", "etag", "versionId")):
                old_chunk_size = int(old.get("chunkSize") or LEGACY_CHUNK_SIZE)
                old_saved = {int(k): v for k, v in old.get("chunks", {}).items()
                             if isinstance(v, str) and re.fullmatch(r"[a-f0-9]{64}", v)}
                if old_chunk_size == chunk_size:
                    saved = old_saved
                elif old_chunk_size > chunk_size and old_chunk_size % chunk_size == 0:
                    # Verify each old, larger range before deriving new subrange
                    # hashes. This safely preserves progress after a chunk-size
                    # adjustment without trusting partial or unverified bytes.
                    for old_index, expected_digest in old_saved.items():
                        _check_stop(stopping)
                        old_start = old_index * old_chunk_size
                        old_length = min(old_chunk_size, expected["size"] - old_start)
                        if old_index < 0 or old_length <= 0:
                            continue
                        if _hash_fd_range(fd, old_start, old_length) != expected_digest:
                            continue
                        new_index = old_start // chunk_size
                        old_end = old_start + old_length
                        while new_index * chunk_size < old_end:
                            new_start = new_index * chunk_size
                            new_length = min(chunk_size, expected["size"] - new_start)
                            if new_start < old_start or new_start + new_length > old_end:
                                break
                            digest = _hash_fd_range(fd, new_start, new_length)
                            if digest is None:
                                break
                            saved[new_index] = digest
                            new_index += 1
                    migrated = bool(saved)
        except (OSError, ValueError, TypeError):
            pass
        for index in list(saved):
            _check_stop(stopping)
            start = index * chunk_size
            end = min(expected["size"], start + chunk_size) - 1
            if index >= count or start > end:
                saved.pop(index, None)
                continue
            actual = _hash_fd_range(fd, start, end - start + 1)
            if actual != saved[index]:
                saved.pop(index, None)

        if migrated and saved:
            expected_state["chunks"] = {str(k): v for k, v in saved.items()}
            os.fsync(fd)
            _atomic_json(state_path, expected_state)
            if progress:
                progress(min(expected["size"], len(saved) * chunk_size), expected["size"])

        pending = []
        for index in range(count):
            if index in saved:
                continue
            start = index * chunk_size
            end = min(expected["size"], start + chunk_size) - 1
            pending.append((index, start, end))

        def write_progress(index, chunk_digest):
            saved[index] = chunk_digest
            expected_state["chunks"] = {str(k): v for k, v in saved.items()}
            os.fsync(fd)
            _atomic_json(state_path, expected_state)
            if progress:
                progress(min(expected["size"], len(saved) * chunk_size), expected["size"])

        with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
            futures = {pool.submit(_stream_range_resilient, client, obj, metadata["etag"], metadata["versionId"],
                                   expected["size"], start, end, fd,
                                   retries=retries, backoff=backoff, sleep=sleep, stopping=stopping): (index, start, end)
                       for index, start, end in pending}
            for future in as_completed(futures):
                index, _, _ = futures[future]
                try:
                    write_progress(index, future.result())
                except Exception:
                    for waiting in futures:
                        waiting.cancel()
                    raise
        if len(saved) != count:
            raise RestoreFailure("network", "部分原片分段尚未验证，保留进度并稍后续传。", retryable=True)
        os.fsync(fd)
    finally:
        os.close(fd)

    _check_stop(stopping)
    if part.stat().st_size != expected["size"] or _file_edge(part) != expected["edgeHash"]:
        raise RestoreFailure("identity", "恢复文件与原视频指纹不一致，未发布。")
    _publish_exclusive(part, final_path, expected["size"], expected["edgeHash"])
    state_path.unlink(missing_ok=True)
    return {"path": str(final_path), "size": expected["size"], "existing": False}


class OriginalRestoreStore:
    def __init__(self, db):
        self.db = db
        db.execute("CREATE TABLE IF NOT EXISTS simple_original_restore (id TEXT PRIMARY KEY, payload TEXT NOT NULL)")
        db.commit()

    def load(self):
        row = self.db.execute("SELECT payload FROM simple_original_restore WHERE id='current'").fetchone()
        return json.loads(row[0]) if row else None

    def save(self, job):
        job["updatedAt"] = now_iso()
        self.db.execute("INSERT INTO simple_original_restore VALUES('current',?) ON CONFLICT(id) DO UPDATE SET payload=excluded.payload",
                        (json.dumps(job, ensure_ascii=False),))
        self.db.commit()


class OriginalRestoreManager:
    def __init__(self, db, uploads, slicing, leases):
        self.store = OriginalRestoreStore(db)
        self.uploads, self.slicing, self.leases = uploads, slicing, leases
        self.job = self.store.load()
        self.loop_task = None
        self.stopping = False
        self.active = False
        self.changed = asyncio.Event()

    def state(self):
        job = self.job or {"state": "idle", "items": []}
        counts = {}
        public = []
        for item in job.get("items", []):
            counts[item.get("state", "queued")] = counts.get(item.get("state", "queued"), 0) + 1
            public.append({k: item.get(k) for k in ("name", "state", "bytes", "size", "message", "updatedAt")})
        return {"state": job.get("state", "idle"), "counts": counts, "items": public,
                "updatedAt": job.get("updatedAt"), "active": self.active}

    async def start(self, request_id: str):
        try:
            UUID(request_id)
        except (ValueError, TypeError, AttributeError):
            raise ValueError("恢复请求编号无效。") from None
        if self.active:
            return self.state()
        if not self.job or not self.job.get("items"):
            raise ValueError("没有已登记的原片恢复记录。")
        if request_id not in self.job.setdefault("requestIds", []):
            self.job["requestIds"].append(request_id)
        for row in self.job.get("items", []):
            if row.get("state") not in {"complete", "attention"}:
                row.update(state="queued", message="等待分段续传。")
        self.job.update(state="running", message="正在从绑定对象恢复原视频。")
        self.store.save(self.job)
        if not self.loop_task or self.loop_task.done():
            self.loop_task = asyncio.create_task(self._run_loop(), name="original-video-restore")
        self.changed.set()
        return self.state()

    async def startup(self):
        if self.job and self.job.get("state") in {"running", "waiting", "waiting_connection", "waiting_disk", "retry_wait"}:
            for row in self.job.get("items", []):
                if row.get("state") in {"running", "waiting_connection", "waiting_disk", "retry_wait"}:
                    row["state"] = "queued"
            self.job["state"] = "running"
            self.store.save(self.job)
            self.loop_task = asyncio.create_task(self._run_loop(), name="original-video-restore")

    async def shutdown(self):
        self.stopping = True
        if self.loop_task and not self.loop_task.done():
            self.loop_task.cancel()
            await asyncio.gather(self.loop_task, return_exceptions=True)

    def _save_row(self, row, **updates):
        row.update(updates)
        row["updatedAt"] = now_iso()
        self.store.save(self.job)

    def _destination(self, path, name):
        if path.name != name:
            raise RestoreFailure("identity", "原片名称与恢复记录不符。")
        workspace = getattr(self.uploads, 'workspace', None)
        try:
            if workspace and workspace.enabled and workspace.asset_for(path):
                return workspace.media_path(path)
            # Earlier repairs may have a registered path outside the managed workspace.
            if path.parts[:2] == ('/', 'Volumes') and path.parts[3:-1] == ('视频', '已上传'):
                return checked_path(path)
        except FileEnvironmentError as error:
            raise RestoreFailure("disk", str(error), retryable=True) from None
        except ValueError:
            pass
        raise RestoreFailure("identity", "原片记录不在已确认的工作区或原归档目录。")

    def _records(self, name):
        source = [i for i in self.slicing.store.list("items") if i.get("name") == name]
        if not source:
            raise RestoreFailure("identity", "原始切片记录不存在，未尝试其他对象。")
        paths = {i.get("path") for i in source}
        versions = [i.get("fileVersion") or {} for i in source]
        if (len(paths) != 1 or not next(iter(paths))
                or len({(v.get("size"), v.get("edgeHash")) for v in versions}) != 1):
            raise RestoreFailure("identity", "切片记录中的原片位置或指纹不唯一。")
        path = self._destination(Path(next(iter(paths))), name)
        expected = versions[0]
        if expected.get("size") is None or not re.fullmatch(r"[a-f0-9]{64}", str(expected.get("edgeHash", ""))):
            raise RestoreFailure("identity", "原片记录缺少可验证的文件指纹。")
        candidates = [i for i in self.uploads.store.list("items")
                      if i.get("name") == name and i.get("state") == "complete"
                      and str(i.get("dataset")) in AUTH_DATASETS and i.get("object")
                      and i.get("size") == expected["size"]]
        candidates.sort(key=lambda i: AUTH_DATASETS.index(str(i.get("dataset"))))
        if not candidates:
            raise RestoreFailure("identity", "没有找到与原片绑定且已完成的云端对象。")
        for candidate in candidates:
            obj = candidate["object"]
            if (not isinstance(obj, dict) or not all(isinstance(obj.get(k), str) and obj[k]
                    for k in ("bucket", "key", "endpoint"))
                    or not obj["key"].endswith(f"/simple-{candidate['id']}/{name}")):
                continue
            return source, path, expected, candidate
        raise RestoreFailure("identity", "云端对象绑定与原片记录不符。")

    async def _restore_one(self, row):
        name = row["name"]
        try:
            records = [i for i in self.slicing.store.list('items') if i.get('name') == name]
            if records and all(i.get('localCleanupState') == 'complete' for i in records):
                self._save_row(row, state='cleaned', message='已核验并按工作区规则清理，保留云端原片和恢复凭证。')
                return
            source, path, expected, upload_item = self._records(name)
            if path.exists():
                from .slicing import source_version
                current = source_version(path)
                if current["size"] != expected["size"] or current["edgeHash"] != expected["edgeHash"]:
                    raise RestoreFailure("conflict", "目标路径已有不同文件，未覆盖。")
                self.leases.notify_move(str(path), str(path), current)
                self._save_row(row, state="complete", bytes=expected["size"], size=expected["size"],
                               message="本地原片已存在且指纹一致。")
                return
            if not path.parent.is_dir():
                raise RestoreFailure("disk", "外置盘目标目录当前不可用。", retryable=True)
            issued = await self.uploads.remote.signature(str(upload_item["dataset"]), binding=upload_item["object"])
            if (issued.get("bucket") != upload_item["object"]["bucket"]
                    or issued.get("endpoint") != upload_item["object"]["endpoint"]):
                raise RestoreFailure("identity", "续期授权没有指向原云端对象位置。")
            # Dataset upload signatures are write-only. Exchange them for the
            # platform's bounded management credentials, which can read the
            # exact object already pinned by the uploader ID and file version.
            credentials = await asyncio.to_thread(runtime_management_credentials, issued, upload_item)
            import boto3
            from botocore.config import Config
            client = boto3.client("s3", endpoint_url=credentials["endpoint"],
                region_name=credentials.get("region") or "us-east-1",
                aws_access_key_id=credentials["accessKey"], aws_secret_access_key=credentials["secretKey"],
                aws_session_token=credentials.get("sessionToken") or None,
                config=Config(signature_version="s3v4", s3={"addressing_style": "path"}, proxies={},
                    connect_timeout=15, read_timeout=90, retries={"max_attempts": 0},
                    request_checksum_calculation="when_required", response_checksum_validation="when_required",
                    max_pool_connections=RANGE_WORKERS))
            self._save_row(row, state="running", bytes=0, size=expected["size"], message="正在校验云端对象并续传。")
            loop = asyncio.get_running_loop()
            def progress(done, total):
                loop.call_soon_threadsafe(partial(self._save_row, row, state="running", bytes=done,
                                                  size=total, message="正在恢复并校验原片分段。"))
            with self.leases.write(path):
                self._destination(path, name)
                await finish_io(download_bound_object, client, upload_item, expected, path,
                                progress=progress, stopping=lambda: self.stopping)
                from .slicing import source_version
                current = source_version(path)
                if current["size"] != expected["size"] or current["edgeHash"] != expected["edgeHash"]:
                    raise RestoreFailure("identity", "恢复后的文件指纹不匹配，未关联到任务。")
                self.leases.notify_move(str(path), str(path), current)
            self._save_row(row, state="complete", bytes=expected["size"], size=expected["size"],
                           message="原片已恢复并校验。")
        except RestoreFailure as error:
            if error.code == "stopping":
                return
            state = {"disk": "waiting_disk", "authorization": "waiting_connection",
                     "network": "retry_wait"}.get(error.code, "attention")
            self._save_row(row, state=state, errorCode=error.code, message=str(error))
        except Exception as error:
            name = type(error).__name__
            code = getattr(error, "code", "")
            if code in {"auth", "network"}:
                state = "waiting_connection" if code == "auth" else "retry_wait"
                code = "authorization" if code == "auth" else "network"
                message = "云端授权暂不可用，稍后自动续接。" if code == "authorization" else "云端连接中断，已保留已验证分段，稍后续传。"
            elif isinstance(error, httpx.HTTPError) or name in {"ClientError", "EndpointConnectionError", "ConnectionClosedError", "ReadTimeoutError",
                          "ResponseStreamingError", "ConnectTimeoutError", "HTTPClientError"}:
                state, code, message = "retry_wait", "network", "云端连接中断，已保留已验证分段，稍后续传。"
            elif isinstance(error, ValueError) and "此视频正在上传" in str(error):
                state, code, message = "retry_wait", "lease_busy", "原片正被其他操作使用，稍后再恢复。"
            elif isinstance(error, OSError):
                state, code, message = "waiting_disk", "disk", "外置盘写入暂不可用，进度已保留。"
            else:
                state, code, message = "attention", "restore_error", "原片恢复遇到无法自动确认的错误，未发布不完整文件。"
            self._save_row(row, state=state, errorCode=code, message=message)

    async def _run_loop(self):
        self.active = True
        try:
            while not self.stopping and self.job and self.job.get("state") != "complete":
                todo = [row for row in self.job.get("items", [])
                        if row.get("state") in {"queued", "running", "retry_wait", "waiting_connection", "waiting_disk"}]
                if not todo:
                    self.job["state"] = "partial" if any(i.get("state") == "attention" for i in self.job["items"]) else "complete"
                    if any(i.get('state') == 'cleaned' for i in self.job['items']):
                        self.job['message'] = '恢复记录已核对；已按规则清理的媒体不再下载。'
                    self.store.save(self.job)
                    return
                self.job["state"] = "running"
                self.store.save(self.job)
                await asyncio.gather(*(self._restore_one(row) for row in todo))
                if all(i.get("state") in {"complete", "cleaned"} for i in self.job["items"]):
                    self.job.update(state="complete", message='恢复记录已核对；已清理的媒体不再下载。'
                                    if any(i.get('state') == 'cleaned' for i in self.job['items']) else "本次原片均已恢复并校验。")
                    self.store.save(self.job)
                    return
                retryable = any(i.get("state") in {"queued", "running", "retry_wait", "waiting_connection", "waiting_disk"}
                                for i in self.job["items"])
                if not retryable:
                    self.job["state"] = "partial"
                    self.store.save(self.job)
                    return
                self.job["state"] = "waiting"
                self.store.save(self.job)
                await asyncio.sleep(30)
        except asyncio.CancelledError:
            raise
        finally:
            self.active = False
