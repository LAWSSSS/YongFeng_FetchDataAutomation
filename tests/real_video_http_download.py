"""Real loopback TCP resume contract for the production video downloader.

Only the unique test media URL is forwarded to a local HTTP server. Platform
result metadata is simulated; every other outbound request is rejected. The
protected source is read only and copied into an exclusively owned test folder.
"""
from __future__ import annotations

import asyncio
import json
import re
import shutil
import socket
import sys
import threading
import time
from datetime import datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from uuid import uuid4

import httpx

from app.main import create_app
from app.media import file_digest
from app.remote import RemoteClient

BREAK_AT = 16 * 1024 * 1024


class MediaTransport(httpx.AsyncBaseTransport):
    """Allow exactly our metadata request and media route, with no external I/O."""

    def __init__(self, loopback: str, object_path: str, result: dict, metrics: dict):
        self.loopback, self.object_path, self.result, self.metrics = loopback, object_path, result, metrics
        self.network = httpx.AsyncHTTPTransport(trust_env=False)

    async def handle_async_request(self, request):
        if request.url.path == "/platform/api/data-acquisition/task-result" and request.method == "POST":
            body = json.loads(request.content)
            if str(body.get("taskId")) != self.result["taskId"]:
                return httpx.Response(409, json={"code": "identity-conflict"})
            self.metrics["resultRefreshes"] += 1
            return httpx.Response(200, json={"code": "0", "data": {"records": [self.result], "total": 1}})
        if request.url.path == "/platform/oss" + self.object_path and request.method in {"GET", "HEAD"}:
            # Do not forward original cookies, host headers or authorization.
            headers = {key: request.headers[key] for key in ("range", "if-range", "accept-encoding") if key in request.headers}
            forwarded = httpx.Request(request.method, self.loopback + self.object_path, headers=headers)
            return await self.network.handle_async_request(forwarded)
        self.metrics["rejectedRequests"] += 1
        raise httpx.ConnectError("Unexpected outbound request blocked by the isolated TCP contract.", request=request)

    async def aclose(self):
        await self.network.aclose()


def _server(path: Path, object_path: str, etag: str, metrics: dict):
    lock = threading.Lock()

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *_args):
            return

        def do_HEAD(self):
            self.serve(False)

        def do_GET(self):
            self.serve(True)

        def serve(self, body: bool):
            if self.path != object_path:
                self.send_error(404)
                return
            size = path.stat().st_size
            begin, end, status = 0, size - 1, 200
            requested = self.headers.get("Range", "")
            if requested:
                match = re.fullmatch(r"bytes=(\d+)-(\d*)", requested)
                if not match or int(match[1]) >= size:
                    self.send_response(416)
                    self.send_header("Content-Range", f"bytes */{size}")
                    self.send_header("Content-Length", "0")
                    self.end_headers()
                    return
                begin = int(match[1])
                end = min(end, int(match[2])) if match[2] else end
                status = 206
            with lock:
                broken = bool(body and not requested and metrics["interruptions"] == 0)
                if broken:
                    metrics["interruptions"] += 1
                metrics["requests"].append({"method": self.command, "status": status,
                                             "offset": begin, "bytes": end - begin + 1,
                                             "interrupted": broken})
            self.send_response(status)
            self.send_header("Content-Type", "video/mp4")
            self.send_header("ETag", etag)
            self.send_header("Accept-Ranges", "bytes")
            self.send_header("Content-Length", str(end - begin + 1))
            if status == 206:
                self.send_header("Content-Range", f"bytes {begin}-{end}/{size}")
            self.end_headers()
            if not body:
                return
            sent = 0
            try:
                with path.open("rb") as source:
                    source.seek(begin)
                    while begin + sent <= end:
                        amount = min(1024 * 1024, end - begin - sent + 1)
                        if broken:
                            amount = min(amount, BREAK_AT - sent)
                            if amount <= 0:
                                self.wfile.flush()
                                self.connection.shutdown(socket.SHUT_RDWR)
                                self.connection.close()
                                self.close_connection = True
                                return
                        block = source.read(amount)
                        if not block:
                            return
                        self.wfile.write(block)
                        sent += len(block)
                        with lock:
                            metrics["sentBytes"] += len(block)
            except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
                self.close_connection = True

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server.daemon_threads = True
    return server


async def run_tcp(root: Path, sourceOriginal: Path, settings: dict) -> dict:
    """Exercise real TCP, download resume, ffprobe, EOF decode and SHA-256."""
    root = Path(root).resolve()
    supplied_source = Path(sourceOriginal)
    if supplied_source.is_symlink():
        raise ValueError("TCP rehearsal source must not be a symlink.")
    source = supplied_source.resolve(strict=True)
    before = source.stat()
    if before.st_size <= BREAK_AT:
        raise ValueError("The real TCP contract requires a source larger than the interruption offset.")
    started = time.monotonic()
    original_digest = await asyncio.to_thread(file_digest, source)
    run_id = uuid4().hex
    work = root / "tcp-download-contract" / run_id
    work.mkdir(parents=True, exist_ok=False)
    clone = work / "source-clone.mp4"
    clone_started = time.monotonic()
    if sys.platform == "darwin":
        process = await asyncio.create_subprocess_exec("/bin/cp", "-c", "-p", str(source), str(clone),
                                                     stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL)
        if await process.wait():
            raise RuntimeError("APFS test clone failed; no existing files were replaced.")
        copy_method = "APFS clone (cp -c -p)"
    else:
        await asyncio.to_thread(shutil.copy2, source, clone)
        copy_method = "independent copy"
    clone_seconds = time.monotonic() - clone_started
    if clone.stat().st_size != before.st_size:
        clone.unlink(missing_ok=True)
        raise RuntimeError("The test clone does not match the source size.")
    metrics = {"requests": [], "interruptions": 0, "sentBytes": 0,
               "resultRefreshes": 0, "rejectedRequests": 0}
    object_path = f"/tcp-contract/{run_id}/media.mp4"
    server = _server(clone, object_path, '"sha256-' + original_digest + '"', metrics)
    thread = threading.Thread(target=server.serve_forever, daemon=True, name="isolated-tcp-video")
    thread.start()
    task_id, result_id = "tcp-task-" + run_id, "tcp-result-" + run_id
    remote_result = {"id": result_id, "taskId": task_id, "status": "FINISH",
                     "url": "http://media.test" + object_path, "beginTime": "2026-09-13 09:54:00"}
    transport = MediaTransport(f"http://127.0.0.1:{server.server_port}", object_path, remote_result, metrics)
    target = RemoteClient("target", transport=transport)
    target.connected = True
    source_client = RemoteClient("source", transport=httpx.MockTransport(lambda _: httpx.Response(404)))
    app = create_app(source_client, target, work / "database", staging_directory=work / "cache")
    app.state.slicing.store.settings["frameToolPath"] = str(settings["frameToolPath"])
    start = datetime(2026, 9, 13, 9, 54)
    version = {"revision": 0, "name": "TCP-REHEARSAL", "taskId": task_id, "resultId": result_id,
               "startTime": start.isoformat(sep=" "), "endTime": (start + timedelta(seconds=4500)).isoformat(sep=" "),
               "durationSeconds": 4500, "state": "ready"}
    row = {"id": "tcp-" + run_id, "jobId": "isolated-tcp", "name": "TCP-REHEARSAL", "plate": "TCP-REHEARSAL",
           "revision": 0, "activeRevision": 0, "acceptedRevision": None, "requests": {}, "versions": [version]}
    part = app.state.videos.work / row["id"] / "v0.part"
    app.state.videos.store.save(row)
    stage = {"node": "视频下载", "case": "真实本地HTTP断socket后Range续传", "kind": "真实TCP+真实下载逻辑+受控断流",
             "expected": "16MiB中断后同版本续传，真实ffprobe/尾帧解码/SHA成功，原片不变", "passed": False}
    download_started = time.monotonic()
    try:
        saved, bound_result, digest, size = await app.state.videos.download_bound(row, version)
        actual_digest = await asyncio.to_thread(file_digest, saved)
        resumed = any(request["offset"] == BREAK_AT and request["status"] == 206 for request in metrics["requests"])
        stage["actual"] = {"bytes": size, "sha256": digest, "readbackSha256": actual_digest,
                           "attempts": version.get("attempts"), "resumedAt16MiB": resumed,
                           "boundResultMatched": bound_result["id"] == result_id,
                           "ffprobeAndTail": "passed (unmodified VideoManager.ffprobe)"}
        stage["passed"] = bool(size == before.st_size and digest == original_digest == actual_digest
                               and metrics["interruptions"] == 1 and resumed
                               and version.get("attempts") == 2 and bound_result["id"] == result_id
                               and metrics["rejectedRequests"] == 0)
    except Exception as error:
        stage["actual"] = {"errorType": type(error).__name__, "code": getattr(error, "code", None),
                           "attempts": version.get("attempts"), "retainedCheckpointBytes": part.stat().st_size if part.exists() else 0}
    finally:
        stage["downloadSeconds"] = round(time.monotonic() - download_started, 6)
        await app.state.videos.shutdown()
        await app.state.upload_audit.shutdown()
        await source_client.close()
        await target.close()
        app.state.store.close()
        await asyncio.to_thread(server.shutdown)
        server.server_close()
        await asyncio.to_thread(thread.join, 5)
        # These exact paths were created by this invocation. Unknown files and
        # the caller's other media/diagnostics are never recursively removed.
        part.unlink(missing_ok=True)
        clone.unlink(missing_ok=True)
        after = source.stat()
        after_digest = await asyncio.to_thread(file_digest, source)
        unchanged = ((before.st_size, before.st_mtime_ns) == (after.st_size, after.st_mtime_ns)
                     and original_digest == after_digest)
        stage["passed"] = bool(stage["passed"] and unchanged)
        stage.update(seconds=round(time.monotonic() - started, 6), detail={
            "copyMethod": copy_method, "cloneSeconds": round(clone_seconds, 6), "metrics": metrics,
            "sourceUnchanged": unchanged, "sourceBytes": after.st_size,
            "sourceSha256": after_digest, "sourceMtimeUnchanged": before.st_mtime_ns == after.st_mtime_ns,
            "registeredMediaRemoved": not part.exists() and not clone.exists(),
            "boundary": "只有媒体传输为真实loopback TCP；平台结果查询仍为隔离模拟"})
        outcome = {"stages": [stage], "metrics": metrics, "originalUnchanged": unchanged}
        (root / "tcp-result.json").write_text(json.dumps(outcome, ensure_ascii=False, indent=2), encoding="utf-8")
    return outcome
