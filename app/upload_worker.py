"""One expendable process per file. Credentials arrive only over stdin.

Critical S3 transitions wait for a parent journal acknowledgement. Stdout is a
small, whitelisted event protocol; SDK errors/URLs/credentials are never logged.
"""
import hashlib
import io
import json
import os
import re
import stat
import sys
import tempfile
import threading
import time
from pathlib import Path
from urllib.parse import quote

from .upload_storage import BoundStorage, StorageBindingError, runtime_management_credentials

PART_SIZE = 32 * 1024 * 1024


class WorkerError(Exception):
    def __init__(self, code):
        self.code = code


def emit(event, *, critical=False, **fields):
    print(json.dumps({"event": event, **fields}), flush=True)
    if critical:
        if sys.stdin.readline().strip() != "ack":
            raise SystemExit(2)


def version(info):
    return {"device": info.st_dev, "inode": info.st_ino, "size": info.st_size, "modifiedNs": info.st_mtime_ns}


def error_code(error):
    from httpx import HTTPError
    from botocore.exceptions import (ClientError, ConnectionClosedError, ConnectTimeoutError,
        EndpointConnectionError, ReadTimeoutError, HTTPClientError)
    if isinstance(error, WorkerError):
        return error.code
    if isinstance(error, StorageBindingError):
        return "multipart_unknown"
    if isinstance(error, (ConnectionClosedError, ConnectTimeoutError, EndpointConnectionError, ReadTimeoutError, HTTPClientError, HTTPError)):
        return "network"
    if isinstance(error, OSError):
        return "disk"
    if isinstance(error, ClientError):
        code = str(error.response.get("Error", {}).get("Code", ""))
        if code in ("ExpiredToken", "InvalidToken", "InvalidAccessKeyId", "SignatureDoesNotMatch", "AccessDenied", "TokenRefreshRequired", "403"):
            return "authorization"
        if code in ("SlowDown", "RequestTimeout", "ServiceUnavailable", "InternalError", "500", "503"):
            return "network"
        if code in ("NoSuchUpload",):
            return "multipart_missing"
    return "upload_failed"


def missing(error, *codes):
    return str((getattr(error, "response", None) or {}).get("Error", {}).get("Code")) in codes


def error_details(error, item, stage):
    """The exception message, request URL, headers and credentials are excluded."""
    response = getattr(error, "response", None) or {}
    service_code = str(response.get("Error", {}).get("Code", ""))
    safe_codes = {"403", "404", "500", "503", "AccessDenied", "NoSuchKey", "NotFound", "NoSuchUpload",
                  "ExpiredToken", "InvalidToken", "InvalidAccessKeyId", "SignatureDoesNotMatch", "TokenRefreshRequired",
                  "SlowDown", "RequestTimeout", "ServiceUnavailable", "InternalError", "NotImplemented",
                  "InvalidPart", "InvalidPartOrder", "EntityTooSmall", "InvalidRequest", "BadDigest"}
    details = {"id": item.get("id", ""), "stage": stage, "type": type(error).__name__,
            "serviceCode": service_code if service_code in safe_codes else "",
            "httpStatus": response.get("ResponseMetadata", {}).get("HTTPStatusCode")}
    if isinstance(item.get("_keyInIssuedPrefix"), bool):
        details["keyInIssuedPrefix"] = item["_keyInIssuedPrefix"]
    return details


def diagnostic(error, item, stage):
    """Per-process private diagnostics contain categories only, never raw SDK output."""
    identity = item.get("id", "")
    if not re.fullmatch(r"[a-f0-9]{32}", identity):
        return
    try:
        fd, _path = tempfile.mkstemp(prefix="simple-upload-error-" + identity + "-", suffix=".json")
        with os.fdopen(fd, "w") as output:
            json.dump(error_details(error, item, stage), output)
    except OSError:
        pass


class ProgressBody(io.BytesIO):
    def __init__(self, body, base, event):
        super().__init__(body)
        self.base, self.event, self.last, self.high = base, event, 0, 0
        self.reporting = False

    def start_transfer(self):
        # SigV4/checksum preparation reads and rewinds the body before sending.
        self.reporting, self.high, self.last = True, 0, 0

    def reset_reporting(self):
        self.reporting, self.high, self.last = False, 0, 0

    def read(self, size=-1):
        data = super().read(size)
        current = self.tell()
        if self.reporting and current > self.high:
            self.high = current
            now = time.monotonic()
            if now - self.last >= 1 or current == len(self.getbuffer()):
                self.event("progress", bytes=self.base + current)
                self.last = now
        return data


class Multipart:
    def __init__(self, client, item, event=emit):
        self.s3, self.item, self.event = client, item, event
        self.obj = item["object"]
        self.base = {"Bucket": self.obj["bucket"], "Key": self.obj["key"]}
        self.upload_id = item.get("uploadId")
        self.stage = "initializing"
        self.marker = hashlib.sha256(json.dumps(item["fileVersion"], sort_keys=True).encode()).hexdigest()

    def request(self, operation, **kwargs):
        self.stage = operation
        return getattr(self.s3, operation)(**kwargs)

    def head(self):
        try:
            result = self.request("head_object", **self.base)
        except Exception as error:
            if missing(error, "404", "NoSuchKey", "NotFound"):
                return None
            if not missing(error, "403", "AccessDenied"):
                raise
            # Some S3 gateways reject HEAD without an XML error body. A bounded
            # GET both confirms absence and preserves the actual service error.
            try:
                result = self.request("get_object", **self.base, Range="bytes=0-0")
            except Exception as get_error:
                if missing(get_error, "404", "NoSuchKey", "NotFound"):
                    return None
                raise
            body = result.get("Body")
            if body:
                try:
                    body.read(1)
                finally:
                    body.close()
            content_range = re.fullmatch(r"bytes 0-0/(\d+)", result.get("ContentRange", ""))
            if content_range:
                result["ContentLength"] = int(content_range[1])
        metadata = result.get("Metadata", {})
        if (result.get("ContentLength") != self.item["size"] or metadata.get("uploader-id") != self.item["id"]
                or metadata.get("file-version") != self.marker):
            raise WorkerError("object_changed")
        return {"etag": result.get("ETag", ""), "size": result["ContentLength"],
                "versionId": result.get("VersionId", "")}

    def parts(self):
        parts, marker = [], 0
        while True:
            data = self.request("list_parts", **self.base, UploadId=self.upload_id, PartNumberMarker=marker)
            parts.extend(data.get("Parts", []))
            if not data.get("IsTruncated"):
                break
            next_marker = data.get("NextPartNumberMarker", 0)
            if next_marker <= marker:
                raise WorkerError("parts_invalid")
            marker = next_marker
        return parts

    def discover(self):
        """Only recover the exact program-generated key after a lost create response."""
        candidates, args = [], {"Bucket": self.obj["bucket"], "Prefix": self.obj["key"]}
        while True:
            try:
                data = self.request("list_multipart_uploads", **args)
            except Exception as error:
                if missing(error, "AccessDenied", "NotImplemented"):
                    raise WorkerError("multipart_unknown") from None
                raise
            candidates.extend(u["UploadId"] for u in data.get("Uploads", []) if u.get("Key") == self.obj["key"])
            if not data.get("IsTruncated"):
                break
            if not data.get("NextUploadIdMarker"):
                raise WorkerError("multipart_unknown")
            args.update(KeyMarker=data.get("NextKeyMarker", ""), UploadIdMarker=data["NextUploadIdMarker"])
        if len(candidates) > 1:
            raise WorkerError("multipart_unknown")
        if candidates:
            self.upload_id = candidates[0]
            self.event("created", critical=True, uploadId=self.upload_id)

    def cleanup(self):
        complete = self.head()
        if complete:
            self.event("complete", critical=True, **complete)
            return
        if not self.upload_id and self.item.get("createAttempted"):
            self.discover()
        if self.upload_id:
            try:
                self.request("abort_multipart_upload", **self.base, UploadId=self.upload_id)
            except Exception as error:
                if not missing(error, "NoSuchUpload", "404"):
                    raise
            try:
                self.parts()
            except Exception as error:
                if not missing(error, "NoSuchUpload", "404"):
                    raise
            else:
                # Abort can race with in-flight parts: retain cleanup intent.
                raise WorkerError("cleanup_pending")
        self.event("cleaned", critical=True)

    def run(self):
        # A fresh, nonce-named object has no prior write intent. Some restricted
        # S3 credentials return 403 for absent keys instead of revealing absence.
        # Once creation was attempted, never bypass reconciliation on a retry.
        own_nonce = "/simple-" + self.item["id"] + "/"
        fresh = (own_nonce in "/" + self.obj["key"] and not self.item.get("createAttempted")
                 and not self.upload_id and not self.item.get("parts"))
        try:
            complete = None if fresh else self.head()
        except Exception as error:
            if not missing(error, "403", "AccessDenied"):
                raise
            # Upload-only grants can still resume their exact known multipart ID.
            # They cannot prove an unknown create/complete, which remains manual.
            if not self.upload_id:
                raise WorkerError("multipart_unknown") from None
            complete = None
        if complete:
            self.event("complete", critical=True, **complete)
            return
        path = Path(self.item["path"])
        flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
        with os.fdopen(os.open(path, flags), "rb") as file:
            info = os.fstat(file.fileno())
            source_version = self.item.get("sourceFileVersion", self.item["fileVersion"])
            if not stat.S_ISREG(info.st_mode) or version(info) != source_version:
                raise WorkerError("file_changed")
            if not self.upload_id and self.item.get("createAttempted"):
                self.discover()
            created_now = False
            if not self.upload_id:
                self.event("creating", critical=True)
                response = self.request("create_multipart_upload", **self.base, ContentType="video/" + (path.suffix[1:] or "mp4"),
                    ContentDisposition="attachment; filename=" + quote(self.item["name"]),
                    Metadata={"uploader-id": self.item["id"], "file-version": self.marker})
                self.upload_id = response["UploadId"]
                self.event("created", critical=True, uploadId=self.upload_id)
                created_now = True
            if created_now:
                confirmed = []
            else:
                try:
                    confirmed = self.parts()
                except Exception as error:
                    if missing(error, "NoSuchUpload", "404"):
                        raise WorkerError("multipart_unknown") from None
                    if not missing(error, "403", "AccessDenied"):
                        raise
                    # Server validates these exact ETags at CompleteMultipartUpload.
                    # Unacknowledged parts are safely overwritten in the same ID.
                    confirmed = [{"PartNumber": p["number"], "Size": p["size"], "ETag": p["etag"]}
                                 for p in self.item.get("parts", {}).values()]
            size, number, base, entries = self.item["size"], 1, 0, []
            by_number = {p["PartNumber"]: p for p in confirmed}
            total_parts = (size + PART_SIZE - 1) // PART_SIZE
            if len(by_number) != len(confirmed) or any(n < 1 or n > total_parts for n in by_number):
                raise WorkerError("parts_invalid")
            while base < size:
                length = min(PART_SIZE, size - base)
                part = by_number.get(number)
                if part and (part.get("Size") != length or not part.get("ETag")):
                    raise WorkerError("parts_invalid")
                if not part:
                    file.seek(base)
                    body = file.read(length)
                    if len(body) != length or version(os.fstat(file.fileno())) != source_version:
                        raise WorkerError("file_changed")
                    result = self.request("upload_part", **self.base, UploadId=self.upload_id, PartNumber=number,
                        Body=ProgressBody(body, base, self.event), ContentLength=length)
                    part = {"PartNumber": number, "ETag": result["ETag"], "Size": length}
                entries.append({"PartNumber": number, "ETag": part["ETag"]})
                base += length
                self.event("part", critical=True, number=number, etag=part["ETag"], size=length, bytes=base)
                number += 1
            if version(os.fstat(file.fileno())) != source_version:
                raise WorkerError("file_changed")
            self.event("completing", critical=True)
            try:
                receipt = self.request("complete_multipart_upload", **self.base, UploadId=self.upload_id, MultipartUpload={"Parts": entries})
            except Exception:
                # A lost success response is checked before the process reports failure.
                try:
                    complete = self.head()
                except Exception as error:
                    if missing(error, "403", "AccessDenied"):
                        raise WorkerError("multipart_unknown") from None
                    raise
                if not complete:
                    raise
                receipt = None
            if (receipt and receipt.get("Bucket") == self.base["Bucket"] and receipt.get("Key") == self.base["Key"]
                    and isinstance(receipt.get("ETag"), str) and receipt["ETag"]):
                # A positive, bound S3 completion receipt proves assembly of the
                # acknowledged parts without requiring bucket read/list rights.
                complete = {"etag": receipt["ETag"], "size": size, "versionId": receipt.get("VersionId", "")}
            else:
                complete = self.head()
            if not complete:
                raise WorkerError("completion_unknown")
            self.event("complete", critical=True, **complete)


def main():
    parent = os.getppid()
    def parent_watchdog():
        while True:
            time.sleep(0.5)
            if os.getppid() != parent:
                os._exit(2)
    threading.Thread(target=parent_watchdog, daemon=True).start()
    payload, worker = {}, None
    try:
        import boto3
        from botocore.config import Config
        payload = json.loads(sys.stdin.readline())
        credentials = payload["credentials"]
        prefix = credentials.get("prefix")
        if isinstance(prefix, str):
            payload["item"]["_keyInIssuedPrefix"] = payload["item"]["object"]["key"].startswith(prefix)
        def transfer_started(request, **_):
            body = request.body
            if isinstance(body, ProgressBody):
                body.start_transfer()
        def create_client(values):
            client = boto3.client("s3", endpoint_url=values["endpoint"], region_name=values.get("region") or "us-east-1",
                aws_access_key_id=values["accessKey"], aws_secret_access_key=values["secretKey"],
                aws_session_token=values.get("sessionToken") or None,
                config=Config(signature_version="s3v4", s3={"addressing_style": "path"}, proxies={},
                    connect_timeout=15, read_timeout=60, retries={"max_attempts": 0},
                    request_checksum_calculation="when_required", response_checksum_validation="when_required"))
            client.meta.events.register("before-send.s3.UploadPart", transfer_started)
            return client
        primary = create_client(credentials)
        client = BoundStorage(primary, payload["item"], credentials,
            lambda: create_client(runtime_management_credentials(credentials, payload["item"])))
        worker = Multipart(client, payload["item"])
        if payload.get("operation") == "cleanup":
            worker.cleanup()
        else:
            worker.run()
    except Exception as error:
        # Exception strings may contain signed URLs or authorization material.
        try:
            code = error_code(error)
        except Exception:
            code = "worker_unavailable"
        diagnostic(error, payload.get("item", {}), worker.stage if worker else "initializing")
        emit("error", code=code, **{k: v for k, v in error_details(error, payload.get("item", {}), worker.stage if worker else "initializing").items() if k != "id"})
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
