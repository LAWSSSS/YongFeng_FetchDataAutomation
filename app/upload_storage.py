"""Scoped recovery using Athena's own runtime MinIO connection configuration.

The browser loads /conf/config.json for object management too. Its credential
values remain in a single worker's memory; this wrapper can access only that
worker's recorded object and multipart IDs, never an arbitrary bucket/key.
"""
import hashlib
import json
import re
from urllib.parse import urlsplit

import httpx


class StorageBindingError(Exception):
    pass


ALLOWED = {"head_object", "get_object", "list_parts", "list_multipart_uploads",
           "create_multipart_upload", "upload_part", "complete_multipart_upload", "abort_multipart_upload"}


def denied(error):
    return str((getattr(error, "response", None) or {}).get("Error", {}).get("Code")) in ("403", "AccessDenied")


def runtime_management_credentials(credentials, item):
    endpoint = urlsplit(credentials["endpoint"])
    if (endpoint.hostname != "pre-athena-mv-fss.cisdigital.cn" or endpoint.scheme not in ("http", "https")
            or endpoint.username or endpoint.password or endpoint.query):
        raise StorageBindingError()
    with httpx.Client(trust_env=False, timeout=15, follow_redirects=False) as client:
        response = client.get("http://pre-athena-mv.cisdigital.cn/conf/config.json")
        if not response.is_success:
            raise StorageBindingError()
        try:
            config = response.json()
        except ValueError:
            raise StorageBindingError() from None
    if (not isinstance(config, dict) or not isinstance(config.get("MINIO_USERNAME"), str)
            or not isinstance(config.get("MINIO_PASSWORD"), str) or not config["MINIO_USERNAME"] or not config["MINIO_PASSWORD"]):
        raise StorageBindingError()
    return {"endpoint": credentials["endpoint"], "region": credentials.get("region") or "us-east-1",
            "accessKey": config["MINIO_USERNAME"], "secretKey": config["MINIO_PASSWORD"]}


class BoundStorage:
    def __init__(self, primary, item, credentials, recovery_factory):
        self.primary, self.item, self.credentials = primary, item, credentials
        self.recovery_factory, self.recovery = recovery_factory, None
        self.bucket, self.key = item["object"]["bucket"], item["object"]["key"]
        self.ids = {item["uploadId"]} if item.get("uploadId") else set()

    def management(self):
        identity, name = self.item["id"], self.item["name"]
        if (not re.fullmatch(r"[a-f0-9]{32}", identity) or self.item.get("dataset") not in ("27912", "27944")
                or not self.key.endswith("/simple-" + identity + "/" + name)):
            raise StorageBindingError()
        if self.recovery is None:
            self.recovery = self.recovery_factory()
        return self.recovery

    def check(self, operation, kwargs):
        if kwargs.get("Bucket") != self.bucket:
            raise StorageBindingError()
        if operation == "list_multipart_uploads":
            if kwargs.get("Prefix") != self.key:
                raise StorageBindingError()
        elif kwargs.get("Key") != self.key:
            raise StorageBindingError()
        if "UploadId" in kwargs and kwargs["UploadId"] not in self.ids:
            raise StorageBindingError()

    def recover(self, operation, kwargs):
        client = self.management()
        if operation == "create_multipart_upload":
            # A renewed upload-only grant may refer to a new namespace.
            # Only renew a previously bound, provably unwritten nonce key.
            prefix = self.credentials.get("prefix")
            if not isinstance(prefix, str) or self.key.startswith(prefix):
                raise StorageBindingError()
            expected = hashlib.sha256(json.dumps(self.item["fileVersion"], sort_keys=True).encode()).hexdigest()
            if kwargs.get("Metadata") != {"uploader-id": self.item["id"], "file-version": expected}:
                raise StorageBindingError()
            try:
                client.head_object(Bucket=self.bucket, Key=self.key)
            except Exception as missing:
                if str((getattr(missing, "response", None) or {}).get("Error", {}).get("Code")) not in ("404", "NoSuchKey", "NotFound"):
                    raise
            else:
                raise StorageBindingError()
            uploads = client.list_multipart_uploads(Bucket=self.bucket, Prefix=self.key)
            if uploads.get("IsTruncated") or any(u.get("Key") == self.key for u in uploads.get("Uploads", [])):
                raise StorageBindingError()
        return getattr(client, operation)(**kwargs)

    def __getattr__(self, operation):
        if operation not in ALLOWED:
            raise AttributeError(operation)
        def request(**kwargs):
            self.check(operation, kwargs)
            if self.recovery is not None:
                # Once this bound namespace requires recovery, avoid streaming
                # every part once through an already-rejected temporary grant.
                result = self.recover(operation, kwargs)
            else:
                body = kwargs.get("Body")
                position = body.tell() if operation == "upload_part" and hasattr(body, "tell") else None
                try:
                    result = getattr(self.primary, operation)(**kwargs)
                except Exception as error:
                    if not denied(error):
                        raise
                    # A rejected PUT can have consumed the body. Rewind before
                    # signing/sending the same part through the recovery client.
                    if position is not None:
                        body.seek(position)
                        if hasattr(body, "reset_reporting"):
                            body.reset_reporting()
                    result = self.recover(operation, kwargs)
            if operation == "create_multipart_upload" and result.get("UploadId"):
                self.ids.add(result["UploadId"])
            elif operation == "list_multipart_uploads":
                self.ids.update(u["UploadId"] for u in result.get("Uploads", []) if u.get("Key") == self.key and u.get("UploadId"))
            return result
        return request
