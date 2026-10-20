"""Explicit Athena upload/import adapter. Auth remains in the shared runtime session."""
import json
from urllib.parse import urlsplit, urlunsplit

import httpx

from .upload_audit import AuditError, DATASETS, name_key

TARGET_IDS = {d["id"] for d in DATASETS}
PACKAGE_IDS = {"27892", "27893"}


def source_keys(job):
    config = job.get("config")
    if isinstance(config, str):
        try:
            config = json.loads(config)
        except ValueError:
            return None
    files = (config or {}).get("source", {}).get("files") if isinstance(config, dict) else None
    if not isinstance(files, list) or not files:
        return None
    keys = [f.get("key") for f in files if isinstance(f, dict)]
    return frozenset(keys) if len(keys) == len(files) and all(isinstance(k, str) and k for k in keys) and len(set(keys)) == len(keys) else None


class UploadRemote:
    def __init__(self, auth):
        self.auth = auth

    async def request(self, method, path, *, params=None, body=None):
        """Never replay a write after a timeout/5xx. A 401 is a proven rejection."""
        if not self.auth.connected:
            raise AuditError("请在上传核对中连接视觉AI平台，队列进度已保留。", "auth", 401)
        for attempt in range(2):
            try:
                response = await self.auth.client.request(method, path, params=params, json=body,
                    headers={"Authorization": f"Bearer {self.auth.access}"})
            except httpx.HTTPError:
                raise AuditError("平台连接中断，正在保留进度并等待恢复。", "network") from None
            if response.status_code == 401 and attempt == 0:
                # The read-only client owns the synchronized refresh protocol.
                await self.auth.get("devel/datasets/27912/", {"namespace": 1})
                continue
            return self.auth.unpack(response), response.headers
        raise AuditError("视觉AI会话已失效，请重新连接。", "auth", 401)

    async def signature(self, dataset, binding=None):
        if dataset not in TARGET_IDS and dataset not in PACKAGE_IDS:
            raise ValueError("自动上传只支持固定的视频或切片照片数据集。")
        params = {"bulk": 1}
        if binding:
            # Ask for the original namespace when renewing. A server that ignores
            # this hint must still pass the worker's exact-object reconciliation.
            key = binding.get("key", "")
            before, marker, _ = key.partition("/simple-")
            if marker:
                params["prefix"] = before + "/"
        data, _ = await self.request("GET", f"devel/datasets/{dataset}/upload/", params=params)
        # Athena's Pe response interceptor applies H4.injectByDefault recursively:
        # access_key / secret_key / session_token become camel case in its UI.
        # Keep this boundary explicit; never copy credentials into a persisted row.
        if isinstance(data, dict):
            normalized = {}
            for field in ("endpoint", "bucket", "prefix", "key", "region", "accessKey", "secretKey", "sessionToken"):
                snake = {"accessKey": "access_key", "secretKey": "secret_key", "sessionToken": "session_token"}.get(field, field)
                if field != snake and data.get(field) and data.get(snake) and data[field] != data[snake]:
                    raise AuditError("平台授权字段存在冲突，未使用这份授权。", "authorization_fields")
                value = data.get(field) if data.get(field) is not None else data.get(snake)
                if value is not None:
                    normalized[field] = value
            data = normalized
        required = ("endpoint", "accessKey", "secretKey", "bucket")
        missing = [k for k in required if not isinstance(data, dict) or not isinstance(data.get(k), str) or not data[k]]
        if missing:
            raise AuditError("平台上传授权缺少字段：" + "、".join(missing) + "。尚未上传文件。", "authorization_fields")
        # Athena explicitly accepts scheme-relative MinIO endpoints in its own uploader.
        # Resolve using the platform protocol, just as window.location.protocol does.
        endpoint_value = data["endpoint"]
        if endpoint_value.startswith("//"):
            endpoint_value = self.auth.client.base_url.scheme + ":" + endpoint_value
        endpoint = urlsplit(endpoint_value)
        if endpoint.scheme not in ("http", "https") or not endpoint.hostname or endpoint.username or endpoint.password or endpoint.query:
            raise AuditError("平台上传地址格式异常。", "authorization_endpoint")
        data = {**data, "endpoint": urlunsplit((endpoint.scheme, endpoint.netloc, endpoint.path.rstrip('/'), '', ''))}
        if not isinstance(data.get("prefix", data.get("key")), str):
            raise AuditError("平台没有返回上传对象位置。", "authorization_prefix")
        return data

    async def jobs(self, dataset):
        return await self.auth.jobs(dataset)

    async def submit(self, dataset, keys):
        if dataset in PACKAGE_IDS:
            if len(keys) != 1 or len(set(keys)) != 1:
                raise ValueError("切片照片每次只能导入一个压缩包。")
        elif dataset not in TARGET_IDS or not 1 <= len(keys) <= 4 or len(set(keys)) != len(keys):
            raise ValueError("导入清单无效。")
        data, _ = await self.request("POST", "devel/dataset-jobs/", body={
            "dataset": int(dataset), "type": 1, "namespace": 1,
            "config": {"source": {"files": [{"key": k} for k in keys]}, "options": {"mode": 1}},
        })
        if not isinstance(data, dict) or not str(data.get("id", "")).isdigit():
            raise AuditError("导入提交响应不完整，正在按对象清单对账。", "uncertain")
        bound = data.get("dataset")
        if isinstance(bound, dict):
            bound = bound.get("id")
        if bound is not None and str(bound) != dataset:
            raise AuditError("导入响应指向其他数据集，请核对。", "identity")
        if source_keys(data) is not None and source_keys(data) != frozenset(keys):
            raise AuditError("导入响应的对象清单不一致，请核对。", "identity")
        return str(data["id"])

    async def videos(self, dataset):
        """A fully paged inventory is an additional duplicate/import-result check."""
        rows, ids, total, page = [], set(), None, 1
        while page <= 10000:
            data, headers = await self.request("GET", f"devel/datasets/{dataset}/videos/",
                params={"namespace": 1, "page": page, "page_size": 100})
            if not isinstance(data, list):
                raise AuditError("视频清单格式异常，不能确认是否已导入。", "response")
            try:
                count = int(headers["x-total"]) if "x-total" in headers else None
                next_page = int(headers.get("x-next-page") or 0) if "x-next-page" in headers else None
            except ValueError:
                raise AuditError("视频清单分页无效，请重新核对。", "pagination") from None
            if page == 1:
                total = count
            elif count != total:
                raise AuditError("视频清单正在变化，稍后重试。", "pagination")
            for row in data:
                if not isinstance(row, dict) or not str(row.get("id", "")).isdigit() or str(row["id"]) in ids:
                    raise AuditError("视频清单身份重复或缺失。", "identity")
                bound = row.get("dataset")
                if isinstance(bound, dict):
                    bound = bound.get("id")
                if bound is not None and str(bound) != dataset:
                    raise AuditError("视频清单返回其他数据集的内容。", "identity")
                ids.add(str(row["id"])); rows.append(row)
            if next_page is None and total is None:
                raise AuditError("视频清单缺少完整分页凭证。", "pagination")
            if next_page is not None:
                if next_page <= 0:
                    break
                if next_page != page + 1 or not data:
                    raise AuditError("视频清单分页不连续。", "pagination")
            elif len(rows) >= total:
                break
            elif not data:
                raise AuditError("视频清单缺少记录。", "pagination")
            page += 1
        else:
            raise AuditError("视频清单超过分页上限。", "pagination")
        if total is not None and total != len(rows):
            raise AuditError("视频清单数量不完整。", "pagination")
        return rows

    async def evidence(self, dataset, job):
        return await self.auth.job_evidence(dataset, job)

    async def images(self, dataset):
        if dataset not in PACKAGE_IDS:
            raise ValueError("切片照片清单只读取固定的两个导入数据集。")
        rows, page = [], 1
        while page <= 5:
            data, headers = await self.request("GET", f"devel/datasets/{dataset}/images/",
                params={"namespace": 1, "page": page, "page_size": 100})
            if not isinstance(data, list):
                raise AuditError("切片照片清单格式异常。", "response")
            rows.extend(row for row in data if isinstance(row, dict))
            next_page = headers.get("x-next-page")
            if not next_page or not str(next_page).strip() or str(next_page) in {"0", "-1"}:
                break
            page += 1
        return rows


def video_names(row):
    """Only platform video names, never thumbnails or guessed plate fragments."""
    result = set()
    for key in ("name", "filename", "file_name", "original_name"):
        value = row.get(key)
        if isinstance(value, str) and value:
            result.add(name_key(value))
    return result
