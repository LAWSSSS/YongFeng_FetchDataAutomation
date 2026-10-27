"""Allowlisted Athena video-cut adapter; kept separate from import-log audit."""
from __future__ import annotations

import json
import re

import httpx

from .upload_audit import AuditError

SOURCE_DATASETS = {"27912", "27944"}
CUT_TYPES = {"21", "22"}
ACTIVE_STATUSES = {"1", "2", "pending", "queued", "waiting", "running", "processing", "in_progress"}
SUCCESS_STATUSES = {"3", "success", "complete", "completed", "succeeded", "finished"}
FAILED_STATUSES = {"4", "5", "failed", "error", "cancelled", "canceled", "terminated"}


def parse_config(value):
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except ValueError:
            return {}
    return value if isinstance(value, dict) else {}


def source_video_ids(job):
    config = parse_config(job.get("config"))
    source = config.get("source") if isinstance(config.get("source"), dict) else {}
    found: set[str] = set()
    for dataset in source.get("datasets", []) if isinstance(source.get("datasets"), list) else []:
        if not isinstance(dataset, dict):
            continue
        videos = dataset.get("videos") if isinstance(dataset.get("videos"), dict) else {}
        ids = videos.get("ids") if isinstance(videos.get("ids"), list) else []
        found.update(str(value) for value in ids if str(value).isdigit())
    # Some Athena deployments flatten the source video list in job config.
    direct = source.get("videoIds", source.get("video_ids", []))
    if isinstance(direct, list):
        found.update(str(value) for value in direct if str(value).isdigit())
    return found


def target_dataset_id(job):
    config = parse_config(job.get("config"))
    target = config.get("target") if isinstance(config.get("target"), dict) else {}
    dataset = target.get("dataset") if isinstance(target.get("dataset"), dict) else {}
    identity = dataset.get("id", target.get("datasetId", target.get("dataset_id")))
    return str(identity) if identity is not None else ""


def task_state(job):
    status = str(job.get("status", "")).casefold()
    if status in ACTIVE_STATUSES:
        return "active"
    if status in SUCCESS_STATUSES:
        return "complete"
    if status in FAILED_STATUSES:
        return "failed"
    return "unknown"


def _rows(data, kind):
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for key in ("results", "records", kind, "items", "data", "result"):
            if isinstance(data.get(key), list):
                return data[key]
    raise AuditError(f"平台{kind}清单格式异常，已停止本次切片。", "response")


class SlicingRemote:
    """Read-only inventory plus exact, journaled type-21 manual-cut writes."""

    def __init__(self, auth):
        self.auth = auth

    async def _get(self, path, params=None):
        return await self.auth.get_slicing(path, params)

    async def dataset(self, identity):
        if not str(identity).isdigit():
            raise ValueError("数据集编号无效。")
        data, _ = await self._get(f"devel/datasets/{identity}/", {"namespace": 1})
        if not isinstance(data, dict) or str(data.get("id")) != str(identity):
            raise AuditError("平台返回的数据集身份不一致。", "identity")
        return data

    async def list_datasets(self):
        """Walk the same lazy dataset-group tree used by the platform picker.

        The flat ``devel/datasets/`` endpoint returns every historical dataset
        and can exceed the server's page limit. The platform UI itself loads
        children through ``dataset-groups/<id>/details/``; walking that finite
        tree gives complete, namespace-scoped results without paginating the
        entire dataset archive.
        """
        rows, seen_datasets, seen_groups = [], set(), set()
        pending = ["0"]
        while pending:
            group_id = pending.pop(0)
            if group_id in seen_groups:
                continue
            seen_groups.add(group_id)
            if len(seen_groups) > 10000:
                raise AuditError("数据集分组过多，无法安全完成目标定位。", "group_limit")
            data, _ = await self._get(f"devel/dataset-groups/{group_id}/details/", {"namespace": 1})
            node = self._dataset_group_node(data, group_id)
            child_groups = node.get("groups")
            datasets = node.get("datasets")
            if not isinstance(child_groups, list) or not isinstance(datasets, list):
                raise AuditError("数据集分组目录格式异常，不能确认图片目标。", "response")
            for group in child_groups:
                if not isinstance(group, dict) or not str(group.get("id", "")).isdigit():
                    raise AuditError("数据集分组身份不完整。", "identity")
                child_id = str(group["id"])
                if child_id not in seen_groups and child_id not in pending:
                    pending.append(child_id)
            for row in datasets:
                if not isinstance(row, dict) or not str(row.get("id", "")).isdigit():
                    raise AuditError("数据集清单身份不完整。", "identity")
                identity = str(row["id"])
                if identity in seen_datasets:
                    continue
                seen_datasets.add(identity)
                rows.append(row)
        return rows

    @staticmethod
    def _dataset_group_node(data, group_id):
        """Normalize the response envelope used by the platform's picker."""
        candidate = data
        for _ in range(3):
            if isinstance(candidate, list):
                matches = [row for row in candidate if isinstance(row, dict)
                           and isinstance(row.get("groups"), list) and isinstance(row.get("datasets"), list)]
                if len(matches) == 1:
                    return matches[0]
                if len(matches) > 1:
                    selected = next((row for row in matches if str(row.get("id")) == str(group_id)), None)
                    if selected:
                        return selected
                    raise AuditError("数据集分组响应包含多个不确定目录。", "identity")
                return {}
            if isinstance(candidate, dict):
                if isinstance(candidate.get("groups"), list) and isinstance(candidate.get("datasets"), list):
                    return candidate
                nested = next((candidate.get(key) for key in ("results", "records", "result", "data", "items")
                               if isinstance(candidate.get(key), (dict, list))), None)
                if nested is not None:
                    candidate = nested
                    continue
            break
        raise AuditError("数据集分组响应格式异常。", "response")

    async def videos(self, dataset):
        if str(dataset) not in SOURCE_DATASETS:
            raise ValueError("仅支持固定的两个视频数据集。")
        rows, seen, total, page = [], set(), None, 1
        while page <= 10000:
            data, headers = await self._get(f"devel/datasets/{dataset}/videos/", {
                "namespace": 1, "page": page, "page_size": 100, "cut_status": 0,
            })
            batch = _rows(data, "视频")
            try:
                reported = int(headers["x-total"]) if headers.get("x-total") else None
                next_page = int(headers["x-next-page"]) if headers.get("x-next-page") else None
            except (TypeError, ValueError):
                raise AuditError("视频列表分页信息异常。", "pagination") from None
            if page == 1:
                total = reported
            elif total is not None and reported != total:
                raise AuditError("读取期间视频列表发生变化，请重新扫描。", "pagination")
            for row in batch:
                if not isinstance(row, dict) or not str(row.get("id", "")).isdigit():
                    raise AuditError("视频清单缺少有效的视频编号。", "identity")
                identity = str(row["id"])
                if identity in seen:
                    raise AuditError("视频分页返回重复记录。", "pagination")
                seen.add(identity)
                row_cut = row.get("cutStatus", row.get("cut_status", 0))
                if str(row_cut) == "0":
                    rows.append(row)
            if next_page is not None:
                if next_page <= 0:
                    break
                if next_page != page + 1 or not batch:
                    raise AuditError("视频列表分页不连续。", "pagination")
            elif total is not None:
                if page * 100 >= total:
                    break
                if not batch:
                    raise AuditError("视频列表缺少记录。", "pagination")
            elif len(batch) < 100:
                break
            page += 1
        else:
            raise AuditError("视频列表超过分页上限。", "pagination")
        return rows

    async def jobs(self, dataset, job_type):
        if str(job_type) not in CUT_TYPES:
            raise ValueError("只允许读取视频切片任务类型21、22。")
        rows, seen, total, page = [], set(), None, 1
        while page <= 1000:
            data, headers = await self._get("devel/dataset-jobs/", {
                "dataset": dataset, "type": str(job_type), "page": page, "page_size": 100,
            })
            batch = _rows(data, "切片任务")
            try:
                reported = int(headers["x-total"]) if headers.get("x-total") else None
                next_page = int(headers["x-next-page"]) if headers.get("x-next-page") else None
            except (TypeError, ValueError):
                raise AuditError("切片任务分页信息异常。", "pagination") from None
            if page == 1:
                total = reported
            elif total is not None and total != reported:
                raise AuditError("切片任务列表正在变化，请稍后扫描。", "pagination")
            for row in batch:
                if not isinstance(row, dict) or not str(row.get("id", "")).isdigit() or "status" not in row:
                    raise AuditError("切片任务编号或状态缺失。", "identity")
                if str(row.get("type", job_type)) != str(job_type):
                    continue
                identity = str(row["id"])
                if identity in seen:
                    raise AuditError("切片任务分页返回重复记录。", "pagination")
                seen.add(identity)
                rows.append(row)
            if next_page is not None:
                if next_page <= 0:
                    break
                if next_page != page + 1 or not batch:
                    raise AuditError("切片任务分页不连续。", "pagination")
            elif total is not None:
                if len(rows) >= total:
                    break
                if not batch:
                    raise AuditError("切片任务列表不完整。", "pagination")
            elif len(batch) < 100:
                break
            page += 1
        else:
            raise AuditError("切片任务超过分页上限。", "pagination")
        return rows

    async def job(self, identity):
        if not str(identity).isdigit():
            raise ValueError("切片任务编号无效。")
        data, _ = await self._get(f"devel/dataset-jobs/{identity}/")
        if (not isinstance(data, dict) or str(data.get("id")) != str(identity)
                or str(data.get("type")) not in CUT_TYPES or "status" not in data):
            raise AuditError("平台返回的切片任务身份不一致。", "identity")
        return data

    async def logs(self, dataset, job_id):
        if not str(dataset).isdigit() or not str(job_id).isdigit():
            raise ValueError("切片日志身份无效。")
        result, seen, index = [], set(), 0
        for _ in range(100):
            data, _ = await self._get(f"devel/dataset-jobs/{job_id}/logs/", {"dataset": dataset, "index": index})
            if not isinstance(data, dict) or not isinstance(data.get("result"), list) or not isinstance(data.get("has_more", data.get("hasMore")), bool):
                raise AuditError("切片任务日志不完整。", "response")
            page_count = 0
            for stream in data["result"]:
                if not isinstance(stream, dict) or not isinstance(stream.get("values"), list):
                    raise AuditError("切片任务日志格式异常。", "response")
                for pair in stream["values"]:
                    if not isinstance(pair, list) or len(pair) != 2 or not isinstance(pair[1], str):
                        raise AuditError("切片任务日志行格式异常。", "response")
                    key = (str(pair[0]), pair[1])
                    if key not in seen:
                        seen.add(key)
                        result.append({"time": str(pair[0]), "message": pair[1]})
                        page_count += 1
            more = data.get("has_more", data.get("hasMore"))
            if not more:
                return result
            next_index = data.get("next_index", data.get("nextIndex"))
            index = int(next_index) if next_index is not None else index + page_count
        raise AuditError("切片任务日志超过读取上限。", "log_limit")

    async def image_rows(self, dataset, *, prefix=None, progress=None):
        if not str(dataset).isdigit() or (prefix is not None and not prefix):
            raise ValueError("图片查询范围无效。")
        rows, seen, total, page = [], set(), None, 1
        while page <= 10000:
            # The native UI maps its name field to search, then converts
            # pageSize to page_size in its request interceptor.
            params = {"namespace": 1, "page": page, "page_size": 100}
            if prefix is not None:
                params["search"] = prefix
            data, headers = await self._get(f"devel/datasets/{dataset}/images/", params)
            batch = _rows(data, "图片")
            try:
                reported = int(headers["x-total"]) if headers.get("x-total") else None
                next_page = int(headers["x-next-page"]) if headers.get("x-next-page") else None
            except (TypeError, ValueError):
                raise AuditError("目标图片分页信息异常。", "pagination") from None
            if page == 1:
                total = reported
            elif total is not None and total != reported:
                raise AuditError("目标图片列表正在变化，无法完成关联验证。", "pagination")
            for row in batch:
                if not isinstance(row, dict) or not str(row.get("id", "")).isdigit():
                    raise AuditError("目标图片身份不完整。", "identity")
                name = str(row.get("name", row.get("fileName", "")))
                if prefix is not None and prefix.casefold() not in name.casefold():
                    raise AuditError("平台未按图片名称筛选，已停止核验并保留任务凭证。", "image_filter")
                identity = str(row["id"])
                if identity in seen:
                    raise AuditError("目标图片清单返回重复记录。", "pagination")
                seen.add(identity)
                rows.append({key: row[key] for key in ("id", "name", "fileName", "taskId", "task_id",
                    "sourceVideoId", "source_video_id") if key in row})
            if progress:
                progress({"read": len(rows), "total": total, "pages": page, "scope": "prefix" if prefix else "dataset"})
            if next_page is not None:
                if next_page <= 0:
                    break
                if next_page != page + 1 or not batch:
                    raise AuditError("目标图片分页不连续。", "pagination")
            elif total is not None and len(rows) >= total:
                break
            elif total is None and len(batch) < 100:
                break
            page += 1
        else:
            raise AuditError("目标图片超过分页上限。", "pagination")
        if total is not None and len(rows) != total:
            raise AuditError("目标图片未完整读取。", "pagination")
        return rows

    async def is_image_dataset(self, dataset):
        """Use the namespace-scoped image content endpoint when type metadata is absent."""
        if not str(dataset).isdigit():
            return False
        data, _ = await self._get(f"devel/datasets/{dataset}/images/", {
            "namespace": 1, "page": 1, "page_size": 1,
        })
        rows = _rows(data, "图片")
        if any(not isinstance(row, dict) or not str(row.get("id", "")).isdigit() for row in rows):
            raise AuditError("图片目标集内容身份不完整，不能安全使用。", "identity")
        return True

    async def create_cut(self, source_dataset, video_id, target_dataset, intervals, interval_seconds, prefix, request_id):
        if str(source_dataset) not in SOURCE_DATASETS or not str(video_id).isdigit() or not str(target_dataset).isdigit():
            raise ValueError("切片任务绑定的源或目标数据集无效。")
        if not intervals or interval_seconds < 1 or len(intervals) > 100:
            raise ValueError("切片区间或间隔无效。")
        if not self.auth.connected:
            raise AuditError("视觉AI平台未连接，切片任务已保留。", "auth", 401)
        body = {
            "namespace": 1,
            "dataset": int(source_dataset),
            "type": 21,
            "config": {
                "source": {"datasets": [{"videos": {"ids": [int(video_id)]}}]},
                "target": {"dataset": {"id": int(target_dataset)}},
                "options": {
                    "slice_periods": [{"start": float(v["start"]), "end": float(v["end"])} for v in intervals],
                    "image": {"interval": int(interval_seconds), "interval_unit": "second", "prefix": prefix},
                    "sliceImage": True, "sliceVideo": False,
                },
            },
        }
        for attempt in range(2):
            token = self.auth.access
            try:
                response = await self.auth.client.post("devel/dataset-jobs/", json=body,
                    headers={"Authorization": f"Bearer {token}", "Idempotency-Key": request_id})
            except httpx.HTTPError:
                raise AuditError("切片提交响应不确定，平台对账后再继续。", "uncertain") from None
            if response.status_code == 401 and attempt == 0:
                # Refreshing requires a safe read; no write is replayed until the
                # server has explicitly rejected this request with 401.
                await self._get("devel/datasets/27912/", {"namespace": 1})
                continue
            if response.status_code in (429, 500, 502, 503, 504):
                raise AuditError("切片提交结果不确定，先核对平台任务，未重复提交。", "uncertain")
            if not response.is_success:
                raise AuditError(f"平台拒绝切片任务（HTTP {response.status_code}）。", "submit_rejected")
            try:
                data = self.auth.unpack(response)
            except AuditError as error:
                if error.code == "auth":
                    raise
                raise AuditError("切片提交响应不完整，先按任务清单对账。", "uncertain") from None
            if not isinstance(data, dict) or not str(data.get("id", "")).isdigit():
                raise AuditError("切片提交响应没有任务编号，先按任务清单对账。", "uncertain")
            if str(data.get("type", 21)) != "21":
                raise AuditError("平台返回的任务类型不符，未标记为成功。", "identity")
            return data
        raise AuditError("视觉AI会话已失效，请重新连接。", "auth", 401)


def log_matches_video(logs, *, video_id, name):
    id_re = re.compile(rf"(?<!\d){re.escape(str(video_id))}(?!\d)")
    for row in logs:
        message = row.get("message", "")
        if id_re.search(message) or (name and name in message):
            return True
    return False
