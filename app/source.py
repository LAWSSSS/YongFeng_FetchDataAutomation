import asyncio
from datetime import timedelta

from .domain import date_windows, format_time, matches_record, normalize_record, split_values
from .models import SearchFilters
from .remote import RemoteClient, RemoteError


def page_records(data) -> tuple[list[dict], int]:
    if not isinstance(data, dict) or not isinstance(data.get("records"), list) or "total" not in data:
        raise RemoteError("实绩列表结构发生变化，请检查原站接口。", "SCHEMA")
    rows = data["records"]
    if any(not isinstance(row, dict) for row in rows):
        raise RemoteError("实绩列表包含无法识别的记录。", "SCHEMA")
    try:
        total = int(data.get("total", len(rows)))
    except (TypeError, ValueError):
        raise RemoteError("实绩总数格式发生变化，请检查原站接口。", "SCHEMA") from None
    return rows, total


async def scan_source(client: RemoteClient, filters: SearchFilters, run: dict):
    windows = date_windows(filters)
    # 列表按创建时间查询；前后各扩展一天，再按详情中的所选实绩时间精确筛选。
    first, last = windows[0][0] - timedelta(days=1), windows[-1][1] + timedelta(days=1)
    semaphore = asyncio.Semaphore(4)
    seen = set()
    plates = split_values(filters.plates)
    params = {"startTime": format_time(first), "endTime": format_time(last), "pageSize": 100}
    if len(plates) == 1:
        params["carNumber"] = plates[0]
    if filters.weightBillNo:
        params["weightBillNo"] = filters.weightBillNo

    async def enrich(raw: dict) -> dict:
        async with semaphore:
            detail, issue = {}, ""
            if raw.get("flowCode"):
                try:
                    detail = await client.request("/intelligence/intelliTaskInfo/getCheckDetail", params={"flowCode": raw["flowCode"]})
                    if not isinstance(detail, dict):
                        raise RemoteError("详情数据格式异常。", "SCHEMA")
                except RemoteError as error:
                    if error.code in ("401", "11015", "403"):
                        raise
                    issue = f"详情读取失败：{error}"
            else:
                issue = "缺少实绩流水号，无法核对质检开始时间。"
            record = normalize_record(raw, detail, filters, issue, run.get("admissionRule"))
            run["scanned"] += 1
            run["message"] = f"正在核对实绩时间，已检查 {run['scanned']} 条"
            return record

    for page in range(1, 102):
        run["message"] = f"正在读取第 {page} 页实绩"
        rows, total = page_records(await client.request("/intelligence/intelliTaskInfo/page", params={**params, "pageIndex": page}))
        run["total"] = total
        if total > 10000 or page > 100:
            raise RemoteError("结果超过 10000 条，请缩小日期范围或增加车牌条件。", "LIMIT")
        if not rows:
            if len(seen) < total:
                raise RemoteError("原站分页数据不完整，请重新查询。", "PAGINATION")
            break
        ids = [str(row.get("flowCode") or row.get("id") or repr(row)) for row in rows]
        if any(identity in seen for identity in ids):
            raise RemoteError("原站分页数据发生重复，请重新查询，避免漏采。", "PAGINATION")
        seen.update(ids)
        for record in await asyncio.gather(*(enrich(row) for row in rows)):
            if matches_record(record, filters):
                run["records"].append(record)
            elif record.get("issue") and not record["sourceTime"]:
                # 时间缺失的行保留用于告知，不能提交。
                run["records"].append(record)
        if len(seen) >= total:
            break
    run["records"].sort(key=lambda row: (row["sourceTime"], row["id"]))


def demo_records(filters: SearchFilters) -> list[dict]:
    start, end = date_windows(filters)[0]
    examples = ["鲁Q8K327", "VIRTUAL", "鲁N6F218", "鲁Q7M506", "VIRTUAL", "鲁N9C632"]
    records = []
    step = min(timedelta(minutes=47), (end - start) / 8)
    for index, plate in enumerate(examples):
        timestamp = start + step * (index + 1)
        records.append({
            "id": f"demo-{index}", "flowCode": f"DEMO{index:04d}", "plate": plate,
            "station": str([2, 4, 6, 8][index % 4]), "weightBillNo": f"示例{index + 1:03d}",
            "createTime": format_time(timestamp), "checkStartTime": format_time(timestamp),
            "checkCompleteTime": format_time(timestamp + timedelta(minutes=18)),
            "photoCount": 1, "inspectionDurationSeconds": 18 * 60,
            "sourceTime": format_time(timestamp + timedelta(minutes=18) if filters.timeSource == "checkCompleteTime" else timestamp),
        })
    return [record for record in records if matches_record(record, filters)]
