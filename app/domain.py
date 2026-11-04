import csv
import hashlib
import io
import math
import re
from datetime import datetime, timedelta, timezone
from typing import Any

from .models import CollectionRule, SearchFilters
from .admission import admission_rule

BEIJING = timezone(timedelta(hours=8))
ORIGIN = "http://vision.lg.china-yongfeng.com"
SOURCE_URL = f"{ORIGIN}/srape-steel/#/judgement/judgement-list"
DEFAULT_TARGET = {
    "origin": ORIGIN,
    "cameraId": "40d496692fd8d3a55822b199be3289b7",
    "embedId": "ef259982807aa9de45f668892174bf09",
    "channelId": "1",
}
TARGET_URL = (
    f"{ORIGIN}/platform/#/camera/{DEFAULT_TARGET['cameraId']}/gather"
    f"?embedId={DEFAULT_TARGET['embedId']}&type=server"
)
BATCH_LIMIT = 20


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_time(value: Any) -> datetime | None:
    if value is None or value == "":
        return None
    try:
        if isinstance(value, (int, float)) or re.fullmatch(r"\d{10}(\d{3})?", str(value)):
            number = float(value)
            return datetime.fromtimestamp(number / 1000 if number > 1e11 else number, BEIJING)
        text = str(value).strip().replace("/", "-").replace("Z", "+00:00")
        parsed = datetime.fromisoformat(text)
        return parsed.replace(tzinfo=BEIJING) if parsed.tzinfo is None else parsed.astimezone(BEIJING)
    except (ValueError, OverflowError, OSError):
        return None


def format_time(value: Any) -> str:
    parsed = value if isinstance(value, datetime) else parse_time(value)
    return parsed.astimezone(BEIJING).strftime("%Y-%m-%d %H:%M:%S") if parsed else ""


def date_windows(filters: SearchFilters) -> list[tuple[datetime, datetime]]:
    try:
        first = datetime.strptime(filters.dateFrom, "%Y-%m-%d").replace(tzinfo=BEIJING)
        last = datetime.strptime(filters.dateTo, "%Y-%m-%d").replace(tzinfo=BEIJING)
        clock = lambda text: datetime.strptime(text, "%H:%M:%S" if text.count(":") == 2 else "%H:%M").time()
        start_clock, end_clock = clock(filters.timeFrom), clock(filters.timeTo)
    except ValueError as error:
        raise ValueError("日期或时间格式无效，请重新选择。") from error
    if last < first:
        raise ValueError("结束日期不能早于开始日期。")
    if (last - first).days > 30:
        raise ValueError("一次最多查询 31 天，请缩小日期范围。")
    windows = []
    for offset in range((last - first).days + 1):
        day = first + timedelta(days=offset)
        start, end = day.replace(hour=start_clock.hour, minute=start_clock.minute, second=start_clock.second), day.replace(
            hour=end_clock.hour, minute=end_clock.minute, second=end_clock.second
        )
        if end <= start:
            end += timedelta(days=1)
        windows.append((start, end))
    return windows


def split_values(text: str) -> list[str]:
    return [item for item in re.split(r"[\s,，;；、]+", text.strip()) if item]


def matches_record(record: dict, filters: SearchFilters) -> bool:
    selected_time = parse_time(record["sourceTime"])
    if not selected_time or not any(start <= selected_time <= end for start, end in date_windows(filters)):
        return False
    plates, stations = split_values(filters.plates.upper()), split_values(filters.station)
    return (
        (not plates or any(plate in record["plate"].upper() for plate in plates))
        and (not stations or bool(set(stations) & set(split_values(record["station"]))))
        and (not filters.weightBillNo or filters.weightBillNo in record["weightBillNo"])
    )


def intelligent_photo_count(detail: dict) -> int:
    # 原站“智能判级照片”只展示此字段；人工照片、扣重图和识别结果图不能代替。
    result = detail.get("totalCheckResult")
    photos = result.get("allOriginImageUrls") if isinstance(result, dict) else None
    if not isinstance(photos, list):
        return 0
    return len({photo.strip() for photo in photos if isinstance(photo, str) and photo.strip()
                and photo.strip().lower() not in ("null", "undefined", "[]")})


def has_real_plate(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    plate = value.strip().upper()
    placeholders = ("VIRTUAL", "未识别", "未知", "暂无车牌", "暂无号牌", "无车牌", "无号牌", "无牌",
                    "识别失败", "车牌未识别", "号牌未识别", "车牌识别失败", "UNKNOWN", "UNRECOGNIZED")
    return bool(plate and not plate.startswith(placeholders) and plate not in {
        "NULL", "UNDEFINED", "NONE", "N/A", "[]", "-", "--", "—", "无", "无牌", "无车牌",
        "未知", "未知车牌", "未识别", "未识别车牌",
    })


def approval_issue(record: dict, rule=None) -> str:
    rule = admission_rule(record, rule)
    problems = []
    count, seconds = record.get("photoCount"), record.get("inspectionDurationSeconds")
    checks = []
    if rule.requireRealPlate:
        checks.append((has_real_plate(record.get("plate")), "无真实车牌"))
    if rule.requireIntelligentPhotos:
        has_photo = isinstance(count, int) and not isinstance(count, bool) and count >= rule.minPhotoCount
        checks.append((has_photo, f"智能判级有效照片不足{rule.minPhotoCount}张"))
    if rule.matchMode == "all":
        problems.extend(message for passed, message in checks if not passed)
    elif checks and not any(passed for passed, _ in checks):
        problems.append("且".join(message for _, message in checks))
    if not isinstance(seconds, (int, float)) or isinstance(seconds, bool) or not math.isfinite(seconds):
        problems.append("缺少完整的质检起止时间")
    elif seconds <= rule.minInspectionMinutes * 60:
        problems.append(f"质检时长未超过{rule.minInspectionMinutes:g}分钟")
    return "；".join(problems)


def apply_admission(record: dict, rule=None) -> dict:
    """Reclassify facts while keeping missing/failed source data unapproved."""
    result = dict(record)
    result["sourceIssue"] = result.get("sourceIssue", result.get("issue", ""))
    result["admissionRule"] = admission_rule(record, rule).model_dump()
    result.pop("issue", None)
    if issue := result["sourceIssue"] or approval_issue(result):
        result["issue"] = issue
    return result


def normalize_record(raw: dict, detail: dict, filters: SearchFilters, issue: str = "", admission=None) -> dict:
    results = detail.get("stationResults") or []
    # 与原站实绩卡片一致：stationResults 最后一条是首次质检，第一条是最后一次质检。
    first, last = (results[-1], results[0]) if results else ({}, {})
    station = detail.get("stationNumbers") or raw.get("stationNumber") or ""
    if isinstance(station, list):
        station = "、".join(dict.fromkeys(str(value) for value in station))
    raw_plate = str(raw.get("carNumber") or "").strip()
    record = {
        "id": str(raw.get("flowCode") or raw.get("id") or ""),
        "flowCode": str(raw.get("flowCode") or ""),
        "plate": raw_plate if has_real_plate(raw_plate) else "VIRTUAL",
        "station": str(station),
        "weightBillNo": str(detail.get("weightBillNo") or raw.get("weightBillNo") or ""),
        "createTime": format_time(raw.get("createTime")),
        "checkStartTime": format_time(first.get("checkStartTime") or raw.get("checkStartTime")),
        "checkCompleteTime": format_time(last.get("checkCompleteTime") or raw.get("checkCompleteTime")),
    }
    record["sourceTime"] = record[filters.timeSource]
    start, end = parse_time(record["checkStartTime"]), parse_time(record["checkCompleteTime"])
    record["photoCount"] = intelligent_photo_count(detail)
    record["inspectionDurationSeconds"] = (end - start).total_seconds() if start and end else None
    if issue:
        record["issue"] = issue
    if not record["id"]:
        record["id"] = hashlib.sha256(repr(sorted(raw.items())).encode()).hexdigest()[:24]
        record["issue"] = "实绩缺少唯一标识，请在原站核对。"
    if not record["sourceTime"]:
        record["issue"] = record.get("issue") or "缺少所选实绩时间，请在原站核对。"
    return apply_admission(record, admission)


def task_key(target: dict, start: str, end: str) -> str:
    # 去重基于真实视频时段；同一实绩重复导入或两个实绩共用同一时段只创建一次。
    text = "|".join([target["origin"], target["cameraId"], target["embedId"], start, end])
    return hashlib.sha256(text.encode()).hexdigest()


def build_tasks(records: list[dict], rule: CollectionRule, target: dict, existing: set[str] | None = None,
                admission=None) -> list[dict]:
    seen = set(existing or ())
    existing_keys = set(seen)
    tasks, virtual_index = [], 0
    for record in sorted(records, key=lambda row: (row["sourceTime"], row["id"])):
        record = apply_admission(record, admission)
        parsed = parse_time(record["sourceTime"])
        start = parsed + timedelta(minutes=rule.offsetMinutes) if parsed else None
        end = start + timedelta(minutes=rule.durationMinutes) if start else None
        plate = record["plate"].strip()
        if plate.upper() == "VIRTUAL":
            virtual_index += 1
            name = f"VIRTUAL_{virtual_index:03d}"
        else:
            name = plate
        start_text, end_text = format_time(start), format_time(end)
        key = task_key(target, start_text, end_text)
        reason = record.get("issue") or approval_issue(record) or (
            "缺少车牌" if not plate else "缺少有效的实绩时间" if not parsed else "名称超过原站 15 字符限制" if len(name) > 15 else ""
        )
        status = "invalid" if reason else "duplicate" if key in seen else "ready"
        duplicate_source = "local" if key in existing_keys else "selection"
        if status == "duplicate":
            reason = ("本条已有采集，可打开已有视频查看或按当前区间重新采集"
                      if duplicate_source == "local" else "本次选择包含相同相机和采集时段，仅创建一次")
        if status == "ready":
            seen.add(key)
        tasks.append({
            "key": key, "recordId": record["id"], "flowCode": record["flowCode"], "plate": plate,
            "sourceTime": record["sourceTime"], "startTime": start_text, "endTime": end_text,
            "videoName": name, "taskName": name, "durationMinutes": rule.durationMinutes,
            "photoCount": record.get("photoCount", 0),
            "inspectionDurationSeconds": record.get("inspectionDurationSeconds"),
            "admissionRule": record["admissionRule"],
            "checkStartTime": record.get("checkStartTime", ""),
            "checkCompleteTime": record.get("checkCompleteTime", ""),
            "status": status, **({"reason": reason} if reason else {}),
            **({"duplicateSource": duplicate_source} if status == "duplicate" else {}),
        })
    return tasks


def gather_payload(task: dict, target: dict) -> dict:
    if issue := approval_issue(task):
        raise ValueError(f"任务未通过采集准入校验：{issue}。请重新查询实绩。")
    # 原站批量弹窗将时长转换为起止时间，videoDuration 置 0，其他空字段由原站清洗函数移除。
    return {
        "cameraId": target["cameraId"], "embedId": target["embedId"], "channelId": target.get("channelId") or "1",
        "isRepeat": False, "repeatCycle": "1h", "startTime": task["startTime"], "endTime": task["endTime"],
        "taskEndTime": task["endTime"], "taskName": task["taskName"], "videoName": task["videoName"],
        "videoDuration": 0, "taskType": "VIDEO", "cleanTime": "1",
    }


def export_csv(headers: list[str], rows: list[list]) -> str:
    stream = io.StringIO(newline="")
    writer = csv.writer(stream)
    writer.writerow(headers)
    for row in rows:
        values = [str(value if value is not None else "") for value in row]
        writer.writerow([f"'{value}" if re.match(r"^[=+\-@\t\r]", value) else value for value in values])
    return "\ufeff" + stream.getvalue()
