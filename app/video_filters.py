"""Read-only record filtering; time and identity facts are never rewritten."""
from datetime import datetime
from .recovery_results import partial_complete


def record_status(row):
    if row.get("deletion", {}).get("state") in ("pending", "error"):
        return "deleting"
    active = next(v for v in row["versions"] if v["revision"] == row["activeRevision"])
    if partial_complete(active):
        return 'saved'
    operation = row.get('operation') or {}
    if active['state'] == 'withdrawing':
        return 'review' if operation.get('state') in {'attention', 'conflict'} else 'deleting'
    if (operation.get('kind') == 'recover-decodable'
            and operation.get('version') == row['activeRevision']
            and operation.get('expectedRevision') == row['revision']
            and operation.get('state') in {'queued', 'running', 'waiting_connection', 'reconciling'}):
        return 'processing'
    if active["state"] in ("attention", "failed", "uncertain"):
        return "review"
    if active["state"] == "saved":
        return "saved"
    if active["state"] in ("downloading", "publishing") or active.get("downloadRequested"):
        return "download"
    return "ready" if active["state"] == "ready" else "waiting"


def matches_status(row, status):
    if status == 'complete':
        # Older clients may still request the partial-results subset. The
        # current UI groups these records with all other saved results.
        active = next(v for v in row['versions'] if v['revision'] == row['activeRevision'])
        return record_status(row) != 'deleting' and partial_complete(active)
    return status == 'all' or record_status(row) == status


def filter_records(rows, query):
    query = query or {}
    status, correction = query.get("status", "all"), query.get("correction", "all")
    basis = query.get("timeBasis", "quality")
    if status not in ("all", "ready", "waiting", "review", "download", "saved", "complete", "deleting", "processing"):
        raise ValueError("视频状态筛选无效。")
    if correction not in ("all", "corrected", "withdrawn", "uncorrected") or basis not in ("quality", "capture"):
        raise ValueError("更正或时间依据筛选无效。")
    first, last = query.get("dateFrom", ""), query.get("dateTo", "")
    for date in (first, last):
        if date:
            try:
                if datetime.strptime(date, "%Y-%m-%d").strftime("%Y-%m-%d") != date:
                    raise ValueError()
            except ValueError:
                raise ValueError("筛选日期无效。") from None
    if first and last and first > last:
        raise ValueError("筛选结束日期不能早于开始日期。")
    needles = [v.strip().upper().replace("未识别", "VIRTUAL") for v in query.get("plate", "").replace("，", ",").split(",") if v.strip()]
    found = []
    for row in rows:
        active = next(v for v in row["versions"] if v["revision"] == row["activeRevision"])
        date = (row["qualityStart"] if basis == "quality" else active["startTime"])[:10]
        has_correction = any(v["revision"] > 0 for v in row["versions"])
        actions = [v for v in row["requests"].values() if v.get("kind") in ("correction", "withdraw")]
        withdrawn = bool(actions and actions[-1]["kind"] == "withdraw")
        if (first and date < first or last and date > last
                or needles and not any(n in (row["name"] + " " + row["plate"]).upper() for n in needles)
                or not matches_status(row, status)
                or correction == "corrected" and not has_correction
                or correction == "uncorrected" and has_correction
                or correction == "withdrawn" and not withdrawn):
            continue
        found.append(row)
    return found
