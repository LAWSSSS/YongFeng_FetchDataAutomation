"""Durable date-driven collection, using the existing guarded submitter.

Only normalized source facts and submission receipts are persisted.  A page is
committed as a unit, and an attempted remote write is never blindly replayed.
"""
import asyncio
import copy
import hashlib
import inspect
import json
import re
from datetime import timedelta
from uuid import uuid4

from .domain import (build_tasks, date_windows, format_time, matches_record,
                     normalize_record, now_iso, split_values)
from .models import CollectionRule, SearchFilters
from .remote import RemoteError
from .source import page_records
from .videos import Conflict


_TERMINAL = {"complete", "attention", "cancelled"}
_WAIT_CODES = {"NETWORK", "401", "403", "11015", "408", "425", "429", "500", "502", "503", "504"}


class CollectionRunRecords:
    """Visibility adapter; presentation removal never changes the run ledger."""

    def __init__(self, manager):
        self.manager = manager

    def get(self, identity):
        return self.manager.public(self.manager.get(identity))


class CollectionRunManager:
    def __init__(self, store, source, jobs, videos, admission_settings, target_config, on_job=None):
        self.store, self.db = store, store.db
        self.source, self.jobs, self.videos = source, jobs, videos
        self.admission_settings = admission_settings
        self.target_config, self.on_job = dict(target_config), on_job
        self.db.execute("CREATE TABLE IF NOT EXISTS simple_collection_runs "
                        "(id TEXT PRIMARY KEY, request_id TEXT UNIQUE NOT NULL, payload TEXT NOT NULL)")
        self.db.commit()
        from .record_visibility import RecordVisibility
        self.visibility = RecordVisibility(self.db, 'collection_runs')
        self.records = CollectionRunRecords(self)
        self.wake = asyncio.Event()
        self.task = None
        self.tick_lock = asyncio.Lock()
        self.resume_checked = set()
        self.poll_seconds = 30
        from .collection_review import CollectionReview
        self.review = CollectionReview(self)
        self.notified_jobs = set()

    def _save(self, row, *, commit=True):
        row["updatedAt"] = now_iso()
        self.db.execute("INSERT INTO simple_collection_runs VALUES(?,?,?) "
                        "ON CONFLICT(id) DO UPDATE SET payload=excluded.payload",
                        (row["id"], row["requestId"], json.dumps(row, ensure_ascii=False)))
        if commit:
            self.db.commit()
            if hasattr(self, 'review'):
                self.review.track(row)

    def get(self, identity):
        row = self.db.execute("SELECT payload FROM simple_collection_runs WHERE id=?", (identity,)).fetchone()
        return json.loads(row[0]) if row else None

    def list(self):
        return [json.loads(row[0]) for row in self.db.execute(
            "SELECT payload FROM simple_collection_runs ORDER BY rowid DESC")]

    def public(self, row, *, details=False, page=1, page_size=20):
        if row is None:
            return None
        if (row.get('status') in {'complete', 'attention'} and not row.get('pendingSubmissionId')
                and row.get('stage') in {'confirm', 'reconcile'} and any(
                    item['status'] == 'duplicate' for identity in row.get('jobIds', [])
                    for item in (self.store.get(identity) or {}).get('items', []))):
            # Old releases closed reused captures with zero ready items. Present
            # their current receipts without rewriting the submitted plan/history.
            row = copy.deepcopy(row)
            self.review.aggregate_counts(row)
            ready, waiting, attention = (row['counts'][key] for key in ('ready', 'waiting', 'attention'))
            row.update(status='waiting' if waiting else 'attention' if attention else 'complete',
                       stage='platform' if waiting else 'confirm',
                       errorCode='platform_attention' if attention else '',
                       message=f'等待平台采集完成，已可查看 {ready} 条。' if waiting else
                               f'{attention} 条采集需要核对，已保留原记录。' if attention else
                               f'{ready} 条采集已可查看，请确认最终区间。')
        fields = ("id", "revision", "status", "stage", "message", "filters", "rule", "admissionRule",
                  "admissionRevision", "counts", "scanned", "total", "createdAt", "updatedAt", "jobId", "errorCode", "planRevision", "pendingSubmissionId")
        value = {key: copy.deepcopy(row[key]) for key in fields if key in row}
        # Use a read-only UI snapshot version. The run's historical revision is
        # not a progress counter, and record removal must not rewrite that ledger.
        value['revision'] = int(hashlib.sha256(json.dumps(value, sort_keys=True,
            ensure_ascii=False, separators=(',', ':')).encode()).hexdigest()[:13], 16)
        if details:
            size = min(20, max(1, int(page_size)))
            current = max(1, int(page))
            records = row.get("records", [])
            value.update(records=copy.deepcopy(records[(current - 1) * size:current * size]),
                         recordCount=len(records), page=current, pageSize=size)
        return value

    def state(self, *, page=1, page_size=20, current_only=False):
        size = min(20, max(1, int(page_size)))
        hidden = self.visibility.hidden()
        rows = [row for row in self.list() if row['id'] not in hidden]
        if current_only:
            # The form needs outstanding plans and the result of the latest
            # query. Submitted history belongs to the submission ledger.
            return {"runs": [self.public(row) for index, row in enumerate(rows)
                             if index == 0 or row["status"] not in _TERMINAL]}
        pages = max(1, (len(rows) + size - 1) // size)
        current = min(max(1, int(page)), pages)
        return {"runs": [self.public(row) for row in rows[(current - 1) * size:current * size]],
                "total": len(rows), "page": current, "pages": pages, "pageSize": size}

    def start(self, payload):
        request_id = payload.get("requestId")
        if not isinstance(request_id, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,100}", request_id):
            raise ValueError("请提供有效的请求编号。")
        supplied = payload.get("filters") or {key: payload[key] for key in
            ("dateFrom", "dateTo", "timeFrom", "timeTo", "plates", "station", "weightBillNo") if key in payload}
        filters = SearchFilters.model_validate(supplied)
        date_windows(filters)
        normalized = filters.model_dump()
        previous = self.db.execute("SELECT payload FROM simple_collection_runs WHERE request_id=?", (request_id,)).fetchone()
        if previous:
            row = json.loads(previous[0])
            if row["filters"] != normalized:
                raise Conflict("同一请求编号已用于其他日期条件，请重新开始。")
            return row
        admission = self.admission_settings.state()
        identity = uuid4().hex
        row = {"id": identity, "requestId": request_id, "revision": 0,
               "status": "queued", "stage": "query", "message": "已登记，正在准备查询质检。",
               "filters": normalized, "admissionRule": copy.deepcopy(admission["rule"]),
               "admissionRevision": admission["revision"], "target": dict(self.target_config),
               "rule": CollectionRule(offsetMinutes=5, durationMinutes=60).model_dump(),
               "createdAt": now_iso(), "records": [], "scanned": 0, "total": None,
               "page": 1, "seenIds": [], "pageIds": {}, "queryComplete": False,
               "planId": "collection-run-" + identity, "jobId": "", "counts": {},
               "errorCode": "", "reconcileAttempts": 0, "reviewVersion": 1}
        self._save(row)
        self.wake.set()
        return row

    async def startup(self):
        if self.task is None or self.task.done():
            # Old unsent plans require the same human approval as new plans.
            for row in self.list():
                if row.get('reviewVersion') or row.get('status') in _TERMINAL or not row.get('queryComplete'):
                    continue
                self.review.ensure(row)
                job = self.store.get(row.get('jobId', ''))
                if job:
                    for item in job['items']:
                        if item.get('status') == 'pending' and not item.get('submission'):
                            item['status'] = 'awaiting_confirmation'
                    job['status'] = 'stopped'
                    self.store.save(job)
                    by_record = {str(t.get('recordId')): t for t in job['items']}
                    for task in row['reviewTasks']:
                        old = by_record.get(task['recordId'])
                        if old:
                            task.update(status='ready' if old['status'] == 'awaiting_confirmation' else old['status'])
                row.update(reviewVersion=1, status='awaiting_review', stage='review',
                           message='未发送的采集计划等待人工确认；已有提交保留。')
                self._save(row)
            for row in self.list():
                if self.review.refresh_historical_duplicates(row):
                    self._save(row)
            self.resume_checked.clear()
            self.task = asyncio.create_task(self._loop())
            self.wake.set()

    async def shutdown(self):
        if self.task:
            self.task.cancel()
            await asyncio.gather(self.task, return_exceptions=True)
            self.task = None

    async def _loop(self):
        while True:
            self.wake.clear()
            await self.tick()
            try:
                await asyncio.wait_for(self.wake.wait(), timeout=self.poll_seconds)
            except asyncio.TimeoutError:
                pass

    async def tick(self):
        async with self.tick_lock:
            for row in reversed(self.list()):
                if row["status"] in _TERMINAL:
                    continue
                if row.get('status') == 'awaiting_review':
                    continue
                try:
                    if not row["queryComplete"]:
                        await self._query_page(row)
                    else:
                        if row.get('pendingSubmissionId'):
                            await self._advance_submission(row)
                        else:
                            self.review.ensure(row)
                            row.update(status='awaiting_review', stage='review', message='请核对并勾选要采集的视频。')
                            self._save(row)
                except asyncio.CancelledError:
                    # Every page and plan boundary was persisted before any side effect.
                    raise
                except RemoteError as error:
                    if error.code in _WAIT_CODES:
                        row.update(status="waiting_connection", errorCode="connection_unavailable",
                                   message="等待平台连接恢复；已查询和已提交的进度保留。")
                    else:
                        row.update(status="attention", errorCode="source_or_platform_data",
                                   message="平台数据无法可靠核对，请检查本次采集记录。")
                    self._save(row)
                except (ValueError, KeyError, TypeError):
                    row.update(status="attention", errorCode="collection_data_invalid",
                               message="本次采集信息无法核对，已保留进度，请查看记录。")
                    self._save(row)
                except Exception:
                    # Do not serialize exceptions: remote messages may contain credentials.
                    row.update(status="attention", errorCode="collection_error",
                               message="本次采集遇到异常，已保留进度；其他日期任务继续。")
                    self._save(row)

    @staticmethod
    def _ids(rows):
        ids = [str(raw.get("flowCode") or raw.get("id") or
                   hashlib.sha256(json.dumps(raw, sort_keys=True, ensure_ascii=False).encode()).hexdigest())
               for raw in rows]
        if len(set(ids)) != len(ids):
            raise RemoteError("实绩分页重复。", "PAGINATION")
        return ids

    async def _query_page(self, row):
        if not self.source.connected:
            raise RemoteError("等待质检连接。", "401")
        filters = SearchFilters.model_validate(row["filters"])
        windows = date_windows(filters)
        params = {"startTime": format_time(windows[0][0] - timedelta(days=1)),
                  "endTime": format_time(windows[-1][1] + timedelta(days=1)), "pageSize": 100}
        plates = split_values(filters.plates)
        if len(plates) == 1:
            params["carNumber"] = plates[0]
        if filters.weightBillNo:
            params["weightBillNo"] = filters.weightBillNo
        # Re-read the previous page after a restart before trusting a saved cursor.
        if row["page"] > 1 and row["id"] not in self.resume_checked:
            previous = row["page"] - 1
            rows, total = page_records(await self.source.request(
                "/intelligence/intelliTaskInfo/page", params={**params, "pageIndex": previous}))
            if total != row["total"] or self._ids(rows) != row["pageIds"].get(str(previous)):
                raise RemoteError("重启后实绩分页已变化。", "PAGINATION")
        self.resume_checked.add(row["id"])
        row.update(status="querying", stage="query", message=f"正在读取第 {row['page']} 页质检实绩。", errorCode="")
        self._save(row)
        rows, total = page_records(await self.source.request(
            "/intelligence/intelliTaskInfo/page", params={**params, "pageIndex": row["page"]}))
        if total < 0 or total > 10000 or row["page"] > 100:
            raise RemoteError("实绩结果超过查询范围。", "LIMIT")
        if row["total"] is not None and row["total"] != total:
            raise RemoteError("实绩分页总数发生变化。", "PAGINATION")
        ids, seen = self._ids(rows), set(row["seenIds"])
        if seen.intersection(ids) or len(seen) + len(ids) > total or (not rows and len(seen) < total):
            raise RemoteError("实绩分页不完整。", "PAGINATION")
        signature = hashlib.sha256(json.dumps({'page': row['page'], 'total': total, 'rows': rows},
            sort_keys=True, ensure_ascii=False, separators=(',', ':')).encode()).hexdigest()
        checkpoint = row.get('detailCheckpoint')
        if checkpoint and checkpoint.get('signature') != signature:
            raise RemoteError('待恢复页面已变化，未混用旧详情。', 'PAGINATION')
        if not checkpoint:
            checkpoint = {'signature': signature, 'page': row['page'], 'records': {}}
            row['detailCheckpoint'] = checkpoint
            self._save(row)
        semaphore = asyncio.Semaphore(4)

        async def enrich(raw, identity):
            async with semaphore:
                detail, issue = {}, ""
                if raw.get("flowCode"):
                    detail = await self.source.request("/intelligence/intelliTaskInfo/getCheckDetail",
                                                       params={"flowCode": raw["flowCode"]})
                    if not isinstance(detail, dict):
                        issue, detail = "质检详情格式异常，无法批准采集。", {}
                else:
                    issue = "缺少实绩流水号，无法核对质检时间。"
                record = normalize_record(raw, detail, filters, issue, row["admissionRule"])
                checkpoint['records'][identity] = record
                self._save(row)  # Success survives one peer's timeout or a restart.
                return record

        results = await asyncio.gather(*(enrich(raw, identity) for raw, identity in zip(rows, ids)
            if identity not in checkpoint['records']), return_exceptions=True)
        for result in results:
            if isinstance(result, BaseException):
                raise result
        for record in (checkpoint['records'][identity] for identity in ids):
            if matches_record(record, filters) or (record.get("issue") and not record["sourceTime"]):
                row["records"].append(record)
        row.pop('detailCheckpoint', None)
        row["pageIds"][str(row["page"])] = ids
        row["seenIds"].extend(ids)
        row["scanned"], row["total"] = len(row["seenIds"]), total
        row["page"] += 1
        row["queryComplete"] = len(row["seenIds"]) == total
        if row["queryComplete"]:
            row["records"].sort(key=lambda item: (item["sourceTime"], item["id"]))
            self.review.ensure(row)
            row.update(status="awaiting_review", stage="review", message="查询完成，请核对采集时间并选择要采集的视频。")
        self._save(row)
        self.wake.set()

    def _plan(self, row):
        # Store the exact rule snapshot, rather than applying settings changed later.
        tasks = build_tasks(row["records"], CollectionRule.model_validate(row["rule"]), row["target"],
                            self.store.known_keys(include_retired=False), row["admissionRule"])
        known_records = self.store.known_records(row["target"])
        for item in tasks:
            if item["status"] == "ready" and str(item["recordId"]) in known_records:
                item.update(status="duplicate", duplicateSource="local", reason="本条实绩已登记采集，不重复创建。")
        row["rejected"] = [item for item in tasks if item["status"] == "invalid"]
        row["duplicates"] = [item for item in tasks if item["status"] == "duplicate"]
        row["counts"] = {"matched": len(tasks), "approved": sum(item["status"] != "invalid" for item in tasks),
                         "rejected": len(row["rejected"]), "duplicate": len(row["duplicates"]),
                         "submitted": 0, "ready": 0, "waiting": 0, "attention": 0}
        plan = {"id": row["planId"], "searchId": row["id"], "demo": False, "target": row["target"],
                "rule": row["rule"], "admissionRule": row["admissionRule"],
                "admissionRevision": row["admissionRevision"], "createdAt": row["createdAt"],
                "tasks": [item for item in tasks if item["status"] != "invalid"]}
        row["plan"] = plan
        self._save(row)
        return plan

    async def _advance_submission(self, row):
        job = self.store.get(row["jobId"]) if row["jobId"] else next(
            (job for job in self.store.list() if job["planId"] == row["planId"]), None)
        if not job:
            plan = row.get("plan") or self._plan(row)
            known = self.store.known_records(row["target"])
            for task in plan['tasks']:
                if task['status']=='ready' and str(task['recordId']) in known:
                    task.update(status='duplicate', reason='本条实绩已登记，不重复采集。')
            if not any(item["status"] == "ready" for item in plan["tasks"]):
                row.update(status="complete", stage="complete", message="查询完成，没有需要新建的采集任务。")
                self._save(row)
                self.review.finish_empty(row, plan['tasks'])
                return
            if not self.jobs.client.connected:
                raise RemoteError("等待采集平台连接。", "401")
            if self.jobs.busy:
                row.update(status="queued", stage="submit", message="等待前一批提交结束。")
                self._save(row)
                return
            job = self.jobs.start(plan)  # No download consent before final interval confirmation.
        row["jobId"] = job["id"]
        row['jobIds'] = list(dict.fromkeys([*row.get('jobIds', []), job['id']]))
        self._save(row)  # Replays also find the deterministic plan if a crash precedes this save.
        if self.on_job:
            result = self.on_job(job, row)
            if inspect.isawaitable(result):
                await result
        self.videos.sync_jobs()
        self.videos.wake.set()
        if job["id"] in self.jobs.running:
            if job['id'] not in self.notified_jobs:
                self.notified_jobs.add(job['id'])
                self.jobs.running[job['id']].add_done_callback(lambda _: self.wake.set())
            row.update(status="submitting", stage="submit", message="正在分批提交采集，每批最多 20 条。")
            self._save(row)
            return
        if job["status"] == "stopped":
            row.update(status="cancelled", stage="paused", message="后续提交已停止；已提交采集保留。")
            self._save(row)
            return
        if job["status"] != "complete":
            job = await self.jobs.reconcile(job["id"])
            self.videos.sync_jobs()
            uncertain = [item for item in job["items"] if item["status"] in {"uncertain", "submitting"}]
            identity_issues = [item for item in job["items"] if item.get("bindingIssue") and
                               item.get("bindingIssue") != "submission_missing"]
            if identity_issues or any(item["status"] == "failed" for item in job["items"]):
                row.update(status="attention", stage="reconcile", errorCode="submission_identity",
                           message="采集任务身份需要核对；已提交任务保留，不重复发送。")
                self._save(row)
                self.review.finish(row, job)
                return
            if uncertain:
                row["reconcileAttempts"] += 1
                finished = row["reconcileAttempts"] >= 3
                row.update(status="attention" if finished else "waiting", stage="reconcile",
                           errorCode="submission_unconfirmed" if finished else "",
                           message="采集提交结果尚未确认，保留凭证核对，不重复发送。")
                self._save(row)
                if finished:
                    self.review.finish(row, job)
                return
            if any(item["status"] == "pending" for item in job["items"]):
                if not self.jobs.busy:
                    job["status"] = "running"
                    self.store.save(job)
                    self.jobs.launch(job)
                row.update(status="submitting", stage="submit", message="继续提交尚未发送的采集任务。")
                self._save(row)
                return
        self.videos.sync_jobs()
        progress = self.videos.job_progress(job)
        ready, waiting, attention = (progress[key] for key in ('ready', 'waiting', 'attention'))
        row["counts"].update(**progress, duplicate=sum(item["status"] == "duplicate" for item in job["items"]))
        row.update(status="waiting" if waiting else "attention" if attention else "complete",
                   stage="platform" if waiting else "confirm", errorCode="platform_attention" if attention else "",
                   message=f"等待平台采集完成，已可查看 {ready} 条。" if waiting else
                           f"{attention} 条采集需要核对，已保留原记录。" if attention else
                           f"{ready} 条采集已可查看，请确认最终区间。")
        self._save(row)
        if row['status'] in {'complete', 'attention'}:
            self.review.finish(row, job)
