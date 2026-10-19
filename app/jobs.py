import asyncio
from uuid import uuid4

from .domain import BATCH_LIMIT, approval_issue, format_time, gather_payload, now_iso, task_key
from .admission import admission_rule
from .remote import RemoteClient, RemoteError
from .source import page_records
from .store import JobStore


class JobManager:
    def __init__(self, client: RemoteClient, store: JobStore):
        self.client, self.store = client, store
        self.running: dict[str, asyncio.Task] = {}
        self.stops: set[str] = set()
        self.lock = asyncio.Lock()

    @property
    def busy(self) -> bool:
        return bool(self.running)

    async def camera(self, target: dict) -> dict:
        camera = await self.client.request(f"/camera/{target['cameraId']}")
        if not isinstance(camera, dict) or str(camera.get("id")) != target["cameraId"]:
            raise RemoteError("无法读取指定相机，请检查采集平台的账号权限。", "CAMERA")
        # get-embed-info 返回 CPU/温度等运行信息；实体详情接口才提供节点 id。
        embed = await self.client.request(f"/embed/{target['embedId']}")
        if not isinstance(embed, dict) or str(embed.get("id")) != target["embedId"]:
            raise RemoteError("无法读取指定采集节点，请检查账号权限。", "EMBED")
        if camera.get("embedId") and str(camera["embedId"]) != target["embedId"]:
            raise RemoteError("相机所属节点与指定采集节点不一致，请在原站核对。", "CAMERA")
        target["channelId"] = str(camera.get("channelId") or "1")
        return camera

    async def remote_parents(self, target: dict) -> dict[str, dict]:
        """Read a complete camera inventory, retaining identity even for equal windows."""
        # 原站列表的 startTime/endTime 筛选的是创建时间，不能拿视频历史时段做筛选。
        result, expected_total = {}, None
        for page in range(1, 101):
            data = await self.client.request("/data-acquisition/condition-query", method="POST", body={
                "cameraId": target["cameraId"], "currPage": page, "pageSize": 100,
                "orderList": [{"asc": False, "column": "gmt_modify"}],
            })
            rows, total = page_records(data)
            if total < 0 or (expected_total is not None and total != expected_total):
                raise RemoteError("原站任务总数在分页核对时发生变化，请稍后重新核对。", "PAGINATION")
            expected_total = total
            if total > 10000:
                raise RemoteError("相机已有任务超过 10000 条，无法完整核对重复任务，请先在原站整理。", "LIMIT")
            for row in rows:
                identity = str(row.get("id", "")) if row.get("id") is not None else ""
                if not identity or identity in result:
                    raise RemoteError("原站任务标识缺失或分页数据重复，暂不提交，请稍后重新核对。", "PAGINATION")
                if row.get("cameraId") is not None and str(row["cameraId"]) != str(target["cameraId"]):
                    raise RemoteError("原站任务列表包含其他相机，暂不提交，请稍后重新核对。", "CAMERA")
                result[identity] = row
            if len(result) > total:
                raise RemoteError("原站任务数量与分页总数不一致，请稍后重新核对。", "PAGINATION")
            if len(result) == total:
                return result
            if not rows:
                raise RemoteError("原站任务分页数据不完整，暂不提交，请稍后重新核对。", "PAGINATION")
        raise RemoteError("原站任务数量超出核对范围，暂不提交。", "LIMIT")

    async def remote_tasks(self, target: dict, tasks: list[dict]) -> dict[str, dict]:
        if not tasks:
            return {}
        result = {}
        for row in (await self.remote_parents(target)).values():
            start, end = format_time(row.get("startTime")), format_time(row.get("endTime"))
            if start and end and row.get("taskType", "VIDEO") == "VIDEO":
                result[task_key(target, start, end)] = row
        return result

    @staticmethod
    def parent_matches(row: dict, target: dict, item: dict) -> bool:
        return (str(row.get("cameraId", target["cameraId"])) == str(target["cameraId"])
                and str(row.get("taskType", "VIDEO")).upper() == "VIDEO"
                and not row.get("isRepeat")
                and format_time(row.get("startTime")) == format_time(item.get("startTime"))
                and format_time(row.get("endTime")) == format_time(item.get("endTime"))
                and str(row.get("videoName") or row.get("taskName") or "") == str(item.get("videoName") or ""))

    def bind_remote_item(self, target: dict, item: dict, parents: dict[str, dict]) -> tuple[dict | None, str]:
        """Bind only a known ID or a unique post-submission inventory difference."""
        identity = str(item.get("remoteId") or "")
        code, message, row = "", "", None
        if identity:
            candidate = parents.get(identity)
            if candidate and self.parent_matches(candidate, target, item):
                row = candidate
            else:
                code = "bound_identity_mismatch" if candidate else "bound_identity_missing"
                message = "已记录的原站任务 ID 缺失或名称、相机、采集时段不一致，请人工核对；不会改认其他任务。"
        else:
            snapshot = item.get("submission")
            valid = (isinstance(snapshot, dict) and snapshot.get("snapshotVersion") == 1
                     and isinstance(snapshot.get("beforeIds"), list)
                     and all(isinstance(value, str) and value for value in snapshot["beforeIds"])
                     and len(set(snapshot["beforeIds"])) == len(snapshot["beforeIds"])
                     and str(snapshot.get("cameraId")) == str(target["cameraId"])
                     and snapshot.get("startTime") == format_time(item.get("startTime"))
                     and snapshot.get("endTime") == format_time(item.get("endTime"))
                     and snapshot.get("videoName") == item.get("videoName")
                     and bool(snapshot.get("submittedAt")))
            if not valid:
                code, message = "legacy_binding_missing", "缺少提交前的完整任务 ID 快照，请人工关联原站任务 ID；不能凭同名或相同时段自动认领。"
            else:
                before = set(snapshot["beforeIds"])
                candidates = [candidate for key, candidate in parents.items()
                              if key not in before and self.parent_matches(candidate, target, snapshot)]
                if len(candidates) == 1:
                    row = candidates[0]
                elif candidates:
                    code, message = "submission_ambiguous", "提交后出现多个名称、相机和时段相同的新任务，请人工关联确切 ID；不会重复提交。"
                else:
                    code, message = "submission_missing", "尚未找到提交后新建的对应任务，请先在原站确认不存在，再继续提交。"
        if row is None:
            item.update(bindingVerified=False, bindingIssue=code, bindingMessage=message)
            return None, message
        item.update(remoteId=str(row["id"]), remoteStatus=str(row.get("taskStatus") or row.get("status") or ""),
                    bindingVerified=True, bindingIssue="", bindingMessage="")
        if not identity:
            item["bindingSource"] = "submission_difference"
        return row, ""

    def start(self, plan: dict, download_directory: str = "") -> dict:
        previous = next((job for job in self.store.list() if job["planId"] == plan["id"]), None)
        if previous:
            return previous
        if self.busy:
            raise ValueError("已有一批任务正在提交，请等待结束。")
        if plan["demo"]:
            raise ValueError("演示数据不能提交到采集平台，请查询真实实绩。")
        if any(task["status"] == "invalid" for task in plan["tasks"]):
            raise ValueError("预览中有无效任务，请取消选择有问题的实绩后重试。")
        if any(approval_issue(task) for task in plan["tasks"] if task["status"] == "ready"):
            raise ValueError("实绩未通过采集准入校验，请重新查询后生成预览。")
        if not any(task["status"] == "ready" for task in plan["tasks"]):
            raise ValueError("没有需要提交的新任务。")
        job = {
            "id": uuid4().hex, "planId": plan["id"], "target": dict(plan["target"]), "status": "running",
            "createdAt": now_iso(), "updatedAt": now_iso(),
            "admissionRule": admission_rule(rule=plan.get("admissionRule")).model_dump(),
            "downloadAfterCollection": bool(download_directory), "downloadDirectory": download_directory,
            "items": [{**task, "status": "duplicate" if task["status"] == "duplicate" else "pending"} for task in plan["tasks"]],
            "events": [{"at": now_iso(), "message": "已加入提交队列，正在核对相机和已有任务。"}],
        }
        self.store.save(job)
        self.launch(job)
        return job

    def launch(self, job: dict):
        job_id = job["id"]
        self.stops.discard(job_id)
        task = asyncio.create_task(self.execute(job))
        self.running[job_id] = task
        task.add_done_callback(lambda _: self.running.pop(job_id, None))

    async def execute(self, job: dict):
        async with self.lock:
            try:
                pending = [item for item in job["items"] if item["status"] == "pending"]
                for item in pending:
                    if issue := approval_issue(item):
                        item.update(status="failed", message=f"未批准采集：{issue}。请重新查询实绩。")
                if any(item["status"] == "failed" for item in pending):
                    raise ValueError("实绩未通过采集准入校验，已阻止提交。请重新查询。")
                await self.camera(job["target"])
                remote = await self.remote_tasks(job["target"], pending)
                local = self.store.known_keys(exclude_job=job["id"])
                for item in pending:
                    if row := remote.get(item["key"]):
                        item.update(status="duplicate", duplicateSource="remote", remoteId=str(row["id"]),
                                    remoteStatus=str(row.get("taskStatus") or row.get("status") or ""),
                                    message="原站已有相同相机和采集时段的任务，无需重复创建。")
                        if row.get("videoName"):
                            item.update(videoName=str(row["videoName"]), taskName=str(row.get("taskName") or row["videoName"]))
                    elif item["key"] in local:
                        item.update(status="duplicate", duplicateSource="local",
                                    message="本地记录已登记相同相机和采集时段，请核对原站状态。")
                pending = [item for item in pending if item["status"] == "pending"]
                self.event(job, f"核对完成，准备提交 {len(pending)} 条，每批最多 {BATCH_LIMIT} 条。")
                for offset in range(0, len(pending), BATCH_LIMIT):
                    if job["id"] in self.stops:
                        job["status"] = "stopped"
                        self.event(job, "已停止后续批次；已提交到原站的任务继续采集。")
                        return
                    batch = pending[offset:offset + BATCH_LIMIT]
                    before_ids = sorted(await self.remote_parents(job["target"]))
                    for item in batch:
                        item["status"] = "submitting"
                        item["submission"] = {
                            "snapshotVersion": 1, "beforeIds": before_ids, "cameraId": job["target"]["cameraId"],
                            "startTime": format_time(item["startTime"]), "endTime": format_time(item["endTime"]),
                            "videoName": item["videoName"], "submittedAt": now_iso(),
                        }
                    # 网络请求之前先持久化，崩溃恢复时该批次变为“待核对”。
                    self.event(job, f"正在提交第 {offset // BATCH_LIMIT + 1} 批，共 {len(batch)} 条。")
                    write_error = None
                    try:
                        await self.client.request("/data-acquisition/batchSave", method="POST",
                                                  body=[gather_payload(item, job["target"]) for item in batch], write=True)
                    except RemoteError as error:
                        # 原站可能部分写入后返回业务错误；任何写入失败都先核对，绝不直接重发。
                        write_error = error
                    for item in batch:
                        item.update(status="uncertain", message="原站写入已尝试，正在核对确切任务 ID。")
                    self.store.save(job)
                    try:
                        parents = await self.remote_parents(job["target"])
                    except RemoteError as error:
                        for item in batch:
                            item["message"] = f"提交结果需核对：{error}"
                        job["status"] = "attention"
                        self.event(job, "当前批次结果未确认，已暂停后续提交。请点击“核对原站状态”。")
                        return
                    for item in batch:
                        row, issue = self.bind_remote_item(job["target"], item, parents)
                        item.update(status="submitted" if row else "uncertain",
                                    message="已核实原站任务 ID，采集由平台执行。" if row else issue)
                    if write_error or any(item["status"] == "uncertain" for item in batch):
                        job["status"] = "attention"
                        self.event(job, "已核对当前批次的确切任务 ID，并暂停后续提交；可核对后继续剩余任务。")
                        return
                    self.event(job, f"第 {offset // BATCH_LIMIT + 1} 批已提交成功。")
                job["status"] = "complete"
                self.event(job, "全部任务已处理；可刷新原站状态查看视频采集结果。")
            except asyncio.CancelledError:
                for item in job["items"]:
                    if item["status"] == "submitting":
                        item.update(status="uncertain", message="程序退出时正在提交，请先核对原站。")
                job["status"] = "attention"
                self.event(job, "程序正在退出，运行状态已保存。")
                raise
            except (RemoteError, ValueError) as error:
                job["status"] = "attention"
                self.event(job, str(error))
            except Exception:
                job["status"] = "attention"
                self.event(job, "执行遇到异常，已暂停。请核对原站状态后继续。")
            finally:
                self.store.save(job)

    def event(self, job: dict, message: str):
        job["events"].append({"at": now_iso(), "message": message})
        self.store.save(job)

    async def reconcile(self, job_id: str) -> dict:
        if job_id in self.running:
            raise ValueError("任务正在提交，请等待当前批次结束。")
        job = self.store.get(job_id)
        if not job:
            raise KeyError("任务不存在。")
        active = [item for item in job["items"] if item["status"] != "removed"]
        remote = await self.remote_parents(job["target"]) if active else {}
        matched = 0
        for item in job["items"]:
            if item["status"] == "removed":
                continue
            if item["status"] == "pending" and not item.get("remoteId") and not item.get("submission"):
                continue  # 尚未写入的新任务由 execute 按原有规则完成初次去重。
            row, issue = self.bind_remote_item(job["target"], item, remote)
            if row:
                matched += 1
                if item["status"] in ("uncertain", "pending", "failed"):
                    item.update(status="submitted", message="已在原站找到对应采集任务。")
            else:
                item["message"] = issue
        if all(item["status"] == "removed" or (item["status"] in ("submitted", "duplicate") and item.get("bindingVerified"))
               for item in job["items"]):
            job["status"] = "complete"
        elif any(item.get("bindingIssue") for item in active):
            job["status"] = "attention"
        self.event(job, f"原站核对完成，找到 {matched} 条对应任务。")
        return job

    async def retry(self, job_id: str, confirmed_absent: bool) -> dict:
        if self.busy:
            raise ValueError("已有任务正在提交，请等待结束。")
        job = await self.reconcile(job_id)
        uncertain = [item for item in job["items"] if item["status"] == "uncertain"]
        if uncertain and not confirmed_absent:
            raise ValueError("仍有提交结果不明的任务；请先在原站确认不存在，再勾选确认并继续。")
        if any(item.get("remoteId") or item.get("bindingIssue") == "submission_ambiguous"
               for item in job["items"] if item["status"] in ("uncertain", "failed")):
            raise ValueError("已有任务 ID 或存在多个新任务，不能作为不存在而重发，请人工核对确切 ID。")
        for item in job["items"]:
            if item["status"] in ("uncertain", "failed"):
                item["status"] = "pending"
                if item.get("submission"):
                    item.setdefault("submissionHistory", []).append(item.pop("submission"))
        if not any(item["status"] == "pending" for item in job["items"]):
            return job
        job["status"] = "running"
        self.event(job, "继续提交剩余任务。")
        self.launch(job)
        return job

    async def shutdown(self):
        tasks = list(self.running.values())
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
