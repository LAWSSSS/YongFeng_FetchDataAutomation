"""Read local video names and compare them with the two datasets' import logs."""
import asyncio
import html
import json
import os
import re
import stat
import unicodedata
from collections import Counter
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from urllib.parse import unquote
from uuid import uuid4

import httpx

from .domain import now_iso, parse_time
from . import secure_store

ATHENA_ORIGIN = "http://pre-athena-mv.cisdigital.cn"
# DatasetJobType.IMPORT in Athena; VIDEO_CUT / VIDEO_AUTO_CUT are 21 / 22.
IMPORT_JOB_TYPE = "1"
AUDIT_POLICY_VERSION = 2
DATASETS = (
    {"id": "27912", "name": "永锋车斗检测"},
    {"id": "27944", "name": "永锋磁盘吊"},
)
VIDEO_EXTENSIONS = {".avi", ".mp4", ".mov", ".mpg", ".mpeg", ".wmv", ".mkv", ".flv", ".m4v", ".webm"}
_EXT = "|".join(sorted(value[1:] for value in VIDEO_EXTENSIONS))
_PATH_FILE = re.compile(rf"([^/\\\r\n\"'<>]+?\.(?:{_EXT}))(?=$|[\s\"',;\]\)])", re.I)
_NAME_MARKER = re.compile(r"视频无后缀名称\s*[:：]\s*(.+)$")
_PROBLEM = re.compile(r"\b(?:ERROR|CRITICAL|FATAL|Traceback)\b|失败|不支持|无法解析|已存在|already exists", re.I)
_ANSI = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]")


def timestamp_ns(value):
    """Athena log keys are Unix nanoseconds; ISO task dates carry a zone."""
    if value is None or value == "" or isinstance(value, bool):
        return None
    try:
        if re.fullmatch(r"\d{10,19}(?:\.\d+)?", str(value)):
            number = Decimal(str(value))
            scale = 1 if number >= Decimal('1e17') else 1000 if number >= Decimal('1e14') else 1000000 if number >= Decimal('1e11') else 1000000000
            ns = int(number * scale)
        else:
            date = parse_time(value)
            if not date:
                return None
            ns = int(Decimal(str(date.timestamp())) * 1000000000)
        # Invalid/opaque cursors must not be mistaken for an actual log date.
        return ns if 946684800000000000 <= ns < 4102444800000000000 else None
    except (InvalidOperation, ValueError, OverflowError):
        return None


def ns_iso(value):
    return datetime.fromtimestamp(value / 1000000000, timezone.utc).isoformat() if value is not None else ""


def creation_time(info):
    """Use the filesystem's birth time, never mtime, ctime or the video name."""
    value = getattr(info, "st_birthtime_ns", None)
    if value is None and getattr(info, "st_birthtime", None) is not None:
        value = int(Decimal(str(info.st_birthtime)) * 1000000000)
    return timestamp_ns(value)


def job_latest_ns(job):
    # A running import may continue after the file was created, even when its
    # start date is older. For finished imports, include their last update.
    if str(job["status"]) in ("1", "2"):
        return None
    dates = [timestamp_ns(job.get(key)) for key in
             ("utc_create", "utcCreate", "created_at", "createdAt", "utc_modify", "utcModify",
              "updated_at", "updatedAt", "finished_at", "finishedAt", "end_time", "endTime")]
    return max((value for value in dates if value is not None), default=None)


def in_file_window(latest, created):
    return latest is None or created is None or latest >= created


class AuditError(Exception):
    def __init__(self, message, code="audit_error", status=502):
        super().__init__(message)
        self.code, self.status = code, status


def is_import_job(job):
    return isinstance(job, dict) and str(job.get("type")) == IMPORT_JOB_TYPE


def dataset_url(identity):
    return f"{ATHENA_ORIGIN}/#/dataset/{identity}?tab=base&namespace=1"


def name_key(value):
    return unicodedata.normalize("NFC", value).casefold()


def clean_reference(value):
    value = html.unescape(unquote(value)).strip().strip("\"'` ")
    return value.replace("\\", "/").rsplit("/", 1)[-1]


def log_references(line):
    """Keep complete names/IDs; never compare only a plate or a timestamp."""
    line = _ANSI.sub("", line)
    marker = _NAME_MARKER.search(line)
    if marker:
        candidates = [marker[1]]
    else:
        candidates = [match[1] for match in _PATH_FILE.finditer(line)]
    result = set()
    for candidate in candidates:
        name = clean_reference(candidate)
        if not name or len(name) > 1024 or any(ord(char) < 32 for char in name):
            continue
        full = Path(name).suffix.lower() in VIDEO_EXTENSIONS
        result.add((name_key(name), "full" if full else "stem"))
    return result


def scan_directory(directory, recursive=False):
    path = Path(directory).expanduser()
    if not path.is_absolute():
        raise ValueError("请填写视频文件夹的完整路径。")
    if not path.is_dir():
        raise ValueError("视频文件夹不存在或外置盘尚未连接；核对不会创建新目录。")
    rows, ignored = [], 0
    try:
        def walk_error(error):
            raise error

        for folder, dirs, names in os.walk(path, followlinks=False, onerror=walk_error):
            dirs[:] = sorted(d for d in dirs if not d.startswith(".") and not (Path(folder) / d).is_symlink()) if recursive else []
            for name in sorted(names):
                file = Path(folder) / name
                if name.startswith(".") or file.suffix.lower() not in VIDEO_EXTENSIONS:
                    ignored += 1
                    continue
                info = file.lstat()
                if not stat.S_ISREG(info.st_mode):
                    ignored += 1
                    continue
                born = creation_time(info)
                rows.append({"name": name, "relativePath": str(file.relative_to(path)), "size": info.st_size,
                             "createdAt": ns_iso(born), "createdNs": str(born) if born is not None else "",
                             "fileVersion": {"device": info.st_dev, "inode": info.st_ino,
                                             "size": info.st_size, "modifiedNs": info.st_mtime_ns},
                             "issue": "文件为空，可能尚未保存完成。" if not info.st_size else ""})
                if len(rows) > 20000:
                    raise ValueError("目录超过 20000 个视频，请选择更小的目录分次核对。")
    except OSError:
        raise ValueError("视频目录暂时无法完整读取，请检查外置盘后重试。") from None
    rows.sort(key=lambda row: name_key(row["relativePath"]))
    return rows, ignored


class AthenaClient:
    """Only authentication and allowlisted GET requests; no upload/write API."""
    def __init__(self, transport=None, persist_connection=None):
        self.client = httpx.AsyncClient(base_url=f"{ATHENA_ORIGIN}/api/", trust_env=False,
            follow_redirects=False, timeout=httpx.Timeout(60, connect=12), transport=transport,
            headers={"Accept": "application/json", "aics-fronted": "true"})
        self.access, self.refresh, self.user = "", "", ""
        self.persist_connection = transport is None if persist_connection is None else persist_connection
        self.saved_connection = False
        self.connection_warning = ""
        self.auth_lock = asyncio.Lock()
        self.log_slots = asyncio.Semaphore(4)

    @property
    def connected(self):
        return bool(self.access)

    def clear(self):
        self.access = self.refresh = self.user = ""
        self.client.cookies.clear()

    @staticmethod
    def unpack(response):
        if response.status_code == 401:
            raise AuditError("视觉AI平台登录已失效，请重新连接后核对。", "auth", 401)
        if response.status_code == 403:
            raise AuditError("账号无权读取这个数据集或日志。", "permission", 403)
        if not response.is_success:
            raise AuditError(f"视觉AI平台返回 HTTP {response.status_code}，请稍后重试。", "remote")
        try:
            body = response.json()
        except ValueError:
            raise AuditError("视觉AI平台返回了无法识别的内容。", "response") from None
        if not isinstance(body, dict) or "data" not in body or body["data"] is None:
            raise AuditError("视觉AI平台返回的数据不完整，请核对登录状态。", "response")
        return body["data"]

    async def auth_request(self, path, payload):
        try:
            response = await self.client.post(path, json=payload)
        except httpx.HTTPError:
            raise AuditError("无法连接视觉AI平台，请检查网络。", "network") from None
        return self.unpack(response)

    async def login(self, account, password):
        async with self.auth_lock:
            self.clear()
            try:
                data = await self.auth_request("auth/login/", {"username": account.strip(), "password": password})
                if not isinstance(data, dict) or not isinstance(data.get("access"), str) or not data["access"]:
                    raise AuditError("视觉AI平台没有返回有效会话，请检查账号和密码。", "auth", 401)
                self.access, self.refresh, self.user = data["access"], data.get("refresh", ""), account.strip()
                await self._save_connection()
            except Exception:
                self.clear()
                raise

    async def _save_connection(self):
        if not self.persist_connection:
            return
        if not self.refresh:
            self.saved_connection = False
            self.connection_warning = "平台没有提供可续期凭证；当前连接有效，但关闭程序后需要重新连接。"
            return
        try:
            await asyncio.to_thread(secure_store.save_connection, self.refresh, self.user)
            self.saved_connection = True
            self.connection_warning = ""
        except Exception:
            self.saved_connection = False
            self.connection_warning = "连接有效，但系统钥匙串未能保存续期凭证；检查本机钥匙串访问后重新连接。"

    async def restore_saved_connection(self):
        if not self.persist_connection:
            return False
        try:
            saved = await asyncio.to_thread(secure_store.load_connection)
        except Exception:
            self.connection_warning = "无法读取本机钥匙串中的连接；请重新连接平台。"
            return False
        if not saved:
            return False
        old_refresh = saved["refresh"]
        self.refresh, self.user = old_refresh, saved.get("user", "")
        self.saved_connection = True
        try:
            data = await self.auth_request("auth/token/refresh/", {"refresh": old_refresh})
            access = data.get("access") if isinstance(data, dict) else None
            if not isinstance(access, str) or not access:
                raise AuditError("保存的视觉AI会话已失效，请重新连接。", "auth", 401)
            self.access = access
            self.refresh = data.get("refresh") or old_refresh
            if self.refresh != old_refresh:
                await self._save_connection()
            else:
                self.saved_connection = True
                self.connection_warning = ""
            return True
        except AuditError as error:
            self.clear()
            if error.status == 401:
                try:
                    await asyncio.to_thread(secure_store.delete_connection)
                except Exception:
                    pass
                self.saved_connection = False
                self.connection_warning = "保存的视觉AI会话已过期，请重新连接一次。"
            else:
                self.connection_warning = "保存的视觉AI会话暂时无法恢复；检查网络后可重新连接。"
            return False

    async def forget_saved_connection(self):
        if self.persist_connection:
            try:
                await asyncio.to_thread(secure_store.delete_connection)
            except Exception:
                self.connection_warning = "已断开本次会话，但系统钥匙串中的保存项未能清除。"
                raise AuditError(self.connection_warning, "secure_store") from None
        self.saved_connection = False
        self.connection_warning = ""

    async def get(self, path, params=None):
        # Upload-audit stays limited to its two datasets and IMPORT-only routes.
        # The separate slicing module uses its own guarded adapter for video cuts.
        if not re.fullmatch(r"devel/(?:datasets/(?:27912|27944)/|dataset-jobs/(?:\d+/logs/)?)", path):
            raise ValueError("不支持这个核对地址。")
        return await self._get_authenticated(path, params)

    async def get_slicing(self, path, params=None):
        # A distinct, exact allowlist prevents the slicing feature from
        # widening the existing import-audit route boundary.
        pattern = r"devel/(?:datasets/\d+/(?:videos|images)?/?|datasets/?|dataset-groups/\d+/details/?|dataset-jobs/(?:\d+/logs/?|\d+/?|)?)"
        if not re.fullmatch(pattern, path):
            raise ValueError("不支持这个切片地址。")
        return await self._get_authenticated(path, params)

    async def _get_authenticated(self, path, params=None):
        if not self.connected:
            raise AuditError("请先连接视觉AI平台。", "auth", 401)
        for attempt in range(3):
            token = self.access
            try:
                response = await self.client.get(path, params=params, headers={"Authorization": f"Bearer {token}"})
            except httpx.HTTPError:
                if attempt < 2:
                    await asyncio.sleep(attempt + 1)
                    continue
                raise AuditError("读取视觉AI平台超时或网络中断，已有核对结果已保留。", "network") from None
            if response.status_code == 401 and attempt < 2:
                async with self.auth_lock:
                    if token == self.access:
                        if not self.refresh:
                            self.clear()
                            raise AuditError("视觉AI平台登录已失效，请重新连接。", "auth", 401)
                        try:
                            data = await self.auth_request("auth/token/refresh/", {"refresh": self.refresh})
                            if not isinstance(data, dict) or not data.get("access"):
                                raise AuditError("视觉AI平台登录已失效，请重新连接。", "auth", 401)
                            self.access = data["access"]
                            next_refresh = data.get("refresh") or self.refresh
                            if next_refresh != self.refresh:
                                self.refresh = next_refresh
                                await self._save_connection()
                        except AuditError as error:
                            self.clear()
                            if error.status == 401:
                                try:
                                    await self.forget_saved_connection()
                                except AuditError:
                                    pass
                            raise
                continue
            if response.status_code in (429, 500, 502, 503, 504) and attempt < 2:
                await asyncio.sleep(attempt + 1)
                continue
            if response.status_code == 401:
                self.clear()
            data = self.unpack(response)
            return data, response.headers
        raise AuditError("视觉AI平台登录已失效，请重新连接。", "auth", 401)

    async def jobs(self, dataset_id):
        rows, seen, total, page = [], set(), None, 1
        while page <= 1000:
            data, headers = await self.get("devel/dataset-jobs/", {
                "dataset": dataset_id, "type": IMPORT_JOB_TYPE, "page": page, "page_size": 100,
            })
            if not isinstance(data, list):
                raise AuditError("平台任务列表格式不正确，无法确认日志是否完整。", "response")
            try:
                reported = int(headers["x-total"]) if "x-total" in headers else None
            except ValueError:
                raise AuditError("平台任务分页总数无效，无法完成核对。", "pagination") from None
            if reported is not None and reported < 0:
                raise AuditError("平台任务分页总数无效，无法完成核对。", "pagination")
            if page == 1:
                total = reported
            elif total is not None and reported != total:
                raise AuditError("读取期间任务列表发生变化，请重新核对。", "pagination")
            for row in data:
                if not isinstance(row, dict) or not str(row.get("id", "")).isdigit() or "type" not in row or "status" not in row:
                    raise AuditError("平台任务身份或状态不完整，无法完成核对。", "response")
                identity = str(row["id"])
                bound = row.get("dataset")
                if isinstance(bound, dict):
                    bound = bound.get("id")
                if bound is not None and str(bound) != dataset_id:
                    raise AuditError("平台任务与所选数据集不一致。", "identity")
                if identity in seen:
                    raise AuditError("平台分页返回重复任务，核对已停止。", "pagination")
                seen.add(identity)
                rows.append(row)
            next_page = headers.get("x-next-page")
            if total is None and next_page is None:
                raise AuditError("平台没有返回任务分页信息，无法确认历史日志是否读全。", "pagination")
            if next_page is not None:
                try:
                    # Athena's own client treats an empty X-Next-Page as the
                    # last page. Missing headers still require a proven total;
                    # the final count check below also detects truncated pages.
                    next_number = int(next_page) if next_page.strip() else -1
                except ValueError:
                    raise AuditError("平台任务分页信息无效。", "pagination") from None
                if next_number <= 0:
                    break
                if next_number != page + 1 or not data:
                    raise AuditError("平台任务分页不连续，无法确认日志是否完整。", "pagination")
            elif total is not None:
                if len(rows) >= total:
                    break
                if not data:
                    raise AuditError("平台任务分页缺少记录。", "pagination")
            elif len(data) < 100:
                break
            page += 1
        else:
            raise AuditError("平台任务页数超出限制，请联系维护人员。", "pagination")
        if total is not None and total != len(rows):
            raise AuditError("实际读取的任务数与平台总数不符，请重新核对。", "pagination")
        # Some server versions ignore the type filter. Pagination totals still
        # refer to the returned raw rows; only imports leave this boundary.
        return [row for row in rows if is_import_job(row)]

    async def job_evidence(self, dataset_id, job, since_ns=None):
        if not is_import_job(job):
            raise AuditError("仅允许读取数据集导入任务的日志；视频切片及其他任务不参与上传核对。", "task_type")
        evidence, index, seen, lines, reason, characters = {}, 0, set(), 0, "", 0
        retained, latest, undated = 0, None, False
        success = str(job["status"]) == "3"
        running = str(job["status"]) in ("1", "2")
        global_problems = []
        problem_stems = {}
        async with self.log_slots:
            for _ in range(100):
                data, _headers = await self.get(f"devel/dataset-jobs/{job['id']}/logs/", {"dataset": dataset_id, "index": index})
                more = data.get("has_more", data.get("hasMore")) if isinstance(data, dict) else None
                if not isinstance(data, dict) or not isinstance(data.get("result"), list) or not isinstance(more, bool):
                    raise AuditError("导入日志格式不完整，无法确认上传情况。", "response")
                for stream in data["result"]:
                    if not isinstance(stream, dict) or not isinstance(stream.get("values"), list):
                        raise AuditError("导入日志内容不完整。", "response")
                    for value in stream["values"]:
                        if not isinstance(value, list) or len(value) != 2 or not isinstance(value[1], str):
                            raise AuditError("导入日志行格式异常。", "response")
                        if (str(value[0]), value[1]) in seen:
                            continue
                        seen.add((str(value[0]), value[1]))
                        lines += 1
                        characters += len(value[1])
                        if characters > 8 * 1024 * 1024:
                            raise AuditError("单条任务日志超出读取上限，本条需要核对。", "log_limit")
                        observed = timestamp_ns(value[0])
                        if observed is not None:
                            latest = max(latest or observed, observed)
                        if observed is not None and since_ns is not None and observed < since_ns:
                            continue
                        undated |= observed is None
                        retained += 1
                        refs = log_references(value[1])
                        problem = bool(_PROBLEM.search(value[1]))
                        if problem and not refs:
                            global_problems.append(observed)
                        for key in refs:
                            if problem:
                                stem = Path(key[0]).stem if key[1] == "full" else key[0]
                                problem_stems.setdefault(stem, []).append(observed)
                            evidence.setdefault(key, []).append({"jobId": str(job["id"]),
                                "at": ns_iso(observed) or job.get("utc_create") or job.get("utcCreate") or job.get("created_at") or job.get("createdAt") or "",
                                "observedNs": str(observed) if observed is not None else "",
                                "fileProblem": problem,
                                "status": "uploaded" if success and not problem else "in_progress" if running else "review",
                            })
                if not more or running:
                    break
                new_index = data.get("index")
                if (not isinstance(new_index, (str, int)) or not str(new_index).isdigit()
                        or str(new_index) == str(index)):
                    reason = "日志分页游标没有前进。"
                    break
                index = new_index
            else:
                reason = "导入日志页数超出读取上限。"
        if success and not lines:
            reason = "成功任务已无可读取日志，可能超过日志保留期。"
        elif success and retained and not evidence:
            reason = "成功任务日志中没有可核对的视频文件名。"
        if since_ns is not None and undated:
            reason = reason or "导入日志缺少可确认的发生时间，需要核对。"
        for key, values in evidence.items():
            stem = Path(key[0]).stem if key[1] == "full" else key[0]
            for value in values:
                value["problemNs"] = [str(t) if t is not None else "" for t in global_problems + problem_stems.get(stem, [])]
                if success and reason:
                    value["status"] = "review"
        return {"evidence": evidence, "reason": reason, "running": running, "lines": lines,
                "latestNs": str(latest) if latest is not None else "", "undated": undated}

    async def close(self):
        await self.client.aclose()


def compare_files(files, targets):
    full_counts = Counter(name_key(row["name"]) for row in files)
    stem_counts = Counter(name_key(Path(row["name"]).stem) for row in files)
    output = []
    priority = {"uploaded": 3, "in_progress": 2, "review": 1}
    for local in files:
        item = {**local, "targets": {}}
        created = timestamp_ns(local.get("createdNs"))
        for target in targets:
            full = name_key(local["name"])
            stem = name_key(Path(local["name"]).stem)
            matches = list(target["evidence"].get((full, "full"), []))
            stem_matches = target["evidence"].get((stem, "stem"), [])
            if stem_counts[stem] == 1:
                matches += stem_matches
            if target.get("timeScoped"):
                matches = [value for value in matches if in_file_window(timestamp_ns(value.get("observedNs")), created)]
                matches = [{**value, "status": "review" if
                    (created is not None and timestamp_ns(value.get("observedNs")) is None)
                    or any(in_file_window(timestamp_ns(t), created) for t in value.get("problemNs", []))
                    else value["status"]} for value in matches]
                incomplete = any(in_file_window(timestamp_ns(issue.get("latestNs")), created) for issue in target["issues"])
            else:
                incomplete = not target["complete"]
                matches = [{**value,"status":"review" if value.get("problemNs") else value["status"]} for value in matches]
            if local["issue"] or full_counts[full] > 1:
                result = {"status": "review", "message": local["issue"] or "多个子目录存在同名视频，日志不能区分。"}
            elif matches:
                best = max(matches, key=lambda value: priority[value["status"]])
                result = {**best, "message": {"uploaded": "成功导入日志中已找到", "in_progress": "运行中任务的日志已出现此文件", "review": "失败任务或异常日志中出现，需要核对"}[best["status"]]}
            elif stem_counts[stem] > 1 and any(in_file_window(timestamp_ns(value.get("observedNs")), created)
                    for value in stem_matches):
                result = {"status": "review", "message": "日志未带扩展名，而本地有多个同名不同格式的视频。"}
            elif incomplete:
                result = {"status": "review", "message": "文件创建后的相关日志未能完整读取，暂不能确定。" if created else "部分历史日志未能完整读取，暂不能确定。"}
            elif target["runningJobs"]:
                result = {"status": "review", "message": "尚无成功记录；平台有未结束的导入任务，请完成后重查。"}
            else:
                result = {"status": "missing", "message": "文件创建后的导入日志中未见成功上传记录" if created else "全部可读导入日志中未见成功上传记录"}
            result.pop("observedNs", None)
            result.pop("problemNs", None)
            item["targets"][target["id"]] = result
        output.append(item)
    return output


class UploadAuditManager:
    def __init__(self, store, client=None):
        self.db, self.client = store.db, client or AthenaClient()
        self.db.execute("CREATE TABLE IF NOT EXISTS simple_upload_audit (id INTEGER PRIMARY KEY CHECK(id=1), payload TEXT NOT NULL)")
        self.db.commit()
        value = self.db.execute("SELECT payload FROM simple_upload_audit WHERE id=1").fetchone()
        self.saved = json.loads(value[0]) if value else {}
        self.run = self.saved.get("run")
        if self.run and self.run.get("status") == "running":
            self.run.update(status="interrupted", message="上次核对已中断，已有报告保留。请重新核对。")
        self.task = None
        self.file_move_busy = False

    @property
    def busy(self):
        return self.task is not None and not self.task.done()

    def save(self):
        self.saved["run"] = self.run
        self.db.execute("INSERT INTO simple_upload_audit VALUES (1, ?) ON CONFLICT(id) DO UPDATE SET payload=excluded.payload", (json.dumps(self.saved, ensure_ascii=False),))
        self.db.commit()

    def state(self, default_directory=""):
        workspace = getattr(self, 'workspace', None)
        managed = bool(workspace and workspace.enabled)
        return {"connected": self.client.connected, "user": self.client.user,
                "savedConnection": self.client.saved_connection,
                "connectionWarning": self.client.connection_warning,
                "directory": str(workspace.root) if managed else self.saved.get("directory") or default_directory,
                "workspaceManaged": managed, "recursive": True if managed else self.saved.get("recursive", False),
                "datasets": [{**d, "url": dataset_url(d["id"])} for d in DATASETS],
                "run": self.run, "report": self.saved.get("report")}

    def start(self, directory, recursive=False):
        workspace = getattr(self, 'workspace', None)
        if workspace and workspace.enabled:
            directory = workspace.require_directory(directory)
            recursive = True
        if workspace and workspace.maintenance:
            raise ValueError('工作区正在整理，请稍后核对。')
        if not self.client.connected:
            raise AuditError("请先连接视觉AI平台。", "auth", 401)
        if self.busy or self.file_move_busy:
            raise ValueError("已有核对正在进行，请等待完成。")
        self.run = {"id": uuid4().hex, "status": "running", "message": "正在读取本地视频文件名…",
                    "startedAt": now_iso(), "completedLogs": 0, "totalLogs": 0}
        self.saved.update(directory=directory, recursive=recursive)
        self.save()
        self.task = asyncio.create_task(self.audit(directory, recursive))
        return self.state()

    async def audit(self, directory, recursive):
        try:
            workspace = getattr(self, 'workspace', None)
            scanner = workspace.scan_directory if workspace and workspace.enabled else scan_directory
            files, ignored = await asyncio.wait_for(asyncio.to_thread(scanner, directory, recursive), timeout=20)
            if not files:
                raise ValueError("这个目录没有可核对的视频；旧报告已保留。")
            births = [timestamp_ns(file.get("createdNs")) for file in files]
            unknown_times = sum(value is None for value in births)
            # Unknown creation dates require the full history for those files.
            # Known files still get their own time window in compare_files.
            since_ns = min(births) if not unknown_times else None
            scope = {"basis": "local_birthtime", "since": ns_iso(since_ns), "unknownTimes": unknown_times}
            self.run.update(localCount=len(files), message="正在读取两个数据集的导入任务…")
            self.save()

            async def load_target(dataset):
                info, _ = await self.client.get(f"devel/datasets/{dataset['id']}/", {"namespace": 1})
                if not isinstance(info, dict) or str(info.get("id")) != dataset["id"]:
                    raise AuditError("返回的数据集身份与核对目标不一致。", "identity")
                jobs = await self.client.jobs(dataset["id"])
                imports = [job for job in jobs if is_import_job(job)]
                selected = [job for job in imports if in_file_window(job_latest_ns(job), since_ns)]
                return {**dataset, "name": info.get("name") or dataset["name"], "url": dataset_url(dataset["id"]),
                    "jobCount": len(jobs), "importCount": len(imports), "readImportCount": len(selected),
                    "skippedBeforeCreation": len(imports) - len(selected),
                    "runningJobs": sum(str(j["status"]) in ("1", "2") for j in selected),
                    "timeScoped": True, "complete": True, "issues": [], "evidence": {}, "jobs": selected}

            targets = list(await asyncio.gather(*(load_target(dataset) for dataset in DATASETS)))
            self.run["totalLogs"] = sum(len(t["jobs"]) for t in targets)
            self.run["skippedBeforeCreation"] = sum(t["skippedBeforeCreation"] for t in targets)
            self.run["scope"] = scope
            self.save()

            async def read_log(target, job):
                latest = job_latest_ns(job)
                def issue(message):
                    target["complete"] = False
                    target["issues"].append({"jobId": str(job["id"]), "message": message,
                        "latestNs": str(latest) if latest is not None else "", "latestAt": ns_iso(latest)})
                try:
                    found = await self.client.job_evidence(target["id"], job, since_ns=since_ns)
                    observed = timestamp_ns(found.get("latestNs"))
                    if str(job["status"]) not in ("1", "2") and latest is not None and observed is not None:
                        latest = max(latest, observed)
                    if found.get("undated"):
                        latest = None
                    if found["reason"]:
                        issue(found["reason"])
                    for key, values in found["evidence"].items():
                        target["evidence"].setdefault(key, []).extend(values)
                except AuditError as error:
                    issue(str(error))
                except Exception:
                    issue("这条日志内容异常，无法完成核对。")
                self.run["completedLogs"] += 1
                self.run["message"] = f"已核对 {self.run['completedLogs']} / {self.run['totalLogs']} 条导入日志"
                self.save()

            await asyncio.gather(*(read_log(target, job) for target in targets for job in target["jobs"]))
            rows = compare_files(files, targets)
            summaries = []
            for target in targets:
                summary = {key: value for key, value in target.items() if key not in ("jobs", "evidence")}
                summary["counts"] = dict(Counter(row["targets"][target["id"]]["status"] for row in rows))
                summaries.append(summary)
            self.saved["report"] = {"id": self.run["id"], "createdAt": now_iso(), "directory": directory,
                "policyVersion": AUDIT_POLICY_VERSION, "scope": scope,
                "recursive": recursive, "localCount": len(files), "ignoredCount": ignored, "datasets": summaries, "files": rows}
            message = "核对完成。结果按两个数据集分别列出。" if all(t["complete"] for t in targets) else "核对已结束；部分日志不完整，相关文件已标为待核对。"
            self.run.update(status="complete", message=message, finishedAt=now_iso())
            self.save()
        except asyncio.CancelledError:
            self.run.update(status="interrupted", message="核对已中断，之前的报告已保留。")
            self.save()
            raise
        except (AuditError, ValueError) as error:
            self.run.update(status="failed", message=str(error))
            self.save()
        except TimeoutError:
            self.run.update(status="failed", message="读取视频目录超时，请检查外置盘后重试。之前的报告已保留。")
            self.save()
        except Exception:
            self.run.update(status="failed", message="核对未能完成，之前的报告已保留。请重新连接后重试。")
            self.save()

    async def shutdown(self):
        if self.busy:
            self.task.cancel()
            await asyncio.gather(self.task, return_exceptions=True)
        await self.client.close()
