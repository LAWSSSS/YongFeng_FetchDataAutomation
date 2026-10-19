"""Independent local website for manual review and batch video collection."""
import asyncio
import os
import re
from contextlib import asynccontextmanager
from pathlib import Path
from uuid import uuid4

import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from starlette.background import BackgroundTask
from starlette.middleware.trustedhost import TrustedHostMiddleware

from .domain import DEFAULT_TARGET, SOURCE_URL, TARGET_URL, apply_admission, build_tasks, date_windows, now_iso
from .admission import AdmissionSettings
from . import credentials
from .jobs import JobManager
from .media import download_url
from .models import (BulkVideoInput, CollectionRule, CorrectionInput, DirectoryInput, HistoryRemoveInput,
                     LoginInput, PreviewInput, RecordRemoveInput, SearchInput, SubmitInput, VersionActionInput,
                     UploadAuditInput, UploadAuditLoginInput, AuditMovePreviewInput, AuditMoveConfirmInput)
from .models import AdmissionSettingsInput, UploadScanInput
from .remote import RemoteClient, RemoteError
from .source import scan_source
from .store import JobStore
from .videos import EXTERNAL_VIDEO_DIR, Conflict, VideoManager
from .upload_audit import AuditError, AthenaClient, UploadAuditManager
from .audit_moves import AuditMoveManager
from .file_leases import FileLeases
from .uploads import UploadManager

ROOT = Path(__file__).resolve().parents[1]


def create_app(source: RemoteClient | None = None, target: RemoteClient | None = None,
               data_dir: Path | None = None, video_directory: Path | None = None,
               staging_directory: Path | None = None, audit_client: AthenaClient | None = None,
               upload_remote=None, upload_worker=None, upload_file_check=None) -> FastAPI:
    source = source or RemoteClient("source")
    target = target or RemoteClient("target")
    production_data = data_dir is None
    data_dir = data_dir or ROOT / "data"
    store = JobStore(data_dir / "collection.sqlite3")
    admission_settings = AdmissionSettings(store.db)
    jobs = JobManager(target, store)
    fixed_directory = video_directory or (EXTERNAL_VIDEO_DIR if production_data else None)
    if production_data and staging_directory is None:
        staging_directory = data_dir / "work"
    videos = VideoManager(jobs, target, data_dir, fixed_directory=fixed_directory,
                          require_mounted_external=production_data and video_directory is None,
                          staging_directory=staging_directory)
    upload_audit = UploadAuditManager(store, audit_client)
    audit_moves = AuditMoveManager(store, upload_audit, videos)
    leases = FileLeases()
    videos.file_leases = leases
    audit_moves.file_leases = leases
    uploads = UploadManager(store, upload_audit, leases, remote=upload_remote, worker=upload_worker,
                            file_check=upload_file_check)
    searches: dict[str, dict] = {}
    plans: dict[str, dict] = {}
    search_tasks: set[asyncio.Task] = set()
    target_config = videos.target_config
    saved_credentials: dict[str, dict[str, str]] = {}
    credential_errors: dict[str, str] = {}

    async def restore_saved_credentials():
        for service, client in (("source", source), ("target", target)):
            try:
                saved = await asyncio.to_thread(credentials.load, service)
            except Exception:
                credential_errors[service] = "无法读取本机配置中的登录信息。"
                continue
            if not saved:
                continue
            saved_credentials[service] = saved
            try:
                await client.login(saved["account"], saved["password"])
                if service == "target":
                    await jobs.camera(target_config)
                    videos.wake.set()
            except Exception:
                client.clear()
                credential_errors[service] = "本机配置中的登录信息已失效，请重新登录并更新保存项。"

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        await restore_saved_credentials()
        await videos.start()
        await uploads.start()
        yield
        for task in search_tasks:
            task.cancel()
        if search_tasks:
            await asyncio.gather(*search_tasks, return_exceptions=True)
        await uploads.shutdown()
        await audit_moves.shutdown()
        await videos.shutdown()
        await jobs.shutdown()
        await upload_audit.shutdown()
        await source.close()
        await target.close()
        store.close()

    application = FastAPI(title="简易版质检视频采集", lifespan=lifespan,
                          docs_url=None, redoc_url=None, openapi_url=None)
    application.state.jobs = jobs
    application.state.videos = videos
    application.state.store = store
    application.state.upload_audit = upload_audit
    application.state.audit_moves = audit_moves
    application.state.admission_settings = admission_settings
    application.state.uploads = uploads
    application.add_middleware(TrustedHostMiddleware, allowed_hosts=["localhost", "127.0.0.1", "[::1]"])

    @application.middleware("http")
    async def local_only(request: Request, call_next):
        if request.url.path.startswith("/api/"):
            origin = request.headers.get("origin")
            if origin and origin != f"{request.url.scheme}://{request.url.netloc}":
                return JSONResponse({"message": "仅允许本地网页访问此接口。"}, status_code=403)
            if request.method in ("POST", "PUT", "PATCH") and "application/json" not in request.headers.get("content-type", ""):
                return JSONResponse({"message": "请求必须使用 JSON。"}, status_code=415)
        response = await call_next(request)
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["Referrer-Policy"] = "no-referrer"
        if request.url.path.startswith("/api/"):
            response.headers["Cache-Control"] = "no-store"
        return response

    @application.exception_handler(RemoteError)
    async def remote_error(_: Request, error: RemoteError):
        code = 401 if error.code in ("401", "11015") else 502
        return JSONResponse({"message": str(error), "code": error.code}, status_code=code)

    @application.exception_handler(Conflict)
    async def conflict_error(_: Request, error: Conflict):
        return JSONResponse({"message": str(error)}, status_code=409)

    @application.exception_handler(AuditError)
    async def audit_error(_: Request, error: AuditError):
        return JSONResponse({"message": str(error), "code": error.code}, status_code=error.status)

    @application.exception_handler(ValueError)
    async def value_error(_: Request, error: ValueError):
        return JSONResponse({"message": str(error)}, status_code=400)

    @application.exception_handler(RequestValidationError)
    async def validation_error(_: Request, error: RequestValidationError):
        fields = [".".join(str(value) for value in item["loc"] if value != "body") for item in error.errors()]
        return JSONResponse({"message": "请核对输入：" + "、".join(fields)}, status_code=422)

    def session():
        def details(service, client):
            saved = saved_credentials.get(service)
            return {"connected": client.connected, "user": client.user,
                    "hasSavedCredentials": bool(saved), "savedAccount": saved.get("account", "") if saved else "",
                    "credentialWarning": credential_errors.get(service, "")}
        return {"source": details("source", source),
                "target": details("target", target),
                "capabilities": {"batchDownload": True},
                "sourceUrl": SOURCE_URL, "targetUrl": TARGET_URL}

    @application.get("/api/health")
    async def health():
        return {"ok": True, "app": "inspection-video-gather-simple"}

    @application.get("/api/session")
    async def get_session():
        return session()

    @application.get("/api/upload-audit")
    async def upload_audit_state():
        return upload_audit.state(videos.store.directory())

    @application.post("/api/upload-audit/login")
    async def upload_audit_login(payload: UploadAuditLoginInput):
        if upload_audit.busy or uploads.busy:
            raise ValueError("核对或上传正在进行，请先暂停上传再切换视觉AI账号。")
        await upload_audit.client.login(payload.account, payload.password)
        uploads.wake.set()
        return upload_audit.state(videos.store.directory())

    @application.post("/api/upload-audit/logout")
    async def upload_audit_logout():
        if upload_audit.busy or uploads.busy:
            raise ValueError("核对或上传正在进行，请先暂停上传再断开。")
        uploads.grants.clear()
        upload_audit.client.clear()
        return upload_audit.state(videos.store.directory())

    @application.post("/api/upload-audit/run", status_code=202)
    async def start_upload_audit(payload: UploadAuditInput):
        return upload_audit.start(payload.directory.strip(), payload.recursive)

    @application.get("/api/uploads")
    async def upload_state():
        return uploads.state()

    @application.post("/api/uploads/check-connection")
    async def check_upload_connection():
        # Return only capability booleans; credentials and object locations remain in memory.
        results = []
        for dataset in ("27912", "27944"):
            try:
                authorization = await uploads.remote.signature(dataset)
                results.append({"id": dataset, "ok": True, "multipart": True,
                                "hasPrefix": bool(authorization.get("prefix"))})
            except AuditError as error:
                results.append({"id": dataset, "ok": False, "code": error.code, "message": str(error)})
        return {"targets": results}

    @application.post("/api/uploads/scan", status_code=202)
    async def upload_scan(payload: UploadScanInput):
        return uploads.scan(payload.directory.strip(), payload.requestId)

    @application.post("/api/uploads/pause")
    async def pause_uploads():
        return await uploads.pause()

    @application.post("/api/uploads/resume")
    async def resume_uploads():
        return uploads.resume()

    @application.post("/api/uploads/{identity}/retry")
    async def retry_upload(identity: str):
        return uploads.retry(identity)

    @application.get("/api/upload-audit/moves")
    async def audit_move_state():
        return {"plan": audit_moves.latest(), "busy": audit_moves.busy}

    @application.post("/api/upload-audit/moves/preview")
    async def preview_audit_moves(payload: AuditMovePreviewInput):
        return await audit_moves.preview(payload.model_dump())

    @application.post("/api/upload-audit/moves/{plan_id}/confirm", status_code=202)
    async def confirm_audit_moves(plan_id: str, payload: AuditMoveConfirmInput):
        return audit_moves.start(plan_id, payload.requestId)

    @application.post("/api/session/{service}/login")
    async def login(service: str, payload: LoginInput):
        if service not in ("source", "target"):
            raise HTTPException(404, "系统不存在。")
        if (service == "source" and search_tasks) or (service == "target" and (jobs.busy or videos.running)):
            raise ValueError("这个网站有操作正在进行，请等待结束后切换账号。")
        client = source if service == "source" else target
        await client.login(payload.account, payload.password)
        if service == "target":
            try:
                await jobs.camera(target_config)
            except Exception:
                client.clear()
                raise
            videos.wake.set()
        if payload.remember:
            try:
                await asyncio.to_thread(credentials.save, service, payload.account.strip(), payload.password)
                saved_credentials[service] = {"account": payload.account.strip(), "password": payload.password}
                credential_errors.pop(service, None)
            except Exception:
                credential_errors[service] = "已登录，但本机配置未能保存密码。"
        return session()

    @application.post("/api/session/{service}/logout")
    async def logout(service: str):
        if service not in ("source", "target"):
            raise HTTPException(404, "系统不存在。")
        if (service == "source" and search_tasks) or (service == "target" and (jobs.busy or videos.running)):
            raise ValueError("这个网站有操作正在进行，请稍后断开。")
        (source if service == "source" else target).clear()
        return session()

    @application.get("/api/settings/admission")
    async def get_admission_settings():
        return admission_settings.state()

    @application.post("/api/settings/admission")
    async def save_admission_settings(payload: AdmissionSettingsInput):
        run = searches.get(payload.searchId) if payload.searchId else None
        if payload.searchId and (not run or run["status"] != "complete"):
            raise Conflict("请等待查询完成，或重新查询后再应用准入条件。")
        saved = admission_settings.save(payload.rule, payload.expectedRevision)
        if run:
            run["records"] = [apply_admission(row, saved["rule"]) for row in run["records"]]
            run.update(admissionRule=saved["rule"], admissionRevision=saved["revision"])
        return {**saved, "search": run}

    async def run_search(run, filters):
        try:
            await scan_source(source, filters, run)
            run.update(status="complete", message=f"找到 {len(run['records'])} 条质检实绩。")
        except asyncio.CancelledError:
            run.update(status="failed", message="查询已停止。")
            raise
        except (RemoteError, ValueError):
            run.update(status="failed", message="查询失败，请检查平台连接后重试。", records=[])

    @application.post("/api/search", status_code=202)
    async def search(payload: SearchInput):
        date_windows(payload.filters)
        if not source.connected:
            raise RemoteError("请先登录质检网站。", "401")
        if search_tasks:
            raise ValueError("已有查询正在进行。")
        admission = admission_settings.state()
        run = {"id": uuid4().hex, "status": "running", "filters": payload.filters.model_dump(),
               "admissionRule": admission["rule"], "admissionRevision": admission["revision"],
               "records": [], "scanned": 0, "total": 0, "message": "正在读取质检实绩。", "createdAt": now_iso()}
        searches[run["id"]] = run
        while len(searches) > 20:
            searches.pop(next(iter(searches)))
        task = asyncio.create_task(run_search(run, payload.filters))
        search_tasks.add(task)
        task.add_done_callback(search_tasks.discard)
        return run

    @application.get("/api/search/{run_id}")
    async def search_state(run_id: str):
        run = searches.get(run_id)
        if not run:
            raise HTTPException(404, "查询已过期，请重新查询。")
        return run if run["status"] != "running" else {**run, "records": []}

    @application.post("/api/preview")
    async def preview(payload: PreviewInput):
        run = searches.get(payload.searchId)
        if not run or run["status"] != "complete":
            raise ValueError("请先完成质检查询。")
        admission = admission_settings.state()
        if run.get("admissionRevision", 0) != admission["revision"]:
            raise Conflict("准入条件已改变，请将当前条件应用到查询结果，或重新查询后生成预览。")
        wanted = set(payload.recordIds)
        records = [row for row in run["records"] if row["id"] in wanted]
        if len(records) != len(wanted):
            raise ValueError("所选实绩已变化，请重新选择。")
        known = store.known_keys()
        rule = CollectionRule()
        provisional = build_tasks(records, rule, dict(target_config), known, admission["rule"])
        remote = {}
        if target.connected:
            await jobs.camera(target_config)
            remote = await jobs.remote_tasks(target_config, provisional)
            known.update(remote)
        tasks = build_tasks(records, rule, dict(target_config), known, admission["rule"])
        for task in tasks:
            if task["status"] == "duplicate" and task["key"] in remote:
                task.update(duplicateSource="remote", remoteId=str(remote[task["key"]].get("id") or ""),
                            reason="平台已有相同时段任务；简易版不自动认领其他程序的任务。")
        plan = {"id": uuid4().hex, "searchId": run["id"], "demo": False,
                "target": dict(target_config), "rule": rule.model_dump(),
                "admissionRule": admission["rule"], "admissionRevision": admission["revision"],
                "tasks": tasks, "createdAt": now_iso()}
        plans[plan["id"]] = plan
        while len(plans) > 50:
            plans.pop(next(iter(plans)))
        return plan

    @application.post("/api/jobs", status_code=202)
    async def submit(payload: SubmitInput):
        plan = plans.get(payload.planId)
        if not plan:
            raise ValueError("任务预览已过期，请重新生成。")
        already_submitted = any(job["planId"] == plan["id"] for job in store.list())
        if not already_submitted and plan.get("admissionRevision", 0) != admission_settings.state()["revision"]:
            raise Conflict("准入条件已改变，请按新条件重新核对采集预览；当前未提交任何任务。")
        if not target.connected:
            raise RemoteError("请先登录采集平台。", "401")
        directory = videos.store.directory() if payload.downloadAfterCollection else ""
        if payload.downloadAfterCollection and not directory:
            raise ValueError("请先设置最终视频保存文件夹，再选择采集完成后下载。")
        job = jobs.start(plan, download_directory=directory)
        videos.sync_jobs()
        videos.wake.set()
        return job

    @application.get("/api/jobs")
    async def job_list():
        return store.list()

    @application.post("/api/jobs/{job_id}/refresh")
    async def refresh_job(job_id: str):
        if not store.get(job_id):
            raise HTTPException(404, "批次不存在。")
        result = await jobs.reconcile(job_id)
        videos.wake.set()
        return result

    @application.get("/api/videos")
    async def video_state(request: Request):
        return videos.state(dict(request.query_params))

    @application.post("/api/videos/bulk-confirm", status_code=202)
    async def bulk_confirm(payload: BulkVideoInput):
        return await videos.bulk_action("confirm", payload.model_dump())

    @application.post("/api/videos/bulk-remove", status_code=202)
    async def bulk_remove(payload: BulkVideoInput):
        return await videos.bulk_action("remove", payload.model_dump())

    @application.post("/api/videos/check")
    async def check_videos():
        return await videos.check_once()

    @application.post("/api/settings/directory")
    async def save_directory(payload: DirectoryInput):
        return videos.configure_directory(payload.directory)

    @application.post("/api/videos/{identity}/corrections", status_code=202)
    async def correct(identity: str, payload: CorrectionInput):
        return await videos.correct(identity, payload.model_dump())

    @application.post("/api/videos/{identity}/check")
    async def check_video(identity: str):
        return await videos.check_video(identity)

    @application.post("/api/videos/{identity}/recapture", status_code=202)
    async def recapture_unavailable(identity: str, payload: VersionActionInput):
        return await videos.recapture_unavailable(identity, payload.model_dump())

    @application.post("/api/videos/{identity}/remove-uncorrected")
    async def remove_uncorrected(identity: str, payload: HistoryRemoveInput):
        return await videos.remove_uncorrected(identity, payload.model_dump())

    @application.post("/api/videos/{identity}/remove-record", status_code=202)
    async def remove_record(identity: str, payload: RecordRemoveInput):
        return await videos.remove_record(identity, payload.model_dump())

    @application.post("/api/videos/{identity}/cancel-clear")
    async def cancel_clear(identity: str, payload: HistoryRemoveInput):
        return await videos.cancel_clear(identity, payload.model_dump())

    @application.post("/api/videos/{identity}/cancel-download")
    async def cancel_download(identity: str, payload: VersionActionInput):
        return await videos.cancel_download(identity, payload.model_dump())

    @application.post("/api/videos/{identity}/withdraw", status_code=202)
    async def withdraw(identity: str, payload: VersionActionInput):
        return await videos.withdraw(identity, payload.model_dump())

    @application.post("/api/videos/{identity}/confirm", status_code=202)
    async def confirm(identity: str, payload: VersionActionInput):
        return await videos.confirm(identity, payload.model_dump())

    @application.post("/api/videos/{identity}/retry-download", status_code=202)
    async def retry_download(identity: str):
        return await videos.retry_download(identity)

    @application.api_route("/api/videos/{identity}/versions/{revision}/stream", methods=["GET", "HEAD"])
    async def stream_video(identity: str, revision: int, request: Request):
        row = videos.store.get(identity)
        if not row or row.get("deletedAt"):
            raise HTTPException(404, "视频记录不存在。")
        version = videos.version(row, revision)
        if not version or version["state"] not in ("ready", "saved", "failed", "publishing", "attention"):
            raise HTTPException(404, "视频尚未采集完成。")
        requested_range = request.headers.get("range", "")
        if requested_range and not re.fullmatch(r"bytes=(?:\d+-\d*|-\d+)", requested_range):
            raise HTTPException(416, "视频范围无效。")
        result = await videos.bound_result(version)
        url = download_url(result["url"])
        remote_request = target.client.build_request(request.method, url,
            headers={"Accept-Encoding": "identity", **({"Range": requested_range} if requested_range else {})})
        try:
            remote = await target.client.send(remote_request, stream=True, follow_redirects=False)
        except httpx.HTTPError:
            raise ValueError("视频暂时无法播放，请稍后重试。") from None
        if remote.status_code not in (200, 206):
            await remote.aclose()
            raise ValueError("平台暂时无法提供视频。")
        if remote.status_code == 206:
            bounds = re.fullmatch(r"bytes (\d+)-(\d+)/(\d+)", remote.headers.get("content-range", ""))
            requested = re.fullmatch(r"bytes=(\d+)-(\d*)", requested_range)
            if (not bounds or int(bounds[1]) > int(bounds[2]) or int(bounds[2]) >= int(bounds[3])
                    or (requested and (int(bounds[1]) != int(requested[1])
                                       or requested[2] and int(bounds[2]) > int(requested[2])))):
                await remote.aclose()
                raise ValueError("平台返回的视频范围与请求不符。")
        kind = remote.headers.get("content-type", "").split(";", 1)[0].lower()
        if kind and not kind.startswith("video/") and kind not in ("application/octet-stream", "binary/octet-stream", "application/mp4"):
            await remote.aclose()
            raise ValueError("平台返回的内容不是视频。")
        headers = {key: remote.headers[key] for key in ("content-length", "content-range", "accept-ranges") if key in remote.headers}
        headers["Cache-Control"] = "no-store"

        async def chunks():
            try:
                if remote.is_stream_consumed:
                    yield remote.content
                else:
                    async for chunk in remote.aiter_raw(64 * 1024):
                        yield chunk
            finally:
                await remote.aclose()

        return StreamingResponse(chunks(), status_code=remote.status_code, headers=headers,
                                 media_type="video/mp4", background=BackgroundTask(remote.aclose))

    dist = ROOT / "dist"
    if (dist / "assets").is_dir():
        application.mount("/assets", StaticFiles(directory=dist / "assets"), name="assets")

    @application.get("/")
    async def homepage():
        path = dist / "index.html"
        if not path.exists():
            return JSONResponse({"message": "请先运行 npm run build 构建网页。"}, status_code=503)
        return FileResponse(path)

    return application


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(create_app(), host="127.0.0.1", port=int(os.environ.get("PORT", "8786")),
                access_log=False, timeout_graceful_shutdown=15)
