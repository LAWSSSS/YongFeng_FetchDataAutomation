"""Independent local website for manual review and batch video collection."""
import asyncio
import hashlib
import mimetypes
import os
import re
from contextlib import asynccontextmanager
from pathlib import Path
from uuid import uuid4

import httpx
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import FileResponse, JSONResponse, Response, StreamingResponse
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
from .models import FileCleanupInput
from .models import SliceClassifyConfirmInput
from .slice_classify import SliceClassification
from .slice_output_organizer import SliceOutputOrganizer
from .original_restore import OriginalRestoreManager
from .models import (SlicingSettingsInput, SlicingScanInput, SlicingStartInput,
                     SlicingItemActionInput, SlicingBindInput, SlicingReviewInput,
                     SlicingProcessInput, SliceOutputOrganizationPreviewInput,
                     SliceOutputOrganizationConfirmInput)
from .remote import RemoteClient, RemoteError
from .source import scan_source
from .store import JobStore
from .submissions import SubmissionLog
from .submission_views import SubmissionViews, collection_record
from .models import SlicingRecordRemoveInput, RecordRestoreInput, GroupRecordRemoveInput
from .record_visibility import hide_groups, restore_groups
from .videos import EXTERNAL_VIDEO_DIR, Conflict, VideoManager
from .upload_audit import AuditError, AthenaClient, UploadAuditManager
from .audit_moves import AuditMoveManager
from .file_index import FileIndex
from .file_leases import FileLeases
from .uploads import UploadManager
from .slicing import SlicingManager, source_version
from .workspace import WorkspaceManager
from .models import WorkspacePreviewInput, WorkspaceActionInput
from . import runtime

ROOT = Path(__file__).resolve().parents[1]


def state_response(payload, request):
    response = JSONResponse(payload)
    etag = '"' + hashlib.sha256(response.body).hexdigest()[:24] + '"'
    headers = {"ETag": etag, "Cache-Control": "private, no-cache"}
    candidates = {value.strip().removeprefix("W/") for value in request.headers.get("if-none-match", "").split(",")}
    if etag in candidates:
        return Response(status_code=304, headers=headers)
    response.headers.update(headers)
    return response


def create_app(source: RemoteClient | None = None, target: RemoteClient | None = None,
               data_dir: Path | None = None, video_directory: Path | None = None,
               staging_directory: Path | None = None, audit_client: AthenaClient | None = None,
               upload_remote=None, upload_worker=None, upload_file_check=None, slicing_remote=None) -> FastAPI:
    source = source or RemoteClient("source")
    target = target or RemoteClient("target")
    production_data = data_dir is None
    data_dir = data_dir or (runtime.profile_directory() / "data" if runtime.portable() else ROOT / "data")
    store = JobStore(data_dir / "collection.sqlite3")
    submissions = SubmissionLog(store.db)
    admission_settings = AdmissionSettings(store.db)
    jobs = JobManager(target, store)
    fixed_directory = video_directory or (EXTERNAL_VIDEO_DIR if production_data and not runtime.portable() else None)
    if production_data and staging_directory is None:
        staging_directory = data_dir / "work"
    videos = VideoManager(jobs, target, data_dir, fixed_directory=fixed_directory,
                          require_mounted_external=production_data and video_directory is None and not runtime.portable(),
                          staging_directory=staging_directory)
    upload_audit = UploadAuditManager(store, audit_client or AthenaClient(persist_connection=production_data))
    audit_moves = AuditMoveManager(store, upload_audit, videos)
    leases = FileLeases()
    videos.file_leases = leases
    audit_moves.file_leases = leases
    uploads = UploadManager(store, upload_audit, leases, remote=upload_remote, worker=upload_worker,
                            file_check=upload_file_check)
    slicing = SlicingManager(store, upload_audit.client, data_dir, leases, remote=slicing_remote)
    slice_classification = SliceClassification(slicing.packages)
    files = FileIndex(store, videos, uploads, slicing, audit_moves, leases)
    slice_output_organizer = SliceOutputOrganizer(slicing)
    original_restore = OriginalRestoreManager(store.db, uploads, slicing, leases)
    slicing.files = files
    slicing.output_organizer = slice_output_organizer
    files.refresh()
    workspace = WorkspaceManager(store, data_dir, videos, uploads, slicing, upload_audit, audit_moves, files, leases)
    submission_views = SubmissionViews(store, slicing, submissions)
    slicing.submission_views = submission_views
    searches: dict[str, dict] = {}
    plans: dict[str, dict] = {}
    search_tasks: set[asyncio.Task] = set()
    target_config = videos.target_config
    saved_credentials: dict[str, dict[str, str]] = {}
    credential_errors: dict[str, str] = {}

    async def restore_saved_credentials():
        if not production_data:
            return
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
        await upload_audit.client.restore_saved_connection()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        await restore_saved_credentials()
        await workspace.startup()
        await videos.start()
        await uploads.start()
        await slicing.startup()
        await original_restore.startup()
        # Reconcile only legacy rows that were already archived as complete;
        # pending/attention items and restoration targets stay in place.
        files.schedule(slicing.wake.set, archive_only=True)
        yield
        for task in search_tasks:
            task.cancel()
        if search_tasks:
            await asyncio.gather(*search_tasks, return_exceptions=True)
        await original_restore.shutdown()
        await workspace.shutdown()
        await slicing.shutdown()
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
    application.state.original_restore = original_restore
    application.state.admission_settings = admission_settings
    application.state.uploads = uploads
    application.state.slicing = slicing
    application.state.files = files
    application.state.slice_output_organizer = slice_output_organizer
    application.state.workspace = workspace
    application.add_middleware(TrustedHostMiddleware, allowed_hosts=["localhost", "127.0.0.1", "[::1]"])

    @application.middleware("http")
    async def local_only(request: Request, call_next):
        if request.url.path.startswith("/api/"):
            if (workspace.maintenance and request.method in ('POST', 'PUT', 'PATCH', 'DELETE')
                    and not request.url.path.startswith('/api/workspace/')
                    and not request.url.path.endswith('/pause')):
                return JSONResponse({'message': '工作区正在整理，草稿和进度保留；完成后可继续操作。'}, status_code=409)
            origin = request.headers.get("origin")
            if (request.headers.get("sec-fetch-site") == "cross-site"
                    or (origin and origin != f"{request.url.scheme}://{request.url.netloc}")):
                return JSONResponse({"message": "仅允许本地网页访问此接口。"}, status_code=403)
            if request.method in ("POST", "PUT", "PATCH") and "application/json" not in request.headers.get("content-type", ""):
                return JSONResponse({"message": "请求必须使用 JSON。"}, status_code=415)
        response = await call_next(request)
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["Referrer-Policy"] = "no-referrer"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["Content-Security-Policy"] = (
            "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; "
            "img-src 'self' blob: data:; media-src 'self' blob:; connect-src 'self'; "
            "object-src 'none'; base-uri 'self'; form-action 'self'; frame-ancestors 'none'")
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
        return {"ok": True, "app": "inspection-video-gather-simple", **runtime.identity()}

    @application.get("/api/session")
    async def get_session():
        return session()

    @application.get('/api/workspace')
    async def workspace_state(request: Request):
        return state_response(workspace.state(), request)

    @application.post('/api/workspace/preview')
    async def workspace_preview(payload: WorkspacePreviewInput):
        return await workspace.preview(payload.parent.strip(), payload.expectedRevision, payload.requestId)

    @application.post('/api/workspace/preserve-uploaded')
    async def workspace_preserve_uploaded(payload: WorkspaceActionInput):
        return await workspace.preserve_uploaded(payload.expectedRevision, payload.requestId)

    @application.post('/api/workspace/{identity}/cancel')
    async def workspace_cancel(identity: str, payload: WorkspaceActionInput):
        return await workspace.cancel_preview(identity, payload.expectedRevision, payload.requestId)

    @application.post('/api/workspace/{identity}/apply', status_code=202)
    async def workspace_apply(identity: str, payload: WorkspaceActionInput):
        return await workspace.apply(identity, payload.expectedRevision, payload.requestId, payload.pilot)

    @application.post('/api/workspace/{identity}/retry', status_code=202)
    async def workspace_retry(identity: str, payload: WorkspaceActionInput):
        return await workspace.retry(identity, payload.expectedRevision, payload.requestId)

    @application.post('/api/workspace/reconcile')
    async def workspace_reconcile(payload: WorkspaceActionInput):
        if payload.expectedRevision != workspace.config['revision']:
            raise Conflict('工作区已更新，请刷新后重试。')
        return await workspace.reconcile()

    def unified_directory_only():
        if workspace.enabled or workspace.maintenance:
            raise Conflict('文件已由视频工作区统一整理，请使用“文件工作区”入口。')

    @application.get("/api/upload-audit")
    async def upload_audit_state():
        return upload_audit.state(videos.store.directory())

    @application.post("/api/upload-audit/login")
    async def upload_audit_login(payload: UploadAuditLoginInput):
        reconnecting = not upload_audit.client.connected
        if not reconnecting and (upload_audit.busy or uploads.busy or slicing.busy or slicing.packages.running):
            raise ValueError("切片、核对或上传正在进行，请先暂停对应队列再切换视觉AI账号。")
        await upload_audit.client.login(payload.account, payload.password)
        uploads.wake.set()
        await slicing.connection_restored()
        return upload_audit.state(videos.store.directory())

    @application.post("/api/upload-audit/logout")
    async def upload_audit_logout():
        if upload_audit.busy or uploads.busy or slicing.busy:
            raise ValueError("切片、核对或上传正在进行，请先暂停对应队列再断开。")
        uploads.grants.clear()
        try:
            await upload_audit.client.forget_saved_connection()
        finally:
            upload_audit.client.clear()
        return upload_audit.state(videos.store.directory())

    @application.post("/api/upload-audit/run", status_code=202)
    async def start_upload_audit(payload: UploadAuditInput):
        return upload_audit.start(payload.directory.strip(), payload.recursive)

    @application.get("/api/uploads")
    async def upload_state():
        return uploads.state()

    @application.get("/api/original-restore")
    async def original_restore_state(request: Request):
        if not request.client or request.client.host not in {"127.0.0.1", "::1"}:
            raise HTTPException(403, "仅允许本机访问。")
        return original_restore.state()

    @application.post("/api/original-restore/start", status_code=202)
    async def original_restore_start(request: Request):
        if not request.client or request.client.host not in {"127.0.0.1", "::1"}:
            raise HTTPException(403, "仅允许本机访问。")
        try:
            payload = await request.json()
        except Exception:
            raise ValueError("恢复请求格式无效。") from None
        if not isinstance(payload, dict) or set(payload) != {"requestId"}:
            raise ValueError("恢复请求格式无效。")
        return await original_restore.start(payload["requestId"])

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

    @application.post("/api/uploads/{identity}/remove")
    async def remove_upload(identity: str):
        return await uploads.remove(identity)

    @application.post("/api/uploads/{identity}/restore")
    async def restore_upload(identity: str):
        return await uploads.restore(identity)

    @application.get("/api/slicing")
    async def slicing_state(request: Request):
        return state_response(slicing.state(), request)

    @application.post('/api/slicing/records/remove')
    async def slicing_record_remove(payload: SlicingRecordRemoveInput):
        result = slicing.visibility.hide(slicing.store, payload.requestId,
                                        [choice.model_dump() for choice in payload.selections])
        return {**slicing.state(), 'recordRemoval': result}

    @application.post('/api/slicing/records/restore')
    async def slicing_record_restore(payload: RecordRestoreInput):
        slicing.visibility.restore(payload.requestId, payload.removalId)
        return slicing.state()

    @application.post('/api/slicing/records/remove-group')
    async def slicing_record_remove_group(payload: GroupRecordRemoveInput):
        if not payload.selections and not payload.receipts:
            raise ValueError('请先选择记录。')
        if any((submissions.get(choice.id) or {}).get('kind') != 'slicing' for choice in payload.receipts):
            raise ValueError('切片提交记录不存在，请刷新。')
        result = hide_groups(payload.requestId, [
            (slicing.visibility, slicing.store, [s.model_dump() for s in payload.selections]),
            (submissions.visibility, submissions, [s.model_dump() for s in payload.receipts]),
        ])
        return {**slicing.state(), 'recordRemoval': result}

    @application.post('/api/slicing/records/restore-group')
    async def slicing_record_restore_group(payload: RecordRestoreInput):
        restore_groups(payload.requestId, payload.removalId, [slicing.visibility, submissions.visibility])
        return slicing.state()

    @application.post("/api/slicing/execute-all", status_code=202)
    async def slicing_execute_all(payload: SlicingItemActionInput):
        result = await slicing.execute_all(payload.requestId, payload.expectedRevision)
        run = result.get("run") or {}
        submissions.add("slicing", "全部切片", {
            "requestId": payload.requestId,
            "phase": result.get("phase") or "",
            "message": run.get("message") or "已接受全部切片",
        }, request_id=payload.requestId)
        return result

    _collection_record = collection_record

    submissions.import_jobs(store.list(), _collection_record)

    @application.get("/api/submissions")
    async def submission_list(kind: str = ""):
        hidden = submissions.visibility.hidden()
        return [submission_views.project(row) for row in submissions.list(kind) if row['id'] not in hidden]

    @application.post('/api/submissions/records/remove')
    async def submission_remove_records(payload: SlicingRecordRemoveInput):
        return submissions.visibility.hide(submissions, payload.requestId, [s.model_dump() for s in payload.selections])

    @application.post('/api/submissions/records/restore')
    async def submission_restore_records(payload: RecordRestoreInput):
        return submissions.visibility.restore(payload.requestId, payload.removalId)

    @application.post('/api/submissions/items/remove')
    async def submission_remove_items(payload: SlicingRecordRemoveInput):
        return submissions.item_visibility.hide(submission_views, payload.requestId, [s.model_dump() for s in payload.selections])

    @application.post('/api/submissions/items/restore')
    async def submission_restore_items(payload: RecordRestoreInput):
        return submissions.item_visibility.restore(payload.requestId, payload.removalId)

    @application.get('/api/submissions/{identity}')
    async def submission_detail(identity: str, page: int = Query(1, ge=1), page_size: int = Query(20, ge=1, le=100)):
        row = submissions.get(identity)
        if not row or identity in submissions.visibility.hidden():
            raise HTTPException(404, '提交记录不存在。')
        return submission_views.project(row, detail=True, page=page, page_size=page_size)

    @application.delete("/api/submissions/{identity}")
    async def submission_delete(identity: str):
        row = submissions.get(identity)
        if not row:
            raise HTTPException(404, "记录不存在。")
        # A presentation log is not the durable task/identity ledger.
        submissions.delete(identity)
        return {"ok": True}

    @application.get("/api/files")
    async def file_state():
        files.refresh()
        return files.state()

    @application.post("/api/files/recheck")
    async def file_recheck():
        return await files.reconcile()

    @application.post("/api/files/{identity}/archive-retry")
    async def archive_retry(identity: str):
        return await files.retry(identity)

    @application.post("/api/files/{identity}/cleanup-confirmed")
    async def cleanup_confirmed_files(identity: str, payload: FileCleanupInput):
        return await files.cleanup_confirmed(identity, payload.requestId)

    @application.post("/api/slicing/classify/preview")
    async def preview_slice_classification(payload: DirectoryInput):
        unified_directory_only()
        return slice_classification.preview(payload.directory)

    @application.post("/api/slicing/classify/confirm")
    async def confirm_slice_classification(payload: SliceClassifyConfirmInput):
        unified_directory_only()
        return await slice_classification.confirm(payload.planId, payload.requestId, payload.expectedRevision)

    @application.post("/api/slicing/packages/{identity}/confirm")
    async def confirm_slice_package(identity: str):
        return slicing.confirm_package(identity)

    @application.post("/api/slicing/packages/pause")
    async def pause_slice_package_uploads():
        slicing.packages.pause()
        return slicing.state()

    @application.post("/api/slicing/packages/resume")
    async def resume_slice_package_uploads():
        slicing.packages.resume()
        return slicing.state()

    @application.post("/api/slicing/packages/{identity}/retry")
    async def retry_slice_package(identity: str, payload: SlicingItemActionInput):
        return await slicing.retry_package(identity, payload.model_dump())

    @application.get("/api/slicing/packages/{identity}/images/{name}")
    async def slice_package_image(identity: str, name: str):
        from fastapi.responses import FileResponse
        try:
            path = slicing.package_image(identity, name)
        except ValueError as error:
            raise HTTPException(404, str(error)) from None
        guard = leases.read(path)
        guard.__enter__()
        return FileResponse(path, media_type="image/jpeg", headers={"Cache-Control": "private, no-store"},
                            background=BackgroundTask(guard.__exit__, None, None, None))

    @application.post("/api/slicing/settings")
    async def slicing_settings(payload: SlicingSettingsInput):
        return slicing.update_settings(payload.model_dump())

    @application.post("/api/slicing/process-limit")
    async def slicing_process_limit(payload: SlicingProcessInput):
        return slicing.update_process_limit(payload.model_dump())

    @application.post("/api/slicing/organize-output/preview")
    async def slicing_output_organization_preview(payload: SliceOutputOrganizationPreviewInput):
        unified_directory_only()
        return slice_output_organizer.preview(payload.model_dump())

    @application.post("/api/slicing/organize-output/{identity}/confirm")
    async def slicing_output_organization_confirm(identity: str, payload: SliceOutputOrganizationConfirmInput):
        unified_directory_only()
        return await slice_output_organizer.confirm(identity, payload.model_dump())

    @application.post("/api/slicing/scan", status_code=202)
    async def slicing_scan(payload: SlicingScanInput):
        return slicing.scan(payload.requestId)

    @application.post("/api/slicing/start", status_code=202)
    async def slicing_start(payload: SlicingStartInput):
        result = await slicing.start(payload.requestId, payload.expectedRevision, payload.mode, payload.itemId)
        run = result.get('run') or {}
        submissions.add('slicing', '单条切片' if payload.mode == 'single' else '批量切片',
                        {'requestId': payload.requestId, 'runId': run.get('id'),
                         'itemIds': [payload.itemId] if payload.itemId else run.get('items', []),
                         'message': run.get('message') or '已接受切片操作。'}, request_id=payload.requestId)
        return result

    @application.post("/api/slicing/pause")
    async def slicing_pause():
        return slicing.pause()

    @application.post("/api/slicing/resume", status_code=202)
    async def slicing_resume(payload: SlicingScanInput):
        return slicing.resume(payload.requestId)

    @application.post("/api/slicing/recover-decodable-all", status_code=202)
    async def slicing_recover_decodable_all(payload: SlicingItemActionInput):
        return slicing.recover_decodable_all(payload.expectedRevision, payload.requestId)

    @application.post("/api/slicing/{identity}/retry", status_code=202)
    async def slicing_retry(identity: str, payload: SlicingItemActionInput):
        return slicing.retry(identity, payload.expectedRevision, payload.requestId)

    @application.post("/api/slicing/{identity}/recover-decodable", status_code=202)
    async def slicing_recover_decodable(identity: str, payload: SlicingItemActionInput):
        return slicing.retry(identity, payload.expectedRevision, payload.requestId, recover_decodable=True)

    @application.post("/api/slicing/{identity}/bind")
    async def slicing_bind(identity: str, payload: SlicingBindInput):
        return await slicing.bind(identity, payload.model_dump())

    @application.post("/api/slicing/{identity}/review")
    async def slicing_review(identity: str, payload: SlicingReviewInput):
        return await slicing.review(identity, payload.model_dump())

    @application.api_route("/api/slicing/{identity}/stream", methods=["GET", "HEAD"])
    async def slicing_stream(identity: str, request: Request):
        item = slicing.store.get("items", identity)
        if not item or not item.get("path"):
            raise HTTPException(404, "本地视频尚未绑定。")
        path = Path(item["path"])
        try:
            current = source_version(path)
        except (OSError, ValueError):
            raise HTTPException(409, "本地视频暂不可用，请重新绑定后查看。") from None
        if current != item.get("fileVersion"):
            raise HTTPException(409, "本地视频版本已变化，请重新扫描或绑定。")
        size = current["size"]
        range_header = request.headers.get("range", "")
        start, end, status = 0, size - 1, 200
        if range_header:
            match = re.fullmatch(r"bytes=(\d*)-(\d*)", range_header.strip())
            if not match or (not match.group(1) and not match.group(2)):
                return StreamingResponse(iter(()), status_code=416, headers={"Content-Range": f"bytes */{size}"})
            if match.group(1):
                start = int(match.group(1))
                end = int(match.group(2)) if match.group(2) else size - 1
            else:
                suffix = int(match.group(2))
                start, end = max(0, size - suffix), size - 1
            if start >= size or end < start:
                return StreamingResponse(iter(()), status_code=416, headers={"Content-Range": f"bytes */{size}"})
            end = min(end, size - 1)
            status = 206
        length = max(0, end - start + 1)
        headers = {"Accept-Ranges": "bytes", "Content-Length": str(length),
                   "Content-Type": mimetypes.guess_type(path.name)[0] or "application/octet-stream",
                   "Cache-Control": "no-store"}
        if status == 206:
            headers["Content-Range"] = f"bytes {start}-{end}/{size}"

        async def body():
            with slicing.leases.read(path):
                with path.open("rb") as stream:
                    stream.seek(start)
                    remaining = length
                    while remaining > 0:
                        block = await asyncio.to_thread(stream.read, min(1024 * 1024, remaining))
                        if not block:
                            break
                        remaining -= len(block)
                        yield block

        if request.method == "HEAD":
            return StreamingResponse(iter(()), status_code=status, headers=headers)
        return StreamingResponse(body(), status_code=status, headers=headers)

    @application.get("/api/slicing/{identity}/frame")
    async def slicing_frame(identity: str, time_seconds: float):
        from fastapi.responses import Response
        content = await slicing.frame(identity, time_seconds)
        return Response(content, media_type="image/jpeg", headers={"Cache-Control": "no-store"})

    @application.get("/api/upload-audit/moves")
    async def audit_move_state():
        if workspace.enabled:
            return {'plan': None, 'busy': workspace.maintenance, 'workspaceManaged': True, 'directory': str(workspace.root)}
        return {"plan": audit_moves.latest(), "busy": audit_moves.busy}

    @application.post("/api/upload-audit/moves/preview")
    async def preview_audit_moves(payload: AuditMovePreviewInput):
        unified_directory_only()
        return await audit_moves.preview(payload.model_dump())

    @application.post("/api/upload-audit/moves/{plan_id}/confirm", status_code=202)
    async def confirm_audit_moves(plan_id: str, payload: AuditMoveConfirmInput):
        unified_directory_only()
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
        if payload.remember and production_data:
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
        if not already_submitted:
            submissions.add("collection", "采集提交" if directory else "仅采集", _collection_record(job))
        return job

    @application.delete("/api/jobs/{job_id}")
    async def delete_job(job_id: str):
        jobs.remove(job_id)
        return {"ok": True}

    @application.get("/api/jobs")
    async def job_list(request: Request, summary: bool = False):
        rows = [row for row in store.list() if not row.get("deletedAt")]
        if not summary:
            return state_response(rows, request)
        result = [{key: row[key] for key in ("id", "status", "createdAt", "downloadAfterCollection", "deletedAt")
                 if key in row} | {"items": [{"status": item.get("status"), "videoName": item.get("videoName", "")}
                                           for item in row.get("items", [])]} for row in rows]
        return state_response(result, request)

    @application.post("/api/jobs/{job_id}/refresh")
    async def refresh_job(job_id: str):
        if not store.get(job_id):
            raise HTTPException(404, "批次不存在。")
        result = await jobs.reconcile(job_id)
        videos.wake.set()
        return result

    @application.get("/api/videos")
    async def video_state(request: Request):
        result = videos.state(dict(request.query_params))
        if request.query_params.get("summary") == "true":
            result = {**result, "videos": []}
        elif request.query_params.get("compact") == "true":
            result = {**result, "videos": [{key: value for key, value in row.items() if key != "requests"} |
                {"versions": [{key: value for key, value in version.items() if key != "beforeIds"}
                              for version in row.get("versions", [])]} for row in result["videos"]]}
        return state_response(result, request)

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

    @application.post('/api/videos/{identity}/hide-record')
    async def hide_video_record(identity: str, payload: HistoryRemoveInput):
        result = videos.record_visibility.hide(videos.store, payload.requestId,
                                               [{'id': identity, 'expectedRevision': payload.expectedRevision}])
        return {'id': identity, 'recordRemoval': result}

    @application.post('/api/videos/records/restore')
    async def restore_video_record(payload: RecordRestoreInput):
        videos.record_visibility.restore(payload.requestId, payload.removalId)
        return videos.state()

    @application.post('/api/videos/records/remove')
    async def hide_video_records(payload: SlicingRecordRemoveInput):
        result = videos.record_visibility.hide(videos.store, payload.requestId,
                                               [s.model_dump() for s in payload.selections])
        return {'recordRemoval': result}

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
        return FileResponse(path, headers={'Cache-Control': 'no-cache, must-revalidate'})

    return application


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(create_app(), host="127.0.0.1", port=int(os.environ.get("PORT", "8786")),
                access_log=False, timeout_graceful_shutdown=15)
