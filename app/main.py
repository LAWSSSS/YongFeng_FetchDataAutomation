"""Independent local website for manual review and batch video collection."""
import asyncio
import hashlib
import mimetypes
import os
import re
import zipfile
from contextlib import asynccontextmanager
from contextlib import AsyncExitStack, ExitStack
from pathlib import Path
from urllib.parse import quote
from uuid import uuid4

import httpx
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import FileResponse, JSONResponse, Response, StreamingResponse
from fastapi.staticfiles import StaticFiles
from starlette.background import BackgroundTask
from starlette.middleware.trustedhost import TrustedHostMiddleware

from .domain import SOURCE_URL, TARGET_URL, apply_admission, build_tasks, date_windows, now_iso
from .admission import AdmissionSettings
from . import credentials
from .jobs import JobManager
from .media import download_url, file_digest, video_filename
from .models import (BulkVideoInput, CollectionRule, CorrectionInput, DirectoryInput, HistoryRemoveInput,
                     LoginInput, PreviewInput, RecordRemoveInput, SearchInput, SubmitInput, VersionActionInput,
                     UploadAuditInput, UploadAuditLoginInput, AuditMovePreviewInput, AuditMoveConfirmInput)
from .models import AdmissionSettingsInput, UploadScanInput, VideoDownloadLimitInput
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
from .models import SlicingRecordRemoveInput, RecordRestoreInput, GroupRecordRemoveInput, ProcessingContinueInput
from .record_visibility import hide_groups, restore_groups
from .videos import EXTERNAL_VIDEO_DIR, Conflict, VideoManager
from .upload_audit import AuditError, AthenaClient, UploadAuditManager
from .audit_moves import AuditMoveManager
from .file_index import FileIndex
from .file_leases import FileLeases, finish_io
from .local_playback import LocalPlayback
from .availability import connection_state
from .connection_restore import ConnectionRestorer, connection_waiting, retryable_connection
from .uploads import UploadManager, inspect_video
from .slicing import SlicingManager, source_version
from .workspace import WorkspaceManager
from .media_control import MediaControl
from .pipeline import PipelineManager
from .final_processing import FinalProcessing
from .collection_runs import CollectionRunManager
from .models import CollectionRunInput, CollectionDraftInput, CollectionSubmitInput, FinalConfirmationInput, RecordDeletionInput
from .presentation import slicing_view
from .package_gallery import PackageGallery
from .models import WorkspacePreviewInput, WorkspaceActionInput
from .models import PipelineControlInput, PipelineBatchInput
from . import runtime
from .event_loop_health import EventLoopHealth

ROOT = Path(__file__).resolve().parents[1]


class DownloadReadLease:
    def __init__(self, *args, release, close_stream=None, **kwargs):
        self.release_read = release
        self.close_stream = close_stream
        super().__init__(*args, **kwargs)

    async def __call__(self, scope, receive, send):
        try:
            await super().__call__(scope, receive, send)
        finally:
            try:
                if self.close_stream:
                    await self.close_stream()
            finally:
                self.release_read()


class DownloadFileResponse(DownloadReadLease, FileResponse):
    pass


class DownloadStreamResponse(DownloadReadLease, StreamingResponse):
    pass


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
               upload_remote=None, upload_worker=None, upload_file_check=None, slicing_remote=None,
               web_directory: Path | None = None) -> FastAPI:
    source = source or RemoteClient("source")
    target = target or RemoteClient("target")
    production_data = data_dir is None
    managed_root = runtime.data_directory() if production_data else None
    if managed_root:
        from .managed_storage import prepare
        prepare(managed_root)
    data_dir = data_dir or runtime.profile_directory()
    store = JobStore(data_dir / "collection.sqlite3", managed_root=managed_root)
    submissions = SubmissionLog(store.db)
    admission_settings = AdmissionSettings(store.db)
    jobs = JobManager(target, store)
    fixed_directory = video_directory or (managed_root / '媒体' / '采集视频' if managed_root else None)
    if production_data and staging_directory is None:
        staging_directory = data_dir / "work"
    videos = VideoManager(jobs, target, data_dir, fixed_directory=fixed_directory,
                          require_mounted_external=False,
                          staging_directory=staging_directory)
    upload_audit = UploadAuditManager(store, audit_client or AthenaClient(persist_connection=production_data))
    # The app-level restorer remains active while a disconnected disk gates
    # local slicing. Its cloud receipts must still be able to reconnect.
    upload_audit.client.connection_managed = production_data
    audit_moves = AuditMoveManager(store, upload_audit, videos)
    leases = FileLeases()
    videos.file_leases = leases
    audit_moves.file_leases = leases
    uploads = UploadManager(store, upload_audit, leases, remote=upload_remote, worker=upload_worker,
                            file_check=upload_file_check)
    slicing = SlicingManager(store, upload_audit.client, data_dir, leases, remote=slicing_remote)
    if upload_file_check is None:
        # The LaunchAgent PATH usually omits Homebrew. Resolve ffprobe beside
        # the user's configured FFmpeg path, matching slicing and playback.
        uploads.file_check = lambda path, expected: inspect_video(
            path, expected, slicing.store.settings.get("frameToolPath"))
    videos.slicing = slicing
    slice_classification = SliceClassification(slicing.packages)
    files = FileIndex(store, videos, uploads, slicing, audit_moves, leases)
    slice_output_organizer = SliceOutputOrganizer(slicing)
    original_restore = OriginalRestoreManager(store.db, uploads, slicing, leases)
    slicing.files = files
    slicing.output_organizer = slice_output_organizer
    files.refresh()
    workspace = WorkspaceManager(store, data_dir, videos, uploads, slicing, upload_audit, audit_moves, files, leases)
    if managed_root:
        from .managed_storage import configure
        configure(workspace, managed_root)
        slicing.store.settings.update(runtime.slicing_paths() if runtime.portable() else runtime.source_model_paths())
        slicing.store.settings['localExport'] = True
        slicing.store.save_settings()
    media_control = MediaControl(store.db, videos, slicing, leases)
    media_control.submissions = submissions
    videos.control = slicing.control = slicing.packages.control = media_control
    pipeline = PipelineManager(store, workspace, media_control, videos, slicing)
    final_processing = FinalProcessing(store, videos, pipeline, workspace, media_control, slicing)
    collection_runs = CollectionRunManager(store, source, jobs, videos, admission_settings, videos.target_config,
        on_job=lambda job, run: submissions.add('collection', '日期采集', collection_record(job),
                                                request_id='collection-run:' + run['id']))
    slicing.on_recovery_change = videos.reconcile_recovery_results
    gallery = PackageGallery(slicing.packages, leases)
    submission_views = SubmissionViews(store, slicing, submissions)
    slicing.submission_views = submission_views
    from .record_deletion import RecordDeletion
    record_deletion = RecordDeletion(store.db, workspace, {
        'collection': (videos.record_visibility, videos.store),
        'slicing': (slicing.visibility, slicing.store),
        'receipts': (submissions.visibility, submissions),
        'receipt_items': (submissions.item_visibility, submission_views),
        'collection_runs': (collection_runs.visibility, collection_runs.records),
    })
    from .history_downloads import HistoryDownloads
    history_downloads = HistoryDownloads(videos, workspace)
    videos.history_downloads = history_downloads
    from .retired_receipts import RetiredReceipts
    retired_receipts = RetiredReceipts(store.db, slicing.packages.remote, slicing.remote, videos)
    searches: dict[str, dict] = {}
    plans: dict[str, dict] = {}
    search_tasks: set[asyncio.Task] = set()
    target_config = videos.target_config
    saved_credentials: dict[str, dict[str, str]] = {}
    credential_errors: dict[str, str] = {}
    connection_restorer = ConnectionRestorer()

    async def restore_saved_credentials():
        if not production_data:
            return
        async def restore_one(service, client):
            try:
                saved = await asyncio.to_thread(credentials.load, service)
            except Exception:
                credential_errors[service] = "无法读取本机配置中的登录信息。"
                return False
            if not saved:
                return False
            saved_credentials[service] = saved
            try:
                await client.login(saved["account"], saved["password"])
                if service == "target":
                    await jobs.camera(target_config)
                    videos.wake.set()
                credential_errors.pop(service, None)
                return False
            except Exception as error:
                if retryable_connection(error):
                    credential_errors[service] = "平台暂时不可达，已保留登录信息，连接恢复后自动重试。"
                    return True
                credential_errors[service] = "本机配置中的登录信息已失效，请重新登录并更新保存项。"
                return False
        async def restore_visual():
            restored = await upload_audit.client.restore_saved_connection()
            if restored:
                slicing.wake.set()
                slicing.packages.wake.set()
            return not restored and connection_waiting(upload_audit.client)
        for service, client in (("source", source), ("target", target)):
            connection_restorer.start(service, lambda service=service, client=client: restore_one(service, client))
        connection_restorer.start('visual', restore_visual)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        async def stop_searches():
            for task in search_tasks:
                task.cancel()
            if search_tasks:
                await asyncio.gather(*search_tasks, return_exceptions=True)

        # Register before starting: partial startup, cancellation and a failing
        # shutdown must still release every worker/client before closing SQLite.
        async with AsyncExitStack() as cleanup:
            cleanup.callback(store.close)
            for close in reversed((connection_restorer.shutdown, stop_searches,
                    retired_receipts.shutdown, history_downloads.shutdown, record_deletion.shutdown, gallery.shutdown, original_restore.shutdown, collection_runs.shutdown,
                    final_processing.shutdown, pipeline.shutdown, workspace.shutdown,
                    slicing.shutdown, uploads.shutdown, audit_moves.shutdown,
                    videos.shutdown, jobs.shutdown, upload_audit.shutdown, source.close, target.close)):
                cleanup.push_async_callback(close)
            await retired_receipts.start()
            await history_downloads.start()
            await record_deletion.start()
            await gallery.start()
            await restore_saved_credentials()
            await workspace.startup()
            await videos.start()
            # Video uploads remain disabled; retain historical receipts only.
            uploads.store.settings['enabled'] = False
            uploads.store.save_settings()
            await uploads.start()
            await slicing.startup()
            await pipeline.startup()
            await final_processing.startup()
            await collection_runs.startup()
            await original_restore.startup()
            files.schedule(slicing.wake.set, archive_only=True)
            if production_data:
                loop_health = EventLoopHealth()
                loop_health.start()
                cleanup.callback(loop_health.stop)
            yield

    application = FastAPI(title="简易版质检视频采集", lifespan=lifespan,
                          docs_url=None, redoc_url=None, openapi_url=None)
    application.state.history_downloads = history_downloads
    application.state.record_deletion = record_deletion
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
    application.state.media_control = media_control
    application.state.pipeline = pipeline
    application.state.final_processing = final_processing
    application.state.collection_runs = collection_runs
    application.state.package_gallery = gallery
    application.add_middleware(TrustedHostMiddleware, allowed_hosts=["localhost", "127.0.0.1", "[::1]"])

    @application.middleware("http")
    async def local_only(request: Request, call_next):
        if request.url.path.startswith("/api/"):
            if request.method == 'POST' and any(part in request.url.path for part in ('/records/restore', '/records/restore-group', '/submissions/items/restore', '/submissions/restore-group')):
                return JSONResponse({'message': '记录删除后不再提供恢复，请刷新页面。'}, status_code=410)
            if managed_root and request.method in ('POST', 'PUT', 'PATCH') and request.url.path in (
                    '/api/workspace/preview', '/api/videos/directory', '/api/directory', '/api/settings/directory'):
                return JSONResponse({'message': '文件由程序自动管理，无需设置目录。'}, status_code=409)
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
                    "availability": connection_state(client),
                    "hasSavedCredentials": bool(saved), "savedAccount": saved.get("account", "") if saved else "",
                    "credentialWarning": credential_errors.get(service, "")}
        return {"source": details("source", source),
                "target": details("target", target),
                "capabilities": {"batchDownload": True},
                "sourceUrl": SOURCE_URL, "targetUrl": TARGET_URL}

    def delete_records(request_id, groups):
        return JSONResponse(record_deletion.register(request_id, groups), status_code=202)

    @application.post('/api/records/delete', status_code=202)
    async def delete_record_selection(payload: RecordDeletionInput):
        groups = {}
        for choice in payload.selections:
            groups.setdefault(choice.kind, []).append({'id': choice.id, 'expectedRevision': choice.expectedRevision})
        return delete_records(payload.requestId, list(groups.items()))

    @application.get('/api/record-operations/{identity}')
    async def record_operation(identity: str):
        operation = record_deletion.get(identity)
        if not operation:
            raise HTTPException(404, '删除操作不存在。')
        return record_deletion.public(operation)

    @application.get("/api/health")
    async def health():
        return {"ok": True, "app": "inspection-video-gather-simple", **runtime.identity()}

    @application.get("/api/session")
    async def get_session():
        return session()

    @application.get('/api/workspace')
    async def workspace_state(request: Request):
        return state_response(workspace.state(), request)

    @application.post('/api/workspace/open')
    async def open_data_directory():
        if not managed_root:
            raise HTTPException(409, '当前为隔离测试实例。')
        import subprocess
        await asyncio.to_thread(subprocess.run, ['/usr/bin/open', str(managed_root)], check=True)
        return {'ok': True}

    @application.post('/api/workspace/cleanup-completed', status_code=202)
    async def cleanup_completed(payload: WorkspaceActionInput):
        return await workspace.request_cleanup_completed(payload.expectedRevision, payload.requestId)

    @application.get('/api/workspace/cleanup-summary')
    async def completed_cleanup_summary(request: Request):
        return state_response({'revision': workspace.config['revision'], 'cleanupCompleted': workspace.cleanup_summary()}, request)

    @application.get('/api/workspace/cleanup-operations/{identity}')
    async def cleanup_operation(identity: str, request: Request):
        value = workspace.cleanup_operation(identity)
        if not value:
            raise HTTPException(404, '清理操作不存在。')
        return state_response(value, request)

    @application.post('/api/collection-runs', status_code=202)
    async def create_collection_run(payload: CollectionRunInput):
        return collection_runs.public(collection_runs.start(payload.model_dump()))

    @application.get('/api/collection-runs')
    async def list_collection_runs(request: Request, page: int = Query(1, ge=1)):
        return state_response(collection_runs.state(page=page), request)

    @application.post('/api/collection-runs/records/remove')
    async def hide_collection_run_records(payload: SlicingRecordRemoveInput):
        return delete_records(payload.requestId, [('collection_runs', [s.model_dump() for s in payload.selections])])


    @application.post('/api/collection-runs/records/restore')
    async def restore_collection_run_records(payload: RecordRestoreInput):
        result = collection_runs.visibility.restore(payload.requestId, payload.removalId)
        return {'recordRestore': result}

    @application.get('/api/collection-runs/{identity}')
    async def collection_run(identity: str, request: Request, page: int = Query(1, ge=1)):
        value = collection_runs.get(identity)
        if not value or identity in collection_runs.visibility.hidden():
            raise HTTPException(404, '日期采集记录不存在。')
        return state_response(collection_runs.public(value, details=True, page=page), request)

    @application.get('/api/collection-runs/{identity}/plan')
    async def collection_plan(identity: str, request: Request, page: int = Query(1, ge=1), category: str = 'all'):
        return state_response(collection_runs.review.get(identity, page, category), request)

    @application.patch('/api/collection-runs/{identity}/plan')
    async def edit_collection_plan(identity: str, payload: CollectionDraftInput):
        return collection_runs.review.edit(identity, payload.model_dump())

    @application.post('/api/collection-runs/{identity}/submit', status_code=202)
    async def confirm_collection_plan(identity: str, payload: CollectionSubmitInput):
        return collection_runs.review.submit(identity, payload.model_dump())

    @application.get('/api/collection-operations/{identity}')
    async def collection_operation(identity: str):
        operation = collection_runs.review.operation(identity)
        if not operation:
            raise HTTPException(404, '采集操作不存在。')
        return {key:value for key,value in operation.items() if key != 'signature'}

    @application.get('/api/pipeline')
    async def pipeline_state(request: Request, page: int = Query(1, ge=1), pageSize: int = Query(20, ge=1, le=20),
                             filter: str = Query('all', pattern='^(all|processing|waiting|attention|complete)$'),
                             search: str = Query('', max_length=500)):
        return state_response(pipeline.state(page=page, page_size=pageSize, status=filter, search=search), request)

    @application.post('/api/pipeline/start')
    async def pipeline_start(payload: PipelineControlInput):
        return pipeline.change('start', payload.model_dump())

    @application.post('/api/pipeline/pause')
    async def pipeline_pause(payload: PipelineControlInput):
        return pipeline.change('pause', payload.model_dump())

    @application.post('/api/pipeline/resume')
    async def pipeline_resume(payload: PipelineControlInput):
        return pipeline.change('resume', payload.model_dump())

    @application.post('/api/pipeline/continue-batch', status_code=202)
    async def pipeline_continue_batch(payload: PipelineBatchInput):
        return pipeline.enqueue(payload.model_dump(exclude_none=True))

    @application.get('/api/pipeline/operations/{identity}')
    async def pipeline_operation(identity: str):
        try:
            return pipeline.operation(identity)
        except KeyError:
            raise HTTPException(404, '自动处理操作不存在。') from None

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
        return workspace.request_reconcile(payload.expectedRevision, payload.requestId)

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
        await connection_restorer.stop('visual')
        await upload_audit.client.login(payload.account, payload.password)
        uploads.wake.set()
        await slicing.connection_restored()
        return upload_audit.state(videos.store.directory())

    @application.post("/api/upload-audit/logout")
    async def upload_audit_logout():
        if upload_audit.busy or uploads.busy or slicing.busy:
            raise ValueError("切片、核对或上传正在进行，请先暂停对应队列再断开。")
        uploads.grants.clear()
        await connection_restorer.stop('visual')
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
        raise Conflict('原视频上传已停用；只导入识别后的照片。')

    @application.post("/api/uploads/pause")
    async def pause_uploads():
        return await uploads.pause()

    @application.post("/api/uploads/resume")
    async def resume_uploads():
        raise Conflict('原视频上传已停用；只导入识别后的照片。')

    @application.post("/api/uploads/{identity}/retry")
    async def retry_upload(identity: str):
        raise Conflict('原视频上传已停用；只导入识别后的照片。')

    @application.post("/api/uploads/{identity}/remove")
    async def remove_upload(identity: str):
        return await uploads.remove(identity)

    @application.post("/api/uploads/{identity}/restore")
    async def restore_upload(identity: str):
        return await uploads.restore(identity)

    @application.get("/api/slicing")
    async def slicing_state(request: Request, summary: bool = False, recordsDay: str | None = None,
                            page: int = Query(1, ge=1), pageSize: int = Query(20, ge=1, le=20),
                            status: str = 'all', source: str = 'all', search: str = ''):
        value = slicing.state(summary=summary, records_day=recordsDay, page=page,
                              page_size=pageSize, status=status, source=source, search=search)
        return state_response(value, request)

    @application.post('/api/slicing/records/remove')
    async def slicing_record_remove(payload: SlicingRecordRemoveInput):
        return delete_records(payload.requestId, [('slicing', [s.model_dump() for s in payload.selections])])


    @application.post('/api/slicing/records/restore')
    async def slicing_record_restore(payload: RecordRestoreInput):
        slicing.visibility.restore(payload.requestId, payload.removalId)
        return slicing.state()

    @application.post('/api/slicing/records/remove-group')
    async def slicing_record_remove_group(payload: GroupRecordRemoveInput):
        if any((submissions.get(choice.id) or {}).get('kind') != 'slicing' for choice in payload.receipts):
            raise ValueError('切片提交记录不存在，请刷新。')
        return delete_records(payload.requestId, [
            ('slicing', [s.model_dump() for s in payload.selections]),
            ('receipts', [s.model_dump() for s in payload.receipts]),
        ])


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
        return delete_records(payload.requestId, [('receipts', [s.model_dump() for s in payload.selections])])


    @application.post('/api/submissions/records/restore')
    async def submission_restore_records(payload: RecordRestoreInput):
        return submissions.visibility.restore(payload.requestId, payload.removalId)

    @application.post('/api/submissions/items/remove')
    async def submission_remove_items(payload: SlicingRecordRemoveInput):
        return delete_records(payload.requestId, [('receipt_items', [s.model_dump() for s in payload.selections])])


    @application.post('/api/submissions/items/restore')
    async def submission_restore_items(payload: RecordRestoreInput):
        return submissions.item_visibility.restore(payload.requestId, payload.removalId)

    @application.post('/api/submissions/remove-group')
    async def submission_remove_group(payload: GroupRecordRemoveInput):
        for choice in payload.receipts:
            if (submissions.get(choice.id) or {}).get('kind') != 'collection':
                raise ValueError('采集提交记录不存在，请刷新。')
        for choice in payload.selections:
            if (submissions.get(choice.id.partition(':')[0]) or {}).get('kind') != 'collection':
                raise ValueError('采集明细不存在，请刷新。')
        return delete_records(payload.requestId, [
            ('receipt_items', [s.model_dump() for s in payload.selections]),
            ('receipts', [s.model_dump() for s in payload.receipts]),
        ])


    @application.post('/api/submissions/restore-group')
    async def submission_restore_group(payload: RecordRestoreInput):
        return restore_groups(payload.requestId, payload.removalId,
                              [submissions.item_visibility, submissions.visibility])

    @application.get('/api/submissions/{identity}')
    async def submission_detail(identity: str, page: int = Query(1, ge=1), page_size: int = Query(20, ge=1, le=100)):
        row = submissions.get(identity)
        if not row or identity in submissions.visibility.hidden():
            raise HTTPException(404, '提交记录不存在。')
        return submission_views.project(row, detail=True, page=page, page_size=page_size)

    @application.delete("/api/submissions/{identity}")
    async def submission_delete(identity: str):
        if managed_root:
            raise HTTPException(409, '请使用记录删除入口，携带记录版本和请求编号。')
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
        file = next((row for row in files.rows("files") if row["id"] == identity), {})
        linked = file.get("links", {}).get("slicing", [])
        return video_control_entry(linked[0] if linked else None)

    def video_control_entry(item_id=None):
        media_control.refresh()
        item = slicing.store.get("items", item_id) if item_id else None
        return JSONResponse({"message": "单视频操作已移到“视频查看与更正”。",
                             "processingEntry": media_control.processing_entry(item),
                             "videoId": (item or {}).get("collectionVideoId"),
                             "mediaId": (item or {}).get("mediaId")}, status_code=409)

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
        package = slicing.packages.store.get("items", identity) or {}
        return video_control_entry(package.get("itemId"))

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
        package = slicing.packages.store.get("items", identity) or {}
        return video_control_entry(package.get("itemId"))

    @application.get("/api/slicing/packages/{identity}/images/{name}")
    async def slice_package_image(identity: str, name: str, session: str | None = None, thumbnail: bool = False):
        try:
            data = await asyncio.to_thread(gallery.read, identity, name, session, thumbnail)
        except (OSError, zipfile.BadZipFile):
            raise HTTPException(409, "本地照片或压缩包暂不可读取，请核对文件和磁盘。") from None
        except ValueError as error:
            raise HTTPException(404, str(error)) from None
        return Response(data, media_type="image/jpeg", headers={"Cache-Control": "private, no-store"})

    @application.post("/api/slicing/packages/{identity}/view")
    async def open_package_view(identity: str, payload: dict):
        if payload.get("sessionId") is not None and (not isinstance(payload["sessionId"], str) or
                not re.fullmatch(r"[a-f0-9]{32}", payload["sessionId"])):
            raise HTTPException(422, "照片查看会话无效。")
        try:
            return await gallery.view(identity, payload.get("sessionId"))
        except (OSError, zipfile.BadZipFile):
            raise HTTPException(409, "本地照片包暂不可读取，请核对文件和磁盘。") from None

    @application.post("/api/slicing/packages/{identity}/view/close")
    async def close_package_view(identity: str, payload: dict):
        key = payload.get("sessionId")
        if isinstance(key, str) and key in gallery.sessions and gallery.sessions[key]["package"]["id"] == identity:
            gallery.close(key)
        return {"ok": True}

    @application.get("/api/slicing/packages/{identity}/contents")
    async def package_contents(identity: str, session: str, page: int = Query(1, ge=1)):
        return gallery.contents(identity, session, page)

    @application.api_route("/api/slicing/packages/{identity}/download", methods=["GET", "HEAD"])
    async def package_download(identity: str, session: str):
        path, guard = await gallery.download(identity, session)
        package = gallery.package(identity)
        name = Path(package.get("videoPath") or package.get("name") or "照片包").stem
        return DownloadFileResponse(path, filename=f"{name}_{package.get('category')}_{identity[:8]}.zip",
                                    media_type="application/zip", release=guard.close)

    @application.get("/api/video-processing")
    async def processing_state(request: Request):
        return state_response(media_control.state(), request)

    @application.post("/api/video-processing/{identity}/continue", status_code=202)
    async def processing_continue(identity: str, payload: ProcessingContinueInput):
        return await media_control.continue_safe(identity, payload.model_dump())

    @application.post("/api/video-processing/{identity}/actions")
    async def processing_action(identity: str, payload: dict):
        if (not isinstance(payload.get("requestId"), str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,100}", payload["requestId"])
                or type(payload.get("generation")) is not int or type(payload.get("expectedRevision")) is not int):
            raise HTTPException(422, "请刷新后重新操作。")
        return await media_control.action(identity, payload)

    @application.post("/api/slicing/settings")
    async def slicing_settings(payload: SlicingSettingsInput):
        value = payload.model_dump()
        if managed_root:
            keys = ('directories', 'magnetDeployDirectory', 'magnetModelPath', 'bedDeployDirectory', 'bedModelPath',
                    'bedGeometryPath', 'frameToolPath', 'workerPythonPath', 'sliceOutputDirectory')
            for key in keys:
                if key in value and value[key] != slicing.store.settings.get(key):
                    raise Conflict('模型和文件位置由程序管理，请刷新后只修改处理参数。')
        return slicing.update_settings(value)

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
        if payload.mode != "all":
            return video_control_entry(payload.itemId)
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
        return video_control_entry()

    @application.post("/api/slicing/{identity}/retry", status_code=202)
    async def slicing_retry(identity: str, payload: SlicingItemActionInput):
        return video_control_entry(identity)

    @application.post("/api/slicing/{identity}/recover-decodable", status_code=202)
    async def slicing_recover_decodable(identity: str, payload: SlicingItemActionInput):
        return video_control_entry(identity)

    @application.post("/api/slicing/{identity}/bind")
    async def slicing_bind(identity: str, payload: SlicingBindInput):
        return video_control_entry(identity)

    @application.post("/api/slicing/{identity}/review")
    async def slicing_review(identity: str, payload: SlicingReviewInput):
        return video_control_entry(identity)

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
        await connection_restorer.stop(service)
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
        await connection_restorer.stop(service)
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
        if not already_submitted and not payload.confirmed:
            raise Conflict('请先核对采集计划，再明确确认所选视频。')
        if payload.confirmed:
            plan.setdefault('confirmedAt', now_iso())
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
        if managed_root:
            raise HTTPException(409, '请从提交记录删除；后台凭证会保留。')
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

    def compact_video(row):
        return {key: value for key, value in row.items() if key != 'requests'} | {
            'versions': [{key: value for key, value in version.items() if key != 'beforeIds'}
                         for version in row.get('versions', [])]}

    @application.get("/api/videos")
    async def video_state(request: Request):
        result = videos.state(dict(request.query_params))
        if request.query_params.get("summary") == "true":
            result = {**result, "videos": []}
        elif request.query_params.get("compact") == "true":
            result = {**result, "videos": [compact_video(row) for row in result['videos']]}
        return state_response(result, request)

    @application.get('/api/videos/{identity}')
    async def exact_video(identity: str, request: Request):
        # A user may hide a history card while its approved workflow still
        # runs. An explicit processing link can inspect that exact record;
        # it never restores visibility or substitutes a similarly named video.
        rows = videos.state(identity=identity)['videos']
        if not rows:
            raise HTTPException(404, '这条视频记录不存在或已清除，未打开其他同名视频。')
        return state_response({'video': compact_video(rows[0]),
                               'recordHidden': identity in videos.record_visibility.hidden()}, request)

    @application.post("/api/videos/download-limit")
    async def video_download_limit(payload: VideoDownloadLimitInput):
        return videos.update_download_limit(payload.model_dump())

    @application.post("/api/videos/bulk-confirm", status_code=202)
    async def bulk_confirm(payload: BulkVideoInput):
        if workspace.enabled:
            return final_processing.batch(payload.model_dump())
        return await videos.bulk_action("confirm", payload.model_dump())

    @application.post('/api/videos/bulk-confirm-and-process', status_code=202)
    async def bulk_final_confirm(payload: BulkVideoInput):
        return final_processing.batch(payload.model_dump())

    @application.post('/api/videos/{identity}/confirm-and-process', status_code=202)
    async def final_confirm(identity: str, payload: FinalConfirmationInput):
        return final_processing.confirm(identity, payload.model_dump())

    @application.post('/api/videos/{identity}/continue-processing', status_code=202)
    async def resume_final_processing(identity: str, payload: FinalConfirmationInput):
        return final_processing.resume(identity, payload.model_dump())

    @application.get('/api/final-processing/{identity}')
    async def final_processing_operation(identity: str, request: Request):
        return state_response(final_processing.operation(identity), request)

    @application.post("/api/videos/bulk-remove", status_code=202)
    async def bulk_remove(payload: BulkVideoInput):
        if managed_root:
            return delete_records(payload.requestId, [('collection', [
                {'id': s.id, 'expectedRevision': s.expectedRevision} for s in payload.selections])])
        return await videos.bulk_action("remove", payload.model_dump())

    @application.post('/api/videos/bulk-classify-saved')
    async def bulk_classify_saved(payload: BulkVideoInput):
        return await videos.bulk_action('classify-saved', payload.model_dump())

    @application.post("/api/videos/check")
    async def check_videos():
        return await videos.check_once()

    @application.post("/api/settings/directory")
    async def save_directory(payload: DirectoryInput):
        return videos.configure_directory(payload.directory)

    @application.post("/api/videos/{identity}/corrections", status_code=202)
    async def correct(identity: str, payload: CorrectionInput):
        return await videos.correct(identity, payload.model_dump())

    @application.post("/api/videos/{identity}/check", status_code=202)
    async def check_video(identity: str, payload: SlicingItemActionInput):
        return await videos.check_video(identity, payload.model_dump())

    @application.post("/api/videos/{identity}/recover-decodable", status_code=202)
    async def recover_decodable_video(identity: str, payload: SlicingItemActionInput):
        return await videos.recover_decodable(identity, payload.model_dump())

    @application.post("/api/videos/{identity}/recover-decodable/pause")
    async def pause_decodable_recovery(identity: str, payload: VersionActionInput):
        return await videos.pause_recovery(identity, payload.model_dump())

    @application.post("/api/videos/{identity}/recover-decodable/resume", status_code=202)
    async def resume_decodable_recovery(identity: str, payload: VersionActionInput):
        return await videos.resume_recovery(identity, payload.model_dump())

    @application.get("/api/video-operations/{operation_id}")
    async def video_operation_state(operation_id: str):
        try:
            return videos.operation_state(operation_id)
        except ValueError as error:
            raise HTTPException(404, str(error)) from None

    @application.post("/api/videos/{identity}/recapture", status_code=202)
    async def recapture_unavailable(identity: str, payload: VersionActionInput):
        return await videos.recapture_unavailable(identity, payload.model_dump())

    @application.post("/api/videos/{identity}/remove-uncorrected")
    async def remove_uncorrected(identity: str, payload: HistoryRemoveInput):
        if managed_root:
            return delete_records(payload.requestId, [('collection', [{'id': identity, 'expectedRevision': payload.expectedRevision}])])
        return await videos.remove_uncorrected(identity, payload.model_dump())

    @application.post("/api/videos/{identity}/remove-record", status_code=202)
    async def remove_record(identity: str, payload: RecordRemoveInput):
        if managed_root:
            return delete_records(payload.requestId, [('collection', [{'id': identity, 'expectedRevision': payload.expectedRevision}])])
        return await videos.remove_record(identity, payload.model_dump())

    @application.post('/api/videos/{identity}/hide-record')
    async def hide_video_record(identity: str, payload: HistoryRemoveInput):
        return delete_records(payload.requestId, [('collection', [{'id': identity, 'expectedRevision': payload.expectedRevision}])])


    @application.post('/api/videos/records/restore')
    async def restore_video_record(payload: RecordRestoreInput):
        videos.record_visibility.restore(payload.requestId, payload.removalId)
        return videos.state()

    @application.post('/api/videos/records/remove')
    async def hide_video_records(payload: SlicingRecordRemoveInput):
        return delete_records(payload.requestId, [('collection', [s.model_dump() for s in payload.selections])])


    @application.post("/api/videos/{identity}/cancel-clear")
    async def cancel_clear(identity: str, payload: HistoryRemoveInput):
        if managed_root:
            raise HTTPException(410, '记录删除不再支持恢复。采集更正草稿仍可取消。')
        return await videos.cancel_clear(identity, payload.model_dump())

    @application.post("/api/videos/{identity}/cancel-download")
    async def cancel_download(identity: str, payload: VersionActionInput):
        return await videos.cancel_download(identity, payload.model_dump())

    @application.post("/api/videos/{identity}/withdraw", status_code=202)
    async def withdraw(identity: str, payload: VersionActionInput):
        return await videos.withdraw(identity, payload.model_dump())

    @application.post("/api/videos/{identity}/confirm", status_code=202)
    async def confirm(identity: str, payload: VersionActionInput):
        if workspace.enabled:
            return final_processing.confirm(identity, {key: value for key, value in payload.model_dump().items()
                                                       if key != 'waitForCompletion'})
        return await videos.confirm(identity, payload.model_dump())

    @application.post('/api/videos/{identity}/history-download', status_code=202)
    async def history_download(identity: str, payload: VersionActionInput):
        return history_downloads.register(identity, payload.model_dump(exclude={'waitForCompletion'}))

    @application.post("/api/videos/{identity}/retry-download", status_code=202)
    async def retry_download(identity: str):
        return await videos.retry_download(identity)

    @application.post("/api/videos/{identity}/restart-download", status_code=202)
    async def restart_download(identity: str, payload: VersionActionInput):
        return await videos.restart_download(identity, payload.model_dump())

    local_playback = LocalPlayback(leases)

    def local_video_response(local, request, filename=None):
        path, release, stream, expected = local
        try:
            size = expected['size']
            etag = '"' + hashlib.sha256(repr(expected).encode()).hexdigest()[:24] + '"'
            requested = request.headers.get('range', '')
            if request.headers.get('if-range') not in (None, etag):
                requested = ''
            start, end, status = 0, size - 1, 200
            if requested:
                bounds = re.fullmatch(r'bytes=(\d*)-(\d*)', requested)
                if not bounds or not any(bounds.groups()):
                    raise HTTPException(416, '视频范围无效。', headers={'Content-Range': f'bytes */{size}'})
                if bounds[1]:
                    start, end = int(bounds[1]), min(int(bounds[2]), end) if bounds[2] else end
                else:
                    start = max(0, size - int(bounds[2]))
                if start > end or start < 0 or start >= size:
                    raise HTTPException(416, '视频范围无效。', headers={'Content-Range': f'bytes */{size}'})
                status = 206
            headers = {'Cache-Control': 'no-store', 'Accept-Ranges': 'bytes', 'ETag': etag,
                       'Content-Length': str(max(0, end - start + 1))}
            if status == 206:
                headers['Content-Range'] = f'bytes {start}-{end}/{size}'
            if filename:
                headers['Content-Disposition'] = f"attachment; filename=video.mp4; filename*=UTF-8''{quote(filename, safe='')}"

            def read_chunk(length):
                info = os.fstat(stream.fileno())
                if info.st_size != expected['size'] or info.st_mtime_ns != expected['modifiedNs']:
                    raise ValueError('播放中的视频已被外部修改，已停止读取。')
                return stream.read(length)

            async def chunks():
                if request.method == 'HEAD':
                    return
                await finish_io(stream.seek, start)
                remaining = max(0, end - start + 1)
                while remaining:
                    block = await finish_io(read_chunk, min(1024 * 1024, remaining))
                    if not block:
                        raise ValueError('播放中的视频长度发生变化，已停止读取。')
                    remaining -= len(block)
                    yield block
            return DownloadStreamResponse(chunks(), status_code=status, media_type='video/mp4',
                                          headers=headers, release=release)
        except BaseException:
            release()
            raise

    @application.api_route("/api/videos/{identity}/versions/{revision}/download", methods=["GET", "HEAD"])
    async def download_version(identity: str, revision: int, request: Request):
        row = videos.store.get(identity)
        if not row or row.get("deletedAt"):
            raise HTTPException(404, "视频记录不存在。")
        videos.require_editable(row)
        version = videos.version(row, revision)
        if not version or version["state"] in ("withdrawn", "submitting", "uncertain", "waiting", "submitted"):
            raise HTTPException(404, "这个版本尚无可下载的视频。")
        guards = ExitStack()
        try:
            local = await local_playback.prepare(version)
            if local:
                path = local[0]
                filename = f"{path.stem}_v{revision + 1}{path.suffix}"
                return local_video_response(local, request, filename)
            if version["state"] == "cloud_deleted":
                raise HTTPException(404, "这个版本已清理，无法重新下载；其他版本和更正保留。")
            if not version.get("taskId") or not version.get("resultId"):
                raise HTTPException(404, "这个版本的结果尚未确认，请先核对。")
            guards.enter_context(leases.read(videos.cloud_lease_key(version["taskId"])))
            response = await stream_video(identity, revision, request)
            name = video_filename({"videoName": row["name"], "startTime": version["startTime"]},
                                  {"id": version["resultId"], "url": "https://media.invalid/video.mp4"})
            filename = f"{Path(name).stem}_v{revision + 1}.mp4"
            headers = dict(response.headers)
            headers["Content-Disposition"] = f"attachment; filename=video_v{revision + 1}.mp4; filename*=UTF-8''{quote(filename, safe='')}"
            if isinstance(response, DownloadReadLease):
                # A local publish may finish between the first check and the
                # fallback. The outer response now owns the pinned descriptor.
                guards.callback(response.release_read)
            return DownloadStreamResponse(response.body_iterator, status_code=response.status_code, headers=headers,
                                          close_stream=response.background, release=guards.close)
        except BaseException:
            guards.close()
            raise

    @application.api_route("/api/videos/{identity}/versions/{revision}/stream", methods=["GET", "HEAD"])
    async def stream_video(identity: str, revision: int, request: Request):
        row = videos.store.get(identity)
        if not row or row.get("deletedAt"):
            raise HTTPException(404, "视频记录不存在。")
        version = videos.version(row, revision)
        if not version or version["state"] not in ("ready", "saved", "failed", "publishing", "attention", "cloud_deleted"):
            raise HTTPException(404, "视频尚未采集完成。")
        requested_range = request.headers.get("range", "")
        if requested_range and not re.fullmatch(r"bytes=(?:\d+-\d*|-\d+)", requested_range):
            raise HTTPException(416, "视频范围无效。")
        local = await local_playback.prepare(version)
        if local:
            return local_video_response(local, request)
        if version['state'] == 'cloud_deleted':
            raise HTTPException(404, '这个版本的文件已清理，无法播放。')
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

    dist = Path(web_directory) if web_directory else ROOT / "dist"
    if (dist / "assets").is_dir():
        application.mount("/assets", StaticFiles(directory=dist / "assets"), name="assets")

    @application.get("/")
    @application.get("/package-view/{package_id}")
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
