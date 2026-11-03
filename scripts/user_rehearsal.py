#!/usr/bin/env python3
"""Interactive, persistent real-video rehearsal. Never contacts a real platform.

Uses the ordinary app and schedulers with an isolated database. Only inspection
and capture are simulated; downloads, FFmpeg, models and photo export are real.
Uploads are permanently paused and additionally denied at every transport.
"""
from __future__ import annotations

import argparse
import asyncio
from datetime import datetime, timedelta
import hashlib
import json
import os
from pathlib import Path
import sys
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import httpx
from fastapi.responses import HTMLResponse, JSONResponse, Response

from app.main import create_app
from app.managed_storage import configure, prepare
from app.remote import RemoteClient
from app.runtime import source_model_paths
from app.upload_audit import AthenaClient, AuditError
from tests.real_video_capture import CapturePlatform, source_handler

DATE = '2026-09-13'
NOTICE = '真实视频体验 · 质检与采集为本地模拟 · 上传及导入已锁定关闭'


def write(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix('.tmp')
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2) + '\n')
    os.chmod(temporary, 0o600)
    temporary.replace(path)


def fingerprint(path):
    info = path.stat()
    with path.open('rb') as stream:
        digest = hashlib.file_digest(stream, 'sha256').hexdigest()
    after = path.stat()
    if (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns) != (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns):
        raise ValueError('测试原片在校验时发生变化。')
    return {'size': info.st_size, 'modifiedNs': info.st_mtime_ns, 'sha256': digest}


def inspection(request, record_count=1):
    """Distinct synthetic inspections share one read-only real-video sample."""
    response = source_handler(request)
    if request.url.path.endswith('/intelliTaskInfo/getCheckDetail'):
        codes = {'R-GOOD' if index == 0 else f'R-GOOD-{index+1:02d}': index
                 for index in range(record_count)}
        index = codes.get(request.url.params.get('flowCode'), 0)
        if index:
            payload = response.json()
            for station in payload['data']['stationResults']:
                for field in ('checkStartTime', 'checkCompleteTime'):
                    station[field] = (datetime.fromisoformat(station[field]) + timedelta(minutes=2*index)).isoformat(sep=' ')
            response = httpx.Response(200, json=payload)
    if request.url.path.endswith('/intelliTaskInfo/page'):
        row = response.json()['data']['records'][0]
        first = request.url.params.get('startTime', '')
        last = request.url.params.get('endTime', '')
        rows = [{**row, 'flowCode': 'R-GOOD' if index == 0 else f'R-GOOD-{index+1:02d}',
                 'createTime': (datetime.fromisoformat(row['createTime']) + timedelta(minutes=2*index)).isoformat(sep=' ')}
                for index in range(record_count)]
        plate = request.url.params.get('carNumber', '')
        rows = [item for item in rows if (not first or first <= item['createTime'])
                and (not last or item['createTime'] <= last) and (not plate or plate in item['carNumber'])]
        page = int(request.url.params.get('pageIndex', 1))
        size = max(1, int(request.url.params.get('pageSize', 100)))
        response = httpx.Response(200, json={'success': True, 'data': {
            'records': rows[(page-1)*size:page*size], 'total': len(rows)}})
    return response


class PersistentCapture(CapturePlatform):
    def __init__(self, root, source, ffmpeg, run_id):
        super().__init__(root, source, ffmpeg, run_id)
        self.journal = root / 'platform.json'
        self.mutations = asyncio.Lock()
        if self.journal.exists():
            state = json.loads(self.journal.read_text())
            self.parents, self.results = state['parents'], state['results']
            self.objects = {key: root / path for key, path in state['objects'].items()}
            self.clip_cache = {tuple(map(int, key.split(':'))): root / path for key, path in state['clips'].items()}
            self.metrics = state['metrics']

    def save(self):
        write(self.journal, {'parents': self.parents, 'results': self.results,
            'objects': {key: str(path.relative_to(self.root)) for key, path in self.objects.items()},
            'clips': {':'.join(map(str, key)): str(path.relative_to(self.root)) for key, path in self.clip_cache.items()},
            'metrics': self.metrics})

    async def clip(self, offset, duration):
        if offset < 0 or duration <= 0 or offset + duration > 4499:
            raise ValueError('测试原片仅覆盖 2026-09-13 09:54:00 至 11:08:59，请缩小采集区间。')
        key = offset, duration
        if key in self.clip_cache and self.clip_cache[key].is_file():
            return self.clip_cache[key]
        destination = self.root / f'clip-{offset}-{duration}.mp4'
        partial = destination.with_suffix('.partial.mp4')
        # This exact temporary filename belongs only to this simulated capture.
        partial.unlink(missing_ok=True)
        process = await asyncio.create_subprocess_exec(self.ffmpeg, '-nostdin', '-v', 'error',
            '-ss', str(offset), '-i', str(self.source), '-t', str(duration), '-map', '0:v:0',
            '-an', '-c', 'copy', '-tag:v', 'hvc1', '-avoid_negative_ts', 'make_zero',
            '-movflags', '+faststart', str(partial), stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE)
        try:
            _, error = await asyncio.wait_for(process.communicate(), 600)
            if process.returncode:
                raise ValueError('测试采集截取失败，原片未改动：' + error.decode(errors='replace')[-200:])
            partial.replace(destination)
        finally:
            if process.returncode is None:
                process.kill()
                await process.wait()
        self.clip_cache[key] = destination
        self.metrics['clips'].append({'offsetSeconds': offset, 'durationSeconds': duration,
                                      'bytes': destination.stat().st_size})
        self.save()
        return destination

    async def handle(self, request):
        if request.url.path.endswith('/data-acquisition/batchSave'):
            for task in json.loads(request.content):
                begin = datetime.fromisoformat(task['startTime'])
                end = datetime.fromisoformat(task['endTime'])
                if begin < self.start or end > self.start + timedelta(seconds=4499):
                    return httpx.Response(200, json={'code':'CAPTURE_RANGE', 'message':'采集区间超出测试原片范围。'})
        if request.method in {'POST', 'DELETE'}:
            async with self.mutations:
                try:
                    return await super().handle(request)
                finally:
                    self.save()
        return await super().handle(request)


class DeniedUploads:
    def __getattr__(self, name):
        async def denied(*args, **kwargs):
            raise AuditError('体验测试不执行上传或导入。', 'rehearsal_upload_disabled', 409)
        return denied


def create_rehearsal(root, source, *, record_count=1):
    if isinstance(record_count, bool) or not isinstance(record_count, int) or not 1 <= record_count <= 5:
        raise ValueError('模拟质检记录数必须为1–5条。')
    root, source = Path(root).absolute(), Path(source).absolute()
    if root.is_symlink() or any(parent.is_symlink() for parent in root.parents):
        raise ValueError('测试目录不能经过符号链接。')
    if source.is_symlink() or any(parent.is_symlink() for parent in source.parents):
        raise ValueError('测试原片不能经过符号链接。')
    root, source = root.resolve(), source.resolve(strict=True)
    if source.is_relative_to(root):
        raise ValueError('测试原片必须在体验工作目录之外，只读使用。')
    if root.exists() and not (root / 'rehearsal.json').exists():
        raise ValueError('请使用新的体验目录；不复用未登记目录。')
    root.mkdir(parents=True, exist_ok=True)
    manifest_path = root / 'rehearsal.json'
    stamp = fingerprint(source)
    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text())
        if manifest['source'] != str(source) or manifest['sourceVersion'] != stamp:
            raise ValueError('测试原片已变化，不覆盖原测试进度。')
        if manifest.get('recordCount', 1) != record_count:
            raise ValueError('模拟质检记录数已变化，请使用新的测试目录，原进度保持不变。')
    else:
        manifest = {'id': uuid4().hex, 'source': str(source), 'sourceVersion': stamp,
                    'createdAt': datetime.now().isoformat(), 'uploadsAllowed': False,
                    'recordCount': record_count}
        write(manifest_path, manifest)
    data = root / '数据'
    prepare(data)
    settings = source_model_paths()
    platform = PersistentCapture(root / '模拟平台', source, settings['frameToolPath'], manifest['id'])
    source_client = RemoteClient('source', transport=httpx.MockTransport(lambda request: inspection(request, record_count)))
    target_client = RemoteClient('target', transport=httpx.MockTransport(platform.handle))
    for client in (source_client, target_client):
        client.connected, client.user = True, '本地体验'

    async def deny(request):
        raise httpx.ConnectError('体验测试已阻止外部平台请求。', request=request)
    visual = AthenaClient(transport=httpx.MockTransport(deny), persist_connection=False)
    visual.access, visual.user = 'local-rehearsal-session', '本地体验（不上传）'
    app = create_app(source_client, target_client, data / '系统',
                     video_directory=data / '媒体/采集视频', staging_directory=data / '临时/下载',
                     audit_client=visual, upload_remote=DeniedUploads(), upload_worker=DeniedUploads(),
                     slicing_remote=DeniedUploads(), web_directory=Path(os.environ['REHEARSAL_WEB_DIST']) if os.environ.get('REHEARSAL_WEB_DIST') else None)
    configure(app.state.workspace, data)
    slicing = app.state.slicing
    def validate_interval(start, seconds):
        from app.domain import parse_time
        begin = parse_time(start).replace(tzinfo=None)
        offset = (begin - platform.start).total_seconds()
        if offset < 0 or offset + seconds > 4499:
            raise ValueError('测试原片仅覆盖2026-09-13 09:54:00至11:08:59，请缩小采集区间。')
    app.state.videos.validate_capture_interval = validate_interval
    # Older rehearsals raised a local ValueError after registering an uncertain
    # version. Repair only that explicit simulator refusal, with its exact
    # operation and parameters. An empty real-platform inventory is no proof.
    for operation in app.state.videos.store.list_operations():
        if operation.get('kind') != 'correction' or operation.get('failureType') != 'ValueError' or '测试原片' not in operation.get('message', ''):
            continue
        row = app.state.videos.store.get(operation['videoId'])
        attempted = (row or {}).get('requests', {}).get(operation['requestId'], {})
        version = app.state.videos.version(row, attempted.get('version')) if row else None
        payload = operation.get('payload', {})
        if (not version or version.get('taskId') or version.get('state') != 'uncertain'
                or version.get('startTime') != payload.get('startTime')
                or version.get('durationSeconds') != payload.get('durationSeconds')):
            continue
        try:
            validate_interval(version['startTime'], version['durationSeconds'])
        except ValueError:
            if any(app.state.videos.matches(parent, version, app.state.videos.target_config['cameraId']) for parent in platform.parents.values()):
                continue
            version.update(state='attention', submissionOutcome='rejected', errorCode='capture_rejected',
                           message='测试区间超出原片，采集已明确拒绝；可以修改或撤回更正。',
                           rejectionProof={'kind':'simulator-range-validation','operationId':operation['id']})
            app.state.videos.store.save(row)
    slicing.store.settings.update(settings, localExport=True, reviewBeforeUpload=False,
                                  targetDatasetIds={'27912': '27892', '27944': '27893'})
    slicing.store.save_settings()
    slicing.packages.pause()
    # A global resume must also preserve the test's permanent upload boundary.
    slicing.packages.resume = slicing.packages.pause
    slicing.packages.remote = DeniedUploads()
    slicing.packages.worker = DeniedUploads()
    app.state.rehearsal_platform = platform
    app.state.rehearsal_manifest = manifest

    @app.middleware('http')
    async def rehearsal_boundary(request, call_next):
        path = request.url.path
        if path.startswith('/api/'):
            origin = request.headers.get('origin')
            if request.headers.get('sec-fetch-site') == 'cross-site' or (origin and origin != str(request.base_url).rstrip('/')):
                return JSONResponse({'message': '仅允许本地测试网页访问。'}, status_code=403)
        if request.method not in {'GET', 'HEAD', 'OPTIONS'}:
            blocked = (path.startswith(('/api/uploads', '/api/upload-audit', '/api/login', '/api/logout'))
                or path in {'/api/slicing/packages/resume', '/api/slicing/scan', '/api/slicing/execute-all',
                            '/api/workspace/preview', '/api/videos/directory', '/api/directory'})
            if blocked:
                return JSONResponse({'message': '体验测试只处理本地视频；上传、导入和真实平台连接均已关闭。'}, status_code=409)
        if path == '/' or path.startswith('/package-view/'):
            web = Path(os.environ['REHEARSAL_WEB_DIST']) if os.environ.get('REHEARSAL_WEB_DIST') else ROOT / 'dist'
            html = (web / 'index.html').read_text()
            note = '<aside role="note" style="padding:10px 24px;background:#e4f0e7;color:#234735;border-bottom:1px solid #b7cebd;font:14px/1.6 system-ui">' + NOTICE + f'<br>查询日期：2026-09-13；{record_count}条模拟质检，共用同一真实原片；可采集范围：09:54:00–11:08:59。<a href="/rehearsal-guide" target="_blank">测试说明</a></aside>'
            html = html.replace('<head>', '<head><script src="/rehearsal-init.js"></script>', 1)
            html = html.replace('<body>', '<body>' + note, 1)
            return HTMLResponse(html, headers={'Cache-Control': 'no-store'})
        if path == '/api/workspace/open' and request.method == 'POST':
            if 'application/json' not in request.headers.get('content-type', ''):
                return JSONResponse({'message': '请求必须使用 JSON。'}, status_code=415)
            # Only the fixed rehearsal directory; no supplied path is accepted.
            process = await asyncio.create_subprocess_exec('/usr/bin/open', str(data))
            await process.wait()
            return JSONResponse({'ok': process.returncode == 0})
        return await call_next(request)

    @app.get('/rehearsal-init.js')
    async def defaults():
        return Response('try{if(!sessionStorage.getItem("input-draft:collection-dates"))'
            'sessionStorage.setItem("input-draft:collection-dates",JSON.stringify({dateFrom:"2026-09-13",dateTo:"2026-09-13"}));}catch(e){}',
            media_type='text/javascript')

    @app.get('/rehearsal-guide')
    async def guide():
        return HTMLResponse('<!doctype html><html lang="zh"><meta charset="utf-8"><title>体验说明</title>'
            '<body style="max-width:760px;margin:40px auto;padding:20px;font:17px/1.9 system-ui;color:#234735">'
            f'<h1>从查询开始体验</h1><p>本次共{record_count}条模拟质检，均来自同一真实原片，各区间错开2分钟。</p>'
            '<ol><li>日期选择2026-09-13，点击“查询待采集视频”，核对并勾选后点击“确认采集所选”；可以逐条选择或全选。</li>'
            '<li>到“视频确认”查看鲁NH1113。首版09:54开始，60分钟。</li>'
            '<li>可以反复更正。快速体验建议改为10:04开始、3分钟，再改为10:05开始、2分钟；在最终确认前可“撤回本次更正”，恢复上一有效版本，再次更正。</li>'
            '<li>查看后点击“确认最终区间并处理”，会实际下载、识别并生成照片与ZIP。</li>'
            '<li>到“处理结果”查看照片。上传已锁定暂停，所以结果保留“等待上传”，不会冒充全部完成或删除原片。</li></ol>'
            '<p>可采集范围：2026-09-13 09:54:00至11:08:59。质检信息与平台任务为模拟，画面来自真实4K原片；'
            '仅此测试网站的副本会变化，日常网站、原片及云端成果不受影响。关闭网页后后台继续。</p>'
            '<p><a href="/">返回测试网站</a></p></body></html>')

    @app.get('/api/rehearsal')
    async def status():
        packages = slicing.packages.store.list('items')
        return {'testMode': True, 'date': DATE, 'sourceName': source.name, 'uploadsAllowed': False,
            'recordCount': record_count,
            'uploadPaused': slicing.packages.paused, 'captureTasks': len(platform.parents),
            'downloaded': sum(v.get('state') == 'saved' for r in app.state.videos.store.list() for v in r.get('versions', [])),
            'packages': [{key: row.get(key) for key in ('id', 'category', 'state', 'imageCount', 'bytes', 'jobId')} for row in packages]}
    return app


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--source', type=Path, required=True)
    parser.add_argument('--port', type=int, default=8790)
    parser.add_argument('--records', type=int, choices=range(1, 6), default=1, help='模拟质检条数；全部使用同一真实原片')
    args = parser.parse_args()
    if args.port in {8765, 8766, 8786}:
        parser.error('体验测试不能占用日常网站端口。')
    import uvicorn
    app = create_rehearsal(args.root, args.source, record_count=args.records)
    write(args.root / 'server.json', {'pid': os.getpid(), 'port': args.port, 'uploadsAllowed': False})
    uvicorn.run(app, host='127.0.0.1', port=args.port, access_log=False, timeout_graceful_shutdown=30)
