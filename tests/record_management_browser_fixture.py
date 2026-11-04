"""Disposable UI records for desktop/narrow checks; no live credentials or IO."""
import json
import os
import tempfile
from pathlib import Path

from fastapi.responses import HTMLResponse, JSONResponse

from app.main import create_app
from app.submissions import SubmissionLog
from app.workspace_files import file_version


def run():
    import uvicorn
    with tempfile.TemporaryDirectory(prefix='record-ui-check-') as folder:
        root = Path(folder).resolve()
        if os.environ.get('RECORD_UI_DIST'):
            from app import main as app_main
            (root / 'dist').symlink_to(Path(os.environ['RECORD_UI_DIST']).resolve(), target_is_directory=True)
            app_main.ROOT = root
        app = create_app(data_dir=root / 'db', video_directory=root / '视频')
        media = root / '视频'
        media.mkdir(exist_ok=True)
        tasks = []
        fixture_names = ('鲁Q09501D', 'VIRTUAL_001') if os.environ.get('RECORD_UI_DAMAGED') else ('鲁DJ1397', 'VIRTUAL_001')
        if os.environ.get('RECORD_UI_COUNT'):
            fixture_names = tuple(f'鲁测试{i:03d}' for i in range(int(os.environ['RECORD_UI_COUNT'])))
        for index, name in enumerate(fixture_names):
            tasks.append({'key': f'key-{index}', 'recordId': f'inspection-{index}', 'plate': name, 'videoName': name,
                          'checkStartTime': '2026-09-30 08:00:00', 'checkCompleteTime': '2026-09-30 09:00:00',
                          'startTime': '2026-09-30 08:05:00', 'endTime': '2026-09-30 09:05:00',
                          'photoCount': 1, 'inspectionDurationSeconds': 3600, 'status': 'submitted'})
        job = {'id': 'browser-fixture-batch', 'createdAt': '2026-10-23T08:15:00Z', 'status': 'complete',
               'items': tasks, 'events': []}
        app.state.store.save(job)
        extra = {**job, 'id': 'second-same-day', 'createdAt': '2026-10-23T10:30:00Z', 'items': [{**tasks[0], 'key': 'other', 'videoName': '苏GB3338'}]}
        app.state.store.save(extra)
        earlier = {**job, 'id': 'previous-date', 'createdAt': '2026-10-22T10:30:00Z', 'items': [{**tasks[0], 'key': 'old', 'videoName': '鲁AG7205'}]}
        app.state.store.save(earlier)
        app.state.videos.sync_jobs()
        slices = app.state.slicing
        for row in app.state.videos.store.list():
            path = media / (row['name'] + '_20260930080500_fixture.mp4')
            path.write_bytes(b'disposable-browser-video')
            version = row['versions'][0]
            version.update(state='saved', taskId='collection-' + row['id'], resultId='result-' + row['id'],
                           filePath=str(path), fileVersion=file_version(path, True))
            row.update(acceptedRevision=0)
            if os.environ.get('RECORD_UI_DAMAGED') and row['name'] == '鲁Q09501D':
                version.update(state='failed', errorCode='invalid_video', downloadRequested=True,
                               message='视频末段缺失或无法解码，尚未替换旧版。请使用完整视频重新采集或下载。')
                version.pop('filePath', None)
                row['acceptedRevision'] = None
            app.state.videos.store.save(row)
            for category, dataset in (('bed', '27912'), ('magnet', '27944')):
                identity = category + '-' + row['id']
                slices.store.save('items', {'id': identity, 'name': path.name, 'path': str(path), 'revision': 5,
                    'runId': 'fixture-run', 'sourceDatasetId': dataset, 'sourceVideoId': 'fixture-' + identity,
                    'fileVersion': file_version(path, True), 'state': 'complete', 'remote': {'duration': 3600, 'size': path.stat().st_size},
                    'categories': {category: {'state': 'complete', 'intervals': [{'start': 30, 'end': 900}],
                        'intervalSeconds': 6, 'actualImages': 150, 'estimatedImages': 150,
                        'targetDatasetId': '27892' if category == 'bed' else '27893', 'taskId': 'task-' + identity},
                        ('magnet' if category == 'bed' else 'bed'): {'state': 'not_applicable', 'intervals': []}}})
        identities = [i['id'] for i in slices.store.list('items')]
        slices.store.save('runs', {'id': 'fixture-run', 'requestId': 'fixture-start', 'items': identities,
                                  'state': 'complete', 'message': '示例批次已结束。'})
        slices.store.settings['latestRunId'] = 'fixture-run'
        slices.store.save_settings()
        hidden_video = None
        if os.environ.get('RECORD_UI_HIDDEN'):
            hidden_video = next(row for row in app.state.videos.store.list() if row['name'] == fixture_names[0])
            hidden_video['versions'][0]['state'] = 'waiting'
            hidden_video['acceptedRevision'] = None
            app.state.videos.store.save(hidden_video)
            app.state.videos.record_visibility.hide(app.state.videos.store, 'fixture-hide',
                [{'id':hidden_video['id'],'expectedRevision':hidden_video['revision']}])
        if os.environ.get('RECORD_UI_PARTIAL'):
            from tests.test_archive_completion import seed
            partial = root / 'partial'
            partial.mkdir()
            seed(app, partial, 'c' * 24)
            app.state.videos.reconcile_recovery_results()
        log = SubmissionLog(app.state.store.db)
        for record in (job, extra, earlier):
            log.add('collection', '采集提交', {'jobId': record['id']}, submitted_at=record['createdAt'])
        log.add('slicing', '全部切片', {'itemIds': identities, 'runId': 'fixture-run', 'message': '示例切片已核验。'},
                submitted_at='2026-10-23T08:30:00Z')
        for index in range(2):
            app.state.collection_runs._save({'id':f'date-fixture-{index}','requestId':f'date-fixture-request-{index}',
                'revision':0,'status':'complete','stage':'complete','message':'日期采集已结束。',
                'filters':{'dateFrom':'2026-09-29','dateTo':'2026-10-08'},'counts':{'submitted':2,'ready':2},
                'scanned':2,'total':2,'createdAt':'2026-10-23T08:00:00Z','records':[]})

        @app.middleware('http')
        async def test_frame(request, call_next):
            # UI-only fake platform; never logs in or accesses external data.
            if request.url.path == '/api/session':
                return JSONResponse({'source': {'connected': True, 'user': '测试'}, 'target': {'connected': True, 'user': '测试'}, 'capabilities': {'batchDownload': False}})
            if request.url.path == '/api/jobs' and request.method == 'GET' and os.environ.get('RECORD_UI_JOBS_FAILURE'):
                return JSONResponse({'message':'测试：提交记录暂时不可读'},status_code=503)
            if request.url.path == '/api/search' and request.method == 'POST':
                return JSONResponse({'id':'fixture-search','status':'complete','message':'找到 2 条实绩。','records':[
                    {'id':item['recordId'],'plate':item['plate'],'checkStartTime':item['checkStartTime'],'checkCompleteTime':item['checkCompleteTime'],
                     'photoCount':1,'inspectionDurationSeconds':3600,'issue':''} for item in tasks], 'scanned':2,'total':2,'admissionRevision':0})
            if request.url.path == '/api/search/fixture-search':
                return JSONResponse({'id':'fixture-search','status':'complete','records':[], 'scanned':2,'total':2,'admissionRevision':0})
            if request.url.path == '/api/preview':
                return JSONResponse({'id':'fixture-plan','tasks':[{'recordId':item['recordId'],'status':'ready'} for item in tasks]})
            if request.url.path == '/api/jobs' and request.method == 'POST':
                return JSONResponse({'id':'fixture-submit', 'status':'complete','createdAt':'2026-10-26T00:00:00Z','items':[]})
            response = await call_next(request)
            response.headers['X-Frame-Options'] = 'SAMEORIGIN'
            response.headers['Content-Security-Policy'] = "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' blob: data:; media-src 'self' blob:; connect-src 'self'; frame-ancestors 'self'"
            return response

        @app.get('/phone', response_class=HTMLResponse)
        async def phone():
            return '<!doctype html><html lang="zh"><title>390px 记录操作检查</title><body style="margin:0;background:#ddd"><iframe title="窄屏记录" src="/" style="display:block;border:0;width:390px;height:900px;margin:auto"></iframe></body></html>'
        print(json.dumps({'fixtureDirectory': str(root),
                          'hiddenVideoUrl': f"http://127.0.0.1:8795/?tab=videos&videoId={hidden_video['id']}" if hidden_video else None},
                         ensure_ascii=False), flush=True)
        uvicorn.run(app, host='127.0.0.1', port=8795, access_log=False)


if __name__ == '__main__':
    run()
