"""Disconnected browser fixture, including a 390px iframe for narrow-layout QA.

Run only as `python -m tests.workspace_browser_fixture`. No live credentials,
database, models, mount points or platform calls are used.
"""
import json
import os
import tempfile
from pathlib import Path

from fastapi.responses import HTMLResponse, JSONResponse

from app.main import create_app
from app.slicing import source_version
from app.submissions import SubmissionLog
from tests.test_media_control import collection


def run():
    import uvicorn
    with tempfile.TemporaryDirectory(prefix='workspace-ui-check-') as folder:
        root = Path(folder).resolve()
        if os.environ.get('RECORD_UI_DIST'):
            import app.main as app_main
            app_main.ROOT = Path(os.environ['RECORD_UI_DIST']).resolve().parent
        app = create_app(data_dir=root / 'db')
        path = root / '旧目录' / '示例视频.mp4'
        path.parent.mkdir()
        path.write_bytes(b'isolated-ui-fixture')
        collection(app, path, 'fixture-video')
        job = {'id': 'fixture-job', 'createdAt': '2026-10-27T10:00:00Z', 'status': 'complete', 'events': [],
               'items': [{'key': 'a', 'videoName': '示例视频', 'startTime': '2026-10-27 10:05:00',
                          'endTime': '2026-10-27 11:05:00', 'status': 'submitted', 'taskId': 'fixture-task'},
                         {'key': 'b', 'videoName': '另一视频', 'startTime': '2026-10-27 11:05:00',
                          'endTime': '2026-10-27 12:05:00', 'status': 'submitted', 'taskId': 'fixture-task-2'}]}
        SubmissionLog(app.state.store.db).add('collection', '示例采集', {'items': job['items']}, submitted_at=job['createdAt'])
        manager = app.state.slicing
        for name, dataset in [('bed', '27912'), ('magnet', '27944')]:
            item = {'id': name, 'runId': 'demo', 'sourceDatasetId': dataset, 'sourceVideoId': dataset + '01',
                    'name': path.name, 'path': str(path), 'fileVersion': source_version(path), 'state': 'no_target',
                    'revision': 0, 'remote': {'duration': 60, 'size': path.stat().st_size}, 'categories': {
                        name: {'state': 'no_target', 'intervals': [], 'estimatedImages': 0,
                               'actualImages': 0, 'intervalSeconds': 1},
                        ('bed' if name == 'magnet' else 'magnet'): {'state': 'not_applicable', 'intervals': []}}}
            manager.store.save('items', item)
        local_path = root / 'local-only.mp4'
        local_path.write_bytes(b'isolated-local-source')
        manager.store.save('items', {'id':'local-only','runId':'demo','sourceDatasetId':'27912','sourceVideoId':'',
            'name':'本地待处理.mp4','path':str(local_path),'fileVersion':source_version(local_path),'state':'attention',
            'errorCode':'worker_stalled','message':'模拟临时故障', 'revision':0,'remote':{'duration':60,'size':local_path.stat().st_size},'categories':{
                'bed':{'state':'attention','errorCode':'worker_stalled','intervals':[],'message':'示例待核对'},
                'magnet':{'state':'not_applicable','intervals':[]}}})
        samples = [('local-second','worker_failed'),('local-uncertain','identity_unknown')]
        if os.environ.get('WORKSPACE_UI_SEARCH'):
            samples += [(f'search-{index}', 'worker_failed') for index in range(4)]
        for identity,code in samples:
            sample = root / f'{identity}.mp4'
            sample.write_bytes(identity.encode())
            manager.store.save('items', {'id':identity,'runId':'demo','sourceDatasetId':'27912','sourceVideoId':'',
                'name':f'{identity}.mp4','path':str(sample),'fileVersion':source_version(sample),'state':'attention',
                'errorCode':code,'message':'身份需核对' if code=='identity_unknown' else '模拟临时故障',
                'revision':0,'remote':{'duration':60,'size':sample.stat().st_size},'categories':{
                    'bed':{'state':'attention','errorCode':code,'intervals':[]},'magnet':{'state':'not_applicable','intervals':[]}}})
        app.state.media_control.refresh()
        # No worker or platform access in this disposable browser fixture.
        manager.auth.access = 'fixture-session-only'
        manager._wake_scheduler = lambda: None
        @app.middleware('http')
        async def test_frame(request, call_next):
            if os.environ.get('WORKSPACE_UI_PROGRESS') and request.url.path == '/api/workspace':
                total = 8 * 1024**3
                targets = [dict(id=identity,label=label,total=4,complete=1,active=2,queued=1,uploaded=1,importing=0,
                    attention=0,bytes=3*1024**3,totalBytes=total,speed=6*1024**2,percent=37.5)
                    for identity,label in [('bed','车斗照片'),('magnet','磁盘吊照片')]]
                task = dict(id='sample',name='示例长视频名称_20261027080500_1234567890.mp4',target='本地识别',stage='识别中',
                            done=42,total=121,bytes=0,totalBytes=0,speed=0,reason='')
                return JSONResponse(dict(enabled=True,revision=1,parent=str(root),root=str(root/'视频工作区'),online=True,
                    maintenance=False,branches=[],assets=[],migration=None,counts={'preserved':3},totalBytes=total,
                    lastAction=dict(state='complete',pipelineState='waiting',pipelineEnabled=True,
                        message='文件位置已核对。',pipelineMessage='本地识别及照片导入进行中。'),
                    progress=dict(updatedAt='2026-10-28T08:00:00Z',uploads=dict(targets=targets,active=4,queued=2,complete=2,total=8),
                        slicing=dict(total=4,complete=1,active=1,queued=2,samplesDone=42,samplesTotal=121,packagesActive=1,imagesComplete=150),
                        files=dict(total=4,organized=3,cleaned=1,waiting=1),activeTasks=[task],activeTaskCount=1)))
            response = await call_next(request)
            response.headers['X-Frame-Options'] = 'SAMEORIGIN'
            response.headers['Content-Security-Policy'] = "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' blob: data:; media-src 'self' blob:; connect-src 'self'; frame-ancestors 'self'"
            return response
        @app.get('/phone', response_class=HTMLResponse)
        async def phone():
            return '<!doctype html><html lang="zh"><title>390px 工作区检查</title><body style="margin:0;background:#ddd"><iframe title="窄屏工作区" src="/" style="display:block;border:0;width:390px;height:900px;margin:auto"></iframe></body></html>'
        @app.get('/desktop', response_class=HTMLResponse)
        async def desktop():
            return '<!doctype html><html lang="zh"><title>1120px 工作区检查</title><body style="margin:0;width:1120px"><iframe title="桌面工作区" src="/" style="border:0;width:1120px;height:1050px"></iframe></body></html>'
        print(json.dumps({'uiFixture': str(root)}), flush=True)
        uvicorn.run(app, host='127.0.0.1', port=8795, access_log=False)


if __name__ == '__main__':
    run()
