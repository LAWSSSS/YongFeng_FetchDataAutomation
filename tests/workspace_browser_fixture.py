"""Disconnected browser fixture, including a 390px iframe for narrow-layout QA.

Run only as `python -m tests.workspace_browser_fixture`. No live credentials,
database, models, mount points or platform calls are used.
"""
import json
import tempfile
from pathlib import Path

from fastapi.responses import HTMLResponse

from app.main import create_app
from app.slicing import source_version


def run():
    import uvicorn
    with tempfile.TemporaryDirectory(prefix='workspace-ui-check-') as folder:
        root = Path(folder).resolve()
        app = create_app(data_dir=root / 'db')
        path = root / '旧目录' / '示例视频.mp4'
        path.parent.mkdir()
        path.write_bytes(b'isolated-ui-fixture')
        manager = app.state.slicing
        for name, dataset in [('bed', '27912'), ('magnet', '27944')]:
            item = {'id': name, 'runId': 'demo', 'sourceDatasetId': dataset, 'sourceVideoId': dataset + '01',
                    'name': path.name, 'path': str(path), 'fileVersion': source_version(path), 'state': 'no_target',
                    'revision': 0, 'remote': {'duration': 60, 'size': path.stat().st_size}, 'categories': {
                        name: {'state': 'no_target', 'intervals': [], 'estimatedImages': 0,
                               'actualImages': 0, 'intervalSeconds': 1},
                        ('bed' if name == 'magnet' else 'magnet'): {'state': 'not_applicable', 'intervals': []}}}
            manager.store.save('items', item)
        @app.middleware('http')
        async def test_frame(request, call_next):
            response = await call_next(request)
            response.headers['X-Frame-Options'] = 'SAMEORIGIN'
            response.headers['Content-Security-Policy'] = "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' blob: data:; media-src 'self' blob:; connect-src 'self'; frame-ancestors 'self'"
            return response
        @app.get('/phone', response_class=HTMLResponse)
        async def phone():
            return '<!doctype html><html lang="zh"><title>390px 工作区检查</title><body style="margin:0;background:#ddd"><iframe title="窄屏工作区" src="/" style="display:block;border:0;width:390px;height:900px;margin:auto"></iframe></body></html>'
        print(json.dumps({'uiFixture': str(root)}), flush=True)
        uvicorn.run(app, host='127.0.0.1', port=8795, access_log=False)


if __name__ == '__main__':
    run()
