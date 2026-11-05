import copy
from pathlib import Path

import httpx
import pytest

from app.main import create_app
from app.videos import Conflict
from app.workspace_files import create_root
from tests.test_simple_flow import FakePlatform
from tests.test_slicing_performance import close_app


@pytest.mark.asyncio
async def test_history_download_resumes_without_confirming_or_overwriting_current(tmp_path, monkeypatch):
    platform = FakePlatform()
    platform.objects['/history.mp4'] = platform.media
    app = create_app(data_dir=tmp_path/'db')
    videos, worker = app.state.videos, app.state.history_downloads
    await videos.target.client.aclose()
    videos.target.client = httpx.AsyncClient(transport=httpx.MockTransport(platform.handle))
    videos.target.connected = True
    app.state.workspace.config.update(enabled=True, root=str(tmp_path/'media'))
    create_root(app.state.workspace.root, app.state.workspace.config['workspaceId'])
    app.state.workspace.online = True
    old = {'revision':0,'state':'ready','taskId':'task-old','resultId':'result-old',
           'startTime':'2026-10-01 00:00:00','etag':'"v1"','totalBytes':len(platform.media)}
    row = {'id':'video','name':'test','revision':4,'activeRevision':1,'acceptedRevision':None,
           'versions':[old,{'revision':1,'state':'waiting','taskId':'new-task'}]}
    videos.store.save(row)
    before = copy.deepcopy(videos.store.get(row['id']))
    async def bound(version):
        assert version['resultId'] == 'result-old'
        return {'id':'result-old','url':'http://storage.test/history.mp4'}
    async def probe(path):
        assert path.read_bytes() == platform.media
    monkeypatch.setattr(videos, 'bound_result', bound)
    monkeypatch.setattr(videos, 'ffprobe', probe)
    body = {'version':0,'expectedRevision':4,'requestId':'history-one'}
    result = worker.register('video', body)
    part = app.state.workspace.root/'.缓存/历史下载'/result['operationId']/'v0.part'
    part.parent.mkdir(parents=True);part.write_bytes(platform.media[:1024])
    operation = worker.rows()[0]
    await worker.execute(operation)
    assert operation['state'] == 'complete' and not part.exists()
    assert Path(operation['filePath']).read_bytes() == platform.media
    assert videos.store.get('video') == before
    assert worker.register('video',body)['operationId'] == result['operationId']
    assert worker.register('video',{**body,'requestId':'other'})['operationId'] == result['operationId']
    assert len(worker.rows()) == 1
    with pytest.raises(Conflict):worker.register('video',{**body,'version':1})
    await close_app(app)


@pytest.mark.asyncio
async def test_history_offline_retains_binding_and_conflict_never_publishes(tmp_path):
    app=create_app(data_dir=tmp_path/'db');worker=app.state.history_downloads
    row={'id':'v','name':'v','revision':0,'activeRevision':1,'versions':[
        {'revision':0,'state':'ready','taskId':'task','resultId':'r'}, {'revision':1,'state':'waiting'}]}
    app.state.videos.store.save(row)
    worker.register('v',{'version':0,'expectedRevision':0,'requestId':'id'})
    operation=worker.rows()[0]
    await worker.execute(operation)
    assert operation['state']=='waiting' and 'publishing' not in operation
    app.state.workspace.online=True;app.state.videos.target.connected=True
    row['versions'][0]['resultId']='changed';app.state.videos.store.save(row)
    with pytest.raises(Conflict):await worker.execute(operation)
    assert 'filePath' not in operation
    await close_app(app)
