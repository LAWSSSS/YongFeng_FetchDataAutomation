import httpx
import pytest
from pathlib import Path

from app.main import create_app
from app.record_deletion import RecordDeletion
from tests.test_media_control import collection
from tests.test_manual_completed_cleanup import prepared
from tests.test_slicing_performance import close_app


@pytest.mark.asyncio
async def test_deleted_video_cannot_be_reopened_by_an_exact_link(tmp_path):
    app = create_app(data_dir=tmp_path / 'db')
    path = tmp_path / 'one.mp4'; path.write_bytes(b'old')
    other = tmp_path / 'two.mp4'; other.write_bytes(b'new')
    row = collection(app, path, identity='hidden-exact')
    collection(app, other, identity='visible-same-name')
    app.state.videos.record_visibility.hide(app.state.videos.store, 'hide',
                                          [{'id': row['id'], 'expectedRevision': row['revision']}])
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://127.0.0.1') as client:
            listing = (await client.get('/api/videos?compact=true')).json()
            assert [r['id'] for r in listing['videos']] == ['visible-same-name']
            assert (await client.get('/api/videos/hidden-exact')).status_code == 404
            detail = (await client.get('/api/videos/visible-same-name')).json()
            assert detail['video']['id'] == 'visible-same-name'
            assert 'requests' not in detail['video']
            assert app.state.videos.record_visibility.hidden() == {'hidden-exact'}
            row['deletedAt'] = '2026-11-02T01:00:00Z'
            app.state.videos.store.save(row)
            assert (await client.get('/api/videos/hidden-exact')).status_code == 404
            assert (await client.get('/api/videos/missing')).status_code == 404
    finally:
        await close_app(app)


@pytest.mark.asyncio
async def test_hidden_running_record_can_be_deleted_while_download_progress_changes(tmp_path):
    app = create_app(data_dir=tmp_path / 'db')
    path = tmp_path / 'running.mp4'; path.write_bytes(b'keep-original')
    row = collection(app, path, identity='hidden-running', accepted=False)
    row['versions'][0].update(state='downloading', downloadRequested=True, receivedBytes=1)
    app.state.videos.store.save(row)
    app.state.videos.record_visibility.hide(app.state.videos.store, 'first-hide',
        [{'id': row['id'], 'expectedRevision': row['revision']}])
    payload = {'expectedRevision': row['revision'], 'requestId': 'delete-hidden'}
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://127.0.0.1') as client:
            response = await client.post('/api/videos/hidden-running/hide-record', json=payload)
            assert response.status_code == 202
            assert (await client.post('/api/videos/hidden-running/hide-record', json=payload)).json() == response.json()
            row['versions'][0]['receivedBytes'] = 50
            app.state.videos.store.save(row)
            await app.state.record_deletion.tick()
            result = (await client.get('/api/record-operations/delete-hidden')).json()
            assert result['state'] == 'complete'
            current = app.state.videos.store.get(row['id'])
            assert current['versions'][0]['state'] == 'downloading'
            assert current['versions'][0]['receivedBytes'] == 50
            assert current['versions'][0]['downloadRequested']
            assert path.read_bytes() == b'keep-original'
            assert not (await client.get('/api/videos?compact=true')).json()['videos']
            assert (await client.get('/api/videos/hidden-running')).status_code == 404
    finally:
        await close_app(app)


@pytest.mark.asyncio
async def test_hidden_delete_checks_revision_and_batch_keeps_new_corrections(tmp_path):
    app = create_app(data_dir=tmp_path / 'db')
    path = tmp_path / 'one.mp4'; path.write_bytes(b'keep')
    hidden = collection(app, path, identity='hidden')
    visible = collection(app, path, identity='visible')
    app.state.videos.record_visibility.hide(app.state.videos.store, 'hide', [{'id':'hidden','expectedRevision':0}])
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://127.0.0.1') as client:
            stale = await client.post('/api/videos/hidden/hide-record', json={'expectedRevision':99,'requestId':'stale'})
            assert stale.status_code == 409
            assert app.state.record_deletion.get('stale') is None
            payload = {'requestId':'batch','selections':[{'kind':'collection','id':r['id'],'expectedRevision':0} for r in (hidden,visible)]}
            assert (await client.post('/api/records/delete',json=payload)).status_code == 202
            hidden.update(revision=1, activeRevision=1)
            app.state.videos.store.save(hidden)
            await app.state.record_deletion.tick()
            result = (await client.get('/api/record-operations/batch')).json()
            assert {o['id']:o['state'] for o in result['outcomes']} == {'hidden':'attention','visible':'complete'}
            assert app.state.videos.store.get('hidden')['activeRevision'] == 1 and path.exists()
    finally:
        await close_app(app)


@pytest.mark.asyncio
async def test_hidden_completed_record_cleans_verified_files_and_restarts_idempotently(tmp_path, monkeypatch):
    app, workspace, asset = await prepared(tmp_path)
    videos = app.state.videos
    row = videos.store.list()[0]
    visibility = videos.record_visibility
    visibility.hide(videos.store, 'first-hide', [{'id':row['id'],'expectedRevision':row['revision']}])
    controller = app.state.record_deletion
    try:
        response = controller.register('delete-ended', [('collection',[{'id':row['id'],'expectedRevision':row['revision']}])])
        await controller.tick()
        with workspace.leases.read(asset['path']):
            await workspace.cleanup(workspace.get('assets',asset['id']))
        assert Path(asset['path']).is_file()
        await workspace.cleanup(workspace.get('assets',asset['id']))
        assert not Path(asset['path']).exists()
        hide = visibility.hide
        def interrupted(*args, **kwargs):
            hide(*args, **kwargs)
            raise RuntimeError('restart after persisting visibility')
        monkeypatch.setattr(visibility, 'hide', interrupted)
        with pytest.raises(RuntimeError):
            await controller.tick()
        monkeypatch.setattr(visibility, 'hide', hide)
        resumed = RecordDeletion(controller.db, workspace, controller.registries)
        await resumed.tick()
        assert resumed.get(response['operationId'])['state'] == 'complete'
        assert row['id'] in visibility.hidden()
        assert videos.store.get(row['id'])['versions'][0]['taskId'] == 'capture-task'
    finally:
        await close_app(app)


@pytest.mark.asyncio
async def test_video_finishing_after_delete_click_does_not_expand_cleanup(tmp_path):
    app, workspace, asset = await prepared(tmp_path)
    row = app.state.videos.store.list()[0]
    package = workspace.packages(asset)[0]
    store = app.state.slicing.packages.store
    original = package['state']
    package['state'] = 'importing'
    store.save('items', package)
    controller = app.state.record_deletion
    try:
        controller.register('before-finish',[('collection',[{'id':row['id'],'expectedRevision':row['revision']}])])
        assert controller.get('before-finish')['selections'][0]['presentationOnly']
        package['state'] = original
        store.save('items', package)
        assert workspace.cleanup_eligibility(asset)[0]
        await controller.tick()
        assert controller.get('before-finish')['state'] == 'complete'
        assert Path(asset['path']).exists()
        assert not workspace.get('assets',asset['id']).get('cleanup',{}).get('requestId')
    finally:
        await close_app(app)
