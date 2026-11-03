"""An upload pause is a wait, not running work; reads never resume it."""
import asyncio

import httpx
import pytest
import pytest_asyncio

from app.main import create_app
from tests.test_media_control import sliced
from tests.test_slicing_performance import close_app


@pytest_asyncio.fixture
async def app(tmp_path, monkeypatch):
    value = create_app(data_dir=tmp_path / 'db')
    monkeypatch.setattr(value.state.slicing, '_wake_scheduler', lambda: None)
    yield value
    await close_app(value)


def queued_photos(app, tmp_path, identity='video', states=None):
    states = states or {'bed': 'queued', 'magnet': 'queued'}
    path = tmp_path / f'{identity}.mp4'
    path.write_bytes(identity.encode())
    manager = app.state.slicing
    item = sliced(app, path, identity, state='platform_processing')
    for category, state in states.items():
        package_id = f'{identity}-{category}'
        item['categories'][category] = {'state': 'platform_processing', 'packageId': package_id}
        manager.packages.store.save('items', {
            'id': package_id, 'itemId': identity, 'revision': 0, 'category': category,
            'dataset': '27892' if category == 'bed' else '27893',
            'state': state, 'imageCount': 150, 'size': 1000, 'bytes': 0,
            'videoPath': str(path), 'videoFileVersion': item['fileVersion']})
    manager.store.save('items', item)
    app.state.media_control.refresh()
    return manager.store.get('items', identity)


@pytest.mark.asyncio
@pytest.mark.parametrize('state', ['queued', 'uploaded', 'uploading'])
async def test_paused_unsent_photos_are_waiting_and_reads_keep_the_pause(app, tmp_path, state):
    queued_photos(app, tmp_path, states={'bed': state})
    packages = app.state.slicing.packages
    packages.pause()
    before = packages.store.get('items', 'video-bed')
    value = app.state.pipeline.state()
    assert value['counts'] == {'processing': 0, 'waiting': 1, 'attention': 0, 'complete': 0}
    action = value['items'][0]['nextAction']
    assert action['kind'] == 'waiting' and action['action'] == 'none'
    assert action['stage'] == '照片上传已暂停'
    assert not action['canContinue']
    assert value['photoUploads']['queued'] == 1
    assert packages.paused and packages.store.get('items', 'video-bed') == before


@pytest.mark.asyncio
async def test_unpaused_photos_remain_running_without_recreating_any_packages(app, tmp_path):
    queued_photos(app, tmp_path)
    before = app.state.slicing.packages.store.list('items')
    state = app.state.pipeline.state()
    assert state['counts']['processing'] == 1 and state['counts']['waiting'] == 0
    assert state['photoUploads'] == {'paused': False, 'queued': 2, 'videos': 1, 'images': 300}
    assert state['items'][0]['stage'] == '排队上传'
    assert app.state.slicing.packages.store.list('items') == before


@pytest.mark.asyncio
@pytest.mark.parametrize('submitted_state', ['submitting', 'importing'])
async def test_submitted_imports_take_priority_and_reconcile_while_uploads_paused(app, tmp_path, submitted_state):
    queued_photos(app, tmp_path, states={'bed': 'queued', 'magnet': submitted_state})
    app.state.slicing.packages.pause()
    state = app.state.pipeline.state()
    assert state['counts']['processing'] == 1 and state['counts']['waiting'] == 0
    assert state['items'][0]['stage'] == ('提交导入' if submitted_state == 'submitting' else '导入照片')
    assert state['photoUploads']['queued'] == 1


@pytest.mark.asyncio
async def test_local_export_is_running_even_when_another_category_waits_for_upload(app, tmp_path):
    row = queued_photos(app, tmp_path)
    manager = app.state.slicing
    manager.packages.pause()
    task = asyncio.create_task(asyncio.Event().wait())
    manager.export_tasks[row['id']] = task
    state = app.state.pipeline.state()
    assert state['counts']['processing'] == 1
    assert state['items'][0]['stage'] == '导出照片'
    assert not task.done() and manager.packages.paused


@pytest.mark.asyncio
async def test_photo_summary_is_global_across_filters_pages_and_excludes_superseded_versions(app, tmp_path):
    queued_photos(app, tmp_path, 'one')
    queued_photos(app, tmp_path, 'two')
    old = queued_photos(app, tmp_path, 'old')
    old['superseded'] = True
    app.state.slicing.store.save('items', old)
    app.state.slicing.packages.pause()
    for query in ({'page_size': 1}, {'page': 2, 'page_size': 1}, {'status': 'attention'}, {'search': 'absent'}):
        state = app.state.pipeline.state(**query)
        assert state['photoUploads'] == {'paused': True, 'queued': 4, 'videos': 2, 'images': 600}
        assert state['counts']['waiting'] == 2 and state['counts']['processing'] == 0


@pytest.mark.asyncio
async def test_explicit_photo_resume_is_idempotent_and_preserves_other_pauses(app, tmp_path):
    row = queued_photos(app, tmp_path)
    pipeline, control = app.state.pipeline, app.state.media_control
    pipeline.config.update(enabled=True, paused=True)
    pipeline.save('control', pipeline.config)
    media = control.get(row['mediaId'])
    media['paused'] = True
    control.save(media)
    app.state.slicing.packages.pause()
    before = app.state.slicing.packages.store.list('items')
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://127.0.0.1') as client:
        assert (await client.get('/api/pipeline')).json()['photoUploads']['paused']
        for _ in range(2):
            response = await client.post('/api/slicing/packages/resume', json={})
            assert response.status_code == 200 and not response.json()['packages']['paused']
        state = (await client.get('/api/pipeline')).json()
    assert not state['photoUploads']['paused'] and state['paused']
    assert control.get(row['mediaId'])['paused']
    assert app.state.slicing.packages.store.list('items') == before
    assert not app.state.uploads.store.settings.get('enabled')
