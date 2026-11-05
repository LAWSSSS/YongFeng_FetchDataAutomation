"""Video confirmation and results share the same version-bound live facts."""
import copy
from pathlib import Path

import httpx
import pytest

from tests.test_final_processing import env, payload
from tests.test_media_control import sliced


def confirmed_photos(app, row, state='queued'):
    flow = app.state.final_processing
    result = flow.confirm(row['id'], payload(row))
    operation = flow.get(result['operationId'])
    current = app.state.videos.store.get(row['id'])
    current.update(acceptedRevision=0)
    current['versions'][0]['state'] = 'saved'
    app.state.videos.store.save(current)
    item = sliced(app, Path(current['versions'][0]['filePath']), 'bound-slice', state='platform_processing')
    item.update(mediaId='collection:'+row['id'], collectionVideoId=row['id'], collectionRevision=0,
                mediaGeneration=operation['mediaGeneration'])
    item['categories'] = {'bed': {'state': 'queued', 'packageId': 'exact-package', 'intervals': []},
                          'magnet': {'state': 'no_target', 'intervals': []}}
    app.state.slicing.store.save('items', item)
    app.state.slicing.packages.store.save('items', {
        'id': 'exact-package', 'itemId': item['id'], 'category': 'bed', 'state': state,
        'imageCount': 61, 'size': 1000, 'bytes': 25, 'dataset': '27892',
        'videoPath': item['path'], 'videoFileVersion': item['fileVersion']})
    operation.update(state='running', stage='photo_upload', message='old upload message')
    flow.save(operation)
    app.state.media_control.refresh()
    return current, app.state.slicing.store.get('items', item['id']), operation


@pytest.mark.asyncio
async def test_final_approval_immediately_exposes_real_download_progress_in_both_views(env):
    app, row = env
    assert not app.state.pipeline.state()['items'], 'Unconfirmed captures stay in video confirmation.'
    app.state.final_processing.confirm(row['id'], payload(row))
    current = app.state.videos.store.get(row['id'])
    current['versions'][0].update(state='downloading', receivedBytes=25, totalBytes=100)
    app.state.videos.store.save(current)
    before = app.state.store.db.total_changes
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://127.0.0.1') as client:
        video = (await client.get('/api/videos/'+row['id'])).json()['video']['finalProcessing']
        result = (await client.get('/api/pipeline')).json()['items'][0]
    assert app.state.store.db.total_changes == before
    assert video['stageLabel'] == result['stage'] == '下载'
    assert video['version'] == result['version'] == 0 and result['updatedAt']
    assert video['progress'] == result['nextAction']['progress'] == {'done': 25, 'total': 100, 'unit': 'bytes'}
    assert not result['hasResultRecord'] and not result['packages']


@pytest.mark.asyncio
async def test_paused_photos_stay_waiting_after_history_is_hidden_without_resurrection(env):
    app, row = env
    current, item, _ = confirmed_photos(app, row)
    packages = app.state.slicing.packages
    packages.pause()
    visible = app.state.pipeline.state()
    assert visible['items'][0]['stage'] == '照片上传已暂停'
    shown = app.state.videos.state(identity=row['id'])['videos'][0]['finalProcessing']
    assert shown['state'] == 'waiting' and not shown['terminal']
    assert visible['items'][0]['nextAction']['kind'] == 'waiting'
    summary = app.state.slicing.state(summary=True)
    history = summary['items'][0]
    assert history['processing']['stageLabel'] == shown['stageLabel']
    assert history['submittedAt'] == shown['confirmedAt']
    assert summary['recordDays'][0]['day'], 'A local approval must not disappear into an undated bucket.'
    assert not app.state.slicing.store.get('items', item['id']).get('submittedAt'), 'Presentation must preserve original receipts.'
    app.state.slicing.visibility.hide(app.state.slicing.store, 'hide-result', [{'id': item['id'], 'expectedRevision': item['revision']}])
    app.state.videos.record_visibility.hide(app.state.videos.store, 'hide-video', [{'id': row['id'], 'expectedRevision': current['revision']}])
    before = packages.store.get('items', 'exact-package')
    hidden = app.state.pipeline.state()
    assert hidden['items'] == [] and hidden['counts']['waiting'] == 0
    assert hidden['backgroundCounts']['waiting'] == 1
    assert hidden['photoUploads'] == {'paused': True, 'queued': 1, 'videos': 1, 'images': 61}
    assert not app.state.slicing.state(summary=True)['items']
    assert not app.state.videos.state()['videos']
    assert packages.store.get('items', 'exact-package') == before and packages.paused


@pytest.mark.asyncio
@pytest.mark.parametrize('state,stage', [('queued', '排队上传'), ('uploading', '照片上传'),
                                         ('uploaded', '照片导入'), ('importing', '照片导入'),
                                         ('complete', '结果核验')])
async def test_package_stage_updates_both_views_without_claiming_unverified_completion(env, state, stage):
    app, row = env
    confirmed_photos(app, row, state)
    app.state.slicing.packages.paused = False
    shown = app.state.pipeline.state()['items'][0]
    video = app.state.videos.state(identity=row['id'])['videos'][0]['finalProcessing']
    assert shown['stage'] == video['stageLabel'] == stage
    assert shown['packages'][0]['id'] == 'exact-package'
    assert shown['hasResultRecord'] and not video['terminal'] and shown['state'] != 'complete'
    if state == 'uploading':
        assert shown['nextAction']['progress'] == {'done': 25, 'total': 1000, 'unit': 'bytes'}
    if state == 'importing':
        app.state.slicing.packages.pause()
        assert app.state.pipeline.state()['items'][0]['state'] == 'processing'


@pytest.mark.asyncio
async def test_one_verified_package_does_not_hide_the_other_category_export(env):
    app, row = env
    _, item, _ = confirmed_photos(app, row, state='complete')
    item['categories']['magnet'].update(state='ready')
    app.state.slicing.store.save('items', item)
    shown = app.state.pipeline.state()['items'][0]
    assert shown['stage'] == '照片导出' and shown['state'] == 'processing'


@pytest.mark.asyncio
async def test_wrong_package_or_new_unconfirmed_version_never_acquires_the_old_results(env):
    app, row = env
    current, item, operation = confirmed_photos(app, row)
    store = app.state.slicing.packages.store
    wrong = store.get('items', 'exact-package')
    wrong['itemId'] = 'another-video'
    store.save('items', wrong)
    assert app.state.pipeline.state()['photoUploads']['queued'] == 0
    assert not app.state.pipeline.state()['items'][0]['packages']
    previous = copy.deepcopy(operation)
    current.update(activeRevision=1, revision=current['revision']+1)
    new = {**current['versions'][0], 'revision': 1, 'state': 'ready', 'taskId': 'new-task'}
    new.pop('finalConfirmation', None)
    current['versions'].append(new)
    app.state.videos.store.save(current)
    assert app.state.final_processing.presentations() == []
    assert app.state.pipeline.state()['items'] == [], 'Old generations remain history, not active work for the unconfirmed version.'
    shown = app.state.videos.state(identity=row['id'])['videos'][0]
    assert shown['finalProcessing'] is None
    assert app.state.final_processing.get(operation['id']) == previous
