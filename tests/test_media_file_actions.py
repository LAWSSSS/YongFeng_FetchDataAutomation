"""File controls use their own current processing binding, not a group anchor."""
import httpx
import pytest

from app.main import create_app
from tests.test_media_control import collection, sliced
from tests.test_slicing_performance import close_app


def grouped_sources(app, tmp_path):
    first_path, second_path = tmp_path / 'original.mp4', tmp_path / 'recovery.mp4'
    first_path.write_bytes(b'original-source')
    second_path.write_bytes(b'recovered-source')
    collection(app, first_path)
    sliced(app, first_path, 'first', '27912', 'complete')
    second = sliced(app, second_path, 'second', '27944', 'complete')
    second.update(collectionVideoId='collection', collectionRevision=0, collectionRecovery=True)
    app.state.slicing.store.save('items', second)
    app.state.media_control.refresh()
    files = app.state.slicing.files
    files.refresh()
    own_file = next(row for row in files.rows('files') if row['path'] == str(second_path))
    own_file.update(archiveState='attention', archiveMessage='文件整理等待重试。')
    files.save('files', own_file)
    return own_file


@pytest.mark.asyncio
@pytest.mark.parametrize('action', ['retry-archive', 'cleanup'])
async def test_projected_file_binding_executes_and_replays_the_correct_action(tmp_path, monkeypatch, action):
    app = create_app(data_dir=tmp_path / 'db')
    own_file = grouped_sources(app, tmp_path)
    calls = []

    async def perform(identity, request_id):
        calls.append((identity, request_id))
        return {}

    files = app.state.slicing.files
    monkeypatch.setattr(files, 'retry' if action == 'retry-archive' else 'cleanup_confirmed', perform)
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://127.0.0.1') as client:
            group = (await client.get('/api/video-processing')).json()['groups'][0]
            assert [item['id'] for item in group['items']] == ['first', 'second']
            projected = next(row for row in group['files'] if row['id'] == own_file['id'])
            assert projected['actionItemId'] == 'second'
            assert group['nextAction']['actionItemId'] == 'second'
            assert group['nextAction']['fileId'] == own_file['id']
            url = '/api/video-processing/' + group['id'] + '/actions'
            body = dict(action=action, itemId='first', fileId=own_file['id'],
                        generation=group['generation'], expectedRevision=0, requestId='wrong-anchor')
            rejected = await client.post(url, json=body)
            assert rejected.status_code == 409
            assert calls == []
            body.update(itemId=projected['actionItemId'], requestId='correct-binding')
            assert (await client.post(url, json=body)).status_code == 200
            assert (await client.post(url, json=body)).status_code == 200
            assert calls == [(own_file['id'], 'correct-binding')]
    finally:
        await close_app(app)


@pytest.mark.asyncio
async def test_file_binding_is_not_authorized_when_its_item_needs_identity_review(tmp_path):
    app = create_app(data_dir=tmp_path / 'db')
    own_file = grouped_sources(app, tmp_path)
    item = app.state.slicing.store.get('items', 'second')
    item.update(identityConflict=True, processingReason='原片身份需要核对。')
    app.state.slicing.store.save('items', item)
    try:
        group = app.state.media_control.state()['groups'][0]
        projected = next(row for row in group['files'] if row['id'] == own_file['id'])
        assert projected['actionItemId'] is None
        assert group['nextAction']['action'] == 'open'
        assert not group['nextAction']['canContinue']
    finally:
        await close_app(app)


@pytest.mark.asyncio
async def test_exhausted_decode_retries_show_the_actual_failure(tmp_path):
    from tests.test_slicing_continue import failed, setup_run
    app = create_app(data_dir=tmp_path / 'db')
    path = tmp_path / 'video.mp4'
    path.write_bytes(b'preserved-video')
    item = failed(app, path, 'timed-out', '27912', 'frame_timeout')
    item['message'] = '85分30秒位置解码超时，原片保留。'
    item['categories']['bed']['automaticRetries'] = 2
    app.state.slicing.store.save('items', item)
    setup_run(app)
    try:
        action = app.state.media_control.state()['groups'][0]['nextAction']
        assert action['kind'] == 'manual' and not action['autoEligible']
        assert action['reason'] == item['message']
        assert action['stage'] == '需要核对后继续'
    finally:
        await close_app(app)
