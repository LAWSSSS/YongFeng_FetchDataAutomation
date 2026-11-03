"""Batch presentation changes never mutate workers, source files, or receipts."""
import copy

import httpx
import pytest

from app.main import create_app
from tests.test_record_management import seed_slice
from tests.test_slicing_performance import close_app


async def delete_records(app, client, url, **kwargs):
    response = await client.post(url, **kwargs)
    return response


@pytest.mark.asyncio
async def test_batch_child_validation_projects_each_submission_once(tmp_path, monkeypatch):
    app = create_app(data_dir=tmp_path / 'db')
    try:
        store, views = app.state.store, app.state.slicing.submission_views
        receipts = []
        for parent, count in (('one', 125), ('two', 4)):
            store.save({'id': parent, 'status': 'complete', 'createdAt': '2026-10-25T00:00:00Z',
                        'events': [], 'items': [{'key': f'{parent}-{index}', 'videoName': f'truck-{index}',
                            'startTime': '2026-10-25 09:00:00', 'endTime': '2026-10-25 10:00:00',
                            'status': 'submitted', 'taskId': f'cloud-{parent}-{index}'} for index in range(count)]})
            receipts.append(views.submissions.add('collection', '采集提交', {'jobId': parent}))
        before = {parent: copy.deepcopy(store.get(parent)) for parent in ('one', 'two')}
        children = [item for receipt in receipts
                    for item in views.project(receipt, detail=True, page_size=100000)['detail']['items']]
        original = views.project
        calls = []

        def project_once(row, **kwargs):
            calls.append(row['id'])
            return original(row, **kwargs)

        monkeypatch.setattr(views, 'project', project_once)
        body = {'requestId': 'all-children', 'selections': [
            {'id': item['deletionKey'], 'expectedRevision': 0} for item in children]}
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://127.0.0.1') as client:
            stale = copy.deepcopy(body)
            stale['selections'][-1]['expectedRevision'] = 1
            assert (await delete_records(app, client, '/api/submissions/items/remove', json=stale)).status_code == 409
            assert not views.submissions.item_visibility.hidden()
            assert set(calls) == {row['id'] for row in receipts} and len(calls) == 2
            calls.clear()
            response = await delete_records(app, client, '/api/submissions/items/remove', json=body)
            assert response.status_code == 202 and response.json()['total'] == 129
            assert set(calls) == {row['id'] for row in receipts} and len(calls) == 2
            calls.clear()
            assert (await delete_records(app, client, '/api/submissions/items/remove', json=body)).status_code == 202
            assert calls == [], 'A repeated request must reuse its visibility receipt.'
            restored = await client.post('/api/submissions/items/restore', json={
                'requestId': 'undo-all', 'removalId': 'all-children'})
            assert restored.status_code == 410
            await app.state.record_deletion.tick()
            assert len(views.submissions.item_visibility.hidden()) == 129
        assert all(store.get(parent) == expected for parent, expected in before.items())
    finally:
        await close_app(app)


@pytest.mark.asyncio
async def test_local_control_card_batch_hide_keeps_the_actual_processing_queue(tmp_path):
    app = create_app(data_dir=tmp_path / 'db')
    try:
        path = tmp_path / 'source.mp4'
        path.write_bytes(b'keep-original')
        items = []
        for name in ('bed', 'magnet'):
            row = seed_slice(app, path, name)
            row['state'] = 'pending'
            row['categories'][name]['state'] = 'pending'
            row['categories'][name].pop('taskId')
            app.state.slicing.store.save('items', row)
            items.append(row)
        app.state.media_control.refresh()
        before = copy.deepcopy(app.state.slicing.store.list('items'))
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://127.0.0.1') as client:
            original = (await client.get('/api/video-processing')).json()['groups']
            assert len(original) == 1 and len(original[0]['items']) == 2
            body = {'requestId': 'hide-local-card', 'selections': [
                {'id': row['id'], 'expectedRevision': row['revision']} for row in before]}
            assert (await delete_records(app, client, '/api/slicing/records/remove', json=body)).status_code == 202
            await app.state.record_deletion.tick()
            assert (await client.get('/api/video-processing')).json()['groups'] == []
            assert {row['id'] for row in app.state.media_control.effective_items()} == {'bed', 'magnet'}
            assert app.state.slicing.store.list('items') == before
            assert (await client.post('/api/slicing/records/restore', json={
                'requestId': 'undo-local', 'removalId': 'hide-local-card'})).status_code == 410
            restored = (await client.get('/api/video-processing')).json()['groups']
            assert restored == []
        assert path.read_bytes() == b'keep-original'
    finally:
        await close_app(app)
