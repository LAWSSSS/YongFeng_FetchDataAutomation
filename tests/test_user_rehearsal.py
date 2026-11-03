import json
import httpx
import pytest

from scripts.user_rehearsal import PersistentCapture, create_rehearsal, inspection


def test_inspection_returns_only_the_sample_in_the_requested_date():
    url = 'http://local.test/intelligence/intelliTaskInfo/page'
    rows = inspection(httpx.Request('GET', url, params={
        'startTime': '2026-09-13 00:00:00', 'endTime': '2026-09-13 23:59:59'})).json()['data']
    assert rows['total'] == 1 and rows['records'][0]['carNumber'] == '鲁NH1113'
    assert inspection(httpx.Request('GET', url, params={'startTime': '2026-09-14 00:00:00'})).json()['data']['total'] == 0
    assert not inspection(httpx.Request('GET', url, params={'pageIndex': 2})).json()['data']['records']


def test_batch_inspections_have_unique_intervals_and_honest_pagination():
    url = 'http://local.test/intelligence/intelliTaskInfo/'
    first = inspection(httpx.Request('GET', url+'page', params={'pageSize': 2}), 3).json()['data']
    last = inspection(httpx.Request('GET', url+'page', params={'pageSize': 2, 'pageIndex': 2}), 3).json()['data']
    rows = first['records'] + last['records']
    assert first['total'] == last['total'] == 3 and len(rows) == 3
    assert len({row['flowCode'] for row in rows}) == 3
    starts = [inspection(httpx.Request('GET', url+'getCheckDetail', params={'flowCode': row['flowCode']}), 3)
              .json()['data']['stationResults'][0]['checkStartTime'] for row in rows]
    assert starts == ['2026-09-13 09:49:00', '2026-09-13 09:51:00', '2026-09-13 09:53:00']


@pytest.mark.asyncio
async def test_batch_rehearsal_queries_three_ready_items_without_capture(tmp_path):
    import asyncio
    source = tmp_path / 'source.mp4'
    source.write_bytes(b'isolated batch fixture')
    root = tmp_path / 'batch'
    app = create_rehearsal(root, source, record_count=3)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://127.0.0.1:8790') as client:
            response = await client.post('/api/collection-runs', json={
                'filters': {'dateFrom': '2026-09-13', 'dateTo': '2026-09-13'}, 'requestId': 'batch-query'})
            assert response.status_code == 202
            identity = response.json()['id']
            for _ in range(100):
                state = (await client.get('/api/collection-runs/'+identity)).json()
                if state['status'] == 'awaiting_review':
                    break
                await asyncio.sleep(.02)
            plan = (await client.get('/api/collection-runs/'+identity+'/plan')).json()
            assert len(plan['readyIds']) == 3
            assert [row['startTime'] for row in plan['tasks']] == [
                '2026-09-13 09:54:00', '2026-09-13 09:56:00', '2026-09-13 09:58:00']
            status = (await client.get('/api/rehearsal')).json()
            assert status['recordCount'] == 3 and status['captureTasks'] == 0
            assert not status['uploadsAllowed'] and status['uploadPaused']
    with pytest.raises(ValueError, match='记录数已变化'):
        create_rehearsal(root, source, record_count=2)


@pytest.mark.asyncio
async def test_capture_inventory_survives_restart_without_duplicate_task(tmp_path):
    source = tmp_path / 'sample.mp4'
    source.write_bytes(b'real file bytes for transport contract')
    root = tmp_path / 'platform'
    platform = PersistentCapture(root, source, 'unused', 'stable-run')
    clip = root / 'clip.mp4'
    clip.write_bytes(source.read_bytes())
    platform.clip_cache[(0, 3600)] = clip
    payload = [{'startTime': '2026-09-13 09:54:00', 'endTime': '2026-09-13 10:54:00', 'videoName': '鲁NH1113'}]
    reply = await platform.handle(httpx.Request('POST', 'http://local.test/data-acquisition/batchSave', json=payload))
    assert reply.status_code == 200
    restored = PersistentCapture(root, source, 'unused', 'stable-run')
    assert restored.parents == platform.parents and restored.results == platform.results
    assert restored.metrics['createdTasks'] == 1
    assert await restored.clip(0, 3600) == clip
    object_path = next(iter(restored.objects))
    reply = await restored.handle(httpx.Request('GET', 'http://storage.test' + object_path, headers={'Range': 'bytes=1-4'}))
    assert reply.status_code == 206 and await reply.aread() == source.read_bytes()[1:5]
    with pytest.raises(ValueError, match='09:54:00'):
        await restored.clip(4400, 300)


@pytest.mark.asyncio
async def test_interactive_test_cannot_resume_upload_or_load_real_credentials(tmp_path, monkeypatch):
    def denied(*args, **kwargs):
        raise AssertionError('Rehearsal must not read saved credentials')
    monkeypatch.setattr('app.credentials.load', denied)
    source = tmp_path / 'source.mp4'
    source.write_bytes(b'private sample bytes')
    app = create_rehearsal(tmp_path / 'exercise', source)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://127.0.0.1:8790') as client:
            assert (await client.get('/api/session')).json()['source']['connected']
            for path in ('/api/slicing/packages/resume', '/api/uploads/resume', '/api/upload-audit/connect'):
                assert (await client.post(path, json={})).status_code == 409
            app.state.slicing.packages.resume()
            assert app.state.slicing.packages.paused
            with pytest.raises(Exception, match='不执行上传'):
                await app.state.slicing.packages.remote.jobs('27892')
            state = (await client.get('/api/rehearsal')).json()
            assert not state['uploadsAllowed'] and state['uploadPaused']
            assert not state['captureTasks'] and not state['packages']
            assert (await client.get('/api/workspace')).json()['fixedLocation']
            assert (await client.post('/api/workspace/open', json={}, headers={'Origin': 'https://example.invalid'})).status_code == 403
    assert source.read_bytes() == b'private sample bytes'
    marker = json.loads((tmp_path / 'exercise/rehearsal.json').read_text())
    assert not marker['uploadsAllowed']
    source.write_bytes(b'changed source')
    with pytest.raises(ValueError, match='原片已变化'):
        create_rehearsal(tmp_path / 'exercise', source)


def test_rehearsal_refuses_unknown_directory(tmp_path):
    source = tmp_path / 'sample.mp4'
    source.write_bytes(b'untouched')
    unknown = tmp_path / 'unknown'
    unknown.mkdir()
    with pytest.raises(ValueError, match='未登记'):
        create_rehearsal(unknown, source)
