import asyncio
import json

import httpx
import pytest

from tests.test_simple_flow import app_factory, platform, new_video
from tests.test_collection_review import queried
from tests.test_slicing_performance import close_app


@pytest.mark.asyncio
@pytest.mark.parametrize('lost', ['', 'stop', 'delete'])
async def test_terminate_exact_capture_reconciles_and_unblocks_same_interval(app_factory, platform, lost):
    app = app_factory()
    manager = app.state.videos
    platform.new_tasks_waiting = True
    platform.require_stopped_delete = True
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://127.0.0.1') as client:
            row = await new_video(client, app)
            task_id = row['versions'][0]['taskId']
            assert row['capabilities']['canTerminateCapture']
            platform.lose_stop_response = lost == 'stop'
            platform.lose_delete_response = 'task' if lost == 'delete' else ''
            endpoint = f'/api/videos/{row["id"]}'
            assert (await client.post(endpoint + '/hide-record', json={'expectedRevision': 0, 'requestId': 'hide'})).status_code == 409
            payload = {'version': 0, 'expectedRevision': 0, 'requestId': 'terminate'}
            async with manager.jobs.lock:
                response = await client.post(endpoint + '/terminate-capture', json=payload)
                assert response.status_code == 202, response.text
                operation_id = response.json()['operationId']
                assert (await client.post(endpoint + '/terminate-capture', json=payload)).json()['operationId'] == operation_id
                current = (await client.get(endpoint)).json()['video']
                assert current['versions'][0]['state'] == 'terminating'
                assert not current['capabilities']['canTerminateCapture']
            await asyncio.gather(*tuple(manager.operation_tasks.values()))
            operation = manager.store.get_operation(operation_id)
            if lost:
                assert operation['state'] == 'waiting_connection'
                assert not manager.store.get(row['id']).get('deletedAt')
                operation['nextAttemptAt'] = 0
                manager.store.save_operation(operation)
                await manager.resume_operations()
                await asyncio.gather(*tuple(manager.operation_tasks.values()))
            assert manager.store.get_operation(operation_id)['state'] == 'complete'
            assert platform.stop_calls == [task_id] and task_id not in platform.parents
            assert (await client.get(endpoint)).status_code == 404
            assert (await client.post(endpoint + '/terminate-capture', json=payload)).json()['operationId'] == operation_id
            job = manager.jobs.store.get(row['jobId'])
            assert job['items'][0]['status'] == 'removed'
            assert row['taskKey'] not in manager.jobs.store.known_keys(include_retired=False)
            assert not (await client.get('/api/videos')).json()['videos']
    finally:
        await close_app(app)


@pytest.mark.asyncio
async def test_terminate_preserves_result_created_during_stop(app_factory, platform):
    app = app_factory()
    manager = app.state.videos
    platform.new_tasks_waiting = True
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://127.0.0.1') as client:
            row = await new_video(client, app)
            task_id = row['versions'][0]['taskId']
            result = {'id': 'late-result', 'taskId': task_id, 'status': 'FINISH', 'url': 'http://storage.test/late.mp4'}

            def finish_on_stop(request):
                response = platform.handle(request)
                if request.url.path.endswith('/data-acquisition/end-task') and json.loads(request.content)['taskId'] == task_id:
                    platform.parents[task_id]['taskStatus'] = 'FIXED'
                    platform.results[task_id].append(result)
                return response

            manager.target.client._transport = httpx.MockTransport(finish_on_stop)
            endpoint = f'/api/videos/{row["id"]}'
            response = await client.post(endpoint + '/terminate-capture',
                                         json={'version': 0, 'expectedRevision': 0, 'requestId': 'terminate'})
            assert response.status_code == 202
            await asyncio.gather(*tuple(manager.operation_tasks.values()))
            op = manager.store.get_operation(response.json()['operationId'])
            assert op['state'] == 'conflict' and '视频结果' in op['message']
            assert task_id in platform.parents and platform.results[task_id] == [result]
            await manager.check_once()
            assert (await client.get(endpoint)).json()['video']['versions'][0]['resultId'] == result['id']
    finally:
        await close_app(app)


@pytest.mark.asyncio
@pytest.mark.parametrize('conflict', ['invalid-pagination', 'shared-task'])
async def test_terminate_never_writes_when_capture_or_results_are_ambiguous(app_factory, platform, conflict):
    app = app_factory()
    videos = app.state.videos
    platform.new_tasks_waiting = True
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://127.0.0.1') as client:
            row = await new_video(client, app)
            task_id = row['versions'][0]['taskId']
            if conflict == 'shared-task':
                other = videos.store.get(row['id'])
                videos.store.save({**other, 'id': 'other-live-record', 'recordId': 'other-inspection'})
            else:
                def invalid_result_total(request):
                    if request.url.path.endswith('/data-acquisition/task-result'):
                        return platform.answer({'records': [], 'total': -1})
                    return platform.handle(request)

                videos.target.client._transport = httpx.MockTransport(invalid_result_total)
            operation = videos.queue_video_operation(row['id'], 'terminate-capture', {
                'version': 0, 'expectedRevision': 0, 'requestId': conflict})
            await asyncio.gather(*tuple(videos.operation_tasks.values()))
            saved = videos.store.get_operation(operation['operationId'])
            assert saved['state'] in {'attention', 'conflict'}
            assert ('分页' if conflict == 'invalid-pagination' else '其他视频记录') in saved['message']
            assert not platform.stop_calls and task_id in platform.parents
            assert not videos.store.get(row['id']).get('deletedAt')
    finally:
        await close_app(app)


@pytest.mark.asyncio
async def test_delayed_stop_and_delete_are_polled_without_repeating_writes(app_factory, platform):
    app = app_factory()
    videos = app.state.videos
    platform.new_tasks_waiting = True
    writes = []
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://127.0.0.1') as client:
            row = await new_video(client, app)
            task_id = row['versions'][0]['taskId']

            def delayed_platform(request):
                if request.url.path.endswith(('/data-acquisition/end-task', '/data-acquisition/delete')):
                    writes.append(request.url.path.rsplit('/', 1)[1])
                    return httpx.Response(503)  # The operation takes effect after the lost response.
                return platform.handle(request)

            videos.target.client._transport = httpx.MockTransport(delayed_platform)
            queued = videos.queue_video_operation(row['id'], 'terminate-capture', {
                'version': 0, 'expectedRevision': 0, 'requestId': 'delayed'})
            await asyncio.gather(*tuple(videos.operation_tasks.values()))
            operation_id = queued['operationId']

            async def poll():
                operation = videos.store.get_operation(operation_id)
                operation['nextAttemptAt'] = 0
                videos.store.save_operation(operation)
                await videos.resume_operations()
                await asyncio.gather(*tuple(videos.operation_tasks.values()))
                return videos.store.get_operation(operation_id)['state']

            assert await poll() == 'reconciling' and writes == ['end-task']
            platform.parents[task_id]['taskStatus'] = 'ENDED'
            assert await poll() == 'waiting_connection' and writes == ['end-task', 'delete']
            assert await poll() == 'reconciling' and writes == ['end-task', 'delete']
            del platform.parents[task_id]
            assert await poll() == 'complete' and writes == ['end-task', 'delete']
    finally:
        await close_app(app)


@pytest.mark.asyncio
async def test_terminated_capture_can_be_submitted_again_from_the_same_date_plan(app_factory, platform):
    app = app_factory()
    platform.new_tasks_waiting = True
    try:
        manager, run = await queried(app)
        plan = manager.review.get(run)
        manager.review.submit(run, {'recordIds': plan['readyIds'], 'expectedPlanRevision': 0, 'requestId': 'first'})
        await manager.tick()
        await asyncio.gather(*tuple(app.state.jobs.running.values()))
        await app.state.videos.check_once()
        await manager.tick()
        row, = app.state.videos.store.list()
        result = app.state.videos.queue_video_operation(row['id'], 'terminate-capture', {
            'version': 0, 'expectedRevision': 0, 'requestId': 'stop'})
        await asyncio.gather(*tuple(app.state.videos.operation_tasks.values()))
        assert app.state.videos.store.get_operation(result['operationId'])['state'] == 'complete'
        await manager.tick()
        plan = manager.review.get(run)
        assert plan['selectableIds'] and plan['tasks'][0]['status'] == 'ready'
        manager.review.submit(run, {'recordIds': plan['selectableIds'], 'expectedPlanRevision': plan['planRevision'],
                                    'requestId': 'second'})
        await manager.tick()
        await asyncio.gather(*tuple(app.state.jobs.running.values()))
        assert platform.batch_sizes == [1, 1]
        assert len(platform.parents) == 1 and row['versions'][0]['taskId'] not in platform.parents
    finally:
        await close_app(app)
