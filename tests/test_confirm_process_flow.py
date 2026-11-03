"""Real backend stages with simulated capture, inference and import transports.

No production connection or external file is used. HTTP APIs, download bytes,
interval computation, photo/ZIP files, queue state and receipt checks are real.
"""
import asyncio
import io
import zipfile
from pathlib import Path

import httpx
from PIL import Image
import pytest

from tests.test_simple_flow import app_factory, platform
from tests.test_slicing_performance import close_app
from tests.test_slice_packages import Remote, ImmediateWorker
from tests.test_workspace import enable


class PhotoPlatform(Remote):
    def __init__(self, packages, fault=''):
        super().__init__()
        self.packages, self.fault = packages, fault
        self.imported = {}

    async def submit(self, dataset, keys):
        identity = await super().submit(dataset, keys)
        package = next(row for row in self.packages.store.list('items')
                       if (row.get('object') or {}).get('key') == keys[0])
        # Import reads the actual generated ZIP rather than fabricating its count.
        with zipfile.ZipFile(package['path']) as archive:
            names = [Path(name).name for name in archive.namelist() if name.startswith('images/')]
        assert names == package['images']
        images = [{'id': f'{identity}-image-{i}', 'name': name, 'taskId': identity}
                  for i, name in enumerate(names)]
        if dataset == '27892' and self.fault == 'missing':
            images = images[:-1]
        if dataset == '27892' and self.fault == 'wrong-job':
            for image in images:
                image['taskId'] = 'another-job'
        self.imported[dataset] = images
        self.jobs_by_dataset[dataset][-1]['status'] = '3'
        return identity

    async def images(self, dataset, *, prefix):
        return list(self.imported.get(dataset, []))


@pytest.mark.asyncio
@pytest.mark.parametrize('fault', ['', 'missing', 'wrong-job'])
async def test_confirm_runs_every_stage_without_extra_start_and_retains_until_manual_cleanup(
        app_factory, tmp_path, monkeypatch, fault):
    app = app_factory()
    videos, slicing, workspace = app.state.videos, app.state.slicing, app.state.workspace
    try:
        await enable(workspace, tmp_path)
        slicing.auth.access = 'isolated-test'
        slicing.store.settings.update(localExport=True, reviewBeforeUpload=False,
            targetDatasetIds={'27912': '27892', '27944': '27893'})
        slicing.store.save_settings()
        slicing.packages.worker = ImmediateWorker()
        slicing.packages.remote = PhotoPlatform(slicing.packages, fault)
        clock = [1000.]
        slicing.packages.clock = lambda: clock[0]
        # Deterministic inference transport; orchestration and interval logic stay real.
        metadata = {'duration': 3.0, 'frameRate': 25, 'width': 3840, 'height': 2160}
        monkeypatch.setattr('app.slicing.ffprobe_file', lambda *_: dict(metadata))
        monkeypatch.setattr(slicing, '_model_fingerprint', lambda name: 'isolated-' + name)
        monkeypatch.setattr(slicing, '_wake_scheduler', lambda: None)
        image = io.BytesIO()
        Image.new('RGB', (16, 9), color='green').save(image, format='JPEG')
        slicing.slice_reader = lambda *_: image.getvalue()
        slicing.magnet_classifier = lambda _blob: 'target'
        videos.ffprobe = lambda _path: asyncio.sleep(0, result=3600.0)

        async def worker(item, meta, points, keys, phase):
            for category, positions in points.items():
                for position in positions:
                    slicing.store.save('observations', {'id': keys[category][position], 'time': position,
                        'label': 'target', 'score': .99, 'actualTime': position, 'attempts': 1})

        monkeypatch.setattr(slicing, '_run_worker', worker)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://127.0.0.1:8799') as client:
            created = await client.post('/api/collection-runs', json={
                'filters': {'dateFrom': '2026-09-30', 'dateTo': '2026-09-30'}, 'requestId': 'flow-date'})
            assert created.status_code == 202, created.text
            run_id = created.json()['id']
            for _ in range(6):
                await app.state.collection_runs.tick()
                planned=app.state.collection_runs.get(run_id)
                if planned['status']=='awaiting_review':
                    preview=app.state.collection_runs.review.get(run_id)
                    if preview['readyIds']:
                        app.state.collection_runs.review.submit(run_id,{'recordIds':preview['readyIds'],'expectedPlanRevision':preview['planRevision'],'requestId':'explicit-test-approval'})
                if app.state.jobs.running:
                    await asyncio.gather(*tuple(app.state.jobs.running.values()))
                await videos.check_once()
                if app.state.collection_runs.get(run_id)['status'] == 'complete':
                    break
            row = videos.store.list()[0]
            assert row['versions'][0]['state'] == 'ready'
            assert not row['versions'][0].get('downloadRequested')
            assert not app.state.pipeline.active()
            response = await client.post('/api/videos/' + row['id'] + '/confirm-and-process', json={
                'version': 0, 'expectedRevision': row['revision'], 'requestId': 'flow-confirm'})
            assert response.status_code == 202, response.text
            operation_id = response.json()['operationId']
            assert not slicing.packages.paused
            for _ in range(50):
                await app.state.final_processing.tick()
                if videos.running:
                    await asyncio.gather(*tuple(videos.running.values()))
                await workspace.discover()
                await app.state.pipeline.tick()
                await workspace._auto_start_pipeline()
                await workspace._advance_pipeline()
                await slicing._tick()
                for tasks in (slicing.analysis_tasks, slicing.export_tasks):
                    if tasks:
                        await asyncio.gather(*tuple(tasks.values()))
                clock[0] += 31
                await slicing.packages.tick()
                for tasks in (slicing.packages.running, slicing.packages.target_tasks):
                    if tasks:
                        await asyncio.gather(*tuple(tasks.values()))
                await workspace.reconcile(cleanup=True)
                await app.state.final_processing.tick()
                packages = slicing.packages.store.list('items')
                state = app.state.final_processing.operation(operation_id)
                if (not fault and state['stage'] == 'awaiting_cleanup') or (fault and any(
                        package.get('state') == 'attention' for package in packages)):
                    break
                await asyncio.sleep(.01)
            packages = slicing.packages.store.list('items')
            state = (await client.get('/api/final-processing/' + operation_id)).json()
            assert len(packages) == 2, {'items': slicing.store.list('items'), 'operation': state}
            assert len(slicing.packages.remote.submitted) == 2
            assert not app.state.uploads.store.list('items')
            current = videos.store.get(row['id'])
            assert Path(current['versions'][0]['filePath']).is_file()
            if fault:
                assert state['stage'] != 'awaiting_cleanup'
                assert workspace.cleanup_summary()['eligibleCount'] == 0
                assert any(package['state'] == 'attention' for package in packages)
                assert any(package['state'] == 'complete' for package in packages)
            else:
                assert state['stage'] == 'awaiting_cleanup', state
                assert state['state'] == 'complete'
                assert {package['state'] for package in packages} == {'complete'}
                assert all(package['actualImages'] == package['imageCount']
                           and package['verifiedJobId'] == package['jobId'] for package in packages)
                assert workspace.cleanup_summary()['eligibleCount'] == 1
                assert workspace.rows('requests') and not any(
                    request.get('kind') == 'cleanup-completed' for request in workspace.rows('requests'))
    finally:
        await app.state.collection_runs.shutdown()
        await app.state.final_processing.shutdown()
        await app.state.pipeline.shutdown()
        await workspace.shutdown()
        await close_app(app)


@pytest.mark.asyncio
async def test_correction_preempts_final_confirmed_recovery_and_does_not_confirm_new_version(
        app_factory, tmp_path, monkeypatch):
    from tests.test_simple_flow import new_video
    from tests.test_media_control import sliced
    app = app_factory()
    started = asyncio.Event()
    try:
        await enable(app.state.workspace, tmp_path)
        monkeypatch.setattr(app.state.slicing, '_wake_scheduler', lambda: None)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://127.0.0.1:8799') as client:
            row = await new_video(client, app)
            approved = await client.post('/api/videos/' + row['id'] + '/confirm-and-process', json={
                'version': 0, 'expectedRevision': 0, 'requestId': 'approved-before-recovery'})
            assert approved.status_code == 202
            path = app.state.workspace.root / '采集视频' / 'old' / 'old.mp4'
            path.parent.mkdir(); path.write_bytes(b'old-version')
            from app.slicing import source_version
            current = app.state.videos.store.get(row['id'])
            current['versions'][0].update(state='failed', downloadRequested=True, errorCode='media_incomplete',
                recovery={'state': 'slicing', 'filePath': str(path), 'fileVersion': source_version(path),
                          'slicingItemId': 'old-recovery'})
            app.state.videos.store.save(current)
            item = sliced(app, path, identity='old-recovery', state='analyzing')
            item.update(collectionRecovery=True, collectionVideoId=row['id'], collectionRevision=0,
                        mediaId='collection:' + row['id'], mediaGeneration=0)
            app.state.slicing.store.save('items', item)
            app.state.media_control.refresh()

            async def analysis():
                with app.state.slicing.leases.read(path):
                    started.set()
                    await asyncio.Event().wait()

            worker = asyncio.create_task(analysis())
            app.state.slicing.analysis_tasks[item['id']] = worker
            await started.wait()
            corrected = await client.post('/api/videos/' + row['id'] + '/corrections', json={
                'startTime': '2026-09-30 02:00:00', 'durationSeconds': 3600,
                'expectedRevision': 0, 'requestId': 'correct-after-confirm'})
            assert corrected.status_code == 202, corrected.text
            await asyncio.gather(*tuple(app.state.videos.operation_tasks.values()))
            operation = app.state.videos.store.get_operation(corrected.json()['operationId'])
            assert operation['state'] == 'complete', operation
            assert worker.cancelled()
            assert app.state.slicing.store.get('items', 'old-recovery')['superseded']
            newer = app.state.videos.store.get(row['id'])
            assert newer['activeRevision'] == 1
            assert not newer['versions'][1].get('finalConfirmation')
            assert not newer['versions'][1].get('downloadRequested')
            old_approval = app.state.final_processing.get(approved.json()['operationId'])
            await app.state.final_processing.advance(old_approval)
            assert app.state.final_processing.operation(old_approval['id'])['state'] == 'superseded'
            assert path.is_file()
    finally:
        await app.state.final_processing.shutdown()
        await app.state.pipeline.shutdown()
        await app.state.workspace.shutdown()
        await close_app(app)
