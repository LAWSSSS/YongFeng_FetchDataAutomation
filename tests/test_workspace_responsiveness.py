"""Archive bookkeeping must not monopolize the website's event loop."""
import asyncio
import time

import httpx
import pytest

from app.main import create_app
from tests.test_attention_presentation import recovery_pair
from tests.test_slicing_performance import close_app
from tests.test_workspace import pair


@pytest.mark.asyncio
async def test_asset_projection_never_reloads_global_history_and_rechecks_receipts(tmp_path, monkeypatch):
    app = create_app(data_dir=tmp_path / 'db')
    path = tmp_path / 'source.mp4'
    path.write_bytes(b'isolated-source')
    try:
        old = recovery_pair(app, path)
        workspace = app.state.workspace
        await workspace.discover()
        asset = workspace.rows('assets')[0]
        monkeypatch.setattr(app.state.media_control, 'effective_items',
                            lambda *_: pytest.fail('A single asset must not load the global projection.'))
        monkeypatch.setattr(app.state.slicing.packages.store, 'list',
                            lambda *_: pytest.fail('A single asset must not load every photo package.'))
        assert {item['id'] for item in workspace.effective_links(asset)} == {'recovered'}
        package = app.state.slicing.packages.store.get('items', 'pack-bed')
        package['verifiedJobId'] = 'wrong-job'
        app.state.slicing.packages.store.save('items', package)
        assert old['id'] in {item['id'] for item in workspace.effective_links(asset)}
        assert path.exists()
    finally:
        await close_app(app)


@pytest.mark.asyncio
async def test_photo_plan_reads_each_package_once_instead_of_once_per_image(tmp_path, monkeypatch):
    app = create_app(data_dir=tmp_path / 'db')
    workspace = app.state.workspace
    try:
        path = pair(workspace, tmp_path / 'original')
        await workspace.discover()
        owner = workspace.linked(workspace.rows('assets')[0])[0]
        names = [f'frame-{index}.jpg' for index in range(600)]
        workspace.slicing.packages.store.save('items', {
            'id': 'photos', 'itemId': owner['id'], 'category': 'bed', 'state': 'queued',
            'path': str(tmp_path / 'pictures.zip'), 'fileVersion': {'size': 1},
            'imageDir': str(tmp_path / 'pictures'), 'images': names,
            'imageVersions': {name: {'size': 1} for name in names},
        })
        monkeypatch.setattr(workspace, 'stage', lambda _: '未切片')
        monkeypatch.setattr(workspace, 'photo_results_verified', lambda _: False)
        original = workspace.slicing.packages.store.get
        reads = []

        def get(kind, identity):
            reads.append(identity)
            return original(kind, identity)

        monkeypatch.setattr(workspace.slicing.packages.store, 'get', get)
        entries = workspace.plan_assets(tmp_path / 'destination', checkpoints=False)
        assert sum(entry['kind'] == 'image' for entry in entries) == 600
        assert len(reads) <= 1
        assert path.exists()
    finally:
        await close_app(app)


@pytest.mark.asyncio
async def test_status_reads_run_between_assets_during_background_arrangement(tmp_path, monkeypatch):
    app = create_app(data_dir=tmp_path / 'db')
    workspace = app.state.workspace
    workspace.config.update(enabled=True, root=str(tmp_path))
    assets = [dict(id=str(index), name=str(index), path=str(tmp_path / f'{index}.mp4'),
                   fileVersion={'size': 1}, status='managed') for index in range(50)]
    entered = asyncio.Event()

    async def no_discovery(**_):
        pass

    async def available_root():
        return tmp_path

    async def synchronous_bookkeeping(asset):
        entered.set()
        time.sleep(.02)  # Simulate CPU/SQLite work which contains no I/O await.
        return asset

    monkeypatch.setattr(workspace, 'check_root', available_root)
    monkeypatch.setattr(workspace, 'discover', no_discovery)
    monkeypatch.setattr(workspace, '_targets', lambda: assets)
    monkeypatch.setattr(workspace, '_reconcile_asset', synchronous_bookkeeping)
    processing = None
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                                     base_url='http://127.0.0.1') as client:
            started = time.monotonic()

            async def read():
                await entered.wait()
                response = await client.get('/api/health')
                return response, time.monotonic() - started

            probe = asyncio.create_task(read())
            processing = asyncio.create_task(workspace.reconcile(cleanup=False))
            response, elapsed = await asyncio.wait_for(probe, 3)
            assert response.status_code == 200
            assert elapsed < .5
            assert not processing.done(), 'The website must respond before the batch finishes.'
            await processing
    finally:
        if processing:
            await asyncio.gather(processing, return_exceptions=True)
        await close_app(app)
