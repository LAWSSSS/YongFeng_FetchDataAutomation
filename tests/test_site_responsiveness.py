"""Real status predicates must stay bounded as archived photo history grows."""
import copy

import pytest

from app.main import create_app
from tests.test_shared_photo_completion import shared_receipts
from tests.test_slicing_performance import close_app


@pytest.mark.asyncio
async def test_archive_status_reads_photo_inventory_once_and_preserves_proofs(tmp_path, monkeypatch):
    app = create_app(data_dir=tmp_path / 'db')
    w = app.state.workspace
    w.config.update(enabled=True, root=str(tmp_path / '视频工作区'))
    assets = []
    for index in range(30):
        owner, mirror, packages = shared_receipts()
        path = str(w.root / '已切片' / str(index) / 'same-name.mp4')
        owner.update(id=f'owner-{index}', path=path, mediaId=f'local:{index}')
        owner.pop('collectionVideoId'); owner.pop('collectionRevision')
        for package in packages:
            old_id = package['id']
            package.update(id=old_id + f'-{index}', itemId=owner['id'])
            owner['categories'][package['category']]['packageId'] = package['id']
            if index == 3 and package['category'] == 'bed':
                package['verifiedJobId'] = 'wrong-job'
            w.slicing.packages.store.save('items', package)
        w.slicing.store.save('items', owner)
        asset = dict(id=str(index), name='same-name.mp4', folder=str(index), path=path,
                     status='managed', fileVersion=copy.deepcopy(owner['fileVersion']))
        if index == 4:
            asset['fileVersion']['size'] += 1  # Same filename is never proof.
        w.save('assets', asset)
        assets.append(asset)
    expected = {asset['id']: w.stage(asset) for asset in assets}
    calls = []
    w.db.set_trace_callback(calls.append)
    try:
        actual = w.stages(assets)
        assert actual == expected
        assert actual['0'] == '已切片'
        assert actual['3'] == '车斗未切片'
        assert actual['4'] == '采集视频'
        assert sum('FROM simple_slice_package_items' in sql for sql in calls) == 1
        # Both full and summary page reads use the same batched evidence.
        monkeypatch.setattr(w, 'effective_links', lambda *_: pytest.fail('per-video reload'))
        calls.clear()
        state = app.state.files.state()
        assert state['counts'] == {'complete': 28, 'waiting': 2, 'attention': 0, 'preserved': 0}
        assert sum('FROM simple_slice_package_items' in sql for sql in calls) == 1
        assert app.state.files.state(summary=True)['counts'] == state['counts']
    finally:
        w.db.set_trace_callback(None)
        await close_app(app)


@pytest.mark.asyncio
async def test_single_archive_plan_never_reads_all_asset_payloads(tmp_path, monkeypatch):
    app = create_app(data_dir=tmp_path / 'db')
    w = app.state.workspace
    root = tmp_path / '视频工作区'
    version = dict(device=1, inode=2, size=3, modifiedNs=4)
    w.save('assets', dict(id='one', path=str(root / '未切片' / 'one.mp4'), name='one.mp4',
                         folder='one', fileVersion=version, status='managed'))
    original = w.rows
    monkeypatch.setattr(w, 'rows', lambda kind: pytest.fail('single plan read entire inventory')
                        if kind == 'assets' else original(kind))
    entries = w.plan_assets(root, only='one', checkpoints=False)
    assert len(entries) == 1 and entries[0]['assetId'] == 'one'
    assert w.plan_assets(root, only='missing', checkpoints=False) == []
    await close_app(app)


@pytest.mark.asyncio
async def test_batched_archive_projection_keeps_verified_recovery_coverage(tmp_path):
    from tests.test_attention_presentation import recovery_pair
    app = create_app(data_dir=tmp_path / 'db')
    path = tmp_path / 'source.mp4'; path.write_bytes(b'fixture-video')
    recovery_pair(app, path)
    w = app.state.workspace
    await w.discover()
    assets = w.rows('assets')
    assert w.stages(assets) == {asset['id']: w.stage(asset) for asset in assets}
    package = w.slicing.packages.store.get('items', 'pack-bed')
    package['verifiedJobId'] = 'unverified'
    w.slicing.packages.store.save('items', package)
    assert w.stages(assets) == {asset['id']: w.stage(asset) for asset in assets}
    assert set(w.stages(assets).values()) != {'已切片'}
    await close_app(app)


@pytest.mark.asyncio
async def test_scheduler_uses_only_live_work_and_never_builds_page_or_archive_history(tmp_path, monkeypatch):
    from tests.test_media_control import sliced
    from tests.test_slicing_continue import failed
    app = create_app(data_dir=tmp_path / 'db')
    control = app.state.media_control
    paths = set()
    for index in range(20):
        path = tmp_path / f'done-{index}.mp4'; path.write_bytes(str(index).encode())
        paths.add(str(path))
        sliced(app, path, identity=f'done-{index}', state='complete')
    path = tmp_path / 'retry.mp4'; path.write_bytes(b'retry-this-version')
    paths.add(str(path))
    item = failed(app, path, 'retry', '27912', 'frame_timeout')
    control.refresh()
    current = app.state.slicing.store.get('items', item['id'])
    expected = control.next_action({'id': current['mediaId'], 'items': [current], 'packages': []})
    monkeypatch.setattr(control, 'state', lambda **_: pytest.fail('scheduler rendered UI history'))
    monkeypatch.setattr(app.state.slicing, 'state', lambda **_: pytest.fail('scheduler rendered slicing page'))
    monkeypatch.setattr(app.state.files, 'state', lambda **_: pytest.fail('scheduler rendered archive history'))
    groups = control.execution_groups(paths)
    assert len(groups) == 1 and groups[0]['nextAction'] == expected
    assert groups[0]['nextAction']['autoEligible']
    assert control.execution_groups(paths - {str(path)}) == []
    paused = control.get(current['mediaId']); paused['paused'] = True; control.save(paused)
    assert control.execution_groups(paths)[0]['nextAction']['action'] == 'resume'
    assert not control.execution_groups(paths)[0]['nextAction'].get('autoEligible')
    await close_app(app)


@pytest.mark.asyncio
async def test_control_view_does_not_render_slicing_dashboard_and_batches_cleanup(tmp_path, monkeypatch):
    from tests.test_pipeline_status_projection import completed_workspace
    app = await completed_workspace(tmp_path)
    control, workspace = app.state.media_control, app.state.workspace
    expected = control.state(include_completed=True)
    assets = workspace.rows('assets')
    decisions = {asset['id']: workspace.cleanup_eligibility(asset) for asset in assets}
    queries = []
    workspace.db.set_trace_callback(queries.append)
    try:
        assert workspace.cleanup_statuses(assets) == decisions
        assert sum('FROM simple_slice_package_items' in query for query in queries) == 1
        monkeypatch.setattr(app.state.slicing, 'state', lambda **_: pytest.fail('unrelated dashboard rendered'))
        monkeypatch.setattr(workspace, 'cleanup_eligibility', lambda *_: pytest.fail('per-file cleanup query'))
        assert control.state(include_completed=True) == expected
        # A changed import receipt affects the very next view; no time-based
        # cache may leave a completed or deletable badge behind.
        package = app.state.slicing.packages.store.list('items')[0]
        package['verifiedJobId'] = 'wrong-task'
        app.state.slicing.packages.store.save('items', package)
        assert not workspace.cleanup_statuses(assets)[assets[0]['id']][0]
    finally:
        workspace.db.set_trace_callback(None)
        await close_app(app)


@pytest.mark.asyncio
async def test_idle_pipeline_does_not_repeatedly_wake_file_arrangement(tmp_path, monkeypatch):
    app = create_app(data_dir=tmp_path / 'db')
    pipeline, workspace = app.state.pipeline, app.state.workspace
    pipeline.config.update(enabled=True, paused=False)
    workspace.config['enabled'] = True
    workspace.online = True
    monkeypatch.setattr(pipeline, '_enroll_downloads', lambda: False)
    workspace.wake.clear()
    await pipeline.tick()
    assert not workspace.wake.is_set()
    monkeypatch.setattr(pipeline, '_enroll_downloads', lambda: True)
    await pipeline.tick()
    assert workspace.wake.is_set(), 'A newly enrolled download still starts arrangement immediately.'
    await close_app(app)


@pytest.mark.asyncio
async def test_background_arrangement_drains_request_callbacks_without_building_history(tmp_path, monkeypatch):
    import asyncio
    import time
    import httpx
    app = create_app(data_dir=tmp_path / 'db')
    workspace = app.state.workspace
    workspace.config.update(enabled=True, root=str(tmp_path))
    assets = [{'id': str(n), 'name': str(n), 'status': 'managed', 'path': str(tmp_path / f'{n}.mp4'),
               'fileVersion': {'size': 1, 'device': 1, 'inode': n, 'modifiedNs': 1}} for n in range(8)]
    callbacks_ready = asyncio.Event()
    visited = []

    async def ready(**_):
        return tmp_path

    async def cpu_phase(asset):
        visited.append(asset['id'])
        if len(visited) == 1:
            asyncio.get_running_loop().call_later(.001, callbacks_ready.set)
        time.sleep(.03)
        return asset

    monkeypatch.setattr(workspace, 'check_root', ready)
    monkeypatch.setattr(workspace, 'discover', ready)
    monkeypatch.setattr(workspace, '_targets', lambda: assets)
    monkeypatch.setattr(workspace, '_reconcile_asset', cpu_phase)
    monkeypatch.setattr(workspace, 'state', lambda: pytest.fail('background built full page history'))
    processing = asyncio.create_task(workspace.reconcile(cleanup=False, include_state=False))
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://127.0.0.1') as client:
            await asyncio.wait_for(callbacks_ready.wait(), 2)
            result = await client.get('/api/health')
            assert result.status_code == 200
            assert len(visited) == 1, 'Queued socket/request callbacks run before the next heavy asset.'
        await processing
    finally:
        if not processing.done():
            processing.cancel()
            await asyncio.gather(processing, return_exceptions=True)
        await close_app(app)


@pytest.mark.asyncio
async def test_single_cleanup_reads_only_its_bound_request_history(tmp_path, monkeypatch):
    from app.workspace_cleanup import authorization, registered_cleanup
    app = create_app(data_dir=tmp_path / 'db')
    w = app.state.workspace
    asset = {'id': 'target', 'status': 'managed'}
    for index in range(30):
        w.save('requests', {'id': f'unrelated-{index}', 'kind': 'cleanup-completed', 'state': 'running',
                           'assets': [{'id': f'other-{index}', 'state': 'queued'}]})
    w.save('requests', {'id': 'actual-cleanup', 'kind': 'cleanup-completed', 'state': 'running',
                       'assets': [{'id': 'target', 'state': 'queued'}]})
    original = w.rows
    monkeypatch.setattr(w, 'rows', lambda kind: pytest.fail('unrelated request inventory decoded')
                        if kind == 'requests' else original(kind))
    assert registered_cleanup(w, asset)
    assert not registered_cleanup(w, {'id': 'unknown'})
    assert authorization(w, asset)[0]['id'] == 'actual-cleanup'
    actual = w.get('requests', 'actual-cleanup'); actual['state'] = 'attention'; w.save('requests', actual)
    assert authorization(w, asset) == (None, None)
    assert registered_cleanup(w, asset), 'Rejected history must not become new cleanup permission.'
    await close_app(app)
