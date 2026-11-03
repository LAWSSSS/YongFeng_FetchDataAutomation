import asyncio

import pytest

from app.main import create_app
from tests.test_attention_presentation import recovery_pair
from tests.test_media_control import sliced
from tests.test_slicing_continue import failed, setup_run
from tests.test_slicing_performance import close_app


@pytest.mark.asyncio
async def test_covered_attempt_outside_the_selected_run_cannot_block_organizing(tmp_path):
    app = create_app(data_dir=tmp_path / 'db')
    path = tmp_path / 'original.mp4'; path.write_bytes(b'original')
    old = recovery_pair(app, path)
    assert app.state.media_control.effective_items([old]) == []
    assert [row['id'] for row in app.state.media_control.effective_items()] == ['recovered']
    workspace = app.state.workspace
    await workspace.discover()
    asset = workspace.rows('assets')[0]
    request = {'id': 'check', 'kind': 'reconcile'}
    workspace.save('requests', request)
    workspace._advance_local_pipeline(request, [{'id': asset['id']}], [])
    assert workspace.get('requests', 'check')['pipelineStage'] == 'organizing'
    assert app.state.slicing.store.get('items', 'old')['state'] == old['state']
    pack = app.state.slicing.packages.store.get('items', 'pack-bed')
    pack['verifiedJobId'] = 'wrong-job'
    app.state.slicing.packages.store.save('items', pack)
    assert app.state.media_control.effective_items([old])
    await close_app(app)


@pytest.mark.asyncio
async def test_offline_continue_uses_known_local_work_without_platform_login(tmp_path, monkeypatch):
    app = create_app(data_dir=tmp_path / 'db')
    path = tmp_path / 'original.mp4'; path.write_bytes(b'original')
    failed(app, path, 'bed', '27912')
    manager = setup_run(app, enabled=False)
    manager.auth.access = ''
    monkeypatch.setattr(manager, '_wake_scheduler', lambda: None)
    control = app.state.media_control; control.refresh()
    item = manager.store.get('items', 'bed')
    result = await control.continue_safe(item['mediaId'], {'requestId': 'offline', 'generation': item['mediaGeneration'],
        'selections': [{'id': item['id'], 'expectedRevision': item['revision']}], 'packages': []})
    assert result['outcomes'][0]['ok']
    assert manager.store.get('items', 'bed')['state'] == 'pending'
    assert not manager.auth.connected
    await close_app(app)


@pytest.mark.asyncio
async def test_media_pause_stops_only_its_worker_and_survives_replay(tmp_path, monkeypatch):
    app = create_app(data_dir=tmp_path / 'db')
    manager = setup_run(app)
    monkeypatch.setattr(manager, '_wake_scheduler', lambda: None)
    for name in ('one', 'other'):
        path = tmp_path / (name + '.mp4'); path.write_bytes(name.encode())
        sliced(app, path, name, state='pending')
    control = app.state.media_control; control.refresh()
    one = manager.store.get('items', 'one')
    other = manager.store.get('items', 'other')
    worker = asyncio.create_task(asyncio.Event().wait())
    unrelated = asyncio.create_task(asyncio.Event().wait())
    manager.analysis_tasks.update(one=worker, other=unrelated)
    body = {'requestId': 'pause-one', 'generation': one['mediaGeneration'],
            'selections': [{'id': one['id'], 'expectedRevision': one['revision']}]}
    await control.pause_media(one['mediaId'], body)
    assert worker.cancelled() and not unrelated.done()
    assert control.get(one['mediaId'])['paused']
    assert not control.get(other['mediaId']).get('paused')
    assert (await control.pause_media(one['mediaId'], body))['ok']
    action = control.next_action({'id': one['mediaId'], 'items': [one]})
    assert action['kind'] == 'waiting' and action['action'] == 'resume'
    await close_app(app)


@pytest.mark.asyncio
async def test_exhausted_retry_is_manual_even_if_its_error_was_transient(tmp_path):
    app = create_app(data_dir=tmp_path / 'db')
    path = tmp_path / 'source.mp4'; path.write_bytes(b'video')
    failed(app, path, 'bed', '27912')
    control = app.state.media_control; control.refresh()
    row = app.state.slicing.store.get('items', 'bed')
    group = {'id': row['mediaId'], 'items': [row]}
    assert control.next_action(group)['autoEligible']
    row['automaticRetries'] = 2
    app.state.slicing.store.save('items', row)
    action = control.next_action(group)
    assert action['kind'] == 'manual' and not action['autoEligible']
    assert action['canContinue']
    await close_app(app)


@pytest.mark.asyncio
async def test_incomplete_import_does_not_get_unlimited_automatic_verification(tmp_path):
    app = create_app(data_dir=tmp_path / 'db')
    path = tmp_path / 'source.mp4'; path.write_bytes(b'video')
    row = failed(app, path, 'bed', '27912', 'verification')
    row['categories']['bed']['packageId'] = 'package'
    app.state.slicing.store.save('items', row)
    pack = {'id': 'package', 'itemId': 'bed', 'category': 'bed', 'state': 'attention',
            'errorCode': 'verification', 'jobId': 'job', 'revision': 1, 'verificationFailures': 2}
    app.state.slicing.packages.store.save('items', pack)
    control = app.state.media_control; control.refresh()
    row = app.state.slicing.store.get('items', 'bed')
    action = control.next_action({'id': row['mediaId'], 'items': [row], 'packages': [pack]})
    assert action['kind'] == 'manual' and not action['autoEligible']
    assert action['canContinue'] and action['packages'][0]['id'] == 'package'
    await close_app(app)
