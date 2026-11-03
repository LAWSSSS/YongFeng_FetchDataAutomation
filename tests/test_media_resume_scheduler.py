"""Resuming the last paused source must restart its exited dispatcher."""
import asyncio
import copy
import hashlib
import json

import pytest

from app.main import create_app
from app.videos import Conflict
from tests.test_media_control import sliced
from tests.test_slicing_continue import failed, setup_run
from tests.test_slicing_performance import close_app


@pytest.mark.asyncio
@pytest.mark.parametrize('finished_scheduler', [False, True])
async def test_resume_restarts_dispatcher_and_keeps_other_source_paused(tmp_path, monkeypatch, finished_scheduler):
    app = create_app(data_dir=tmp_path / 'db')
    manager = setup_run(app, enabled=False)
    control = app.state.media_control
    try:
        for name in ('one', 'other'):
            path = tmp_path / (name + '.mp4')
            path.write_bytes(name.encode())
            sliced(app, path, name, state='pending')
        control.refresh()
        one = manager.store.get('items', 'one')
        other = manager.store.get('items', 'other')
        for item in (one, other):
            current = control.get(item['mediaId'])
            current['paused'] = True
            control.save(current)
        if finished_scheduler:
            manager.scheduler_task = asyncio.create_task(asyncio.sleep(0))
            await manager.scheduler_task
        started = asyncio.Event()
        seen = []

        async def dispatch():
            seen.extend(row['id'] for row in manager._scope_run_items(manager.store.list('items')))
            started.set()
            manager.store.settings['enabled'] = False  # Finish this isolated work pass.

        monkeypatch.setattr(manager, '_tick', dispatch)
        body = {'requestId': 'resume-one', 'generation': one['mediaGeneration'],
                'selections': [{'id': one['id'], 'expectedRevision': one['revision']}]}
        await control.pause_media(one['mediaId'], body, paused=False)
        await asyncio.wait_for(started.wait(), timeout=1)
        await manager.scheduler_task
        assert seen == ['one']
        assert control.get(other['mediaId'])['paused']
        task = manager.scheduler_task
        assert (await control.pause_media(one['mediaId'], body, paused=False))['ok']
        assert manager.scheduler_task is task and seen == ['one']
    finally:
        await close_app(app)


@pytest.mark.asyncio
@pytest.mark.parametrize('action', ['resume', 'continue'])
async def test_single_resume_cannot_bypass_global_pause(tmp_path, action):
    app = create_app(data_dir=tmp_path / 'db')
    manager = setup_run(app, enabled=False)
    try:
        path = tmp_path / 'one.mp4'
        path.write_bytes(b'one')
        sliced(app, path, 'one', state='pending')
        control = app.state.media_control
        control.refresh()
        one = manager.store.get('items', 'one')
        current = control.get(one['mediaId'])
        current['paused'] = True
        control.save(current)
        baseline = copy.deepcopy(control.get(one['mediaId']))
        app.state.pipeline.config.update(enabled=True, paused=True)
        with pytest.raises(Conflict, match='全局暂停'):
            body = {'requestId': 'resume-during-pause', 'generation': one['mediaGeneration'],
                    'selections': [{'id': 'one', 'expectedRevision': one['revision']}], 'packages': []}
            if action == 'resume':
                await control.pause_media(one['mediaId'], body, paused=False)
            else:
                await control.continue_safe(one['mediaId'], body)
        assert control.get(one['mediaId']) == baseline
        assert not manager.store.settings['enabled'] and manager.scheduler_task is None
    finally:
        await close_app(app)


@pytest.mark.asyncio
async def test_interrupted_continue_restarts_after_persisted_child_receipt(tmp_path, monkeypatch):
    app = create_app(data_dir=tmp_path / 'db')
    manager = setup_run(app, enabled=False)
    try:
        path = tmp_path / 'one.mp4'
        path.write_bytes(b'one')
        failed(app, path, 'one', '27912')
        control = app.state.media_control
        control.refresh()
        one = manager.store.get('items', 'one')
        body = {'requestId': 'continued-before-crash', 'generation': one['mediaGeneration'],
                'selections': [{'id': 'one', 'expectedRevision': one['revision']}], 'packages': []}
        with monkeypatch.context() as patch:
            patch.setattr(manager, '_wake_scheduler', lambda: None)
            result = await control.continue_safe(one['mediaId'], body)
        assert result['outcomes'][0]['ok']
        key = hashlib.sha256(('continue:' + one['mediaId'] + ':' + body['requestId']).encode()).hexdigest()
        receipt = json.loads(control.db.execute('SELECT payload FROM simple_media_actions WHERE id=?', (key,)).fetchone()[0])
        receipt['state'] = 'running'  # Crash after the child checkpoint, before the parent completes.
        control.db.execute('UPDATE simple_media_actions SET payload=? WHERE id=?', (json.dumps(receipt), key))
        control.db.commit()
        manager.store.settings['enabled'] = False
        revision = manager.store.get('items', 'one')['revision']
        dispatched = asyncio.Event()

        async def tick():
            assert manager.run['items'] == ['one']
            dispatched.set()
            manager.store.settings['enabled'] = False

        monkeypatch.setattr(manager, '_tick', tick)
        await control.continue_safe(one['mediaId'], body)
        await asyncio.wait_for(dispatched.wait(), timeout=1)
        await manager.scheduler_task
        assert manager.store.get('items', 'one')['revision'] == revision
    finally:
        await close_app(app)
