"""A rejected resume must leave all previously paused stages paused."""
import asyncio
import copy

import pytest
import pytest_asyncio

from app.main import create_app
from app.videos import Conflict
from tests.test_media_control import sliced
from tests.test_slicing_continue import setup_run
from tests.test_slicing_performance import close_app


@pytest_asyncio.fixture
async def paused_app(tmp_path, monkeypatch):
    app = create_app(data_dir=tmp_path / 'db')
    manager = setup_run(app, enabled=False)
    monkeypatch.setattr(manager, '_wake_scheduler', lambda: None)
    path = tmp_path / 'source.mp4'; path.write_bytes(b'isolated-video')
    sliced(app, path, identity='source', state='pending')
    manager.run['items'] = ['source']
    manager.store.save('runs', manager.run)
    manager.packages.pause()
    pipeline = app.state.pipeline
    app.state.workspace.config.update(enabled=True, root=str(tmp_path / 'workspace'))
    pipeline.config.update(enabled=True, paused=True, pausedStages={'slicing': True, 'packages': True})
    pipeline.save('control', pipeline.config)
    yield app
    await pipeline.shutdown()
    await app.state.workspace.shutdown()
    await close_app(app)


@pytest.mark.asyncio
@pytest.mark.parametrize('action', ['start', 'resume'])
@pytest.mark.parametrize('problem', ['settings_changed', 'starting', 'stopping'])
async def test_rejected_resume_has_no_partial_side_effects(paused_app, problem, action):
    app = paused_app
    pipeline, manager = app.state.pipeline, app.state.slicing
    if problem == 'settings_changed':
        manager.store.settings['revision'] += 1
        manager.store.save_settings()
    elif problem == 'starting':
        await manager.start_lock.acquire()
    else:
        manager.scan_task = asyncio.create_task(asyncio.Event().wait())
    baseline = copy.deepcopy(pipeline.config)
    persisted = pipeline.get('control', 'control')
    download_events = pipeline.rows('events')
    try:
        with pytest.raises(Conflict):
            pipeline.change(action, {'requestId': 'rejected-resume', 'expectedRevision': baseline['revision']})
        assert pipeline.config == baseline
        assert pipeline.get('control', 'control') == persisted
        assert pipeline.rows('events') == download_events
        assert not pipeline.active()
        assert manager.packages.paused
        assert not manager.store.settings['enabled']
        assert not pipeline.get('requests', 'rejected-resume')
    finally:
        if manager.start_lock.locked():
            manager.start_lock.release()


@pytest.mark.asyncio
async def test_valid_resume_restores_owned_stages_from_persisted_run(paused_app):
    pipeline, manager = paused_app.state.pipeline, paused_app.state.slicing
    manager.run = None  # A new backend process may not have cached the run yet.
    result = pipeline.change('resume', {'requestId': 'valid-resume', 'expectedRevision': 0})
    assert result['enabled'] and not result['paused']
    assert manager.store.settings['enabled'] and not manager.packages.paused
    assert pipeline.get('control', 'control')['revision'] == 1
    assert pipeline.change('resume', {'requestId': 'valid-resume', 'expectedRevision': 0})['revision'] == 1


@pytest.mark.asyncio
async def test_resume_preserves_a_preexisting_manual_photo_pause(paused_app):
    pipeline, manager = paused_app.state.pipeline, paused_app.state.slicing
    pipeline.config['pausedStages']['packages'] = False
    pipeline.save('control', pipeline.config)
    pipeline.change('resume', {'requestId': 'local-only-resume', 'expectedRevision': 0})
    assert pipeline.active() and manager.store.settings['enabled']
    assert manager.packages.paused


@pytest.mark.asyncio
async def test_replayed_batch_normalizes_duplicate_outcomes_without_repeating_work(paused_app, monkeypatch):
    pipeline = paused_app.state.pipeline
    calls = []
    async def unexpected(*args, **kwargs):
        calls.append(args)
        raise AssertionError('Completed work must not run again')
    monkeypatch.setattr(pipeline.control, 'continue_safe', unexpected)
    pipeline.save('operations', {'id': 'old-operation', 'state': 'queued', 'automatic': False,
        'selections': [{'mediaId': 'source', 'generation': 0}],
        'outcomes': [{'mediaId': 'source', 'ok': True}, {'mediaId': 'source', 'ok': True}]})
    await pipeline._run_operation('old-operation')
    result = pipeline.operation('old-operation')
    assert result['state'] == 'complete' and result['finished'] == result['total'] == 1
    assert calls == []
