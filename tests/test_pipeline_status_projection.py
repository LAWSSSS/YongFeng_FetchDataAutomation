"""Compact pipeline reads retain decisions without computing unused buttons."""
import copy

import pytest

from app.main import create_app
from app.workspace_files import create_root
from tests.test_slicing_performance import close_app
from tests.test_workspace import pair, proofs


async def completed_workspace(tmp_path):
    app = create_app(data_dir=tmp_path / 'db')
    workspace = app.state.workspace
    root = tmp_path / 'workspace'
    create_root(root, workspace.config['workspaceId'])
    workspace.config.update(enabled=True, root=str(root), parent=str(tmp_path))
    path = pair(workspace, root / '已切片' / 'sample', ('complete', 'complete'))
    proofs(workspace, path, tmp_path)
    await workspace.discover()
    app.state.media_control.refresh()
    return app


@pytest.mark.asyncio
async def test_pipeline_omits_unused_cleanup_qualification_but_detail_keeps_it(tmp_path, monkeypatch):
    app = await completed_workspace(tmp_path)
    control, workspace = app.state.media_control, app.state.workspace
    calls = []
    original = workspace.cleanup_statuses
    monkeypatch.setattr(workspace, 'cleanup_statuses', lambda assets: (calls.extend(asset['id'] for asset in assets), original(assets))[1])
    try:
        compact = control.state(include_completed=True, include_cleanup_actions=False)
        assert compact['groups'] and compact['groups'][0]['files']
        assert not calls
        expected_actions = [group['nextAction'] for group in compact['groups']]
        detail = control.state(include_completed=True)
        assert calls, 'Only the detailed view needs the cleanup-button reason.'
        assert [group['nextAction'] for group in detail['groups']] == expected_actions
        assert all('cleanupEligible' in file for group in detail['groups'] for file in group['files'])
        assert all('cleanupEligible' not in file for group in compact['groups'] for file in group['files'])
        calls.clear()
        app.state.pipeline.state()
        assert not calls, 'Polling pipeline summary must not repeatedly evaluate deletion receipts.'
    finally:
        await close_app(app)


@pytest.mark.asyncio
async def test_pipeline_pagination_filter_and_actions_match_full_control_projection(tmp_path, monkeypatch):
    app = await completed_workspace(tmp_path)
    pipeline, control = app.state.pipeline, app.state.media_control
    try:
        full = control.state(include_completed=True)['groups']
        for query in ({}, {'status': 'complete'}, {'status': 'attention'}, {'search': 'truck'},
                      {'search': 'no-such-video'}, {'page': 3, 'page_size': 1}):
            with monkeypatch.context() as context:
                context.setattr(pipeline, '_groups', lambda: copy.deepcopy(full))
                expected = pipeline.state(**query)
            assert pipeline.state(**query) == expected
        # A new persisted pause must affect the next read, without a stale TTL.
        identity = full[0]['id']
        row = control.get(identity)
        row['paused'] = True
        control.save(row)
        updated = pipeline.state()
        assert updated['items'][0]['state'] == 'waiting'
        assert updated['items'][0]['nextAction']['action'] == 'resume'
    finally:
        await close_app(app)
