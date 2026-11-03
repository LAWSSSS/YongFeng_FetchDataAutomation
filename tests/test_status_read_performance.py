"""Status polling must not repeatedly parse all media/receipt history."""
from pathlib import Path

import pytest

from app.main import create_app
from tests.test_slicing_performance import close_app


@pytest.mark.asyncio
async def test_workspace_counts_classify_each_registered_path_once(tmp_path, monkeypatch):
    app = create_app(data_dir=tmp_path / 'db')
    workspace = app.state.workspace
    root = tmp_path / '视频工作区'
    workspace.config.update(enabled=True, root=str(root))
    version = {'size': 20, 'device': 1, 'inode': 2, 'modifiedNs': 3}
    workspace.save('assets', {'id': 'a', 'name': 'sample.mp4', 'path': str(root / '采集视频' / 'sample.mp4'),
                              'folder': 'sample', 'fileVersion': version, 'status': 'managed'})
    entries = [{'path': str(root / '车斗已切片' / 'sample' / '图片' / f'{n}.jpg'),
                'expected': {'size': 7}} for n in range(1000)]
    entries += [entries[0], {'path': str(root / '车斗已切片' / 'sample' / '压缩包.zip'), 'expected': {'size': 9}},
                {'path': str(tmp_path / '视频工作区-other' / '采集视频' / 'outside.mp4'), 'expected': {'size': 999}}]
    monkeypatch.setattr(workspace, 'media_entries', lambda *args, **kwargs: entries)
    calls = []
    original = Path.is_relative_to
    monkeypatch.setattr(Path, 'is_relative_to', lambda self, other: (calls.append(1), original(self, other))[1])
    snapshot = workspace.state()
    branches = {branch['name']: branch for branch in snapshot['branches']}
    assert branches['车斗已切片']['files'] == 1001
    assert branches['车斗已切片']['bytes'] == 7009
    assert snapshot['totalBytes'] == 7009
    assert branches['采集视频']['files'] == 0  # inventory, not an invented disk scan
    assert len(calls) < 10  # not one ancestor walk per photo per branch
    await close_app(app)


@pytest.mark.asyncio
async def test_asset_lookup_uses_exact_indexed_path_without_reloading_inventory(tmp_path, monkeypatch):
    app = create_app(data_dir=tmp_path / 'db')
    workspace = app.state.workspace
    path = tmp_path / 'with spaces' / '视频.mp4'
    for index in range(80):
        workspace.save('assets', {'id': str(index), 'path': str(path) if index in (2, 3) else str(tmp_path / str(index))})
    original = workspace.rows
    monkeypatch.setattr(workspace, 'rows', lambda kind: pytest.fail('must not reload assets') if kind == 'assets' else original(kind))
    assert workspace.asset_for(path)['id'] == '2'  # preserve first-row semantics on legacy duplicates
    assert workspace.asset_for(str(path) + '.other') is None
    await close_app(app)


@pytest.mark.asyncio
async def test_receipt_fallback_reads_only_exact_request_runs_and_rejects_ambiguity(tmp_path, monkeypatch):
    app = create_app(data_dir=tmp_path / 'db')
    store = app.state.slicing.store
    views = app.state.slicing.submission_views
    store.save('runs', {'id': 'one', 'requestId': 'matched', 'items': ['bound-id']})
    for index in range(30):
        store.save('runs', {'id': 'history-' + str(index), 'requestId': 'unrelated-' + str(index),
                            'items': [], 'unused': ['large history'] * 500})
    original = store.list
    monkeypatch.setattr(store, 'list', lambda kind: pytest.fail('must not load all runs') if kind == 'runs' else original(kind))
    assert views._slice_scope({'detail': {'requestId': 'matched'}}) == (['bound-id'], 'one', '')
    store.save('runs', {'id': 'two', 'requestId': 'matched', 'items': ['different-id']})
    result = views._slice_scope({'detail': {'requestId': 'matched'}})
    assert result[0] == [] and result[1] is None and result[2]
    assert views._slice_scope({'detail': {'requestId': 'unknown'}})[0] == []
    # Explicit immutable scope still takes precedence over request history.
    assert views._slice_scope({'detail': {'itemIds': ['explicit'], 'runId': 'one'}}) == (['explicit'], 'one', '')
    await close_app(app)
