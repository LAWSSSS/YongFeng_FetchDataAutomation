"""Migration must not turn retained or detached saved history into new work."""
import copy

import pytest

from tests.test_final_processing import env
from tests.test_media_control import collection, sliced


def saved(app, row):
    current = app.state.videos.store.get(row['id'])
    current.update(acceptedRevision=current['activeRevision'])
    current['versions'][0].update(state='saved', downloadRequested=False)
    app.state.videos.store.save(current)
    return current


def asset(app, row, status='managed', **kwargs):
    version = row['versions'][0]
    result = {'id': 'test-asset', 'path': version['filePath'], 'fileVersion': version['fileVersion'],
              'status': status, **kwargs}
    app.state.workspace.save('assets', result)
    return result


@pytest.mark.asyncio
async def test_migration_skips_unbound_saved_history(env):
    app, row = env
    saved(app, row)
    await app.state.final_processing.adopt_existing()
    assert not app.state.final_processing.rows()
    assert 'finalConfirmation' not in app.state.videos.store.get(row['id'])['versions'][0]


@pytest.mark.asyncio
@pytest.mark.parametrize('kind', ['preserved', 'cleaned'])
async def test_migration_respects_asset_history_without_collection_slice_binding(env, kind):
    app, row = env
    current = saved(app, row)
    value = asset(app, current, status=kind,
                  retention={'preserve': True} if kind == 'preserved' else {},
                  cleanup={'state': 'complete'} if kind == 'cleaned' else {})
    await app.state.final_processing.adopt_existing()
    assert not app.state.final_processing.rows()
    assert app.state.workspace.get('assets', value['id']) == value


@pytest.mark.asyncio
async def test_migration_does_not_use_other_content_with_same_path_as_admission(env):
    app, row = env
    current = saved(app, row)
    value = asset(app, current)
    value['fileVersion'] = {**value['fileVersion'], 'size': value['fileVersion']['size'] + 1}
    app.state.workspace.save('assets', value)
    await app.state.final_processing.adopt_existing()
    assert not app.state.final_processing.rows()


@pytest.mark.asyncio
async def test_migration_preserves_terminal_legacy_local_slice_results(env, monkeypatch):
    app, row = env
    current = saved(app, row)
    value = asset(app, current)
    old = {'id': 'local-legacy', 'state': 'complete', 'mediaId': 'local:old',
           'fileVersion': value['fileVersion'], 'path': value['path']}
    monkeypatch.setattr(app.state.workspace, 'effective_links', lambda _asset: [copy.deepcopy(old)])
    await app.state.final_processing.adopt_existing()
    assert not app.state.final_processing.rows()


@pytest.mark.asyncio
async def test_migration_still_adopts_managed_pending_file_and_explicit_unbound_recovery(env):
    app, row = env
    current = saved(app, row)
    asset(app, current)
    await app.state.final_processing.adopt_existing()
    assert len(app.state.final_processing.rows()) == 1
    current['id'] = 'unbound-recovery'
    version = current['versions'][0]
    version.update(state='failed', filePath='/isolated/unbound-source.mp4',
                   recovery={'state': 'slicing', 'slicingItemId': 'bound-recovery-task'})
    app.state.videos.store.save(current)
    app.state.pipeline.config.pop('finalFlowMigration')
    await app.state.final_processing.adopt_existing()
    assert len(app.state.final_processing.rows()) == 2
