import asyncio

import pytest

from app.main import create_app
from tests.test_slicing_performance import close_app


@pytest.mark.asyncio
async def test_finished_upload_releases_its_slot_before_the_done_callback(tmp_path, monkeypatch):
    app = create_app(data_dir=tmp_path / 'db'); packages = app.state.slicing.packages
    first = {'id': 'first', 'dataset': '27892', 'state': 'uploaded'}
    second = {'id': 'second', 'dataset': '27892', 'state': 'queued'}
    packages.store.save('items', first); packages.store.save('items', second)
    finished = asyncio.create_task(asyncio.sleep(0))
    await finished
    packages.running['first'] = finished
    packages.runtime['first'] = {'bytes': 1}
    launched = []
    monkeypatch.setattr(packages, 'launch', lambda item: launched.append(item['id']))
    packages._launch_next('27892', [first, second])
    assert launched == ['second']
    assert 'first' not in packages.running and 'first' not in packages.runtime
    await close_app(app)


@pytest.mark.asyncio
async def test_old_completion_callback_cannot_release_a_new_attempt(tmp_path):
    app = create_app(data_dir=tmp_path / 'db'); packages = app.state.slicing.packages
    old = asyncio.create_task(asyncio.sleep(0)); await old
    release = asyncio.Event()
    current = asyncio.create_task(release.wait())
    packages.running['video'] = current
    packages.runtime['video'] = {'bytes': 42}
    packages.target_next_check['27892'] = 50
    packages._release_upload_task('video', old, '27892')
    assert packages.running['video'] is current
    assert packages.runtime['video']['bytes'] == 42
    assert packages.target_next_check['27892'] == 50
    release.set(); await current
    packages._release_upload_task('video', current, '27892')
    await close_app(app)
