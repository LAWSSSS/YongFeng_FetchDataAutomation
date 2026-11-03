"""One physical decoder never bypasses another category's recovery deadline."""
import copy

import pytest

from app.main import create_app
from tests.test_media_control import sliced
from tests.test_slicing_performance import close_app


@pytest.mark.asyncio
async def test_shared_analysis_does_not_start_or_rewrite_a_deferred_model(tmp_path, monkeypatch):
    app = create_app(data_dir=tmp_path / 'db')
    manager = app.state.slicing
    path = tmp_path / 'video.mp4'; path.write_bytes(b'isolated-video')
    bed = sliced(app, path, 'bed', '27912')
    magnet = sliced(app, path, 'magnet', '27944')
    bed['metadata'] = {'duration': 30, 'frameRate': 25}
    magnet['categories']['magnet'].update(retryNotBefore=1100, automaticRetries=1)
    manager.store.save('items', bed); manager.store.save('items', magnet)
    manager.store.settings['targetDatasetIds'] = {'27912': '27892', '27944': '27893'}
    manager.clock = lambda: 1000
    manager.control.refresh()
    bed = manager.store.get('items', 'bed'); magnet = manager.store.get('items', 'magnet')
    before = copy.deepcopy(magnet)
    monkeypatch.setattr(manager, '_model_fingerprint', lambda name: 'fake-' + name)
    requested = []
    async def worker(item, metadata, points, keys, phase):
        requested.append(sorted(points))
        raise ValueError('frame_timeout')
    monkeypatch.setattr(manager, '_run_worker', worker)
    await manager._process_shared(bed, [bed, magnet])
    assert requested == [['bed']]
    assert manager.store.get('items', 'magnet') == before
    assert manager.store.get('items', 'bed')['categories']['bed']['automaticRetries'] == 1
    await close_app(app)


@pytest.mark.asyncio
async def test_both_models_share_frames_once_their_deadlines_have_passed(tmp_path, monkeypatch):
    app = create_app(data_dir=tmp_path / 'db')
    manager = app.state.slicing
    path = tmp_path / 'video.mp4'; path.write_bytes(b'isolated-video')
    bed = sliced(app, path, 'bed', '27912')
    magnet = sliced(app, path, 'magnet', '27944')
    bed['metadata'] = {'duration': 30, 'frameRate': 25}
    bed['categories']['bed']['retryNotBefore'] = 1005
    magnet['categories']['magnet']['retryNotBefore'] = 1015
    manager.store.save('items', bed); manager.store.save('items', magnet)
    manager.store.settings['targetDatasetIds'] = {'27912': '27892', '27944': '27893'}
    manager.clock = lambda: 1020
    manager.control.refresh()
    bed = manager.store.get('items', 'bed'); magnet = manager.store.get('items', 'magnet')
    monkeypatch.setattr(manager, '_model_fingerprint', lambda name: 'fake-' + name)
    requested = []
    async def worker(item, metadata, points, keys, phase):
        requested.append(sorted(points))
        raise ValueError('frame_timeout')
    monkeypatch.setattr(manager, '_run_worker', worker)
    await manager._process_shared(bed, [bed, magnet])
    assert requested == [['bed', 'magnet']]
    assert all(manager.store.get('items', identity)['categories'][identity]['automaticRetries'] == 1
               for identity in ('bed', 'magnet'))
    await close_app(app)
