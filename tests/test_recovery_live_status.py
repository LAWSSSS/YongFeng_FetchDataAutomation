"""Recovery status is current, version-bound, and cannot be cancelled by a rejected correction."""
import copy

import pytest

from app.main import create_app
from app.video_filters import record_status
from app.videos import Conflict
from tests.test_media_control import collection
from tests.test_slicing_performance import close_app


def recovering(app, tmp_path):
    path = tmp_path / 'original.mp4'
    path.write_bytes(b'original-preserved')
    row = collection(app, path, accepted=False)
    row['versions'][0].update(state='failed', errorCode='invalid_video', downloadRequested=True,
        recovery={'state': 'slicing', 'slicingItemId': 'recovery', 'message': '旧工具故障'})
    app.state.videos.store.save(row)
    operation = dict(id='current-op', videoId=row['id'], version=0, expectedRevision=0,
                     kind='recover-decodable', state='running', stage='partial_slicing')
    app.state.videos.store.save_operation(operation)
    app.state.slicing.store.save('items', dict(id='recovery', revision=0, state='platform_processing',
        collectionVideoId=row['id'], collectionRevision=0,
        categories={'bed': {'state': 'platform_processing', 'packageId': 'photos'}}))
    app.state.slicing.packages.store.save('items', dict(id='photos', itemId='recovery', state='queued'))
    app.state.slicing.packages.paused = True
    return row, operation


@pytest.mark.asyncio
async def test_current_recovery_is_not_manual_review_and_uses_live_photo_pause(tmp_path):
    app = create_app(data_dir=tmp_path / 'db')
    try:
        row, operation = recovering(app, tmp_path)
        before = copy.deepcopy(app.state.videos.store.get(row['id']))
        state = app.state.videos.state({'status': 'processing'})
        assert state['counts']['review'] == 0 and state['counts']['processing'] == 1
        public = state['videos'][0]
        assert public['operation']['version'] == 0 and public['operation']['expectedRevision'] == 0
        assert public['recovery']['stage'] == 'photos_paused'
        assert '照片上传已暂停' in public['recovery']['message']
        assert public['versions'][0]['state'] == 'failed'
        assert app.state.videos.store.get(row['id']) == before
        operation['state'] = 'paused'
        app.state.videos.store.save_operation(operation)
        assert app.state.videos.state()['counts']['review'] == 1
        app.state.slicing.packages.paused = False
        assert app.state.videos.state()['videos'][0]['recovery']['stage'] == 'photos_processing'
    finally:
        await close_app(app)


@pytest.mark.asyncio
@pytest.mark.parametrize('field,value', [('version', 4), ('expectedRevision', 4)])
async def test_old_recovery_operation_never_owns_current_video_status(tmp_path, field, value):
    app = create_app(data_dir=tmp_path / 'db')
    try:
        row, operation = recovering(app, tmp_path)
        operation[field] = value
        app.state.videos.store.save_operation(operation)
        public = app.state.videos.state()['videos'][0]
        assert public['operation'] is None and record_status(public) == 'review'
    finally:
        await close_app(app)


@pytest.mark.asyncio
@pytest.mark.parametrize('kind', ['correction', 'recapture'])
async def test_invalid_correction_does_not_preempt_working_recovery(tmp_path, monkeypatch, kind):
    app = create_app(data_dir=tmp_path / 'db')
    try:
        row, _ = recovering(app, tmp_path)
        before = copy.deepcopy(app.state.slicing.store.get('items', 'recovery'))
        monkeypatch.setattr(app.state.media_control, 'reserve', lambda *_: pytest.fail('Must reject before taking ownership'))
        with pytest.raises(Conflict, match='先暂停'):
            app.state.videos.queue_video_operation(row['id'], kind,
                dict(requestId='correction', version=0, expectedRevision=0,
                     startTime='2026-10-01 00:20:00', durationSeconds=3600))
        assert app.state.slicing.store.get('items', 'recovery') == before
        assert len(app.state.videos.store.list_operations()) == 1
    finally:
        await close_app(app)
