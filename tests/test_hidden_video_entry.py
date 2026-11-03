import httpx
import pytest

from app.main import create_app
from tests.test_media_control import collection
from tests.test_slicing_performance import close_app


@pytest.mark.asyncio
async def test_explicit_hidden_video_read_keeps_identity_and_hidden_scope(tmp_path):
    app = create_app(data_dir=tmp_path / 'db')
    path = tmp_path / 'one.mp4'; path.write_bytes(b'old')
    other = tmp_path / 'two.mp4'; other.write_bytes(b'new')
    row = collection(app, path, identity='hidden-exact')
    collection(app, other, identity='visible-same-name')
    app.state.videos.record_visibility.hide(app.state.videos.store, 'hide',
                                          [{'id': row['id'], 'expectedRevision': row['revision']}])
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://127.0.0.1') as client:
            listing = (await client.get('/api/videos?compact=true')).json()
            assert [r['id'] for r in listing['videos']] == ['visible-same-name']
            detail = (await client.get('/api/videos/hidden-exact')).json()
            assert detail['recordHidden'] is True and detail['video']['id'] == 'hidden-exact'
            assert detail['video']['versions'][0]['filePath'] == str(path)
            assert 'requests' not in detail['video']
            assert app.state.videos.record_visibility.hidden() == {'hidden-exact'}
            row['deletedAt'] = '2026-11-02T01:00:00Z'
            app.state.videos.store.save(row)
            assert (await client.get('/api/videos/hidden-exact')).status_code == 404
            assert (await client.get('/api/videos/missing')).status_code == 404
    finally:
        await close_app(app)
