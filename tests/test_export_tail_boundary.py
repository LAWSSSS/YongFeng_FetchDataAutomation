"""A missing last PTS is not a broken video; real middle faults remain errors."""
import pytest
from app.main import create_app
from tests.test_slicing_performance import close_app


@pytest.mark.asyncio
@pytest.mark.parametrize('timestamp,metadata,code,skip', [
    (600, {'duration': 600.031, 'frameRate': 25.035}, 'decode_unavailable', True),
    (600.031, {'duration': 600.031, 'frameRate': 25.035}, 'decode_unavailable', True),
    (599, {'duration': 600.031, 'frameRate': 25.035}, 'decode_unavailable', False),
    (600.1, {'duration': 600.031, 'frameRate': 25.035}, 'decode_unavailable', False),
    (600, {}, 'decode_unavailable', False),
    (600, {'duration': 600.031, 'frameRate': 25.035}, 'frame_timeout', False),
    (600, {'duration': 600.031, 'frameRate': 25.035}, 'frame_io_error', False),
    (600, {'duration': 600.031, 'frameRate': 25.035}, 'frame_tool_error', False),
])
async def test_only_the_nonexistent_eof_grid_point_is_skipped(tmp_path, timestamp, metadata, code, skip):
    app = create_app(data_dir=tmp_path / 'db')
    manager = app.state.slicing
    def failed(*_):
        raise RuntimeError(code)
    manager.slice_reader = failed
    item = {'path': str(tmp_path / 'video.mp4'), 'metadata': metadata}
    try:
        if skip:
            assert await manager._read_export_frame(item, timestamp) is None
            assert not item.get('partialRecovery')  # No false corruption/recovery claim.
        else:
            with pytest.raises(RuntimeError, match='frame_extract_failed' if code == 'decode_unavailable' else code):
                await manager._read_export_frame(item, timestamp)
    finally:
        await close_app(app)


@pytest.mark.asyncio
async def test_a_real_decodable_tail_picture_is_kept(tmp_path):
    app = create_app(data_dir=tmp_path / 'db')
    manager = app.state.slicing
    image = b'\xff\xd8real-tail-image\xff\xd9'
    manager.slice_reader = lambda *_: image
    try:
        item = {'path': str(tmp_path / 'video.mp4'),
                'metadata': {'duration': 600.031, 'frameRate': 25.035}}
        assert await manager._read_export_frame(item, 600) == image
    finally:
        await close_app(app)
