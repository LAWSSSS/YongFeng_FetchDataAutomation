"""Recover interrupted photo reads without replaying completed categories."""
import asyncio
import copy
import threading
from types import SimpleNamespace

import pytest

from app.frame_decode import DecodedPosition, decode_position
from app.main import create_app
from app.slicing import source_version
from tests.test_slicing_performance import close_app


def prepared(tmp_path):
    app = create_app(data_dir=tmp_path / 'db')
    manager = app.state.slicing
    manager.store.settings.update(enabled=True, localExport=True)
    manager.control = None
    manager.clock = lambda: 1000
    path = tmp_path / 'original.mp4'; path.write_bytes(b'bound-original')
    tool = tmp_path / 'ffmpeg'; tool.write_bytes(b'tool'); tool.chmod(0o755)
    manager.store.settings['frameToolPath'] = str(tool)
    item = {'id': 'recovery', 'revision': 1, 'path': str(path), 'fileVersion': source_version(path),
        'name': path.name, 'sourceDatasetId': '27944', 'sourceVideoId': '', 'collectionRevision': 3,
        'mediaGeneration': 3, 'state': 'attention', 'errorCode': 'frame_io_error',
        'metadata': {'duration': 90, 'frameRate': 25, 'streamStartTime': 0},
        'exportDecodeObservations': {'0.0': {'time': 0, 'actualTime': 0, 'code': ''}},
        'categories': {'bed': {'state': 'complete', 'packageId': 'verified-bed', 'actualImages': 150},
            'magnet': {'state': 'attention', 'errorCode': 'frame_io_error', 'automaticRetries': 1,
                       'intervals': [{'start': 0, 'end': 60}], 'intervalSeconds': 30}}}
    manager.store.save('items', item)
    return app, manager, item


@pytest.mark.asyncio
async def test_legacy_export_environment_resumes_only_failed_category(tmp_path, monkeypatch):
    app, manager, item = prepared(tmp_path)
    seen = []
    def decoded(path, tool, position, metadata, **kwargs):
        seen.append(position)
        return DecodedPosition(position, position, b'jpeg', 1)
    monkeypatch.setattr('app.frame_decode.decode_position', decoded)
    try:
        await manager._restore_waiting_exports()
        latest = manager.store.get('items', item['id'])
        assert seen == [30]  # first untested export point, not a fresh model run
        assert latest['categories']['bed'] == item['categories']['bed']
        magnet = latest['categories']['magnet']
        assert magnet['state'] == 'ready' and magnet['automaticRetries'] == 1
        assert latest['exportDecodeObservations'] == item['exportDecodeObservations']
        assert latest['mediaGeneration'] == 3 and latest['collectionRevision'] == 3
        await manager._restore_waiting_exports()
        assert seen == [30]
    finally:
        await close_app(app)


@pytest.mark.asyncio
async def test_persistent_io_waits_without_export_or_retry_budget(tmp_path, monkeypatch):
    app, manager, item = prepared(tmp_path); calls = []
    def broken(*args, **kwargs):
        calls.append(1); raise RuntimeError('frame_io_error')
    monkeypatch.setattr('app.frame_decode.decode_position', broken)
    try:
        await manager._restore_waiting_exports()
        latest = manager.store.get('items', item['id'])
        assert latest['state'] == 'waiting_connection'
        slot = latest['categories']['magnet']
        assert slot['retryNotBefore'] == 1030 and slot['automaticRetries'] == 1
        await manager._restore_waiting_exports()
        await manager._restore_waiting_files()
        assert len(calls) == 1 and manager.store.get('items', item['id'])['state'] == 'waiting_connection'
        assert manager.export_tasks == {}
    finally:
        await close_app(app)


@pytest.mark.asyncio
@pytest.mark.parametrize('change', ['version', 'generation', 'pause', 'receipt', 'identity', 'interval'])
async def test_probe_cannot_overwrite_newer_version_pause_or_receipt(tmp_path, monkeypatch, change):
    app, manager, item = prepared(tmp_path)
    started, release = threading.Event(), threading.Event()
    def probe(*args): started.set(); release.wait(3)
    monkeypatch.setattr(manager, '_probe_export_source', probe)
    task = asyncio.create_task(manager._restore_waiting_exports())
    try:
        await asyncio.to_thread(started.wait, 1)
        current = manager.store.get('items', item['id'])
        if change == 'version': current['fileVersion'] = {'size': 1}
        if change == 'generation': current['mediaGeneration'] += 1
        if change == 'pause': current['pipelinePaused'] = True
        if change == 'receipt': current['categories']['magnet'].update(state='complete', packageId='already-done')
        if change == 'identity': current['collectionRevision'] += 1
        if change == 'interval': current['categories']['magnet']['intervals'] = [{'start': 10, 'end': 20}]
        manager.store.save('items', current)
        expected = copy.deepcopy(current)
        release.set(); await task
        assert manager.store.get('items', item['id']) == expected
    finally:
        release.set(); await task; await close_app(app)


@pytest.mark.asyncio
async def test_source_changed_is_not_resumed_or_converted_to_damage(tmp_path):
    app, manager, item = prepared(tmp_path)
    from pathlib import Path
    Path(item['path']).write_bytes(b'different-media')
    try:
        await manager._restore_waiting_exports()
        result = manager.store.get('items', item['id'])
        assert result['categories']['magnet']['errorCode'] == 'source_changed'
        assert result['state'] == 'attention'
        assert result['categories']['bed'] == item['categories']['bed']
    finally:
        await close_app(app)


@pytest.mark.asyncio
async def test_slow_export_probe_keeps_scheduler_enabled_and_does_not_hold_lock(tmp_path, monkeypatch):
    app, manager, item = prepared(tmp_path)
    manager.run = {'id': 'run', 'state': 'running'}; manager.store.save('runs', manager.run)
    entered, release = threading.Event(), threading.Event()
    def probe(*args): entered.set(); release.wait(3)
    monkeypatch.setattr(manager, '_probe_export_source', probe)
    monkeypatch.setattr(manager, '_schedule_platform_work', lambda: None)
    try:
        await asyncio.wait_for(manager._tick(), .5)
        assert manager.store.settings['enabled']
        assert not manager.tick_lock.locked()
        await asyncio.to_thread(entered.wait, 1)
        await asyncio.wait_for(manager._tick(), .5)
        assert not manager.export_recovery_task.done()
        release.set(); await manager.export_recovery_task
        assert manager.store.get('items', item['id'])['categories']['magnet']['state'] == 'ready'
    finally:
        release.set(); await close_app(app)


@pytest.mark.asyncio
async def test_export_records_exact_failed_position_as_wait_not_attention(tmp_path, monkeypatch):
    app, manager, item = prepared(tmp_path)
    item['categories']['magnet'].update(state='ready', errorCode='')
    manager.store.save('items', item)
    async def failed(current, points):
        current['_exportEnvironmentPosition'] = 30
        raise RuntimeError('frame_io_error')
    monkeypatch.setattr(manager, '_recognized_magnet_frames', failed)
    try:
        await manager._export_category_owned(item, 'magnet')
        result = manager.store.get('items', item['id'])
        slot = result['categories']['magnet']
        assert result['state'] == 'waiting_connection'
        assert slot['exportEnvironmentPosition'] == 30 and slot['retryNotBefore'] == 1030
        assert slot['automaticRetries'] == 1 and slot['waitStage'] == 'export'
        assert result['categories']['bed']['packageId'] == 'verified-bed'
    finally:
        await close_app(app)


@pytest.mark.parametrize('diagnostic,is_environment', [
    (b'Error opening input: Invalid data found when processing input', False),
    (b'Error opening input: End of file', False),
    (b'Error opening input: Input/output error', True),
    (b'Error opening input: Permission denied', True),
])
def test_ffmpeg_generic_open_error_does_not_masquerade_as_filesystem_fault(tmp_path, monkeypatch, diagnostic, is_environment):
    source = tmp_path / 'source'; source.write_bytes(b'bound')
    tool = tmp_path / 'ffmpeg'; tool.write_bytes(b'tool'); tool.chmod(0o755)
    calls = []
    def run(*args, **kwargs):
        calls.append(1); return SimpleNamespace(returncode=1, stdout=b'', stderr=diagnostic)
    monkeypatch.setattr('app.frame_decode.subprocess.run', run)
    if is_environment:
        with pytest.raises(RuntimeError, match='frame_io_error'):
            decode_position(source, tool, 30, {'frameRate': 25}, expected=source_version(source))
        assert len(calls) == 1
    else:
        result = decode_position(source, tool, 30, {'frameRate': 25}, expected=source_version(source))
        assert result.image is None and result.attempts == 2 and result.reason == 'decode_unavailable'
        assert len(calls) == 2
