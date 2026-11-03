"""Faulty media positions are isolated without confusing infrastructure faults."""
import shutil
import subprocess
from types import SimpleNamespace

import pytest

from app.frame_decode import decode_position
from app.slicing import source_version
from app.slicing_core import (build_category_result, decode_exclusions, position_excluded,
                              processing_conclusion, record_decode_coverage)
from app.slicing_worker import extract_requested_frames, probe


@pytest.fixture
def source(tmp_path):
    path = tmp_path / 'source.mp4'; path.write_bytes(b'stable-media-bytes')
    tool = tmp_path / 'ffmpeg'; tool.write_text('fixture'); tool.chmod(0o755)
    return path, tool, source_version(path)


def frame(stamp, data=b'\xff\xd8frame\xff\xd9'):
    return SimpleNamespace(returncode=0, stdout=data,
        stderr=f'[Parsed_showinfo_0] n: 0 pts: 0 pts_time:{stamp}'.encode())


def test_5130_timeout_is_bounded_skipped_and_later_samples_continue(source, monkeypatch):
    path, tool, version = source; calls = []
    def run(command, **options):
        if "-version" in command:
            return SimpleNamespace(returncode=0, stdout=b"ffmpeg version fixture", stderr=b"")
        stamp = float(command[command.index('-ss') + 1]); calls.append(stamp)
        assert options['timeout'] == 30
        if stamp == 5130:
            raise subprocess.TimeoutExpired('ffmpeg', 30)
        return frame(stamp)
    monkeypatch.setattr('app.frame_decode.subprocess.run', run)
    metadata = {'duration': 5200, 'frameRate': 25, 'streamStartTime': 0, '_expectedFileVersion': version}
    rows = list(extract_requested_frames(str(path), str(tool), metadata,
        {'bed': [5100, 5130, 5160], 'magnet': [5100, 5130, 5160]}, skip_undecodable=True))
    assert calls == [5100, 5130, 5130, 5160]  # shared decode, not 2 models × calls
    assert rows[1] == (5130, None) and rows[2][1] is not None
    assert metadata['_decodeObservations'][5130]['decodeAttempts'] == 2
    assert metadata['_decodeObservations'][5130]['decodeReason'] == 'frame_timeout'


@pytest.mark.parametrize('failure,code', [('disk', 'frame_io_error'), ('tool', 'frame_tool_error'),
    ('change', 'source_changed'), ('timing', 'frame_timestamp_missing')])
def test_environment_and_identity_faults_cannot_become_empty_success(source, monkeypatch, failure, code):
    path, tool, version = source
    def run(*args, **kwargs):
        if failure == 'tool': raise OSError('unavailable')
        if failure == 'change':
            path.write_bytes(b'changed'); raise subprocess.TimeoutExpired('ffmpeg', 30)
        if failure == 'timing': return SimpleNamespace(returncode=0, stdout=b'\xff\xd8x\xff\xd9', stderr=b'')
        return SimpleNamespace(returncode=1, stdout=b'', stderr=b'Input/output error')
    monkeypatch.setattr('app.frame_decode.subprocess.run', run)
    with pytest.raises(RuntimeError, match=code):
        decode_position(path, tool, 5130, {'frameRate': 25}, expected=version)


def test_jumped_frame_is_not_labeled_as_requested_and_normalized_start_is_supported(source, monkeypatch):
    path, tool, version = source
    monkeypatch.setattr('app.frame_decode.subprocess.run', lambda *a, **kw: frame(130))
    out = decode_position(path, tool, 30, {'frameRate': 25, 'streamStartTime': 100}, expected=version)
    assert out.actual == 30 and out.image is not None
    out = decode_position(path, tool, 25, {'frameRate': 25, 'streamStartTime': 100}, expected=version)
    assert out.requested == 25 and out.actual == 30 and out.image is None
    assert out.reason == 'frame_position_mismatch' and out.attempts == 2


def test_unresponsive_tool_cannot_turn_every_position_into_unusable_media(source, monkeypatch):
    path, tool, version = source
    def hung(*args, **kwargs):
        raise subprocess.TimeoutExpired('ffmpeg', kwargs['timeout'])
    monkeypatch.setattr('app.frame_decode.subprocess.run', hung)
    with pytest.raises(RuntimeError, match='frame_tool_error'):
        decode_position(path, tool, 5130, {'frameRate': 25}, expected=version)


def test_one_model_failure_does_not_repeat_or_discard_other_model(source, monkeypatch):
    from app import slicing_worker as worker
    path, tool, version = source; events = []
    class Models:
        def __init__(self, *args): pass
        def load(self, category): pass
        def classify(self, image, category):
            if category == 'bed': raise RuntimeError('model process failed')
            return {'label': 'target', 'score': .9}
    monkeypatch.setattr(worker, 'Models', Models)
    monkeypatch.setattr(worker, 'emit', events.append)
    monkeypatch.setattr(worker, 'extract_requested_frames', lambda *a, **kw: iter(
        [(0, b'image'), (30, b'image'), (59.96, b'image')]))
    worker.process({'filePath': str(path), 'frameToolPath': str(tool), 'expectedFileVersion': version,
        'metadata': {'size': version['size'], 'duration': 60, 'width': 3840, 'height': 2160,
                     'frameRate': 25, 'streamStartTime': 0}, 'settings': {},
        'points': {'bed': [0, 30, 59.96], 'magnet': [0, 30, 59.96]}, 'skipUndecodableSamples': True})
    assert len([event for event in events if event['event'] == 'error' and event['category'] == 'bed']) == 1
    assert [event['time'] for event in events if event['event'] == 'observation'] == [0, 30, 59.96]
    assert all(event['category'] == 'magnet' for event in events if event['event'] == 'observation')


def test_consecutive_failures_use_adjacent_verified_boundaries_and_no_merging_across_gap():
    observations = [{'time': value, 'label': 'target'} for value in (0, 90, 120)]
    observations += [{'time': value, 'code': 'decode_unavailable'} for value in (30, 60)]
    exclusions = decode_exclusions(observations, 150)
    assert exclusions == [(.001, 89.999)]
    result = build_category_result([row for row in observations if 'label' in row], interval=30,
        duration=150, positive_labels={'target'}, exclusion_intervals=exclusions)
    assert result['intervals'] == [{'start': 0, 'end': 0}, {'start': 90, 'end': 150}]
    assert position_excluded(10, exclusions) and not position_excluded(90, exclusions)


def test_shared_coverage_and_zero_outcome_have_exact_file_version(source):
    _, _, version = source
    item = {'fileVersion': version}
    rows = [{'time': value, 'code': 'decode_unavailable', 'decodeAttempts': 2} for value in (0, 30, 59.96)]
    assert record_decode_coverage(item, rows, 60) == [(0, 60)]
    conclusion = processing_conclusion(item, rows, 'no_usable_frames')
    assert conclusion['fileVersion'] == version and conclusion['completed']
    assert (conclusion['requestedSamples'], conclusion['decodedSamples'], conclusion['skippedSamples']) == (3, 0, 3)
    # Another model's older cached result cannot remove confirmed damage.
    record_decode_coverage(item, [{'time': 30}], 60)
    assert item['decodeCoverage']['positions']['30.0']['code'] == 'decode_unavailable'


@pytest.mark.skipif(not shutil.which('ffmpeg'), reason='FFmpeg required')
def test_real_healthy_frames_keep_bytes_and_sampling_positions(tmp_path):
    ffmpeg = shutil.which('ffmpeg'); path = tmp_path / 'healthy.mp4'
    subprocess.run([ffmpeg, '-v', 'error', '-f', 'lavfi', '-i', 'testsrc2=size=160x120:rate=10',
        '-t', '12', '-c:v', 'mpeg4', str(path)], check=True, timeout=30)
    metadata = probe(str(path), shutil.which('ffprobe')); metadata['_expectedFileVersion'] = source_version(path)
    points = {'bed': [0, 5, 10, 11.9], 'magnet': [0, 5, 10, 11.9]}
    old = list(extract_requested_frames(str(path), ffmpeg, metadata, points))
    new = list(extract_requested_frames(str(path), ffmpeg, metadata, points, skip_undecodable=True))
    assert old == new  # Exact JPEG inputs, hence unchanged normal model scores.
    assert [metadata['_decodeObservations'][stamp]['actualTime'] for stamp, _ in new] == [0, 5, 10, 11.9]


@pytest.mark.asyncio
@pytest.mark.parametrize('label,expected', [('unavailable', 'uncuttable'), ('other', 'no_target')])
async def test_zero_photo_outcomes_finish_without_empty_packages(tmp_path, monkeypatch, label, expected):
    from app.main import create_app
    from app.photo_completion import verified_empty_conclusion
    from tests.test_slicing_performance import close_app
    app = create_app(data_dir=tmp_path / 'db'); manager = app.state.slicing
    path = tmp_path / 'source.mp4'; path.write_bytes(b'bound-media')
    item = {'id': 'empty', 'sourceDatasetId': '27912', 'sourceVideoId': '', 'path': str(path),
        'fileVersion': source_version(path), 'metadata': {'duration': 90, 'size': path.stat().st_size,
        'frameRate': 25, 'width': 3840, 'height': 2160, 'streamStartTime': 0},
        'categories': {name: {'state': 'pending'} for name in ('bed', 'magnet')}, 'revision': 0}
    async def worker(current, metadata, points, keys, phase):
        for name, stamps in points.items():
            for stamp in stamps:
                manager.store.save('observations', {'id': keys[name][stamp], 'time': stamp,
                    'label': label, 'code': 'decode_unavailable' if label == 'unavailable' else '',
                    'actualTime': None if label == 'unavailable' else stamp, 'decodeAttempts': 2 if label == 'unavailable' else 1})
    monkeypatch.setattr(manager, '_run_worker', worker)
    manager.store.save('items', item)
    try:
        await manager._analyze_item(item)
        for name in ('bed', 'magnet'):
            assert item['categories'][name]['state'] == expected
            assert verified_empty_conclusion(item, name)
        manager._save_category_progress(item)
        saved = manager.store.get('items', 'empty')
        assert saved['state'] == expected and saved['progress'] == 100
        assert manager.packages.store.list('items') == []
        assert path.exists()
    finally:
        await close_app(app)


@pytest.mark.asyncio
async def test_normal_export_eof_grid_does_not_falsely_report_partial_recovery(tmp_path, monkeypatch):
    from app.main import create_app
    from app.frame_decode import DecodedPosition
    from tests.test_slicing_performance import close_app
    app = create_app(data_dir=tmp_path / 'db'); manager = app.state.slicing
    source = tmp_path / 'bound.mp4'; source.write_bytes(b'bound')
    monkeypatch.setattr('app.frame_decode.decode_position', lambda *args, **kwargs:
        DecodedPosition(75, None, None, 2, 'decode_unavailable'))
    item = {'path': str(source), 'fileVersion': source_version(source),
            'metadata': {'duration': 75.01, 'frameRate': 25, 'streamStartTime': 0}}
    try:
        assert await manager._read_export_frame(item, 75) is None
        assert not item.get('partialRecovery') and not item.get('decodeCoverage')
        assert item['exportDecodeObservations']['75.0']['code'] == 'eof_no_frame'
    finally:
        await close_app(app)
