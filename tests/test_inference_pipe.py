"""No media or prediction changes when JPEGs bypass removable-disk staging."""
import asyncio
import hashlib
import io
import json
from pathlib import Path
import subprocess
import sys
import threading

import pytest

from app.inference_pipe import MAX_JPEG_BYTES, abort, read_jpeg_payload, read_reply, send_request
from app.magnet_export import classify_batch
from app.main import create_app
from tests.test_slicing_performance import close_app


WORKER = '''
import hashlib, sys
import app.slicing_worker
class Models:
    def __init__(self, settings, categories): self.expected = settings['expectedSha']
    def load(self, category): pass
    def classify(self, data, category):
        assert category == 'magnet'
        assert hashlib.sha256(data).hexdigest() == self.expected
        return {'label': 'target'}
app.slicing_worker.Models = Models
from app.magnet_export import main
sys.exit(main())
'''


def worker(blob):
    process = subprocess.Popen([sys.executable, '-B', '-c', WORKER],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    send_request(process, {'settings': {'expectedSha': hashlib.sha256(blob).hexdigest()}})
    assert read_reply(process)['jpegBytes'] is True
    return process


def test_raw_jpeg_and_legacy_paths_feed_the_exact_same_bytes_in_order(tmp_path):
    jpeg = b'\xff\xd8' + bytes(range(256)) * 1024 + b'\xff\xd9'
    path = tmp_path / '图片 with spaces.jpg'
    path.write_bytes(jpeg)
    process = worker(jpeg)
    try:
        results = []
        for request, payload in [({'paths': [str(path)]}, b''),
                                 ({'jpegBytes': len(jpeg)}, jpeg),
                                 ({'jpegBytes': len(jpeg)}, jpeg),
                                 ({'paths': [str(path)]}, b'')]:
            send_request(process, request, payload)
            results.append(read_reply(process))
        assert results == [['target']] * 4
        send_request(process, {'stop': True})
        assert process.wait(timeout=5) == 0
    finally:
        abort(process)


@pytest.mark.parametrize('size,data', [
    (True, b'\xff\xd8\xff\xd9'), (0, b''), (-1, b''), (MAX_JPEG_BYTES + 1, b''),
    (10, b'\xff\xd8\xff\xd9'), (4, b'abcd'), (4, b'\xff\xd8ab'),
])
def test_invalid_or_truncated_payload_never_reaches_the_model(size, data):
    class Models:
        def classify(self, *_):
            pytest.fail('Malformed payload must not become a negative prediction.')
    with pytest.raises(ValueError, match='invalid_jpeg_payload'):
        classify_batch(Models(), {'jpegBytes': size}, io.BytesIO(data))


def test_exact_binary_input_bytes_are_preserved():
    jpeg = b'\xff\xd8\x00\n\xff\xd9'
    assert read_jpeg_payload(io.BytesIO(jpeg), len(jpeg)) == jpeg


def test_child_exits_on_truncated_frame_instead_of_reading_the_next_header(tmp_path):
    jpeg = b'\xff\xd8\xff\xd9'
    process = worker(jpeg)
    try:
        send_request(process, {'jpegBytes': len(jpeg) + 10}, jpeg)
        process.stdin.close()
        assert process.wait(timeout=5) == 2
        assert process.stdout.read() == b''
    finally:
        abort(process)


def test_stalled_model_reply_is_bounded_and_is_not_a_decode_failure():
    process = subprocess.Popen([sys.executable, '-c',
        'import sys,time;sys.stdout.buffer.write(b"[");sys.stdout.flush();time.sleep(30)'],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE)
    try:
        with pytest.raises(RuntimeError, match='^inference_failed$'):
            read_reply(process, timeout=.1)
        assert process.poll() is not None
        assert process.stdin.closed and process.stdout.closed
    finally:
        abort(process)


def test_pipe_backpressure_has_a_deadline_and_releases_its_worker():
    process = subprocess.Popen([sys.executable, '-c', 'import time;time.sleep(30)'],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE)
    try:
        with pytest.raises(RuntimeError, match='^inference_failed$'):
            send_request(process, {'jpegBytes': 1024 * 1024}, b'x' * (1024 * 1024), timeout=.1)
        assert process.poll() is not None
        assert process.stdin.closed and process.stdout.closed
    finally:
        abort(process)


def test_closed_pipe_is_an_inference_failure_and_does_not_leave_a_worker():
    process = subprocess.Popen([sys.executable, '-c', 'import time;time.sleep(30)'],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE)
    try:
        process.stdin.close()
        with pytest.raises(RuntimeError, match='^inference_failed$'):
            send_request(process, {'stop': True})
        assert process.poll() is not None and process.stdout.closed
    finally:
        abort(process)


@pytest.mark.parametrize('reply', [None, [], 4, {'ready': False, 'code': 'model_load_failed'}])
@pytest.mark.asyncio
async def test_invalid_initial_reply_stops_the_process(tmp_path, monkeypatch, reply):
    app = create_app(data_dir=tmp_path / 'db')
    manager = app.state.slicing
    manager.store.settings['workerPythonPath'] = sys.executable
    actual_popen = subprocess.Popen
    processes = []
    code = f'import sys,time;sys.stdin.buffer.readline();sys.stdout.write({json.dumps(json.dumps(reply) + chr(10))});sys.stdout.flush();time.sleep(30)'
    def fake_worker(*_, **options):
        process = actual_popen([sys.executable, '-B', '-c', code], **options)
        processes.append(process)
        return process
    monkeypatch.setattr('app.slicing.subprocess.Popen', fake_worker)
    try:
        with pytest.raises(RuntimeError, match='inference_failed|model_load_failed'):
            await asyncio.to_thread(manager._open_magnet_labeler)
        assert processes[0].poll() is not None
        assert processes[0].stdin.closed and processes[0].stdout.closed
    finally:
        for process in processes:
            abort(process)
        await close_app(app)


@pytest.mark.parametrize('reply', [None, {}, [], [{}], ['unexpected'], [None], ['target', 'other']])
@pytest.mark.asyncio
async def test_invalid_prediction_reply_is_never_a_negative_label(tmp_path, monkeypatch, reply):
    app = create_app(data_dir=tmp_path / 'db')
    manager = app.state.slicing
    manager.store.settings['workerPythonPath'] = sys.executable
    actual_popen = subprocess.Popen
    processes = []
    code = '''import json,sys,time
incoming=sys.stdin.buffer
incoming.readline()
sys.stdout.write('{"ready":true,"jpegBytes":true}\\n');sys.stdout.flush()
request=json.loads(incoming.readline());incoming.read(request['jpegBytes'])
sys.stdout.write(REPLY);sys.stdout.flush();time.sleep(30)
'''.replace('REPLY', repr(json.dumps(reply) + '\n'))
    def fake_worker(*_, **options):
        process = actual_popen([sys.executable, '-B', '-c', code], **options)
        processes.append(process)
        return process
    monkeypatch.setattr('app.slicing.subprocess.Popen', fake_worker)
    labeler = None
    try:
        labeler = await asyncio.to_thread(manager._open_magnet_labeler)
        with pytest.raises(RuntimeError, match='^inference_failed$'):
            await asyncio.to_thread(labeler, b'\xff\xd8\xff\xd9')
        assert processes[0].poll() is not None
    finally:
        if labeler:
            await asyncio.to_thread(labeler.close)
        for process in processes:
            abort(process)
        await close_app(app)


@pytest.mark.asyncio
async def test_current_backend_uses_memory_without_touching_working_disk(tmp_path, monkeypatch):
    app = create_app(data_dir=tmp_path / 'db')
    manager = app.state.slicing
    jpeg = b'\xff\xd8' + b'unchanged bytes' * 100 + b'\xff\xd9'
    manager.store.settings['workerPythonPath'] = sys.executable
    actual_popen = subprocess.Popen
    def fake_worker(*_, **options):
        return actual_popen([sys.executable, '-B', '-c', WORKER.replace(
            "settings['expectedSha']", repr(hashlib.sha256(jpeg).hexdigest()))], **options)
    monkeypatch.setattr('app.slicing.subprocess.Popen', fake_worker)
    class Workspace:
        enabled = True
        def require_root(self):
            pytest.fail('The raw-byte protocol must not create a JPEG on the workspace disk.')
    manager.workspace = Workspace()
    labeler = None
    try:
        labeler = await asyncio.to_thread(manager._open_magnet_labeler)
        assert await asyncio.to_thread(labeler, jpeg) == 'target'
    finally:
        if labeler:
            await asyncio.to_thread(labeler.close)
        await close_app(app)


@pytest.mark.asyncio
async def test_cancel_finishes_the_current_exchange_before_closing_the_worker(tmp_path, monkeypatch):
    app = create_app(data_dir=tmp_path / 'db')
    manager = app.state.slicing
    entered, release = threading.Event(), threading.Event()
    events = []
    def labeler(_):
        entered.set()
        assert release.wait(3)
        events.append('classified')
        return 'target'
    labeler.close = lambda: events.append('closed')
    async def image(*_):
        return b'\xff\xd8\xff\xd9'
    monkeypatch.setattr(manager, '_open_magnet_labeler', lambda: labeler)
    monkeypatch.setattr(manager, '_read_export_frame', image)
    task = asyncio.create_task(manager._recognized_magnet_frames({}, [0]))
    try:
        assert await asyncio.to_thread(entered.wait, 2)
        task.cancel()
        await asyncio.sleep(.02)
        assert not task.done() and not events
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert events == ['classified', 'closed']
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)
        await close_app(app)


@pytest.mark.asyncio
async def test_cancel_while_loading_closes_the_worker_when_initialization_finishes(tmp_path, monkeypatch):
    app = create_app(data_dir=tmp_path / 'db')
    manager = app.state.slicing
    entered, release = threading.Event(), threading.Event()
    events = []
    def labeler(_):
        pytest.fail('A cancelled initialization must never classify a frame.')
    labeler.close = lambda: events.append('closed')
    def opening():
        entered.set()
        assert release.wait(3)
        events.append('loaded')
        return labeler
    monkeypatch.setattr(manager, '_open_magnet_labeler', opening)
    task = asyncio.create_task(manager._recognized_magnet_frames({}, [0]))
    try:
        assert await asyncio.to_thread(entered.wait, 2)
        task.cancel()
        await asyncio.sleep(.02)
        assert not task.done(), 'Wait for initialization before releasing the process and source lease.'
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert events == ['loaded', 'closed']
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)
        await close_app(app)
