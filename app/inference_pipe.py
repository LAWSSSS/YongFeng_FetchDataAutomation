"""Bounded local model messages; JPEG payloads stay in memory."""
import json
import os
import selectors
import subprocess
import time


MAX_JPEG_BYTES = 32 * 1024 * 1024
MAX_MESSAGE_BYTES = 64 * 1024


def json_line(value):
    return (json.dumps(value) + "\n").encode()


def abort(process):
    try:
        process.kill()
        process.wait(timeout=5)
    except (OSError, subprocess.TimeoutExpired):
        pass
    finally:
        for stream in (process.stdin, process.stdout):
            if stream:
                try:
                    stream.close()
                except OSError:
                    pass


def send_request(process, value, payload=b'', *, timeout=60):
    """Bound pipe backpressure too: a stalled reader cannot hang cancellation."""
    header = json_line(value)
    if len(header) > MAX_MESSAGE_BYTES or len(payload) > MAX_JPEG_BYTES:
        abort(process)
        raise RuntimeError('inference_failed')
    deadline = time.monotonic() + timeout
    fd = None
    try:
        fd = process.stdin.fileno()
        os.set_blocking(fd, False)
        with selectors.DefaultSelector() as selector:
            selector.register(fd, selectors.EVENT_WRITE)
            for data in (header, payload):
                remaining_data = memoryview(data)
                while remaining_data:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0 or not selector.select(remaining):
                        raise RuntimeError('inference_failed')
                    try:
                        written = os.write(fd, remaining_data[:65536])
                    except BlockingIOError:
                        continue
                    remaining_data = remaining_data[written:]
    except (OSError, ValueError, RuntimeError, TypeError):
        abort(process)
        raise RuntimeError('inference_failed') from None
    finally:
        if fd is not None and not process.stdin.closed:
            os.set_blocking(fd, True)


def read_jpeg_payload(stream, size):
    if type(size) is not int or not 0 < size <= MAX_JPEG_BYTES:
        raise ValueError("invalid_jpeg_payload")
    data = stream.read(size)
    if len(data) != size or not data.startswith(b"\xff\xd8") or not data.endswith(b"\xff\xd9"):
        raise ValueError("invalid_jpeg_payload")
    return data


def read_reply(process, *, timeout=60):
    """Read one bounded reply without leaving a stalled model alive for reuse."""
    deadline = time.monotonic() + timeout
    response = bytearray()
    try:
        with selectors.DefaultSelector() as selector:
            selector.register(process.stdout, selectors.EVENT_READ)
            while len(response) <= MAX_MESSAGE_BYTES:
                remaining = deadline - time.monotonic()
                if remaining <= 0 or not selector.select(remaining):
                    raise RuntimeError("inference_failed")
                data = os.read(process.stdout.fileno(), min(4096, MAX_MESSAGE_BYTES + 1 - len(response)))
                if not data:
                    raise RuntimeError("inference_failed")
                response.extend(data)
                if b"\n" in response:
                    line, remainder = bytes(response).split(b"\n", 1)
                    if remainder or len(line) > MAX_MESSAGE_BYTES:
                        raise RuntimeError("inference_failed")
                    return json.loads(line)
        raise RuntimeError("inference_failed")
    except (OSError, ValueError, RuntimeError):
        abort(process)
        raise RuntimeError("inference_failed") from None
