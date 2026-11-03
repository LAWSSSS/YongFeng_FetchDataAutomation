"""Keep one disk-crane model loaded; accept JPEG bytes or legacy file batches."""
import json
import sys
from pathlib import Path

from .inference_pipe import MAX_MESSAGE_BYTES, read_jpeg_payload


def classify_batch(models, batch, stream):
    if 'jpegBytes' in batch:
        images = [read_jpeg_payload(stream, batch['jpegBytes'])]
    else:
        images = (Path(path).read_bytes() for path in batch.get('paths') or [])
    return ['target' if models.classify(blob, 'magnet').get('label') == 'target' else 'other'
            for blob in images]


def main():
    from .slicing_worker import Models
    # Use the same binary stream for headers and payloads. A text wrapper may
    # read ahead into a JPEG and make the following fixed-size read hang.
    incoming = sys.stdin.buffer
    request = json.loads(incoming.readline(MAX_MESSAGE_BYTES + 1) or "{}")
    models = Models(request.get("settings") or {}, {"magnet"})
    try:
        models.load("magnet")
    except Exception as error:
        code = str(error) if str(error) in {"magnet_model_missing"} else "model_load_failed"
        sys.stdout.write(json.dumps({"ready": False, "code": code}) + "\n")
        sys.stdout.flush()
        return 1
    sys.stdout.write(json.dumps({"ready": True, "jpegBytes": True}) + "\n")
    sys.stdout.flush()
    while True:
        line = incoming.readline(MAX_MESSAGE_BYTES + 1)
        if not line:
            break
        if len(line) > MAX_MESSAGE_BYTES:
            return 2
        if not line.strip():
            continue
        batch = json.loads(line)
        if batch.get("stop"):
            break
        try:
            labels = classify_batch(models, batch, incoming)
        except (ValueError, OSError):
            # A truncated/invalid frame cannot be interpreted as another
            # request or a negative prediction. End this worker immediately.
            return 2
        sys.stdout.write(json.dumps(labels) + "\n")
        sys.stdout.flush()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
