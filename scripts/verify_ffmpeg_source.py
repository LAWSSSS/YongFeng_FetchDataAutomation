"""Verify the pinned upstream FFmpeg source before compiling."""
import hashlib
import json
import sys
from pathlib import Path

path = Path(sys.argv[1])
pin = json.loads((Path(__file__).resolve().parents[1] / "packaging.lock.json").read_text())["ffmpeg"]
actual = hashlib.file_digest(path.open("rb"), "sha256").hexdigest()
if actual != pin["sha256"]:
    raise SystemExit("FFmpeg source checksum mismatch; build stopped.")
print("FFmpeg source checksum verified.")
