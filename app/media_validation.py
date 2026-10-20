"""Check decoded video content instead of trusting a readable container header."""
from __future__ import annotations

import subprocess


class MediaValidationError(ValueError):
    pass


def verify_video_tail(path, ffmpeg, duration):
    # A truncated MP4 can retain its original duration and return exit code 0.
    # Require an actual decoded frame near EOF before spending time on inference
    # or publishing a replacement. JPEG bytes remain in memory only.
    command = [str(ffmpeg), "-nostdin", "-hide_banner", "-loglevel", "error", "-threads", "2",
               "-sseof", f"-{min(1.0, float(duration)):.6f}", "-i", str(path),
               "-map", "0:v:0", "-an", "-sn", "-frames:v", "1", "-threads", "2",
               "-f", "image2pipe", "-vcodec", "mjpeg", "pipe:1"]
    try:
        result = subprocess.run(command, capture_output=True, timeout=45, check=False)
    except subprocess.TimeoutExpired:
        raise MediaValidationError("media_check_timeout") from None
    if (result.returncode or not result.stdout.startswith(b"\xff\xd8")
            or not result.stdout.endswith(b"\xff\xd9")):
        raise MediaValidationError("media_incomplete")
