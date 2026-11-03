"""Bounded, position-preserving decoding shared by sampling and photo export."""
from __future__ import annotations

import hashlib
import math
import os
import re
import stat
import subprocess
from dataclasses import dataclass


@dataclass(frozen=True)
class DecodedPosition:
    requested: float
    actual: float | None
    image: bytes | None
    attempts: int
    reason: str = ""

    def observation(self):
        return {"requestedTime": self.requested, "actualTime": self.actual,
                "decodeAttempts": self.attempts, "decodeReason": self.reason}


def check_source(path, expected, *, edges=False):
    try:
        info = os.stat(path, follow_symlinks=False)
        if not stat.S_ISREG(info.st_mode):
            raise RuntimeError("frame_io_error")
        if expected and (info.st_size, info.st_mtime_ns, info.st_ino, info.st_dev) != (
                expected.get("size"), expected.get("modifiedNs"), expected.get("inode"), expected.get("device")):
            raise RuntimeError("source_changed")
        with open(path, "rb") as stream:
            first = stream.read(65536 if edges else 1)
            if edges and expected and expected.get("edgeHash"):
                digest = hashlib.sha256(first)
                if info.st_size > 65536:
                    stream.seek(max(0, info.st_size - 65536))
                    digest.update(stream.read(65536))
                if digest.hexdigest() != expected["edgeHash"]:
                    raise RuntimeError("source_changed")
    except OSError:
        raise RuntimeError("frame_io_error") from None


def decode_position(path, ffmpeg, timestamp, metadata, *, expected=None, quality=None, tail=False):
    """Two attempts at most; uncertain environment failures are never skipped.

    ``-copyts`` lets showinfo report the input frame's PTS. Subtracting the
    stream start makes that PTS a playback position, independently of seeking.
    A decoder that jumps across corruption must not label its next good frame
    as the requested position.
    """
    timestamp = float(timestamp)
    if not math.isfinite(timestamp) or timestamp < 0:
        raise RuntimeError("sample_plan_invalid")
    if not os.path.isfile(ffmpeg) or not os.access(ffmpeg, os.X_OK):
        raise RuntimeError("frame_tool_error")
    if not expected:
        # Standalone callers still get an immutable baseline for both attempts.
        try:
            info = os.stat(path, follow_symlinks=False)
            expected = {"size": info.st_size, "modifiedNs": info.st_mtime_ns,
                        "device": info.st_dev, "inode": info.st_ino}
        except OSError:
            raise RuntimeError("frame_io_error") from None
    check_source(path, expected)
    rate = float(metadata.get("frameRate") or 25)
    tolerance = max(.002, 1.01 / rate) if math.isfinite(rate) and rate > 0 else .041
    start = float(metadata.get("streamStartTime") or 0)
    seek = ["-sseof", f"-{min(1.0, max(.05, float(metadata['duration']))):.6f}"] if tail else ["-ss", f"{timestamp:.6f}"]
    command = [str(ffmpeg), "-nostdin", "-hide_banner", "-loglevel", "info", "-threads", "2",
               "-copyts", *seek, "-i", str(path), "-map", "0:v:0", "-an", "-sn",
               "-vf", "showinfo,setpts=PTS-STARTPTS"]
    if not tail:
        command.extend(("-frames:v", "1"))
    command.extend(("-threads", "2"))
    if quality is not None:
        command.extend(("-q:v", str(quality)))
    command.extend(("-f", "image2pipe", "-vcodec", "mjpeg", "pipe:1"))
    reason, actual = "decode_unavailable", None
    # FFmpeg also says "Error opening input" for malformed containers and EOF.
    # Only concrete filesystem errors can classify a stable source as an
    # environmental failure; damaged positions still get the bounded retry.
    io_markers = ("input/output error", "permission denied", "device not configured", "no such file or directory",
                  "i/o error", "resource temporarily unavailable", "operation not permitted")
    for attempt in (1, 2):
        try:
            result = subprocess.run(command, capture_output=True, timeout=30, check=False)
        except subprocess.TimeoutExpired:
            reason = "frame_timeout"
        except OSError:
            raise RuntimeError("frame_tool_error") from None
        else:
            stderr = result.stderr.decode(errors="replace")
            if any(marker in stderr.lower() for marker in io_markers):
                raise RuntimeError("frame_io_error")
            if any(marker in stderr.lower() for marker in ("no such filter", "unknown encoder", "unrecognized option")):
                raise RuntimeError("frame_tool_error")
            times = re.findall(r"\bn:\s*\d+\s+pts:\s*-?\d+\s+pts_time:\s*([-+\d.eE]+)", stderr)
            image = result.stdout
            if result.returncode == 0 and image.startswith(b"\xff\xd8") and image.endswith(b"\xff\xd9"):
                if not times:
                    # Missing timing is a tool/protocol fault, not damaged media.
                    raise RuntimeError("frame_timestamp_missing")
                if tail:
                    # Preserve the old tail window's encoder context as well
                    # as its last valid frame, rather than changing JPEG input
                    # quality seen by either existing model.
                    image = image[image.rfind(b"\xff\xd8"):]
                actual = round(float(times[-1] if tail else times[0]) - start, 6)
                if math.isfinite(actual) and abs(actual - timestamp) <= tolerance:
                    check_source(path, expected)
                    return DecodedPosition(timestamp, actual, image, attempt)
                reason = "frame_position_mismatch"
            else:
                reason = "decode_unavailable"
        # Check bytes and tool after a failure/timeout, before classifying the
        # position as unusable. A disconnected/replaced source never qualifies.
        check_source(path, expected, edges=True)
        if not os.path.isfile(ffmpeg) or not os.access(ffmpeg, os.X_OK):
            raise RuntimeError("frame_tool_error")
    if reason == "frame_timeout":
        # A globally unresponsive executable is an environment fault. Only a
        # functioning decoder with a stable readable source can skip timeout.
        try:
            health = subprocess.run([str(ffmpeg), "-version"], capture_output=True, timeout=5, check=False)
        except (OSError, subprocess.TimeoutExpired):
            raise RuntimeError("frame_tool_error") from None
        if health.returncode:
            raise RuntimeError("frame_tool_error")
    return DecodedPosition(timestamp, actual, None, 2, reason)
