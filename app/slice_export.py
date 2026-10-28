"""Turn recognized intervals into one JPEG archive per category."""
import hashlib
import json
import re
import subprocess
import zipfile
from pathlib import Path

_NAME = re.compile(r"^(.+)_(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})_")
PACKAGE_DATASETS = {"bed": "27892", "magnet": "27893"}
CATEGORY_FOLDERS = {"bed": "车斗", "magnet": "磁盘吊"}


def package_directory(video_path, category, *, output_root=None, video_id=None):
    """Return a per-video category folder, optionally under a central output root."""
    video = Path(video_path)
    if output_root is not None:
        safe_stem = re.sub(r"[^\w.-]+", "_", video.stem, flags=re.UNICODE).strip("._-")[:120] or "video"
        safe_id = re.sub(r"[^A-Za-z0-9_-]+", "_", str(video_id or "")).strip("_-")[:64]
        identity = f"_{safe_id}" if safe_id else ""
        return Path(output_root) / CATEGORY_FOLDERS[category] / f"{safe_stem}{identity}"
    return video.parent / video.stem / CATEGORY_FOLDERS[category]


def select_recognized_frames(samples, *, target=150, maximum=200):
    """Return only timestamps already labeled target, thinning a long run toward the target count."""
    positive = sorted({round(float(stamp), 3) for stamp, label in samples if label == "target"})
    if len(positive) <= maximum:
        return positive
    if target <= 1 or len(positive) == 1:
        return positive[:1]
    step = (len(positive) - 1) / (target - 1)
    chosen = []
    for index in range(target):
        stamp = positive[round(index * step)]
        if not chosen or chosen[-1] != stamp:
            chosen.append(stamp)
    return chosen


def magnet_refine_windows(samples, gap):
    """Windows between a hit and a miss, and the open gap between two hits."""
    rows = sorted((round(float(stamp), 3), "target" if label == "target" else "other") for stamp, label in samples)
    windows = []
    for (left, left_label), (right, right_label) in zip(rows, rows[1:]):
        if left_label != right_label or (left_label == "target" and right - left > gap + 1e-3):
            windows.append((left, right))
    return windows


def interior_timestamps(start, end, step):
    points = []
    stamp = round(float(start) + step, 3)
    limit = float(end) - 1e-6
    while stamp < limit:
        points.append(stamp)
        stamp = round(stamp + step, 3)
    return points


def slice_timestamps(intervals, seconds):
    """Same inclusive grid the image-count estimate uses."""
    step = int(seconds)
    if step < 1:
        raise ValueError("切片间隔无效。")
    points = []
    for window in intervals or []:
        if isinstance(window, dict):
            start, end = float(window["start"]), float(window["end"])
        else:
            start, end = float(window[0]), float(window[1])
        if end < start:
            continue
        stamp = start
        while stamp <= end + 1e-6:
            points.append(round(stamp, 3))
            stamp += step
    return points


def photo_export_identity(item, category, positions, frames):
    """Stable export identity, independent of paths, queue edits and retries."""
    if len(positions) != len(frames):
        raise ValueError("照片与实际抽帧位置不一致。")
    source = {key: item.get(key) for key in ('sourceIdentity', 'sourceDatasetId', 'sourceVideoId',
                                             'mediaId', 'mediaGeneration', 'collectionRevision')}
    source.update(version=item.get('observationVersion') or item.get('fileVersion'), category=category,
                  positions=positions)
    digest = hashlib.sha256(json.dumps(source, sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode())
    for frame in frames:
        digest.update(len(frame).to_bytes(8, 'big'))
        digest.update(frame)
    return digest.hexdigest()


def photo_names(video_name, count, *, identity=None):
    match = _NAME.match(Path(video_name or "").name)
    if match:
        plate, year, month, day, hour, minute, second = match.groups()
        base = f"{plate}_{year}{month}{day}_{hour}{minute}{second}"
    else:
        base = Path(video_name or "image").stem or "image"
    base = re.sub(r'[\x00-\x1f\x7f\\/:*?"<>|]+', "_", base)[:60].strip(' .') or 'image'
    base = base.encode()[:180].decode(errors='ignore').rstrip(' .') or 'image'
    token = hashlib.sha256(str(identity if identity is not None else video_name or 'image').encode()).hexdigest()[:24]
    return [f"{base}_v{token}_{index:03d}.jpg" for index in range(1, count + 1)]


def package_identity(source_dataset, video_id, category):
    text = f"{source_dataset}:{video_id}:{category}"
    return hashlib.sha256(text.encode()).hexdigest()[:24]


def read_jpeg(path, ffmpeg, timestamp, timeout=60):
    command = [ffmpeg, "-nostdin", "-hide_banner", "-loglevel", "error", "-ss", f"{timestamp:.3f}",
               "-i", str(path), "-frames:v", "1", "-q:v", "3", "-f", "image2pipe", "-vcodec", "mjpeg", "pipe:1"]
    try:
        result = subprocess.run(command, capture_output=True, timeout=timeout, check=False)
    except subprocess.TimeoutExpired as error:
        raise RuntimeError("frame_timeout") from error
    except OSError as error:
        raise RuntimeError("frame_tool_error") from error
    data = result.stdout
    if result.returncode or not data.startswith(b"\xff\xd8") or not data.endswith(b"\xff\xd9"):
        stderr = result.stderr.decode(errors="replace").lower()
        if any(message in stderr for message in ("input/output error", "permission denied", "device not configured",
                "no such file or directory", "read error", "i/o error", "error opening input")):
            raise RuntimeError("frame_io_error")
        raise RuntimeError("decode_unavailable")
    return data


def write_image_archive(destination, names, frames):
    """Athena's unannotated image importer requires an images/ directory."""
    destination = Path(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_suffix(destination.suffix + ".partial")
    if temporary.exists():
        temporary.unlink()
    # JPEG is already compressed; storing it avoids another CPU pass with
    # negligible size savings and is supported by the platform importer.
    with zipfile.ZipFile(temporary, "w", compression=zipfile.ZIP_STORED) as archive:
        for name, blob in zip(names, frames):
            if not blob.startswith(b"\xff\xd8"):
                raise ValueError("切片照片不是 JPEG。")
            archive.writestr(f"images/{name}", blob)
    temporary.replace(destination)
    return destination
