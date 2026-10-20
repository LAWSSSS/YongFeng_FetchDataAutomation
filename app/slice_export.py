"""Turn recognized intervals into one JPEG archive per category."""
import hashlib
import re
import subprocess
import zipfile
from pathlib import Path

_NAME = re.compile(r"^(.+)_(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})_")
PACKAGE_DATASETS = {"bed": "27892", "magnet": "27893"}


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


def photo_names(video_name, count):
    match = _NAME.match(Path(video_name or "").name)
    if match:
        plate, year, month, day, hour, minute, second = match.groups()
        base = f"{plate}_{year}{month}{day}_{hour}{minute}{second}"
    else:
        base = Path(video_name or "image").stem or "image"
    base = re.sub(r'[\\/:*?"<>|]+', "_", base)[:80]
    return [f"{base}_{index:03d}.jpg" for index in range(1, count + 1)]


def package_identity(source_dataset, video_id, category):
    text = f"{source_dataset}:{video_id}:{category}"
    return hashlib.sha256(text.encode()).hexdigest()[:24]


def read_jpeg(path, ffmpeg, timestamp):
    command = [ffmpeg, "-nostdin", "-hide_banner", "-loglevel", "error", "-ss", f"{timestamp:.3f}",
               "-i", str(path), "-frames:v", "1", "-q:v", "3", "-f", "image2pipe", "-vcodec", "mjpeg", "pipe:1"]
    try:
        result = subprocess.run(command, capture_output=True, timeout=60, check=False)
    except subprocess.TimeoutExpired as error:
        raise RuntimeError("frame_extract_failed") from error
    data = result.stdout
    if result.returncode or not data.startswith(b"\xff\xd8") or not data.endswith(b"\xff\xd9"):
        raise RuntimeError("frame_extract_failed")
    return data


def write_image_archive(destination, names, frames):
    """Zip only an image/ directory. The archive name stays outside that directory."""
    destination = Path(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_suffix(destination.suffix + ".partial")
    if temporary.exists():
        temporary.unlink()
    with zipfile.ZipFile(temporary, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for name, blob in zip(names, frames):
            if not blob.startswith(b"\xff\xd8"):
                raise ValueError("切片照片不是 JPEG。")
            archive.writestr(f"image/{name}", blob)
    temporary.replace(destination)
    return destination
