"""Pure sampling and clip-window rules for model-guided dataset slicing."""
from __future__ import annotations

import math
from collections.abc import Iterable


def sample_points(duration: float, interval: int, frame_rate: float = 25.0) -> list[float]:
    """Return regular timestamps plus the first and last decodable frames."""
    if not math.isfinite(duration) or duration <= 0:
        raise ValueError("视频时长无效。")
    if isinstance(interval, bool) or not isinstance(interval, int) or interval < 1:
        raise ValueError("抽帧间隔必须是正整数秒。")
    rate = frame_rate if math.isfinite(frame_rate) and frame_rate > 0 else 25.0
    tail = max(0.0, duration - 1.0 / rate)
    values = [float(i) for i in range(0, int(duration) + 1, interval) if i <= tail]
    values.extend((0.0, tail))
    return sorted(set(round(value, 6) for value in values))


def merge_intervals(intervals: Iterable[tuple[float, float]], *, tolerance: float = 1e-6) -> list[tuple[float, float]]:
    ordered = sorted((float(start), float(end)) for start, end in intervals)
    merged: list[list[float]] = []
    for start, end in ordered:
        if not math.isfinite(start) or not math.isfinite(end) or start < 0 or end < start:
            raise ValueError("切片区间无效。")
        if merged and start <= merged[-1][1] + tolerance:
            merged[-1][1] = max(merged[-1][1], end)
        else:
            merged.append([start, end])
    return [(round(start, 3), round(end, 3)) for start, end in merged]


def intervals_from_observations(
    observations: Iterable[dict], *, interval: int, duration: float, positive_labels: set[str],
) -> list[tuple[float, float]]:
    """Group adjacent positive samples, extending each run by one sample step."""
    if interval < 1 or duration <= 0:
        raise ValueError("抽帧间隔或视频时长无效。")
    rows = sorted((float(row["time"]), str(row["label"])) for row in observations)
    if not rows:
        return []
    runs: list[list[float]] = []
    for timestamp, label in rows:
        if timestamp < 0 or timestamp > duration:
            continue
        if label not in positive_labels:
            continue
        if runs and timestamp - runs[-1][1] <= interval + 1e-4:
            runs[-1][1] = timestamp
        else:
            runs.append([timestamp, timestamp])
    return merge_intervals((max(0.0, first - interval), min(duration, last + interval)) for first, last in runs)


def expected_image_count(intervals: Iterable[tuple[float, float]], seconds: int) -> int:
    """Athena's interval count includes the first sample in each fragment."""
    if isinstance(seconds, bool) or not isinstance(seconds, int) or seconds < 1:
        raise ValueError("切片间隔最小为1秒。")
    return sum(max(1, math.floor((end - start) / seconds + 1e-9) + 1) for start, end in intervals)


def choose_slice_interval(
    intervals: Iterable[tuple[float, float]], *, target: int = 150, minimum: int = 100, maximum: int = 200,
) -> dict:
    """Choose the integer interval closest to target without dropping fragments."""
    windows = merge_intervals(intervals)
    if not windows:
        return {"interval": None, "count": 0, "withinTarget": False, "tooMany": False}
    if not 1 <= minimum <= target <= maximum:
        raise ValueError("切片目标张数设置无效。")
    at_one = expected_image_count(windows, 1)
    if at_one < minimum:
        return {"interval": 1, "count": at_one, "withinTarget": False, "tooMany": False}
    cap = max(1, math.ceil(max(end - start for start, end in windows)))
    candidates = []
    for seconds in range(1, cap + 1):
        count = expected_image_count(windows, seconds)
        if minimum <= count <= maximum:
            candidates.append((abs(count - target), abs(seconds - max(1, round(sum(end - start for start, end in windows) / target))), seconds, count))
    if not candidates:
        return {"interval": None, "count": at_one, "withinTarget": False, "tooMany": at_one > maximum}
    _, _, seconds, count = min(candidates)
    return {"interval": seconds, "count": count, "withinTarget": True, "tooMany": False}


def build_category_result(
    observations: Iterable[dict], *, interval: int, duration: float, positive_labels: set[str],
    target: int = 150, minimum: int = 100, maximum: int = 200,
) -> dict:
    windows = intervals_from_observations(observations, interval=interval, duration=duration,
                                          positive_labels=positive_labels)
    picked = choose_slice_interval(windows, target=target, minimum=minimum, maximum=maximum)
    return {"intervals": [{"start": start, "end": end} for start, end in windows],
            "intervalSeconds": picked["interval"], "estimatedImages": picked["count"],
            "withinTarget": picked["withinTarget"], "tooMany": picked["tooMany"]}
