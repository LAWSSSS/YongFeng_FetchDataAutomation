"""Small real-model profile, isolated from platform queues and credentials.

Public entry point: ``await run_profile(run_root, copied_video, settings)``.
Only four shared JPEGs pass through memory; no video or image is generated.
The child interpreter is the configured Python 3.11 inference environment.
"""
from __future__ import annotations

import asyncio
import json
import os
import resource
import sys
import time
from pathlib import Path

PROJECT = Path(__file__).resolve().parents[1]
POSITIONS = [0.0, 1200.0, 2400.0, 3600.0]
CATEGORIES = ("bed", "magnet")


def _memory():
    unit = "bytes" if sys.platform == "darwin" else "KiB"
    self_peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    child_peak = resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss
    divisor = 1024 * 1024 if unit == "bytes" else 1024
    return {"ruMaxRss": self_peak, "ruMaxRssUnit": unit,
            "peakRssMiB": round(self_peak / divisor, 3),
            "childRuMaxRss": child_peak, "childPeakRssMiB": round(child_peak / divisor, 3),
            "scope": "self为Python与模型；children为ffprobe/ffmpeg子进程峰值，不能将两者相加当作同时总量"}


def _worker(payload):
    if sys.version_info[:2] != (3, 11):
        raise RuntimeError("This profile must run in the configured Python 3.11 inference environment.")
    sys.path.insert(0, str(PROJECT))
    from app.slicing_worker import Models, extract_requested_frames, probe

    started = time.monotonic()
    source = str(Path(payload["source"]).resolve(strict=True))
    settings = payload["settings"]
    ffmpeg = settings["frameToolPath"]
    outcome = {"kind": "真实视频与两个模型直接验证", "python": sys.version.split()[0],
               "positions": POSITIONS, "modelLoads": {}, "frames": [], "errors": [],
               "decodedPositions": 0, "classificationCount": 0, "sharedDecoding": True}
    stage = time.monotonic()
    metadata = probe(source, str(Path(ffmpeg).with_name("ffprobe")))
    outcome.update(metadata=metadata, probeSeconds=round(time.monotonic() - stage, 6))
    models = Models(settings, set(CATEGORIES))
    available = []
    for category in CATEGORIES:
        stage = time.monotonic()
        try:
            models.load(category)
            available.append(category)
            outcome["modelLoads"][category] = {"passed": True, "seconds": round(time.monotonic() - stage, 6)}
        except Exception as error:
            outcome["modelLoads"][category] = {"passed": False, "seconds": round(time.monotonic() - stage, 6),
                                               "errorType": type(error).__name__}
            outcome["errors"].append({"stage": "load", "category": category, "type": type(error).__name__})
    frames = extract_requested_frames(source, ffmpeg, metadata, {category: POSITIONS for category in CATEGORIES})
    while True:
        decode_started = time.monotonic()
        try:
            timestamp, jpeg = next(frames)
        except StopIteration:
            break
        except Exception as error:
            outcome["errors"].append({"stage": "decode", "type": type(error).__name__,
                                       "code": getattr(error, "code", None), "position": getattr(error, "timestamp", None)})
            break
        frame = {"position": timestamp, "decodeWaitSeconds": round(time.monotonic() - decode_started, 6),
                 "jpegBytes": len(jpeg), "classifications": {}}
        outcome["decodedPositions"] += 1
        for category in available:
            classify_started = time.monotonic()
            try:
                classification = models.classify(jpeg, category)
                outcome["classificationCount"] += 1
                frame["classifications"][category] = {"passed": True,
                    "seconds": round(time.monotonic() - classify_started, 6), "result": classification}
            except Exception as error:
                frame["classifications"][category] = {"passed": False,
                    "seconds": round(time.monotonic() - classify_started, 6), "errorType": type(error).__name__}
                outcome["errors"].append({"stage": "classify", "category": category,
                                           "position": timestamp, "type": type(error).__name__})
        outcome["frames"].append(frame)
        del jpeg
    frames.close()
    outcome["totals"] = {
        "modelLoadSeconds": round(sum(entry["seconds"] for entry in outcome["modelLoads"].values()), 6),
        "decodeWaitSeconds": round(sum(frame["decodeWaitSeconds"] for frame in outcome["frames"]), 6),
        "classifySeconds": {category: round(sum(frame["classifications"].get(category, {}).get("seconds", 0)
                                                  for frame in outcome["frames"]), 6) for category in CATEGORIES}}
    outcome.update(_memory())
    outcome["workerSeconds"] = round(time.monotonic() - started, 6)
    outcome["passed"] = (not outcome["errors"] and outcome["decodedPositions"] == 4
                          and outcome["classificationCount"] == 8
                          and [frame["position"] for frame in outcome["frames"]] == POSITIONS)
    return outcome


async def run_profile(root: Path, source: Path, settings: dict) -> dict:
    """Profile real implementations without touching the production app or DB."""
    root = Path(root).resolve()
    supplied_source = Path(source)
    if supplied_source.is_symlink():
        raise ValueError("The profile source must not be a symlink.")
    source = supplied_source.resolve(strict=True)
    if source.is_relative_to((Path.home() / "Desktop" / "视频抽帧").resolve()):
        raise ValueError("The profile requires the disposable rehearsal copy.")
    root.mkdir(parents=True, exist_ok=True)
    python = str(settings["workerPythonPath"])
    # Deliberately do not pass inherited tokens, proxies or platform credentials.
    env = {key: value for key, value in os.environ.items()
           if key in {"PATH", "HOME", "TMPDIR", "LANG", "LC_ALL", "SYSTEMROOT"}}
    env.update(PYTHONDONTWRITEBYTECODE="1", PYTHONNOUSERSITE="1", OMP_NUM_THREADS="2")
    request = {"source": str(source), "settings": dict(settings)}
    started = time.monotonic()
    process = await asyncio.create_subprocess_exec(python, "-B", str(Path(__file__).resolve()), "--worker",
        stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL,
        cwd=PROJECT, env=env)
    try:
        output, _ = await asyncio.wait_for(process.communicate(json.dumps(request).encode()), 180)
    except BaseException:
        if process.returncode is None:
            process.kill()
            await process.wait()
        raise
    try:
        outcome = json.loads(output)
    except (ValueError, UnicodeError):
        outcome = {"passed": False, "errors": [{"stage": "worker", "type": "invalid_profile_response"}]}
    outcome.update(exitCode=process.returncode, totalSeconds=round(time.monotonic() - started, 6))
    outcome["passed"] = bool(outcome.get("passed") and process.returncode == 0)
    (root / "profile-result.json").write_text(json.dumps(outcome, ensure_ascii=False, indent=2), encoding="utf-8")
    return outcome


if __name__ == "__main__":
    if sys.argv[1:] != ["--worker"]:
        raise SystemExit("Import run_profile(root, copied_video, settings) from the rehearsal runner.")
    try:
        result = _worker(json.loads(sys.stdin.read()))
    except Exception as error:
        result = {"passed": False, "errors": [{"stage": "worker", "type": type(error).__name__}]}
    sys.stdout.write(json.dumps(result, ensure_ascii=False) + "\n")
