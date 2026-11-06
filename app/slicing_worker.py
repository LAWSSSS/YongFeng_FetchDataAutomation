"""Isolated CPU inference worker; decoded frames are passed through memory only."""
from __future__ import annotations

import json
import math
import os
import subprocess
import sys
import hashlib
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from .media_validation import MediaValidationError, verify_video_tail
from .frame_decode import decode_position


class FrameExtractionError(RuntimeError):
    def __init__(self, code="frame_extract_failed", timestamp=None):
        super().__init__(code)
        self.code = code
        self.timestamp = timestamp


def emit(value):
    sys.stdout.write(json.dumps(value, ensure_ascii=False, separators=(",", ":")) + "\n")
    sys.stdout.flush()


def jpeg_frames(command):
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, bufsize=1024 * 1024)
    assert process.stdout is not None
    pending = bytearray()
    try:
        while True:
            # BufferedReader.read(size) may wait until the full 1 MiB arrives
            # or FFmpeg reaches EOF. Sparse sampling can produce only a few
            # JPEGs across a long source, so that stalls observations until
            # the whole video has been decoded. read1 returns available pipe
            # bytes immediately while still keeping memory bounded.
            chunk = process.stdout.read1(64 * 1024)
            if chunk:
                pending.extend(chunk)
            else:
                if pending:
                    raise FrameExtractionError()
                break
            while True:
                start = pending.find(b"\xff\xd8")
                if start < 0:
                    if len(pending) > 1:
                        del pending[:-1]
                    break
                end = pending.find(b"\xff\xd9", start + 2)
                if end < 0:
                    if start:
                        del pending[:start]
                    break
                yield bytes(pending[start:end + 2])
                del pending[:end + 2]
        if process.wait() != 0:
            raise FrameExtractionError()
    finally:
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()


def probe(path, ffprobe):
    result = subprocess.run([ffprobe, "-v", "error", "-select_streams", "v:0", "-show_entries",
        "format=duration,size:stream=width,height,avg_frame_rate,r_frame_rate,start_time", "-of", "json", path],
        capture_output=True, timeout=40, check=False)
    if result.returncode:
        raise RuntimeError("video_probe_failed")
    data = json.loads(result.stdout)
    streams = data.get("streams") or []
    fmt = data.get("format") or {}
    if not streams or float(fmt.get("duration", 0)) <= 0:
        raise RuntimeError("video_probe_failed")
    stream = streams[0]
    rate = stream.get("avg_frame_rate") or stream.get("r_frame_rate") or "25/1"
    try:
        numerator, denominator = str(rate).split("/", 1)
        frame_rate = float(numerator) / float(denominator)
    except (ValueError, ZeroDivisionError):
        frame_rate = 25.0
    return {"duration": float(fmt["duration"]), "size": int(fmt.get("size", 0)),
            "width": int(stream.get("width", 0)), "height": int(stream.get("height", 0)),
            "frameRate": frame_rate if math.isfinite(frame_rate) and frame_rate > 0 else 25.0,
            "streamStartTime": float(stream.get("start_time") or 0)}


def verify_file_version(path, expected, *, check_edges=True):
    if not expected:
        return
    try:
        info = os.stat(path, follow_symlinks=False)
        if (info.st_size, info.st_mtime_ns, info.st_ino, info.st_dev) != (
                expected.get("size"), expected.get("modifiedNs"), expected.get("inode"), expected.get("device")):
            raise FrameExtractionError("source_changed")
        if not check_edges:
            return
        digest = hashlib.sha256()
        with open(path, "rb") as stream:
            digest.update(stream.read(65536))
            if info.st_size > 65536:
                stream.seek(max(0, info.st_size - 65536))
                digest.update(stream.read(65536))
        if digest.hexdigest() != expected.get("edgeHash"):
            raise FrameExtractionError("source_changed")
    except FrameExtractionError:
        raise
    except OSError:
        raise FrameExtractionError("frame_io_error") from None


def extract_requested_frames(path, ffmpeg, metadata, points, *, skip_undecodable=False):
    """Yield requested JPEGs one at a time; never materialize them on disk."""
    if not os.path.isfile(path) or not os.access(path, os.R_OK):
        raise RuntimeError("local_video_missing")
    if not os.path.isfile(ffmpeg) or not os.access(ffmpeg, os.X_OK):
        raise RuntimeError("frame_tool_missing")
    all_points = sorted(set(round(float(value), 6) for values in points.values() for value in values))
    duration, frame_rate = float(metadata["duration"]), float(metadata.get("frameRate") or 25)
    if skip_undecodable:
        expected = metadata.get("_expectedFileVersion")
        verify_file_version(path, expected)
        details = metadata.setdefault("_decodeObservations", {})

        def decode(timestamp):
            verify_file_version(path, expected, check_edges=False)
            try:
                return decode_position(path, ffmpeg, timestamp, metadata, expected=expected,
                    tail=abs(timestamp - max(0, duration - 1 / frame_rate)) < 1e-5)
            except RuntimeError as error:
                raise FrameExtractionError(str(error), timestamp=timestamp) from None

        if all_points:
            # Decode one frame ahead of inference. Keep seek order and bound
            # each video's lookahead to one additional image in memory.
            with ThreadPoolExecutor(max_workers=1) as decoder:
                pending = decoder.submit(decode, all_points[0])
                for index, timestamp in enumerate(all_points):
                    decoded = pending.result()
                    verify_file_version(path, expected, check_edges=False)
                    if index + 1 < len(all_points):
                        pending = decoder.submit(decode, all_points[index + 1])
                    details[timestamp] = decoded.observation()
                    yield timestamp, decoded.image
        verify_file_version(path, expected)
        return
    regular_by_category = {
        category: sorted(set(value for value in values if abs(value - round(value)) < 1e-6
                            and value < duration - 1.0 / frame_rate / 2))
        for category, values in points.items()
    }
    regular = sorted(set(value for values in regular_by_category.values() for value in values))
    tails = [value for value in all_points if value not in regular]
    # Coarse sampling should decode only the nearest keyframe window, not the
    # entire 4K source. A position shared by two models is still read once.
    sparse = bool(regular) and (len(regular) == 1 or regular[0] != 0 or
        min(second - first for first, second in zip(regular, regular[1:])) >= 5 or
        any(len({round(b - a, 6) for a, b in zip(values, values[1:])}) > 1
            for values in regular_by_category.values()))
    # Sparse sample plans are decoded independently to keep timestamps attached
    # to their requested positions.
    if sparse:
        for timestamp in regular:
            command = [ffmpeg, "-nostdin", "-hide_banner", "-loglevel", "error", "-threads", "2",
                       "-ss", f"{timestamp:.6f}", "-i", path, "-map", "0:v:0", "-an", "-sn",
                       "-frames:v", "1", "-threads", "2", "-f", "image2pipe", "-vcodec", "mjpeg", "pipe:1"]
            image_bytes = None
            try:
                for decoded in jpeg_frames(command):
                    if image_bytes is not None:
                        raise RuntimeError("frame_extract_failed")
                    image_bytes = decoded
            except FrameExtractionError:
                raise FrameExtractionError(timestamp=timestamp) from None
            if image_bytes is None or not image_bytes.startswith(b"\xff\xd8") or not image_bytes.endswith(b"\xff\xd9"):
                raise FrameExtractionError(timestamp=timestamp)
            yield timestamp, image_bytes
            del image_bytes
        regular = []
    interval = 0
    for value in regular:
        integer = int(round(value))
        interval = math.gcd(interval, integer) if integer else interval
    interval = max(1, interval)
    if regular:
        indices = sorted(set(max(0, int(round(value / interval))) for value in regular))
        # A long per-frame OR expression exceeds FFmpeg's expression parser
        # limit (the common 30-second plan creates >1,000 characters). Use
        # the sampling-grid periods to express the union compactly instead.
        # For the usual single shared cadence, all indices are contiguous and
        # no select filter is needed at all.
        contiguous = indices == list(range(indices[0], indices[-1] + 1))
        vf = f"fps=1/{interval}:round=up"
        if not contiguous:
            periods = set()
            include_zero_only = False
            for values in regular_by_category.values():
                if len(values) < 2:
                    include_zero_only = include_zero_only or bool(values)
                    continue
                period = 0
                for first, second in zip(values, values[1:]):
                    period = math.gcd(period, int(round(second - first)))
                ratio = max(1, period // interval)
                if ratio > 1:
                    periods.add(ratio)
            terms = [f"not(mod(n\\,{ratio}))" for ratio in sorted(periods)]
            if include_zero_only:
                terms.append("eq(n\\,0)")
            if not terms:
                raise RuntimeError("sample_plan_invalid")
            vf += ",select=" + "+".join(terms)
        command = [ffmpeg, "-hide_banner", "-loglevel", "error", "-i", path, "-vf", vf,
                   "-fps_mode", "vfr", "-frames:v", str(len(indices)),
                   "-f", "image2pipe", "-vcodec", "mjpeg", "pipe:1"]
        count = 0
        for image_bytes in jpeg_frames(command):
            if count >= len(indices):
                del image_bytes
                break
            timestamp = round(indices[count] * interval, 6)
            count += 1
            yield timestamp, image_bytes
            del image_bytes
        if count != len(indices):
            raise FrameExtractionError(timestamp=indices[count] * interval)
    for timestamp in tails:
        # Seeking to duration - 1/avg_frame_rate can land a few microseconds
        # after the final PTS on variable-rate files. Read a small window from
        # EOF and retain its final decoded JPEG instead of trusting that
        # floating-point seek position to identify the last frame.
        tail_window = min(1.0, max(0.05, duration))
        command = [ffmpeg, "-nostdin", "-hide_banner", "-loglevel", "error", "-threads", "2",
                   "-sseof", f"-{tail_window:.6f}", "-i", path, "-map", "0:v:0", "-an", "-sn",
                   "-threads", "2", "-f", "image2pipe", "-vcodec", "mjpeg", "pipe:1"]
        image_bytes = None
        try:
            for extra in jpeg_frames(command):
                if image_bytes is not None:
                    del image_bytes
                image_bytes = extra
        except FrameExtractionError:
            raise FrameExtractionError("media_incomplete", timestamp=timestamp) from None
        if image_bytes is None:
            raise FrameExtractionError("media_incomplete", timestamp=timestamp)
        yield timestamp, image_bytes
        del image_bytes


class Models:
    def __init__(self, settings, categories):
        self.settings = settings
        self.categories = categories
        self.magnet = None
        self.bed = None
        self.bed_threshold = None

    def load(self, category):
        if category == "magnet":
            deploy = Path(self.settings["magnetDeployDirectory"]).expanduser().resolve()
            model_path = Path(self.settings["magnetModelPath"]).expanduser().resolve()
            if not model_path.is_file():
                raise RuntimeError("magnet_model_missing")
            import importlib.util
            import onnxruntime as ort
            spec = importlib.util.spec_from_file_location("slicing_magnet_infer", deploy / "infer.py")
            if not spec or not spec.loader:
                raise RuntimeError("magnet_model_missing")
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            options = ort.SessionOptions()
            options.intra_op_num_threads = 2
            options.inter_op_num_threads = 1
            self.magnet = (module, ort.InferenceSession(str(model_path), sess_options=options,
                                                       providers=["CPUExecutionProvider"]))
        if category == "bed":
            deploy = Path(self.settings["bedDeployDirectory"]).expanduser().resolve()
            model_path = Path(self.settings["bedModelPath"]).expanduser().resolve()
            geometry_path = Path(self.settings["bedGeometryPath"]).expanduser().resolve()
            if not model_path.is_file() or not geometry_path.is_file():
                raise RuntimeError("bed_model_missing")
            sys.path.insert(0, str(deploy))
            import cv2
            import numpy as np
            import torch
            from training.supervised import PresenceNet
            torch.backends.mkldnn.enabled = False
            torch.set_num_threads(2)
            cv2.setNumThreads(1)
            checkpoint = torch.load(model_path, map_location="cpu", weights_only=True)
            network = PresenceNet(input_pool=checkpoint.get("input_pool", 2))
            network.load_state_dict(checkpoint["state_dict"])
            network.eval()
            geometry_npz = np.load(geometry_path)
            geometry = {key: geometry_npz[key] for key in ("template", "wall_mask", "roi_mask", "bounds")}
            self.bed_threshold = float(checkpoint["score_threshold"])
            self.bed = (torch, cv2, np, network, geometry, deploy)

    def classify(self, image_bytes, category):
        if category == "magnet":
            from PIL import Image
            import numpy as np
            module, session = self.magnet
            with Image.open(__import__("io").BytesIO(image_bytes)) as image:
                rgb = image.convert("RGB")
            boxes, scores = module.predict(session, rgb, 640, float(self.settings.get("magnetConfidence", 0.25)), 0.70, 300)
            result = {"label": "target" if len(scores) else "clear", "score": float(scores.max()) if len(scores) else 0.0,
                      "width": rgb.width, "height": rgb.height, "threshold": float(self.settings.get("magnetConfidence", 0.25))}
            rgb.close()
            return result
        torch, cv2, np, network, geometry, deploy = self.bed
        image = cv2.imdecode(np.frombuffer(image_bytes, dtype=np.uint8), cv2.IMREAD_COLOR)
        if image is None:
            raise RuntimeError("frame_decode_failed")
        height, width = image.shape[:2]
        if (width, height) != (3840, 2160):
            del image
            return {"label": "uncertain", "code": "bed_resolution", "score": None, "width": width, "height": height}
        from training.supervised import prepare_image
        crop, _mask, quality = prepare_image(image, geometry)
        del image
        if quality.get("quality") != "ok":
            del crop
            return {"label": "uncertain", "code": "bed_alignment_uncertain", "score": None,
                    "width": width, "height": height, "quality": quality}
        tensor = torch.from_numpy(crop).permute(2, 0, 1)[None].contiguous().float() / 255
        del crop
        with torch.no_grad():
            score = float(torch.sigmoid(network(tensor))[0])
        del tensor
        return {"label": "target" if score > self.bed_threshold else "clear", "score": score,
                "width": width, "height": height, "threshold": self.bed_threshold, "quality": quality}


def process(request):
    path = str(Path(request["filePath"]).expanduser().resolve())
    ffmpeg = request["frameToolPath"]
    if not os.path.isfile(path) or not os.access(path, os.R_OK):
        emit({"event": "error", "category": "both", "code": "local_video_missing"})
        return
    if not os.path.isfile(ffmpeg) or not os.access(ffmpeg, os.X_OK):
        emit({"event": "error", "category": "both", "code": "frame_tool_missing"})
        return
    ffprobe = str(Path(ffmpeg).with_name("ffprobe"))
    emit({'event': 'stage', 'stage': 'preparing_video'})
    cached = request.get('metadata') or {}
    expected = request.get('expectedFileVersion') or {}
    valid_cached = bool(expected and cached.get('size') == expected.get('size')
            and isinstance(cached.get('duration'), (int, float)) and math.isfinite(cached['duration'])
            and cached['duration'] > 0 and cached.get('width', 0) > 0 and cached.get('height', 0) > 0)
    if valid_cached and (not request.get('skipUndecodableSamples') or 'streamStartTime' in cached):
        verify_file_version(path, expected, check_edges=False)
        metadata = dict(cached)
    else:
        metadata = probe(path, ffprobe)
    metadata["_expectedFileVersion"] = request.get("expectedFileVersion")
    settings = request["settings"]
    points = request["points"]
    categories = {category: sorted(set(round(float(value), 6) for value in values))
                  for category, values in points.items() if values}
    if not categories:
        emit({"event": "complete", "metadata": metadata})
        return
    emit({"event": "stage", "stage": "checking_media", "total": len(set(
        value for values in categories.values() for value in values))})
    skip_undecodable = bool(request.get("skipUndecodableSamples"))
    if not skip_undecodable and not (request.get('verifiedMedia') and valid_cached):
        try:
            verify_video_tail(path, ffmpeg, metadata["duration"])
        except MediaValidationError as error:
            emit({"event": "error", "category": "both", "code": str(error),
                  "stage": "checking_media", "time": metadata["duration"]})
            return
    emit({"event": "stage", "stage": "loading_model"})
    models = Models(settings, set(categories))
    available = set(categories)
    for category in tuple(available):
        try:
            models.load(category)
        except ImportError:
            emit({"event": "error", "category": category, "code": "inference_environment_missing"})
            available.discard(category)
        except Exception as error:
            code = str(error) if str(error) in {"magnet_model_missing", "bed_model_missing"} else "model_load_failed"
            emit({"event": "error", "category": category, "code": code})
            available.discard(category)
    categories = {category: values for category, values in categories.items() if category in available}
    if not categories:
        emit({"event": "complete", "metadata": metadata, "observations": 0})
        return
    observations = 0
    emit({"event": "stage", "stage": "sampling"})
    try:
        frames = extract_requested_frames(path, ffmpeg, metadata, categories,
                                          skip_undecodable=skip_undecodable)
        for timestamp, image_bytes in frames:
            active = [category for category, values in categories.items()
                      if category in available and any(abs(value - timestamp) < 0.05 for value in values)]
            if image_bytes is None:
                for category in active:
                    emit({"event": "observation", "category": category, "time": timestamp,
                          "label": "unavailable", "score": None, "code": "decode_unavailable",
                          **metadata.get("_decodeObservations", {}).get(timestamp, {})})
                    observations += 1
                continue
            for category in active:
                try:
                    result = models.classify(image_bytes, category)
                except Exception:
                    available.discard(category)
                    emit({"event": "error", "category": category, "code": "inference_failed"})
                    continue
                emit({"event": "observation", "category": category, "time": timestamp, **metadata.get("_decodeObservations", {}).get(timestamp, {}), **result})
                observations += 1
            del image_bytes
            if not available:
                break
    except RuntimeError as error:
        allowed = {"local_video_missing", "frame_tool_missing", "media_incomplete", "frame_timeout",
                   "frame_tool_error", "frame_io_error", "source_changed", "frame_timestamp_missing"}
        code = str(error) if str(error) in allowed else "frame_extract_failed"
        emit({"event": "error", "category": "both", "code": code, "stage": "sampling",
              "time": getattr(error, "timestamp", None)})
        return
    emit({"event": "complete", "metadata": metadata, "observations": observations})


def main():
    line = sys.stdin.readline()
    if not line:
        return
    try:
        request = json.loads(line)
        process(request)
    except Exception as error:
        code = str(error) if str(error) in {"video_probe_failed", "frame_extract_failed", "frame_timeout",
            "frame_tool_error", "frame_io_error", "source_changed", "local_video_missing", "frame_tool_missing"} else "worker_failed"
        emit({"event": "error", "category": "both", "code": code})


if __name__ == "__main__":
    main()
