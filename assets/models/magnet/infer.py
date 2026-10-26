#!/usr/bin/env python3
"""Lightweight CPU inference for the Yongfeng cipandiao ONNX detector."""
from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import onnxruntime as ort
from PIL import Image, ImageDraw

IMAGE_SUFFIXES = {".jpg", ".jpeg", ".png", ".bmp", ".webp"}


def letterbox(image: Image.Image, size: int):
    width, height = image.size
    scale = min(size / width, size / height)
    new_width = int(round(width * scale))
    new_height = int(round(height * scale))
    resized = image.resize((new_width, new_height), Image.Resampling.BILINEAR)
    pad_x = (size - new_width) / 2
    pad_y = (size - new_height) / 2
    left = int(round(pad_x - 0.1))
    top = int(round(pad_y - 0.1))
    canvas = Image.new("RGB", (size, size), (114, 114, 114))
    canvas.paste(resized, (left, top))
    array = np.asarray(canvas, dtype=np.float32) / 255.0
    tensor = np.transpose(array, (2, 0, 1))[None, ...]
    return tensor, scale, left, top


def nms(boxes: np.ndarray, scores: np.ndarray, threshold: float, max_det: int):
    order = np.argsort(scores)[::-1]
    kept = []
    while order.size and len(kept) < max_det:
        current = int(order[0])
        kept.append(current)
        rest = order[1:]
        if not rest.size:
            break
        x1 = np.maximum(boxes[current, 0], boxes[rest, 0])
        y1 = np.maximum(boxes[current, 1], boxes[rest, 1])
        x2 = np.minimum(boxes[current, 2], boxes[rest, 2])
        y2 = np.minimum(boxes[current, 3], boxes[rest, 3])
        intersection = np.maximum(0.0, x2 - x1) * np.maximum(0.0, y2 - y1)
        area_current = max(0.0, boxes[current, 2] - boxes[current, 0]) * max(
            0.0, boxes[current, 3] - boxes[current, 1]
        )
        area_rest = np.maximum(0.0, boxes[rest, 2] - boxes[rest, 0]) * np.maximum(
            0.0, boxes[rest, 3] - boxes[rest, 1]
        )
        iou = intersection / (area_current + area_rest - intersection + 1e-9)
        order = rest[iou <= threshold]
    return kept


def predict(session, image: Image.Image, size: int, conf_threshold: float,
            iou_threshold: float, max_det: int):
    original_width, original_height = image.size
    tensor, scale, left, top = letterbox(image, size)
    input_name = session.get_inputs()[0].name
    output = session.run(None, {input_name: tensor})[0][0]
    # This export returns [5, 8400]: xywh followed by one class confidence.
    predictions = output.T if output.shape[0] == 5 else output
    scores = predictions[:, 4]
    selected = scores >= conf_threshold
    predictions = predictions[selected]
    scores = scores[selected]
    if not scores.size:
        return np.empty((0, 4), dtype=np.float32), scores

    boxes = predictions[:, :4].copy()
    boxes[:, 0] = predictions[:, 0] - predictions[:, 2] / 2
    boxes[:, 1] = predictions[:, 1] - predictions[:, 3] / 2
    boxes[:, 2] = predictions[:, 0] + predictions[:, 2] / 2
    boxes[:, 3] = predictions[:, 1] + predictions[:, 3] / 2
    keep = nms(boxes, scores, iou_threshold, max_det)
    boxes = boxes[keep]
    scores = scores[keep]
    boxes[:, [0, 2]] = (boxes[:, [0, 2]] - left) / scale
    boxes[:, [1, 3]] = (boxes[:, [1, 3]] - top) / scale
    boxes[:, [0, 2]] = np.clip(boxes[:, [0, 2]], 0, original_width)
    boxes[:, [1, 3]] = np.clip(boxes[:, [1, 3]], 0, original_height)
    return boxes, scores


def list_images(source: Path):
    if source.is_file():
        if source.suffix.lower() not in IMAGE_SUFFIXES:
            raise ValueError("Input must be an image or a folder of images.")
        return [source]
    if source.is_dir():
        images = sorted(p for p in source.iterdir() if p.is_file() and not p.name.startswith(".") and p.suffix.lower() in IMAGE_SUFFIXES)
        if not images:
            raise FileNotFoundError("No supported images found in: {}".format(source))
        return images
    raise FileNotFoundError("Input does not exist: {}".format(source))


def main():
    root = Path(__file__).resolve().parent
    parser = argparse.ArgumentParser(description="磁盘吊图片推理（ONNX Runtime CPU）")
    parser.add_argument("source", type=Path, help="一张图片或一个图片文件夹")
    parser.add_argument("--model", type=Path, default=root / "best.onnx")
    parser.add_argument("--output", type=Path, default=root / "predictions")
    parser.add_argument("--imgsz", type=int, default=640)
    parser.add_argument("--conf", type=float, default=0.25)
    parser.add_argument("--iou", type=float, default=0.70)
    parser.add_argument("--max-det", type=int, default=300)
    args = parser.parse_args()

    if not args.model.is_file():
        raise FileNotFoundError("Model not found: {}".format(args.model))
    session = ort.InferenceSession(str(args.model), providers=["CPUExecutionProvider"])
    args.output.mkdir(parents=True, exist_ok=True)

    for image_path in list_images(args.source):
        with Image.open(image_path) as source_image:
            image = source_image.convert("RGB")
        boxes, scores = predict(session, image, args.imgsz, args.conf, args.iou, args.max_det)
        draw = ImageDraw.Draw(image)
        for box, score in zip(boxes, scores):
            x1, y1, x2, y2 = [int(round(v)) for v in box]
            draw.rectangle((x1, y1, x2, y2), outline=(255, 32, 32), width=max(2, image.width // 1000))
            label = "cipandiao {:.2f}".format(float(score))
            draw.text((x1 + 3, max(0, y1 - 16)), label, fill=(255, 32, 32))
        output_path = args.output / (image_path.stem + "_pred.jpg")
        image.save(output_path, quality=92)
        print("{}: {} detection(s) -> {}".format(image_path.name, len(scores), output_path))


if __name__ == "__main__":
    main()
