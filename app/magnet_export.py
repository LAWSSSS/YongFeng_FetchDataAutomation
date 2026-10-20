"""Keep one disk-crane model loaded and label batches of JPEG files."""
import json
import sys
from pathlib import Path


def main():
    from .slicing_worker import Models
    request = json.loads(sys.stdin.readline() or "{}")
    models = Models(request.get("settings") or {}, {"magnet"})
    try:
        models.load("magnet")
    except Exception as error:
        code = str(error) if str(error) in {"magnet_model_missing"} else "model_load_failed"
        sys.stdout.write(json.dumps({"ready": False, "code": code}) + "\n")
        sys.stdout.flush()
        return 1
    sys.stdout.write(json.dumps({"ready": True}) + "\n")
    sys.stdout.flush()
    for line in sys.stdin:
        if not line.strip():
            continue
        batch = json.loads(line)
        if batch.get("stop"):
            break
        labels = []
        for path in batch.get("paths") or []:
            label = models.classify(Path(path).read_bytes(), "magnet").get("label")
            labels.append("target" if label == "target" else "other")
        sys.stdout.write(json.dumps(labels) + "\n")
        sys.stdout.flush()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
