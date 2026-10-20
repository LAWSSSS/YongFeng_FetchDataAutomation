"""Group slice archives the same way uploaded videos are sorted."""
import os
from pathlib import Path

HELD = {"importing", "submitting", "uploading", "uploaded", "attention", "review"}


def side_status(state):
    if state == "complete":
        return "uploaded"
    if state in HELD:
        return "held"
    return "missing"


def classify_pair(bed_state, magnet_state):
    """Match the video rule: both imported, only one side missing, both absent, or still undecided."""
    bed, magnet = side_status(bed_state), side_status(magnet_state)
    if bed == "held" or magnet == "held":
        return "held"
    if bed == magnet:
        return "uploaded" if bed == "uploaded" else "both_missing"
    return "missing_magnet" if magnet == "missing" else "missing_bed"


def group_packages(items):
    groups = {}
    for item in items:
        if item.get("state") == "dismissed":
            continue
        key = str(item.get("sourceVideoId") or Path(item.get("name", "")).stem)
        groups.setdefault(key, {"name": item.get("name", key), "packages": {}})
        groups[key]["packages"][item.get("category")] = item
        if item.get("name"):
            groups[key]["name"] = item["name"]
    return groups


def plan_moves(items, root):
    root = Path(root)
    folders = {
        "uploaded": root / "已上传",
        "missing_magnet": root / "磁盘吊未上传",
        "missing_bed": root / "车斗未上传",
    }
    rows = []
    for key, group in group_packages(items).items():
        packages = group["packages"]
        kind = classify_pair(packages.get("bed", {}).get("state"), packages.get("magnet", {}).get("state"))
        files = []
        for package in packages.values():
            path = package.get("path")
            if not path:
                continue
            source = Path(path)
            destination = folders[kind] / source.name if kind in folders else None
            files.append({"category": package.get("category"), "source": str(source),
                          "destination": str(destination) if destination else "", "name": source.name})
        rows.append({"id": key, "name": group["name"], "kind": kind, "files": files,
                     "message": {
                         "uploaded": "两个图片集都已导入。",
                         "missing_magnet": "只缺磁盘吊导入。",
                         "missing_bed": "只缺车斗导入。",
                         "both_missing": "两处都未导入，留在原处。",
                         "held": "仍在上传、导入或待校对，留在原处。",
                     }[kind]})
    return {"root": str(root), "folders": {key: str(value) for key, value in folders.items()}, "items": rows}


def apply_moves(plan):
    moved, skipped = [], []
    for row in plan["items"]:
        if row["kind"] not in {"uploaded", "missing_magnet", "missing_bed"}:
            skipped.append(row["id"])
            continue
        for file in row["files"]:
            source, destination = Path(file["source"]), Path(file["destination"])
            if not source.is_file():
                skipped.append(file["name"])
                continue
            if destination.exists():
                skipped.append(file["name"])
                continue
            destination.parent.mkdir(parents=True, exist_ok=True)
            os.replace(source, destination)
            moved.append(str(destination))
    return {"moved": moved, "skipped": skipped}
