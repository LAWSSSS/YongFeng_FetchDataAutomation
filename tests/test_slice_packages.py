import zipfile
from pathlib import Path

import pytest

from app.main import create_app
from app.slice_export import photo_names, slice_timestamps
from app.upload_audit import name_key


JPEG = b"\xff\xd8\xff\xd9"


def reader(path, ffmpeg, timestamp):
    return JPEG


class Remote:
    def __init__(self):
        self.jobs_by_dataset = {"27892": [], "27893": []}
        self.submitted = []
        self.rows = []

    async def signature(self, dataset, binding=None):
        return {"endpoint": "https://storage.example", "bucket": "slices", "prefix": "in/",
                "accessKey": "a", "secretKey": "b"}

    async def jobs(self, dataset):
        return list(self.jobs_by_dataset[dataset])

    async def submit(self, dataset, keys):
        assert len(keys) == 1
        self.submitted.append((dataset, keys[0]))
        job = {"id": str(700 + len(self.submitted)), "status": "1", "dataset": dataset,
               "config": {"source": {"files": [{"key": keys[0]}]}}}
        self.jobs_by_dataset[dataset].append(job)
        return job["id"]

    async def evidence(self, dataset, job):
        return {"evidence": {}, "reason": ""}

    async def images(self, dataset):
        return list(self.rows)


class ImmediateWorker:
    async def run(self, item, credentials, callback, operation="upload"):
        await callback({"event": "complete", "size": item["size"], "etag": "etag"})


def ready_item(tmp_path, identity="1"):
    path = tmp_path / "鲁A12345_20261005050441_abc.mp4"
    path.write_bytes(b"video")
    from app.slicing import source_version
    return {"id": f"item-{identity}", "sourceDatasetId": "27912", "sourceVideoId": identity,
            "name": path.name, "path": str(path), "fileVersion": source_version(path),
            "state": "platform_processing", "revision": 1,
            "categories": {"bed": {"state": "ready", "intervals": [{"start": 0, "end": 2}],
                                   "intervalSeconds": 1},
                           "magnet": {"state": "not_applicable"}}}


@pytest.fixture
def manager(tmp_path):
    app = create_app(data_dir=tmp_path / "data")
    slicing = app.state.slicing
    slicing.slice_reader = reader
    slicing.packages.remote = Remote()
    slicing.packages.worker = ImmediateWorker()
    slicing.auth.access = "isolated"
    return app, slicing


@pytest.mark.asyncio
async def test_one_video_runs_bed_and_magnet_in_the_same_pass(manager, monkeypatch):
    app, slicing = manager
    calls = []

    async def worker(item, metadata, points, keys, phase):
        calls.append(set(points))
        for name, values in points.items():
            for value in values:
                slicing.store.save("observations", {"id": keys[name][value], "time": value, "label": "clear", "score": 0.1})

    monkeypatch.setattr(slicing, "_run_worker", worker)
    monkeypatch.setattr(slicing, "_model_fingerprint", lambda name: name)
    item = ready_item(Path(slicing.data_dir))
    item["metadata"] = {"duration": 60.0, "frameRate": 25}
    item["categories"]["bed"] = {"state": "pending", "intervals": []}
    slicing.store.save("items", item)
    await slicing._analyze_item(slicing.store.get("items", item["id"]))
    assert calls and calls[0] == {"bed", "magnet"}
    saved = slicing.store.get("items", item["id"])
    assert set(saved["categories"]) >= {"bed", "magnet"}
    assert saved["categories"]["magnet"]["state"] in {"ready", "no_target", "attention"}


def test_new_bed_videos_aim_at_180_frames_and_magnet_stays_at_150():
    from app.slicing_core import category_image_target
    assert category_image_target({}, "bed") == 180
    assert category_image_target({"bedTargetImages": 160}, "bed") == 160
    assert category_image_target({"targetImages": 150}, "magnet") == 150


def test_timestamps_follow_the_inclusive_interval_grid():
    assert slice_timestamps([{"start": 0, "end": 10}], 5) == [0, 5, 10]
    assert photo_names("鲁A12345_20261005050441_abc.mp4", 2) == [
        "鲁A12345_20261005_050441_001.jpg", "鲁A12345_20261005_050441_002.jpg"]


@pytest.mark.asyncio
async def test_review_keeps_the_archive_local_until_confirmed(manager):
    app, slicing = manager
    slicing.store.settings["reviewBeforeUpload"] = True
    item = ready_item(Path(slicing.data_dir))
    item["sourceDatasetId"] = "27944"
    item["categories"] = {"magnet": {"state": "ready", "intervals": [{"start": 0, "end": 2}], "intervalSeconds": 1},
                          "bed": {"state": "not_applicable"}}
    slicing.store.save("items", item)
    await slicing._export_ready(slicing.store.get("items", item["id"]))
    saved = slicing.store.get("items", item["id"])
    package = slicing.packages.store.get("items", saved["categories"]["magnet"]["packageId"])
    assert saved["categories"]["magnet"]["state"] == "review"
    assert package["state"] == "review" and slicing.packages.remote.submitted == []
    with zipfile.ZipFile(package["path"]) as archive:
        assert archive.namelist() == [f"image/{name}" for name in package["images"]]
    await slicing._export_ready(slicing.store.get("items", item["id"]))
    assert len(slicing.packages.store.list("items")) == 1
    slicing.confirm_package(package["id"])
    assert slicing.packages.store.get("items", package["id"])["state"] == "queued"
    await slicing.packages.tick()
    if slicing.packages.running:
        await __import__("asyncio").gather(*slicing.packages.running.values())
    await slicing.packages.tick()
    uploaded = slicing.packages.store.get("items", package["id"])
    assert slicing.packages.remote.submitted == [("27893", uploaded["object"]["key"])]
    await app.state.slicing.shutdown()


@pytest.mark.asyncio
async def test_disabled_review_enqueues_immediately_and_imports_one_at_a_time(manager, tmp_path):
    app, slicing = manager
    slicing.store.settings["reviewBeforeUpload"] = False
    first = ready_item(tmp_path, "1")
    second = ready_item(tmp_path, "2")
    second["id"] = "item-2"
    second["name"] = "鲁B00001_20261005060606_def.mp4"
    path = tmp_path / second["name"]
    path.write_bytes(b"video")
    from app.slicing import source_version
    second["path"] = str(path)
    second["fileVersion"] = source_version(path)
    for item in (first, second):
        slicing.store.save("items", item)
        await slicing._export_ready(slicing.store.get("items", item["id"]))
    assert {row["state"] for row in slicing.packages.store.list("items")} == {"queued"}
    await slicing.packages.tick()
    if slicing.packages.running:
        await __import__("asyncio").gather(*slicing.packages.running.values())
    await slicing.packages.tick()
    if slicing.packages.running:
        await __import__("asyncio").gather(*slicing.packages.running.values())
    await slicing.packages.tick()
    assert len(slicing.packages.remote.submitted) == 1
    waiting = [row for row in slicing.packages.store.list("items") if row["state"] == "uploaded"]
    importing = [row for row in slicing.packages.store.list("items") if row["state"] == "importing"]
    assert len(importing) == 1 and len(waiting) == 1
    job = slicing.packages.remote.jobs_by_dataset["27892"][0]
    job["status"] = "3"
    stem = Path(importing[0]["name"]).stem
    slicing.packages.remote.rows = [{"name": f"{stem}_001.jpg"}]
    await slicing.packages.tick()
    assert slicing.packages.store.get("items", importing[0]["id"])["state"] == "complete"
    assert name_key(stem)
    await app.state.slicing.shutdown()
