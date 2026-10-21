import zipfile
from pathlib import Path

import pytest

from app.main import create_app
from app.slice_export import package_directory, photo_names, slice_timestamps
from app.upload_audit import name_key


JPEG = b"\xff\xd8\xff\xd9"


def test_slice_outputs_are_central_and_grouped_by_model_and_video():
    result = package_directory("/videos/鲁A12345_20261005050441_abc.mp4", "bed",
                               output_root="/output/切片结果", video_id="981")
    assert result == Path("/output/切片结果/车斗/鲁A12345_20261005050441_abc_981")


@pytest.mark.asyncio
async def test_local_slicing_and_package_upload_pause_controls_are_independent(tmp_path):
    from tests.test_slicing_performance import close_app
    app = create_app(data_dir=tmp_path)
    slicing = app.state.slicing
    slicing.store.settings["enabled"] = True

    slicing.pause()
    assert slicing.packages.paused is False

    slicing.store.settings["enabled"] = True
    slicing.packages.pause()
    assert slicing.store.settings["enabled"] is True
    slicing.packages.resume()
    await close_app(app)


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
        job = {"id": str(700 + len(self.submitted)), "type": 1, "status": "1", "dataset": dataset,
               "config": {"source": {"files": [{"key": keys[0]}]}}}
        self.jobs_by_dataset[dataset].append(job)
        return job["id"]

    async def evidence(self, dataset, job):
        return {"evidence": {}, "reason": ""}

    async def images(self, dataset, *, prefix):
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


def test_magnet_image_keeps_only_recognized_frames_and_marks_refine_gaps():
    from app.slice_export import magnet_refine_windows, select_recognized_frames
    samples = [(0, "clear"), (30, "target"), (60, "clear"), (120, "target"), (150, "target")]
    assert select_recognized_frames(samples, target=150, maximum=200) == [30, 120, 150]
    windows = magnet_refine_windows(samples, 30)
    assert (0, 30) in windows and (30, 60) in windows
    assert (60, 120) in windows


def test_photos_and_video_stay_until_both_categories_are_imported(tmp_path):
    from app.main import create_app
    app = create_app(data_dir=tmp_path / "data")
    packages = app.state.slicing.packages
    video = tmp_path / "source.mp4"
    video.write_bytes(b"video")
    photo = tmp_path / "image"
    photo.mkdir()
    (photo / "a.jpg").write_bytes(b"\xff\xd8\xff\xd9")
    archive = tmp_path / "pack.zip"
    archive.write_bytes(b"zip")
    common = {"sourceVideoId": "9", "videoPath": str(video), "imageDir": str(photo), "path": str(archive)}
    packages.store.save("items", {**common, "id": "bed", "category": "bed", "state": "complete", "name": "a.zip"})
    packages.store.save("items", {**common, "id": "magnet", "category": "magnet", "state": "importing", "name": "b.zip"})
    packages.delete_if_both_complete(packages.store.get("items", "bed"))
    assert video.exists() and (photo / "a.jpg").exists()
    packages.store.save("items", {**packages.store.get("items", "magnet"), "state": "complete"})
    packages.delete_if_both_complete(packages.store.get("items", "magnet"))
    # Legacy rows without immutable file receipts must never authorize deletion.
    assert video.exists() and archive.exists() and (photo / "a.jpg").exists()
    from app.audit_moves import source_fingerprint
    for row in packages.store.list("items"):
        row.update(videoFileVersion=source_fingerprint(video), fileVersion=source_fingerprint(archive),
                   images=["a.jpg"], imageVersions={"a.jpg": source_fingerprint(photo / "a.jpg")},
                   imageCount=1, actualImages=1, verifiedJobId="7")
        packages.store.save("items", row)
    unrelated = photo / "user.jpg"
    unrelated.write_bytes(JPEG)
    packages.delete_if_both_complete(packages.store.get("items", "magnet"))
    assert video.exists() and not archive.exists() and not (photo / "a.jpg").exists()
    assert unrelated.exists()


def test_timestamps_follow_the_inclusive_interval_grid():
    assert slice_timestamps([{"start": 0, "end": 10}], 5) == [0, 5, 10]
    assert photo_names("鲁A12345_20261005050441_abc.mp4", 2) == [
        "鲁A12345_20261005_050441_001.jpg", "鲁A12345_20261005_050441_002.jpg"]


@pytest.mark.asyncio
async def test_review_keeps_the_archive_local_until_confirmed(manager):
    app, slicing = manager
    slicing.store.settings["reviewBeforeUpload"] = True
    slicing.magnet_classifier = lambda _blob: "target"
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
        assert archive.namelist() == [f"images/{name}" for name in package["images"]]
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
    import copy
    second = copy.deepcopy(first)
    second["sourceVideoId"] = "2"
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
    for row in slicing.packages.store.list("items"):
        video = Path(row["videoPath"])
        from app.slice_export import package_directory
        assert Path(row["path"]).parent == package_directory(video, "bed",
            output_root=slicing.slice_output_root(), video_id=row["sourceVideoId"])
    await slicing.packages.tick()
    if slicing.packages.running:
        await __import__("asyncio").gather(*slicing.packages.running.values())
    await slicing.packages.tick()
    if slicing.packages.running:
        await __import__("asyncio").gather(*slicing.packages.running.values())
    await slicing.packages.tick()
    assert len(slicing.packages.remote.submitted) == 1
    assert "/simple-" in slicing.packages.remote.submitted[0][1]
    waiting = [row for row in slicing.packages.store.list("items") if row["state"] == "uploaded"]
    importing = [row for row in slicing.packages.store.list("items") if row["state"] == "importing"]
    assert len(importing) == 1 and len(waiting) == 1
    job = slicing.packages.remote.jobs_by_dataset["27892"][0]
    job["status"] = "3"
    stem = Path(importing[0]["name"]).stem
    slicing.packages.remote.rows = [{"id": str(900+i), "name": name, "taskId": job["id"]}
                                   for i,name in enumerate(importing[0]["images"])]
    await slicing.packages.tick()
    assert slicing.packages.store.get("items", importing[0]["id"])["state"] == "complete"
    assert name_key(stem)
    await app.state.slicing.shutdown()
