"""Regression tests use disposable files, databases and a simulated platform only."""
import asyncio
import copy
import sqlite3
import zipfile
from pathlib import Path

import httpx
import pytest

from app.audit_moves import source_fingerprint
from app.file_leases import FileLeases
from app.main import create_app
from app.package_archive import legacy_layout, repair_archive
from app.slice_classify import SliceClassification, apply_moves, plan_moves
from app.slice_export import write_image_archive
from app.slice_packages import SlicePackageManager
from app.submissions import SubmissionLog
from app.upload_audit import AuditError
from app.upload_remote import UploadRemote
from app.videos import Conflict

JPEG = b"\xff\xd8unchanged-model-output\xff\xd9"


class Platform:
    def __init__(self):
        self.rows = []
        self.tasks = []
        self.submits = 0
        self.lose_response = False

    async def jobs(self, dataset):
        return [copy.deepcopy(row) for row in self.tasks if row["dataset"] == dataset]

    async def images(self, dataset, *, prefix):
        return copy.deepcopy(self.rows)

    async def submit(self, dataset, keys):
        self.submits += 1
        self.tasks.append({"id": str(900+self.submits), "type": 1, "dataset": dataset, "status": 1,
                           "config": {"source": {"files": [{"key": key} for key in keys]}}})
        if self.lose_response:
            raise AuditError("offline", "network")
        return self.tasks[-1]["id"]


@pytest.fixture
def packages():
    db = sqlite3.connect(":memory:")
    manager = SlicePackageManager(db, Platform())
    yield manager
    db.close()


def package(tmp_path, identity="bed", category="bed", state="queued", legacy=False):
    path = tmp_path / category / (identity + ".zip")
    path.parent.mkdir(parents=True, exist_ok=True)
    names = ["vehicle_20261020_010000_001.jpg", "vehicle_20261020_010000_002.jpg"]
    with zipfile.ZipFile(path, "w") as archive:
        for name in names:
            archive.writestr(("image/" if legacy else "images/") + name, JPEG)
    return {"id": identity, "dataset": "27892" if category == "bed" else "27893", "category": category,
            "sourceDatasetId": "27912", "sourceVideoId": "42", "path": str(path), "videoPath": str(tmp_path / "video.mp4"),
            "name": path.name, "fileVersion": source_fingerprint(path), "size": path.stat().st_size,
            "state": state, "images": names, "imageCount": len(names), "bytes": 0, "retries": 0}


def job_for(item):
    return {"id": "901", "type": 1, "status": 3, "dataset": item["dataset"],
            "config": {"source": {"files": [{"key": "owned/key.zip"}]}}}


def bind(item):
    item.update(jobId="901", object={"key": "owned/key.zip"}, keys=["owned/key.zip"], beforeImageIds=[])
    return item


def test_archive_root_matches_native_unannotated_import_without_changing_frames(tmp_path):
    target = write_image_archive(tmp_path / "photos.zip", ["a.jpg"], [JPEG])
    with zipfile.ZipFile(target) as archive:
        assert archive.namelist() == ["images/a.jpg"]
        assert archive.read("images/a.jpg") == JPEG


def test_same_size_and_symlink_move_conflicts_keep_both_files(tmp_path):
    source, target = tmp_path / "source.zip", tmp_path / "target.zip"
    source.write_bytes(b"AAAA")
    target.write_bytes(b"BBBB")
    plan = {"items":[{"kind":"uploaded","files":[{"source":str(source),"destination":str(target),
        "name":source.name,"fileVersion":source_fingerprint(source),"state":"planned"}]}]}
    assert apply_moves(plan)["errors"]
    assert source.read_bytes() == b"AAAA" and target.read_bytes() == b"BBBB"
    target.unlink()
    target.symlink_to(source)
    assert apply_moves(plan)["errors"]
    assert source.exists()


def test_startup_does_not_move_existing_packages_or_adopt_same_named_files(packages, tmp_path):
    item = package(tmp_path)
    packages.store.save("items", item)
    SlicePackageManager(packages.store.db, Platform())
    assert packages.store.get("items", item["id"])["path"] == item["path"]
    assert Path(item["path"]).exists()


@pytest.mark.asyncio
async def test_legacy_repair_preserves_jpeg_bytes_and_old_import_receipts(packages, tmp_path):
    item = bind(package(tmp_path, state="attention", legacy=True))
    item["errorCode"] = "archive_layout"
    packages.store.save("items", item)
    assert legacy_layout(item)
    result = await packages.retry(item["id"], "repair-1", item["revision"])
    assert result["state"] == "queued" and "jobId" not in result
    assert result["archiveHistory"][0]["jobId"] == "901"
    assert Path(item["path"]).exists()
    with zipfile.ZipFile(result["path"]) as archive:
        assert archive.namelist() == ["images/" + name for name in item["images"]]
        assert all(archive.read(name) == JPEG for name in archive.namelist())
    replay = await packages.retry(item["id"], "repair-1", item["revision"])
    assert replay["path"] == result["path"] and len(replay["archiveHistory"]) == 1
    assert packages.remote.submits == 0


def test_repair_never_overwrites_an_existing_file(tmp_path):
    item = package(tmp_path, legacy=True)
    destination = Path(item["path"]).with_name("bed_r1.zip")
    destination.write_bytes(b"user archive")
    with pytest.raises(ValueError):
        repair_archive(item)
    assert destination.read_bytes() == b"user archive"


@pytest.mark.asyncio
@pytest.mark.parametrize("rows", [[], [{"id": "1", "name": "unrelated_vehicle.jpg", "taskId": "901"}],
    [{"id": "1", "name": "vehicle_20261020_010000_001.jpg", "taskId": "901"}],
    [{"id": "1", "name": "vehicle_20261020_010000_001.jpg", "taskId": "999"},
     {"id": "2", "name": "vehicle_20261020_010000_002.jpg", "taskId": "999"}]])
async def test_import_requires_every_exact_image_from_this_job(packages, tmp_path, rows):
    item = bind(package(tmp_path, state="importing"))
    packages.store.save("items", item)
    packages.remote.rows = rows
    await packages.verify(item, job_for(item))
    assert packages.store.get("items", item["id"])["state"] == "attention"
    assert Path(item["path"]).exists()


@pytest.mark.asyncio
async def test_old_same_named_images_are_not_new_import_evidence(packages, tmp_path):
    item = bind(package(tmp_path, state="importing"))
    item["beforeImageIds"] = ["1", "2"]
    packages.store.save("items", item)
    packages.remote.rows = [{"id": str(i+1), "name": name} for i,name in enumerate(item["images"])]
    await packages.verify(item, job_for(item))
    assert item["state"] == "attention"
    for row in packages.remote.rows:
        row["taskId"] = "901"
    await packages.verify(item, job_for(item))
    assert item["state"] == "complete" and item["actualImages"] == 2


@pytest.mark.asyncio
async def test_legacy_job_without_snapshot_or_association_stays_for_review(packages, tmp_path):
    item = bind(package(tmp_path, state="importing"))
    item.pop("beforeImageIds")
    packages.store.save("items", item)
    packages.remote.rows = [{"id": str(i+1), "name": name} for i,name in enumerate(item["images"])]
    await packages.verify(item, job_for(item))
    assert item["state"] == "attention"


@pytest.mark.asyncio
async def test_mismatched_job_object_is_not_approved(packages, tmp_path):
    item = bind(package(tmp_path, state="importing"))
    packages.store.save("items", item)
    job = job_for(item)
    job["config"]["source"]["files"][0]["key"] = "other/key.zip"
    await packages.verify(item, job)
    assert item["errorCode"] == "import_identity"


@pytest.mark.asyncio
async def test_serial_ticks_reconcile_lost_response_without_duplicate_submit(packages, tmp_path):
    item = bind(package(tmp_path, state="uploaded"))
    item.pop("jobId")
    packages.store.save("items", item)
    packages.remote.lose_response = True
    gate, entered = asyncio.Event(), asyncio.Event()
    original = packages.remote.images
    async def blocked(dataset, *, prefix):
        entered.set()
        await gate.wait()
        return await original(dataset, prefix=prefix)
    packages.remote.images = blocked
    first = asyncio.create_task(packages.tick())
    await entered.wait()
    await packages.tick()
    assert packages.remote.submits == 0
    gate.set()
    await first
    await packages.tick()
    saved = packages.store.get("items", item["id"])
    assert packages.remote.submits == 1 and saved["jobId"] == "901"


@pytest.mark.asyncio
async def test_pause_during_pre_submit_inventory_cannot_submit_after_pause(packages,tmp_path):
    item=bind(package(tmp_path,state='uploaded'))
    packages.store.save('items',item)
    entered,release=asyncio.Event(),asyncio.Event()
    async def images(dataset,*,prefix):
        entered.set()
        await release.wait()
        return []
    packages.remote.images=images
    task=asyncio.create_task(packages.tick())
    await entered.wait()
    packages.pause()
    release.set()
    await task
    assert packages.remote.submits == 0
    assert packages.store.get('items',item['id'])['state']=='uploaded'


@pytest.mark.asyncio
async def test_uncertain_import_blocks_new_imports_for_just_that_target(packages, tmp_path):
    first = package(tmp_path, "unknown", state="attention")
    first["errorCode"] = "import_uncertain"
    second = bind(package(tmp_path, "next", state="uploaded"))
    packages.store.save("items", first)
    packages.store.save("items", second)
    await packages.tick()
    assert packages.remote.submits == 0


@pytest.mark.asyncio
async def test_multipart_receipts_persist_and_duplicate_progress_is_not_progress(packages, tmp_path):
    item = package(tmp_path)
    packages.store.save("items", item)
    time = [10]
    packages.clock = lambda: time[0]
    await packages.worker_event(item["id"], {"event": "creating"})
    await packages.worker_event(item["id"], {"event": "created", "uploadId": "owned-upload"})
    await packages.worker_event(item["id"], {"event": "part", "number": 1, "size": 2, "etag": "one"})
    time[0] = 100
    await packages.worker_event(item["id"], {"event": "progress", "bytes": 2})
    time[0] = 200
    await packages.worker_event(item["id"], {"event": "progress", "bytes": 2})
    assert packages.runtime[item["id"]]["lastProgress"] == 100
    saved = packages.store.get("items", item["id"])
    assert saved["createAttempted"] and saved["uploadId"] == "owned-upload"
    assert saved["parts"]["1"]["etag"] == "one"


@pytest.mark.parametrize("code", ["auth", "network", "disk", "authorization"])
def test_disconnects_do_not_consume_retry_budget(packages, tmp_path, code):
    item = package(tmp_path)
    packages.store.save("items", item)
    packages.fail(item, code)
    assert item["retries"] == 0 and item["state"] == "queued"


def test_pause_is_durable_and_public_state_contains_no_storage_receipts(packages, tmp_path):
    item = bind(package(tmp_path))
    item["archiveHistory"] = [{"object": {"key": "private-key"}}]
    packages.store.save("items", item)
    packages.pause()
    restored = SlicePackageManager(packages.store.db, Platform())
    assert restored.paused
    public = restored.state()["items"][0]
    assert not {"object", "keys", "archiveHistory", "beforeImageIds"} & public.keys()


def test_classification_groups_actual_file_not_ids_from_different_datasets(tmp_path):
    first = package(tmp_path, state="complete")
    second = package(tmp_path, "magnet", "magnet", "complete")
    second.update(sourceDatasetId="27944", sourceVideoId="700")
    plan = plan_moves([first,second], tmp_path / "sorted")
    assert len(plan["items"]) == 1 and plan["items"][0]["kind"] == "uploaded"
    second["videoPath"] = str(tmp_path / "other.mp4")
    second["sourceVideoId"] = first["sourceVideoId"]
    assert len(plan_moves([first,second], tmp_path / "sorted")["items"]) == 2


def test_classification_preserves_categories_and_does_not_flatten_same_names(tmp_path):
    bed = package(tmp_path, "same", "bed", "complete")
    magnet = package(tmp_path, "same", "magnet", "complete")
    result = apply_moves(plan_moves([bed,magnet], tmp_path / "sorted"))
    assert len(result["moved"]) == 2
    assert len(set(result["moved"])) == 2
    assert all(Path(path).exists() for path in result["moved"])


@pytest.mark.asyncio
async def test_classification_uses_snapshot_and_exact_ids_and_replays_safely(packages, tmp_path):
    first = package(tmp_path, "same", state="complete")
    second = package(tmp_path, "second", "magnet", "complete")
    for item in (first,second): packages.store.save("items", item)
    other = copy.deepcopy(first)
    other.update(id="unrelated", videoPath=str(tmp_path / "other.mp4"), path=str(tmp_path / "elsewhere" / "same.zip"))
    packages.store.save("items", other)
    manager = SliceClassification(packages)
    plan = manager.preview(str(tmp_path / "sorted"))
    result = await manager.confirm(plan["id"], "move-once", plan["revision"])
    assert len(result["moved"]) == 2
    assert packages.store.get("items", "unrelated")["path"] == other["path"]
    replay = await manager.confirm(plan["id"], "move-once", plan["revision"])
    assert replay == result


@pytest.mark.asyncio
async def test_stale_classification_is_rejected_before_any_file_moves(packages, tmp_path):
    item = package(tmp_path, state="complete")
    packages.store.save("items", item)
    manager = SliceClassification(packages)
    plan = manager.preview(str(tmp_path / "sorted"))
    packages.store.save("items", item)
    with pytest.raises(Conflict):
        await manager.confirm(plan["id"], "stale", plan["revision"])
    assert Path(item["path"]).exists()


def test_invalid_and_symlink_classification_paths_are_rejected(tmp_path):
    with pytest.raises(ValueError): plan_moves([], "data/slices")
    alias = tmp_path / "alias"
    alias.symlink_to(tmp_path, target_is_directory=True)
    with pytest.raises(ValueError): plan_moves([], alias / "sorted")


@pytest.mark.asyncio
async def test_cross_volume_classification_verifies_then_unlinks_source(packages, tmp_path, monkeypatch):
    import app.slice_classify as classify
    monkeypatch.setattr(classify, "same_device", lambda *args: False)
    first = package(tmp_path, state="complete")
    second = package(tmp_path, "second", "magnet", "complete")
    for item in (first,second): packages.store.save("items", item)
    manager = SliceClassification(packages)
    plan = manager.preview(str(tmp_path / "sorted"))
    original = Path(first["path"]).read_bytes()
    result = await manager.confirm(plan["id"], "cross-disk", plan["revision"])
    assert not result["errors"] and len(result["moved"]) == 2
    saved = packages.store.get("items", first["id"])
    assert Path(saved["path"]).read_bytes() == original
    assert not Path(first["path"]).exists()


@pytest.mark.asyncio
async def test_receipt_deletion_does_not_delete_durable_job_and_does_not_reappear(tmp_path):
    app = create_app(data_dir=tmp_path / "data")
    job = {"id": "job", "planId": "plan", "createdAt": "2026-10-20T00:00:00Z", "status": "complete",
           "items": [{"key": "retained-key", "status": "submitted"}], "events": []}
    app.state.store.save(job)
    log = SubmissionLog(app.state.store.db)
    row = log.add("collection", "采集提交", {"jobId": "job"})
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1") as client:
        assert (await client.delete('/api/submissions/'+row["id"])).status_code == 200
        assert (await client.get('/api/submissions')).json() == []
        assert "retained-key" in app.state.store.known_keys()
        response = await client.delete('/api/jobs/job')
        assert response.status_code == 200
        assert (await client.get('/api/jobs')).json() == []
        assert app.state.store.get("job")["deletedAt"]
    log.import_jobs([job], lambda value: {"jobId":value["id"]})
    assert log.list() == []


@pytest.mark.asyncio
async def test_foreign_fetch_and_framing_are_blocked_and_errors_redact_password(tmp_path):
    app = create_app(data_dir=tmp_path / "data")
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1") as client:
        response = await client.get('/api/session', headers={"sec-fetch-site":"cross-site"})
        assert response.status_code == 403
        response = await client.get('/api/session', headers={
            "host": "rebound.attacker.test",
            "origin": "http://rebound.attacker.test",
            "sec-fetch-site": "same-origin",
        })
        assert response.status_code == 400
        response = await client.get('/api/session')
        assert response.headers['x-frame-options'] == 'DENY'
        assert "frame-ancestors 'none'" in response.headers['content-security-policy']
        response = await client.post('/api/upload-audit/login', json={"password":"never-echo-this"})
        assert response.status_code == 422 and 'never-echo-this' not in response.text


@pytest.mark.asyncio
async def test_image_inventory_uses_search_and_full_pagination_beyond_page_five():
    remote = UploadRemote(None)
    calls = []
    async def request(method, path, *, params=None):
        calls.append(params)
        page = params['page']
        rows = [{"id":str(page), "name":f"prefix_{page}.jpg", "dataset":27892}]
        return rows, {"x-total":"6", "x-next-page":str(page+1) if page<6 else ""}
    remote.request = request
    assert len(await remote.images('27892', prefix='prefix')) == 6
    assert all(call['search'] == 'prefix' for call in calls)


@pytest.mark.asyncio
async def test_export_keeps_other_models_new_receipts_and_blocks_stale_manual_edits(tmp_path):
    import threading
    from test_slice_packages import ready_item
    app = create_app(data_dir=tmp_path / "data")
    slicing = app.state.slicing
    item = ready_item(tmp_path)
    item["metadata"] = {"duration": 3000}
    item["categories"]["magnet"] = {"state":"complete", "taskId":"previous", "actualImages":1}
    slicing.store.save("items", item)
    entered, release = threading.Event(), threading.Event()
    def reader(*args):
        entered.set()
        assert release.wait(5)
        return JPEG
    slicing.slice_reader = reader
    running = asyncio.create_task(slicing._export_category(slicing.store.get("items",item["id"]), 'bed'))
    assert await asyncio.to_thread(entered.wait,5)
    assert slicing.state()["items"][0]["categories"]["bed"]["state"] == "exporting"
    with pytest.raises(Conflict):
        await slicing.review(item["id"], {"requestId":"stale-edit","expectedRevision":1,"category":"bed","confirmedNoTarget":True})
    current = slicing.store.get("items", item["id"])
    current["categories"]["magnet"].update(actualImages=150,taskId="new-receipt")
    slicing.store.save("items",current)
    release.set()
    await running
    saved = slicing.store.get("items",item["id"])
    assert saved["categories"]["magnet"]["taskId"] == "new-receipt"
    assert saved["categories"]["magnet"]["actualImages"] == 150
    assert saved["categories"]["bed"]["packageId"]


def test_cleanup_validates_all_versions_before_deleting_anything(packages, tmp_path):
    source = tmp_path / 'video.mp4'
    source.write_bytes(b'original')
    for kind in ('bed','magnet'):
        item = package(tmp_path, kind, kind, 'complete')
        image_dir = tmp_path / kind / 'image'
        image_dir.mkdir()
        for name in item['images']: (image_dir/name).write_bytes(JPEG)
        item.update(videoFileVersion=source_fingerprint(source), imageDir=str(image_dir),
                    imageVersions={name:source_fingerprint(image_dir/name) for name in item['images']},
                    actualImages=2,verifiedJobId='901')
        packages.store.save('items',item)
    # A newly replaced video at the same path is not the original we may clean.
    source.write_bytes(b'user replacement')
    item = packages.store.get('items','magnet')
    packages.delete_if_both_complete(item)
    assert source.read_bytes() == b'user replacement'
    assert all(Path(row['path']).is_file() for row in packages.store.list('items'))
    assert all(row['cleanupState'] == 'attention' for row in packages.store.list('items'))


@pytest.mark.asyncio
async def test_manual_intervals_with_missing_fields_return_validation_error(tmp_path):
    app = create_app(data_dir=tmp_path / 'data')
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url='http://127.0.0.1') as client:
        response = await client.post('/api/slicing/one/review',json={
            'requestId':'intervals','expectedRevision':0,'category':'bed','intervals':[{'start':2}]})
        assert response.status_code == 422
