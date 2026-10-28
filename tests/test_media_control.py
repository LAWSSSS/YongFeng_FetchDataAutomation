import asyncio
import hashlib
import io
import time
import zipfile
from pathlib import Path

import httpx
from PIL import Image
import pytest
import pytest_asyncio

from app.audit_moves import source_fingerprint
from app.main import create_app
from app.media_control import MediaControl
from app.slicing import source_version
from app.videos import Conflict
from tests.test_slicing_performance import close_app


@pytest_asyncio.fixture
async def app(tmp_path):
    value = create_app(data_dir=tmp_path / "data")
    yield value
    await value.state.package_gallery.shutdown()
    await close_app(value)


def collection(app, path, identity="collection", revision=0, accepted=True):
    row = {"id": identity, "jobId": "job", "name": "车牌", "plate": "车牌", "qualityStart": "2026-10-01 00:00:00",
           "qualityEnd": "2026-10-01 01:00:00", "revision": revision, "activeRevision": revision,
           "acceptedRevision": revision if accepted else None, "requests": {}, "versions": [{
               "revision": revision, "state": "saved" if accepted else "ready", "startTime": "2026-10-01 00:10:00",
               "endTime": "2026-10-01 01:10:00", "durationSeconds": 3600, "filePath": str(path),
               "fileVersion": source_version(path), "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
               "totalBytes": path.stat().st_size}]}
    app.state.videos.store.save(row)
    return row


def sliced(app, path, identity="bed", source="27912", state="pending"):
    name = "bed" if source == "27912" else "magnet"
    item = {"id": identity, "name": path.name, "path": str(path), "revision": 0, "state": state,
            "sourceDatasetId": source, "sourceVideoId": identity, "fileVersion": source_version(path),
            "categories": {name: {"state": state, "intervals": []},
                           "magnet" if name == "bed" else "bed": {"state": "not_applicable"}}}
    app.state.slicing.store.save("items", item)
    return item


@pytest.mark.asyncio
async def test_correction_preempts_only_same_media_and_stale_saves_cannot_resurrect_it(app, tmp_path):
    path = tmp_path / "one.mp4"; path.write_bytes(b"video-one")
    other = tmp_path / "other.mp4"; other.write_bytes(b"video-two")
    collection(app, path)
    sliced(app, path, state="analyzing")
    sliced(app, path, "magnet", "27944", "analyzing")
    sliced(app, other, "other", state="analyzing")
    control, manager = app.state.media_control, app.state.slicing
    control.refresh()
    stale = manager.store.get("items", "bed")
    assert manager.store.get("items", "magnet")["mediaId"] == stale["mediaId"]
    entered = asyncio.Event()

    async def worker():
        with manager.leases.read(path):
            entered.set()
            try:
                await asyncio.Event().wait()
            finally:
                stale["state"] = "pending"
                manager.store.save("items", stale)

    own = asyncio.create_task(worker()); other_task = asyncio.create_task(asyncio.Event().wait())
    manager.analysis_tasks.update(bed=own, other=other_task)
    await entered.wait()
    await control.take_over("collection", "correct-1")
    assert own.cancelled() and not other_task.done() and not manager.leases.busy(path)
    assert manager.store.get("items", "bed")["state"] == "superseded"
    assert manager.store.get("items", "magnet")["superseded"] is True
    assert control.get("collection:collection")["generation"] == 1
    await control.take_over("collection", "correct-1")
    assert control.get("collection:collection")["generation"] == 1
    resumed = MediaControl(control.db, app.state.videos, manager, manager.leases)
    assert not resumed.allowed(manager.store.get("items", "bed"))


@pytest.mark.asyncio
async def test_new_version_waits_for_acceptance_and_old_platform_receipts_stay_historical(app, tmp_path):
    path = tmp_path / "old.mp4"; path.write_bytes(b"old-video")
    collection(app, path); sliced(app, path)
    control = app.state.media_control; control.refresh()
    packages = app.state.slicing.packages
    for identity, state in (("unsent", "uploaded"), ("submitted", "importing")):
        packages.store.save("items", {"id": identity, "itemId": "bed", "state": state, "category": "bed",
            "dataset": "27892", "jobId": "real-job" if state == "importing" else ""})
    await control.take_over("collection", "change")
    assert packages.store.get("items", "unsent")["state"] == "superseded"
    assert packages.store.get("items", "submitted")["state"] == "importing"
    new_path = tmp_path / "new.mp4"; new_path.write_bytes(b"new-video")
    row = collection(app, new_path, revision=1, accepted=False)
    sliced(app, new_path, "new")
    control.release_review("collection"); control.refresh()
    assert "人工确认" in control.reason(app.state.slicing.store.get("items", "new"))
    row.update(acceptedRevision=1); row["versions"][0]["state"] = "saved"; app.state.videos.store.save(row)
    assert control.allowed(app.state.slicing.store.get("items", "new"))
    late = packages.store.get("items", "submitted")
    late.update(state="complete", actualImages=150); packages._save(late)
    assert app.state.videos.store.get("collection")["activeRevision"] == 1
    assert app.state.slicing.store.get("items", "new")["state"] == "pending"


@pytest.mark.asyncio
async def test_same_names_never_bind_distinct_files_and_explicit_ambiguous_link_is_version_checked(app, tmp_path):
    a, b = tmp_path / "a" / "same.mp4", tmp_path / "b" / "same.mp4"
    a.parent.mkdir(); b.parent.mkdir(); a.write_bytes(b"same-size-a"); b.write_bytes(b"same-size-b")
    collection(app, a); sliced(app, b)
    app.state.media_control.refresh()
    item = app.state.slicing.store.get("items", "bed")
    assert item["mediaId"].startswith("local:")
    with pytest.raises(Conflict, match="不一致"):
        await app.state.media_control.link(item, {"videoId": "collection", "collectionRevision": 0, "collectionExpectedRevision": 0})
    collection(app, b, "other-collection")
    collection(app, b, "ambiguous")
    app.state.media_control.refresh()
    item = app.state.slicing.store.get("items", "bed")
    assert item["identityConflict"] is True
    await app.state.media_control.link(item, {"videoId": "ambiguous", "collectionRevision": 0, "collectionExpectedRevision": 0})
    assert app.state.slicing.store.get("items", "bed")["collectionVideoId"] == "ambiguous"


@pytest.mark.asyncio
async def test_new_control_endpoint_replays_and_old_mutation_endpoints_are_closed(app, tmp_path, monkeypatch):
    path = tmp_path / "source.mp4"; path.write_bytes(b"source")
    sliced(app, path)
    calls = []
    async def start(*_args):
        calls.append(True); return {"run": {"id": "run"}}
    monkeypatch.setattr(app.state.slicing, "start", start)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1") as c:
        group = (await c.get("/api/video-processing")).json()["groups"][0]
        body = {"action": "start", "itemId": "bed", "generation": 0, "expectedRevision": 0, "requestId": "once"}
        url = "/api/video-processing/" + group["id"] + "/actions"
        assert (await c.post(url, json=body)).status_code == 200
        assert (await c.post(url, json=body)).status_code == 200
        assert len(calls) == 1
        assert (await c.post(url, json={**body,"generation":99,"requestId":"stale"})).status_code == 409
        for suffix in ("retry", "recover-decodable"):
            legacy = await c.post("/api/slicing/bed/" + suffix, json={"expectedRevision":0,"requestId":"legacy"})
            assert legacy.status_code == 409
            assert legacy.json()["processingEntry"].startswith("/?tab=videos&mediaId=local%3A")


@pytest.mark.asyncio
async def test_takeover_waits_for_frame_reader_before_releasing_its_file(app, tmp_path):
    import threading
    path = tmp_path / "source.mp4"; path.write_bytes(b"source")
    collection(app, path)
    item = sliced(app, path, state="platform_processing")
    item["categories"]["bed"].update(state="ready", intervals=[{"start":0,"end":1}], intervalSeconds=1)
    app.state.slicing.store.save("items", item)
    app.state.media_control.refresh()
    entered, release = threading.Event(), threading.Event()
    def reader(*_args):
        entered.set(); assert release.wait(5)
        return b"\xff\xd8\xff\xd9"
    manager = app.state.slicing; manager.slice_reader = reader
    manager._schedule_export(manager.store.get("items", "bed"))
    assert await asyncio.to_thread(entered.wait, 5)
    takeover = asyncio.create_task(app.state.media_control.take_over("collection", "new-source"))
    await asyncio.sleep(.01)
    assert not takeover.done() and manager.leases.busy(path)
    release.set(); await takeover
    assert not manager.leases.busy(path)
    assert not manager.packages.store.list("items")


@pytest.mark.asyncio
async def test_takeover_during_pre_import_query_never_sends_the_old_package(app, tmp_path, monkeypatch):
    path = tmp_path / "source.mp4"; path.write_bytes(b"source")
    collection(app, path); sliced(app, path)
    app.state.media_control.refresh()
    packages = app.state.slicing.packages
    item = {"id":"old-package","itemId":"bed","state":"uploaded","dataset":"27892",
            "object":{"key":"bound-old-object"},"images":["image_001.jpg"],"name":"old.zip"}
    packages.store.save("items", item)
    entered, release = asyncio.Event(), asyncio.Event()
    async def images(*_args, **_kwargs):
        entered.set(); await release.wait(); return []
    async def never_submit(*_args):
        pytest.fail("superseded package must not be imported")
    monkeypatch.setattr(packages.remote, "images", images)
    monkeypatch.setattr(packages.remote, "submit", never_submit)
    task = asyncio.create_task(packages.submit_one(item, []))
    await entered.wait()
    await app.state.media_control.take_over("collection", "correction")
    release.set(); await task
    assert packages.store.get("items", "old-package")["state"] == "superseded"


@pytest.mark.asyncio
async def test_last_confirmed_command_wins_before_any_old_submission(app, tmp_path, monkeypatch):
    path = tmp_path / "source.mp4"; path.write_bytes(b"source")
    collection(app, path)
    submitted = []
    async def perform(_identity, payload):
        submitted.append(payload["startTime"])
        return {"revision":0}
    manager = app.state.videos
    monkeypatch.setattr(manager, "_perform_correction", perform)
    first = manager.queue_video_operation("collection", "correction", {"requestId":"first", "expectedRevision":0,
        "startTime":"2026-10-01 02:00:00", "durationSeconds":600})
    second = manager.queue_video_operation("collection", "correction", {"requestId":"second", "expectedRevision":0,
        "startTime":"2026-10-01 03:00:00", "durationSeconds":600})
    await asyncio.gather(*list(manager.operation_tasks.values()))
    assert submitted == ["2026-10-01 03:00:00"]
    assert manager.store.get_operation(first["operationId"])["state"] == "conflict"
    assert manager.store.get_operation(second["operationId"])["state"] == "complete"


def package(app, tmp_path, identity="one", zip_only=False, color="green"):
    root = tmp_path / identity; root.mkdir()
    stream = io.BytesIO(); Image.new("RGB", (800, 450), color=color).save(stream, format="JPEG")
    data = stream.getvalue()
    image = root / "same_001.jpg"; image.write_bytes(data)
    archive = root / "压缩包.zip"
    with zipfile.ZipFile(archive, "w") as out:
        out.writestr("images/" + image.name, data)
    item = {"id": identity, "itemId": identity, "name": archive.name, "videoPath": str(root / "原片.mp4"),
            "path": str(archive), "fileVersion": source_fingerprint(archive), "imageDir": str(root),
            "images": [image.name], "imageVersions": {image.name: source_fingerprint(image)},
            "category": "bed", "dataset": "27892", "state": "complete", "imageCount": 1}
    app.state.slicing.packages.store.save("items", item)
    if zip_only:
        image.unlink()
    return item, data


@pytest.mark.asyncio
async def test_zip_only_gallery_reads_exact_package_without_extracting_and_holds_cleanup_lease(app, tmp_path):
    one, first = package(app, tmp_path, zip_only=True)
    two, second = package(app, tmp_path, "two", zip_only=True, color="red")
    gallery = app.state.package_gallery
    before = set(tmp_path.rglob("*"))
    view = await gallery.view("one")
    key = view["sessionId"]
    assert view["count"] == 1 and view["canDownload"] is True
    assert gallery.read("one", "same_001.jpg", key) == first
    assert gallery.read("two", "same_001.jpg") == second
    assert set(tmp_path.rglob("*")) == before
    with pytest.raises(ValueError):
        with gallery.leases.write(one["path"]):
            pass
    with pytest.raises(Conflict):
        gallery.read("two", "same_001.jpg", key)
    gallery.sessions[key]["expires"] = time.monotonic() - 1
    gallery.prune()
    assert not gallery.leases.busy(one["path"])


@pytest.mark.asyncio
async def test_gallery_http_thumbnail_zip_download_and_cleaned_state_are_local_only(app, tmp_path, monkeypatch):
    item, data = package(app, tmp_path)
    async def no_cloud(*_a, **_kw):
        pytest.fail("gallery must never fetch cloud media")
    monkeypatch.setattr(app.state.slicing.packages.remote, "images", no_cloud)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1") as c:
        view = (await c.post("/api/slicing/packages/one/view", json={})).json()
        key = view["sessionId"]
        content = (await c.get(f"/api/slicing/packages/one/contents?session={key}")).json()
        assert content["names"] == ["same_001.jpg"]
        response = await c.get(f"/api/slicing/packages/one/images/same_001.jpg?session={key}&thumbnail=true")
        assert response.status_code == 200
        assert Image.open(io.BytesIO(response.content)).size == (480, 270)
        downloaded = await c.get(f"/api/slicing/packages/one/download?session={key}")
        assert downloaded.status_code == 200 and downloaded.headers["content-type"] == "application/zip"
        assert (await c.post("/api/slicing/packages/one/view",json={"sessionId":[]})).status_code == 422
        await c.post("/api/slicing/packages/one/view/close",json={"sessionId":key})
        Path(item["path"]).unlink(); (Path(item["imageDir"])/"same_001.jpg").unlink()
        cleaned = (await c.post("/api/slicing/packages/one/view",json={})).json()
        assert cleaned["available"] is False


@pytest.mark.asyncio
@pytest.mark.parametrize("member", ["../same_001.jpg", "/same_001.jpg", "images/extra.jpg"])
async def test_gallery_rejects_unregistered_or_traversing_zip_entries(app, tmp_path, member):
    item, data = package(app, tmp_path, zip_only=True)
    with zipfile.ZipFile(item["path"],"w") as out:
        out.writestr(member,data)
    item["fileVersion"] = source_fingerprint(Path(item["path"]))
    app.state.slicing.packages.store.save("items", item)
    with pytest.raises(ValueError):
        await app.state.package_gallery.view("one")
    assert not app.state.package_gallery.leases.busy(item["path"])


@pytest.mark.asyncio
async def test_gallery_never_follows_registered_file_replaced_by_symlink(app, tmp_path):
    item, _ = package(app, tmp_path)
    image = Path(item["imageDir"])/"same_001.jpg"
    external = tmp_path/"outside.jpg"; external.write_bytes(image.read_bytes())
    image.unlink(); image.symlink_to(external)
    with pytest.raises(ValueError):
        await app.state.package_gallery.view("one")


@pytest.mark.asyncio
async def test_verified_move_retains_control_and_links_second_category_at_new_path(app, tmp_path):
    old, new = tmp_path / "old.mp4", tmp_path / "new.mp4"
    old.write_bytes(b"source-version")
    row = collection(app, old)
    row["versions"][0]["recovery"] = {"filePath": str(old), "destination": str(old),
                                      "fileVersion": source_version(old)}
    app.state.videos.store.save(row)
    sliced(app, old); app.state.media_control.refresh()
    previous = app.state.slicing.store.get("items", "bed")["mediaId"]
    old.rename(new)
    app.state.audit_moves.update_bindings({"source": str(old), "destination": str(new),
                                          "destinationVersion": source_fingerprint(new)})
    sliced(app, new, "magnet", "27944")
    app.state.media_control.refresh()
    current = app.state.videos.store.get("collection")["versions"][0]
    assert current["filePath"] == current["recovery"]["filePath"] == str(new)
    assert current["fileVersion"] == current["recovery"]["fileVersion"] == source_version(new)
    assert app.state.slicing.store.get("items", "magnet")["mediaId"] == previous
    assert app.state.slicing.store.get("items", "bed")["mediaId"] == previous
    assert str(new) in app.state.media_control.get(previous)["paths"]


@pytest.mark.asyncio
async def test_retry_download_stops_same_video_recovery_and_preserves_other_videos(app, tmp_path, monkeypatch):
    path = tmp_path / "retry.mp4"; path.write_bytes(b"source")
    row = collection(app, path)
    row["versions"][0].update(state="failed", downloadRequested=True)
    app.state.videos.store.save(row)
    sliced(app, path, state="analyzing"); app.state.media_control.refresh()
    entered = asyncio.Event()
    async def worker():
        with app.state.slicing.leases.read(path):
            entered.set(); await asyncio.Event().wait()
    task = asyncio.create_task(worker())
    app.state.slicing.analysis_tasks["bed"] = task
    await entered.wait()
    scheduled = []
    monkeypatch.setattr(app.state.videos, "schedule", lambda row: scheduled.append(row["id"]))
    result = await app.state.videos.retry_download("collection")
    assert task.cancelled() and not app.state.slicing.leases.busy(path)
    assert result["versions"][0]["state"] == "ready" and result["versions"][0]["downloadRequested"]
    assert scheduled == ["collection"]
    assert app.state.slicing.store.get("items", "bed")["superseded"]


@pytest.mark.asyncio
async def test_terminal_operation_releases_gate_after_restart_and_rebind_rejects_wrong_bytes(app, tmp_path):
    path = tmp_path / "source.mp4"; path.write_bytes(b"good")
    wrong = tmp_path / "elsewhere" / path.name; wrong.parent.mkdir(); wrong.write_bytes(b"bad!")
    collection(app, path); sliced(app, path); app.state.media_control.refresh()
    item = app.state.slicing.store.get("items", "bed")
    with pytest.raises(Conflict, match="不一致"):
        await app.state.media_control.verify_collection_file(item, wrong)
    await app.state.media_control.verify_collection_file(item, path)
    generation = app.state.media_control.reserve("collection", "finished-op")
    app.state.videos.store.save_operation({"id": "finished-op", "videoId": "collection", "state": "attention"})
    restored = MediaControl(app.state.media_control.db, app.state.videos, app.state.slicing, app.state.slicing.leases)
    assert restored.get("collection:collection")["generation"] == generation
    assert not restored.get("collection:collection")["blocked"]


@pytest.mark.asyncio
async def test_corrupt_bound_zip_returns_actionable_error_without_cloud_fetch(app, tmp_path):
    item, _ = package(app, tmp_path, zip_only=True)
    archive = Path(item["path"]); archive.write_bytes(b"not-a-zip")
    item["fileVersion"] = source_fingerprint(archive)
    app.state.slicing.packages.store.save("items", item)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1") as client:
        result = await client.post("/api/slicing/packages/one/view", json={})
        assert result.status_code == 409
    assert not app.state.slicing.leases.busy(archive)


@pytest.mark.asyncio
async def test_gallery_follows_verified_archive_move_without_matching_another_same_name_package(app, tmp_path):
    item, data = package(app, tmp_path, zip_only=True)
    package(app, tmp_path, "different", zip_only=True, color="blue")
    archive = Path(item["path"])
    moved = tmp_path / "归档" / archive.name; moved.parent.mkdir(); archive.rename(moved)
    app.state.slicing.leases.notify_move(str(archive), str(moved), source_fingerprint(moved))
    view = await app.state.package_gallery.view(item["id"])
    assert app.state.package_gallery.read(item["id"], "same_001.jpg", view["sessionId"]) == data
    assert view["count"] == 1 and view["canDownload"]
    app.state.package_gallery.close(view["sessionId"])
    assert not app.state.slicing.leases.busy(moved)
