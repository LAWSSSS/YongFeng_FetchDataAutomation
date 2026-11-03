import asyncio
import json
import shutil
import subprocess

import httpx
import pytest

from app.main import create_app
from app.slicing import source_version
from app.slicing_remote import SlicingRemote
from app.slicing_worker import extract_requested_frames, probe
from app.upload_audit import AthenaClient, AuditError
from tests.test_slicing import FakeSlicingRemote


async def close_app(app):
    for name in ("slicing", "uploads", "videos", "jobs", "upload_audit"):
        await getattr(app.state, name).shutdown()
    app.state.store.close()


@pytest.mark.asyncio
async def test_image_query_reads_only_the_operation_prefix_and_keeps_no_media_links():
    calls, progress = [], []
    prefix = "鲁Q09613D_249308_C"

    def handler(request):
        calls.append(request)
        assert request.url.params["search"] == prefix
        assert request.url.params["page_size"] == "100"
        assert "pageSize" not in request.url.params
        page = int(request.url.params["page"])
        start, end = (0, 100) if page == 1 else (100, 146)
        rows = [{"id": index + 1, "name": f"{prefix}_{index}.jpg", "taskId": 810,
                 "url": "not-for-state", "extra": "unused"} for index in range(start, end)]
        return httpx.Response(200, json={"data": rows},
            headers={"x-total": "146", "x-next-page": "2" if page == 1 else "0"})

    auth = AthenaClient(transport=httpx.MockTransport(handler)); auth.access = "test-token"
    rows = await SlicingRemote(auth).image_rows("900", prefix=prefix, progress=progress.append)
    assert len(rows) == 146 and len(calls) == 2
    assert all("url" not in row and "extra" not in row for row in rows)
    assert progress[-1] == {"read": 146, "total": 146, "pages": 2, "scope": "prefix"}
    await auth.client.aclose()


@pytest.mark.asyncio
async def test_ignored_image_filter_is_not_mistaken_for_an_empty_safe_snapshot():
    auth = AthenaClient(transport=httpx.MockTransport(lambda request:
        httpx.Response(200, json={"data": [{"id": 1, "name": "other-video.jpg"}]})))
    auth.access = "test-token"
    with pytest.raises(AuditError) as failure:
        await SlicingRemote(auth).image_rows("900", prefix="expected_C")
    assert failure.value.code == "image_filter"
    await auth.client.aclose()


@pytest.mark.asyncio
async def test_public_state_excludes_large_snapshots_without_changing_durable_evidence(tmp_path):
    app = create_app(data_dir=tmp_path)
    manager = app.state.slicing
    snapshots = [str(index) for index in range(21022)]
    item = {"id": "large", "name": "source.mp4", "state": "complete", "categories": {
        "bed": {"state": "complete", "actualImages": 146, "preSubmitImageIds": snapshots,
                "preSubmitTaskIds": ["810"], "outputImageIds": ["9001"]}}}
    manager.store.save("items", item)
    public = manager.state()["items"][0]
    assert public["categories"]["bed"] == {"state": "complete", "actualImages": 146}
    assert manager.store.get("items", "large")["categories"]["bed"]["preSubmitImageIds"] == snapshots
    assert len(json.dumps(public)) < len(json.dumps(item)) / 100
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1") as client:
        first = await client.get("/api/slicing")
        same = await client.get("/api/slicing", headers={"If-None-Match": first.headers["etag"]})
        assert same.status_code == 304 and not same.content
        item["state"] = "attention"; manager.store.save("items", item)
        changed = await client.get("/api/slicing", headers={"If-None-Match": first.headers["etag"]})
        assert changed.status_code == 200 and changed.headers["etag"] != first.headers["etag"]
    await close_app(app)


@pytest.mark.asyncio
async def test_process_limit_can_be_changed_without_invalidating_an_active_run(tmp_path):
    app = create_app(data_dir=tmp_path / "process-data")
    manager = app.state.slicing
    manager.store.settings.update(enabled=True, revision=7)
    manager.store.save_settings()
    body = {"expectedRevision": 0, "requestId": "process-limit-1", "maxLocalSlicingProcesses": 5}
    initial = manager.state()
    assert initial["localWorkers"]["max"] == 3
    updated = manager.update_process_limit(body)
    assert updated["localWorkers"]["max"] == 5
    assert updated["localWorkers"]["revision"] == 1
    assert updated["settingsRevision"] == 7
    assert manager.store.settings["enabled"] is True
    replay = manager.update_process_limit(body)
    assert replay["localWorkers"]["max"] == 5
    assert replay["localWorkers"]["revision"] == 1
    with pytest.raises(Exception, match="进程设置已在其他页面修改"):
        manager.update_process_limit({**body, "requestId": "process-limit-stale", "expectedRevision": 0,
                                      "maxLocalSlicingProcesses": 6})
    with pytest.raises(ValueError, match="1到8"):
        manager.update_process_limit({**body, "requestId": "process-limit-invalid", "expectedRevision": 1,
                                      "maxLocalSlicingProcesses": 9})
    await close_app(app)


@pytest.mark.asyncio
async def test_model_settings_cannot_overwrite_a_new_process_limit_from_a_stale_draft(tmp_path):
    from app.videos import Conflict
    app = create_app(data_dir=tmp_path / 'data')
    manager = app.state.slicing
    draft = {**manager.state()['settings'], 'expectedRevision': 0, 'requestId': 'old-model-draft'}
    manager.update_process_limit({'expectedRevision': 0, 'requestId': 'new-process-limit',
                                  'maxLocalSlicingProcesses': 5})
    with pytest.raises(Conflict, match='进程数量已单独调整'):
        manager.update_settings(draft)
    assert manager.store.settings['maxLocalSlicingProcesses'] == 5
    assert manager.store.settings['processLimitRevision'] == 1
    assert manager.store.settings['revision'] == 0
    assert not manager.store.settings.get('settingsRequestIds', {}).get('old-model-draft')
    await close_app(app)




@pytest.mark.asyncio
async def test_bound_task_polling_and_output_check_do_not_reload_task_or_image_history(tmp_path):
    app = create_app(data_dir=tmp_path); manager = app.state.slicing
    manager.auth.access = "test-token"; remote = FakeSlicingRemote(); manager.remote = remote
    manager.store.settings["targetDatasetIds"] = {"27912": "900", "27944": "901"}
    path = tmp_path / "source.mp4"; path.write_bytes(b"immutable source")
    item = {"id": "fast", "sourceDatasetId": "27912", "sourceVideoId": "55", "name": path.name,
        "path": str(path), "fileVersion": source_version(path), "state": "platform_processing", "revision": 0,
        "categories": {"bed": {"state": "ready", "intervals": [{"start": 0, "end": 2190}],
            "intervalSeconds": 15, "modelFingerprint": "bed-v1"}, "magnet": {"state": "not_applicable"}}}
    remote.video_rows["27912"] = [{"id": 55, "name": path.name, "cutStatus": 0}]
    image_queries, direct_reads = [], []

    async def images(dataset, *, prefix=None, progress=None):
        assert prefix == "source_55_C"
        image_queries.append(prefix)
        if len(image_queries) == 1:
            return []
        if progress: progress({"read": 146, "total": 146, "pages": 2, "scope": "prefix"})
        return [{"id": index + 1000, "name": f"{prefix}_{index}.jpg"} for index in range(146)]

    async def direct(identity):
        direct_reads.append(identity)
        return {**remote.job_rows["27912"]["21"][0], "status": 3}

    async def no_history(*args):
        pytest.fail("a bound task must use its exact ID rather than enumerate task history")

    remote.image_rows = images; remote.job = direct
    manager.store.save("items", item)
    await manager._submit_ready(item)
    remote.jobs = no_history
    await manager._reconcile_waiting()
    result = manager.store.get("items", "fast")
    assert result["state"] == "complete"
    assert result["categories"]["bed"]["actualImages"] == 146
    assert remote.created == 1 and direct_reads == ["810"] and len(image_queries) == 2
    await close_app(app)


@pytest.mark.asyncio
async def test_platform_checks_continue_during_analysis_and_only_one_local_worker_runs(tmp_path, monkeypatch):
    app = create_app(data_dir=tmp_path); manager = app.state.slicing
    manager.auth.access = "test-token"
    manager.run = {"id": "concurrent", "state": "running"}
    manager.store.settings['enabled'] = True
    manager.store.save("runs", manager.run)
    for identity in ("first", "second"):
        manager.store.save("items", {"id": identity, "state": "pending", "path": "/fixture.mp4",
            "sourceDatasetId": "27912", "categories": {"bed": {"state": "pending"}}})
    manager.store.save('items', {'id': 'remote-wait', 'state': 'platform_processing', 'sourceDatasetId': '27912',
        'categories': {'bed': {'state': 'waiting', 'taskId': 'bound'}}})
    release = asyncio.Event(); calls = []; polls = []

    async def analyze(item):
        calls.append(item["id"])
        await release.wait()

    async def poll(**kwargs): polls.append(True)

    monkeypatch.setattr(manager, "_process_item", analyze)
    monkeypatch.setattr(manager, "_reconcile_waiting", poll)
    await asyncio.wait_for(manager._tick(), 1)
    await asyncio.sleep(0)
    await asyncio.wait_for(manager._tick(), 1)
    assert calls == ["first"] and len(polls) == 1
    release.set(); await manager.analysis_task
    await close_app(app)


@pytest.mark.asyncio
async def test_local_slicing_uses_configured_parallel_workers_for_distinct_files(tmp_path, monkeypatch):
    app = create_app(data_dir=tmp_path)
    manager = app.state.slicing
    manager.auth.access = "test-token"
    manager.store.settings["enabled"] = True
    manager.store.settings["maxLocalSlicingProcesses"] = 2
    release = asyncio.Event()
    started = []

    for identity in ("first", "second", "third"):
        manager.store.save("items", {"id": identity, "state": "pending",
            "path": str(tmp_path / f"{identity}.mp4"), "fileVersion": {"size": 1, "edgeHash": identity},
            "sourceDatasetId": "27912", "sourceVideoId": identity, "revision": 0,
            "categories": {"bed": {"state": "pending"}, "magnet": {"state": "not_applicable"}}})

    async def analyze(item, _candidates):
        started.append(item["id"])
        await release.wait()
        current = manager.store.get("items", item["id"])
        current.update(state="complete", categories={"bed": {"state": "complete"},
                                                       "magnet": {"state": "not_applicable"}})
        manager.store.save("items", current)

    async def poll(**_kwargs):
        return None

    monkeypatch.setattr(manager, "_process_shared", analyze)
    monkeypatch.setattr(manager, "_reconcile_waiting", poll)
    await manager._tick()
    await asyncio.sleep(0)
    assert len(started) == 2
    assert len(manager.analysis_tasks) == 2
    assert manager.state()["localWorkers"] == {"active": 2, "processes": 0, "max": 2,
                                                "revision": 0, "queued": 1}

    first_wave = list(manager.analysis_tasks.values())
    release.set()
    await asyncio.gather(*first_wave)
    await manager._tick()
    await asyncio.sleep(0)
    assert started == ["first", "second", "third"]
    await asyncio.gather(*list(manager.analysis_tasks.values()), return_exceptions=True)
    await close_app(app)


@pytest.mark.asyncio
async def test_local_recovery_batch_does_not_touch_unrelated_waiting_history(tmp_path, monkeypatch):
    app = create_app(data_dir=tmp_path)
    manager = app.state.slicing
    manager.auth.access = "test-token"
    selected_path = tmp_path / "selected.mp4"
    selected_path.write_bytes(b"selected recovery media")
    selected_version = source_version(selected_path)
    manager.run = {"id": "recovery-run", "state": "running", "localRecoveryOnly": True,
                   "singleMode": False, "items": ["selected"]}
    manager.store.save("items", {"id": "selected", "path": str(selected_path),
        "fileVersion": selected_version, "state": "waiting_connection", "sourceDatasetId": "27912",
        "categories": {"bed": {"state": "pending"}}})
    manager.store.save("items", {"id": "unrelated", "path": str(tmp_path / "not-mounted.mp4"),
        "state": "waiting_connection", "sourceDatasetId": "27944",
        "categories": {"magnet": {"state": "waiting", "taskId": "old-task"}}})

    manager.remote.job = lambda _identity: pytest.fail("recovery polling touched an unrelated historical task")
    manager.remote.jobs = lambda *_args: pytest.fail("recovery polling loaded unrelated task history")
    original_source_version = source_version
    checked_paths = []

    def track_version(path):
        checked_paths.append(str(path))
        return original_source_version(path)

    monkeypatch.setattr("app.slicing.source_version", track_version)
    await manager._restore_waiting_files()
    await manager._reconcile_waiting()

    assert checked_paths == [str(selected_path)]
    assert manager.store.get("items", "selected")["state"] == "pending"
    assert manager.store.get("items", "unrelated")["state"] == "waiting_connection"
    assert manager._poll_timeout() == 30
    await close_app(app)


@pytest.mark.asyncio
async def test_slow_photo_export_does_not_block_the_next_video_analysis(tmp_path, monkeypatch):
    app = create_app(data_dir=tmp_path)
    manager = app.state.slicing
    manager.auth.access = "test-token"
    manager.run = {"id": "parallel-export", "state": "running"}
    manager.store.settings.update(enabled=True, sharedSampling=False)
    manager.files = None
    manager.store.save("items", {"id": "export", "state": "platform_processing", "path": "/export.mp4",
        "categories": {"bed": {"state": "ready", "intervals": [{"start": 0, "end": 1}]}}})
    manager.store.save("items", {"id": "next", "state": "pending", "path": "/next.mp4",
        "sourceDatasetId": "27912", "categories": {"bed": {"state": "pending"}}})
    release, analyzed = asyncio.Event(), asyncio.Event()

    async def export(_item):
        await release.wait()

    async def analyze(_item):
        analyzed.set()
        await release.wait()

    monkeypatch.setattr(manager, "_export_ready", export)
    monkeypatch.setattr(manager, "process_with_recovery", analyze)
    await asyncio.wait_for(manager._tick(), .5)
    await asyncio.wait_for(analyzed.wait(), .5)
    assert len(manager.export_tasks) == len(manager.analysis_tasks) == 1
    release.set()
    await asyncio.gather(*list(manager.export_tasks.values()), *list(manager.analysis_tasks.values()))
    await close_app(app)


@pytest.mark.asyncio
async def test_paused_recovery_survives_stale_progress_and_is_not_restarted(tmp_path):
    app = create_app(data_dir=tmp_path)
    manager = app.state.slicing
    manager.auth.access = "test-token"
    manager.files = None
    manager.run = {"id": "paused-run", "state": "running", "localRecoveryOnly": True, "items": ["paused"]}
    manager.store.settings["enabled"] = True
    item = {"id": "paused", "path": "/paused.mp4", "state": "pending", "sourceDatasetId": "27912",
            "collectionRecovery": True, "collectionRecoveryPaused": True,
            "categories": {"bed": {"state": "pending"}}}
    manager.store.save("items", item)
    stale = {**item, "collectionRecoveryPaused": False, "analysisDone": 2}
    manager._save_analysis_item(stale)
    assert manager.store.get("items", "paused")["collectionRecoveryPaused"] is True
    await manager._tick()
    assert not manager.analysis_tasks and not manager.export_tasks
    assert manager.run["state"] == "paused" and manager.store.settings["enabled"] is False
    await close_app(app)


def test_partial_sampling_reads_edges_at_boundaries_not_for_each_frame(tmp_path, monkeypatch):
    from types import SimpleNamespace
    from app import slicing_worker as worker

    path, ffmpeg = tmp_path / "video.mp4", tmp_path / "ffmpeg"
    path.write_bytes(b"stable source")
    ffmpeg.write_text("fake tool")
    ffmpeg.chmod(0o755)
    expected = source_version(path)
    checks = []
    original = worker.verify_file_version

    def track(path, version, *, check_edges=True):
        checks.append(check_edges)
        return original(path, version, check_edges=check_edges)

    monkeypatch.setattr(worker, "verify_file_version", track)
    monkeypatch.setattr(worker.subprocess, "run", lambda command, **_kw:
                        SimpleNamespace(returncode=0, stdout=b"\xff\xd8\xff\xd9", stderr=(
                            f"[Parsed_showinfo_0] n: 0 pts: 0 pts_time:{command[command.index('-ss') + 1]}".encode())))
    rows = list(worker.extract_requested_frames(str(path), str(ffmpeg),
        {"duration": 61, "frameRate": 25, "_expectedFileVersion": expected},
        {"bed": [0, 30, 60], "magnet": [0, 30, 60]}, skip_undecodable=True))
    assert [time for time, _ in rows] == [0, 30, 60]
    assert checks.count(True) == 2 and checks.count(False) == 6


@pytest.mark.asyncio
async def test_cancelled_worker_requeues_only_its_physical_video_peers(tmp_path):
    app = create_app(data_dir=tmp_path)
    manager = app.state.slicing
    path = str(tmp_path / "shared.mp4")
    version = {"size": 1, "edgeHash": "same"}
    for identity, source, state in (("bed", "27912", "analyzing"),
                                    ("magnet", "27944", "analyzing"),
                                    ("other", "27912", "analyzing")):
        manager.store.save("items", {"id": identity, "state": state,
            "path": path if identity != "other" else str(tmp_path / "other.mp4"),
            "fileVersion": version if identity != "other" else {"size": 1, "edgeHash": "other"},
            "sourceDatasetId": source, "sourceVideoId": identity, "revision": 0,
            "categories": {"bed": {"state": "pending"}, "magnet": {"state": "pending"}}})

    task = asyncio.create_task(asyncio.Event().wait())
    manager.analysis_tasks["bed"] = task
    manager.analysis_video_keys["bed"] = manager._analysis_video_key(manager.store.get("items", "bed"))
    manager.analysis_task = task
    task.add_done_callback(lambda finished: manager._analysis_finished("bed", finished))
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    await asyncio.sleep(0)

    assert manager.store.get("items", "bed")["state"] == "pending"
    assert manager.store.get("items", "magnet")["state"] == "pending"
    assert manager.store.get("items", "other")["state"] == "analyzing"
    await close_app(app)


@pytest.mark.asyncio
async def test_scan_reuses_completed_logs_but_refreshes_active_logs(tmp_path, monkeypatch):
    app = create_app(data_dir=tmp_path); manager = app.state.slicing
    manager.auth.access = "test-token"; remote = FakeSlicingRemote(); manager.remote = remote
    remote.video_rows["27912"] = [{"id": 31, "name": "first.mp4", "cutStatus": 0},
                                 {"id": 32, "name": "second.mp4", "cutStatus": 0}]
    remote.job_rows["27912"]["21"] = [{"id": 101, "type": 21, "status": 3,
        "config": {"source": {"datasets": [{"videos": {"ids": [31]}}]}}},
        {"id": 102, "type": 21, "status": 2,
        "config": {"source": {"datasets": [{"videos": {"ids": [32]}}]}}}]
    reads = []
    async def logs(dataset, identity): reads.append(identity); return []
    remote.logs = logs
    monkeypatch.setattr("app.slicing.scan_local_files", lambda directories: ([], directories, []))
    manager.scan("first-scan"); await manager.scan_task
    manager.scan("second-scan"); await manager.scan_task
    assert reads.count("101") == 1 and reads.count("102") == 2
    assert any(entry.get("cacheHit") for entry in manager.state()["run"]["taskLogIndex"])
    await close_app(app)


@pytest.mark.asyncio
async def test_job_summary_keeps_submission_counts_without_sending_source_payloads(tmp_path):
    app = create_app(data_dir=tmp_path)
    app.state.store.save({"id": "j", "status": "complete", "createdAt": "2026-10-19T00:00:00Z",
        "events": [], "originalRecords": "x" * 100000,
        "items": [{"status": "submitted", "videoName": "source", "privateSnapshot": "x" * 10000}]})
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1") as client:
        full = await client.get("/api/jobs"); summary = await client.get("/api/jobs?summary=true")
    assert summary.json()[0]["items"] == [{"status": "submitted", "videoName": "source"}]
    assert len(summary.content) < len(full.content) / 100
    assert app.state.store.get("j")["originalRecords"] == "x" * 100000
    await close_app(app)


@pytest.mark.asyncio
async def test_video_view_omits_internal_snapshots_while_preserving_correction_fields(tmp_path, monkeypatch):
    app = create_app(data_dir=tmp_path)
    record = {"id": "video", "revision": 4, "activeRevision": 2, "acceptedRevision": 1,
        "requests": {"old": "proof"}, "versions": [{"revision": 2, "state": "ready",
        "startTime": "2026-10-19 10:00:00", "durationSeconds": 600, "taskId": "bound",
        "beforeIds": [str(index) for index in range(10000)]}]}
    monkeypatch.setattr(app.state.videos, "state", lambda query: {"videos": [record], "counts": {"ready": 1}})
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1") as client:
        response = (await client.get("/api/videos?compact=true")).json()
    public = response["videos"][0]
    assert public["revision"] == 4 and public["activeRevision"] == 2 and public["acceptedRevision"] == 1
    assert public["versions"][0]["taskId"] == "bound" and "beforeIds" not in public["versions"][0]
    assert len(record["versions"][0]["beforeIds"]) == 10000 and "requests" in record
    await close_app(app)


@pytest.mark.skipif(not shutil.which("ffmpeg"), reason="ffmpeg is required")
def test_sparse_seeking_preserves_the_actual_first_frame_and_shared_sample_positions(tmp_path):
    ffmpeg = shutil.which("ffmpeg"); video = tmp_path / "colors.mp4"
    subprocess.run([ffmpeg, "-hide_banner", "-loglevel", "error", "-f", "lavfi", "-i",
        "color=c=red:s=64x64:r=5:d=5[a];color=c=blue:s=64x64:r=5:d=5[b];[a][b]concat=n=2:v=1:a=0",
        "-c:v", "mpeg4", "-y", str(video)], check=True, timeout=20)
    metadata = probe(str(video), shutil.which("ffprobe"))
    frames = list(extract_requested_frames(str(video), ffmpeg, metadata,
        {"bed": [0, 5, 9.8], "magnet": [0, 9.8]}))
    assert [position for position, _ in frames] == [0, 5, 9.8]
    pixels = []
    for _, image in frames:
        decoded = subprocess.run([ffmpeg, "-hide_banner", "-loglevel", "error", "-i", "pipe:0",
            "-f", "rawvideo", "-pix_fmt", "rgb24", "pipe:1"], input=image, capture_output=True, check=True, timeout=10)
        pixels.append(tuple(decoded.stdout[:3]))
    assert pixels[0][0] > 200 and pixels[0][2] < 20
    assert all(pixel[2] > 200 and pixel[0] < 20 for pixel in pixels[1:])


@pytest.mark.skipif(not shutil.which("ffmpeg"), reason="ffmpeg is required")
def test_dense_sampling_keeps_frame_zero_instead_of_the_middle_of_the_first_second(tmp_path):
    ffmpeg = shutil.which("ffmpeg"); video = tmp_path / "dense.mp4"
    subprocess.run([ffmpeg, "-hide_banner", "-loglevel", "error", "-f", "lavfi", "-i",
        "color=c=red:s=64x64:r=5:d=0.2[a];color=c=blue:s=64x64:r=5:d=2.8[b];[a][b]concat=n=2:v=1:a=0",
        "-c:v", "mpeg4", "-y", str(video)], check=True, timeout=20)
    metadata = probe(str(video), shutil.which("ffprobe"))
    frames = list(extract_requested_frames(str(video), ffmpeg, metadata, {"bed": [0, 1, 2]}))
    decoded = subprocess.run([ffmpeg, "-hide_banner", "-loglevel", "error", "-i", "pipe:0",
        "-f", "rawvideo", "-pix_fmt", "rgb24", "pipe:1"], input=frames[0][1], capture_output=True, check=True, timeout=10)
    assert decoded.stdout[0] > 200 and decoded.stdout[2] < 20


def test_partial_dense_cache_retries_seek_to_the_missing_positions(tmp_path, monkeypatch):
    import app.slicing_worker as worker
    ffmpeg = shutil.which("ffmpeg")
    if not ffmpeg: pytest.skip("ffmpeg is required")
    video = tmp_path / "partial.mp4"; video.write_bytes(b"fixture"); seeks = []
    def frames(command):
        seeks.append(float(command[command.index("-ss")+1])); yield b"\xff\xd8fixture\xff\xd9"
    monkeypatch.setattr(worker, "jpeg_frames", frames)
    output = list(worker.extract_requested_frames(str(video), ffmpeg,
        {"duration": 120, "frameRate": 25}, {"bed": [30, 31]}))
    assert seeks == [30, 31] and [position for position, _ in output] == [30, 31]
