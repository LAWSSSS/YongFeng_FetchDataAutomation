import asyncio
import json
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

from app.main import create_app
from app.media import DownloadFailure
from app.media_validation import MediaValidationError, verify_video_tail
from app.slicing import normalize_item_categories, source_version
from app.slicing_worker import probe
from app.videos import Conflict
from tests.test_slicing import FakeSlicingRemote
from tests.test_slicing_performance import close_app


def item_fixture(tmp_path, identity="selected", state="attention"):
    path = tmp_path / f"{identity}.mp4"
    path.write_bytes(b"immutable file fixture")
    return {"id": identity, "runId": "run", "name": path.name, "sourceDatasetId": "27912",
        "sourceVideoId": identity, "path": str(path), "fileVersion": source_version(path),
        "metadata": {"duration": 60.0, "frameRate": 25, "width": 3840, "height": 2160},
        "state": state, "revision": 3, "errorCode": "frame_extract_failed" if state == "attention" else "",
        "categories": {"bed": {"state": state, "errorCode": "frame_extract_failed", "intervals": []},
                       "magnet": {"state": "not_applicable", "intervals": []}}}


def prepare_run(manager, monkeypatch):
    manager.auth.access = "test-token"
    manager.remote = FakeSlicingRemote()
    run = {"id": "run", "state": "attention", "finishedAt": "2026-10-19T00:00:00Z",
           "settingsRevision": 0, "singleMode": True, "singleItemId": "failed-other"}
    manager.run = run
    manager.store.save("runs", run)
    manager.store.settings.update(latestRunId="run", targetDatasetIds={"27912": "900", "27944": "901"})
    manager.store.save_settings()
    monkeypatch.setattr(manager, "_wake_scheduler", lambda: None)


@pytest.fixture
def truncated_mp4(tmp_path):
    ffmpeg = shutil.which("ffmpeg")
    if not ffmpeg:
        pytest.skip("FFmpeg is required")
    path = tmp_path / "partial.mp4"
    subprocess.run([ffmpeg, "-v", "error", "-f", "lavfi", "-i", "testsrc2=size=160x120:rate=10",
        "-t", "4", "-c:v", "mpeg4", "-q:v", "3", "-movflags", "+faststart", str(path)], check=True)
    verify_video_tail(path, ffmpeg, 4)
    contents = path.read_bytes()
    path.write_bytes(contents[:len(contents) // 2])
    assert probe(str(path), str(Path(ffmpeg).with_name("ffprobe")))["duration"] == 4
    return path, ffmpeg


def test_readable_header_is_not_accepted_as_complete_media(truncated_mp4):
    path, ffmpeg = truncated_mp4
    with pytest.raises(MediaValidationError, match="media_incomplete"):
        verify_video_tail(path, ffmpeg, 4)


@pytest.mark.asyncio
async def test_download_cannot_publish_a_video_with_a_truncated_body(tmp_path, truncated_mp4):
    app = create_app(data_dir=tmp_path / "data")
    with pytest.raises(DownloadFailure) as failure:
        await app.state.videos.ffprobe(truncated_mp4[0])
    assert failure.value.code == "invalid_video"
    assert truncated_mp4[0].is_file()
    await close_app(app)


@pytest.mark.asyncio
async def test_idle_retry_really_starts_only_that_video_and_replays_safely(tmp_path, monkeypatch):
    app = create_app(data_dir=tmp_path / "data"); manager = app.state.slicing
    prepare_run(manager, monkeypatch)
    item = item_fixture(tmp_path); other = item_fixture(tmp_path, "other", "pending")
    manager.store.save("items", item); manager.store.save("items", other)
    manager.store.save("observations", {"id": "valid-observation", "itemId": item["id"], "time": 0})
    with pytest.raises(ValueError, match="上次单条试切已结束"):
        manager.run["singleItemId"] = item["id"]; manager.store.save("runs", manager.run)
        manager.resume("cannot-resume-attention")
    state = manager.retry(item["id"], 3, "retry-once")
    assert state["enabled"] and state["run"]["singleMode"]
    assert state["run"]["singleItemId"] == item["id"]
    assert manager.store.get("items", "other")["state"] == "pending"
    assert manager.store.get("observations", "valid-observation")["time"] == 0
    assert manager.retry(item["id"], 3, "retry-once")["enabled"]
    with pytest.raises(Conflict):
        manager.retry("other", 3, "retry-once")
    await close_app(app)


@pytest.mark.asyncio
async def test_partial_recovery_queues_only_media_incomplete_items_once(tmp_path, monkeypatch):
    app = create_app(data_dir=tmp_path / "data"); manager = app.state.slicing
    prepare_run(manager, monkeypatch)
    item = item_fixture(tmp_path)
    item.update(errorCode="media_incomplete", message="末段不可解码")
    item["categories"]["bed"].update(errorCode="media_incomplete", message="末段不可解码")
    manager.store.save("items", item)
    other = item_fixture(tmp_path, "other")
    other.update(errorCode="frame_extract_failed")
    manager.store.save("items", other)

    state = manager.recover_decodable_all(0, "partial-all")
    recovered = manager.store.get("items", item["id"])
    untouched = manager.store.get("items", other["id"])
    assert state["enabled"] and state["run"]["executeAll"] and not state["run"]["singleMode"]
    assert recovered["state"] == "pending" and recovered["partialRecovery"]
    assert recovered["categories"]["bed"]["state"] == "pending"
    assert untouched["state"] == "attention" and not untouched.get("partialRecovery")
    assert manager.recover_decodable_all(0, "partial-all")["enabled"]
    await close_app(app)


@pytest.mark.asyncio
async def test_another_single_video_can_start_after_last_selected_video_failed(tmp_path, monkeypatch):
    app = create_app(data_dir=tmp_path / "data"); manager = app.state.slicing
    prepare_run(manager, monkeypatch)
    selected = item_fixture(tmp_path, "73", "pending")
    manager.store.save("items", selected)
    started = await manager.start("new-selected", 0, "single", selected["id"])
    assert started["run"]["singleItemId"] == selected["id"]
    assert manager.remote.created == 0
    assert (await manager.start("new-selected", -1, "single", selected["id"]))["enabled"]
    await close_app(app)


@pytest.mark.asyncio
async def test_a_worker_failure_only_changes_the_requested_category_and_keeps_position(tmp_path, monkeypatch):
    app = create_app(data_dir=tmp_path / "data"); manager = app.state.slicing
    item = item_fixture(tmp_path, state="pending"); manager.store.save("items", item)
    manager.store.settings["workerPythonPath"] = sys.executable
    monkeypatch.setattr(manager, "_model_fingerprint", lambda category: "model-v1")
    native_exec = asyncio.create_subprocess_exec

    async def fake_exec(*args, **kwargs):
        body = "import sys,json;sys.stdin.readline();print(json.dumps({'event':'error','category':'both','code':'frame_extract_failed','stage':'sampling','time':3360}))"
        return await native_exec(sys.executable, "-c", body, **kwargs)

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_exec)
    await manager._run_worker(item, item["metadata"], {"bed": [3360]}, {"bed": {}}, "initial")
    assert item["categories"]["bed"]["state"] == "attention"
    assert item["categories"]["bed"]["failure"]["time"] == 3360
    assert item["categories"]["magnet"]["state"] == "not_applicable"
    await close_app(app)


@pytest.mark.asyncio
async def test_silent_incomplete_worker_output_is_not_approved_or_no_target(tmp_path, monkeypatch):
    app = create_app(data_dir=tmp_path / "data"); manager = app.state.slicing
    item = item_fixture(tmp_path, state="pending"); manager.store.save("items", item)
    monkeypatch.setattr(manager, "_model_fingerprint", lambda category: "model-v1")
    monkeypatch.setattr(manager, "_run_worker", lambda *args: asyncio.sleep(0))
    await manager._analyze_item(item)
    assert item["categories"]["bed"]["state"] == "attention"
    assert item["categories"]["bed"]["errorCode"] == "observations_incomplete"
    await close_app(app)


def test_legacy_category_flags_repair_only_unsupported_states():
    item = {"sourceDatasetId": "27912", "state": "attention", "categories": {
        "bed": {"state": "pending"}, "magnet": {"state": "attention", "errorCode": "frame_extract_failed"}}}
    assert normalize_item_categories(item)
    assert item["state"] == "pending" and item["categories"]["magnet"]["state"] == "not_applicable"
    blocked = {"sourceDatasetId": "27912", "state": "attention", "errorCode": "task_log_unavailable",
        "categories": {"bed": {"state": "pending"}}}
    normalize_item_categories(blocked)
    assert blocked["state"] == "attention"
    verified = {"sourceDatasetId": "27912", "state": "complete", "categories": {
        "bed": {"state": "complete", "taskId": "810", "outputImageIds": ["1", "2"]}}}
    assert not normalize_item_categories(verified)


@pytest.mark.asyncio
async def test_active_analysis_prevents_settings_and_manual_edits_overwriting_progress(tmp_path, monkeypatch):
    app = create_app(data_dir=tmp_path / "data"); manager = app.state.slicing
    prepare_run(manager, monkeypatch)
    manager.store.settings["enabled"] = True
    with pytest.raises(Conflict, match="暂停"):
        manager.update_settings({"requestId": "edit", "expectedRevision": 0})
    item = item_fixture(tmp_path, state="analyzing"); manager.store.save("items", item)
    with pytest.raises(Conflict, match="不能覆盖"):
        await manager.review(item["id"], {"expectedRevision": 3, "requestId": "review",
            "category": "bed", "confirmedNoTarget": True})
    assert manager.store.get("items", item["id"])["state"] == "analyzing"
    await close_app(app)


@pytest.mark.asyncio
async def test_manual_attention_does_not_keep_scheduler_alive_or_submit_partial_ready_state(tmp_path, monkeypatch):
    app = create_app(data_dir=tmp_path / "data"); manager = app.state.slicing
    prepare_run(manager, monkeypatch)
    manager.run["singleMode"] = False; manager.store.save("runs", manager.run)
    item = item_fixture(tmp_path)
    item["errorCode"] = "task_log_unavailable"
    item["categories"]["bed"]["state"] = "ready"
    manager.store.save("items", item)
    manager.store.settings["enabled"] = True
    await asyncio.wait_for(manager._scheduler(), timeout=0.5)
    assert not manager.store.settings["enabled"]
    assert manager.run["state"] == "attention" and manager.remote.created == 0
    assert manager.store.get("items", item["id"])["state"] == "attention"
    await close_app(app)


@pytest.mark.asyncio
async def test_no_target_remains_distinct_from_verified_slice_completion(tmp_path, monkeypatch):
    app = create_app(data_dir=tmp_path / "data"); manager = app.state.slicing
    prepare_run(manager, monkeypatch)
    item = item_fixture(tmp_path, state="pending"); manager.store.save("items", item)

    async def no_target(value):
        value["categories"]["bed"].update(state="no_target", errorCode="", intervals=[])
        return value["metadata"]

    monkeypatch.setattr(manager, "_analyze_item", no_target)
    await manager._process_item(item)
    saved = manager.store.get("items", item["id"])
    assert saved["state"] == "no_target" and not saved["errorCode"]
    assert manager.state()["counts"] == {"no_target": 1}
    assert manager.remote.created == 0
    await close_app(app)


@pytest.mark.asyncio
async def test_reviewed_intervals_can_start_without_reanalysis_and_keep_failed_task_evidence(tmp_path, monkeypatch):
    app = create_app(data_dir=tmp_path / "data"); manager = app.state.slicing
    prepare_run(manager, monkeypatch)
    item = item_fixture(tmp_path, "73")
    item["categories"]["bed"].update(errorCode="platform_failed", taskId="809", operationId="failed-operation",
        intervals=[{"start": 0, "end": 50}], intervalSeconds=1, modelFingerprint="model-v1")
    manager.store.save("items", item)
    manager.store.save("operations", {"id": "failed-operation", "taskId": "809", "state": "failed"})
    request = {"requestId": "manual-review", "expectedRevision": 3, "category": "bed",
        "intervals": [{"start": 10, "end": 40}]}
    result = await manager.review(item["id"], request)
    assert not result["enabled"]  # Saving a draft does not submit a platform task.
    category = manager.store.get("items", item["id"])["categories"]["bed"]
    assert category["previousAttempts"][0]["taskId"] == "809" and not category.get("taskId")
    assert category["state"] == "ready"
    assert (await manager.review(item["id"], request))["run"]["state"] == "attention"
    await manager.start("submit-confirmed-window", 0, "single", item["id"])
    manager.remote.video_rows["27912"] = [{"id": 73, "cutStatus": 0, "name": item["name"]}]
    await manager._submit_ready(manager.store.get("items", item["id"]))
    saved = manager.store.get("items", item["id"])["categories"]["bed"]
    assert saved["taskId"] == "810" and saved["prefix"].endswith("_R1")
    assert saved["operationId"] != "failed-operation"
    assert manager.store.get("operations", "failed-operation")["taskId"] == "809"
    assert manager.remote.created == 1
    await close_app(app)


@pytest.mark.asyncio
async def test_uncertain_submission_retry_reconciles_without_a_file_or_new_submission(tmp_path, monkeypatch):
    app = create_app(data_dir=tmp_path / "data"); manager = app.state.slicing
    prepare_run(manager, monkeypatch)
    item = item_fixture(tmp_path)
    item.update(path="", errorCode="submit_uncertain")
    item["categories"]["bed"].update(errorCode="submit_uncertain", operationId="uncertain-operation",
        taskId="", preSubmitTaskIds=[])
    manager.store.save("items", item)
    with pytest.raises(Conflict):
        await manager.review(item["id"], {"requestId": "unsafe-new-window", "expectedRevision": 3,
            "category": "bed", "intervals": [{"start": 0, "end": 30}]})
    manager.retry(item["id"], 3, "reconcile-only")
    saved = manager.store.get("items", item["id"])
    assert saved["state"] == "platform_processing" and saved["categories"]["bed"]["state"] == "submitting"
    await manager._reconcile_waiting()
    assert manager.remote.created == 0
    assert manager.store.get("items", item["id"])["categories"]["bed"]["errorCode"] == "submit_uncertain"
    await close_app(app)


@pytest.mark.asyncio
async def test_submission_precheck_failure_is_saved_as_attention_instead_of_running_forever(tmp_path, monkeypatch):
    app = create_app(data_dir=tmp_path / "data"); manager = app.state.slicing
    prepare_run(manager, monkeypatch)
    item = item_fixture(tmp_path, "73", state="platform_processing")
    item["categories"]["bed"].update(state="ready", errorCode="", intervals=[{"start": 0, "end": 30}], intervalSeconds=1)
    manager.store.save("items", item)
    manager.store.settings["targetDatasetIds"]["27912"] = None
    await manager._submit_ready(item)
    saved = manager.store.get("items", item["id"])
    assert saved["state"] == "attention" and saved["errorCode"] == "target_dataset_unresolved"
    assert manager.remote.created == 0
    await close_app(app)
