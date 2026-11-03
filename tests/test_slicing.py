import json
import shutil
import subprocess
from contextlib import redirect_stdout
from io import StringIO

import httpx
import pytest

from app.main import create_app
from app.slicing_core import (build_category_result, choose_slice_interval, expected_image_count,
                              intervals_from_observations, merge_intervals, sample_points)
from app.slicing import data_category, normalize_dataset_name, source_version, target_dataset_identity_matches
from app.slicing_remote import SlicingRemote, source_video_ids, target_dataset_id, task_state
from app.slicing_worker import FrameExtractionError, extract_requested_frames, probe
from app.upload_audit import AthenaClient, AuditError


def test_sampling_always_includes_both_edges_and_deduplicates():
    assert sample_points(61, 30, 25) == [0.0, 30.0, 60.0, 60.96]
    assert sample_points(60.02, 30, 25) == [0.0, 30.0, 59.98]
    assert sample_points(4, 30, 25) == [0.0, 3.96]


def test_concurrent_local_recoveries_promote_single_run_and_stay_scoped(tmp_path, monkeypatch):
    app = create_app(data_dir=tmp_path / "recovery-batch-data")
    manager = app.state.slicing
    manager.auth.access = "test-token"
    manager.store.settings.update(targetDatasetIds={"27912": "900", "27944": "901"})
    manager.store.save_settings()
    monkeypatch.setattr(manager, "_wake_scheduler", lambda: None)

    recoveries = []
    for index in range(3):
        path = tmp_path / f"recovery-{index}.mp4"
        path.write_bytes(f"source-{index}".encode())
        version = source_version(path)
        item = manager.queue_collection_recovery(
            video_id=f"video-{index}", revision=1, path=path, file_version=version,
            metadata={"duration": 120, "frameRate": 25, "width": 3840, "height": 2160},
            name=path.name, request_id=f"recovery-request-{index}")
        recoveries.append(item["id"])

    run = manager.run
    assert run["localRecoveryOnly"] is True
    assert run["singleMode"] is False
    assert "singleItemId" not in run
    assert run["items"] == recoveries
    assert run["itemCount"] == 3

    unrelated = {"id": "unrelated-old-attention", "state": "attention", "categories": {}}
    manager.store.save("items", unrelated)
    scoped = manager._scope_run_items(manager.store.list("items"))
    assert [row["id"] for row in scoped] == recoveries


def test_adjacent_positive_samples_expand_and_merge_at_video_edges():
    observations = [
        {"time": 0, "label": "target"}, {"time": 30, "label": "target"},
        {"time": 60, "label": "clear"}, {"time": 90, "label": "target"},
        {"time": 180, "label": "target"},
    ]
    assert intervals_from_observations(observations, interval=30, duration=200,
                                       positive_labels={"target"}) == [(0.0, 120.0), (150.0, 200.0)]
    assert merge_intervals([(0, 3), (3, 7), (9, 10)]) == [(0.0, 7.0), (9.0, 10.0)]


def test_partial_recovery_keeps_good_fragments_away_from_undecodable_points():
    rows = [{"time": 0, "label": "target"}, {"time": 30, "label": "unavailable"},
            {"time": 60, "label": "target"}]
    assert intervals_from_observations(rows, interval=30, duration=90, positive_labels={"target"},
                                       undecodable_points=[30]) == [(0.0, 0.0), (60.0, 90.0)]


def test_salvage_frame_sampling_skips_bad_positions_without_shifting_timestamps(tmp_path, monkeypatch):
    from types import SimpleNamespace
    source = tmp_path / "source.mp4"; source.write_bytes(b"source")
    ffmpeg = tmp_path / "ffmpeg"; ffmpeg.write_text("stub"); ffmpeg.chmod(0o755)

    def fake_run(command, **_kwargs):
        timestamp = float(command[command.index("-ss") + 1])
        if timestamp in {30, 60.96}:
            return SimpleNamespace(returncode=1, stdout=b"", stderr=b"Error while decoding")
        return SimpleNamespace(returncode=0, stdout=b"\xff\xd8frame\xff\xd9", stderr=f"[Parsed_showinfo_0] n: 0 pts: 0 pts_time:{timestamp}".encode())

    monkeypatch.setattr("app.slicing_worker.subprocess.run", fake_run)
    result = list(extract_requested_frames(str(source), str(ffmpeg),
        {"duration": 62, "frameRate": 25}, {"bed": [0, 30, 60, 60.96]}, skip_undecodable=True))
    assert [timestamp for timestamp, _ in result] == [0, 30, 60, 60.96]
    assert result[1][1] is None and result[3][1] is None
    assert result[0][1] is not None and result[2][1] is not None


@pytest.mark.parametrize("failure", ["io", "tool"])
def test_salvage_sampling_keeps_operational_failures_as_errors(tmp_path, monkeypatch, failure):
    from types import SimpleNamespace
    source = tmp_path / "source.mp4"; source.write_bytes(b"source")
    ffmpeg = tmp_path / "ffmpeg"; ffmpeg.write_text("stub"); ffmpeg.chmod(0o755)

    def fail(_command, **_kwargs):
        if failure == "timeout":
            raise subprocess.TimeoutExpired("ffmpeg", 30)
        if failure == "tool":
            raise OSError("missing tool")
        return SimpleNamespace(returncode=1, stdout=b"", stderr=b"Input/output error")

    monkeypatch.setattr("app.slicing_worker.subprocess.run", fail)
    with pytest.raises(FrameExtractionError) as error:
        list(extract_requested_frames(str(source), str(ffmpeg), {"duration": 10, "frameRate": 25},
                                      {"bed": [0]}, skip_undecodable=True))
    assert error.value.code == {"timeout": "frame_timeout", "io": "frame_io_error",
                                "tool": "frame_tool_error"}[failure]


def test_interval_counts_include_each_segment_start_and_target_around_150():
    intervals = [(0, 599), (900, 1200)]
    assert expected_image_count(intervals, 1) == 901
    choice = choose_slice_interval(intervals)
    assert choice["withinTarget"]
    assert 100 <= choice["count"] <= 200
    assert abs(choice["count"] - 150) <= 2
    short = choose_slice_interval([(0, 20)])
    assert short == {"interval": 1, "count": 21, "withinTarget": False, "tooMany": False}


def test_no_target_and_more_than_200_at_one_second():
    empty = build_category_result([], interval=30, duration=100, positive_labels={"target"})
    assert empty["intervals"] == [] and empty["estimatedImages"] == 0
    many = choose_slice_interval([(0, 250)])
    assert many["withinTarget"] and many["count"] <= 200


def test_task_source_and_target_identity_parse_only_native_config():
    job = {"config": json.dumps({"source": {"datasets": [{"videos": {"ids": [123, "456"]}}]},
                                 "target": {"dataset": {"id": 789}}})}
    assert source_video_ids(job) == {"123", "456"}
    assert target_dataset_id(job) == "789"
    assert task_state({"status": 2}) == "active"
    assert task_state({"status": 3}) == "complete"
    assert task_state({"status": 4}) == "failed"


def test_target_dataset_matching_uses_scoped_namespace_and_normalizes_punctuation():
    expected = {"targetName": "永锋-磁盘吊检测"}
    actual = {"id": 901, "name": "永锋 磁盘吊检测", "dataCategory": "image"}
    assert normalize_dataset_name(actual["name"]) == normalize_dataset_name(expected["targetName"])
    assert target_dataset_identity_matches(actual, expected)
    assert data_category({"dataCategory": 1}) == "image"
    assert data_category({"dataCategory": 2}) == "video"


@pytest.mark.asyncio
async def test_source_video_dataset_is_never_selected_as_output_image_dataset():
    from app.slicing import target_dataset_verified
    remote = FakeSlicingRemote()
    descriptor = {"targetName": "永锋车斗检测"}
    assert await target_dataset_verified(remote, "27912",
        {"id": 27912, "name": "永锋车斗检测", "dataCategory": "image", "namespace": 1}, descriptor) is False
    assert await target_dataset_verified(remote, "900",
        {"id": 900, "name": "永锋车斗检测", "dataCategory": "image", "namespace": 1}, descriptor) is True


class FakeSlicingRemote:
    def __init__(self):
        self.job_rows = {"27912": {"21": [], "22": []}, "27944": {"21": [], "22": []}}
        self.video_rows = {"27912": [], "27944": []}
        self.created = 0
        self.lose_submit = False

    async def list_datasets(self):
        return [
            {"id": 900, "name": "永锋车斗检测", "dataCategory": "image", "namespace": 1},
            {"id": 901, "name": "永锋-磁盘吊检测", "dataCategory": "image", "namespace": 1},
            {"id": 27912, "name": "永锋车斗检测", "dataCategory": "image", "namespace": 1},
        ]

    async def dataset(self, identity):
        return {"id": int(identity), "name": "永锋车斗检测" if str(identity) in {"900", "27912"} else "永锋-磁盘吊检测",
                # Generic dataset metadata is ambiguous for the fixed source
                # collections; the guarded /videos endpoint is authoritative.
                "dataCategory": "image", "namespace": 1}

    async def videos(self, dataset):
        return list(self.video_rows[str(dataset)])

    async def jobs(self, dataset, job_type):
        return list(self.job_rows[str(dataset)][str(job_type)])

    async def logs(self, dataset, job_id):
        return [{"time": "1", "message": f"切片成功 source {job_id}"}]

    async def image_rows(self, dataset, **kwargs):
        return []

    async def is_image_dataset(self, dataset):
        return str(dataset) in {"900", "901"}

    async def create_cut(self, source_dataset, video_id, target_dataset, intervals, interval_seconds, prefix, request_id):
        self.created += 1
        if self.lose_submit:
            self.lose_submit = False
            raise AuditError("提交响应丢失", "uncertain")
        task = {"id": "810", "type": 21, "status": 1}
        config = {"source": {"datasets": [{"videos": {"ids": [int(video_id)]}}]},
                  "target": {"dataset": {"id": int(target_dataset)}},
                  "options": {"slice_periods": intervals,
                      "image": {"interval": interval_seconds, "prefix": prefix},
                      "sliceImage": True, "sliceVideo": False}}
        task["config"] = config
        self.job_rows[str(source_dataset)]["21"].append(task)
        return task


@pytest.mark.skipif(not shutil.which("ffmpeg"), reason="ffmpeg is required for the in-memory extraction check")
def test_requested_video_frames_are_unique_and_stay_in_memory(tmp_path):
    ffmpeg = shutil.which("ffmpeg")
    video = tmp_path / "sample.mp4"
    subprocess.run([ffmpeg, "-hide_banner", "-loglevel", "error", "-f", "lavfi", "-i",
                    "color=c=red:s=320x240:r=5:d=3", "-c:v", "mpeg4", "-y", str(video)],
                   check=True, timeout=20)
    metadata = probe(str(video), shutil.which("ffprobe"))
    points = {"bed": [0, 1, 2, metadata["duration"] - 1 / metadata["frameRate"]],
              "magnet": [0, 2, metadata["duration"] - 1 / metadata["frameRate"]]}
    frames = list(extract_requested_frames(str(video), ffmpeg, metadata, points))
    times = [row[0] for row in frames]
    assert times == sorted(set(times))
    assert times[0] == 0
    assert times[-1] == pytest.approx(metadata["duration"] - 1 / metadata["frameRate"], abs=.02)
    assert len([time for time in times if time == 2]) == 1
    assert all(frame.startswith(b"\xff\xd8") and frame.endswith(b"\xff\xd9") for _, frame in frames)
    assert not list(tmp_path.glob("*.jpg"))


def test_frame_worker_reports_missing_video_and_missing_ffmpeg_separately(tmp_path):
    from app.slicing_worker import process

    request = {"filePath": str(tmp_path / "missing.mp4"), "frameToolPath": "/missing/ffmpeg",
               "settings": {}, "points": {"bed": [0]}}
    output = StringIO()
    with redirect_stdout(output):
        process(request)
    assert json.loads(output.getvalue())["code"] == "local_video_missing"

    video = tmp_path / "video.mp4"
    video.write_bytes(b"video")
    request["filePath"] = str(video)
    output = StringIO()
    with redirect_stdout(output):
        process(request)
    assert json.loads(output.getvalue())["code"] == "frame_tool_missing"


def test_sparse_frame_extraction_limits_ffmpeg_to_requested_outputs(tmp_path, monkeypatch):
    import app.slicing_worker as worker

    ffmpeg = shutil.which("ffmpeg")
    if not ffmpeg:
        pytest.skip("ffmpeg is required for the extraction command check")
    video = tmp_path / "sparse.mp4"
    video.write_bytes(b"fixture")
    observed = []

    def fake_jpeg_frames(command):
        observed.append(command)
        yield b"\xff\xd8fixture\xff\xd9"

    monkeypatch.setattr(worker, "jpeg_frames", fake_jpeg_frames)
    frames = list(worker.extract_requested_frames(str(video), ffmpeg,
        {"duration": 120.0, "frameRate": 25.0}, {"bed": [0.0, 30.0]}))
    assert len(observed) == 2
    assert all(command[command.index("-frames:v") + 1] == "1" for command in observed)
    assert [command[command.index("-ss") + 1] for command in observed] == ["0.000000", "30.000000"]
    assert [timestamp for timestamp, _ in frames] == [0, 30]


def test_frame_filter_compacts_multiple_sampling_grids(tmp_path, monkeypatch):
    import app.slicing_worker as worker

    ffmpeg = shutil.which("ffmpeg")
    if not ffmpeg:
        pytest.skip("ffmpeg is required for the extraction command check")
    video = tmp_path / "grids.mp4"
    video.write_bytes(b"fixture")
    observed = {}

    def fake_jpeg_frames(command):
        observed["filter"] = command[command.index("-vf") + 1]
        yield from [b"\xff\xd8fixture\xff\xd9"] * int(command[command.index("-frames:v") + 1])

    monkeypatch.setattr(worker, "jpeg_frames", fake_jpeg_frames)
    points = {"bed": [0, 2, 4, 6, 8], "magnet": [0, 3, 6, 9]}
    frames = list(worker.extract_requested_frames(str(video), ffmpeg,
        {"duration": 10.0, "frameRate": 25.0}, points))
    assert [timestamp for timestamp, _ in frames] == [0, 2, 3, 4, 6, 8, 9]
    assert len(observed["filter"]) < 100
    assert "not(mod(n\\,2))" in observed["filter"]
    assert "not(mod(n\\,3))" in observed["filter"]


def test_tail_sampling_reads_from_eof_and_keeps_the_last_frame(tmp_path, monkeypatch):
    import app.slicing_worker as worker

    ffmpeg = shutil.which("ffmpeg")
    if not ffmpeg:
        pytest.skip("ffmpeg is required for the extraction command check")
    video = tmp_path / "tail.mp4"
    video.write_bytes(b"fixture")
    observed = {}

    def fake_jpeg_frames(command):
        observed["command"] = command
        yield b"\xff\xd8first\xff\xd9"
        yield b"\xff\xd8last\xff\xd9"

    monkeypatch.setattr(worker, "jpeg_frames", fake_jpeg_frames)
    frames = list(worker.extract_requested_frames(str(video), ffmpeg,
        {"duration": 10.0, "frameRate": 25.0}, {"bed": [9.96]}))
    assert observed["command"][observed["command"].index("-sseof") + 1] == "-1.000000"
    assert frames == [(9.96, b"\xff\xd8last\xff\xd9")]


@pytest.mark.asyncio
async def test_slicing_relinks_moved_video_by_unique_exact_filename(tmp_path, monkeypatch):
    app = create_app(data_dir=tmp_path / "slicing-data")
    manager = app.state.slicing
    original = tmp_path / "moved.mp4"
    original.write_bytes(b"unchanged video")
    version = source_version(original)
    folder = tmp_path / "已上传"
    folder.mkdir()
    video = folder / "moved.mp4"
    original.rename(video)
    manager.store.settings["directories"] = [str(tmp_path)]
    monkeypatch.setattr("app.slicing.ffprobe_file", lambda path, _tool: {"duration": 60.0, "size": version["size"],
        "width": 3840, "height": 2160, "frameRate": 25.0})
    item = {"id": "moved", "sourceDatasetId": "27912", "name": video.name, "path": str(tmp_path / video.name), "fileVersion": version,
            "categories": {category: {"state": "attention", "errorCode": "frame_extract_failed"}
                           for category in ("bed", "magnet")}}
    manager.store.save("items", item)

    assert await manager._refresh_local_binding(item)
    assert item["path"] == str(video)
    assert item["fileVersion"] == version
    assert item["categories"]["bed"]["state"] == "pending"
    assert item["categories"]["magnet"]["state"] == "not_applicable"
    assert manager.store.get("items", "moved")["path"] == str(video)
    await manager.shutdown()
    await app.state.upload_audit.shutdown(); await app.state.videos.shutdown(); await app.state.jobs.shutdown()
    app.state.store.close()


@pytest.mark.asyncio
async def test_startup_recovers_moved_frame_extraction_failures_but_keeps_unrelated_attention(tmp_path, monkeypatch):
    app = create_app(data_dir=tmp_path / "slicing-data")
    manager = app.state.slicing
    original = tmp_path / "recover.mp4"
    original.write_bytes(b"same video")
    version = source_version(original)
    folder = tmp_path / "已上传"
    folder.mkdir()
    moved = folder / original.name
    original.rename(moved)
    manager.store.settings["directories"] = [str(tmp_path)]
    monkeypatch.setattr("app.slicing.ffprobe_file", lambda path, _tool: {"duration": 90.0, "size": version["size"],
        "width": 3840, "height": 2160, "frameRate": 25.0})
    item = {"id": "recover", "sourceDatasetId": "27912", "name": moved.name, "path": str(original), "fileVersion": version,
            "state": "attention", "errorCode": "frame_extract_failed", "revision": 1,
            "categories": {category: {"state": "attention", "errorCode": "frame_extract_failed"}
                           for category in ("bed", "magnet")}}
    unrelated = {"id": "unrelated", "name": "platform-error.mp4", "path": "", "fileVersion": None,
                 "state": "attention", "errorCode": "task_log_unavailable", "revision": 1,
                 "categories": {"bed": {"state": "attention", "errorCode": "task_log_unavailable"}}}
    same_path = tmp_path / "same-path.mp4"
    same_path.write_bytes(b"video with prior ffmpeg failure")
    same_version = source_version(same_path)
    retry_once = {"id": "retry-once", "sourceDatasetId": "27912", "name": same_path.name, "path": str(same_path), "fileVersion": same_version,
                  "state": "attention", "errorCode": "frame_extract_failed", "revision": 1,
                  "categories": {"bed": {"state": "attention", "errorCode": "frame_extract_failed"}}}
    manager.store.save("items", item)
    manager.store.save("items", unrelated)
    manager.store.save("items", retry_once)

    await manager.startup()
    recovered = manager.store.get("items", "recover")
    assert recovered["state"] == "pending" and recovered["errorCode"] == ""
    assert recovered["path"] == str(moved)
    assert recovered["categories"]["bed"]["state"] == "pending"
    assert recovered["categories"]["magnet"]["state"] == "not_applicable"
    preserved = manager.store.get("items", "unrelated")
    assert preserved["state"] == "attention" and preserved["errorCode"] == "task_log_unavailable"
    retried = manager.store.get("items", "retry-once")
    assert retried["state"] == "pending" and retried["frameRepairAttemptedVersion"] == same_version
    retried.update(state="attention", errorCode="frame_extract_failed")
    retried["categories"]["bed"].update(state="attention", errorCode="frame_extract_failed")
    manager.store.save("items", retried)
    await manager.startup()
    assert manager.store.get("items", "retry-once")["state"] == "attention"
    await manager.shutdown()
    await app.state.upload_audit.shutdown(); await app.state.videos.shutdown(); await app.state.jobs.shutdown()
    app.state.store.close()


@pytest.mark.asyncio
async def test_slicing_does_not_choose_between_duplicate_local_video_names(tmp_path):
    app = create_app(data_dir=tmp_path / "slicing-data")
    manager = app.state.slicing
    for folder in (tmp_path / "a", tmp_path / "b"):
        folder.mkdir()
        (folder / "same.mp4").write_bytes(folder.name.encode())
    manager.store.settings["directories"] = [str(tmp_path)]
    item = {"id": "ambiguous", "name": "same.mp4", "path": str(tmp_path / "old" / "same.mp4"),
            "fileVersion": {"size": 1}, "categories": {}}

    with pytest.raises(ValueError, match="local_video_ambiguous"):
        await manager._refresh_local_binding(item)
    await manager.shutdown()
    await app.state.upload_audit.shutdown(); await app.state.videos.shutdown(); await app.state.jobs.shutdown()
    app.state.store.close()


@pytest.mark.asyncio
async def test_scan_uses_video_endpoint_when_generic_category_conflicts_and_deduplicates_cut_jobs(tmp_path, monkeypatch):
    app = create_app(data_dir=tmp_path / "slicing-data")
    manager = app.state.slicing
    manager.auth.access = "test-slicing-token"
    remote = FakeSlicingRemote()
    manager.remote = remote
    remote.video_rows["27912"] = [
        {"id": 31, "name": "active.mp4", "cutStatus": 0},
        {"id": 32, "name": "history.mp4", "cutStatus": 0},
        {"id": 33, "name": "failed.mp4", "cutStatus": 0},
        {"id": 34, "name": "new.mp4", "cutStatus": 0},
        {"id": 35, "name": "already-cut.mp4", "cutStatus": 1},
    ]
    remote.job_rows["27912"]["21"] = [
        {"id": 101, "type": 21, "status": 1, "config": {"source": {"datasets": [{"videos": {"ids": [31]}}]},
            "target": {"dataset": {"id": 900}}}},
        {"id": 102, "type": 21, "status": 3},  # Job log identifies video 32.
        {"id": 103, "type": 21, "status": 4, "config": {"source": {"datasets": [{"videos": {"ids": [33]}}]}}},
    ]
    async def logs(dataset, job_id):
        if job_id == "102":
            return [{"time": "1", "message": "finished source video 32"}]
        return []
    remote.logs = logs
    monkeypatch.setattr("app.slicing.scan_local_files", lambda directories: ([], directories, []))
    async def make_item(source_id, row, name, matches, targets, run_id):
        return {"id": f"item-{row['id']}", "runId": run_id, "sourceDatasetId": source_id,
                "sourceVideoId": str(row["id"]), "name": name, "state": "pending", "revision": 0,
                "categories": {"bed": {"state": "pending"}, "magnet": {"state": "pending"}}}
    manager._make_item = make_item
    result = manager.scan("scan-dedup-1")
    await manager.scan_task
    assert result["run"]["state"] == "scanning"
    state = manager.state()
    assert state["run"]["state"] == "complete", state["run"]
    assert {row["sourceVideoId"] for row in state["items"]} == {"31", "33", "34"}
    active = next(row for row in state["items"] if row["sourceVideoId"] == "31")
    assert active["state"] == "platform_processing"
    assert active["categories"]["bed"]["taskId"] == "101"
    assert active["categories"]["bed"]["state"] == "waiting"
    assert state["run"]["excluded"] == {"cut": 1, "active": 0, "complete": 1, "submitted": 0}
    assert state["run"]["taskLogIndex"] and all(row["type"] in {"21", "22"} for row in state["run"]["taskLogIndex"])


@pytest.mark.asyncio
async def test_scan_resolves_scoped_targets_when_dataset_rows_omit_namespace(tmp_path, monkeypatch):
    app = create_app(data_dir=tmp_path / "slicing-data")
    manager = app.state.slicing
    manager.auth.access = "test-slicing-token"
    remote = FakeSlicingRemote()
    async def list_datasets():
        return [
            {"id": 900, "name": "永锋车斗检测", "dataCategory": "image"},
            {"id": 901, "name": "永锋磁盘吊检测"},
        ]
    remote.list_datasets = list_datasets
    manager.remote = remote
    monkeypatch.setattr("app.slicing.scan_local_files", lambda directories: ([], directories, []))
    manager.scan("scan-scoped-targets")
    await manager.scan_task
    run = manager.state()["run"]
    assert run["state"] == "complete"
    assert run["targetDatasets"]["27912"]["resolved"] is True
    assert run["targetDatasets"]["27912"]["selectedId"] == "900"
    assert run["targetDatasets"]["27912"]["candidates"][0]["namespaceInferred"] is True
    assert run["targetDatasets"]["27944"]["resolved"] is True
    assert run["targetDatasets"]["27944"]["selectedId"] == "901"


@pytest.mark.asyncio
async def test_unique_local_filename_does_not_require_remote_source_metadata_match(tmp_path, monkeypatch):
    app = create_app(data_dir=tmp_path / "slicing-data")
    manager = app.state.slicing
    video = tmp_path / "source.mp4"
    video.write_bytes(b"local video fixture")
    version = source_version(video)
    monkeypatch.setattr("app.slicing.ffprobe_file", lambda path, tool: {"duration": 120.0, "size": version["size"],
        "width": 1920, "height": 1080, "frameRate": 25.0})
    local = {"name": video.name, "path": str(video), "size": version["size"], "version": version}
    item = await manager._make_item("27912", {"id": 123, "name": video.name, "cutStatus": 0,
                                    "size": version["size"] + 100, "duration": 240}, video.name,
                                    [local], {"27912": {"resolved": True}}, "run-test")
    assert item["state"] == "pending"
    assert item["errorCode"] == ""
    assert item["path"] == str(video)
    assert item["metadata"]["duration"] == 120.0


@pytest.mark.asyncio
async def test_restart_releases_unchanged_unique_local_file_from_old_confirmation_gate(tmp_path):
    app = create_app(data_dir=tmp_path / "slicing-data")
    manager = app.state.slicing
    video = tmp_path / "source.mp4"
    video.write_bytes(b"local video fixture")
    version = source_version(video)
    item = {"id": "old-metadata-error", "sourceDatasetId": "27912", "sourceVideoId": "123",
        "path": str(video), "fileVersion": version,
        "remote": {"size": None, "duration": None}, "errorCode": "local_video_metadata_missing",
        "state": "attention", "revision": 2,
        "categories": {"bed": {"state": "attention", "errorCode": "local_video_metadata_missing"},
                       "magnet": {"state": "attention", "errorCode": "local_video_metadata_missing"}}}
    manager.store.save("items", item)
    run = {"id": "old-run", "state": "complete", "message": "scan complete"}
    manager.store.settings.update(latestRunId=run["id"], enabled=False)
    manager.store.save_settings()
    manager.store.save("runs", run)
    restarted = type(manager)(manager.store, manager.auth, tmp_path / "slicing-data")
    migrated = restarted.store.get("items", item["id"])
    assert migrated["errorCode"] == ""
    assert migrated["state"] == "pending"
    assert migrated["revision"] == 4
    assert migrated["categories"]["bed"]["state"] == "pending"
    assert migrated["categories"]["magnet"]["state"] == "not_applicable"


def test_restart_keeps_changed_local_video_in_attention(tmp_path):
    app = create_app(data_dir=tmp_path / "slicing-data")
    manager = app.state.slicing
    video = tmp_path / "changed.mp4"
    video.write_bytes(b"new content")
    item = {"id": "changed-local", "sourceDatasetId": "27912", "sourceVideoId": "123",
        "path": str(video), "fileVersion": {"size": 1}, "metadata": {"duration": 12.0},
        "remote": {"size": None, "duration": None}, "errorCode": "local_video_metadata_missing",
        "state": "attention", "revision": 2,
        "categories": {"bed": {"state": "attention", "errorCode": "local_video_metadata_missing"},
                       "magnet": {"state": "attention", "errorCode": "local_video_metadata_missing"}}}
    manager.store.save("items", item)
    restarted = type(manager)(manager.store, manager.auth, tmp_path / "slicing-data")
    migrated = restarted.store.get("items", item["id"])
    assert migrated["state"] == "attention"
    assert migrated["revision"] == 3
    assert migrated["categories"]["bed"]["state"] == "attention"
    assert migrated["categories"]["magnet"]["state"] == "not_applicable"


@pytest.mark.asyncio
async def test_lost_cut_response_is_reconciled_without_a_second_submit(tmp_path):
    app = create_app(data_dir=tmp_path / "slicing-data")
    manager = app.state.slicing
    manager.auth.access = "test-slicing-token"
    remote = FakeSlicingRemote()
    manager.remote = remote
    manager.store.settings["targetDatasetIds"] = {"27912": "900", "27944": "901"}
    path = tmp_path / "source.mp4"
    path.write_bytes(b"immutable fixture video")
    from app.slicing import source_version
    item = {"id": "item-55", "sourceDatasetId": "27912", "sourceVideoId": "55", "name": "source.mp4",
            "path": str(path), "fileVersion": source_version(path), "metadata": {"duration": 60}, "state": "pending",
            "revision": 0, "categories": {"bed": {"state": "ready", "intervals": [{"start": 0, "end": 59}],
                "intervalSeconds": 1, "modelFingerprint": "bed-v1"}, "magnet": {"state": "no_target", "intervals": []}}}
    manager.store.save("items", item)
    remote.video_rows["27912"] = [{"id": 55, "name": "source.mp4", "cutStatus": 0}]
    remote.lose_submit = True
    await manager._submit_ready(item)
    saved = manager.store.get("items", item["id"])
    assert remote.created == 1
    assert saved["categories"]["bed"]["state"] == "submitting"
    discovered = {"id": 810, "type": 21, "status": 1,
        "config": {"source": {"datasets": [{"videos": {"ids": [55]}}]}, "target": {"dataset": {"id": 900}},
            "options": {"slice_periods": [{"start": 0, "end": 59}],
                "image": {"interval": 1, "prefix": "source_55_C"}, "sliceImage": True, "sliceVideo": False}}}
    remote.job_rows["27912"]["21"].append(discovered)
    await manager._reconcile_waiting()
    saved = manager.store.get("items", item["id"])
    assert saved["categories"]["bed"]["taskId"] == "810"
    assert saved["categories"]["bed"]["state"] == "waiting"
    await manager._submit_ready(saved)
    assert remote.created == 1


@pytest.mark.asyncio
async def test_enabled_queue_intent_survives_shutdown_and_restart(tmp_path):
    app = create_app(data_dir=tmp_path / "slicing-data")
    manager = app.state.slicing
    run = {"id": "run-resume", "state": "running", "message": "processing"}
    manager.run = run
    manager.store.settings.update(enabled=True, latestRunId=run["id"])
    manager.store.save_settings()
    manager.store.save("runs", run)
    await manager.shutdown()
    assert manager.store.settings["enabled"] is True

    restarted = type(manager)(manager.store, manager.auth, tmp_path / "slicing-data")
    assert restarted.store.settings["enabled"] is True
    assert restarted.state()["run"]["state"] == "interrupted"


def test_manual_resolution_does_not_start_an_unenabled_batch(tmp_path):
    app = create_app(data_dir=tmp_path / "slicing-data")
    manager = app.state.slicing
    run = {"id": "run-attention", "state": "attention", "message": "needs review"}
    manager.store.settings["latestRunId"] = run["id"]
    manager.store.settings["enabled"] = False
    manager.store.save_settings()
    manager.store.save("runs", run)
    manager.run = run
    manager._continue_after_manual_action()
    assert manager.store.settings["enabled"] is False
    assert manager.run["state"] == "attention"


def test_manual_resolution_continues_a_batch_that_was_already_enabled(tmp_path):
    app = create_app(data_dir=tmp_path / "slicing-data")
    manager = app.state.slicing
    run = {"id": "run-attention", "state": "attention", "message": "needs review"}
    manager.store.settings.update(latestRunId=run["id"], enabled=True)
    manager.store.save_settings()
    manager.store.save("runs", run)
    manager.run = run
    manager._continue_after_manual_action()
    assert manager.store.settings["enabled"] is True
    assert manager.run["state"] == "running"


@pytest.mark.asyncio
async def test_pilot_finishes_only_after_both_target_outputs_are_verified(tmp_path):
    app = create_app(data_dir=tmp_path / "slicing-data")
    manager = app.state.slicing
    manager.auth.access = "test-slicing-token"
    manager.remote = FakeSlicingRemote()
    run = {"id": "run-pilot", "state": "running", "pilotMode": True, "pilotDoneTargets": ["900", "901"]}
    manager.run = run
    manager.store.settings.update(enabled=True, latestRunId=run["id"], targetDatasetIds={"27912": "900", "27944": "901"})
    manager.store.save_settings()
    manager.store.save("runs", run)
    await manager._tick()
    assert manager.state()["run"]["state"] == "pilot_complete"
    assert manager.store.settings["enabled"] is False


@pytest.mark.asyncio
async def test_single_slice_processes_only_the_selected_video_and_stops(tmp_path, monkeypatch):
    app = create_app(data_dir=tmp_path / "slicing-data")
    manager = app.state.slicing
    manager.auth.access = "test-slicing-token"
    manager.remote = FakeSlicingRemote()
    manager.store.settings.update(latestRunId="run-single", targetDatasetIds={"27912": "900", "27944": None})
    manager.store.save_settings()
    video = tmp_path / "selected.mp4"
    video.write_bytes(b"immutable local source")
    from app.slicing import source_version
    run = {"id": "run-single", "state": "complete", "settingsRevision": manager.store.settings["revision"]}
    manager.run = run
    manager.store.save("runs", run)
    selected = {"id": "selected", "runId": "run-single", "sourceDatasetId": "27912",
        "sourceVideoId": "71", "name": video.name, "path": str(video), "fileVersion": source_version(video),
        "state": "pending", "revision": 0,
        "categories": {"bed": {"state": "pending", "taskId": ""},
                       "magnet": {"state": "not_applicable", "taskId": ""}}}
    other = {"id": "other", "runId": "run-single", "sourceDatasetId": "27912",
        "sourceVideoId": "72", "name": "other.mp4", "path": "", "state": "pending", "revision": 0,
        "categories": {"bed": {"state": "pending"}, "magnet": {"state": "not_applicable"}}}
    manager.store.save("items", selected)
    manager.store.save("items", other)
    monkeypatch.setattr(manager, "_wake_scheduler", lambda: None)

    started = await manager.start("single-request", manager.store.settings["revision"], "single", "selected")
    assert started["run"]["singleItemId"] == "selected"
    assert started["run"]["singleMode"] is True
    manager.run.update(state="attention", message="the selected single run needs retry")
    manager.store.settings["enabled"] = False
    manager.store.save_settings()
    manager.store.save("runs", manager.run)
    resumed = await manager.start("single-retry-request", manager.store.settings["revision"], "single", "selected")
    assert resumed["run"]["state"] == "running"
    touched = []

    async def finish_selected(item):
        touched.append(item["id"])
        item["categories"]["bed"].update(state="complete", taskId="task-1")
        item["state"] = "complete"
        manager.store.save("items", item)

    monkeypatch.setattr(manager, "_process_item", finish_selected)
    await manager._tick()
    await manager.analysis_task
    await manager._tick()
    assert touched == ["selected"]
    assert manager.store.get("items", "other")["state"] == "pending"
    assert manager.state()["run"]["state"] == "complete"
    assert manager.store.settings["enabled"] is False
    await manager.shutdown()
    await app.state.upload_audit.shutdown(); await app.state.videos.shutdown(); await app.state.jobs.shutdown()
    app.state.store.close()


@pytest.mark.asyncio
async def test_batch_mode_opens_only_after_pilot_and_releases_deferred_categories(tmp_path, monkeypatch):
    app = create_app(data_dir=tmp_path / "slicing-data")
    manager = app.state.slicing
    manager.auth.access = "test-slicing-token"
    manager.remote = FakeSlicingRemote()
    run = {"id": "run-pilot", "state": "pilot_complete", "settingsRevision": 0,
           "pilotMode": True, "pilotDoneTargets": ["900", "901"]}
    manager.run = run
    manager.store.settings.update(latestRunId=run["id"], targetDatasetIds={"27912": "900", "27944": "901"})
    manager.store.save_settings()
    manager.store.save("runs", run)
    item = {"id": "deferred-video", "sourceDatasetId": "27912", "sourceVideoId": "71", "revision": 0,
            "categories": {"bed": {"state": "pilot_deferred"}, "magnet": {"state": "no_target"}}}
    manager.store.save("items", item)
    woken = []
    monkeypatch.setattr(manager, "_wake_scheduler", lambda: woken.append(True))
    result = await manager.start("start-full-1", 0, "all")
    assert result["run"]["state"] == "running"
    assert result["run"]["pilotMode"] is False
    assert next(row for row in result["items"] if row["id"] == item["id"])["categories"]["bed"]["state"] == "ready"
    assert woken


@pytest.mark.asyncio
async def test_unread_existing_task_log_blocks_candidate_submission(tmp_path, monkeypatch):
    app = create_app(data_dir=tmp_path / "slicing-data")
    manager = app.state.slicing
    manager.auth.access = "test-slicing-token"
    remote = FakeSlicingRemote()
    manager.remote = remote
    remote.video_rows["27912"] = [
        {"id": 41, "name": "active.mp4", "cutStatus": 0},
        {"id": 42, "name": "new.mp4", "cutStatus": 0},
    ]
    remote.job_rows["27912"]["21"] = [
        {"id": 201, "type": 21, "status": 1, "config": {"source": {"datasets": [{"videos": {"ids": [41]}}]}}},
    ]
    async def bad_log(dataset, job_id):
        raise AuditError("log unavailable", "response")
    remote.logs = bad_log
    monkeypatch.setattr("app.slicing.scan_local_files", lambda directories: ([], directories, []))
    async def make_item(source_id, row, name, matches, targets, run_id):
        return {"id": f"item-{row['id']}", "runId": run_id, "sourceDatasetId": source_id,
                "sourceVideoId": str(row["id"]), "name": name, "state": "pending", "revision": 0,
                "categories": {"bed": {"state": "pending"}, "magnet": {"state": "pending"}}}
    manager._make_item = make_item
    manager.scan("scan-log-error-1")
    await manager.scan_task
    state = manager.state()
    candidate = next(row for row in state["items"] if row["sourceVideoId"] == "42")
    assert candidate["state"] == "attention"
    assert candidate["errorCode"] == "task_log_unavailable"
    with pytest.raises(ValueError, match="重新扫描"):
        manager.retry(candidate["id"], candidate["revision"], "retry-blocked")


@pytest.mark.asyncio
async def test_cut_write_uses_type_21_target_and_does_not_replay_uncertain_response():
    captured = []

    def handler(request):
        captured.append(request)
        return httpx.Response(200, json={"data": {"id": "810", "type": 21, "status": 1}})

    auth = AthenaClient(transport=httpx.MockTransport(handler))
    auth.access = "test-token"
    remote = SlicingRemote(auth)
    result = await remote.create_cut("27912", "123", "88001", [{"start": 10, "end": 500}, {"start": 800, "end": 1000}],
                                    4, "sample_C", "slice-request-1")
    assert result["id"] == "810"
    assert len(captured) == 1
    body = json.loads(captured[0].content)
    assert body["type"] == 21 and body["dataset"] == 27912
    assert body["config"]["target"]["dataset"]["id"] == 88001
    assert body["config"]["source"]["datasets"][0]["videos"]["ids"] == [123]
    assert body["config"]["options"]["slice_periods"] == [{"start": 10, "end": 500}, {"start": 800, "end": 1000}]
    assert body["config"]["options"]["sliceVideo"] is False
    assert captured[0].headers["idempotency-key"] == "slice-request-1"
    await auth.client.aclose()


@pytest.mark.asyncio
async def test_image_dataset_probe_uses_read_only_namespace_scoped_content_route():
    captured = []
    def handler(request):
        captured.append(request)
        return httpx.Response(200, json={"data": [{"id": 901}]})
    auth = AthenaClient(transport=httpx.MockTransport(handler))
    auth.access = "test-token"
    assert await SlicingRemote(auth).is_image_dataset("901") is True
    assert len(captured) == 1
    assert captured[0].url.path.endswith("/devel/datasets/901/images/")
    assert captured[0].url.params["namespace"] == "1"
    await auth.client.aclose()


@pytest.mark.asyncio
async def test_target_dataset_listing_walks_platform_picker_group_tree():
    captured = []
    def handler(request):
        captured.append(request)
        if request.url.path.endswith("/dataset-groups/0/details/"):
            data = {"data": [{"id": 0, "groups": [{"id": 7}],
                               "datasets": [{"id": 899, "name": "其他数据集"}]}]}
        elif request.url.path.endswith("/dataset-groups/7/details/"):
            data = {"data": {"result": [{"id": 7, "groups": [{"id": 0}],
                                           "datasets": [{"id": 900, "name": "永锋车斗检测", "dataCategory": 1}]}]}}
        else:
            return httpx.Response(404)
        return httpx.Response(200, json=data)
    auth = AthenaClient(transport=httpx.MockTransport(handler))
    auth.access = "test-token"
    rows = await SlicingRemote(auth).list_datasets()
    assert [row["id"] for row in rows] == [899, 900]
    assert len(captured) == 2
    assert all(request.url.params["namespace"] == "1" for request in captured)
    assert {request.url.path.split("/")[-3] for request in captured} == {"0", "7"}
    await auth.client.aclose()


@pytest.mark.asyncio
async def test_cut_uncertain_response_is_not_replayed():
    calls = []

    def handler(request):
        calls.append(request)
        return httpx.Response(503)

    auth = AthenaClient(transport=httpx.MockTransport(handler))
    auth.access = "test-token"
    with pytest.raises(Exception, match="先核对平台任务"):
        await SlicingRemote(auth).create_cut("27944", "123", "88002", [{"start": 0, "end": 90}], 1, "clip_D", "lost-response")
    assert len(calls) == 1
    await auth.client.aclose()


@pytest.mark.asyncio
async def test_slicing_state_and_settings_are_separate_and_revisioned(tmp_path):
    app = create_app(data_dir=tmp_path / "slicing-data")
    manager = app.state.slicing
    initial = manager.state()
    assert initial["settings"]["directories"] == ["/Volumes/out/视频"]
    assert initial["items"] == []
    payload = {**initial["settings"], "expectedRevision": initial["settingsRevision"], "requestId": "settings-test-1",
               "directories": [str(tmp_path / "videos")], "magnetDeployDirectory": str(tmp_path / "magnet-model"),
               "magnetModelPath": str(tmp_path / "magnet-model" / "model.onnx"),
               "bedDeployDirectory": str(tmp_path / "bed-model"), "bedModelPath": str(tmp_path / "bed-model" / "model.onnx"),
               "bedGeometryPath": str(tmp_path / "bed-model" / "geometry.yaml"),
               "frameToolPath": str(tmp_path / "ffmpeg"), "workerPythonPath": str(tmp_path / "python"),
               "targetDatasetIds": {"27912": "801", "27944": "802"}}
    (tmp_path / "videos").mkdir()
    saved = manager.update_settings(payload)
    assert saved["settingsRevision"] == 1
    assert saved["settings"]["targetDatasetIds"] == {"27912": "801", "27944": "802"}
    with pytest.raises(Exception, match="已被其他页面修改"):
        manager.update_settings({**payload, "requestId": "settings-test-2"})


def test_open_platform_work_shortens_the_next_check_without_changing_idle_wait(tmp_path):
    from app.slicing import ACTIVE_TICK_SECONDS, TICK_SECONDS
    from app.uploads import ACTIVE_POLL_SECONDS, CHECK_SECONDS
    from app.videos import ACTIVE_POLL_SECONDS as VIDEO_ACTIVE, IDLE_POLL_SECONDS
    app = create_app(data_dir=tmp_path / "poll-data")
    assert app.state.videos._poll_timeout() == IDLE_POLL_SECONDS == 30
    assert app.state.uploads._poll_timeout() == CHECK_SECONDS == 30
    assert app.state.slicing._poll_timeout() == TICK_SECONDS == 30
    app.state.videos.store.save({"id": "waiting-video", "versions": [{"state": "waiting"}]})
    app.state.uploads.store.save("items", {"id": "queued-upload", "state": "queued", "dataset": "27912"})
    app.state.slicing.store.save("items", {"id": "cutting", "state": "platform_processing", "categories": {}})
    assert app.state.videos._poll_timeout() == VIDEO_ACTIVE == 8
    assert app.state.uploads._poll_timeout() == ACTIVE_POLL_SECONDS == 8
    assert app.state.slicing._poll_timeout() == ACTIVE_TICK_SECONDS == 8
    app.state.uploads.store.save("items", {"id": "queued-upload", "state": "complete", "dataset": "27912"})
    assert app.state.uploads._poll_timeout() == 30


def test_progress_updates_keep_the_slicing_revision_until_the_outcome_changes(tmp_path):
    app = create_app(data_dir=tmp_path / "revision-data")
    manager = app.state.slicing
    item = {"id": "analysis", "sourceDatasetId": "27912", "sourceVideoId": "7", "state": "analyzing",
            "revision": 4, "progress": 1, "categories": {"bed": {"state": "pending", "intervals": []},
                                                        "magnet": {"state": "not_applicable", "intervals": []}}}
    manager.store.save("items", item)
    item.update(progress=40, analysisDone=3, analysisPosition=12, revision=99)
    manager._save_analysis_item(item)
    assert manager.store.get("items", "analysis")["revision"] == 4
    item["categories"]["bed"].update(state="ready", intervals=[{"start": 0, "end": 10}])
    manager._save_analysis_item(item)
    assert manager.store.get("items", "analysis")["revision"] == 5


@pytest.mark.asyncio
async def test_two_slots_cut_different_videos_and_a_duplicate_reuses_the_existing_task(tmp_path):
    app = create_app(data_dir=tmp_path / "slots")
    manager = app.state.slicing
    manager.auth.access = "isolated"
    remote = FakeSlicingRemote()
    manager.remote = remote
    manager.store.settings["targetDatasetIds"] = {"27912": "900", "27944": "901"}
    created = []

    async def create_cut(source, video, target, intervals, seconds, prefix, request):
        created.append(str(video))
        task = {"id": str(800 + len(created)), "type": 21, "status": 1,
                "config": {"source": {"datasets": [{"videos": {"ids": [int(video)]}}]},
                           "target": {"dataset": {"id": int(target)}}}}
        remote.job_rows[str(source)]["21"].append(task)
        return task

    remote.create_cut = create_cut
    for number in (1, 2, 3):
        path = tmp_path / f"v{number}.mp4"
        path.write_bytes(b"video")
        manager.store.save("items", {"id": f"item-{number}", "sourceDatasetId": "27912", "sourceVideoId": str(number),
            "name": path.name, "path": str(path), "fileVersion": source_version(path), "state": "platform_processing",
            "revision": 2, "categories": {"bed": {"state": "ready", "intervals": [{"start": 0, "end": 10}],
                "intervalSeconds": 1, "modelFingerprint": "bed"}, "magnet": {"state": "not_applicable", "intervals": []}}})
        remote.video_rows["27912"].append({"id": number, "name": path.name, "cutStatus": 0})
    duplicate = tmp_path / "dup.mp4"
    duplicate.write_bytes(b"video")
    manager.store.save("items", {"id": "item-dup", "sourceDatasetId": "27912", "sourceVideoId": "1",
        "name": duplicate.name, "path": str(duplicate), "fileVersion": source_version(duplicate),
        "state": "platform_processing", "revision": 2,
        "categories": {"bed": {"state": "ready", "intervals": [{"start": 0, "end": 10}],
            "intervalSeconds": 1, "modelFingerprint": "bed"}, "magnet": {"state": "not_applicable", "intervals": []}}})
    for item in manager.store.list("items"):
        if any(category.get("state") in {"ready", "waiting_slot"} for category in item["categories"].values()):
            await manager._submit_ready(manager.store.get("items", item["id"]))
    saved = {row["id"]: row for row in manager.store.list("items")}
    assert created == ["1", "2"]
    assert saved["item-3"]["categories"]["bed"]["state"] == "waiting_slot"
    assert saved["item-dup"]["categories"]["bed"]["state"] == "waiting"
    assert saved["item-dup"]["categories"]["bed"]["taskId"] == saved["item-1"]["categories"]["bed"]["taskId"]
    assert saved["item-1"]["categories"]["bed"]["state"] == "waiting"
