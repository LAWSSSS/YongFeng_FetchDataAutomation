"""Repeatable fault evidence for the isolated real-video rehearsal.

This module never opens the production database or platform. ``source`` must be
the rehearsal's disposable copy. Small derived files are removed after checking;
only JSON/JUnit evidence is retained. Contract cases are deliberately labelled
separately from real-media/real-worker checks.
"""
from __future__ import annotations

import asyncio
import json
import os
import shutil
import sys
import tempfile
import time
import xml.etree.ElementTree as ET
from pathlib import Path

PROJECT = Path(__file__).resolve().parents[1]

# Existing isolated tests exercise actual managers with disposable SQLite stores.
# Their platform/tool boundary injections are not evidence of real inference.
CONTRACTS = [
    ("抽帧运行故障", "tests/test_slicing.py::test_salvage_sampling_keeps_operational_failures_as_errors"),
    ("坏位置跳过", "tests/test_slicing.py::test_salvage_frame_sampling_skips_bad_positions_without_shifting_timestamps"),
    ("共享抽帧", "tests/test_slicing.py::test_requested_video_frames_are_unique_and_stay_in_memory"),
    ("类别隔离", "tests/test_slicing_recovery.py::test_a_worker_failure_only_changes_the_requested_category_and_keeps_position"),
    ("不完整输出", "tests/test_slicing_recovery.py::test_silent_incomplete_worker_output_is_not_approved_or_no_target"),
    ("未检出终态", "tests/test_slicing_recovery.py::test_no_target_remains_distinct_from_verified_slice_completion"),
    ("单条本地流程", "tests/test_pipeline_scheduling.py::test_single_retry_mode_never_changes_local_photo_export_route"),
    ("双类别汇总", "tests/test_pipeline_scheduling.py::test_one_empty_category_cannot_hide_another_completed_result"),
    ("临时类别重试", "tests/test_pipeline_scheduling.py::test_category_retries_have_independent_delays_and_exhaustion"),
    ("慢平台不阻塞补槽", "tests/test_pipeline_scheduling.py::test_slow_remote_poll_cannot_hold_up_refilling_the_local_pool"),
    ("双图片集独立", "tests/test_pipeline_scheduling.py::test_slow_photo_target_never_blocks_the_other_target_next_cycle"),
    ("后台登记及时返回", "tests/test_continuous_pipeline.py::test_batch_ack_is_immediate_and_replay_does_not_repeat_slow_work"),
    ("后台登记重启", "tests/test_continuous_pipeline.py::test_restart_replays_same_subrequest_and_retains_completed_outcomes"),
    ("确认后才接入", "tests/test_continuous_pipeline.py::test_new_accepted_download_enrolls_but_unknown_file_and_new_draft_do_not"),
    ("暂停与重复请求", "tests/test_continuous_pipeline.py::test_default_off_and_requests_are_revision_checked_and_idempotent"),
    ("更正优先", "tests/test_media_control.py::test_correction_preempts_only_same_media_and_stale_saves_cannot_resurrect_it"),
    ("版本确认与旧凭证", "tests/test_media_control.py::test_new_version_waits_for_acceptance_and_old_platform_receipts_stay_historical"),
    ("读租约与更正", "tests/test_media_control.py::test_takeover_waits_for_frame_reader_before_releasing_its_file"),
    ("历史版本下载", "tests/test_video_recovery.py::test_historical_download_ranges_do_not_change_active_or_accepted_version"),
    ("工具超时", "tests/test_upload_queue.py::test_probe_timeout_is_retriable_and_is_not_an_incomplete_video"),
    ("缺图错任务错版本", "tests/test_photo_archive_gate.py::test_unverified_photo_never_archives_or_cleans_any_result"),
    ("两类核验后归档", "tests/test_photo_archive_gate.py::test_final_archive_waits_until_both_imports_are_verified"),
    ("归档中核验变化", "tests/test_photo_archive_gate.py::test_old_move_plan_is_rechecked_if_photo_status_changes"),
    ("图片身份不能靠同名", "tests/test_package_import_reuse.py::test_reused_result_does_not_accept_same_names_with_different_cloud_ids"),
    ("重复清单键", "tests/test_upload_queue.py::test_upload_receipt_manifest_does_not_ignore_duplicate_keys"),
    ("缺备份保留", "tests/test_workspace.py::test_missing_cloud_or_image_proof_never_deletes"),
    ("精确备份测试副本清理", "tests/test_workspace.py::test_verified_cleanup_exact_manifest_and_tombstones"),
    ("归档读取占用", "tests/test_workspace.py::test_lease_delays_move_and_cleanup"),
    ("发布中断恢复", "tests/test_workspace.py::test_restart_after_publication_replays_bindings_not_bytes"),
    ("删除中断恢复", "tests/test_workspace.py::test_cleanup_restarts_after_unlink_before_acknowledgement"),
    ("移动冲突保留", "tests/test_workspace.py::test_conflict_after_preview_preserves_both_files"),
    ("跨盘身份保留", "tests/test_workspace.py::test_cross_disk_move_retains_upload_identity"),
    ("断盘保留下载意图", "tests/test_workspace.py::test_disk_disconnect_before_download_preserves_ready_version_and_intent"),
    ("过期媒体刷新", "tests/test_real_video_capture.py::test_expired_media_link_refreshes_without_losing_platform_session"),
    ("磁盘不足禁止发布", "tests/test_real_video_capture.py::test_download_disk_full_stops_before_writing_or_publishing"),
]


def _row(node, case, kind, expected, actual, passed, seconds, detail=None):
    return dict(node=node, case=case, kind=kind, expected=expected, actual=actual,
                passed=bool(passed), seconds=round(seconds, 4), detail=detail or {})


async def _worker(request, python, timeout=150):
    """Actual worker with inherited credentials intentionally excluded."""
    env = {key: value for key, value in os.environ.items()
           if key in {"PATH", "HOME", "TMPDIR", "LANG", "LC_ALL", "SYSTEMROOT"}}
    env.update(PYTHONDONTWRITEBYTECODE="1", PYTHONNOUSERSITE="1", OMP_NUM_THREADS="2")
    process = await asyncio.create_subprocess_exec(str(python), "-B", "-m", "app.slicing_worker",
        stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.DEVNULL, cwd=PROJECT, env=env)
    try:
        output, _ = await asyncio.wait_for(process.communicate(
            (json.dumps(request, ensure_ascii=False) + "\n").encode()), timeout)
    except (asyncio.TimeoutError, asyncio.CancelledError):
        process.kill()
        await process.wait()
        raise
    events = []
    for line in output.decode(errors="replace").splitlines():
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(event, dict):
            events.append(event)
    return process.returncode, events


async def run_faults(root: Path, source: Path, settings: dict) -> list[dict]:
    """Run actual-media faults and isolated regression contracts; no live calls."""
    from app.media_validation import MediaValidationError, verify_video_tail
    from app.slicing import source_version
    from app.slicing_worker import probe

    root = Path(root).resolve()
    source = Path(source).resolve(strict=True)
    root.mkdir(parents=True, exist_ok=True)
    # Defense in depth: never accept the protected original as our test source.
    protected = Path.home() / "Desktop" / "视频抽帧"
    if source.is_relative_to(protected.resolve()):
        raise ValueError("fault harness requires a disposable source copy")
    ffmpeg = str(settings.get("frameToolPath") or shutil.which("ffmpeg") or "")
    ffprobe = str(Path(ffmpeg).with_name("ffprobe"))
    python = settings.get("workerPythonPath") or sys.executable
    baseline_version = source_version(source)
    rows: list[dict] = []
    metadata = None

    start = time.monotonic()
    try:
        metadata = await asyncio.to_thread(probe, str(source), ffprobe)
        rows.append(_row("文件绑定与完整性", "真实原片FFprobe", "真实视频验证",
            "视频元数据及文件大小正确", "已读取真实视频元数据",
            metadata["size"] == source.stat().st_size and metadata["duration"] > 0,
            time.monotonic() - start, metadata))
    except Exception as error:
        rows.append(_row("文件绑定与完整性", "真实原片FFprobe", "真实视频验证",
            "视频元数据正确", type(error).__name__, False, time.monotonic() - start))

    if metadata:
        start = time.monotonic()
        try:
            await asyncio.to_thread(verify_video_tail, source, ffmpeg, metadata["duration"])
            rows.append(_row("文件绑定与完整性", "真实末帧解码", "真实视频验证",
                "末段至少一张可解码JPEG", "末帧解码通过", True, time.monotonic() - start))
        except Exception as error:
            rows.append(_row("文件绑定与完整性", "真实末帧解码", "真实视频验证",
                "末段可解码", str(error) if isinstance(error, MediaValidationError) else type(error).__name__,
                False, time.monotonic() - start))

    with tempfile.TemporaryDirectory(prefix="fault-media-", dir=root) as owned:
        owned = Path(owned)
        short = owned / "同源8秒.mp4"
        start = time.monotonic()
        try:
            proc = await asyncio.create_subprocess_exec(ffmpeg, "-nostdin", "-hide_banner", "-loglevel", "error",
                "-i", str(source), "-t", "8", "-map", "0:v:0", "-c", "copy", "-movflags", "+faststart",
                str(short), stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL)
            try:
                returncode = await asyncio.wait_for(proc.wait(), 90)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                proc.kill()
                await proc.wait()
                raise
            if returncode:
                raise RuntimeError("stream_copy_failed")
            short_meta = await asyncio.to_thread(probe, str(short), ffprobe)
            await asyncio.to_thread(verify_video_tail, short, ffmpeg, short_meta["duration"])
            rows.append(_row("文件绑定与完整性", "同源短片正向", "真实视频验证",
                "无重编码片段元数据及尾部通过", "真实短片通过", True, time.monotonic() - start,
                {"bytes": short.stat().st_size, "duration": short_meta["duration"]}))
            broken = owned / "真实截断.mp4"
            shutil.copyfile(short, broken)
            with broken.open("r+b") as handle:
                handle.truncate(max(1, broken.stat().st_size // 2))
            start = time.monotonic()
            try:
                broken_meta = await asyncio.to_thread(probe, str(broken), ffprobe)
                header_readable = True
            except RuntimeError:
                broken_meta = short_meta
                header_readable = False
            try:
                await asyncio.to_thread(verify_video_tail, broken, ffmpeg, broken_meta["duration"])
                actual, rejected = "意外接受截断视频", False
            except MediaValidationError as error:
                actual, rejected = str(error), str(error) == "media_incomplete"
            rows.append(_row("文件绑定与完整性", "真实截断片尾拒绝", "真实视频故障注入",
                "media_incomplete，不发布完整视频", actual, rejected, time.monotonic() - start,
                {"headerReadable": header_readable, "bytesBefore": short.stat().st_size,
                 "bytesAfter": broken.stat().st_size}))
            rows.extend(await _partial_media_cases(owned, broken, short_meta, settings))
        except Exception as error:
            rows.append(_row("文件绑定与完整性", "同源截断准备", "真实视频验证",
                "生成短片及执行截断故障", type(error).__name__, False, time.monotonic() - start))

        empty = owned / "零可用画面.mp4"
        empty.touch()
        start = time.monotonic()
        try:
            await asyncio.to_thread(probe, str(empty), ffprobe)
            actual, rejected = "意外接受空文件", False
        except RuntimeError as error:
            actual, rejected = str(error), str(error) == "video_probe_failed"
        rows.append(_row("文件绑定与完整性", "零可用画面", "真实文件故障注入",
            "video_probe_failed，不能伪称未检出目标", actual, rejected,
            time.monotonic() - start, {"bytes": 0}))

        if metadata:
            request = dict(filePath=str(source), frameToolPath=ffmpeg, settings=dict(settings),
                expectedFileVersion=baseline_version, metadata=metadata, verifiedMedia=True,
                points={"bed": [0], "magnet": [0]})
            absent = dict(settings, bedModelPath=str(owned / "missing-model.pt"),
                          bedGeometryPath=str(owned / "missing-geometry.npz"))
            start = time.monotonic()
            try:
                status, events = await _worker(dict(request, settings=absent), python)
                bed_missing = any(e.get("category") == "bed" and e.get("code") == "bed_model_missing" for e in events)
                good = [e for e in events if e.get("event") == "observation" and e.get("category") == "magnet"
                        and e.get("label") in {"target", "clear"}]
                rows.append(_row("模型加载", "真实车斗模型缺失，磁盘吊继续", "真实工作进程故障注入",
                    "bed_model_missing；真实磁盘吊仍输出判断", "类别错误隔离" if bed_missing and good else "缺少预期事件",
                    status == 0 and bed_missing and bool(good), time.monotonic() - start,
                    {"exitCode": status, "errors": [dict(category=e.get("category"), code=e.get("code"))
                        for e in events if e.get("event") == "error"],
                     "magnetObservations": [{key: e.get(key) for key in ("time", "label", "score")} for e in good]}))
            except Exception as error:
                rows.append(_row("模型加载", "真实车斗模型缺失，磁盘吊继续", "真实工作进程故障注入",
                    "单模型失败隔离", type(error).__name__, False, time.monotonic() - start))

            start = time.monotonic()
            changed = dict(baseline_version, modifiedNs=baseline_version["modifiedNs"] + 1)
            try:
                status, events = await _worker(dict(request, expectedFileVersion=changed), python, 45)
                codes = [e.get("code") for e in events if e.get("event") == "error"]
                loaded = any(e.get("stage") == "loading_model" or e.get("event") == "observation" for e in events)
                rows.append(_row("共享抽帧与识别", "版本改变不得识别", "真实工作进程故障注入",
                    "source_changed；模型和抽帧不启动", ",".join(codes),
                    status == 0 and "source_changed" in codes and not loaded, time.monotonic() - start,
                    {"modelStarted": loaded, "sourceBytesUnchanged": source_version(source) == baseline_version}))
            except Exception as error:
                rows.append(_row("共享抽帧与识别", "版本改变不得识别", "真实工作进程故障注入",
                    "source_changed", type(error).__name__, False, time.monotonic() - start))

        rows.extend(await _contract_cases(root, owned))

    rows.append(_row("原片保护", "隔离源版本保持", "真实文件验证", "隔离源版本与初始完全一致",
        "完全一致" if source_version(source) == baseline_version else "文件版本变化",
        source_version(source) == baseline_version, 0))
    (root / "fault-results.json").write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")
    return rows


async def _partial_media_cases(owned: Path, broken: Path, metadata: dict, settings: dict) -> list[dict]:
    """Exercise real FFmpeg bad positions and the actual zero-image exporter."""
    from app.main import create_app
    from app.slicing import source_version
    from app.slicing_worker import extract_requested_frames
    from tests.test_slicing_performance import close_app

    rows = []
    ffmpeg = str(settings["frameToolPath"])
    bad_stamp = round(float(metadata["duration"]) * 0.85, 3)
    start = time.monotonic()
    try:
        frames = await asyncio.to_thread(lambda: list(extract_requested_frames(str(broken), ffmpeg,
            metadata, {"bed": [0, bad_stamp]}, skip_undecodable=True)))
        positions = [stamp for stamp, _ in frames]
        available = [stamp for stamp, blob in frames if blob is not None]
        unavailable = [stamp for stamp, blob in frames if blob is None]
        rows.append(_row("共享抽帧与识别", "真实截断位置独立跳过", "真实视频故障注入",
            "0秒解码成功；截断后位置不可用，时间不偏移", "已保留真实位置并跳过坏处",
            positions == [0, bad_stamp] and available == [0] and unavailable == [bad_stamp],
            time.monotonic() - start, {"positions": positions, "available": available, "unavailable": unavailable}))
        del frames
    except Exception as error:
        rows.append(_row("共享抽帧与识别", "真实截断位置独立跳过", "真实视频故障注入",
            "独立跳过不可解码位置", type(error).__name__, False, time.monotonic() - start))

    app = create_app(data_dir=owned / "zero-export-db")
    manager = app.state.slicing
    manager.store.settings.update(frameToolPath=ffmpeg, enabled=False, reviewBeforeUpload=False)
    item = dict(id="fault-zero-export", sourceDatasetId="27912", sourceVideoId="",
        sourceIdentity="fault-local-zero", name=broken.name, path=str(broken),
        fileVersion=source_version(broken), partialRecovery=True, localWorkspace=True,
        state="platform_processing", revision=1,
        categories={"bed": {"state": "ready", "intervals": [{"start": bad_stamp, "end": bad_stamp}],
                            "intervalSeconds": 1}, "magnet": {"state": "not_applicable"}})
    manager.store.save("items", item)
    start = time.monotonic()
    try:
        await manager._export_category(manager.store.get("items", item["id"]), "bed")
        category = manager.store.get("items", item["id"])["categories"]["bed"]
        packages = manager.packages.store.list("items")
        outputs = list((owned / "zero-export-db").rglob("*.zip"))
        rows.append(_row("区间、照片与ZIP", "真实零张导出不上传", "真实视频＋业务导出验证",
            "no_cuttable_fragments；零照片不打包、不入上传队列", category.get("errorCode", category.get("state")),
            category.get("errorCode") == "no_cuttable_fragments" and not packages and not outputs,
            time.monotonic() - start, {"state": category.get("state"), "packages": len(packages),
                "zipFiles": len(outputs), "skippedExportFrames": category.get("skippedExportFrames"),
                "sourcePreserved": broken.is_file()}))
    except Exception as error:
        rows.append(_row("区间、照片与ZIP", "真实零张导出不上传", "真实视频＋业务导出验证",
            "无照片不创建上传", type(error).__name__, False, time.monotonic() - start))
    finally:
        await close_app(app)
    return rows


async def run_fault_supplement(root: Path, source: Path, settings: dict) -> list[dict]:
    """Incremental evidence for an existing run; never repeats earlier tests."""
    from app.slicing_worker import probe
    root = Path(root).resolve()
    root.mkdir(parents=True, exist_ok=True)
    source = Path(source).resolve(strict=True)
    if source.is_relative_to((Path.home() / "Desktop" / "视频抽帧").resolve()):
        raise ValueError("fault harness requires a disposable source copy")
    ffmpeg = str(settings["frameToolPath"])
    with tempfile.TemporaryDirectory(prefix="fault-supplement-", dir=root) as folder:
        owned = Path(folder)
        short = owned / "same-source-eight-seconds.mp4"
        process = await asyncio.create_subprocess_exec(ffmpeg, "-nostdin", "-hide_banner", "-loglevel", "error",
            "-i", str(source), "-t", "8", "-map", "0:v:0", "-c", "copy", "-movflags", "+faststart", str(short),
            stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL)
        try:
            status = await asyncio.wait_for(process.wait(), 90)
        except (asyncio.TimeoutError, asyncio.CancelledError):
            process.kill()
            await process.wait()
            raise
        if status:
            raise RuntimeError("stream_copy_failed")
        metadata = await asyncio.to_thread(probe, str(short), str(Path(ffmpeg).with_name("ffprobe")))
        with short.open("r+b") as stream:
            stream.truncate(short.stat().st_size // 2)
        rows = await _partial_media_cases(owned, short, metadata, settings)
        rows.extend(await _contract_cases(root, owned, cases=CONTRACTS[-2:], prefix="fault-supplement"))
    return rows


async def _contract_cases(root: Path, owned: Path, *, cases=None, prefix="fault-contracts") -> list[dict]:
    cases = CONTRACTS if cases is None else cases
    python = PROJECT / ".venv" / "bin" / "python"
    if not python.is_file():
        python = Path(sys.executable)
    xml = root / f"{prefix}.xml"
    log = root / f"{prefix}.log"
    started = time.monotonic()
    with log.open("wb") as output:
        process = await asyncio.create_subprocess_exec(str(python), "-m", "pytest", "-q",
            *(nodeid for _, nodeid in cases), f"--junitxml={xml}", f"--basetemp={owned / 'pytest'}",
            cwd=PROJECT, stdout=output, stderr=asyncio.subprocess.STDOUT,
            env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"})
        try:
            status = await asyncio.wait_for(process.wait(), 300)
        except (asyncio.TimeoutError, asyncio.CancelledError):
            process.kill()
            await process.wait()
            raise
    if not xml.is_file():
        return [_row("隔离契约集", "pytest执行", "受控故障注入／契约测试", "生成逐项JUnit证据",
                     "未生成JUnit", False, time.monotonic() - started, {"exitCode": status})]
    rows = []
    for case in ET.parse(xml).getroot().iter("testcase"):
        name = case.attrib.get("name", "unknown")
        base = name.split("[", 1)[0]
        mapping = next(((node, nodeid) for node, nodeid in cases if nodeid.endswith("::" + base)), None)
        node, nodeid = mapping or ("隔离契约集", case.attrib.get("classname", ""))
        failed = case.find("failure") is not None or case.find("error") is not None
        skipped = case.find("skipped") is not None
        rows.append(_row(node, name, "受控故障注入／契约测试",
            "隔离管理器按断言保留、拒绝、等待或恢复；不调用真实平台",
            "跳过，未验证" if skipped else "断言失败" if failed else "断言通过", not failed and not skipped,
            float(case.attrib.get("time", 0)), {"nodeid": nodeid, "evidence": xml.name}))
    observed = {r["detail"]["nodeid"] for r in rows}
    for node, nodeid in cases:
        if nodeid not in observed:
            rows.append(_row(node, nodeid.split("::")[-1], "受控故障注入／契约测试",
                "实际执行此案例", "未运行", False, 0, {"nodeid": nodeid, "exitCode": status}))
    return rows
