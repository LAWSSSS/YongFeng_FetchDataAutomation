#!/usr/bin/env python3
"""Opt-in, isolated real-video rehearsal. Never starts the production service.

Usage: .venv/bin/python -B scripts/run_real_video_e2e.py --source VIDEO
       --settings JSON --root data/test-runs/RUN
Settings must be an explicit, non-secret model/tool configuration. No saved
platform credentials or production task database are loaded by this runner.
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import resource
import shutil
import subprocess
import sys
import time
import traceback
from collections import Counter
from pathlib import Path

PROJECT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT))

import httpx

ALLOW_SETTINGS = {
    'workerPythonPath', 'frameToolPath', 'bedDeployDirectory', 'bedModelPath',
    'bedGeometryPath', 'magnetDeployDirectory', 'magnetModelPath',
    'sampleIntervalSeconds', 'magnetIntervalSeconds', 'bedIntervalSeconds',
    'sharedSampling', 'targetImages', 'bedTargetImages', 'magnetConfidence',
}


def write(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + '.tmp')
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2, default=str))
    temporary.replace(path)


def sha256(path):
    digest = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda: stream.read(4 * 1024 * 1024), b''):
            digest.update(block)
    return digest.hexdigest()


def stamp(path):
    stat = path.stat()
    return dict(size=stat.st_size, mtimeNs=stat.st_mtime_ns, inode=stat.st_ino, device=stat.st_dev)


def prepare(root, source, settings):
    if root.exists() and (root / 'run.json').exists():
        prior = json.loads((root / 'run.json').read_text())
        allowed = {key: value for key, value in settings.items() if key in ALLOW_SETTINGS}
        if (str(source) != prior['source'] or stamp(source) != prior['sourceStat']
                or sha256(source) != prior['sourceSha256'] or allowed != prior['settings']):
            raise ValueError('Source or configuration differs from the saved rehearsal; use a new run directory.')
        return prior
    root.mkdir(parents=True, exist_ok=False)
    before = stamp(source)
    started = time.monotonic()
    digest = sha256(source)
    destination = root / 'source.mp4'
    copied = subprocess.run(['cp', '-c', '-p', str(source), str(destination)],
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode == 0
    if not copied:
        shutil.copy2(source, destination)
    assert sha256(destination) == digest and stamp(source) == before
    result = dict(root=str(root), source=str(source), sourceStat=before,
                  sourceSha256=digest, copySeconds=time.monotonic() - started,
                  settings={key: value for key, value in settings.items() if key in ALLOW_SETTINGS})
    write(root / 'run.json', result)
    return result


async def close_app(app):
    await app.state.pipeline.shutdown()
    await app.state.workspace.shutdown()
    await app.state.package_gallery.shutdown()
    await app.state.original_restore.shutdown()
    from tests.test_slicing_performance import close_app as close
    await close(app)


async def locked_app(database):
    """Every platform client is denied except explicitly supplied local adapters."""
    from app.main import create_app
    from app.remote import RemoteClient
    async def deny(request):
        raise RuntimeError('Rehearsal rejected an unbound remote request: ' + request.url.path)
    source = RemoteClient('source', transport=httpx.MockTransport(deny))
    target = RemoteClient('target', transport=httpx.MockTransport(deny))
    app = create_app(source, target, database)
    client = app.state.upload_audit.client
    await client.client.aclose()
    client.client = httpx.AsyncClient(transport=httpx.MockTransport(deny))
    client.persist_connection = False
    client.access = 'isolated-rehearsal-only'
    return app


def snapshot(app):
    manager = app.state.slicing
    return {
        'items': [{key: item.get(key) for key in ('id', 'name', 'state', 'analysisStage',
            'analysisDone', 'analysisTotal', 'analysisPosition', 'errorCode', 'message', 'categories')}
                  for item in manager.store.list('items')],
        'packages': [{key: item.get(key) for key in ('id', 'category', 'state', 'errorCode',
            'imageCount', 'actualImages', 'bytes', 'size', 'jobId', 'retries', 'path')}
                     for item in manager.packages.store.list('items')],
        'assets': [{key: asset.get(key) for key in ('id', 'name', 'stage', 'status', 'message', 'path', 'cleanup')}
                   for asset in app.state.workspace.rows('assets')],
        'run': manager.run,
    }


def instrument_calls(root, owner, names, scope):
    """Observe real node calls without changing their arguments or outcomes."""
    for name in names:
        original = getattr(owner, name)
        async def measured(*args, _method=original, _name=name, **kwargs):
            started = time.monotonic()
            identity = args[0].get('id') if args and isinstance(args[0], dict) else None
            record = {'scope': scope, 'method': _name, 'id': identity, 'event': 'start'}
            with (root / 'node-timings.jsonl').open('a') as stream:
                stream.write(json.dumps({**record, 'monotonic': started}) + '\n')
            try:
                return await _method(*args, **kwargs)
            except Exception as error:
                record['errorType'] = type(error).__name__
                raise
            finally:
                record.update(event='finish', seconds=round(time.monotonic() - started, 6))
                with (root / 'node-timings.jsonl').open('a') as stream:
                    stream.write(json.dumps(record) + '\n')
        setattr(owner, name, measured)


async def run_models(root, settings, *, timeout=1800, resume_category=None, photo_faults=False):
    """Run normal continuous intake; do not seed observations or positive labels."""
    from tests.real_video_platform import PhotoPlatform
    capture = json.loads((root / 'capture-result.json').read_text())
    app = await locked_app(root / 'capture' / 'database')
    platform = PhotoPlatform(root / 'natural-photo-platform').start()
    started = time.monotonic()
    stages, transitions = [], []
    timed_out = True
    query_counts = Counter()
    app.state.store.db.set_trace_callback(lambda sql: query_counts.update([sql.split(' ', 1)[0].upper()]))
    manager, workspace = app.state.slicing, app.state.workspace
    manager.packages.remote = platform
    if photo_faults:
        platform.faults.update(drop_part_once=True, drop_complete_once=True, lose_submit_once=True)
    instrument_calls(root, manager, ('_run_worker', '_export_category', '_recognized_magnet_frames'), 'slicing')
    instrument_calls(root, workspace, ('process_move',), 'workspace')
    instrument_calls(root, manager.packages, ('upload_one', 'verify'), 'photos')
    manager.store.settings.update(settings, localExport=True, reviewBeforeUpload=False,
                                  maxLocalSlicingProcesses=1,
                                  targetDatasetIds={'27912': '27892', '27944': '27893'},
                                  directories=[str(root / 'capture' / 'downloads')],
                                  sliceOutputDirectory=str(root / 'outputs'))
    manager.store.save_settings()
    if workspace.enabled:
        workspace.derive_config()
    try:
        if not workspace.enabled:
            (root / 'workspace').mkdir(exist_ok=True)
            plan = await workspace.preview(str(root / 'workspace'), workspace.config['revision'], 'real-preview')
            await workspace.apply(plan['id'], workspace.config['revision'], 'real-apply', False)
            await workspace.task
        assert workspace.enabled, workspace.state()
        if resume_category:
            # Diagnostic checkpoint continuation, deliberately NOT presented
            # as a working public retry button or an automatic baseline pass.
            # Run the unchanged real node before starting competing schedulers.
            app.state.media_control.refresh()
            for item in manager.store.list('items'):
                if item.get('categories', {}).get(resume_category, {}).get('state') != 'pending':
                    continue
                began = time.monotonic()
                await manager.process_with_recovery(item)
                write(root / 'diagnostic-checkpoint.json', {'category': resume_category,
                    'seconds': time.monotonic() - began, 'itemId': item['id'],
                    'mode': 'backend-node-diagnostic-not-public-UI', 'state': snapshot(app)})
        await workspace.startup()
        await manager.startup()
        await app.state.pipeline.startup()
        if not app.state.pipeline.active():
            app.state.pipeline.change('start', {'expectedRevision': app.state.pipeline.config['revision'],
                                                'requestId': 'real-continuous-start'})
        # Ordinary managers retain their real 30 s platform checks and model work.
        last_signature, terminal_since, blocked_since = '', None, None
        while time.monotonic() - started < timeout:
            current = snapshot(app)
            signature = json.dumps(current, sort_keys=True, default=str)
            if signature != last_signature:
                transitions.append({'seconds': round(time.monotonic() - started, 3), **current})
                with (root / 'natural-progress.jsonl').open('a') as journal:
                    journal.write(json.dumps(transitions[-1], ensure_ascii=False, default=str) + '\n')
                write(root / 'natural-latest.json', transitions[-1])
                last_signature = signature
            items, packages = current['items'], current['packages']
            terminal = items and all(item['state'] in {'complete', 'no_target', 'attention'} for item in items)
            terminal = terminal and all(p['state'] in {'complete', 'attention', 'review', 'superseded'} for p in packages)
            busy = bool(manager._active_analysis_tasks() or manager.exporting or manager.packages.running
                        or any(not task.done() for task in manager.packages.target_tasks.values()))
            stranded = [item for item in current['items'] if item.get('state') == 'platform_processing'
                        and any(category.get('state') == 'pending'
                                and category.get('retryNotBefore', 0) <= manager.clock()
                                for category in item.get('categories', {}).values())]
            if stranded and not busy and packages and all(p['state'] == 'complete' for p in packages):
                blocked_since = blocked_since or time.monotonic()
                if time.monotonic() - blocked_since >= 3:
                    write(root / 'natural-blocked.json', {'items': current['items'],
                        'packages': current['packages'], 'wallSeconds': time.monotonic() - started,
                        'reason': 'Due pending category is blocked by the root state.'})
                    break
            else:
                blocked_since = None
            if terminal and not busy:
                terminal_since = terminal_since or time.monotonic()
                if time.monotonic() - terminal_since >= 3:
                    timed_out = False
                    break
            else:
                terminal_since = None
            await asyncio.sleep(.5)
        await workspace.reconcile(cleanup=True)
        current = snapshot(app)
        stages.append(dict(node='持续登记与最终状态', case='baseline', kind='real_manager',
            expected='纳入真实已确认版本并到达明确终态，不能用超时冒充完成',
            actual={'items': len(current['items']), 'timedOut': timed_out},
            passed=bool(current['items']) and not timed_out,
            seconds=round(time.monotonic() - started, 3)))
        observations = manager.store.list('observations')
        labels = {name: dict(Counter(o.get('label') for o in observations if o.get('category') == name))
                  for name in ('bed', 'magnet')}
        for item in current['items']:
            for name, category in (item.get('categories') or {}).items():
                stages.append(dict(node='真实识别与照片链', case=name, kind='real_model_video',
                    expected='真实模型结果不改写，目标照片需导入核验；无目标保留no_target',
                    actual=category, passed=category.get('state') in {'complete', 'no_target'},
                    seconds=round(time.monotonic() - started, 3),
                    detail={'itemId': item['id'], 'timeBasis': '累计墙钟时间，非该类别独占耗时'}))
        packages = manager.packages.store.list('items')
        for package in packages:
            stages.append(dict(node='真实ZIP/分片/照片导入', case=package['category'],
                kind='real_export_protocol_simulation', expected='实际ZIP经Boto3上传后解析并严格核验照片',
                actual={key: package.get(key) for key in ('state','imageCount','actualImages','jobId','verifiedJobId')},
                passed=package.get('state') == 'complete' and package.get('imageCount') == package.get('actualImages'),
                seconds=round(time.monotonic() - started, 3)))
        if photo_faults:
            for fault in ('drop_part_once', 'drop_complete_once', 'lose_submit_once'):
                stages.append(dict(node='在途故障自动恢复', case=fault, kind='real_wire_injected_failure',
                    expected='故障仅触发一次，自动恢复且不重复创建两个目标的导入任务',
                    actual={'injections': platform.metrics.get('fault:' + fault, 0),
                            'importsByTarget': {target: platform.metrics.get('imports:' + target, 0)
                                                for target in ('27892', '27893')}},
                    passed=platform.metrics.get('fault:' + fault, 0) == 1
                        and all(p.get('state') == 'complete' for p in packages)
                        and all(platform.metrics.get('imports:' + target, 0) <= 1 for target in ('27892', '27893')),
                    seconds=round(time.monotonic() - started, 3)))
        assets = workspace.rows('assets')
        stages.append(dict(node='归档与原片保留', case='没有双视频备份凭证', kind='real_files_protocol_simulation',
            expected='照片核验后整理，但不得删除没有精确原片云端备份的工作副本',
            actual={'paths': [a['path'] for a in assets],
                    'allOriginalsRetained': bool(assets) and all(Path(a['path']).exists() for a in assets),
                    'stages': [workspace.stage(a) for a in assets]},
            passed=bool(assets) and all(Path(a['path']).exists() for a in assets), seconds=0))
        result = dict(stages=stages, status=current, labels=labels, timedOut=timed_out,
                      packages=packages, transitions=len(transitions),
                      metrics={'wallSeconds': round(time.monotonic() - started, 3),
                               'processLifetimePeakRss': resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
                               'largestTerminatedChildPeakRss': resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss,
                               'rssUnit': 'bytes on macOS; KiB on Linux',
                               'sqlStatements': dict(query_counts), 'platform': dict(platform.metrics)})
        write(root / 'natural-result.json', result)
        write(root / 'natural-progress.json', transitions)
        return result
    finally:
        app.state.store.db.set_trace_callback(None)
        await close_app(app)
        platform.close()


async def transport_contract(root, settings, natural):
    """Use a natural ZIP if available; otherwise export actual frames explicitly.

    This contract never claims that an absent model target was detected.
    """
    import zipfile
    from tests.real_video_platform import PhotoPlatform, exercise_transfer_faults
    archives = [Path(p['path']) for p in natural.get('packages', []) if Path(p.get('path', '')).is_file()]
    contract = root / 'transport-contract'
    contract.mkdir(exist_ok=True)
    if archives:
        archive = archives[0]
    else:
        archive = contract / 'actual-frames.zip'
        source = root / 'source.mp4'
        with zipfile.ZipFile(archive, 'w', zipfile.ZIP_STORED) as output:
            for index, second in enumerate((0, 300, 600, 1200, 1800)):
                process = await asyncio.create_subprocess_exec(settings['frameToolPath'], '-nostdin', '-v', 'error',
                    '-ss', str(second), '-i', str(source), '-frames:v', '1', '-f', 'image2pipe', '-vcodec', 'mjpeg', '-',
                    stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL)
                try:
                    data, _ = await asyncio.wait_for(process.communicate(), 30)
                finally:
                    if process.returncode is None:
                        process.kill()
                        await process.wait()
                assert process.returncode == 0 and data[:2] == b'\xff\xd8'
                output.writestr(f'real_truck_{index:04d}.jpg', data)
    with PhotoPlatform(contract / 'platform') as platform:
        rows = await exercise_transfer_faults(platform, archive, contract / 'faults')
        write(root / 'transport-result.json', {'stages': rows, 'metrics': dict(platform.metrics),
              'source': 'natural-model-ZIP' if archives else 'actual-frames-contract-only'})
    return rows


async def main(args):
    root, source = args.root.resolve(), args.source.resolve(strict=True)
    config = prepare(root, source, json.loads(args.settings.read_text()))
    settings = config['settings']
    try:
        if not (root / 'capture-result.json').exists():
            from tests.real_video_capture import run_capture
            capture = await run_capture(root / 'capture', root / 'source.mp4', settings)
            write(root / 'capture-result.json', {k: v for k, v in capture.items() if k not in {'app','platform'}})
            await close_app(capture['app'])
        saved_natural = json.loads((root / 'natural-result.json').read_text()) if (root / 'natural-result.json').exists() else None
        if not saved_natural or saved_natural.get('timedOut') or args.resume_category:
            print('真实模型与照片流水线启动', flush=True)
            natural = await run_models(root, settings, timeout=args.timeout, resume_category=args.resume_category,
                                       photo_faults=args.photo_faults)
        else:
            natural = saved_natural
        if not (root / 'transport-result.json').exists():
            print('分片上传与照片核验故障测试启动', flush=True)
            await transport_contract(root, settings, natural)
        if not (root / 'tcp-result.json').exists():
            from tests.real_video_http_download import run_tcp
            print('真实loopback TCP断流续传测试启动', flush=True)
            await run_tcp(root, source, settings)
        if not (root / 'fault-result.json').exists():
            from tests.real_video_faults import run_faults
            print('逐节点隔离故障测试启动', flush=True)
            rows = await run_faults(root / 'faults', root / 'source.mp4', settings)
            write(root / 'fault-result.json', {'stages': rows})
    finally:
        after = {'sourceStat': stamp(source), 'sourceSha256': await asyncio.to_thread(sha256, source)}
        after['unchanged'] = after['sourceStat'] == config['sourceStat'] and after['sourceSha256'] == config['sourceSha256']
        write(root / 'source-after.json', after)
        if not after['unchanged']:
            raise RuntimeError('Protected original identity changed during rehearsal.')
    print('测试检查点已保存：' + str(root), flush=True)
    from tests.real_video_report import register_outputs, cleanup_media, build_report
    register_outputs(root)
    if args.clean_media:
        cleanup_media(root)
    summary = build_report(root)
    print(json.dumps(summary, ensure_ascii=False), flush=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', type=Path, required=True)
    parser.add_argument('--settings', type=Path, required=True)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--timeout', type=int, default=1800)
    parser.add_argument('--resume-category', choices=['bed', 'magnet'], help='Explicit checkpoint-recovery scenario; preserves natural failure evidence.')
    parser.add_argument('--clean-media', action='store_true', help='Delete only registered test media after evidence is saved.')
    parser.add_argument('--photo-faults', action='store_true', help='Inject one part disconnect, completion loss, and import-response loss into the real baseline.')
    arguments = parser.parse_args()
    try:
        asyncio.run(main(arguments))
    except Exception:
        arguments.root.mkdir(parents=True, exist_ok=True)
        (arguments.root / 'runner-failure.txt').write_text(traceback.format_exc())
        raise
