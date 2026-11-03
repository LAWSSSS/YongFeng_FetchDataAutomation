#!/usr/bin/env python3
"""Opt-in real-model check: isolated work area; all platform writes are denied.

The source is read only. A stream-copy excerpt is the only video that is enrolled.
Photos remain queued in a persistent upload pause, never falsely completed.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import subprocess
import sys
import time
import zipfile
from pathlib import Path

PROJECT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT))
from scripts.run_real_video_e2e import ALLOW_SETTINGS, close_app, locked_app, sha256, stamp, write
from app.slicing import source_version


async def run(args):
    source, root = args.source.resolve(strict=True), args.root.resolve()
    if root.exists() and not args.resume:
        raise ValueError('Use a new isolated test directory, or explicitly resume the same one.')
    root.mkdir(parents=True, exist_ok=args.resume)
    before = stamp(source)
    digest = await asyncio.to_thread(sha256, source)
    settings = json.loads(args.settings.read_text())
    settings = {k: v for k, v in settings.get('settings', settings).items() if k in ALLOW_SETTINGS}
    parent = root / 'input'
    parent.mkdir(exist_ok=args.resume)
    excerpt = parent / '鲁NH1113_本地流水线验证.mp4'
    manifest = dict(source=str(source), sourceStat=before, sourceSha256=digest,
                    settings=settings, start=args.start, duration=args.duration)
    prior = root / 'run.json'
    if args.resume:
        if not prior.is_file() or json.loads(prior.read_text()) != manifest:
            raise ValueError('Resume source or model settings changed.')
    else:
        write(prior, manifest)
    if not args.resume:
        result = subprocess.run([settings['frameToolPath'], '-v', 'error', '-nostdin',
            '-ss', str(args.start), '-i', str(source), '-t', str(args.duration),
            '-map', '0:v:0', '-c', 'copy', '-movflags', '+faststart', str(excerpt)], capture_output=True)
        if result.returncode:
            raise RuntimeError('Could not create the isolated stream-copy excerpt.')
    app = await locked_app(root / 'db')
    slicing, workspace, pipeline = app.state.slicing, app.state.workspace, app.state.pipeline
    slicing.store.settings.update(settings, localExport=True, reviewBeforeUpload=False,
        maxLocalSlicingProcesses=1, targetDatasetIds={'27912': '27892', '27944': '27893'},
        directories=[str(parent)], sliceOutputDirectory=str(root / 'outputs'))
    slicing.store.save_settings()
    slicing.packages.pause()
    if not args.resume:
        version = source_version(excerpt)
        app.state.videos.store.save({'id': 'isolated-local-pipeline', 'jobId': 'isolated-no-submit',
            'name': excerpt.stem, 'plate': '鲁NH1113', 'revision': 0, 'activeRevision': 0,
            'acceptedRevision': 0, 'requests': {}, 'versions': [{'revision': 0, 'state': 'saved',
            'filePath': str(excerpt), 'fileVersion': version, 'sha256': sha256(excerpt),
            'totalBytes': version['size'], 'durationSeconds': args.duration}]})
    started = time.monotonic()
    assertions = []
    def check(name, passed, **detail):
        assertions.append({'name': name, 'passed': bool(passed), **detail})
        if not passed:
            write(root / 'result.json', {'passed': False, 'assertions': assertions})
            raise AssertionError(name)
    (root / 'workspace-parent').mkdir(exist_ok=args.resume)
    try:
        if not workspace.enabled:
            preview = await workspace.preview(str(root / 'workspace-parent'), 0, 'isolated-preview')
            await workspace.apply(preview['id'], 0, 'isolated-apply', False)
            await workspace.task
        await workspace.startup()
        await slicing.startup()
        await pipeline.startup()
        if args.resume:
            if (root / 'result.json').exists():
                (root / 'result.json').replace(root / 'result-before-retry.json')
            app.state.media_control.refresh()
            for item in slicing.store.list('items'):
                if item.get('state') == 'attention':
                    slicing.retry(item['id'], item['revision'], 'local-only-fixed-export-retry')
        pipeline.change('start', {'requestId': 'isolated-local-only-start' + ('-resume' if args.resume else ''),
                                  'expectedRevision': pipeline.config['revision']})
        check('continuous-start-preserves-manual-upload-pause', slicing.packages.paused)
        terminal = False
        while time.monotonic() - started < args.timeout:
            items = slicing.store.list('items')
            packages = slicing.packages.store.list('items')
            states = [category.get('state') for item in items for category in item.get('categories', {}).values()]
            write(root / 'progress.json', {'seconds': round(time.monotonic() - started, 2),
                'items': [{k: i.get(k) for k in ('id','state','analysisStage','analysisDone','analysisTotal','categories')} for i in items],
                'packages': [{k: p.get(k) for k in ('id','category','state','imageCount','bytes')} for p in packages],
                'paused': slicing.packages.paused})
            if items and states and all(value in {'queued','no_target'} for value in states):
                terminal = True
                break
            if any(value == 'attention' for value in states):
                break
            await asyncio.sleep(.25)
        check('real-models-finish-local-work', terminal, seconds=round(time.monotonic() - started, 2))
        check('both-categories-were-processed', {name for item in items for name in item['categories']} == {'bed', 'magnet'})
        check('photos-wait-without-claiming-complete', len(packages) == states.count('queued')
              and all(p['state'] == 'queued' for p in packages))
        check('no-upload-bytes-or-object-or-import-job', all(not p.get('bytes') and not p.get('object') and not p.get('jobId') for p in packages))
        check('original-video-upload-still-disabled', not app.state.uploads.store.list('items'))
        for package in packages:
            with zipfile.ZipFile(package['path']) as archive:
                jpg = [name for name in archive.namelist() if name.endswith('.jpg')]
                check('real-jpeg-count-' + package['category'], len(jpg) == package['imageCount'] and len(jpg) > 0,
                      imageCount=len(jpg), size=Path(package['path']).stat().st_size)
        item_ids = [i['id'] for i in items]
        package_ids = [p['id'] for p in packages]
        observations = len(slicing.store.list('observations'))
        pipeline.change('start', {'requestId': 'isolated-duplicate-start', 'expectedRevision': pipeline.config['revision']})
        await pipeline.tick()
        await workspace.reconcile(cleanup=True)
        check('repeat-start-does-not-duplicate-work', item_ids == [i['id'] for i in slicing.store.list('items')]
              and package_ids == [p['id'] for p in slicing.packages.store.list('items')]
              and observations == len(slicing.store.list('observations')))
        check('unimported-photos-retain-original-and-packages',
              all(Path(i['path']).is_file() for i in slicing.store.list('items'))
              and all(Path(p['path']).is_file() for p in slicing.packages.store.list('items'))
              and all(not workspace.photo_results_verified(a) for a in workspace.rows('assets')))
    finally:
        await close_app(app)
    reopened = await locked_app(root / 'db')
    try:
        check('upload-pause-survives-restart', reopened.state.slicing.packages.paused)
        await reopened.state.slicing.packages.start()
        await reopened.state.slicing.packages.tick()
        check('restart-does-not-upload-queued-packages', all(p['state'] == 'queued' and not p.get('bytes')
              for p in reopened.state.slicing.packages.store.list('items')))
        check('restart-reuses-observations', observations == len(reopened.state.slicing.store.list('observations')))
    finally:
        await close_app(reopened)
    check('original-unchanged', stamp(source) == before and await asyncio.to_thread(sha256, source) == digest)
    write(root / 'result.json', {'passed': True, 'assertions': assertions, 'source': str(source),
        'sourceSha256': digest, 'sourceStat': before, 'seconds': round(time.monotonic() - started, 2),
        'observations': observations, 'packages': [{'category': p['category'], 'imageCount': p['imageCount'],
            'state': p['state'], 'path': p['path']} for p in packages],
        'boundary': 'All actual platform clients denied; no upload or import; production service untouched.'})
    print(json.dumps({'passed': True, 'assertions': len(assertions), 'root': str(root)}, ensure_ascii=False))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--source', required=True, type=Path)
    parser.add_argument('--settings', required=True, type=Path)
    parser.add_argument('--root', required=True, type=Path)
    parser.add_argument('--start', type=float, default=600)
    parser.add_argument('--duration', type=float, default=600)
    parser.add_argument('--timeout', type=float, default=600)
    parser.add_argument('--resume', action='store_true')
    asyncio.run(run(parser.parse_args()))
