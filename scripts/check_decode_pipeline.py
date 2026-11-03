#!/usr/bin/env python3
"""Real models/media, isolated databases and loopback-only photo import.

Read-only source; create a short stream-copy excerpt and a truncated copy.
No saved sessions are read and no production platform writes are possible.
"""
import argparse
import asyncio
from collections import Counter
import json
from pathlib import Path
import shutil
import subprocess
import sys
import time
import zipfile

PROJECT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT))
from scripts.run_real_video_e2e import ALLOW_SETTINGS, close_app, locked_app, sha256, stamp, write
from app.slicing import source_version, ffprobe_file
from app.photo_completion import photo_results_verified
from tests.real_video_platform import PhotoPlatform


async def scenario(root, source, settings, partial):
    root.mkdir()
    path = root / ('鲁NH1113_损坏副本.mp4' if partial else '鲁NH1113_正常副本.mp4')
    shutil.copyfile(source, path)
    if partial:
        with path.open('r+b') as stream:
            stream.truncate(path.stat().st_size * 3 // 5)
    app = await locked_app(root / 'database')
    platform = PhotoPlatform(root / 'photo-platform').start()
    manager = app.state.slicing
    manager._wake_scheduler = lambda: None  # Node execution uses the same real public manager methods below.
    manager.packages.remote = platform
    manager.store.settings.update(settings, localExport=True, reviewBeforeUpload=False,
        directories=[str(root)], sliceOutputDirectory=str(root / 'photos'),
        targetDatasetIds={'27912': '27892', '27944': '27893'})
    manager.store.save_settings()
    video_id = 'isolated-damaged' if partial else 'isolated-healthy'
    version = source_version(path)
    metadata = await asyncio.to_thread(ffprobe_file, path, settings['frameToolPath'])
    app.state.videos.store.save({'id': video_id, 'jobId': 'no-real-platform', 'name': path.stem,
        'revision': 0, 'activeRevision': 0, 'acceptedRevision': 0, 'requests': {},
        'versions': [{'revision': 0, 'state': 'saved', 'filePath': str(path), 'fileVersion': version,
                     'sha256': await asyncio.to_thread(sha256, path), 'totalBytes': version['size']}]})
    item = manager.queue_collection_recovery(video_id=video_id, revision=0, path=path,
        file_version=version, metadata=metadata, name=path.name, request_id='isolated-' + video_id)
    item.update(partialRecovery=partial, finalProcessingIntent='isolated-user-final-confirmation')
    manager.store.save('items', item)
    began = time.monotonic()
    async def progress():
        while True:
            current = manager.store.get('items', item['id']) or {}
            write(root / 'progress.json', {'seconds': round(time.monotonic() - began, 2),
                **{key: current.get(key) for key in ('state', 'analysisStage', 'analysisPosition', 'analysisDone', 'analysisTotal')},
                'categories': {name: {key: value.get(key) for key in ('state', 'errorCode', 'observations', 'skippedSamples', 'imageCount', 'actualImages')}
                    for name, value in current.get('categories', {}).items()}})
            await asyncio.sleep(1)
    reporter = asyncio.create_task(progress())
    try:
        await asyncio.wait_for(manager._process_item(item), 600)
        current = manager.store.get('items', item['id'])
        await asyncio.wait_for(manager._export_ready(current), 900)
        await manager.packages.start()
        deadline = time.monotonic() + 180
        while time.monotonic() < deadline:
            packages = manager.packages.store.list('items')
            if all(row['state'] in {'complete', 'attention'} for row in packages):
                break
            await asyncio.sleep(.25)
        current = manager.store.get('items', item['id'])
        packages = manager.packages.store.list('items')
        observations = manager.store.list('observations')
        counts = {name: dict(Counter(row.get('label') for row in observations if row.get('category') == name)) for name in ('bed', 'magnet')}
        manifests = []
        for package in packages:
            with zipfile.ZipFile(package['path']) as archive:
                actual = [name for name in archive.namelist() if name.endswith('.jpg')]
            manifests.append({'category': package['category'], 'actualZipImages': len(actual),
                **{key: package.get(key) for key in ('state', 'imageCount', 'actualImages', 'verifiedJobId')}})
            assert len(actual) == package['imageCount']
        verified = photo_results_verified([current], packages)
        result = {'passed': bool(verified), 'seconds': round(time.monotonic() - began, 2),
            'boundary': 'Real decoding/models/export/Boto3; isolated loopback photo platform; no production reads except explicit non-secret settings.',
            'scenario': 'truncated-copy' if partial else 'healthy-copy', 'labels': counts,
            'metadata': metadata, 'state': current['state'], 'endReason': current.get('endReason'),
            'categories': current['categories'], 'packages': manifests,
            'decodeCoverage': current.get('decodeCoverage'), 'localOriginalRetained': path.is_file(),
            'photosVerified': verified, 'platformMetrics': dict(platform.metrics),
            'ownedMedia': [str(path), *[p['path'] for p in packages], *[str(Path(p['imageDir']) / name) for p in packages for name in p['images']]]}
        write(root / 'result.json', result)
        return result
    finally:
        reporter.cancel()
        await asyncio.gather(reporter, return_exceptions=True)
        await close_app(app)
        platform.close()


async def run(args):
    root = args.root.resolve(); root.mkdir(parents=True, exist_ok=False)
    source = args.source.resolve(strict=True)
    before = stamp(source); digest = await asyncio.to_thread(sha256, source)
    settings = json.loads(args.settings.read_text())
    settings = {key: value for key, value in settings.get('settings', settings).items() if key in ALLOW_SETTINGS}
    excerpt = root / 'excerpt.mp4'
    subprocess.run([settings['frameToolPath'], '-nostdin', '-v', 'error', '-ss', str(args.start), '-i', str(source),
        '-t', str(args.duration), '-map', '0:v:0', '-c', 'copy', '-movflags', '+faststart', str(excerpt)],
        check=True, timeout=90)
    results = []
    try:
        for name, partial in [('normal', False), ('damaged', True)]:
            results.append(await scenario(root / name, excerpt, settings, partial))
    finally:
        unchanged = stamp(source) == before and await asyncio.to_thread(sha256, source) == digest
        write(root / 'result.json', {'passed': unchanged and len(results) == 2 and all(row['passed'] for row in results),
            'sourceUnchanged': unchanged, 'sourceSha256': digest, 'sourceStat': before,
            'scenarios': [{key: row[key] for key in ('scenario', 'passed', 'seconds', 'state', 'labels', 'packages', 'photosVerified')} for row in results]})
    print(json.dumps({'root': str(root), 'passed': all(row['passed'] for row in results) and unchanged}, ensure_ascii=False))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', required=True, type=Path)
    parser.add_argument('--settings', required=True, type=Path)
    parser.add_argument('--root', required=True, type=Path)
    parser.add_argument('--start', default=600, type=float)
    parser.add_argument('--duration', default=75, type=float)
    asyncio.run(run(parser.parse_args()))
