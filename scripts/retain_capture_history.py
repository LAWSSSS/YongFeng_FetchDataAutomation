"""One-time, offline retention of capture records and their exclusive files."""
import argparse
import json
import os
import socket
import sqlite3
import subprocess
import sys
from collections import Counter, defaultdict
from contextlib import closing
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from app.audit_moves import checked_path
from app.capture_history import keep_receipts
from app.managed_storage import paths
from scripts.reset_managed_data import delete_manifest, private_json


LOCAL_ZONE = ZoneInfo('Asia/Shanghai')
ACTIVE = {'queued', 'running', 'waiting_connection', 'reconciling', 'submitting', 'downloading', 'publishing'}
TABLES = (
    'jobs', 'simple_videos', 'simple_submissions', 'simple_collection_runs',
    'simple_history_downloads', 'simple_slicing_items', 'simple_slice_package_items',
    'simple_workspace_assets', 'simple_files', 'simple_video_operations', 'simple_final_processing',
)


def submission_day(value):
    try:
        timestamp = datetime.fromisoformat(value)
    except (ValueError, TypeError):
        return None
    return (timestamp.astimezone(LOCAL_ZONE) if timestamp.tzinfo else timestamp).date()


def read_records(db, root, table, archived):
    if not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                      ('retired_records' if archived else table,)).fetchone():
        return {}
    records = (db.execute('SELECT record_id,payload FROM retired_records WHERE table_name=?', (table,))
               if archived else db.execute(f'SELECT id,payload FROM {table}'))
    return {identity: paths(json.loads(raw), root, False) for identity, raw in records}


def collect_files(tables, excluded, protected):
    candidates, retained = defaultdict(list), set()

    def remember(path, version, owned):
        if not path:
            return
        path = Path(path)
        if not path.is_absolute():
            return
        if owned:
            candidates[str(path)].append(version or {})
        else:
            retained.add(str(path))

    slicing_owners = defaultdict(set)
    for records in tables:
        for identity, item in records['simple_slicing_items'].items():
            slicing_owners[identity].add(item.get('collectionVideoId'))
    owned_slices = {identity for identity, owners in slicing_owners.items() if owners and owners <= excluded}
    for records in tables:
        for identity, video in records['simple_videos'].items():
            for version in video.get('versions', []):
                owned = identity in excluded
                remember(version.get('filePath'), version.get('fileVersion'), owned)
                recovery = version.get('recovery') or {}
                remember(recovery.get('filePath') or recovery.get('destination'), recovery.get('fileVersion'), owned)
        for record in records['simple_history_downloads'].values():
            remember(record.get('filePath') or record.get('path') or record.get('destination'),
                     record.get('fileVersion'), record.get('videoId') in excluded)
        for package in records['simple_slice_package_items'].values():
            owned = package.get('itemId') in owned_slices
            remember(package.get('path'), package.get('fileVersion'), owned)
            if not owned:
                remember(package.get('videoPath'), None, False)
            for name, version in (package.get('imageVersions') or {}).items():
                remember((package.get('imagePaths') or {}).get(name) or
                         (str(Path(package['imageDir']) / name) if package.get('imageDir') else ''),
                         version, owned)
        for identity, item in records['simple_slicing_items'].items():
            remember(item.get('path'), item.get('fileVersion'), identity in owned_slices)
        for asset in records['simple_workspace_assets'].values():
            links = asset.get('links') or []
            exclusively_old = bool(links) and all(isinstance(link, dict) for link in links) and all(
                (link.get('kind') == 'collection' and link.get('id') in excluded)
                or (link.get('kind') == 'slicing' and link.get('id') in owned_slices)
                for link in links)
            remember(asset.get('path'), asset.get('fileVersion'), exclusively_old)
        for row in records['simple_files'].values():
            links = row.get('links') or {}
            exclusively_old = (bool(links.get('collection') or links.get('slicing'))
                and not links.get('uploads')
                and all(isinstance(link, dict) and link.get('id') in excluded for link in links.get('collection', []))
                and all(link in owned_slices for link in links.get('slicing', [])))
            remember(row.get('path'), row.get('fileVersion'), exclusively_old)

    output = []
    for name, versions in sorted(candidates.items()):
        if not Path(name).suffix.lower() in {'.mp4', '.mov', '.mkv', '.avi', '.m4v', '.zip', '.jpg', '.jpeg', '.png'}:
            continue
        expected = max(versions, key=len)
        entry = {'path': name, 'expected': expected, 'state': 'planned'}
        try:
            path = checked_path(name)
            if (name in retained or any(path.is_relative_to(root) for root in protected)
                    or path.exists() and path.stat().st_nlink > 1):
                entry.update(state='retained', reason='其他记录共用或文件受保护。')
            elif any(version and any(version.get(k) != expected.get(k) for k in
                                     ('device', 'inode', 'size', 'modifiedNs')) for version in versions):
                entry.update(state='retained', reason='多份文件指纹不一致。')
            elif path.parts[1:2] == ('Volumes',) and not Path(*path.parts[:3]).is_mount():
                entry.update(state='retained', reason='磁盘未挂载，无法核对文件。')
        except (ValueError, OSError):
            entry.update(state='retained', reason='无法核对文件路径。')
        output.append(entry)
    for name in sorted(retained - candidates.keys()):
        try:
            if checked_path(name).is_file():
                output.append({'path': name, 'expected': {}, 'state': 'retained',
                               'reason': '其他处理记录引用或无法确认属于本次旧采集，保留。'})
        except (OSError, ValueError):
            pass
    return output


def retention_plan(db, root, cutoff, frozen=None):
    live, archive = ({table: read_records(db, root, table, archived) for table in TABLES}
                     for archived in (False, True))
    jobs = {**archive['jobs'], **live['jobs']}
    videos = {**archive['simple_videos'], **live['simple_videos']}
    excluded_jobs = {identity for identity, job in jobs.items()
                     if (day := submission_day(job.get('createdAt'))) and day < cutoff}
    excluded = {identity for identity, video in videos.items() if video.get('jobId') in excluded_jobs
                or (video.get('jobId') not in jobs
                    and (day := submission_day(video.get('submittedAt'))) and day < cutoff)}
    if frozen:
        excluded &= set(frozen['excludedVideos'])
        excluded_jobs &= set(frozen['excludedJobs'])
    if (any(job.get('status') == 'running' for job in live['jobs'].values())
            or any(row.get('state') in ACTIVE and row.get('videoId') in excluded
                   for table in ('simple_video_operations', 'simple_final_processing') for row in live[table].values())
            or any(v.get('state') in ACTIVE for identity, row in live['simple_videos'].items() if identity in excluded
                   for v in row.get('versions', []))):
        raise ValueError('有采集或文件操作尚未结束，请先完成后再清理。')
    restore = {identity for identity, video in archive['simple_videos'].items()
               if identity not in live['simple_videos'] and identity not in excluded
               and not video.get('deletedAt') and video.get('jobId') in jobs
               and submission_day(jobs[video['jobId']].get('createdAt')) is not None}
    if frozen:
        restore &= set(frozen['restoredVideos'])
    protected = (root / '测试样本', root / '临时', root / '系统', Path.home() / 'Desktop')
    return live, archive, excluded_jobs, excluded, restore, collect_files((live, archive), excluded, protected)


def restored_video(row):
    # Archived work queues, checkpoint paths and consent cannot be reused.
    clean = {key: row[key] for key in ('id', 'jobId', 'taskKey', 'recordId', 'plate', 'name', 'qualityStart',
             'qualityEnd', 'photoCount', 'inspectionDurationSeconds', 'admissionRule', 'revision',
             'activeRevision', 'createdAt') if key in row}
    clean.update(acceptedRevision=None, requests={}, error='', batchDownloadApplied=True)
    clean['versions'] = []
    for old in row['versions']:
        version = {key: old[key] for key in ('revision', 'startTime', 'endTime', 'durationSeconds', 'name',
                    'taskId', 'resultId', 'createdAt', 'previousActiveRevision') if key in old}
        if old['state'] in {'withdrawn', 'cloud_deleted'}:
            version['state'] = old['state']
        elif old.get('taskId'):
            version.update(state='submitted', restoredFromArchive=True,
                           message='旧采集已恢复，等待核对原平台任务。')
        else:
            version.update(state='attention', message='旧记录没有确切任务 ID，请先核对原平台。')
        clean['versions'].append(version)
    return clean


def protect_open_files(files):
    """The service is stopped; also preserve media opened by another process."""
    existing = [entry for entry in files if entry['state'] in {'planned', 'deleting'} and Path(entry['path']).is_file()]
    opened = set()
    for offset in range(0, len(existing), 50):
        batch = existing[offset:offset+50]
        result = subprocess.run(['/usr/sbin/lsof', '-Fn', '--', *(entry['path'] for entry in batch)],
                                capture_output=True, text=True, timeout=20)
        if result.returncode not in {0, 1}:
            raise ValueError('无法核对文件占用，未执行删除。')
        opened.update(line[1:] for line in result.stdout.splitlines() if line.startswith('n'))
    for entry in existing:
        if entry['path'] in opened:
            entry.update(state='retained', reason='其他程序正在使用文件，保留。')


def apply_records(db, live, archive, excluded_jobs, excluded, restore, cutoff):
    def put(table, identity, value):
        raw = json.dumps(value, ensure_ascii=False)
        if table == 'jobs':
            db.execute('INSERT OR IGNORE INTO jobs(id,created_at,payload) VALUES(?,?,?)',
                       (identity, value['createdAt'], raw))
        else:
            db.execute(f'INSERT OR IGNORE INTO {table}(id,payload) VALUES(?,?)', (identity, raw))

    def remove(table, identity):
        db.execute('DELETE FROM retired_records WHERE table_name=? AND record_id=?', (table, identity))
        if identity in live[table]:
            db.execute(f'DELETE FROM {table} WHERE id=?', (identity,))

    with db:
        jobs = {**archive['jobs'], **live['jobs']}
        for identity in excluded:
            row = live['simple_videos'].get(identity) or archive['simple_videos'][identity]
            keep_receipts(db, row, jobs.get(row.get('jobId')))
            remove('simple_videos', identity)
            db.execute('DELETE FROM simple_collection_hidden_records WHERE id=?', (identity,))
        for table in ('simple_video_operations', 'simple_final_processing', 'simple_history_downloads'):
            for identity, entry in live[table].items():
                if entry.get('videoId') in excluded:
                    # Preserve receipts, never revive an old operation on startup.
                    db.execute('INSERT OR IGNORE INTO retired_records VALUES(?,?,?)',
                               (table, identity, json.dumps(entry, ensure_ascii=False)))
                    db.execute(f'DELETE FROM {table} WHERE id=?', (identity,))
        for identity in excluded_jobs:
            remove('jobs', identity)
        restored_jobs = defaultdict(set)
        for identity in restore:
            row = restored_video(archive['simple_videos'][identity])
            put('simple_videos', identity, row)
            restored_jobs[row['jobId']].add(row['taskKey'])
            db.execute('DELETE FROM simple_collection_hidden_records WHERE id=?', (identity,))
            db.execute("DELETE FROM retired_records WHERE table_name IN ('simple_videos','simple_collection_hidden_records') AND record_id=?", (identity,))
        for identity, keys in restored_jobs.items():
            if identity not in live['jobs']:
                job = dict(jobs[identity], status='complete', downloadAfterCollection=False, downloadDirectory='')
                job.pop('deletedAt', None)
                job['items'] = [item for item in job['items'] if item['key'] in keys]
                put('jobs', identity, job)
            db.execute("DELETE FROM retired_records WHERE table_name='jobs' AND record_id=?", (identity,))
            db.execute('DELETE FROM simple_submission_hidden_jobs WHERE id=?', (identity,))
        for identity, receipt in {**archive['simple_submissions'], **live['simple_submissions']}.items():
            if receipt.get('kind') != 'collection':
                continue
            job_id = (receipt.get('detail') or {}).get('jobId')
            day = submission_day(receipt.get('submittedAt'))
            if job_id in excluded_jobs or not job_id and day and day < cutoff:
                remove('simple_submissions', identity)
            elif job_id in restored_jobs:
                put('simple_submissions', identity, receipt)
                db.execute('DELETE FROM simple_receipts_hidden_records WHERE id=?', (identity,))
                db.execute("DELETE FROM retired_records WHERE table_name='simple_submissions' AND record_id=?", (identity,))
        for identity, run in {**archive['simple_collection_runs'], **live['simple_collection_runs']}.items():
            day = submission_day(run.get('createdAt'))
            linked = set(run.get('jobIds') or ([run['jobId']] if run.get('jobId') else []))
            if day and day < cutoff and linked <= excluded_jobs:
                remove('simple_collection_runs', identity)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--database', type=Path, required=True)
    parser.add_argument('--keep-from', type=date.fromisoformat, required=True)
    parser.add_argument('--apply-after-service-stop', action='store_true')
    parser.add_argument('--port', type=int, default=8786)
    parser.add_argument('--resume', type=Path, help='已有 files.json，用原清单恢复，不扩大范围。')
    args = parser.parse_args()
    database = args.database.resolve(strict=True)
    root = database.parent.parent
    if database != (ROOT / '数据' / '系统' / 'collection.sqlite3').resolve():
        parser.error('仅允许指定本项目日常站的 数据/系统/collection.sqlite3。')
    if args.apply_after_service_stop:
        try:
            with socket.create_connection(('127.0.0.1', args.port), timeout=2):
                raise ValueError('日常站端口仍在监听，未执行清理。')
        except ConnectionRefusedError:
            pass
    uri = database.as_uri() + ('?mode=rw' if args.apply_after_service_stop else '?mode=ro')
    with closing(sqlite3.connect(uri, uri=True)) as db:
        frozen = json.loads(args.resume.read_text()) if args.resume else None
        if frozen and (frozen['database'] != str(database) or frozen['keepFrom'] != args.keep_from.isoformat()):
            raise ValueError('恢复清单不属于本次数据库和日期范围。')
        if frozen and frozen.get('recordsApplied'):
            print(json.dumps(frozen['result'], ensure_ascii=False), flush=True)
            return
        live, archive, excluded_jobs, excluded, restore, files = retention_plan(db, root, args.keep_from, frozen)
        summary = {'restore': len(restore), 'removeRecords': len(excluded), 'removeBatches': len(excluded_jobs),
                   'fileCandidates': len(files), 'existingFiles': sum(Path(entry['path']).is_file() for entry in files)}
        print(json.dumps(summary, ensure_ascii=False), flush=True)
        if not args.apply_after_service_stop:
            return
        folder = (args.resume.parent if args.resume else database.parent / '备份' /
                  ('采集保留-' + datetime.now(LOCAL_ZONE).strftime('%Y%m%d-%H%M%S-%f')))
        folder.mkdir(parents=True, mode=0o700, exist_ok=True)
        backup = folder / 'collection-before.sqlite3'
        if not frozen:
            with closing(sqlite3.connect(backup)) as copy:
                db.backup(copy)
                if copy.execute('PRAGMA quick_check').fetchone() != ('ok',):
                    raise ValueError('数据库备份校验失败，未执行清理。')
            os.chmod(backup, 0o600)
        journal = folder / 'files.json'
        manifest = frozen or {'files': files, 'keepFrom': args.keep_from.isoformat(), 'database': str(database),
                              'excludedJobs': sorted(excluded_jobs), 'excludedVideos': sorted(excluded),
                              'restoredVideos': sorted(restore), 'summary': summary}
        if frozen:
            current = {entry['path']: entry for entry in files}
            # A resumed operation cannot delete media now shared with a new record.
            if any(current.get(entry['path'], {}).get('state') == 'retained'
                   for entry in manifest['files'] if entry['state'] in {'planned', 'deleting'}):
                raise ValueError('恢复时发现新增引用，请重新核对原清单；未继续删除。')
        protect_open_files(manifest['files'])
        private_json(journal, manifest)
        print(f'备份和文件清单：{folder}', flush=True)
        delete_manifest(manifest, journal, [str(root / name) for name in ('测试样本', '临时', '系统')])
        apply_records(db, live, archive, excluded_jobs, excluded, restore, args.keep_from)
        manifest['result'] = {'backup': str(backup), **manifest['summary'],
                              'files': dict(Counter(entry['state'] for entry in manifest['files']))}
        manifest['recordsApplied'] = True
        private_json(journal, manifest)
        print(json.dumps(manifest['result'], ensure_ascii=False), flush=True)


if __name__ == '__main__':
    main()
