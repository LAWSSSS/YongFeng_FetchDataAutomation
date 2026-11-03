"""Explicit one-time reset. Never called by application startup or a web button.

Run only after the service has stopped. Unknown files, models, desktop originals
and all cloud entities are excluded. Every local deletion has a durable receipt.
"""
from __future__ import annotations
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import sqlite3
import sys
from datetime import datetime

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from app.audit_moves import checked_path
from app.workspace_files import file_version, compatible, reconnected_version


def private_json(path, value):
    temporary = path.with_name(path.name + '.tmp')
    with temporary.open('w') as stream:
        os.chmod(temporary, 0o600)
        json.dump(value, stream, ensure_ascii=False, indent=2)
        stream.flush(); os.fsync(stream.fileno())
    temporary.replace(path)


def sha(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def preserve_sample(source, destination):
    from app.slicing import safe_path
    source = safe_path(str(source), must_exist=True, kind="file")
    before = file_version(source, True)
    destination = checked_path(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    fingerprint = sha(source)
    if destination.exists():
        if sha(destination) != fingerprint:
            raise ValueError('测试样本目标已有不同内容，未覆盖。')
    else:
        part = checked_path(destination.with_suffix(destination.suffix + '.part'))
        # A known interrupted copy may be overwritten; the source never is.
        with source.open('rb') as reader, part.open('wb') as writer:
            os.chmod(part, 0o600)
            shutil.copyfileobj(reader, writer, 4 * 1024 * 1024)
            writer.flush(); os.fsync(writer.fileno())
        shutil.copystat(source, part)
        if sha(part) != fingerprint or not compatible(file_version(source, True), before):
            raise ValueError('测试样本复制校验失败，原片保留。')
        part.replace(destination)
    if not compatible(file_version(source, True), before):
        raise ValueError('复制期间原片版本变化，停止整理。')
    return {'sourceName': source.name, 'relativePath': str(destination.relative_to(destination.parents[2])),
            'sha256': fingerprint, 'size': before['size'], 'sourceVersion': before,
            'fileVersion': file_version(destination, True)}


def read_rows(db, table):
    if not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone():
        return []
    return [json.loads(row[0]) for row in db.execute(f'SELECT payload FROM {table}')]


def collect_manifest(db, old_data, protected):
    candidates = {}
    def add(path, expected, receipt):
        if not path or not isinstance(expected, dict) or not expected.get('size'):
            return
        path = Path(path)
        if not path.is_absolute() or any(path.is_relative_to(Path(p)) for p in protected):
            return
        if path.suffix.lower() not in {'.mp4','.mov','.mkv','.avi','.m4v','.zip','.jpg','.jpeg','.png','.part','.tmp'}:
            return
        entry = candidates.setdefault(str(path), {'path': str(path), 'expected': expected, 'receipts': [], 'state': 'planned'})
        entry['receipts'].append(receipt)
        if len(expected) > len(entry['expected']):
            entry['expected'] = expected
    for asset in read_rows(db, 'simple_workspace_assets'):
        add(asset.get('path'), asset.get('fileVersion'), 'asset:' + asset['id'])
    for item in read_rows(db, 'simple_slicing_items'):
        add(item.get('path'), item.get('fileVersion'), 'slice:' + item['id'])
    for package in read_rows(db, 'simple_slice_package_items'):
        add(package.get('path'), package.get('fileVersion'), 'package:' + package['id'])
        for name, version in (package.get('imageVersions') or {}).items():
            path = (package.get('imagePaths') or {}).get(name)
            if path:
                add(path, version, 'image:' + package['id'] + ':' + name)
    for row in read_rows(db, 'simple_videos'):
        for version in row.get('versions', []):
            add(version.get('filePath'), version.get('fileVersion'), f"video:{row['id']}:{version['revision']}")
            recovery = version.get('recovery') or {}
            add(recovery.get('filePath') or recovery.get('destination'), recovery.get('fileVersion'), 'recovery:' + row['id'])
            # Private checkpoint names derive from persisted video/version IDs.
            # Freeze the current identity, never sweep their containing folders.
            if not re.fullmatch('[a-zA-Z0-9_-]+', row['id']):
                continue
            work_roots = [Path(old_data) / 'work']
            for config in read_rows(db, 'simple_workspace_settings'):
                if config.get('root'):
                    work_roots.append(Path(config['root']) / '.缓存' / '下载')
            for work in work_roots:
                part = work / row['id'] / f"v{int(version['revision'])}.part"
                try:
                    if part.is_file():
                        add(str(part), file_version(checked_path(part), True), 'checkpoint:' + row['id'])
                except (OSError, ValueError):
                    pass
    return list(candidates.values())


def prune_sensitive(value):
    if isinstance(value, dict):
        return {key: prune_sensitive(item) for key, item in value.items()
                if not any(part in key.lower() for part in ('password','token','authorization','credential','secret'))
                and key.lower() not in {'url','downloadurl','signedurl','object','keys'}}
    if isinstance(value, list):
        return [prune_sensitive(item) for item in value]
    if isinstance(value, str) and value.startswith(('http:', 'https:', '//')):
        return value.split('?', 1)[0]
    return value


KEEP = {'simple_admission_settings','simple_download_settings','simple_slicing_settings','simple_upload_settings'}


def retire_database(source, destination, reset_id):
    if destination.exists():
        with sqlite3.connect(destination) as db:
            present = db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='retired_reset'").fetchone()
            if present and db.execute('SELECT 1 FROM retired_reset WHERE id=?', (reset_id,)).fetchone():
                return
        raise ValueError('新数据库已经存在其他进度，未覆盖。')
    temporary = destination.with_suffix('.building')
    if temporary.exists():
        temporary.unlink()
    with sqlite3.connect(f'file:{source}?mode=ro', uri=True) as old, sqlite3.connect(temporary) as db:
        old.backup(db)
        db.execute('CREATE TABLE retired_records (table_name TEXT, record_id TEXT, payload TEXT NOT NULL, PRIMARY KEY(table_name,record_id))')
        db.execute('CREATE TABLE retired_task_keys (key TEXT PRIMARY KEY)')
        db.execute('CREATE TABLE retired_reset (id TEXT PRIMARY KEY, created_at TEXT)')
        db.execute('CREATE TABLE IF NOT EXISTS retired_pending (id TEXT PRIMARY KEY,payload TEXT NOT NULL)')
        for video in read_rows(db, 'simple_videos'):
            for version in video.get('versions', []):
                if version.get('state') in {'submitted','waiting','uncertain','submitting'}:
                    receipt={'id':video['id']+':'+str(version['revision']),'kind':'capture',
                             'taskId':str(version.get('taskId') or ''),
                             'state':'waiting' if version.get('taskId') else 'attention',
                             'version':prune_sensitive(version)}
                    db.execute('INSERT OR REPLACE INTO retired_pending VALUES(?,?)',(receipt['id'],json.dumps(receipt)))
        for package in read_rows(db, 'simple_slice_package_items'):
            if package.get('state') in {'submitting','importing'}:
                receipt = {'id': package['id'], 'kind': 'photo-import', 'dataset': str(package.get('dataset') or ''),
                           'taskId': str(package.get('jobId') or ''),
                           'state': 'waiting' if package.get('jobId') else 'attention'}
                db.execute('INSERT OR REPLACE INTO retired_pending VALUES(?,?)', (receipt['id'], json.dumps(receipt)))
        for item in read_rows(db, 'simple_slicing_items'):
            for category, value in item.get('categories',{}).items():
                if value.get('state') in {'submitting','platform_processing','waiting'} and value.get('taskId'):
                    receipt={'id':item['id']+':'+category,'kind':'platform-slice','taskId':str(value['taskId']),'state':'waiting'}
                    db.execute('INSERT OR REPLACE INTO retired_pending VALUES(?,?)',(receipt['id'],json.dumps(receipt)))
        for job in read_rows(db, 'jobs'):
            for item in job.get('items', []):
                if item.get('key') and item.get('status') in {'submitted','duplicate','uncertain','submitting'}:
                    db.execute('INSERT OR IGNORE INTO retired_task_keys VALUES(?)', (item['key'],))
        tables = [row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")]
        for table in tables:
            if table.startswith(('sqlite_', 'retired_')) or table in KEEP:
                continue
            if not re.fullmatch('[a-z_]+', table):
                continue
            columns = {row[1] for row in db.execute(f'PRAGMA table_info({table})')}
            if {'id', 'payload'}.issubset(columns):
                for identity, raw in db.execute(f'SELECT id,payload FROM {table}').fetchall():
                    payload = prune_sensitive(json.loads(raw))
                    db.execute('INSERT INTO retired_records VALUES(?,?,?)', (table, str(identity), json.dumps(payload, ensure_ascii=False)))
            db.execute(f'DELETE FROM {table}')
        for table in ('simple_slicing_settings','simple_upload_settings'):
            for row in read_rows(db, table):
                row.update(enabled=False, latestRunId='')
                for key in ('settingsRequestIds','processLimitRequests'):
                    row[key] = {}
                db.execute(f'UPDATE {table} SET payload=?', (json.dumps(row, ensure_ascii=False),))
        db.execute('INSERT INTO retired_reset VALUES(?,?)', (reset_id, datetime.now().isoformat()))
        db.commit()
        db.execute('PRAGMA wal_checkpoint(TRUNCATE)')
    os.chmod(temporary, 0o600)
    temporary.replace(destination)


def delete_manifest(manifest, journal, protected):
    ledger = sqlite3.connect(journal.with_suffix('.sqlite3'))
    ledger.execute('CREATE TABLE IF NOT EXISTS files(path TEXT PRIMARY KEY,payload TEXT NOT NULL)')
    saved = {p:json.loads(raw) for p,raw in ledger.execute('SELECT path,payload FROM files')}
    def persist(entry):
        ledger.execute('INSERT INTO files VALUES(?,?) ON CONFLICT(path) DO UPDATE SET payload=excluded.payload',
                       (entry['path'],json.dumps(entry,ensure_ascii=False)))
        ledger.commit()
    for entry in manifest['files']:
        if entry['path'] in saved:
            entry.update(saved[entry['path']])
        if entry['state'] in {'deleted', 'missing', 'retained'}:
            continue
        try:
            path = checked_path(entry['path'])
            if any(path.is_relative_to(Path(root)) for root in protected):
                raise ValueError('受保护文件，保留。')
            if not path.exists():
                entry.update(state='deleted' if entry['state'] == 'deleting' else 'missing')
            else:
                expected = entry['expected']
                if not {'device','inode','size','modifiedNs'}.issubset(expected):
                    raise ValueError('缺少完整文件版本，保留。')
                actual = file_version(path, 'edgeHash' in expected)
                if not compatible(actual, expected) and not reconnected_version(actual, expected):
                    raise ValueError('文件版本不匹配，保留。')
                entry['state'] = 'deleting'
                persist(entry)
                # No awaits or new readers: this command runs after service exit.
                if file_version(path, False) != {k:actual[k] for k in ('device','inode','size','modifiedNs')}:
                    raise ValueError('删除前文件发生变化，保留。')
                path.unlink()
                entry['state'] = 'deleted'
                for parent in (path.parent, path.parent.parent):
                    try:
                        parent.rmdir()  # Unknown siblings prevent removal.
                    except OSError:
                        break
        except (ValueError, OSError) as error:
            entry.update(state='retained', reason='版本或磁盘不可核实，文件保留。', errorType=type(error).__name__)
        persist(entry)
    ledger.close()
    manifest['state'] = 'complete' if all(r['state'] != 'retained' for r in manifest['files']) else 'attention'
    private_json(journal, manifest)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--source-db', type=Path, required=True)
    parser.add_argument('--data-root', type=Path, required=True)
    parser.add_argument('--normal', type=Path, required=True)
    parser.add_argument('--abnormal', type=Path, required=True)
    parser.add_argument('--apply-after-service-stop', action='store_true')
    args = parser.parse_args()
    from app.managed_storage import prepare
    root = args.data_root.absolute()
    prepare(root)
    samples_path = root / '测试样本' / 'samples.json'
    samples = []
    for name, source in [('正常', args.normal), ('异常', args.abnormal)]:
        destination = root / '测试样本' / name / source.name
        samples.append(preserve_sample(source, destination))
        print(name + '样本已保存并通过完整校验。', flush=True)
    private_json(samples_path, {'samples': samples})
    if not args.apply_after_service_stop:
        return
    # Refuse resets while the source application's server is listening.
    import urllib.request
    try:
        with urllib.request.urlopen('http://127.0.0.1:8786/api/health', timeout=2) as response:
            if response.status == 200:
                raise ValueError('本机服务仍在运行，未执行清空。')
    except (OSError, TimeoutError):
        pass
    system = root / '系统'
    journal = system / 'reset-manifest.json'
    protected = [Path.home()/'Desktop'/'视频抽帧', ROOT/'assets', ROOT/'build', root/'测试样本']
    if journal.exists():
        manifest = json.loads(journal.read_text())
    else:
        with sqlite3.connect(f'file:{args.source_db.resolve()}?mode=ro', uri=True) as db:
            backup = system / '备份' / 'reset-before.sqlite3'
            backup.parent.mkdir(exist_ok=True)
            with sqlite3.connect(backup) as target:
                db.backup(target)
            manifest = {'id': 'managed-data-v1', 'state': 'prepared',
                        'files': collect_manifest(db, args.source_db.parent, protected)}
        private_json(journal, manifest)
    retire_database(args.source_db.resolve(), system/'collection.sqlite3', manifest['id'])
    # Retirement fences restart before any source file is deleted.
    delete_manifest(manifest, journal, protected)
    counts = {state: sum(row['state'] == state for row in manifest['files']) for state in ('deleted','missing','retained')}
    print(json.dumps(counts, ensure_ascii=False))


if __name__ == '__main__':
    main()
