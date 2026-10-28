"""Verified move receipts fence late cloud callbacks from restoring old paths.

Only a complete filesystem version can follow an alias. Names and sizes alone
never establish identity. This module does no filesystem or network I/O.
"""
import hashlib
import json
from pathlib import Path


FIELDS = ('device', 'inode', 'size', 'modifiedNs')


def initialize(db):
    db.execute('CREATE TABLE IF NOT EXISTS simple_file_locations (id TEXT PRIMARY KEY, source TEXT NOT NULL, serial INTEGER NOT NULL, payload TEXT NOT NULL)')
    db.execute('CREATE INDEX IF NOT EXISTS file_location_source ON simple_file_locations(source)')


def identity(path, version):
    if not path or not version or not all(key in version for key in FIELDS):
        return None
    return hashlib.sha256((str(path) + json.dumps({k: version[k] for k in FIELDS}, sort_keys=True)).encode()).hexdigest()


def remember(db, entry):
    expected = entry.get('expected') or entry.get('fileVersion')
    key = identity(entry.get('source'), expected)
    destination = entry.get('destinationVersion')
    if not key or not identity(entry.get('destination'), destination):
        return
    value = {'source': entry['source'], 'destination': entry['destination'],
             'fromVersion': expected, 'toVersion': destination}
    encoded = json.dumps(value, sort_keys=True)
    previous = db.execute('SELECT payload FROM simple_file_locations WHERE id=?', (key,)).fetchone()
    if previous and previous[0] == encoded:
        return
    serial = db.execute('SELECT coalesce(max(serial),0)+1 FROM simple_file_locations').fetchone()[0]
    db.execute('INSERT INTO simple_file_locations VALUES(?,?,?,?) ON CONFLICT(id) DO UPDATE SET serial=excluded.serial,payload=excluded.payload',
               (key, entry['source'], serial, encoded))
    db.commit()


def resolve(db, path, version):
    current, fingerprint, serial = path, dict(version or {}), 0
    for _ in range(64):
        key = identity(current, fingerprint)
        if not key:
            break
        row = db.execute('SELECT serial,payload FROM simple_file_locations WHERE id=?', (key,)).fetchone()
        if not row or row[0] <= serial:
            break
        move = json.loads(row[1])
        # The old content hash survives a verified rename/copy even when the
        # target filesystem records different inode/time metadata.
        current, fingerprint, serial = move['destination'], {**fingerprint, **move['toVersion']}, row[0]
    return current, fingerprint


def field(db, row, path_key, version_key):
    path = row.get(path_key)
    if not path or not row.get(version_key):
        return False
    target, version = resolve(db, path, row[version_key])
    if target == path and version == row[version_key]:
        return False
    row[path_key], row[version_key] = target, version
    return True


def keep_cleanup(previous, row):
    if previous and previous.get('localCleanupState') == 'complete':
        row['localCleanupState'] = 'complete'
        if previous.get('localCleanupAt'):
            row['localCleanupAt'] = previous['localCleanupAt']
        for key in ('filePath', 'destination'):
            if key in previous and not previous[key]:
                row[key] = previous[key]


def video(db, row, previous=None):
    for version in row.get('versions', []):
        prior = next((v for v in (previous or {}).get('versions', []) if v.get('revision') == version.get('revision')), {})
        keep_cleanup(prior, version)
        field(db, version, 'filePath', 'fileVersion')
        recovery = version.get('recovery') or {}
        keep_cleanup(prior.get('recovery') or {}, recovery)
        original_version = dict(recovery.get('fileVersion') or {})
        field(db, recovery, 'filePath', 'fileVersion')
        if recovery.get('destination') and recovery.get('filePath'):
            # Destination is the same bound recovery file, not a new output.
            target, _ = resolve(db, recovery['destination'], original_version)
            if target != recovery['destination']:
                recovery['destination'] = target


def package(db, row):
    previous = dict(row.get('fileVersion') or {})
    if field(db, row, 'path', 'fileVersion') and row.get('object'):
        row.setdefault('uploadFileVersion', previous)
    field(db, row, 'videoPath', 'videoFileVersion')
    for name in row.get('images', []):
        path = row.get('imagePaths', {}).get(name) or str(Path(row.get('imageDir') or '/') / name)
        target, version = resolve(db, path, row.get('imageVersions', {}).get(name))
        if target != path:
            row.setdefault('imagePaths', {})[name] = target
            row.setdefault('imageVersions', {})[name] = version
    paths = row.get('imagePaths', {})
    if row.get('images') and all(n in paths for n in row['images']):
        parents = {str(Path(paths[n]).parent) for n in row['images']}
        if len(parents) == 1:
            row['imageDir'] = parents.pop()
