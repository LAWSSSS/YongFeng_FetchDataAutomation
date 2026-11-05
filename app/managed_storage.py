"""Application data and durable relative paths for the default media location.

Operational JSON keeps resolved paths for existing SQLite projections. A
transactional relative journal is the portable authority, rebuilt only for
changed rows. On a move, those paths are resolved before managers start.
"""
import json
import os
import re
import sqlite3
from pathlib import Path
from uuid import uuid4

from .workspace_files import create_root, read_marker

PREFIX = "ivg-data:/"


def prepare(root):
    root = Path(root).absolute()
    if any(path.is_symlink() for path in (root, *root.parents)):
        raise ValueError('程序数据目录不能是符号链接。')
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    for name in ('系统', '临时', '测试样本'):
        path = root / name
        if path.is_symlink():
            raise ValueError('程序数据子目录不能是符号链接。')
        path.mkdir(exist_ok=True, mode=0o700)
    media = root / '媒体'
    marker = read_marker(media)
    workspace_id = marker['id'] if marker else uuid4().hex
    create_root(media, workspace_id)
    return workspace_id


def paths(value, root, encode):
    def convert(text):
        if encode:
            if not text.startswith(str(root) + os.sep):
                return PREFIX if text == str(root) else text
            return PREFIX + text[len(str(root)) + 1:]
        if not text.startswith(PREFIX):
            return text
        relative = Path(text[len(PREFIX):])
        if relative.is_absolute() or '..' in relative.parts:
            raise ValueError('保存的数据路径不合法。')
        return str(root / relative)
    if isinstance(value, str):
        return convert(value)
    if isinstance(value, list):
        return [paths(item, root, encode) for item in value]
    if isinstance(value, dict):
        return {convert(key): paths(item, root, encode) for key, item in value.items()}
    return value


class ManagedConnection(sqlite3.Connection):
    """Journal only touched payload rows, in the same commit as their changes."""
    managed_root = None
    tracked_schema = None

    def initialize(self, root):
        self.managed_root = Path(root)
        self.execute('CREATE TABLE IF NOT EXISTS managed_paths (table_name TEXT, row_id TEXT, payload TEXT NOT NULL, PRIMARY KEY(table_name,row_id))')
        self.execute('CREATE TABLE IF NOT EXISTS managed_dirty (table_name TEXT, row_id TEXT, PRIMARY KEY(table_name,row_id))')
        self.execute('CREATE TABLE IF NOT EXISTS managed_location (id INTEGER PRIMARY KEY CHECK(id=1), root TEXT NOT NULL)')
        previous = self.execute('SELECT root FROM managed_location WHERE id=1').fetchone()
        if previous and previous[0] != str(root):
            # An external maintenance connection may have committed dirty rows.
            # Journal them against the old root before resolving a moved folder.
            self.managed_root = Path(previous[0])
            self.commit()
            self.managed_root = Path(root)
            for table, identity, raw in self.execute('SELECT table_name,row_id,payload FROM managed_paths').fetchall():
                if not re.fullmatch(r'[a-z_]+', table):
                    raise ValueError('保存的路径记录不合法。')
                payload = json.dumps(paths(json.loads(raw), root, False), ensure_ascii=False)
                self.execute(f'UPDATE {table} SET payload=? WHERE id=?', (payload, identity))
        self.execute('INSERT INTO managed_location VALUES(1,?) ON CONFLICT(id) DO UPDATE SET root=excluded.root', (str(root),))
        self.commit()

    def _register(self):
        version = self.execute('PRAGMA schema_version').fetchone()[0]
        if self.tracked_schema == version:
            return
        tables = [r[0] for r in self.execute("SELECT name FROM sqlite_master WHERE type='table'")]
        for table in tables:
            if not re.fullmatch(r'[a-z_]+', table) or table.startswith('managed_'):
                continue
            columns = {r[1] for r in self.execute(f'PRAGMA table_info({table})')}
            if not {'id', 'payload'}.issubset(columns):
                continue
            exists = self.execute("SELECT 1 FROM sqlite_master WHERE type='trigger' AND name=?", ('managed_insert_' + table,)).fetchone()
            if exists:
                continue
            for event, row in [('insert', 'new'), ('update', 'new'), ('delete', 'old')]:
                self.execute(f"CREATE TRIGGER managed_{event}_{table} AFTER {event} ON {table} BEGIN INSERT OR IGNORE INTO managed_dirty VALUES('{table}',CAST({row}.id AS TEXT)); END")
            self.execute(f"INSERT OR IGNORE INTO managed_dirty SELECT ?,CAST(id AS TEXT) FROM {table}", (table,))
        self.tracked_schema = self.execute('PRAGMA schema_version').fetchone()[0]

    def commit(self):
        if self.managed_root is not None:
            self._register()
            for table, identity in self.execute('SELECT table_name,row_id FROM managed_dirty').fetchall():
                raw = self.execute(f'SELECT payload FROM {table} WHERE id=?', (identity,)).fetchone()
                if raw:
                    encoded = json.dumps(paths(json.loads(raw[0]), self.managed_root, True), ensure_ascii=False)
                    self.execute('INSERT INTO managed_paths VALUES(?,?,?) ON CONFLICT(table_name,row_id) DO UPDATE SET payload=excluded.payload', (table, identity, encoded))
                else:
                    self.execute('DELETE FROM managed_paths WHERE table_name=? AND row_id=?', (table, identity))
            self.execute('DELETE FROM managed_dirty')
        return super().commit()

    def __exit__(self, kind, value, traceback):
        if kind is None:
            try:
                self.commit()
            except BaseException:
                self.rollback()
                raise
        else:
            self.rollback()
        return False


def configure(workspace, root):
    workspace.fixed_data_root = Path(root)
    # A previously selected disk may be unplugged during startup. Its saved
    # path and workspace marker are still authoritative.
    if workspace.config.get('locationMode') != 'custom':
        media = Path(root) / '媒体'
        marker = read_marker(media)
        workspace.config.update(enabled=True, fixedLocation=True, locationMode='default',
                                root=str(media), parent=str(root), workspaceId=marker['id'])
        workspace.save('settings', workspace.config)
    workspace.derive_config()
