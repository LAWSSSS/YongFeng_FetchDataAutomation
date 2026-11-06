"""One durable record deletion queue; no cloud mutations and no undo endpoint."""
import asyncio
import hashlib
import json
import sqlite3

from .domain import now_iso
from .videos import Conflict
from .video_termination import capture_in_flight
from .workspace_files import compatible


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False).encode()).hexdigest()


def logical_record(row):
    # Cleanup advances display revisions, but cannot authorize a human edit.
    def clean(value):
        if isinstance(value, list):
            return [clean(item) for item in value]
        if isinstance(value, dict):
            return {k: clean(v) for k, v in value.items() if k not in {
                'updatedAt', 'localCleanupState', 'localCleanupAt', 'cleanupState'}}
        return value
    value = clean(row)
    value.pop('revision', None)
    return digest(value)


class RecordDeletion:
    def __init__(self, db, workspace, registries):
        self.db, self.workspace, self.registries = db, workspace, registries
        db.execute('CREATE TABLE IF NOT EXISTS simple_record_deletions (id TEXT PRIMARY KEY, payload TEXT NOT NULL)')
        db.commit()
        self.task = None
        self.wake = asyncio.Event()
        self.lock = asyncio.Lock()

    def get(self, identity):
        row = self.db.execute('SELECT payload FROM simple_record_deletions WHERE id=?', (identity,)).fetchone()
        return json.loads(row[0]) if row else None

    def save(self, operation):
        operation['updatedAt'] = now_iso()
        self.db.execute('INSERT INTO simple_record_deletions VALUES(?,?) ON CONFLICT(id) DO UPDATE SET payload=excluded.payload',
                        (operation['id'], json.dumps(operation, ensure_ascii=False)))
        self.db.commit()

    def row(self, kind, identity):
        _, store = self.registries[kind]
        return store.get('items', identity) if kind == 'slicing' else store.get(identity)

    def public(self, operation):
        return {'operationId': operation['id'], 'operationKind': 'record-delete',
                'state': operation['state'], 'total': len(operation['selections']),
                'finished': sum(r['state'] in ('complete', 'attention') for r in operation['selections']),
                'outcomes': [{k: row.get(k) for k in ('id', 'kind', 'name', 'state', 'message')} for row in operation['selections']],
                'removalId': operation['id'], 'recordRemoval': {'removalId': operation['id'],
                   'ids': [r['id'] for r in operation['selections']]}}

    def register(self, request_id, groups):
        choices = [{'kind': kind, **choice} for kind, rows in groups for choice in rows]
        if not choices or len(choices) > 10000:
            raise ValueError('请选择需要删除的记录。')
        if len({(r['kind'], r['id']) for r in choices}) != len(choices):
            raise ValueError('不能重复选择记录。')
        signature = digest(sorted(choices, key=lambda r: (r['kind'], r['id'])))
        prior = self.get(request_id)
        if prior:
            if prior['signature'] != signature:
                raise Conflict('请求编号已用于其他删除范围。')
            return self.public(prior)
        snapshots = {}
        for kind, rows in groups:
            visibility, store = self.registries[kind]
            snapshot = (store.get_many([r['id'] for r in rows]) if kind == 'receipt_items'
                        else {r['id']: self.row(kind, r['id']) for r in rows})
            visibility.prepare_hide(store, request_id, rows, rows=snapshot, allow_hidden=kind == 'collection')
            snapshots[kind] = snapshot
        selections = []
        for choice in choices:
            row = snapshots[choice['kind']][choice['id']]
            selection = {**choice, 'name': row.get('name') or row.get('title') or row.get('videoName') or '记录',
                         'logicalSignature': logical_record(row), 'state': 'queued'}
            if choice['kind'] == 'collection':
                if any(v['revision'] == row['activeRevision'] and capture_in_flight(v)
                       for v in row['versions']):
                    raise Conflict('所选记录仍在采集，请打开视频详情终止采集后删除。')
                # Deleting a running card is a display action. Do not wait for
                # stable byte counters or clean files that finish after this click.
                assets = self.assets('collection', row)
                permissions = [(asset, *self.workspace.cleanup_eligibility(asset)) for asset in assets
                               if asset.get('cleanup', {}).get('state') != 'complete']
                selection['cleanupAssetIds'] = [asset['id'] for asset, permitted, _ in permissions if permitted]
                selection['initialRetainedReasons'] = list(dict.fromkeys(reason
                    for _, permitted, reason in permissions if not permitted))
                selection['presentationOnly'] = not selection['cleanupAssetIds']
                selection['activeRevision'] = row.get('activeRevision')
            selections.append(selection)
        operation = {'id': request_id, 'signature': signature, 'selections': selections,
                     'state': 'queued', 'createdAt': now_iso()}
        self.save(operation)
        self.wake.set()
        return self.public(operation)

    def assets(self, kind, row):
        w = self.workspace
        bindings = []
        if kind == 'collection':
            for version in row.get('versions', []):
                recovery = version.get('recovery') or {}
                bindings += [(version.get('filePath'), version.get('fileVersion')),
                             (recovery.get('filePath') or recovery.get('destination'), recovery.get('fileVersion'))]
        elif kind == 'slicing':
            bindings = [(row.get('path'), row.get('fileVersion'))]
        # Submission/date records are receipts, not ownership of each child's
        # media. Removing one must not delete videos still listed for review.
        return [a for a in w.rows('assets') if any(path == a['path'] and expected
                and compatible(a.get('fileVersion') or {}, expected) for path, expected in bindings)]

    def shared_record(self, operation, selected, asset):
        kind = selected['kind']
        if kind not in {'collection', 'slicing'}:
            return False
        visibility, store = self.registries[kind]
        removed = visibility.hidden() | {r['id'] for r in operation['selections']
            if r['kind'] == kind and r['state'] != 'attention'}
        rows = store.list('items') if kind == 'slicing' else store.list()
        for other in rows:
            if other['id'] in removed:
                continue
            if any(a['id'] == asset['id'] for a in self.assets(kind, other)):
                return True
        return False

    @staticmethod
    def selection_matches(row, selected):
        if not row:
            return False
        if selected.get('presentationOnly'):
            return (row.get('revision', 0) == selected['expectedRevision']
                    and row.get('activeRevision') == selected.get('activeRevision'))
        return logical_record(row) == selected['logicalSignature']

    async def advance(self, operation, selected):
        kind, identity = selected['kind'], selected['id']
        row = self.row(kind, identity)
        if not self.selection_matches(row, selected):
            selected.update(state='attention', message='记录已有新进度，本条未删除，请刷新后重新选择。')
            return
        if kind == 'collection' and any(v['revision'] == row['activeRevision'] and capture_in_flight(v)
                                        for v in row['versions']):
            selected.update(state='attention', message='视频仍在采集，请打开详情终止采集后删除。')
            return
        assets = self.assets(kind, row)
        if 'cleanupAssetIds' in selected:
            assets = [asset for asset in assets if asset['id'] in selected['cleanupAssetIds']]
        if 'cleanupIds' not in selected:
            selected['cleanupIds'] = []
            retained = list(selected.get('initialRetainedReasons', []))
            for asset in assets:
                if self.shared_record(operation, selected, asset):
                    retained.append('其他记录仍引用此文件。')
                    continue
                if asset.get('cleanup', {}).get('state') == 'complete':
                    continue
                permitted, reason = self.workspace.cleanup_eligibility(asset)
                if not permitted:
                    retained.append(reason)
                    continue
                cleanup_id = 'record-' + digest([operation['id'], asset['id']])[:48]
                result = await self.workspace.request_cleanup_completed(self.workspace.config['revision'], cleanup_id, only=asset['id'])
                if result['total']:
                    selected['cleanupIds'].append(cleanup_id)
                else:
                    retained.append('本地文件已有其他清理操作或凭证变化，未重复清理。')
            selected['retainedReasons'] = list(dict.fromkeys(retained))
            self.save(operation)
        for cleanup_id in selected['cleanupIds']:
            result = self.workspace.cleanup_operation(cleanup_id)
            if not result or result['state'] == 'attention':
                selected.update(state='attention', message='部分本地文件未能安全清理，记录保留，请查看文件状态。')
                return
            if result['state'] != 'complete':
                selected.update(state='waiting', message=result.get('message') or '等待本地文件清理。')
                return
        # Re-read after asynchronous cleanup registration. Only our own cleanup
        # annotations may have changed since the accepted selection.
        row = self.row(kind, identity)
        if not self.selection_matches(row, selected):
            selected.update(state='attention', message='记录已有新进度，未移除网页记录。')
            return
        visibility, store = self.registries[kind]
        action_id = 'delete-' + digest([operation['id'], kind, identity])[:48]
        visibility.hide(store, action_id, [{'id': identity, 'expectedRevision': int(row.get('revision', 0))}],
                        allow_hidden=kind == 'collection')
        selected.update(state='complete', message='记录已移除；' + ('本地文件保留：' + '；'.join(selected['retainedReasons'])
            if selected.get('retainedReasons') else '已核验的绑定文件已清理。' if selected['cleanupIds'] else '后台进度和云端成果保留。'))

    async def tick(self):
        async with self.lock:
            rows = self.db.execute("SELECT payload FROM simple_record_deletions WHERE json_extract(payload,'$.state') IN ('queued','running','waiting')").fetchall()
            for (raw,) in rows:
                operation = json.loads(raw)
                for selected in operation['selections']:
                    if selected['state'] in ('complete', 'attention'):
                        continue
                    try:
                        await self.advance(operation, selected)
                    except (Conflict, ValueError):
                        selected.update(state='attention', message='记录或文件状态已变化，本条保留，请重新核对。')
                    except OSError:
                        selected.update(state='waiting', message='等待本地磁盘恢复。')
                    self.save(operation)
                states = {row['state'] for row in operation['selections']}
                operation['state'] = 'waiting' if states & {'queued', 'waiting'} else 'attention' if 'attention' in states else 'complete'
                self.save(operation)

    async def start(self):
        if not self.task or self.task.done():
            self.task = asyncio.create_task(self.loop())

    async def loop(self):
        while True:
            self.wake.clear()
            try:
                await self.tick()
            except (sqlite3.Error, RuntimeError):
                # A queue failure never turns a destructive operation into success.
                pass
            try:
                await asyncio.wait_for(self.wake.wait(), timeout=2)
            except asyncio.TimeoutError:
                pass

    async def shutdown(self):
        if self.task:
            self.task.cancel()
            await asyncio.gather(self.task, return_exceptions=True)
