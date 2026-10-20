"""Shared video locations and recoverable archive moves.

The journal owns only its generated temporary files. Original videos are moved
only after both categories have definitive results; conflicting paths are held.
"""
import asyncio
import json
from pathlib import Path
from uuid import uuid4

from .audit_moves import (checked_path, copy_verified, finish_source, move_direct,
                          prepare_paths, publish_copied, recover_published,
                          same_device, source_fingerprint)
from .domain import now_iso


class FileIndex:
    def __init__(self, store, videos, uploads, slicing, moves, leases):
        self.db, self.videos, self.uploads, self.slicing = store.db, videos, uploads, slicing
        self.moves, self.leases = moves, leases
        self.lock = asyncio.Lock()
        self.task = None
        for table in ('files', 'file_moves'):
            self.db.execute(f'CREATE TABLE IF NOT EXISTS simple_{table} (id TEXT PRIMARY KEY, payload TEXT NOT NULL)')
        self.db.commit()
        leases.subscribe_moves(self.moved)

    def rows(self, table):
        assert table in {'files', 'file_moves'}
        return [json.loads(row[0]) for row in self.db.execute(f'SELECT payload FROM simple_{table}')]

    def save(self, table, row):
        assert table in {'files', 'file_moves'}
        row['updatedAt'] = now_iso()
        self.db.execute(f'INSERT INTO simple_{table} VALUES(?,?) ON CONFLICT(id) DO UPDATE SET payload=excluded.payload',
                        (row['id'], json.dumps(row, ensure_ascii=False)))
        self.db.commit()

    def refresh(self):
        known = {row['path']: row for row in self.rows('files')}
        references = {}
        for item in self.slicing.store.list('items'):
            if item.get('path'):
                references.setdefault(item['path'], {'slicing': [], 'uploads': [], 'collection': []})['slicing'].append(item['id'])
        for item in self.uploads.store.list('items'):
            if item.get('path'):
                references.setdefault(item['path'], {'slicing': [], 'uploads': [], 'collection': []})['uploads'].append(item['id'])
        for item in self.videos.store.list(include_deleted=True):
            for version in item.get('versions', []):
                if version.get('filePath'):
                    references.setdefault(version['filePath'], {'slicing': [], 'uploads': [], 'collection': []})['collection'].append(
                        {'id': item['id'], 'revision': version.get('revision')})
        for path, links in references.items():
            row = known.get(path) or {'id': uuid4().hex, 'path': path, 'archiveState': 'not_ready'}
            if row.get('links') != links or path not in known:
                row['links'] = links
                self.save('files', row)
        return self.rows('files')

    def moved(self, source, destination, version):
        for row in self.rows('files'):
            if row['path'] == source:
                row.update(path=destination, fileVersion=version, previousPath=source)
                if row.get('archiveState') == 'complete':
                    row['archivePath'] = destination
                for journal in self.rows('file_moves'):
                    if (journal['fileId'] == row['id'] and journal['state'] not in {'complete', 'superseded'}
                            and journal['destination'] != destination and not journal.get('destinationVersion')
                            and not journal.get('temporaryVersion')):
                        journal.update(state='superseded', message='原片已通过其他受控移动重新定位。')
                        self.save('file_moves', journal)
                        row.update(archiveState='waiting', archiveMessage='位置已更新，等待归档。')
                self.save('files', row)

    def state(self):
        rows = self.rows('files')
        counts = {}
        for row in rows:
            state = row.get('archiveState', 'not_ready')
            counts[state] = counts.get(state, 0) + 1
        return {'items': rows, 'counts': counts}

    def destination(self, path, kind, settings):
        source = checked_path(path)
        roots = [checked_path(root) for root in settings['directories']]
        classifications = ('已上传', '磁盘吊未上传', '车斗未上传')
        roots += [root / name for root in list(roots) for name in classifications]
        last_plan = self.moves.latest() or {}
        roots += [checked_path(last_plan[key]) for key in ('uploadedDirectory', 'magnetMissingDirectory', 'bedMissingDirectory') if last_plan.get(key)]
        roots.sort(key=lambda path: len(path.parts), reverse=True)
        root = next((root for root in roots if source.is_relative_to(root)), None)
        if root is None:
            raise ValueError('文件不在视频或分类目录内，保留原位。')
        parts = list(source.relative_to(root).parts)
        archive_names = {settings['archiveCompletedDirectory'], settings['archiveEmptyDirectory']}
        if parts and parts[0] in archive_names:
            parts.pop(0)
        folder = settings['archiveCompletedDirectory'] if kind == 'complete' else settings['archiveEmptyDirectory']
        return root / folder / Path(*parts)

    def eligible(self, row):
        items = [self.slicing.store.get('items', identity) for identity in row.get('links', {}).get('slicing', [])]
        categories = {}
        for item in items:
            if not item:
                continue
            name = 'bed' if item['sourceDatasetId'] == '27912' else 'magnet'
            categories.setdefault(name, []).append(item['categories'][name])
        if set(categories) != {'bed', 'magnet'}:
            return None
        values = [category for group in categories.values() for category in group]
        if not all(value.get('state') in {'complete', 'no_target'} for value in values):
            return None
        if any(value.get('state') == 'complete' and not value.get('actualImages') for value in values):
            return None
        return 'complete' if any(value['state'] == 'complete' for value in values) else 'empty'

    @property
    def busy(self):
        return bool(self.task and not self.task.done())

    def schedule(self, wake):
        if self.busy or not self.slicing.store.settings.get('autoArchive', True):
            return
        for row in self.refresh():
            kind = self.eligible(row)
            if kind and row.get('archiveState') != 'attention' and (row.get('archiveState') != 'complete' or row.get('archiveKind', kind) != kind):
                row['archiveState'] = 'waiting'
                self.save('files', row)
        self.task = asyncio.create_task(self.tick())
        def finished(task):
            if not task.cancelled():
                # Retrieve errors, preserving the journal for a later retry.
                task.exception()
            wake()
        self.task.add_done_callback(finished)

    async def shutdown(self):
        if self.task:
            await asyncio.gather(self.task, return_exceptions=True)

    async def tick(self):
        async with self.lock:
            settings = self.slicing.store.settings
            if not settings.get('autoArchive', True):
                return
            rows = self.refresh()
            journals = {row['fileId']: row for row in self.rows('file_moves') if row['state'] not in {'complete', 'superseded'}}
            for row in rows:
                kind = self.eligible(row)
                journal = journals.get(row['id'])
                if not kind or row.get('archiveState') == 'attention':
                    continue
                if row.get('archiveState') == 'complete':
                    if row.get('archiveKind', kind) == kind:
                        continue
                    row['archiveState'] = 'waiting'
                if self.leases.busy(row['path']):
                    row.update(archiveState='waiting', archiveMessage='文件使用中，释放后归档。')
                    self.save('files', row)
                    continue
                try:
                    if not journal:
                        from .slicing import source_version
                        current = source_version(row['path'])
                        linked = [self.slicing.store.get('items', value) for value in row['links']['slicing']]
                        if any(item.get('fileVersion') != current for item in linked if item):
                            raise ValueError('文件版本已改变，原位保留。')
                        destination = str(self.destination(row['path'], kind, settings))
                        if destination == row['path']:
                            row.update(archiveState='complete', archivePath=destination, archiveKind=kind)
                            self.save('files', row)
                            continue
                        journal = {'id': uuid4().hex, 'fileId': row['id'], 'source': row['path'],
                                   'destination': destination, 'relativePath': Path(row['path']).name,
                                   'category': 'slicing', 'fileVersion': source_fingerprint(Path(row['path'])),
                                   'size': Path(row['path']).stat().st_size,
                                   'reservation': {'token': uuid4().hex}, 'state': 'planned'}
                        self.save('file_moves', journal)
                    await self.process(journal)
                    row.update(path=journal['destination'], archiveState='complete', archivePath=journal['destination'], archiveKind=kind, archiveMessage='已归档')
                except (OSError, ValueError) as error:
                    disconnected = isinstance(error, FileNotFoundError) or ('尚未挂载' in str(error))
                    row.update(archiveState='waiting' if disconnected else 'attention',
                               archiveMessage='等待原磁盘连接。' if disconnected else '归档未完成；原片保留，请检查重名、空间或路径。')
                except Exception as error:
                    row.update(archiveState='attention', archiveMessage='归档记录未同步完成，可重试核对。', failureType=type(error).__name__)
                self.save('files', row)

    async def process(self, journal):
        with self.leases.write(journal['source']), self.leases.write(journal['destination']):
            async def io(fn, *args):
                return await self.videos.file_operation(fn, *args)
            journal['state'] = 'moving'
            self.save('file_moves', journal)
            if await io(recover_published, journal):
                journal['destinationVersion'] = await io(source_fingerprint, Path(journal['destination']))
                self.save('file_moves', journal)
            else:
                source, destination = await io(prepare_paths, journal)
                if not journal.get('temporaryVersion'):
                    if await io(same_device, source, destination):
                        journal['destinationVersion'] = await io(move_direct, journal)
                    else:
                        journal['temporary'] = str(destination.parent / f".{destination.name}.{journal['reservation']['token']}.moving")
                        self.save('file_moves', journal)
                        journal.update(await io(copy_verified, journal))
                    self.save('file_moves', journal)
                if journal.get('temporaryVersion'):
                    journal['destinationVersion'] = await io(publish_copied, journal)
                    self.save('file_moves', journal)
            await io(finish_source, journal)
            # Fan out after publishing, replayable after interruption.
            self.moves.update_bindings(journal)
            journal.update(state='complete', completedAt=now_iso())
            self.save('file_moves', journal)

    async def retry(self, identity):
        row = next((row for row in self.rows('files') if row['id'] == identity), None)
        if not row:
            raise ValueError('文件记录不存在。')
        row.update(archiveState='waiting', archiveMessage='等待重试归档。')
        self.save('files', row)
        await self.tick()
        return self.state()

    async def reconcile(self):
        await self.slicing._recover_moved_sources()
        self.refresh()
        return self.state()
