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
        self.next_check = 0
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
        rows = self.rows('files')
        known = {}
        unified = bool(getattr(self, 'workspace', None) and self.workspace.enabled)
        for row in rows:
            if row.get('aliasOf'):
                continue
            if row['path'] not in known:
                known[row['path']] = row
            elif unified:
                row.update(aliasOf=known[row['path']]['id'], links={'slicing': [], 'uploads': [], 'collection': []})
                self.save('files', row)
        references = {}
        for item in self.slicing.store.list('items'):
            if item.get('path'):
                references.setdefault(item['path'], {'slicing': [], 'uploads': [], 'collection': []})['slicing'].append(item['id'])
        for item in self.uploads.store.list('items'):
            if item.get('path'):
                references.setdefault(item['path'], {'slicing': [], 'uploads': [], 'collection': []})['uploads'].append(item['id'])
        for item in self.videos.store.list(include_deleted=True):
            for version in item.get('versions', []):
                for path in {version.get('filePath'), version.get('recovery', {}).get('filePath') or version.get('recovery', {}).get('destination')} - {None, ''}:
                    references.setdefault(path, {'slicing': [], 'uploads': [], 'collection': []})['collection'].append(
                        {'id': item['id'], 'revision': version.get('revision')})
        for path, links in references.items():
            row = known.get(path) or {'id': uuid4().hex, 'path': path, 'archiveState': 'not_ready'}
            if row.get('links') != links or path not in known:
                row['links'] = links
                self.save('files', row)
        if unified:
            for row in rows:
                if row['path'] not in references and any(row.get('links', {}).values()):
                    # Keep journals/history, but old locations are not active
                    # controls for the new file after a verified move.
                    row.update(indexState='historical', links={'slicing': [], 'uploads': [], 'collection': []})
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
        if getattr(self, 'workspace', None) and self.workspace.enabled:
            known = {row['path']: row['id'] for row in self.rows('files') if not row.get('aliasOf')}
            public = []
            for asset in self.workspace.rows('assets'):
                state = ('complete' if asset.get('cleanup', {}).get('state') == 'complete' else
                         'preserved' if self.workspace.is_preserved(asset) else
                         'attention' if asset.get('status') == 'attention' else
                         'waiting' if asset.get('status') == 'waiting' else
                         'complete' if asset.get('stage') == '已切片' and
                         Path(asset['path']).is_relative_to(self.workspace.root) else 'waiting')
                public.append({'id': known.get(asset['path'], asset['id']), 'path': asset['path'],
                    'archiveState': state, 'archivePath': asset['path'], 'archiveMessage': asset.get('message', ''),
                    'cleanup': {k: asset.get('cleanup', {}).get(k) for k in ('state', 'message', 'completedAt')}})
            return {'items': public, 'counts': {s: sum(r['archiveState'] == s for r in public)
                                                for s in ('complete', 'waiting', 'attention', 'preserved')}}
        rows = self.rows('files')
        counts = {}
        public = []
        for row in rows:
            state = row.get('archiveState', 'not_ready')
            counts[state] = counts.get(state, 0) + 1
            value = dict(row)
            if value.get('cleanup'):
                value['cleanup'] = {key: value['cleanup'].get(key) for key in
                                    ('state', 'message', 'startedAt', 'completedAt')}
            public.append(value)
        return {'items': public, 'counts': counts}

    def destination(self, path, kind, settings):
        source = checked_path(path)
        roots = [checked_path(root) for root in settings['directories']]
        # Always classify against the configured source root, never against an
        # intermediate folder such as 已上传/车斗未上传. If roots overlap, use
        # the most specific configured root for deterministic relative paths.
        matches = [root for root in roots if source.is_relative_to(root)]
        if not matches:
            # A user may have moved a source outside the configured scan roots
            # through UploadAudit. Keep that binding usable without treating a
            # sibling classification folder as the new global root.
            last_plan = self.moves.latest() or {}
            plan_roots = [checked_path(last_plan[key]) for key in
                          ('uploadedDirectory', 'magnetMissingDirectory', 'bedMissingDirectory') if last_plan.get(key)]
            matches = [root for root in plan_roots if source.is_relative_to(root)]
        root = max(matches, key=lambda path: len(path.parts), default=None)
        if root is None:
            raise ValueError('文件不在视频或分类目录内，保留原位。')
        if kind == 'complete':
            return root / settings['archiveCompletedDirectory'] / source.name
        parts = list(source.relative_to(root).parts)
        # Flatten old classification wrappers while retaining real date/source
        # subdirectories. New videos never become nested under stale labels.
        wrappers = {'已上传', '磁盘吊未上传', '车斗未上传', '切片分类',
                    settings['archiveCompletedDirectory'], settings['archiveEmptyDirectory']}
        while parts and parts[0] in wrappers:
            parts.pop(0)
        if not parts:
            parts = [source.name]
        return root / settings['archiveEmptyDirectory'] / Path(*parts)

    def archive_count(self, state, item_id=None):
        if getattr(self, 'workspace', None) and self.workspace.enabled:
            return sum(asset.get('status') == state and not self.workspace.is_preserved(asset)
                       and asset.get('cleanup', {}).get('state') != 'complete'
                       and self.workspace.stage(asset) == '已切片'
                       and (not item_id or any(i['id'] == item_id for i in self.workspace.linked(asset)))
                       for asset in self.workspace.rows('assets'))
        return sum(row.get('archiveState') == state and bool(self.eligible(row))
                   and (not item_id or item_id in row.get('links', {}).get('slicing', []))
                   for row in self.rows('files'))

    def eligible(self, row):
        items = [self.slicing.store.get('items', identity) for identity in row.get('links', {}).get('slicing', [])]
        categories = {}
        for item in items:
            if not item:
                continue
            for name in ('bed', 'magnet'):
                category = item.get('categories', {}).get(name, {})
                if category.get('state') != 'not_applicable':
                    categories.setdefault(name, []).append(category)
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

    def schedule(self, wake, item_id=None, *, archive_only=False):
        if getattr(self, 'workspace', None) and self.workspace.maintenance:
            return
        if getattr(self, 'workspace', None) and self.workspace.enabled:
            self.workspace.wake.set()
            return
        if not self.slicing.store.settings.get('autoArchive', True):
            return
        candidates = []
        for row in self.refresh():
            if item_id and item_id not in row.get('links', {}).get('slicing', []):
                continue
            if archive_only and row.get('archiveState') != 'complete':
                continue
            kind = self.eligible(row)
            if not kind or row.get('archiveState') == 'attention':
                continue
            try:
                destination = str(self.destination(row['path'], kind, self.slicing.store.settings))
            except (OSError, ValueError):
                destination = ''
            if (not destination or row.get('archiveState') != 'complete'
                    or row.get('archiveKind', kind) != kind or row.get('path') != destination):
                row['archiveState'] = 'waiting'
                row['archiveMessage'] = '正在归入统一完成文件夹。' if archive_only else row.get('archiveMessage', '')
                self.save('files', row)
                candidates.append(row['id'])
        if not candidates or self.busy or self.slicing.clock() < self.next_check:
            return
        # A deferred move must not wake a tight loop that floods cloud polls.
        self.next_check = self.slicing.clock() + 30
        self.task = asyncio.create_task(self.tick(file_ids=set(candidates)))
        def finished(task):
            if not task.cancelled():
                task.exception()
            wake()
        self.task.add_done_callback(finished)

    async def shutdown(self):
        if self.task:
            await asyncio.gather(self.task, return_exceptions=True)

    async def tick(self, file_ids=None):
        if getattr(self, 'workspace', None) and self.workspace.enabled:
            await self.workspace.reconcile()
            return
        async with self.lock:
            settings = self.slicing.store.settings
            if not settings.get('autoArchive', True):
                return
            rows = self.refresh()
            journals = {row['fileId']: row for row in self.rows('file_moves') if row['state'] not in {'complete', 'superseded'}}
            for row in rows:
                if file_ids is not None and row['id'] not in file_ids:
                    continue
                kind = self.eligible(row)
                journal = journals.get(row['id'])
                if not kind or row.get('archiveState') == 'attention':
                    continue
                destination = str(self.destination(row['path'], kind, settings))
                if (row.get('archiveState') == 'complete' and row.get('archiveKind', kind) == kind
                        and row.get('path') == destination):
                    continue
                if row.get('archiveState') == 'complete':
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

    async def retry(self, identity, request_id=None):
        if getattr(self, 'workspace', None) and self.workspace.enabled:
            self.refresh()
            row = next((row for row in self.rows('files') if row['id'] == identity), None)
            asset = self.workspace.asset_for(row['path']) if row else self.workspace.get('assets', identity)
            if not asset:
                raise ValueError('文件与工作区记录未关联，请重新核对文件。')
            self.workspace.request_archive_retry(asset['id'], request_id or uuid4().hex)
            return self.state()
        row = next((row for row in self.rows('files') if row['id'] == identity), None)
        if not row:
            raise ValueError('文件记录不存在。')
        row.update(archiveState='waiting', archiveMessage='等待重试归档。')
        self.save('files', row)
        await self.tick(file_ids={identity})
        return self.state()

    async def cleanup_confirmed(self, identity, request_id):
        """Delete only verified source/output files recorded by this program."""
        if getattr(self, 'workspace', None) and self.workspace.enabled:
            row = next((r for r in self.rows('files') if r['id'] == identity), None)
            asset = self.workspace.asset_for(row['path']) if row else None
            if not asset:
                raise ValueError('请在工作区重新核对文件。')
            if asset.get('cleanup', {}).get('state') == 'complete':
                return self.state()
            eligible, reason = self.workspace.cleanup_eligibility(asset)
            if not eligible:
                raise ValueError(reason or '清理凭证尚不完整，文件继续保留。')
            await self.workspace.reconcile(only=asset['id'])
            current = self.workspace.get('assets', asset['id'])
            if current.get('cleanup', {}).get('state') != 'complete':
                raise ValueError(current.get('cleanup', {}).get('message') or current.get('message') or '文件仍在使用或等待平台核验，清理尚未执行。')
            return self.state()
        row = next((row for row in self.rows('files') if row['id'] == identity), None)
        if not row:
            raise ValueError('文件记录不存在。')
        cleanup = row.get('cleanup') or {}
        if cleanup.get('state') == 'complete':
            return self.state()

        linked = [self.slicing.store.get('items', item_id)
                  for item_id in row.get('links', {}).get('slicing', [])]
        linked = [item for item in linked if item]
        eligibility = self.eligible(row)
        if eligibility not in {'complete', 'empty'} or row.get('archiveState') != 'complete':
            raise ValueError('仅能清理已集中归档且切片结果核验完成的视频。')
        if any(item.get('state') not in {'complete', 'no_target'} for item in linked):
            raise ValueError('仍有未完成或需处理的类别，原片和照片继续保留。')
        item_ids = {item['id'] for item in linked}
        package_rows = [package for package in self.slicing.packages.store.list('items')
                        if package.get('itemId') in item_ids and package.get('state') != 'dismissed']
        if any(package.get('state') != 'complete' for package in package_rows):
            raise ValueError('存在尚未完成的平台图片包，暂不清理本地视频或照片。')
        for item in linked:
            category_name = 'bed' if item['sourceDatasetId'] == '27912' else 'magnet'
            category = item['categories'][category_name]
            if category.get('state') != 'complete':
                continue
            if not any(package.get('itemId') == item['id'] and package.get('category') == category_name
                       for package in package_rows):
                raise ValueError('已切片类别缺少已核验的图片包记录，原片和照片继续保留。')

        settings = self.slicing.store.settings
        roots = [checked_path(root) for root in settings.get('directories', [])]
        output = str(settings.get('sliceOutputDirectory') or '').strip()
        if output:
            roots.append(checked_path(output))
        manifest = cleanup.get('manifest') if cleanup.get('state') in {'running', 'attention'} else None
        if manifest is None:
            from .slicing import source_version

            original_path = row['path']
            expected_versions = {json.dumps(item.get('fileVersion'), sort_keys=True) for item in linked
                                 if item.get('path') == original_path and item.get('fileVersion')}
            if len(expected_versions) != 1:
                raise ValueError('原视频版本记录不唯一，未删除任何文件。')
            expected_original = json.loads(next(iter(expected_versions)))
            if source_version(original_path) != expected_original:
                raise ValueError('原视频版本与已核验记录不同，未删除任何文件。')

            entries = [{'path': original_path, 'kind': 'video', 'expected': expected_original,
                        'state': 'planned'}]
            for package in package_rows:
                archive = package.get('path')
                if archive and not package.get('fileVersion'):
                    raise ValueError('图片包缺少文件版本记录，未删除任何文件。')
                if archive:
                    entries.append({'path': archive, 'kind': 'archive',
                                    'expected': package['fileVersion'], 'state': 'planned'})
                image_dir = package.get('imageDir')
                versions = package.get('imageVersions') or {}
                for name in package.get('images') or []:
                    expected = versions.get(name)
                    if not image_dir or not expected:
                        raise ValueError('图片缺少目录或版本记录，未删除任何文件。')
                    entries.append({'path': str(Path(image_dir) / name), 'kind': 'image',
                                    'expected': expected, 'state': 'planned'})
            seen = {}
            for entry in entries:
                entry_path = checked_path(entry['path'])
                if not any(entry_path.is_relative_to(root) for root in roots):
                    raise ValueError('待清理文件不在配置的视频或切片目录内，未删除任何文件。')
                prior = seen.get(entry_path)
                if prior:
                    for key in ('device', 'inode', 'size', 'modifiedNs', 'edgeHash', 'sha256'):
                        first, second = prior.get('expected', {}).get(key), entry.get('expected', {}).get(key)
                        if first is not None and second is not None and first != second:
                            raise ValueError('多个结果对同一文件记录了不同版本，未删除任何文件。')
                else:
                    seen[entry_path] = entry
                if entry_path.exists():
                    if not entry_path.is_file() or entry_path.is_symlink():
                        raise ValueError('待清理路径不是普通文件，未删除任何文件。')
                    actual = source_fingerprint(entry_path)
                    if any(actual.get(key) != entry['expected'].get(key)
                           for key in ('device', 'inode', 'size', 'modifiedNs') if key in entry['expected']):
                        raise ValueError('待清理文件版本与核验记录不同，未删除任何文件。')
                else:
                    entry['state'] = 'missing'
            cleanup = {'requestId': request_id, 'state': 'running', 'manifest': entries,
                       'startedAt': now_iso(), 'message': '正在清理已核验的视频和程序生成照片。'}
            row['cleanup'] = cleanup
            self.save('files', row)
        else:
            cleanup.update(requestId=request_id, state='running', message='正在续接已核验的本地清理记录。')
            row['cleanup'] = cleanup
            self.save('files', row)

        try:
            for entry in cleanup.get('manifest', []):
                if entry.get('state') in {'deleted', 'missing'}:
                    continue
                path = checked_path(entry['path'])
                with self.leases.write(path):
                    if not path.exists():
                        entry['state'] = 'missing'
                    else:
                        if not path.is_file() or path.is_symlink():
                            raise ValueError('待清理路径不是普通文件，未删除其他用户文件。')
                        if entry.get('kind') == 'video':
                            from .slicing import source_version
                            actual = source_version(path)
                        else:
                            actual = source_fingerprint(path)
                        if any(actual.get(key) != entry['expected'].get(key)
                               for key in ('device', 'inode', 'size', 'modifiedNs') if key in entry['expected']):
                            raise ValueError('待清理文件已变化，剩余文件保留。')
                        path.unlink()
                        entry['state'] = 'deleted'
                        entry['deletedAt'] = now_iso()
                cleanup['manifest'] = cleanup.get('manifest', [])
                row['cleanup'] = cleanup
                self.save('files', row)
            cleanup.update(state='complete', completedAt=now_iso(), message='已清理核验完成的本地原视频和程序生成照片。')
            for item in linked:
                item.update(localCleanupState='complete', localCleanupAt=cleanup['completedAt'])
                self.slicing.store.save('items', item)
            for package in package_rows:
                package['localCleanupState'] = 'complete'
                package['localCleanupAt'] = cleanup['completedAt']
                self.slicing.packages._save(package)
        except (OSError, ValueError) as error:
            cleanup.update(state='attention', message=str(error) or '本地清理未完成；剩余文件保留。')
        row['cleanup'] = cleanup
        self.save('files', row)
        return self.state()

    async def reconcile(self):
        if getattr(self, 'workspace', None) and self.workspace.enabled:
            self.workspace.wake.set()
            return self.state()
        await self.slicing._recover_moved_sources()
        self.refresh()
        return self.state()
