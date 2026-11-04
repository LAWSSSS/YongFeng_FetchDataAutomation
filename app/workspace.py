"""Single workspace ownership, recoverable relocation and derived configuration.

SQLite stays in the application data directory. Only explicitly registered media
is moved; discovery never grants permission to delete an unknown file.
"""
import asyncio
import json
import os
import sqlite3
import time
import zipfile
from contextlib import ExitStack
from pathlib import Path
from uuid import uuid4

from .audit_moves import (checked_path, copy_verified, finish_source, move_direct,
                          prepare_paths, publish_copied, recover_published, same_device,
                          source_fingerprint)
from .domain import now_iso
from .file_leases import FileEnvironmentError, ReadProbes, filesystem_wait_code
from .videos import Conflict
from . import file_locations
from .workspace_scheduler import WorkspaceScheduler
from .workspace_files import (BRANCHES, OUTPUT_BRANCHES, VIDEO_BRANCHES, birth_ns,
                              choose_root, compatible, content_id, create_root,
                              file_version, folder_name, read_marker, scan_managed,
                              reconnected_version)


class WorkspaceManager(WorkspaceScheduler):
    def __init__(self, store, data_dir, videos, uploads, slicing, audit, moves, files, leases):
        self.db, self.data_dir = store.db, Path(data_dir)
        self.videos, self.uploads, self.slicing = videos, uploads, slicing
        self.audit, self.moves, self.files, self.leases = audit, moves, files, leases
        self.lock = asyncio.Lock()
        self.cleanup_registration_lock = asyncio.Lock()
        self.task = self.loop_task = None
        self.wake = asyncio.Event()
        self.file_checks = ReadProbes(self.wake.set, limit=1)
        self.stopping = False
        self.online = False
        self.availability = {'online': False, 'reasonCode': 'disk_offline',
                             'message': '等待检查工作区磁盘。', 'checkedAt': None}
        self.last_check = 0
        self.created_times = {}
        self.session_checked = set()
        self.maintenance = False
        self.activity = {'stage': 'idle', 'message': '等待整理。', 'currentName': ''}
        self.waiting_paths = set()
        self.event_loop = None
        self.cleanup_task = None
        self.cleanup_pending = set()
        self.cleanup_active = set()
        self.cleanup_deferred = set()
        self.cleanup_next = {}
        self.discovery_waiting = set()
        self.discovery_errors = {}
        for table in ('settings', 'assets', 'plans', 'moves', 'requests', 'retentions'):
            self.db.execute(f'CREATE TABLE IF NOT EXISTS simple_workspace_{table} (id TEXT PRIMARY KEY, payload TEXT NOT NULL)')
        self.db.execute("CREATE INDEX IF NOT EXISTS workspace_move_plan ON simple_workspace_moves(json_extract(payload, '$.planId'))")
        self.db.execute("CREATE INDEX IF NOT EXISTS workspace_move_state_asset ON simple_workspace_moves(json_extract(payload, '$.state'), json_extract(payload, '$.assetId'))")
        self.db.execute("CREATE INDEX IF NOT EXISTS workspace_slice_path ON simple_slicing_items(json_extract(payload, '$.path'))")
        self.db.execute("CREATE INDEX IF NOT EXISTS workspace_upload_path ON simple_upload_items(json_extract(payload, '$.path'))")
        self.db.execute("CREATE INDEX IF NOT EXISTS workspace_package_item ON simple_slice_package_items(json_extract(payload, '$.itemId'))")
        self.db.execute("CREATE INDEX IF NOT EXISTS workspace_asset_path ON simple_workspace_assets(json_extract(payload, '$.path'))")
        self.db.commit()
        # Existing completed journals remain the authority after an upgrade.
        if not self.db.execute('SELECT 1 FROM simple_file_locations LIMIT 1').fetchone():
            for entry in self.rows('moves'):
                if entry.get('state') == 'complete':
                    file_locations.remember(self.db, entry)
        self.config = self.get('settings', 'config') or {'id': 'config', 'revision': 0,
                         'workspaceId': uuid4().hex, 'enabled': False, 'root': '', 'parent': ''}
        self.save('settings', self.config)
        self.retained_sources = self.rows('retentions')
        self._restore_locations()
        for manager in (videos, uploads, slicing, slicing.packages, audit, files):
            manager.workspace = self
        self.leases.subscribe_moves(self.moved)
        self.leases.subscribe_release(self._released)
        self.created_times = {a['path']: a.get('originalCreatedNs') for a in self.rows('assets')}
        if self.enabled:
            self.derive_config()

    def _restore_locations(self):
        # A crash can occur after publication but before path callbacks. Apply
        # that exact receipt before discovery, preserving the original folder.
        for asset in self.rows('assets'):
            target, version = file_locations.resolve(self.db, asset['path'], asset.get('fileVersion'))
            if target != asset['path'] or version != asset.get('fileVersion'):
                previous_path = asset['path']
                if target != asset['path']:
                    asset.setdefault('pathHistory', []).append(asset['path'])
                asset.update(path=target, fileVersion=version)
                self.save('assets', asset)
                self.created_times.pop(previous_path, None)
                self.created_times[target] = asset.get('originalCreatedNs')
        for row in self.videos.store.list(include_deleted=True):
            before = json.dumps(row, sort_keys=True)
            file_locations.video(self.db, row)
            if json.dumps(row, sort_keys=True) != before:
                self.videos.store.save(row)
        for store in (self.uploads.store, self.slicing.store, self.slicing.packages.store):
            for row in store.list('items'):
                before = json.dumps(row, sort_keys=True)
                original_version = dict(row.get('fileVersion') or {})
                if store is self.slicing.packages.store:
                    file_locations.package(self.db, row)
                else:
                    file_locations.field(self.db, row, 'path', 'fileVersion')
                if json.dumps(row, sort_keys=True) != before:
                    if store is self.uploads.store:
                        row.setdefault('uploadFileVersion', original_version)
                    if store is self.slicing.store:
                        row.setdefault('observationVersion', original_version)
                        row.setdefault('submissionFileVersion', original_version)
                    store.save('items', row)

    def _released(self, path):
        if self.stopping:
            return
        if path not in self.waiting_paths:
            return
        self.waiting_paths.discard(path)
        if self.event_loop and not self.event_loop.is_closed():
            self.event_loop.call_soon_threadsafe(self.wake.set)
        else:
            self.wake.set()

    def rows(self, table):
        assert table in {'settings', 'assets', 'plans', 'moves', 'requests', 'retentions'}
        return [json.loads(r[0]) for r in self.db.execute(f'SELECT payload FROM simple_workspace_{table} ORDER BY rowid')]

    def get(self, table, identity):
        assert table in {'settings', 'assets', 'plans', 'moves', 'requests', 'retentions'}
        row = self.db.execute(f'SELECT payload FROM simple_workspace_{table} WHERE id=?', (identity,)).fetchone()
        return json.loads(row[0]) if row else None

    def save(self, table, value):
        assert table in {'settings', 'assets', 'plans', 'moves', 'requests', 'retentions'}
        value['updatedAt'] = now_iso()
        self.db.execute(f'INSERT INTO simple_workspace_{table} VALUES(?,?) ON CONFLICT(id) DO UPDATE SET payload=excluded.payload',
                        (value['id'], json.dumps(value, ensure_ascii=False)))
        self.db.commit()

    @property
    def enabled(self):
        return bool(self.config.get('enabled'))

    @property
    def root(self):
        return Path(self.config['root'])

    @property
    def blocked(self):
        return self.maintenance or (self.enabled and not self.online)

    def require_root(self):
        if not self.enabled:
            raise Conflict('请先预览并启用视频工作区。')
        try:
            root = self._inspect_root(self.root, self.config['workspaceId'])
        except FileEnvironmentError as error:
            self._availability(False, error.code, str(error))
            raise
        except ValueError:
            self._availability(False, 'workspace_identity', '工作区身份不符，文件保留待核对。')
            raise
        self._availability(True)
        return root

    @staticmethod
    def _inspect_root(root, workspace_id):
        marker = read_marker(root)
        if not marker:
            raise FileEnvironmentError('工作区不可用，请连接原磁盘；任务和记录已保留。', 'disk_offline')
        if marker['id'] != workspace_id:
            raise ValueError('工作区身份不符，不能使用另一工作区的文件。')
        return checked_path(root)

    def _availability(self, online, code='', message=''):
        self.online = online
        self.availability = {'online': online, 'reasonCode': code, 'message': message, 'checkedAt': now_iso()}

    async def check_root(self):
        """Background availability probe; GETs use the stored snapshot only."""
        if not self.enabled:
            raise Conflict('请先预览并启用视频工作区。')
        root, identity = self.root, self.config['workspaceId']
        try:
            result = await self.file_checks.run((str(root), identity), self._inspect_root, root, identity)
        except FileEnvironmentError as error:
            if root != self.root or identity != self.config['workspaceId']:
                raise Conflict('工作区配置已变化，稍后重新检查。') from None
            self._availability(False, error.code, str(error))
            raise
        except ValueError:
            if root != self.root or identity != self.config['workspaceId']:
                raise Conflict('工作区配置已变化，稍后重新检查。') from None
            self._availability(False, 'workspace_identity', '工作区身份不符，文件保留待核对。')
            raise
        if root != self.root or identity != self.config['workspaceId']:
            raise Conflict('工作区配置已变化，稍后重新检查。')
        self._availability(True)
        import shutil
        self.free_bytes = (await asyncio.to_thread(shutil.disk_usage, result)).free
        return result

    def require_directory(self, value, *, download=False):
        if not self.enabled:
            return value
        expected = self.root / '采集视频' if download else self.root
        if str(Path(value).expanduser()) != str(expected):
            raise Conflict('目录已由视频工作区统一管理，请在“文件工作区”修改位置。')
        return str(expected)

    def derive_config(self):
        """No filesystem creation here: unplugged history remains readable."""
        root = self.root
        self.videos.store.set_directory(str(root / '采集视频'))
        self.videos.fixed_directory = root / '采集视频'
        self.videos.work = root / '.缓存' / '下载'
        self.videos.legacy_work = None
        self.uploads.store.settings['directory'] = str(root)
        self.uploads.store.save_settings()
        self.slicing.store.settings.update(directories=[str(root / b) for b in VIDEO_BRANCHES],
                                          sliceOutputDirectory=str(root / '.处理中'))
        self.slicing.store.save_settings()

    def state(self):
        assets = self.rows('assets')
        # Build the media inventory once. Re-reading large historical submit
        # snapshots for every branch and every file can monopolize the API loop.
        inventory_assets = [asset for asset in assets if asset.get('cleanup', {}).get('state') != 'complete'
                            and (not self.is_preserved(asset) or (self.enabled and Path(asset['path']).is_relative_to(self.root)))]
        links, packages = self.photo_inventory(inventory_assets)
        for asset in assets:
            packages.setdefault(asset['id'], [])
        inventory = [entry for asset in inventory_assets
                     for entry in self.media_entries(asset, existing_only=False, cached_packages=packages[asset['id']])]
        # Each photo belongs to at most one top-level branch. Classify it once
        # instead of walking its parents for every branch on every poll.
        branch_paths = {name: {} for name in BRANCHES}
        if self.enabled:
            root_parts = self.root.parts
            depth = len(root_parts)
            for entry in inventory:
                parts = Path(entry['path']).parts
                if len(parts) > depth and parts[:depth] == root_parts and parts[depth] in branch_paths:
                    branch_paths[parts[depth]][entry['path']] = entry.get('expected', {}).get('size', 0)
        branches = [{'name': name, 'path': str(self.root / name) if self.enabled else '',
                     'files': len(paths), 'bytes': sum(paths.values())}
                    for name, paths in branch_paths.items()]
        plans = self.rows('plans')
        plan = plans[-1] if plans else None
        last_action = next((r for r in reversed(self.rows('requests')) if r.get('kind') == 'reconcile'), None)
        if last_action and last_action.get('canonicalId'):
            last_action = self.get('requests', last_action['canonicalId']) or last_action
        public_assets = [{k: a.get(k) for k in ('id', 'name', 'path', 'folder', 'stage', 'status', 'message', 'retention',
                            'originalCreatedNs', 'categories', 'organization')} | {'cleanup': {k: (a.get('cleanup') or {}).get(k)
                            for k in ('state', 'message', 'completedAt')}} for a in assets]
        from .workspace_progress import snapshot
        return {'enabled': self.enabled, 'revision': self.config['revision'], 'parent': self.config.get('parent', ''),
                'fixedLocation': bool(self.config.get('fixedLocation')), 'freeBytes': getattr(self, 'free_bytes', None),
                'root': self.config.get('root', ''), 'online': self.online, 'maintenance': self.maintenance,
                'availability': dict(self.availability),
                'branches': branches, 'assets': public_assets, 'totalBytes': sum(b['bytes'] for b in branches),
                'migration': self.public_plan(plan), 'lastCheck': self.last_check,
                'lastAction': {k: last_action.get(k) for k in ('id', 'state', 'stage', 'message', 'total', 'finished', 'waiting', 'failed', 'movedFiles', 'currentName', 'updatedAt',
                    'pipelineEnabled', 'pipelineStage', 'pipelineState', 'pipelineMessage', 'pipelineUploadDone', 'pipelineUploadTotal',
                    'pipelineSliceDone', 'pipelineSliceTotal', 'pipelineBlockers')} if last_action else None,
                'activity': dict(self.activity), 'cleanupCompleted': self.cleanup_summary(assets, links, packages),
                'progress': snapshot(self, last_action, assets,
                    slices=[item for group in links.values() for item in group],
                    packages=[item for group in packages.values() for item in group]),
                'preservedUploadedAt': self.config.get('preservedUploadedAt'),
                'counts': {s: sum(a.get('status') == s for a in assets) for s in ('managed', 'waiting', 'attention', 'cleaned', 'preserved')}}

    def is_preserved(self, asset):
        if asset.get('retention', {}).get('preserve'):
            return True
        return any(source['path'] == asset['path'] and
                   compatible(asset.get('fileVersion') or {}, source['fileVersion'])
                   for source in self.retained_sources)

    async def preserve_uploaded(self, revision, request_id):
        """Freeze uploaded history, never future completions, without changing queues.

        A bound platform source ID also proves prior upload for this retention-only
        rule. It is deliberately not sufficient authority for deletion.
        """
        async with self.lock:
            prior = self.get('requests', request_id)
            if prior:
                if prior.get('kind') != 'preserve-uploaded':
                    raise Conflict('请求编号已用于其他操作。')
                return self.state()
            if revision != self.config['revision']:
                raise Conflict('工作区已更新，请刷新后重试。')
            if self.maintenance:
                raise Conflict('请等待当前整理结束后保存保留规则。')
            backup = self.backup('preserve-' + request_id)
            snapshots = {}
            uploaded = [i for i in self.uploads.store.list('items') if i.get('state') == 'complete']
            platform_sources = [i for i in self.slicing.store.list('items')
                                if i.get('sourceVideoId') and str(i.get('sourceDatasetId')) in {'27912', '27944'}]
            for item in [*uploaded, *platform_sources]:
                if not item.get('path') or not item.get('fileVersion') or item.get('localCleanupState') == 'complete':
                    continue
                key = (item['path'], json.dumps(item['fileVersion'], sort_keys=True))
                snapshots.setdefault(key, {'id': uuid4().hex, 'path': item['path'], 'fileVersion': item['fileVersion']})
            sources = [*self.retained_sources, *snapshots.values()]
            at = now_iso()
            config = {**self.config, 'revision': revision + 1, 'preservedUploadedAt': at}
            assets = self.rows('assets')
            for asset in assets:
                if asset.get('cleanup', {}).get('state') == 'complete':
                    continue
                if any(s['path'] == asset['path'] and compatible(asset['fileVersion'], s['fileVersion']) for s in sources):
                    asset.update(retention={'preserve': True, 'since': at, 'reason': '已上传历史视频，保留原位。'},
                                 status='preserved', message='已上传历史视频，跳过整理和本地清理。')
            # Old previews must not regain authority after a policy change.
            plans = [p for p in self.rows('plans') if p['state'] == 'preview']
            for plan in plans:
                plan.update(state='superseded', message='保留规则已更新，请重新预览。')
            values = [('retentions', list(snapshots.values())), ('assets', assets), ('plans', plans),
                      ('settings', [config]), ('requests', [{'id': request_id, 'kind': 'preserve-uploaded', 'backup': backup}])]
            with self.db:
                for table, rows in values:
                    self.db.executemany(f'INSERT INTO simple_workspace_{table} VALUES(?,?) ON CONFLICT(id) DO UPDATE SET payload=excluded.payload',
                                        [(r['id'], json.dumps(r, ensure_ascii=False)) for r in rows])
            self.config, self.retained_sources = config, sources
            self.wake.set()
            return self.state()

    def public_plan(self, plan):
        if not plan:
            return None
        return {k: plan.get(k) for k in ('id', 'root', 'parent', 'revision', 'state', 'message', 'createdAt',
                                         'backup', 'completedAt', 'counts', 'bytes', 'totalItems', 'preserved')} | {
            'items': [{k: i.get(k) for k in ('id', 'source', 'destination', 'state', 'message', 'size')}
                      for i in self.plan_items(plan, limit=100)]}

    def plan_items(self, plan, limit=None):
        query = "SELECT payload FROM simple_workspace_moves WHERE json_extract(payload, '$.planId')=? ORDER BY rowid"
        params = [plan['id']]
        if limit:
            query += ' LIMIT ?'
            params.append(limit)
        return [json.loads(r[0]) for r in self.db.execute(query, params)]

    def references(self):
        """Snapshot program bindings on the event loop, never query SQLite in IO workers."""
        refs = {}
        from .upload_audit import VIDEO_EXTENSIONS
        deployed = [Path(self.slicing.store.settings[k]) for k in ('bedDeployDirectory', 'magnetDeployDirectory')
                    if self.slicing.store.settings.get(k)]
        def add(path, kind, identity, version=None, **extra):
            if path and Path(path).suffix.lower() in VIDEO_EXTENSIONS and not any(Path(path).is_relative_to(p) for p in deployed):
                refs.setdefault(path, []).append({'kind': kind, 'id': identity, 'version': version, **extra})
        for row in self.videos.store.list(include_deleted=True):
            for v in row.get('versions', []):
                if not v.get('localCleanupState'):
                    expected = v.get('fileVersion') or ({'size': v['totalBytes']} if v.get('totalBytes') is not None else None)
                    add(v.get('filePath'), 'collection', row['id'], expected, revision=v.get('revision'))
        for row in self.uploads.store.list('items'):
            if not row.get('localCleanupState'):
                add(row.get('path'), 'upload', row['id'], row.get('fileVersion'))
        for row in self.slicing.store.list('items'):
            if not row.get('localCleanupState'):
                add(row.get('path'), 'slicing', row['id'], row.get('fileVersion'))
        return refs

    async def discover(self, only=None):
        refs = self.references()
        known = {a['path']: a for a in self.rows('assets')}
        selected = self.get('assets', only) if only else None
        if only:
            refs = {key: value for key, value in refs.items() if selected and key == selected['path']}
        preserved_paths = {p for p, a in known.items() if self.is_preserved(a)}
        if self.enabled and not self.online:
            await self.check_root()
        owned_root = self.enabled and self.online
        self.discovery_waiting.clear()
        environment_errors = {}
        def inspect():
            inspected = []
            for raw, links in refs.items():
                if raw in preserved_paths or (raw in known and known[raw].get('cleanup', {}).get('state') == 'complete'):
                    continue
                if self.leases.busy(raw):
                    inspected.append((raw, None, '', '文件正在使用，释放后继续整理。'))
                    continue
                try:
                    saved = (known.get(raw) or {}).get('fileVersion')
                    observed = file_version(raw, edge=False)
                    # Routine inventory is metadata-only for a known immutable
                    # version. Copy/delete and first processing still verify
                    # the content fingerprint under their own file lease.
                    version = saved if (saved and saved.get('edgeHash') and compatible(saved, observed)) else file_version(raw, edge=True)
                    reconnect = (owned_root and Path(raw).is_relative_to(self.root)
                                 and reconnected_version(version, saved))
                    # Contradictory bindings may not be silently replaced with today's bytes.
                    if ((saved and not compatible(version, saved) and not reconnect)
                            or any(link.get('version') and not compatible(version, link['version'])
                                   and not (reconnect and compatible(saved, link['version'])) for link in links)):
                        inspected.append((raw, None, '', '文件版本与绑定记录不同，原位保留。'))
                    else:
                        inspected.append((raw, version, (known.get(raw) or {}).get('originalCreatedNs') or birth_ns(raw), ''))
                except (OSError, ValueError) as error:
                    if code := filesystem_wait_code(error):
                        environment_errors[raw] = code
                    previous = known.get(raw) or {}
                    acknowledged = any(e.get('path') == raw and e.get('state') in {'deleting', 'deleted'}
                        and compatible(previous.get('fileVersion') or {}, e.get('expected'))
                        for e in previous.get('cleanup', {}).get('manifest', []))
                    missing = False
                    if acknowledged:
                        try:
                            checked_path(raw).lstat()
                        except FileNotFoundError:
                            missing = True
                        except (OSError, ValueError):
                            pass
                    inspected.append((raw, None, '', '' if missing else
                                      '文件当前不可读；保留已有记录，等待原磁盘或人工核对。'))
            return inspected
        inspected = await asyncio.to_thread(inspect)
        busy_message = '文件正在使用，释放后继续整理。'
        self.discovery_environment = environment_errors
        self.discovery_errors = {path:error for path, _version, _created, error in inspected
                                 if error and error != busy_message and path not in environment_errors}
        reconnected = False
        for path, version, created, error in inspected:
            before = known.get(path)
            row = self.get('assets', before['id']) if before else None
            # Disk inspection runs in another thread. A move or a new retry
            # may have updated this asset while that inspection was running.
            if before and (not row or row.get('path') != path or row.get('fileVersion') != before.get('fileVersion')):
                continue
            if row and version and reconnected_version(version, row.get('fileVersion')):
                file_locations.remember(self.db, {'source':path, 'destination':path,
                    'expected':row['fileVersion'], 'destinationVersion':version})
                row['fileVersion'] = version
                reconnected = True
            if error == '文件正在使用，释放后继续整理。':
                self.waiting_paths.add(str(path))
                if not row:
                    self.discovery_waiting.add(str(path))
            if not row:
                # Missing history is retained in its existing module, never recreated.
                if not version:
                    continue
                identity = content_id(Path(path).name, version)
                existing = self.get('assets', identity)
                if existing:
                    target, observed = file_locations.resolve(self.db, existing['path'], existing.get('fileVersion'))
                    if target == path and compatible(observed, version):
                        row = existing
                        row.update(path=path, fileVersion=version)
                    else:
                        import hashlib
                        identity = hashlib.sha256((identity + path).encode()).hexdigest()[:24]
                used = {a['folder'] for a in self.rows('assets')}
                folder = folder_name(Path(path).name)
                if folder in used:
                    folder += '_' + identity[:8]
                row = row or {'id': identity, 'path': path, 'name': Path(path).name, 'folder': folder,
                       'fileVersion': version, 'originalFileVersion': version, 'originalCreatedNs': created,
                       'status': 'managed', 'createdAt': now_iso()}
            if row.get('organization', {}).get('state') == 'attention' and row.get('organization', {}).get('message') == busy_message:
                row['organization'] = {'state': 'waiting', 'message': busy_message}
            if error:
                deletion = row.get('cleanup', {}).get('manifest', [])
                recovering = any(e.get('kind') == 'video' and e.get('state') in {'deleting', 'deleted'} for e in deletion)
                if not recovering:
                    row.update(status='attention', message=error)
            else:
                row['links'] = refs[path]
                if not row.get('originalCreatedNs') and created:
                    row['originalCreatedNs'] = created
                if row.get('organization', {}).get('state') != 'attention' and row.get('archiveRetry', {}).get('state') != 'queued':
                    row.update(status='managed', message='')
            if error == '文件正在使用，释放后继续整理。':
                row.update(status='waiting', message=error)
                self.waiting_paths.add(str(path))
            elif path in environment_errors:
                row.update(status='waiting', message='磁盘暂不可用，恢复后自动继续整理。',
                           organization={'state': 'waiting', 'reasonCode': environment_errors[path],
                                         'message': '磁盘暂不可用，恢复后自动继续整理。'})
            if self.is_preserved(row):
                row.update(retention={'preserve': True, 'since': self.config.get('preservedUploadedAt'),
                                      'reason': '已上传历史视频，保留原位。'},
                           status='preserved', message='已上传历史视频，跳过整理和本地清理。')
            self.save('assets', row)
        if reconnected:
            self._restore_locations()
        self.created_times = {a['path']: a.get('originalCreatedNs') for a in self.rows('assets')}
        return self.rows('assets')

    def linked(self, asset):
        rows = self.db.execute("SELECT payload FROM simple_slicing_items WHERE json_extract(payload, '$.path')=?", (asset['path'],))
        return [i for (payload,) in rows if compatible(asset['fileVersion'], (i := json.loads(payload)).get('fileVersion'))]

    def outcomes(self, asset):
        groups = {'bed': [], 'magnet': []}
        for item in self.effective_links(asset):
            if item.get('superseded'):
                continue
            for name in groups:
                category = item.get('categories', {}).get(name, {})
                state = category.get('state')
                if not state or state == 'not_applicable':
                    continue
                irreversible = (item.get('partialRecovery') and
                                (category.get('errorCode') or item.get('errorCode')) in
                                {'no_decodable_samples', 'no_cuttable_fragments'})
                groups[name].append('uncuttable' if irreversible else state)
        return {k: (v[0] if v and len(set(v)) == 1 else 'pending') for k, v in groups.items()}

    def photo_results_verified(self, asset):
        from .photo_completion import photo_results_verified
        linked = self.effective_links(asset)
        return photo_results_verified(linked, self.packages(asset, linked))

    def effective_links(self, asset):
        linked = self.linked(asset)
        control = getattr(self.slicing, 'control', None)
        # Asset saves invalidate the global projection. Scope the same proof to
        # these exact sources rather than reparsing every historical photo pack.
        return control.scoped_effective_items(linked) if control else [item for item in linked if not item.get('superseded')]

    def photo_inventory(self, assets, *, effective=False):
        """Read exact file bindings and photo receipts once for a status view."""
        paths = tuple({asset['path'] for asset in assets})
        if not paths:
            return {}, {}
        placeholders = ','.join('?' for _ in paths)
        linked = [json.loads(row[0]) for row in self.db.execute(
            f"SELECT payload FROM simple_slicing_items WHERE json_extract(payload, '$.path') IN ({placeholders})", paths)]
        control = getattr(self.slicing, 'control', None)
        if control and effective:
            linked = control.scoped_effective_items(linked)
        ids = tuple({item['id'] for item in linked})
        packages = []
        if ids:
            placeholders = ','.join('?' for _ in ids)
            packages = [json.loads(row[0]) for row in self.db.execute(
                f"SELECT payload FROM simple_slice_package_items WHERE json_extract(payload, '$.itemId') IN ({placeholders})", ids)]
        by_path, by_item = {}, {}
        for item in linked:
            by_path.setdefault(item['path'], []).append(item)
        for package in packages:
            by_item.setdefault(package.get('itemId'), []).append(package)
        links_by_asset, packages_by_asset = {}, {}
        for asset in assets:
            items = [item for item in by_path.get(asset['path'], [])
                     if compatible(asset['fileVersion'], item.get('fileVersion'))]
            links_by_asset[asset['id']] = items
            packages_by_asset[asset['id']] = [package for item in items for package in by_item.get(item['id'], [])]
        return links_by_asset, packages_by_asset

    def stages(self, assets):
        """Classify one consistent inventory without reloading packs per video."""
        links, packages = self.photo_inventory(assets, effective=True)
        return {asset['id']: self.stage(asset, linked=links[asset['id']], packages=packages[asset['id']])
                for asset in assets}

    def stage(self, asset, *, linked=None, packages=None):
        from .photo_completion import category_verified, photo_results_verified
        linked = [item for item in (self.effective_links(asset) if linked is None else linked)
                  if not item.get('superseded')]
        if not linked:
            return '采集视频'
        packages = self.packages(asset, linked) if packages is None else packages
        done = {}
        for name in ('bed', 'magnet'):
            items = [item for item in linked if (item.get('categories', {}).get(name) or {}).get('state') not in (None, 'not_applicable')]
            done[name] = bool(items) and all(category_verified(item, name, packages, linked) for item in items)
        if done['bed'] and done['magnet'] and photo_results_verified(linked, packages):
            return '已切片'
        if done['bed'] and not done['magnet']:
            return '磁盘吊未切片'
        if done['magnet'] and not done['bed']:
            return '车斗未切片'
        return '未切片'

    def packages(self, asset, linked=None):
        ids = {i['id'] for i in (self.linked(asset) if linked is None else linked)}
        if not ids:
            return []
        placeholders = ','.join('?' for _ in ids)
        rows = self.db.execute(f"SELECT payload FROM simple_slice_package_items WHERE json_extract(payload, '$.itemId') IN ({placeholders})", tuple(ids))
        return [json.loads(payload) for (payload,) in rows]

    def media_entries(self, asset, existing_only=True, cached_packages=None):
        entries = [{'path': asset['path'], 'expected': asset['fileVersion'], 'kind': 'video'}]
        for p in (self.packages(asset) if cached_packages is None else cached_packages):
            if p.get('localCleanupState') == 'complete' or p.get('cleanupState') == 'complete':
                continue
            if p.get('path') and p.get('fileVersion'):
                entries.append({'path': p['path'], 'expected': p['fileVersion'], 'kind': 'archive', 'packageId': p['id']})
            for name in p.get('images', []):
                if Path(name).name == name and p.get('imageDir') and (p.get('imageVersions') or {}).get(name):
                    entries.append({'path': p.get('imagePaths', {}).get(name, str(Path(p['imageDir']) / name)), 'expected': p['imageVersions'][name],
                                    'kind': 'image', 'packageId': p['id']})
        return entries

    def move_entry(self, source, dest, expected, asset_id, kind):
        return {'id': uuid4().hex, 'assetId': asset_id, 'kind': kind, 'source': str(source), 'destination': str(dest),
                'fileVersion': {k: v for k, v in expected.items() if k != 'edgeHash'}, 'expected': expected,
                'size': expected['size'], 'relativePath': Path(source).name,
                'reservation': {'token': uuid4().hex}, 'state': 'planned'}

    def plan_assets(self, root, only=None, checkpoints=True):
        entries = []
        assets = [self.get('assets', only)] if only is not None else self.rows('assets')
        for asset in assets:
            if not asset:
                continue
            if self.is_preserved(asset):
                continue
            if asset.get('cleanup', {}).get('state') == 'complete' or asset.get('status') == 'attention':
                continue
            stage = self.stage(asset)
            photos_ready = self.photo_results_verified(asset)
            packages = self.packages(asset)
            packages_by_id = {package['id']: package for package in packages}
            for media in self.media_entries(asset, cached_packages=packages):
                if media['kind'] == 'video':
                    destination = root / stage / asset['folder'] / asset['name']
                else:
                    package = packages_by_id[media['packageId']]
                    branch = OUTPUT_BRANCHES[package['category']] if photos_ready else '.处理中'
                    base = root / branch / asset['folder']
                    if branch == '.处理中':
                        base /= package['category']
                    destination = base / ('压缩包.zip' if media['kind'] == 'archive' else '图片/' + Path(media['path']).name)
                if not photos_ready and media['kind'] != 'video':
                    try:
                        if Path(media['path']).relative_to(root).parts[0] in OUTPUT_BRANCHES.values():
                            continue
                    except (ValueError, IndexError):
                        pass
                if not photos_ready and media['kind'] == 'video':
                    try:
                        if Path(media['path']).relative_to(root).parts[0] == '已切片':
                            continue
                    except (ValueError, IndexError):
                        pass
                if str(destination) != media['path']:
                    entry = self.move_entry(media['path'], destination, media['expected'], asset['id'], media['kind'])
                    if media.get('packageId'):
                        entry['packageId'] = media['packageId']
                    entries.append(entry)
        # Only registered version-specific download checkpoints, never arbitrary caches.
        old_work = self.videos.work
        if checkpoints and old_work != root / '.缓存' / '下载':
            for row in self.videos.store.list(include_deleted=True):
                for v in row.get('versions', []):
                    part = old_work / row['id'] / f"v{v['revision']}.part"
                    try:
                        expected = source_fingerprint(part)
                    except (OSError, ValueError):
                        continue
                    entries.append(self.move_entry(part, root / '.缓存' / '下载' / row['id'] / part.name,
                                                   expected, '', 'checkpoint'))
        return entries

    async def preview(self, parent, revision, request_id):
        async with self.lock:
            old = self.get('requests', request_id)
            if old:
                if old.get('kind') != 'preview' or old.get('parent') != parent:
                    raise Conflict('请求编号已用于其他操作。')
                return self.public_plan(self.get('plans', old['planId']))
            if revision != self.config['revision']:
                raise Conflict('工作区已被其他页面修改，请刷新后重试。')
            if self.maintenance:
                raise Conflict('工作区正在整理，请等待完成。')
            root = await asyncio.to_thread(choose_root, parent, self.config['workspaceId'], self.config.get('root'))
            await self.discover()
            for asset in self.rows('assets'):
                if self.is_preserved(asset) or asset.get('cleanup', {}).get('state') == 'complete':
                    continue
                try:
                    await self.refresh_archive_photos(asset)
                except (OSError, ValueError, zipfile.BadZipFile):
                    asset.update(status='attention', message='照片与已核验压缩包暂不能确认一致，原位保留。')
                    self.save('assets', asset)
            plan = {'id': uuid4().hex, 'root': str(root), 'parent': str(root.parent), 'revision': revision,
                    'state': 'preview', 'createdAt': now_iso(),
                    'message': '仅整理程序已绑定的文件；未知文件保持原位。'}
            entries = self.plan_assets(root)
            def validate():
                for entry in entries:
                    try:
                        checked_path(entry['source'])
                        actual = file_version(entry['source'], 'edgeHash' in entry['expected'])
                        if not compatible(actual, entry['expected']):
                            raise ValueError('changed')
                    except (OSError, ValueError):
                        entry.update(state='attention', message='文件不可读或版本不符，保留原记录。')
            await asyncio.to_thread(validate)
            for entry in entries:
                entry['planId'] = plan['id']
            plan['preserved'] = sum(self.is_preserved(a) and a.get('cleanup', {}).get('state') != 'complete'
                                    for a in self.rows('assets'))
            self.db.executemany('INSERT INTO simple_workspace_moves VALUES(?,?)',
                                [(e['id'], json.dumps(e, ensure_ascii=False)) for e in entries])
            self.db.commit()
            self.update_counts(plan, entries)
            self.save('plans', plan)
            self.save('requests', {'id': request_id, 'kind': 'preview', 'parent': parent, 'planId': plan['id']})
            return self.public_plan(plan)

    @staticmethod
    def update_counts(plan, entries):
        plan['counts'] = {s: sum(i['state'] == s for i in entries)
                          for s in ('planned', 'moving', 'complete', 'attention', 'waiting')}
        plan['bytes'] = sum(i.get('size', 0) for i in entries)
        plan['totalItems'] = len(entries)

    async def apply(self, identity, revision, request_id, pilot=False):
        async with self.lock:
            prior = self.get('requests', request_id)
            if prior:
                if prior.get('kind') != 'apply' or prior.get('planId') != identity or prior.get('pilot') != pilot:
                    raise Conflict('请求编号已用于其他操作。')
                return self.state()
            if self.maintenance:
                raise Conflict('整理正在进行，请等待完成。')
            plan = self.get('plans', identity)
            if not plan:
                raise ValueError('整理预览不存在，请重新预览。')
            if revision != self.config['revision'] or plan['revision'] != revision:
                raise Conflict('工作区配置已更新，请重新预览。')
            if plan['state'] != 'preview':
                raise Conflict('此预览已执行，请重试整理或重新预览。')
            plan.update(state='waiting', pilot=pilot, message='等待本地写入和文件使用结束。')
            self.save('plans', plan)
            self.save('requests', {'id': request_id, 'kind': 'apply', 'planId': identity, 'pilot': pilot})
            self.maintenance = True
            self.task = asyncio.create_task(self.run_plan(identity))
            return self.state()

    async def cancel_preview(self, identity, revision, request_id):
        async with self.lock:
            prior = self.get('requests', request_id)
            if prior:
                if prior.get('kind') != 'cancel-preview' or prior.get('planId') != identity:
                    raise Conflict('请求编号已用于其他操作。')
                return self.state()
            if revision != self.config['revision']:
                raise Conflict('工作区已更新，请刷新后重试。')
            plan = self.get('plans', identity)
            if not plan or plan['state'] != 'preview':
                raise Conflict('此预览已更新，请刷新后重试。')
            plan.update(state='cancelled', message='预览已取消，文件未移动。')
            self.save('plans', plan)
            self.save('requests', {'id': request_id, 'kind': 'cancel-preview', 'planId': identity})
            return self.state()

    def local_busy(self):
        return bool(self.videos.running or self.uploads.running or self.slicing._active_analysis_tasks()
                    or self.slicing.exporting or self.slicing.packages.running or self.leases.readers or self.leases.writers
                    or self.audit.busy or self.moves.busy or self.uploads.busy or
                    self.files.busy or self.videos.jobs.busy or any(l.locked() for l in self.videos.row_locks.values())
                    or self.uploads.tick_lock.locked() or self.slicing.tick_lock.locked()
                    or self.slicing.packages.tick_lock.locked()
                    or (self.slicing.scan_task and not self.slicing.scan_task.done()))

    def backup(self, identity):
        directory = self.data_dir / 'backups'
        directory.mkdir(parents=True, exist_ok=True)
        path = directory / f'workspace-{identity}.sqlite3'
        if not path.exists():
            with sqlite3.connect(path) as target:
                self.db.backup(target)
        return str(path)

    async def run_plan(self, identity):
        try:
            while self.local_busy():
                await asyncio.sleep(1)
            async with self.lock:
                plan = self.get('plans', identity)
                if not plan.get('backup'):
                    plan['backup'] = self.backup(identity)
                    self.save('plans', plan)
                await asyncio.to_thread(create_root, plan['root'], self.config['workspaceId'])
                plan.update(state='moving', reasonCode='', message='正在按文件清单整理，任务进度保留。')
                self.save('plans', plan)
                selected = plan.get('pilotAssetId')
                if plan.get('pilot') and not selected:
                    selected = next((a['id'] for a in self.rows('assets') if self.cleanup_eligibility(a)[0]), None)
                    if not selected:
                        raise ValueError('没有凭证完整的本地原片可用于首条验证；其余文件尚未移动。')
                    plan['pilotAssetId'] = selected
                    self.save('plans', plan)
                entries = self.plan_items(plan)
                previous_moves = {(j['source'], j['destination']): j for j in self.rows('moves')
                                  if j.get('planId') != plan['id'] and j['state'] in {'moving', 'attention'}}
                updated = time.monotonic()
                for index, entry in enumerate(entries):
                    if plan.get('pilot') and entry.get('assetId') != selected:
                        continue
                    if entry['state'] in {'complete', 'attention'}:
                        continue
                    try:
                        previous = previous_moves.get((entry['source'], entry['destination']))
                        if previous and previous.get('expected') != entry['expected']:
                            previous = None
                        if previous:
                            for key in ('reservation', 'temporary', 'temporaryVersion', 'destinationVersion', 'sha256'):
                                if key in previous:
                                    entry[key] = previous[key]
                        await self.process_move(entry)
                        if previous:
                            previous['state'] = 'superseded'
                            self.save('moves', previous)
                    except (OSError, ValueError) as error:
                        if filesystem_wait_code(error):
                            # The original plan and any published destination
                            # receipt remain authoritative after reconnecting.
                            self.save('moves', entry)
                            raise
                        entry.update(state='attention', message='文件不可用、版本变化或目标冲突；原文件保留，可重试核对。')
                        self.save('moves', entry)
                    if index % 25 == 0 or time.monotonic() - updated > 2:
                        self.update_counts(plan, entries)
                        self.save('plans', plan)
                        updated = time.monotonic()
                # Enabling derived paths is one durable commit after migration,
                # including held rows whose original bindings remain usable.
                self.config.update(enabled=True, parent=plan['parent'], root=plan['root'], revision=self.config['revision'] + 1,
                                   pilotAssetId=selected if plan.get('pilot') else None)
                self.save('settings', self.config)
                self._availability(True)
                self.derive_config()
                self.videos.storage_ready = True
                self.videos.storage_error = ''
                self.videos.work.mkdir(parents=True, exist_ok=True)
                for asset in self.rows('assets'):
                    asset.update(stage=self.stage(asset), categories=self.outcomes(asset))
                    self.save('assets', asset)
                self.update_counts(plan, entries)
                plan.update(state='attention' if any(e['state'] == 'attention' for e in entries) else
                            'pilot_complete' if plan.get('pilot') else 'complete', completedAt=now_iso(),
                            message='首条整理已完成，等待全量整理。' if plan.get('pilot') else '整理完成；无法确认的文件保留原位。')
                self.save('plans', plan)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            plan = self.get('plans', identity)
            if code := filesystem_wait_code(error):
                plan.update(state='waiting', reasonCode=code, retryNotBefore=time.time() + 5,
                            message='等待磁盘或文件恢复，随后继续原整理计划。')
            else:
                plan.update(state='attention', message=str(error) if isinstance(error, ValueError) else '整理未完成，原文件与进度保留。')
            self.save('plans', plan)
        finally:
            self.maintenance = False
            self.videos.wake.set()
            self.uploads.wake.set()
            self.slicing.wake.set()
            self.wake.set()

    @staticmethod
    def is_final_move(entry):
        parts = Path(entry.get('destination') or '').parts
        kind = entry.get('kind')
        return ((kind == 'video' and len(parts) >= 3 and parts[-3] == '已切片') or
                (kind == 'archive' and len(parts) >= 3 and parts[-3] in OUTPUT_BRANCHES.values()) or
                (kind == 'image' and len(parts) >= 4 and parts[-4] in OUTPUT_BRANCHES.values()))

    def _record_move_publication(self, entry):
        """A committed location is historical fact, not a new processing command."""
        file_locations.remember(self.db, entry)
        entry['category'] = 'workspace'
        if entry['kind'] == 'video':
            # Apply exact (path, version) aliases before notifications. A listener
            # may fail, or a correction may have advanced while rename returned;
            # neither can leave path and version pointing at different files.
            self._restore_locations()
            current = self.get('assets', entry.get('assetId', ''))
            if current and self._asset_wait_reason(current):
                self.files.refresh()
            else:
                self.moves.update_bindings(entry)
        elif entry.get('packageId'):
            package = self.slicing.packages.store.get('items', entry['packageId'])
            if not package:
                raise ValueError('照片包记录已变化，已保存移动凭证，原文件保留。')
            # Resolve only the recorded version, preserving any newer package
            # identity, result receipt and upload object.
            file_locations.package(self.db, package)
            self.slicing.packages.store.save('items', package)

    async def process_move(self, entry):
        if entry['state'] == 'complete':
            return
        asset = self.get('assets', entry.get('assetId', ''))
        if asset and self.is_preserved(asset):
            entry.update(state='preserved', message='已上传历史视频，保留原位。')
            self.save('moves', entry)
            return
        def require_current():
            current = self.get('assets', entry.get('assetId', ''))
            if current:
                reason = self._asset_wait_reason(current)
                if reason:
                    raise ValueError(reason)
            if self.is_final_move(entry) and (not current or not self.photo_results_verified(current)):
                raise ValueError('照片尚未全部导入核验，暂不归档。')

        with self.leases.write(entry['source']), self.leases.write(entry['destination']):
            async def io(fn, *args):
                return await self.videos.file_operation(fn, *args)
            entry['state'] = 'moving'
            self.save('moves', entry)
            if await io(recover_published, entry):
                entry['destinationVersion'] = await io(source_fingerprint, Path(entry['destination']))
                self.save('moves', entry)
            else:
                require_current()
                source, destination = await io(prepare_paths, entry)
                current = await io(file_version, source, 'edgeHash' in entry['expected'])
                if not compatible(current, entry['expected']):
                    raise ValueError('file_changed')
                if not entry.get('temporaryVersion'):
                    direct = await io(same_device, source, destination)
                    # Hashing and volume checks yield to newer photo receipts
                    # and manual instructions. Re-read at the mutation boundary.
                    require_current()
                    if direct:
                        entry['destinationVersion'] = await io(move_direct, entry)
                    else:
                        entry['temporary'] = str(destination.parent / f".{destination.name}.{entry['reservation']['token']}.moving")
                        self.save('moves', entry)
                        entry.update(await io(copy_verified, entry))
                    self.save('moves', entry)
                if entry.get('temporaryVersion'):
                    require_current()
                    entry['destinationVersion'] = await io(publish_copied, entry)
                    self.save('moves', entry)
            # Persist and propagate the committed location even if a newer
            # command now prohibits further writes. This does not restart an
            # old generation or change any category outcome.
            self._record_move_publication(entry)
            if await io(os.path.lexists, entry['source']):
                # Cross-volume copies may still own a source. Deleting that
                # copy remains subject to the latest command and photo proof.
                require_current()
                await io(finish_source, entry)
            elif not await io(recover_published, entry):
                raise ValueError('无法确认目标文件的身份，已保存移动凭证。')
            entry.update(state='complete', completedAt=now_iso())
            self.save('moves', entry)

    def moved(self, source, destination, version):
        for row in self.videos.store.list(include_deleted=True):
            changed = False
            for v in row.get('versions', []):
                if v.get('filePath') == source and v.get('fileVersion'):
                    v['fileVersion'] = file_version(destination, True)
                    changed = True
            if changed:
                self.videos.store.save(row)
        for asset in self.rows('assets'):
            if asset['path'] == source:
                current = file_version(destination, True)
                asset.setdefault('pathHistory', []).append(source)
                asset.update(path=destination, fileVersion=current, status='managed', message='')
                self.save('assets', asset)
                self.created_times.pop(source, None)
                self.created_times[destination] = asset.get('originalCreatedNs')
        # Image paths are per-file; never move an unregistered sibling image.
        for package in self.slicing.packages.store.list('items'):
            directory = package.get('imageDir')
            if directory and Path(source).parent == Path(directory) and Path(source).name in package.get('images', []):
                package.setdefault('imagePaths', {})[Path(source).name] = destination
                package.setdefault('imageVersions', {})[Path(source).name] = version
                mapped = package['imagePaths']
                if all(n in mapped for n in package.get('images', [])):
                    parents = {str(Path(p).parent) for p in mapped.values()}
                    if len(parents) == 1:
                        package['imageDir'] = parents.pop()
                self.slicing.packages.store.save('items', package)

    def asset_for(self, path):
        row = self.db.execute("SELECT payload FROM simple_workspace_assets WHERE json_extract(payload, '$.path')=? ORDER BY rowid LIMIT 1",
                              (str(path),)).fetchone()
        return json.loads(row[0]) if row else None

    def download_destination(self, filename):
        root = self.require_root()
        base = folder_name(filename)
        matches = [a for a in self.rows('assets') if a['name'] == filename and a.get('cleanup', {}).get('state') != 'complete']
        folder = matches[0]['folder'] if len(matches) == 1 else base
        path = root / '采集视频' / folder / filename
        # Publication itself remains exclusive, including unregistered collisions.
        path.parent.mkdir(parents=True, exist_ok=True)
        return path

    def output_directory(self, item, category):
        root = self.require_root()
        asset = self.asset_for(item['path'])
        name = asset['folder'] if asset else folder_name(item['name']) + '_' + str(item['id'])[:8]
        return root / '.处理中' / name / category

    def scan_directory(self, directory, recursive=True):
        if not self.enabled:
            from .upload_audit import scan_directory
            return scan_directory(directory, recursive)
        self.require_directory(directory)
        self.require_root()
        return scan_managed(self.root, self.created_times)

    async def retry(self, identity, revision, request_id):
        plan = self.get('plans', identity)
        if not plan:
            raise ValueError('整理记录不存在。')
        # Re-preview current bindings, never replay an obsolete destination plan.
        preview = await self.preview(plan['parent'], revision, request_id + '-preview')
        return await self.apply(preview['id'], revision, request_id)

    def request_archive_retry(self, identity, request_id):
        asset = self.get('assets', identity)
        if not asset:
            raise ValueError('工作区文件记录不存在。')
        prior = self.get('requests', request_id)
        if prior:
            if prior.get('kind') != 'archive-retry' or prior.get('assetId') != identity:
                raise Conflict('请求编号已用于其他操作。')
            return
        if self.maintenance:
            raise Conflict('工作区正在整理，请完成后重试。')
        if self.is_preserved(asset):
            raise ValueError('已上传历史视频按你的设置保留原位，不参与归档。')
        if asset.get('cleanup', {}).get('state') == 'complete':
            raise ValueError('本地文件已清理，历史记录保留，无需归档。')
        asset.update(status='waiting', message='归档已排队，文件使用结束后自动处理。',
                     archiveRetry={'state': 'queued', 'requestId': request_id})
        package_ids = {p['id'] for p in self.packages(asset)}
        self.session_checked = {key for key in self.session_checked if not (
            isinstance(key, tuple) and len(key) > 1 and key[0] == 'archive-photos' and key[1] in package_ids)}
        self.save('assets', asset)
        self.save('requests', {'id': request_id, 'kind': 'archive-retry', 'assetId': identity, 'state': 'queued'})
        self.wake.set()

    async def refresh_archive_photos(self, asset):
        from .workspace_archive import refresh_photo_versions
        from .photo_completion import verified_package
        for package in self.packages(asset):
            if (package.get('state') != 'complete' or not package.get('verifiedJobId')
                    or package.get('actualImages') != package.get('imageCount')
                    or package.get('cleanupState') == 'complete' or package.get('localCleanupState') == 'complete'):
                continue
            cache_key = ('archive-photos', package['id'], package.get('revision'))
            if cache_key in self.session_checked:
                continue
            item = self.slicing.store.get('items', package.get('itemId') or '')
            allow_remount = bool(item and verified_package(package, item, package.get('category')))
            previous_version = dict(package.get('fileVersion') or {})
            with ExitStack() as guards:
                guards.enter_context(self.leases.read(package['path']))
                for name in package.get('images', []):
                    guards.enter_context(self.leases.read(package.get('imagePaths', {}).get(name, str(Path(package['imageDir']) / name))))
                changes = await self.videos.file_operation(refresh_photo_versions, package, allow_remount)
            if changes or package.get('fileVersion') != previous_version:
                current = self.slicing.packages.store.get('items', package['id'])
                if current['revision'] != package['revision']:
                    raise Conflict('照片包记录已更新，稍后继续归档。')
                current.setdefault('imageVersions', {}).update(changes)
                current['fileVersion'] = package['fileVersion']
                current['photoMetadataVerifiedAt'] = now_iso()
                self.slicing.packages.store.save('items', current)
                cache_key = ('archive-photos', current['id'], current.get('revision'))
            self.session_checked.add(cache_key)

    def request_reconcile(self, revision, request_id):
        previous = self.get('requests', request_id)
        if previous:
            if previous.get('kind') != 'reconcile':
                raise Conflict('请求编号已用于其他操作。')
            return self.state()
        if revision != self.config['revision']:
            raise Conflict('工作区已更新，请刷新后重试。')
        if not self.enabled or self.maintenance:
            raise Conflict('请先等待工作区启用和整理完成。')
        active = next((r for r in reversed(self.rows('requests')) if r.get('kind') == 'reconcile'
                       and (r.get('state') in {'queued', 'running', 'waiting'} or r.get('pipelineState') in {'queued', 'running', 'waiting'}) and not r.get('canonicalId')), None)
        if active:
            active.update(pipelineEnabled=True, pipelineState=active.get('pipelineState') or 'queued',
                          pipelineStage=active.get('pipelineStage') or 'queued',
                          pipelineMessage=active.get('pipelineMessage') or '正在准备上传核对、切片和整理。')
            self.save('requests', active)
            self.save('requests', {'id': request_id, 'kind': 'reconcile', 'state': 'queued', 'canonicalId': active['id']})
            self.wake.set()
            return self.state()
        for asset in self.rows('assets'):
            if asset.get('status') == 'attention' and not self.is_preserved(asset) and asset.get('cleanup', {}).get('state') != 'complete':
                self.request_archive_retry(asset['id'], request_id + '-' + asset['id'])
        self.save('requests', {'id': request_id, 'kind': 'reconcile', 'state': 'queued',
                              'pipelineEnabled': True, 'pipelineState': 'queued', 'pipelineStage': 'queued',
                              'pipelineMessage': '已排队：核对原片、继续切片、归类并检查安全清理条件。',
                              'message': '已排队：核对原片、继续切片、归类并检查安全清理条件。'})
        self.wake.set()
        return self.state()

    async def startup(self):
        self.stopping = False
        self.event_loop = asyncio.get_running_loop()
        unfinished = next((p for p in reversed(self.rows('plans')) if p['state'] in {'moving', 'waiting'}), None)
        if unfinished:
            self.maintenance = True
            self.task = asyncio.create_task(self.run_plan(unfinished['id']))
        self.loop_task = asyncio.create_task(self.loop())

    async def shutdown(self):
        self.stopping = True
        for task in (self.loop_task, self.task, self.cleanup_task):
            if task and not task.done():
                task.cancel()
        await asyncio.gather(*(t for t in (self.loop_task, self.task, self.cleanup_task) if t), return_exceptions=True)

    async def loop(self):
        while not self.stopping:
            # Consume notifications before doing work. A click or file release
            # during this pass must survive and trigger the next pass.
            self.wake.clear()
            try:
                if not self.maintenance and (not self.task or self.task.done()):
                    pending_plan = next((plan for plan in reversed(self.rows('plans'))
                        if plan.get('state') == 'waiting' and plan.get('reasonCode') in {'disk_offline', 'disk_io', 'file_busy'}
                        and plan.get('retryNotBefore', 0) <= time.time()), None)
                    if pending_plan:
                        self.maintenance = True
                        self.task = asyncio.create_task(self.run_plan(pending_plan['id']))
                if self.enabled and not self.maintenance:
                    await self._auto_start_pipeline()
                    await self._advance_pipeline()
                    await self.reconcile(background_cleanup=True, include_state=False)
                    self._finish_pipeline()
            except asyncio.CancelledError:
                raise
            except Exception as error:
                message = '等待原磁盘恢复，整理请求已保留。' if not self.online else '整理暂未完成，进度已保留，将继续重试。'
                self._activity('waiting', message)
                if isinstance(error, FileEnvironmentError):
                    from .workspace_cleanup import wait_for_environment
                    wait_for_environment(self, error.code, str(error))
                elif isinstance(error, ValueError) and self.availability.get('reasonCode') == 'workspace_identity':
                    from .workspace_cleanup import wait_for_environment
                    wait_for_environment(self, 'workspace_identity', self.availability['message'])
                for request in self.rows('requests'):
                    if request.get('kind') == 'reconcile' and request.get('state') in {'queued', 'running', 'waiting'} and not request.get('canonicalId'):
                        request.update(state='waiting', stage='waiting', message=message, failureType=type(error).__name__)
                        self.save('requests', request)
            try:
                pending = (self._pipeline_request() is not None or
                           any(a.get('status') == 'waiting' for a in self._targets()))
                await asyncio.wait_for(self.wake.wait(), timeout=5 if pending else 30)
            except asyncio.TimeoutError:
                pass

    def cleanup_eligibility(self, asset):
        from .workspace_cleanup import eligibility
        return eligibility(self, asset)

    def cleanup_statuses(self, assets):
        """Button availability only; deletion rechecks fresh authoritative data."""
        from .workspace_cleanup import CleanupView, eligibility
        if not assets:
            return {}
        links, packages = self.photo_inventory(assets)
        view = CleanupView(self, assets, links, packages)
        return {asset['id']: eligibility(view, asset) for asset in assets}

    def cleanup_summary(self, assets=None, links=None, packages=None):
        from .workspace_cleanup import summary
        return summary(self, assets, links, packages)

    def cleanup_asset_bytes(self, asset):
        from .workspace_cleanup import snapshot_entries
        try:
            return sum(entry['expected']['size'] for entry in snapshot_entries(self, asset, validate=False)
                       if entry['state'] != 'deleted')
        except ValueError:
            return 0

    async def request_cleanup_completed(self, expected_revision, request_id, only=None, *, token=None):
        from .workspace_cleanup import request_completed
        return await request_completed(self, expected_revision, request_id, only, token=token)

    def cleanup_operation(self, identity):
        from .workspace_cleanup import public_operation
        request = self.get('requests', identity)
        return public_operation(request, self.rows('assets')) if request and request.get('kind') == 'cleanup-completed' else None

    async def cleanup(self, asset):
        from .workspace_cleanup import cleanup
        return await cleanup(self, asset)
