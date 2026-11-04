"""Per-video arrangement: persistent requests, bounded waits and visible progress."""
import asyncio
import hashlib
import json
import time
from contextlib import AsyncExitStack, asynccontextmanager
from pathlib import Path

from .domain import now_iso
from .recovery_results import verified_result
from .videos import Conflict
from .workspace_files import compatible
from .file_leases import filesystem_wait_code


ACTIVE = {'queued', 'running', 'waiting'}
PIPELINE_ACTIVE = {'queued', 'running', 'waiting'}
PIPELINE_TERMINAL = {'complete', 'attention'}
SLICE_ACTIVE = {'pending', 'analyzing', 'platform_processing', 'waiting', 'waiting_slot',
                'waiting_connection', 'retry_wait', 'ready', 'submitting', 'exporting',
                'queued', 'uploading', 'uploaded', 'importing'}


class WorkspaceScheduler:
    def _asset_wait_reason(self, asset):
        """Current commands fence filesystem work as well as model workers."""
        pipeline = getattr(self, 'pipeline', None)
        if pipeline and pipeline.config.get('enabled') and pipeline.paused and pipeline.allowed_asset(asset):
            return '此视频已暂停，继续后再整理。'
        if self._asset_manually_paused(asset):
            return '此视频已暂停，继续后再整理。'
        control = getattr(self.slicing, 'control', None)
        if control:
            return next((reason for item in self.effective_links(asset)
                         if (reason := control.reason(item))), '')
        return ''

    def _asset_manually_paused(self, asset):
        control = getattr(self.slicing, 'control', None)
        return any(item.get('collectionRecoveryPaused') or item.get('pipelinePaused') or
                   (control and (control.get(item.get('mediaId', '')) or {}).get('paused'))
                   for item in self.effective_links(asset))

    def collection_source_binding(self, source, rows=None):
        rows = self.videos.store.list(include_deleted=True) if rows is None else rows
        matches = [(row, version) for row in rows for version in row.get('versions', [])
                   if version.get('filePath') == source.get('path')
                   and {'device', 'inode', 'size', 'modifiedNs'}.issubset(source.get('fileVersion') or {})
                   and compatible(version.get('fileVersion') or {}, source.get('fileVersion') or {})]
        if len(matches) > 1:
            return None, '原片对应多个采集版本，请人工绑定。'
        if not matches:
            return None, ''
        row, version = matches[0]
        revision = version.get('revision')
        if (row.get('deletedAt') or row.get('activeRevision') != revision
                or row.get('acceptedRevision') != revision or version.get('state') != 'saved'):
            return None, '采集版本已变更或尚未确认保存，原片和断点保留。'
        binding = {'collectionVideoId': row['id'], 'collectionRevision': revision}
        intent = (version.get('finalConfirmation') or {}).get('operationId')
        if intent:
            binding['finalProcessingIntent'] = intent
        return binding, ''

    def _targets(self):
        return [a for a in self.rows('assets') if not self.is_preserved(a)
                and a.get('cleanup', {}).get('state') != 'complete']

    def _activity(self, stage, message, name=''):
        self.activity.update(stage=stage, message=message, currentName=name, updatedAt=now_iso())

    def _wait_asset(self, asset, message, paths=()):
        latest = self.get('assets', asset['id']) or asset
        latest.update(status='waiting', message=message,
                      organization={'state': 'waiting', 'message': message, 'stage': self.stage(latest)})
        self.save('assets', latest)
        self.waiting_paths.update(map(str, paths))
        self._record_outcome(latest)
        return latest

    def _record_outcome(self, asset):
        result = 'complete' if asset.get('status') in {'managed', 'preserved', 'cleaned'} else asset.get('status', 'waiting')
        rows = self.db.execute("""SELECT payload FROM simple_workspace_requests WHERE
            (json_extract(payload,'$.kind')='reconcile' AND
             (json_extract(payload,'$.state') IN ('queued','running','waiting') OR
              json_extract(payload,'$.pipelineState') IN ('queued','running','waiting')) AND
             coalesce(json_extract(payload,'$.canonicalId'),'')='' AND EXISTS (
                SELECT 1 FROM json_each(simple_workspace_requests.payload,'$.assetIds') WHERE value=?))
            OR (json_extract(payload,'$.kind')='archive-retry' AND json_extract(payload,'$.assetId')=?
                AND json_extract(payload,'$.state') IN ('queued','running','waiting')) ORDER BY rowid""",
            (asset['id'], asset['id'])).fetchall()
        for (raw,) in rows:
            request = json.loads(raw)
            if request.get('kind') == 'reconcile' and (request.get('state') in ACTIVE or request.get('pipelineState') in PIPELINE_ACTIVE) and not request.get('canonicalId'):
                if asset['id'] in request.get('assetIds', []):
                    request.setdefault('outcomes', {})[asset['id']] = result
                    request.setdefault('movedByAsset', {})[asset['id']] = asset.get('organization', {}).get('filesMoved', 0)
                    request['currentName'] = asset.get('name')
                    self.save('requests', request)
            elif (request.get('kind') == 'archive-retry' and request.get('assetId') == asset['id']
                  and request.get('state') in ACTIVE):
                request.update(state='queued' if result == 'waiting' else result,
                               message=asset.get('message') or '整理完成。')
                self.save('requests', request)
        retry = (self.get('assets', asset['id']) or asset).get('archiveRetry') or {}
        if retry.get('requestId'):
            request = self.get('requests', retry['requestId'])
            if request:
                latest = self.get('assets', asset['id'])
                latest['archiveRetry'] = {**retry, 'state': request['state']}
                self.save('assets', latest)

    def _restore_archive_retries(self):
        """Recover accepted retries even if an older asset snapshot won a race."""
        pending = {}
        for request in self.rows('requests'):
            if request.get('kind') == 'archive-retry' and request.get('state') in ACTIVE:
                pending.setdefault(request.get('assetId'), []).append(request)
        for identity, requests in pending.items():
            asset = self.get('assets', identity or '')
            if not asset:
                continue
            if self.is_preserved(asset) or asset.get('cleanup', {}).get('state') == 'complete':
                for request in requests:
                    request.update(state='attention', message='文件已保留或清理，未继续整理。')
                    self.save('requests', request)
                continue
            retry = asset.get('archiveRetry') or {}
            if retry.get('state') in ACTIVE and any(request['id'] == retry.get('requestId') for request in requests):
                continue
            asset.update(status='waiting', message='已恢复归档队列，等待文件使用结束。',
                         archiveRetry={'state':'queued','requestId':requests[-1]['id']})
            self.save('assets', asset)

    def _begin_requests(self):
        targets = [a['id'] for a in self._targets()]
        for request in self.rows('requests'):
            if request.get('kind') != 'reconcile' or (request.get('state') not in ACTIVE and request.get('pipelineState') not in PIPELINE_ACTIVE) or request.get('canonicalId'):
                continue
            if not request.get('scopeReady'):
                request.update(assetIds=targets, scopeReady=True)
            else:
                newly_visible = [a['id'] for a in self._targets() if a['path'] in request.get('waitingPaths', [])]
                request['assetIds'] = list(dict.fromkeys([*request.get('assetIds', []), *newly_visible]))
            request['waitingPaths'] = sorted(self.discovery_waiting)
            if request.get('pipelineEnabled'):
                request.update(state='waiting' if request.get('pipelineState') == 'waiting' else 'running',
                               stage=request.get('pipelineStage') or 'queued',
                               message=request.get('pipelineMessage') or '准备继续处理工作区视频。')
            else:
                request.update(state='running', stage='arranging', message='正在按当前绑定整理视频和照片。')
            self.save('requests', request)

    def _finish_requests(self):
        for request in self.rows('requests'):
            if request.get('kind') != 'reconcile' or (request.get('state') not in ACTIVE and request.get('pipelineState') not in PIPELINE_ACTIVE) or request.get('canonicalId'):
                continue
            outcomes = request.get('outcomes', {})
            for key in request.get('assetIds', []):
                asset = self.get('assets', key)
                if asset and (self.is_preserved(asset) or asset.get('cleanup', {}).get('state') == 'complete'):
                    outcomes[key] = 'complete'
            request['outcomes'] = outcomes
            total = len(request.get('assetIds', [])) + len(request.get('waitingPaths', []))
            finished = sum(outcomes.get(k) == 'complete' for k in request.get('assetIds', []))
            failed = sum(outcomes.get(k) == 'attention' for k in request.get('assetIds', []))
            waiting = total - finished - failed
            moved = sum(request.get('movedByAsset', {}).values())
            request.update(total=total, finished=finished, failed=failed, waiting=waiting, movedFiles=moved)
            if waiting:
                request.update(state='waiting', stage='waiting', message=f'已整理 {finished}/{total} 条，{waiting} 条等待文件释放或磁盘恢复。')
            else:
                request.update(state='attention' if failed else 'complete', stage='complete', completedAt=now_iso(),
                               message=f'整理结束：{finished} 条完成，{failed} 条需处理。' if failed else
                               f'整理完成：{finished} 条，移动 {moved} 个文件。' if moved else f'已核对 {finished} 条，位置均正确，无需移动。')
            self.save('requests', request)

        for alias in self.rows('requests'):
            if alias.get('kind') != 'reconcile' or not alias.get('canonicalId'):
                continue
            original = self.get('requests', alias['canonicalId'])
            if not original:
                continue
            keys = ('state', 'stage', 'message', 'total', 'finished', 'waiting', 'failed', 'completedAt',
                    'pipelineState', 'pipelineStage', 'pipelineMessage')
            changes = {key: original.get(key) for key in keys}
            if any(alias.get(key) != value for key, value in changes.items()):
                alias.update(changes)
                self.save('requests', alias)

    @asynccontextmanager
    async def _asset_locks(self, asset):
        # A platform query for another video must not block this file. Location
        # receipts fence late callbacks in every store; only this video's row
        # locks are needed while changing its paths.
        rows = self.moves.bindings(asset['path'])
        locks = [self.videos.row_lock(r['id']) for r in sorted(rows, key=lambda r: r['id'])]
        async with AsyncExitStack() as guards:
            if any(lock.locked() for lock in locks):
                yield False
                return
            for lock in locks:
                await guards.enter_async_context(lock)
            yield True

    async def _reconcile_asset(self, asset):
        reason = self._asset_wait_reason(asset)
        if reason:
            return self._wait_asset(asset, reason)
        if getattr(self, 'discovery_environment', {}).get(asset['path']):
            return self._wait_asset(asset, '磁盘暂不可用，恢复后自动继续整理。')
        if asset['id'] in self.cleanup_active:
            self.cleanup_deferred.add(asset['id'])
            return self._wait_asset(asset, '此视频正在核验清理，结束后自动更新整理结果。')
        async with self._asset_locks(asset) as acquired:
            if not acquired:
                return self._wait_asset(asset, '此视频正在下载、查询或更正，完成后自动整理。')
            asset = self.get('assets', asset['id']) or asset
            attempt_retry = (asset.get('archiveRetry') or {}).get('requestId')
            busy_paths = [e['path'] for e in self.media_entries(asset) if self.leases.busy(e['path'])]
            if busy_paths:
                return self._wait_asset(asset, '此视频正在播放或处理，释放后自动整理。', busy_paths)
            if self.discovery_errors.get(asset['path']):
                raise ValueError(self.discovery_errors[asset['path']])
            from .workspace_cleanup import authorization, registered_cleanup
            if authorization(self, asset)[0]:
                # The user's cleanup click fixed these exact paths. Do not
                # reorganize them before the deletion journal can validate them.
                asset.update(status='managed', message='正在核对本次清理清单。')
                self.save('assets', asset)
                self._record_outcome(asset)
                return asset
            if registered_cleanup(self, asset):
                # Failed cleanup still owns its fixed paths. Neither ordinary
                # arrangement nor a display refresh may repurpose that scope.
                asset.update(status='managed', message=asset.get('cleanup', {}).get('message') or '原清理记录保留。')
                self.save('assets', asset)
                self._record_outcome(asset)
                return asset
            cleanup_journal = asset.get('cleanup', {})
            if cleanup_journal.get('state') != 'complete' and any(
                    e.get('state') in {'deleting', 'deleted'} for e in cleanup_journal.get('manifest', [])):
                # The cleanup manifest owns these paths until its remaining
                # entries are reconciled. Never move or re-open removed photos.
                asset.update(status='managed', message='正在恢复已登记的清理进度。',
                             organization={'state':'complete', 'stage':self.stage(asset), 'filesMoved':0})
                self.save('assets', asset)
                self._record_outcome(asset)
                return asset
            self._activity('checking', '正在核对文件版本。', asset['name'])
            for package in self.packages(asset):
                if not package.get('videoFileVersion') or not compatible(asset['fileVersion'], package['videoFileVersion']):
                    raise ValueError('照片包与原视频的版本无法确认一致，文件保留待核对。')
            await self.refresh_archive_photos(asset)
            latest = self.get('assets', asset['id']) or asset
            latest_retry = latest.get('archiveRetry') or {}
            if latest_retry.get('state') == 'queued' and latest_retry.get('requestId') != attempt_retry:
                self._record_outcome(latest)
                return latest
            asset.update(stage=self.stage(asset), categories=self.outcomes(asset), status='managed', message='')
            self.save('assets', asset)
            entries = self.plan_assets(self.root, only=asset['id'], checkpoints=False)
            if entries and (self.uploads.scan_lock.locked() or self.moves.busy):
                return self._wait_asset(asset, '正在读取或移动视频目录，结束后自动整理。')
            pending = {}
            for (raw,) in self.db.execute("SELECT payload FROM simple_workspace_moves WHERE json_extract(payload,'$.assetId')=? AND json_extract(payload,'$.state') IN ('moving','attention','planned') ORDER BY rowid", (asset['id'],)):
                journal = json.loads(raw)
                pending[(journal['source'], journal['destination'], json.dumps(journal.get('expected'), sort_keys=True))] = journal
            for index, entry in enumerate(entries):
                if registered_cleanup(self, asset):
                    # A cleanup request may have been registered while the
                    # organizer was checking files. Its snapshot now owns them.
                    current = self.get('assets', asset['id']) or asset
                    self._record_outcome(current)
                    return current
                if self.leases.busy(entry['source']) or self.leases.busy(entry['destination']):
                    return self._wait_asset(asset, '文件使用中，释放后自动整理。', [entry['source'], entry['destination']])
                self._activity('moving', f'正在整理 {index + 1}/{len(entries)} 个文件。', asset['name'])
                journal = pending.get((entry['source'], entry['destination'], json.dumps(entry['expected'], sort_keys=True))) or entry
                try:
                    await self.process_move(journal)
                except BaseException:
                    if journal.get('state') == 'moving':
                        journal.update(state='moving' if journal.get('destinationVersion') or journal.get('temporaryVersion') else 'attention',
                                       message='进度已保存，重启或重试时先核对移动凭证。')
                        self.save('moves', journal)
                    raise
            current = self.get('assets', asset['id'])
            reason = self._asset_wait_reason(current)
            if reason:
                return self._wait_asset(current, reason)
            current.update(stage=self.stage(current), categories=self.outcomes(current), status='managed', message='',
                           organization={'state': 'complete', 'stage': self.stage(current), 'completedAt': now_iso(), 'filesMoved': len(entries)})
            self.save('assets', current)
            self._record_outcome(current)
            return current

    async def reconcile(self, only=None, cleanup=True, background_cleanup=False, *, include_state=True):
        if not self.enabled or self.maintenance:
            return self.state() if include_state else None
        cleanup_ids = []
        async with self.lock:
            self._activity('discovering', '正在读取已登记视频。')
            await self.check_root()
            # Finish acknowledged publications before inspecting stale source
            # paths. No unrecorded destination may be adopted.
            query = "SELECT payload FROM simple_workspace_moves WHERE json_extract(payload, '$.state')='moving'"
            if only:
                query += " AND json_extract(payload, '$.assetId')=?"
            journals = self.db.execute(query + ' ORDER BY rowid', (only,) if only else ()).fetchall()
            for (raw,) in journals:
                await asyncio.sleep(.01)
                journal = json.loads(raw)
                if self.leases.busy(journal['source']) or self.leases.busy(journal['destination']):
                    continue
                asset = self.get('assets', journal.get('assetId', ''))
                if not asset:
                    continue
                async with self._asset_locks(asset) as acquired:
                    if acquired:
                        try:
                            await self.process_move(journal)
                        except (OSError, ValueError):
                            journal.update(state='attention', message='移动凭证尚未核对完成，原文件保留。')
                            self.save('moves', journal)
            await self.discover(only=only)
            self._restore_archive_retries()
            self._begin_requests()
            assets = self._targets()
            assets.sort(key=lambda a: (a.get('archiveRetry', {}).get('state') != 'queued', a.get('createdAt', '')))
            for asset in assets:
                # Already-arranged assets may perform only synchronous DB work.
                # A zero-delay yield keeps another CPU-heavy asset ahead of
                # socket accept/read callbacks. A brief timed yield lets those
                # callbacks drain before the next bounded unit of work.
                await asyncio.sleep(.01)
                if only and asset['id'] != only:
                    continue
                if self.config.get('pilotAssetId') and asset['id'] != self.config['pilotAssetId']:
                    self._wait_asset(asset, '当前为历史试运行模式，等待全量整理。')
                    continue
                retry = asset.get('archiveRetry') or {}
                if (asset.get('status') == 'waiting' and asset.get('organization', {}).get('retryNotBefore', 0)
                        > self.slicing.clock()):
                    self._record_outcome(asset)
                    continue
                proven = not asset.get('organization') and any(verified_result(i, lambda key: self.slicing.packages.store.get('items', key)) for i in self.linked(asset))
                if asset.get('status') == 'attention' and retry.get('state') != 'queued' and not proven:
                    self._record_outcome(asset)
                    continue
                try:
                    current = await self._reconcile_asset(asset)
                except asyncio.CancelledError:
                    raise
                except Exception as error:
                    current = self.get('assets', asset['id']) or asset
                    latest_retry = current.get('archiveRetry') or {}
                    if latest_retry.get('state') == 'queued' and latest_retry.get('requestId') != retry.get('requestId'):
                        self._record_outcome(current)
                        continue
                    if code := filesystem_wait_code(error):
                        current = self._wait_asset(current, '文件使用中，释放后自动继续。' if code == 'file_busy'
                                                   else '磁盘读写暂未完成，文件和凭证保留，将自动重试。')
                        current['organization'].update(retryNotBefore=self.slicing.clock() + 5,
                                                       failureType=type(error).__name__, reasonCode=code)
                        if code == 'disk_offline':
                            self._availability(False, code, str(error))
                        self.save('assets', current)
                        continue
                    message = str(error) if isinstance(error, ValueError) and str(error) != 'file_changed' else '文件不可读、版本变化或目标冲突；原文件保留。'
                    current.update(status='attention', message=message, failureType=type(error).__name__,
                                   organization={'state': 'attention', 'message': message})
                    self.save('assets', current)
                    self._record_outcome(current)
                    continue
                if cleanup and current.get('status') == 'managed':
                    from .workspace_cleanup import authorization, registered_cleanup
                    eligible, reason = self.cleanup_eligibility(current)
                    authorized, _snapshot = authorization(self, current)
                    if eligible and authorized:
                        if not background_cleanup or time.monotonic() >= self.cleanup_next.get(current['id'], 0):
                            cleanup_ids.append(current['id'])
                    elif registered_cleanup(self, current):
                        # An existing fixed-scope rejection remains history,
                        # not a new invitation to clean the same files again.
                        continue
                    elif eligible and current.get('cleanup', {}).get('state') != 'pending_manual':
                        current['cleanup'] = {**current.get('cleanup', {}), 'state': 'pending_manual',
                                              'message': '处理完成，等待点击清理已结束文件。'}
                        self.save('assets', current)
                    elif current.get('cleanup', {}).get('message') != reason:
                        if not eligible:
                            current['cleanup'] = {**current.get('cleanup', {}), 'state': 'waiting', 'message': reason}
                            self.save('assets', current)
            self.last_check = now_iso()
            self._finish_requests()
            self._activity('idle', '本轮整理已结束。')
        # Network verification for deletion never occupies the organizer lock.
        if cleanup_ids and background_cleanup:
            self.cleanup_pending.update(cleanup_ids)
            if not self.cleanup_task or self.cleanup_task.done():
                self.cleanup_task = asyncio.create_task(self._cleanup_pass())
        elif cleanup_ids:
            await self._cleanup_ids(cleanup_ids)
        from .workspace_cleanup import update_operations
        update_operations(self)
        return self.state() if include_state else None

    def _pipeline_request(self):
        row = self.db.execute("""SELECT payload FROM simple_workspace_requests
            WHERE json_extract(payload,'$.kind')='reconcile' AND json_extract(payload,'$.pipelineEnabled')=1
                AND coalesce(json_extract(payload,'$.pipelineState'),'') NOT IN ('complete','attention')
                AND coalesce(json_extract(payload,'$.canonicalId'),'')='' ORDER BY rowid DESC LIMIT 1""").fetchone()
        return json.loads(row[0]) if row else None

    def _pipeline_update(self, request, stage, message, state='running', **extra):
        request.update(pipelineStage=stage, pipelineState=state, pipelineMessage=message,
                       state=state, stage=stage, message=message, pipelineUpdatedAt=now_iso(), **extra)
        self.save('requests', request)

    async def _auto_start_pipeline(self):
        pipeline = getattr(self, 'pipeline', None)
        if pipeline and not pipeline.active():
            return
        if not self.enabled or self.maintenance or self._pipeline_request():
            return
        candidates = []
        for asset in self._targets():
            if pipeline and not pipeline.allowed_asset(asset):
                continue
            if self._asset_manually_paused(asset):
                continue
            if (self.is_preserved(asset) or asset.get('cleanup', {}).get('state') == 'complete'
                    or asset.get('status') != 'managed'):
                continue
            linked = self.effective_links(asset)
            if any(item.get('superseded') for item in linked):
                continue
            stage = self.stage(asset)
            unstarted = stage == '采集视频' and not linked
            partial = stage in {'未切片', '车斗未切片', '磁盘吊未切片'} and linked and not any(
                item.get('state') == 'attention' for item in linked)
            if not (unstarted or partial) or self.leases.busy(asset.get('path', '')):
                continue
            if not Path(asset.get('path') or '').is_file():
                continue
            candidates.append(asset)
        if not candidates:
            return
        signature = []
        for asset in candidates:
            linked = self.effective_links(asset)
            signature.append({
                'id': asset.get('id'), 'fileVersion': asset.get('fileVersion'),
                'stage': asset.get('stage'),
                'categories': {name: value.get('state') for item in linked
                               for name, value in item.get('categories', {}).items()
                               if isinstance(value, dict)},
            })
        trigger = hashlib.sha256(json.dumps(signature, sort_keys=True, separators=(',', ':')).encode()).hexdigest()[:32]
        if any(row.get('kind') == 'reconcile' and row.get('pipelineAutoKey') == trigger for row in self.rows('requests')):
            return
        identity = f'auto-workspace-{trigger}'
        self.save('requests', {'id': identity, 'kind': 'reconcile', 'state': 'queued',
            'pipelineEnabled': True, 'continuous': bool(pipeline), 'pipelineAutoKey': trigger, 'pipelineAssetIds': [a['id'] for a in candidates],
            'pipelineState': 'queued', 'pipelineStage': 'queued',
            'pipelineMessage': f'发现 {len(candidates)} 条未进入切片队列的下载视频，正在自动续处理。',
            'assetIds': [a['id'] for a in candidates], 'scopeReady': True, 'waitingPaths': [], 'createdAt': now_iso()})

    def _pipeline_snapshot(self, request):
        assets, excluded = [], []
        selected = set(request.get('pipelineAssetIds') or [])
        collection_rows = self.videos.store.list(include_deleted=True)
        for asset in self._targets():
            if selected and asset['id'] not in selected:
                continue
            if self._asset_manually_paused(asset):
                excluded.append({'id': asset['id'], 'name': asset.get('name', ''), 'reason': '此视频已手动暂停，其他视频继续。'})
                continue
            if self.is_preserved(asset) or asset.get('cleanup', {}).get('state') == 'complete':
                continue
            linked = self.linked(asset)
            if any(item.get('superseded') for item in linked):
                excluded.append({'id': asset['id'], 'name': asset.get('name', ''),
                                 'reason': '旧版本已被更正，保留原记录。'})
                continue
            if asset.get('status') == 'attention':
                excluded.append({'id': asset['id'], 'name': asset.get('name', ''),
                                 'reason': asset.get('message') or '文件身份需核对。'})
                continue
            path = Path(asset.get('path') or '')
            try:
                relative = path.relative_to(self.root).as_posix()
            except (ValueError, TypeError):
                excluded.append({'id': asset['id'], 'name': asset.get('name', ''),
                                 'reason': '文件不在当前工作区，需重新绑定。'})
                continue
            if not asset.get('fileVersion'):
                excluded.append({'id': asset['id'], 'name': asset.get('name', ''),
                                 'reason': '缺少原片版本凭证。'})
                continue
            _binding, reason = self.collection_source_binding(asset, collection_rows)
            if reason:
                excluded.append({'id': asset['id'], 'name': asset.get('name', ''), 'reason': reason})
                continue
            assets.append({'id': asset['id'], 'name': asset.get('name', ''), 'path': str(path),
                           'relativePath': relative, 'fileVersion': asset['fileVersion']})
        if not request.get('pipelineSnapshot'):
            signature = [{key: row.get(key) for key in ('id', 'relativePath', 'fileVersion')} for row in assets]
            digest = hashlib.sha256(json.dumps(signature, sort_keys=True, separators=(',', ':')).encode()).hexdigest()[:32]
            request_tag = hashlib.sha256(request['id'].encode()).hexdigest()[:12]
            request.update(pipelineSnapshot=digest, pipelineAssetIds=[row['id'] for row in assets],
                           pipelineSources=assets, pipelineExcluded=excluded,
                           pipelineUploadRequestId=f"ws-upload-{digest}-{request_tag}",
                           pipelineSliceRequestId=f"ws-slice-{digest}-{request_tag}")
            self.save('requests', request)
        from . import file_locations
        resolved, saved_sources, exclusions = [], [], list(excluded)
        for saved in request.get('pipelineSources') or []:
            asset = self.get('assets', saved['id'])
            if asset and asset.get('cleanup', {}).get('state') == 'complete':
                resolved.append(saved)
                saved_sources.append(saved)
                continue
            if not asset or self.is_preserved(asset):
                exclusions.append({'id': saved['id'], 'name': saved.get('name', ''), 'reason': '记录已移除或设为保留。'})
                saved_sources.append(saved)
                continue
            if self._asset_manually_paused(asset):
                exclusions.append({'id': saved['id'], 'name': saved.get('name', ''), 'reason': '此视频已手动暂停，其他视频继续。'})
                saved_sources.append(saved)
                continue
            _binding, reason = self.collection_source_binding(asset, collection_rows)
            if reason:
                exclusions.append({'id': saved['id'], 'name': saved.get('name', ''), 'reason': reason})
                saved_sources.append(saved)
                continue
            target, version = file_locations.resolve(self.db, saved['path'], saved['fileVersion'])
            if target != asset.get('path') or not compatible(version, asset.get('fileVersion') or {}):
                exclusions.append({'id': saved['id'], 'name': saved.get('name', ''),
                                   'reason': '文件位置或版本变化缺少移动凭证，原位保留。'})
                saved_sources.append(saved)
                continue
            current_source = {**saved, 'path': target, 'fileVersion': asset['fileVersion'],
                              'relativePath': Path(target).relative_to(self.root).as_posix()}
            resolved.append(current_source)
            saved_sources.append(current_source)
        unique = {(row.get('id'), row.get('reason')): row for row in exclusions}
        if (saved_sources != request.get('pipelineSources') or list(unique.values()) != request.get('pipelineExcluded')):
            request.update(pipelineSources=saved_sources, pipelineExcluded=list(unique.values()))
            self.save('requests', request)
        return resolved, request['pipelineExcluded']

    def _queue_local_pipeline_sources(self, request, sources):
        candidates, blockers = [], []
        rows = self.videos.store.list(include_deleted=True)
        for source in sources:
            asset = self.get('assets', source['id'])
            if not asset or asset.get('cleanup', {}).get('state') == 'complete':
                continue
            if all(value in {'complete', 'no_target', 'uncuttable'} for value in self.outcomes(asset).values()):
                continue
            if self.leases.key(source['path']) in self.leases.writers:
                continue
            binding, reason = self.collection_source_binding(source, rows)
            candidate = dict(source)
            if reason:
                blockers.append({'name': source['name'], 'reason': reason})
                continue
            if binding:
                candidate.update(binding)
            linked = self.effective_links(asset)
            if linked:
                candidate['existingItemIds'] = [item['id'] for item in linked]
            candidates.append(candidate)
        if candidates:
            try:
                self.slicing.queue_workspace_sources(candidates, 'ws-local-' + request['id'])
            except (Conflict, ValueError) as error:
                blockers.append({'name': '', 'reason': str(error)})
        return blockers

    def _advance_local_pipeline(self, request, sources, blockers):
        assets = [self.get('assets', row['id']) for row in sources]
        assets = [asset for asset in assets if asset]
        linked = [item for asset in assets for item in self.effective_links(asset)]
        ids = {item['id'] for item in linked}
        packages = [p for p in self.slicing.packages.store.list('items') if p.get('itemId') in ids]
        local_active = [item for item in linked if item.get('state') in SLICE_ACTIVE or any(
            category.get('state') in SLICE_ACTIVE for category in item.get('categories', {}).values())]
        package_active = [p for p in packages if p.get('state') in SLICE_ACTIVE]
        done = sum(self.photo_results_verified(asset) for asset in assets)
        for item in linked:
            if item.get('state') == 'attention':
                blockers.append({'name': item.get('name', ''), 'reason': item.get('message') or '本地识别需核对。'})
        for package in packages:
            if package.get('state') in {'review', 'attention'}:
                blockers.append({'name': package.get('name', ''), 'reason': package.get('message') or '图片包需核对。'})
        if local_active or package_active:
            message = f'识别及照片导入 {done}/{len(assets)}，原视频保留。'
            self._pipeline_update(request, 'processing', message, 'waiting',
                pipelineUploadDone=0, pipelineUploadTotal=0,
                pipelineSliceDone=done, pipelineSliceTotal=len(assets), pipelineBlockers=blockers)
        else:
            self._pipeline_update(request, 'organizing', '处理已结束，正在归类并核对可清理文件。', 'running',
                pipelineUploadDone=0, pipelineUploadTotal=0,
                pipelineSliceDone=done, pipelineSliceTotal=len(assets), pipelineBlockers=blockers)

    async def _advance_pipeline(self):
        request = self._pipeline_request()
        if not request or not self.enabled or self.maintenance:
            return
        pipeline = getattr(self, 'pipeline', None)
        if pipeline and pipeline.config.get('enabled') and pipeline.paused:
            return
        if request.get('continuous') and pipeline and not pipeline.active():
            return
        if request.get('pipelineNextAttempt', 0) > time.time():
            return
        if not self.online:
            self._pipeline_update(request, 'waiting_connection', '工作区磁盘暂不可用，连接后会继续。', 'waiting')
            return
        try:
            sources, excluded = self._pipeline_snapshot(request)
            blockers = list({(row.get('id'), row.get('reason')): row for row in excluded}.values())
            if request.get('pipelineBlockers') != blockers:
                request['pipelineBlockers'] = blockers
                self.save('requests', request)
            if not sources:
                state = 'attention' if excluded else 'complete'
                self._pipeline_update(request, state, '没有可自动处理的视频。' if excluded else '工作区没有待继续的视频。',
                                      state, pipelineBlockers=excluded, completedAt=now_iso())
                return
            if not self.slicing.store.settings.get('localExport', True):
                self._pipeline_update(request, 'attention', '当前设置为平台视频切片，请切换为本地识别后继续。',
                                      'attention', pipelineBlockers=blockers, completedAt=now_iso())
                return
            blockers.extend(self._queue_local_pipeline_sources(request, sources))
            self._advance_local_pipeline(request, sources, blockers)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            self._pipeline_update(request, 'waiting_retry', '续处理暂时未完成，进度已保存，将自动重试。', 'waiting',
                                  pipelineFailureType=type(error).__name__, pipelineNextAttempt=time.time() + 30)

    def _finish_pipeline(self):
        request = self._pipeline_request()
        if not request or request.get('pipelineStage') != 'organizing':
            return
        sources = request.get('pipelineSources') or []
        blockers = list(request.get('pipelineBlockers') or [])
        finished = waiting = retained = 0
        for source in sources:
            asset = self.get('assets', source['id'])
            if not asset:
                blockers.append({'name': source.get('name', ''), 'reason': '工作区记录已变化，需重新核对。'})
                continue
            # The deletion journal is terminal. Old arrangement wait markers
            # and absent media must not reopen a verified, cleaned source.
            if asset.get('cleanup', {}).get('state') == 'complete':
                finished += 1
                continue
            if self._asset_manually_paused(asset):
                blockers.append({'name': asset['name'], 'reason': '此视频已手动暂停，其他视频继续。'})
                continue
            outcomes = self.outcomes(asset)
            if not all(value in {'complete', 'no_target', 'uncuttable'} for value in outcomes.values()):
                related = self.linked(asset)
                reason = next((item.get('message') for item in related if item.get('state') == 'attention'), '')
                if not reason:
                    package = next((row for row in self.packages(asset) if row.get('state') in {'review', 'attention'}), None)
                    if package:
                        reason = package.get('message') or '图片包等待人工确认。'
                if not reason:
                    category = next((name for name, value in outcomes.items() if value == 'pending'), '')
                    if category and any(not item.get('categories', {}).get(category) or
                                        item.get('categories', {}).get(category, {}).get('state') == 'not_applicable'
                                        for item in related):
                        label = '车斗' if category == 'bed' else '磁盘吊'
                        reason = f'{label}视频集尚未关联到该原片。'
                if not related and not reason:
                    reason = '两个视频集中都没有唯一对应的原片记录。'
                if reason:
                    blockers.append({'name': asset['name'], 'reason': reason})
                else:
                    waiting += 1
                continue
            if not self.photo_results_verified(asset):
                blockers.append({'name': asset['name'], 'reason': '照片尚未全部导入核验，视频和照片保留。'})
                continue
            finished += 1
            if asset.get('status') == 'waiting' or (asset.get('organization') or {}).get('state') in {'queued', 'running', 'waiting'}:
                waiting += 1
                continue
            if asset.get('status') == 'attention':
                blockers.append({'name': asset['name'], 'reason': asset.get('message') or '文件整理需要核对。'})
                continue
            if asset.get('cleanup', {}).get('state') == 'complete':
                continue
            eligible, reason = self.cleanup_eligibility(asset)
            if asset.get('cleanup', {}).get('state') in {'running', 'deleting'}:
                waiting += 1
            elif eligible or asset.get('cleanup', {}).get('state') == 'pending_manual':
                # Finishing processing does not authorize deleting local media.
                # A separate click snapshots the completed set for cleanup.
                retained += 1
            else:
                # Deletion eligibility is independent of completed photo work.
                # User originals and legacy no-photo conclusions may remain on
                # disk without turning a finished pipeline into an exception.
                retained += 1
        if waiting:
            unique = {(row.get('id'), row.get('name'), row.get('reason')): row for row in blockers}
            blockers = list(unique.values())
            self._pipeline_update(request, 'organizing', f'处理完成 {finished}/{len(sources)} 条，等待归类或安全清理。',
                'waiting', finished=finished, total=len(sources), waiting=waiting, failed=len(blockers),
                pipelineBlockers=blockers)
            return
        final_state = 'attention' if blockers else 'complete'
        blockers = list({(row.get('id'), row.get('name'), row.get('reason')): row for row in blockers}.values())
        message = (f'可处理视频已完成：{finished}/{len(sources)} 条；另有 {len(blockers)} 条保留待核对。'
                   if blockers else f'识别和照片导入完成：{finished} 条；原视频保留 {retained} 条。')
        self._pipeline_update(request, final_state, message, final_state, finished=finished, total=len(sources),
                              waiting=0, failed=len(blockers), retained=retained, completedAt=now_iso(), pipelineBlockers=blockers)

    async def _cleanup_ids(self, identities):
        for identity in identities:
            current = self.get('assets', identity)
            if not current or current.get('status') != 'managed':
                continue
            async with self._asset_locks(current) as acquired:
                if not acquired:
                    continue
                self.cleanup_active.add(identity)
                try:
                    await self.cleanup(current)
                except asyncio.CancelledError:
                    raise
                except Exception as error:
                    # One broken cleanup must not discard the other claimed
                    # assets. Retain its persisted manifest for a later pass.
                    latest = self.get('assets', identity) or current
                    if latest.get('cleanup', {}).get('state') != 'complete':
                        latest['cleanup'] = {**latest.get('cleanup', {}), 'state': 'waiting',
                            'failureType': type(error).__name__,
                            'message': '本条清理暂未完成，文件和凭证保留，将自动重试。'}
                        self.save('assets', latest)
                finally:
                    self.cleanup_active.discard(identity)
                    self.cleanup_next[identity] = time.monotonic() + 30
                    latest = self.get('assets', identity) or {}
                    if self.event_loop and (identity in self.cleanup_deferred or latest.get('status') == 'cleaned'):
                        self.wake.set()
                    self.cleanup_deferred.discard(identity)

    async def _cleanup_pass(self):
        while self.cleanup_pending and not self.stopping:
            identities, self.cleanup_pending = list(self.cleanup_pending), set()
            try:
                await self._cleanup_ids(identities)
            except asyncio.CancelledError:
                raise
            except Exception:
                # Arrangement remains available; cleanup retries with its own
                # persisted per-file manifest on the next ordinary pass.
                return
