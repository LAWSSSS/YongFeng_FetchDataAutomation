"""Per-video arrangement: persistent requests, bounded waits and visible progress."""
import asyncio
import json
import time
from contextlib import AsyncExitStack, asynccontextmanager

from .domain import now_iso
from .recovery_results import verified_result
from .workspace_files import compatible


ACTIVE = {'queued', 'running', 'waiting'}


class WorkspaceScheduler:
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
        for request in self.rows('requests'):
            if request.get('kind') == 'reconcile' and request.get('state') in ACTIVE and not request.get('canonicalId'):
                if asset['id'] in request.get('assetIds', []):
                    request.setdefault('outcomes', {})[asset['id']] = result
                    request.setdefault('movedByAsset', {})[asset['id']] = asset.get('organization', {}).get('filesMoved', 0)
                    request['currentName'] = asset.get('name')
                    self.save('requests', request)
        retry = (self.get('assets', asset['id']) or asset).get('archiveRetry') or {}
        if retry.get('requestId'):
            request = self.get('requests', retry['requestId'])
            if request and request.get('state') in ACTIVE:
                request.update(state='queued' if result == 'waiting' else result,
                               message=asset.get('message') or '整理完成。')
                self.save('requests', request)
                latest = self.get('assets', asset['id'])
                latest['archiveRetry'] = {**retry, 'state': request['state']}
                self.save('assets', latest)

    def _begin_requests(self):
        targets = [a['id'] for a in self._targets()]
        for request in self.rows('requests'):
            if request.get('kind') != 'reconcile' or request.get('state') not in ACTIVE or request.get('canonicalId'):
                continue
            if not request.get('scopeReady'):
                request.update(assetIds=targets, scopeReady=True)
            else:
                newly_visible = [a['id'] for a in self._targets() if a['path'] in request.get('waitingPaths', [])]
                request['assetIds'] = list(dict.fromkeys([*request.get('assetIds', []), *newly_visible]))
            request['waitingPaths'] = sorted(self.discovery_waiting)
            request.update(state='running', stage='arranging', message='正在按当前绑定整理视频和照片。')
            self.save('requests', request)

    def _finish_requests(self):
        for request in self.rows('requests'):
            if request.get('kind') != 'reconcile' or request.get('state') not in ACTIVE or request.get('canonicalId'):
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
        if asset['id'] in self.cleanup_active:
            self.cleanup_deferred.add(asset['id'])
            return self._wait_asset(asset, '此视频正在核验清理，结束后自动更新整理结果。')
        async with self._asset_locks(asset) as acquired:
            if not acquired:
                return self._wait_asset(asset, '此视频正在下载、查询或更正，完成后自动整理。')
            asset = self.get('assets', asset['id']) or asset
            busy_paths = [e['path'] for e in self.media_entries(asset) if self.leases.busy(e['path'])]
            if busy_paths:
                return self._wait_asset(asset, '此视频正在播放或处理，释放后自动整理。', busy_paths)
            if self.discovery_errors.get(asset['path']):
                raise ValueError(self.discovery_errors[asset['path']])
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
            asset.update(stage=self.stage(asset), categories=self.outcomes(asset), status='managed', message='')
            self.save('assets', asset)
            entries = self.plan_assets(self.root, only=asset['id'], checkpoints=False)
            if entries and (self.uploads.scan_lock.locked() or self.moves.busy):
                return self._wait_asset(asset, '正在读取或移动视频目录，结束后自动整理。')
            pending = {}
            for (raw,) in self.db.execute("SELECT payload FROM simple_workspace_moves WHERE json_extract(payload,'$.assetId')=? ORDER BY rowid", (asset['id'],)):
                journal = json.loads(raw)
                if journal['state'] in {'moving', 'attention', 'planned'}:
                    pending[(journal['source'], journal['destination'], json.dumps(journal.get('expected'), sort_keys=True))] = journal
            for index, entry in enumerate(entries):
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
            current.update(stage=self.stage(current), categories=self.outcomes(current), status='managed', message='',
                           organization={'state': 'complete', 'stage': self.stage(current), 'completedAt': now_iso(), 'filesMoved': len(entries)})
            self.save('assets', current)
            self._record_outcome(current)
            return current

    async def reconcile(self, only=None, cleanup=True, background_cleanup=False):
        if not self.enabled or self.maintenance:
            return self.state()
        cleanup_ids = []
        async with self.lock:
            self._activity('discovering', '正在读取已登记视频。')
            self.require_root()
            # Finish acknowledged publications before inspecting stale source
            # paths. No unrecorded destination may be adopted.
            for journal in self.rows('moves'):
                if journal['state'] != 'moving' or (only and journal.get('assetId') != only):
                    continue
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
            self._begin_requests()
            assets = self._targets()
            assets.sort(key=lambda a: (a.get('archiveRetry', {}).get('state') != 'queued', a.get('createdAt', '')))
            for asset in assets:
                if only and asset['id'] != only:
                    continue
                if self.config.get('pilotAssetId') and asset['id'] != self.config['pilotAssetId']:
                    self._wait_asset(asset, '当前为历史试运行模式，等待全量整理。')
                    continue
                retry = asset.get('archiveRetry') or {}
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
                    message = str(error) if isinstance(error, ValueError) and str(error) != 'file_changed' else '文件不可读、版本变化或目标冲突；原文件保留。'
                    current.update(status='attention', message=message, failureType=type(error).__name__,
                                   organization={'state': 'attention', 'message': message})
                    self.save('assets', current)
                    self._record_outcome(current)
                    continue
                if cleanup and current.get('status') == 'managed':
                    eligible, reason = self.cleanup_eligibility(current)
                    if eligible:
                        if not background_cleanup or time.monotonic() >= self.cleanup_next.get(current['id'], 0):
                            cleanup_ids.append(current['id'])
                    elif current.get('cleanup', {}).get('message') != reason:
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
        return self.state()

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
