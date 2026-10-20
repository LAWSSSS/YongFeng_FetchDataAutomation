"""Durable one-click intent and recovery, separate from platform submission."""
from .domain import now_iso
from .videos import Conflict


class SlicingWorkflow:
    async def execute_all(self, request_id, expected_revision):
        async with self.start_lock:
            old = self.store.get('requests', request_id)
            if old:
                if old.get('kind') != 'execute-all':
                    raise Conflict('请求编号已用于其他操作。')
                return self.state()
            if expected_revision != self.store.settings['revision']:
                raise Conflict('设置已更新，请刷新后重试。')
            # A refresh scan must not write stale copies over a running worker.
            # Coalesce clicks into one durable follow-up scan instead.
            workflow = self.store.settings.get('workflow') or {}
            workflow.update(enabled=True, phase='scan', generation=workflow.get('generation', 0) + 1, requestedAt=now_iso(),
                            settingsRevision=expected_revision, requestId=request_id)
            self.store.settings['workflow'] = workflow
            self.store.settings['enabled'] = True
            self.store.save_settings()
            self.store.save('requests', {'id': request_id, 'kind': 'execute-all', 'state': 'accepted'})
            self._wake_scheduler()
            return self.state()

    async def _workflow_scan(self):
        """Run one refresh between analyses; retain the same batch identity."""
        if self.analysis_task and self.analysis_task.done():
            finished, self.analysis_task = self.analysis_task, None
            if not finished.cancelled():
                finished.result()
        if self.analysis_task or (self.files and self.files.busy) or (self.scan_task and not self.scan_task.done()):
            return False
        from uuid import uuid4
        workflow = self.store.settings['workflow']
        generation = workflow.get('generation', 0)
        self.run = self.store.get('runs', self.store.settings.get('latestRunId'))
        if not self.run:
            self.run = {'id': uuid4().hex, 'items': [], 'issues': [], 'excluded': {}, 'startedAt': now_iso()}
        run = self.run
        run.update(requestId=workflow['requestId'], state='scanning', executeAll=True,
                   settingsRevision=self.store.settings['revision'], singleMode=False, pilotMode=False)
        run.pop('singleItemId', None)
        self.store.settings['latestRunId'] = run['id']
        self.store.save_settings()
        self.store.save('runs', run)
        # Await the refresh here: no analysis or submit can race its snapshots.
        await self._scan(run['id'])
        self.run = self.store.get('runs', run['id'])
        if self.run['state'] == 'complete':
            await self._recover_moved_sources()
            if self.files:
                self.files.refresh()
            workflow = self.store.settings['workflow']
            # A click after either inventory was read needs a new snapshot;
            # coalesce those clicks into one follow-up scan, not a lost intent.
            if workflow.get('generation', 0) != generation:
                workflow['phase'] = 'scan'
                self.store.save_settings()
                self.wake.set()
                return False
            workflow.update(phase='processing', scannedAt=now_iso())
            self.store.settings['workflow'] = workflow
            self.store.save_settings()
            self.run.update(state='running', message='全部切片已启动。', executeAll=True,
                            singleMode=False, pilotMode=False)
            self.store.save('runs', self.run)
            for item in self.store.list('items'):
                category = item['categories']['bed' if item['sourceDatasetId'] == '27912' else 'magnet']
                if category.get('state') == 'pilot_deferred':
                    category.update(state='ready', message='等待平台空位。')
                    item['state'] = 'platform_processing'
                    self.store.save('items', item)
            return True
        if self.run['state'] != 'waiting_connection':
            workflow.update(phase='attention')
            self.store.settings['enabled'] = False
            self.store.save_settings()
        return False

    def moved(self, source, destination, version):
        from .slicing import source_version
        current = source_version(destination)
        for item in self.store.list('items'):
            if item.get('path') != source:
                continue
            # Retain the extraction cache's immutable identity across verified
            # renames, while refreshing the actual filesystem identity.
            item.setdefault('observationVersion', item.get('fileVersion'))
            item.setdefault('submissionFileVersion', item.get('fileVersion'))
            item.update(path=destination, fileVersion=current, movedAt=now_iso(),
                        revision=int(item.get('revision', 0)) + 1)
            self.store.save('items', item)

    async def process_with_recovery(self, item):
        """Only transient extraction errors retry. Facts and identity do not."""
        try:
            await self._process_item(item)
        except Exception:
            # _process_item journals a sanitized failure before raising. A
            # programming error must not discard other files in an all run.
            current = self.store.get('items', item['id']) or item
            current.update(state='attention', errorCode='internal_error', message='本条处理异常，原片和进度保留。')
            self.store.save('items', current)
        current = self.store.get('items', item['id'])
        return self._recover_analysis_outcome(current)

    def _recover_analysis_outcome(self, current):
        from .slicing import applicable_category
        code = current.get('errorCode', '')
        category = current['categories'][applicable_category(current['sourceDatasetId'])]
        disk_absent = False
        if code in {'local_video_missing', 'disk', 'worker_failed', 'magnet_model_missing', 'bed_model_missing', 'frame_extract_failed', 'media_incomplete', 'media_check_timeout'}:
            from pathlib import Path
            paths = [current.get('path') or '/']
            if code in {'magnet_model_missing', 'bed_model_missing'}:
                paths += [self.store.settings.get(key, '/') for key in ('bedModelPath', 'magnetModelPath')]
            disk_absent = any(len(path.parts) > 2 and path.parts[1] == 'Volumes' and not Path(*path.parts[:3]).is_mount()
                              for path in map(Path, paths))
        if code == 'worker_failed' and self.leases.key(current.get('path') or '') in self.leases.writers:
            current.update(state='pending', errorCode='', message='等待文件移动结束。')
            category.update(state='pending', errorCode='')
        elif code in {'auth', 'network'} or disk_absent:
            current.update(state='waiting_connection', message='等待平台或原磁盘恢复。', waitCode=code)
            category.update(state='pending', errorCode='')
        elif code in {'worker_stalled', 'frame_extract_failed', 'media_check_timeout', 'observations_incomplete'}:
            retries = int(current.get('automaticRetries', 0))
            if retries < 2:
                current.update(state='pending', errorCode='', automaticRetries=retries + 1,
                               message=f'临时故障，自动重试 {retries + 1}/2。')
                category.update(state='pending', errorCode='')
        self.store.save('items', current)
        return current

    async def _restore_waiting_files(self):
        from .slicing import source_version
        for item in self.store.list('items'):
            if item.get('state') != 'waiting_connection':
                continue
            try:
                if not self.auth.connected or source_version(item['path']) != item['fileVersion']:
                    continue
            except (OSError, ValueError):
                continue
            item.update(state='pending', errorCode='', message='连接已恢复。')
            self.store.save('items', item)
