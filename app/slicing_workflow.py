"""Durable one-click intent and recovery, separate from platform submission."""
import asyncio
import hashlib
from uuid import uuid4
from .domain import now_iso
from .videos import Conflict


class SlicingWorkflow:
    def pending_analysis(self, item, *, due=True):
        """Unsent category work survives unrelated upload/root-state callbacks."""
        if (item.get('superseded') or item.get('localCleanupState') == 'complete'
                or item.get('collectionRecoveryPaused') or item.get('pipelinePaused')
                or item.get('state') not in {'pending', 'platform_processing', 'attention'}):
            return []
        if item.get('state') == 'attention' and item.get('errorCode') not in {
                'bed_model_missing', 'magnet_model_missing', 'model_load_failed',
                'bed_resolution', 'video_resolution', 'bed_alignment_uncertain', 'inference_failed',
                'inference_environment_missing', 'frame_timeout', 'worker_stalled',
                'observations_incomplete', 'no_decodable_samples', 'no_cuttable_fragments', 'slice_limit'}:
            return []
        pending = []
        for name, category in item.get('categories', {}).items():
            if (category.get('state') != 'pending'
                    or any(category.get(key) for key in ('taskId', 'operationId', 'packageId'))
                    or (due and category.get('retryNotBefore', 0) > self.clock())):
                continue
            if category.get('waitCode') in {'bed_model_missing', 'magnet_model_missing'}:
                from pathlib import Path
                model = self.store.settings.get(name + 'ModelPath')
                if not model or not Path(model).is_file():
                    continue
            pending.append(name)
        return pending

    def defer_occupied_file(self, item):
        """A move/write lease is waiting, not a failed model or retry attempt."""
        for category in item.get('categories', {}).values():
            if category.get('state') == 'pending' and not any(category.get(key) for key in ('taskId', 'operationId', 'packageId')):
                category.update(retryNotBefore=max(category.get('retryNotBefore', 0), self.clock() + .5),
                                waitCode='file_busy', recoveryDisposition='waiting',
                                message='等待文件整理结束后继续识别。')
        item.update(state='pending', errorCode='', message='等待文件整理结束后继续识别。')
        if '_analysisOwned' in item:
            self._save_analysis_item(item)
        else:
            self.store.save('items', item)

    def continue_pending(self, identity, expected_revision, request_id):
        """Resume journaled pending work without retrying completed/submitted slots."""
        previous = self.store.get('requests', request_id)
        if previous:
            if previous.get('kind') != 'continue-pending' or previous.get('itemId') != identity:
                raise Conflict('此请求编号已用于其他操作。')
            return
        item = self.store.get('items', identity)
        self._revision(item, expected_revision)
        if not self.pending_analysis(item, due=False):
            raise Conflict('没有可继续的未提交类别，请刷新后查看。')
        if getattr(self, 'control', None) and not self.control.allowed(item):
            raise Conflict(self.control.reason(item))
        # Deadlines and retry budgets remain intact. In particular a click
        # cannot skip a 5/15 second backoff or revive a verified photo package.
        item.update(state='pending', errorCode='', message='已继续未完成类别，已有成果保留。',
                    revision=int(item.get('revision', 0)) + 1)
        self.store.save('items', item)
        self.store.save('requests', {'id': request_id, 'kind': 'continue-pending', 'itemId': identity, 'state': 'queued'})
        self._wake_scheduler()

    def queue_workspace_sources(self, sources, request_id):
        """Analyze registered local originals and upload only the resulting photos."""
        if not self.store.settings.get('localExport', True):
            raise Conflict('当前使用平台切片，需等待原片导入。')
        run = self.store.get('runs', self.store.settings.get('latestRunId')) or self.run
        if run and run.get('state') == 'paused' and not self.store.settings.get('enabled'):
            raise Conflict('本地切片已暂停，请点继续。')
        if self.store.settings.get('enabled') and run and (
                run.get('state') == 'scanning' or run.get('pilotMode') or
                (run.get('singleMode') and not run.get('localRecoveryOnly'))):
            raise Conflict('等待当前扫描或单条操作结束后继续本地识别。')
        targets = self.store.settings.get('targetDatasetIds') or {}
        if not all(str(targets.get(key) or '').isdigit() for key in ('27912', '27944')):
            raise Conflict('等待车斗和磁盘吊图片目标配置完成。')
        identities = []
        for source in sources:
            if source.get('existingItemIds'):
                existing = self._effective_items([item for identity in source['existingItemIds']
                    if (item := self.store.get('items', identity))])
                existing = [item for item in existing if not item.get('pipelinePaused') and not item.get('collectionRecoveryPaused')
                            and (not getattr(self, 'control', None) or self.control.allowed(item))]
                for current in existing:
                    if (source.get('finalProcessingIntent') and current.get('collectionVideoId') == source.get('collectionVideoId')
                            and current.get('collectionRevision') == source.get('collectionRevision')
                            and current.get('fileVersion') == source.get('fileVersion')):
                        current['finalProcessingIntent'] = source['finalProcessingIntent']
                        self.store.save('items', current)
                present = {name for item in existing for name, category in item.get('categories', {}).items()
                           if category.get('state') not in {None, 'not_applicable'}}
                missing = {'bed', 'magnet'} - present
                if missing and existing:
                    owner = existing[0]
                    allowed = not getattr(self, 'control', None) or self.control.allowed(owner)
                    if allowed and owner.get('fileVersion') == source.get('fileVersion'):
                        assigned = {name for name, slot in owner.get('categories', {}).items()
                                    if slot.get('state') not in {None, 'not_applicable'}} | missing
                        for name in missing:
                            dataset = '27912' if name == 'bed' else '27944'
                            owner.setdefault('categories', {})[name] = {'state': 'pending', 'intervals': [],
                                'targetDatasetId': str(targets[dataset]), 'message': '补充本视频尚未处理的类别。'}
                        owner.update(localWorkspace=True, localWorkspaceCategories=sorted(assigned), state='pending',
                                     errorCode='', revision=int(owner.get('revision', 0)) + 1)
                        self.store.save('items', owner)
                identities.extend(item['id'] for item in existing)
                continue
            identity = hashlib.sha256(('workspace:' + source['id']).encode()).hexdigest()[:32]
            item = self.store.get('items', identity)
            if not item:
                categories = {name: {'state': 'pending', 'intervals': [], 'observations': 0,
                    'estimatedImages': None, 'actualImages': None, 'targetDatasetId': str(targets[dataset]),
                    'taskId': '', 'intervalSeconds': None, 'message': '等待本地识别。'}
                    for name, dataset in (('bed', '27912'), ('magnet', '27944'))}
                item = {'id': identity, 'runId': '', 'workspaceAssetId': source['id'],
                    'sourceIdentity': 'workspace:' + source['id'], 'sourceType': 'local_workspace',
                    'localWorkspace': True, 'sourceDatasetId': '27912', 'sourceVideoId': '', 'remote': {},
                    'name': source['name'], 'path': source['path'], 'fileVersion': source['fileVersion'],
                    'observationVersion': source['fileVersion'], 'metadata': {}, 'categories': categories,
                    'state': 'pending', 'errorCode': '', 'message': '等待本地识别及照片导入。',
                    'revision': 0, 'settingsRevision': self.store.settings['revision'],
                    'targetResolved': True, 'progress': 0, 'createdAt': now_iso()}
                if source.get('collectionVideoId'):
                    item.update(collectionVideoId=source['collectionVideoId'],
                                collectionRevision=source['collectionRevision'])
                    if source.get('finalProcessingIntent'):
                        item['finalProcessingIntent'] = source['finalProcessingIntent']
                self.store.save('items', item)
            identities.append(identity)
        if getattr(self, 'control', None):
            self.control.refresh()
        actionable = [identity for identity in identities if
                      (self.store.get('items', identity) or {}).get('state') not in
                      {'complete', 'no_target', 'uncuttable', 'attention', 'superseded', 'excluded'}
                      and not (self.store.get('items', identity) or {}).get('pipelinePaused')
                      and not (self.store.get('items', identity) or {}).get('collectionRecoveryPaused')
                      and (not getattr(self, 'control', None) or self.control.allowed(self.store.get('items', identity)))]
        needs_attach = actionable and (not self.store.settings.get('enabled') or not run
                                      or not set(actionable).issubset(run.get('items', [])))
        if needs_attach:
            if not self.store.settings.get('enabled') or not run:
                run = {'id': 'workspace-local-' + uuid4().hex, 'requestId': request_id,
                       'state': 'running', 'items': [], 'settingsRevision': self.store.settings['revision'],
                       'executeAll': True, 'localRecoveryOnly': True, 'singleMode': False,
                       'createdAt': now_iso()}
            run.update(state='running', singleMode=False, executeAll=True,
                       message='本地识别及照片导入进行中。')
            run.pop('singleItemId', None)
            run['items'] = list(dict.fromkeys([*run.get('items', []), *actionable]))
            run['itemCount'] = len(run['items'])
            for identity in actionable:
                item = self.store.get('items', identity)
                item['runId'] = run['id']
                self.store.save('items', item)
            self.run = run
            self.store.save('runs', run)
            self.store.settings.update(enabled=True, latestRunId=run['id'])
            self.store.save_settings()
            self._wake_scheduler()
        if not self.store.get('requests', request_id):
            self.store.save('requests', {'id': request_id, 'kind': 'workspace-local',
                                         'itemIds': identities, 'state': 'accepted'})
        return identities

    async def execute_all(self, request_id, expected_revision, source_scope=None):
        async with self.start_lock:
            old = self.store.get('requests', request_id)
            if old:
                if old.get('kind') != 'execute-all':
                    raise Conflict('请求编号已用于其他操作。')
                if old.get('sourceScope') != source_scope:
                    raise Conflict('请求编号已用于其他视频范围。')
                return self.state()
            if expected_revision != self.store.settings['revision']:
                raise Conflict('设置已更新，请刷新后重试。')
            # A refresh scan must not write stale copies over a running worker.
            # Coalesce clicks into one durable follow-up scan instead.
            workflow = self.store.settings.get('workflow') or {}
            workflow.update(enabled=True, phase='scan', generation=workflow.get('generation', 0) + 1, requestedAt=now_iso(),
                            settingsRevision=expected_revision, requestId=request_id,
                            sourceScope=source_scope)
            self.store.settings['workflow'] = workflow
            self.store.settings['enabled'] = True
            self.store.save_settings()
            self.store.save('requests', {'id': request_id, 'kind': 'execute-all', 'state': 'accepted',
                                        'receiptRunId': self.store.settings.get('latestRunId'),
                                        'receiptItemIds': [i['id'] for i in self.store.list('items')],
                                        'sourceScope': source_scope})
            self._wake_scheduler()
            return self.state()

    async def _workflow_scan(self):
        """Run one refresh between analyses; retain the same batch identity."""
        self._reap_analysis_tasks()
        if (self._active_analysis_tasks() or self.export_tasks or (self.files and self.files.busy)
                or (self.scan_task and not self.scan_task.done())):
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
        # Remote snapshots may overlap local work. Only merge into the same
        # still-authorized run; a new correction must keep its own scope.
        await self._scan(run['id'])
        if self.store.settings.get('latestRunId') != run['id']:
            return False
        self.run = self.store.get('runs', run['id'])
        if not self.run or not self.store.settings.get('enabled') or self.run.get('state') == 'paused':
            return False
        if self.run['state'] == 'complete':
            source_scope = workflow.get('sourceScope')
            if source_scope is not None:
                from .workspace_files import compatible
                selected = []
                for identity in self.run.get('items', []):
                    item = self.store.get('items', identity)
                    expected = source_scope.get(item.get('path')) if item else None
                    if item and (item.get('localWorkspace') or item.get('collectionRecovery') or
                                 (expected and compatible(item.get('fileVersion') or {}, expected))):
                        selected.append(identity)
                self.run.update(items=selected, itemCount=len(selected),
                                message=f'工作区内 {len(selected)} 条视频进入切片队列；其他平台视频未启动。',
                                sourceScopeApplied=True)
                self.store.save('runs', self.run)
            await self._recover_moved_sources()
            if self.store.settings.get('latestRunId') != run['id'] or not self.store.settings.get('enabled'):
                return False
            self.run = self.store.get('runs', run['id']) or self.run
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
            from .file_locations import relocate_conclusions
            relocate_conclusions(self.store.db, item, source, destination, current)
            item.update(path=destination, fileVersion=current, movedAt=now_iso(),
                        revision=int(item.get('revision', 0)) + 1)
            self.store.save('items', item)

    async def process_with_recovery(self, item):
        """Only transient extraction errors retry. Facts and identity do not."""
        expected = (item.get('fileVersion'), item.get('mediaGeneration', 0), item.get('collectionRevision'), item.get('path'))
        try:
            await self._process_item(item)
        except Exception:
            # _process_item journals a sanitized failure before raising. A
            # programming error must not discard other files in an all run.
            current = self.store.get('items', item['id']) or item
            if current.get('superseded') or item.get('_analysisFence', expected) != (
                    current.get('fileVersion'), current.get('mediaGeneration', 0), current.get('collectionRevision'), current.get('path')):
                return current
            current.update(state='attention', errorCode='internal_error', message='本条处理异常，原片和进度保留。')
            self.store.save('items', current)
        current = self.store.get('items', item['id'])
        if not current or current.get('superseded') or item.get('_analysisFence', expected) != (
                current.get('fileVersion'), current.get('mediaGeneration', 0), current.get('collectionRevision'), current.get('path')):
            return current
        return self._recover_analysis_outcome(current)

    def _recover_analysis_outcome(self, current):
        """Recover only unsent category failures, retaining independent budgets."""
        import json
        from pathlib import Path
        from .slicing import applicable_category
        transient = {'worker_stalled', 'frame_extract_failed', 'frame_timeout',
                     'media_check_timeout', 'observations_incomplete'}
        root_code = current.get('errorCode', '')
        failed = [(name, category) for name, category in current['categories'].items()
                  if category.get('state') == 'attention' and not any(category.get(key)
                      for key in ('taskId', 'operationId', 'packageId'))]
        if not failed and current.get('state') == 'attention' and root_code in transient:
            name = applicable_category(current['sourceDatasetId'])
            slot = current['categories'].get(name, {})
            if slot.get('state') == 'pending' and not any(slot.get(key) for key in ('taskId', 'operationId', 'packageId')):
                failed = [(name, slot)]
        if not failed:
            return current
        pending = waiting = False
        legacy_retries = int(current.get('automaticRetries', 0)) if not any(
            'automaticRetryVersion' in slot or 'automaticRetries' in slot for slot in current['categories'].values()) else 0
        for name, category in failed:
            code = category.get('errorCode') or root_code
            paths = [current.get('path') or '/']
            if code in {'magnet_model_missing', 'bed_model_missing'}:
                paths.append(self.store.settings.get(name + 'ModelPath') or '/')
            disk_absent = code in {'local_video_missing', 'disk', 'worker_failed', 'magnet_model_missing',
                'bed_model_missing', 'frame_extract_failed', 'media_incomplete', 'media_check_timeout'} and any(
                len(path.parts) > 2 and path.parts[1] == 'Volumes' and not Path(*path.parts[:3]).is_mount()
                for path in map(Path, paths))
            occupied = self.leases.key(current.get('path') or '') in self.leases.writers
            if disk_absent or code in {'auth', 'network', 'disk'}:
                category.update(state='pending', errorCode='', waitCode=code, retryNotBefore=self.clock() + 30,
                                recoveryDisposition='waiting')
                waiting = True
                continue
            if occupied:
                category.update(state='pending', errorCode='', retryNotBefore=self.clock() + .5,
                                recoveryDisposition='waiting')
                pending = True
                continue
            retry_version = json.dumps({'file': current.get('fileVersion'),
                'generation': current.get('mediaGeneration', 0), 'model': category.get('modelFingerprint')}, sort_keys=True)
            previous_version = category.get('automaticRetryVersion')
            retries = int(category.get('automaticRetries', legacy_retries if previous_version is None else 0))
            if previous_version is not None and previous_version != retry_version:
                retries = 0
            category['automaticRetryVersion'] = retry_version
            if code in transient and retries < 2:
                delay = (5, 15)[retries]
                category.update(state='pending', errorCode='', automaticRetries=retries + 1,
                    retryNotBefore=self.clock() + delay, recoveryDisposition='retry',
                    message=f'{delay}秒后自动重试 {retries + 1}/2。')
                pending = True
            else:
                category.update(recoveryDisposition='manual', automaticRetries=retries)
        if pending:
            current.update(state='pending', errorCode='', message='临时故障已排队自动重试；其他有效结果保留。',
                           recoveryDisposition='retry')
        elif waiting:
            current.update(state='waiting_connection', message='等待所需连接、磁盘或文件恢复。',
                           waitCode=next((c.get('waitCode') for _, c in failed if c.get('waitCode')), root_code),
                           recoveryDisposition='waiting')
        else:
            current['recoveryDisposition'] = 'manual'
        current['automaticRetries'] = max((int(c.get('automaticRetries', 0)) for c in current['categories'].values()), default=0)
        self.store.save('items', current)
        return current

    async def _restore_waiting_files(self, items=None):
        from pathlib import Path
        from .slicing import source_version
        rows = self.store.list('items') if items is None else items
        rows = self._scope_run_items(rows)
        for item in rows:
            if item.get('state') != 'waiting_connection':
                continue
            if any(slot.get('waitStage') == 'export' for slot in item.get('categories', {}).values()):
                continue  # The failed export position needs its own read probe.
            try:
                platform_required = not self.store.settings.get('localExport', True) or item.get('waitCode') in {'auth', 'network'}
                if (platform_required and not self.auth.connected) or await asyncio.to_thread(source_version, item['path']) != item['fileVersion']:
                    continue
            except (OSError, ValueError):
                continue
            latest = self.store.get('items', item['id'])
            if (not latest or latest.get('revision') != item.get('revision')
                    or latest.get('state') != 'waiting_connection'
                    or latest.get('superseded') or latest.get('collectionRecoveryPaused')):
                continue
            item = latest
            for name, category in item.get('categories', {}).items():
                if category.get('waitCode') in {'magnet_model_missing', 'bed_model_missing'}:
                    model = self.store.settings.get(name + 'ModelPath')
                    if not model or not await asyncio.to_thread(Path(model).is_file):
                        break
            else:
                fresh = self.store.get('items', item['id'])
                if not fresh or fresh.get('revision') != item.get('revision') or fresh.get('superseded'):
                    continue
                item = fresh
                for category in item.get('categories', {}).values():
                    if category.get('recoveryDisposition') == 'waiting':
                        category.update(retryNotBefore=0, waitCode='')
                item.update(state='pending', errorCode='', message='所需连接或文件已恢复，继续本地处理。')
                self.store.save('items', item)

    def _start_export_environment_recovery(self):
        """A slow removable-volume check must not hold the scheduling lock."""
        task = getattr(self, 'export_recovery_task', None)
        if (task and not task.done()) or self.clock() < getattr(self, 'export_recovery_next', 0):
            return
        self.export_recovery_next = self.clock() + 30
        task = asyncio.create_task(self._restore_waiting_exports(), name='restore-photo-export-source')
        self.export_recovery_task = task
        def finished(done):
            if not done.cancelled():
                done.exception()
            self.wake.set()
        task.add_done_callback(finished)

    def _export_restore_allowed(self, item):
        return bool(self.store.settings.get('enabled') and not self.stopping
            and not item.get('superseded') and item.get('localCleanupState') != 'complete'
            and not item.get('pipelinePaused') and not item.get('collectionRecoveryPaused')
            and (self.run or {}).get('state') != 'paused'
            and (not getattr(self, 'control', None) or self.control.allowed(item)))

    def _probe_export_source(self, item, category):
        """Retry the failed read, not the whole model/export operation."""
        from .frame_decode import check_source, decode_position
        from .slice_export import slice_timestamps
        from .slicing_core import position_excluded
        from .slicing import ffprobe_file
        with self.leases.read(item['path']):
            check_source(item['path'], item['fileVersion'], edges=True)
            stamp = category.get('exportEnvironmentPosition')
            if stamp is None:
                # Legacy export failures had no separate failure position. Only
                # use their exact saved interval grid, never invent a new one.
                observed = item.get('exportDecodeObservations') or {}
                excluded = (item.get('decodeCoverage') or {}).get('exclusionIntervals') or []
                stamp = next((point for point in slice_timestamps(category['intervals'], category['intervalSeconds'])
                    if str(round(float(point), 6)) not in observed and not position_excluded(point, excluded)), None)
            if stamp is not None:
                metadata = item.get('metadata') or {}
                tool = self.store.settings['frameToolPath']
                if 'streamStartTime' not in metadata:
                    metadata = ffprobe_file(item['path'], tool)
                # No model or image publication here. A stable unusable frame
                # can resume the normal bounded skip path; environment cannot.
                decode_position(item['path'], tool, stamp, metadata, expected=item['fileVersion'], quality=3)

    async def _restore_waiting_exports(self):
        import copy
        import subprocess
        from .file_leases import FileEnvironmentError, ReadProbes
        from .slicing import ERROR_TEXT
        if not hasattr(self, 'export_source_probes'):
            # One actual probe at a time, including a probe whose caller timed
            # out. The read lease lives inside the worker until it really ends.
            self.export_source_probes = ReadProbes(wake=self.wake.set, limit=1, timeout=65)
        for item in self._scope_run_items(self.store.list('items')):
            if not self._export_restore_allowed(item):
                continue
            for name, slot in list(item.get('categories', {}).items()):
                legacy = slot.get('state') == 'attention' and slot.get('errorCode') == 'frame_io_error'
                waiting = slot.get('state') == 'waiting_connection' and slot.get('waitStage') == 'export'
                if (not (legacy or waiting) or not slot.get('intervals') or not slot.get('intervalSeconds')
                        or any(slot.get(key) for key in ('packageId', 'taskId', 'operationId'))
                        or float(slot.get('retryNotBefore') or 0) > self.clock()):
                    continue
                if legacy:
                    slot.update(state='waiting_connection', errorCode='', waitCode='frame_io_error',
                        waitStage='export', recoveryDisposition='waiting',
                        message='正在核对原片读取是否恢复，已有成果保留。')
                    self._save_category_progress(item)
                baseline = (item.get('fileVersion'), item.get('mediaGeneration', 0),
                            item.get('collectionRevision'), item.get('path'))
                slot_before = copy.deepcopy(slot)
                failure = ''
                try:
                    await self.export_source_probes.run((item['id'], name), self._probe_export_source, item, slot)
                except (OSError, FileEnvironmentError):
                    failure = 'frame_io_error'
                except RuntimeError as error:
                    failure = str(error)
                except subprocess.TimeoutExpired:
                    failure = 'frame_tool_error'
                except ValueError:
                    failure = 'video_probe_failed'
                current = self.store.get('items', item['id'])
                if (not current or not self._export_restore_allowed(current)
                        or baseline != (current.get('fileVersion'), current.get('mediaGeneration', 0),
                                        current.get('collectionRevision'), current.get('path'))):
                    continue
                category = current.get('categories', {}).get(name, {})
                if (category.get('state') != 'waiting_connection' or category.get('waitStage') != 'export'
                        or category != slot_before
                        or any(category.get(key) for key in ('packageId', 'taskId', 'operationId'))):
                    continue
                if failure == 'frame_io_error':
                    category.update(retryNotBefore=self.clock() + 30,
                        message='原片读取尚未恢复，自动等待；已有成果保留。')
                elif failure:
                    category.update(state='attention', errorCode=failure, waitStage='', waitCode='',
                        recoveryDisposition='manual', message=ERROR_TEXT.get(failure, '原片或抽帧工具需核对，进度保留。'))
                else:
                    category.update(state='ready', errorCode='', waitCode='', waitStage='', retryNotBefore=0,
                        recoveryDisposition='retry', message='原片读取已恢复，继续未完成类别的照片导出。')
                self._save_category_progress(current)
                item = current
