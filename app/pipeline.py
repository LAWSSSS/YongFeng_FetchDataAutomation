"""Persistent continuous intent; existing managers still own all media work."""
import asyncio
import copy
import hashlib
import json
import math

from .domain import now_iso
from .videos import Conflict
from .workspace_files import compatible


class PipelineManager:
    def __init__(self, store, workspace, control, videos, slicing):
        self.db, self.workspace, self.control = store.db, workspace, control
        self.videos, self.slicing = videos, slicing
        for table in ('control', 'requests', 'operations', 'events'):
            self.db.execute(f'CREATE TABLE IF NOT EXISTS simple_pipeline_{table} (id TEXT PRIMARY KEY, payload TEXT NOT NULL)')
        self.db.commit()
        self.config = self.get('control', 'control') or {
            'id': 'control', 'revision': 0, 'enabled': False, 'paused': False,
            'assetIds': [], 'pausedStages': {}, 'createdAt': now_iso()}
        self.wake = asyncio.Event()
        self.stopping = False
        self.loop_task = None
        self.operation_tasks = {}
        self.tick_lock = asyncio.Lock()
        workspace.pipeline = self
        videos.on_saved = self.notify_saved
        videos.prepare_recovery_event = self.prepare_recovery_event
        videos.on_media_failure = self.notify_media_failure
        videos.automatic_operation_allowed = self.automatic_operation_allowed

    def get(self, table, identity):
        assert table in {'control', 'requests', 'operations', 'events'}
        row = self.db.execute(f'SELECT payload FROM simple_pipeline_{table} WHERE id=?', (identity,)).fetchone()
        return json.loads(row[0]) if row else None

    def rows(self, table):
        assert table in {'control', 'requests', 'operations', 'events'}
        return [json.loads(row[0]) for row in self.db.execute(f'SELECT payload FROM simple_pipeline_{table} ORDER BY rowid')]

    def save(self, table, row):
        assert table in {'control', 'requests', 'operations', 'events'}
        row['updatedAt'] = now_iso()
        self.db.execute(f'INSERT INTO simple_pipeline_{table} VALUES(?,?) ON CONFLICT(id) DO UPDATE SET payload=excluded.payload',
                        (row['id'], json.dumps(row, ensure_ascii=False)))
        self.db.commit()

    def active(self):
        return bool(self.config.get('enabled') and not self.config.get('paused'))

    @property
    def paused(self):
        return bool(self.config.get('paused'))

    def allowed_asset(self, asset):
        return asset.get('id') in self.config.get('assetIds', [])

    def _groups(self):
        return self.control.state(include_completed=True, include_cleanup_actions=False)['groups']

    def _final_groups(self, groups):
        """Read-only current-version overlay; scheduling still uses media work."""
        flow = getattr(self.videos, 'final_processing', None)
        if not flow:
            return groups
        indexed = {}
        for group in groups:
            row = self.videos.store.get(group.get('videoId', '')) if group.get('videoId') else None
            linked = group.get('items', [])
            if row and any('collectionRevision' in item for item in linked):
                current = [item for item in linked if item.get('collectionRevision') == row.get('activeRevision')
                           and item.get('mediaGeneration', 0) == group.get('generation', 0)]
                if not current:
                    continue  # Old results remain in history, never in the new version's active queue.
                if len(current) != len(linked):
                    ids = {item['id'] for item in current}
                    group = {**group, 'items': current, 'packages': [p for p in group.get('packages', []) if p.get('itemId') in ids]}
                    group['nextAction'] = self.control.next_action(group)
            indexed[group['id']] = group
        for operation in flow.presentations():
            identity = 'collection:' + operation['videoId']
            old = indexed.get(identity)
            if old and (old.get('generation', 0) != operation.get('mediaGeneration', 0)
                        or old.get('videoId') != operation['videoId']):
                continue
            group = {**(old or {}), 'id': identity, 'videoId': operation['videoId'],
                     'name': (old or {}).get('name') or operation.get('name', ''),
                     'generation': operation.get('mediaGeneration', 0),
                     'processingEntry': (old or {}).get('processingEntry') or '/?tab=videos&videoId=' + operation['videoId'],
                     'packages': operation['packages'], 'recordHidden': operation['recordHidden'],
                     'version': operation['version'], 'updatedAt': operation['updatedAt'],
                     'confirmedAt': operation.get('confirmedAt'),
                     'hasResultRecord': operation['hasResultRecord'] or bool((old or {}).get('items'))}
            base = (old or {}).get('nextAction') or (self.control.next_action(old) if old else {})
            kind = {'queued': 'automatic', 'running': 'automatic', 'waiting': 'waiting',
                    'attention': 'manual', 'complete': 'complete'}.get(operation.get('state'), 'manual')
            stage, reason = operation['stageLabel'], operation.get('message') or ''
            # Explicit pauses are live controls and can be newer than the last
            # final-flow tick. A read must never conceal or resume those pauses.
            wait = flow.wait_reason(operation['videoId']) if kind != 'complete' else ''
            issue = operation.get('issue')
            blocker = operation.get('environmentBlocker')
            if wait and not issue:
                kind, stage, reason = 'waiting', '已暂停', wait
            elif not issue and kind != 'complete' and base.get('kind') == 'waiting' and base.get('stage') == '照片上传已暂停':
                kind, stage, reason = 'waiting', base['stage'], base.get('reason') or reason
            action = {**base, 'kind': kind, 'stage': stage, 'reason': reason,
                      'issue': issue, 'environmentBlocker': blocker,
                      'canContinue': False, 'canPause': bool(old and old.get('items')),
                      'progress': operation.get('progress'),
                      'selections': base.get('selections', []), 'packages': base.get('packages', []),
                      'versionSelections': base.get('versionSelections', [])}
            action['action'] = ('open' if kind == 'manual' or (not old and kind != 'complete') else
                                base.get('action', 'none') if kind == 'waiting' else 'none')
            if kind == 'waiting' and action['action'] == 'resume':
                action['canContinue'] = base.get('canContinue', False)
            group['nextAction'] = action
            indexed[identity] = group
        return list(indexed.values())

    def state(self, *, page=1, page_size=20, status='all', search=''):
        from .availability import connection_state
        groups = self._final_groups(self._groups())
        hidden_videos = self.videos.record_visibility.hidden()
        # Derive this from effective versions before pagination; historical
        # attempts and the current page must not change the global pause notice.
        pending_photos = {}
        photo_videos = set()
        photo_paused = bool(self.slicing.packages.paused)
        pending_states = {'queued', 'uploaded', 'uploading'} if photo_paused else {'queued', 'uploaded'}
        for group in groups:
            for package in group.get('packages', []):
                if not package.get('superseded') and package.get('state') in pending_states:
                    pending_photos[package['id']] = package
                    photo_videos.add(group['id'])
        photo_uploads = {'paused': photo_paused,
                         'queued': len(pending_photos), 'videos': len(photo_videos),
                         'images': sum(int(p.get('imageCount') or 0) for p in pending_photos.values())}
        items, counts = [], dict(processing=0, waiting=0, attention=0, complete=0)
        background_counts = dict(processing=0, waiting=0, attention=0, complete=0)
        for group in groups:
            action = group.get('nextAction') or self.control.next_action(group)
            if action.get('canContinue') and action['kind'] == 'automatic' and not self.active():
                action = {**action, 'kind': 'manual', 'stage': '可以继续'}
            state = {'automatic': 'processing', 'waiting': 'waiting', 'manual': 'attention', 'complete': 'complete'}[action['kind']]
            if group.get('recordHidden'):
                background_counts[state] += 1
                continue
            counts[state] += 1
            if status != 'all' and state != status:
                continue
            needle = search.strip().casefold()
            if needle and needle not in (group.get('name', '') + ' ' + action.get('reason', '')).casefold():
                continue
            items.append({'id': group['id'], 'mediaId': group['id'], 'generation': group.get('generation', 0),
                          'videoId': group.get('videoId'), 'name': group.get('name') or '',
                          'version': group.get('version'), 'updatedAt': group.get('updatedAt'),
                          'confirmedAt': group.get('confirmedAt'), 'packages': group.get('packages', []),
                          'hasResultRecord': group.get('hasResultRecord', bool(group.get('items'))),
                          'state': state, 'stage': action.get('stage') or '',
                          'message': action.get('reason') or '', 'nextAction': action,
                          'processingEntry': None if group.get('videoId') in hidden_videos else group.get('processingEntry')})
        priority = {'attention': 0, 'processing': 1, 'waiting': 2, 'complete': 3}
        items.sort(key=lambda item: (priority[item['state']], item['name'].casefold(), item['mediaId']))
        total = len(items)
        page_size = min(20, max(1, page_size))
        pages = max(1, math.ceil(total / page_size))
        page = min(max(1, page), pages)
        message = ('持续自动处理已暂停。' if self.paused else
                   '等待原磁盘恢复，已登记进度保留。' if self.active() and not self.workspace.online else
                   '新确认下载的视频将自动处理。' if self.active() else '开启后，已确认下载的视频会持续自动处理。')
        unavailable = any(not state['authenticated'] or state['reachability'] == 'offline'
                          for state in (connection_state(self.slicing.auth), connection_state(self.videos.target)))
        return {'enabled': bool(self.config.get('enabled')), 'paused': self.paused,
                'revision': self.config['revision'], 'workspaceOnline': bool(self.workspace.online),
                'offlineWaiting': bool(not counts['processing'] and counts['waiting'] and
                                       (not self.workspace.online or unavailable)),
                'message': message, 'counts': counts, 'backgroundCounts': background_counts, 'photoUploads': photo_uploads,
                'total': total, 'page': page, 'pages': pages,
                'pageSize': page_size, 'items': items[(page - 1) * page_size:page * page_size]}

    def change(self, action, data):
        old = self.get('requests', data['requestId'])
        signature = json.dumps({'action': action, **data}, sort_keys=True)
        if old:
            if old.get('signature') != signature:
                raise Conflict('请求编号已用于其他自动处理操作。')
            return self.state()
        if data['expectedRevision'] != self.config['revision']:
            raise Conflict('自动处理设置已变化，请刷新后重试。')
        if action in {'resume', 'pause'} and not self.config.get('enabled'):
            raise Conflict('尚未开启持续自动处理，请先开启。')
        if action in {'start', 'resume'}:
            if not self.workspace.enabled:
                raise ValueError('请先在设置中启用视频工作区。')
            if not self.slicing.store.settings.get('localExport', True):
                raise Conflict('持续自动处理只用于本地识别和照片导入，请先切换本地处理方式。')
            next_config = copy.deepcopy(self.config)
            if action == 'start':
                ids = [asset['id'] for asset in self.workspace._targets()
                       if not self.workspace.is_preserved(asset)]
                next_config['assetIds'] = list(dict.fromkeys([*self.config.get('assetIds', []), *ids]))
            stages = next_config.pop('pausedStages', {})
            run = self.slicing.store.get('runs', self.slicing.store.settings.get('latestRunId')) or self.slicing.run
            if stages.get('slicing') and self.slicing.can_resume(run):
                # resume() checks the run revision, locks and stopping state
                # before changing enabled. Let those checks fail before touching
                # the pipeline configuration, photo pause or download events.
                self.slicing.resume('pipeline-resume-' + hashlib.sha256(data['requestId'].encode()).hexdigest()[:24])
            if action == 'start':
                # Reconcile saved versions once; only accepted files create events.
                for row in self.videos.store.list():
                    self.notify_saved(row['id'], row.get('activeRevision'))
            if stages.get('packages'):
                self.slicing.packages.resume()
            next_config.update(enabled=True, paused=False)
            self.config = next_config
        elif action == 'pause':
            if not self.paused:
                self.config.update(paused=True, pausedStages={
                    'slicing': bool(self.slicing.store.settings.get('enabled')),
                    'packages': not self.slicing.packages.paused})
            for identity, task in list(self.operation_tasks.items()):
                if (self.get('operations', identity) or {}).get('automatic'):
                    task.cancel()
            for video in self.videos.store.list():
                current = self.videos.version(video, video.get('activeRevision'))
                task = self.videos.running.get(video['id'])
                if (current or {}).get('finalConfirmation') and task and not task.done():
                    # The download worker's existing cancellation path persists
                    # its byte checkpoint before releasing the write lease.
                    task.cancel()
            for event in self.rows('events'):
                if event.get('kind') == 'recover-decodable' and event.get('operationId'):
                    task = self.videos.operation_tasks.get(event['operationId'])
                    if task and not task.done():
                        task.cancel()
            if self.config['pausedStages']['slicing']:
                self.slicing.pause()
            if self.config['pausedStages']['packages']:
                self.slicing.packages.pause()
        else:
            raise ValueError('未知自动处理操作。')
        self.config['revision'] += 1
        self.save('control', self.config)
        self.save('requests', {'id': data['requestId'], 'signature': signature, 'action': action})
        self.wake.set()
        self.workspace.wake.set()
        return self.state()

    def notify_saved(self, video_id, revision):
        row = self.videos.store.get(video_id)
        version = self.videos.version(row, revision) if row else None
        if (not row or row.get('deletedAt') or row.get('activeRevision') != revision
                or row.get('acceptedRevision') != revision or not version or version.get('state') != 'saved'
                or version.get('localCleanupState') == 'complete'
                or not version.get('filePath') or not version.get('fileVersion')):
            return
        generation = int((self.control.get('collection:' + video_id) or {}).get('generation') or 0)
        source = {'videoId': video_id, 'version': revision, 'expectedRevision': row.get('revision'),
                  'mediaGeneration': generation, 'fileVersion': copy.deepcopy(version['fileVersion']),
                  'source': {key: version.get(key) for key in ('taskId', 'resultId')}}
        identity = hashlib.sha256(json.dumps(source, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
        if not self.get('events', identity):
            self.save('events', {**source, 'id': identity, 'state': 'pending'})
        self.wake.set()
        self.workspace.wake.set()

    @staticmethod
    def _recovery_source(version):
        return {key: version.get(key) for key in ('taskId', 'resultId', 'etag', 'totalBytes', 'receivedBytes')}

    def prepare_recovery_event(self, row, version):
        """Only a newly failed, explicitly approved full download creates intent.

        The caller stores this marker with the failure in the same video save.
        Existing attention records are never enrolled by scanning history.
        """
        if (not self.active() or not self.workspace.enabled
                or not self.slicing.store.settings.get('localExport', True)
                or row.get('deletedAt') or row.get('deletion', {}).get('state') in {'pending', 'error'}
                or row.get('activeRevision') != version.get('revision')
                or version.get('state') != 'failed'
                or version.get('errorCode') not in {'invalid_video', 'media_incomplete'}
                or not version.get('downloadRequested') or version.get('recovery')
                or not version.get('taskId') or not version.get('resultId')):
            return None
        size, received, etag = version.get('totalBytes'), version.get('receivedBytes'), version.get('etag')
        if (type(size) is not int or size <= 0 or type(received) is not int or received != size
                or not isinstance(etag, str) or not etag or etag.startswith('W/')):
            return None
        control = self.control.get('collection:' + row['id']) or {}
        if control.get('paused') or control.get('blocked'):
            return None
        source = self._recovery_source(version)
        old = version.get('automaticRecoveryEvent')
        if old:
            return old if (old.get('expectedRevision') == row.get('revision')
                           and old.get('source') == source) else None
        if any(op.get('kind') == 'recover-decodable' and op.get('version') == version['revision']
               for op in self.videos.store.list_operations(row['id'])):
            return None
        content = {'videoId': row['id'], 'version': version['revision'],
                   'expectedRevision': row['revision'], 'mediaGeneration': control.get('generation', 0),
                   'source': source}
        digest = hashlib.sha256(json.dumps(content, sort_keys=True).encode()).hexdigest()[:40]
        return {**content, 'id': 'recovery-' + digest, 'kind': 'recover-decodable', 'createdAt': now_iso()}

    def notify_media_failure(self, video_id, revision):
        row = self.videos.store.get(video_id)
        version = self.videos.version(row, revision) if row else None
        marker = (version or {}).get('automaticRecoveryEvent') or {}
        if (marker.get('kind') != 'recover-decodable' or marker.get('videoId') != video_id
                or marker.get('version') != revision or not marker.get('id')):
            return
        if not self.get('events', marker['id']):
            self.save('events', {**marker, 'state': 'pending'})
        self.wake.set()

    def automatic_operation_allowed(self, operation):
        if not operation or operation.get('kind') != 'recover-decodable':
            return True
        owned = next((event for event in self.rows('events')
                      if event.get('kind') == 'recover-decodable'
                      and event.get('operationId') == operation.get('id')), None)
        if not owned:
            return True
        control = self.control.get('collection:' + owned['videoId']) or {}
        return bool(self.active() and not control.get('paused') and not control.get('blocked'))

    def _enroll_recoveries(self):
        if not self.active():
            return
        for event in self.rows('events'):
            if event.get('kind') != 'recover-decodable' or event.get('state') != 'pending':
                continue
            row = self.videos.store.get(event['videoId'])
            version = self.videos.version(row, event['version']) if row else None
            control = self.control.get('collection:' + event['videoId']) or {}
            if (not row or row.get('deletedAt') or row.get('revision') != event['expectedRevision']
                    or row.get('activeRevision') != event['version'] or not version
                    or version.get('automaticRecoveryEvent', {}).get('id') != event['id']
                    or self._recovery_source(version) != event.get('source')
                    or not version.get('downloadRequested')
                    or control.get('generation', 0) != event.get('mediaGeneration', 0)):
                event.update(state='superseded', message='采集版本、来源或处理指令已变化，未自动恢复旧版。')
                self.save('events', event)
                continue
            if control.get('paused') or control.get('blocked'):
                continue
            existing = next((op for op in self.videos.store.list_operations(row['id'])
                             if op.get('kind') == 'recover-decodable' and op.get('version') == event['version']), None)
            if existing or version.get('recovery'):
                event.update(state='handled', message='已有恢复操作，保持当前进度与人工决定。')
                if existing:
                    event['existingOperationId'] = existing['id']
                self.save('events', event)
                continue
            if (version.get('state') != 'failed'
                    or version.get('errorCode') not in {'invalid_video', 'media_incomplete'}):
                event.update(state='superseded', message='当前异常已变化，未自动处理。')
                self.save('events', event)
                continue
            request_id = 'automatic-' + event['id']
            # Persist ownership before queue_video_operation launches its task.
            event['operationId'] = self.videos.operation_identity(row['id'], 'recover-decodable', request_id)
            self.save('events', event)
            try:
                result = self.videos.queue_video_operation(row['id'], 'recover-decodable', {
                    'requestId': request_id, 'version': event['version'], 'expectedRevision': event['expectedRevision']})
                event.update(state='enrolled', operationId=result['operationId'])
            except Exception as error:
                # One event is one attempt. A failed recovery remains explicit;
                # polling must never create an unlimited automatic retry loop.
                event.update(state='attention', failureType=type(error).__name__,
                             message='自动恢复未登记，原缓存保留，请查看此视频。')
            self.save('events', event)

    def _enroll_downloads(self):
        assets = self.workspace.rows('assets')
        changed = False
        for event in self.rows('events'):
            if event.get('kind') == 'recover-decodable' or event.get('state') != 'pending':
                continue
            row = self.videos.store.get(event['videoId'])
            version = self.videos.version(row, event['version']) if row else None
            if (not row or row.get('activeRevision') != event['version'] or row.get('deletedAt')
                    or (version or {}).get('localCleanupState') == 'complete'):
                event['state'] = 'superseded'
                self.save('events', event)
                continue
            if row.get('acceptedRevision') != event['version'] or not version or version.get('state') != 'saved':
                continue
            generation = int((self.control.get('collection:' + row['id']) or {}).get('generation') or 0)
            if (event.get('mediaGeneration') != generation or event.get('expectedRevision') != row.get('revision')
                    or not event.get('fileVersion')
                    or not compatible(version.get('fileVersion') or {}, event['fileVersion'])
                    or not compatible(event['fileVersion'], version.get('fileVersion') or {})
                    or any(version.get(key) != value for key, value in event.get('source', {}).items())):
                event['state'] = 'superseded'
                self.save('events', event)
                if 'mediaGeneration' not in event:
                    self.notify_saved(row['id'], event['version'])
                continue
            matched = [asset for asset in assets if asset.get('path') == version.get('filePath')
                       and compatible(asset.get('fileVersion') or {}, version.get('fileVersion') or {})
                       and not self.workspace.is_preserved(asset)]
            if len(matched) != 1:
                continue
            identity = matched[0]['id']
            if identity not in self.config['assetIds']:
                self.config['assetIds'].append(identity)
                changed = True
            event.update(state='enrolled', assetId=identity)
            self.save('events', event)
        if changed:
            self.save('control', self.config)
        return changed

    def enqueue(self, data, *, automatic=False):
        signature = json.dumps(data, sort_keys=True, separators=(',', ':'))
        identity = hashlib.sha256(data['requestId'].encode()).hexdigest()[:40]
        old = self.get('operations', identity)
        if old:
            if old.get('signature') != signature:
                raise Conflict('请求编号已用于其他视频范围。')
            return {'operationId': identity, 'state': old['state']}
        selections = data['selections']
        if len({row['mediaId'] for row in selections}) != len(selections):
            raise ValueError('同一视频不能重复登记。')
        operation = {'id': identity, 'state': 'queued', 'signature': signature,
                     'selections': selections, 'outcomes': [], 'automatic': automatic,
                     'createdAt': now_iso()}
        self.save('operations', operation)
        self.launch_operation(identity)
        return {'operationId': identity, 'state': 'queued'}

    def operation(self, identity):
        row = self.get('operations', identity)
        if not row:
            raise KeyError(identity)
        return {'operationId': identity, 'state': row['state'], 'outcomes': row['outcomes'],
                'total': len(row['selections']), 'finished': len(row['outcomes']),
                'createdAt': row.get('createdAt'), 'updatedAt': row.get('updatedAt')}

    def launch_operation(self, identity):
        if self.stopping or (identity in self.operation_tasks and not self.operation_tasks[identity].done()):
            return
        task = asyncio.create_task(self._run_operation(identity), name='pipeline-operation-' + identity)
        self.operation_tasks[identity] = task
        def finished(done):
            if self.operation_tasks.get(identity) is done:
                self.operation_tasks.pop(identity, None)
            if not done.cancelled() and done.exception():
                row = self.get('operations', identity)
                if row:
                    row.update(state='attention', failureType=type(done.exception()).__name__)
                    self.save('operations', row)
        task.add_done_callback(finished)

    async def _run_operation(self, identity):
        row = self.get('operations', identity)
        row['outcomes'] = list({entry['mediaId']: entry for entry in row.get('outcomes', [])}.values())
        row['state'] = 'running'
        self.save('operations', row)
        for index, choice in enumerate(row['selections']):
            if any(outcome['mediaId'] == choice['mediaId'] for outcome in row['outcomes']):
                continue
            if row.get('automatic') and not self.active():
                row['state'] = 'queued'
                self.save('operations', row)
                return
            request_id = 'pipeline-' + identity[:24] + '-' + str(index)
            try:
                if choice.get('action') in {'pause', 'resume'}:
                    await self.control.pause_media(choice['mediaId'], {'requestId': request_id,
                        'generation': choice['generation'], 'selections': choice.get('selections', []),
                        'packages': choice.get('packages', [])}, paused=choice['action'] == 'pause')
                    outcome = {'mediaId': choice['mediaId'], 'ok': True,
                               'message': '本视频已暂停。' if choice['action'] == 'pause' else '本视频已继续。'}
                elif choice.get('action') == 'archive':
                    await self.control.action(choice['mediaId'], {'action': 'retry-archive',
                        'requestId': request_id, 'generation': choice['generation'],
                        'fileId': choice['fileId'], 'itemId': choice['itemId'],
                        'expectedRevision': choice['expectedRevision']})
                    outcome = {'mediaId': choice['mediaId'], 'ok': True, 'message': '已登记归档核对。'}
                else:
                    result = await self.control.continue_safe(choice['mediaId'], {
                        'requestId': request_id, 'generation': choice['generation'],
                        'selections': choice.get('selections', []), 'packages': choice.get('packages', [])})
                    failures = [entry.get('message') or '本条需要核对。' for entry in result['outcomes'] if not entry.get('ok')]
                    outcome = {'mediaId': choice['mediaId'], 'ok': not failures,
                               'message': '；'.join(failures) if failures else '已登记，后台继续处理。'}
            except asyncio.CancelledError:
                row['state'] = 'queued'
                self.save('operations', row)
                raise
            except Exception as error:
                message = str(error) if isinstance(error, (Conflict, ValueError)) else '本条暂未继续，记录和进度保留。'
                if any(word in message.lower() for word in ('http://', 'https://', 'token', 'password', 'secret')):
                    message = '本条暂未继续，请查看当前视频状态。'
                outcome = {'mediaId': choice['mediaId'], 'ok': False, 'message': message}
            row['outcomes'] = [entry for entry in row['outcomes'] if entry['mediaId'] != choice['mediaId']] + [outcome]
            self.save('operations', row)
            await asyncio.sleep(0)
        row['state'] = 'complete' if all(outcome['ok'] for outcome in row['outcomes']) else 'attention'
        self.save('operations', row)
        self.wake.set()

    async def tick(self):
        if not self.active() or not self.workspace.enabled or self.workspace.maintenance:
            return
        async with self.tick_lock:
            if self._enroll_downloads():
                self.workspace.wake.set()
            if not self.workspace.online:
                return
            self._enroll_recoveries()
            for operation in self.rows('operations'):
                if operation.get('state') in {'queued', 'running'}:
                    self.launch_operation(operation['id'])
            eligible = set(self.config.get('assetIds', []))
            paths = {asset['path'] for asset in self.workspace.rows('assets') if asset['id'] in eligible}
            for group in self.control.execution_groups(paths):
                action = group.get('nextAction') or self.control.next_action(group)
                if not action.get('autoEligible') or not action.get('canContinue'):
                    continue
                choice = {'mediaId': group['id'], 'generation': group.get('generation', 0),
                          'selections': action.get('selections', []), 'packages': action.get('packages', [])}
                signature = hashlib.sha256(json.dumps(choice, sort_keys=True).encode()).hexdigest()[:40]
                self.enqueue({'requestId': 'automatic-' + signature, 'selections': [choice]}, automatic=True)

    async def startup(self):
        self.stopping = False
        for row in self.videos.store.list():
            # Only durable, newly-created failure markers are replayed. Older
            # failed rows without a marker remain untouched for manual review.
            self.notify_media_failure(row['id'], row.get('activeRevision'))
        if self.active():
            for row in self.videos.store.list():
                self.notify_saved(row['id'], row.get('activeRevision'))
        for row in self.rows('operations'):
            if row.get('state') in {'queued', 'running'} and (not row.get('automatic') or self.active()):
                self.launch_operation(row['id'])
        self.loop_task = asyncio.create_task(self.loop(), name='continuous-pipeline')

    async def loop(self):
        while not self.stopping:
            self.wake.clear()
            try:
                await self.tick()
            except asyncio.CancelledError:
                raise
            except Exception:
                # Stored scope and per-operation receipts remain the authority.
                pass
            try:
                await asyncio.wait_for(self.wake.wait(), timeout=5)
            except TimeoutError:
                pass

    async def shutdown(self):
        self.stopping = True
        tasks = [self.loop_task, *self.operation_tasks.values()]
        for task in tasks:
            if task and not task.done():
                task.cancel()
        await asyncio.gather(*(task for task in tasks if task), return_exceptions=True)
