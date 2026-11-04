"""Version-bound final approval, independent of successful local publication."""
import asyncio
import copy
import time
import hashlib
import json

from .domain import now_iso
from .videos import Conflict
from .workspace_files import compatible
from .photo_completion import photo_results_verified, verified_empty_conclusion


class FinalProcessing:
    def __init__(self, store, videos, pipeline, workspace, control, slicing):
        self.db, self.videos, self.pipeline = store.db, videos, pipeline
        self.workspace, self.control, self.slicing = workspace, control, slicing
        self.db.execute('CREATE TABLE IF NOT EXISTS simple_final_processing (id TEXT PRIMARY KEY, payload TEXT NOT NULL)')
        self.db.execute('CREATE TABLE IF NOT EXISTS simple_final_requests (id TEXT PRIMARY KEY, payload TEXT NOT NULL)')
        self.db.commit()
        self.wake = asyncio.Event()
        self.task = None
        self.stopping = False
        self.clock = time.time
        videos.final_processing = self
        slicing.final_processing = self

    def rows(self):
        return [json.loads(row[0]) for row in self.db.execute('SELECT payload FROM simple_final_processing')]

    def get(self, identity):
        row = self.db.execute('SELECT payload FROM simple_final_processing WHERE id=?', (identity,)).fetchone()
        return json.loads(row[0]) if row else None

    def save(self, row):
        previous = self.get(row['id'])
        if previous and {k: v for k, v in previous.items() if k != 'updatedAt'} == {
                k: v for k, v in row.items() if k != 'updatedAt'}:
            return False
        row['updatedAt'] = now_iso()
        self.db.execute('INSERT INTO simple_final_processing VALUES (?,?) ON CONFLICT(id) DO UPDATE SET payload=excluded.payload',
                        (row['id'], json.dumps(row, ensure_ascii=False)))
        self.db.commit()
        return True

    @staticmethod
    def public(row):
        return {'operationId': row['id'], **{key: row.get(key) for key in (
            'videoId', 'version', 'expectedRevision', 'mediaGeneration', 'state', 'stage', 'message',
            'confirmedAt', 'updatedAt', 'successfulSamples', 'skippedSamples', 'endReason', 'cleanupBytes',
            'manualPaused', 'downloadRetryCount', 'issue', 'environmentBlocker')}}

    def operation(self, identity):
        row = self.get(identity)
        if not row:
            raise ValueError('全流程操作不存在。')
        return next((entry for entry in self.presentations() if entry['operationId'] == identity), self.public(row))

    def presentations(self):
        from .processing_status import current_presentations
        return current_presentations(self)

    def capabilities(self, row):
        """Public action eligibility, independent of temporary disk/network health."""
        version = self.videos.version(row, row.get('activeRevision')) or {}
        blocked = bool(row.get('deletedAt') or row.get('deletion', {}).get('state') in {'pending', 'error'})
        current = self.current_operation(row, version) if version else None
        reason = ('记录正在清理。' if blocked else
                  '本版正在撤回，上一有效版本保留。' if version.get('state') == 'withdrawing' else
                  '本版已清理，保留历史。' if version.get('localCleanupState') == 'complete' else
                  '请先启用视频工作区。' if not self.workspace.enabled else
                  '本版已确认；后台继续处理，暂停后请使用继续入口。' if current else
                  '请等待本版采集完成。' if version.get('state') != 'ready' else
                  '本版任务或视频结果尚未唯一绑定。' if not version.get('taskId') or not version.get('resultId') else '')
        restart = (not blocked and version.get('state') in {'failed', 'ready'} and version.get('taskId')
                   and version.get('resultId') and not version.get('publishing') and not version.get('transferWorkerPid'))
        return {'version': row.get('activeRevision'), 'expectedRevision': row.get('revision'),
                'canConfirmFinal': not bool(reason), 'confirmFinalReason': reason,
                'canRestartDownload': bool(restart),
                'restartDownloadReason': '' if restart else '当前版本无可安全重置的下载断点。',
                'canContinueProcessing': bool(not blocked and current and current.get('manualPaused'))}

    def _enabled_config(self):
        config = copy.deepcopy(self.pipeline.config)
        if not config.get('enabled'):
            config.update(enabled=True, revision=config['revision'] + 1, updatedAt=now_iso())
        return config

    def repair_activation(self):
        """Repair an older interrupted first approval without revoking pauses."""
        if self.pipeline.config.get('enabled'):
            return
        for operation in self.rows():
            if operation.get('state') in {'complete', 'superseded'}:
                continue
            row = self.videos.store.get(operation['videoId'])
            version = self.videos.version(row, operation['version']) if row else None
            current = (self.current_operation(row, version) if row and version and not row.get('deletedAt')
                and row.get('deletion', {}).get('state') not in {'pending', 'error'} else None)
            if current and current['id'] == operation['id']:
                config = self._enabled_config()
                self.pipeline.save('control', config)
                self.pipeline.config = config
                self.pipeline.wake.set()
                break

    def confirm(self, identity, data, *, legacy=False):
        # No network or file I/O here: approval is durably acknowledged first.
        signature = json.dumps({'videoId': identity, **data}, sort_keys=True)
        request = self.db.execute('SELECT payload FROM simple_final_requests WHERE id=?', (data['requestId'],)).fetchone()
        if request:
            prior = json.loads(request[0])
            if prior['signature'] != signature:
                raise Conflict('请求编号已用于其他版本的确认。')
            self.repair_activation()
            return self.operation(prior['operationId'])
        row = self.videos.store.get(identity)
        if not row or row.get('deletedAt'):
            raise ValueError('视频记录不存在。')
        if row['revision'] != data['expectedRevision'] or row['activeRevision'] != data['version']:
            raise Conflict('采集区间已有变化，请查看当前版本后确认。')
        self.videos.require_editable(row)
        version = self.videos.version(row, data['version'])
        if version and version.get('state') == 'withdrawing':
            raise Conflict('本版正在撤回，不能确认或下载。')
        allowed = {'ready'} if not legacy else {'ready', 'saved', 'failed', 'downloading', 'publishing'}
        if (not version or version.get('state') not in allowed
                or (not legacy and (not version.get('taskId') or not version.get('resultId')))):
            raise ValueError('请等待本版采集完成并查看后，再确认最终区间。')
        if version.get('localCleanupState') == 'complete':
            raise Conflict('本版已清理，保留历史，不重复下载或识别。')
        if not legacy and not self.workspace.enabled:
            raise ValueError('请先在设置与文件中启用视频工作区。')
        media = self.control.get('collection:' + identity) or {}
        generation = int(media.get('generation') or 0)
        operation_id = hashlib.sha256(f'final:{identity}:{data["version"]}:{generation}'.encode()).hexdigest()[:40]
        operation = self.get(operation_id)
        if operation and operation['expectedRevision'] != row['revision']:
            raise Conflict('此版本已有其他操作，请刷新后核对。')
        if not operation:
            operation = {'id': operation_id, 'videoId': identity, 'version': version['revision'],
                'expectedRevision': row['revision'], 'mediaGeneration': generation,
                'source': {key: version.get(key) for key in ('taskId', 'resultId')},
                'state': 'queued', 'stage': 'queued', 'message': '已确认最终区间，后台将自动处理。',
                'confirmedAt': now_iso(), 'legacy': legacy}
        marker = {key: operation[key] for key in ('version', 'expectedRevision', 'mediaGeneration', 'confirmedAt')}
        version['finalConfirmation'] = {**marker, 'operationId': operation_id}
        # Store together, without store.save's intermediate commit. A restart
        # must see either both approval records or neither of them.
        row['updatedAt'] = now_iso()
        operation['updatedAt'] = now_iso()
        next_pipeline = self._enabled_config()
        with self.db:
            self.db.execute('INSERT INTO simple_final_processing VALUES (?,?) ON CONFLICT(id) DO NOTHING',
                            (operation_id, json.dumps(operation, ensure_ascii=False)))
            self.db.execute('UPDATE simple_videos SET payload=? WHERE id=?', (json.dumps(row, ensure_ascii=False), identity))
            self.db.execute('INSERT INTO simple_final_requests VALUES (?,?)', (data['requestId'], json.dumps({
                'signature': signature, 'operationId': operation_id})))
            self.db.execute('INSERT INTO simple_pipeline_control VALUES (?,?) '
                            'ON CONFLICT(id) DO UPDATE SET payload=excluded.payload',
                            ('control', json.dumps(next_pipeline, ensure_ascii=False)))
        # Approval and first activation are one commit; explicit pause survives.
        self.pipeline.config = next_pipeline
        self.wake.set()
        self.pipeline.wake.set()
        return self.public(operation)

    def batch(self, data):
        results = []
        if len({row['id'] for row in data['selections']}) != len(data['selections']):
            raise ValueError('同一视频只能确认一次。')
        for selected in data['selections']:
            try:
                result = self.confirm(selected['id'], {key: selected[key] for key in ('version', 'expectedRevision')} |
                                      {'requestId': data['requestId'] + '-' + selected['id']})
                results.append({'id': selected['id'], 'ok': True, **result})
            except ValueError as error:
                results.append({'id': selected['id'], 'ok': False, 'message': str(error)})
        return {'outcomes': results}

    def current_operation(self, row, version):
        marker = (version or {}).get('finalConfirmation') or {}
        operation = self.get(marker.get('operationId', ''))
        media = self.control.get('collection:' + row['id']) or {}
        if (not operation or operation.get('state') == 'superseded'
                or operation.get('videoId') != row['id'] or operation.get('version') != version.get('revision')
                or row.get('activeRevision') != version.get('revision')
                or operation.get('expectedRevision') != row.get('revision')
                or operation.get('mediaGeneration', 0) != int(media.get('generation') or 0)
                or any(version.get(key) != value for key, value in operation.get('source', {}).items())):
            return None
        return operation

    def pause_download(self, row, version):
        operation = self.current_operation(row, version)
        if not operation:
            return
        operation.update(manualPaused=True, state='waiting', stage='paused',
                         message='本条下载已暂停，断点保留；点击继续本条后处理。')
        self.save(operation)
        self.wake.set()

    def resume(self, identity, data):
        signature = json.dumps({'kind': 'resume', 'videoId': identity, **data}, sort_keys=True)
        prior = self.db.execute('SELECT payload FROM simple_final_requests WHERE id=?', (data['requestId'],)).fetchone()
        if prior:
            saved = json.loads(prior[0])
            if saved['signature'] != signature:
                raise Conflict('请求编号已用于其他操作。')
            return self.operation(saved['operationId'])
        row = self.videos.store.get(identity)
        version = self.videos.version(row, data['version']) if row else None
        if (not row or row.get('deletedAt') or row.get('revision') != data['expectedRevision']
                or row.get('activeRevision') != data['version'] or not version):
            raise Conflict('采集版本已变化，请查看当前版本后继续。')
        self.videos.require_editable(row)
        operation = self.current_operation(row, version)
        if not operation or version.get('localCleanupState') == 'complete':
            raise Conflict('当前版本没有可继续的最终处理；请重新核对版本。')
        if operation.get('manualPaused'):
            operation.update(manualPaused=False, resumeRevision=int(operation.get('resumeRevision') or 0) + 1,
                             state='queued', stage='queued', message='已登记继续本条，保留原下载断点。')
            if version.get('state') == 'failed':
                from .final_downloads import RECOVERABLE
                version['downloadRequested'] = True
                if version.get('errorCode') in RECOVERABLE:
                    version['state'] = 'ready'
            row['updatedAt'] = now_iso()
        operation['updatedAt'] = now_iso()
        with self.db:
            self.db.execute('UPDATE simple_final_processing SET payload=? WHERE id=?',
                            (json.dumps(operation, ensure_ascii=False), operation['id']))
            self.db.execute('UPDATE simple_videos SET payload=? WHERE id=?', (json.dumps(row, ensure_ascii=False), identity))
            self.db.execute('INSERT INTO simple_final_requests VALUES (?,?)', (data['requestId'], json.dumps({
                'signature': signature, 'operationId': operation['id']})))
        self.wake.set()
        self.videos.wake.set()
        return self.public(operation)

    def rebind_confirmed_retry(self, row, version, request_id):
        """An explicit same-version retry inherits approval, not old callbacks."""
        marker = version.get('finalConfirmation') or {}
        if not marker:
            return
        prior = self.get(marker.get('operationId', ''))
        if (not prior or prior.get('videoId') != row['id'] or prior.get('version') != version.get('revision')
                or row.get('activeRevision') != version.get('revision')
                or prior.get('expectedRevision') != row.get('revision')
                or not all(version.get(key) and version.get(key) == prior.get('source', {}).get(key)
                           for key in ('taskId', 'resultId'))):
            version.update(downloadRequested=False, state='attention', message='重试版本与原确认不符，未重新下载。')
            self.videos.store.save(row)
            raise Conflict('重试版本与原最终确认不符，请重新核对。')
        generation = int((self.control.get('collection:' + row['id']) or {}).get('generation') or 0)
        identity = hashlib.sha256(f'final:{row["id"]}:{version["revision"]}:{generation}'.encode()).hexdigest()[:40]
        if identity == prior['id']:
            return
        operation = {'id': identity, 'videoId': row['id'], 'version': version['revision'],
            'expectedRevision': row['revision'], 'mediaGeneration': generation, 'source': copy.deepcopy(prior['source']),
            'state': 'queued', 'stage': 'queued', 'message': '按已确认区间继续处理，原断点与历史凭证保留。',
            'confirmedAt': prior['confirmedAt'], 'legacy': prior.get('legacy', False),
            'priorOperationId': prior['id'], 'retryRequestId': request_id, 'manualPaused': False,
            'updatedAt': now_iso()}
        history = {key: copy.deepcopy(version[key]) for key in
                   ('finalConfirmation', 'recovery', 'automaticRecoveryEvent') if key in version}
        history.update(operationId=prior['id'], archivedAt=now_iso())
        version.setdefault('processingHistory', []).append(history)
        version.pop('recovery', None)
        version.pop('automaticRecoveryEvent', None)
        version['finalConfirmation'] = {key: operation[key] for key in
            ('version', 'expectedRevision', 'mediaGeneration', 'confirmedAt')}
        version['finalConfirmation']['operationId'] = identity
        prior.update(state='superseded', stage='superseded', message='此处理代次已由同版本重试接替，历史凭证保留。', updatedAt=now_iso())
        row['updatedAt'] = now_iso()
        with self.db:
            self.db.execute('UPDATE simple_final_processing SET payload=? WHERE id=?',
                            (json.dumps(prior, ensure_ascii=False), prior['id']))
            self.db.execute('INSERT INTO simple_final_processing VALUES (?,?) ON CONFLICT(id) DO NOTHING',
                            (identity, json.dumps(operation, ensure_ascii=False)))
            self.db.execute('UPDATE simple_videos SET payload=? WHERE id=?',
                            (json.dumps(row, ensure_ascii=False), row['id']))
        self.wake.set()
        self.pipeline.wake.set()

    def wait_reason(self, identity):
        media = self.control.get('collection:' + identity) or {}
        if self.pipeline.paused or media.get('paused'):
            return '已暂停，保留确认和当前进度。'
        if media.get('blocked'):
            return '正在更正或核对视频身份。'
        row = self.videos.store.get(identity)
        version = self.videos.version(row, row.get('activeRevision')) if row else None
        operation = self.current_operation(row, version) if row and version else None
        if operation and operation.get('manualPaused'):
            return '本条下载已暂停，断点保留；点击继续本条后处理。'
        run = self.slicing.store.get('runs', self.slicing.store.settings.get('latestRunId')) or self.slicing.run or {}
        if run.get('state') == 'paused' and not self.slicing.store.settings.get('enabled'):
            return '识别已手动暂停，保留确认和下载进度。'
        return ''

    def download_allowed(self, row):
        version = self.videos.version(row, row.get('activeRevision'))
        if not (version or {}).get('finalConfirmation'):
            return True
        return bool(self.current_operation(row, version)) and not self.wait_reason(row['id'])

    def linked_items(self, row, version, generation, *, allow_paused=False):
        """Use the exact approved source version, never a filename or plate."""
        source_versions = [value for value in (version.get('fileVersion'),
            (version.get('recovery') or {}).get('fileVersion')) if isinstance(value, dict)]
        required = {'device', 'inode', 'size', 'modifiedNs'}
        source_versions = [value for value in source_versions if required.issubset(value)]
        candidates = [item for item in self.slicing.store.list('items')
            if item.get('collectionVideoId') == row['id'] and item.get('collectionRevision') == version['revision']
            and item.get('mediaId') == 'collection:' + row['id']
            and item.get('mediaGeneration', 0) == generation and not item.get('superseded')]
        candidates = self.control.scoped_effective_items(candidates)
        linked, conflicts = [], []
        for item in candidates:
            actual = item.get('fileVersion') or {}
            if (not required.issubset(actual) or not any(compatible(actual, expected)
                    and compatible(expected, actual) for expected in source_versions)):
                conflicts.append(item)
            elif not self.control.reason(item, allow_paused=allow_paused):
                linked.append(item)
        return linked, conflicts

    def linked_packages(self, linked):
        ids = [item['id'] for item in linked]
        references = [category['packageId'] for item in linked
                      for category in item.get('categories', {}).values() if category.get('packageId')]
        if not ids:
            return []
        clauses = ["json_extract(payload, '$.itemId') IN (" + ','.join('?' for _ in ids) + ')']
        if references:
            clauses.append('id IN (' + ','.join('?' for _ in references) + ')')
        rows = self.db.execute('SELECT payload FROM simple_slice_package_items WHERE ' + ' OR '.join(clauses),
                               (*ids, *references))
        return [json.loads(value[0]) for value in rows]

    @staticmethod
    def end_reason(linked):
        categories = [category for item in linked for category in item.get('categories', {}).values()
                      if category.get('state') != 'not_applicable']
        if any(category.get('state') == 'complete' for category in categories):
            return 'partial_complete' if any(item.get('partialRecovery') for item in linked) else 'complete'
        return 'no_usable_frames' if categories and all(category.get('state') == 'uncuttable'
                                                      for category in categories) else 'no_target'

    def recover_decode_once(self, operation, linked):
        """Finish the migration after transient scheduler contention, once per item."""
        requests = operation.setdefault('decodeRecoveryRequests', {})
        for item in linked:
            if (item.get('state') != 'attention' or item.get('errorCode') not in {
                    'media_incomplete', 'frame_timeout', 'frame_extract_failed', 'decode_unavailable'}
                    or item.get('pipelinePaused') or item.get('collectionRecoveryPaused')
                    or item['id'] in requests):
                continue
            run = self.slicing.store.get('runs', self.slicing.store.settings.get('latestRunId')) or {}
            if (self.slicing.start_lock.locked() or (self.slicing.busy and not self.slicing.store.settings.get('enabled'))
                    or (self.slicing.store.settings.get('enabled') and run.get('singleMode')
                        and run.get('singleItemId') != item['id'])):
                operation.update(state='waiting', stage='recognizing', message='等待当前识别操作安全结束，随后处理可用片段。')
                continue
            request_id = 'final-decode-' + operation['id'] + '-' + item['id'][:24]
            try:
                self.slicing.retry(item['id'], item['revision'], request_id, recover_decodable=True)
                requests[item['id']] = {'requestId': request_id, 'revision': item['revision'], 'state': 'registered'}
            except ValueError:
                requests[item['id']] = {'requestId': request_id, 'revision': item['revision'], 'state': 'attention'}
                operation.update(state='attention', stage='recognizing', message='采集版本或识别设置已有变化，请核对后继续。')
            self.save(operation)

    async def advance(self, operation):
        operation = self.get(operation['id']) or operation
        action_revision = (bool(operation.get('manualPaused')), int(operation.get('resumeRevision') or 0))
        row = self.videos.store.get(operation['videoId'])
        version = self.videos.version(row, operation['version']) if row else None
        media = self.control.get('collection:' + operation['videoId']) or {}
        if (not row or row.get('deletedAt') or not version or row['activeRevision'] != operation['version']
                or row['revision'] != operation['expectedRevision']
                or int(media.get('generation') or 0) != operation['mediaGeneration']
                or any(version.get(k) != v for k, v in operation['source'].items())):
            operation.update(state='superseded', stage='superseded', message='已有新的采集操作，本版仅保留历史。')
            self.save(operation)
            return
        from .final_downloads import RECOVERABLE
        issue = operation.get('issue')
        if version.get('state') in {'ready', 'saved'} and (issue or {}).get('stage') == 'download':
            issue = None
        if (version.get('state') in {'failed', 'attention'} and not version.get('recovery')
                and (version.get('errorCode') or version.get('review', {}).get('code')) not in
                    RECOVERABLE | {'invalid_video', 'media_incomplete'}):
            issue = {'code': version.get('review', {}).get('code') or version.get('errorCode') or 'download_attention',
                     'stage': 'download', 'message': version.get('message') or '下载结果需要核对。'}
        elif not issue and operation.get('state') == 'attention':
            issue = {'code': operation.get('stage') or 'processing_attention', 'stage': operation.get('stage'),
                     'message': operation.get('message') or '处理结果需要核对。'}
        reason = self.wait_reason(row['id'])
        operation['environmentBlocker'] = None
        if reason or not self.workspace.enabled or not self.workspace.online:
            blocker = {'code': 'paused' if reason else 'disk',
                       'message': reason or '等待工作区磁盘连接，恢复后继续。'}
            if issue and not reason:
                blocker['message'] = '原工作区磁盘尚未连接；连接后仍需核对上述问题。'
            operation['environmentBlocker'] = blocker
            operation['issue'] = issue
            if issue:
                operation.update(state='attention', stage=issue.get('stage') or 'verification', message=issue['message'])
            else:
                operation.update(state='waiting', stage='paused' if reason else 'waiting_disk', message=blocker['message'])
        elif version.get('localCleanupState') == 'complete':
            operation.update(state='complete', stage='cleaned', message='处理完成，本地文件已清理。')
        else:
            operation.update(state='running', stage='download', message='正在下载已确认的最终版本。')
            if version.get('state') == 'ready' and not version.get('downloadRequested'):
                await self.videos.confirm(row['id'], {'version': operation['version'],
                    'expectedRevision': operation['expectedRevision'], 'requestId': 'final-download-' + operation['id'] +
                    '-r' + str(operation.get('resumeRevision', 0))})
            elif version.get('state') == 'failed' and version.get('downloadRequested'):
                from .final_downloads import continue_download
                if not await continue_download(self, operation, row, version):
                    marker = self.pipeline.prepare_recovery_event(row, version)
                    if marker:
                        version['automaticRecoveryEvent'] = marker
                        self.videos.store.save(row)
                        self.pipeline.notify_media_failure(row['id'], version['revision'])
                    elif not version.get('recovery') and not version.get('automaticRecoveryEvent'):
                        operation.update(state='attention', stage='download', message=version.get('message') or '下载需要核对。')
            elif version.get('state') in {'attention', 'uncertain', 'failed'} and not version.get('recovery'):
                operation.update(state='attention', stage='download', message=version.get('message') or '视频身份或下载结果需要核对。')
            if version.get('state') == 'saved':
                if not operation.get('downloadEnrolled'):
                    self.pipeline.notify_saved(row['id'], version['revision'])
                    operation['downloadEnrolled'] = True
                operation.update(stage='recognizing', message='正在识别、导出并导入照片。')
            linked, conflicts = self.linked_items(row, version, operation['mediaGeneration'])
            self.recover_decode_once(operation, linked)
            linked, conflicts = self.linked_items(row, version, operation['mediaGeneration'])
            if conflicts:
                operation.update(state='attention', stage='identity', message='识别记录的原片版本不符，保留文件等待核对。')
            if linked and not conflicts:
                operation['successfulSamples'] = sum(max((int(value.get('observations') or 0)
                    for value in item.get('categories', {}).values()), default=0) for item in linked)
                operation['skippedSamples'] = sum(max((int(value.get('skippedSamples') or 0)
                    for value in item.get('categories', {}).values()), default=0) for item in linked)
                if operation.get('state') != 'waiting':
                    operation.update(stage='photos' if any(i.get('state') == 'platform_processing' for i in linked) else 'recognizing',
                                     message=next((i.get('message') for i in linked if i.get('message')), '正在处理照片。'))
                    if operation['stage'] == 'photos' and self.slicing.store.settings.get('localExport', True):
                        packages = self.linked_packages(linked)
                        stages = {package.get('state') for package in packages if package.get('state') != 'complete'}
                        if stages.intersection({'submitting', 'importing', 'reconciling'}):
                            operation.update(stage='photo_import', message='照片已上传，正在导入并核验结果。')
                        elif stages.intersection({'queued', 'uploading', 'uploaded'}):
                            operation.update(stage='photo_upload', message='照片已打包，正在上传或等待导入。')
                        else:
                            operation.update(stage='photo_export', message='正在导出照片并打包，随后自动上传和导入。')
                if operation.get('state') != 'waiting' and any(i.get('state') == 'attention' for i in linked):
                    operation.update(state='attention', message=next((i.get('message') for i in linked if i.get('state') == 'attention'), '需要核对。'))
            asset = next((asset for asset in self.workspace.rows('assets')
                          if any(i['id'] in {link['id'] for link in self.workspace.effective_links(asset)} for i in linked)), None)
            empty_verified = all(verified_empty_conclusion(item, name) or
                (operation.get('legacy') and category.get('state') == 'no_target'
                 and type(category.get('observations')) is int and category['observations'] > 0
                 and not category.get('errorCode'))
                for item in linked for name, category in item.get('categories', {}).items()
                if category.get('state') in {'no_target', 'uncuttable'})
            terminal = bool(linked) and all(item.get('state') in {'complete', 'no_target', 'uncuttable'} for item in linked)
            finished = (not conflicts and terminal and empty_verified
                        and photo_results_verified(linked, self.linked_packages(linked)))
            if terminal and not finished and not conflicts:
                operation.update(state='attention', stage='verification',
                    message='照片导入凭证或空结果结论未核实，原片和成果保留。')
            cleanup_reason = '本地文件不在可清理工作区内，原位保留。'
            if asset and not conflicts:
                operation['workspaceAssetId'] = asset['id']
                if asset.get('cleanup', {}).get('state') == 'complete':
                    operation.update(state='complete', stage='cleaned', cleanupBytes=0, message='处理完成，本地文件已清理。')
                    self.save(operation)
                    return
                eligible, cleanup_reason = self.workspace.cleanup_eligibility(asset)
                if eligible:
                    operation.update(state='complete', stage='awaiting_cleanup', endReason=self.end_reason(linked),
                                     cleanupBytes=self.workspace.cleanup_asset_bytes(asset),
                                     message='处理已结束，等待你点击“清理已结束文件”。')
            if finished and operation.get('stage') not in {'awaiting_cleanup', 'cleaned'}:
                # Old no-target conclusions and explicitly retained originals
                # are finished work, but are not new deletion authority.
                operation.update(state='complete', stage='retained', endReason=self.end_reason(linked),
                                 cleanupBytes=0, message='处理已结束；' + cleanup_reason)
        if operation.get('state') == 'attention':
            operation['issue'] = issue if operation.get('environmentBlocker') and issue else {
                'code': version.get('review', {}).get('code') or version.get('errorCode') or operation.get('stage'),
                'stage': operation.get('stage'), 'message': operation.get('message')}
        elif not operation.get('environmentBlocker'):
            operation['issue'] = None
        latest = self.get(operation['id'])
        if latest and (latest.get('state') == 'superseded' or action_revision !=
                (bool(latest.get('manualPaused')), int(latest.get('resumeRevision') or 0))):
            self.wake.set()
            return
        if self.save(operation):
            self.pipeline.wake.set()
            self.workspace.wake.set()

    async def adopt_existing(self):
        """Upgrade only already-approved current versions, once, without a scan."""
        if self.pipeline.config.get('finalFlowMigration') == 1:
            return
        self.control.refresh()
        assets = self.workspace.rows('assets')
        items = self.slicing.store.list('items')
        required = {'device', 'inode', 'size', 'modifiedNs'}
        for row in self.videos.store.list():
            version = self.videos.version(row, row.get('activeRevision'))
            if (not version or row.get('deletedAt') or version.get('finalConfirmation')
                    or version.get('localCleanupState') == 'complete'
                    or not version.get('taskId') or not version.get('resultId')
                    or not (version.get('downloadRequested') or row.get('acceptedRevision') == version.get('revision'))):
                continue
            recovery = version.get('recovery') or {}
            sources = [(version.get('filePath'), version.get('fileVersion') or {}),
                       (recovery.get('filePath') or recovery.get('destination'), recovery.get('fileVersion') or {})]
            bound_assets = [asset for asset in assets if any(path and asset.get('path') == path
                and required.issubset(expected) and compatible(asset.get('fileVersion') or {}, expected)
                and compatible(expected, asset.get('fileVersion') or {}) for path, expected in sources)]
            # A historical saved file is not pending work merely because its
            # legacy slice record has no collection ID. Retention is stronger
            # than migration; no filename-based ownership is inferred here.
            if any(self.workspace.is_preserved(asset) or asset.get('status') == 'cleaned'
                   or asset.get('cleanup', {}).get('state') == 'complete' for asset in bound_assets):
                continue
            linked = [item for item in items if item.get('mediaId') == 'collection:' + row['id']
                      and item.get('collectionRevision') == version['revision'] and not item.get('superseded')]
            linked = self.control.scoped_effective_items(linked)
            asset_items = [item for asset in bound_assets for item in self.workspace.effective_links(asset)]
            known = list({item['id']: item for item in [*linked, *asset_items]}.values())
            if known and all(item.get('state') in {'complete', 'no_target', 'uncuttable'} for item in known):
                continue
            in_flight = (version.get('downloadRequested') or recovery.get('state') in
                         {'queued', 'moving', 'slicing', 'paused', 'waiting_connection'} or known)
            if not in_flight and not bound_assets:
                continue
            try:
                result = self.confirm(row['id'], {'version': version['revision'], 'expectedRevision': row['revision'],
                    'requestId': 'upgrade-final-' + row['id'] + '-' + str(version['revision'])}, legacy=True)
            except ValueError:
                continue
            for item in linked:
                if self.control.allowed(item) and not item.get('pipelinePaused') and not item.get('collectionRecoveryPaused'):
                    item['finalProcessingIntent'] = result['operationId']
                    self.slicing.store.save('items', item)
            # The regular advance path owns the bounded recovery request so a
            # busy scheduler during upgrade cannot permanently lose the retry.
        self.pipeline.config['finalFlowMigration'] = 1
        self.pipeline.save('control', self.pipeline.config)

    async def tick(self):
        for operation in self.rows():
            if operation.get('state') in {'superseded'} or operation.get('stage') == 'cleaned':
                continue
            if operation.get('state') == 'complete':
                row = self.videos.store.get(operation['videoId'])
                if not row or row.get('activeRevision') != operation['version']:
                    operation.update(state='superseded', stage='superseded', message='已有新版本，本版成果保留在历史中。')
                    self.save(operation)
                    continue
                if operation.get('stage') == 'awaiting_cleanup':
                    asset = self.workspace.get('assets', operation.get('workspaceAssetId', ''))
                    version = self.videos.version(row, operation['version']) or {}
                    if version.get('localCleanupState') == 'complete' or (asset or {}).get('cleanup', {}).get('state') == 'complete':
                        operation.update(stage='cleaned', cleanupBytes=0, message='处理完成，本地文件已清理。')
                        self.save(operation)
                continue
            try:
                await self.advance(operation)
            except asyncio.CancelledError:
                raise
            except (OSError, ValueError) as error:
                operation.update(state='waiting' if isinstance(error, OSError) else 'attention',
                                 message='等待文件或设置恢复。' if isinstance(error, OSError) else str(error))
                self.save(operation)

    async def startup(self):
        self.stopping = False
        self.repair_activation()
        # Integration tests and ordinary fresh profiles never enroll arbitrary
        # local videos. The migration uses only existing confirmed collection IDs.
        if self.workspace.enabled:
            await self.adopt_existing()
        self.task = asyncio.create_task(self.loop(), name='final-video-processing')

    async def loop(self):
        while not self.stopping:
            self.wake.clear()
            try:
                await self.tick()
            except asyncio.CancelledError:
                raise
            except Exception:
                # Unanticipated environment faults retain durable intentions.
                pass
            try:
                await asyncio.wait_for(self.wake.wait(), 5)
            except TimeoutError:
                pass

    async def shutdown(self):
        self.stopping = True
        if self.task:
            self.task.cancel()
            await asyncio.gather(self.task, return_exceptions=True)
