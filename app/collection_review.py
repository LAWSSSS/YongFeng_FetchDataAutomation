"""Persistent, user-reviewed initial capture plans. No writes before approval."""
import copy
import hashlib
import json
from datetime import timedelta

from .capture_history import matching_receipts, same_target, target_of
from .domain import TARGET_URL, format_time, now_iso, parse_time, task_key
from .videos import Conflict


class CollectionReview:
    def __init__(self, manager):
        self.manager = manager
        self.db = manager.db
        self.db.execute('CREATE TABLE IF NOT EXISTS simple_collection_submissions (id TEXT PRIMARY KEY, run_id TEXT NOT NULL, payload TEXT NOT NULL)')
        self.db.commit()

    def ensure(self, row):
        if 'reviewTasks' not in row:
            self.manager._plan(row)
            row['reviewTasks'] = copy.deepcopy(row['plan']['tasks'] + row.get('rejected', []))
            row.update(planRevision=0, reviewVersion=1, jobIds=[row['jobId']] if row.get('jobId') else [])
        return row['reviewTasks']

    def get(self, identity, page=1, category='all'):
        if category not in {'all','ready','other'}:
            raise ValueError('记录筛选无效。')
        row = self.manager.get(identity)
        if not row or identity in self.manager.visibility.hidden():
            raise ValueError('日期查询不存在。')
        if not row.get('queryComplete'):
            raise Conflict('质检尚未查询完成。')
        new = 'reviewTasks' not in row
        tasks = self.ensure(row)
        if self.refresh_historical_duplicates(row) or new:
            self.manager._save(row)
        selected = [task for task in tasks if category == 'all' or
                    (task['status'] == 'ready' if category == 'ready' else task['status'] != 'ready')]
        pages = max(1, (len(selected) + 19) // 20)
        page = min(max(1, page), pages)
        visible = copy.deepcopy(selected[(page-1)*20:page*20])
        job_results = {job['id']: {str(t.get('recordId')): t for t in job['items']}
                       for job in self.manager.store.list() if job['id'] in row.get('jobIds', [])}
        linked = {}
        for video in self.manager.videos.store.list():
            if video.get('duplicateOfVideoId'):
                continue
            if same_target(target_of(self.manager.store, video), row['target']):
                linked.setdefault(str(video.get('recordId')), []).append(video['id'])
        for task in visible:
            actual = job_results.get(task.get('jobId') or row.get('jobId'), {}).get(str(task['recordId']))
            if actual and actual['status'] in {'submitted', 'duplicate', 'failed', 'uncertain'} and task['status'] != 'ready':
                task.update(status=actual['status'], reason=actual.get('message', ''))
                if actual['status'] == 'duplicate':
                    task.update(duplicateSource=actual.get('duplicateSource'), existingTaskId=actual.get('remoteId'))
            matches = linked.get(str(task['recordId']), [])
            if len(matches) == 1 and task['status'] != 'invalid':
                task['existingVideoId'] = matches[0]
                if task['status'] == 'duplicate' and task.get('duplicateSource') == 'local':
                    task['reason'] = '本条已有采集，可打开已有视频查看或按当前区间重新采集。'
            elif not matches and task['status'] in {'ready', 'duplicate'}:
                receipts = self.old_receipts(row, task)
                if len({receipt['videoId'] for receipt in receipts}) == 1:
                    task['canRecaptureArchived'] = True
        return {'runId': identity, 'planRevision': row['planRevision'],
                'tasks': visible, 'total': len(selected),
                'page': page, 'pages': pages, 'readyIds': [t['recordId'] for t in tasks if t['status'] == 'ready'],
                'pendingSubmission': row.get('pendingSubmissionId', ''), 'platformUrl': TARGET_URL}

    def refresh_historical_duplicates(self, row):
        if not row.get('reviewTasks') or row.get('pendingSubmissionId'):
            return False
        candidates = [task for task in row['reviewTasks']
                      if task['status'] == 'duplicate' and task.get('duplicateSource') == 'local']
        if not candidates:
            return False
        keys = self.manager.store.known_keys(include_retired=False)
        records = self.manager.store.known_records(row['target'])
        changed = False
        for task in candidates:
            if task['key'] not in keys and str(task['recordId']) not in records:
                task['status'] = 'ready'
                task.pop('reason', None)
                task.pop('duplicateSource', None)
                changed = True
        if changed:
            row['planRevision'] += 1
            row['counts']['duplicate'] = sum(task['status'] == 'duplicate' for task in row['reviewTasks'])
            if row['status'] == 'complete':
                row.update(status='awaiting_review', stage='review', message='旧采集记录已重新核对，请确认要采集的条目。')
        return changed

    def old_receipts(self, row, task):
        return matching_receipts(self.db, task['recordId'], row['target'], task['startTime'], task['endTime'])

    async def recapture(self, identity, data):
        from .domain import approval_issue
        videos = self.manager.videos
        request_id = hashlib.sha256(('archived-recapture:' + identity + ':' + data['requestId']).encode()).hexdigest()
        source = {'runId': identity, 'recordId': data['recordId'], 'planRevision': data['expectedPlanRevision']}
        existing = next((entry for entry in videos.store.list_operations()
                         if entry.get('requestId') == request_id and entry.get('kind') == 'recapture'), None)
        if existing:
            if existing['payload'].get('collectionSource') != source:
                raise Conflict('请求编号已用于其他采集计划。')
            return await videos.recapture_unavailable(existing['videoId'], {
                'version': existing['version'], 'expectedRevision': existing['expectedRevision'],
                'requestId': request_id, 'mode': 'force', 'collectionSource': source})
        row = self.manager.get(identity)
        if not row or identity in self.manager.visibility.hidden() or not row.get('queryComplete'):
            raise ValueError('请先查询并核对采集计划。')
        task = next((task for task in row.get('reviewTasks', []) if task['recordId'] == data['recordId']), None)
        if not task or task['status'] not in {'ready', 'duplicate'}:
            raise Conflict('该条采集计划不可重采，请刷新后核对。')
        receipts = self.old_receipts(row, task)
        if len({receipt['videoId'] for receipt in receipts}) != 1:
            raise Conflict('找不到唯一归属于本条实绩的旧任务。')
        newest = receipts[-1]
        video_id = newest['videoId']
        if (row['planRevision'] != data['expectedPlanRevision'] or row.get('pendingSubmissionId')
                or row['admissionRevision'] != self.manager.admission_settings.state()['revision']):
            raise Conflict('采集计划或准入条件已变化，请刷新后重新核对。')
        if issue := approval_issue(task, row['admissionRule']):
            raise ValueError('质检实绩未通过采集准入：' + issue)
        existing_video = videos.store.get(video_id)
        if existing_video:
            if (str(existing_video.get('recordId')) != str(task['recordId'])
                    or not same_target(target_of(self.manager.store, existing_video), row['target'])):
                raise Conflict('现有视频与旧任务凭证不一致，请先核对。')
            if existing_video.get('deletedAt'):
                raise Conflict('旧记录已删除，请从日期查询发起新的采集。')
            if existing_video.get('activeRevision') != newest['version']['revision']:
                raise Conflict('已有更新的采集版本，请在“视频确认”打开该条记录。')
        else:
            # Verify the receipt before making the archived lineage visible.
            # The queued operation repeats this check under the submission lock.
            versions = [{**receipt['version'], 'state': 'submitted', 'restoredFromArchive': True} for receipt in receipts]
            active = versions[-1]
            existing_video = {'id': video_id, 'jobId': newest['jobId'], 'recordId': task['recordId'],
                              'taskKey': task['key'], 'name': active['name'], 'plate': task['plate'],
                              'qualityStart': task['checkStartTime'], 'qualityEnd': task['checkCompleteTime'],
                              'photoCount': task['photoCount'], 'inspectionDurationSeconds': task['inspectionDurationSeconds'],
                              'admissionRule': row['admissionRule'], 'submissionTarget': newest['target'],
                              'createdAt': now_iso(), 'submittedAt': now_iso(), 'revision': active['revision'],
                              'activeRevision': active['revision'], 'acceptedRevision': None, 'versions': versions,
                              'requests': {}, 'error': ''}
            if not same_target(newest['target'], videos.target_config):
                raise Conflict('旧任务所属的采集节点与当前配置不同。')
            validator = getattr(videos, 'validate_capture_interval', None)
            if validator:
                validator(active['startTime'], active['durationSeconds'])
            parents = await videos.remote_parents_cached(fresh=True)
            videos.verify_forced_interval(existing_video, active, parents)
            await videos._check_row(existing_video, parents)
            current_plan = self.manager.get(identity)
            if (not current_plan or current_plan['planRevision'] != data['expectedPlanRevision']
                    or current_plan.get('pendingSubmissionId')
                    or row['admissionRevision'] != self.manager.admission_settings.state()['revision']):
                raise Conflict('核对期间采集计划或准入条件已变化，请刷新。')
            if videos.store.get(video_id):
                raise Conflict('旧记录已由其他操作打开，请刷新。')
            if not videos.can_force_recapture(existing_video, active):
                raise Conflict('旧版本尚未通过平台核对。')
            videos.store.save(existing_video)
        return await videos.recapture_unavailable(video_id, {
            'version': existing_video['activeRevision'], 'expectedRevision': existing_video['revision'],
            'requestId': request_id, 'mode': 'force', 'collectionSource': source})

    def edit(self, identity, data):
        row = self.manager.get(identity)
        if not row or not row.get('queryComplete'):
            raise ValueError('质检查询尚未完成。')
        self.ensure(row)
        prior = row.get('draftRequests', {}).get(data['requestId'])
        signature = json.dumps(data, sort_keys=True)
        if prior:
            if prior != signature:
                raise Conflict('请求编号已用于其他修改。')
            return self.get(identity, data.get('page', 1))
        if data['expectedPlanRevision'] != row['planRevision']:
            raise Conflict('采集计划已变化，请刷新后重新核对。')
        if row.get('pendingSubmissionId'):
            raise Conflict('所选任务正在提交，请等待结果后再修改其他区间。')
        task = next((t for t in row['reviewTasks'] if t['recordId'] == data['recordId']), None)
        if not task or task['status'] != 'ready':
            raise Conflict('这条实绩不可修改，可能已经提交或未通过准入。')
        start = parse_time(data['startTime'])
        if not start or not 60 <= data['durationSeconds'] <= 7200:
            raise ValueError('请输入有效时间和1–120分钟时长。')
        validator = getattr(self.manager.videos, 'validate_capture_interval', None)
        if validator:
            validator(format_time(start), data['durationSeconds'])
        task.update(startTime=format_time(start), endTime=format_time(start+timedelta(seconds=data['durationSeconds'])),
                    durationMinutes=data['durationSeconds']/60, durationSeconds=data['durationSeconds'])
        task['key'] = task_key(row['target'], task['startTime'], task['endTime'])
        row['planRevision'] += 1
        row.setdefault('draftRequests', {})[data['requestId']] = signature
        self.manager._save(row)
        return self.get(identity, data.get('page', 1))

    def operation(self, identity):
        found = self.db.execute('SELECT payload FROM simple_collection_submissions WHERE id=?', (identity,)).fetchone()
        return json.loads(found[0]) if found else None

    def save_operation(self, operation, *, commit=True):
        operation['updatedAt'] = now_iso()
        self.db.execute('INSERT INTO simple_collection_submissions VALUES(?,?,?) ON CONFLICT(id) DO UPDATE SET payload=excluded.payload',
                        (operation['operationId'], operation['runId'], json.dumps(operation, ensure_ascii=False)))
        if commit:
            self.db.commit()

    def track(self, row):
        if not row.get('pendingSubmissionId'):
            return
        operation = self.operation(row['pendingSubmissionId'])
        if not operation:
            return
        state = {'submitting':'running', 'waiting':'waiting', 'waiting_connection':'waiting_connection',
                 'attention':'attention', 'complete':'complete'}.get(row['status'], 'queued')
        update = {'state':state, 'stage':row.get('stage'), 'message':row.get('message'), 'jobId':row.get('jobId','')}
        if any(operation.get(key) != value for key,value in update.items()):
            operation.update(update)
            self.save_operation(operation)

    def submit(self, identity, data):
        operation_id = hashlib.sha256(('collect:'+identity+':'+data['requestId']).encode()).hexdigest()[:32]
        signature = json.dumps(data, sort_keys=True)
        old = self.operation(operation_id)
        if old:
            if old['signature'] != signature:
                raise Conflict('请求编号已用于其他采集计划。')
            return old
        row = self.manager.get(identity)
        if not row or not row.get('queryComplete'):
            raise ValueError('请先查询并核对采集计划。')
        self.ensure(row)
        if row.get('pendingSubmissionId'):
            raise Conflict('本次选择正在提交，请等待结果。')
        if row['planRevision'] != data['expectedPlanRevision']:
            raise Conflict('采集计划已变化，请重新核对后确认。')
        if row['admissionRevision'] != self.manager.admission_settings.state()['revision']:
            raise Conflict('准入条件已变化，请重新查询并核对。')
        selected = set(data['recordIds'])
        tasks = [copy.deepcopy(t) for t in row['reviewTasks'] if t['recordId'] in selected]
        if not tasks or len(tasks) != len(selected) or any(t['status'] != 'ready' for t in tasks):
            raise Conflict('所选记录已有变化，未提交，请重新选择可采集记录。')
        validator = getattr(self.manager.videos, 'validate_capture_interval', None)
        for task in tasks:
            if validator:
                validator(task['startTime'], round(task['durationMinutes']*60))
        operation = {'operationId': operation_id, 'runId': identity, 'requestId': data['requestId'],
                     'signature': signature, 'state': 'queued', 'message': '已确认，后台核对后分批采集。',
                     'recordIds': list(data['recordIds']), 'createdAt': now_iso(), 'results': []}
        row['planId'] = 'approved-' + operation_id
        row['plan'] = {**row['plan'], 'id': row['planId'], 'tasks': tasks, 'confirmedAt': now_iso(),
                       'confirmationId': operation_id}
        row.update(jobId='', pendingSubmissionId=operation_id, status='queued', stage='submit',
                   message=operation['message'], errorCode='')
        row['planRevision'] += 1
        for task in row['reviewTasks']:
            if task['recordId'] in selected:
                task['status'] = 'confirmed'
        # The intent and exact reviewed parameters are one durable boundary.
        with self.db:
            self.save_operation(operation, commit=False)
            self.manager._save(row, commit=False)
        self.manager.wake.set()
        return operation

    def finish(self, row, job):
        operation = self.operation(row.get('pendingSubmissionId', ''))
        if not operation:
            return
        results = {str(t.get('recordId')): t for t in job['items']}
        for task in row['reviewTasks']:
            if task['recordId'] in results:
                actual = results[task['recordId']]
                task.update(status=actual['status'], reason=actual.get('message', ''), jobId=job['id'])
                if actual['status'] == 'duplicate':
                    task.update(duplicateSource=actual.get('duplicateSource'), existingTaskId=actual.get('remoteId'))
        operation.update(state='attention' if row['status'] == 'attention' else 'complete',
                         message=row['message'], jobId=job['id'],
                         results=[{k:t.get(k) for k in ('recordId', 'status', 'message', 'remoteId')} for t in job['items']])
        self.save_operation(operation)
        row['pendingSubmissionId'] = ''
        row['planRevision'] += 1
        if any(t['status'] == 'ready' for t in row['reviewTasks']):
            row.update(status='awaiting_review', stage='review', message='所选采集已处理，剩余记录等待确认。')
        self.aggregate_counts(row)
        self.manager._save(row)

    def aggregate_counts(self, row):
        ids = set(row.get('jobIds', []))
        jobs = [self.manager.store.get(identity) for identity in ids]
        progress = [self.manager.videos.job_progress(job) for job in jobs if job]
        row['counts'].update({key: sum(p[key] for p in progress)
                              for key in ('submitted', 'reused', 'ready', 'attention', 'waiting')})
        row['counts']['duplicate'] = sum(t['status']=='duplicate' for t in row['reviewTasks'])

    def finish_empty(self, row, tasks):
        operation = self.operation(row.get('pendingSubmissionId', ''))
        if operation:
            operation.update(state='complete', message='所选记录已登记，无需重复采集。',
                             results=[{k:t.get(k) for k in ('recordId','status','reason')} for t in tasks])
            self.save_operation(operation)
        actual = {str(t['recordId']):t for t in tasks}
        for task in row['reviewTasks']:
            if task['recordId'] in actual:
                task.update(status=actual[task['recordId']]['status'], reason=actual[task['recordId']].get('reason',''))
        row.update(pendingSubmissionId='', planRevision=row['planRevision']+1)
        row['status']='awaiting_review' if any(t['status']=='ready' for t in row['reviewTasks']) else 'complete'
        self.aggregate_counts(row)
        self.manager._save(row)
