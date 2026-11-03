"""Persistent, user-reviewed initial capture plans. No writes before approval."""
import copy
import hashlib
import json
from datetime import timedelta

from .domain import format_time, now_iso, parse_time, task_key
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
        if new:
            self.manager._save(row)
        selected = [task for task in tasks if category == 'all' or
                    (task['status'] == 'ready' if category == 'ready' else task['status'] != 'ready')]
        pages = max(1, (len(selected) + 19) // 20)
        page = min(max(1, page), pages)
        return {'runId': identity, 'planRevision': row['planRevision'],
                'tasks': copy.deepcopy(selected[(page-1)*20:page*20]), 'total': len(selected),
                'page': page, 'pages': pages, 'readyIds': [t['recordId'] for t in tasks if t['status'] == 'ready'],
                'pendingSubmission': row.get('pendingSubmissionId', '')}

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
        submitted = sum(t.get('status') == 'submitted' for job in jobs if job for t in job.get('items', []))
        versions = [video['versions'][0] for video in self.manager.videos.store.list() if video.get('jobId') in ids]
        ready = sum(bool(v.get('resultId')) for v in versions)
        attention = sum(v.get('state') in {'attention','failed'} and not v.get('resultId') for v in versions)
        row['counts'].update(submitted=submitted, ready=ready, attention=attention, waiting=max(0,submitted-ready-attention),
                             duplicate=sum(t['status']=='duplicate' for t in row['reviewTasks']))

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
