"""Small, read-only receipt views; durable operation evidence stays in its stores."""
import json
from math import ceil
from hashlib import sha256


def collection_record(job):
    return {'jobId': job['id'], 'downloadAfterCollection': bool(job.get('downloadAfterCollection')),
            'items': [{key: item.get(key) or '' for key in
                       ('videoName', 'startTime', 'endTime', 'status', 'reason', 'message', 'taskId')}
                      | {'id': item.get('key') or item.get('recordId'), 'camera': item.get('cameraName') or ''}
                      for item in job.get('items') or [] if item.get('status') != 'removed']}


class SubmissionViews:
    def __init__(self, jobs, slicing, submissions=None):
        self.jobs, self.slicing, self.submissions = jobs, slicing, submissions
        self.slicing.store.db.execute("CREATE INDEX IF NOT EXISTS slicing_run_request ON simple_slicing_runs(json_extract(payload, '$.requestId'))")
        self.slicing.store.db.commit()

    def get(self, identity):
        """Adapter for immutable, presentation-only receipt child identities."""
        return self.get_many([identity]).get(identity)

    def get_many(self, identities):
        """Build each receipt once while validating a batch of child records."""
        wanted = set(identities)
        result = {}
        for parent_id in {identity.partition(':')[0] for identity in wanted}:
            row = self.submissions.get(parent_id) if self.submissions else None
            if not row:
                continue
            items = self.project(row, detail=True, page_size=100000)['detail']['items']
            result.update((item['deletionKey'], item) for item in items if item.get('deletionKey') in wanted)
        return result

    def slicing_receipts(self):
        hidden = self.submissions.visibility.hidden()
        return [{**{key: row.get(key) for key in ('id', 'title', 'submittedAt')}, 'revision': 0,
                 'itemIds': list(dict.fromkeys(self._slice_scope(row)[0])),
                 'message': (row.get('detail') or {}).get('message') or '',
                 'notice': self._slice_scope(row)[2]}
                for row in self.submissions.list('slicing') if row['id'] not in hidden]

    def slicing_dates(self):
        """Exact submission scope only; filenames/last-update times are not dates."""
        dates = {}
        for row in reversed(self.submissions.list('slicing')):
            for identity in self._slice_scope(row)[0]:
                if row.get('submittedAt'):
                    dates.setdefault(identity, row['submittedAt'])
        return dates

    def _slice_scope(self, row):
        detail = row.get('detail') or {}
        request_id = detail.get('requestId') or row.get('requestId')
        request = self.slicing.store.get('requests', request_id or '') or {}
        if 'itemIds' in detail:
            return detail['itemIds'], detail.get('runId'), ''
        if 'receiptItemIds' in request:
            return request['receiptItemIds'], request.get('receiptRunId'), ''
        run_id = detail.get('runId') or request.get('runId')
        run = self.slicing.store.get('runs', run_id or '')
        if not run and request_id:
            # Only exact request matches are evidence. Do not deserialize every
            # old run (and its snapshots) while rendering each receipt.
            matches = [json.loads(value[0]) for value in self.slicing.store.db.execute(
                "SELECT payload FROM simple_slicing_runs WHERE json_extract(payload, '$.requestId')=? LIMIT 2", (request_id,))]
            run = matches[0] if len(matches) == 1 else None
        if run:
            return ([request['itemId']] if request.get('itemId') else run.get('items', [])), run['id'], ''
        return [], None, '这条旧记录未保存视频清单，保留当时的启动信息；新提交会记录完整明细。'

    def project(self, row, *, detail=False, page=1, page_size=20):
        value = {key: row.get(key) for key in ('id', 'kind', 'title', 'submittedAt')}
        saved = row.get('detail') or {}
        metadata = {key: saved.get(key) for key in ('message', 'phase', 'jobId', 'requestId') if saved.get(key)}
        if row['kind'] == 'collection':
            job = self.jobs.get(saved.get('jobId') or '')
            items = collection_record(job)['items'] if job else saved.get('items', [])
            if job:
                value.update(status=job['status'], submitted=sum(i.get('status') == 'submitted' for i in items))
            else:
                value.update(status=row.get('status'), submitted=row.get('submitted', 0))
        else:
            identities, run_id, notice = self._slice_scope(row)
            metadata.update(runId=run_id)
            if notice:
                metadata['notice'] = notice
            items = []
            for identity in dict.fromkeys(identities):
                source = self.slicing.store.get('items', identity)
                if not source:
                    items.append({'id': identity, 'status': 'unavailable', 'message': '原任务明细当前不可用。'})
                    continue
                category_name = 'bed' if str(source.get('sourceDatasetId')) == '27912' else 'magnet'
                category = source.get('categories', {}).get(category_name) or {}
                items.append({'id': identity, 'videoName': source.get('name') or '', 'category': category_name,
                              'status': category.get('state') or source.get('state'),
                              'message': category.get('message') or source.get('message') or '',
                              'intervals': category.get('intervals') or [], 'intervalSeconds': category.get('intervalSeconds'),
                              'actualImages': category.get('actualImages'), 'estimatedImages': category.get('estimatedImages'),
                              'taskId': category.get('taskId'), 'importJobId': category.get('importJobId'),
                              'targetDatasetId': category.get('targetDatasetId'), 'packageId': category.get('packageId')})
            terminal = {'complete', 'no_target', 'excluded'}
            status = ('attention' if any(i['status'] in {'attention', 'unavailable', 'review'} for i in items) else
                      'complete' if items and all(i['status'] in terminal for i in items) else 'running' if items else 'recorded')
            value.update(status=status, submitted=sum(bool(i.get('taskId') or i.get('packageId')) for i in items),
                         completed=sum(i['status'] in terminal for i in items))
        for item in items:
            # Immutable times/name, never status or list position: survives polling/reordering.
            key = item.get('id') or [item.get(k) for k in ('videoName', 'startTime', 'endTime', 'camera')]
            digest = sha256(str(key).encode()).hexdigest()[:32]
            item.update(deletionKey=f"{row['id']}:{digest}", revision=0)
        hidden = self.submissions.item_visibility.hidden() if self.submissions else set()
        items = [item for item in items if item['deletionKey'] not in hidden]
        value['submitted'] = sum(item.get('status') == 'submitted' if row['kind'] == 'collection'
                                 else bool(item.get('taskId') or item.get('packageId')) for item in items)
        if row['kind'] == 'slicing':
            value['completed'] = sum(item.get('status') in terminal for item in items)
        value['revision'] = 0
        value['total'] = len(items)
        value['detail'] = metadata
        if detail:
            pages = max(1, ceil(len(items) / page_size))
            current = min(max(1, page), pages)
            metadata.update(items=items[(current - 1) * page_size:current * page_size],
                            page=current, pages=pages, total=len(items))
        return value
