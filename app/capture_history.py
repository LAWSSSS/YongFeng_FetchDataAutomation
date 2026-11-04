"""Minimal task receipts kept after local capture history is pruned."""
import json
from contextlib import closing

from .domain import task_key


TARGET_FIELDS = ('origin', 'cameraId', 'embedId')


def same_target(left, right):
    return all(left and right and left.get(key) and left.get(key) == right.get(key)
               for key in TARGET_FIELDS)


def target_of(store, video):
    job = store.get(video.get('jobId', ''))
    return (job or {}).get('target') or video.get('submissionTarget')


def keep_receipts(db, video, job):
    target = (job or {}).get('target') or video.get('submissionTarget')
    if not same_target(target, target) or not video.get('recordId'):
        return
    target = {key: target[key] for key in TARGET_FIELDS}
    item = next((item for item in (job or {}).get('items', [])
                 if item.get('key') == video.get('taskKey')
                 and str(item.get('recordId')) == str(video['recordId'])), None)
    db.execute('CREATE TABLE IF NOT EXISTS retired_capture_receipts ('
               'video_id TEXT, version INTEGER, record_id TEXT, task_key TEXT, payload TEXT NOT NULL, '
               'PRIMARY KEY(video_id,version))')
    db.execute('CREATE INDEX IF NOT EXISTS retired_capture_lookup ON retired_capture_receipts(record_id,task_key)')
    for version in video.get('versions', []):
        # A positive binding comes from the original job ID or a versioned
        # correction receipt. A matching name/time alone is insufficient.
        task_id = str(version.get('taskId') or '')
        linked = (item and item.get('status') == 'submitted' and str(item.get('remoteId')) == task_id
                  if version['revision'] == 0 and job else
                  version.get('writeAttempted') or video.get('submissionTarget'))
        if not (linked and task_id and version.get('startTime') and version.get('endTime') and version.get('name')):
            continue
        facts = {k: version[k] for k in ('revision', 'taskId', 'resultId', 'startTime', 'endTime',
                                        'durationSeconds', 'name') if k in version}
        value = {'videoId': video['id'], 'jobId': video.get('jobId', ''), 'target': target, 'version': facts}
        key = task_key(target, facts['startTime'], facts['endTime'])
        db.execute('INSERT OR IGNORE INTO retired_capture_receipts VALUES(?,?,?,?,?)',
                   (video['id'], version['revision'], str(video['recordId']), key, json.dumps(value, ensure_ascii=False)))


def matching_receipts(db, record_id, target, start, end):
    if not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='retired_capture_receipts'").fetchone():
        return []
    with closing(db.execute('SELECT payload FROM retired_capture_receipts WHERE record_id=? AND task_key=? ORDER BY version',
                            (str(record_id), task_key(target, start, end)))) as rows:
        return [json.loads(raw) for raw, in rows]
