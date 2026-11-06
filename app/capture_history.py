"""Pending capture identities and receipts retained after local history is pruned."""
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


def unconfirmed_capture_keys(store, target, *, exclude_job=''):
    jobs = store.list()
    keys = {item['key'] for job in jobs if job['id'] != exclude_job and same_target(job.get('target'), target)
            for item in job['items'] if item['status'] in {'submitting', 'uncertain'}
            and not (item.get('bindingVerified') and item.get('remoteId'))}
    if not store.db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='simple_videos'").fetchone():
        return keys
    targets = {job['id']: job.get('target') for job in jobs}
    # A correction can have reached the platform before the new task is visible
    # in its inventory. Local hiding does not resolve that pending write.
    with closing(store.db.execute("""
        SELECT json_extract(v.payload,'$.jobId'), json_extract(v.payload,'$.submissionTarget'),
               json_extract(version.value,'$.startTime'), json_extract(version.value,'$.endTime')
        FROM simple_videos v, json_each(v.payload,'$.versions') version
        WHERE json_extract(version.value,'$.writeAttempted')=1
          AND coalesce(json_extract(version.value,'$.taskId'),'')=''
          AND coalesce(json_extract(version.value,'$.submissionOutcome'),'')!='rejected'
          AND coalesce(json_extract(version.value,'$.state'),'') NOT IN ('withdrawn','cloud_deleted')
    """)) as versions:
        for job_id, recorded_target, start, end in versions:
            owner = targets.get(job_id) or (json.loads(recorded_target) if recorded_target else None)
            if start and end and same_target(owner, target):
                keys.add(task_key(target, start, end))
    return keys


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
