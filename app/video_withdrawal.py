"""Version-bound, resumable withdrawal without holding a record lock on I/O."""
import copy
import time

from .domain import now_iso
from .file_leases import FileEnvironmentError
from .videos import Conflict, TERMINAL

RETRYABLE_CLEANUP_MESSAGES = {
    '存储端未确认删除旧文件，稍后继续核对。',
    '旧视频对象仍可访问，云端清理尚未完成。',
    '旧任务仍存在，稍后继续核对。',
    '旧结果记录仍存在，稍后继续核对。',
    '更正任务仍存在，暂不能确认撤回完成。',
    '更正任务仍在停止，稍后继续核对。',
}


def _previous_version(manager, row, version):
    def available(candidate):
        return (candidate and candidate.get('state') not in TERMINAL
                and candidate.get('submissionOutcome') != 'rejected')

    previous = manager.version(row, version.get('previousActiveRevision'))
    if available(previous):
        return previous
    revision = version.get('revision', 0)
    return next((candidate for candidate in reversed(row['versions'])
                 if candidate['revision'] < revision and available(candidate)), None)


def capability(manager, row):
    version = manager.version(row, row.get('activeRevision')) or {}
    if row.get('deletion', {}).get('state') in {'pending', 'error'}:
        reason = '记录正在清理。'
    elif not version.get('revision'):
        reason = '初次采集无需撤回。'
    elif (version.get('finalConfirmation') or row.get('acceptedRevision') == version['revision']
          or version.get('state') in {'saved', 'downloading', 'publishing'}):
        reason = '此版已最终确认，请通过再次更正调整。'
    elif not _previous_version(manager, row, version):
        reason = '没有可恢复的上一版本。'
    elif version.get('state') in TERMINAL:
        reason = '此版本已经撤回。'
    else:
        reason = ''
    return {'canWithdraw': not bool(reason), 'withdrawReason': reason}


def current(manager, operation):
    row = manager.store.get(operation['videoId'])
    if not row or row.get('deletedAt') or row['revision'] != operation['expectedRevision'] or row['activeRevision'] != operation['version']:
        raise Conflict('视频已有新操作，未撤回其他版本。')
    version = manager.version(row, operation['version'])
    allowed = capability(manager, row)
    if not allowed['canWithdraw']:
        raise Conflict(allowed['withdrawReason'])
    if getattr(manager, 'control', None):
        control = manager.control.get('collection:' + row['id']) or {}
        if control.get('generation') != operation.get('mediaGeneration') or control.get('requestId') != operation['id']:
            raise Conflict('已有更新的视频指令，撤回未覆盖新操作。')
    return row, version


async def execute(manager, operation):
    row, version = current(manager, operation)
    # Fence older queued withdrawals too, before yielding to the platform.
    if version['state'] != 'withdrawing':
        version['state'] = 'withdrawing'
        manager.store.save(row)
    if getattr(manager, 'control', None):
        await manager.control.take_over(row['id'], operation['id'], operation.get('mediaGeneration'))
    if version.get('writeAttempted') and not version.get('taskId') and version.get('submissionOutcome') != 'rejected':
        if not isinstance(version.get('beforeIds'), list):
            raise ValueError('缺少提交前快照，撤回需要人工核对任务身份。')
        parents = await manager.remote_parents_cached(fresh=True)
        matches = [p for key,p in parents.items() if key not in version['beforeIds']
                   and manager.matches(p, version, manager.target_config['cameraId'])]
        row, version = current(manager, operation)
        if len(matches) > 1:
            raise ValueError('多个新任务匹配本次更正，保留原片，请核对准确任务ID。')
        if not matches:
            operation.update(state='reconciling', stage='waiting_binding', nextAttemptAt=time.time()+30,
                             message='撤回已登记，等待确认原提交结果；不会重复采集。')
            manager.store.save_operation(operation)
            return
        version['taskId'] = str(matches[0]['id'])
        manager.store.save(row)
    if version.get('taskId'):
        operation.update(stage='cleaning_correction', message='正在清理本次更正的确切任务，上一版保留。')
        manager.store.save_operation(operation)
        snapshot = copy.deepcopy(row)
        snapshot['_withdrawCheckpoint'] = operation['version']
        target = manager.version(snapshot, operation['version'])
        protected = {v.get('taskId') for v in row['versions'] if v['revision'] != operation['version']}
        try:
            await manager.cleanup_cloud(snapshot, target, protected=protected, allow_incomplete=True)
        except ValueError as error:
            occupied = isinstance(error, FileEnvironmentError)
            if occupied or str(error) in RETRYABLE_CLEANUP_MESSAGES:
                operation['cleanupChecks'] = operation.get('cleanupChecks', 0) + (0 if occupied else 1)
                if operation['cleanupChecks'] <= 2:
                    operation.update(state='reconciling', stage='waiting_cleanup', nextAttemptAt=time.time()+5,
                                     message='撤回已登记，等待占用释放或核对删除结果；上一版保留。')
                    manager.store.save_operation(operation)
                    return
            raise
    async with manager.row_lock(row['id']):
        row, version = current(manager, operation)
        previous = _previous_version(manager, row, version)
        version.update(state='withdrawn', withdrawnAt=now_iso())
        row['activeRevision'] = previous['revision']
        row['revision'] += 1
        row['requests'][operation['requestId']] = {'kind':'withdraw', 'version':version['revision']}
        operation.update(state='complete', stage='complete', resultRevision=row['revision'],
                         message='更正已撤回，已恢复上一有效版本。')
        for field in ('nextAttemptAt', 'errorCode', 'failedStage', 'failureType', 'failureErrno'):
            operation.pop(field, None)
        with manager.store.db:
            manager.store.save(row, commit=False)
            manager.store.save_operation(operation, commit=False)


def cleanup_checkpoint(manager, row, version):
    if '_withdrawCheckpoint' not in row:
        manager.store.save(row)
        return
    latest = manager.store.get(row['id'])
    target = manager.version(latest, version['revision']) if latest else None
    if not target or target.get('taskId') != version.get('taskId'):
        raise Conflict('清理期间任务身份变化，未覆盖其他版本。')
    for key in ('cleanup', 'resultId'):
        if key in version:
            target[key] = copy.deepcopy(version[key])
    manager.store.save(latest)
