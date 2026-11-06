"""Stop an exact in-progress capture and remove its task and local record."""
import time

from .capture_history import same_target, target_of
from .domain import now_iso, task_key
from .jobs import ENDED_CAPTURES
from .remote import RemoteError
from .videos import Conflict


COLLECTING = {'pending', 'submitted', 'waiting'}


def capture_in_flight(version):
    if not version:
        return False
    state = version.get('state')
    return (state in {'pending', 'submitted', 'submitting', 'uncertain', 'terminating'}
            or state == 'waiting' and bool(version.get('taskId') or version.get('writeAttempted')))


def can_terminate(row, version):
    return bool(version and version.get('state') in COLLECTING and version.get('taskId')
                and not version.get('resultId')
                and not version.get('finalConfirmation') and not row.get('cleanupPending')
                and not row.get('duplicateOfVideoId')
                and row.get('deletion', {}).get('state') not in {'pending', 'error'})


def active_version(manager, operation):
    row = manager.store.get(operation['videoId'])
    version = manager.version(row, operation['version']) if row else None
    if (not row or row.get('deletedAt') or row['revision'] != operation['expectedRevision']
            or row['activeRevision'] != operation['version'] or not version
            or version.get('state') != 'terminating' or version.get('taskId') != operation['taskId']):
        raise Conflict('记录或绑定任务已变化，请刷新后核对。')
    return row, version


def status(parent):
    return str(parent.get('taskStatus') or parent.get('status') or '').upper()


def reconcile_later(manager, operation):
    operation['checks'] = operation.get('checks', 0) + 1
    if operation['checks'] >= 4:
        raise Conflict('平台尚未确认终止或删除，记录已保留，请核对后重试。')
    operation.update(state='reconciling', stage='reconciling', nextAttemptAt=time.time() + 5,
                     message='等待平台确认终止和删除。')
    manager.store.save_operation(operation)


def refresh_inventory(manager):
    manager.inventory_cached_at = 0
    manager.inventory_generation = getattr(manager, 'inventory_generation', 0) + 1
    manager.wake.set()


async def request_change(manager, operation, flag, stage, message, path, body):
    operation.update({flag: True, 'stage': stage, 'message': message})
    manager.store.save_operation(operation)
    try:
        await manager.target.request(path, method='POST', body=body, write=True)
    except RemoteError as error:
        if not error.ambiguous and error.code in {'401', '403', '11015'}:
            operation.pop(flag, None)  # Reconnect can retry an explicitly rejected request.
            manager.store.save_operation(operation)
        raise


async def execute(manager, operation):
    task_id = operation['taskId']
    target = manager.target_config
    async with manager.jobs.lock:
        row, _ = active_version(manager, operation)
        if (not same_target(target_of(manager.jobs.store, row), target)
                or any(v['revision'] != operation['version'] and v.get('taskId') == task_id for v in row['versions'])):
            raise Conflict('任务所属采集节点或版本无法确认，记录已保留。')
        if any(v.get('taskId') == task_id for other in manager.store.list()
               if other['id'] != row['id'] and not other.get('duplicateOfVideoId')
               for v in other['versions']):
            raise Conflict('其他视频记录也绑定了此平台任务，记录已保留，请先核对。')
        parent = (await manager.jobs.remote_parents(target)).get(task_id)
        if parent is None and not (operation.get('deleteRequested') or operation.get('previousDeleteRequested')):
            raise Conflict('未找到绑定的平台任务，记录已保留，请先核对。')
        if parent is not None:
            _, version = active_version(manager, operation)
            if not manager.jobs.parent_matches(parent, target, {**version, 'videoName': version['name']}):
                raise Conflict('平台任务的相机、名称或时间已变化，记录已保留。')
            if status(parent) == 'FIXED' or await manager.results(task_id):
                raise Conflict('平台已生成视频结果，记录和视频均已保留。')
            if status(parent) not in ENDED_CAPTURES:
                if operation.get('stopRequested') or operation.get('deleteRequested'):
                    reconcile_later(manager, operation)
                    return
                active_version(manager, operation)
                await request_change(manager, operation, 'stopRequested', 'stopping_capture', '正在终止平台采集。',
                                     '/data-acquisition/end-task', {'taskId': task_id})
                parent = (await manager.jobs.remote_parents(target)).get(task_id)
                if (not parent or not manager.jobs.parent_matches(parent, target, {**version, 'videoName': version['name']})):
                    raise Conflict('终止后无法核实原任务，记录已保留。')
                if status(parent) not in ENDED_CAPTURES:
                    reconcile_later(manager, operation)
                    return
                if status(parent) == 'FIXED' or await manager.results(task_id):
                    raise Conflict('终止期间平台生成了视频结果，记录和视频均已保留。')
            if operation.get('deleteRequested'):
                reconcile_later(manager, operation)
                return
            active_version(manager, operation)
            operation['checks'] = 0
            await request_change(manager, operation, 'deleteRequested', 'deleting_capture',
                                 '平台采集已停止，正在删除任务记录。',
                                 '/data-acquisition/delete', {'id': task_id, 'type': 'task'})
            if task_id in await manager.jobs.remote_parents(target):
                reconcile_later(manager, operation)
                return
        if await manager.results(task_id):
            raise Conflict('平台仍有视频结果，采集记录已保留，请核对。')
        async with manager.row_lock(operation['videoId']):
            row, version = active_version(manager, operation)
            finished_at = now_iso()
            row.update(deletedAt=finished_at, deletionReason='terminated-capture', revision=row['revision'] + 1)
            version.update(state='cloud_deleted', platformStatus='ENDED', downloadRequested=False)
            operation.update(state='complete', stage='complete', resultRevision=row['revision'],
                             message='采集已终止，平台任务和网页记录已删除。')
            for key in ('nextAttemptAt', 'errorCode', 'failedStage', 'failureType', 'failureErrno'):
                operation.pop(key, None)
            interval = task_key(target, version['startTime'], version['endTime'])
            with manager.store.db:
                for job in manager.jobs.store.list():
                    if not same_target(job['target'], target):
                        continue
                    items = [item for item in job['items'] if item['status'] != 'removed' and (
                        str(item.get('remoteId')) == task_id or
                        (operation['version'] == 0 and item['status'] == 'duplicate' and not item.get('remoteId')
                         and item.get('existingVideoId') == row['id'] and item.get('key') == interval))]
                    if items:
                        for item in items:
                            item.update(status='removed', removedAt=finished_at, message=operation['message'])
                        manager.jobs.store.save(job, commit=False)
                manager.store.save(row, commit=False)
                manager.store.save_operation(operation, commit=False)
    refresh_inventory(manager)


async def release_failed(manager, operation):
    async with manager.row_lock(operation['videoId']):
        try:
            row, version = active_version(manager, operation)
        except Conflict:
            return
        version['state'] = operation['previousState']
        manager.store.save(row)
    refresh_inventory(manager)
