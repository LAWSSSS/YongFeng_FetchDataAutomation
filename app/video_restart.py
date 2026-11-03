"""Durable full-download restart; preflight never revokes current approval."""
import time
from contextlib import nullcontext

from .domain import now_iso
from .file_leases import FileEnvironmentError


def wait(manager, operation, stage, message):
    operation.update(state='waiting_connection', stage=stage, message=message, nextAttemptAt=time.time() + 30)
    manager.store.save_operation(operation)


def current(manager, operation):
    from .videos import Conflict
    row = manager.store.get(operation['videoId'])
    version = manager.version(row, operation['version']) if row else None
    control = manager.control.get('collection:' + operation['videoId']) if getattr(manager, 'control', None) else {}
    if (not row or row.get('deletedAt') or not version or row['revision'] != operation['expectedRevision']
            or row['activeRevision'] != operation['version']
            or any(version.get(k) != value for k, value in operation['source'].items())):
        raise Conflict('视频版本或结果身份已变化，未清除其他版本的断点。')
    generation = int((control or {}).get('generation') or 0)
    expected = operation.get('mediaGeneration', operation.get('sourceGeneration', 0))
    if generation != expected and (control or {}).get('requestId') != operation['id']:
        raise Conflict('已有更新的视频指令，原断点保留。')
    manager.require_editable(row)
    return row, version, control or {}


async def execute(manager, operation):
    row, version, control = current(manager, operation)
    flow = getattr(manager, 'final_processing', None)
    approved = flow.current_operation(row, version) if flow else None
    if (control.get('paused') or (flow and flow.pipeline.paused) or (approved or {}).get('manualPaused')):
        wait(manager, operation, 'paused', '此视频已暂停，完整重下已登记，等待明确继续。')
        return
    workspace = getattr(manager, 'workspace', None)
    if workspace and workspace.enabled:
        if workspace.maintenance:
            wait(manager, operation, 'waiting_disk', '等待工作区整理结束，原断点保留。')
            return
        try:
            if hasattr(workspace, 'check_root'):
                await workspace.check_root()
            else:
                await manager.file_operation(workspace.require_root)
        except (FileEnvironmentError, OSError):
            wait(manager, operation, 'waiting_disk', '等待原工作区磁盘连接，原批准和断点保留。')
            return
    if not manager.target.connected:
        wait(manager, operation, 'waiting_connection', '等待采集平台连接，原批准和断点保留。')
        return
    operation.update(state='running', stage='preflight', message='正在核对当前视频结果和保存位置。')
    manager.store.save_operation(operation)
    await manager.bound_result(version)
    if not manager.store.directory():
        wait(manager, operation, 'waiting_disk', '等待设置保存目录，原断点保留。')
        return
    part = manager.work / row['id'] / f"v{version['revision']}.part"
    def check_path():
        if part.is_symlink() or part.parent.is_symlink() or not part.resolve().is_relative_to(manager.work.resolve()):
            raise ValueError('断点路径异常，未清除任何文件。')
    await manager.file_operation(check_path)
    async with manager.row_lock(row['id']):
        row, version, control = current(manager, operation)
        approved = flow.current_operation(row, version) if flow else None
        if control.get('paused') or (flow and flow.pipeline.paused) or (approved or {}).get('manualPaused'):
            wait(manager, operation, 'paused', '此视频已暂停，保留原批准和断点。')
            return
        if version.get('state') not in {'failed', 'ready'} or version.get('publishing') or version.get('transferWorkerPid'):
            raise ValueError('当前版本正在保存或已有成果，未清除任何断点。')
        if getattr(manager, 'control', None):
            operation['mediaGeneration'] = manager.control.reserve(row['id'], operation['id'])
        operation.update(stage='taking_over', message='核对通过，正在安全接管本次重下。')
        manager.store.save_operation(operation)
    if getattr(manager, 'control', None):
        await manager.control.take_over(row['id'], operation['id'], operation['mediaGeneration'])
    async with manager.row_lock(row['id']):
        row, version, _ = current(manager, operation)
        guard = manager.file_leases.write(part) if getattr(manager, 'file_leases', None) else nullcontext()
        with guard:
            if flow and version.get('finalConfirmation'):
                flow.rebind_confirmed_retry(row, version, operation['id'])
            operation.update(stage='clearing_checkpoint', message='正在清除本版本的旧断点。')
            manager.store.save_operation(operation)
            await manager.file_operation(part.unlink, True)
            for key in ('etag', 'totalBytes', 'receivedBytes', 'attempts', 'errorCode', 'message'):
                version.pop(key, None)
            version.update(state='ready', downloadRequested=True,
                           outputDirectory=version.get('outputDirectory') or manager.store.directory())
            row['requests'][operation['requestId']] = {'kind': 'restart-download', 'version': operation['version'],
                'expectedRevision': operation['expectedRevision'], 'state': 'complete'}
            manager.store.save(row)
            operation.update(state='complete', stage='download_queued', message='完整重下已登记，正在等待下载槽。',
                             completedAt=now_iso())
            operation.pop('nextAttemptAt', None)
            manager.store.save_operation(operation)
    if getattr(manager, 'control', None):
        manager.control.release_review(row['id'], operation['id'])
    manager.schedule(row)
    manager.wake.set()
