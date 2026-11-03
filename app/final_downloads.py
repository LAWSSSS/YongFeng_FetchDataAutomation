"""Bounded per-round waiting for approved downloads; never changes media identity."""
import asyncio
import os
from pathlib import Path
import shutil

from .workspace_files import read_marker
from .file_leases import FileEnvironmentError


RECOVERABLE = {'network', 'temporary_media', 'media_unavailable', 'incomplete', 'disk_full',
               'filesystem', 'directory_missing', 'media_tool_missing', 'media_check_timeout'}
DELAYS = (30, 60, 120, 300)
REASONS = {
    'network': '网络暂不可用', 'temporary_media': '媒体服务暂不可用', 'media_unavailable': '平台媒体暂不可用',
    'incomplete': '下载连接中断', 'disk_full': '磁盘空间不足', 'filesystem': '文件目录暂不可用',
    'directory_missing': '保存目录暂不可用', 'media_tool_missing': '视频检查工具不可用',
    'media_check_timeout': '视频检查暂时超时',
}


def environment_ready(flow, version, code):
    """Read-only probes; no directory creation on an absent external volume."""
    if not flow.videos.storage_ready:
        return False, '等待保存目录恢复'
    directory = flow.videos.store.directory()
    if not directory:
        return False, '等待设置保存目录'
    roots = [Path(directory), Path(flow.videos.work)]
    try:
        workspace = flow.workspace
        boundary = workspace.root if workspace.enabled else None
        if boundary:
            marker = read_marker(boundary)
            if not marker or marker['id'] != workspace.config['workspaceId']:
                return False, '等待原工作区磁盘连接'
        for root in roots:
            if boundary and not root.is_relative_to(boundary):
                return False, '下载目录与工作区不一致，等待核对'
            # storage_ready is advisory and may predate an unplug. Never walk
            # out of a managed root, or through an absent macOS mount point.
            if root.parts[:2] == ('/', 'Volumes') and len(root.parts) > 2:
                mount = Path(*root.parts[:3])
                if not mount.is_mount():
                    return False, '等待外置盘挂载'
            while not root.exists() and root != root.parent and root != boundary:
                root = root.parent
            if not root.is_dir() or not os.access(root, os.W_OK | os.X_OK):
                return False, '等待文件目录恢复可写'
            remaining = max(0, int(version.get('totalBytes') or 0) - int(version.get('receivedBytes') or 0))
            if shutil.disk_usage(root).free < remaining + 16 * 1024 * 1024:
                return False, '等待磁盘释放空间'
        if code in {'media_tool_missing', 'media_check_timeout'}:
            tool = flow.slicing.store.settings.get('frameToolPath') or shutil.which('ffmpeg')
            probe = str(Path(tool).with_name('ffprobe')) if tool else ''
            if not tool or not os.access(tool, os.X_OK) or not os.access(probe, os.X_OK):
                return False, '等待视频检查工具恢复'
    except (OSError, TypeError, ValueError):
        return False, '等待文件目录恢复'
    return True, ''


async def continue_download(flow, operation, row, version):
    """Return true when this is an environmental wait, whether or not retried."""
    code = version.get('errorCode')
    if code not in RECOVERABLE:
        return False
    now = flow.clock()
    retry = operation.setdefault('downloadRecovery', {'attempts': 0, 'nextRetryAt': now + DELAYS[0]})
    operation.update(state='waiting', stage='download_wait',
        message=f"{REASONS[code]}，保留断点，恢复后自动继续。", downloadRetryCount=retry['attempts'])
    if now < retry['nextRetryAt']:
        return True
    if not flow.videos.target.connected:
        retry['nextRetryAt'] = now + 30
        operation['message'] = '等待采集平台连接，下载断点保留。'
        return True
    try:
        probes = getattr(flow.workspace, 'file_checks', None)
        ready, reason = (await probes.run(('final-download-environment', operation['id']),
            environment_ready, flow, version, code) if probes else
            await asyncio.to_thread(environment_ready, flow, version, code))
    except FileEnvironmentError:
        ready, reason = False, '等待磁盘检查恢复响应'
    if not ready:
        retry['nextRetryAt'] = now + 30
        operation['message'] = reason + '，下载断点保留。'
        return True
    lock = flow.videos.row_lock(row['id'])
    if lock.locked():
        retry['nextRetryAt'] = now + 30
        operation['message'] = '等待本视频的文件操作结束，随后自动继续下载。'
        return True
    async with lock:
        current = flow.videos.store.get(row['id'])
        active = flow.videos.version(current, operation['version']) if current else None
        approved = flow.current_operation(current, active) if current and active else None
        if (not approved or approved['id'] != operation['id'] or not active.get('downloadRequested')
                or active.get('state') != 'failed' or active.get('errorCode') != code
                or flow.wait_reason(row['id'])):
            return True
        active.update(state='ready')
        active.pop('message', None)
        flow.videos.store.save(current)
        retry['attempts'] += 1
        retry['nextRetryAt'] = now + DELAYS[min(retry['attempts'], len(DELAYS) - 1)]
        operation.update(state='running', stage='download', downloadRetryCount=retry['attempts'],
                         message='连接和目录已恢复，正在从原断点继续下载。')
        flow.videos.schedule(current)
    return True
