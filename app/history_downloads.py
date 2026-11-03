"""Managed historical downloads. Never confirms or publishes a current version."""
import asyncio
import copy
import hashlib
import json
from functools import partial
from pathlib import Path

from .domain import now_iso
from .media import publish_file, file_digest
from .videos import Conflict
from .remote import RemoteError
from .workspace_files import file_version, folder_name


class HistoryDownloads:
    def __init__(self, videos, workspace):
        self.videos, self.workspace, self.db = videos, workspace, videos.store.db
        self.db.execute('CREATE TABLE IF NOT EXISTS simple_history_downloads (id TEXT PRIMARY KEY,payload TEXT NOT NULL)')
        self.db.commit()
        self.task = None
        self.wake = asyncio.Event()

    def rows(self):
        return [json.loads(r[0]) for r in self.db.execute('SELECT payload FROM simple_history_downloads')]

    def save(self, row):
        row['updatedAt'] = now_iso()
        self.db.execute('INSERT INTO simple_history_downloads VALUES(?,?) ON CONFLICT(id) DO UPDATE SET payload=excluded.payload',
                        (row['id'], json.dumps(row, ensure_ascii=False)))
        self.db.commit()

    def register(self, identity, payload):
        key = hashlib.sha256(f"{identity}:{payload['requestId']}".encode()).hexdigest()[:32]
        prior = next((r for r in self.rows() if r['id'] == key), None)
        if prior:
            if prior['request'] != payload:
                raise Conflict('请求编号已用于其他历史下载。')
            return {'operationId': key, 'state': prior['state']}
        row = self.videos.store.get(identity)
        if not row or row.get('revision') != payload['expectedRevision']:
            raise Conflict('视频记录已变化，请刷新后重试。')
        version = self.videos.version(row, payload['version'])
        if not version or version['revision'] == row['activeRevision'] or version['state'] not in {'ready','saved','failed','attention'}:
            raise ValueError('此版本不可下载；当前版本请使用确认处理。')
        if not version.get('taskId') or not version.get('resultId'):
            raise ValueError('历史版本缺少唯一平台凭证。')
        same = next((r for r in self.rows() if r['videoId'] == identity and r['version'] == version['revision']
                     and r['source'] == {k: version[k] for k in ('taskId','resultId')} and r['state'] != 'attention'), None)
        if same:
            return {'operationId': same['id'], 'state': same['state']}
        self.save({'id': key, 'videoId': identity, 'version': version['revision'], 'request': payload,
                   'source': {k: version[k] for k in ('taskId','resultId')}, 'snapshot': copy.deepcopy(version),
                   'state': 'queued', 'createdAt': now_iso()})
        self.wake.set()
        return {'operationId': key, 'state': 'queued'}

    async def execute(self, operation):
        if not self.workspace.online or not self.videos.target.connected:
            operation.update(state='waiting', message='等待磁盘或平台连接。')
            self.save(operation)
            return
        row = self.videos.store.get(operation['videoId'])
        current = self.videos.version(row, operation['version']) if row else None
        if not current or any(current.get(k) != v for k, v in operation['source'].items()):
            raise Conflict('历史视频绑定已变化，未继续下载。')
        version = operation['snapshot']
        clone = copy.copy(self.videos)
        class ProgressStore:
            def save(_, _row):
                operation.update(state='running', receivedBytes=version.get('receivedBytes', 0), totalBytes=version.get('totalBytes', 0))
                self.save(operation)
        clone.store = ProgressStore()
        clone.work = self.workspace.root / '.缓存' / '历史下载'
        folder = self.workspace.root / '采集视频' / (folder_name(row['name']) + '_历史_' + operation['id'])
        destination = folder / f"{folder_name(row['name'])}_v{operation['version']+1}.mp4"
        with self.videos.file_leases.read(self.videos.cloud_lease_key(version['taskId'])):
            async with self.videos.download_slots:
                if operation.get('publishing'):
                    publication = operation['publishing']
                    part = Path(publication['part'])
                    expected = publication['sha256']
                else:
                    part, _result, expected, _size = await clone.download_bound({'id': operation['id']}, version)
                    operation['publishing'] = {'part': str(part), 'sha256': expected}
                    self.save(operation)
                latest = self.videos.store.get(operation['videoId'])
                bound = self.videos.version(latest, operation['version']) if latest else None
                if not bound or any(bound.get(k) != v for k, v in operation['source'].items()):
                    raise Conflict('下载期间历史绑定已变化，缓存保留。')
                await self.videos.file_operation(partial(folder.mkdir, parents=True, exist_ok=True))
                with self.videos.file_leases.write(destination):
                    if destination.exists():
                        if await self.videos.file_operation(file_digest, destination) != expected:
                            raise Conflict('历史文件目标已被不同内容占用，未覆盖。')
                    else:
                        await self.videos.file_operation(publish_file, part, destination, expected)
                    operation.update(state='complete', filePath=str(destination), fileVersion=await self.videos.file_operation(file_version, destination, True),
                                     sha256=expected, message='历史版本已保存在数据文件夹。')
                    self.save(operation)
                    await self.videos.file_operation(partial(part.unlink, missing_ok=True))

    async def start(self):
        self.task = asyncio.create_task(self.loop())

    async def loop(self):
        while True:
            self.wake.clear()
            for row in self.rows():
                if row['state'] not in {'queued','running','waiting'}:
                    continue
                try:
                    await self.execute(row)
                except (OSError, ValueError, RemoteError) as error:
                    code = getattr(error, 'code', '')
                    row.update(state='waiting' if isinstance(error, (OSError, RemoteError)) or code in {'network','disk_full','temporary_media'} else 'attention',
                               message='下载暂未完成，已保留断点。', errorCode=code or type(error).__name__)
                    self.save(row)
            try:
                await asyncio.wait_for(self.wake.wait(), 15)
            except asyncio.TimeoutError:
                pass

    async def shutdown(self):
        if self.task:
            self.task.cancel()
            await asyncio.gather(self.task, return_exceptions=True)
