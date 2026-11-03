"""One bounded metadata check per immutable local file, shared by both targets."""
import asyncio
import json
from pathlib import Path

from .audit_moves import source_fingerprint
from .file_leases import finish_io
from .workspace_files import compatible


def same_stat(actual, expected):
    keys = ('device', 'inode', 'size', 'modifiedNs')
    return all(key in expected and actual.get(key) == expected[key] for key in keys)


class UploadPreflight:
    def setup_preflight(self):
        self.preflight_tasks = {}
        self.preflight_receipts = {}
        self.preflight_slots = asyncio.Semaphore(2)
        self.probe_recovery_seen = set()
        self.probe_recovery_task = None

    def start_probe_recovery(self):
        if self.probe_recovery_task and not self.probe_recovery_task.done():
            return
        candidates = [item for item in self.store.list('items') if item.get('state') == 'attention'
                      and item.get('errorCode') == 'incomplete_video' and not item.get('createAttempted')
                      and not item.get('uploadId') and not item.get('parts')
                      and (item['id'], json.dumps(item.get('fileVersion'), sort_keys=True)) not in self.probe_recovery_seen]
        if not any(self.known_source_version(item) for item in candidates):
            return
        self.probe_recovery_task = asyncio.create_task(self.restore_probe_timeouts(), name='recover-probe-timeouts')
        def finished(task):
            if not task.cancelled():
                task.exception()  # Consume diagnostic failures without logging source paths.
            self.wake.set()
        self.probe_recovery_task.add_done_callback(finished)

    async def stop_probe_recovery(self):
        if self.probe_recovery_task and not self.probe_recovery_task.done():
            self.probe_recovery_task.cancel()
            await asyncio.gather(self.probe_recovery_task, return_exceptions=True)

    def known_source_version(self, item):
        expected, path = item.get('fileVersion') or {}, item.get('path')
        if not {'device', 'inode', 'size', 'modifiedNs'}.issubset(expected):
            return None
        workspace = getattr(self, 'workspace', None)
        if workspace:
            binding, reason = workspace.collection_source_binding(item)
            if reason:
                return None
            if binding:
                row = workspace.videos.store.get(binding['collectionVideoId'])
                version = workspace.videos.version(row, binding['collectionRevision']) or {}
                saved = version.get('fileVersion') or {}
                if version.get('sha256') and saved.get('edgeHash') and compatible(saved, expected):
                    return saved
        # Persisted ffprobe metadata from the exact slicing version is also a
        # container receipt. It says nothing about individual frame decodability.
        rows = self.store.db.execute("SELECT payload FROM simple_slicing_items WHERE json_extract(payload, '$.path')=?", (path,))
        for (raw,) in rows:
            source = json.loads(raw)
            saved, metadata = source.get('fileVersion') or {}, source.get('metadata') or {}
            if (not source.get('superseded') and saved.get('edgeHash') and compatible(saved, expected)
                    and metadata.get('size') == expected['size'] and float(metadata.get('duration') or 0) > 0):
                return saved
        return None

    async def validate_upload_source(self, item):
        path, expected = item['path'], item['fileVersion']
        if not same_stat(source_fingerprint(Path(path)), expected):
            raise ValueError('file_changed')
        proof = self.known_source_version(item)
        key = json.dumps([path, expected, proof], sort_keys=True)
        from .slicing import source_version
        if key in self.preflight_receipts:
            with self.leases.read(path):
                if await finish_io(source_version, path) != self.preflight_receipts[key]:
                    raise ValueError('file_changed')
            return
        task = self.preflight_tasks.get(key)
        if not task:
            async def check():
                async with self.preflight_slots:
                    with self.leases.read(path):
                        if proof:
                            actual = await finish_io(source_version, path)
                            if actual != proof:
                                raise ValueError('file_changed')
                        else:
                            await finish_io(self.file_check, path, expected)
                            actual = await finish_io(source_version, path)
                        if not same_stat(source_fingerprint(Path(path)), expected):
                            raise ValueError('file_changed')
                self.preflight_receipts[key] = actual
            task = asyncio.create_task(check(), name='upload-preflight')
            self.preflight_tasks[key] = task
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            await asyncio.gather(task, return_exceptions=True)
            raise
        finally:
            if task.done() and self.preflight_tasks.get(key) is task:
                self.preflight_tasks.pop(key, None)

    async def restore_probe_timeouts(self):
        pending = []
        for item in self.store.list('items'):
            if (item.get('state') != 'attention' or item.get('errorCode') != 'incomplete_video'
                    or item.get('createAttempted') or item.get('uploadId') or item.get('parts')):
                continue
            key = (item['id'], json.dumps(item.get('fileVersion'), sort_keys=True))
            if key in self.probe_recovery_seen or not self.known_source_version(item):
                continue
            self.probe_recovery_seen.add(key)
            pending.append(item)
        async def recover(item):
            try:
                await self.validate_upload_source(item)
            except (OSError, ValueError):
                return
            latest = self.store.get('items', item['id'])
            if not latest or latest.get('state') != 'attention' or latest.get('errorCode') != 'incomplete_video':
                return
            item = latest
            self.detach(item)
            item.update(state='queued', phase='queued', errorCode='', nextAttempt=0,
                        message='已复用该文件版本的保存凭证，继续上传。')
            self.store.save('items', item)
            self.wake.set()
        if pending:
            await asyncio.gather(*(recover(item) for item in pending))
