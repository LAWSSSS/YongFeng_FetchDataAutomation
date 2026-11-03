"""One durable authority for collection versions and their local slicing work."""
import asyncio
import hashlib
import json
from pathlib import Path
from urllib.parse import urlencode

from .domain import now_iso
from .videos import Conflict


TERMINAL = {"complete", "no_target", "excluded", "superseded"}
SAFE_RETRY_ERRORS = {'worker_stalled', 'worker_failed', 'frame_extract_failed', 'frame_timeout',
                     'media_check_timeout', 'observations_incomplete', 'media_incomplete'}


class MediaControl:
    def __init__(self, db, videos, slicing, leases):
        self.db, self.videos, self.slicing, self.leases = db, videos, slicing, leases
        self.db.execute("CREATE TABLE IF NOT EXISTS simple_media_controls (id TEXT PRIMARY KEY, payload TEXT NOT NULL)")
        self.db.execute("CREATE TABLE IF NOT EXISTS simple_media_actions (id TEXT PRIMARY KEY, payload TEXT NOT NULL)")
        self.db.commit()
        self.locks = {}
        leases.subscribe_moves(self.moved)
        self.reconcile_finished_controls()
        self.refresh()

    def reconcile_finished_controls(self):
        """Recover a crash between the operation receipt and releasing its gate."""
        for (raw,) in self.db.execute("SELECT payload FROM simple_media_controls").fetchall():
            row = json.loads(raw)
            if not row.get("blocked") or not row.get("requestId"):
                continue
            operation = self.videos.store.get_operation(row["requestId"])
            if (operation and operation.get("videoId") == row.get("videoId")
                    and operation.get("state") in {"complete", "attention", "failed", "conflict", "cancelled"}):
                row.update(blocked=False, owner="review")
                self.save(row)

    async def verify_collection_file(self, item, path):
        """A path change must not silently change the accepted collection source."""
        from .file_leases import finish_io
        from .media import file_digest
        from .slicing import source_version
        video_id = item.get("collectionVideoId")
        if not video_id:
            return
        row = self.videos.store.get(video_id)
        version = self.videos.version(row, item.get("collectionRevision")) if row else None
        if not version or row.get("activeRevision") != item.get("collectionRevision"):
            raise Conflict("对应采集版本已变化，请刷新后重新绑定。")
        with self.leases.read(path):
            before = await finish_io(source_version, path)
            expected = version.get("fileVersion") or version.get("recovery", {}).get("fileVersion") or {}
            if not self.same_version(expected, before):
                digest = version.get("sha256") or version.get("recovery", {}).get("sha256")
                if not digest or await finish_io(file_digest, Path(path)) != digest:
                    raise Conflict("原片与采集版本不一致，未修改绑定。")
            if await finish_io(source_version, path) != before:
                raise Conflict("原片在核对时发生变化，未修改绑定。")
        latest = self.videos.store.get(video_id)
        if not latest or latest.get("revision") != row.get("revision") or not self.allowed(item):
            raise Conflict("采集指令已变化，请刷新后重新绑定。")

    def get(self, identity):
        row = self.db.execute("SELECT payload FROM simple_media_controls WHERE id=?", (identity,)).fetchone()
        return json.loads(row[0]) if row else None

    def save(self, row, *, commit=True):
        row["updatedAt"] = now_iso()
        self.db.execute("INSERT INTO simple_media_controls VALUES(?,?) ON CONFLICT(id) DO UPDATE SET payload=excluded.payload",
                        (row["id"], json.dumps(row, ensure_ascii=False)))
        if commit:
            self.db.commit()

    @staticmethod
    def same_version(left, right):
        keys = {"device", "inode", "size", "modifiedNs", "edgeHash"} & left.keys() & right.keys()
        return "size" in keys and len(keys) >= 3 and all(left[k] == right[k] for k in keys)

    def refresh(self):
        # Index only when persisted inputs changed. Repeated status reads must
        # not rebuild all collection-version and file identities.
        changes = (self.db.total_changes, self.db.execute('PRAGMA data_version').fetchone()[0])
        if getattr(self, '_indexed_changes', None) == changes:
            return
        index = {}
        for row in self.videos.store.list(include_deleted=True):
            for version in row.get("versions", []):
                recovery = version.get("recovery") or {}
                for path, fingerprint in ((version.get("filePath"), version.get("fileVersion") or {}),
                                          (recovery.get("filePath") or recovery.get("destination"), recovery.get("fileVersion") or {})):
                    if path:
                        index.setdefault(path, []).append((row["id"], version["revision"], fingerprint))
        items = self.slicing.store.list("items")
        physical = {}
        for item in items:
            if item.get("mediaId") and not item.get("superseded"):
                physical.setdefault(self.physical_key(item), []).append(item)
        for item in items:
            original = json.dumps(item, sort_keys=True)
            if not item.get("mediaId") or (item["mediaId"].startswith("local:") and not item.get("superseded")):
                matches = {(identity, revision) for identity, revision, version in index.get(item.get("path"), [])
                           if self.same_version(version, item.get("fileVersion") or {})}
                if item.get("collectionVideoId"):
                    matches = {(item["collectionVideoId"], item["collectionRevision"])}
                if len(matches) > 1:
                    item.update(identityConflict=True, processingReason="原片对应多个采集版本，请人工绑定。")
                    matches = set()
                if matches:
                    identity, revision = next(iter(matches))
                    item.update(collectionVideoId=identity, collectionRevision=revision, mediaId="collection:" + identity)
                else:
                    version = item.get("fileVersion") or {"unboundPath": item.get("path") or item["id"]}
                    content = {"version": version}
                    if not {"device", "inode", "size", "modifiedNs"}.issubset(version):
                        content["path"] = item.get("path") or item["id"]
                    digest = hashlib.sha256(json.dumps(content, sort_keys=True).encode()).hexdigest()[:32]
                    peers = physical.get(self.physical_key(item), [])
                    identities = {peer["mediaId"] for peer in peers}
                    if len(identities) == 1:
                        peer = peers[0]
                        for key in ("mediaId", "mediaGeneration", "collectionVideoId", "collectionRevision"):
                            if key in peer:
                                item[key] = peer[key]
                    else:
                        item["mediaId"] = "local:" + digest
            control = self.get(item["mediaId"])
            if not control:
                control = {"id": item["mediaId"], "videoId": item.get("collectionVideoId"), "generation": 0,
                           "owner": "slicing", "blocked": False, "createdAt": now_iso(), "paths": []}
            if item.get("path") and item["path"] not in control.setdefault("paths", []):
                control["paths"].append(item["path"])
                self.save(control)
            elif not self.get(control["id"]):
                self.save(control)
            item.setdefault("mediaGeneration", control["generation"])
            if original != json.dumps(item, sort_keys=True):
                self.slicing.store.save("items", item)
            physical.setdefault(self.physical_key(item), []).append(item)
        self._indexed_changes = (self.db.total_changes, changes[1])

    @staticmethod
    def physical_key(item):
        version = item.get("fileVersion") or {}
        key = json.dumps(version, sort_keys=True)
        if not {"device", "inode", "size", "modifiedNs"}.issubset(version):
            key += ":" + str(item.get("path") or item["id"])
        return key

    def moved(self, source, destination, version):
        for (raw,) in self.db.execute("SELECT payload FROM simple_media_controls").fetchall():
            row = json.loads(raw)
            if source in row.get("paths", []) and destination not in row["paths"]:
                row["paths"].append(destination)
                self.save(row)

    def reason(self, item, allow_paused=False):
        if item.get("identityConflict"):
            return item.get("processingReason") or "原片身份需要核对。"
        control = self.get(item.get("mediaId", ""))
        if control and control.get('paused') and not allow_paused:
            return '此视频已手动暂停，继续后才会处理。'
        if item.get("superseded") or (control and item.get("mediaGeneration", 0) != control["generation"]):
            return "旧版处理已停止，按视频查看与更正中的指令执行。"
        if control and control.get("blocked"):
            return "视频查看与更正正在处理此视频。"
        video_id = item.get("collectionVideoId")
        if video_id:
            row = self.videos.store.get(video_id)
            revision = item.get("collectionRevision")
            if not row or row.get("deletedAt") or row.get("activeRevision") != revision:
                return "此采集版本已撤回或被更正，停止旧版切片。"
            version = self.videos.version(row, revision) or {}
            if not item.get("collectionRecovery") and (row.get("acceptedRevision") != revision or version.get("state") != "saved"):
                return "等待当前版本经人工确认并下载完成。"
        return ""

    def recovery_covered_items(self, items, *, proof_inventory=None):
        """Identify unsent attempts covered by the same verified recovery.

        This never changes a task or approves a result. The recovery's own
        complete photo receipts remain mandatory for any archive or cleanup.
        """
        from .photo_completion import photo_results_verified
        from .recovery_results import partial_complete, verified_result

        candidates = [item for item in items if not item.get('superseded') and not item.get('collectionRecovery')
                      and item.get('state') in {'pending', 'attention'}
                      and item.get('collectionVideoId') and item.get('mediaId')]
        if not candidates:
            return {}
        by_id = {item['id']: item for item in items}
        if proof_inventory is None:
            changes = (self.db.total_changes, self.db.execute('PRAGMA data_version').fetchone()[0])
            if getattr(self, '_proof_changes', None) != changes:
                packages = self.slicing.packages.store.list('items')
                submitted_ids = {package.get('itemId') for package in packages if not package.get('superseded')}
                submitted_ids.update(operation.get('itemId') for operation in self.slicing.store.list('operations'))
                self._proof_inventory = packages, submitted_ids
                self._proof_changes = changes
            proof_inventory = self._proof_inventory
        packages, submitted_ids = proof_inventory
        covered, checked = {}, {}
        for item in candidates:
            if item['id'] in submitted_ids:
                continue
            categories = (item.get('categories') or {}).values()
            if any(category.get(key) for category in categories
                   for key in ('taskId', 'operationId', 'packageId', 'preSubmitTaskIds', 'preSubmitImageIds')):
                continue
            video_id, revision = item['collectionVideoId'], item.get('collectionRevision')
            key = (video_id, revision, item['mediaId'], item.get('mediaGeneration', 0))
            if key not in checked:
                row = self.videos.store.get(video_id)
                version = self.videos.version(row, revision) if row and row.get('activeRevision') == revision else None
                recovery = (version or {}).get('recovery') or {}
                completed = by_id.get(recovery.get('slicingItemId'))
                valid = (version and not (row or {}).get('deletedAt') and partial_complete(version)
                         and completed and completed.get('collectionRecovery')
                         and completed.get('mediaId') == item['mediaId']
                         and completed.get('collectionRevision') == revision
                         and completed.get('mediaGeneration', 0) == item.get('mediaGeneration', 0)
                         and photo_results_verified([completed], packages))
                if valid:
                    proof = verified_result(completed, lambda identity: self.slicing.packages.store.get('items', identity))
                    valid = bool(proof and recovery.get('results') == proof)
                checked[key] = completed if valid else None
            completed = checked[key]
            expected, actual = item.get('fileVersion') or {}, (completed or {}).get('fileVersion') or {}
            required = {'device', 'inode', 'size', 'modifiedNs'}
            if (completed and required.issubset(expected) and required.issubset(actual)
                    and self.same_version(expected, actual)):
                covered[item['id']] = completed['id']
        return covered

    def scoped_effective_items(self, items):
        """The same coverage proof, with indexed reads for status-only scopes."""
        context = {item['id']: item for item in items}
        candidates = [item for item in items if not item.get('superseded')
                      and not item.get('collectionRecovery') and item.get('state') in {'pending', 'attention'}
                      and item.get('collectionVideoId') and item.get('mediaId')]
        if candidates:
            for item in candidates:
                row = self.videos.store.get(item['collectionVideoId'])
                version = self.videos.version(row, item.get('collectionRevision')) if row else None
                identity = ((version or {}).get('recovery') or {}).get('slicingItemId')
                if identity and identity not in context:
                    recovered = self.slicing.store.get('items', identity)
                    if recovered:
                        context[identity] = recovered
            ids = tuple(context)
            placeholders = ','.join('?' for _ in ids)
            packages = [json.loads(raw[0]) for raw in self.db.execute(
                f"SELECT payload FROM simple_slice_package_items WHERE json_extract(payload, '$.itemId') IN ({placeholders})", ids)]
            submitted = {p.get('itemId') for p in packages if not p.get('superseded')}
            submitted.update(raw[0] for raw in self.db.execute(
                f"SELECT json_extract(payload, '$.itemId') FROM simple_slicing_operations WHERE json_extract(payload, '$.itemId') IN ({placeholders})", ids))
            covered = self.recovery_covered_items(list(context.values()), proof_inventory=(packages, submitted))
        else:
            covered = {}
        return [item for item in items if not item.get('superseded') and item['id'] not in covered
                and not item.get('coveredByRecovery')]

    def allowed(self, item):
        return not self.reason(item)

    def effective_items(self, items=None):
        """One execution view; historical attempts remain immutable receipts.

        Coverage is proved against the complete inventory even when a caller
        requests a single run whose old IDs omit the recovery's own row.
        """
        changes = (self.db.total_changes, self.db.execute('PRAGMA data_version').fetchone()[0])
        if getattr(self, '_effective_changes', None) != changes:
            universe = self.slicing.store.list('items')
            self._effective_covered = self.recovery_covered_items(universe)
            self._effective_changes = changes
        rows = self.slicing.store.list('items') if items is None else items
        return [item for item in rows if not item.get('superseded')
                and item.get('id') not in self._effective_covered
                and not item.get('coveredByRecovery')]

    def next_action(self, group):
        """Read-only recommendation shared by automatic and manual entrypoints."""
        rows = self.effective_items(group.get('items', []))
        packages = [p for p in group.get('packages', []) if not p.get('superseded')
                    and p.get('itemId') in {item['id'] for item in rows}]
        def result(kind, action, stage, reason, selections=None, pending=None, **extra):
            if kind == 'complete':
                extra.pop('progress', None)
            return dict(kind=kind, action=action, stage=stage, reason=reason, terminal=kind == 'complete',
                        canContinue=action in {'continue', 'archive'},
                        versionSelections=[{'id': row['id'], 'expectedRevision': row.get('revision', 0)} for row in rows],
                        selections=selections or [], packages=pending or [], **extra)
        if not rows:
            return result('complete', 'none', '已完成', '旧尝试已由验证成功的结果覆盖。')
        control = self.get(group.get('id') or group.get('mediaId') or rows[0].get('mediaId', '')) or {}
        paused = control.get('paused') or next((row for row in rows if row.get('collectionRecoveryPaused') or row.get('pipelinePaused')), None)
        if paused:
            return result('waiting', 'resume' if control.get('paused') else 'open', '已暂停',
                          '此视频已手动暂停，继续后才会处理。',
                          [{'id': row['id'], 'expectedRevision': row.get('revision', 0)} for row in rows])
        blocked = next(((row, reason) for row in rows if (reason := self.reason(row))), None)
        if blocked:
            row, reason = blocked
            control = self.get(row.get('mediaId', '')) or {}
            version = {}
            video = self.videos.store.get(row.get('collectionVideoId', ''))
            if video:
                version = self.videos.version(video, row.get('collectionRevision')) or {}
            waiting = bool(control.get('blocked') or version.get('downloadRequested') or
                           version.get('state') in {'downloading', 'publishing', 'waiting', 'submitted', 'submitting'})
            return result('waiting' if waiting else 'manual', 'none' if waiting else 'open',
                          '等待视频处理' if waiting else '需要你处理', reason)
        active = {'pending', 'analyzing', 'platform_processing', 'ready', 'exporting', 'waiting_slot',
                  'queued', 'uploading', 'uploaded', 'submitting', 'importing', 'waiting', 'retry_wait'}
        waiting = next((item for item in rows if item.get('state') == 'waiting_connection'), None)
        if waiting:
            return result('waiting', 'none', '等待恢复', waiting.get('message') or '等待所需网络、磁盘或文件恢复。')
        selections = []
        for item in rows:
            # Legacy export callbacks could hide unsent pending analysis under
            # platform_processing. The action resumes those slots only; it
            # never reopens an uploaded or verified category.
            if (item.get('state') == 'platform_processing' and self.slicing.pending_analysis(item, due=False)
                    and not any(task and not task.done() for task in
                                (self.slicing.analysis_tasks.get(item['id']), self.slicing.export_tasks.get(item['id'])))):
                selections.append({'id': item['id'], 'expectedRevision': item.get('revision', 0)})
                continue
            failed = [slot for slot in item.get('categories', {}).values()
                      if slot.get('state') == 'attention' and not slot.get('packageId')]
            if (item.get('state') == 'attention' and item.get('errorCode') in SAFE_RETRY_ERRORS and failed
                    and all(not slot.get('taskId') and not slot.get('operationId')
                            and (slot.get('errorCode') or item.get('errorCode')) in SAFE_RETRY_ERRORS for slot in failed)):
                selections.append({'id': item['id'], 'expectedRevision': item.get('revision', 0)})
        pending = [{'id': pack['id'], 'expectedRevision': pack.get('revision', 0)} for pack in packages
                   if pack.get('state') == 'attention' and pack.get('errorCode') == 'verification' and pack.get('jobId')]
        if selections or pending:
            automatic = all((row.get('errorCode') != 'worker_failed' or row.get('recoveryDisposition') == 'retry')
                            and all(int(slot.get('automaticRetries', row.get('automaticRetries', 0)) or 0) < 2
                                    for slot in row.get('categories', {}).values()
                                    if slot.get('state') == 'attention' and not slot.get('packageId'))
                            for row in rows if row['id'] in {choice['id'] for choice in selections})
            for choice in pending:
                stored = self.slicing.packages.store.get('items', choice['id']) or {}
                automatic = automatic and int(stored.get('verificationFailures') or 0) < 2 and (
                    stored.get('verificationAutoRetryForJob') != str(stored.get('jobId') or ''))
            reason = '可继续已有断点或核对已提交的照片结果。'
            if not automatic:
                problem = next((row for row in rows if row.get('state') == 'attention' and row.get('message')), None)
                if not problem:
                    problem = next((pack for pack in packages if pack.get('state') == 'attention' and pack.get('message')), None)
                reason = (problem or {}).get('message') or '自动重试已停止，请核对本条后继续。'
            return result('automatic' if automatic else 'manual', 'continue', '可以继续' if automatic else '需要核对后继续',
                          reason, selections, pending, autoEligible=automatic)
        problem = next((pack for pack in packages if pack.get('state') in {'attention', 'review'}), None)
        if not problem:
            problem = next((row for row in rows if row.get('state') in {'attention', 'review'}), None)
        if problem:
            return result('manual', 'open', '需要你处理', problem.get('message') or '请核对本视频的身份、画面或处理结果。')
        if any(row.get('state') in active for row in rows) or any(pack.get('state') in active for pack in packages):
            analysis = next((row for row in rows if row.get('state') == 'analyzing'), None)
            if analysis:
                labels = {'checking_media': '检查视频', 'loading_model': '加载模型', 'sampling': '抽帧识别'}
                done, total = int(analysis.get('analysisDone') or 0), int(analysis.get('analysisTotal') or 0)
                return result('automatic', 'none', labels.get(analysis.get('analysisStage'), '识别中'),
                              f'已识别 {done}/{total} 个位置。' if total else analysis.get('message') or '准备识别。',
                              progress={'done': done, 'total': total, 'unit': '个位置'})
            exporting = next((row for row in rows if row.get('state') == 'exporting' or
                              ((task := self.slicing.export_tasks.get(row['id'])) and not task.done())), None)
            if exporting:
                return result('automatic', 'none', '导出照片', exporting.get('message') or '正在生成本地照片包。')
            # Already submitted imports continue reconciling during an upload
            # pause. Unsent packages must not masquerade as running work.
            package = next((pack for pack in packages if pack.get('state') in {'submitting', 'importing'}), None)
            package = package or next((pack for pack in packages if pack.get('state') in active), None)
            if package:
                labels = {'queued': '排队上传', 'uploading': '上传照片', 'uploaded': '等待导入',
                          'submitting': '提交导入', 'importing': '导入照片'}
                runtime = self.slicing.packages.runtime.get(package['id']) or {}
                done = max(int(package.get('bytes') or 0), int(runtime.get('bytes') or 0))
                if self.slicing.packages.paused and package.get('state') in {'queued', 'uploading', 'uploaded'}:
                    return result('waiting', 'none', '照片上传已暂停',
                                  '照片包与断点保留。继续照片上传后，将自动导入并整理。',
                                  progress={'done': done, 'total': int(package.get('size') or 0), 'unit': 'bytes'})
                return result('automatic', 'none', labels.get(package.get('state'), '照片处理中'),
                              package.get('message') or '照片包按队列继续处理。',
                              progress={'done': done, 'total': int(package.get('size') or 0), 'unit': 'bytes'})
            reason = next((row.get('message') for row in rows if row.get('state') in active and row.get('message')), '')
            return result('automatic', 'none', '自动处理中', reason or '正在按已登记的步骤处理。')
        archive = next((file for file in group.get('files', []) if file.get('archiveState') in {'attention', 'waiting'}), None)
        if archive:
            if archive.get('archiveState') == 'attention':
                if not archive.get('actionItemId'):
                    return result('manual', 'open', '整理待处理', '文件与当前视频记录的关联需核对，原文件保留。')
                return result('manual', 'archive', '整理待处理', archive.get('archiveMessage') or '整理需要再次核对。',
                              fileId=archive['id'], actionItemId=archive['actionItemId'])
            return result('waiting', 'none', '等待整理', archive.get('archiveMessage') or '等待文件释放或磁盘恢复。')
        return result('complete', 'none', '已完成', '本视频的适用处理已结束。')

    async def pause_media(self, identity, body, paused=True):
        """Pause one physical source without revoking its submitted evidence."""
        from uuid import uuid4
        async with self.locks.setdefault(identity, asyncio.Lock()):
            self.refresh()
            control = self.get(identity)
            signature = json.dumps({**body, 'paused': paused}, sort_keys=True)
            key = 'pause:' + hashlib.sha256((identity + ':' + body['requestId']).encode()).hexdigest()
            old = self.db.execute('SELECT payload FROM simple_media_actions WHERE id=?', (key,)).fetchone()
            if old:
                receipt = json.loads(old[0])
                if receipt.get('signature') != signature:
                    raise Conflict('请求编号已用于其他操作。')
                if receipt.get('state') == 'complete':
                    return {'ok': True, 'message': '此视频已暂停。' if paused else '此视频已继续。'}
            if not control or control.get('generation') != body.get('generation'):
                raise Conflict('视频版本已变化，请刷新后重试。')
            pipeline = getattr(getattr(self.slicing, 'workspace', None), 'pipeline', None)
            if not paused and pipeline and pipeline.paused:
                raise Conflict('自动处理已全局暂停，请先继续自动处理；本视频的进度保留。')
            rows = [row for row in self.effective_items() if row.get('mediaId') == identity]
            choices = {choice['id']: choice['expectedRevision'] for choice in body.get('selections', [])}
            if not rows or (not old and any(row['id'] in choices and row.get('revision', 0) != choices[row['id']] for row in rows)):
                raise Conflict('视频记录已变化，请刷新后重试。')
            if set(choices) - {row['id'] for row in rows}:
                raise Conflict('所选记录不属于当前视频。')
            self.db.execute('INSERT INTO simple_media_actions VALUES(?,?) ON CONFLICT(id) DO UPDATE SET payload=excluded.payload',
                            (key, json.dumps({'signature': signature, 'state': 'running'})))
            self.db.commit()
            control.update(paused=paused)
            self.save(control)
            ids = {row['id'] for row in rows}
            if paused:
                tasks = [task for identity in ids for task in
                         (self.slicing.analysis_tasks.get(identity), self.slicing.export_tasks.get(identity))
                         if task and not task.done()]
                tasks.extend(task for pack in self.slicing.packages.store.list('items')
                             if pack.get('itemId') in ids and not pack.get('jobId')
                             and (task := self.slicing.packages.running.get(pack['id'])) and not task.done())
                for task in tasks:
                    task.cancel()
                if tasks:
                    await asyncio.gather(*tasks, return_exceptions=True)
            else:
                if not self.slicing.store.settings.get('enabled'):
                    run = {'id': 'resume-media-' + uuid4().hex, 'items': sorted(ids), 'state': 'running',
                           'settingsRevision': self.slicing.store.settings['revision'], 'localRecoveryOnly': True,
                           'singleMode': False, 'createdAt': now_iso(), 'message': '继续此视频；其他暂停项目保持原状。'}
                    self.slicing.run = run
                    self.slicing.store.save('runs', run)
                    self.slicing.store.settings.update(enabled=True, latestRunId=run['id'])
                    self.slicing.store.save_settings()
                else:
                    run = self.slicing.store.get('runs', self.slicing.store.settings.get('latestRunId')) or self.slicing.run
                    if run and (run.get('localRecoveryOnly') or run.get('singleMode')):
                        previous = ([run['singleItemId']] if run.get('singleMode') and run.get('singleItemId')
                                    else run.get('items', []))
                        run.update(items=list(dict.fromkeys([*previous, *sorted(ids)])),
                                   singleMode=False, localRecoveryOnly=True, state='running')
                        run.pop('singleItemId', None)
                        self.slicing.store.save('runs', run)
                        self.slicing.run = run
                self.slicing.packages.wake.set()
            if paused:
                self.slicing.wake.set()
            else:
                # The last paused video can make the scheduler exit. Setting
                # its event alone cannot restart that finished coroutine.
                self.slicing._wake_scheduler()
            if (self.get(identity) or {}).get('generation') != body['generation']:
                raise Conflict('视频已被更正，旧版暂停操作已停止。')
            self.db.execute('INSERT INTO simple_media_actions VALUES(?,?) ON CONFLICT(id) DO UPDATE SET payload=excluded.payload',
                            (key, json.dumps({'signature': signature, 'state': 'complete'})))
            self.db.commit()
            return {'ok': True, 'message': '此视频已暂停，断点保留。' if paused else '此视频已继续。'}

    def package_allowed(self, package, allow_paused=False):
        item = self.slicing.store.get("items", package.get("itemId"))
        return not package.get("superseded") and (not item or not self.reason(item, allow_paused=allow_paused))

    def claim(self, item):
        if not self.allowed(item):
            raise Conflict(self.reason(item))
        row = self.get(item.get("mediaId", ""))
        if row and row.get("owner") != "slicing":
            row["owner"] = "slicing"
            self.save(row)

    def reserve(self, video_id, request_id, *, commit=True, refresh=True):
        if refresh:
            self.refresh()
        identity = "collection:" + video_id
        row = self.get(identity) or {"id": identity, "videoId": video_id, "generation": 0, "paths": []}
        if row.get("requestId") != request_id:
            row.update(generation=row["generation"] + 1, requestId=request_id, owner="review", blocked=True)
            self.save(row, commit=commit)
        return row["generation"]

    async def take_over(self, video_id, request_id, expected_generation=None):
        if expected_generation is None:
            self.reserve(video_id, request_id)
        self.refresh()
        identity = "collection:" + video_id
        row = self.get(identity)
        if (not row or (expected_generation is not None and
                (row["generation"] != expected_generation or row.get("requestId") != request_id))):
            raise Conflict("已有更新的视频指令，本次旧操作已停止。")
        tasks = []
        affected = []
        for item in self.slicing.store.list("items"):
            if item.get("mediaId") != identity or item.get("mediaGeneration", 0) >= row["generation"]:
                continue
            affected.append(item["id"])
            item.update(superseded=True, state=item["state"] if item.get("state") in TERMINAL else "superseded",
                        message="旧版处理已停止，等待更正版确认。")
            self.slicing.store.save("items", item)
            for task in (self.slicing.analysis_tasks.get(item["id"]), self.slicing.export_tasks.get(item["id"])):
                if task and not task.done():
                    task.cancel(); tasks.append(task)
        for package in self.slicing.packages.store.list("items"):
            if package.get("itemId") not in affected:
                continue
            package["superseded"] = True
            submitted = package.get("jobId") or package.get("state") in {"submitting", "importing", "complete"}
            if not submitted:
                package.update(state="superseded", message="旧版照片包保留，不再上传或导入。")
                task = self.slicing.packages.running.get(package["id"])
                if task and not task.done():
                    task.cancel(); tasks.append(task)
            self.slicing.packages._save(package)
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self.require_command(identity, request_id, row["generation"])
        download = self.videos.running.get(video_id)
        if download and download is not asyncio.current_task() and not download.done() and download.get_name() != "clear-record":
            download.cancel()
            await asyncio.gather(download, return_exceptions=True)
        self.require_command(identity, request_id, row["generation"])
        recovery_tasks = []
        for operation in self.videos.store.list_operations(video_id):
            if operation.get("kind") == "recover-decodable" and operation.get("state") in {"queued", "running", "waiting_connection", "paused", "reconciling"}:
                operation.update(state="conflict", stage="superseded", message="恢复已停止，按最新视频指令执行。")
                self.videos.store.save_operation(operation)
                task = self.videos.operation_tasks.get(operation["id"])
                if task and task is not asyncio.current_task() and not task.done():
                    task.cancel(); recovery_tasks.append(task)
        if recovery_tasks:
            await asyncio.gather(*recovery_tasks, return_exceptions=True)
        self.require_command(identity, request_id, row["generation"])
        video = self.videos.store.get(video_id)
        if video:
            for version in video.get("versions", []):
                if version.get("downloadRequested"):
                    version["downloadRequested"] = False
                if version.get("state") in {"downloading", "publishing"}:
                    version.update(state="attention", message="旧版保存已停止，文件和断点保留。")
                recovery = version.get("recovery") or {}
                if recovery.get("state") in {"queued", "moving", "slicing", "paused"}:
                    recovery.update(state="superseded", message="旧版恢复已停止。")
            self.videos.store.save(video)
        self.slicing.wake.set()

    def require_command(self, identity, request_id, generation):
        current = self.get(identity)
        if not current or current.get("requestId") != request_id or current["generation"] != generation:
            raise Conflict("已有更新的视频指令，本次旧操作已停止。")

    def release_review(self, video_id, request_id=None):
        row = self.get("collection:" + video_id)
        if row and (request_id is None or row.get("requestId") == request_id):
            row.update(blocked=False, owner="review")
            self.save(row)

    async def link(self, item, data):
        from .media import file_digest
        from .slicing import source_version
        from .file_leases import finish_io
        if item.get("superseded"):
            raise Conflict("旧版任务不能重新关联为当前版本。")
        row = self.videos.store.get(data.get("videoId"))
        revision = data.get("collectionRevision")
        if (not row or row.get("deletedAt") or row.get("revision") != data.get("collectionExpectedRevision")
                or row.get("activeRevision") != revision):
            raise Conflict("所选采集版本已变化，请重新选择。")
        version = self.videos.version(row, revision) or {}
        expected = version.get("fileVersion") or version.get("recovery", {}).get("fileVersion") or {}
        if not self.same_version(expected, item.get("fileVersion") or {}):
            digest = version.get("sha256") or version.get("recovery", {}).get("sha256")
            if not digest or not item.get("path"):
                raise Conflict("原片与所选采集版本不一致，未建立关联。")
            with self.leases.read(item["path"]):
                before = await finish_io(source_version, item["path"])
                if (before != item.get("fileVersion") or await finish_io(file_digest, Path(item["path"])) != digest
                        or await finish_io(source_version, item["path"]) != before):
                    raise Conflict("原片与所选采集版本不一致，未建立关联。")
        latest = self.videos.store.get(row["id"])
        if not latest or latest.get("revision") != row["revision"]:
            raise Conflict("核对期间采集记录已变化。")
        target_id = "collection:" + row["id"]
        target = self.get(target_id) or {"id": target_id, "videoId": row["id"], "generation": 0,
                                        "owner": "review", "blocked": False, "paths": []}
        if target.get("blocked"):
            raise Conflict("所选视频正在执行更正，请稍后关联。")
        for peer in self.slicing.store.list("items"):
            if peer.get("mediaId") != item["mediaId"]:
                continue
            if peer["id"] in self.slicing.analysis_tasks or peer["id"] in self.slicing.export_tasks:
                raise Conflict("请先暂停本地处理，再关联采集版本。")
        for peer in self.slicing.store.list("items"):
            if peer.get("mediaId") == item["mediaId"]:
                peer.update(mediaId=target_id, mediaGeneration=target["generation"], collectionVideoId=row["id"],
                            collectionRevision=revision, identityConflict=False, processingReason="")
                peer["revision"] = int(peer.get("revision", 0)) + 1
                self.slicing.store.save("items", peer)
                if peer.get("path") not in target["paths"]:
                    target["paths"].append(peer["path"])
        self.save(target)

    @staticmethod
    def processing_entry(item=None):
        item = item or {}
        query = {"tab": "videos"}
        if item.get("collectionVideoId"):
            query["videoId"] = item["collectionVideoId"]
        if item.get("mediaId"):
            query["mediaId"] = item["mediaId"]
        return "/?" + urlencode(query)

    def execution_groups(self, paths):
        """Schedule current work without rendering every historical file card."""
        if not paths:
            return []
        self.refresh()
        items = [item for item in self.effective_items() if item.get('path') in paths]
        ids = tuple(item['id'] for item in items)
        if not ids:
            return []
        placeholders = ','.join('?' for _ in ids)
        packages = [json.loads(row[0]) for row in self.db.execute(
            f"SELECT payload FROM simple_slice_package_items WHERE json_extract(payload, '$.itemId') IN ({placeholders})", ids)]
        by_item = {}
        for package in packages:
            if not package.get('superseded'):
                by_item.setdefault(package.get('itemId'), []).append(package)
        groups = {}
        for item in items:
            identity = item.get('mediaId') or 'unbound:' + item['id']
            if identity not in groups:
                groups[identity] = {'id': identity, 'items': [], 'packages': [],
                    'generation': (self.get(identity) or {}).get('generation', 0)}
            group = groups[identity]
            group['items'].append(item)
            group['packages'].extend(by_item.get(item['id'], []))
        pending = []
        for group in groups.values():
            if (all(item.get('state') in TERMINAL | {'uncuttable'}
                    and all(slot.get('state') in TERMINAL | {'uncuttable', 'not_applicable'}
                            for slot in item.get('categories', {}).values()) for item in group['items'])
                    and all(package.get('state') in {'complete', 'dismissed', 'superseded'}
                            for package in group['packages'])):
                continue
            group['nextAction'] = self.next_action(group)
            pending.append(group)
        return pending

    def state(self, include_completed=False, *, include_cleanup_actions=True):
        files = getattr(self.slicing, 'files', None)
        if files:
            files.refresh()
        self.refresh()
        all_items = self.slicing.store.list('items')
        hidden = self.slicing.visibility.hidden()
        visible = [item for item in all_items if item['id'] not in hidden]
        public_items = {item['id']: item for item in self.slicing.public_items(visible,
            covered=self.recovery_covered_items(all_items))}
        groups = {}
        for item in self.effective_items(all_items):
            if item["id"] not in public_items:
                continue
            identity = item.get("mediaId") or "unbound:" + item["id"]
            group = groups.setdefault(identity, {"id": identity, "videoId": item.get("collectionVideoId"),
                "name": item.get("name"), "createdAt": item.get("createdAt") or item.get("updatedAt"), "items": []})
            reason = self.reason(item)
            group["items"].append({**public_items[item["id"]], "blockedReason": reason})
            control = self.get(identity) or {}
            group.update(generation=control.get("generation", 0),
                         owner=control.get("owner", "slicing"), blockedReason=reason,
                         processingEntry=self.processing_entry(item))
        packages = self.slicing.packages.state(item_ids=public_items)['items']
        public_files = {row['id']: row for row in files.state()['items']} if files else {}
        indexed_files = files.rows('files') if files else []
        workspace = getattr(files, 'workspace', None) if files else None
        assets = {}
        if workspace and workspace.enabled:
            for asset in workspace.rows('assets'):
                assets.setdefault(asset['path'], asset)
        cleanup_files = []
        for group in groups.values():
            ids = {item["id"] for item in group["items"]}
            group["packages"] = [p for p in packages if p.get("itemId") in ids]
            group["files"] = []
            for file in indexed_files:
                linked_ids = ids.intersection(file.get("links", {}).get("slicing", []))
                if not linked_ids:
                    continue
                # A physical video can have several processing records and
                # several original/recovery files. A file action must use its
                # own current binding, never the group's first record.
                binding = next((item for item in group['items'] if item['id'] in linked_ids
                                and not item.get('coveredByRecovery') and not self.reason(item)), None)
                value = {k: public_files.get(file['id'], file).get(k)
                         for k in ("id", "archiveState", "archiveMessage", "cleanup")}
                value['actionItemId'] = binding['id'] if binding else None
                group['files'].append(value)
            # Cleanup-button eligibility is expensive and is not part of the
            # pipeline's recommendation. The detailed control view keeps it;
            # actual cleanup always checks the current receipts independently.
            if include_cleanup_actions and workspace and workspace.enabled:
                for file in group['files']:
                    asset = assets.get(public_files.get(file['id'], {}).get('path', ''))
                    if asset and file['archiveState'] == 'complete' and file.get('cleanup', {}).get('state') != 'complete':
                        cleanup_files.append((file, asset))
            group['nextAction'] = self.next_action(group)
        if cleanup_files:
            statuses = workspace.cleanup_statuses(list({asset['id']: asset for _, asset in cleanup_files}.values()))
            for file, asset in cleanup_files:
                file['cleanupEligible'], file['cleanupReason'] = statuses[asset['id']]
        return {"groups": [g for g in groups.values() if include_completed or g.get("videoId") or
                           any(i.get("state") not in TERMINAL for i in g["items"])]}

    async def continue_safe(self, identity, body):
        """Continue a physical video without guessing its identity or approving results."""
        from uuid import uuid4
        safe_errors = SAFE_RETRY_ERRORS
        lock = self.locks.setdefault(identity, asyncio.Lock())
        async with lock:
            signature = json.dumps(body, sort_keys=True, separators=(',', ':'))
            key = hashlib.sha256(('continue:' + identity + ':' + body['requestId']).encode()).hexdigest()
            old = self.db.execute('SELECT payload FROM simple_media_actions WHERE id=?', (key,)).fetchone()
            receipt = json.loads(old[0]) if old else None
            if receipt and receipt['signature'] != signature:
                raise Conflict('请求编号已用于其他操作。')
            if receipt and receipt.get('state') == 'complete':
                return {'outcomes': receipt['outcomes'], 'state': self.slicing.state()}
            self.refresh()
            control = self.get(identity)
            if not control or control.get('generation') != body['generation']:
                raise Conflict('视频版本已变化，请刷新后重试。')
            pipeline = getattr(getattr(self.slicing, 'workspace', None), 'pipeline', None)
            if pipeline and pipeline.paused:
                raise Conflict('自动处理已全局暂停，请先继续自动处理；本视频的进度保留。')
            choices, package_choices = body['selections'], body['packages']
            if not choices and not package_choices:
                raise ValueError('没有可自动继续的项目，请查看具体原因。')
            if len({c['id'] for c in choices}) != len(choices) or len({c['id'] for c in package_choices}) != len(package_choices):
                raise ValueError('不能重复选择同一条记录。')
            if not self.slicing.auth.connected and not self.slicing.store.settings.get('localExport', True):
                raise ValueError('视觉AI平台未连接，请先到设置中连接。')
            done = {entry['key'] for entry in (receipt or {}).get('outcomes', []) if entry.get('ok')}
            def sub_id(kind, item_id):
                return 'continue-' + hashlib.sha256((key + kind + item_id).encode()).hexdigest()[:48]
            selected = []
            pending_ids = set()
            for choice in choices:
                item = self.slicing.store.get('items', choice['id'])
                if not item or item.get('mediaId') != identity:
                    raise Conflict('切片记录不属于此视频，请刷新后重试。')
                if not self.allowed(item):
                    raise Conflict(self.reason(item))
                marker = self.slicing.store.get('requests', sub_id('item', item['id']))
                pending = item.get('state') != 'attention' and bool(self.slicing.pending_analysis(item, due=False))
                if 'item:' + item['id'] not in done and not marker:
                    if item.get('revision') != choice['expectedRevision'] or (item.get('state') != 'attention' and not pending):
                        raise Conflict('切片记录已变化，请刷新后重试。')
                    if not pending and item.get('errorCode') not in safe_errors:
                        raise Conflict('此异常需要人工核对，未自动修改。')
                    for category in item.get('categories', {}).values():
                        if pending or category.get('state') != 'attention' or category.get('packageId'):
                            continue
                        if category.get('taskId') or category.get('operationId') or (category.get('errorCode') or item.get('errorCode')) not in safe_errors:
                            raise Conflict('已有平台提交或需要人工核对的异常，未自动修改。')
                if (pending and not marker) or (marker and marker.get('kind') == 'continue-pending'):
                    pending_ids.add(item['id'])
                selected.append(item)
            packages = []
            for choice in package_choices:
                package = self.slicing.packages.store.get('items', choice['id'])
                item = self.slicing.store.get('items', (package or {}).get('itemId'))
                if not package or not item or item.get('mediaId') != identity or not self.allowed(item):
                    raise Conflict('照片包不属于当前视频版本，请刷新后重试。')
                marker = self.slicing.packages.store.get('batches', 'retry:' + sub_id('package', package['id']))
                if 'package:' + package['id'] not in done and not marker:
                    if package.get('revision', 0) != choice['expectedRevision'] or package.get('state') != 'attention':
                        raise Conflict('照片包状态已变化，请刷新后重试。')
                    if package.get('errorCode') != 'verification' or not package.get('jobId'):
                        raise Conflict('此照片包需要人工核对，不会自动重传。')
                packages.append(package)
            active_ids = {item['id'] for item in self.slicing.store.list('items') if item.get('mediaId') == identity}
            if any(task and not task.done() for key in active_ids for task in
                   (self.slicing.analysis_tasks.get(key), self.slicing.export_tasks.get(key))):
                raise Conflict('该视频仍在处理，完成后再继续。')
            run = self.slicing.store.get('runs', self.slicing.store.settings.get('latestRunId')) or self.slicing.run
            if selected and self.slicing.store.settings.get('enabled') and run and run.get('singleMode'):
                if any(item['id'] != run.get('singleItemId') for item in selected):
                    raise Conflict('其他单条任务正在运行，请稍后重试。')
            if selected and not self.slicing.store.settings.get('enabled'):
                if self.slicing.busy:
                    raise Conflict('切片进程正在停止，请稍后重试。')
                # A targeted request resumes only these items, leaving other paused work untouched.
                run = {'id': 'continue-' + uuid4().hex, 'requestId': body['requestId'], 'state': 'running',
                       'items': [item['id'] for item in selected], 'settingsRevision': self.slicing.store.settings['revision'],
                       'localRecoveryOnly': True, 'singleMode': False, 'createdAt': now_iso(), 'message': '正在继续选中视频。'}
                self.slicing.store.save('runs', run)
                self.slicing.run = run
                self.slicing.store.settings.update(enabled=True, latestRunId=run['id'])
                self.slicing.store.save_settings()
            if not receipt:
                receipt = {'signature': signature, 'state': 'running', 'outcomes': []}
                self.db.execute('INSERT INTO simple_media_actions VALUES(?,?)', (key, json.dumps(receipt)))
                self.db.commit()
            for item in selected:
                result_key = 'item:' + item['id']
                if result_key in done:
                    continue
                try:
                    previous = self.slicing.store.get('requests', sub_id('item', item['id'])) or {}
                    if item['id'] in pending_ids:
                        self.slicing.continue_pending(item['id'], item['revision'], sub_id('item', item['id']))
                    else:
                        recovery = previous.get('kind') == 'recover-decodable' if previous else item.get('errorCode') == 'media_incomplete'
                        self.slicing.retry(item['id'], item['revision'], sub_id('item', item['id']), recover_decodable=recovery)
                    outcome = {'key': result_key, 'ok': True, 'message': '已加入识别队列。'}
                except (Conflict, ValueError) as error:
                    outcome = {'key': result_key, 'ok': False, 'message': str(error)}
                receipt['outcomes'] = [x for x in receipt['outcomes'] if x['key'] != result_key] + [outcome]
                self.db.execute('UPDATE simple_media_actions SET payload=? WHERE id=?', (json.dumps(receipt), key)); self.db.commit()
            for package in packages:
                result_key = 'package:' + package['id']
                if result_key in done:
                    continue
                try:
                    await self.slicing.packages.retry(package['id'], sub_id('package', package['id']), package.get('revision', 0))
                    outcome = {'key': result_key, 'ok': True, 'message': '正在重新核对已有照片导入。'}
                except (Conflict, ValueError) as error:
                    outcome = {'key': result_key, 'ok': False, 'message': str(error)}
                receipt['outcomes'] = [x for x in receipt['outcomes'] if x['key'] != result_key] + [outcome]
                self.db.execute('UPDATE simple_media_actions SET payload=? WHERE id=?', (json.dumps(receipt), key)); self.db.commit()
            receipt['state'] = 'complete'
            self.db.execute('UPDATE simple_media_actions SET payload=? WHERE id=?', (json.dumps(receipt), key)); self.db.commit()
            if selected:
                # Persisted child requests may return idempotently after a
                # restart without waking the dispatcher themselves.
                self.slicing._wake_scheduler()
            else:
                self.slicing.wake.set()
            return {'outcomes': receipt['outcomes'], 'state': self.slicing.state()}

    async def action(self, identity, body):
        lock = self.locks.setdefault(identity, asyncio.Lock())
        async with lock:
            self.refresh()
            control = self.get(identity)
            if not control:
                raise ValueError("视频控制记录不存在。")
            signature = json.dumps(body, sort_keys=True)
            key = hashlib.sha256((identity + ":" + str(body.get("requestId"))).encode()).hexdigest()
            old = self.db.execute("SELECT payload FROM simple_media_actions WHERE id=?", (key,)).fetchone()
            if old:
                receipt = json.loads(old[0])
                if receipt["signature"] != signature:
                    raise Conflict("请求编号已用于其他操作。")
                if receipt["state"] == "complete":
                    return self.state()
            if body.get("generation") != control["generation"]:
                raise Conflict("视频指令已更新，请刷新后重试。")
            item = self.slicing.store.get("items", body.get("itemId"))
            if not item or item.get("mediaId") != identity or (body.get("action") != "link" and not self.allowed(item)):
                raise Conflict(self.reason(item or {}) or "原片身份不匹配。")
            data = {**body, "expectedRevision": body.get("expectedRevision")}
            action = body.get("action")
            if action in {"bind", "review", "link", "start", "retry", "recover"}:
                ids = {row["id"] for row in self.slicing.store.list("items") if row.get("mediaId") == identity}
                if any(key in self.slicing.analysis_tasks or key in self.slicing.export_tasks for key in ids):
                    raise Conflict("这个视频仍在本地处理，完成或暂停后再调整；其他视频继续。")
                if action == "bind" and any(p.get("itemId") in ids and p["id"] in self.slicing.packages.running
                                            for p in self.slicing.packages.store.list("items")):
                    raise Conflict("此视频的照片包正在上传，请完成或暂停后重新绑定。")
            if action in {"retry", "recover", "bind", "review", "start", "link"} and item.get("revision") != data["expectedRevision"]:
                raise Conflict("切片记录已变化，请刷新后重试。")
            receipt = {"id": key, "signature": signature, "state": "running"}
            self.db.execute("INSERT INTO simple_media_actions VALUES(?,?) ON CONFLICT(id) DO UPDATE SET payload=excluded.payload", (key,json.dumps(receipt)))
            self.db.commit()
            if action == "link":
                await self.link(item, data)
            elif action in {"retry", "recover"}:
                self.slicing.retry(item["id"], data["expectedRevision"], data["requestId"], recover_decodable=action == "recover")
            elif action == "bind":
                from .models import SlicingBindInput
                validated = SlicingBindInput.model_validate(data).model_dump()
                await self.verify_collection_file(item, validated["path"])
                await self.slicing.bind(item["id"], validated)
                bound = self.slicing.store.get("items", item["id"])
                if bound.get("fileVersion") != item.get("fileVersion"):
                    control["generation"] += 1
                    self.save(control)
                    bound["mediaGeneration"] = control["generation"]
                    self.slicing.store.save("items", bound)
                    old_ids = set()
                    for peer in self.slicing.store.list("items"):
                        if peer.get("mediaId") == identity and peer["id"] != item["id"]:
                            old_ids.add(peer["id"])
                            peer.update(superseded=True, state=peer["state"] if peer.get("state") in TERMINAL else "superseded")
                            self.slicing.store.save("items", peer)
                    for package in self.slicing.packages.store.list("items"):
                        if package.get("itemId") in old_ids:
                            package["superseded"] = True
                            if not package.get("jobId") and package.get("state") not in {"submitting", "importing"}:
                                package["state"] = "superseded"
                            self.slicing.packages._save(package)
            elif action == "review":
                from .models import SlicingReviewInput
                await self.slicing.review(item["id"], SlicingReviewInput.model_validate(data).model_dump())
            elif action == "start":
                result = await self.slicing.start(data["requestId"], self.slicing.store.settings["revision"], "single", item["id"])
                if getattr(self, "submissions", None):
                    self.submissions.add("slicing", "单条切片", {"requestId": data["requestId"],
                        "runId": (result.get("run") or {}).get("id"), "itemIds": [item["id"]],
                        "mode": "single", "message": "单条切片已启动。"}, request_id=data["requestId"])
            elif action in {"confirm-package", "retry-package"}:
                package = self.slicing.packages.store.get("items", data.get("packageId"))
                if not package or package.get("itemId") != item["id"]:
                    raise Conflict("照片包不属于此视频。")
                if action == "confirm-package":
                    self.slicing.confirm_package(package["id"])
                else:
                    await self.slicing.retry_package(package["id"], data)
            elif action in {"cleanup", "retry-archive"}:
                file = next((f for f in self.slicing.files.rows("files") if f["id"] == data.get("fileId")
                             and item["id"] in f.get("links", {}).get("slicing", [])), None)
                if not file:
                    raise Conflict("文件清理记录不属于此视频。")
                if action == 'retry-archive':
                    await self.slicing.files.retry(file['id'], data['requestId'])
                else:
                    await self.slicing.files.cleanup_confirmed(file["id"], data["requestId"])
            else:
                raise ValueError("操作无效。")
            receipt["state"] = "complete"
            self.db.execute("UPDATE simple_media_actions SET payload=? WHERE id=?", (json.dumps(receipt),key)); self.db.commit()
            return self.state()
