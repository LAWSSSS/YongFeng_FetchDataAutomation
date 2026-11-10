"""Deletion gates. Completion labels alone are never deletion authority."""
import copy
import hashlib
import json
import time
from contextlib import ExitStack

from .audit_moves import checked_path
from .domain import now_iso
from .workspace_files import compatible, file_version, unlink_workspace_file
from .file_leases import FileEnvironmentError


def collection_bindings(manager, asset):
    """Load only records bound to this original or its managed recovery copy."""
    if hasattr(manager, '_cleanup_binding_cache'):
        return [(row, version) for row, version, expected in manager._cleanup_binding_cache.get(asset['path'], [])
                if {'device', 'inode', 'size', 'modifiedNs'}.issubset(expected or {})
                and compatible(asset['fileVersion'], expected)]
    rows = manager.db.execute("""SELECT payload FROM simple_videos WHERE EXISTS (
        SELECT 1 FROM json_each(simple_videos.payload, '$.versions') version
        WHERE json_extract(version.value, '$.filePath')=?
           OR json_extract(version.value, '$.recovery.filePath')=?
           OR json_extract(version.value, '$.recovery.destination')=?)""",
        (asset['path'], asset['path'], asset['path']))
    result = []
    for (raw,) in rows:
        row = json.loads(raw)
        for version in row.get('versions', []):
            recovery = version.get('recovery') or {}
            choices = [(version.get('filePath'), version.get('fileVersion')),
                       (recovery.get('filePath') or recovery.get('destination'), recovery.get('fileVersion'))]
            if any(path == asset['path'] and {'device', 'inode', 'size', 'modifiedNs'}.issubset(expected or {})
                   and compatible(asset['fileVersion'], expected)
                   for path, expected in choices):
                result.append((row, version))
    return result


def eligibility(manager, asset):
    reason = manager._asset_wait_reason(asset)
    if reason:
        return False, reason
    if any(item.get('superseded') for item in manager.effective_links(asset)):
        return False, '旧版结果保留，等待视频更正流程处理。'
    if manager.is_preserved(asset):
        return False, '已上传历史视频，按保留规则跳过本地清理。'
    outcomes = manager.outcomes(asset)
    if not all(v in {'complete', 'no_target', 'uncuttable'} for v in outcomes.values()):
        return False, '两类处理尚未结束。'
    if not manager.photo_results_verified(asset):
        return False, '照片尚未全部导入并核验，原片和照片保留。'
    links = manager.effective_links(asset)
    from .photo_completion import verified_empty_conclusion
    for item in links:
        for name, category in item.get('categories', {}).items():
            if (category.get('state') != 'not_applicable' and outcomes.get(name) in {'no_target', 'uncuttable'}
                    and not verified_empty_conclusion(item, name)):
                return False, '没有照片的处理结论尚未核实，文件保留。'
    if not all(i.get('fileVersion') and compatible(asset['fileVersion'], i['fileVersion']) for i in links):
        return False, '缺少稳定的原片版本。'
    bindings = collection_bindings(manager, asset)
    for row, version in bindings:
        confirmed = (version.get('revision') == row.get('activeRevision') and
                     (version.get('finalConfirmation') or {}).get('version') == version.get('revision')
                     and (version.get('finalConfirmation') or {}).get('operationId'))
        if (row.get('cleanupPending') or (row.get('activeRevision') != row.get('acceptedRevision') and not confirmed)
                or row.get('deletion', {}).get('state') in {'pending', 'running'}
                or any(v.get('downloadRequested') for v in row.get('versions', []))):
            return False, '正在更正、下载或清理旧版，稍后核对。'
    if len(bindings) != 1:
        return False, '不是唯一绑定的已确认下载原片，用户原文件保留。'
    row, version = bindings[0]
    if (version.get('revision') != row.get('activeRevision')
            or not version.get('taskId') or not version.get('resultId')):
        return False, '缺少下载原片的当前版本及采集凭证，文件保留。'
    if version.get('filePath') != asset['path']:
        recovery = version.get('recovery') or {}
        matched = [item for item in links if item.get('id') == recovery.get('slicingItemId')
                   and item.get('collectionVideoId') == row['id']
                   and item.get('collectionRevision') == version['revision']
                   and item.get('collectionRecovery')]
        if len(matched) != 1:
            return False, '恢复原片与处理结果尚未唯一绑定，文件保留。'
    packages = manager.packages(asset)
    for name, outcome in outcomes.items():
        relevant = [p for p in packages if p.get('category') == name]
        if outcome == 'complete' and not relevant:
            return False, '缺少可关联的图片导入凭证。'
        for p in relevant:
            if (p.get('state') != 'complete' or not p.get('verifiedJobId')
                    or not p.get('jobId') or str(p['verifiedJobId']) != str(p['jobId'])
                    or not p.get('imageCount') or p.get('actualImages') != p['imageCount']
                    or len(set(map(str, p.get('outputImageIds') or []))) != p['imageCount']):
                return False, '图片尚未全部导入核验。'
            images = p.get('images')
            if (not isinstance(images, list) or len(images) != p['imageCount']
                    or any(not isinstance(name, str) for name in images) or len(set(images)) != len(images)):
                return False, '照片清单与实际导入数量不符，文件保留。'
            if (not p.get('videoFileVersion') or not compatible(asset['fileVersion'], p['videoFileVersion'])):
                return False, '图片包缺少对应原片版本，需先对账。'
            if p.get('cleanupState') != 'complete' and p.get('localCleanupState') != 'complete':
                if (not p.get('fileVersion') or not p.get('imageVersions')
                        or set(p.get('imageVersions', {})) != set(p.get('images', []))):
                    return False, '本地照片或压缩包缺少版本凭证。'
    return True, ''


def receipt_payload(manager, asset):
    links = manager.effective_links(asset)
    payload = {'version': asset.get('fileVersion'), 'items': [
        {key: item.get(key) for key in ('id', 'mediaId', 'mediaGeneration', 'collectionVideoId',
                                       'collectionRevision', 'fileVersion', 'categories')}
        for item in sorted(links, key=lambda row: row['id'])],
        'packages': [{key: package.get(key) for key in ('id', 'itemId', 'category', 'dataset',
           'jobId', 'verifiedJobId', 'imageCount', 'actualImages', 'outputImageIds', 'images',
           'videoFileVersion')} for package in sorted(manager.packages(asset), key=lambda row: row['id'])]}
    return payload


def receipt_signature(manager, asset):
    return hashlib.sha256(json.dumps(receipt_payload(manager, asset), sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def authorization(manager, asset):
    if hasattr(manager, '_cleanup_authorization_cache'):
        return manager._cleanup_authorization_cache.get(asset['id'], (None, None))
    rows = manager.db.execute("""SELECT payload FROM simple_workspace_requests
        WHERE json_extract(payload,'$.kind')='cleanup-completed' AND EXISTS (
            SELECT 1 FROM json_each(simple_workspace_requests.payload,'$.assets')
            WHERE json_extract(value,'$.id')=?) ORDER BY rowid DESC""", (asset['id'],))
    for (raw,) in rows:
        request = json.loads(raw)
        if request.get('kind') != 'cleanup-completed' or request.get('state') == 'complete':
            continue
        saved = next((row for row in request.get('assets', []) if row['id'] == asset['id']), None)
        if saved and authorized_snapshot(asset, request, saved):
            return request, saved
    return None, None


def authorized_snapshot(asset, request, saved):
    from .cleanup_remount import candidate
    journal = asset.get('cleanup') or {}
    return (candidate(asset, saved, request['id'])
            or (request.get('state') != 'attention' and saved.get('state') not in {'complete', 'attention'}
                and (journal.get('requestId') != request['id'] or not rejected_journal(journal))))


REJECTION_REASONS = {'remount_unverified', 'receipt_changed', 'file_changed', 'file_missing', 'workspace_identity'}


def rejected_journal(journal):
    """A display refresh cannot turn a recorded rejection into retry consent."""
    return (journal.get('state') == 'attention' or
            (journal.get('requestId') and journal.get('state') in {'pending_manual', 'waiting'}
             and journal.get('reasonCode') in REJECTION_REASONS))


def registered_cleanup(manager, asset):
    if asset.get('cleanup', {}).get('requestId'):
        return True
    if hasattr(manager, '_cleanup_registered_ids'):
        return asset['id'] in manager._cleanup_registered_ids
    return manager.db.execute("""SELECT 1 FROM simple_workspace_requests
        WHERE json_extract(payload,'$.kind')='cleanup-completed' AND EXISTS (
            SELECT 1 FROM json_each(simple_workspace_requests.payload,'$.assets')
            WHERE json_extract(value,'$.id')=?) LIMIT 1""", (asset['id'],)).fetchone() is not None


def file_progress(manifest):
    sizes = [(row, max(0, int((row.get('expected') or {}).get('size') or 0))) for row in manifest]
    return {'totalFiles': len(sizes), 'deletedFiles': sum(row.get('state') == 'deleted' for row, _ in sizes),
            'deletedBytes': sum(size for row, size in sizes if row.get('state') == 'deleted'),
            'remainingBytes': sum(size for row, size in sizes if row.get('state') != 'deleted')}


def public_operation(request, assets=()):
    result = {key: copy.deepcopy(request.get(key)) for key in ('id', 'operationId', 'state',
        'total', 'totalBytes', 'finished', 'waiting', 'failed', 'message', 'createdAt', 'updatedAt', 'completedAt')}
    result['assets'] = []
    indexed = {asset['id']: asset for asset in assets}
    for row in request.get('assets', []):
        progress = file_progress(row.get('manifest', []))
        journal = (indexed.get(row['id']) or {}).get('cleanup') or {}
        deleted_archives = None
        if journal.get('requestId') == request['id']:
            # Older requests stored a count, while their per-file deletion
            # receipts live in the asset journal. Project those exact receipts
            # without changing or expanding the user's original manifest.
            receipts = {entry['path']: entry for entry in journal.get('manifest', [])}
            manifest = []
            for entry in row.get('manifest', []):
                receipt = receipts.get(entry['path']) or {}
                deleted = receipt.get('state') == 'deleted' and receipt.get('expected') == entry.get('expected')
                manifest.append({**entry, 'state': 'deleted' if deleted else entry.get('state')})
            progress = file_progress(manifest)
            deleted_archives = sum(entry.get('kind') == 'archive' and entry.get('state') == 'deleted'
                                   for entry in manifest)
        else:
            manifest_deleted = progress['deletedFiles']
            progress = {key: row.get(key, value) for key, value in progress.items()}
            if row.get('state') == 'complete':
                deleted_archives = sum(entry.get('kind') == 'archive' for entry in row.get('manifest', []))
            elif progress['deletedFiles'] == manifest_deleted:
                deleted_archives = sum(entry.get('kind') == 'archive' and entry.get('state') == 'deleted'
                                       for entry in row.get('manifest', []))
        result['assets'].append({key: row.get(key) for key in (
            'id', 'name', 'state', 'message', 'reasonCode', 'currentName')} |
            progress | {'deletedArchives': deleted_archives})
    for key in ('totalFiles', 'deletedFiles', 'deletedBytes', 'remainingBytes'):
        result[key] = sum(row[key] for row in result['assets'])
    if all(row['deletedArchives'] is not None for row in result['assets']):
        result['deletedArchives'] = sum(row['deletedArchives'] for row in result['assets'])
    waiting = next((row for row in result['assets'] if row.get('reasonCode') and row['state'] != 'complete'), {})
    result['reasonCode'] = waiting.get('reasonCode', '')
    current = next((row for row in result['assets'] if row.get('currentName') and row['state'] in {'running', 'deleting'}), waiting)
    result['currentName'] = request.get('currentName') or current.get('currentName') or current.get('name', '')
    return result


class CleanupView:
    """One bounded read projection for counts; never used to authorize deletion."""
    def __init__(self, manager, assets, links=None, packages=None):
        self.manager = manager
        self._links = links if links is not None else {asset['id']: manager.linked(asset) for asset in assets}
        self._effective = {}
        self._packages = packages if packages is not None else {
            asset['id']: manager.packages(asset, linked=self._links.get(asset['id'], [])) for asset in assets}
        self._cleanup_binding_cache = {}
        for row in manager.videos.store.list(include_deleted=True):
            for version in row.get('versions', []):
                recovery = version.get('recovery') or {}
                choices = [(version.get('filePath'), version.get('fileVersion')),
                           (recovery.get('filePath') or recovery.get('destination'), recovery.get('fileVersion'))]
                seen = set()
                for path, expected in choices:
                    if path and path not in seen:
                        seen.add(path)
                        self._cleanup_binding_cache.setdefault(path, []).append((row, version, expected))
        self._cleanup_authorization_cache = {}
        self.requests = manager.rows('requests')
        self._cleanup_registered_ids = {saved['id'] for request in self.requests
            if request.get('kind') == 'cleanup-completed' for saved in request.get('assets', [])}
        indexed_assets = {asset['id']: asset for asset in assets}
        for request in self.requests:
            if request.get('kind') == 'cleanup-completed' and request.get('state') != 'complete':
                for saved in request.get('assets', []):
                    asset = indexed_assets.get(saved['id']) or {}
                    if authorized_snapshot(asset, request, saved):
                        self._cleanup_authorization_cache[saved['id']] = (request, saved)

    def __getattr__(self, name):
        return getattr(self.manager, name)

    def linked(self, asset):
        return self._links.get(asset['id'], [])

    def effective_links(self, asset):
        if asset['id'] not in self._effective:
            control = getattr(self.slicing, 'control', None)
            rows = self.linked(asset)
            self._effective[asset['id']] = (control.scoped_effective_items(rows) if control
                                            else [row for row in rows if not row.get('superseded')])
        return self._effective[asset['id']]

    def packages(self, asset, linked=None):
        rows = self._packages.get(asset['id'], [])
        if linked is None:
            return rows
        identities = {row['id'] for row in linked}
        return [row for row in rows if row.get('itemId') in identities]

    def outcomes(self, asset):
        return type(self.manager).outcomes(self, asset)

    def media_entries(self, asset, existing_only=True, cached_packages=None):
        return type(self.manager).media_entries(self, asset, existing_only,
            self.packages(asset) if cached_packages is None else cached_packages)

    def photo_results_verified(self, asset):
        from .photo_completion import photo_results_verified
        links = self.effective_links(asset)
        return photo_results_verified(links, self.packages(asset, linked=links))

    def _asset_wait_reason(self, asset):
        from .workspace_scheduler import WorkspaceScheduler
        return WorkspaceScheduler._asset_wait_reason(self, asset)

    def _asset_manually_paused(self, asset):
        from .workspace_scheduler import WorkspaceScheduler
        return WorkspaceScheduler._asset_manually_paused(self, asset)


def summary(manager, assets=None, links=None, packages=None):
    changes = (manager.db.total_changes, manager.db.execute('PRAGMA data_version').fetchone()[0])
    cached = getattr(manager, '_cleanup_summary_cache', None)
    if cached and cached[0] == changes:
        return copy.deepcopy(cached[1])
    assets = manager.rows('assets') if assets is None else assets
    view = CleanupView(manager, assets, links, packages)
    eligible = cleanup_candidates(view, assets)
    requests = [row for row in reversed(view.requests) if row.get('kind') == 'cleanup-completed' and row.get('total')]
    latest = next((row for row in requests if row.get('state') in {'queued', 'running', 'waiting'}), None)
    if not latest and requests:
        latest = requests[0]
    result = {'eligibleCount': len(eligible), 'eligibleBytes': sum(
        entry['expected']['size'] for row in eligible for entry in row['manifest'] if entry['state'] != 'deleted'),
        'previewToken': preview_token(manager, eligible),
        'latestOperation': public_operation(latest, assets) if latest else None}
    manager._cleanup_summary_cache = (changes, result)
    return copy.deepcopy(result)


def snapshot_entries(manager, asset, validate=True):
    from pathlib import Path
    if not manager.enabled:
        raise ValueError('工作区尚未启用。')
    entries = {}
    for row in manager.media_entries(asset):
        prior = entries.setdefault(row['path'], row)
        if prior != row:
            raise ValueError('同一路径绑定了不同文件记录，原位保留。')
    entries = copy.deepcopy(list(entries.values()))
    previous = {row.get('path'): row for row in asset.get('cleanup', {}).get('manifest', [])}
    for entry in entries:
        path = checked_path(entry['path']) if validate else Path(entry['path'])
        if not path.is_relative_to(manager.root) or len(path.relative_to(manager.root).parts) < 3:
            raise ValueError('文件不属于程序工作区，原位保留。')
        if not {'device', 'inode', 'size', 'modifiedNs'}.issubset(entry.get('expected') or {}):
            raise ValueError('缺少文件版本凭证，原位保留。')
        entry['state'] = 'planned'
        prior = previous.get(entry['path']) or {}
        if (prior.get('state') in {'deleting', 'deleted'}
                and prior.get('expected') == entry['expected']):
            # Preserve an already journaled deletion after an older interrupted
            # operation; renewed consent must not pretend these bytes reappear.
            entry['state'] = prior['state']
            if prior.get('deletedAt'):
                entry['deletedAt'] = prior['deletedAt']
    return entries


def moving_asset_ids(manager):
    return {identity for identity, in manager.db.execute(
        """SELECT json_extract(payload, '$.assetId') FROM simple_workspace_moves
           WHERE json_extract(payload, '$.state') = 'moving'
              OR (json_extract(payload, '$.state') = 'attention'
                  AND (json_extract(payload, '$.destinationVersion') IS NOT NULL
                       OR json_extract(payload, '$.temporaryVersion') IS NOT NULL))""")}


def eligible_entries(manager, asset, moving):
    if asset['id'] in moving or not eligibility(manager, asset)[0]:
        return None
    registered = registered_cleanup(manager, asset)
    journal = asset.get('cleanup') or {}
    if registered and (journal.get('state') != 'attention' or journal.get('reasonCode') != 'file_missing'
                       or authorization(manager, asset)[0]):
        return None
    try:
        entries = snapshot_entries(manager, asset, validate=False)
    except ValueError:
        return None
    if not registered:
        return entries

    # A new click may retry a stale snapshot only if verified moves explain
    # the change, with exactly the same files and no partial deletion.
    from .file_locations import resolve
    current = {(e['path'], e['kind'], e.get('packageId')): e['expected'] for e in entries}
    moved = False
    for entry in journal.get('manifest', []):
        if entry.get('state') != 'planned':
            return None
        target, version = resolve(manager.db, entry['path'], entry['expected'])
        expected = current.pop((target, entry['kind'], entry.get('packageId')), None)
        if not expected or not compatible(expected, version):
            return None
        moved |= target != entry['path']
    return entries if moved and not current else None


def cleanup_candidates(manager, assets, only=None):
    moving = moving_asset_ids(manager)
    rows = []
    for asset in assets:
        if ((only and asset['id'] != only) or asset.get('status') not in {'managed', 'waiting'}
                or asset.get('cleanup', {}).get('state') == 'complete'):
            continue
        entries = eligible_entries(manager, asset, moving)
        if entries:
            rows.append({'id': asset['id'], 'name': asset.get('name', ''), 'path': asset['path'],
                         'fileVersion': copy.deepcopy(asset['fileVersion']),
                         'manifest': entries, 'state': 'queued', 'message': '等待安全清理。'})
    return rows


def preview_token(manager, rows):
    from .cleanup_remount import digest
    scope = [{'id': row['id'], 'manifest': row['manifest']} for row in rows]
    return digest({'revision': manager.config['revision'], 'scope': scope})


async def request_completed(manager, revision, request_id, only=None, *, token=None):
    from .videos import Conflict
    async with manager.cleanup_registration_lock:
        prior = manager.get('requests', request_id)
        if prior:
            if (prior.get('kind') != 'cleanup-completed' or prior.get('only') != only
                    or prior.get('previewToken') != token):
                raise Conflict('请求编号已用于其他操作。')
            return public_operation(prior, manager.rows('assets'))
        if revision != manager.config['revision']:
            raise Conflict('工作区配置已更新，请刷新后重试。')
        if manager.maintenance:
            raise Conflict('工作区正在迁移，完成后再清理。')
        control = getattr(manager.slicing, 'control', None)
        if control:
            control.refresh()
        assets = manager.rows('assets')
        view = CleanupView(manager, assets)
        rows = cleanup_candidates(view, assets, only)
        if token is not None and token != preview_token(manager, rows):
            raise Conflict('可清理的文件已变化，请核对更新后的数量和空间，再确认清理。')
        by_id = {asset['id']: asset for asset in assets}
        for row in rows:
            row['receiptSignature'] = receipt_signature(view, by_id[row['id']])
        request = {'id': request_id, 'operationId': request_id, 'kind': 'cleanup-completed', 'only': only,
                   'previewToken': token,
                   'state': 'queued' if rows else 'complete', 'assets': rows, 'total': len(rows),
                   'totalBytes': sum(e['expected']['size'] for row in rows for e in row['manifest'] if e['state'] != 'deleted'),
                   'finished': 0, 'waiting': len(rows), 'failed': 0, 'createdAt': now_iso(),
                   'message': (f'已登记 {len(rows)} 条；以后完成的视频等待下次清理。'
                               if rows else '当前没有可登记的文件。正在归档的文件将在归档后可清理。')}
        manager.save('requests', request)
        manager.wake.set()
        return public_operation(request)


def update_operations(manager):
    for request in manager.rows('requests'):
        if request.get('kind') != 'cleanup-completed' or request.get('state') == 'complete':
            continue
        before = json.dumps(request, sort_keys=True)
        for saved in request.get('assets', []):
            asset = manager.get('assets', saved['id']) or {}
            journal = asset.get('cleanup') or {}
            if journal.get('requestId') == request['id']:
                if rejected_journal(journal) and (journal.get('state') != 'attention' or
                        journal.get('message') == '处理完成，等待点击清理已结束文件。'):
                    # Repair only the old display-state overwrite; immutable
                    # scope, content receipts and rejection reason stay intact.
                    journal['state'] = 'attention'
                    if journal.get('message') == '处理完成，等待点击清理已结束文件。':
                        journal['message'] = ('原压缩包或照片凭证不足，文件保留待核对。'
                            if journal.get('reasonCode') == 'remount_unverified' else
                            '原清理范围未通过文件或照片凭证核验，保留待核对。')
                    manager.save('assets', asset)
                saved.update(state=journal.get('state', 'waiting'), message=journal.get('message', ''),
                             reasonCode=journal.get('reasonCode', ''), currentName=journal.get('currentName', ''),
                             **file_progress(journal.get('manifest', [])))
        finished = sum(row['state'] == 'complete' for row in request['assets'])
        failed = sum(row['state'] == 'attention' for row in request['assets'])
        waiting = request['total'] - finished - failed
        running = any(row['state'] in {'running', 'deleting'} for row in request['assets'])
        request.update(finished=finished, failed=failed, waiting=waiting,
                       state='running' if running else 'waiting' if waiting else 'attention' if failed else 'complete',
                       message=f'已清理 {finished}/{request["total"]} 条，等待 {waiting} 条，保留 {failed} 条。')
        if not waiting:
            request.setdefault('completedAt', now_iso())
        if json.dumps(request, sort_keys=True) != before:
            manager.save('requests', request)


def wait_for_environment(manager, code, message):
    """A disconnected root must not leave a manual request looking unstarted."""
    for request in manager.rows('requests'):
        if request.get('kind') != 'cleanup-completed' or request.get('state') in {'complete', 'attention'}:
            continue
        changed = False
        state = 'attention' if code == 'workspace_identity' else 'waiting'
        for saved in request.get('assets', []):
            if saved.get('state') in {'complete', 'attention'}:
                continue
            if saved.get('reasonCode') != code or saved.get('state') != state:
                saved.update(state=state, reasonCode=code, message=message)
                changed = True
        if changed:
            request.update(state=state, message=message)
            if state == 'attention':
                request.update(waiting=0, failed=sum(row.get('state') == 'attention' for row in request['assets']))
            manager.save('requests', request)


async def cleanup(manager, asset):
    from .cleanup_remount import (digest, effective_snapshot, expected_version, scope_digest, mapping,
                                 validate as validate_remount, verify_current as verify_remount_current)
    if asset.get('cleanup', {}).get('state') == 'complete':
        return
    request, saved = authorization(manager, asset)
    if not request:
        if registered_cleanup(manager, asset):
            update_operations(manager)
            return
        eligible, reason = eligibility(manager, asset)
        asset['cleanup'] = {**asset.get('cleanup', {}), 'state': 'pending_manual' if eligible else 'waiting',
                            'message': '处理完成，等待点击清理已结束文件。' if eligible else reason}
        manager.save('assets', asset)
        return
    if request.get('state') == 'attention':
        # A legacy device-only failure gets one content-check attempt under
        # the existing request. No path or expected version is rewritten.
        request.update(state='waiting')
        request.pop('completedAt', None)
        saved.update(state='waiting', message='正在核对原磁盘与既有清理凭证。')
        manager.save('requests', request)
    journal = asset.get('cleanup') or {}
    if journal.get('requestId') != request['id']:
        journal = {'state': 'running', 'requestId': request['id'], 'startedAt': now_iso(),
                   'manifest': copy.deepcopy(saved['manifest']), 'message': '正在清理本次已登记文件。'}
    manifest = journal['manifest']
    eligible, reason = eligibility(manager, asset)
    if not eligible:
        journal.update(state='waiting', reasonCode='paused' if '暂停' in reason else 'processing', message=reason)
        asset['cleanup'] = journal
        manager.save('assets', asset)
        update_operations(manager)
        return
    paths = [e['path'] for e in manifest if e['state'] != 'deleted']
    if any(manager.leases.busy(p) for p in paths):
        journal.update(state='waiting', reasonCode='file_busy', message='播放或处理使用中，释放后清理。')
        manager.waiting_paths.update(paths)
        asset['cleanup'] = journal
        manager.save('assets', asset)
        update_operations(manager)
        return
    try:
        await manager.check_root()
        root_device = await manager.videos.file_operation(lambda: manager.root.stat().st_dev)
        if any(expected_version(entry, saved, journal).get('device') != root_device
               for entry in manifest if entry.get('state') not in {'deleted', 'deleting'}):
            payload = receipt_payload(manager, asset)
            try:
                with ExitStack() as guards:
                    for path in paths:
                        guards.enter_context(manager.leases.read(path))
                    proof = await manager.videos.file_operation(validate_remount, manager.root,
                        manager.config['workspaceId'], copy.deepcopy(asset), copy.deepcopy(saved),
                        copy.deepcopy(journal), copy.deepcopy(manager.packages(asset)), payload)
                current = manager.get('assets', asset['id']) or {}
                latest = manager.get('requests', request['id']) or {}
                latest_saved = next((row for row in latest.get('assets', []) if row['id'] == asset['id']), {})
                if (current.get('fileVersion') != asset.get('fileVersion')
                        or receipt_signature(manager, current) != digest(payload)
                        or scope_digest(latest_saved) != scope_digest(saved)
                        or latest_saved.get('receiptSignature') != saved['receiptSignature']):
                    raise ValueError('校验期间处理凭证已变化，原清理范围保留。')
                journal.update(remountProof=proof, state='waiting', reasonCode='',
                               message='原磁盘内容已核验，继续原清理范围。')
                asset['cleanup'] = journal
                manager.save('assets', asset)
                latest_saved.update(state='waiting', reasonCode='', message=journal['message'])
                latest.update(state='waiting')
                latest.pop('completedAt', None)
                manager.save('requests', latest)
            except ValueError as error:
                journal.update(state='attention', reasonCode='remount_unverified', message=str(error))
                asset['cleanup'] = journal
                manager.save('assets', asset)
                update_operations(manager)
                return
        accepted = effective_snapshot(saved, journal)
        remount = mapping(saved, journal)
        if (asset.get('path') != accepted['path']
                or not compatible(asset.get('fileVersion') or {}, accepted['fileVersion'])
                or receipt_signature(manager, asset) != accepted['receiptSignature']):
            journal.update(state='attention', reasonCode='receipt_changed', message='视频版本、路径或照片凭证已变化，本次不清理。')
            asset['cleanup'] = journal
            manager.save('assets', asset)
            update_operations(manager)
            return
        # Validate the complete deletion set first. Missing planned files are
        # identity problems, not proof that an unrecorded deletion succeeded.
        for entry in manifest:
            path = await manager.videos.file_operation(checked_path, entry['path'])
            if not path.is_relative_to(manager.root) or len(path.relative_to(manager.root).parts) < 3:
                raise ValueError('待清理文件不属于已登记工作区。')
            if entry['state'] == 'deleted':
                continue
            if entry['state'] == 'deleting' and not await manager.videos.file_operation(path.exists):
                continue
            expected = expected_version(entry, saved, journal)
            actual = await manager.videos.file_operation(file_version, path, 'edgeHash' in expected)
            if not compatible(actual, expected):
                raise ValueError('文件版本变化，剩余文件保留。')
            if remount:
                await manager.videos.file_operation(verify_remount_current, path, entry, saved, journal)
        journal.update(state='running', reasonCode='', currentName=asset.get('name', ''))
        asset['cleanup'] = journal
        manager.save('assets', asset)
        progress_at = time.monotonic()
        for entry in manifest:
            if entry['state'] == 'deleted':
                continue
            path = await manager.videos.file_operation(checked_path, entry['path'])
            with manager.leases.write(path):
                permitted, reason = eligibility(manager, asset)
                if not permitted:
                    raise ValueError(reason or '处理指令已更新，停止清理。')
                if await manager.videos.file_operation(path.exists):
                    expected = expected_version(entry, saved, journal)
                    actual = await manager.videos.file_operation(file_version, path, 'edgeHash' in expected)
                    if not compatible(actual, expected):
                        raise ValueError('文件已改变，剩余文件保留。')
                    # File hashing can overlap a pause, correction or changed
                    # photo receipt. Check again immediately before unlinking.
                    permitted, reason = eligibility(manager, manager.get('assets', asset['id']) or asset)
                    if not permitted:
                        raise ValueError(reason or '处理指令已更新，停止清理。')
                    if receipt_signature(manager, asset) != accepted['receiptSignature']:
                        raise ValueError('照片或处理版本已变化，本次剩余文件保留。')
                    if remount:
                        await manager.videos.file_operation(verify_remount_current, path, entry, saved, journal)
                        # The ctime check also yields to the event loop. Keep a
                        # pause/correction arriving during that last disk read.
                        permitted, reason = eligibility(manager, manager.get('assets', asset['id']) or asset)
                        if not permitted or receipt_signature(manager, asset) != accepted['receiptSignature']:
                            raise ValueError(reason or '照片或处理版本已变化，本次剩余文件保留。')
                    entry['state'] = 'deleting'
                    manager.save('assets', asset)
                    ctime = remount['files'][entry['path']]['ctimeNs'] if remount else None
                    await manager.videos.file_operation(
                        unlink_workspace_file, manager.root, manager.config['workspaceId'], path, expected, ctime)
                elif entry['state'] != 'deleting':
                    raise ValueError('缺少文件删除凭证，停止清理。')
                entry.update(state='deleted', deletedAt=now_iso())
                manager.save('assets', asset)
                if time.monotonic() - progress_at >= 1:
                    update_operations(manager)
                    progress_at = time.monotonic()
        journal.update(state='complete', reasonCode='', currentName='', completedAt=now_iso(), message='本地媒体已清理；云端成果和历史记录保留。')
        asset.update(status='cleaned', message=journal['message'])
        # Tombstones prevent historical items from rebuilding removed media.
        for item in manager.linked(asset):
            item.update(localCleanupState='complete', localCleanupAt=journal['completedAt'])
            manager.slicing.store.save('items', item)
        for package in manager.packages(asset):
            package.update(localCleanupState='complete', localCleanupAt=journal['completedAt'],
                           cleanupState='complete', cleanupMessage='本地照片和压缩包已清理。')
            manager.slicing.packages.store.save('items', package)
        for item in manager.uploads.store.list('items'):
            if item.get('path') == asset['path'] and compatible(asset['fileVersion'], item.get('fileVersion')):
                item.update(localCleanupState='complete', localCleanupAt=journal['completedAt'])
                manager.uploads.store.save('items', item)
        for row, bound in collection_bindings(manager, asset):
            changed = False
            for v in row.get('versions', []):
                if v.get('revision') == bound.get('revision'):
                    v.update(localCleanupState='complete', localCleanupAt=journal['completedAt'])
                    if v.get('recovery'):
                        v['recovery'].update(localCleanupState='complete', localCleanupAt=journal['completedAt'])
                    changed = True
            if changed:
                manager.videos.store.save(row)
    except (OSError, ValueError) as error:
        permitted, reason = eligibility(manager, manager.get('assets', asset['id']) or asset)
        identity_error = getattr(manager, 'availability', {}).get('reasonCode') == 'workspace_identity'
        offline = not manager.online and not identity_error
        missing = isinstance(error, FileNotFoundError) and not offline
        environment = isinstance(error, FileEnvironmentError)
        journal.update(state='waiting' if not permitted or environment or offline or (isinstance(error, OSError) and not missing) else 'attention',
                       reasonCode='paused' if not permitted and '暂停' in reason else 'processing' if not permitted
                       else 'workspace_identity' if identity_error else error.code if environment else 'disk_offline' if offline else 'file_missing' if missing
                       else 'file_changed' if isinstance(error, ValueError) else 'disk_io',
                       message=reason if not permitted else '等待原工作区磁盘恢复，清理清单和文件保留。' if offline
                       else '清单中的文件已在外部移除，无法核实版本；剩余文件保留。' if missing
                       else str(error) if isinstance(error, ValueError)
                       else '磁盘暂不可用，剩余文件保留，将自动重试。')
    asset['cleanup'] = journal
    manager.save('assets', asset)
    update_operations(manager)
