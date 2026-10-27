"""Deletion gates. Completion labels alone are never deletion authority."""
from .audit_moves import checked_path
from .domain import now_iso
from .workspace_files import compatible, file_version


def eligibility(manager, asset):
    if manager.is_preserved(asset):
        return False, '已上传历史视频，按保留规则跳过本地清理。'
    outcomes = manager.outcomes(asset)
    if not all(v in {'complete', 'no_target', 'uncuttable'} for v in outcomes.values()):
        return False, '两类处理尚未结束。'
    links = manager.linked(asset)
    if not all(i.get('fileVersion') and compatible(asset['fileVersion'], i['fileVersion']) for i in links):
        return False, '缺少稳定的原片版本。'
    for row in manager.videos.store.list(include_deleted=True):
        if any(v.get('filePath') == asset['path'] for v in row.get('versions', [])):
            if (row.get('cleanupPending') or row.get('activeRevision') != row.get('acceptedRevision')
                    or row.get('deletion', {}).get('state') in {'pending', 'running'}
                    or any(v.get('downloadRequested') for v in row.get('versions', []))):
                return False, '正在更正、下载或清理旧版，稍后核对。'
    packages = manager.packages(asset)
    for name, outcome in outcomes.items():
        relevant = [p for p in packages if p.get('category') == name]
        if outcome == 'complete' and not relevant:
            return False, '缺少可关联的图片导入凭证。'
        for p in relevant:
            if (p.get('state') != 'complete' or not p.get('verifiedJobId')
                    or not p.get('imageCount') or p.get('actualImages') != p['imageCount']
                    or len(set(map(str, p.get('outputImageIds') or []))) != p['imageCount']):
                return False, '图片尚未全部导入核验。'
            if (not p.get('videoFileVersion') or not compatible(asset['fileVersion'], p['videoFileVersion'])):
                return False, '图片包缺少对应原片版本，需先对账。'
            if p.get('cleanupState') != 'complete' and p.get('localCleanupState') != 'complete':
                if (not p.get('fileVersion') or not p.get('imageVersions')
                        or set(p.get('imageVersions', {})) != set(p.get('images', []))):
                    return False, '本地照片或压缩包缺少版本凭证。'
    uploads = manager.uploads.store.list('items')
    for dataset in ('27912', '27944'):
        matched = [i for i in uploads if i.get('dataset') == dataset and i.get('path') == asset['path']
                   and compatible(asset['fileVersion'], i.get('fileVersion')) and i.get('state') == 'complete'
                   and i.get('remoteVideoId')]
        # Own exact import manifest, bound object and result ID are required.
        # A same-name inventory match or an expired log alone is insufficient.
        proved = []
        for item in matched:
            batch = manager.uploads.store.get('batches', item.get('batchId', ''))
            if (batch and item['id'] in batch.get('items', []) and item.get('object')
                    and item.get('object', {}).get('key') in batch.get('keys', []) and item.get('jobId')
                    and str(batch.get('jobId')) == str(item['jobId'])):
                proved.append(item)
        if len(proved) != 1:
            return False, f'视频集 {dataset} 缺少该版本的唯一导入凭证，原片保留。'
    return True, ''


async def remote_receipts(manager, asset):
    if not manager.uploads.audit.client.connected:
        return False
    inventory_cache = getattr(manager, '_cleanup_inventory', {})
    now = manager.slicing.clock()
    for dataset in ('27912', '27944'):
        cached = inventory_cache.get(dataset)
        if not cached or now - cached[0] > 30:
            cached = (now, await manager.uploads.remote.videos(dataset))
            inventory_cache[dataset] = cached
        items = [i for i in manager.uploads.store.list('items') if i.get('dataset') == dataset
                 and i.get('path') == asset['path'] and i.get('state') == 'complete'
                 and compatible(asset['fileVersion'], i.get('fileVersion'))]
        ids = {str(i.get('remoteVideoId')) for i in items if i.get('remoteVideoId')}
        found = [v for v in cached[1] if str(v.get('id')) in ids]
        if len(found) != 1:
            return False
        size = found[0].get('file_size', found[0].get('size'))
        if size is not None:
            try:
                if int(size) != asset['fileVersion']['size']:
                    return False
            except (TypeError, ValueError):
                return False
    manager._cleanup_inventory = inventory_cache
    return True


async def cleanup(manager, asset):
    if asset.get('cleanup', {}).get('state') == 'complete':
        return
    eligible, reason = eligibility(manager, asset)
    if not eligible:
        asset['cleanup'] = {**asset.get('cleanup', {}), 'state': 'waiting', 'message': reason}
        manager.save('assets', asset)
        return
    try:
        if not await remote_receipts(manager, asset):
            asset['cleanup'] = {**asset.get('cleanup', {}), 'state': 'waiting',
                                'message': '等待核实两个视频集中的绑定原片。'}
            manager.save('assets', asset)
            return
    except Exception:
        asset['cleanup'] = {**asset.get('cleanup', {}), 'state': 'waiting',
                            'message': '平台暂不可核对，文件保留。'}
        manager.save('assets', asset)
        return
    journal = asset.get('cleanup') or {}
    manifest = journal.get('manifest')
    if not manifest:
        manifest = manager.media_entries(asset)
        manifest = list({e['path']: e for e in manifest}.values())
        for e in manifest:
            e['state'] = 'planned'
        journal = {'state': 'running', 'startedAt': now_iso(), 'manifest': manifest,
                   'message': '正在清理已核验的本地媒体。'}
    paths = [e['path'] for e in manifest if e['state'] != 'deleted']
    if any(manager.leases.busy(p) for p in paths):
        journal.update(state='waiting', message='播放或处理使用中，释放后清理。')
        asset['cleanup'] = journal
        manager.save('assets', asset)
        return
    try:
        manager.require_root()
        # Validate the complete deletion set first. Missing planned files are
        # identity problems, not proof that an unrecorded deletion succeeded.
        for entry in manifest:
            path = checked_path(entry['path'])
            if not path.is_relative_to(manager.root) or len(path.relative_to(manager.root).parts) < 3:
                raise ValueError('待清理文件不属于已登记工作区。')
            if entry['state'] == 'deleted':
                continue
            if not path.exists() and entry['state'] == 'deleting':
                continue
            actual = await manager.videos.file_operation(file_version, path, 'edgeHash' in entry['expected'])
            if not compatible(actual, entry['expected']):
                raise ValueError('文件版本变化，剩余文件保留。')
        journal['state'] = 'running'
        asset['cleanup'] = journal
        manager.save('assets', asset)
        for entry in manifest:
            if entry['state'] == 'deleted':
                continue
            path = checked_path(entry['path'])
            with manager.leases.write(path):
                if path.exists():
                    actual = await manager.videos.file_operation(file_version, path, 'edgeHash' in entry['expected'])
                    if not compatible(actual, entry['expected']):
                        raise ValueError('文件已改变，剩余文件保留。')
                    entry['state'] = 'deleting'
                    manager.save('assets', asset)
                    await manager.videos.file_operation(path.unlink)
                elif entry['state'] != 'deleting':
                    raise ValueError('缺少文件删除凭证，停止清理。')
                entry.update(state='deleted', deletedAt=now_iso())
                manager.save('assets', asset)
            # Remove empty program-created leaf folders only. Unknown siblings stay.
            for parent in (path.parent, path.parent.parent):
                if parent.parent == manager.root or parent == manager.root:
                    continue
                try:
                    parent.rmdir()
                except OSError:
                    pass
        journal.update(state='complete', completedAt=now_iso(), message='本地媒体已清理；云端成果和历史记录保留。')
        asset.update(status='cleaned', message=journal['message'])
        # Tombstones prevent historical items from rebuilding removed media.
        for item in manager.linked(asset):
            item.update(localCleanupState='complete', localCleanupAt=journal['completedAt'])
            manager.slicing.store.save('items', item)
        for package in manager.packages(asset):
            package.update(localCleanupState='complete', localCleanupAt=journal['completedAt'], cleanupState='complete')
            manager.slicing.packages.store.save('items', package)
        for item in manager.uploads.store.list('items'):
            if item.get('path') == asset['path'] and compatible(asset['fileVersion'], item.get('fileVersion')):
                item.update(localCleanupState='complete', localCleanupAt=journal['completedAt'])
                manager.uploads.store.save('items', item)
        for row in manager.videos.store.list(include_deleted=True):
            changed = False
            for v in row.get('versions', []):
                if v.get('filePath') == asset['path']:
                    v.update(localCleanupState='complete', localCleanupAt=journal['completedAt'])
                    changed = True
            if changed:
                manager.videos.store.save(row)
    except (OSError, ValueError) as error:
        journal.update(state='attention', message=str(error) if isinstance(error, ValueError) else '磁盘暂不可用，剩余文件保留。')
    asset['cleanup'] = journal
    manager.save('assets', asset)
