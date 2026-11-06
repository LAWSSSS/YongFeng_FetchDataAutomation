"""Current-version presentation shared by video confirmation and results.

Reads only registered bindings; never changes a worker, file or task receipt.
"""
import copy


STAGES = {
    'queued': '已登记', 'download': '下载', 'download_wait': '等待下载恢复',
    'recognition_queue': '等待识别', 'recognizing': '识别', 'photos': '照片处理', 'photo_export': '照片导出',
    'photo_export_wait': '等待原片读取', 'photo_upload': '照片上传', 'photos_paused': '照片上传已暂停',
    'photo_queued': '排队上传', 'photo_import': '照片导入', 'verification': '结果核验',
    'identity': '身份核对', 'waiting_disk': '等待磁盘', 'paused': '已暂停',
    'awaiting_cleanup': '待清理', 'retained': '已完成并保留', 'cleaned': '已清理',
}
PACKAGE_FIELDS = ('id', 'itemId', 'category', 'state', 'revision', 'imageCount',
                  'bytes', 'size', 'updatedAt', 'jobId', 'message', 'errorCode',
                  'cleanupState', 'localCleanupState')


def _progress(done, total, unit):
    if not isinstance(total, (int, float)) or isinstance(total, bool) or total <= 0:
        return None
    if not isinstance(done, (int, float)) or isinstance(done, bool):
        return None
    return {'done': max(0, min(done, total)), 'total': total, 'unit': unit}


def current_presentations(flow):
    hidden_slices = flow.slicing.visibility.hidden()
    hidden_videos = flow.videos.record_visibility.hidden()
    results = []
    for operation in flow.rows():
        row = flow.videos.store.get(operation.get('videoId', ''))
        if not row or row.get('deletedAt'):
            continue
        version = flow.videos.version(row, operation.get('version'))
        current = flow.current_operation(row, version) if version else None
        if not current or current['id'] != operation['id']:
            continue
        value = flow.public(operation)
        linked, conflicts = flow.linked_items(row, version, operation.get('mediaGeneration', 0), allow_paused=True)
        # An explicit package reference still needs its exact item and category.
        # Names and visible history are never upload identities.
        references = {(slot.get('packageId'), item['id'], category)
                      for item in linked for category, slot in item.get('categories', {}).items()
                      if slot.get('packageId')}
        packages = [package for package in flow.linked_packages(linked)
                    if not package.get('superseded') and
                    (package['id'], package.get('itemId'), package.get('category')) in references]
        value.update(name=row.get('name', ''), confirmedAt=operation.get('confirmedAt') or operation.get('createdAt'),
                     progress=None, packages=[{key: p.get(key) for key in PACKAGE_FIELDS if key in p} for p in packages],
                     hasResultRecord=any(item['id'] not in hidden_slices for item in linked),
                     recordHidden=(all(item['id'] in hidden_slices for item in linked) if linked
                                   else row['id'] in hidden_videos))
        timestamps = [v.get('updatedAt') or '' for v in [operation, row, version, *linked, *packages]]
        value['updatedAt'] = max(timestamps, default='')
        terminal = value.get('state') == 'complete'
        issue = value.get('issue')
        if not terminal and not issue and not conflicts and not value.get('environmentBlocker'):
            waiting_export = next((slot for item in linked for slot in item.get('categories', {}).values()
                                   if slot.get('state') == 'waiting_connection' and slot.get('waitStage') == 'export'
                                   and not slot.get('packageId')), None)
            if version.get('state') in {'downloading', 'publishing'}:
                value.update(state='running', stage='download', message='正在保存视频。' if version['state'] == 'publishing' else '正在下载已确认的最终版本。')
                value['progress'] = _progress(version.get('receivedBytes'), version.get('totalBytes'), 'bytes')
            elif any(item.get('state') == 'analyzing' for item in linked):
                value.update(state='running', stage='recognizing', message='正在识别视频画面。')
                positions = [_progress(item.get('analysisDone'), item.get('analysisTotal'), '个位置') for item in linked]
                value['progress'] = max((p for p in positions if p), key=lambda p: p['total'], default=None)
            elif waiting_export and all(package.get('state') == 'complete' for package in packages):
                value.update(state='waiting', stage='photo_export_wait',
                             message=waiting_export.get('message') or '等待原片读取恢复，再继续导出。')
            elif packages:
                states = {package.get('state') for package in packages}
                failed = next((p for p in packages if p.get('state') == 'attention'), None)
                if failed:
                    value.update(state='attention', stage='verification' if failed.get('jobId') else 'photo_upload',
                                 message=failed.get('message') or '照片处理需要核对。')
                    value['issue'] = {'code': failed.get('errorCode') or 'photo_attention',
                                      'stage': value['stage'], 'message': value['message']}
                elif states.intersection({'submitting', 'importing', 'reconciling'}):
                    value.update(state='running', stage='verification' if states == {'reconciling'} else 'photo_import',
                                 message='正在核对已提交的照片导入任务。')
                elif states.intersection({'queued', 'uploading', 'uploaded'}):
                    if flow.slicing.packages.paused:
                        value.update(state='waiting', stage='photos_paused', message='照片已打包；上传已暂停，文件和进度保留。')
                    elif 'uploading' in states:
                        value.update(state='running', stage='photo_upload', message='正在上传照片包。')
                        uploading = [p for p in packages if p.get('state') == 'uploading']
                        progress = [_progress(p.get('bytes'), p.get('size'), 'bytes') for p in uploading]
                        if progress and all(progress):
                            value['progress'] = {'done': sum(p['done'] for p in progress),
                                                 'total': sum(p['total'] for p in progress), 'unit': 'bytes'}
                    elif 'uploaded' in states:
                        value.update(state='running', stage='photo_import', message='照片已上传，等待导入空位。')
                    else:
                        value.update(state='queued', stage='photo_queued', message='照片已打包，排队等待上传。')
                elif states == {'complete'}:
                    unfinished = {slot.get('state') for item in linked for slot in item.get('categories', {}).values()
                                  if not slot.get('packageId')}
                    if unfinished & {'ready', 'waiting_slot', 'exporting'}:
                        value.update(state='running', stage='photo_export', message='正在导出另一类照片。')
                    elif 'pending' in unfinished:
                        value.update(state='queued', stage='recognition_queue', message='等待识别剩余类别。')
                    else:
                        value.update(state='running', stage='verification', message='正在核对两类照片成果与处理结论。')
        if not terminal:
            wait = flow.wait_reason(row['id'])
            if wait and not value.get('issue'):
                value.update(state='waiting', stage='paused', message=wait, progress=None)
        value.update(terminal=terminal, stageLabel=STAGES.get(value.get('stage'), value.get('stage') or '处理中'))
        if terminal:
            value['progress'] = None
        results.append(copy.deepcopy(value))
    return results
