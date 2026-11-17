"""Current-version presentation shared by video confirmation and results.

Reads only registered bindings; never changes a worker, file or task receipt.
"""
import copy

from .photo_completion import terminal_slice_results, verified_processing_outcome

STAGES = {
    'queued': '已登记', 'download': '下载', 'download_wait': '等待下载恢复',
    'recognition_queue': '等待识别', 'recognizing': '识别', 'photos': '照片处理', 'photo_export': '照片导出',
    'recognition_review': '识别需核对',
    'photo_export_wait': '等待原片读取', 'photo_upload': '照片上传', 'photos_paused': '照片上传已暂停',
    'photo_queued': '排队上传', 'photo_import': '照片导入', 'verification': '结果核验',
    'identity': '身份核对', 'waiting_disk': '等待磁盘', 'paused': '已暂停',
    'processed': '处理完成', 'awaiting_cleanup': '待清理', 'retained': '已完成并保留', 'cleaned': '已清理',
}
PACKAGE_FIELDS = ('id', 'itemId', 'category', 'state', 'revision', 'imageCount',
                  'bytes', 'size', 'updatedAt', 'jobId', 'message', 'errorCode',
                  'cleanupState', 'localCleanupState')


def referenced_packages(items, packages):
    """An old or unreferenced receipt cannot supply the current stage."""
    references = {(slot.get('packageId'), item['id'], category)
                  for item in items for category, slot in item.get('categories', {}).items()
                  if slot.get('packageId')}
    return [p for p in packages if not p.get('superseded') and
            (p['id'], p.get('itemId'), p.get('category')) in references]


def linked_processing_issue(items, packages):
    """Report the current failed stage, not the original download's error."""
    package = next((p for p in referenced_packages(items, packages) if p.get('state') == 'attention'), None)
    if package:
        return {'code': package.get('errorCode') or 'photo_attention',
                'stage': 'verification' if package.get('jobId') else 'photo_upload',
                'message': package.get('message') or '照片处理需要核对。'}
    for item in items:
        category = next((slot for slot in item.get('categories', {}).values()
                         if slot.get('state') == 'attention'), None)
        if category or item.get('state') == 'attention':
            problem = category or item
            code = problem.get('errorCode') or item.get('errorCode') or 'processing_attention'
            return {'code': code, 'stage': 'identity' if code in {'source_changed', 'model_changed'} else 'recognition_review',
                    'message': problem.get('message') or item.get('message') or '识别结果需要核对。'}
    return None


def identity_issue():
    return {'code': 'source_binding_conflict', 'stage': 'identity',
            'message': '识别记录的原片版本不符，保留文件等待核对。'}


def verification_issue():
    return {'code': 'result_unverified', 'stage': 'verification',
            'message': '照片导入凭证或空结果结论未核实，原片和成果保留。'}


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
        receipts = flow.linked_packages(linked)
        packages = referenced_packages(linked, receipts)
        value.update(name=row.get('name', ''), confirmedAt=operation.get('confirmedAt') or operation.get('createdAt'),
                     progress=None, packages=[{key: p.get(key) for key in PACKAGE_FIELDS if key in p} for p in packages],
                     hasResultRecord=any(item['id'] not in hidden_slices for item in linked),
                     recordHidden=(all(item['id'] in hidden_slices for item in linked) if linked
                                   else row['id'] in hidden_videos))
        timestamps = [v.get('updatedAt') or '' for v in [operation, row, version, *linked, *packages]]
        value['updatedAt'] = max(timestamps, default='')
        terminal = value.get('state') == 'complete'
        issue = value.get('issue')
        if not terminal:
            outcome = None if conflicts else verified_processing_outcome(linked, receipts, legacy=operation.get('legacy'))
            current_issue = identity_issue() if conflicts else linked_processing_issue(linked, receipts)
            if outcome:
                # A verified callback can arrive before the coordinator's next
                # tick. Report that result now; cleanup still needs its own approval.
                terminal = True
                value.update(state='complete', stage='processed', endReason=outcome,
                             message='当前版本处理已核验完成。')
            elif current_issue or terminal_slice_results(linked):
                issue = current_issue or verification_issue()
                value.update(state='attention', stage=issue['stage'], message=issue['message'], issue=issue)
            elif linked and issue and (issue.get('stage') in {'recognition_review', 'photo_upload', 'verification'}
                                       or issue.get('code') in {'invalid_video', 'media_incomplete', 'bed_alignment_uncertain'}):
                # Once the bound category resumes, its old error is history.
                issue = None
                value.update(issue=None, state='queued', stage='recognition_queue', message='继续本地识别与照片处理。')
        if not terminal and not issue and not value.get('environmentBlocker'):
            slots = [slot for item in linked for slot in item.get('categories', {}).values()]
            waiting_export = next((slot for slot in slots
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
                if states.intersection({'submitting', 'importing', 'reconciling'}):
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
                    unfinished = {slot.get('state') for slot in slots if not slot.get('packageId')}
                    if unfinished & {'ready', 'waiting_slot', 'exporting'}:
                        value.update(state='running', stage='photo_export', message='正在导出另一类照片。')
                    elif 'pending' in unfinished:
                        value.update(state='queued', stage='recognition_queue', message='等待识别剩余类别。')
                    else:
                        value.update(state='running', stage='verification', message='正在核对两类照片成果与处理结论。')
            elif any(slot.get('state') in {'ready', 'waiting_slot', 'exporting'} for slot in slots):
                value.update(state='running', stage='photo_export', message='正在导出照片。')
        if not terminal:
            wait = flow.wait_reason(row['id'])
            if wait and not value.get('issue'):
                value.update(state='waiting', stage='paused', message=wait, progress=None)
            elif not flow.workspace.online and not value.get('issue'):
                message = flow.workspace.availability.get('message') or '等待工作区恢复，已确认的下载会自动继续。'
                value.update(state='waiting', stage='waiting_disk', message=message, progress=None,
                             environmentBlocker={'code': 'disk', 'message': message})
        value.update(terminal=terminal, stageLabel=STAGES.get(value.get('stage'), value.get('stage') or '处理中'))
        if terminal:
            value.update(progress=None, issue=None, environmentBlocker=None)
        elif issue and value.get('state') == 'attention':
            value['progress'] = None
        results.append(copy.deepcopy(value))
    return results
