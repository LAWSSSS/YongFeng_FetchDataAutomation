"""Live, scoped progress. Reading this snapshot never probes media or platforms."""
import json

from .domain import now_iso
from .workspace_files import compatible
from .photo_completion import photo_results_verified


LABELS = {'queued': '排队', 'uploading': '上传中', 'uploaded': '等待导入', 'importing': '平台导入中',
          'submitting': '提交导入', 'waiting_connection': '等待网络', 'waiting_auth': '等待连接平台',
          'waiting_disk': '等待磁盘', 'retry_wait': '等待重试', 'cleanup': '恢复上传断点',
          'waiting_local': '稍后重试视频检查',
          'analyzing': '识别中', 'pending': '等待识别', 'attention': '需处理', 'review': '需确认'}
TERMINAL = {'complete', 'no_target', 'uncuttable'}


def snapshot(manager, request, assets, *, slices=None, packages=None):
    scope = {row['id'] for row in (request or {}).get('pipelineSources', [])}
    selected = [asset for asset in assets if (asset['id'] in scope if scope else
                not manager.is_preserved(asset) and asset.get('cleanup', {}).get('state') != 'complete')]
    paths = {asset['path']: asset for asset in selected}
    def path_rows(table):
        if not paths:
            return []
        placeholders = ','.join('?' for _ in paths)
        return [json.loads(row[0]) for row in manager.db.execute(
            f"SELECT payload FROM {table} WHERE json_extract(payload, '$.path') IN ({placeholders})", tuple(paths))]
    slices = [item for item in (path_rows('simple_slicing_items') if slices is None else slices)
              if item.get('path') in paths and not item.get('superseded')
              and compatible(paths[item['path']]['fileVersion'], item.get('fileVersion') or {})]
    control = getattr(manager.slicing, 'control', None)
    if control:
        slices = control.scoped_effective_items(slices)
    slice_ids = {item['id'] for item in slices}
    if packages is None:
        placeholders = ','.join('?' for _ in slice_ids)
        packages = [json.loads(row[0]) for row in manager.db.execute(
            f"SELECT payload FROM simple_slice_package_items WHERE json_extract(payload, '$.itemId') IN ({placeholders})",
            tuple(slice_ids))] if slice_ids else []
    packages = [item for item in packages if item.get('itemId') in slice_ids and not item.get('superseded')]
    tasks, targets = [], []
    for category, label in (('bed', '车斗照片'), ('magnet', '磁盘吊照片')):
        rows = [item for item in packages if item.get('category') == category]
        target = dict(id=category, label=label, total=len(rows), complete=0, uploaded=0, importing=0,
                      active=0, queued=0, attention=0, bytes=0,
                      totalBytes=sum(int(item.get('size') or 0) for item in rows), speed=0)
        for item in rows:
            state = item.get('state') or 'queued'
            runtime = manager.slicing.packages.runtime.get(item['id']) or {}
            size = int(item.get('size') or 0)
            count = size if state in {'uploaded', 'importing', 'complete'} else min(size, max(
                int(item.get('bytes') or 0), int(runtime.get('bytes') or 0)))
            speed = max(0, runtime.get('speed') or 0) if state == 'uploading' else 0
            target['bytes'] += count
            target['speed'] += speed
            key = {'uploading': 'active', 'complete': 'complete', 'uploaded': 'uploaded',
                   'importing': 'importing', 'attention': 'attention', 'review': 'attention'}.get(state, 'queued')
            target[key] += 1
        target['percent'] = min(100, round(100 * target['bytes'] / target['totalBytes'], 1)) if target['totalBytes'] else 0
        targets.append(target)
    done = active = queued = attention = samples_done = samples_total = 0
    for asset in selected:
        if asset.get('cleanup', {}).get('state') == 'complete':
            # Its retained completion journal is authoritative even when state()
            # deliberately omits this source's already removed media inventory.
            done += 1
            continue
        related = [item for item in slices if item['path'] == asset['path']]
        groups = {'bed': [], 'magnet': []}
        for item in related:
            for name, category in item.get('categories', {}).items():
                if name not in groups or category.get('state') in {None, 'not_applicable'}:
                    continue
                state = category['state']
                if item.get('partialRecovery') and (category.get('errorCode') or item.get('errorCode')) in {'no_decodable_samples', 'no_cuttable_fragments'}:
                    state = 'uncuttable'
                groups[name].append(state)
        outcomes = {name: states[0] if states and len(set(states)) == 1 else 'pending' for name, states in groups.items()}
        terminal = all(state in TERMINAL for state in outcomes.values())
        bound_ids = {item['id'] for item in related}
        bound_packages = [p for p in packages if p.get('itemId') in bound_ids]
        verified = terminal and photo_results_verified(related, bound_packages)
        done += verified
        working = [item for item in related if item.get('state') == 'analyzing']
        problem = [item for item in related if item.get('state') == 'attention']
        if terminal and not verified and not any(p.get('state') in {'queued', 'uploading', 'uploaded', 'submitting', 'importing'} for p in bound_packages):
            problem = problem or related[:1]
        active += bool(working)
        attention += bool(problem)
        queued += not working and not problem and not verified
        if working:
            item = max(working, key=lambda row: row.get('analysisTotal') or 0)
            count, total = int(item.get('analysisDone') or 0), int(item.get('analysisTotal') or 0)
            samples_done += count
            samples_total += total
            tasks.insert(0, dict(id='analysis-' + item['id'], name=item['name'], target='本地识别',
                stage={'initial': '识别', 'refine': '细化识别', 'checking_media': '核对视频尾部',
                       'loading_model': '加载模型', 'sampling': '抽帧识别'}.get(item.get('analysisStage'), '准备识别'),
                done=count, total=total, bytes=0, totalBytes=0, speed=0, reason=item.get('processingBlockedReason', '')))
    for package in packages:
        state = package.get('state')
        if state in {'complete', 'attention', 'dismissed'}:
            continue
        runtime = manager.slicing.packages.runtime.get(package['id']) or {}
        tasks.insert(0, dict(id='package-' + package['id'], name=package.get('videoName') or package.get('name') or '照片包',
            target='车斗照片' if package.get('category') == 'bed' else '磁盘吊照片',
            stage=LABELS.get(state, '照片处理中'), done=package.get('actualImages') or 0,
            total=package.get('imageCount') or 0, bytes=max(package.get('bytes') or 0, runtime.get('bytes') or 0),
            totalBytes=package.get('size') or 0, speed=runtime.get('speed') or 0, reason=''))
    return dict(updatedAt=now_iso(), uploads=dict(targets=targets, paused=manager.slicing.packages.paused,
        **{key: sum(t[key] for t in targets) for key in
           ('total', 'complete', 'uploaded', 'importing', 'active', 'queued', 'attention', 'bytes', 'totalBytes', 'speed')}),
        slicing=dict(total=len(selected), complete=done, active=active, queued=queued, attention=attention,
                     paused=(manager.slicing.run or {}).get('state') == 'paused' and not manager.slicing.store.settings.get('enabled'),
                     samplesDone=samples_done, samplesTotal=samples_total,
                     packagesActive=sum(p.get('state') not in {'complete', 'attention', 'dismissed', 'superseded', 'review'} for p in packages),
                     imagesComplete=sum(p.get('actualImages') or 0 for p in packages if p.get('state') == 'complete')),
        files=dict(total=len(selected), organized=sum((a.get('organization') or {}).get('state') == 'complete' for a in selected),
                   cleaned=sum((a.get('cleanup') or {}).get('state') == 'complete' for a in selected),
                   waiting=sum(a.get('status') == 'waiting' for a in selected)),
        activeTasks=tasks[:12], activeTaskCount=len(tasks))
