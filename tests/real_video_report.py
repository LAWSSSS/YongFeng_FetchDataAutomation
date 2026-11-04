"""Rehearsal evidence assembly and explicit, run-owned media cleanup only."""
from __future__ import annotations

import hashlib
import json
import os
from collections import Counter
from pathlib import Path


def read(root, name, default=None):
    path = root / name
    return json.loads(path.read_text()) if path.exists() else default


def save(path, value):
    temporary = path.with_suffix(path.suffix + '.tmp')
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2))
    temporary.replace(path)


def edge(path):
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        digest.update(stream.read(65536))
        stream.seek(max(0, path.stat().st_size - 65536))
        digest.update(stream.read(65536))
    return digest.hexdigest()


def register_outputs(root):
    """Collect only paths returned by the test writers, never media globbing."""
    root = Path(root).resolve()
    paths = {str(root / 'source.mp4')}
    capture = read(root, 'capture-result.json', {})
    if capture.get('filePath'):
        paths.add(capture['filePath'])
    for clip in capture.get('metrics', {}).get('clips', []):
        paths.add(str(root / 'capture' / 'simulated-capture' /
                      f'clip-{clip["offsetSeconds"]}-{clip["durationSeconds"]}.mp4'))
    natural = read(root, 'natural-result.json', {})
    for asset in natural.get('status', {}).get('assets', []):
        paths.add(asset['path'])
    for package in natural.get('packages', []):
        paths.update(filter(None, (package.get('path'), package.get('videoPath'))))
        if package.get('imageDir'):
            paths.update(str(Path(package['imageDir']) / name) for name in package.get('images', []))
    # Discover registry documents only; they do not authorize arbitrary media
    # neighbors. Service instances write their exact object/part paths.
    for registry in root.rglob('owned-media.json'):
        if registry == root / 'owned-media.json':
            continue
        paths.update(read(registry.parent, registry.name, {}).get('paths', []))
    paths.update(str(root / name) for name in read(root, 'owned-media.json', {}).get('paths', []))
    relative = []
    for value in paths:
        path = Path(value)
        if not path.is_absolute() or not path.resolve().is_relative_to(root):
            raise ValueError('Writer registered media outside the isolated root.')
        relative.append(str(path.relative_to(root)))
    result = {'paths': sorted(set(relative))}
    save(root / 'owned-media.json', result)
    return result


def cleanup_media(root):
    """No recursive delete: journal known test media, recheck, then unlink."""
    root = Path(root).resolve(strict=True)
    config = read(root, 'run.json')
    if not config or Path(config['root']).resolve() != root:
        raise ValueError('Run ownership marker required.')
    if Path(config['source']).resolve().is_relative_to(root):
        raise ValueError('Protected original must be outside the disposable run root.')
    prior = read(root, 'media-cleanup-manifest.json')
    if prior and prior.get('complete'):
        return prior
    entries = prior['entries'] if prior else []
    if not prior:
        owned = read(root, 'owned-media.json')
        if not owned or not isinstance(owned.get('paths'), list):
            raise ValueError('An explicit test artifact registry is required; directory scanning cannot authorize deletion.')
        for relative_name in sorted(set(owned['paths'])):
            path = root / relative_name
            if not path.exists():
                continue
            if path.is_symlink() or not path.is_file() or not path.resolve().is_relative_to(root):
                raise ValueError('Run artifact escaped its owned root.')
            relative = path.relative_to(root)
            info = path.stat()
            entries.append({'path': str(relative), 'size': info.st_size,
                            'modifiedNs': info.st_mtime_ns, 'inode': info.st_ino, 'device': info.st_dev,
                            'edgeHash': edge(path), 'state': 'planned'})
        save(root / 'media-cleanup-manifest.json', {'complete': False, 'entries': entries})
    with (root / 'media-cleanup-journal.jsonl').open('a') as journal:
        for entry in entries:
            path = root / entry['path']
            if entry.get('state') == 'deleted':
                continue
            if path.is_symlink() or not path.resolve().is_relative_to(root):
                entry['state'] = 'retained-conflict'
            elif not path.exists():
                entry['state'] = 'deleted'
            elif (path.stat().st_size != entry['size'] or edge(path) != entry['edgeHash'] or any(
                    key in entry and entry[key] != getattr(path.stat(), field)
                    for key, field in (('modifiedNs', 'st_mtime_ns'), ('inode', 'st_ino'), ('device', 'st_dev')))):
                entry['state'] = 'retained-version-changed'
            else:
                path.unlink()
                entry['state'] = 'deleted'
            journal.write(json.dumps(entry, ensure_ascii=False) + '\n')
            journal.flush()
            os.fsync(journal.fileno())
    result = {'complete': all(row['state'] == 'deleted' for row in entries), 'entries': entries,
              'deletedFiles': sum(row['state'] == 'deleted' for row in entries),
              'logicalBytes': sum(row['size'] for row in entries if row['state'] == 'deleted')}
    save(root / 'media-cleanup-manifest.json', result)
    return result


def row(node, case, passed, actual, kind='隔离状态验证', expected='', seconds=0):
    return dict(node=node, case=case, kind=kind, passed=bool(passed), actual=actual,
                expected=expected, seconds=round(seconds, 3))


def build_report(root):
    root = Path(root).resolve()
    if read(root, 'context.json', {}).get('repairRun') or not (root / 'diagnostic-checkpoint.json').exists():
        from tests.repair_report import build_repair_report
        return build_repair_report(root)
    config = read(root, 'run.json')
    natural = read(root, 'natural-result.json', {})
    stages = []
    for name in ('capture-result.json', 'natural-result.json', 'transport-result.json', 'fault-result.json', 'tcp-result.json'):
        rows = read(root, name, {}).get('stages', [])
        for item in rows:
            if name == 'natural-result.json' and item['node'] == '持续登记与最终状态' and (root / 'diagnostic-checkpoint.json').exists():
                item = {**item, 'case': '后端诊断推进后的终态',
                        'expected': '仅证明检查点后的真实节点完成，不代表自动基线成功'}
            stages.append(item)
    stages.extend(read(root, 'browser/browser-results.json', []))
    profile = read(root, 'profile-result.json')
    if profile:
        stages.append(row('共享解码与真实模型计时', '4位置双模型', profile.get('passed'), profile,
                          kind='真实视频与模型微基准'))
    blocked = read(root, 'natural-blocked.json')
    if blocked:
        stages.append(row('自动调度', '类别等待后的继续执行', False,
            {'state': blocked['items'][0]['state'], 'bed': blocked['items'][0]['categories']['bed']['state'],
             'magnet': blocked['items'][0]['categories']['magnet']['state'],
             'observedSeconds': blocked.get('wallSeconds')},
            expected='到期pending类别应自动重新入槽，不能永久等待'))
        stages.append(row('人工恢复入口', '继续处理到期pending类别', False,
            {'error': 'Conflict: 切片记录已变化，请刷新后重试。',
             'reproduction': 'tests/test_real_video_scheduler_probe.py'},
            kind='真实状态与隔离最小复现', expected='有效剩余类别可继续，不能无可用入口'))
    after = read(root, 'source-after.json', {})
    stages.append(row('原片保护', '前后SHA-256及身份', after.get('unchanged'), after,
                      kind='真实完整文件校验', expected='大小、修改时间、inode、设备和SHA-256不变'))
    assets = natural.get('status', {}).get('assets', [])
    archive_ok = bool(assets) and all('已切片' in Path(a['path']).parts for a in assets)
    stages.append(row('归档位置', '两个类别核验后原片进入已切片', archive_ok,
                      {'paths': [a['path'] for a in assets]}, kind='真实文件移动',
                      expected='单份原片进入工作区已切片，缺原片备份凭证时保留'))
    save(root / 'node-results.json', stages)
    counts = Counter('passed' if item.get('passed') else 'failed' for item in stages)
    validation = read(root, 'validation.json', {})
    cleanup = read(root, 'media-cleanup-manifest.json', {})
    images = {p['category']: p.get('actualImages') for p in natural.get('packages', [])}
    groups = Counter(item['node'] for item in stages)
    failures = Counter(item['node'] for item in stages if not item.get('passed'))
    initial = read(root, 'natural-initial-progress.json', [])
    latest = read(root, 'natural-progress.json', [])
    metrics = natural.get('metrics', {})
    lines = [
        '# 真实视频全流程模拟验收', '',
        f'运行编号：`{root.name}`。本轮只测试与分析，未修改业务算法或生产服务。', '',
        '**结论：自动基线未通过。** 类别调度存在可复现阻塞，网页继续处理无法恢复；'
        '保留现场后，通过后端诊断直接执行真实业务节点，完成两个类别的照片导入核验。'
        '另发现重复图片 ID 可被错误接受。不能将这次结果写为“全自动全部通过”。', '',
        f'- 样本：鲁NH1113，4499.669 秒，3840×2160 HEVC，{config["sourceStat"]["size"]:,} 字节。',
        f'- 真实成果：车斗 {images.get("bed", 0)} 张、磁盘吊 {images.get("magnet", 0)} 张，'
        '实际 JPEG → ZIP → Boto3 子进程 → 本地 S3 → ZIP 解析 → 图片逐张核验。',
        '- 两个类别均有真实命中；未更改预测标签、置信度、阈值或区间算法。'
        '传输故障案例使用自然生成的 ZIP，额外导入记录仅存在独立模拟数据库。',
        '- 补充真实 loopback TCP 下载：3,356,729,799 字节，16 MiB 处断 socket，'
        '第 2 次准确 Range 续传；真实解码与全量 SHA 一致。下载 8.93 秒、总 12.11 秒。'
        '主采集链的其他 HTTP 接口为协议适配器模拟，两者分别记录。',
        '- 自动场景缺少两个视频集的精确原片备份凭证，程序正确保留并归档测试原片。'
        '清理成功分支使用独立契约中预置的精确模拟凭证，不上传原视频。',
        f'- 节点断言 {len(stages)} 项：{counts["passed"]} 项符合预期，'
        f'{counts["failed"]} 项暴露缺陷。pytest 通过包含“缺陷被成功复现”，并不推翻业务失败结论。',
        f'- 受保护原片前后校验：{"一致" if after.get("unchanged") else "未通过"}；'
        '日常队列、生产登录、真实采集和照片平台均未操作。', '',
        '## 关键缺陷与复现', '',
        '1. **类别饥饿（优先修复）**：工作区移动占用文件时，车斗等待 0.5 秒，'
        '磁盘吊先运行。照片阶段将整条状态改为 `platform_processing`，调度器只领取'
        '`item.state == pending`，因此剩余车斗不会再启动。`natural-blocked.json` 保留真实现场；'
        '`tests/test_real_video_scheduler_probe.py` 用 ready/importing/complete 三种状态复现。',
        '2. **恢复入口失效**：同一现场的“继续处理”只接受 `attention`，对当前状态返回冲突。'
        '诊断使用 `process_with_recovery` 原样执行剩余类别，复用另一类别成果，未伪造 attention。'
        '建议调度、计数和下一步按钮共用类别级可执行投影。',
        '3. **图片身份核验不完整（优先修复）**：101 个不同文件名共用一个图片 ID 时，'
        '当前核验仍可标 complete。传输测试的 `duplicate_id` 断言失败。建议要求期望数量、'
        '逐名唯一、全局 ID 唯一、任务归属及版本同时成立；失败不得归档或删除。', '',
        '## 节点结果', '',
        '|节点|断言数|不符合预期|', '|---|---:|---:|',
    ]
    lines += [f'|{name}|{count}|{failures[name]}|' for name, count in groups.items()]
    lines += ['', '完整的逐案例预期、实际、耗时、任务关联与恢复信息见 '
              '[node-results.json](node-results.json)。原始 JSON 保留实际阶段和数量，没有使用虚构百分比。', '',
              '## 实测耗时与优化顺序', '',
              '- **先修正确性，再优化速度**：类别饥饿及无效继续入口会无限延长等待；'
              '应让到期 pending 类别优先进入调度，文件占用使用等待状态，不能误报 worker_failed。',
              '- **导出进度**：首轮磁盘吊采样约 85.5 秒完成，约 242.5 秒才登记照片包，'
              '中间约 157 秒缺少真实导出进度。车斗后端诊断识别 33.4 秒，导出至上传约 42.8 秒。'
              '建议持久化导出阶段、位置、已成功/跳过数量；保持现有选择算法。',
              '- **重复模型成本**：4 位置真实微基准总约 4.10 秒，其中模型加载 0.92 秒、'
              '解码 0.98 秒、车斗推理 0.22 秒、磁盘吊推理 1.69 秒。4 次解码供 8 次分类，'
              '共享已生效。优先复用同视频版本/同位置/同模型配置的判断、复用进程内模型；'
              '不减少必要采样或改变阈值。微基准不是全片端到端时间；本次全流程因类别等待被分成两轮，'
              '同一批位置再次解码，不能称整条仅解码一次。修复调度后应让两个可运行类别合并领取。',
              f'- **数据库读取**：诊断续跑 {metrics.get("wallSeconds", 0):.1f} 秒记录 '
              f'{metrics.get("sqlStatements", {}).get("SELECT", 0):,} 次 SELECT、'
              f'{metrics.get("sqlStatements", {}).get("COMMIT", 0):,} 次提交。'
              '建议在单个调度轮次共享身份/有效任务/文件映射快照，观察批量写入，减少唤醒后的重复全表读取。'
              '写入安全、版本栅栏和清理凭证不能省略。',
              '- **导入等待**：模拟平台立即完成导入，但两类从 importing 到 complete 都约 60 秒，'
              '来自两轮 30 秒核对。可在已知任务状态变化后立即唤醒定向核验，并保留逐张、版本和唯一性检查。'
              '真实网络收益需要恢复连接后再测。',
              '- **交互**：隔离桌面列表 62.6 ms、批量登记反馈 195.3 ms；390px 无横向溢出，'
              '折叠历史 4.4 秒窗口没有详情轮询。建议把可自动恢复项与确需人工判断项分开统计。',
              '- **内存**：4 位置独立 Python 3.11 推理进程峰值约 650.4 MiB，'
              'FFmpeg 子进程历史峰值约 111.6 MiB。这些不是同时峰值总和；多进程上限应结合可用内存。', '',
              '## 范围与限制', '',
              '- 采集、下载、导入协议和故障条件在本地模拟；不是生产平台兼容性或真实云端成果验收。'
              '真实服务恢复后仍需只读检查协议与授权。',
              '- 成功采集采用无重编码剪切；三版 FFprobe 实测 3600.014、3600.454、3600.853 秒。'
              '存在 GOP/容器级亚秒边界差，不能称为逐帧精确的 3600 秒采集。未修改算法补偿该差异。',
              '- 断网、磁盘不足、单帧/模型/工具超时、提交与完成响应丢失为受控故障注入；'
              '未破坏原片、实际拔盘、耗尽系统磁盘或停止日常服务。',
              '- 页面控制 POST 在浏览器路由中模拟；真实后台登记与暂停行为由隔离契约另行验证。'
              '截图使用隔离页面数据，不是日常77条记录的交付验收。',
              '- 耗时来自当前电脑单次实跑，存在后台测试与缓存影响；不能据此承诺固定吞吐。', '',
              '## 检查、原片与测试产物', '',
              f'- 检查结果：`{json.dumps(validation, ensure_ascii=False)}`。',
              f'- 原片 SHA-256：`{config["sourceSha256"]}`；最终校验 '
              f'`unchanged={str(after.get("unchanged")).lower()}`。',
              f'- 测试媒体清单清理：{cleanup.get("deletedFiles", 0)} 个文件，'
              f'逻辑大小约 {cleanup.get("logicalBytes", 0) / 1024**3:.2f} GiB。'
              'APFS 克隆共享块，逻辑大小不等于实际释放空间。仅删除本运行目录中的测试产物，'
              '报告、JUnit、脱敏检查点与浏览器截图保留。',
              '- `media-cleanup-manifest.json` / `media-cleanup-journal.jsonl` 保存逐文件清单；'
              '业务先按安全策略保留测试原片，再由演练清理明确删除测试副本，两者分别记录。', '',
              '复现入口：`scripts/run_real_video_e2e.py --help`。'
              '清理后请使用新的运行目录进行下一次完整媒体演练。', '',
              '## 逐案例索引', '',
              '|节点 / 场景|类型|符合预期|墙钟秒|', '|---|---|---|---:|']
    def cell(value):
        return str(value).replace('|', '\\|').replace('\n', ' ')
    lines += [f'|{cell(item["node"])} / {cell(item["case"])}|{cell(item.get("kind", ""))}|'
              f'{"是" if item.get("passed") else "**否**"}|{float(item.get("seconds", 0)):.3f}|'
              for item in stages]
    (root / 'report.md').write_text('\n'.join(lines) + '\n')
    summary = {'assertions': len(stages), **dict(counts), 'actualPhotos': images,
               'automaticBaselinePassed': False if blocked else all(row.get('passed') for row in stages),
               'originalUnchanged': after.get('unchanged'), 'report': str(root / 'report.md')}
    save(root / 'summary.json', summary)
    return summary
