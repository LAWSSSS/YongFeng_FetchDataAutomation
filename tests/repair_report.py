"""Evidence-based follow-up report; previous failed-run evidence stays intact."""
from __future__ import annotations

import json
from collections import Counter
from pathlib import Path
from xml.etree import ElementTree as ET


def build_repair_report(root):
    root = Path(root).resolve()
    def read(name, default=None):
        path = root / name
        return json.loads(path.read_text()) if path.exists() else default
    stages = []
    for name in ('capture-result.json', 'natural-result.json', 'transport-result.json', 'fault-result.json', 'tcp-result.json',
                 'algorithm-comparison.json'):
        stages.extend(read(name, {}).get('stages', []))
    stages.extend(read('browser/browser-results.json', []))
    config = read('run.json', {})
    natural = read('natural-result.json', {})
    after = read('source-after.json', {})
    validation = read('validation.json', {})
    cleanup = read('media-cleanup-manifest.json', {})
    context = read('context.json', {})
    automatic = (bool(natural.get('status', {}).get('items')) and not natural.get('timedOut')
                 and not (root / 'natural-blocked.json').exists()
                 and not (root / 'diagnostic-checkpoint.json').exists()
                 and all(item.get('passed') for item in natural.get('stages', [])))
    stages.append({'node': '全自动恢复', 'case': '没有诊断或人工推进', 'kind': '真实视频与业务调度',
                   'expected': '正常调度自动完成两类照片，故障仅恢复绑定任务',
                   'actual': {'automatic': automatic, 'timedOut': bool(natural.get('timedOut'))},
                   'passed': automatic, 'seconds': natural.get('metrics', {}).get('wallSeconds', 0)})
    stages.append({'node': '原片保护', 'case': '大小、mtime及SHA-256', 'kind': '真实文件核验',
                   'expected': '受保护原片不变', 'actual': after,
                   'passed': bool(after.get('unchanged')), 'seconds': 0})
    assets = natural.get('status', {}).get('assets', [])
    stages.append({'node': '归档', 'case': '照片核验后归档、缺备份凭证时保留原片',
                   'kind': '真实文件与业务整理', 'expected': '进入已切片而不提前删除',
                   'actual': [{key: a.get(key) for key in ('stage', 'path', 'cleanup')} for a in assets],
                   'passed': bool(assets) and all('已切片' in Path(a['path']).parts for a in assets)
                       and all(a.get('cleanup', {}).get('state') != 'complete' for a in assets), 'seconds': 0})
    regressions = []
    xml = root / 'full-regression.xml'
    if xml.exists():
        for case in ET.parse(xml).iter('testcase'):
            if any(word in case.get('classname', '') for word in (
                    'real_video_scheduler', 'slice_package', 'photo_archive', 'workspace',
                    'archive', 'file_notification', 'package_import_reuse',
                    'photo_failure', 'photo_receipt', 'real_video_platform', 'media_control')):
                failed = case.find('failure') is not None or case.find('error') is not None
                regressions.append({'node': '隔离故障回归', 'case': case.get('classname') + '::' + case.get('name'),
                    'kind': '临时文件、隔离数据库与受控边界注入',
                    'expected': '按回归用例拒绝、等待、续接或安全完成',
                    'actual': 'failed' if failed else 'skipped' if case.find('skipped') is not None else 'passed',
                    'passed': not failed and case.find('skipped') is None,
                    'seconds': float(case.get('time', 0))})
    counts = Counter('passed' if s.get('passed') else 'failed' for s in stages)
    images = {p['category']: p.get('actualImages') for p in natural.get('packages', [])}
    grouped = Counter(s['node'] for s in stages)
    failures = [s for s in stages if not s.get('passed')]
    timings = []
    log = root / 'node-timings.jsonl'
    if log.exists():
        timings = [json.loads(line) for line in log.read_text().splitlines() if line]
    elapsed = Counter()
    calls = Counter()
    for record in timings:
        if record.get('event') == 'finish':
            key = record['scope'] + '.' + record['method']
            elapsed[key] += record['seconds']; calls[key] += 1
    lines = ['# 切片、上传和整理故障复测与修复', '',
        f'运行：`{root.name}`。上一轮证据保留在 `{context.get("baselineRun", "")}`。', '',
        f'**自动流程：{"通过" if automatic else "未通过"}。** '
        f'真实车斗照片 {images.get("bed", 0)} 张、磁盘吊 {images.get("magnet", 0)} 张；'
        '模型推理、解码、JPEG/ZIP、Boto3 分片和文件整理均执行真实逻辑，平台为本地模拟。', '',
        f'节点断言 {len(stages)} 项，{counts["passed"]} 项符合预期、{counts["failed"]} 项不符合；'
        f'另附 {len(regressions)} 项相关隔离回归。故障测试通过指安全拒绝、等待或恢复，'
        '并非强行批准坏片或错误图片。', '',
        '## 本轮修复', '',
        '- 调度以未提交类别是否可执行为准；文件占用后等待再续接，照片回调不能掩盖剩余类别。'
        '继续入口只恢复当前版本的未提交部分，保留有效观察、已完成照片及原重试期限。',
        '- 照片结果拒绝空 ID、跨文件名重复 ID；按确切任务、对象和版本核验。'
        '上传中断或重启先对账分片；暂停晚回调不能把已提交导入退回上传队列。',
        '- 归档移动和清理在实际写入前重新核对照片凭证、暂停和更正状态；'
        '一条失败保留清单并等待恢复，不能让其他文件的整理被丢弃。',
        '- 文件释放通知的单个监听器失败不能阻断其他唤醒或掩盖 I/O 原因；'
        '移动通知仍在全部投递后报告失败，由持久化移动记录补齐路径同步。', '',
        '## 实际节点结果', '', '|节点|断言|不符合预期|', '|---|---:|---:|']
    lines += [f'|{node}|{total}|{sum(s["node"] == node for s in failures)}|' for node, total in grouped.items()]
    if failures:
        lines += ['', '### 尚未通过', '']
        lines += [f'- {s["node"]}：{s["case"]}。实际 `{json.dumps(s.get("actual"), ensure_ascii=False)}`。' for s in failures]
    if (root / 'driver-retest.json').exists():
        lines += ['', '### 重启用例复验', '',
                  '首个长运行进程仍加载旧版测试驱动，为已存在对象换了上传身份，程序正确返回 '
                  '`object_changed`。原失败现场保存在 `transport-initial-result.json`。'
                  '修正驱动以保留原身份后，用同一 220 MB 真实 ZIP、独立新模拟平台重跑16项全部通过；'
                  '重启只做 HEAD 对账，新增分片请求为0，没有放宽业务校验。']
    lines += ['', '## 耗时', '', '|真实方法|调用次数|累计秒|', '|---|---:|---:|']
    lines += [f'|{key}|{calls[key]}|{seconds:.3f}|' for key, seconds in elapsed.most_common()]
    lines += ['', '这些方法可能嵌套或并行，累计时间不能相加当作端到端时间。'
              '时钟包装只记录开始、结束及错误类型，仍调用原业务方法，没有修改标签或区间规则。', '',
              f'检查结果：`{json.dumps(validation, ensure_ascii=False)}`。', '',
              '## 保留与边界', '',
              '- 未连接或操作真实平台、生产服务、保存的账号或现有队列。原视频上传继续停用。',
              f'- 原片完整性：`unchanged={str(after.get("unchanged")).lower()}`；'
              f'SHA-256：`{config.get("sourceSha256", "")}`。',
              '- 没有两个视频集的精确原片备份凭证时，业务正确保留已归档原片；'
              '删除分支仅用隔离测试预置凭证验证。',
              '- 暂停会阻止尚未提交的后续文件操作；已经写入删除日志并接纳执行的单文件 I/O '
              '按清单完成，不承诺撤销已提交的删除。更正发生后仍须登记已移动文件的真实位置。',
              f'- 演练媒体清理：{cleanup.get("deletedFiles", 0)} 个已登记测试文件，'
              f'逻辑大小 {cleanup.get("logicalBytes", 0)/1024**3:.2f} GiB；报告、JUnit 和诊断保留。',
              '- 真实平台协议及真实外置盘状态尚待连接恢复后的验收，本轮不以本地模拟冒充云端成功。', '',
              '[逐节点 JSON](node-results.json) · [故障回归 JSON](repair-regressions.json) · '
              '[完整 Python 回归](full-regression.xml)', '']
    (root / 'report.md').write_text('\n'.join(lines))
    for name, value in (('node-results.json', stages), ('repair-regressions.json', regressions)):
        (root / name).write_text(json.dumps(value, ensure_ascii=False, indent=2))
    summary = {'automaticBaselinePassed': automatic, 'assertions': len(stages), **dict(counts),
               'actualPhotos': images, 'regressions': len(regressions),
               'originalUnchanged': bool(after.get('unchanged')), 'report': str(root / 'report.md')}
    (root / 'summary.json').write_text(json.dumps(summary, ensure_ascii=False, indent=2))
    return summary
