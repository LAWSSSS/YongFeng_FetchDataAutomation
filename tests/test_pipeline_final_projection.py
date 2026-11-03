"""Current final approvals appear even before a slicing media group exists."""
import copy

import pytest

from tests.test_final_processing import env, payload


def final(app, row, state, stage, message):
    flow = app.state.final_processing
    result = flow.confirm(row['id'], payload(row))
    operation = flow.get(result['operationId'])
    operation.update(state=state, stage=stage, message=message)
    flow.save(operation)
    return operation


@pytest.mark.asyncio
async def test_final_download_attention_without_slice_is_visible_and_readonly(env, monkeypatch):
    app, row = env
    operation = final(app, row, 'attention', 'download', 'Range不符，请核对当前文件')
    monkeypatch.setattr(app.state.pipeline, '_groups', lambda: [])
    before = app.state.store.db.total_changes
    state = app.state.pipeline.state(status='attention')
    assert app.state.store.db.total_changes == before
    assert state['counts']['attention'] == 1
    assert state['total'] == 1
    shown = state['items'][0]
    assert shown['videoId'] == row['id'] and shown['stage'] == '下载'
    assert 'Range' in shown['message']
    assert shown['nextAction']['action'] == 'open'
    assert shown['nextAction']['canPause'] is False
    assert shown['processingEntry'] == '/?tab=videos&videoId=' + row['id']
    assert app.state.final_processing.get(operation['id']) == operation


@pytest.mark.asyncio
async def test_photo_phase_overrides_legacy_stage_without_losing_pause_binding(env, monkeypatch):
    app, row = env
    final(app, row, 'running', 'photo_import', '照片已上传，正在导入并核验结果')
    old = {'id': 'collection:' + row['id'], 'videoId': row['id'], 'generation': 0, 'name': '视频名',
           'processingEntry': '/?videoId=' + row['id'], 'items': [{'id': 'slice', 'revision': 3}],
           'nextAction': {'kind': 'automatic', 'action': 'none', 'stage': '平台切片处理中',
                         'reason': '旧提示', 'canContinue': False, 'selections': [], 'packages': [],
                         'versionSelections': [{'id': 'slice', 'expectedRevision': 3}]}}
    monkeypatch.setattr(app.state.pipeline, '_groups', lambda: [copy.deepcopy(old)])
    state = app.state.pipeline.state()
    shown = state['items'][0]
    assert state['counts']['processing'] == 1 and len(state['items']) == 1
    assert shown['stage'] == '照片导入' and shown['message'].startswith('照片已上传')
    assert shown['nextAction']['canPause'] is True
    assert shown['nextAction']['versionSelections'] == old['nextAction']['versionSelections']
    assert shown['processingEntry'] == old['processingEntry']
    assert old['nextAction']['stage'] == '平台切片处理中'


@pytest.mark.asyncio
async def test_final_cleanup_is_terminal_but_old_generation_cannot_override_new_work(env, monkeypatch):
    app, row = env
    operation = final(app, row, 'complete', 'awaiting_cleanup', '等待手动清理')
    state = app.state.pipeline.state()
    assert state['counts'] == {'processing': 0, 'waiting': 0, 'attention': 0, 'complete': 1}
    assert state['items'][0]['stage'] == '待清理'
    app.state.media_control.save({'id': 'collection:' + row['id'], 'generation': 1, 'videoId': row['id']})
    old = {'id': 'collection:' + row['id'], 'videoId': row['id'], 'generation': 1,
           'name': '新代次', 'items': [{'id': 'new-slice'}], 'nextAction': {
               'kind': 'automatic', 'action': 'none', 'stage': '识别', 'reason': '新代次正在识别',
               'canContinue': False, 'selections': [], 'packages': []}}
    monkeypatch.setattr(app.state.pipeline, '_groups', lambda: [copy.deepcopy(old)])
    state = app.state.pipeline.state()
    assert state['counts']['processing'] == 1 and state['counts']['complete'] == 0
    assert state['items'][0]['message'] == '新代次正在识别'
    assert app.state.final_processing.get(operation['id'])['state'] == 'complete'
