"""A rejected fixed cleanup scope cannot become a new implicit cleanup request."""
import copy
from pathlib import Path

import pytest

from app.workspace_cleanup import authorization, update_operations
from tests.test_manual_completed_cleanup import prepared
from tests.test_slicing_performance import close_app
from tests.test_cleanup_remount import versions_old


@pytest.mark.asyncio
@pytest.mark.parametrize('old_state', ['attention', 'pending_manual', 'waiting'])
async def test_rejected_missing_zip_stays_terminal_during_reconcile_and_direct_cleanup(tmp_path, monkeypatch, old_state):
    app, w, asset = await prepared(tmp_path)
    try:
        await w.request_cleanup_completed(w.config['revision'], 'fixed-scope')
        request = w.get('requests', 'fixed-scope')
        saved = request['assets'][0]
        original_scope = copy.deepcopy(saved['manifest'])
        saved.update(state='attention' if old_state == 'attention' else 'waiting',
                     reasonCode='remount_unverified', message='处理完成，等待点击清理已结束文件。')
        # Another real request member may still be running. Its state must not
        # let the organizer revive this independently rejected member.
        request['assets'].append({'id': 'other-member', 'state': 'waiting', 'manifest': [], 'name': 'other'})
        request.update(state='waiting', total=2)
        w.save('requests', request)
        asset['cleanup'] = {'requestId': request['id'], 'state': old_state, 'reasonCode': 'remount_unverified',
                            'message': saved['message'], 'manifest': copy.deepcopy(original_scope)}
        w.save('assets', asset)
        archive = next(row['path'] for row in original_scope if row['kind'] == 'archive')
        Path(archive).unlink()
        actual_cleanup = w.cleanup

        async def no_repeat(*args, **kwargs):
            pytest.fail('A terminal cleanup must not revalidate, move or delete its fixed files automatically.')

        monkeypatch.setattr(w, 'cleanup', no_repeat)
        monkeypatch.setattr(w, 'refresh_archive_photos', no_repeat)
        for _ in range(2):
            await w.reconcile()
        current = w.get('assets', asset['id'])
        assert current['cleanup']['state'] == 'attention'
        assert current['cleanup']['reasonCode'] == 'remount_unverified'
        assert '待核对' in current['cleanup']['message']
        assert current['cleanup']['manifest'] == original_scope
        assert authorization(w, current) == (None, None)
        await actual_cleanup(current)  # Compatibility/direct endpoint cannot revive it either.
        operation = w.cleanup_operation('fixed-scope')
        assert operation['failed'] == 1 and operation['waiting'] == 1
        assert operation['assets'][0]['state'] == 'attention'
        assert operation['deletedFiles'] == 0
        assert w.cleanup_summary()['eligibleCount'] == 0
        assert (await w.request_cleanup_completed(w.config['revision'], 'new-click'))['total'] == 0
        assert Path(asset['path']).exists()
        assert w.get('requests', 'fixed-scope')['assets'][0]['manifest'] == original_scope
    finally:
        await close_app(app)


@pytest.mark.asyncio
async def test_registered_rejection_is_not_overwritten_by_new_eligibility_failure(tmp_path):
    app, w, asset = await prepared(tmp_path)
    try:
        await w.request_cleanup_completed(w.config['revision'], 'fixed-scope')
        request = w.get('requests', 'fixed-scope')
        saved = request['assets'][0]
        saved.update(state='attention', reasonCode='receipt_changed', message='原照片凭证不符，保留待核对。')
        request['state'] = 'attention'
        w.save('requests', request)
        asset['cleanup'] = {'requestId': request['id'], 'state': 'attention', 'reasonCode': 'receipt_changed',
                            'message': saved['message'], 'manifest': copy.deepcopy(saved['manifest'])}
        w.save('assets', asset)
        package = w.packages(asset)[0]
        package['verifiedJobId'] = 'wrong-task'
        w.slicing.packages.store.save('items', package)
        await w.reconcile()
        await w.cleanup(w.get('assets', asset['id']))
        current = w.get('assets', asset['id'])
        assert current['cleanup']['state'] == 'attention'
        assert current['cleanup']['message'] == saved['message']
        assert w.cleanup_operation('fixed-scope')['state'] == 'attention'
        changes = w.db.total_changes
        update_operations(w)
        assert w.db.total_changes == changes, 'Stable terminal operations should not be rewritten on every poll.'
        assert Path(asset['path']).is_file()
    finally:
        await close_app(app)


@pytest.mark.asyncio
async def test_polluted_device_only_failure_reenters_original_content_check_without_new_consent(tmp_path, monkeypatch):
    app, w, asset = await prepared(tmp_path)
    try:
        await w.request_cleanup_completed(w.config['revision'], 'original-remount')
        request = w.get('requests', 'original-remount')
        request['assets'][0] = versions_old(request['assets'][0])
        saved = request['assets'][0]
        saved.update(state='waiting', reasonCode='receipt_changed')
        request['state'] = 'waiting'
        w.save('requests', request)
        asset['cleanup'] = {'requestId': request['id'], 'state': 'pending_manual', 'reasonCode': 'receipt_changed',
                            'message': '处理完成，等待点击清理已结束文件。', 'manifest': copy.deepcopy(saved['manifest'])}
        w.save('assets', asset)
        scope = copy.deepcopy(saved['manifest'])
        attempted = []

        async def validate_original_scope(current):
            # This test checks scheduling only. Actual ZIP/byte validation and
            # restart deletion remain covered by test_cleanup_remount.
            owned, bound = authorization(w, current)
            assert owned['id'] == request['id'] and bound['manifest'] == scope
            attempted.append(current['id'])

        monkeypatch.setattr(w, 'cleanup', validate_original_scope)
        await w.reconcile()
        assert not attempted
        assert w.get('assets', asset['id'])['cleanup']['state'] == 'attention'
        await w.reconcile()
        assert attempted == [asset['id']]
        assert w.get('requests', request['id'])['assets'][0]['manifest'] == scope
        assert w.cleanup_summary()['eligibleCount'] == 0
        assert Path(asset['path']).is_file()
    finally:
        await close_app(app)
