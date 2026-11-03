import copy

from app.workspace_cleanup import public_operation


def test_old_partial_deletion_uses_exact_asset_receipts_without_expanding_fixed_scope():
    entries = [{'path': str(index), 'state': 'pending', 'expected': {'size': 10, 'inode': index}}
               for index in range(5)]
    request = {'id': 'fixed', 'assets': [{'id': 'asset', 'name': 'video', 'state': 'waiting',
        'manifest': entries, 'deletedFiles': 2}], 'total': 1}
    original = copy.deepcopy(request)
    journal = copy.deepcopy(entries)
    journal[0]['state'] = journal[1]['state'] = 'deleted'
    journal[2].update(state='deleted', expected={'size': 99, 'inode': 9})
    journal.append({'path': 'not-authorized', 'state': 'deleted', 'expected': {'size': 9999}})
    assets = [{'id': 'asset', 'cleanup': {'requestId': 'fixed', 'manifest': journal}}]
    result = public_operation(request, assets)
    assert result['totalFiles'] == 5 and result['deletedFiles'] == 2
    assert result['deletedBytes'] == 20 and result['remainingBytes'] == 30
    assert request == original
