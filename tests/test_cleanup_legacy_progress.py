import copy

import pytest

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


def test_archive_count_uses_only_matching_fixed_scope_receipts():
    entries = [{'path': 'first.zip', 'kind': 'archive', 'state': 'planned', 'expected': {'size': 10}},
               {'path': 'second.zip', 'kind': 'archive', 'state': 'planned', 'expected': {'size': 20}},
               {'path': 'photo.jpg', 'kind': 'image', 'state': 'planned', 'expected': {'size': 3}}]
    request = {'id': 'fixed', 'assets': [{'id': 'asset', 'state': 'waiting', 'manifest': entries}], 'total': 1}
    journal = copy.deepcopy(entries)
    journal[0]['state'] = journal[1]['state'] = 'deleted'
    journal[1]['expected'] = {'size': 99}
    journal.append({'path': 'outside.zip', 'kind': 'archive', 'state': 'deleted', 'expected': {'size': 1}})
    result = public_operation(request, [{'id': 'asset', 'cleanup': {'requestId': 'fixed', 'manifest': journal}}])
    assert result['deletedArchives'] == 1
    assert result['deletedFiles'] == 1


def test_legacy_partial_progress_without_archive_receipts_does_not_guess():
    request = {'id': 'fixed', 'assets': [{'id': 'asset', 'state': 'waiting', 'deletedFiles': 1,
               'manifest': [{'path': 'first.zip', 'kind': 'archive', 'state': 'planned', 'expected': {'size': 10}}]}]}
    assert 'deletedArchives' not in public_operation(request)


@pytest.mark.parametrize('assets', [[], [{'id': 'asset', 'cleanup': {'requestId': 'later'}}]])
def test_saved_manifest_retains_archive_count_without_matching_asset_journal(assets):
    request = {'id': 'fixed', 'assets': [{'id': 'asset', 'state': 'waiting', 'manifest': [
        {'path': 'old.zip', 'kind': 'archive', 'state': 'deleted', 'expected': {'size': 10}},
        {'path': 'photo.jpg', 'kind': 'image', 'state': 'planned', 'expected': {'size': 3}}]}]}
    original = copy.deepcopy(request)
    result = public_operation(request, assets)
    assert result['deletedFiles'] == 1
    assert result['deletedArchives'] == 1
    assert request == original
