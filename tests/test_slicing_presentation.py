import copy
import json

from app.presentation import slicing_view


def source(index, dataset='27912', **extra):
    category = 'bed' if dataset == '27912' else 'magnet'
    return dict(id=f'{category}-{index}', mediaId=f'media-{index}', mediaGeneration=2,
                collectionRevision=1, name=f'video-{index:03d}.mp4', path=f'/managed/video-{index}.mp4',
                sourceDatasetId=dataset, sourceVideoId=f'{dataset}-{index}', state='complete', revision=3,
                submittedAt='2026-10-31T18:00:00Z', fileVersion={'size': 1000, 'edgeHash': 'private-proof'},
                categories={category: {'state': 'complete', 'packageId': f'pack-{category}-{index}',
                    'intervals': [{'start': 5, 'end': 900}], 'observations': 200, 'actualImages': 150}}, **extra)


def payload():
    items = [source(index, dataset) for index in range(25) for dataset in ('27912', '27944')]
    return {'settings': {'targetImages': 150}, 'settingsRevision': 9, 'counts': {'complete': 50},
            'run': {'id': 'run', 'state': 'complete', 'items': ['secret-history-id'], 'preSubmitImageIds': ['hidden']},
            'items': items, 'packages': {'targets': [{'id': 'p', 'name': '照片', 'queued': 0, 'items': ['large']}],
                'items': [{'id': f'pack-{row["id"]}', 'itemId': row['id'], 'state': 'complete'} for row in items]},
            'archive': {'counts': {'complete': 25}, 'items': [{'id': str(index), 'path': f'/managed/video-{index}.mp4'}
                       for index in range(25)]},
            'submissionReceipts': [{'id': 'receipt', 'revision': 2, 'submittedAt': '2026-11-01 03:00:00',
                                    'itemIds': [row['id'] for row in items]}]}


def test_initial_summary_omits_detail_and_preserves_beijing_date_grouping():
    value = payload(); before = copy.deepcopy(value)
    summary = slicing_view(value)
    assert summary['recordDays'][0] == {'day': '2026-11-01', 'total': 25, 'itemCount': 50,
                                      'receiptCount': 1, 'revision': summary['recordDays'][0]['revision']}
    assert summary['run'] == {'id': 'run', 'state': 'complete'}
    assert summary['packages']['items'] == [] and summary['archive']['items'] == []
    row = summary['items'][0]
    assert 'fileVersion' not in row and 'intervals' not in row['categories']['bed']
    assert 'observations' not in row['categories']['bed']
    assert value == before
    summary['settings']['targetImages'] = 100
    assert value['settings']['targetImages'] == 150


def test_pages_keep_both_categories_together_and_scope_never_changes_with_page():
    value = payload()
    first = slicing_view(value, day='2026-11-01')
    second = slicing_view(value, day='2026-11-01', page=2)
    assert len(first['items']) == 40 and len(second['items']) == 10
    assert len(first['archive']['items']) == 20
    assert len(second['packages']['items']) == 10
    assert first['recordPagination']['scopeRevision'] == second['recordPagination']['scopeRevision']
    assert set(row['mediaId'] for row in first['items']).isdisjoint(row['mediaId'] for row in second['items'])
    assert first['items'][0]['categories']['bed']['intervals'] == [{'start': 5, 'end': 900}]
    assert slicing_view(value, day='2026-11-01', page=999)['recordPagination']['page'] == 2


def test_unrecorded_dates_and_name_collisions_do_not_merge_unknown_video_identity():
    value = payload()
    value['items'] = [{'id': str(index), 'name': 'same.mp4', 'state': 'pending', 'revision': 0, 'categories': {}}
                      for index in range(2)]
    value['submissionReceipts'] = []
    summary = slicing_view(value)
    assert summary['recordDays'][0]['day'] == '' and summary['recordDays'][0]['total'] == 2
    detail = slicing_view(value, day='')
    assert len(detail['items']) == 2


def test_later_record_or_revision_changes_the_whole_day_snapshot():
    value = payload()
    before = slicing_view(value)['recordDays'][0]['revision']
    value['items'][0]['revision'] += 1
    after = slicing_view(value)['recordDays'][0]['revision']
    assert before != after
    value['items'].append(source(26))
    assert slicing_view(value)['recordDays'][0]['revision'] != after


def test_terminal_outcomes_filter_without_mislabeling_or_continuation():
    value = payload()
    value['items'] = [source(0)]
    for state in ('complete', 'partial_complete', 'no_target', 'uncuttable'):
        value['items'][0]['state'] = state
        value['items'][0]['categories']['bed']['state'] = state
        assert len(slicing_view(value, status=state)['items']) == 1
        assert not slicing_view(value, status='unsliced')['items']
    value['items'][0]['state'] = 'unexpected'
    assert not slicing_view(value, status='complete')['items']


def test_summary_payload_does_not_copy_large_submit_snapshots_or_image_names():
    value = payload()
    value['packages']['items'][0]['images'] = ['frame.jpg'] * 10000
    value['run']['taskLogIndex'] = ['snapshot'] * 10000
    summary = slicing_view(value)
    assert len(json.dumps(summary)) < len(json.dumps(value)) / 5
    assert len(value['packages']['items'][0]['images']) == 10000


def test_filters_are_applied_to_whole_physical_groups_before_paging():
    value = payload()
    value['items'][0]['state'] = 'attention'
    value['items'][0]['categories']['bed']['state'] = 'attention'
    bed = slicing_view(value, status='attention', source='27912')
    assert bed['recordDays'][0]['total'] == 1
    assert len(bed['items']) == 2  # Keep both category receipts for that physical file.
    assert not slicing_view(value, status='attention', source='27944')['recordDays']
    detail = slicing_view(value, day='2026-11-01', search='video-024')
    assert detail['recordPagination']['total'] == 1 and len(detail['items']) == 2
    assert slicing_view(value, search='not-present')['recordDays'] == []


def test_photo_import_problem_is_visible_even_when_its_model_has_completed():
    value = payload()
    value['packages']['items'][0].update(state='attention', category='bed')
    summary = slicing_view(value, source='27912', status='attention')
    assert summary['recordDays'][0]['total'] == 1
    assert not slicing_view(value, source='27944', status='attention')['recordDays']
