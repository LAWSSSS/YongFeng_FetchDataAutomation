"""Read-only slicing summaries and physical-video history pages.

The durable source records and photo receipts are never rewritten by a view.
"""
import copy
import hashlib
import json
from datetime import datetime, timedelta, timezone


BEIJING = timezone(timedelta(hours=8))
SUMMARY_ITEM = {'id', 'name', 'mediaId', 'mediaGeneration', 'collectionRevision', 'collectionVideoId',
                'sourceDatasetId', 'sourceVideoId', 'revision', 'state', 'submittedAt', 'progress', 'message',
                'errorCode', 'processingBlockedReason', 'superseded', 'coveredByRecovery', 'partialRecovery', 'processing'}
SUMMARY_CATEGORY = {'state', 'packageId', 'taskId', 'operationId', 'actualImages', 'errorCode', 'message'}
SUMMARY_RUN = {'id', 'state', 'message', 'executeAll', 'singleMode', 'singleItemId', 'finishedAt',
               'settingsRevision', 'excluded', 'targetDatasets', 'itemCount', 'localRecoveryOnly'}
TERMINAL = {'complete', 'partial_complete', 'no_target', 'uncuttable'}


def terminal_state(states):
    if states and all(state == 'uncuttable' for state in states):
        return 'uncuttable'
    if states and all(state in {'no_target', 'uncuttable'} for state in states):
        return 'no_target'
    return 'partial_complete' if any(state in {'partial_complete', 'uncuttable'} for state in states) else 'complete'


def category_display_state(category):
    if category.get('state') == 'complete' and any(
            isinstance(category.get(key), (int, float)) and category[key] > 0
            for key in ('skippedSamples', 'skippedExportFrames')):
        return 'partial_complete'
    return category.get('state')


def _day(value):
    if not isinstance(value, str) or not value.strip():
        return ''
    try:
        instant = datetime.fromisoformat(value.strip().replace('Z', '+00:00'))
        if instant.tzinfo is None:
            instant = instant.replace(tzinfo=BEIJING)
        return instant.astimezone(BEIJING).date().isoformat()
    except ValueError:
        return ''


def _physical_key(item):
    if item.get('mediaId'):
        return ('media', item['mediaId'], str(item.get('mediaGeneration', 0)), str(item.get('collectionRevision', '')))
    # Unbound sources never acquire an identity through their filename alone.
    return ('path', item['path']) if item.get('path') else ('unbound', item['id'])


def _groups(items):
    groups = {}
    for item in items:
        groups.setdefault(_physical_key(item), []).append(item)
    result = []
    for key, rows in groups.items():
        days = [_day(item.get('submittedAt')) for item in rows]
        known = sorted(day for day in days if day)
        result.append((key, known[0] if known else '', rows))
    return result


def _scope(groups, receipts):
    value = {'items': sorted((item['id'], item.get('revision', 0)) for _, _, rows in groups for item in rows),
             'receipts': sorted((receipt['id'], receipt.get('revision', 0)) for receipt in receipts)}
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':')).encode()).hexdigest()[:24]


def _pick(value, keys):
    return {key: copy.deepcopy(value[key]) for key in keys if key in value}


def progress_view(item):
    value = copy.deepcopy(item)
    terminal = value.get('state') in TERMINAL | {'cleaned','superseded','cancelled','excluded'} or bool(value.get('superseded') or value.get('coveredByRecovery'))
    value['terminal'] = terminal
    value['displayProgress'] = None
    if terminal:
        value['progress'] = None
    elif value.get('analysisTotal') and value.get('state') == 'analyzing':
        value['displayProgress'] = {'done': min(int(value.get('analysisDone') or 0), int(value['analysisTotal'])),
                                    'total': int(value['analysisTotal']), 'unit': '个位置'}
    return value


def slicing_view(payload, summary=True, day=None, page=1, page_size=20, status='all', source='all', search=''):
    """Project summary metadata or a single Beijing submission-day page.

    Pagination counts physical video groups, so both model records always
    travel on the same page. An empty day is the explicit undated bucket.
    """
    groups = _groups(payload.get('items') or [])
    receipts = payload.get('submissionReceipts') or []
    needle = str(search or '').strip().casefold()
    selected_category = 'bed' if source == '27912' else 'magnet' if source == '27944' else None
    package_issues = {item.get('itemId') for item in (payload.get('packages') or {}).get('items') or []
                      if item.get('state') in {'attention', 'review'} and
                      (not selected_category or item.get('category') == selected_category)}
    def matches(group):
        rows = [item for item in group[2] if not item.get('superseded') and not item.get('coveredByRecovery')]
        rows = rows or group[2]
        values = [category for item in rows for name, category in (item.get('categories') or {}).items()
                  if (not selected_category or name == selected_category) and category.get('state') != 'not_applicable']
        if selected_category and not values:
            return False
        states = [category.get('state') for category in values]
        overall = next((state for state in ('attention', 'review', 'analyzing', 'waiting_connection', 'pending', 'platform_processing', 'waiting', 'submitting')
                        if any(item.get('state') == state for item in rows)),
                       terminal_state([item.get('state') for item in rows])
                       if rows and all(item.get('state') in TERMINAL for item in rows) else 'attention')
        issue = any(item['id'] in package_issues or item.get('processingBlockedReason') for item in rows)
        if issue and not selected_category:
            overall = 'attention'
        accepted = (status == 'all' or (status == 'unsliced' and overall not in TERMINAL)
                    or (selected_category and status in states) or (not selected_category and status == overall)
                    or (status == 'attention' and issue)
                    or (status == 'analyzing' and overall == 'analyzing' and 'pending' in states)
                    or (status == 'platform_processing' and any(state in {'ready', 'exporting', 'waiting_slot',
                         'waiting', 'submitting', 'queued', 'uploading', 'uploaded', 'importing'} for state in states)))
        return accepted and (not needle or any(needle in (str(item.get('name') or '') + ' ' +
                                                       str(item.get('sourceVideoId') or '')).casefold() for item in rows))
    groups = [group for group in groups if matches(group)]
    if status != 'all' or source != 'all' or needle:
        selected_ids = {item['id'] for _, _, rows in groups for item in rows}
        receipts = [receipt for receipt in receipts if selected_ids.intersection(receipt.get('itemIds') or [])]
    days = sorted({row[1] for row in groups} | {_day(row.get('submittedAt')) for row in receipts}, reverse=True)
    record_days = []
    for date in days:
        dated = [row for row in groups if row[1] == date]
        dated_receipts = [row for row in receipts if _day(row.get('submittedAt')) == date]
        record_days.append({'day': date, 'total': len(dated), 'itemCount': sum(len(row[2]) for row in dated),
                            'receiptCount': len(dated_receipts), 'revision': _scope(dated, dated_receipts)})
    if not summary and day is None:
        return {**copy.deepcopy(payload), 'items': [progress_view(item) for item in payload.get('items') or []], 'recordDays': record_days}

    excluded = {'items', 'packages', 'archive', 'run', 'submissionReceipts'}
    value = {key: copy.deepcopy(item) for key, item in payload.items() if key not in excluded}
    value['recordDays'] = record_days
    value['run'] = _pick(payload['run'], SUMMARY_RUN) if payload.get('run') else None
    packages = payload.get('packages') or {}
    value['packages'] = {key: copy.deepcopy(item) for key, item in packages.items() if key not in {'items', 'targets'}}
    value['packages']['targets'] = [_pick(target, {'id', 'name', 'importing', 'uploading', 'queued'})
                                    for target in packages.get('targets') or []]
    archive = payload.get('archive') or {}
    value['archive'] = {'counts': copy.deepcopy(archive.get('counts') or {})}

    if day is None:
        value['items'] = [{**_pick(progress_view(item), SUMMARY_ITEM | {'terminal','displayProgress'}),
                           'categories': {name: _pick(category, SUMMARY_CATEGORY)
                                          for name, category in (item.get('categories') or {}).items()}}
                          for _, _, rows in groups for item in rows]
        value['packages']['items'] = []
        value['archive']['items'] = []
        value['submissionReceipts'] = [_pick(receipt, {'id', 'title', 'revision', 'submittedAt', 'message', 'notice'}) |
                                        {'itemCount': len(receipt.get('itemIds') or [])} for receipt in receipts]
        return value

    dated = sorted((row for row in groups if row[1] == day),
                   key=lambda row: (str(row[2][0].get('name') or '').casefold(), row[0]))
    dated_receipts = [row for row in receipts if _day(row.get('submittedAt')) == day]
    size = max(1, min(20, int(page_size)))
    pages = max(1, (len(dated) + size - 1) // size)
    current = min(max(1, int(page)), pages)
    selected = dated[(current - 1) * size:current * size]
    selected_items = [item for _, _, rows in selected for item in rows]
    ids = {item['id'] for item in selected_items}
    paths = {item.get('path') for item in selected_items if item.get('path')}
    value['items'] = [progress_view(item) for item in selected_items]
    value['packages']['items'] = [copy.deepcopy(item) for item in packages.get('items') or [] if item.get('itemId') in ids]
    value['archive']['items'] = [copy.deepcopy(item) for item in archive.get('items') or [] if item.get('path') in paths]
    value['submissionReceipts'] = copy.deepcopy(dated_receipts)
    value['recordPagination'] = {'day': day, 'page': current, 'pages': pages, 'pageSize': size,
                                 'total': len(dated), 'scopeRevision': _scope(dated, dated_receipts)}
    return value
