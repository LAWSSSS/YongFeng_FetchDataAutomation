"""Date collection exercises only mock HTTP and isolated databases."""
import asyncio
from datetime import datetime, timedelta

import pytest

from app.collection_runs import CollectionRunManager
from app.domain import DEFAULT_TARGET
from app.models import AdmissionRule
from app.remote import RemoteError
from app.videos import Conflict
from tests.test_simple_flow import app_factory, platform
from tests.test_slicing_performance import close_app


def make(app):
    # create_app exposes the clients through the video manager, not all versions
    # expose the source client. Tests replace it with a deterministic local source.
    from app.admission import AdmissionSettings
    source = Source()
    service = CollectionRunManager(app.state.store, source, app.state.jobs,
                                   app.state.videos, AdmissionSettings(app.state.store.db), dict(DEFAULT_TARGET))
    return service, source


class Source:
    connected = True

    def __init__(self, count=1):
        self.count = count
        self.calls = []
        self.duration = 12
        self.fail_detail = None
        self.page_size = 100
        self.shift = False

    async def request(self, path, params=None):
        self.calls.append((path, dict(params or {})))
        if path.endswith('/page'):
            page = params['pageIndex']
            begin = (page - 1) * self.page_size
            rows = [{'flowCode': f'F{i:04}', 'carNumber': f'鲁A{i:05}',
                     'createTime': '2026-09-30 00:00:00'}
                    for i in range(begin, min(begin + self.page_size, self.count))]
            if self.shift and rows:
                rows[0]['flowCode'] = 'CHANGED'
            return {'records': rows, 'total': self.count}
        if self.fail_detail:
            error, self.fail_detail = self.fail_detail, None
            raise error
        i = int(params['flowCode'][1:])
        start = datetime(2026, 9, 30, 0) + timedelta(minutes=i * 2)
        end = start + timedelta(minutes=self.duration)
        return {'stationResults': [{'checkStartTime': start.strftime('%Y-%m-%d %H:%M:%S'),
                                    'checkCompleteTime': end.strftime('%Y-%m-%d %H:%M:%S')}],
                'totalCheckResult': {'allOriginImageUrls': []}}


async def settle(service, app, identity, count=15):
    for _ in range(count):
        await service.tick()
        if app.state.jobs.running:
            await asyncio.gather(*tuple(app.state.jobs.running.values()))
        if app.state.jobs.store.list():
            await app.state.videos.check_once()
        row = service.get(identity)
        if row['status']=='awaiting_review':
            preview=service.review.get(identity)
            if not preview['readyIds']:return row
            service.review.submit(identity,{'recordIds':preview['readyIds'],'expectedPlanRevision':preview['planRevision'],'requestId':'explicit-test-approval-'+identity})
        if row['status'] in {'complete', 'attention'}:
            return row
    raise AssertionError(service.get(identity))


@pytest.mark.asyncio
async def test_date_run_defaults_and_final_confirmation_gate(app_factory):
    app = app_factory()
    service, _ = make(app)
    try:
        row = service.start({'dateFrom': '2026-09-30', 'dateTo': '2026-09-30', 'requestId': 'first'})
        assert row['status'] == 'queued'
        assert row['filters']['timeFrom'] == '00:00:00'
        assert not app.state.jobs.store.list()
        result = await settle(service, app, row['id'])
        assert result['status'] == 'complete'
        assert result['counts']['ready'] == 1
        job = app.state.jobs.store.get(result['jobId'])
        assert job['items'][0]['startTime'] == '2026-09-30 00:05:00'
        assert job['items'][0]['endTime'] == '2026-09-30 01:05:00'
        assert not job['downloadAfterCollection']
        video = app.state.videos.store.list()[0]
        assert video['acceptedRevision'] is None
        assert not video['versions'][0].get('downloadRequested')
    finally:
        await close_app(app)


@pytest.mark.asyncio
async def test_replay_and_overlapping_date_ranges_do_not_duplicate(app_factory, platform):
    app = app_factory()
    service, _ = make(app)
    try:
        payload = {'dateFrom': '2026-09-30', 'dateTo': '2026-09-30', 'requestId': 'same'}
        first = service.start(payload)
        assert service.start(payload)['id'] == first['id']
        with pytest.raises(Conflict):
            service.start({**payload, 'dateTo': '2026-10-01'})
        await settle(service, app, first['id'])
        second = service.start({**payload, 'dateTo': '2026-10-01', 'requestId': 'overlap'})
        result = await settle(service, app, second['id'])
        assert result['counts']['duplicate'] == 1
        assert platform.batch_sizes == [1]
    finally:
        await close_app(app)


@pytest.mark.asyncio
async def test_rule_snapshot_and_rejected_rows_never_submit(app_factory, platform):
    app = app_factory()
    service, source = make(app)
    try:
        source.duration = 10
        row = service.start({'dateFrom': '2026-09-30', 'dateTo': '2026-09-30', 'requestId': 'rule'})
        service.admission_settings.save(AdmissionRule(minInspectionMinutes=0), 0)
        result = await settle(service, app, row['id'])
        assert result['counts']['rejected'] == 1
        assert result['counts']['approved'] == 0
        assert platform.batch_sizes == []
        assert result['admissionRule']['minInspectionMinutes'] == 10
    finally:
        await close_app(app)


@pytest.mark.asyncio
async def test_more_than_twenty_uses_existing_batch_limit(app_factory, platform):
    app = app_factory()
    service, source = make(app)
    source.count = 41
    try:
        row = service.start({'dateFrom': '2026-09-30', 'dateTo': '2026-09-30', 'requestId': 'large'})
        result = await settle(service, app, row['id'])
        assert result['counts']['ready'] == 41
        assert platform.batch_sizes == [20, 20, 1]
    finally:
        await close_app(app)


@pytest.mark.asyncio
async def test_accepted_write_lost_response_reconciles_without_resending(app_factory, platform):
    app = app_factory()
    service, source = make(app)
    source.count = 21
    platform.fail_after_write = True
    try:
        row = service.start({'dateFrom': '2026-09-30', 'dateTo': '2026-09-30', 'requestId': 'lost-response'})
        result = await settle(service, app, row['id'])
        assert result['status'] == 'complete', {k:result.get(k) for k in ['status','stage','errorCode','counts','message']}
        assert result['counts']['submitted'] == 21
        assert platform.batch_sizes == [20, 1]
        assert all(item.get('bindingVerified') for item in app.state.jobs.store.get(result['jobId'])['items'])
    finally:
        await close_app(app)


@pytest.mark.asyncio
async def test_network_failure_preserves_page_and_restart_does_not_repeat_details(app_factory):
    app = app_factory()
    service, source = make(app)
    source.count, source.page_size = 3, 2
    try:
        row = service.start({'dateFrom': '2026-09-30', 'dateTo': '2026-09-30', 'requestId': 'resume-page'})
        await service.tick()
        assert service.get(row['id'])['page'] == 2
        source.fail_detail = RemoteError('network detail contains no secrets', 'NETWORK')
        await service.tick()
        stopped = service.get(row['id'])
        assert stopped['status'] == 'waiting_connection'
        assert len(stopped['records']) == 2
        resumed = CollectionRunManager(service.store, source, service.jobs, service.videos,
                                       service.admission_settings, service.target_config)
        source.calls.clear()
        result = await settle(resumed, app, row['id'])
        assert result['counts']['ready'] == 3
        detail_ids = [params['flowCode'] for path, params in source.calls if path.endswith('getCheckDetail')]
        assert detail_ids == ['F0002']
    finally:
        await close_app(app)


@pytest.mark.asyncio
async def test_disconnected_registration_returns_without_network_and_resumes(app_factory):
    app = app_factory()
    service, source = make(app)
    source.connected = False
    try:
        row = service.start({'dateFrom': '2026-09-30', 'dateTo': '2026-09-30', 'requestId': 'offline'})
        assert not source.calls
        await service.tick()
        assert service.get(row['id'])['status'] == 'waiting_connection'
        assert not source.calls
        source.connected = True
        result = await settle(service, app, row['id'])
        assert result['status'] == 'complete'
    finally:
        await close_app(app)


@pytest.mark.asyncio
async def test_changed_pagination_after_restart_is_not_silently_accepted(app_factory, platform):
    app = app_factory()
    service, source = make(app)
    source.count, source.page_size = 3, 2
    try:
        row = service.start({'dateFrom': '2026-09-30', 'dateTo': '2026-09-30', 'requestId': 'changed-page'})
        await service.tick()
        resumed = CollectionRunManager(service.store, source, service.jobs, service.videos,
                                       service.admission_settings, service.target_config)
        source.shift = True
        await resumed.tick()
        assert resumed.get(row['id'])['status'] == 'attention'
        assert not platform.batch_sizes
    finally:
        await close_app(app)


@pytest.mark.asyncio
async def test_restart_between_job_persist_and_run_binding_reuses_job(app_factory, platform):
    app = app_factory()
    service, _ = make(app)
    try:
        row = service.start({'dateFrom': '2026-09-30', 'dateTo': '2026-09-30', 'requestId': 'crash-link'})
        await service.tick()
        persisted = service.get(row['id'])
        preview=service.review.get(row['id'])
        service.review.submit(row['id'],{'recordIds':preview['readyIds'],'expectedPlanRevision':preview['planRevision'],'requestId':'approve-before-crash'})
        persisted=service.get(row['id'])
        app.state.jobs.start(persisted['plan'])
        await asyncio.gather(*tuple(app.state.jobs.running.values()))
        assert service.get(row['id'])['jobId'] == ''
        resumed = CollectionRunManager(service.store, service.source, service.jobs, service.videos,
                                       service.admission_settings, service.target_config)
        result = await settle(resumed, app, row['id'])
        assert result['status'] == 'complete'
        assert platform.batch_sizes == [1]
        assert len(app.state.jobs.store.list()) == 1
    finally:
        await close_app(app)


@pytest.mark.asyncio
async def test_unconfirmed_missing_write_is_not_replayed(app_factory, platform):
    app = app_factory()
    service, _ = make(app)
    try:
        row = service.start({'dateFrom': '2026-09-30', 'dateTo': '2026-09-30', 'requestId': 'missing-write'})
        await service.tick()
        preview=service.review.get(row['id'])
        service.review.submit(row['id'],{'recordIds':preview['readyIds'],'expectedPlanRevision':preview['planRevision'],'requestId':'confirm-missing-write'})
        plan = service.get(row['id'])['plan']
        job = app.state.jobs.start(plan)
        await asyncio.gather(*tuple(app.state.jobs.running.values()))
        saved = app.state.jobs.store.get(job['id'])
        item = saved['items'][0]
        item.update(status='uncertain', remoteId='', bindingVerified=False)
        saved['status'] = 'attention'
        app.state.jobs.store.save(saved)
        platform.parents.clear()
        result = await settle(service, app, row['id'])
        assert result['status'] == 'attention'
        assert result['errorCode'] == 'submission_unconfirmed'
        assert platform.batch_sizes == [1]
    finally:
        await close_app(app)


@pytest.mark.asyncio
async def test_new_binding_during_inventory_read_discards_stale_response(app_factory, monkeypatch):
    app = app_factory()
    started, release = asyncio.Event(), asyncio.Event()
    calls = 0

    async def inventory(_):
        nonlocal calls
        calls += 1
        if calls == 1:
            started.set()
            await release.wait()
            return {'old': {'id': 'old'}}
        return {'old': {'id': 'old'}, 'new': {'id': 'new'}}

    monkeypatch.setattr(app.state.jobs, 'remote_parents', inventory)
    try:
        reader = asyncio.create_task(app.state.videos.remote_parents_cached())
        await started.wait()
        app.state.videos.inventory_generation = 1
        release.set()
        result = await reader
        assert 'new' in result
        assert calls == 2
    finally:
        await close_app(app)


@pytest.mark.asyncio
async def test_public_runs_and_details_are_paged_without_internal_snapshots(app_factory):
    app = app_factory()
    service, source = make(app)
    source.count = 41
    try:
        row = service.start({'dateFrom': '2026-09-30', 'dateTo': '2026-09-30', 'requestId': 'public'})
        await service.tick()
        stored = service.get(row['id'])
        summary = service.state()['runs'][0]
        assert all(field not in summary for field in ['records', 'pageIds', 'seenIds', 'target', 'plan'])
        detail = service.public(stored, details=True, page=2)
        assert len(detail['records']) == 20
        assert detail['recordCount'] == 41
        assert detail['records'][0]['id'] == 'F0020'
    finally:
        await close_app(app)
