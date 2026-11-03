"""Only fake credentials/keychains; a late recovery cannot replace user intent."""
import asyncio
import threading

import httpx
import pytest

from app.upload_audit import AthenaClient, AuditError


@pytest.mark.asyncio
@pytest.mark.parametrize('old_status', [200, 401])
async def test_late_saved_restore_cannot_overwrite_manual_login_or_delete_new_credentials(monkeypatch, old_status):
    entered, release = asyncio.Event(), asyncio.Event()
    saved = {'refresh': 'fixture-stored', 'user': 'previous'}
    deleted = []

    async def handler(request):
        if request.url.path.endswith('/token/refresh/'):
            entered.set()
            await release.wait()
            return httpx.Response(old_status, json={'data': {'access': 'fixture-old', 'refresh': 'fixture-old-rotated'}})
        return httpx.Response(200, json={'data': {'access': 'fixture-new', 'refresh': 'fixture-new-refresh'}})

    monkeypatch.setattr('app.upload_audit.secure_store.load_connection', lambda: dict(saved))
    monkeypatch.setattr('app.upload_audit.secure_store.save_connection', lambda refresh, user: saved.update(refresh=refresh, user=user))
    monkeypatch.setattr('app.upload_audit.secure_store.delete_connection', lambda: deleted.append(True))
    client = AthenaClient(httpx.MockTransport(handler), persist_connection=True)
    try:
        pending = asyncio.create_task(client.restore_saved_connection())
        await entered.wait()
        await asyncio.wait_for(client.login('manual-user', 'fixture-password'), .2)
        release.set()
        assert await pending is False
        assert client.access == 'fixture-new' and client.user == 'manual-user'
        assert saved == {'refresh': 'fixture-new-refresh', 'user': 'manual-user'}
        assert not deleted and client.saved_connection
    finally:
        release.set()
        await client.close()


@pytest.mark.asyncio
async def test_cancelled_old_keychain_save_finishes_before_manual_login_can_save(monkeypatch):
    writing, release = threading.Event(), threading.Event()
    saved = {'refresh': 'fixture-stored', 'user': 'previous'}
    calls = []

    def save(refresh, user):
        if user == 'previous':
            writing.set()
            release.wait(2)
        saved.update(refresh=refresh, user=user)
        calls.append(user)

    def handler(request):
        old = request.url.path.endswith('/token/refresh/')
        return httpx.Response(200, json={'data': {'access': 'old' if old else 'new',
                                                'refresh': 'rotated-old' if old else 'new-refresh'}})

    monkeypatch.setattr('app.upload_audit.secure_store.load_connection', lambda: dict(saved))
    monkeypatch.setattr('app.upload_audit.secure_store.save_connection', save)
    client = AthenaClient(httpx.MockTransport(handler), persist_connection=True)
    pending = login = None
    try:
        pending = asyncio.create_task(client.restore_saved_connection())
        while not writing.is_set():
            await asyncio.sleep(.001)
        pending.cancel()
        login = asyncio.create_task(client.login('manual', 'fixture'))
        await asyncio.sleep(.02)
        pending.cancel()  # Shutdown can cancel an already-cancelled restore.
        await asyncio.sleep(.01)
        assert not pending.done() and not login.done()
        release.set()
        await asyncio.gather(pending, return_exceptions=True)
        await login
        assert calls == ['previous', 'manual']
        assert client.user == 'manual' and saved == {'refresh': 'new-refresh', 'user': 'manual'}
    finally:
        release.set()
        await asyncio.gather(*(task for task in (pending, login) if task), return_exceptions=True)
        await client.close()


@pytest.mark.asyncio
async def test_cancelled_keychain_delete_finishes_before_new_credentials_are_saved(monkeypatch):
    deleting, release = threading.Event(), threading.Event()
    saved = {'refresh': 'old', 'user': 'previous'}

    def delete():
        deleting.set()
        release.wait(2)
        saved.clear()

    def handler(request):
        return httpx.Response(200, json={'data': {'access': 'new', 'refresh': 'new-refresh'}})

    monkeypatch.setattr('app.upload_audit.secure_store.delete_connection', delete)
    monkeypatch.setattr('app.upload_audit.secure_store.save_connection', lambda refresh, user: saved.update(refresh=refresh, user=user))
    client = AthenaClient(httpx.MockTransport(handler), persist_connection=True)
    pending = login = None
    try:
        pending = asyncio.create_task(client.forget_saved_connection())
        while not deleting.is_set():
            await asyncio.sleep(.001)
        pending.cancel()
        login = asyncio.create_task(client.login('manual', 'fixture'))
        await asyncio.sleep(.02)
        pending.cancel()
        await asyncio.sleep(.01)
        assert not pending.done() and not login.done()
        release.set()
        await asyncio.gather(pending, return_exceptions=True)
        await login
        assert saved == {'refresh': 'new-refresh', 'user': 'manual'}
        assert client.access == 'new'
    finally:
        release.set()
        await asyncio.gather(*(task for task in (pending, login) if task), return_exceptions=True)
        await client.close()


@pytest.mark.asyncio
async def test_late_runtime_refresh_cannot_resurrect_explicitly_cleared_connection():
    entered, release = asyncio.Event(), asyncio.Event()

    async def handler(request):
        if request.method == 'GET':
            return httpx.Response(401)
        entered.set()
        await release.wait()
        return httpx.Response(200, json={'data': {'access': 'late-access', 'refresh': 'late-refresh'}})

    client = AthenaClient(httpx.MockTransport(handler), persist_connection=False)
    client.access, client.refresh = 'previous', 'previous-refresh'
    try:
        pending = asyncio.create_task(client.get_slicing('devel/datasets/'))
        await entered.wait()
        client.clear()
        release.set()
        with pytest.raises(AuditError):
            await pending
        assert not client.connected and not client.refresh
    finally:
        release.set()
        await client.close()


@pytest.mark.asyncio
async def test_final_late_401_does_not_clear_new_manual_session():
    entered, release = asyncio.Event(), asyncio.Event()
    reads = 0

    async def handler(request):
        nonlocal reads
        if request.method == 'GET':
            reads += 1
            if reads == 3:
                entered.set()
                await release.wait()
            return httpx.Response(401)
        if request.url.path.endswith('/login/'):
            return httpx.Response(200, json={'data': {'access': 'manual', 'refresh': 'manual-refresh'}})
        return httpx.Response(200, json={'data': {'access': 'refreshed-' + str(reads)}})

    client = AthenaClient(httpx.MockTransport(handler), persist_connection=False)
    client.access, client.refresh = 'previous', 'previous-refresh'
    try:
        pending = asyncio.create_task(client.get_slicing('devel/datasets/'))
        await entered.wait()
        await client.login('manual-user', 'fixture')
        release.set()
        with pytest.raises(AuditError):
            await pending
        assert client.access == 'manual' and client.user == 'manual-user'
    finally:
        release.set()
        await client.close()
