import sqlite3

import pytest

from app.main import create_app


def assert_closed(app):
    assert app.state.package_gallery.task is None or app.state.package_gallery.task.done()
    assert app.state.videos.stopping
    assert app.state.slicing.stopping
    with pytest.raises(sqlite3.ProgrammingError, match="closed"):
        app.state.store.db.execute("SELECT 1")


@pytest.mark.asyncio
async def test_partial_startup_still_closes_workers_and_database(tmp_path, monkeypatch):
    app = create_app(data_dir=tmp_path)
    async def fail_start():
        raise RuntimeError("startup failure")
    monkeypatch.setattr(app.state.workspace, "startup", fail_start)
    with pytest.raises(RuntimeError, match="startup failure"):
        async with app.router.lifespan_context(app):
            pytest.fail("startup must fail")
    assert_closed(app)


@pytest.mark.asyncio
async def test_exception_during_lifespan_still_releases_everything(tmp_path):
    app = create_app(data_dir=tmp_path)
    with pytest.raises(RuntimeError, match="application failure"):
        async with app.router.lifespan_context(app):
            raise RuntimeError("application failure")
    assert_closed(app)


@pytest.mark.asyncio
async def test_one_shutdown_failure_does_not_skip_other_cleanup(tmp_path, monkeypatch):
    app = create_app(data_dir=tmp_path)
    original = app.state.workspace.shutdown
    async def fail_shutdown():
        await original()
        raise RuntimeError("shutdown failure")
    monkeypatch.setattr(app.state.workspace, "shutdown", fail_shutdown)
    with pytest.raises(RuntimeError, match="shutdown failure"):
        async with app.router.lifespan_context(app):
            pass
    assert_closed(app)
