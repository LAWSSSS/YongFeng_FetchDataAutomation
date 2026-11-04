"""Interactive legacy-capture UI check with mock platforms and disposable data."""
import os
import tempfile
from contextlib import asynccontextmanager
from pathlib import Path

import httpx
import uvicorn

from app.main import create_app
from app.remote import RemoteClient
from tests.test_simple_flow import FakePlatform, new_video, source_handler


def run():
    platform = FakePlatform()
    with tempfile.TemporaryDirectory(prefix='capture-history-ui-') as directory:
        root = Path(directory)
        source = RemoteClient('source', transport=httpx.MockTransport(source_handler))
        target = RemoteClient('target', transport=httpx.MockTransport(platform.handle))
        source.connected = target.connected = True
        web = Path(os.environ.get('RECORD_UI_DIST', 'build/legacy-capture-repair')).resolve()
        app = create_app(source, target, data_dir=root / 'db', video_directory=root / 'media', web_directory=web)
        lifecycle = app.router.lifespan_context

        @asynccontextmanager
        async def seeded(application):
            async with lifecycle(application):
                async with httpx.AsyncClient(transport=httpx.ASGITransport(app=application),
                                             base_url='http://127.0.0.1') as client:
                    await new_video(client, application)
                runs = application.state.collection_runs
                runs.start({'dateFrom': '2026-09-30', 'dateTo': '2026-09-30', 'requestId': 'ui-query'})
                await runs.tick()
                yield

        app.router.lifespan_context = seeded
        uvicorn.run(app, host='127.0.0.1', port=8795, access_log=False)


if __name__ == '__main__':
    run()
