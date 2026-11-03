"""Recover saved sessions off the startup path; explicit disconnect wins."""
import asyncio

from .availability import connection_state
from .remote import RemoteError


class ConnectionRestorer:
    def __init__(self, delay=30):
        self.delay, self.tasks, self.errors = delay, {}, {}

    def start(self, key, attempt):
        async def run():
            try:
                while await attempt():
                    await asyncio.sleep(self.delay)
            except Exception as error:
                self.errors[key] = type(error).__name__
        self.tasks[key] = asyncio.create_task(run())

    async def stop(self, key):
        task = self.tasks.pop(key, None)
        if task:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def shutdown(self):
        await asyncio.gather(*(self.stop(key) for key in list(self.tasks)))


def retryable_connection(error):
    return isinstance(error, RemoteError) and str(error.code) in {
        'NETWORK', '429', '500', '502', '503', '504'}


def connection_waiting(client):
    state = connection_state(client)
    return (state['reachability'] == 'offline' or bool(getattr(client, 'restore_pending', False))) and bool(
        getattr(client, 'saved_connection', False))
