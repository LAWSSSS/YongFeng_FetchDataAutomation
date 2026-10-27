"""Resizable asyncio slots: a lower limit never cancels admitted work."""
import asyncio


class DownloadSlots:
    def __init__(self, limit):
        self.active = 0
        self.changed = asyncio.Event()
        self.set_limit(limit)

    def set_limit(self, limit):
        if type(limit) is not int or limit < 1:
            raise ValueError("下载并行数量必须是正整数。")
        self.limit = limit
        self.changed.set()

    async def __aenter__(self):
        while self.active >= self.limit:
            self.changed.clear()
            await self.changed.wait()
        self.active += 1
        return self

    async def __aexit__(self, *_):
        self.active -= 1
        self.changed.set()
