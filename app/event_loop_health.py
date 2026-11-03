"""Bounded diagnostics for a stalled local API, without arguments or locals."""
import asyncio
import logging
from pathlib import Path
import sys
import threading
import time
import traceback


logger = logging.getLogger(__name__)


class EventLoopHealth:
    def __init__(self, threshold=3.0):
        self.threshold = threshold
        self.stopped = threading.Event()
        self.last_tick = time.monotonic()
        self.last_report = 0.0
        self.timer = None

    def start(self):
        self.loop = asyncio.get_running_loop()
        self.thread_id = threading.get_ident()
        self.last_tick = time.monotonic()
        self._tick()
        self.thread = threading.Thread(target=self._watch, name='local-api-watchdog', daemon=True)
        self.thread.start()

    def _tick(self):
        if not self.stopped.is_set():
            self.last_tick = time.monotonic()
            self.timer = self.loop.call_later(.5, self._tick)

    def _watch(self):
        while not self.stopped.wait(1):
            now = time.monotonic()
            last_tick = self.last_tick
            if now - last_tick < self.threshold or now - self.last_report < 30:
                continue
            self.last_report = now
            frame = sys._current_frames().get(self.thread_id)
            if frame is not None:
                # Never include source text, function arguments or frame locals.
                stack = ' > '.join(f'{Path(row.filename).name}:{row.lineno}:{row.name}'
                                   for row in traceback.extract_stack(frame)[-16:])
                logger.warning('Local API loop stalled %.1fs: %s', now - last_tick, stack)
                del frame

    def stop(self):
        self.stopped.set()
        if self.timer:
            self.timer.cancel()
