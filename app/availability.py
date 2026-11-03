"""Passive connection health; a cached login is not a reachability probe."""
import time

from .domain import now_iso


class ConnectionAvailability:
    def __init__(self):
        self.reachability = 'unknown'
        self.checked_at = None
        self.checked_clock = 0
        self.failure = ''

    def received(self):
        self.reachability = 'online'
        self.checked_at, self.checked_clock = now_iso(), time.monotonic()
        self.failure = ''

    def unavailable(self):
        self.reachability = 'offline'
        self.checked_at, self.checked_clock = now_iso(), time.monotonic()
        # Do not retain exceptions: request URLs may contain credentials.
        self.failure = 'network'

    def state(self, authenticated):
        reachability = self.reachability
        if reachability == 'online' and time.monotonic() - self.checked_clock > 120:
            reachability = 'unknown'
        return {'authenticated': bool(authenticated), 'reachability': reachability,
                'checkedAt': self.checked_at, 'lastFailure': self.failure}


def connection_state(client):
    monitor = getattr(client, 'availability', None)
    authenticated = bool(getattr(client, 'connected', False))
    return monitor.state(authenticated) if isinstance(monitor, ConnectionAvailability) else {
        'authenticated': authenticated, 'reachability': 'unknown', 'checkedAt': None, 'lastFailure': ''}
