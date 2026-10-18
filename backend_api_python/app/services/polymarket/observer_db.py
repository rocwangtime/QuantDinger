"""One shared database slot for public observer work, away from WS readers.

The paper account keeps its own lease and persistence connections. Serializing
observer SQL leaves those headroom even with the process's four-slot pool.
"""
import threading
import time
from contextlib import contextmanager

from app.utils.db import get_db_connection

_SLOT = threading.RLock()
_METRICS_LOCK = threading.Lock()
_METRICS = dict(active=0, waiting=0, acquisitions=0, max_wait_seconds=0.)


@contextmanager
def connection():
    began = time.monotonic()
    with _METRICS_LOCK:
        _METRICS['waiting'] += 1
    with _SLOT:
        with _METRICS_LOCK:
            _METRICS['waiting'] -= 1
            _METRICS['active'] += 1
            _METRICS['acquisitions'] += 1
            _METRICS['max_wait_seconds'] = max(_METRICS['max_wait_seconds'], time.monotonic() - began)
        try:
            with get_db_connection() as db:
                yield db
        finally:
            with _METRICS_LOCK:
                _METRICS['active'] -= 1


def metrics():
    with _METRICS_LOCK:
        return dict(_METRICS, limit=1, scope='public_observer_sql')
