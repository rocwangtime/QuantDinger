"""Fast admission remains independent of saturated bulk model work."""
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

from app.services.automation import worker


@pytest.mark.parametrize('market', ['HKStock', 'USStock'])
@pytest.mark.parametrize('status', ['queued', 'planned'])
def test_fast_analysis_and_order_do_not_wait_for_bulk(monkeypatch, status, market):
    release = threading.Event()
    started = threading.Barrier(3)
    called = threading.Event()

    def slow():
        started.wait(timeout=3)
        release.wait(timeout=5)

    def decision(*args):
        called.set()

    monkeypatch.setattr(worker.store, 'task', lambda *_: {'config': {'kind': 'price_trigger', 'market': market}})
    monkeypatch.setattr(worker, 'analyze', decision)
    monkeypatch.setattr(worker, 'execute', decision)
    run = {'id': 3, 'user_id': 1, 'task_id': 3, 'status': status, 'preview': False}
    with ThreadPoolExecutor(max_workers=2) as bulk, ThreadPoolExecutor(max_workers=1) as fast:
        try:
            bulk_pending = {i: bulk.submit(slow) for i in (1, 2)}
            started.wait(timeout=3)
            pending = {}
            worker.dispatch_runs(fast, pending, 1, [run], {3: ({}, {})})
            assert called.wait(timeout=1)
            pending[3].result(timeout=1)
            assert all(not f.done() for f in bulk_pending.values())
        finally:
            release.set()


def test_fast_capacity_is_bounded_and_missing_context_expires(monkeypatch):
    class Pool:
        def submit(self, *_):
            pytest.fail('Must not queue behind a busy fast slot or rebuild trigger context')

    run = {'id': 2, 'user_id': 1, 'task_id': 2, 'status': 'queued', 'preview': False}
    worker.dispatch_runs(Pool(), {1: object()}, 1, [run], {})
    monkeypatch.setattr(worker.store, 'task', lambda *_: {'config': {'kind': 'price_trigger'}})
    updates = []
    monkeypatch.setattr(worker.store, 'update_run', lambda *a, **kw: updates.append(kw))
    worker.dispatch_runs(Pool(), {}, 1, [run], {})
    assert updates[0]['status'] == 'expired'
