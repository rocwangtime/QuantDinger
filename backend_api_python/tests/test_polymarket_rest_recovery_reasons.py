"""REST /books repair: failure reasons, stale back-off, ended markets left to WS."""
import json
import logging
import threading
import time
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from app.services.polymarket import observer, ws_recovery
from app.services.polymarket.ws_group_process import _merge_recovery


class _Books:
    mode = 'fresh'
    calls = 0


class _Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_POST(self):
        _Books.calls += 1
        body = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
        mode = _Books.mode
        if mode == 'http429':
            self.send_response(429)
            self.end_headers()
            return
        if mode == 'slow':
            time.sleep(1.5)
        age = 3.0 if mode == 'stale' else 0.0
        stamp = str(int((time.time() - age) * 1000))
        rows = [dict(asset_id=row['token_id'], timestamp=stamp, hash='h', bids=[dict(price='.4', size='1')],
                     asks=[dict(price='.6', size='1')]) for row in body]
        if mode == 'incomplete':
            rows = rows[:1]
        data = json.dumps(rows).encode()
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(data)))
        self.end_headers()
        self.wfile.write(data)


@pytest.fixture
def books(monkeypatch):
    server = ThreadingHTTPServer(('127.0.0.1', 0), _Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    monkeypatch.setattr(ws_recovery, 'CLOB', f'http://127.0.0.1:{server.server_address[1]}')
    _Books.mode, _Books.calls = 'fresh', 0
    yield _Books
    server.shutdown()
    server.server_close()


def _run(recovery, state, mode, books, publish=None, assets=('a', 'b')):
    books.mode = mode
    state.pop('rest_retry_at', None)
    done = threading.Event()

    def sink(events, started):
        try:
            if publish:
                publish(events, started)
        finally:
            done.set()

    before = recovery.completed + recovery.failures
    assert recovery.request(state, list(assets), sink) is True
    deadline = time.monotonic() + 5
    while recovery.completed + recovery.failures == before and time.monotonic() < deadline:
        time.sleep(0.01)
    time.sleep(0.05)
    return recovery.metrics()


def test_each_failure_has_a_reason_and_stale_backs_off(books, monkeypatch):
    recovery = ws_recovery.SnapshotRecovery()
    state = {}
    metrics = _run(recovery, state, 'fresh', books)
    assert metrics['completed'] == 1 and metrics['failures'] == 0
    metrics = _run(recovery, state, 'stale', books)
    assert metrics['failure_reasons']['recovery'] == {'stale': 1}
    assert metrics['last_failure']['reason'] == 'stale' and metrics['last_failure']['age_s'] >= 2.5
    # First stale answer of a streak keeps the normal 0.5s retry (live books).
    assert 0.2 < state['rest_retry_at'] - time.monotonic() <= 0.5
    assert recovery.request(state, ['a', 'b'], lambda *a: None) is False
    assert metrics['categories']['recovery']['total'].get('stale_backoff', 0) == 0
    # Still stale: the cached book stays for seconds, wait 5s.
    _run(recovery, state, 'stale', books)
    assert state['rest_retry_at'] - time.monotonic() > 4 and state['_rest_stale_streak'] == 2
    assert recovery.metrics()['categories']['recovery']['total']['stale_backoff'] == 1
    _run(recovery, state, 'fresh', books)
    assert '_rest_stale_streak' not in state
    for mode, reason in (('http429', 'http_429'), ('incomplete', 'incomplete'), ('slow', 'read_timeout')):
        metrics = _run(recovery, state, mode, books)
        assert metrics['failure_reasons']['recovery'][reason] == 1, mode
        assert state['rest_retry_at'] - time.monotonic() < 1, mode

    def broken(events, started):
        raise KeyError('x')
    metrics = _run(recovery, state, 'fresh', books, publish=broken)
    assert metrics['failure_reasons']['recovery']['publish_KeyError'] == 1
    assert metrics['failures'] == 6


def test_connect_timeout_reason(monkeypatch):
    recovery = ws_recovery.SnapshotRecovery()
    assert ws_recovery.failure_reason(ws_recovery.requests.ConnectTimeout()) == 'connect_timeout'
    assert ws_recovery.failure_reason(ws_recovery.requests.ConnectionError()) == 'connection_error'
    assert ws_recovery.failure_reason(ValueError('Expecting value')) == 'bad_json'
    assert recovery.metrics()['last_failure'] is None


def test_failure_log_is_rate_limited(books, caplog):
    recovery = ws_recovery.SnapshotRecovery()
    state = {}
    with caplog.at_level(logging.WARNING, logger=ws_recovery.__name__):
        for _ in range(3):
            _run(recovery, state, 'incomplete', books)
    lines = [r for r in caplog.records if 'REST book repair failed' in r.getMessage()]
    assert len(lines) == 1 and 'reason=incomplete' in lines[0].getMessage()
    assert recovery.metrics()['failure_reasons']['recovery']['incomplete'] == 3


def _manager(end):
    manager = observer.MarketConnections()
    manager.markets = [dict(id='1', yesAssetId='a', noAssetId='b', endDate=end.isoformat().replace('+00:00', 'Z')),
                       dict(id='2', yesAssetId='c', noAssetId='d',
                            endDate=(datetime.now(timezone.utc) + timedelta(minutes=5)).isoformat().replace('+00:00', 'Z'))]
    sent = []
    manager.snapshot_recovery.request = lambda state, assets, publish, pressure=False: sent.append(sorted(assets)) or True
    resubscribed = []
    manager._resubscribe_books = lambda index, state: resubscribed.append(index)
    return manager, sent, resubscribed


def test_ended_market_is_left_to_ws_and_live_market_still_repairs():
    ended = datetime.now(timezone.utc) - timedelta(seconds=30)
    manager, sent, resubscribed = _manager(ended)
    state = dict(need_book={'a', 'b'}, inbox=None)
    manager.states[0] = state
    assert manager.request_snapshot(0, state, {'a', 'b'}) is False
    assert sent == [] and resubscribed == [0]
    assert manager.snapshot_recovery.metrics()['categories']['recovery']['total']['skipped_ended'] == 2
    assert manager.request_snapshot(0, state, ['a', 'b', 'c', 'd']) is True
    assert sent == [['c', 'd']]
    # Unknown assets and markets without an end time are repaired as before.
    assert manager.request_snapshot(0, state, ['zz']) is True and sent[-1] == ['zz']


def test_live_market_is_not_skipped():
    manager, sent, _ = _manager(datetime.now(timezone.utc) + timedelta(seconds=30))
    state = dict(inbox=None)
    assert manager.request_snapshot(0, state, ['a', 'b']) is True and sent == [['a', 'b']]


def test_children_merge_reasons_and_latest_failure():
    left = dict(failures=2, failure_reasons={'recovery': {'stale': 2}},
                last_failure=dict(at='2026-10-20T06:00:00+00:00', reason='stale'))
    right = dict(failures=1, failure_reasons={'recovery': {'stale': 1, 'http_429': 1}, 'warmup': {'incomplete': 1}},
                 last_failure=dict(at='2026-10-20T06:00:05+00:00', reason='http_429'))
    merged = _merge_recovery(left, right)
    assert merged['failure_reasons'] == {'recovery': {'stale': 3, 'http_429': 1}, 'warmup': {'incomplete': 1}}
    assert merged['last_failure']['reason'] == 'http_429' and merged['failures'] == 3
