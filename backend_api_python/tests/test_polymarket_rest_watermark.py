"""REST /books freshness vs the WS watermark (PR B). Cold books are legitimately old; WS never rolls back."""
import itertools
import json
import threading
import time
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from app.services.polymarket import observer, ws_recovery
from app.services.polymarket.ws_recovery import SnapshotRejected, check_freshness
from app.services.polymarket.ws_redundancy import FeedMerge

NOW = datetime(2026, 10, 20, 2, 34, 7, tzinfo=timezone.utc)


def _reason(at, mark, hot, digest='h1'):
    try:
        check_freshness(at, (NOW - at).total_seconds(), digest, mark, hot)
    except SnapshotRejected as exc:
        return exc.reason
    return 'ok'


def test_cold_current_book_is_accepted_however_old():
    last = NOW - timedelta(seconds=108.8)
    assert _reason(last, (last, 'h1'), hot=False) == 'ok'
    assert _reason(last, (last, None), hot=False) == 'ok'
    assert _reason(NOW - timedelta(minutes=30), (NOW - timedelta(minutes=30), None), hot=False) == 'ok'


def test_cold_behind_ws_is_rejected_unless_same_book():
    mark = NOW - timedelta(seconds=10)
    assert _reason(mark - timedelta(seconds=5), (mark, None), hot=False) == 'behind_ws'
    assert _reason(mark - timedelta(seconds=5), (mark, 'h2'), hot=False) == 'behind_ws'
    assert _reason(mark - timedelta(seconds=5), (mark, 'h1'), hot=False) == 'ok'
    assert _reason(mark + timedelta(seconds=3), (mark, None), hot=False) == 'ok'


def test_cold_without_watermark_has_a_cap():
    assert _reason(NOW - timedelta(seconds=300), None, hot=False) == 'ok'
    assert _reason(NOW - timedelta(seconds=ws_recovery.COLD_UNMARKED_MAX_SECONDS + 1), None, hot=False) == 'stale'


def test_future_skew_is_rejected():
    for hot in (True, False):
        assert _reason(NOW + timedelta(seconds=2.5), None, hot) == 'future'
        assert _reason(NOW + timedelta(seconds=1.5), None, hot) == 'ok'


def test_hot_keeps_the_wall_clock_limit_and_adds_the_watermark():
    assert _reason(NOW - timedelta(seconds=1), None, hot=True) == 'ok'
    assert _reason(NOW - timedelta(seconds=3), None, hot=True) == 'stale'
    assert _reason(NOW - timedelta(seconds=3), (NOW - timedelta(seconds=3), 'h1'), hot=True) == 'stale'
    assert _reason(NOW - timedelta(seconds=1.5), (NOW - timedelta(seconds=0.5), None), hot=True) == 'behind_ws'


def test_hot_is_never_looser_than_the_old_rule():
    """Old rule: |now - ts| <= 2. Anything the new hot rule accepts, the old one did too."""
    ages = [-3, -2.5, -2, -1, 0, 0.5, 1, 1.9, 2, 2.1, 3, 10, 120]
    marks = [None, -1, 0, 0.5, 1, 5, 200]
    for age, offset, digest in itertools.product(ages, marks, ('h1', 'h2', None)):
        at = NOW - timedelta(seconds=age)
        mark = None if offset is None else (NOW - timedelta(seconds=offset), 'h1')
        if _reason(at, mark, hot=True, digest=digest) == 'ok':
            assert abs(age) <= ws_recovery.STALE_SECONDS, (age, offset, digest)


def test_feed_merge_never_rolls_ws_back_with_a_rest_book():
    merge = FeedMerge({})
    newer = int((NOW - timedelta(seconds=10)).timestamp() * 1000)
    older = newer - 5000
    ws = dict(event_type='book', asset_id='a', timestamp=str(newer), hash='w', bids=[dict(price='.5', size='1')], asks=[])
    assert merge.filter(ws, 'feed-1', {'a'})
    rest_old = dict(event_type='book', asset_id='a', timestamp=str(older), bids=[dict(price='.1', size='1')], asks=[])
    assert not merge.filter(rest_old, 'rest', {'a'}, backup=True)
    rest_same = dict(event_type='book', asset_id='a', timestamp=str(newer), bids=[dict(price='.5', size='1')], asks=[])
    assert merge.filter(rest_same, 'rest', {'a'}, backup=True)


def test_request_snapshot_passes_watermarks_and_hotness():
    manager = observer.MarketConnections()
    manager.balancer.assignment = {'cy': 3, 'cn': 3}
    manager.balancer.groups = {'cy': 'market:4662693', 'cn': 'market:4662693'}
    merge = FeedMerge({})
    t_book = NOW - timedelta(seconds=108)
    t_change = NOW - timedelta(seconds=50)
    merge.latest = {'cy': t_book, 'cn': t_change}
    manager.merges[3] = merge
    seen = {}

    def request(state, assets, publish, pressure=False):
        seen.update(water=dict(state['_rest_watermarks']), hot=state['_rest_hot'], assets=sorted(assets))
        return True
    manager.snapshot_recovery.request = request
    state = dict(assets=['cy', 'cn'], book_marks={'cy': (t_book, 'hb', '1'), 'cn': (t_book, 'hb2', '1')})
    manager.states[3] = state
    assert manager.request_snapshot(3, state, ['cy', 'cn']) is True
    # Book is the latest change for cy: hash kept. cn changed after its book: no hash shortcut.
    assert seen['water'] == {'cy': (t_book, 'hb'), 'cn': (t_change, None)}
    assert seen['hot'] is False
    manager.balancer.groups = {'cy': 'up_down:BTC:5m', 'cn': 'up_down:BTC:5m'}
    manager.request_snapshot(3, state, ['cy', 'cn'])
    assert seen['hot'] is True


class _Books:
    stamps = {}
    calls = 0


class _Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_POST(self):
        _Books.calls += 1
        body = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
        rows = [dict(asset_id=row['token_id'], timestamp=str(_Books.stamps[row['token_id']]), hash='x',
                     bids=[dict(price='.4', size='1')], asks=[dict(price='.6', size='1')]) for row in body]
        data = json.dumps(rows).encode()
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(data)))
        self.end_headers()
        self.wfile.write(data)


@pytest.fixture
def books(monkeypatch):
    server = ThreadingHTTPServer(('127.0.0.1', 0), _Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    monkeypatch.setattr(ws_recovery, 'CLOB', f'http://127.0.0.1:{server.server_address[1]}')
    _Books.stamps, _Books.calls = {}, 0
    yield _Books
    server.shutdown()
    server.server_close()


def _repair(state, assets):
    recovery = ws_recovery.SnapshotRecovery()
    got = []
    assert recovery.request(state, assets, lambda events, started: got.extend(events)) is True
    deadline = time.monotonic() + 5
    while recovery.completed + recovery.failures == 0 and time.monotonic() < deadline:
        time.sleep(0.01)
    time.sleep(0.05)
    return recovery.metrics(), got


def test_cold_reconnect_repairs_on_the_first_attempt(books):
    last = datetime.now(timezone.utc) - timedelta(seconds=108.8)
    stamp = int(last.timestamp() * 1000)
    books.stamps = {'cy': stamp, 'cn': stamp}
    mark = datetime.fromtimestamp(stamp / 1000, timezone.utc)
    state = dict(_rest_watermarks={'cy': (mark, None), 'cn': (mark, None)}, _rest_hot=False)
    metrics, got = _repair(state, ['cy', 'cn'])
    assert metrics['completed'] == 1 and metrics['failures'] == 0 and books.calls == 1
    assert {e['asset_id'] for e in got} == {'cy', 'cn'}


def test_cold_rest_behind_ws_is_rejected_and_backs_off(books):
    now = datetime.now(timezone.utc)
    mark = now - timedelta(seconds=10)
    books.stamps = {'cy': int((mark - timedelta(seconds=5)).timestamp() * 1000), 'cn': int(mark.timestamp() * 1000)}
    state = dict(_rest_watermarks={'cy': (mark, None), 'cn': (mark, None)}, _rest_hot=False)
    metrics, got = _repair(state, ['cy', 'cn'])
    assert metrics['failures'] == 1 and not got
    assert metrics['failure_reasons']['recovery'] == {'behind_ws': 1}


def test_hot_old_book_still_rejected_over_http(books):
    old = datetime.now(timezone.utc) - timedelta(seconds=3)
    stamp = int(old.timestamp() * 1000)
    books.stamps = {'ay': stamp, 'an': stamp}
    mark = datetime.fromtimestamp(stamp / 1000, timezone.utc)
    for state in (dict(_rest_watermarks={'ay': (mark, None), 'an': (mark, None)}, _rest_hot=True), dict()):
        metrics, got = _repair(state, ['ay', 'an'])
        assert metrics['failure_reasons']['recovery'] == {'stale': 1} and not got
