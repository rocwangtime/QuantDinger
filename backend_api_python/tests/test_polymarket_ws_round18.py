"""Round18: local admission, lossless catchup, incident windows and TLS ownership."""
import json
import logging
import random
import socket
import ssl
import threading
import time
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from app.services.polymarket import observer, recording, ws_payloads, ws_transport
from app.services.polymarket.account_history import BookHistory
from app.services.polymarket.ws_redundancy import FeedContinuity, FeedMerge
from app.services.polymarket.ws_subscriptions import SubscriptionPacer
from tests.test_polymarket_ws_rotation import book, feed_manager, wait_for
from tests.test_polymarket_ws_round7 import make_hot, ready_backup


@pytest.mark.parametrize('envelope', [False, True])
def test_normalized_deltas_preserve_immutable_cached_frame_and_subset(envelope):
    payload = dict(timestamp=1000, price_changes=[
        dict(asset_id='a', side='BUY', price='.4', size='2'),
        dict(asset_id='b', side='SELL', price='.6', size='0')])
    wire = dict(type='price_change', payload=payload) if envelope else dict(event_type='price_change', **payload)
    parsed = ws_payloads.ParsedFrames().parse(json.dumps(wire))
    merge = FeedMerge({})
    assert merge.filter(parsed, 'primary', {'a', 'b'}) is parsed
    assert not merge.filter(parsed, 'backup', {'a', 'b'}, backup=True)
    subset = FeedMerge({}).filter(parsed, 'other', {'a'})
    books = {'a': {'bids': {}, 'asks': {}}, 'b': {'bids': {}, 'asks': {'.6': '9'}}}
    assert observer.ws_apply_event(subset, books) == ['a']
    assert books['b']['asks']['.6'] == '9'
    observer.ws_apply_event(parsed, books)
    assert books['a']['bids']['.4'] == '2' and not books['b']['asks']
    assert len((parsed.get('payload') or parsed)['price_changes']) == 2


def test_history_cached_oldest_exactly_matches_scan_reference():
    class Reference(BookHistory):
        def trim_bytes(self):
            while self.charged_bytes > self.byte_limit and self.assets:
                asset, state = min(self.assets.items(), key=lambda item:
                    item[1]['events'][0][0] if item[1]['events'] else item[1]['base_at'])
                if state['events']:
                    self.pop(state)
                else:
                    self.forget(asset)
                self.byte_evictions += 1
    rng = random.Random(18)
    current, reference = BookHistory(maximum=30, byte_limit=12000), Reference(maximum=30, byte_limit=12000)
    for i in range(3000):
        asset = str(rng.randrange(12))
        at = i//8 - (30 if i % 67 == 0 else 0)
        event = book(asset, str(i), at=1000+at)
        if i % 5:
            event = dict(event_type='price_change', timestamp=1000+at, price_changes=[
                dict(asset_id=asset, price='.4', size=str(i % 7), side='SELL') for _ in range(4)])
        for history in (current, reference):
            history.record(event, at)
            if i % 113 == 0:
                history.forget(str((int(asset)+1) % 12))
            if i % 257 == 0:
                history.prune(at)
        assert current.metrics() == reference.metrics()
        assert current.copy_assets(current.assets) == reference.copy_assets(reference.assets)
        assert current.charged_bytes <= current.byte_limit
    current.prune(20000)
    assert not current.assets and current.oldest is None


@pytest.mark.parametrize('value', [None, '', {}, [], 1, '1000', '2026-10-18T10:00:00Z', 'invalid'])
def test_timestamp_cache_keeps_parser_contract(value):
    assert recording.exchange_time(value) == recording._parse_exchange_time(value)


def test_sampling_jitter_does_not_block_other_connection_or_falsify_ready(feed_manager):
    m, _, sockets = feed_manager
    c = ready_backup(m, sockets)
    for state in (m.states[0], c):
        state['socket'].transport = SimpleNamespace(last=dict(scheduler_lag_seconds=5))
    assert not SubscriptionPacer.pressured(c)
    assert m.backup_ready(c)
    with m.lifecycle_lock:
        m.states[0]['inbox'].processing_received_at = datetime.now(timezone.utc)-timedelta(seconds=.4)
        assert SubscriptionPacer.pressured(m.states[0])
        assert not SubscriptionPacer.pressured(c) and m.backup_ready(c)
        m.states[0]['inbox'].processing_received_at = None


@pytest.mark.parametrize('blocker', ['queue', 'missing_book', 'missing_time', 'resync'])
def test_bootstrap_waits_for_own_primary_catchup(feed_manager, blocker):
    m, p, _ = feed_manager
    make_hot(m)
    state = m.states[0]
    with m.lifecycle_lock:
        if blocker == 'queue':
            state['inbox'].processing_received_at = datetime.now(timezone.utc)-timedelta(seconds=6.4)
        elif blocker == 'missing_book':
            p.books.pop('ay')
        elif blocker == 'missing_time':
            p.recording.metadata['ay'] = (None, datetime.now(timezone.utc), 'ws')
        else:
            state['inbox'].resync.set()
        assert not m.primary_caught_up(0, state)
        m.redundancy_tick(time.monotonic())
        assert not m.backups


def test_short_lag_drains_without_discard_or_recovery_http(feed_manager, monkeypatch):
    m, p, sockets = feed_manager
    c = ready_backup(m, sockets)
    calls = []
    monkeypatch.setattr(m.snapshot_recovery, 'request', lambda *a, **kw: calls.append(kw) or True)
    inbox, shadow = c['inbox'], c['shadow']
    received = datetime.now(timezone.utc)-timedelta(seconds=.4)
    # Queued work has a real old receipt but no data loss or epoch change.
    event = json.dumps(book('ay', '9'))
    with m.lifecycle_lock, inbox.lock:
        epoch = inbox.epoch
        inbox.queue.put_nowait((event, received, epoch))
    wait_for(lambda: inbox.queue.empty() and inbox.processing_received_at is None)
    assert c['shadow'] is shadow and inbox.epoch == epoch
    assert inbox.resyncs == inbox.dropped == 0
    m.tick()
    assert not calls
    sockets[1].emit([book('ay', '10'), book('an', '10')])
    wait_for(lambda: m.backup_ready(c) and p.books['ay']['asks']['.4'] == '10')
    assert p.books['ay']['asks']['.4'] == '10'


def test_replenishment_does_not_wait_for_rollover_primary_snapshot(feed_manager, monkeypatch):
    m, p, sockets = feed_manager
    monkeypatch.setenv('POLYMARKET_OBSERVER_WS_HOT_BACKUP_COUNT', '1')
    c = ready_backup(m, sockets)
    sockets[1].callbacks['on_close'](sockets[1], 1013, 'slow consumer')
    with m.lifecycle_lock:
        m.redundancy_tick(time.monotonic())
        p.books.clear()
        m.states[0]['backup_retry_at'] = 0
        m.backup_next_connect_at = 0
        assert not m.primary_caught_up(0, m.states[0])
        m.redundancy_tick(time.monotonic())
    wait_for(lambda: m.backups.get(0) is not None and m.backups[0] is not c)
    assert sockets[2].opened.wait(1)


def test_redial_preserves_runtime_replenishment_instead_of_restarting_bootstrap(feed_manager, monkeypatch):
    m, p, sockets = feed_manager
    monkeypatch.setenv('POLYMARKET_OBSERVER_WS_HOT_BACKUP_COUNT', '1')
    ready_backup(m, sockets)
    old = m.states[0]
    sockets[1].callbacks['on_close'](sockets[1], 1013, 'slow consumer')
    with m.lifecycle_lock:
        m.redundancy_tick(time.monotonic())
    sockets[0].callbacks['on_close'](sockets[0], 1013, 'slow consumer')
    old['retry_at'] = 0
    m.tick()
    assert sockets[2].opened.wait(1)
    assert m.states[0] is not old and m.states[0]['backup_started']
    assert not p.books
    with m.lifecycle_lock:
        m.backup_next_connect_at = 0
        m.states[0]['backup_retry_at'] = 0
        m.tick()
    assert sockets[3].opened.wait(1)
    assert m.backups[0]['connected']


def test_incident_includes_detection_silence_and_phase_logs_cannot_shrink_it(caplog):
    c = FeedContinuity()
    now = datetime.now(timezone.utc)
    before = now-timedelta(milliseconds=2019)
    c.publication({'a': (before, before, 'ws'), 'b': (before, before, 'ws')}, {'a', 'b'})
    event = {}
    c.begin_incident(event, {'a': before, 'b': before}, {'a', 'b'}, now)
    with caplog.at_level(logging.INFO):
        # Backlogged updates advance receipt cadence but cannot erase silence.
        c.publication({'a': (now, now-timedelta(milliseconds=300), 'ws')}, {'a'})
        assert not event['merged_gap_final']
        c.publication({'a': (now, now+timedelta(milliseconds=24), 'ws')}, {'a'})
        assert not event['merged_gap_final']
        c.publication({'b': (now, now+timedelta(milliseconds=553), 'ws')}, {'b'})
    assert event['merged_gap_final'] and event['merged_gap_ms'] == 2572
    rows = [json.loads(r.message.split('business gap ', 1)[1]) for r in caplog.records if 'business gap ' in r.message]
    assert len(rows) == 2 and {r['incident_id'] for r in rows} == {event['incident_id']}
    assert max(r['merged_gap_ms'] for r in rows) == event['merged_gap_ms']
    c.publication({'a': (now, now+timedelta(seconds=30), 'ws')}, {'a'})
    assert event['merged_gap_ms'] == 2572 and not c.incidents


def test_incidents_are_bounded_and_missing_baseline_never_passes():
    c = FeedContinuity();now = datetime.now(timezone.utc)
    events = [{} for _ in range(100)]
    for event in events:
        c.begin_incident(event, {}, {'a'}, now)
    assert len(c.incidents['a']) == 64
    c.publication({'a': (now, now, 'ws')}, {'a'})
    assert not events[-1]['merged_gap_final'] and events[-1]['merged_gap_ms'] is None
    c.prune(set())
    assert not c.incidents


def test_failure_registers_incident_before_fast_rest_and_before_pipeline_lock(feed_manager, monkeypatch):
    m, p, _ = feed_manager;make_hot(m)
    launched = threading.Event()
    def request(*args, **kwargs):
        assert m.continuity.incidents
        launched.set()
        return True
    monkeypatch.setattr(m.snapshot_recovery, 'request', request)
    with p.lock:
        t = threading.Thread(target=m.record_primary_failure, args=(0, m.states[0], 1013))
        t.start()
        assert launched.wait(1)
    t.join(1)
    assert not t.is_alive()


def test_optional_rest_warmup_waits_for_primary_catchup(feed_manager, monkeypatch):
    m, _, _ = feed_manager;make_hot(m)
    monkeypatch.setenv('POLYMARKET_OBSERVER_WS_REST_WARM_SECONDS', '60')
    calls = []
    monkeypatch.setattr(m.snapshot_recovery, 'request', lambda *a, **kw: calls.append(kw) or True)
    state = m.states[0]
    with m.lifecycle_lock:
        state['inbox'].processing_received_at = datetime.now(timezone.utc)-timedelta(seconds=.3)
        state['rest_warm_at'] = 0
        m.background_snapshot_tick(time.monotonic())
        assert not calls
        state['inbox'].processing_received_at = None
        m.background_snapshot_tick(time.monotonic())
        assert calls == [dict(warm=True)]


def test_interrupt_keeps_tls_object_and_fd_until_reader_teardown():
    class OwnedTLS(ssl.SSLSocket):
        def shutdown(self, how):
            pytest.fail('control thread destroyed TLS object')
        def close(self):
            pytest.fail('control thread closed reusable descriptor')
    raw, peer = socket.socketpair()
    # No handshake is needed to prove syscall dispatch/descriptor ownership.
    fd = raw.detach()
    tls = OwnedTLS.__new__(OwnedTLS)
    socket.socket.__init__(tls, fileno=fd)
    tls._sslobj = object()
    app = SimpleNamespace(sock=SimpleNamespace(sock=tls), keep_running=True, stop_ping=threading.Event())
    try:
        original = tls._sslobj
        ws_transport.interrupt_reader(app)
        assert tls._sslobj is original and tls.fileno() == fd
        assert app.stop_ping.is_set() and not app.keep_running
        assert peer.recv(1) == b''
        # The descriptor remains owned and cannot be assigned to a new dial.
        with socket.socket() as next_connection:
            assert next_connection.fileno() != fd
    finally:
        socket.socket.close(tls)
        peer.close()


def test_promoted_reader_teardown_does_not_touch_new_backup(feed_manager, monkeypatch):
    m, _, sockets = feed_manager
    monkeypatch.setenv('POLYMARKET_OBSERVER_WS_HOT_BACKUP_COUNT', '1')
    c = ready_backup(m, sockets)
    old = m.states[0]
    sockets[0].callbacks['on_close'](sockets[0], 1013, 'slow consumer')
    wait_for(lambda: m.states[0] is c)
    failure = m.primary_failures['up_down:BTC:5m']['events'][-1]
    assert failure['ready_backups'] == 1
    with m.lifecycle_lock:
        m.backup_next_connect_at = 0
        m.tick()
    wait_for(lambda: len(sockets) == 3 and sockets[2].opened.is_set())
    sockets[0].callbacks['on_error'](sockets[0], ssl.SSLError('WRONG_VERSION_NUMBER'))
    sockets[0].callbacks['on_close'](sockets[0], None, 'late TLS teardown')
    assert old['retired'] and c['connected'] and m.backups[0]['connected']
    assert m.backups[0]['inbox'] is not old['inbox']


def test_ssl_failure_evidence_distinguishes_upgrade_from_subscribed(feed_manager, monkeypatch):
    m, _, sockets = feed_manager
    c = ready_backup(m, sockets)
    c.pop('opened_at', None)
    c.pop('initial_sent_at', None)
    sockets[1].callbacks['on_error'](sockets[1], ssl.SSLError('WRONG_VERSION_NUMBER'))
    evidence = c['last_transport_failure']
    assert evidence['transport_phase'] == 'tls_or_http_upgrade'
    assert evidence['code'] is None and 'WRONG_VERSION_NUMBER' in evidence['reason']
    assert m.states[0]['connected'] and not m.primary_failures


def test_old_generation_is_fenced_before_promoted_state_is_visible(feed_manager, monkeypatch):
    m, _, sockets = feed_manager
    c = ready_backup(m, sockets)
    old = m.states[0]
    entered, release = threading.Event(), threading.Event()
    original = m.update_hot_coverage
    def delayed(now):
        if m.states[0] is c:
            entered.set()
            assert release.wait(2)
        return original(now)
    monkeypatch.setattr(m, 'update_hot_coverage', delayed)
    try:
        sockets[0].callbacks['on_close'](sockets[0], 1013, 'slow consumer')
        assert entered.wait(1)
        assert m.states[0] is c and old['retired'] and old['inbox'].invalid.is_set()
        sockets[0].callbacks['on_error'](sockets[0], ssl.SSLError('late old error'))
        assert c['connected']
    finally:
        release.set()


def test_bootstrap_prioritizes_btc5_and_limits_one_dial_per_admission(monkeypatch):
    monkeypatch.setattr(observer, '_PIPELINE', None)
    monkeypatch.setenv('POLYMARKET_OBSERVER_WS_HOT_BACKUP_COUNT', '1')
    monkeypatch.setenv('POLYMARKET_OBSERVER_WS_HOT_BACKUP_STAGGER_SECONDS', '0')
    m = observer.MarketConnections()
    now = time.monotonic()
    groups = ('up_down:BTC:15m', 'up_down:BTC:5m', 'up_down:ETH:5m', 'up_down:ETH:15m')
    m.states = {i: dict(assets=[f'{i}y', f'{i}n'], inbox=observer.ReceiveQueue(64),
        connected=True, subscribe_initial=False, opened_at=now-100, socket=None) for i in range(4)}
    m.balancer.set_groups({a: groups[i] for i, s in m.states.items() for a in s['assets']})
    m.balancer.assignment = {a: i for i, s in m.states.items() for a in s['assets']}
    connected = []
    def connect(index, state):
        connected.append(index)
        state.update(inbox=observer.ReceiveQueue(64), connected=True, ready=True,
            initial_sent_at=now, thread=SimpleNamespace(is_alive=lambda: True),
            worker=SimpleNamespace(is_alive=lambda: True))
        state['shadow'].receive([book(a) for a in state['assets']], datetime.now(timezone.utc))
    monkeypatch.setattr(m, 'connect', connect)
    try:
        m.redundancy_tick(now)
        assert connected == [1]
        m.redundancy_tick(now+.1)
        assert connected == [1]
        # One unrelated group is behind; the other primaries still get spares.
        m.states[2]['inbox'].processing_received_at = datetime.now(timezone.utc)-timedelta(seconds=6.4)
        m.redundancy_tick(now+1)
        assert connected == [1, 0]
        m.redundancy_tick(now+2)
        assert connected == [1, 0, 3]
        m.states[2]['inbox'].processing_received_at = None
        m.redundancy_tick(now+3)
        assert connected == [1, 0, 3, 2]
    finally:
        m.snapshot_recovery.close()
