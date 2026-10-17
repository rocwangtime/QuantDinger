"""Round 11: continuous spares at rollover and bounded observer resources."""
import gc
import json
import threading
import time
import weakref
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from app.services.polymarket import observer, updown, ws_payloads
from app.services.polymarket.ws_redundancy import FeedContinuity, FeedMerge, HotBackupCoverage
from app.services.polymarket.ws_rotation import ShadowBooks
from tests.test_polymarket_ws_rotation import book, feed_manager, wait_for
from tests.test_polymarket_ws_round7 import ready_backup
from tests.test_polymarket_ws_round10 import second_backup


def freeze(monkeypatch, at):
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return at if tz else at.replace(tzinfo=None)
    monkeypatch.setattr(observer, 'datetime', Clock)


def rollover_setup(feed_manager, monkeypatch, key='up_down:BTC:5m', seconds=60):
    m, p, sockets = feed_manager
    first, reserve = second_backup(m, sockets)
    p.recording.enabled = False
    _, asset, interval = key.split(':')
    now = datetime.now(timezone.utc)
    end = now + timedelta(seconds=seconds)
    current = dict(id='a', market_type='up_down', asset=asset, interval=interval,
                   yesAssetId='ay', noAssetId='an', eventStartTime=(end - timedelta(minutes=5)).isoformat(),
                   endDate=end.isoformat())
    upcoming = dict(current, id='next', yesAssetId='ny', noAssetId='nn',
                    eventStartTime=end.isoformat(), endDate=(end + timedelta(minutes=15)).isoformat())
    m.sync([current], prewarm=[upcoming])
    return m, p, sockets, first, reserve, current, upcoming, end


def admit_prewarm(m, first, reserve):
    for _ in range(3):
        m.subscriptions.next_at = 0
        m.tick()
    assert not first['subscribe_pending'] and not reserve['subscribe_pending']


@pytest.mark.parametrize('key', sorted(observer.HOT_SERIES))
def test_prewarmed_rollover_keeps_both_sockets_ready_without_evaluating_future(feed_manager, monkeypatch, key):
    m, p, sockets, first, reserve, current, upcoming, end = rollover_setup(feed_manager, monkeypatch, key)
    admit_prewarm(m, first, reserve)
    for candidate in (first, reserve):
        candidate['socket'].emit([book('ny', '7'), book('nn', '7')])
    wait_for(lambda: all({'ny', 'nn'} <= c['shadow'].books.keys() for c in (first, reserve)))
    assert set(p.markets) == {'a'} and set(p.books) == {'ay', 'an'}
    assert all(set(json.loads(s.sent[0])['assets_ids']) == {'ay', 'an'} for s in sockets)
    before = len(sockets)
    freeze(monkeypatch, end + timedelta(milliseconds=1))
    m.tick()  # Discovery has not refreshed; the known next window activates.
    assert set(p.markets) == {'next'} and set(p.books) == {'ny', 'nn'}
    assert p.books['ny']['asks']['.4'] == '7'
    assert m.backups[0] is first and m.reserves[0] is reserve and len(sockets) == before
    assert m.update_hot_coverage(time.monotonic())[key]['ready_backup_count'] == 2
    for candidate in (first, reserve):
        assert m.backup_ready(candidate) and not candidate.get('retired')
        assert set(candidate['assets']) == set(candidate['shadow'].books) == {'ny', 'nn'}
        assert set(candidate['shadow'].merge.latest) == {'ny', 'nn'}
        assert all(k[0] in {'ny', 'nn'} for k in candidate['shadow'].merge.levels)
    assert set(m.merges[0].latest) <= {'ny', 'nn'}


def test_one_prewarmed_spare_covers_rollover_while_other_still_lacks_snapshot(feed_manager, monkeypatch):
    m, p, _, first, reserve, _, _, end = rollover_setup(feed_manager, monkeypatch)
    admit_prewarm(m, first, reserve)
    first['socket'].emit([book('ny'), book('nn')])
    wait_for(lambda: 'nn' in first['shadow'].books)
    freeze(monkeypatch, end + timedelta(milliseconds=1))
    m.tick()
    row = m.update_hot_coverage(time.monotonic())['up_down:BTC:5m']
    assert row['ready'] and row['ready_backup_count'] == 1
    assert m.backup_ready(first) and not m.backup_ready(reserve)
    assert set(p.books) == {'ny', 'nn'}


def test_prewarm_pending_subscription_does_not_remove_current_spare_coverage(feed_manager, monkeypatch):
    m, _, _, first, reserve, *_ = rollover_setup(feed_manager, monkeypatch)
    assert any(c.get('subscribe_pending') for c in (first, reserve))
    assert m.backup_ready(first) and m.backup_ready(reserve)
    assert m.update_hot_coverage(time.monotonic())['up_down:BTC:5m']['ready_backup_count'] == 2
    row = m.backup_metrics(0, reserve, time.monotonic())
    assert row['stage'] == 'ready' and row['pending_current'] == 0 and row['prewarm_assets'] == 2


def test_prewarm_is_bounded_to_nearest_window_and_last_90_seconds(feed_manager, monkeypatch):
    m, _, _, first, reserve, current, upcoming, end = rollover_setup(feed_manager, monkeypatch, seconds=120)
    assert first['assets'] == reserve['assets'] == ['ay', 'an']
    m.prewarm_markets.append(dict(upcoming, id='later', yesAssetId='ly', noAssetId='ln',
                                 eventStartTime=(end + timedelta(minutes=5)).isoformat()))
    freeze(monkeypatch, end - timedelta(seconds=89))
    m.tick()
    assert set(first['assets']) == set(reserve['assets']) == {'ay', 'an', 'ny', 'nn'}
    assert set(m.states[0]['assets']) == {'ay', 'an'}


def test_missing_next_catalogue_keeps_empty_sockets_for_discovery_grace(feed_manager, monkeypatch):
    m, p, sockets, first, reserve, _, upcoming, end = rollover_setup(feed_manager, monkeypatch)
    m.prewarm_markets = []
    freeze(monkeypatch, end + timedelta(milliseconds=1))
    m.tick()
    assert not p.books and not m.backup_ready(first) and not m.backup_ready(reserve)
    assert m.backups[0] is first and m.reserves[0] is reserve
    assert not first.get('retired') and not reserve.get('retired')
    at = time.monotonic()
    before = m.update_hot_coverage(at)['up_down:BTC:5m']['zero_ready_backup_seconds']
    row = m.update_hot_coverage(at + 10)['up_down:BTC:5m']
    assert row['zero_ready_backup_seconds'] == pytest.approx(before + 10)
    assert row['zero_ready_backup_active'] and not row['active']
    count = len(sockets)
    m.sync([upcoming])
    assert len(sockets) == count
    admit_prewarm(m, first, reserve)
    first['socket'].emit([book('ny'), book('nn')])
    wait_for(lambda: m.backup_ready(first))


def test_idle_backup_grace_expires_without_unbounded_empty_connections(feed_manager, monkeypatch):
    m, _, _, first, reserve, _, _, end = rollover_setup(feed_manager, monkeypatch)
    m.prewarm_markets = []
    freeze(monkeypatch, end + timedelta(milliseconds=1))
    m.tick()
    first['idle_since'] -= 91
    reserve['idle_since'] -= 91
    m.tick()
    assert first['retired'] and reserve['retired'] and not m.backup_items()


@pytest.mark.parametrize('admitted', [True, False])
def test_failover_during_prewarm_promotes_only_current_assets_and_unsubscribes_extra(feed_manager, monkeypatch, admitted):
    m, p, sockets, first, reserve, *_ = rollover_setup(feed_manager, monkeypatch)
    if admitted:
        admit_prewarm(m, first, reserve)
        first['socket'].emit([book('ny'), book('nn')])
        wait_for(lambda: 'nn' in first['shadow'].books)
    extra = {id(c): set(c['assets']) - {'ay', 'an'} - set(c['subscribe_pending']) for c in (first, reserve)}
    old = m.states[0]
    sockets[0].callbacks['on_close'](sockets[0], 1013, 'slow consumer')
    wait_for(lambda: m.states[0] is not old)
    promoted = m.states[0]
    assert set(promoted['assets']) == set(p.books) == {'ay', 'an'}
    m.tick()
    commands = [json.loads(raw) for raw in promoted['socket'].sent if raw != 'PING']
    if extra[id(promoted)]:
        assert any(c.get('operation') == 'unsubscribe' and set(c['assets_ids']) == extra[id(promoted)] for c in commands)
    assert set(promoted.get('subscribe_pending', [])) <= {'ay', 'an'}
    assert all(set(c['assets']) <= {'ay', 'an', 'ny', 'nn'} for _, c in m.backup_items())


def test_late_previous_window_rest_cannot_revive_expired_shadow_assets(feed_manager, monkeypatch):
    m, p, _, first, reserve, _, _, end = rollover_setup(feed_manager, monkeypatch)
    admit_prewarm(m, first, reserve)
    callbacks = []
    monkeypatch.setattr(m.snapshot_recovery, 'request', lambda state, assets, publish: callbacks.append(publish))
    m.request_snapshot(0, first, {'ay', 'an'})
    old_callback = callbacks[-1]
    for candidate in (first, reserve):
        candidate['socket'].emit([book('ny', '7'), book('nn', '7')])
    wait_for(lambda: all('nn' in c['shadow'].books for c in (first, reserve)))
    freeze(monkeypatch, end + timedelta(milliseconds=1))
    m.tick()
    old_callback([book('ay', '99'), book('an', '99')], end + timedelta(milliseconds=2))
    assert set(p.books) == set(first['shadow'].books) == {'ny', 'nn'}
    assert p.books['ny']['asks']['.4'] == '7'


def test_rest_or_supervisor_cannot_promote_a_worker_holding_an_unprocessed_delta(feed_manager, monkeypatch):
    m, p, sockets = feed_manager
    candidate = ready_backup(m, sockets)
    popped, release = threading.Event(), threading.Event()
    original = m.parsed_frames.parse
    def blocked(raw):
        if threading.current_thread() is candidate['worker']:
            popped.set()
            assert release.wait(2)
        return original(raw)
    monkeypatch.setattr(m.parsed_frames, 'parse', blocked)
    old = m.states[0]
    try:
        candidate['socket'].emit(book('ay', '9'))
        assert popped.wait(1)
        sockets[0].callbacks['on_close'](sockets[0], 1013, 'slow consumer')
        m.redundant_receive(0, candidate, [], datetime.now(timezone.utc), candidate['inbox'])
        assert m.states[0] is old
    finally:
        release.set()
    wait_for(lambda: m.states[0] is candidate)
    assert p.books['ay']['asks']['.4'] == '9'


def test_lagging_then_busy_fifo_recovers_without_empty_timeout_or_rest(feed_manager, monkeypatch):
    m, _, sockets = feed_manager
    candidate = ready_backup(m, sockets)
    calls, samples = [], []
    monkeypatch.setattr(m.snapshot_recovery, 'request', lambda *args: calls.append(args))
    original = m.redundant_receive
    def observe(*args):
        original(*args)
        samples.append((m.backup_ready(candidate), candidate['inbox'].queue.qsize()))
    monkeypatch.setattr(m, 'redundant_receive', observe)
    inbox = candidate['inbox']
    with m.lifecycle_lock:
        now = datetime.now(timezone.utc)
        raw = json.dumps([book('ay'), book('an')])
        inbox.queue.put((raw, now - timedelta(seconds=1.2), inbox.epoch))
        for _ in range(40):
            inbox.queue.put((raw, now, inbox.epoch))
    wait_for(lambda: any(ready and depth > 0 for ready, depth in samples))
    assert not inbox.delivery_gap and not inbox.resync.is_set() and not calls


@pytest.mark.parametrize('problem', ['overflow', 'invalid', 'missing', 'pending_current'])
def test_real_incomplete_backup_is_never_counted_ready(feed_manager, problem):
    m, _, sockets = feed_manager
    candidate = ready_backup(m, sockets)
    if problem == 'overflow':
        candidate['inbox'].resync.set()
    elif problem == 'invalid':
        candidate['inbox'].invalid.set()
    elif problem == 'missing':
        candidate['shadow'].books.pop('ay')
        m.redundant_receive(0, candidate, [], datetime.now(timezone.utc), candidate['inbox'])
    else:
        candidate['subscribe_pending'] = ['ay']
    assert not m.backup_ready(candidate)


def test_retired_generations_release_shadow_and_only_live_threads_remain_joinable():
    m = observer.MarketConnections()
    refs = []
    for _ in range(200):
        shadow = ShadowBooks(['expired'])
        refs.append(weakref.ref(shadow))
        state = dict(socket=None, thread=None, worker=None, shadow=shadow)
        m.retire(state)
        m.retire(state)
        assert len(m.retiring) == 1
        m.reap_retired()
        assert not m.retiring
    del state, shadow
    gc.collect()
    assert all(ref() is None for ref in refs)
    alive = [True]
    state = dict(socket=None, thread=SimpleNamespace(is_alive=lambda: alive[0]), worker=None)
    m.retire(state)
    m.reap_retired()
    assert m.retiring == [state]
    alive[0] = False
    m.reap_retired()
    assert not m.retiring


def test_many_rollovers_prune_all_shadow_and_merge_asset_watermarks():
    shadow, merge = ShadowBooks([]), FeedMerge({})
    for i in range(200):
        asset = str(i)
        shadow.prune({asset})
        event = book(asset)
        shadow.receive(event, datetime.now(timezone.utc))
        merge.filter(event, 'active', {asset})
        delta = dict(event_type='price_change', timestamp=event['timestamp'],
                     price_changes=[dict(asset_id=asset, side='SELL', price='.401', size='2')])
        shadow.receive(delta, datetime.now(timezone.utc))
        shadow.observe_active(shadow.recorder.metadata)
        merge.filter(delta, 'active', {asset})
        merge.prune({asset})
        for cache in (shadow.books, shadow.recorder.metadata, shadow.merge.latest,
                      shadow.merge.snapshots, shadow.minimum_times, merge.latest, merge.snapshots):
            assert set(cache) == {asset}
        assert all(key[0] == asset for cache in (shadow.merge.levels, merge.levels) for key in cache)
        assert len(shadow.merge.levels) == len(merge.levels) == 1
        assert not shadow.merge.seen
    merge.seen['old'] = (time.monotonic() - 61, {})
    merge.seen.move_to_end('old', last=False)
    merge.expire()
    assert 'old' not in merge.seen


def test_three_copies_share_parse_and_fingerprints_but_preserve_trade_multiplicity(monkeypatch):
    cache = ws_payloads.ParsedFrames()
    raw = json.dumps(dict(event_type='last_trade_price', asset_id='a', price='.5', size='2', timestamp='1000'))
    copies = [cache.parse(raw.encode()) for _ in range(3)]
    assert copies[0] is copies[1] is copies[2]
    calls = []
    original = ws_payloads.json.dumps
    def counted(*args, **kwargs):
        calls.append(1)
        return original(*args, **kwargs)
    monkeypatch.setattr(ws_payloads.json, 'dumps', counted)
    merge = FeedMerge({})
    for _ in range(2):
        assert merge.filter(copies[0], 'primary', {'a'})
        assert not merge.filter(copies[1], 'backup', {'a'})
        assert not merge.filter(copies[2], 'reserve', {'a'})
    assert len(calls) == 1
    assert cache.metrics()['hits'] == 2 and cache.metrics()['misses'] == 1


def test_parse_cache_has_count_byte_ttl_bounds_and_oversized_bypass():
    cache = ws_payloads.ParsedFrames(maximum=3, byte_limit=200, ttl=2)
    for i in range(100):
        cache.parse(json.dumps(dict(asset_id=str(i))))
        assert len(cache.items) <= 3 and cache.bytes <= 200
    assert cache.parse(json.dumps(dict(large='x' * 300)))['large'] == 'x' * 300
    assert len(cache.items) <= 3
    cache.prune(time.monotonic() + 3)
    assert not cache.items and cache.bytes == 0


def test_shared_parse_is_serialized_across_workers():
    cache = ws_payloads.ParsedFrames()
    events = []
    threads = [threading.Thread(target=lambda: events.append(cache.parse(b'{"asset_id":"a"}'))) for _ in range(16)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(1)
    assert len(events) == 16 and all(e is events[0] for e in events)
    assert cache.misses == 1 and cache.hits == 15


@pytest.mark.parametrize('interval,period', [('5m', 300), ('15m', 900)])
def test_zero_backup_seconds_integrate_exactly_across_rollover_and_primary_failure(interval, period):
    key = 'up_down:BTC:' + interval
    coverage = HotBackupCoverage({key})
    coverage.at = 0
    coverage.update({key: {0}}, set(), 0, {0: 0}, wall_now=period - 20)
    row = coverage.update({key: {0}}, {0}, 70, {0: 1}, wall_now=period + 50)[key]
    assert row['zero_ready_backup_seconds'] == 70
    assert row['zero_ready_by_phase_seconds'] == dict(before=15, after=45, steady=10)
    # Primary unavailable, but an independent ready spare still exists.
    coverage.update({key: {0}}, set(), 80, {0: 1}, wall_now=period + 60)
    row = coverage.update({}, set(), 90, {}, wall_now=period + 70)[key]
    assert row['zero_ready_backup_seconds'] == 70
    assert row['time_without_backup_seconds'] == 80
    row = coverage.update({}, set(), 190, {}, wall_now=period + 170)[key]
    assert row['zero_ready_backup_seconds'] == 70


def test_rss_reports_current_resident_pages_and_unsupported_is_null(monkeypatch):
    import io
    monkeypatch.setattr('builtins.open', lambda *args, **kwargs: io.StringIO('9999 12 0 0'))
    monkeypatch.setattr(observer.os, 'sysconf', lambda name: 4096)
    assert observer.process_rss_bytes() == 49152
    monkeypatch.setattr('builtins.open', lambda *args, **kwargs: io.StringIO('bad'))
    assert observer.process_rss_bytes() is None


def test_migration_silence_is_not_reported_as_book_unavailability():
    continuity = FeedContinuity()
    now = datetime.now(timezone.utc)
    at = int((now - timedelta(seconds=18.4)).timestamp() * 1000)
    # Different full-book representation gives no fingerprint overlap; both
    # paths still attest to exactly the same exchange-time watermark.
    continuity.receive(book('a', '1', at), 'old', now - timedelta(seconds=18.4))
    continuity.receive(book('a', '2', at), 'new', now - timedelta(seconds=3))
    assert continuity.switch('old', 'new', {'a'}, 'reassignment', book_preserved=True) == {'a'}
    row = continuity.metrics()['events'][-1]
    assert row['source_silence_ms'] >= 18400
    assert row['destination_exchange_gap_ms'] == 0 and row['market_silence_compatible']
    assert row['book_preserved'] and row['book_unavailable_ms'] == 0
    assert not row['historical_trades_verified']


def test_stale_destination_cannot_claim_preserved_or_silence_verified():
    continuity = FeedContinuity()
    now = datetime.now(timezone.utc)
    at = int(now.timestamp() * 1000)
    continuity.receive(book('a', at=at), 'old', now)
    continuity.receive(book('a', at=at - 1000), 'new', now)
    continuity.switch('old', 'new', {'a'}, 'reassignment', book_preserved=True)
    row = continuity.metrics()['events'][-1]
    assert not row['book_preserved'] and row['book_unavailable_ms'] is None
    assert not row['market_silence_compatible']


def test_discovery_prewarm_does_not_expand_evaluation_cap_or_lookahead(monkeypatch):
    from tests.test_polymarket_updown import BTC, NOW
    monkeypatch.setenv('POLYMARKET_OBSERVER_UPDOWN_ASSETS', 'BTC')
    monkeypatch.setenv('POLYMARKET_OBSERVER_UPDOWN_INTERVALS', '15m')
    monkeypatch.setenv('POLYMARKET_OBSERVER_UPDOWN_PER_PAIR_LIMIT', '1')
    monkeypatch.setenv('POLYMARKET_OBSERVER_UPDOWN_MAX_MARKETS', '1')
    monkeypatch.setenv('POLYMARKET_OBSERVER_UPDOWN_LOOKAHEAD_SECONDS', '0')
    start = updown.timestamp(BTC['endDate'])
    upcoming = dict(BTC, id='next', conditionId='0x' + 'a' * 64, clobTokenIds='["123","456"]',
                    eventStartTime=start.isoformat(), endDate=(start + timedelta(minutes=15)).isoformat())
    class Client:
        def _json(self, method, url, **kwargs):
            if url.endswith('/series'):
                return [dict(id='series', slug='btc-up-or-down-15m')]
            return [dict(seriesSlug='btc-up-or-down-15m', markets=[BTC, upcoming])]
    warm = []
    markets, _ = updown.discover(Client(), {}, now=NOW, prewarm=warm)
    assert [m['id'] for m in markets] == [BTC['id']]
    assert [m['id'] for m in warm] == ['next']
