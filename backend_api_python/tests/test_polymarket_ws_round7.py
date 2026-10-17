"""Round 7: permanent hot fan-in, continuity, expiry and byte capacity."""
import json
import time
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from app.services.polymarket import observer, ws_transport
from app.services.polymarket.ws_balance import balance_key
from app.services.polymarket.ws_redundancy import FeedContinuity
from tests.test_polymarket_ws_rotation import book, feed_manager, wait_for


def make_hot(m, key='up_down:BTC:5m'):
    m.balancer.set_groups({a: key for a in m.states[0]['assets']})
    m.states[0]['opened_at'] -= 121


def ready_backup(m, sockets):
    make_hot(m)
    m.tick()
    assert sockets[1].opened.wait(1)
    candidate = m.backups[0]
    event = [book('ay'), book('an')]
    sockets[0].emit(event)
    sockets[1].emit(event)
    wait_for(lambda: candidate.get('ready'))
    wait_for(lambda: all(m.continuity.latest[a].get(m.states[0]['source'], (None, None))[1]
                         == m.continuity.latest[a].get(candidate['source'], (None, None))[1]
                         for a in ('ay', 'an')))
    return candidate


@pytest.mark.parametrize('key', sorted(observer.HOT_SERIES))
def test_all_four_series_have_backups_without_rate_thresholds(feed_manager, key):
    m, _, sockets = feed_manager
    make_hot(m, key)
    assert not m.busy(0, m.states[0])
    m.tick()
    assert sockets[1].opened.wait(1)
    assert m.backups[0]['opened_at'] - m.states[0]['opened_at'] >= 90


def test_cold_shard_has_no_backup_even_when_extremely_busy(feed_manager, monkeypatch):
    m, _, sockets = feed_manager
    monkeypatch.delenv('POLYMARKET_OBSERVER_WS_COLD_ROTATION_ENABLED')
    m.states[0]['opened_at'] -= 5000
    m.balancer.rates = {'market:a': 5000}
    m.tick()
    assert len(sockets) == 1 and not m.backups and not m.standby


def test_hot_warmup_survives_pressure_missing_pong_and_long_timeout(feed_manager):
    m, p, sockets = feed_manager
    make_hot(m)
    m.tick()
    assert sockets[1].opened.wait(1)
    candidate = m.backups[0]
    candidate['created_at'] -= 1000
    sockets[1].transport = ws_transport.TransportDiagnostics()
    sockets[1].transport.last = dict(tcp_pending_bytes=100000, pongs=0, pong_age_seconds=100)
    m.tick()
    assert m.backups[0] is candidate and not candidate.get('retired')
    # Trades enter the shared stream even before all snapshots are present.
    p.recording.enabled = True
    before = p.messages
    sockets[1].emit(dict(event_type='last_trade_price', asset_id='ay', price='.4', size='1', side='BUY',
                        timestamp=int(datetime.now(timezone.utc).timestamp() * 1000)))
    wait_for(lambda: p.messages > before)
    sockets[1].emit([book('ay'), book('an')])
    wait_for(lambda: candidate.get('ready'))
    assert not m.rotation_failures


def test_idle_ready_backup_takes_over_within_half_second_and_keeps_books(feed_manager):
    m, p, sockets = feed_manager
    candidate = ready_backup(m, sockets)
    old = m.states[0]
    books = dict(p.books)
    started = time.monotonic()
    sockets[0].callbacks['on_close'](sockets[0], 1013, 'slow consumer: send buffer full')
    wait_for(lambda: m.states[0] is candidate, .5)
    assert time.monotonic() - started < .5
    assert p.books == books and m.failover_ms < 500
    rows = m.continuity.metrics()['events']
    assert len(rows) == 2 and all(r['gap_ms'] == 0 and r['status'] == 'overlap' for r in rows)
    assert old['retired']


def test_mature_survivor_replenishes_immediately_after_failover(feed_manager):
    m, _, sockets = feed_manager
    candidate = ready_backup(m, sockets)
    m.states[0]['opened_at'] -= 121
    candidate['opened_at'] -= 121
    m.start_supervisor()  # The global initial-subscription slot may still be occupied.
    sockets[0].callbacks['on_close'](sockets[0], 1013, 'slow consumer')
    wait_for(lambda: m.states[0] is candidate)
    wait_for(lambda: len(sockets) == 3)
    assert sockets[2].opened.wait(1)
    assert m.backups[0]['opened_at'] - candidate['opened_at'] >= 90


def test_repeated_hot_backup_failures_never_use_minutes_of_backoff(feed_manager, monkeypatch):
    m, _, sockets = feed_manager
    candidate = ready_backup(m, sockets)
    monkeypatch.setattr(observer.random, 'uniform', lambda low, high: high)
    for _ in range(12):
        m.rotation_failed(0, m.states[0], candidate, 'wire_failure')
        assert m.rotation_last_failure['retry_seconds'] <= 30
    assert m.rotation_last_failure['retry_seconds'] == 30


def test_uncertain_switch_resubscribes_and_records_snapshot_gap(feed_manager):
    m, _, sockets = feed_manager
    candidate = ready_backup(m, sockets)
    # No comparable overlap evidence: force the conservative recovery path.
    m.continuity.history.clear()
    m.check_continuity(0, m.states[0], candidate, 'test_switch')
    assert m.continuity.metrics()['pending_assets'] == ['an', 'ay']
    m.tick()
    commands = [json.loads(raw) for raw in sockets[1].sent if raw != 'PING']
    assert any(c.get('operation') == 'unsubscribe' for c in commands)
    m.subscriptions.next_at = 0
    m.tick()
    assert any(json.loads(raw).get('operation') == 'subscribe' for raw in sockets[1].sent if raw != 'PING')
    sockets[1].emit([book('ay'), book('an')])
    wait_for(lambda: not m.continuity.pending)
    restored = [r for r in m.continuity.metrics()['events'] if r['status'] == 'snapshot_restored']
    assert len(restored) == 2 and all(r['gap_ms'] >= 0 for r in restored)
    assert all(r['historical_trades_recovered'] is False for r in restored)


def test_continuity_rejects_old_snapshot_and_bounds_history():
    c = FeedContinuity()
    now = datetime.now(timezone.utc)
    at = int(now.timestamp() * 1000)
    c.receive(book('a', at=at), 'primary', now)
    assert c.switch('primary', None, ['a'], 'reconnect') == {'a'}
    c.receive(book('a', at=at - 1), 'new', now)
    assert c.pending
    c.receive(book('a', at=at + 3540), 'new', now + timedelta(milliseconds=3540))
    assert not c.pending and c.metrics()['events'][-1]['gap_ms'] == 3540
    for i in range(600):
        c.receive(book('a', at=at + i), 'new', now)
        c.switch('absent', None, ['a'], 'reconnect')
    assert len(c.records) == 128 and len(c.history['a']) == 512
    c.prune(set())
    assert not c.history and not c.latest and not c.pending


def test_expiry_unsubscribes_without_discovery_and_preserves_backup(feed_manager):
    m, p, sockets = feed_manager
    candidate = ready_backup(m, sockets)
    # Include a retained market on the same shard; expiring assets must leave
    # both sockets without rebuilding the surviving standby.
    m.markets[0].update(market_type='up_down', asset='BTC', interval='5m')
    p.recording.enabled = False
    market = dict(id='b', yesAssetId='by', noAssetId='bn', market_type='up_down', asset='BTC', interval='5m')
    p.set_markets(m.markets + [market])
    m.sync(m.markets + [market])
    assert m.backups[0] is candidate
    m.markets[0] = {**m.markets[0], 'endDate': (datetime.now(timezone.utc) - timedelta(seconds=1)).isoformat()}
    m.tick()
    assert set(m.balancer.assignment) == {'by', 'bn'}
    assert 'ay' not in p.asset_markets and 'ay' not in p.books
    for socket in sockets[:2]:
        assert any(json.loads(raw).get('operation') == 'unsubscribe' and
                   'ay' in json.loads(raw)['assets_ids'] for raw in socket.sent if raw != 'PING')


def test_byte_overloaded_btc_pair_stays_together_and_excess_is_visible(monkeypatch):
    monkeypatch.setenv('POLYMARKET_OBSERVER_WS_SHARDS', '1')
    m = observer.MarketConnections()
    m.markets = [dict(id='btc', market_type='up_down', asset='BTC', interval='5m',
                      yesAssetId='y', noAssetId='n')]
    key = balance_key(m.markets[0])
    m.balancer.set_groups(dict(y=key, n=key), now=0)
    m.balancer.rates = {key: 419}
    m.balancer.byte_rates = {key: 274000}
    m.balancer.measured = {key}
    owners = m.capacity_owners()
    assignment = m.assignment_for(owners)
    assert assignment['y'] == assignment['n']
    assert m.count == 1  # More sockets cannot divide this market's load.
    assert m.balancer.capacity_cost(owners) == (0, 119, 11856)
    assert m.balancer.groups == dict(y=key, n=key)


def test_delivery_latency_is_message_weighted_and_separate_from_silence(feed_manager):
    _, p, _ = feed_manager
    now = datetime.now(timezone.utc)
    p.recording.metadata.pop('ay')
    p.receive(book('ay', at=int((now - timedelta(seconds=100)).timestamp() * 1000)),
              received_at=now - timedelta(seconds=99.9))
    p.recording.delivery_samples.clear()
    p.receive(book('an', at=int((now - timedelta(milliseconds=100)).timestamp() * 1000)), received_at=now)
    observer.STATUS['ws_group_rates'] = {'market:a': 1}
    metrics = p.metrics()
    cold = metrics['book_freshness_by_activity']['cold']
    delivery = metrics['ws_delivery_latency_by_activity']['cold']
    assert cold['update_silence_p95_ms'] >= 99000
    assert 100 <= delivery['p95_ms'] < 200 and delivery['within_1s']
    assert cold['updated_within_30s_tokens'] == 1


def test_failed_source_cannot_falsely_complete_snapshot_recovery():
    c = FeedContinuity()
    now = datetime.now(timezone.utc)
    event = book('a')
    c.receive(event, 'primary', now)
    c.switch('primary', None, ['a'], 'reconnect')
    c.receive(event, 'primary', now)
    assert c.pending_assets() == {'a'}
    c.receive(event, 'new-generation', now)
    assert not c.pending_assets()


def test_hot_data_collapse_promotes_existing_backup_without_warmup_abort(feed_manager, monkeypatch):
    m, _, sockets = feed_manager
    candidate = ready_backup(m, sockets)
    health = m.states[0]['data_health']
    monkeypatch.setattr(health, 'sample', lambda now, transport: True)
    m.tick()
    wait_for(lambda: m.states[0] is candidate, .5)
    assert m.rotation_failures == 0


def test_old_overlap_cannot_hide_an_unseen_final_primary_update():
    c = FeedContinuity()
    now = datetime.now(timezone.utc)
    at = int(now.timestamp() * 1000)
    shared = book('a', at=at)
    c.receive(shared, 'primary', now)
    c.receive(shared, 'backup', now)
    c.receive(book('a', size='2', at=at + 1), 'primary', now)
    c.receive(book('a', size='3', at=at + 2), 'backup', now)
    assert c.switch('primary', 'backup', ['a'], 'failover') == {'a'}
    assert c.metrics()['events'][-1]['status'] == 'unverified'


def test_ingress_counts_utf8_bytes_not_characters(feed_manager):
    m, _, sockets = feed_manager
    inbox = m.states[0]['inbox']
    before = inbox.received_bytes
    raw = json.dumps(dict(event_type='unknown', asset_id='ay', label='行情'), ensure_ascii=False)
    sockets[0].emit(raw)
    assert inbox.received_bytes - before == len(raw.encode('utf-8'))


def test_pre_subscription_backup_snapshot_cannot_complete_recovery(feed_manager):
    m, _, sockets = feed_manager
    candidate = ready_backup(m, sockets)
    m.continuity.history.clear()
    m.check_continuity(0, m.states[0], candidate, 'test_switch')
    received = datetime.now(timezone.utc) - timedelta(seconds=1)
    candidate['inbox'].activated = {a: datetime.now(timezone.utc) for a in candidate['assets']}
    m.redundant_receive(0, candidate, [book('ay'), book('an')], received, candidate['inbox'])
    assert m.continuity.pending_assets() == {'ay', 'an'}


def test_trade_fan_in_has_no_missing_or_duplicate_prints_across_close(feed_manager):
    m, p, sockets = feed_manager
    candidate = ready_backup(m, sockets)
    p.recording.pending.clear()
    at = int(datetime.now(timezone.utc).timestamp() * 1000)
    trades = [dict(event_type='last_trade_price', asset_id='ay', price='.4', size=str(i + 1),
                   side='BUY', timestamp=at + i) for i in range(100)]
    sockets[0].emit(trades[:20])
    sockets[1].emit(trades[:20])
    wait_for(lambda: len(p.recording.pending) == 20)
    sockets[0].callbacks['on_close'](sockets[0], 1013, 'slow consumer: send buffer full')
    sockets[1].emit(trades[20:])
    wait_for(lambda: m.states[0] is candidate, .5)
    wait_for(lambda: len(p.recording.pending) == 100)
    assert all(row[1] == 'trade' for row in p.recording.pending)
    assert m.merges[0].duplicates >= 20
