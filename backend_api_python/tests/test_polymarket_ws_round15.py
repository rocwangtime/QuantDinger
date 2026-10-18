"""Round15 public observer throughput, heartbeat liveness and separate gap scopes."""
import json
import random
import sys
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from app.commands import polymarket_observer as command
from app.services.polymarket import observer, recording, ws_payloads
from app.services.polymarket.ws_redundancy import FeedMerge, FeedContinuity
from app.services.polymarket.ws_rotation import ShadowBooks
from app.services.polymarket.ws_transport import TransportDiagnostics
from tests.test_polymarket_ws_rotation import book, feed_manager, wait_for
from tests.test_polymarket_ws_round7 import ready_backup, make_hot


def delta(at=1000, size='2', asset='ay', n=1):
    return dict(event_type='price_change', timestamp=at, price_changes=[
        dict(asset_id=asset, side='SELL', price=str((400+i)/1000), size=size) for i in range(n)])


@pytest.mark.parametrize('fallback', [False, True])
@pytest.mark.parametrize('wire', [str, bytes, bytearray])
def test_optional_decoder_preserves_complete_books_and_rows(monkeypatch, fallback, wire):
    if fallback:
        monkeypatch.setattr(ws_payloads, 'orjson', None)
    event = [book('ay', '3', 1000), delta(n=4), dict(event_type='unknown', message='行情')]
    raw = json.dumps(event, ensure_ascii=False)
    raw = raw if wire is str else wire(raw.encode())
    cache = ws_payloads.ParsedFrames()
    parsed = cache.parse(raw)
    assert parsed == event and cache.parse(raw) is parsed
    live = {};observer.ws_apply_event(parsed, live)
    assert len(live['ay']['asks']) == 5
    assert cache.metrics()['charged_bytes'] > len(raw)


@pytest.mark.parametrize('fallback', [False, True])
def test_invalid_json_never_enters_parse_cache(monkeypatch, fallback):
    if fallback:
        monkeypatch.setattr(ws_payloads, 'orjson', None)
    cache = ws_payloads.ParsedFrames()
    with pytest.raises(ValueError):
        cache.parse('{"bids":')
    assert cache.metrics()['entries'] == 0


def test_exact_copy_hint_preserves_split_rows_multiplicity_and_eviction():
    cache = ws_payloads.ParsedFrames()
    fast, reference = FeedMerge({}), FeedMerge({})
    messages = [('p', delta(n=4)), ('b', delta(n=4)), ('b', delta(n=2)),
                ('p', delta(n=2)), ('p', delta(n=4)), ('b', delta(n=4))]
    for source, event in messages:
        raw = json.dumps(event)
        assert fast.filter(cache.parse(raw), source, {'ay'}) == reference.filter(json.loads(raw), source, {'ay'})
    event = cache.parse(json.dumps(delta(2000, n=4)))
    fast.filter(event, 'p', {'ay'})
    fast.seen.clear()  # A parser hint may outlive the merge's bounded entries.
    assert fast.filter(event, 'b', {'ay'})
    for i in range(10):
        fast.filter(event, f'feed-{i}', {'ay'})
    assert all(len(counts) <= 4 for _, counts in fast.seen.values())


def test_random_fanin_matches_reference_without_shared_hints():
    rng = random.Random(15)
    cache = ws_payloads.ParsedFrames()
    fast, reference = FeedMerge({}), FeedMerge({})
    books = [{}, {}]
    for i in range(1500):
        t = 1000 + rng.randrange(40)
        event = (book('ay', str(i % 5), t) if i % 13 == 0 else delta(t, str(i % 5), n=rng.randrange(1, 5)))
        source = rng.choice(['p', 'b', 'r'])
        a = fast.filter(cache.parse(json.dumps(event)), source, {'ay'}, backup=source != 'p')
        b = reference.filter(json.loads(json.dumps(event)), source, {'ay'}, backup=source != 'p')
        observer.ws_apply_event(a, books[0]);observer.ws_apply_event(b, books[1])
        assert books[0] == books[1]


def test_light_shadow_keeps_stale_fences_deletions_and_unknown_timestamps():
    shadow = ShadowBooks({'ay'})
    now = datetime.now(timezone.utc)
    shadow.receive(book('ay', '5', 1000), now)
    shadow.receive(delta(1200, '7'), now)
    shadow.receive(delta(1100, '99'), now)
    shadow.receive(book('ay', '99', 1100), now)
    assert shadow.books['ay']['asks']['0.4'] == '7'
    shadow.receive(delta(1201, '0'), now)
    assert '0.4' not in shadow.books['ay']['asks']
    shadow.receive(book('other', '99', 2000), now)
    assert set(shadow.books) == {'ay'}
    assert not shadow.recorder.delivery_samples and not shadow.recorder.pending and not shadow.merge.seen
    assert shadow.recorder.metadata['ay'][0].timestamp() == 1.201


@pytest.mark.parametrize('last_read,last_callback,expired', [(None, None, True), (11, None, False),
    (None, 11, False), (10, 10, True), (32, None, False)])
def test_pong_timeout_requires_pending_ping_and_no_bytes_for_22_seconds(last_read, last_callback, expired):
    t = TransportDiagnostics()
    assert not t.pong_timed_out(100)
    t.ping(10);t.ping(20);t.ping(30)
    t.last_read_at = last_read
    assert t.pong_timed_out(32, last_callback) == expired
    assert t.pending_ping_at == 10
    t.pong()
    assert not t.pong_timed_out(1000)


def expire_primary(m, sock):
    now = time.monotonic()
    sock.transport = TransportDiagnostics()
    sock.transport.ping(now - 23)
    sock.transport.last_read_at = now - 23
    m.states[0]['inbox'].last_byte_at = now - 23
    m.pong_checked_at = 0


def test_pong_timeout_promotes_unready_backup_preserves_merged_books_and_replenishes(feed_manager, monkeypatch):
    m, p, sockets = feed_manager
    monkeypatch.setenv('POLYMARKET_OBSERVER_WS_HOT_BACKUP_COUNT', '1')
    candidate = ready_backup(m, sockets)
    old = m.states[0]
    # This feed is supplying data but lacks a complete private snapshot.
    with m.lifecycle_lock, candidate['shadow'].lock:
        candidate['shadow'].recorder.metadata.pop('an')
        candidate['ready'] = False
    assert not m.backup_ready(candidate)
    before = {a: {s: dict(v) for s, v in b.items()} for a, b in p.books.items()}
    expire_primary(m, sockets[0])
    requests = []
    monkeypatch.setattr(m, 'request_snapshot', lambda *args: requests.append(args))
    started = time.monotonic()
    m.tick()
    wait_for(lambda: m.states[0] is candidate, .9)
    assert time.monotonic() - started < 1
    assert old.get('retired') and p.books == before
    assert requests  # Missing private evidence is repaired after promotion.
    def replenished():
        m.tick()
        return len(sockets) == 3
    wait_for(replenished, .9)
    assert m.backups[0] is not candidate
    event = m.primary_failures['up_down:BTC:5m']['events'][-1]
    assert event['reason'] == 'pong_timeout' and not event['ready_backup_at_kick']


def test_pong_timeout_without_backup_reconnects_and_requests_rest(feed_manager, monkeypatch):
    m, _, sockets = feed_manager
    make_hot(m)
    old = m.states[0]
    requests = []
    monkeypatch.setattr(m, 'request_snapshot', lambda *args: requests.append(args))
    expire_primary(m, sockets[0])
    m.tick()
    wait_for(lambda: m.states[0] is not old)
    assert requests and m.states[0]['socket'] is not sockets[0]
    assert old['inbox'].invalid.is_set()


def test_supervisor_pong_check_is_once_per_second_and_data_prevents_false_positive(feed_manager):
    m, _, sockets = feed_manager
    expire_primary(m, sockets[0])
    now = time.monotonic()
    sockets[0].transport.read(1, now)
    m.pong_timeout_tick(now)
    assert m.states[0]['connected']
    sockets[0].transport.last_read_at = now - 23
    m.pong_timeout_tick(now + .9)
    assert m.states[0]['connected']
    m.pong_timeout_tick(now + 1)
    assert not m.states[0]['connected']


def test_dead_backup_is_removed_and_replaced(feed_manager, monkeypatch):
    m, _, sockets = feed_manager
    monkeypatch.setenv('POLYMARKET_OBSERVER_WS_HOT_BACKUP_COUNT', '1')
    c = ready_backup(m, sockets)
    now = time.monotonic()
    sockets[1].transport = TransportDiagnostics();sockets[1].transport.ping(now-23)
    c['inbox'].last_byte_at = now-23
    m.backup_next_connect_at = 0
    m.pong_checked_at = 0
    m.tick()
    assert c.get('retired') and m.states[0]['connected']
    wait_for(lambda: m.backups.get(0) is not None and m.backups[0] is not c)


def test_backup_loss_discards_old_epoch_and_restores_fresh_snapshots(feed_manager):
    m, p, sockets = feed_manager
    c = ready_backup(m, sockets)
    old_shadow, inbox = c['shadow'], c['inbox']
    with m.lifecycle_lock, inbox.lock:
        inbox.queue.put_nowait((json.dumps(delta(999999, '99')), datetime.now(timezone.utc)-timedelta(seconds=3), inbox.epoch))
        inbox.queue.put_nowait((json.dumps(delta(999999, '99')), datetime.now(timezone.utc)-timedelta(seconds=3), inbox.epoch))
        inbox.epoch += 1
        inbox.delivery_gap = True
        inbox.resync.set()
    wait_for(lambda: c['shadow'] is not old_shadow)
    assert inbox.resyncs == 1 and not c.get('ready')
    assert p.books['ay']['asks']['.4'] == '1'
    assert inbox.queue.charged_bytes == 0 and inbox.drop_reasons['resync_discarded'] >= 1
    m.tick()  # Supervisor requests the replacement snapshots and clears repair work.
    sockets[1].emit([book('ay', '8'), book('an', '8')])
    wait_for(lambda: m.backup_ready(c))


def test_memory_budget_includes_shadow_dedup_frames_queues_and_processing(monkeypatch):
    monkeypatch.setattr(observer, '_PIPELINE', None)
    m = observer.MarketConnections()
    inbox = observer.ReceiveQueue(4)
    inbox.put('frame')
    inbox.processing_charged_bytes = 321
    shadow = ShadowBooks({'ay'});shadow.receive(book('ay'), datetime.now(timezone.utc))
    m.backups[0] = dict(inbox=inbox, shadow=shadow)
    m.merges[0] = FeedMerge({})
    m.merges[0].filter(book('ay'), 'p', {'ay'})
    m.parsed_frames.parse(json.dumps(delta(n=3)))
    memory = m.memory_metrics(time.monotonic())
    for key in ('shadow_charged_bytes', 'dedup_charged_bytes', 'parsed_frames_charged_bytes', 'queue_charged_bytes'):
        assert memory[key] > 0
    assert memory['processing_frame_charged_bytes'] == 321
    assert memory['auxiliary_charged_bytes'] == sum(v for k,v in memory.items() if k.endswith('_bytes') and k != 'auxiliary_charged_bytes')
    observer.STATUS['ws_aux_memory'] = memory
    p = observer.Pipeline();monkeypatch.setattr(observer, '_PIPELINE', p)
    assert observer.snapshot()['ws_book_memory']['shadow_charged_bytes'] > 0


def test_burst_diagnostics_are_bounded_and_include_subsecond_spikes():
    t = TransportDiagnostics()
    for i in range(1000):
        t.read(100, now=i/10)
    t.read(800000, now=99.95)
    burst = t.burst(100)
    assert burst['bytes_previous_1s'] >= 800000 and burst['peak_100ms_bytes'] >= 800000
    assert len(t.burst_buckets) <= 11 and len(t.read_buckets) <= 61
    assert t.burst(102)['bytes_previous_1s'] == 0


def test_1013_evidence_has_own_and_peer_bursts(feed_manager):
    m, _, sockets = feed_manager
    ready_backup(m, sockets)
    for sock in sockets[:2]:
        sock.transport = TransportDiagnostics();sock.transport.read(400000)
    old = m.states[0]
    sockets[0].callbacks['on_close'](sockets[0], 1013, 'slow consumer: send buffer full')
    evidence = old['last_transport_failure']
    assert evidence['byte_burst']['bytes_previous_1s'] == 400000
    assert evidence['peer_byte_bursts'][0]['bytes_previous_1s'] == 400000


def test_merged_gap_keeps_physical_receipt_gap_unchanged(feed_manager):
    m, p, _ = feed_manager
    make_hot(m)
    state = m.states[0];now = datetime.now(timezone.utc)
    # Physical feed is dead for 30s; merged backup receipt remains fresh.
    for a in state['assets']:
        p.recording.metadata[a] = (now, now-timedelta(milliseconds=20), 'ws')
        m.continuity.latest[a] = {state['source']: (now, 'old', now-timedelta(seconds=30))}
    m.record_primary_failure(0, state, None)
    event = state['outage_events'][0]
    assert event['source_silence_ms'] >= 30000 and event['merged_gap_ms'] < 100
    for a in state['assets']:
        p.books[a] = {'asks':{}, 'bids':{}}
        p.recording.metadata[a] = (now, now+timedelta(milliseconds=30), 'ws')
    m.continuity.publication(p.recording.metadata, set(state['assets']))
    m.complete_recovery(0, state, 'backup')
    assert event['merged_gap_ms'] < 100 and event['merged_gap_final']
    assert event['receipt_gap_ms'] >= 30000


def test_merged_publication_gaps_have_summary_and_physical_events_are_alerts():
    c = FeedContinuity();now = datetime.now(timezone.utc)
    c.publication({'a': (now, now, 'ws')}, {'a'})
    c.publication({'a': (now, now+timedelta(milliseconds=7), 'ws')}, {'a'})
    c.switch('missing', None, {'a'}, 'failover')
    metrics = c.metrics()
    assert metrics['merged_gap_ms']['max'] == 7
    assert metrics['events'][0]['merged_gap_ms'] == 7
    assert metrics['events'][0]['physical_evidence_alert_only']


@pytest.mark.parametrize('value', ['', '2', 'invalid', '0', '129'])
def test_allocator_is_opt_in_and_reexecs_before_imports(monkeypatch, value):
    monkeypatch.setenv('POLYMARKET_OBSERVER_MALLOC_ARENA_MAX', value)
    monkeypatch.delenv('MALLOC_ARENA_MAX', raising=False)
    calls=[]
    monkeypatch.setattr(command.os, 'execve', lambda *args: calls.append(args))
    if value in ('invalid', '0', '129'):
        with pytest.raises(ValueError):command.configure_allocator()
    else:
        command.configure_allocator()
    if value == '2':
        assert calls[0][0] == sys.executable and calls[0][2]['MALLOC_ARENA_MAX'] == '2'
        monkeypatch.setenv('MALLOC_ARENA_MAX', '2');calls.clear();command.configure_allocator()
    assert not calls


def test_oldest_hot_tokens_include_identity_and_receipt_timestamps():
    now = datetime.now(timezone.utc)
    r = recording.Recorder()
    r.metadata['a'] = (now-timedelta(seconds=16.9), now-timedelta(seconds=16.8), 'ws')
    metrics = r.metrics({'a': {'asks':{},'bids':{}}}, {'a':'up_down:BTC:5m'}, {'up_down:BTC:5m':180})
    row = metrics['book_hot_oldest_tokens'][0]
    assert row['token'] == 'a' and row['book_age_ms'] >= 16900
    assert row['exchange_at'] and row['received_at'] and row['observed_at']


@pytest.mark.parametrize('name', ['docker-compose.yml', 'docker-compose.ghcr.yml'])
def test_allocator_setting_is_scoped_to_observer(name):
    import yaml
    config = yaml.safe_load((Path(__file__).parents[2]/name).read_text())
    for service, value in config['services'].items():
        env = value.get('environment', {})
        text = str(env)
        assert ('POLYMARKET_OBSERVER_MALLOC_ARENA_MAX' in text) == (service == 'polymarket-observer')


def test_production_rate_dual_reader_replay_has_no_backup_backlog(feed_manager, monkeypatch):
    m, p, sockets = feed_manager
    monkeypatch.setenv('POLYMARKET_OBSERVER_WS_HOT_BACKUP_COUNT', '1')
    c = ready_backup(m, sockets)
    frames = 1080  # Six seconds at 180 frames/s per source, ~443KB/s each.
    raws = []
    for i in range(frames):
        at = int(time.time()*1000) + i*5
        if i % 5 == 0:
            event = [dict(event_type='book', asset_id=a, timestamp=at, hash=str(i)+a,
                bids=[dict(price=str(x/1000), size='3') for x in range(400,470)],
                asks=[dict(price=str(x/1000), size='4') for x in range(530,600)]) for a in ('ay','an')]
        else:
            event = dict(event_type='price_change', timestamp=at, price_changes=[
                dict(asset_id=a, side='SELL', price=str(x/1000), size=str(i), hash=str(i)+a+str(x),
                    best_bid='.469', best_ask='.530') for a in ('ay','an') for x in range(530,534)])
        raws.append(json.dumps(event, separators=(',',':')))
    schedule = sorted([(i,i,0) for i in range(frames)] + [(i+54,i,1) for i in range(frames)])
    start = time.monotonic();cpu = time.process_time()
    peak_depth = 0;peak_wait = 0;ready = samples = 0
    for due, i, source in schedule:
        threading.Event().wait(max(0, start+due/180-time.monotonic()))
        sockets[source].emit(raws[i])
        if due % 9 == 0:
            peak_depth = max(peak_depth, c['inbox'].queue.qsize())
            peak_wait = max(peak_wait, c['inbox'].pending_wait_seconds())
            ready += m.backup_ready(c);samples += 1
    wait_for(lambda: c['inbox'].queue.empty() and c['inbox'].processing_received_at is None)
    assert not c['inbox'].dropped and not c['inbox'].resyncs
    assert peak_wait < 1 and peak_depth < 180 and ready/samples >= .95
    assert p.books == c['shadow'].books
    print('ROUND15_RATE_REPLAY', json.dumps(dict(bytes_per_s=sum(map(len,raws))/6,
        frames=2*frames, cpu_s=time.process_time()-cpu, wall_s=time.monotonic()-start,
        backup_peak_depth=peak_depth, backup_peak_wait_s=peak_wait, ready_sample_ratio=ready/samples)))


def test_shadow_delta_before_snapshot_retains_watermark_and_requires_fresh_base():
    shadow = ShadowBooks({'ay'})
    now = datetime.now(timezone.utc)
    shadow.receive(delta(2000), now)
    shadow.receive(book('ay', '99', 1000), now)
    assert not shadow.books and shadow.merge.latest['ay'].timestamp() == 2
    shadow.receive(book('ay', '4', 2001), now)
    assert shadow.books['ay']['asks']['.4'] == '4'


def test_business_gap_is_finalized_by_publication_while_physical_gap_stays_pending():
    c = FeedContinuity();now = datetime.now(timezone.utc)
    c.publication({'a': (now, now, 'ws')}, {'a'})
    c.switch('missing', None, {'a'}, 'failover')
    assert c.pending_assets() == {'a'}
    c.publication({'a': (now, now+timedelta(milliseconds=15), 'ws')}, {'a'})
    event = c.metrics()['events'][0]
    assert event['merged_gap_ms'] == 15 and event['merged_gap_final']
    assert c.pending_assets() == {'a'} and event['receipt_gap_ms'] is None
    for _ in range(200):
        c.switch('missing', None, {'quiet'}, 'reconnect')
    assert sum(len(v) for v in c.gap_waiting.values()) <= 128
    c.prune(set())
    assert not c.gap_waiting


def test_pong_timeout_ignores_invalid_backup_and_uses_rest(feed_manager, monkeypatch):
    m, _, sockets = feed_manager
    c = ready_backup(m, sockets)
    c['inbox'].resync.set()
    requests = []
    monkeypatch.setattr(m, 'request_snapshot', lambda *args: requests.append(args))
    old = m.states[0]
    expire_primary(m, sockets[0])
    m.tick()
    assert requests
    assert m.states[0] is not c
    assert old['inbox'].invalid.is_set()


@pytest.mark.parametrize('failed', ['thread', 'worker', 'initial', 'resync'])
def test_unready_promotion_still_requires_a_live_subscribed_generation(failed):
    state = dict(connected=True, initial_sent_at=1, inbox=observer.ReceiveQueue(4),
        thread=SimpleNamespace(is_alive=lambda: True), worker=SimpleNamespace(is_alive=lambda: True))
    assert observer.MarketConnections.promotable_backup(state)
    if failed in ('thread', 'worker'):
        state[failed] = SimpleNamespace(is_alive=lambda: False)
    elif failed == 'initial':
        state['initial_sent_at'] = None
    else:
        state['inbox'].resync.set()
    assert not observer.MarketConnections.promotable_backup(state)
