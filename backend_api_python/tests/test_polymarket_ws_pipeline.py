"""Receive isolation, throttles, bounded persistence and lifecycle behavior."""
import json
import threading
import time
from datetime import datetime, timedelta, timezone

from app.services.polymarket import observer


def market(identity='a', **extra):
    return dict(id=identity, slug=identity, question='test', yesAssetId=identity+'y',
                noAssetId=identity+'n', feeRate='0', **extra)


def feed(pipeline, m):
    for token in observer.subscription_assets([m]):
        pipeline.receive(json.dumps(dict(event_type='book', asset_id=token, timestamp=int(datetime.now(timezone.utc).timestamp() * 1000),
                                        bids=[dict(price='.3', size='100')],
                                        asks=[dict(price='.4', size='100')])))


def test_receive_has_no_database_http_or_evaluation(monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError('receive performed external work')
    monkeypatch.setattr(observer, 'get_db_connection', forbidden)
    monkeypatch.setattr(observer, 'opportunity', forbidden)
    monkeypatch.setattr(observer.PublicClient, 'capture', forbidden)
    p = observer.Pipeline()
    m = market()
    p.set_markets([m])
    feed(p, m)
    p.receive(json.dumps(dict(event_type='price_change', price_changes=[dict(asset_id='ay', side='SELL', price='.4', size='2')])))
    assert p.dirty == {'a'}
    assert p.books['ay']['asks']['.4'] == '2'
    assert p.messages == 3
    assert p.queue.snapshot()['queue_depth'] == 0


def test_dirty_evaluator_throttle_and_research_cache(monkeypatch):
    m = market(market_type='up_down', asset='BTC', interval='15m', conditionId='condition',
               eventStartTime=datetime.now(timezone.utc).isoformat(),
               endDate=(datetime.now(timezone.utc)+timedelta(minutes=15)).isoformat())
    p = observer.Pipeline()
    p.set_markets([m])
    observer._LAST_RESEARCH.pop('a', None)
    calls = []
    class Reference:
        def get(self, asset):
            calls.append(asset)
            return dict(price=None, source='unavailable', observed_at=None)
    monkeypatch.setattr(observer, '_REFERENCE', Reference())
    monkeypatch.setattr(observer, 'get_db_connection', lambda: (_ for _ in ()).throw(AssertionError('evaluator DB')))
    feed(p, m)
    p.evaluate(100)
    first = p.queue.snapshot()['enqueued']
    feed(p, m)
    p.evaluate(100.5)
    assert p.queue.snapshot()['enqueued'] == first
    assert p.dirty == {'a'}
    p.evaluate(101)
    assert p.queue.snapshot()['enqueued'] > first
    assert calls == ['BTC']
    observer._LAST_RESEARCH.pop('a', None)


def test_overflow_coalesces_latest_samples_and_preserves_transitions():
    q = observer.WriteQueue(2)
    q.put('a', 'research', [('sample', (1,))])
    q.put('a', 'research', [('sample', (2,))])
    assert q.items[0][2] == [('sample', (2,))]
    q.put('a', 'observation', [('start', ())])
    q.put('a', 'observation', [('end', ())])
    assert [item[2][0][0] for item in q.items] == ['start', 'end']
    q.put('b', 'research', [('sample', ())])
    finished = threading.Event()
    producer = threading.Thread(target=lambda: (q.put('a', 'observation', [('start2', ())]), finished.set()))
    producer.start()
    assert not finished.wait(.05)
    assert q.take(1)[0][2][0][0] == 'start'
    assert finished.wait(1)
    producer.join()
    assert [item[2][0][0] for item in q.take(10)] == ['end', 'start2']
    assert q.metrics['coalesced'] == 1
    assert q.metrics['dropped'] == 2
    assert q.metrics['queue_high_water'] == 2


def test_subscription_changes_deduplicate_and_skip_unchanged():
    assert observer.subscription_changes(['a', 'a'], ['a']) == []
    assert observer.subscription_changes(['a', 'b'], ['b', 'c']) == [
        dict(assets_ids=['a'], operation='unsubscribe'), dict(assets_ids=['c'], operation='subscribe')]


def test_refresh_preserves_existing_books_and_prunes_removed():
    p = observer.Pipeline()
    a, b = market(), market('b')
    p.set_markets([a])
    feed(p, a)
    old = p.books['ay']
    p.set_markets([a, b])
    assert p.books['ay'] is old
    p.set_markets([b])
    assert p.books == {}
    assert p.queue.items[0][2][0][1] == ('a',)


def test_close_reason_slow_consumer(monkeypatch):
    monkeypatch.setattr(observer, 'STATUS', {})
    observer.close_reason(1008, 'slow consumer: send buffer full')
    observer.close_reason(1000, 'normal')
    assert observer.STATUS['slow_consumer_count'] == 1
    assert observer.STATUS['last_close_reason'] == 'normal'


def test_writer_batches_in_order_and_stop_flushes(monkeypatch):
    monkeypatch.setenv('POLYMARKET_OBSERVER_WRITE_BATCH_MS', '10')
    executions, commits = [], []
    class DB:
        def __enter__(self):
            return self
        def __exit__(self, *args):
            pass
        def cursor(self):
            return self
        def executemany(self, sql, params):
            executions.append((sql, params))
        def commit(self):
            commits.append(True)
        def close(self):
            pass
    monkeypatch.setattr(observer, 'get_db_connection', DB)
    p = observer.Pipeline()
    p.queue.put('a', 'observation', [('start', (1,)), ('start', (2,)), ('end', (3,)), ('start', (4,))])
    p.start()
    p.close(2)
    assert executions == [('start', [(1,), (2,)]), ('end', [(3,)]), ('start', [(4,)])]
    assert len(commits) == 1
    assert p.metrics()['written'] == 4
    assert p.metrics()['queue_depth'] == 0
    assert observer.STATUS['flush_complete']


def test_writer_failure_retains_batch_then_retries(monkeypatch):
    monkeypatch.setenv('POLYMARKET_OBSERVER_WRITE_BATCH_MS', '10')
    p = observer.Pipeline()
    attempts = []
    def write(batch):
        attempts.append(batch)
        if len(attempts) == 1:
            raise RuntimeError('database temporarily unavailable')
        p.queue.metrics['written'] += sum(len(item[2]) for item in batch)
    monkeypatch.setattr(p, 'write_batch', write)
    p.queue.put('a', 'observation', [('end', ())])
    p.start()
    p.close(2)
    assert len(attempts) == 2
    assert attempts[0] == attempts[1]
    assert p.metrics()['written'] == 1
    assert not p.writer.is_alive()


def test_normalized_book_and_delta_payload():
    books = {}
    assert observer.ws_apply_event(dict(type='book', payload=dict(assetId='a', bids=[], asks=[dict(price='.5', size='2')])), books) == ['a']
    observer.ws_apply_event(dict(type='price_change', payload=dict(priceChanges=[dict(assetId='a', side='SELL', price='.5', size='0')])), books)
    assert not books['a']['asks']


def test_close_frame_error_text_is_counted_without_on_close_reason(monkeypatch):
    monkeypatch.setattr(observer, 'STATUS', {})
    observer._ws_error('fin=1 opcode=8 data=slow consumer: send buffer full')
    observer._ws_disconnected()
    assert observer.STATUS['slow_consumer_count'] == 1
    assert observer.STATUS['last_close_reason'] == 'slow consumer: send buffer full'


def test_batch_limit_keeps_transition_group_atomic():
    q = observer.WriteQueue(10)
    q.put('a', 'observation', [('end_bucket', ()), ('start', ())])
    q.put('b', 'observation', [('start', ())])
    assert len(q.take(2)) == 1
    assert len(q.take(2)) == 1


def test_stop_persists_final_flushed_status(monkeypatch):
    p = observer.Pipeline()
    monkeypatch.setattr(observer, '_PIPELINE', p)
    monkeypatch.setattr(observer, '_THREAD', None)
    monkeypatch.setattr(observer, '_WS', None)
    monkeypatch.setattr(observer, '_STOP', threading.Event())
    monkeypatch.setattr(observer, 'STATUS', dict(observer.STATUS))
    persisted = []
    monkeypatch.setattr(observer, '_persist_status', lambda: persisted.append(observer.snapshot()))
    p.start()
    observer.stop(2)
    assert persisted[-1]['mode'] == 'stopped'
    assert persisted[-1]['flush_complete']
    assert persisted[-1]['queue_depth'] == 0


def test_stop_timeout_reports_unflushed_writer(monkeypatch):
    p = observer.Pipeline()
    monkeypatch.setattr(observer, 'STATUS', dict(observer.STATUS))
    entered, release = threading.Event(), threading.Event()
    def delayed_write(batch):
        entered.set()
        release.wait(2)
    monkeypatch.setattr(p, 'write_batch', delayed_write)
    p.queue.put('a', 'observation', [('end', ())])
    p.start()
    assert entered.wait(1)
    p.close(.02)
    assert not observer.STATUS['flush_complete']
    assert p.metrics()['writer_inflight'] == 1
    release.set()
    p.close(2)
    assert observer.STATUS['flush_complete']


def test_removal_waits_for_snapshot_evaluation_before_end(monkeypatch):
    p = observer.Pipeline()
    m = market()
    p.set_markets([m])
    feed(p, m)
    entered, release, removed = threading.Event(), threading.Event(), threading.Event()
    original = observer.opportunity
    def delayed_quote(*args):
        entered.set()
        assert release.wait(2)
        return original(*args)
    monkeypatch.setattr(observer, 'opportunity', delayed_quote)
    evaluation = threading.Thread(target=p.evaluate)
    evaluation.start()
    assert entered.wait(1)
    removal = threading.Thread(target=lambda: (p.set_markets([]), removed.set()))
    removal.start()
    assert not removed.wait(.02)
    # Receive remains independent while evaluator/removal are serialized.
    p.receive(json.dumps(dict(event_type='price_change', price_changes=[dict(asset_id='ay', side='SELL', price='.4', size='101')])))
    release.set()
    evaluation.join(2)
    removal.join(2)
    assert removed.is_set()
    statements = [sql for item in p.queue.items for sql, _ in item[2]
                  if 'qd_polymarket_observer_episodes' in sql]
    assert 'INSERT INTO qd_polymarket_observer_episodes' in statements[0]
    assert statements[-1].startswith('UPDATE qd_polymarket_observer_episodes SET ended_at')


def test_writer_waits_for_producer_after_stop_timeout(monkeypatch):
    monkeypatch.setenv('POLYMARKET_OBSERVER_WRITE_BATCH_MS', '10')
    p = observer.Pipeline()
    p.market_done.clear()
    written = []
    monkeypatch.setattr(p, 'write_batch', lambda batch: written.extend(batch))
    p.start()
    p.close(.02)
    assert p.writer.is_alive()
    p.queue.put('a', 'observation', [('end', ())])
    p.market_done.set()
    p.close(2)
    assert not p.writer.is_alive()
    assert written[0][2] == [('end', ())]


def test_shard_assignment_stable_complete_pairs():
    a, b = market(), market('b', market_type='up_down')
    assert observer.shard_markets([a, b], 1) == {0: [a, b]}
    assert observer.shard_markets([a, b], 2) == {0: [a], 1: [b]}
    four = observer.shard_markets([a, b], 4)
    reverse = observer.shard_markets([b, a], 4)
    assert {m['id']: i for i, group in four.items() for m in group} == {m['id']: i for i, group in reverse.items() for m in group}


def test_shards_dynamic_subscription_ping_and_independent_reconnect(monkeypatch):
    import sys
    from types import SimpleNamespace
    monkeypatch.setattr(observer, '_STOP', threading.Event())
    monkeypatch.setattr(observer, '_PIPELINE', None)
    monkeypatch.setattr(observer, 'STATUS', dict(observer.STATUS))
    monkeypatch.setenv('POLYMARKET_OBSERVER_WS_SHARDS', '2')
    clock = [100]
    monkeypatch.setattr(observer, 'time', SimpleNamespace(process_time=time.process_time, monotonic=lambda: clock[0]))
    sockets = []
    class Socket:
        def __init__(self, url, **callbacks):
            self.callbacks = callbacks
            self.sent = []
            self.closed = False
            sockets.append(self)
        def run_forever(self, **kwargs):
            self.callbacks['on_open'](self)
        def send(self, raw):
            self.sent.append(raw if raw == 'PING' else json.loads(raw))
        def close(self):
            self.closed = True
    class Thread:
        def __init__(self, target, **kwargs):
            self.target = target if 'receive' in kwargs.get('name', '') else lambda: None
            self.socket = sockets[-1]
        def start(self):
            self.target()
        def is_alive(self):
            return not self.socket.closed
        def join(self, timeout):
            assert self.socket.closed
    monkeypatch.setattr(observer, 'threading', SimpleNamespace(Thread=Thread))
    monkeypatch.setitem(sys.modules, 'websocket', SimpleNamespace(WebSocketApp=Socket))
    manager = observer.MarketConnections()
    a, b = market(), market('b', market_type='up_down')
    manager.sync([a, b])
    assert len(sockets) == 2
    clock[0] += .5
    manager.tick()  # Shared pacing admits the second connection's first batch.
    assert sockets[0].sent[0]['assets_ids'] == ['ay', 'an']
    assert sockets[1].sent[0]['assets_ids'] == ['by', 'bn']
    manager.sync([a, b])
    assert len(sockets[0].sent) == len(sockets[1].sent) == 1
    c = market('c', market_type='up_down')
    manager.sync([a, c])
    clock[0] += .5
    manager.tick()
    assert len(sockets) == 2
    assert not any(s.closed for s in sockets)
    assert sockets[1].sent[-2:] == [dict(assets_ids=['bn', 'by'], operation='unsubscribe'),
                                          dict(assets_ids=['cn', 'cy'], operation='subscribe')]
    clock[0] += 10
    manager.tick()
    assert sockets[0].sent[-1] == sockets[1].sent[-1] == 'PING'
    sockets[1].closed = True
    sockets[1].callbacks['on_error'](sockets[1], 'slow consumer: send buffer full')
    sockets[1].callbacks['on_close'](sockets[1], 1008, 'slow consumer: send buffer full')
    clock[0] += 16
    manager.tick()
    assert len(sockets) == 3
    assert not sockets[0].closed
    assert observer.STATUS['reconnect_count'] == 1
    assert observer.STATUS['slow_consumer_count'] == 1
    assert observer.STATUS['ws_connected'] == 2
    assert sockets[2].sent[0]['assets_ids'] == ['cy', 'cn']
    assert manager.states[1]['attempts'] == 2
    assert manager.failures() == 0  # A healthy open does not trigger REST fallback.
    clock[0] += 61
    sockets[2].callbacks['on_close'](sockets[2], 1000, 'closed')
    assert manager.states[1]['attempts'] == 1  # Reset to the first-failure fast retry.
    assert manager.states[1]['retry_at'] == clock[0]
    manager.close()
    assert all(s.closed for s in sockets)


def test_rest_fallback_uses_same_ordered_writer_queue(monkeypatch):
    m = market()
    p = observer.Pipeline()
    monkeypatch.setattr(observer, '_PIPELINE', p)
    monkeypatch.setattr(observer, '_WS', None)
    monkeypatch.setattr(observer, 'STATUS', dict(observer.STATUS))
    monkeypatch.setenv('POLYMARKET_OBSERVER_WS_ENABLED', 'false')
    monkeypatch.setenv('POLYMARKET_OBSERVER_UPDOWN_ENABLED', 'false')
    class Stop:
        stopped = False
        def is_set(self):
            return self.stopped
        def wait(self, seconds):
            self.stopped = True
    class Client:
        def discover(self, **kwargs):
            return [m], []
        def capture(self, markets):
            return {'a': {'observedMs': int(datetime.now(timezone.utc).timestamp() * 1000), 'books': {a: {'asks': [dict(price='.4', size='100')], 'bids': []}
                                    for a in observer.subscription_assets(markets)}}}
        def close(self):
            pass
    monkeypatch.setattr(observer, '_STOP', Stop())
    monkeypatch.setattr(observer, 'PublicClient', Client)
    monkeypatch.setattr(observer, '_persist_status', lambda: None)
    monkeypatch.setattr(observer, '_run_retention', lambda *args: None)
    monkeypatch.setattr(observer, '_close_stale_episodes', lambda *args: None)
    monkeypatch.setattr(observer, 'get_db_connection', lambda: (_ for _ in ()).throw(AssertionError('REST bypassed writer')))
    observer._market_loop()
    assert len(p.queue.items) == 3
    assert sum(item[1] == 'book' for item in p.queue.items) == 2
    sql, params = p.queue.items[-1][2][0]
    assert sql.startswith('INSERT INTO qd_polymarket_observer_episodes')
    assert params[8] == 'rest'


def test_receive_queue_overflow_requests_snapshot_without_disconnect():
    q = observer.ReceiveQueue(2)
    assert q.put('first') and q.put('second')
    assert not q.put('overflow')
    assert q.resync.is_set() and not q.invalid.is_set()
    assert q.dropped == 1 and q.high_water == 2
    assert not q.put('late') and q.dropped == 2
    q.discard()
    assert q.queue.empty() and q.dropped == 4


def test_balances_large_and_small_markets_with_pairs_together():
    markets = [dict(id=str(i), yesAssetId=f'{i}y', noAssetId=f'{i}n') for i in range(1177)]
    groups = observer.shard_markets(markets, 4)
    counts = [len(observer.subscription_assets(groups[i])) for i in range(4)]
    assert max(counts) - min(counts) <= 2
    assert sorted(m['id'] for group in groups.values() for m in group) == sorted(m['id'] for m in markets)


def test_socket_drains_while_worker_waits_for_book_lock(monkeypatch):
    import sys
    from types import SimpleNamespace
    entered = threading.Event()
    stop = threading.Event()
    sockets = []

    class Socket:
        def __init__(self, url, **callbacks):
            self.callbacks = callbacks
            sockets.append(self)
        def run_forever(self, **kwargs):
            self.callbacks['on_open'](self)
            entered.set()
            stop.wait(3)
        def send(self, raw):
            pass
        def close(self):
            stop.set()

    monkeypatch.setitem(sys.modules, 'websocket', SimpleNamespace(WebSocketApp=Socket))
    monkeypatch.setattr(observer, '_STOP', threading.Event())
    monkeypatch.setenv('POLYMARKET_OBSERVER_WS_SHARDS', '1')
    monkeypatch.setenv('POLYMARKET_OBSERVER_WS_RECEIVE_QUEUE_MAX', '2')
    p = observer.Pipeline()
    p.set_markets([market()])
    monkeypatch.setattr(observer, '_PIPELINE', p)
    m = observer.MarketConnections()
    with p.lock:
        m.sync([market()])
        assert entered.wait(1)
        # Worker can block on p.lock; the socket callback must still return.
        for i in range(10):
            sockets[0].callbacks['on_message'](sockets[0], json.dumps(dict(
                event_type='book', asset_id='ay', timestamp=str(1780000000000 + i),
                bids=[dict(price='.4', size='1')], asks=[dict(price='.5', size='1')])))
        assert not m.states[0]['inbox'].invalid.is_set()
        assert m.states[0]['inbox'].dropped == 0
        assert m.states[0]['inbox'].queue.maxsize >= 8
        assert not p.books
    m.close()
    assert not m.states[0]['worker'].is_alive()
    assert not p.books


def test_invalidated_generation_cannot_repopulate_book():
    p = observer.Pipeline()
    p.set_markets([market()])
    inbox = observer.ReceiveQueue(1)
    inbox.invalid.set()
    p.receive(dict(event_type='book', asset_id='ay', bids=[], asks=[]), {'ay'}, inbox=inbox)
    assert p.books == {}
    assert p.messages == 0


def test_catalogue_insertion_retains_balanced_owners():
    markets = [market(str(i)) for i in range(40)]
    groups = observer.shard_markets(markets, 4)
    previous = {a: i for i, group in groups.items() for a in observer.subscription_assets(group)}
    updated = observer.shard_markets([market('new'), *markets], 4, previous)
    target = {a: i for i, group in updated.items() for a in observer.subscription_assets(group)}
    assert all(target[a] == owner for a, owner in previous.items())
    assert max(len(observer.subscription_assets(g)) for g in updated.values()) == 22


def test_asset_target_grows_shards_and_keeps_market_pairs(monkeypatch):
    monkeypatch.setenv('POLYMARKET_OBSERVER_WS_SHARDS', '1')
    monkeypatch.setenv('POLYMARKET_OBSERVER_WS_ASSETS_PER_SHARD', '4')
    m = observer.MarketConnections()
    monkeypatch.setattr(m, 'tick', lambda: None)
    m.sync([market(str(i)) for i in range(5)])
    assert m.count == 3
    assert sorted(len(s['assets']) for s in m.states.values()) == [2, 4, 4]
    for i in range(5):
        assert m.balancer.assignment[f'{i}y'] == m.balancer.assignment[f'{i}n']


def test_elastic_receive_fifo_keeps_deltas():
    q = observer.ReceiveQueue(2, 8)
    for i in range(8):
        assert q.put(str(i))
    assert [q.queue.get_nowait()[0] for _ in range(8)] == list(map(str, range(8)))
    assert q.queue.maxsize == 8 and not q.invalid.is_set()


def test_prior_weights_place_cold_start_and_keep_pairs():
    markets = [market(str(i)) for i in range(16)]
    rates = {f'market:{i}': (7 if i < 4 else 1) for i in range(16)}
    groups = observer.shard_markets(markets, 4, rates=rates)
    target = {a: i for i, group in groups.items() for a in observer.subscription_assets(group)}
    loads = [sum(rates['market:' + m['id']] for m in groups[i]) for i in range(4)]
    assert loads == [10] * 4
    assert set(target) == set(observer.subscription_assets(markets))
    for m in markets:
        assert target[m['yesAssetId']] == target[m['noAssetId']]


def updown(asset, interval, window):
    ident = f'{asset}-{interval}-{window}'
    return market(ident, market_type='up_down', asset=asset, interval=interval)


def test_series_windows_share_a_shard_across_rollovers(monkeypatch):
    sent = []
    monkeypatch.setattr(observer, '_PIPELINE', None)
    monkeypatch.setattr(observer, '_STOP', threading.Event())
    manager = observer.MarketConnections()
    monkeypatch.setattr(manager, 'tick', lambda: None)
    series = [('BTC', '5m'), ('ETH', '5m'), ('BTC', '15m'), ('SOL', '5m')]
    catalogue = [updown(a, i, w) for a, i in series for w in (0, 1)] + [market('yn')]
    manager.sync(catalogue)
    owner = {key: manager.balancer.owners[key] for key in manager.balancer.owners}
    assert len(set(owner.values())) == 4
    for a, i in series:
        shards = {manager.balancer.assignment[x] for w in (0, 1)
                  for x in observer.subscription_assets([updown(a, i, w)])}
        assert shards == {owner[f'up_down:{a}:{i}']}

    class Socket:
        def __init__(self, index):
            self.index = index

        def send(self, raw):
            sent.append((self.index, json.loads(raw)))
    for index, state in manager.states.items():
        state.update(connected=True, socket=Socket(index))
    rebalances = observer.STATUS.get('rebalance_count', 0)
    # Window 0 expires, 1 is current, 2 appears: only that series' shard changes.
    rolled = [updown(a, i, w) for a, i in series for w in ((1, 2) if a == 'BTC' and i == '5m' else (0, 1))]
    manager.sync(rolled + [market('yn')])
    shard = owner['up_down:BTC:5m']
    assert {index for index, _ in sent} == {shard}
    assert sent == [(shard, dict(assets_ids=['BTC-5m-0n', 'BTC-5m-0y'], operation='unsubscribe')),
                    (shard, dict(assets_ids=['BTC-5m-2n', 'BTC-5m-2y'], operation='subscribe'))]
    assert observer.STATUS.get('rebalance_count', 0) == rebalances
    assert manager.balancer.owners == owner


def test_reader_never_parses_or_waits_on_balancer_and_pipeline(monkeypatch):
    import sys
    from types import SimpleNamespace
    sockets = []
    stop = threading.Event()

    class Socket:
        def __init__(self, url, **callbacks):
            self.url, self.callbacks = url, callbacks
            sockets.append(self)

        def run_forever(self, **kwargs):
            self.kwargs = kwargs
            self.callbacks['on_open'](self)
            stop.wait(3)

        def send(self, raw):
            pass

        def close(self):
            stop.set()

    monkeypatch.setitem(sys.modules, 'websocket', SimpleNamespace(WebSocketApp=Socket))
    monkeypatch.setattr(observer, '_STOP', threading.Event())
    monkeypatch.setenv('POLYMARKET_OBSERVER_WS_URL', 'ws://127.0.0.1:9/ws/market')
    monkeypatch.setenv('POLYMARKET_OBSERVER_WS_SHARDS', '1')
    monkeypatch.setenv('POLYMARKET_OBSERVER_WS_RECEIVE_QUEUE_MAX', '20')
    monkeypatch.setenv('POLYMARKET_OBSERVER_WS_RECEIVE_QUEUE_CEILING', '20')
    p = observer.Pipeline()
    p.set_markets([market()])
    monkeypatch.setattr(observer, '_PIPELINE', p)
    m = observer.MarketConnections()
    parsed = []
    loads = observer.json.loads
    monkeypatch.setattr(observer.json, 'loads', lambda raw, **kw: parsed.append((threading.current_thread().name, raw)) or loads(raw, **kw))
    m.sync([market()])
    with p.lock, m.balancer.lock:  # Both downstream locks can be held while receiving.
        for _ in range(100):
            if hasattr(sockets[0], 'kwargs'):
                break
            time.sleep(.01)
        assert sockets[0].url == 'ws://127.0.0.1:9/ws/market'
        assert sockets[0].kwargs['skip_utf8_validation'] is True
        receive = sockets[0].callbacks['on_message']
        frame = json.dumps(dict(event_type='price_change', price_changes=[
            dict(asset_id='ay', side='BUY', price='.4', size='1'),
            dict(asset_id='an', side='SELL', price='.6', size='1')])).encode()
        for _ in range(10):
            receive(sockets[0], frame)
        receive(sockets[0], b'PONG')
        receive(sockets[0], b'\xff\xfe not utf-8')
        inbox = m.states[0]['inbox']
        assert inbox.received_frames == 11  # Includes malformed payload; excludes PONG.
        assert inbox.received_bytes == 10 * len(frame) + len(b'\xff\xfe not utf-8')
        assert inbox.dropped == 0
        assert isinstance(inbox.queue.queue[0][0], bytes)
        assert all(name != threading.current_thread().name for name, _ in parsed if _ == frame)
    for _ in range(100):
        if len([raw for _, raw in parsed if raw == frame]) == 10 and inbox.queue.empty():
            break
        time.sleep(.01)
    assert len([raw for _, raw in parsed if raw == frame]) == 10
    assert m.balancer.pending == {'market:a': 10}
    m.close()


def test_read_ahead_reader_drains_batched_frames_with_few_syscalls(monkeypatch):
    import base64
    import hashlib
    import socket
    import struct
    import websocket
    from websocket import _core

    server = socket.socket()
    server.bind(('127.0.0.1', 0))
    server.listen(1)
    frames = [json.dumps(dict(event_type='book', asset_id=str(i), bids=[], asks=[])).encode() for i in range(300)]

    def serve():
        conn, _ = server.accept()
        request = b''
        while b'\r\n\r\n' not in request:
            request += conn.recv(4096)
        key = next(line.split(b':', 1)[1].strip() for line in request.split(b'\r\n')
                   if line.lower().startswith(b'sec-websocket-key'))
        accept = base64.b64encode(hashlib.sha1(key + b'258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest())
        conn.sendall(b'HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n'
                     b'Sec-WebSocket-Accept: ' + accept + b'\r\n\r\n')
        conn.sendall(b''.join(struct.pack('!BB', 0x81, 126) + struct.pack('!H', len(f)) + f
                              if len(f) > 125 else struct.pack('!BB', 0x81, len(f)) + f for f in frames))
        done.wait(5)
        conn.close()

    done = threading.Event()
    threading.Thread(target=serve, daemon=True).start()
    calls = []
    recv = _core.recv
    monkeypatch.setattr(_core, 'recv', lambda sock, size: calls.append(size) or recv(sock, size))
    received = []

    def message(app, raw):
        received.append(raw)
        if len(received) == len(frames):
            done.set()
    app = observer.read_ahead_app(websocket.WebSocketApp)(
        f'ws://127.0.0.1:{server.getsockname()[1]}', on_message=message)
    thread = threading.Thread(target=lambda: app.run_forever(skip_utf8_validation=True), daemon=True)
    thread.start()
    # The stock select-per-frame loop would wait for more data after the first
    # read-ahead batch; draining delivers everything without new bytes arriving.
    assert done.wait(3)
    app.close()
    thread.join(3)
    server.close()
    assert received == frames
    assert len([size for size in calls if size == observer.READ_AHEAD_BYTES]) < len(frames) / 10


def test_hard_overflow_recovers_snapshots_on_same_socket(monkeypatch):
    import sys
    from types import SimpleNamespace
    opened, stopped, subscribed = threading.Event(), threading.Event(), threading.Event()
    sockets, operations = [], []

    class Socket:
        def __init__(self, url, **callbacks):
            self.callbacks = callbacks
            sockets.append(self)
        def run_forever(self, **kwargs):
            self.callbacks['on_open'](self)
            opened.set()
            stopped.wait(3)
        def send(self, raw):
            payload = json.loads(raw)
            operations.append(payload.get('operation'))
            if payload.get('operation') == 'subscribe':
                self.callbacks['on_message'](self, json.dumps(dict(
                    event_type='book', asset_id='ay', bids=[], asks=[dict(price='.6', size='3')])))
                subscribed.set()
        def close(self):
            stopped.set()

    monkeypatch.setitem(sys.modules, 'websocket', SimpleNamespace(WebSocketApp=Socket))
    monkeypatch.setattr(observer, '_STOP', threading.Event())
    monkeypatch.setenv('POLYMARKET_OBSERVER_WS_SHARDS', '1')
    monkeypatch.setenv('POLYMARKET_OBSERVER_WS_RECEIVE_QUEUE_MAX', '2')
    monkeypatch.setenv('POLYMARKET_OBSERVER_WS_RECEIVE_QUEUE_CEILING', '2')
    p = observer.Pipeline()
    p.set_markets([market()])
    monkeypatch.setattr(observer, '_PIPELINE', p)
    m = observer.MarketConnections()
    applied = threading.Event()
    original = p.receive
    def receive(*args, **kwargs):
        original(*args, **kwargs)
        if 'ay' in p.books and '.6' in p.books['ay']['asks']:
            applied.set()
    monkeypatch.setattr(p, 'receive', receive)
    feed(p, market())
    try:
        with p.lock:
            m.sync([market()])
            assert opened.wait(1)
            for _ in range(10):
                sockets[0].callbacks['on_message'](sockets[0], json.dumps(dict(
                    event_type='price_change', price_changes=[dict(asset_id='ay', side='SELL', price='.4', size='999')])))
            assert m.states[0]['inbox'].resync.is_set()
        m.start_supervisor()  # Snapshot recovery now uses paced admission.
        assert subscribed.wait(2) and applied.wait(1)
        assert len(sockets) == 1 and not stopped.is_set()
        assert operations == [None, 'unsubscribe', 'subscribe']
        assert p.books['ay']['asks'] == {'.6': '3'}
        assert 'an' not in p.books
        assert not m.states[0]['inbox'].invalid.is_set()
        assert m.states[0]['inbox'].resyncs == 1
    finally:
        m.close()


def test_popped_frame_from_before_overflow_cannot_repopulate_book():
    p = observer.Pipeline()
    p.set_markets([market()])
    inbox = observer.ReceiveQueue(1)
    assert inbox.put('old')
    _, _, old_epoch = inbox.queue.get_nowait()
    assert inbox.put('queued')
    assert not inbox.put('gap')
    inbox.discard()
    inbox.resync.clear()
    p.receive(dict(event_type='book', asset_id='ay', bids=[], asks=[]), {'ay'},
              inbox=inbox, epoch=old_epoch)
    assert not p.books and not p.messages
    p.receive(dict(event_type='book', asset_id='ay', bids=[], asks=[]), {'ay'},
              inbox=inbox, epoch=inbox.epoch)
    assert 'ay' in p.books and p.messages == 1


def test_rollover_snapshot_processed_on_original_worker(monkeypatch):
    import sys
    from types import SimpleNamespace
    opened, stopped, applied = threading.Event(), threading.Event(), threading.Event()
    sockets = []
    class Socket:
        def __init__(self, url, **callbacks):
            self.callbacks = callbacks
            self.sent = []
            sockets.append(self)
        def run_forever(self, **kwargs):
            self.callbacks['on_open'](self)
            opened.set()
            stopped.wait(5)
        def send(self, raw):
            payload = json.loads(raw)
            self.sent.append(payload)
            if payload.get('operation') == 'subscribe':
                for asset in payload['assets_ids']:
                    self.callbacks['on_message'](self, json.dumps(dict(
                        event_type='book', asset_id=asset, bids=[], asks=[dict(price='.6', size='3')])) )
        def close(self):
            stopped.set()
    monkeypatch.setitem(sys.modules, 'websocket', SimpleNamespace(WebSocketApp=Socket))
    monkeypatch.setattr(observer, '_STOP', threading.Event())
    monkeypatch.setenv('POLYMARKET_OBSERVER_WS_SHARDS', '1')
    monkeypatch.setattr(observer, 'STATUS', dict(observer.STATUS, reconnect_count=0))
    p = observer.Pipeline()
    p.set_markets([market(), market('old'), market('new')])
    original = p.receive
    def receive(*args, **kwargs):
        original(*args, **kwargs)
        if 'newn' in p.books:
            applied.set()
    monkeypatch.setattr(p, 'receive', receive)
    monkeypatch.setattr(observer, '_PIPELINE', p)
    m = observer.MarketConnections()
    try:
        m.sync([market(), market('old')])
        assert opened.wait(1)
        feed(p, market())
        retained = p.books['ay']
        thread, worker = m.states[0]['thread'], m.states[0]['worker']
        m.sync([market(), market('new')])
        m.start_supervisor()
        assert applied.wait(1)
        assert len(sockets) == 1 and not stopped.is_set()
        assert m.states[0]['thread'] is thread and m.states[0]['worker'] is worker
        assert p.books['ay'] is retained
        assert sockets[0].sent[1:] == [dict(assets_ids=['oldn', 'oldy'], operation='unsubscribe'),
                                             dict(assets_ids=['newn', 'newy'], operation='subscribe')]
        assert observer.STATUS['reconnect_count'] == 0
        m.tick()
        metrics = observer.STATUS['ws_receive_queues'][0]
        assert metrics['queue_wait_seconds'] >= 0
        assert metrics['lock_wait_seconds'] >= 0
    finally:
        m.close()


def test_shard_lock_wait_measures_time_before_publication():
    p = observer.Pipeline()
    p.set_markets([market()])
    inbox = observer.ReceiveQueue(4)
    entered, finished = threading.Event(), threading.Event()
    def publish():
        entered.set()
        p.receive(dict(event_type='book', asset_id='ay', bids=[], asks=[]), {'ay'},
                  datetime.now(timezone.utc), inbox, inbox.epoch)
        finished.set()
    with p.lock:
        thread = threading.Thread(target=publish)
        thread.start()
        assert entered.wait(1)
        assert not finished.wait(.03)
    assert finished.wait(1)
    thread.join(1)
    assert inbox.lock_wait_seconds >= .02
    assert inbox.lag_seconds >= inbox.lock_wait_seconds


def test_read_ahead_falls_back_to_ssl_aware_library_dispatcher():
    from types import SimpleNamespace
    calls = []

    class Base:
        def create_dispatcher(self, timeout, dispatcher=None, is_ssl=False, handleDisconnect=None):
            calls.append((timeout, is_ssl))
            return SimpleNamespace(read=lambda *args: calls.append('read'))

    app = observer.read_ahead_app(Base)()
    # A future library exposes no compatible frame_buffer/_recv API.
    app.sock = SimpleNamespace(sock=SimpleNamespace(pending=lambda: 0))
    dispatcher = app.create_dispatcher(1)
    dispatcher.read(None, lambda: None, lambda: None)
    assert calls == [(1, True), 'read']
    assert app.read_ahead_active is False
