"""Receive isolation, throttles, bounded persistence and lifecycle behavior."""
import json
import threading
from datetime import datetime, timedelta, timezone

from app.services.polymarket import observer


def market(identity='a', **extra):
    return dict(id=identity, slug=identity, question='test', yesAssetId=identity+'y',
                noAssetId=identity+'n', feeRate='0', **extra)


def feed(pipeline, m):
    for token in observer.subscription_assets([m]):
        pipeline.receive(json.dumps(dict(event_type='book', asset_id=token,
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
    m = market(market_type='up_down', asset='BTC', interval='15m',
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
    statements = [sql for item in p.queue.items for sql, _ in item[2]]
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
    monkeypatch.setattr(observer, 'time', SimpleNamespace(monotonic=lambda: clock[0]))
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
            self.target = target
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
    assert sockets[0].sent[0]['assets_ids'] == ['ay', 'an']
    assert sockets[1].sent[0]['assets_ids'] == ['by', 'bn']
    manager.sync([a, b])
    assert len(sockets[0].sent) == len(sockets[1].sent) == 1
    c = market('c', market_type='up_down')
    manager.sync([a, c])
    assert len(sockets) == 2
    assert sockets[1].sent[1:] == [dict(assets_ids=['cn', 'cy'], operation='subscribe'),
                                  dict(assets_ids=['bn', 'by'], operation='unsubscribe')]
    clock[0] += 10
    manager.tick()
    assert sockets[0].sent[-1] == sockets[1].sent[-1] == 'PING'
    sockets[1].closed = True
    sockets[1].callbacks['on_error'](sockets[1], 'slow consumer: send buffer full')
    sockets[1].callbacks['on_close'](sockets[1], 1008, 'slow consumer: send buffer full')
    clock[0] += 5
    manager.tick()
    assert len(sockets) == 3
    assert not sockets[0].closed
    assert observer.STATUS['reconnect_count'] == 1
    assert observer.STATUS['slow_consumer_count'] == 1
    assert observer.STATUS['ws_connected'] == 2
    assert sockets[2].sent[0]['assets_ids'] == ['cy', 'cn']
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
            return {'a': {'books': {a: {'asks': [dict(price='.4', size='100')], 'bids': []}
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
    assert len(p.queue.items) == 1
    sql, params = p.queue.items[0][2][0]
    assert sql.startswith('INSERT INTO qd_polymarket_observer_episodes')
    assert params[8] == 'rest'
