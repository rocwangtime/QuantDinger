"""F1 envelope parity, F2 sampled stage timing, F4 backlog business gap."""
import json
import random
import threading

from app.services.polymarket import observer, ws_payloads
from app.services.polymarket.ws_group_process import merge_feed_status, wire_event
from app.services.polymarket.ws_payloads import _cheap_envelope_regex, cheap_envelope

A1 = '71321045679252212594626385532706912750332728571942532289631379312455583992563'
A2 = '52114319501245915516055106046884209969926127482827954674443846427813813222426'


def _dump(value):
    return json.dumps(value, separators=(',', ':'))


def _frames():
    ladder = [dict(price='.48', size='30'), dict(price='.47', size='1}2')]
    change = dict(market='0xm', price_changes=[dict(asset_id=a, price='0.5', size='200', side='BUY',
                                                    hash=f'h{a}', best_bid='0.5', best_ask='1') for a in (A1, A2)],
                  timestamp='1757908892351', event_type='price_change')
    book = dict(market='0xm', asset_id=A1, timestamp='1757908892351', hash='hb', bids=ladder, asks=ladder,
                event_type='book')
    trade = dict(asset_id=A1, fee_rate_bps='0', market='0xm', price='0.5', side='BUY', size='10',
                 timestamp='1757908892351', event_type='last_trade_price')
    tick = dict(event_type='tick_size_change', asset_id=A1, market='0xm', old_tick_size='0.01',
                new_tick_size='0.001', timestamp='1757908892351')
    bba = dict(event_type='best_bid_ask', market='0xm', asset_id=A1, best_bid='0.5', best_ask='0.51',
               spread='0.01', timestamp='1757908892351')
    old = dict(asset_id='ay', changes=[dict(price='.4', size='1', side='BUY')], event_type='price_change',
               timestamp='1600000000000', hash='oldhash')
    normal = [_dump(f) for f in (change, book, trade, tick, bba, old)] + [_dump([book, change]), _dump([])]
    abnormal = [
        _dump(change)[:-7], _dump(change) + _dump(book), 'PONG', '', 'null', '[]', '{}',
        _dump(dict(change, timestamp=1757908892351)), _dump(dict(change, timestamp=-0.0)),
        _dump(dict(change, timestamp=None)), _dump(dict(change, event_type=['x'])),
        _dump(dict(change, event_type='a b')), _dump(dict(change, event_type='')),
        _dump(dict(book, hash='q\\"x')), json.dumps(dict(book, hash='h\u00e9')),
        _dump(dict(book, bids=[dict(asset_id='inner', price='1', size='1')])),
        '{"event_type":"book","asset_id":"a","asset_id":"b","timestamp":"1"}',
        '{"event_type":"book","asset_id":"a","timestamp":-0,"hash":7}',
        '{"event_type":"book","asset_id":"a","timestamp":1e3,"hash":-12}',
        '{"event_type":"book","asset_id":"a","timestamp":18446744073709551616}',
        json.dumps(change), _dump(dict(change, timestamp=True)), _dump(dict(change, timestamp=False)),
    ]
    return normal, abnormal


def test_fast_envelope_matches_the_regex_on_prod_order_and_abnormal_frames():
    normal, abnormal = _frames()
    for raw in normal + abnormal:
        for value in (raw, raw.encode(), bytearray(raw.encode())):
            assert cheap_envelope(value) == _cheap_envelope_regex(value), raw[:80]
    assert cheap_envelope(b'\xff\xfe{"event_type"') == _cheap_envelope_regex(b'\xff\xfe{"event_type"')


def test_fast_envelope_uses_orjson_on_normal_frames(monkeypatch):
    if ws_payloads.orjson is None:
        return
    normal, _ = _frames()
    # The replay harness stamps integer timestamps; they must not take the slow path.
    normal += [_dump(wire_event(3, 3, ('ay', 'an'), 140, 1_700_000_000_000)),
               _dump(wire_event(3, 0, ('ay', 'an'), 140, 1_700_000_000_000))]
    calls = []
    monkeypatch.setattr(ws_payloads, '_cheap_envelope_regex', lambda raw: calls.append(raw) or [])
    for raw in normal:
        cheap_envelope(raw)
    assert calls == []


def test_fast_envelope_fuzz_parity():
    rng = random.Random(20261020)
    normal, abnormal = _frames()
    for _ in range(2000):
        raw = rng.choice(normal + abnormal)
        if raw and rng.random() < 0.5:
            cut = rng.randrange(len(raw))
            raw = raw[:cut] + rng.choice(['', '"', '}', ',', '\\', ' ', '{"timestamp":"9"}']) + raw[cut + 1:]
        assert cheap_envelope(raw) == _cheap_envelope_regex(raw)


def test_stage_timing_is_sampled_and_scaled():
    manager = observer.MarketConnections()
    hits = sum(manager._stage_sampled() for _ in range(observer.STAGE_SAMPLE_EVERY * 4))
    assert hits == 4
    manager._charge('parse', 0.0)
    assert manager.stage_cpu.get('parse', 0) >= 0


class _Inbox:
    def __init__(self, wait=0.0):
        self.wait = wait
        self.invalid = threading.Event()
        self.resync = threading.Event()
        self.resyncs = 0

    def pending_wait_seconds(self):
        return self.wait


def _manager():
    manager = observer.MarketConnections()
    manager.balancer.groups = {'ay': 'up_down:BTC:5m:outcome:up', 'an': 'up_down:BTC:5m:outcome:down',
                               'cold': 'other:x'}
    inbox = _Inbox()
    primary = dict(connected=True, assets=['ay', 'an'], inbox=inbox, source='primary')
    cold = dict(connected=True, assets=['cold'], inbox=_Inbox(9.0), source='cold')
    manager.states = {0: primary, 1: cold}
    return manager, primary, inbox


def test_connected_primary_backlog_counts_as_a_business_gap():
    manager, primary, inbox = _manager()
    inbox.wait = 0.4
    manager.observe_backlog()
    assert manager.business_gap_metrics() == {}
    inbox.wait = 2.0
    manager.observe_backlog()
    inbox.wait = 7.5
    manager.observe_backlog()
    live = manager.business_gap_metrics()['up_down:BTC:5m']
    assert live['over_limit'] == 1 and live['backlog_open'][0]['gap_s'] == 7.5
    inbox.resync.set()
    inbox.resyncs = 1
    inbox.wait = 0.0
    manager.observe_backlog()  # discarded by resync: still a gap until the queue runs again
    assert manager.backlog_open
    inbox.resync.clear()
    manager.observe_backlog()
    gap = manager.business_gap_metrics()['up_down:BTC:5m']
    assert gap['backlog_events'] == 1 and gap['backlog_max_gap_s'] == 7.5 and gap['max_gap_s'] == 7.5
    assert gap['over_limit'] == 1 and gap['recent'][0]['resyncs'] == 1
    assert gap['recent'][0]['ended_by'] == 'drained' and gap['backlog_seconds'] >= 7.5
    assert 'other:x' not in manager.business_gap_metrics()


def test_kick_and_backlog_are_one_metric_and_merge_across_children():
    manager, primary, inbox = _manager()
    inbox.wait = 3.0
    manager.observe_backlog()
    primary['connected'] = False  # kicked: the kick event carries merged_gap_ms
    manager.observe_backlog()
    manager.primary_failures['up_down:BTC:5m'] = dict(total=1, without_ready_backup=0,
                                                      events=[dict(merged_gap_ms=3010.0)])
    gap = manager.business_gap_metrics()['up_down:BTC:5m']
    assert gap['kick_over_limit'] == 1 and gap['backlog_events'] == 1 and gap['over_limit'] == 1
    assert gap['max_gap_s'] == 3.01
    other = dict(limit_s=1.5, over_limit=0, max_gap_s=0)
    merged = merge_feed_status([
        dict(key='up_down:BTC:5m', status={'ws_business_gaps': {'up_down:BTC:5m': gap}}),
        dict(key='up_down:ETH:5m', status={'ws_business_gaps': {'up_down:ETH:5m': other}}),
    ])
    assert merged['ws_business_gaps']['up_down:BTC:5m']['over_limit'] == 1
    assert merged['ws_business_gaps']['up_down:ETH:5m'] == other


def test_replaced_state_closes_its_episode():
    manager, primary, inbox = _manager()
    inbox.wait = 4.0
    manager.observe_backlog()
    manager.states = {}
    manager.observe_backlog()
    gap = manager.business_gap_metrics()['up_down:BTC:5m']
    assert gap['recent'][0]['ended_by'] == 'replaced' and gap['over_limit'] == 1
