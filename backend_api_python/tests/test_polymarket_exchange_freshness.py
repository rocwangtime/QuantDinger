"""Exchange freshness stays independent of callback arrival and persistence."""
import json
from datetime import datetime, timedelta, timezone
from unittest.mock import Mock

import pytest

from app.services.polymarket import observer, recording
from app.services.polymarket.account_history import BookHistory


def snapshot(timestamp, asset='u'):
    return dict(event_type='book', asset_id=asset, timestamp=timestamp,
                asks=[dict(price='.01', size='100')], bids=[])


@pytest.mark.parametrize('timestamp', [None, '', 'bad', 35001, -30000])
def test_recently_received_history_rejects_unknown_future_and_stale(timestamp):
    h = BookHistory()
    h.record(snapshot(timestamp), 35000)
    assert h.lookup(h.copy_assets(['u']), 'u', 35000, 2000) is None


@pytest.mark.parametrize('enveloped', [False, True])
def test_history_checks_exact_arrival_and_each_delta_exchange_time(enveloped):
    h = BookHistory()
    h.record(snapshot(1000), 1100)
    payload = dict(timestamp=5000, priceChanges=[dict(assetId='u', side='SELL', price='.01', size='2')])
    event = dict(type='price_change', payload=payload) if enveloped else dict(
        event_type='price_change', timestamp=5000, price_changes=payload['priceChanges'])
    h.record(event, 1200)
    states = h.copy_assets(['u'])
    assert h.lookup(states, 'u', 1199, 2000)['exchangeMs'] == 1000
    assert h.lookup(states, 'u', 1200, 2000) is None
    assert h.lookup(states, 'u', 5000, 2000) is None  # Receipt age also remains bounded.
    h.record(snapshot(6000), 6000)
    assert h.lookup(h.copy_assets(['u']), 'u', 8000, 2000)['exchangeMs'] == 6000
    assert h.lookup(h.copy_assets(['u']), 'u', 8001, 2000) is None


@pytest.mark.parametrize('timestamp,accepted', [(1000, True), (None, False), (-64000, False), (1101, False)])
@pytest.mark.parametrize('pruning', [dict(maximum=1), dict(seconds=.05)])
def test_pruned_base_preserves_exchange_timestamp(timestamp, accepted, pruning):
    h = BookHistory(**pruning)
    h.record(snapshot(timestamp), 1000)
    h.record(snapshot(1200), 1200)
    states = h.copy_assets(['u'])
    assert states['u']['base_at'] == 1000
    assert states['u']['base_exchange_ms'] == timestamp
    result = h.lookup(states, 'u', 1100, 2000)
    assert (result is not None) == accepted
    if result:
        assert result['exchangeMs'] == timestamp
    assert h.lookup(states, 'u', 999, 2000) is None


@pytest.mark.parametrize('timestamp,accepted', [(1000, True), (None, False), (-64000, False), (1101, False)])
def test_rest_snapshot_keeps_original_exchange_time(timestamp, accepted):
    h = BookHistory()
    book = dict(exchangeTimestamp=timestamp, asks=[dict(price='.01', size='100')], bids=[])
    h.record_rest('u', book, 1100)
    result = h.lookup(h.copy_assets(['u']), 'u', 1100, 2000)
    assert (result is not None) == accepted
    if result:
        assert result['observedMs'] == 1100 and result['exchangeMs'] == timestamp
    assert book['exchangeTimestamp'] == timestamp


@pytest.mark.parametrize('enabled', [True, False])
@pytest.mark.parametrize('age', [None, -1, 2.001, 65])
def test_dirty_stale_market_ends_episode_before_any_evaluation(monkeypatch, enabled, age):
    monkeypatch.setenv('POLYMARKET_OBSERVER_RECORDING_ENABLED', str(enabled))
    p = observer.Pipeline()
    m = dict(id='freshness', yesAssetId='u', noAssetId='d', market_type='yes_no')
    p.set_markets([m])
    at = datetime.now(timezone.utc)
    for token, seconds in [('u', 0), ('d', age)]:
        timestamp = None if seconds is None else int((at - timedelta(seconds=seconds)).timestamp() * 1000)
        p.receive(json.dumps(snapshot(timestamp, token)))
    assert set(p.recording.metadata) == {'u', 'd'}
    p.paper_account = Mock()
    end = Mock()
    monkeypatch.setattr(observer, '_end_episode', end)
    monkeypatch.setattr(observer, 'opportunity', Mock(side_effect=AssertionError('stale quote')))
    monkeypatch.setattr(observer, 'record_research', Mock(side_effect=AssertionError('stale research')))
    p.evaluate(10)
    end.assert_called_once()
    assert end.call_args.args[0] == 'freshness'
    p.paper_account.submit.assert_not_called()
    assert not p.dirty
    if not enabled:
        assert not p.recording.pending
        assert not any(item[1] in {'book', 'trade', 'window'} for item in p.queue.items)


def test_disabled_recorder_tracks_enveloped_duplicate_deltas_without_staging(monkeypatch):
    monkeypatch.setenv('POLYMARKET_OBSERVER_RECORDING_ENABLED', 'false')
    r = recording.Recorder()
    at = datetime.now(timezone.utc)
    r.receive(dict(type='price_change', payload=dict(timestamp=1000, priceChanges=[
        dict(assetId='u'), dict(assetId='u'), dict(assetId='d')])), ['u', 'u', 'd'], {}, {}, at, owned={'u'})
    assert r.metadata == {'u': (recording.exchange_time(1000), at, 'ws')}
    r.receive(dict(event_type='last_trade_price', asset_id='u', price='.5', side='BUY'), [], {}, {}, at)
    assert not r.pending


@pytest.mark.parametrize('timestamp', [None, -64000, 1201])
def test_delta_timestamp_survives_pruning_into_base(timestamp):
    h = BookHistory(maximum=1)
    h.record(snapshot(1000), 1000)
    h.record(dict(event_type='price_change', timestamp=timestamp, price_changes=[
        dict(asset_id='u', side='SELL', price='.01', size='2')]), 1100)
    h.record(snapshot(1300), 1300)
    states = h.copy_assets(['u'])
    assert states['u']['base_at'] == 1100
    assert states['u']['base_exchange_ms'] == timestamp
    assert states['u']['base']['asks']['.01'] == '2'
    assert h.lookup(states, 'u', 1200, 2000) is None


def test_fresh_market_evaluates_with_recording_disabled(monkeypatch):
    monkeypatch.setenv('POLYMARKET_OBSERVER_RECORDING_ENABLED', 'false')
    p = observer.Pipeline()
    m = dict(id='fresh', yesAssetId='u', noAssetId='d')
    p.set_markets([m])
    timestamp = int(datetime.now(timezone.utc).timestamp() * 1000)
    for token in ('u', 'd'):
        p.receive(json.dumps(snapshot(timestamp, token)))
    p.paper_account = Mock()
    quote = Mock(return_value=None)
    research = Mock()
    monkeypatch.setattr(observer, 'opportunity', quote)
    monkeypatch.setattr(observer, 'record_research', research)
    monkeypatch.setattr(observer, '_end_episode', Mock())
    p.evaluate(10)
    quote.assert_called_once()
    research.assert_called_once()
    p.paper_account.submit.assert_called_once()
    assert not p.recording.pending


def test_missing_token_metadata_fails_closed(monkeypatch):
    p = observer.Pipeline()
    m = dict(id='missing', yesAssetId='u', noAssetId='d')
    p.set_markets([m])
    for token in ('u', 'd'):
        p.receive(json.dumps(snapshot(int(datetime.now(timezone.utc).timestamp() * 1000), token)))
    del p.recording.metadata['d']
    end = Mock()
    monkeypatch.setattr(observer, '_end_episode', end)
    monkeypatch.setattr(observer, 'opportunity', Mock(side_effect=AssertionError('missing metadata quote')))
    p.evaluate(10)
    end.assert_called_once()


@pytest.mark.parametrize('times,expected', [
    ([1000, 1250], True), ([1000, 1251], False),
    ([None, 1250], False), ([1000, 3001], False),
    ([999, 1250], False), ([1000], False), ([], False),
])
def test_pair_boundary_and_individual_age(times, expected):
    from app.services.polymarket.account_history import coherent_pair
    assert coherent_pair(times, 3000) is expected


@pytest.mark.parametrize('enabled', [True, False])
@pytest.mark.parametrize('skew,accepted', [(250, True), (251, False)])
def test_observer_pair_admission_and_counters(monkeypatch, enabled, skew, accepted):
    monkeypatch.setenv('POLYMARKET_OBSERVER_RECORDING_ENABLED', str(enabled))
    monkeypatch.setenv('POLYMARKET_OBSERVER_MAX_PAIR_SKEW_MS', '250')
    p = observer.Pipeline()
    p.set_markets([dict(id='pair', yesAssetId='u', noAssetId='d')])
    at = int(datetime.now(timezone.utc).timestamp() * 1000) - 100
    for token, timestamp in [('u', at), ('d', at - skew)]:
        p.receive(json.dumps(snapshot(timestamp, token)))
    p.paper_account = Mock()
    monkeypatch.setattr(observer, 'opportunity', Mock(return_value=None))
    monkeypatch.setattr(observer, 'record_research', Mock())
    monkeypatch.setattr(observer, '_end_episode', Mock())
    p.evaluate(10)
    assert p.paper_account.submit.called is accepted
    # Remove the mock runtime before asking for real status metrics.
    p.paper_account = None
    assert p.metrics()['pair_rejections'] == dict(stale=0, skew=int(not accepted))
    p.recording.metadata.pop('d')
    p.dirty.add('pair')
    p.evaluate(12)
    assert p.metrics()['pair_rejections'] == dict(stale=1, skew=int(not accepted))


@pytest.mark.parametrize('skew,limit,accepted', [(250, 250, True), (251, 250, False), (251, 300, True)])
def test_taker_independent_admission_and_delayed_lookup(monkeypatch, skew, limit, accepted):
    import threading
    from types import SimpleNamespace
    from app.services.polymarket.paper_account import Runtime
    monkeypatch.setenv('POLYMARKET_OBSERVER_MAX_PAIR_SKEW_MS', str(limit))
    runtime = Runtime(SimpleNamespace(lock=threading.Lock()))
    market = dict(id='pair', yesAssetId='u', noAssetId='d')
    for token, timestamp in [('u', 2000), ('d', 2000 - skew)]:
        runtime.history.record(snapshot(timestamp, token), 2000)
    account = Mock()
    runtime.admit(account, market, 2000, 2000)
    assert account.signal.called is accepted
    # A delayed leg uses its own fresh historical book even if the pair is incoherent.
    assert all(runtime.lookup(market, 2100))
    runtime.history.record(snapshot(2200, 'u'), 2200)
    assert runtime.lookup(market, 2100)[0]['exchangeMs'] == 2000


def test_unwind_uses_fresh_held_leg_when_complement_is_missing():
    from tests.test_polymarket_paper_account import market, books
    from app.services.polymarket.paper_account import Account
    a = Account(dict(merge_cost='0', competitor_top='0'))
    trade = a.signal(market(), books(), 1000, dict(one_way_ms=100))
    def lookup(market, at):
        pair = books(bid='.3')
        return [pair[0], None] if at >= 1200 else pair
    for at in (1100, 1200, 1300):
        a.tick(at, lookup)
    assert trade['status'] == 'single_leg_unwound'


@pytest.mark.parametrize('value,expected', [(None, 'unspecified'), (' Unverified ', 'unverified'), ('verified', 'unspecified')])
def test_api_annotation_on_status_and_ws_only_reports(monkeypatch, value, expected):
    from app.services.polymarket import account_api
    if value is None:
        monkeypatch.delenv('POLYMARKET_PAPER_ACCOUNT_TAKER_STATUS', raising=False)
    else:
        monkeypatch.setenv('POLYMARKET_PAPER_ACCOUNT_TAKER_STATUS', value)
    monkeypatch.setattr(account_api, 'query', lambda sql, params: [dict(day='2026-10-15', reconnect_count=1,
        slow_consumer_count=0, rebalance_count=0, shard_msg_totals={})] if 'ws_daily' in sql else [])
    assert account_api.status()['taker_verification_status'] == expected
    assert account_api.reports()[0]['taker_verification_status'] == expected


@pytest.mark.parametrize('timestamp', [None, 2001, -1])
def test_taker_rejects_missing_future_old_signal_timestamp(timestamp):
    import threading
    from types import SimpleNamespace
    from app.services.polymarket.paper_account import Runtime
    runtime = Runtime(SimpleNamespace(lock=threading.Lock()))
    market = dict(id='pair', yesAssetId='u', noAssetId='d')
    runtime.history.record(snapshot(2000, 'u'), 2000)
    runtime.history.record(snapshot(timestamp, 'd'), 2000)
    account = Mock()
    runtime.admit(account, market, 2000, 2000)
    account.signal.assert_not_called()
