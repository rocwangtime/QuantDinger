"""Continuous paper sizing, fills, capital invariants, history and HTTP contracts."""
import copy
import inspect
import time

import pytest

from app.services.polymarket import account_model as m
from app.services.polymarket.account_history import BookHistory
from app.services.polymarket.paper_account import Account, Latency, enabled
from app.services.polymarket.engine import digest, number


def market(**extra):
    return dict(id='42', question='Paper test', conditionId='0x' + 'a' * 64, yesAssetId='11', noAssetId='22',
                feeRate='0', tickSize='0.01', minOrderNotional='1', endDate='2030-01-01T00:00:00Z', **extra)


def books(price='.40', depth='10000', bid='.39'):
    return [dict(assetId=a, observedMs=1000, asks=[dict(price=price, size=depth)],
                 bids=[dict(price=bid, size=depth)]) for a in ('11', '22')]


def sizing(config=None, **values):
    return m.size(market(), books(), {**m.DEFAULTS, 'merge_cost': '0', **(config or {})},
                  **{'free': '5000', 'locked': '0', 'equity': '5000', **values})


@pytest.mark.parametrize('config,values,binding', [
    ({'trade_cap': '500'}, {}, 'trade'), ({}, {'free': '1000'}, 'free'),
    ({}, {'locked': '2950'}, 'portfolio'), ({'full_edge_bps': '10000'}, {}, 'edge'),
])
def test_each_cash_cap_binds(config, values, binding):
    result = sizing(config, **values)
    assert result['binding'] == binding and not result['reason']
    assert number(result['reserve']) <= min(map(number, result['caps'].values()))


def test_depth_cap_and_marginal_both_book_walk():
    b = books(depth='10')
    b[0]['asks'].append(dict(price='.65', size='100'))
    b[1]['asks'].append(dict(price='.5', size='100'))
    d = m.size(market(), b, dict(merge_cost='0'), free=5000, locked=0, equity=5000)
    assert d['binding'] == 'depth' and number('9.9') < number(d['quantity']) <= 10
    b[1]['asks'][0]['size'] = '5'
    d = m.size(market(), b, dict(merge_cost='0'), free=5000, locked=0, equity=5000)
    assert number(d['quantity']) <= 10


@pytest.mark.parametrize('price,expected', [('.4995', 10), ('.49725', 505), ('.495', 1000), ('.4', 1000)])
def test_edge_scaled_ticket_threshold_midpoint_full_cap(price, expected):
    d = m.size({**market(), 'tickSize': '.0001'}, books(price), dict(merge_cost='0'), free=5000, locked=0, equity=5000)
    assert number(d['caps']['edge']) == expected


def test_minimum_notional_size_and_price_tick_round_down():
    d = m.size({**market(), 'minOrderNotional': '1000'}, books(), {}, free=100, locked=0, equity=100)
    assert d['reason'] == 'below_minimum'
    d = m.size(market(), books('.401'), dict(merge_cost='0', size_step='.3', slippage='.009'), free=5000, locked=0, equity=5000)
    assert number(d['quantity']) % number('.3') == 0 and d['limits'] == ['0.41', '0.41']
    assert number(d['reserve']) <= number('1000')


def test_lock_free_loss_stop_and_cooldown_limits():
    assert sizing(locked='3000')['reason'] == 'below_minimum'
    assert number(sizing(free='1000')['reserve']) <= 200
    assert sizing(stopped=True)['reason'] == 'daily_loss_stop'
    assert sizing(cooldown=True)['reason'] == 'cooldown'
    a = Account(dict(taker_enabled=True, merge_cost='0'))
    a.signal(market(), books(), 1000, dict(one_way_ms=100))
    assert a.signal(market(), books(), 1050, dict(one_way_ms=100)) is None
    a.transfer('free', -number('200'))
    a.risk(1050)
    assert a.daily(1050)['loss_stop']
    assert any(e['event'] == 'daily_loss_stop' for e in a.state['events'])


def test_history_returns_book_at_arrival_not_later_and_is_bounded():
    h = BookHistory(seconds=10, maximum=3)
    h.record(dict(event_type='book', asset_id='11', timestamp=1000, asks=[dict(price='.4', size='100')], bids=[]), 1000)
    h.record(dict(event_type='price_change', timestamp=1100, price_changes=[dict(asset_id='11', side='SELL', price='.4', size='1')]), 1100)
    states = h.copy_assets(['11'])
    assert h.lookup(states, '11', 1050, 2000)['asks'][0]['size'] == '100'
    assert h.lookup(states, '11', 1150, 2000)['asks'][0]['size'] == '1'
    assert h.lookup(states, '11', 900, 2000) is None
    assert h.lookup(states, '11', 5000, 2000) is None
    for at in (1200, 1300, 1400, 1500):
        h.record(dict(event_type='price_change', timestamp=1100, price_changes=[dict(asset_id='11', side='SELL', price='.4', size='2')]), at)
    assert len(h.assets['11']['events']) == 3
    assert h.lookup(h.copy_assets(['11']), '11', 1490, 2000)['asks'][0]['size'] == '2'


def test_competitor_share_fok_and_subsequent_levels():
    b = books(depth='100')[0]
    assert m.fill(market(), b, 51, m.DEFAULTS, '.4')['filled'] == '0'
    assert m.fill(market(), b, 50, m.DEFAULTS, '.4')['filled'] == '50'
    b['asks'].append(dict(price='.41', size='20'))
    assert number(m.fill(market(), b, 70, m.DEFAULTS, '.41')['filled']) == 70
    assert number(m.fill(market(), b, 70, dict(m.DEFAULTS, competitor_other='.5'), '.41')['filled']) == 0


def run(second_bad=False, residual=False, config=None):
    a = Account(dict(taker_enabled=True, merge_cost='0', **(config or {})))
    trade = a.signal(market(), books(), 1000, dict(one_way_ms=100))
    def lookup(market, at):
        b = books(depth='10000', bid='.3')
        if second_bad and at >= 1200:
            b[1]['asks'][0]['price'] = '.9'
        if residual:
            b[0]['bids'][0]['size'] = '2'
        return b
    a.tick(1099, lookup)
    assert not trade['evidence']['steps']
    a.tick(1100, lookup)
    a.tick(1200, lookup)
    if second_bad:
        a.tick(1300, lookup)
    return a, trade


def test_merge_locks_cost_and_realizes_pnl_only_at_release():
    a, t = run()
    assert t['status'] == 'filled_merged'
    assert number(a.state['locked']) == number(t['result']['cost'])
    assert number(a.daily(1200)['net_pnl']) == 0
    before = a.equity()
    a.tick(61200, lambda *args: None)
    assert number(a.state['locked']) == 0 and not a.state['holdings']
    assert a.equity() > before and number(a.daily(61200)['net_pnl']) > 0
    assert a.equity() == sum(number(a.state[k]) for k in ('free', 'locked', 'positions'))
    assert number(a.state['free']) >= 0


def test_merge_disabled_holds_to_end_date():
    a, t = run(config={'merge_enabled': False})
    assert t['release_ms'] == 1893456000000
    a.tick(1000000, lambda *args: None)
    assert a.state['holdings']


def test_leg_two_failure_unwind_fees_and_loss():
    a, t = run(second_bad=True, config={'competitor_top': '0'})
    assert t['status'] == 'single_leg_unwound'
    assert number(t['result']['realized_pnl']) < 0
    assert number(a.state['locked']) == 0 and a.equity() < 5000
    evidence = copy.deepcopy(t['evidence'])
    evidence['market']['feeRate'] = '.07'
    result = m.outcome(evidence)
    assert number(result['fees']) > 0
    assert number(result['realized_pnl']) < number(t['result']['realized_pnl'])


def test_residual_zero_mark_cost_basis_resolution_and_no_negative_cash():
    a, t = run(second_bad=True, residual=True)
    assert t['status'] == 'single_leg_residual'
    holding = a.state['holdings'][t['id']]
    assert number(holding['cost']) > 0 and a.state['positions'] == '0'
    assert number(a.state['free']) >= 0 and a.equity() == number(a.state['free'])
    a.resolve(t['id'], '1', 1400)
    assert not a.state['holdings']


@pytest.mark.parametrize('second_bad,residual', [(False, False), (True, False), (True, True)])
def test_frozen_replay_matches_without_mutating_evidence(second_bad, residual):
    _, t = run(second_bad, residual)
    before = copy.deepcopy(t['evidence'])
    result = m.replay(t['evidence'], t['result'], digest(t['evidence']))
    assert result['replayMatches'] and t['evidence'] == before
    with pytest.raises(ValueError, match='HashMismatch'):
        m.replay(t['evidence'], t['result'], 'bad')


def test_first_leg_fok_failure_releases_reservation():
    a = Account({'taker_enabled': True})
    t = a.signal(market(), books(), 1000, dict(one_way_ms=100))
    a.tick(1100, lambda *args: books(depth='1'))
    assert t['status'] == 'not_filled' and number(a.state['free']) == 5000
    assert number(a.state['locked']) == 0


def test_latency_median_p90_fallback_and_flag_default_off(monkeypatch):
    monkeypatch.delenv('POLYMARKET_PAPER_ACCOUNT_ENABLED', raising=False)
    assert not enabled()
    l = Latency(m.DEFAULTS)
    assert l.status(1000)['fallback']
    l.samples.extend([10, 20, 30, 40, 100]); l.checked_ms = 1000
    assert l.status(1000)['median_rtt_ms'] == 30 and l.status(1000)['p90_rtt_ms'] == 100
    assert l.status(1000000)['fallback']


def test_account_routes_auth_and_openapi(client, app):
    from app.openapi import get_openapi_api
    paths = get_openapi_api(app).spec.to_dict()['paths']
    for suffix in ('status', 'trades', 'daily-report', 'export'):
        path = '/api/polymarket/paper-account/' + suffix
        assert client.get(path).status_code == 401 and path in paths
    assert client.post('/api/polymarket/paper-account/trades/test/replay').status_code == 401


def test_route_status_shape_and_invalid_filters(app, client, monkeypatch):
    from app.routes import polymarket as routes
    from app.services.polymarket import account_api
    monkeypatch.setattr(account_api, 'status', lambda: Account({'taker_enabled': True}).status(1000))
    with app.test_request_context():
        response, status = inspect.unwrap(routes.account_status)()
        assert status == 200 and response.get_json()['data']['mode'] == 'paper'
    from app.utils import auth
    monkeypatch.setattr(auth, 'verify_token', lambda _: {'user_id': 7, '_verified_username': 'test', '_verified_user_role': 'user'})
    for suffix in ('trades?limit=101', 'trades?status=live', 'daily-report?days=91', 'export?format=x'):
        assert client.get('/api/polymarket/paper-account/' + suffix, headers={'Authorization': 'Bearer test'}).status_code == 400


def test_utc_day_stop_resets_and_completed_attempt_refreshes_cooldown():
    a, t = run(second_bad=True)
    assert a.daily(1300)['loss_stop']
    assert a.state['cooldowns']['42'] == 61300
    a.tick(86400000, lambda *args: None)
    assert not a.daily(86400000)['loss_stop']
    assert number(a.daily(86400000)['start_equity']) == a.equity()
    assert a.signal(market(), books(), 86400000, dict(one_way_ms=100)) is not None
    assert t['result']['status'] == 'single_leg_unwound'


def test_unresolved_residual_cost_also_counts_against_deployment_cap():
    a, t = run(second_bad=True, residual=True)
    # A zero mark does not make a large unresolved cost basis reusable exposure.
    a.config['locked_fraction'] = '.01'
    d = a.signal({**market(), 'id': '43'}, books(), 1400, dict(one_way_ms=100))
    assert d['status'] == 'cancelled_by_limit'
    assert number(d['evidence']['decision']['inputs']['locked']) == number(t['result']['residual_cost'])


def test_restart_cancellation_replay_and_released_pnl():
    a = Account({'taker_enabled': True})
    t = a.signal(market(), books(), 1000, dict(one_way_ms=100))
    t['evidence']['decision']['reason'] = 'restart_cancelled'
    t['evidence']['restart_cancelled'] = True
    a.finish(t, 1100)
    assert m.replay(t['evidence'], t['result'], digest(t['evidence']))['replayMatches']
    a, t = run()
    a.tick(61200, lambda *args: None)
    released = a.changed[-1]
    assert number(released['result']['realized_pnl']) > 0
    assert m.replay(released['evidence'], released['result'], digest(released['evidence']))['replayMatches']


def test_replay_detects_inconsistent_sizing_even_with_valid_bundle_digest():
    _, t = run()
    t['evidence']['decision']['caps']['trade'] = '999'
    result = m.replay(t['evidence'], t['result'], digest(t['evidence']))
    assert not result['replayMatches'] and not result['sizingMatches']


def test_verified_crypto_fee_can_erase_gross_edge_and_no_attempt_is_fabricated():
    a = Account({'taker_enabled': True})
    mkt = {**market(), 'feeRate': '.07', 'market_type': 'up_down'}
    assert a.signal(mkt, books('.49'), 1000, dict(one_way_ms=100)) is None
    assert a.daily(1000)['opportunities'] == a.daily(1000)['attempted'] == 0


def test_latency_probe_is_public_get_and_failure_uses_configured_fallback():
    from types import SimpleNamespace
    calls = []
    latency = Latency(m.DEFAULTS)
    client = SimpleNamespace(server_time=lambda: calls.append('server_time'))
    latency.measure(client, 1000)
    assert calls == ['server_time']
    assert not latency.status(1000)['fallback']
    def fail(*args):
        raise ValueError('public_api_failed')
    client.server_time = fail
    latency.measure(client, 2000)
    assert latency.status(2000)['fallback'] and latency.status(2000)['one_way_ms'] == 150


def test_authenticated_account_lists_reports_replay_and_download_shapes(client, monkeypatch):
    from app.services.polymarket import account_api
    from app.utils import auth
    monkeypatch.setattr(auth, 'verify_token', lambda _: {'user_id': 7, '_verified_username': 'test', '_verified_user_role': 'user'})
    captured = {}
    def trades(**kw):
        captured.update(kw)
        return dict(items=[dict(id='test', status='not_filled')], total=1, limit=kw['limit'], offset=kw['offset'])
    monkeypatch.setattr(account_api, 'trades', trades)
    monkeypatch.setattr(account_api, 'reports', lambda **kw: [dict(day='2026-10-11', taker_verification_status='unverified', attempted=1, filled=0, opportunities=2, maker=dict(strategy='maker_complete_set_v1', paired_sets='2'))])
    monkeypatch.setattr(account_api, 'replay', lambda identity: dict(replayMatches=True, evidenceHash='f' * 64))
    headers = {'Authorization': 'Bearer test'}
    base = '/api/polymarket/paper-account/'
    row = client.get(base + 'trades?status=not_filled&limit=10&offset=2', headers=headers).get_json()['data']
    assert row['total'] == 1 and captured == dict(status='not_filled', limit=10, offset=2)
    assert client.get(base + 'daily-report', headers=headers).get_json()['data'][0]['opportunities'] == 2
    assert client.post(base + 'trades/test/replay', headers=headers).get_json()['data']['replayMatches']
    response = client.get(base + 'export?format=csv', headers=headers)
    assert response.status_code == 200 and 'attachment' in response.headers['Content-Disposition']
    assert 'day,taker_verification_status,attempted,filled' in response.get_data(as_text=True)
    import csv
    import io
    import json
    csv_row = next(csv.DictReader(io.StringIO(response.get_data(as_text=True))))
    assert csv_row['taker_verification_status'] == 'unverified'
    assert json.loads(csv_row['maker'])['paired_sets'] == '2'
    response = client.get(base + 'export?format=json', headers=headers)
    assert response.get_json()[0]['taker_verification_status'] == 'unverified'
    assert response.get_json()[0]['day'] == '2026-10-11'
    assert response.get_json()[0]['maker']['strategy'] == 'maker_complete_set_v1'


def test_account_history_lookup_copies_events_without_mutating_receive_state():
    h = BookHistory()
    h.record(dict(event_type='book', asset_id='11', timestamp=1000, asks=[dict(price='.4', size='100')], bids=[]), 1000)
    snapshot = h.copy_assets(['11'])
    first = h.lookup(snapshot, '11', 1000, 2000)
    first['asks'][0]['size'] = '0'
    assert h.lookup(h.copy_assets(['11']), '11', 1000, 2000)['asks'][0]['size'] == '100'


def test_enabled_receive_history_stays_free_of_database_work(monkeypatch):
    import json
    from app.services.polymarket import observer, paper_account
    monkeypatch.setattr(paper_account, 'enabled', lambda: True)
    monkeypatch.setattr(observer, 'get_db_connection', lambda: pytest.fail('receive must never access SQL'))
    pipeline = observer.Pipeline()
    pipeline.set_markets([market()])
    for asset in ('11', '22'):
        pipeline.receive(json.dumps(dict(event_type='book', asset_id=asset, timestamp=int(time.time() * 1000), asks=[dict(price='.4', size='100')], bids=[])))
    assert pipeline.messages == 2 and pipeline.dirty == {'42'}
    assert pipeline.paper_account.inbox.empty()
    assert not pipeline.paper_account.thread.is_alive()
    at = max(pipeline.paper_account.history.assets[a]['events'][-1][0] for a in ('11', '22'))
    assert all(number(b['asks'][0]['price']) == number('.4') for b in pipeline.paper_account.lookup(market(), at))


@pytest.mark.parametrize('extra,expected', [
    ({}, '1'), ({'umaResolutionStatus': 'proposed'}, None), ({'umaResolutionStatus': None}, None),
    ({'closed': False}, None), ({'outcomePrices': ['.999', '.001']}, None),
    ({'conditionId': 'different'}, None), ({'outcomes': ['Down', 'Up'], 'outcomePrices': ['0', '1']}, '1'),
])
def test_residual_resolution_requires_final_public_binary_outcomes(extra, expected):
    from app.services.polymarket.paper_account import resolution_price
    row = dict(conditionId=market()['conditionId'], closed=True, umaResolutionStatus='resolved',
               outcomes=['Yes', 'No'], outcomePrices=['1', '0'])
    assert resolution_price({**row, **extra}, market()) == expected


def test_history_handles_zero_size_deltas_and_invalid_depth_fail_closed():
    h = BookHistory()
    h.record(dict(type='book', payload=dict(assetId='11', timestamp=1000, asks=[dict(price='.4', size='100')], bids=[])), 1000)
    h.record(dict(type='price_change', payload=dict(timestamp=1100, priceChanges=[dict(assetId='11', side='SELL', price='.4', size='0.0')])), 1100)
    assert h.lookup(h.copy_assets(['11']), '11', 1150, 2000)['asks'] == []
    h.record(dict(type='book', payload=dict(assetId='11', timestamp=1000, asks=[dict(price='NaN', size='100')], bids=[])), 1200)
    assert h.lookup(h.copy_assets(['11']), '11', 1200, 2000) is None
