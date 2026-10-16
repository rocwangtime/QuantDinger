"""v3 paper maker: order management, pessimistic fills, ledger and decomposition."""
import copy
import math
import random
from decimal import Decimal as D

import pytest

from app.services.polymarket.maker import Maker, TAG, enabled, rebate
from app.services.polymarket.paper_account import Account, day_key, utc

NOW = 1780000000000
CHAINLINK = 'https://data.chain.link/streams/btc-usd'
BINANCE = 'https://www.binance.com/en/trade/BTC_USDT'


def market(identity='42', start=NOW + 120000, duration=300000, interval='5m', asset='BTC', source=CHAINLINK, **extra):
    """Default: BTC 5m whose strike window opens at NOW + 60 s, so NOW is pre-open."""
    return dict(id=identity, market_type='up_down', asset=asset, interval=interval, yesAssetId=identity + 'u',
                noAssetId=identity + 'd', tickSize='.01', feeRate='.07', minOrderNotional='5', resolutionSource=source,
                eventStartTime=utc(start).isoformat(), endDate=utc(start + duration).isoformat(), **extra)


def hourly(identity='77', start=NOW - 600000, strike='60000'):
    """BTC 1h on Binance candles (w=0), already open with a known strike."""
    return market(identity, start=start, duration=3600000, interval='1h', source=BINANCE, price_to_beat=strike)


def book(bid='.49', ask='.51', bid_size='100', ask_size='100', at=NOW, more=()):
    return dict(bids=[dict(price=bid, size=bid_size), *more], asks=[dict(price=ask, size=ask_size)], exchange_at=at)


def books(at=NOW, up=None, down=None):
    return [up or book(at=at), down or book(at=at)]


def print_(identity='p', price='.48', size='3', at=NOW + 300, side='SELL', tx=None):
    return dict(recording_id=identity, price=price, size=size, side=side, exchange_ms=at, received_ms=at + 10,
                transaction_hash=tx)


def admit(m, *cells, mode='full'):
    m.state['selection'] = dict(reviewed=day_key(NOW), active={c: dict(source='admitted', since='', mode=mode) for c in cells},
                                cooloff={}, lcb_history={}, scores={}, breakers={})


def setup(config=None, cells=('BTC:5m', 'BTC:1h', 'ETH:5m', 'BTC:15m'), account=None):
    a = account or Account()
    m = Maker(a, dict(config or {}))
    admit(m, *cells)
    return a, m


def step(m, mk=None, at=NOW, bk=None, spot='60000'):
    mk = mk or market()
    if spot is not None:
        m.observe_spot(mk['asset'], spot, at, at)
    m.step(mk, bk or books(at), at)
    return m.state['markets'].get(mk['id'])


def live(row, side=None):
    return [o for o in row['orders'] if o['kind'] == 'live' and o['cancel_at'] is None
            and (side is None or o['side'] == side)]


def warm(m, mk, start, end, spot='60000'):
    """Spot every 5 s from `start`, covering any strike window before `end`."""
    for at in range(start, end, 5000):
        m.observe_spot(mk['asset'], spot, at, at)


def test_preopen_quotes_fair_minus_hmin_and_reserves_capital(monkeypatch):
    monkeypatch.delenv('POLYMARKET_PAPER_MAKER_ENABLED', raising=False)
    assert not enabled()
    a, m = setup()
    row = step(m)
    assert row['phase'] == 'PREOPEN'
    orders = live(row)
    assert [(o['side'], o['price'], o['size']) for o in orders] == [(0, '0.49', '10.00'), (1, '0.49', '10.00')]
    assert sum(D(o['price']) for o in orders) <= 1 - 2 * D('.01')
    assert D(a.state['locked']) == D('9.80') and a.equity() == 5000
    assert all(o['live_from'] == NOW + 250 and o['ahead'] == '100' for o in orders)


def test_place_latency_then_queue_ahead_consumed_before_us():
    a, m = setup()
    row = step(m)
    m.prints('42', 0, [print_('early', price='.49', size='500', at=NOW + 100)], NOW + 200)
    assert row['qty'] == ['0', '0'] and live(row, 0)[0]['ahead'] == '100'
    m.prints('42', 0, [print_('a', price='.49', size='60')], NOW + 400)
    assert row['qty'] == ['0', '0'] and live(row, 0)[0]['ahead'] == '40'
    m.prints('42', 0, [print_('b', price='.49', size='45', at=NOW + 350)], NOW + 400)
    assert D(row['qty'][0]) == 5
    m.prints('42', 0, [print_('b', price='.49', size='45', at=NOW + 350)], NOW + 400)  # Idempotent.
    assert D(row['qty'][0]) == 5 and D(a.state['positions']) == D('2.45')


def test_trade_through_fills_the_whole_remainder_at_our_price():
    a, m = setup()
    row = step(m)
    m.prints('42', 0, [print_('through', price='.48', size='1')], NOW + 400)
    assert D(row['qty'][0]) == 10 and D(row['cost'][0]) == D('4.9')
    assert a.equity() == 5000


@pytest.mark.parametrize('change', [dict(side='BUY'), dict(exchange_ms=None), dict(at=NOW + 100), dict(price='.50'),
                                    dict(exchange_ms=NOW + 900), dict(at=NOW + 500000)])
def test_invalid_early_late_or_better_priced_prints_never_fill(change):
    _, m = setup()
    row = step(m)
    pr = print_(**{k: v for k, v in change.items() if k in ('side', 'price', 'at')})
    pr.update({k: v for k, v in change.items() if k == 'exchange_ms'})
    m.prints('42', 0, [pr], NOW + 400)
    assert row['qty'] == ['0', '0']


def test_cancel_takes_effect_after_latency_and_then_releases_capital():
    a, m = setup()
    row = step(m)
    stale = books(NOW + 500)
    stale[0]['exchange_at'] = NOW - 5000
    step(m, at=NOW + 500, bk=stale)
    order = [o for o in row['orders'] if o['kind'] == 'live' and o['side'] == 0][0]
    assert order['cancel_at'] == NOW + 750 and order['cancel_reason'] == 'book_stale'
    m.prints('42', 0, [print_('before', price='.48', size='2', at=NOW + 749)], NOW + 800)
    assert D(row['qty'][0]) == 10  # Worst case: the cancel had not landed yet.
    m.prints('42', 1, [print_('after', price='.48', size='2', at=NOW + 750)], NOW + 800)
    assert D(row['qty'][1]) == 0
    step(m, at=NOW + 2700, bk=stale)
    assert D(a.state['locked']) == D('4.9')  # Down reservation still inside the print lag.
    step(m, at=NOW + 2800, bk=stale)
    assert not any(o['kind'] == 'live' for o in row['orders']) and D(a.state['locked']) == 0


def test_lower_target_replaces_at_once_higher_target_waits_for_persistence_and_rest():
    _, m = setup()
    row = step(m, bk=books(up=book(bid='.47', ask='.53')))
    assert live(row, 0)[0]['price'] == '0.48'  # Improve best bid by at most one tick.
    better = lambda at: books(at, up=book(bid='.48', ask='.52', at=at))
    for at in (NOW + 250, NOW + 500):
        step(m, at=at, bk=better(at))
        assert live(row, 0)[0]['price'] == '0.48'
    step(m, at=NOW + 750, bk=better(NOW + 750))
    assert live(row, 0)[0]['price'] == '0.49'
    # Lower target, small fair move (below the adverse guard): cancel at once despite minimum rest.
    worse = lambda at: books(at, up=book(bid='.48', ask='.52', ask_size='124', at=at))
    step(m, at=NOW + 800, bk=worse(NOW + 800))
    assert m.stats(NOW)['guard_cancels']['protect'] == 1 and m.stats(NOW)['guard_cancels']['improve'] == 1
    # Two Up cancels are still in flight (worst case 20 shares = N_cap), so the replacement waits.
    assert not live(row, 0)
    step(m, at=NOW + 1000, bk=worse(NOW + 1000))
    assert live(row, 0)[0]['price'] == '0.48'


def test_same_price_add_on_keeps_queue_and_resize_on_risk():
    _, m = setup({'fill_size_mult': '1', 'fill_ticks': '0'})
    row = step(m)
    m.prints('42', 0, [print_('a', price='.49', size='106')], NOW + 400)
    assert D(row['qty'][0]) == 6
    m.prints('42', 1, [print_('b', price='.49', size='106')], NOW + 400)
    step(m, at=NOW + 500)
    up = live(row, 0)
    assert [(D(o['remaining']), D(o['ahead'])) for o in up] == [(4, 0), (6, 104)]
    m.p.n_cap_d = D('5')  # Tighter cap: resting size now exceeds the risk room.
    m.p.base_size_d = D('5')
    step(m, at=NOW + 600)
    assert not live(row, 0) and m.stats(NOW)['guard_cancels']['resize'] == 2
    step(m, at=NOW + 850)  # Cancels landed, but six actions in the last second exhaust the budget.
    assert not live(row, 0)
    step(m, at=NOW + 1100)
    assert [o['remaining'] for o in live(row, 0)] == ['5.00']


def test_action_budget_defers_placements_never_protective_cancels():
    _, m = setup()
    row = step(m)
    row['actions'] = [NOW + 500] * 5
    stale = books(NOW + 600)
    stale[1]['exchange_at'] = NOW - 9000
    step(m, at=NOW + 600, bk=stale)
    assert not live(row)
    step(m, at=NOW + 900)
    assert not live(row)  # Five actions within the last second.
    step(m, at=NOW + 1700)
    assert len(live(row)) == 2


def test_fair_value_adverse_move_and_depth_pull_guards():
    _, m = setup()
    row = step(m, bk=books(up=book(bid='.48', ask='.52')))
    assert live(row, 0)[0]['price'] == '0.49'
    step(m, at=NOW + 300, bk=books(NOW + 300, up=book(bid='.48', ask='.52', ask_size='300', at=NOW + 300)))
    assert m.stats(NOW)['guard_cancels']['fv_adverse'] == 1
    _, m = setup()
    row = step(m)
    thin = lambda at: books(at, up=book(bid_size='40', ask_size='40', at=at))  # Same microprice.
    step(m, at=NOW + 1000, bk=thin(NOW + 1000))
    assert m.stats(NOW)['guard_cancels']['depth_pull'] == 1 and not live(row, 0) and live(row, 1)
    step(m, at=NOW + 2900, bk=thin(NOW + 2900))
    assert not live(row, 0)  # Same-side cooldown.
    step(m, at=NOW + 3100, bk=thin(NOW + 3100))
    assert live(row, 0)


def test_inventory_skew_monotone_and_cap_stops_only_risk_side():
    raws = []
    for qty in ('0', '5', '10', '15', '20'):
        _, m = setup()
        row = step(m, spot=None) or m.state['markets']['42']
        row['qty'], row['cost'] = [qty, '0'], [str(D(qty) * D('.49')), '0']
        m.changed(row)
        step(m, at=NOW + 100)
        sides = row['decision']['sides']
        raws.append((D(sides[0]['raw']), D(sides[1]['raw']) if 'raw' in sides[1] else None, sides[0]['size'], sides[1].get('size')))
    ups = [r[0] for r in raws]
    downs = [r[1] for r in raws]
    assert ups == sorted(ups, reverse=True) and downs == sorted(downs)
    assert raws[-1][2] == '0'  # |n| = N_cap: the risk side never quotes.
    assert D(raws[-1][3]) == 20  # Hedge side always allowed.


def test_strike_reduce_hedge_phases_quote_hedge_only_flat_none():
    _, m = setup()
    mk = hourly('78', start=NOW + 4000)
    row = step(m, mk, at=NOW - 60000)
    assert row['phase'] == 'PREOPEN' and {o['side'] for o in live(row)} == {0, 1}
    row['qty'], row['cost'] = ['6', '0'], ['2.94', '0']
    m.changed(row)
    end = NOW + 4000 + 3600000
    for at, ph in ((NOW, 'STRIKE'), (end - 290000, 'REDUCE'), (end - 100000, 'HEDGE'), (end - 20000, 'FLAT')):
        step(m, mk, at=at, bk=books(at))
        assert row['phase'] == ph
        assert {o['side'] for o in live(row)} == (set() if ph == 'FLAT' else {1})
        if ph != 'FLAT':
            assert row['decision']['sides'][0]['reason'] == 'phase_' + ph.lower()
    assert row['flat'] is not None


def test_five_minute_post_open_is_too_toxic_even_to_hedge():
    _, m = setup()
    mk = market()
    warm(m, mk, NOW - 5000, NOW + 125000)
    row = step(m, mk)
    row['qty'], row['cost'] = ['6', '0'], ['2.94', '0']
    m.changed(row)
    step(m, mk, at=NOW + 121000, bk=books(NOW + 121000))  # Freezes the strike-window average.
    assert row['a_start'] == pytest.approx(math.log(60000))
    warm(m, mk, NOW + 296000, NOW + 300001)
    step(m, mk, at=NOW + 300000, bk=books(NOW + 300000))
    assert row['phase'] == 'REDUCE' and row['decision']['Z'] > .02
    assert row['decision']['sides'][1]['reason'] == 'hedge_too_far' and not live(row)


def test_open_phase_hourly_quotes_both_sides_and_hedge_phase_at_fair():
    _, m = setup()
    mk = hourly()
    row = step(m, mk)
    assert row['phase'] == 'OPEN' and row['decision']['Z'] < .01
    assert {o['side'] for o in live(row)} == {0, 1}
    late = NOW - 600000 + 3600000 - 100000
    row['qty'], row['cost'] = ['20', '0'], ['9.8', '0']
    m.changed(row)
    step(m, mk, at=late, bk=books(late, down=book(bid='.47', ask='.53', at=late)))
    assert row['phase'] == 'HEDGE'
    assert [o['side'] for o in live(row)] == [1]
    # Full inventory skew exceeds the toxicity base; e_neg = 0 floors the hedge at fair.
    assert row['decision']['sides'][1]['h'] == 0 and row['decision']['sides'][1]['raw'] == '0.50'


def test_data_faults_calm_every_phase_and_coin_jump_waits_for_new_books():
    _, m = setup()
    row = step(m)
    skew = books(NOW + 100)
    skew[1]['exchange_at'] = NOW - 300
    step(m, at=NOW + 100, bk=skew)
    assert row['phase'] == 'FLAT' and not live(row)
    m.observe_spot('BTC', '60000', NOW + 5000, NOW + 5000)
    m.observe_spot('BTC', '60300', NOW + 6000, NOW + 6000)  # Far beyond 4 sigma in one second.
    step(m, at=NOW + 6100, bk=books(NOW + 5900), spot=None)
    assert row['phase'] == 'FLAT' and not live(row)
    step(m, at=NOW + 9200, bk=books(NOW + 5900), spot=None)
    assert row['phase'] == 'FLAT'  # Cooldown over, but no book newer than the jump yet.
    step(m, at=NOW + 9300, bk=books(NOW + 9300), spot=None)
    assert row['phase'] == 'PREOPEN' and live(row)


def test_own_fill_and_sweep_widen_the_opening_side():
    _, m = setup()
    row = step(m)
    m.prints('42', 0, [print_('fill', price='.48', size='1')], NOW + 400)
    step(m, at=NOW + 500)
    up = row['decision']['sides'][0]
    assert up['widen'] >= .01 and up['reason'] == 'behind_best'
    _, m = setup()
    row = step(m)
    sweep = [print_('s1', price='.49', size='1', at=NOW + 300, tx='0xabc'), print_('s2', price='.48', size='1', at=NOW + 300, tx='0xabc')]
    m.prints('42', 1, sweep, NOW + 400)
    assert row['flow_until'][1] == NOW + 400 + 15000


def test_complementary_prints_verify_mode_and_tx_dedupe_in_count_mode():
    _, m = setup()
    row = step(m)
    m.prints('42', 0, [print_('u', price='.52', side='BUY', tx='0x1')], NOW + 400)
    assert row['qty'][1] == '0'  # While verifying, a BUY on Up never fills our Down bid.
    m.prints('42', 1, [print_('d', price='.48', side='SELL', size='0', tx='0x1')], NOW + 400)
    assert m.stats(NOW)['complementary_checked'] == 1 and m.stats(NOW)['complementary_sum_one'] == 1
    assert row['qty'][1] == '0'  # A zero-size print is no evidence of a sweep.
    _, m = setup({'complementary_prints': 'count'})
    row = step(m)
    m.prints('42', 0, [print_('u', price='.52', side='BUY', tx='0x2')], NOW + 400)
    assert D(row['qty'][1]) == 10  # Buying Up at .52 sweeps our Down .49 bid via minting.
    m.prints('42', 1, [print_('d', price='.48', size='9', tx='0x2')], NOW + 400)
    assert D(row['qty'][1]) == 10 and D(m.stats(NOW)['complementary_fills']) == 10


def test_merge_pays_one_and_settlement_zero_one_or_half_with_exact_decomposition():
    for outcome, payout in (('Up', 1), ('Down', 0), ('Tie', D('.5'))):
        a, m = setup({'fill_ticks': '0', 'fill_size_mult': '1'})
        row = step(m)
        m.prints('42', 0, [print_('a', price='.48', size='1')], NOW + 400)
        m.prints('42', 1, [print_('b', price='.48', size='1', at=NOW + 450)], NOW + 500)
        assert row['qty'] == ['0E-8', '0E-8'] and D(m.stats(NOW)['paired_sets']) == 10
        assert D(a.state['free']) + D(a.state['locked']) == D('5000.2')  # Ten sets at .98 redeem for 1.
        step(m, at=NOW + 600)
        m.prints('42', 0, [print_('c', price='.48', size='1', at=NOW + 900)], NOW + 1000)
        assert D(row['qty'][0]) == 10
        m.settle('42', outcome, NOW + 420000)
        assert a.equity() == D('5000.2') + 10 * payout - D('4.9')
        s = m.stats(NOW)
        assert D(s['net_pnl']) == a.equity() - 5000 == D(s['decomposed_pnl'])
        assert D(s['decomposed_pnl']) == D(s['fill_edge']) + D(s['as_60']) + D(s['residual'])
        assert D(a.state['locked']) == D(a.state['positions']) == 0


def test_unknown_outcome_waits_with_inventory_and_flat_accounting():
    a, m = setup()
    row = step(m)
    m.prints('42', 0, [print_('a', price='.48', size='1')], NOW + 400)
    for at in (NOW + 405000, NOW + 410000):
        warm(m, market(), at - 300000, at + 1)
        step(m, at=at)
    assert row['flat'] and row['flat']['qty'] == ['10.00000000', '0']
    m.settle('42', None, NOW + 9000000)
    assert not row['settled']
    m.settle('42', 'Down', NOW + 9000000)
    s = m.stats(NOW + 9000000)
    assert D(s['flat_settlement_pnl']) == 10 * (0 - D(repr(row['flat']['fair'][0])))
    assert a.equity() == D('4995.1')


def test_restart_leaves_resting_orders_fillable_until_the_cancel_lands():
    a, m = setup()
    row = step(m)
    restored = Maker(Account(state=copy.deepcopy(a.state)), {})
    restored.restart(NOW + 10000)
    r = restored.state['markets']['42']
    assert all(o['cancel_at'] == NOW + 10250 for o in r['orders'])
    restored.prints('42', 0, [print_('downtime', price='.48', size='1', at=NOW + 9000)], NOW + 10100)
    assert D(r['qty'][0]) == 10
    m.shutdown(NOW + 500)
    assert all(o['cancel_at'] == NOW + 750 for o in row['orders'])


def test_same_events_replay_to_identical_orders_and_fills():
    def run():
        a, m = setup()
        rng = random.Random(5)
        mk = hourly()
        for k in range(120):
            at = NOW + k * 250
            bid = D(rng.choice(('.47', '.48', '.49')))
            bk = books(at, up=book(bid=str(bid), ask=str(bid + D('.03')), bid_size=str(rng.randrange(20, 200)), at=at),
                       down=book(bid=str(D('.97') - bid - D('.03') + D('.03')), ask=str(D('1') - bid), at=at))
            step(m, mk, at=at, bk=bk, spot=str(60000 + rng.randrange(-30, 30)))
            m.prints('77', k % 2, [print_(f'p{k}', price=str(bid - D('.01') * rng.randrange(0, 2)),
                                          size=str(rng.randrange(1, 80)), at=at - 50)], at)
        row = m.state['markets']['77']
        strip = lambda o: {k: v for k, v in o.items()}
        return [strip(o) for o in row['orders']], row['fills'], row['qty'], m.stats(NOW)
    first, second = run(), run()
    assert first == second
    assert first[1]  # The stream actually produced fills.


def test_legacy_v2_state_upgrade_keeps_inventory_and_releases_reservations():
    legacy = market()
    a = Account()
    a.transfer('free', '-3')
    a.transfer('locked', '3')
    a.transfer('free', '-1.47')
    a.transfer('positions', '1.47')
    a.state['maker'] = dict(strategy='maker_complete_set_v1', markets={'42': dict(market=legacy, qty=['3', '0'], cost=['1.47', '0'],
                            quotes=[dict(price='.49', remaining='3')], reserved='3', marks=[], settled=False)},
                            days={day_key(NOW): dict(strategy='maker_complete_set_v1', net_pnl='-400', quotes=4)},
                            history={'BTC:5m': dict(filled='3', paired='0', adverse='0')})
    m = Maker(a, {})
    row = m.state['markets']['42']
    assert m.state['version'] == 3 and m.state['strategy'] == TAG and 'legacy_history' in m.state
    assert row['qty'] == ['3', '0'] and row['orders'] == [] and row['lots'][0] == [[0, '3']]
    assert D(a.state['locked']) == 0 and a.equity() == 5000
    assert m.report(NOW, current=False) is None  # The v2 day keeps its own report.
    today = m.stats(NOW)
    assert today['strategy'] == TAG and today['legacy']['net_pnl'] == '-400' and today['net_pnl'] == '0'


def test_rebate_scenarios_never_enter_cash_or_pnl():
    a, m = setup()
    step(m)
    m.prints('42', 0, [print_('a', price='.48', size='1')], NOW + 400)
    report = m.report(NOW)
    expected = rebate(10, '.49')
    assert D(report['rebate_scenarios']['100%']) == expected and D(report['rebate_scenarios']['50%']) == expected / 2
    assert report['estimated_rebate'] is None and report['rebate_status'] == 'unknown_pool_not_credited'
    assert report['net_pnl_including_estimated_rebate'] == report['net_pnl'] == '0'
    assert a.equity() == 5000


def test_disabled_quoting_cancels_but_keeps_inventory_for_settlement():
    a, m = setup()
    row = step(m)
    m.prints('42', 0, [print_('a', price='.48', size='1')], NOW + 400)
    m.step(market(), books(NOW + 500), NOW + 500, quoting=False)
    assert not live(row) and not [o for o in row['orders'] if o['kind'] == 'shadow' and o['cancel_at'] is None]
    m.settle('42', 'Up', NOW + 420000)
    assert a.equity() == D('5005.1')


def test_size_bounded_by_trade_cap_and_free_capital():
    for cap in ('1000', '3'):
        a = Account({'trade_cap': cap, 'min_ticket': '1'})
        _, m = setup({'base_size': '25', 'max_order': '25'}, account=a)
        row = step(m)
        sizes = [D(o['size']) for o in live(row)]
        assert all(s * D('.49') <= D(cap) for s in sizes) and sizes
        assert D(a.state['free']) + D(a.state['locked']) == 5000


def test_microprice_anchor_and_gap_switches_to_reduce():
    _, m = setup()
    row = step(m, bk=books(up=book(bid='.53', ask='.56')))
    assert row['phase'] == 'REDUCE' and row['decision']['phase'] == 'REDUCE'
    assert not live(row)  # Flat inventory: nothing to hedge.
    row = step(m, at=NOW + 300, bk=books(NOW + 300, up=book(bid='.51', ask='.53', at=NOW + 300)))
    assert row['phase'] == 'PREOPEN'
    assert math.isclose(row['decision']['M'], .52)


class SettlementCursor:
    """Answers RecordedFeed.outcome queries from fixed evidence; records what was asked."""
    def __init__(self, official=None, prices=()):
        self.official, self.prices, self.queries, self.result = official, list(prices), [], None

    def execute(self, sql, params):
        self.queries.append((sql, params))
        if 'qd_polymarket_observer_windows' in sql:
            self.result = dict(official_outcome=self.official)
        else:
            price = self.prices.pop(0) if self.prices else None
            self.result = dict(reference_price=price) if price is not None else None

    def fetchone(self):
        return self.result


def test_recorded_outcome_reads_hourly_candle_boundaries_from_the_market_samples():
    from app.services.polymarket import maker_model as mm
    from app.services.polymarket.maker import RecordedFeed
    mk = hourly()
    row = dict(market=mk, spec=mm.parse_rules(mk))
    end = row['spec']['t1'] * 1000
    cur = SettlementCursor(prices=['60000', '60000'])
    assert RecordedFeed.outcome(cur, row, end) == ('Up', True)  # 1h rules: close >= open is Up.
    samples = cur.queries[1:]
    assert [params for _, params in samples] == [('77', b, b) for b in (mk['eventStartTime'], mk['endDate'])]
    assert all('market_id=%s' in sql for sql, _ in samples)
    assert RecordedFeed.outcome(SettlementCursor(official='Down', prices=['1', '2']), row, end) == ('Down', False)
    assert RecordedFeed.outcome(SettlementCursor(prices=['60000']), row, end) == (None, False)


def test_recorded_outcome_waits_for_daily_one_minute_candle_closes():
    from app.services.polymarket import maker_model as mm
    from app.services.polymarket.maker import RecordedFeed
    rule = ('This market will resolve to "Up" if the "Close" price for the Binance 1 minute candle for BTC/USDT '
            '12:00 in the ET timezone (noon) is lower than the final "Close" price for the next 12:00 ET candle. '
            'If the final "Close" price for both of these candles is exactly equal on Binance, this market will resolve 50-50.')
    mk = market('88', start=NOW - 86400000, duration=86400000, interval='1d', source=BINANCE, description=rule)
    row = dict(market=mk, spec=mm.parse_rules(mk))
    end = row['spec']['t1'] * 1000
    assert row['spec']['tie'] == .5
    for now in (end, end + 59999):
        cur = SettlementCursor(prices=['60000', '60100'])
        assert RecordedFeed.outcome(cur, row, now) == (None, False)
        assert len(cur.queries) == 1  # Official result only; the closing print is not out yet.
    cur = SettlementCursor(prices=['60000', '60100'])
    assert RecordedFeed.outcome(cur, row, end + 60000) == ('Up', True)
    prints = [utc(NOW - 86400000 + 60000), utc(NOW + 60000)]
    assert [params for _, params in cur.queries[1:]] == [('BTC', at, at, at, at) for at in prints]
    assert all('asset=%s' in sql and 'market_id' not in sql for sql, _ in cur.queries[1:])
    assert RecordedFeed.outcome(SettlementCursor(prices=['60000', '60000']), row, end + 60000) == ('Tie', True)
    assert RecordedFeed.outcome(SettlementCursor(prices=['60000', '59999']), row, end + 60000) == ('Down', True)
    assert RecordedFeed.outcome(SettlementCursor(official='Up'), row, end) == ('Up', False)
