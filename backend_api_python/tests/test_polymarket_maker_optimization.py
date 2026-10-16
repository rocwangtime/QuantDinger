"""v3 paper-only risk layers, market selection and shadow ladder."""
from decimal import Decimal as D

import pytest

from app.services.polymarket.maker import DEFAULTS, Maker, TAG, configuration, params, select
from app.services.polymarket.paper_account import Account, configuration as account_configuration, day_key, utc
from tests.test_polymarket_maker import NOW, book, books, live, market, print_, setup, step, warm

P = params(dict(DEFAULTS))


def test_taker_default_and_env_config(monkeypatch):
    monkeypatch.delenv('POLYMARKET_PAPER_ACCOUNT_TAKER_ENABLED', raising=False)
    a = Account(account_configuration())
    # Gate precedes book access and creates no attempt/event/pending work.
    assert a.signal({}, [], NOW, {}) is None
    assert not a.state['pending']
    assert a.daily(NOW)['attempted'] == 0
    monkeypatch.setenv('POLYMARKET_PAPER_ACCOUNT_TAKER_ENABLED', 'true')
    assert account_configuration()['taker_enabled'] is True


def test_disabled_taker_creates_no_new_attempt_for_executable_opportunity():
    from tests.test_polymarket_paper_account import market as taker_market, books as taker_books
    active = Account({'taker_enabled': True})
    assert active.signal(taker_market(), taker_books(), NOW, {'one_way_ms': 100}) is not None
    assert active.daily(NOW)['attempted'] == 1
    disabled = Account({'taker_enabled': False})
    for at in (NOW, NOW + 1000):
        assert disabled.signal(taker_market(), taker_books(), at, {'one_way_ms': 100}) is None
    assert disabled.daily(NOW)['attempted'] == 0
    assert not disabled.changed
    assert not disabled.state['pending']
    assert not disabled.state['events']


@pytest.mark.parametrize('key,value', [('depth_pull_frac', '2'), ('min_markets', '1.5'), ('fair_min', '.95'),
    ('phases', '5m:10:20:5,15m:180:90:30,1h:300:120:30,4h:600:180:60,1d:900:300:60'), ('cold_start', 'DOGE:1m'),
    ('universe_assets', 'BTC'), ('complementary_prints', 'maybe'), ('review_utc', '25:00'), ('event_times', 'soon'),
    ('h_min', '0'), ('bootstrap_iters', '10'), ('lcb_alpha', '1'), ('preopen_windows', '5m:120')])
def test_invalid_config(monkeypatch, key, value):
    monkeypatch.setenv('POLYMARKET_PAPER_MAKER_' + key.upper(), value)
    with pytest.raises(ValueError, match='configuration'):
        configuration()


def test_defaults_follow_the_v3_parameter_table():
    c = configuration()
    assert (c['n_cap'], c['n_cap_coin_dir'], c['n_cap_global_dir']) == ('20', '60', '100')
    assert (c['k_tox'], c['lam_fee'], c['latency_eff_s'], c['gap_max'], c['z_hard_max']) == ('3', '1', '0.30', '0.04', '0.03')
    assert (c['place_latency_ms'], c['cancel_latency_ms'], c['complementary_prints']) == ('250', '250', 'verify')


def test_directional_caps_reserve_worst_case_fills_across_markets():
    _, m = setup({'n_cap_coin_dir': '15', 'n_cap_global_dir': '18'})
    step(m)
    second = step(m, market('43'), at=NOW)
    assert [o['size'] for o in live(second)] == ['5.00', '5.00']  # Coin room 15 - 10 per direction.
    eth = step(m, market('44', asset='ETH'), at=NOW)
    assert not live(eth)  # Global room 18 - 15 = 3 < minimum size.
    assert eth['decision']['sides'][0]['reason'] == 'size'


def test_worst_loss_and_locked_capital_caps():
    _, m = setup({'worst_loss_market': '3'})
    row = step(m)
    assert [o['size'] for o in live(row)] == ['6.12', '6.12']
    _, m = setup({'lock_market': '6'})
    row = step(m)
    assert [(o['side'], o['size']) for o in live(row)] == [(0, '10.00')]
    _, m = setup({'lock_global': '5'})
    row = step(m)
    assert len(live(row)) == 1 and D(m.account.state['locked']) == D('4.9')


def test_hedge_leg_ignores_caps_that_block_the_risk_side():
    _, m = setup({'lock_market': '1', 'worst_loss_market': '1'})
    row = step(m)
    assert not live(row)
    row['qty'], row['cost'] = ['8', '0'], ['3.92', '0']
    m.changed(row)
    step(m, at=NOW + 100)
    assert [(o['side'], o['size']) for o in live(row)] == [(1, '8.00')]


def test_soft_stop_halves_size_and_widens_hard_stop_hedges_only():
    a, m = setup()
    m.stats(NOW)['net_pnl'] = '-60'
    row = step(m, bk=books(up=book(bid='.47', ask='.53')))
    assert m.risk['soft'] and not m.risk['hard']
    assert [(o['side'], o['price'], o['size']) for o in live(row)] == [(0, '0.48', '5.00')]
    assert row['decision']['sides'][1]['reason'] == 'behind_best'  # +1 tick moves Down behind .49.
    a, m = setup()
    a.daily(NOW)['loss_stop'] = True  # Account-wide 2% stop counts as the hard stop.
    row = step(m)
    assert not live(row) and row['decision']['gates'] == ['hard_stop']
    row['qty'], row['cost'] = ['6', '0'], ['2.94', '0']
    m.changed(row)
    step(m, at=NOW + 100)
    assert [o['side'] for o in live(row)] == [1]


def test_weekly_stop_latches_until_manual_review():
    a, m = setup()
    m.state['days'][day_key(NOW - 2 * 86400000)] = dict(m.stats(NOW - 2 * 86400000), net_pnl='-170')
    m.stats(NOW)['net_pnl'] = '-35'
    row = step(m)
    assert m.state['halt']['reason'] == 'weekly_stop' and 'weekly_stop' in row['decision']['gates']
    m.stats(NOW)['net_pnl'] = '0'
    later = step(m, market('45', start=NOW + 86400000 + 120000), at=NOW + 86400000)
    assert m.state['halt'] and not live(later)
    assert any(e['event'] == 'halt' for e in a.maker_changed)


def test_cell_daily_stop_blocks_the_cell_and_review_exits_with_cooloff():
    _, m = setup()
    m.cell_stats(NOW, 'BTC:5m')['pnl'] = '-30'
    row = step(m)
    assert 'cell_stop' in row['decision']['gates'] and not live(row)
    assert m.state['selection']['breakers'] == {'BTC:5m': day_key(NOW)}
    eth = step(m, market('46', asset='ETH'))
    assert live(eth)  # Other cells keep quoting.
    m.review([], NOW + 86400000)
    selection = m.state['selection']
    assert 'BTC:5m' not in selection['active'] and selection['cooloff']['BTC:5m'] == day_key(NOW + 4 * 86400000)


def test_as60_anomaly_stops_the_cell_for_the_day():
    _, m = setup()
    for back in (1, 2, 3, 4):
        m.stats(NOW - back * 86400000)['cells']['BTC:5m'] = dict(pnl='0', filled='50', as60=str(-.5 - back * .01),
                                                              as60_shares='50', stopped=None)
    m.cell_stats(NOW, 'BTC:5m').update(as60='-5', as60_shares='50')
    row = step(m)
    assert m.cell_stats(NOW, 'BTC:5m')['stopped'] == 'as60_anomaly' and 'cell_stop' in row['decision']['gates']


def test_event_blackout_blocks_new_openings():
    _, m = setup({'event_times': utc(NOW + 300000).isoformat()})
    row = step(m)
    assert row['decision']['gates'] == ['event_blackout'] and not live(row)


def test_cold_start_priors_respect_coin_and_total_limits():
    s = select([], {}, P, NOW)
    assert s['active'] == {c: dict(source='cold_start', since=day_key(NOW), mode=mode) for c, mode in P.cold_start.items()}
    assert s['active']['BTC:5m']['mode'] == 'preopen'
    crowded = select([], {}, params(dict(DEFAULTS, cold_start='BTC:5m,BTC:15m,BTC:1h,BTC:4h,ETH:1h')), NOW)
    assert sorted(crowded['active']) == ['BTC:15m', 'BTC:1h', 'BTC:5m', 'ETH:1h']
    fresh = Maker(Account(), {})
    opened = market('47', start=NOW - 10000)
    warm(fresh, opened, NOW - 75000, NOW)
    row = step(fresh)  # BTC:5m is pre-open only under the priors, at probation size.
    assert [o['size'] for o in live(row)] == ['5.00', '5.00']
    row = step(fresh, opened)
    assert row['phase'] == 'OPEN' and row['decision']['gates'] == ['preopen_only'] and not live(row)


def summaries(cell, days, per_day, pnl, edge=.05, as60=-.01, volume=5000, quotable=60, stale=0, start=0):
    rows = []
    for day in range(1, days + 1):
        for k in range(per_day):
            end = NOW - day * 86400000 + k * 300000 + start
            rows.append(dict(strategy=TAG, cell=cell, end=end, outcome='Up', polls=100, stale=stale, tight=80, open=100,
                             quotable=quotable, taker_volume=str(volume), shadow=dict(eligible=dict(
                                 shares='10', pnl=str(pnl(day, k)), edge=str(edge), as60=str(as60)))))
    return rows


def test_review_admits_on_positive_lcb_with_probation_ranked_with_coin_limit():
    good = lambda day, k: .3 + (k % 5) * .1
    rows = (summaries('BTC:1h', 7, 30, good) + summaries('ETH:1h', 7, 30, lambda d, k: .1 + (k % 9) * .2)
            + summaries('SOL:1h', 7, 30, lambda d, k: -.2) + summaries('XRP:4h', 7, 30, good, quotable=5)
            + summaries('ETH:4h', 7, 30, good, as60=-.04))
    s = select(rows, {}, P, NOW)
    assert s['scores']['BTC:1h']['admissible'] and s['scores']['BTC:1h']['lcb'] > 0
    assert not s['scores']['SOL:1h']['admissible']
    assert not s['scores']['XRP:4h']['admissible'] and not s['scores']['XRP:4h']['filters']  # Quotable share 5%.
    assert not s['scores']['ETH:4h']['admissible']  # AS60 eats 80% of the edge.
    btc = s['active']['BTC:1h']
    assert btc['source'] == 'admitted' and btc['probation_until'] == day_key(NOW + 3 * 86400000)
    assert len(s['active']) <= P.max_active
    for coin in ('BTC', 'ETH'):
        assert sum(c.startswith(coin) for c in s['active']) <= 3
    # A cold-start cell whose evidence is complete but negative leaves and cools off.
    prior = dict(active={'SOL:1h': dict(source='cold_start', since='', mode='full')})
    again = select(rows, prior, P, NOW)
    assert 'SOL:1h' not in again['active'] and again['cooloff']['SOL:1h'] == day_key(NOW + 3 * 86400000)


def test_review_exit_rules_two_negative_lcbs_or_recent_losses():
    bad = summaries('BTC:1h', 7, 30, lambda d, k: -.5 + (k % 3) * .1)
    previous = dict(active={'BTC:1h': dict(source='admitted', since='', mode='full')}, lcb_history={'BTC:1h': [-.1]})
    s = select(bad, previous, P, NOW)
    assert 'BTC:1h' not in s['active'] and 'BTC:1h' in s['cooloff']
    once = select(bad, dict(previous, lcb_history={}), P, NOW)
    assert 'BTC:1h' not in once['active']  # 3-day mean < 0 with >= 50 recent markets.
    few = summaries('BTC:1h', 7, 5, lambda d, k: -.5)
    kept = select(few, dict(previous, lcb_history={}), P, NOW)
    assert 'BTC:1h' in kept['active']  # One negative LCB and too few recent markets: keep.


def test_review_runs_daily_after_0005_utc():
    _, m = setup()
    midnight = int(utc(NOW).replace(hour=0, minute=0, second=0, microsecond=0).timestamp() * 1000) + 86400000
    fresh = Maker(Account(), {})
    fresh.begin(midnight)
    assert fresh.state['selection']['reviewed'] is None and fresh.state['selection']['active']
    assert not fresh.review_due(midnight + 4 * 60000)
    assert fresh.review_due(midnight + 6 * 60000)
    fresh.review([], midnight + 6 * 60000)
    assert not fresh.review_due(midnight + 7 * 60000)
    assert fresh.review_due(midnight + 86400000 + 6 * 60000)


def test_shadow_ladder_independent_queues_and_market_summary():
    a, m = setup()
    row = step(m)
    ladder = sorted((o['side'], o['level'], o['price']) for o in row['orders'] if o['kind'] == 'shadow')
    assert ladder == [(s, lvl, px) for s in (0, 1) for lvl, px in ((-1, '0.48'), (0, '0.49'), (1, '0.50'))]
    m.prints('42', 0, [print_('q', price='.49', size='105')], NOW + 400)
    shadow = {r['level']: D(r['qty']) for r in row['shadow_fills']}
    assert shadow == {0: 5, 1: 10}  # Level 0 waited behind 100 shares; +1 improved; -1 unknown depth.
    assert D(row['qty'][0]) == 5 and D(a.state['locked']) < 10  # Live order shares the print independently.
    m.settle('42', 'Up', NOW + 420000)
    summary = [e for e in a.maker_changed if e['event'] == 'market_summary'][0]['detail']
    assert summary['shadow']['0']['shares'] == '5.00000000' and summary['shadow']['eligible']['shares'] == '5.00000000'
    assert D(summary['shadow']['1']['pnl']) == 10 * (1 - D('.50'))
    assert summary['polls'] == 1 and summary['cell'] == 'BTC:5m'
    assert m.stats(NOW)['shadow']['0']['markets'] == 1
    assert any(e['event'] == 'shadow_fills' for e in a.maker_changed)


def test_report_metrics_and_sources():
    a, m = setup({'fill_ticks': '0'})
    step(m)
    m.prints('42', 0, [print_('a', price='.48', size='1')], NOW + 400)
    m.prints('42', 1, [print_('b', price='.48', size='1', at=NOW + 2400)], NOW + 2500)
    m.settle('42', 'Up', NOW + 420000)
    r = m.report(NOW)
    assert r['strategy'] == TAG and r['config_hash'] and r['selection']['active']
    assert r['inventory_half_life_s'] == pytest.approx(2.1) and r['neutralization_rate'] == '1'
    assert r['net_pnl_per_market'] == r['net_pnl'] == '0.2000000000'
    assert r['fee_source'].startswith('https://docs.polymarket.com') and r['rebate_source'].endswith('maker-rebates')
    assert r['markets_with_fills'] == 1 and 'fills_per_quote_hour_improve' in r
    assert m.report(NOW - 86400000, current=False)['quotes'] == 0
    assert a.equity() == D('5000.2')
