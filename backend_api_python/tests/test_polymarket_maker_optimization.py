"""Paper-only risk controls and accounting policy regression cases."""
from decimal import Decimal as D

import pytest

from app.services.polymarket.maker import Maker, configuration
from app.services.polymarket.paper_account import Account, configuration as account_configuration
from tests.test_polymarket_maker import NOW, books, market, print_


def maker(config=None):
    return Maker(Account(), config or {})


def test_taker_default_and_env_config(monkeypatch):
    monkeypatch.delenv('POLYMARKET_PAPER_ACCOUNT_TAKER_ENABLED', raising=False)
    a = Account(account_configuration())
    # Gate precedes book access and creates no attempt/event/pending work.
    assert a.signal({}, [], NOW, {}) is None
    assert not a.state['pending']
    assert a.daily(NOW)['attempted'] == 0
    monkeypatch.setenv('POLYMARKET_PAPER_ACCOUNT_TAKER_ENABLED', 'true')
    assert account_configuration()['taker_enabled'] is True


def test_close_window_cancels_existing_and_only_pairs():
    m = maker()
    m.quote(market(), books(), 60000, NOW)
    assert m.trade('42', 0, print_(size='3', price='.01'), NOW + 100) == 3
    at = NOW + 231000
    # Old resting leg cannot open exposure after entering the window.
    assert m.trade('42', 0, print_('late', at=at), at) == 0
    m.quote(market(), books(at), 60000, at)
    q = m.state['markets']['42']['quotes']
    assert D(q[0]['remaining']) == 0 and D(q[1]['remaining']) == 3
    assert m.trade('42', 1, print_('pair', at=at + 100, price='.01', size='3'), at + 100) == 3
    m.quote(market(), books(at + 200), 60000, at + 200)
    assert not m.state['markets']['42']['quotes']


@pytest.mark.parametrize('kind', ['jump', 'thin', 'drop'])
def test_adverse_market_cancels_and_cooldown(kind):
    m = maker()
    m.quote(market(), books(), 60000, NOW)
    b = books(NOW + 100)
    if kind == 'jump':
        b[0]['bids'][0]['price'] = '.30'
    else:
        b[0]['bids'][0]['size'] = '.5' if kind == 'thin' else '0.99'
    m.quote(market(), b, 60000, NOW + 100)
    assert not m.state['markets']['42']['quotes']
    m.quote(market(), books(NOW + 200), 60000, NOW + 200)
    assert not m.state['markets']['42']['quotes']
    assert D(m.account.state['locked']) == 0


def test_dynamic_target_time_volatility_and_inventory_skew():
    m = maker({'jump_bps': '100000'})
    m.quote(market(), books(), 60000, NOW)
    r = m.state['markets']['42']
    initial = D(r['target'])
    m.quote(market(), books(NOW + 150000), 60100, NOW + 150000)
    assert D(r['target']) < initial < 1
    assert sum(D(q['price']) for q in r['quotes']) <= D(r['target'])
    # Isolate inventory skew with a large configurable coefficient.
    m.config['inventory_skew'] = '.2'
    r['qty'] = ['10', '0']
    m.quote(market(), books(NOW + 151000), 60100, NOW + 151000)
    assert D(r['quotes'][0]['price']) < D(r['quotes'][1]['price'])
    assert D(r['quotes'][0]['remaining']) == 0


def test_flow_suppresses_eaten_side_and_expires():
    m = maker({'flow_shares': '2', 'time_discount': '0', 'volatility_discount': '0'})
    m.quote(market(), books(), 60000, NOW)
    m.trade('42', 0, print_(size='3', price='.01'), NOW + 100)
    m.trade('42', 1, print_('pair', size='3', price='.01'), NOW + 100)
    m.quote(market(), books(NOW + 200), 60000, NOW + 200)
    assert not m.state['markets']['42']['quotes']
    m.quote(market(), books(NOW + 31000), 60000, NOW + 31000)
    assert m.state['markets']['42']['quotes']


def test_persisted_history_selection_and_pair_recovery():
    m = maker()
    h = m.cohort(market())
    h.update(filled='100', paired='10', adverse='0')
    m.quote(market(), books(), 60000, NOW)
    assert not m.state['markets']['42']['quotes']
    h.update(paired='50', adverse='3')
    assert not m.selected(market())
    h['adverse'] = '1'
    assert m.selected(market())
    restored = Maker(Account(state=m.account.state))
    assert restored.selected(market())
    restored.config['whitelist'] = 'ETH:5m'
    assert not restored.selected(market())
    restored.state['markets']['42']['qty'] = ['2', '0']
    restored.quote(market(), books(), 60000, NOW)
    assert D(restored.state['markets']['42']['quotes'][1]['remaining']) == 2


def test_rebate_scenario_excluded_from_returns():
    m = maker()
    m.quote(market(), books(), 60000, NOW)
    m.trade('42', 0, print_(price='.01'), NOW + 100)
    report = m.report(NOW)
    assert D(report['rebate_scenario_only']) > 0
    assert report['estimated_rebate'] is None
    assert report['net_pnl_including_estimated_rebate'] == report['net_pnl']
    assert report['rebate_status'] == 'unknown_pool_not_credited'


@pytest.mark.parametrize('key,value', [('depth_drop', '2'), ('min_pair_rate', '-1'),
    ('min_combined_bid', '1'), ('flow_shares', '0'), ('whitelist', 'DOGE:1m')])
def test_invalid_config(monkeypatch, key, value):
    monkeypatch.setenv('POLYMARKET_PAPER_MAKER_' + key.upper(), value)
    with pytest.raises(ValueError):
        configuration()


def test_spot_jump_cancels_and_unchanged_tick_budget_keeps_queue():
    m = maker()
    m.quote(market(), books(), 60000, NOW)
    row = m.state['markets']['42']
    m.quote(market(), books(NOW + 1000), 60000, NOW + 1000)
    assert row['joined'] == NOW
    m.quote(market(), books(NOW + 1100), 61000, NOW + 1100)
    assert not row['quotes']


def test_volatility_widens_quote_budget_and_default_caps():
    m = maker({'time_discount': '0', 'spot_jump_bps': '100000'})
    assert m.config['market_cap'] == '20' and m.config['global_cap'] == '100'
    m.quote(market(), books(), 60000, NOW)
    before = D(m.state['markets']['42']['target'])
    m.quote(market(), books(NOW + 100), 60600, NOW + 100)
    assert D(m.state['markets']['42']['target']) < before


def test_pairing_boundary_existing_balanced_quotes_cannot_open_exposure():
    m = maker()
    m.quote(market(), books(), 60000, NOW)
    boundary = NOW + 230000
    assert m.trade('42', 0, print_('boundary', at=boundary, price='.01'), boundary) == 0
    assert m.state['markets']['42']['qty'] == ['0', '0']
