"""Deterministic maker rules; book changes cannot create fills."""
from decimal import Decimal

import pytest

from app.services.polymarket.maker import Maker, DEFAULTS, rebate, enabled
from app.services.polymarket.paper_account import Account

NOW = 1780000000000


def market(identity='42'):
    from app.services.polymarket.paper_account import utc
    return dict(id=identity, market_type='up_down', asset='BTC', interval='5m',
                yesAssetId=identity + 'u', noAssetId=identity + 'd', tickSize='.01',
                eventStartTime=utc(NOW - 10000).isoformat(), endDate=utc(NOW + 290000).isoformat())


def books(now=NOW):
    return [dict(bids=[dict(price='.50', size='4')], asks=[dict(price='.51', size='5')], exchange_at=now) for _ in range(2)]


def setup(config=None):
    a = Account()
    m = Maker(a, {**DEFAULTS, **(config or {})})
    m.quote(market(), books(), '60000', NOW)
    return a, m


def print_(identity='p', price='.48', size='3', at=NOW + 100, side='SELL'):
    return dict(recording_id=identity, price=price, size=size, received_ms=at, exchange_ms=at, side=side)


def test_quotes_tick_combined_budget_and_default_gate(monkeypatch):
    monkeypatch.delenv('POLYMARKET_PAPER_MAKER_ENABLED', raising=False)
    assert not enabled()
    a, m = setup()
    row = m.state['markets']['42']
    assert sum(Decimal(q['price']) for q in row['quotes']) <= Decimal('.99')
    assert Decimal(a.state['locked']) == Decimal('9.90')
    assert a.equity() == 5000
    a2 = Account(state={**a.state, 'maker': dict(strategy='x', markets={}, days={}), 'free': '1', 'locked': '0'})
    m2 = Maker(a2)
    m2.quote(market(), books(), 60000, NOW)
    assert Decimal(a2.state['locked']) <= 1


def test_strict_through_partial_size_no_book_fills_and_idempotence():
    a, m = setup()
    m.quote(market(), books(), 60000, NOW + 1)
    assert m.state['markets']['42']['qty'] == ['0', '0']
    assert m.trade('42', 0, print_(), NOW + 100) == 3
    assert m.trade('42', 0, print_(), NOW + 100) == 0
    assert m.trade('42', 0, print_('next', size='100'), NOW + 100) == 0
    assert Decimal(a.state['positions']) == Decimal('1.47')
    assert a.equity() == 5000


def test_at_price_queue_and_partial_fills():
    _, m = setup()
    # Down quote at .50 joins behind four recorded shares.
    assert m.trade('42', 1, print_('a', price='.50', size='3'), NOW + 100) == 0
    assert m.trade('42', 1, print_('b', price='.50', size='3'), NOW + 100) == 2
    assert m.state['markets']['42']['quotes'][1]['ahead'] == '0'
    assert m.trade('42', 1, print_('c', price='.50', size='2'), NOW + 100) == 0


@pytest.mark.parametrize('change', [dict(side='BUY'), dict(size=None), dict(price='.51'), dict(exchange_ms=None), dict(exchange_ms=NOW - 1), dict(exchange_ms=NOW + 1000), dict(received_ms=NOW - 1)])
def test_invalid_or_prejoin_print_never_fills(change):
    _, m = setup()
    assert m.trade('42', 0, {**print_(), **change}, NOW + 100) == 0


def test_caps_include_worst_case_outstanding_quotes():
    _, m = setup(dict(market_cap='3', global_cap='4'))
    assert Decimal(m.state['markets']['42']['quotes'][0]['remaining']) == 3
    m.quote(market('43'), books(), 60000, NOW)
    assert Decimal(m.state['markets']['43']['quotes'][0]['remaining']) == 1
    m.trade('42', 0, print_(size='100'), NOW + 100)
    assert m.exposure() == 3
    m.quote(market('44'), books(), 60000, NOW)
    assert not m.state['markets']['44']['quotes']


def test_spot_book_cancel_expiry_and_stale():
    a, m = setup()
    m.quote(market(), books(NOW + 1000), 60100, NOW + 1000)
    row = m.state['markets']['42']
    assert row['joined'] == NOW + 1000
    assert m.stats(NOW)['cancels'] == 1
    assert m.trade('42', 0, print_(), NOW + 1000) == 0
    moved = books(NOW + 1100)
    moved[0]['bids'][0]['price'] = '.48'
    m.quote(market(), moved, 60100, NOW + 1100)
    assert m.stats(NOW)['cancels'] == 2
    m.quote(market(), books(), 60100, NOW + 3000)
    assert not row['quotes'] and Decimal(a.state['locked']) == 0
    m.quote(market(), books(NOW + 280000), 60100, NOW + 280000)
    assert not row['quotes']


def test_pair_pnl_rebate_mark_and_residual_settlement():
    a, m = setup()
    m.trade('42', 0, print_(size='3'), NOW + 100)
    m.trade('42', 1, print_('d', size='2'), NOW + 100)
    assert m.stats(NOW)['paired_sets'] == '2'
    assert Decimal(m.report(NOW)['net_pnl_per_completed_set']) == Decimal('.01')
    m.mark('42', 0, '.40', NOW + 5100, NOW + 5100)
    assert Decimal(m.stats(NOW)['adverse_selection_loss']) == Decimal('.27')
    assert rebate(3, '.49') == Decimal('3') * Decimal('.07') * Decimal('.49') * Decimal('.51') * Decimal('.2')
    cash = Decimal(a.state['free'])
    m.settle('42', 'Down', NOW + 300000, estimated=True)
    assert Decimal(a.state['free']) >= cash
    assert Decimal(a.state['locked']) == Decimal(a.state['positions']) == 0
    assert a.equity() == Decimal('4999.53')
    assert Decimal(m.stats(NOW)['net_pnl']) == Decimal('-.47')
    assert m.stats(NOW)['estimated_settlements'] == 1
    m.settle('42', 'Up', NOW + 300001)
    assert a.equity() == Decimal('4999.53')


def test_official_winning_residual_and_unknown_settlement():
    a, m = setup()
    m.trade('42', 0, print_(size='1'), NOW + 100)
    m.settle('42', None, NOW + 300000)
    assert not m.state['markets']['42']['settled']
    m.settle('42', 'Up', NOW + 300000)
    assert a.equity() == Decimal('5000.51')
    assert m.stats(NOW)['estimated_settlements'] == 0


def test_unknown_depth_queue_and_expiry_cutoff():
    _, m = setup()
    # .49 lies below all recorded levels, so equal-price prints cannot prove priority.
    assert m.trade('42', 0, print_('equal', price='.49', size='100'), NOW + 100) == 0
    assert m.trade('42', 0, print_('through', price='.48', size='2'), NOW + 100) == 2
    cutoff = NOW + 260000
    assert m.trade('42', 0, print_('late', at=cutoff), cutoff) == 0


def test_hard_trade_cap_and_account_cap():
    for cap in ('1000', '200'):
        a = Account({'trade_cap': cap})
        m = Maker(a, {**DEFAULTS, 'quote_shares': '10000', 'market_cap': '10000', 'global_cap': '10000'})
        m.quote(market(), books(), 60000, NOW)
        assert 0 < Decimal(a.state['locked']) <= Decimal(cap)
        assert Decimal(a.state['free']) + Decimal(a.state['locked']) == 5000


def test_missing_mark_reported_and_loss_stop_cancels():
    a, m = setup()
    m.trade('42', 0, print_(), NOW + 100)
    m.mark('42', 0, '.4', NOW + 8000, NOW + 8000)
    assert m.stats(NOW)['mark_missing'] == 1
    assert m.stats(NOW)['mark_samples'] == 0
    a.daily(NOW)['loss_stop'] = True
    m.quote(market(), books(), 60000, NOW + 200)
    assert not m.state['markets']['42']['quotes']
    assert Decimal(a.state['locked']) == 0


def test_disabled_quoting_retains_residual_for_resolution():
    a, m = setup()
    m.trade('42', 0, print_(size='1'), NOW + 100)
    m.quote(market(), books(), None, NOW + 200)
    assert not m.state['markets']['42']['quotes']
    assert m.state['markets']['42']['qty'] == ['1', '0']
    m.settle('42', 'Up', NOW + 300000)
    assert a.equity() == Decimal('5000.51')


def test_invalid_settings_rejected(monkeypatch):
    from app.services.polymarket.maker import configuration
    monkeypatch.setenv('POLYMARKET_PAPER_MAKER_COMBINED_BID', '1.01')
    with pytest.raises(ValueError, match='configuration'):
        configuration()


def test_fractional_repeated_pairing_releases_exact_cost_basis():
    import random
    rng = random.Random(91)
    for _ in range(3):
        a = Account()
        m = Maker(a, {**DEFAULTS, 'market_cap': '10000', 'global_cap': '10000'})
        for step in range(100):
            at = NOW + step * 1000
            b = books(at)
            for leg in b:
                leg['bids'][0]['price'] = str(Decimal(rng.randrange(20, 50)) / 100)
            m.quote(market(), b, 60000 + step * 100, at)
            for index in (0, 1):
                size = str(Decimal(rng.randrange(1, 9999)) / 1000)
                m.trade('42', index, print_(str(step) + str(index), price='.01', size=size, at=at + 10), at + 10)
            assert Decimal(a.state['positions']) == sum(map(Decimal, m.state['markets']['42']['cost']))
        m.settle('42', 'Down', NOW + 300000)
        assert Decimal(a.state['positions']) == Decimal(a.state['locked']) == 0
        assert a.equity() - 5000 == Decimal(m.stats(NOW)['net_pnl'])


def test_overfilled_side_pauses_and_pairing_side_recovers_at_reduced_caps():
    a, m = setup(dict(market_cap='3', global_cap='3'))
    assert m.trade('42', 0, print_(size='100'), NOW + 100) == 3
    m.config.update(market_cap='1', global_cap='1')
    m.quote(market(), books(NOW + 200), 60000, NOW + 200)
    row = m.state['markets']['42']
    assert Decimal(row['quotes'][0]['remaining']) == 0
    assert Decimal(row['quotes'][1]['remaining']) == 3
    assert m.trade('42', 0, print_('blocked', size='100', at=NOW + 300), NOW + 300) == 0
    assert m.trade('42', 1, print_('pair', size='100', at=NOW + 300), NOW + 300) == 3
    assert m.exposure() == 0
    assert Decimal(a.state['locked']) == 0


def test_pairing_quote_survives_repeated_polls_and_other_market_rollover():
    _, m = setup(dict(market_cap='3', global_cap='6'))
    assert m.trade('42', 0, print_(size='100'), NOW + 100) == 3
    m.quote(market(), books(NOW + 200), 60000, NOW + 200)
    row = m.state['markets']['42']
    joined, quotes = row['joined'], row['quotes']
    cancels = m.stats(NOW)['cancels']
    for step in range(3, 10):
        m.quote(market('43'), books(NOW + step * 100), 60000, NOW + step * 100)
        m.quote(market(), books(NOW + step * 100), 60000, NOW + step * 100)
        assert row['joined'] == joined
        assert row['quotes'] is quotes
    assert m.stats(NOW)['cancels'] == cancels
