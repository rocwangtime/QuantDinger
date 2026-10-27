"""Regression for the mature-cell deadlock and audited paper capital reset."""
import copy

import pytest

from app.services.polymarket.maker import DATA_FAULTS, DEFAULTS, Maker, params, select
from app.services.polymarket.paper_account import Account, day_key
from tests.test_polymarket_maker import NOW, setup, step
from tests.test_polymarket_maker_optimization import summaries


SCOPE = {'BTC:5m', 'ETH:5m', 'BTC:15m', 'ETH:15m'}


def evidence():
    return sum((summaries(c, 6, 30, lambda d, k: -.5, stale=20, quotable=7) for c in sorted(SCOPE)), [])


def test_mature_failed_filters_reenter_full_only_when_enabled():
    rows = evidence()
    p = params(dict(DEFAULTS, reentry_min_active='0'))
    assert select(rows, {}, p, NOW, SCOPE)['active'] == {}  # Count disabled.
    quiet = select(rows, {}, params(DEFAULTS), NOW, SCOPE)
    assert quiet['active'] == {}  # Negative LCB is not forced back on.
    assert set(quiet['reentry_skipped']) == SCOPE
    legacy = params(dict(DEFAULTS, reentry_negative_lcb='1'))
    result = select(rows, {}, legacy, NOW, SCOPE)
    assert result['active']
    for cell, entry in result['active'].items():
        assert result['scores'][cell]['markets'] >= 150
        assert not result['scores'][cell]['filters']
        assert entry['source'] == 'reentry' and entry['mode'] == 'full'
        assert entry['probation_until'] > day_key(NOW)
    blocked = select(rows, {'cooloff': {c: '2099-01-01' for c in SCOPE}}, legacy, NOW, SCOPE)
    assert not blocked['active']
    assert not select(rows, {}, params(DEFAULTS), NOW, set())['active']
    tomorrow = select(rows, result, legacy, NOW + 86400000, SCOPE)
    assert tomorrow['active'] == result['active']  # Do not eject during probation.
    expired = select(rows, result, legacy, NOW + 2 * 86400000, SCOPE)
    assert all(c not in expired['active'] and c in expired['cooloff'] for c in result['active'])


def test_preopen_without_lookahead_downgrades_with_reason(monkeypatch):
    monkeypatch.setenv('POLYMARKET_OBSERVER_UPDOWN_LOOKAHEAD_SECONDS', '0')
    result = select([], {}, params(dict(DEFAULTS, cold_start='BTC:5m:preopen')), NOW, SCOPE)
    assert result['active']['BTC:5m']['mode'] == 'full'
    assert result['active']['BTC:5m']['reason'] == 'preopen_without_lookahead'


def test_stale_reason_counters_and_legacy_fallback():
    _, maker = setup()
    row = step(maker)
    for reason in DATA_FAULTS:
        maker.sample(row, dict(time_phase='OPEN', reason=reason), NOW)
    assert all(row['summary'][reason] >= 1 for reason in DATA_FAULTS)
    rows = evidence()
    score = select(rows, {}, params(DEFAULTS), NOW, SCOPE)['scores']['BTC:5m']
    assert score['book_stale_share'] == score['stale_share'] == .2
    for row in rows:
        row.update({reason: 0 for reason in DATA_FAULTS})
        row['book_missing'] = 20
    score = select(rows, {}, params(DEFAULTS), NOW, SCOPE)['scores']['BTC:5m']
    assert score['book_stale_share'] == 0 and score['book_missing_share'] == .2


def test_capital_reset_audits_and_preserves_history():
    account = Account()
    account.state['free'] = '4964.34027'
    maker = Maker(account)
    maker.state['halt'] = 'stop'
    maker.state['selection'] = dict(active={}, cooloff={'ETH:5m': '2099-01-01'}, breakers={'ETH:5m': 'stop'})
    maker.state['days']['old'] = {'history': True}
    account.state['days']['2026-01-01'] = {'history': True}
    result = account.reset_capital(NOW)
    assert result['before']['equity'] == '4964.34027'
    assert account.equity() == 5000
    assert result['after']['free'] == account.config['initial_capital']
    assert result['actor'] == 'ops_command'
    assert account.state['events'][-1]['event'] == 'capital_reset'
    assert account.daily(NOW)['start_equity'] == account.daily(NOW)['peak_equity'] == '5000'
    assert maker.state['halt'] is None and not maker.state['selection'].get('cooloff')
    assert maker.state['days']['old'] == {'history': True}
    assert account.state['days']['2026-01-01'] == {'history': True}


@pytest.mark.parametrize('field,value', [('holdings', {'x': {}}), ('pending', {'x': {}}), ('locked', '.01'), ('positions', '.01')])
def test_reset_refuses_exposure_without_mutation(field, value):
    account = Account()
    account.state[field] = value
    before = copy.deepcopy(account.state)
    with pytest.raises(ValueError, match='not_flat'):
        account.reset_capital(NOW)
    assert account.state == before


def test_contaminated_account_rejected():
    from app.commands.polymarket_paper_capital_reset import validate_account_id
    with pytest.raises(ValueError, match='contaminated'):
        validate_account_id('global-contaminated-20261014')


def test_reentry_quotes_full_mode_at_probation_size(monkeypatch):
    from tests.test_polymarket_maker import live, market, warm
    monkeypatch.setattr('app.services.polymarket.maker.recorded_cells', lambda: SCOPE)
    _, maker = setup({'reentry_negative_lcb': '1'})
    maker.state['selection'] = {}
    maker.review(evidence(), NOW)
    opened = market(start=NOW - 10000)
    warm(maker, opened, NOW - 75000, NOW)
    row = step(maker, opened)
    assert row['phase'] == 'OPEN'
    assert [o['size'] for o in live(row)] == ['5.00', '5.00']
    event = next(e for e in maker.account.maker_changed if e['event'] == 'selection')
    assert event['detail']['active']['BTC:5m']['source'] == 'reentry'


def test_reset_does_not_rehalt_on_historical_daily_or_weekly_losses():
    _, maker = setup()
    day = maker.stats(NOW)
    day.update(net_pnl='-300', hard_stop=True, soft_stop=True)
    maker.state['halt'] = {'reason': 'weekly_stop'}
    maker.account.reset_capital(NOW)
    maker.begin(NOW)
    assert maker.state['halt'] is None
    assert not maker.risk['hard'] and not maker.risk['soft']
    assert maker.risk['week'] == 0 and day['net_pnl'] == '-300'


def test_reset_then_worker_begin_review_does_not_cooloff_all_mature_cells(monkeypatch):
    monkeypatch.setattr('app.services.polymarket.maker.recorded_cells', lambda: SCOPE)
    _, maker = setup()
    maker.account.reset_capital(NOW)
    maker.begin(NOW)
    maker.review(evidence(), NOW)
    assert not maker.state['selection']['active']
    assert not maker.state['selection']['cooloff']
    assert set(maker.state['selection']['reentry_skipped']) == SCOPE


def test_reentry_caps_breakers_and_partial_counter_coverage():
    rows = evidence()
    p = params(dict(DEFAULTS, max_active='1', max_active_per_coin='1', reentry_min_active='4', reentry_negative_lcb='1'))
    result = select(rows, {'breakers': {'BTC:15m': 'stop'}}, p, NOW, SCOPE)
    assert len(result['active']) == 1 and 'BTC:15m' not in result['active']
    for row in rows:
        row.update(book_stale=0, reason_polls=1)
    score = select(rows, {}, p, NOW, SCOPE)['scores']['BTC:5m']
    assert score['book_stale_share'] == .2  # Old in-flight rows have partial coverage.


def test_5m_quotable_threshold_preserves_profitability_and_other_intervals():
    rows = sum((summaries(c, 6, 30, lambda d, k: -.5, stale=20, quotable=7.9) for c in SCOPE), [])
    for row in rows:
        row.update({reason: 0 for reason in DATA_FAULTS})
        row['book_missing'] = 20
    result = select(rows, {}, params(DEFAULTS), NOW, SCOPE)
    assert result['scores']['BTC:5m']['filters']
    assert not result['scores']['BTC:5m']['admissible']  # Negative LCB is still fatal to admission.
    assert not result['scores']['BTC:15m']['filters']  # 15m keeps the .20 minimum.
    old_metric = select(rows, {}, params(dict(DEFAULTS, stale_metric='stale')), NOW, SCOPE)
    assert not old_metric['scores']['BTC:5m']['filters']
