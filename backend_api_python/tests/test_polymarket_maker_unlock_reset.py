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
    assert select(rows, {}, p, NOW, SCOPE)['active'] == {}  # Previous behavior.
    result = select(rows, {}, params(DEFAULTS), NOW, SCOPE)
    assert result['active']
    for cell, entry in result['active'].items():
        assert result['scores'][cell]['markets'] >= 150
        assert not result['scores'][cell]['filters']
        assert entry['source'] == 'reentry' and entry['mode'] == 'full'
        assert entry['probation_until'] > day_key(NOW)
    blocked = select(rows, {'cooloff': {c: '2099-01-01' for c in SCOPE}}, params(DEFAULTS), NOW, SCOPE)
    assert not blocked['active']
    assert not select(rows, {}, params(DEFAULTS), NOW, set())['active']
    tomorrow = select(rows, result, params(DEFAULTS), NOW + 86400000, SCOPE)
    assert tomorrow['active'] == result['active']  # Do not eject during probation.
    expired = select(rows, result, params(DEFAULTS), NOW + 2 * 86400000, SCOPE)
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
