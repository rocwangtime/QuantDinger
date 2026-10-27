"""Shadow cohort tagging, admission that follows that evidence, and the offline screen."""
import json
from decimal import Decimal as D

import pytest

from app.services.polymarket.maker import DEFAULTS, params, select
from app.services.polymarket.maker_policy_lab import evaluate, load_fills, parse_compact
from tests.test_polymarket_maker import NOW, books, live, print_, setup, step
from tests.test_polymarket_maker_optimization import summaries


SCOPE = {'BTC:5m', 'ETH:5m', 'BTC:15m', 'ETH:15m'}


def test_fast_sweep_is_not_touch_stable_and_a_quiet_queue_fill_is():
    account, maker = setup()
    row = step(maker)
    maker.prints('42', 0, [print_('q', price='.49', size='105')], NOW + 400)
    level0 = next(record for record in row['shadow_fills'] if record['level'] == 0)
    assert level0['sweep'] is False and level0['touch_stable'] is False
    maker.settle('42', 'Up', NOW + 420000)
    summary = next(event['detail'] for event in account.maker_changed if event['event'] == 'market_summary')
    assert D(summary['shadow']['touch_stable']['shares']) == 0
    audit = next(event['detail'] for event in account.maker_changed if event['event'] == 'shadow_fills')
    assert audit['cell'] == 'BTC:5m' and len(audit['fills'][0]) == 21

    maker = setup()[1]
    maker.feed_at = NOW
    opened = step(maker, at=NOW, bk=books(NOW - 3000))
    maker.prints('42', 0, [print_('quiet', price='.49', size='105', at=NOW + 1500)], NOW + 1600)
    tagged = next(record for record in opened['shadow_fills'] if record['level'] == 0)
    assert tagged['touch_stable'] is True and tagged['rest_ms'] >= 1000 and tagged['touch_age_ms'] >= 2000
    swept = next(record for record in opened['shadow_fills'] if record['level'] == 1)
    assert swept['sweep'] is True and swept['touch_stable'] is False


def test_touch_stable_live_blocks_opening_until_the_touch_is_quiet():
    _, maker = setup({'touch_stable_live': '1'})
    maker.feed_at = NOW
    row = step(maker, bk=books(NOW))
    assert not live(row)
    assert row['decision']['sides'][0]['reason'] == 'touch_unstable'
    step(maker, at=NOW + 2500, bk=books(NOW + 2500))
    assert [order['size'] for order in live(row)] == ['10.00', '10.00']


def test_touch_stable_bucket_admits_and_negative_eligible_does_not():
    rows = summaries('BTC:5m', 6, 30, lambda day, k: -.4, edge='.01', as60='-.05')
    for row in rows:
        row['shadow']['touch_stable'] = dict(shares='10', pnl='0.20', edge='0.05', as60='-0.01')
    denied = select(rows, {}, params(DEFAULTS), NOW, SCOPE)
    assert not denied['scores']['BTC:5m']['admissible'] and 'BTC:5m' not in denied['active']
    allowed = select(rows, {}, params(dict(DEFAULTS, admission_shadow='touch_stable')), NOW, SCOPE)
    assert allowed['scores']['BTC:5m']['admissible']
    assert allowed['scores']['BTC:5m']['admission_shadow'] == 'touch_stable'
    assert allowed['active']['BTC:5m']['source'] == 'admitted'
    cooled = select(rows, {'cooloff': {'BTC:5m': '2099-01-01'}}, params(dict(DEFAULTS, admission_shadow='touch_stable')), NOW, SCOPE)
    assert cooled['active']['BTC:5m']['source'] == 'admitted' and 'BTC:5m' not in cooled['cooloff']
    missing = select(summaries('BTC:5m', 6, 30, lambda day, k: .2), {}, params(dict(DEFAULTS, admission_shadow='touch_stable')), NOW, SCOPE)
    assert not missing['scores']['BTC:5m']['admissible']


def test_config_hash_change_drops_cooloff_and_a_missing_hash_keeps_it():
    _, maker = setup()
    maker.state['selection'] = dict(active={}, cooloff={'ETH:1h': '2099-01-01'}, breakers={})
    maker.review([], NOW)
    assert maker.state['selection']['cooloff'].get('ETH:1h') == '2099-01-01'
    assert maker.state['selection']['config_hash'] == maker.hash
    maker.state['selection']['config_hash'] = 'previous-strategy'
    maker.state['selection']['cooloff'] = {'ETH:1h': '2099-01-01'}
    maker.review([], NOW + 86400000)
    assert 'ETH:1h' not in maker.state['selection']['cooloff']


def test_invalid_admission_bucket_is_rejected():
    with pytest.raises(ValueError):
        params(dict(DEFAULTS, admission_shadow='level0'))


def _fill(identity, m60, sweep):
    price = 0.50
    return dict(market=identity, cell='BTC:5m', qty=10, price=price, pre=price + 0.01, mark60=price + m60, y=price + m60,
                eligible=True, level=0, sweep=sweep, rest_ms=10 if sweep else 5000, touch_age_ms=0 if sweep else 5000)


def test_policy_lab_rejects_the_sweep_book_and_passes_a_quiet_cohort():
    toxic = [_fill(f't{i}', -0.04, True) for i in range(160)]
    quiet = [_fill(f'q{i}', 0.02, False) for i in range(160)]
    report = evaluate(toxic + quiet, iters=200, min_markets=150, min_shares=1000)
    assert report['variants']['eligible']['go'] is False
    assert report['variants']['eligible']['m60_per_share'] == pytest.approx(-0.01)
    assert 'm60_lcb' in report['variants']['eligible']['blocked_by']
    assert report['variants']['touch_stable']['go'] is True
    assert report['variants']['touch_stable']['m60_lcb'] > 0
    assert report['variants']['touch_stable']['pnl_lcb'] > 0
    assert report['variants']['touch_stable']['keep_share'] == pytest.approx(0.5)
    assert 'touch_stable' in report['passed']
    losing = evaluate(toxic, iters=200, min_markets=150, min_shares=1000)
    assert losing['go'] is False
    assert losing['variants']['eligible']['m60_ci90'][1] < 0


def test_compact_shadow_row_without_context_cannot_score_touch_stable(tmp_path):
    raw = [0, 0, '10', '0.49', 0.50, None, None, None, 0.46, None, '0', 3, 2, 0, True]
    parsed = parse_compact(raw, 'm1', 'BTC:5m')
    assert parsed['mark60'] == pytest.approx(0.46) and parsed['sweep'] is None and parsed['eligible'] is True
    path = tmp_path / 'fills.jsonl'
    path.write_text(json.dumps(dict(market_id='m1', event='shadow_fills', detail=dict(cell='BTC:5m', fills=[raw]))) + '\n')
    fills, skipped = load_fills(path)
    assert skipped == 0 and fills[0]['price'] == pytest.approx(0.49)
    report = evaluate(fills, iters=50, min_markets=1, min_shares=1, min_keep=0)
    assert report['variants']['touch_stable']['shares'] == 0
    assert report['go'] is False


def test_defaults_keep_live_quotes_on_the_eligible_bucket():
    typed = params(DEFAULTS)
    assert typed.reentry_negative_lcb == 0 and typed.touch_stable_live == 0
    assert typed.admission_shadow == 'eligible'
    assert (typed.touch_stable_ms, typed.touch_stable_rest_ms) == (2000, 1000)
    _, maker = setup()
    assert maker.config['admission_shadow'] == 'eligible' and maker.config['reentry_negative_lcb'] == '0'
    assert live(step(maker))  # Cohort tagging is not a live gate.
