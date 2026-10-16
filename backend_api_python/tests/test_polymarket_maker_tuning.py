"""v3 maker after the replay calibration: data coherence, fair tolerance, gates, late official results, replay."""
import csv
import gzip
import json
from decimal import Decimal as D

import pytest

from app.services.polymarket import maker_model as mm
from app.services.polymarket import maker_replay
from app.services.polymarket.maker import DEFAULTS, coherent, mirror, params, select
from app.services.polymarket.paper_account import utc
from tests.test_polymarket_maker import NOW, book, books, live, market, print_, setup, step, warm

P = params(dict(DEFAULTS))


def test_mirror_rebuilds_a_lagging_or_missing_side_from_the_newer_one():
    up, down = book(at=NOW), book(at=NOW - 300)
    assert coherent([up, down], 250) == ([up, down], False)  # Tops mirror: coherent at any skew.
    lagging = book(bid='.45', ask='.47', at=NOW - 5000)
    pair, rebuilt = coherent([up, lagging], 250)
    assert rebuilt and pair[0] is up and pair[1]['exchange_at'] == NOW
    assert (pair[1]['bids'][0]['price'], pair[1]['asks'][0]['price']) == ('0.49', '0.51')
    assert coherent([up, book(bid='.45', ask='.47', at=NOW - 100)], 250)[1] is False  # Synced: left to later checks.
    pair, rebuilt = coherent([None, down], 250)
    assert rebuilt and pair[0] == mirror(down) and pair[1] is down
    assert coherent([None, None], 250) == ([None, None], False)
    assert mirror(mirror(up)) == dict(up, bids=[dict(price='0.49', size='100')], asks=[dict(price='0.51', size='100')])


def test_lagging_side_is_rebuilt_and_quoted_instead_of_calming_the_market():
    _, m = setup()
    row = step(m, bk=[book(at=NOW), book(bid='.45', ask='.47', at=NOW - 5000)])
    assert row['phase'] == 'PREOPEN' and m.stats(NOW)['mirror_rebuilt'] == 1
    assert sorted((o['side'], o['price']) for o in live(row)) == [(0, '0.49'), (1, '0.49')]


def test_quiet_book_stays_current_only_while_the_recorder_is_alive():
    _, m = setup()
    m.feed_at = NOW - 500  # Recorder wrote another row half a second ago.
    row = step(m, bk=books(NOW - 30000))
    assert row['phase'] == 'PREOPEN' and len(live(row)) == 2
    m.feed_at = NOW + 100 - 5000  # Writer stalled: strict age limit again.
    step(m, at=NOW + 100, bk=books(NOW - 30000))
    assert row['decision']['reason'] == 'book_stale' and not live(row)
    m.feed_at = NOW + 200
    step(m, at=NOW + 200, bk=books(NOW + 200 - 61000))  # Beyond BOOK_QUIET_MS even with a live feed.
    assert row['decision']['reason'] == 'book_stale'


def test_strike_is_locked_at_t0_even_without_books():
    _, m = setup()
    mk = market()  # Strike window [NOW + 60 s, NOW + 120 s].
    warm(m, mk, NOW, NOW + 125000)
    row = step(m, mk, at=NOW + 121000, bk=[None, None], spot=None)
    assert row['decision']['reason'] == 'book_missing' and row['a_start'] is not None
    strike = row['a_start']
    warm(m, mk, NOW + 125000, NOW + 400000)  # Samples of the strike window are long gone.
    step(m, mk, at=NOW + 400000, bk=books(NOW + 400000), spot=None)
    assert row['a_start'] == strike and row['decision']['reason'] != 'strike_unknown'


def test_fair_tolerance_lets_the_book_lead_within_model_noise():
    skewed = lambda: books(up=book(bid='.52', ask='.54'), down=book(bid='.46', ask='.48'))  # M = .53, F = .5 pre-open.
    _, m = setup()
    row = step(m, bk=skewed())
    assert row['phase'] == 'PREOPEN' and row['fair'] == pytest.approx([.53, .47])
    _, m = setup({'fair_tol': '0'})
    row = step(m, bk=skewed())
    assert row['fair'] == pytest.approx([.5, .47])  # min(F, M): the original conservative fair.


def test_gap_gate_switches_to_reduce_beyond_six_cents():
    _, m = setup()
    row = step(m, bk=books(up=book(bid='.54', ask='.56'), down=book(bid='.44', ask='.46')))  # Gap .05.
    assert row['phase'] == 'PREOPEN'
    row = step(m, at=NOW + 100, bk=books(NOW + 100, up=book(bid='.56', ask='.58', at=NOW + 100),
                                         down=book(bid='.42', ask='.44', at=NOW + 100)))  # Gap .07.
    assert row['phase'] == 'REDUCE' and row['decision']['sides'][0]['reason'] == 'phase_reduce'


def test_opening_quotes_may_rest_a_few_ticks_behind_the_best_bid():
    tick = D('.01')
    assert mm.constrain(D('.47'), False, D('.50'), D('.51'), tick, P) == (D('.47'), None)
    assert mm.constrain(D('.46'), False, D('.50'), D('.51'), tick, P) == (None, 'behind_best')
    strict = params(dict(DEFAULTS, open_max_behind_ticks='0'))
    assert mm.constrain(D('.49'), False, D('.50'), D('.51'), tick, strict) == (None, 'behind_best')


def settled(outcome='Up'):
    a, m = setup()
    row = step(m)
    m.prints('42', 0, [print_('through', price='.48', size='1')], NOW + 400)  # 10 Up at .49.
    assert D(row['qty'][0]) == 10
    end = int(row['spec']['t1'] * 1000)
    m.settle('42', outcome, end + 1000, estimated=True)
    return a, m, end


def test_late_official_result_restates_a_proxy_settlement():
    a, m, end = settled('Up')
    day = m.stats(end)
    assert a.equity() == D('5005.1') and day['estimated_settlements'] == 1 and D(day['net_pnl']) == D('5.1')
    assert set(m.state['estimates']['42']['effects']) == {'Up', 'Down', 'Tie'}
    m.account.maker_changed = []
    m.correct('42', 'Down', end + 900000)
    assert a.equity() == D('4995.1') and '42' not in m.state['estimates']
    assert D(day['net_pnl']) == D('-4.9') and D(day['settlement_pnl']) == D('-4.9')
    assert D(day['decomposed_pnl']) == D('-4.9') and D(day['unpaired_settlement_loss']) == D('4.9')
    assert D(day['cells']['BTC:5m']['pnl']) == D('-4.9')
    assert day['estimated_settlements'] == 0 and day['settlement_corrections'] == 1
    summary = [e['detail'] for e in m.account.maker_changed if e['event'] == 'market_summary'][0]
    assert (summary['outcome'], summary['estimated'], summary['corrected'], D(summary['pnl'])) == ('Down', False, True, D('-4.9'))
    m.correct('42', 'Up', end + 900001)  # Already restated: nothing left to apply.
    assert a.equity() == D('4995.1')


def test_matching_or_unknown_official_result_changes_nothing():
    a, m, end = settled('Up')
    m.correct('42', None, end + 1000)
    assert '42' in m.state['estimates']
    m.correct('42', 'Up', end + 2000)
    assert '42' not in m.state['estimates'] and a.equity() == D('5005.1')
    assert m.stats(end)['settlement_corrections'] == 0


def test_correction_lands_today_once_the_settlement_day_is_pruned():
    a, m, end = settled('Down')
    m.state['estimates']['42']['day'] = '2000-01-01'
    later = end + 3 * 86400000 - 1
    m.correct('42', 'Up', later)
    assert a.equity() == D('5005.1') and D(m.stats(later)['net_pnl']) == D('10')
    assert m.stats(later)['settlement_corrections'] == 1


def test_estimates_expire_after_three_days():
    _, m, end = settled('Up')
    assert m.pending_estimates(end + 1000) == ['42']
    assert m.pending_estimates(end + 1000 + 3 * 86400000 + 1) == []


def summary(market_id, pnl, **extra):
    return dict(market_id=market_id, cell='BTC:15m', end=NOW - 86400000, outcome='Up', polls=10, open=10, tight=10,
                stale=0, quotable=10, taker_volume='5000',
                shadow=dict(eligible=dict(shares='10', pnl=pnl, edge='1', as60='-.1')), **extra)


def test_selection_scores_the_corrected_summary_instead_of_the_proxy_one():
    p = params(dict(DEFAULTS))
    for order in ([summary('9', '-3'), summary('9', '2', corrected=True)], [summary('9', '2', corrected=True), summary('9', '-3')]):
        score = select(order, {}, p, NOW)['scores']['BTC:15m']
        assert score['markets'] == 1 and score['mean'] == pytest.approx(2.0)


def write(path, rows):
    with gzip.open(path, 'wt', newline='') as handle:
        csv.writer(handle).writerows(rows)


def test_replay_drives_the_real_maker_over_recorded_exports(tmp_path):
    start, end = NOW + 120000, NOW + 420000
    iso = lambda ms: utc(ms).isoformat()
    write(tmp_path / 'windows.csv.gz', [['42', 'BTC', '5m', iso(start), iso(end), '60', 'Down', '42u', '42d']])
    write(tmp_path / 'spot.csv.gz', [['BTC', iso(t), '60000'] for t in range(NOW - 600000, end + 60000, 5000)])
    level = lambda price: json.dumps([dict(price=price, size='100')])
    write(tmp_path / 'books_x.csv.gz', [['42', token, iso(t), iso(t + 50), level(bid), level(ask)]
                                        for t in range(start - 200000, end, 1000) for token, bid, ask in (('42u', '.49', '.51'), ('42d', '.49', '.51'))])
    write(tmp_path / 'trades_x.csv.gz', [['t1', '42', '42u', '.48', '50', 'SELL', iso(start - 100000), iso(start - 99900), '0x1']])
    data = maker_replay.load(tmp_path)
    assert (tmp_path / 'markets' / '42.pkl').exists() and len(data['spot']['BTC']) > 100
    out = maker_replay.replay(data, cells=('BTC:5m',), start=NOW - 120000, end=end + 120000)
    assert out['markets_settled'] == 1 and out['cells']['BTC:5m']['open_polls'] > 0
    assert out['filled_up'] > 0 and out['net_pnl'] < 0  # Swept Up bid, market resolved Down.
    assert out['attribution']['all']['qty'] == out['filled_up'] + out['filled_down']
    tally = out['cells']['BTC:5m']['tally']
    assert sum(v for k, v in tally.items() if k.startswith('reason:')) == tally['open_polls']


def test_replay_rebuilds_the_rest_poller_from_public_klines(tmp_path):
    write(tmp_path / 'klines_BTCUSDT.csv.gz', [[str(NOW), '60000'], [str(NOW + 1000), '60010'], [str(NOW + 5000), '60050']])
    series = maker_replay.klines(tmp_path, 'BTC', 2000)
    assert series == [(NOW + 1000.0, '60000'), (NOW + 3000.0, '60010'), (NOW + 5000.0, '60010')]
    assert maker_replay.klines(tmp_path, 'ETH', 2000) is None


def test_place_audit_keeps_only_the_top_of_book():
    a, m = setup()
    step(m, bk=books(up=book(more=[dict(price='.48', size='300')])))
    place = [e['detail'] for e in a.maker_changed if e['event'] == 'place'][0]
    assert place['books'][0] == dict(bid=dict(price='.49', size='100'), ask=dict(price='.51', size='100'), exchange_at=NOW)
