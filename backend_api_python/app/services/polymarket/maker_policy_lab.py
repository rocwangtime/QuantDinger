"""Offline screen for paper-maker shadow cohorts. No database and no live orders.

Reads a JSONL export of `shadow_fills` maker events, or collects fills from a
`maker_replay` CSV directory. A variant is `go` only when both the settlement-PnL
lower confidence bound (same one-sided bootstrap as cell admission) and the 60s
markout lower bound are above zero, with the admission sample floors. `go` is a
report field. Nothing here enables quoting.
"""
import gzip
import json
import math
from pathlib import Path

from . import maker_model as mm
from .maker import OUTCOMES, qualifies_touch_stable

# Compact `shadow_fills` detail.fills row. Fields after `eligible` were added 2026-10-27;
# older exports omit them and cannot score touch_stable.
# 0 level, 1 side, 2 qty, 3 price, 4 pre, 5..9 marks at 1/5/15/60/300s,
# 10 outcome value of the filled token, 11 tau bucket, 12 z bucket, 13 queue bucket, 14 eligible,
# 15 exchange ms, 16 rest_ms, 17 sweep 0/1, 18 spread_ticks, 19 touch_age_ms, 20 touch_stable 0/1
HORIZONS = (1, 5, 15, 60, 300)
SUMMARY_SQL = """
SELECT detail->>'cell' AS cell,
       count(*) AS markets,
       sum((detail->>'polls')::numeric) AS polls,
       sum((detail->>'book_missing')::numeric) / nullif(sum((detail->>'polls')::numeric), 0) AS book_missing_share,
       sum((detail->>'book_stale')::numeric) / nullif(sum((detail->>'polls')::numeric), 0) AS book_stale_share,
       sum((detail->>'quotable')::numeric) / nullif(sum((detail->>'open')::numeric), 0) AS quotable_share,
       sum((detail->'shadow'->'eligible'->>'pnl')::numeric) AS eligible_pnl,
       sum((detail->'shadow'->'eligible'->>'shares')::numeric) AS eligible_shares,
       sum((detail->'shadow'->'touch_stable'->>'pnl')::numeric) AS touch_stable_pnl,
       sum((detail->'shadow'->'touch_stable'->>'shares')::numeric) AS touch_stable_shares
FROM qd_polymarket_maker_events
WHERE account_id = 'global' AND strategy = 'maker_fair_value_v3' AND event = 'market_summary'
  AND observed_at >= TIMESTAMPTZ '2026-10-20 00:00:00+08'
GROUP BY 1
ORDER BY 1;
"""
FILLS_SQL = """
SELECT json_build_object(
         'observed_at', observed_at, 'market_id', market_id, 'event', event, 'detail', detail)::text
FROM qd_polymarket_maker_events
WHERE account_id = 'global' AND strategy = 'maker_fair_value_v3' AND event = 'shadow_fills'
  AND observed_at >= TIMESTAMPTZ '2026-10-20 00:00:00+08'
ORDER BY observed_at;
"""


def _num(value):
    if value is None or value == '':
        return None
    return float(value)


def _flag(value):
    if value is None or value == '':
        return None
    if isinstance(value, bool):
        return value
    return bool(int(value))


def parse_compact(raw, market, cell=None):
    """One compact fill array -> a flat record. Missing tail fields stay None."""
    marks = {horizon: _num(raw[5 + index]) if len(raw) > 5 + index else None for index, horizon in enumerate(HORIZONS)}
    sweep = _flag(raw[17]) if len(raw) > 17 else None
    return dict(market=str(market), cell=cell or '', level=int(raw[0]), side=int(raw[1]), qty=_num(raw[2]) or 0.0,
                price=_num(raw[3]), pre=_num(raw[4]), mark60=marks[60], y=_num(raw[10]) if len(raw) > 10 else None,
                eligible=_flag(raw[14]) if len(raw) > 14 else None, sweep=sweep,
                rest_ms=_num(raw[16]) if len(raw) > 16 else None,
                touch_age_ms=_num(raw[19]) if len(raw) > 19 else None)


def normalize_memory(record, market, cell, y):
    mark = (record.get('marks') or {}).get('60')
    if mark is None:
        mark = (record.get('marks') or {}).get(60)
    return dict(market=str(market), cell=cell or '', level=record.get('level'), side=record.get('side'),
                qty=float(record['qty']), price=float(record['price']), pre=float(record['pre']),
                mark60=None if mark is None else float(mark), y=None if y is None else float(y),
                eligible=record.get('eligible'), sweep=record.get('sweep'), rest_ms=record.get('rest_ms'),
                touch_age_ms=record.get('touch_age_ms'))


def iter_events(path):
    path = Path(path)
    opener = gzip.open if path.suffix == '.gz' else open
    with opener(path, 'rt') as handle:
        payload = handle.read().strip()
    if not payload:
        return
    rows = json.loads(payload) if payload[0] == '[' else [json.loads(line) for line in payload.splitlines() if line.strip()]
    for row in rows:
        if isinstance(row.get('detail'), str):
            row['detail'] = json.loads(row['detail'])
        yield row


def load_fills(path):
    """JSONL (or a JSON array) of shadow_fills events or already-flat fill records."""
    fills, skipped = [], 0
    for row in iter_events(path):
        detail = row.get('detail')
        if isinstance(detail, dict) and 'fills' in detail:
            market = row.get('market_id') or detail.get('market_id')
            cell = detail.get('cell')
            if not market:
                skipped += len(detail['fills'])
                continue
            fills.extend(parse_compact(raw, market, cell) for raw in detail['fills'])
        elif 'qty' in row and row.get('market'):
            fills.append(row)
        else:
            skipped += 1
    return fills, skipped


def _parts(fill):
    price, pre, qty = fill['price'], fill['pre'], fill['qty']
    mark, y = fill.get('mark60'), fill.get('y')
    return dict(qty=qty, edge=None if pre is None or price is None else pre - price,
                as60=None if mark is None or pre is None else mark - pre,
                m60=None if mark is None or price is None else mark - price,
                pnl=None if y is None or price is None else y - price)


def _keep(name, fill, rest_min, touch_min):
    eligible = bool(fill.get('eligible'))
    if name == 'eligible':
        return eligible
    if name == 'no_sweep':
        return eligible and fill.get('level') == 0 and fill.get('sweep') is False
    if name == 'touch_stable':
        return qualifies_touch_stable(eligible, fill.get('level'), fill.get('sweep'), fill.get('rest_ms'),
                                       fill.get('touch_age_ms'), rest_min, touch_min)
    raise ValueError(name)


def _lcb(samples, iters, alpha, seed):
    return mm.cluster_bootstrap(samples, iters, alpha, seed) if samples else None


def _band(samples, iters, seed):
    """5th and 95th percentile of a market bootstrap, for the written report."""
    if not samples:
        return None, None
    low = high = None
    clusters = {}
    for cluster, value, weight in samples:
        num, den = clusters.get(cluster, (0.0, 0.0))
        clusters[cluster] = (num + value * weight, den + weight)
    keys = sorted(clusters)
    nums = [clusters[key][0] for key in keys]
    dens = [clusters[key][1] for key in keys]
    try:
        import numpy as np
        rng = np.random.default_rng(seed + 1)
        picks = rng.integers(0, len(keys), size=(iters, len(keys)))
        num, den = np.asarray(nums)[picks].sum(axis=1), np.asarray(dens)[picks].sum(axis=1)
        means = sorted(float(x) for x in np.divide(num, den, out=np.zeros_like(num), where=den > 0))
    except ImportError:  # pragma: no cover
        import random
        rng, means = random.Random(seed + 1), []
        for _ in range(iters):
            picks = [rng.randrange(len(keys)) for _ in keys]
            den = sum(dens[i] for i in picks)
            means.append(sum(nums[i] for i in picks) / den if den else 0.0)
        means.sort()
    if means:
        high = means[min(len(means) - 1, math.floor(0.95 * iters))]
        low = means[max(0, math.ceil(0.05 * iters) - 1)]
    return low, high


def evaluate(fills, rest_min=1000, touch_min=2000, iters=2000, alpha=0.10, min_markets=150, min_shares=1000,
             min_keep=0.05, max_as_edge=0.7, seed=20261027):
    """Score eligible, no_sweep and touch_stable. Amounts are per share; CI is by market."""
    variants = {}
    prepared = []
    for fill in fills:
        if not fill.get('qty'):
            continue
        prepared.append((fill, _parts(fill)))
    eligible_shares = sum(parts['qty'] for fill, parts in prepared if fill.get('eligible'))
    for name in ('eligible', 'no_sweep', 'touch_stable'):
        markets = {}
        for fill, parts in prepared:
            if not _keep(name, fill, rest_min, touch_min):
                continue
            slot = markets.setdefault(fill['market'], dict(qty=0.0, edge=0.0, as60=0.0, as60_qty=0.0, m60=0.0,
                                                           m60_qty=0.0, pnl=0.0, pnl_qty=0.0))
            slot['qty'] += parts['qty']
            if parts['edge'] is not None:
                slot['edge'] += parts['edge'] * parts['qty']
            if parts['as60'] is not None:
                slot['as60'] += parts['as60'] * parts['qty']
                slot['as60_qty'] += parts['qty']
            if parts['m60'] is not None:
                slot['m60'] += parts['m60'] * parts['qty']
                slot['m60_qty'] += parts['qty']
            if parts['pnl'] is not None:
                slot['pnl'] += parts['pnl'] * parts['qty']
                slot['pnl_qty'] += parts['qty']
        shares = sum(slot['qty'] for slot in markets.values())
        edge = sum(slot['edge'] for slot in markets.values())
        as60 = sum(slot['as60'] for slot in markets.values())
        as60_qty = sum(slot['as60_qty'] for slot in markets.values())
        m60_qty = sum(slot['m60_qty'] for slot in markets.values())
        pnl_qty = sum(slot['pnl_qty'] for slot in markets.values())
        pnl_samples = [(market, slot['pnl'] / slot['pnl_qty'], slot['pnl_qty']) for market, slot in markets.items() if slot['pnl_qty']]
        m60_samples = [(market, slot['m60'] / slot['m60_qty'], slot['m60_qty']) for market, slot in markets.items() if slot['m60_qty']]
        pnl_lcb = _lcb(pnl_samples, iters, alpha, seed)
        m60_lcb = _lcb(m60_samples, iters, alpha, seed + 17)
        m60_low, m60_high = _band(m60_samples, iters, seed + 17)
        ratio = -as60 / edge if edge > 0 and as60_qty else None
        keep = shares / eligible_shares if eligible_shares else None
        reasons = []
        if shares <= 0:
            reasons.append('no_fills')
        if len(markets) < min_markets:
            reasons.append('markets')
        if shares < min_shares:
            reasons.append('shares')
        if keep is None or keep < min_keep:
            reasons.append('keep_share')
        if pnl_lcb is None or not pnl_lcb > 0:
            reasons.append('pnl_lcb')
        if m60_lcb is None or not m60_lcb > 0:
            reasons.append('m60_lcb')
        if ratio is None or ratio > max_as_edge:
            reasons.append('as_edge_ratio')
        if as60_qty < 0.5 * shares:
            reasons.append('mark_coverage')
        variants[name] = dict(markets=len(markets), shares=shares, keep_share=keep,
                              edge_per_share=edge / shares if shares else None,
                              as60_per_share=as60 / as60_qty if as60_qty else None,
                              m60_per_share=sum(slot['m60'] for slot in markets.values()) / m60_qty if m60_qty else None,
                              m60_lcb=m60_lcb, m60_ci90=[m60_low, m60_high],
                              pnl_per_share=sum(slot['pnl'] for slot in markets.values()) / pnl_qty if pnl_qty else None,
                              pnl_lcb=pnl_lcb, as_edge_ratio=ratio,
                              mark_coverage=as60_qty / shares if shares else None, go=not reasons, blocked_by=reasons)
    passed = [name for name, row in variants.items() if row['go']]
    return dict(go=bool(passed), passed=passed, variants=variants, floors=dict(
        min_markets=min_markets, min_shares=min_shares, min_keep=min_keep, max_as_edge=max_as_edge, alpha=alpha,
        rest_min=rest_min, touch_min=touch_min), eligible_shares=eligible_shares)


def collect_replay(data, config=None, **kwargs):
    """Run the real maker replay and return flat shadow fills. Read-only on `data`."""
    from .maker_replay import replay
    found = []

    def grab(row, outcome):
        y_up = OUTCOMES.get(outcome)
        for record in row['shadow_fills']:
            y = None if y_up is None else float(y_up if record['side'] == 0 else 1 - y_up)
            found.append(normalize_memory(record, row['market']['id'], row['cell'], y))

    replay(data, config, before_settle=grab, **kwargs)
    return found
