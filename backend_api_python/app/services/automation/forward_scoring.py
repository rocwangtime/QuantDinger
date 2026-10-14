"""Close-to-close US plan decisions versus frozen, equal-weight shortlist and ETFs.

Anchor is the last session closed at decision time. Targets are 1/5 subsequent
XNYS session closes. All legs use the same QFQ daily series (gross, no costs).
WAIT has zero strategy return; reductions/exits use negative symbol return.
"""
import math
import time
from datetime import datetime, timezone

from app.services.automation import store
from app.services.automation.domain import validate_universe_source

HORIZONS = ('1d', '5d')
BASELINES = ('shortlist', 'SPY', 'QQQ')


def schedule(at):
    from app.services.us_screener.core import calendar
    from app.services.us_screener.service import latest_closed_day
    cal = calendar()
    anchor = cal.date_to_session(str(latest_closed_day(at)))
    targets = {}
    session = anchor
    for n in range(1, 6):
        session = cal.next_session(session)
        if n in (1, 5):
            targets[f'{n}d'] = cal.session_close(session).to_pydatetime()
    return str(anchor.date()), targets


def register(run):
    evidence, result = run.get('evidence') or {}, run.get('result') or {}
    config = evidence.get('task_config') or {}
    if run['preview'] or not evidence.get('screener') or not result.get('decision_at'):
        return
    validate_universe_source(config)
    if config.get('execution_mode') != 'plan_only':
        return
    at = datetime.fromtimestamp(result['decision_at'], timezone.utc)
    anchor, targets = schedule(at)
    for item in result.get('items', []):
        inputs = {'market': 'USStock', 'execution_mode': 'plan_only', 'action': item['action'],
                  'target_weight': item['target_weight'], 'anchor_day': anchor,
                  'screener_run_id': evidence['screener']['run_id'],
                  'shortlist': evidence['screener']['symbols']}
        for horizon, due in targets.items():
            store.query('''INSERT INTO qd_agent_decision_scores
                (run_id,task_id,symbol,horizon,decision_at,due_at,inputs)
                VALUES(%s,%s,%s,%s,%s,%s,%s::jsonb) ON CONFLICT DO NOTHING RETURNING run_id''',
                (run['id'], run['task_id'], item['symbol'], horizon, at, due, store.dumps(inputs)))


def outcome(score, series):
    from app.services.automation.scoring import US
    anchor = score['inputs']['anchor_day']
    target = score['due_at'].astimezone(US).date().isoformat()

    def change(symbol):
        marks = {str(b['time_key'])[:10]: float(b['close']) for b in series.get(symbol, [])}
        prices = [marks.get(anchor), marks.get(target)]
        if any(p is None or not math.isfinite(p) or p <= 0 for p in prices):
            return None
        return (prices[1]/prices[0]-1)*100

    names = score['inputs']['shortlist']
    changes = {s: change(s) for s in set(names + ['SPY', 'QQQ', score['symbol']])}
    if not names or any(v is None for v in changes.values()):
        return None
    raw = changes[score['symbol']]
    action = score['inputs']['action']
    directional = raw * (1 if action in {'BUY', 'HOLD'} else -1 if action in {'REDUCE', 'EXIT'} else 0)
    baselines = {'shortlist': sum(changes[s] for s in names)/len(names),
                 'SPY': changes['SPY'], 'QQQ': changes['QQQ']}
    return {'return_pct': raw, 'directional_return_pct': directional,
            'baseline_return_pct': baselines,
            'excess_return_pct': {k: directional-v for k, v in baselines.items()},
            'source': 'futu_QFQ_K_DAY', 'anchor_day': anchor, 'target_day': target}


def tick(now):
    from app.services.us_screener.data import FutuData
    from app.services.automation.scoring import US
    scores = store.query('''SELECT * FROM qd_agent_decision_scores
        WHERE horizon IN ('1d','5d') AND outcome IS NULL AND due_at<=%s
        AND (checked_at IS NULL OR checked_at<%s-INTERVAL '10 minutes')
        ORDER BY checked_at NULLS FIRST,due_at LIMIT 40''', (now, now))
    provider, cache = None, {}
    deadline = time.monotonic()+50
    try:
        for score in scores:
            if time.monotonic() >= deadline:
                break
            try:
                if score['inputs'].get('execution_mode') != 'plan_only':
                    continue
                day = score['due_at'].astimezone(US).date()
                series = {}
                for symbol in set(score['inputs']['shortlist'] + ['SPY', 'QQQ', score['symbol']]):
                    key = (symbol, day)
                    if key not in cache:
                        if provider is None:
                            provider = FutuData(deadline)
                        cache[key] = provider.bars(symbol, day, required_days=(score['inputs']['anchor_day'], str(day)))
                    series[symbol] = cache[key]
                value = outcome(score, series)
                store.query('''UPDATE qd_agent_decision_scores SET outcome=%s::jsonb,checked_at=%s
                    WHERE run_id=%s AND symbol=%s AND horizon=%s AND outcome IS NULL RETURNING run_id''',
                    (store.dumps(value) if value else None, now, score['run_id'], score['symbol'], score['horizon']))
            except Exception:
                store.query('''UPDATE qd_agent_decision_scores SET checked_at=%s
                    WHERE run_id=%s AND symbol=%s AND horizon=%s RETURNING run_id''',
                    (now, score['run_id'], score['symbol'], score['horizon']))
    finally:
        if provider:
            provider.close()


def metrics(runs, scores):
    selected = [s for s in scores if s['horizon'] in HORIZONS]
    horizons = {}
    for horizon in HORIZONS:
        group = [s for s in selected if s['horizon'] == horizon]
        values = [s['outcome'] for s in group if s['outcome']]
        horizons[horizon] = {'scored': len(values), 'pending': len(group)-len(values),
            'baselines': {b: {'hit_rate': sum(v['excess_return_pct'][b] > 0 for v in values)/len(values) if values else None,
                             'mean_excess_return_pct': sum(v['excess_return_pct'][b] for v in values)/len(values) if values else None}
                          for b in BASELINES}}
    turnovers, ids = [], []
    for run in runs:
        evidence = run.get('evidence') or {}
        if not evidence.get('screener'):
            continue
        ids.append(evidence['screener']['run_id'])
        previous = evidence.get('previous_plan') or {}
        if 'items' not in previous or 'items' not in (run.get('result') or {}):
            continue
        before = {i['symbol']: i['target_weight'] for i in previous['items']}
        after = {i['symbol']: i['target_weight'] for i in run['result']['items']}
        names = set(before) | set(after)
        turnovers.append(sum(before.get(s) != after.get(s) for s in names)/len(names) if names else 0)
    decisions = {(s['run_id'], s['symbol']) for s in selected}
    pending = {(s['run_id'], s['symbol']) for s in selected if s['outcome'] is None}
    return {'scored_decisions': len(decisions-pending), 'pending_decisions': len(pending),
            'horizons': horizons, 'screener_run_ids': sorted(set(ids)),
            'turnover': sum(turnovers)/len(turnovers) if turnovers else None,
            'turnover_samples': len(turnovers),
            'basis': 'Gross directional close-to-close excess; WAIT return=0; last closed session anchor; turnover is fraction of symbols with changed membership/target weight versus previous plan.'}


def rescore(user_id, run_id):
    """Invalidate only forward observations; immutable inputs/due dates survive."""
    store.ensure_schema()
    with store.get_db_connection() as db:
        cur = db.cursor()
        try:
            # Serialize reset with the scoring tick so in-flight marks cannot
            # overwrite a newly requested reset.
            cur.execute('SELECT pg_advisory_xact_lock(824115,5)')
            cur.execute('SELECT id,task_id,created_at FROM qd_agent_automation_runs WHERE id=%s AND user_id=%s',
                        (run_id, user_id))
            run = cur.fetchone()
            if not run:
                raise ValueError('Run not found')
            cur.execute("""UPDATE qd_agent_decision_scores SET outcome=NULL,checked_at=NULL
                WHERE run_id=%s AND horizon IN ('1d','5d') RETURNING symbol,horizon""", (run_id,))
            reset = cur.fetchall()
            if reset:
                # Rebuild pending coverage even when neither horizon is due yet.
                cur.execute("""DELETE FROM qd_agent_daily_reports WHERE task_id=%s
                    AND day=(%s AT TIME ZONE 'America/New_York')::date RETURNING task_id""",
                    (run['task_id'], run['created_at']))
            db.commit()
            return {'reset_scores': len(reset)}
        finally:
            cur.close()
