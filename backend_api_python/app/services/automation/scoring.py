"""Read-only HK decision outcomes. Hypothetical gross PnL is not account profit."""
import math
import time
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from app.services.automation import latency, store

HK = ZoneInfo('Asia/Hong_Kong')
HORIZONS = ('5m', '30m', 'close', 'next_close')


def horizons(at):
    import exchange_calendars as xcals
    cal = xcals.get_calendar('XHKG')
    session = cal.date_to_session(at.astimezone(HK).date().isoformat(), direction='next')
    close = cal.session_close(session).to_pydatetime()
    if at >= close:
        session = cal.next_session(session)
        close = cal.session_close(session).to_pydatetime()
    return {'5m': at + timedelta(minutes=5), '30m': at + timedelta(minutes=30),
            'close': close, 'next_close': cal.session_close(cal.next_session(session)).to_pydatetime()}


def decision_timestamp(result):
    return result.get('decision_at') or ((result.get('fast_latency') or {}).get('timestamps') or {}).get('decision')


def register(run):
    evidence, result = run['evidence'] or {}, run['result'] or {}
    config = evidence.get('task_config') or {}
    if run['preview'] or config.get('market') != 'HKStock' or result.get('source') == 'deterministic_protection':
        return
    stamp = decision_timestamp(result)
    if not stamp:
        return  # Never infer a decision time from later execution completion.
    at = datetime.fromtimestamp(stamp, timezone.utc)
    for item in result.get('items', []):
        instrument = evidence.get('instruments', {}).get(item['symbol'], {})
        price = instrument.get('price')
        if not price or not math.isfinite(float(price)) or price <= 0:
            continue
        lot = instrument.get('lot_size') or (config.get('symbol_params') or {}).get('lot_size')
        qty = hypothetical_quantity(item, config, evidence, price, lot)
        inputs = {'action': item['action'], 'price': price, 'quote_as_of': instrument.get('quote_as_of'),
                  'quantity': qty, 'lot_size': lot, 'execution_mode': config['execution_mode'],
                  'credential_id': config['credential_id'], 'trigger': run['event_key'],
                  'quantity_basis': 'Frozen snapshot budget, cash, exposure and whole lots; not broker executable permission'}
        for horizon, due in horizons(at).items():
            store.query('''INSERT INTO qd_agent_decision_scores
                (run_id,task_id,symbol,horizon,decision_at,due_at,inputs) VALUES(%s,%s,%s,%s,%s,%s,%s::jsonb)
                ON CONFLICT DO NOTHING RETURNING run_id''',
                (run['id'], run['task_id'], item['symbol'], horizon, at, due, store.dumps(inputs)))


def hypothetical_quantity(item, config, evidence, price, lot):
    if item['action'] == 'WAIT':
        return 0
    if not lot or not float(lot).is_integer() or lot <= 0:
        return None
    from app.services.automation.domain import size_order, available_buy_cash
    account = evidence.get('account') or {}
    funds = account.get('funds') or {}
    positions = account.get('positions', [])
    orders = account.get('open_orders', [])
    broker_qty = sum(float(p.get('quantity') or 0) for p in positions
                     if p['symbol'] == item['symbol'] and p.get('side') == 'long')
    owned = evidence.get('managed_quantities', {}).get(item['symbol'], 0)
    if item['action'] == 'HOLD':
        return math.floor(min(owned, broker_qty) / lot) * int(lot)
    exposure = sum(max(0, float(p.get('marketValue') or 0)) for p in positions)
    reserved = sum(max(0, float(o.get('qty') or 0)-float(o.get('filled') or 0))*float(o.get('price') or 0)
                   for o in orders if o.get('side') == 'buy')
    cash = available_buy_cash(funds, 'HKStock')
    if config.get('symbol_params'):
        cash -= config['budget'] * config['reserve_ratio'] + reserved
    power = min(max(0, cash), max(0, config['budget']*(1-config['reserve_ratio'])-exposure-reserved),
                max(0, config['budget']*config['max_weight']-broker_qty*price), config['max_daily_notional'])
    order = size_order(item, config, price=price, lot=lot, owned_qty=owned,
                       broker_qty=broker_qty, power=power, open_symbols={o['symbol'] for o in orders})
    return order['qty'] if order else 0


def outcome(score, bars, receipts):
    due = score['due_at'].timestamp()
    # Minute bars are start-labelled. Intraday targets round up to a minute end;
    # never substitute a later quote or another session for a missing bar.
    candidates = [b for b in bars if 0 <= float(b['time']) + 60 - due < (1 if score['horizon'] in {'close', 'next_close'} else 60)]
    if not candidates:
        return None
    bar = min(candidates, key=lambda b: b['time'])
    price = float(bar['close'])
    if not math.isfinite(price) or price <= 0:
        return None
    inputs = score['inputs']
    direction = 1 if inputs['action'] in {'BUY', 'HOLD'} else -1 if inputs['action'] in {'REDUCE', 'EXIT'} else 0
    change = price / inputs['price'] - 1
    metrics = fill_metrics(score, receipts)
    return {'price': price, 'bar_at': bar['time'], 'source': 'futu_unadjusted_K_1M',
            'return_pct': change * 100, 'directional_return_pct': change * direction * 100 if direction else None,
            'direction_hit': change * direction > 0 if direction else None,
            'would_be_pnl_hkd': (price-inputs['price']) * direction * inputs['quantity'] if inputs['quantity'] is not None else None,
            **metrics}


def fill_metrics(score, receipts):
    inputs = score['inputs']
    direction = 1 if inputs['action'] in {'BUY', 'HOLD'} else -1 if inputs['action'] in {'REDUCE', 'EXIT'} else 0
    fills = [r for r in receipts if r['order_spec']['symbol'] == score['symbol'] and float(r.get('filled_qty') or 0) > 0
             and r.get('avg_fill_price') and r['order_spec']['side'] == ('buy' if direction == 1 else 'sell')]
    filled = sum(float(r['filled_qty']) for r in fills)
    average = sum(float(r['filled_qty']) * float(r['avg_fill_price']) for r in fills) / filled if filled else None
    return {'filled_qty': filled, 'avg_fill_price': average,
            'slippage_bps': (average / inputs['price'] - 1) * direction * 10000 if average and direction else None}


def fetch_bars(row, score):
    from app.services.exchange_execution import resolve_exchange_config
    from app.services.futu_trading.config import config_from_exchange_config
    from app.services.futu_trading.quote_client import FutuQuoteClient
    cfg = resolve_exchange_config({'credential_id': score['inputs']['credential_id']}, user_id=row['user_id'])
    client = FutuQuoteClient(config_from_exchange_config(cfg))
    try:
        if not client.connect():
            return []
        day = score['due_at'].astimezone(HK).date().isoformat()
        return client.get_history_kline(score['symbol'], market_type='HKStock', ktype='K_1M',
                                        start=day, end=day, max_count=1000, autype='NONE')
    finally:
        client.close()


def tick(now=None):
    store.ensure_schema()
    # Session lock covers finite jobs across workers; released on crash/close.
    with store.get_db_connection() as db:
        cur = db.cursor()
        try:
            cur.execute('SELECT pg_try_advisory_lock(824115,5) AS acquired')
            if not cur.fetchone()['acquired']:
                return
            _tick(now)
        finally:
            cur.execute('SELECT pg_advisory_unlock(824115,5)')
            db.commit()
            cur.close()


def _tick(now=None):
    now = now or datetime.now(timezone.utc)
    # Recovery scan is independent of task activation and execution status.
    runs = store.query("""SELECT r.* FROM qd_agent_automation_runs r
        WHERE preview=FALSE AND (jsonb_exists(result,'decision_at')
            OR result->'fast_latency'->'timestamps'->>'decision' IS NOT NULL)
        AND evidence->'task_config'->>'market'='HKStock'
        AND (SELECT COUNT(*) FROM qd_agent_decision_scores s WHERE s.run_id=r.id) <
            4*jsonb_array_length(COALESCE(r.result->'items','[]'::jsonb))
        ORDER BY id LIMIT 100""")
    for run in runs:
        register(run)
    scores = store.query('''SELECT * FROM qd_agent_decision_scores WHERE outcome IS NULL AND due_at<=%s
        AND (checked_at IS NULL OR checked_at<%s) ORDER BY checked_at NULLS FIRST,due_at LIMIT 100''',
        (now-timedelta(seconds=90), now-timedelta(minutes=10)))
    started = time.monotonic()
    bars_cache = {}
    for score in scores:
        if time.monotonic()-started > 60:
            break
        try:
            row = store.query('SELECT * FROM qd_agent_automations WHERE id=%s', (score['task_id'],), one=True)
            key = (row['user_id'], score['inputs']['credential_id'], score['symbol'], score['due_at'].astimezone(HK).date())
            if key not in bars_cache:
                bars_cache[key] = fetch_bars(row, score)
            value = outcome(score, bars_cache[key], store.receipts(row, score['run_id']))
            store.query('''UPDATE qd_agent_decision_scores SET outcome=%s::jsonb,checked_at=%s
                WHERE run_id=%s AND symbol=%s AND horizon=%s AND outcome IS NULL RETURNING run_id''',
                (store.dumps(value) if value else None, now, score['run_id'], score['symbol'], score['horizon']))
        except Exception:
            # No provider exception text: credentials can appear in adapter errors.
            store.query('''UPDATE qd_agent_decision_scores SET checked_at=%s WHERE run_id=%s AND symbol=%s
                AND horizon=%s RETURNING run_id''', (now, score['run_id'], score['symbol'], score['horizon']))
    # Refresh only reconciliation fields; historical horizon marks are immutable.
    completed = store.query('''SELECT s.* FROM qd_agent_decision_scores s
        JOIN qd_agent_automations t ON t.id=s.task_id
        WHERE s.outcome IS NOT NULL AND EXISTS (SELECT 1 FROM qd_agent_trade_intents i
            WHERE i.agent_token_id=t.token_id AND i.order_spec->>'strategy_version'=
                'automation:'||s.task_id::text||':'||s.run_id::text AND i.updated_at>s.checked_at)
        ORDER BY s.checked_at LIMIT 100''')
    for score in completed:
        row = store.query('SELECT * FROM qd_agent_automations WHERE id=%s', (score['task_id'],), one=True)
        metrics = fill_metrics(score, store.receipts(row, score['run_id']))
        store.query('''UPDATE qd_agent_decision_scores SET outcome=outcome || %s::jsonb,checked_at=%s
            WHERE run_id=%s AND symbol=%s AND horizon=%s RETURNING run_id''',
            (store.dumps(metrics), now, score['run_id'], score['symbol'], score['horizon']))
    generate_reports(now)


def read_report(row, day):
    day = datetime.strptime(day, '%Y-%m-%d').date()
    found = store.query('SELECT generated_at,report FROM qd_agent_daily_reports WHERE task_id=%s AND day=%s',
                        (row['id'], day), one=True)
    return found or {'generated_at': None, 'report': None}


def generate_reports(now):
    local = now.astimezone(HK)
    # Rebuild recent completed days so next-close outcomes and late fills arrive.
    recent = store.query("SELECT DISTINCT (r.created_at AT TIME ZONE 'Asia/Hong_Kong')::date AS day FROM qd_agent_decision_scores s JOIN qd_agent_automation_runs r ON r.id=s.run_id WHERE s.checked_at>=%s", (now-timedelta(minutes=2),))
    unreported = store.query("""SELECT DISTINCT (r.created_at AT TIME ZONE 'Asia/Hong_Kong')::date AS day
        FROM qd_agent_automation_runs r JOIN qd_agent_automations t ON t.id=r.task_id
        LEFT JOIN qd_agent_daily_reports d ON d.task_id=r.task_id
            AND d.day=(r.created_at AT TIME ZONE 'Asia/Hong_Kong')::date
        WHERE r.preview=FALSE AND COALESCE(r.evidence->'task_config'->>'market',t.config->>'market')='HKStock'
        AND (d.task_id IS NULL OR r.finished_at>d.generated_at)
        AND r.created_at<%s""", (now,))
    risk_days = store.query("SELECT DISTINCT (occurred_at AT TIME ZONE 'Asia/Hong_Kong')::date AS day FROM qd_agent_risk_events WHERE occurred_at>=%s", (now-timedelta(minutes=2),))
    days = {r['day'] for r in recent+unreported+risk_days}
    if local.hour >= 17 and store.query("SELECT t.id FROM qd_agent_automations t WHERE t.config->>'market'='HKStock' AND NOT EXISTS (SELECT 1 FROM qd_agent_daily_reports d WHERE d.task_id=t.id AND d.day=%s) LIMIT 1", (local.date(),)):
        days.add(local.date())
    for day in sorted(days):
        if day == local.date() and local.hour < 17:
            continue
        start = datetime.combine(day, datetime.min.time(), HK)
        end = start + timedelta(days=1)
        tasks = store.query('''SELECT t.* FROM qd_agent_automations t
            WHERE (t.config->>'market'='HKStock' AND t.created_at<%s) OR EXISTS (
                SELECT 1 FROM qd_agent_automation_runs r WHERE r.task_id=t.id AND r.preview=FALSE
                AND r.created_at>=%s AND r.created_at<%s
                AND COALESCE(r.evidence->'task_config'->>'market',t.config->>'market')='HKStock')''', (end, start, end))
        for row in tasks:
            report = daily(row, start, end)
            store.query('''INSERT INTO qd_agent_daily_reports(task_id,day,generated_at,report) VALUES(%s,%s,%s,%s::jsonb)
                ON CONFLICT(task_id,day) DO UPDATE SET generated_at=EXCLUDED.generated_at,report=EXCLUDED.report
                RETURNING task_id''', (row['id'], day, now, store.dumps(report)))


def daily(row, start, end):
    runs = store.query('''SELECT * FROM qd_agent_automation_runs WHERE task_id=%s AND preview=FALSE
        AND created_at>=%s AND created_at<%s AND event_key NOT LIKE 'protection:%%' ''', (row['id'], start, end))
    scores = store.query('''SELECT s.* FROM qd_agent_decision_scores s JOIN qd_agent_automation_runs r ON r.id=s.run_id
        WHERE s.task_id=%s AND r.created_at>=%s AND r.created_at<%s''', (row['id'], start, end))
    events = store.query('''SELECT occurred_at,event FROM qd_agent_risk_events WHERE task_id=%s
        AND occurred_at>=%s AND occurred_at<%s ORDER BY occurred_at''', (row['id'], start, end))
    symbols = {}
    universe = {s['symbol'] for s in scores} | set(row['config'].get('symbols', []))
    for run in runs:
        universe.update(((run['evidence'] or {}).get('task_config') or row['config']).get('symbols', []))
    for symbol in sorted(universe):
        selected = [s for s in scores if s['symbol'] == symbol]
        related = [r for r in runs if symbol in (((r['evidence'] or {}).get('task_config') or row['config']).get('symbols', []))
                   or any(i['symbol'] == symbol for i in (r['result'] or {}).get('items', []))]
        symbols[symbol] = {'decisions': len({s['run_id'] for s in selected}), 'horizons': summarize(selected),
            'triggers': {kind: sum(r['event_key'].split(':')[0] == kind for r in related)
                         for kind in {r['event_key'].split(':')[0] for r in related}},
            'risk_events': [e for e in events if e['event'].get('reasons') or symbol in e['event'].get('stop_loss_symbols', [])],
            'latency': latency.summarize(related)}
    triggers = {}
    for run in runs:
        key = run['event_key'].split(':')[0]
        triggers[key] = triggers.get(key, 0)+1
    samples = store.query('SELECT sampled_at,report FROM qd_agent_automation_samples WHERE task_id=%s AND sampled_at>=%s AND sampled_at<%s ORDER BY sampled_at', (row['id'], start, end))
    observed = [s for s in samples if s['report'].get('equity') is not None]
    observed_pnl = float(observed[-1]['report']['equity'])-float(observed[0]['report']['equity']) if len(observed)>1 else None
    return {'day': start.date().isoformat(), 'timezone': str(HK), 'task_id': row['id'],
        'decisions': sum('items' in (r['result'] or {}) for r in runs),
        'decisions_without_reliable_timestamp': sum('items' in (r['result'] or {}) and not decision_timestamp(r['result']) for r in runs),
        'triggers': triggers, 'triggered_runs': len(runs), 'symbols': symbols, 'risk_events': events,
        'horizons': summarize(scores),
        'latency': latency.summarize(runs),
        'observed_equity_change_hkd': observed_pnl,
        'observed_equity_window': [observed[0]['sampled_at'], observed[-1]['sampled_at']] if observed else [],
        'coverage': 'Gross hypothetical per-decision PnL; overlapping decisions are not portfolio profit. Fees/taxes excluded. Equity change spans recorded samples, not necessarily the full session. 5m/30m round up by less than one minute. WAIT has no direction hit. Missing lot size means unknown PnL. Missing bars remain pending.'}


def summarize(scores):
    result = {}
    for horizon in HORIZONS:
        group = [s for s in scores if s['horizon'] == horizon]
        values = [s['outcome'] for s in group if s['outcome']]
        hits = [v['direction_hit'] for v in values if v['direction_hit'] is not None]
        pnls = [v['would_be_pnl_hkd'] for v in values if v['would_be_pnl_hkd'] is not None]
        slippages = [v['slippage_bps'] for v in values if v['slippage_bps'] is not None]
        directional = [v['directional_return_pct'] for v in values if v['directional_return_pct'] is not None]
        result[horizon] = {'scored': len(values), 'pending_or_missing': len(group)-len(values),
            'hit_samples': len(hits), 'hit_rate': sum(hits)/len(hits) if hits else None,
            'avg_return_pct': sum(v['return_pct'] for v in values)/len(values) if values else None,
            'avg_directional_return_pct': sum(directional)/len(directional) if directional else None,
            'would_be_pnl_hkd': sum(pnls) if pnls else None, 'pnl_samples': len(pnls),
            'avg_slippage_bps': sum(slippages)/len(slippages) if slippages else None,
            'fill_samples': len(slippages)}
    return result
