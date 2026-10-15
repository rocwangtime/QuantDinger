"""Offline US shortlist safety and persisted forward recovery."""
from datetime import datetime, timedelta, timezone
from unittest.mock import Mock
import pytest
from app.services.automation import domain, forward_scoring as forward, runner, scoring, shortlist, store
from tests.test_agent_automations import config
from tests.test_agent_automations_db import db, create, run_for  # noqa: F401

NOW = datetime(2026, 10, 15, 12, tzinfo=timezone.utc)


@pytest.mark.parametrize('changes', [
    {'execution_mode': 'paper_auto'}, {'execution_mode': 'PAPER_AUTO'},
    {'execution_mode': 'live'}, {'kind': 'price_trigger'}, {'market': 'HKStock'},
    {'universe_source': 'other'}, {'top_n': 21}, {'top_n': 1.5},
    {'universe_max_age_days': -1}, {'universe_source': None},
])
def test_config_rejects_unsafe_source(changes):
    with pytest.raises(ValueError):
        domain.normalize_config({**config(), 'universe_source': 'screener:top_n', **changes})


def test_defaults_and_resolved_whitelist():
    c = domain.normalize_config({**config(), 'universe_source': 'screener:top_n'})
    assert (c['top_n'], c['universe_max_age_days']) == (10, 4)
    c['symbols'] = ['MSFT']
    evidence = {'instruments': {'MSFT': {'price': 100}, 'AAPL': {'price': 100}}}
    item = dict(symbol='AAPL', action='BUY', target_weight=.1, min_price=99, max_price=101)
    with pytest.raises(ValueError, match='universe'):
        domain.parse_decision(store.dumps({'items': [item]}), evidence, c)
    item['symbol'] = 'MSFT'
    assert domain.parse_decision(store.dumps({'items': [item]}), evidence, c)['items'][0]['symbol'] == 'MSFT'


def insert_screen(day, status='completed'):
    r = store.query('''INSERT INTO qd_us_screener_runs(as_of,run_key,params,status,finished_at)
        VALUES(%s,%s,'{}',%s,%s) RETURNING id''',
        (day, f'{day}:{status}', status, NOW-timedelta(hours=1)), one=True)
    for rank, name in enumerate(('MSFT', 'NVDA'), 1):
        store.query('''INSERT INTO qd_us_screener_symbols(run_id,symbol,features,rank,included)
            VALUES(%s,%s,'{}',%s,TRUE) RETURNING run_id''', (r['id'], name, rank))
    return r['id']


@pytest.mark.integration
def test_resolution_latest_completed_and_stale_partial(db):
    c = config(universe_source='screener:top_n', top_n=1)
    insert_screen('2026-10-10')
    assert shortlist.resolve(c, NOW) is None
    insert_screen('2026-10-14', 'partial')
    insert_screen('2026-10-15', 'failed')
    assert shortlist.resolve(c, NOW) is None
    good = insert_screen('2026-10-13')
    assert shortlist.resolve(c, NOW) == {'run_id': good, 'as_of': '2026-10-13', 'symbols': ['MSFT']}
    latest = insert_screen('2026-10-14')
    assert shortlist.resolve(c, NOW)['run_id'] == latest
    store.query("UPDATE qd_us_screener_symbols SET exclusion_reasons='[\"market_data_failed\"]'::jsonb WHERE run_id=%s AND symbol='MSFT' RETURNING symbol", (latest,))
    assert shortlist.resolve(c, NOW)['symbols'] == ['NVDA']
    c['universe_max_age_days'] = 0
    assert shortlist.resolve(c, NOW) is None


def forbidden(*args, **kwargs):
    pytest.fail('Broker/order function must never be called')


@pytest.mark.integration
@pytest.mark.parametrize('mode', ['paper_auto', 'live', 'PAPER_AUTO'])
def test_runtime_rejects_before_market_or_orders(db, monkeypatch, mode):
    row = create()
    row['config'].update(universe_source='screener:top_n', execution_mode=mode)
    run = run_for(row)
    monkeypatch.setattr(runner, 'build_evidence', forbidden)
    monkeypatch.setattr('app.services.agent_trade_intents.submit_intent', forbidden)
    monkeypatch.setattr('app.services.futu_agent_execution._load_client', forbidden)
    runner.analyze(row, run)
    saved = store.query('SELECT * FROM qd_agent_automation_runs WHERE id=%s', (run['id'],), one=True)
    assert saved['status'] == 'failed' and 'plan_only' in saved['phase']
    with pytest.raises(ValueError, match='plan_only'):
        runner.execute(row, run)
    with pytest.raises(ValueError):
        store.edit(row['user_id'], row['id'], row['name'], row['config'])


@pytest.mark.integration
def test_missing_shortlist_skips_without_decisions_or_mutations(db, monkeypatch):
    row = create(universe_source='screener:top_n')
    run = run_for(row)
    monkeypatch.setattr(runner, 'build_evidence', forbidden)
    monkeypatch.setattr('app.services.agent_trade_intents.submit_intent', forbidden)
    runner.analyze(row, run)
    saved = store.query('SELECT * FROM qd_agent_automation_runs WHERE id=%s', (run['id'],), one=True)
    assert saved['result']['skip_reason'] == 'no_fresh_completed_us_screener_shortlist'
    assert 'items' not in saved['result']
    assert store.task(row['user_id'], row['id']) == row
    assert store.query('SELECT * FROM qd_agent_trade_intents') == []


def bars(a, b):
    return [{'time_key': '2026-10-14 00:00:00', 'close': a},
            {'time_key': '2026-10-15 00:00:00', 'close': b}]


@pytest.mark.parametrize('action,direction', [('BUY', 1), ('HOLD', 1), ('REDUCE', -1), ('EXIT', -1), ('WAIT', 0)])
def test_forward_math_and_missing_bars(action, direction):
    score = {'symbol': 'MSFT', 'due_at': datetime(2026, 10, 15, 20, tzinfo=timezone.utc),
             'inputs': {'anchor_day': '2026-10-14', 'shortlist': ['MSFT', 'NVDA'], 'action': action}}
    series = {'MSFT': bars(100, 110), 'NVDA': bars(100, 104), 'SPY': bars(100, 102), 'QQQ': bars(100, 103)}
    value = forward.outcome(score, series)
    assert value['baseline_return_pct'] == pytest.approx({'shortlist': 7, 'SPY': 2, 'QQQ': 3})
    assert value['excess_return_pct'] == pytest.approx({'shortlist': 10*direction-7, 'SPY': 10*direction-2, 'QQQ': 10*direction-3})
    series['QQQ'] = []
    assert forward.outcome(score, series) is None


def test_trading_day_horizons_skip_weekend():
    anchor, targets = forward.schedule(datetime(2026, 10, 16, 22, tzinfo=timezone.utc))
    assert anchor == '2026-10-16'
    assert targets['1d'].date().isoformat() == '2026-10-19'
    assert targets['5d'].date().isoformat() == '2026-10-23'


@pytest.mark.integration
def test_forward_recovery_maturity_idempotency_and_daily_metrics(db, monkeypatch):
    row = create(universe_source='screener:top_n')
    screen = insert_screen('2026-10-14')
    run = run_for(row)
    item = dict(symbol='MSFT', action='BUY', target_weight=.2)
    evidence = {'task_config': {**row['config'], 'symbols': ['MSFT', 'NVDA']},
                'screener': {'run_id': screen, 'symbols': ['MSFT', 'NVDA']},
                'previous_plan': {'items': [dict(item, target_weight=.1)]}}
    result = {'decision_at': NOW.timestamp(), 'items': [item]}
    store.update_run(run['id'], evidence=evidence, result=result, status='completed', finished_at=NOW)
    store.query('UPDATE qd_agent_automation_runs SET preview=FALSE,created_at=%s WHERE id=%s RETURNING id', (NOW, run['id']))
    monkeypatch.setattr('app.services.agent_trade_intents.submit_intent', forbidden)
    monkeypatch.setattr('app.services.futu_agent_execution._load_client', forbidden)
    monkeypatch.setattr(scoring, 'fetch_bars', forbidden)
    provider = Mock()
    changes = {'MSFT': 110, 'NVDA': 104, 'SPY': 102, 'QQQ': 103}
    provider.bars.side_effect = lambda symbol, day, **kwargs: bars(100, changes[symbol])
    factory = Mock(return_value=provider)
    monkeypatch.setattr('app.services.us_screener.data.FutuData', factory)
    scoring.tick(NOW)
    factory.assert_not_called()
    scores = store.query('SELECT * FROM qd_agent_decision_scores WHERE run_id=%s', (run['id'],))
    assert len(scores) == 2 and all(s['outcome'] is None for s in scores)
    scoring.tick(NOW.replace(hour=22))
    scores = store.query('SELECT * FROM qd_agent_decision_scores WHERE run_id=%s ORDER BY horizon', (run['id'],))
    assert scores[0]['outcome']['excess_return_pct']['SPY'] == pytest.approx(8)
    assert scores[1]['outcome'] is None
    report = scoring.read_report(row, '2026-10-15')['report']['forward_scoring']
    assert report['screener_run_ids'] == [screen] and report['turnover'] == 1
    assert report['horizons']['1d']['scored'] == 1 and report['horizons']['5d']['pending'] == 1
    assert report['horizons']['1d']['baselines']['shortlist']['hit_rate'] == 1
    assert report['horizons']['1d']['baselines']['SPY']['mean_excess_return_pct'] == pytest.approx(8)
    scoring.tick(NOW.replace(hour=23))
    assert len(store.query('SELECT * FROM qd_agent_decision_scores WHERE run_id=%s', (run['id'],))) == 2
    assert store.task(row['user_id'], row['id']) == row
    assert store.query('SELECT * FROM qd_agent_trade_intents') == []
    from app.services.automation import review
    assert review.report(row['user_id'], row['id'], 90)['daily_forward_reports'][0]['forward_scoring']['screener_run_ids'] == [screen]
    with pytest.raises(ValueError):
        forward.rescore(900002, run['id'])
    assert forward.rescore(row['user_id'], run['id']) == {'reset_scores': 2}
    def corrected(symbol, day, **kwargs):
        return bars(100, changes[symbol]+1) + [{'time_key': '2026-10-21 00:00:00', 'close': changes[symbol]+2}]
    provider.bars.side_effect = corrected
    scoring.tick(datetime(2026, 10, 21, 22, tzinfo=timezone.utc))
    rescored = store.query('SELECT * FROM qd_agent_decision_scores WHERE run_id=%s ORDER BY horizon', (run['id'],))
    assert all(s['outcome'] is not None for s in rescored)
    assert rescored[0]['inputs'] == scores[0]['inputs']
    assert rescored[0]['outcome']['return_pct'] == pytest.approx(11)
    assert rescored[1]['outcome']['return_pct'] == pytest.approx(12)
    report = scoring.read_report(row, '2026-10-15')['report']['forward_scoring']
    assert report['scored_decisions'] == 1 and report['pending_decisions'] == 0


@pytest.mark.integration
def test_runner_resolves_run_local_symbols_and_never_executes(db, monkeypatch):
    import time
    from types import SimpleNamespace
    row = create(universe_source='screener:top_n')
    screen = insert_screen('2026-10-14')
    run = run_for(row)
    monkeypatch.setattr(runner, 'utcnow', lambda: NOW)
    def evidence(resolved_row, **kwargs):
        assert resolved_row['config']['symbols'] == ['MSFT', 'NVDA']
        assert row['config']['symbols'] == ['AAPL']
        return {'as_of': time.time(), 'account': {'as_of': time.time()},
                'instruments': {'MSFT': {'price': 100}, 'NVDA': {'price': 100}}}
    monkeypatch.setattr(runner, 'build_evidence', evidence)
    class Model:
        provider = SimpleNamespace(value='openai')
        last_provider = 'openai'
        last_model = 'fixture'
        last_usage = {'prompt_tokens': 5, 'completion_tokens': 5}
        def __init__(self, **kwargs): pass
        def stream_llm_api_cancellable(self, *args, **kwargs):
            yield '{"items":[{"symbol":"MSFT","action":"BUY","target_weight":0.1,"min_price":99,"max_price":101}]}'
    monkeypatch.setattr('app.services.llm.LLMService', Model)
    monkeypatch.setattr(runner, 'execute', forbidden)
    monkeypatch.setattr('app.services.agent_trade_intents.submit_intent', forbidden)
    monkeypatch.setattr('app.services.futu_agent_execution._load_client', forbidden)
    runner.analyze(row, run)
    saved = store.query('SELECT * FROM qd_agent_automation_runs WHERE id=%s', (run['id'],), one=True)
    assert saved['status'] == 'completed'
    assert saved['evidence']['screener']['run_id'] == screen
    assert saved['evidence']['task_config']['symbols'] == ['MSFT', 'NVDA']
    assert store.task(row['user_id'], row['id']) == row
    assert store.query('SELECT * FROM qd_agent_trade_intents') == []


@pytest.mark.integration
def test_save_rejects_before_account_scope(db, monkeypatch):
    monkeypatch.setattr('app.services.agent_trade_intents.account_scope', forbidden)
    with pytest.raises(ValueError, match='plan_only'):
        store.create(900001, 'invalid', config(universe_source='screener:top_n', execution_mode='paper_auto'))


def test_provider_allowlist_and_rate_budget():
    from app.services.us_screener.data import FutuData
    provider = FutuData.__new__(FutuData)
    provider.context = Mock()
    provider.last_call = 0
    provider.deadline = 0
    with pytest.raises(ValueError, match='read_only'):
        provider.call('place_order')
    with pytest.raises(TimeoutError, match='runtime_budget'):
        provider.call('request_history_kline')
    assert provider.context.mock_calls == []


def test_quote_only_evidence_has_no_broker_account_calls(monkeypatch):
    from app.services.automation import market
    c = config(universe_source='screener:top_n', symbols=['MSFT'])
    provider = Mock()
    provider.quotes.return_value = ({'MSFT': {'last_price': '100', 'update_time': datetime.now(scoring.US).strftime('%Y-%m-%d %H:%M:%S')}}, {})
    provider.bars.return_value = bars(100, 110)
    monkeypatch.setattr('app.services.us_screener.data.FutuData', lambda *_: provider)
    monkeypatch.setattr(market, 'account_snapshot', forbidden)
    monkeypatch.setattr('app.services.futu_agent_execution._load_client', forbidden)
    monkeypatch.setattr('app.services.agent_trade_intents.submit_intent', forbidden)
    monkeypatch.setattr('app.services.research_workflow.market_clock', lambda *_: {})
    result = market.build_evidence({'config': c}, news=False)
    assert result['account']['funds']['cash'] == c['budget']
    assert result['account']['positions'] == []
    assert result['instruments']['MSFT']['price'] == 100
    provider.close.assert_called_once()


def test_forward_cache_requires_exact_anchor_and_target(monkeypatch):
    import sys
    from types import SimpleNamespace
    monkeypatch.setitem(sys.modules, 'futu', SimpleNamespace(KLType=SimpleNamespace(K_DAY='K_DAY'), AuType=SimpleNamespace(QFQ='QFQ')))
    from datetime import date
    import pandas as pd
    from app.services.us_screener.data import FutuData
    query = Mock(return_value={'bars': [{'time_key': '2026-10-14', 'close': 100}]})
    monkeypatch.setattr('app.services.us_screener.store.query', query)
    provider = FutuData.__new__(FutuData)
    raw = [dict(b, high=b['close'], low=b['close'], volume=1000) for b in bars(100, 110)]
    provider.call = Mock(return_value=(0, pd.DataFrame(raw), None))
    result = provider.bars('MSFT', date(2026, 10, 15), required_days=('2026-10-14', '2026-10-15'))
    assert len(result) == 2
    assert provider.call.call_args.args[0] == 'request_history_kline'
    assert 'DO UPDATE' in query.call_args.args[0]
    query.return_value = {'bars': result}
    provider.call.reset_mock()
    assert provider.bars('MSFT', date(2026, 10, 15), required_days=('2026-10-14', '2026-10-15')) == result
    provider.call.assert_not_called()
