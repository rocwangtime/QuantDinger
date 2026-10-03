from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from app.services.research_workflow import explicit_strategy_creation, market_clock, normalize_research_config, research_gate


def utc(value):
    return datetime.fromisoformat(value).replace(tzinfo=timezone.utc)


@pytest.mark.parametrize('message', [
    '请为美股 SPCX 生成可以保存并回测的完整固定规则策略，日线5/20均线，不要编造结果，不要下单',
    'Write a backtestable strategy for SPCX. Do not place orders.',
])
def test_explicit_artifacts_are_not_general_chat(message):
    assert explicit_strategy_creation(message)


@pytest.mark.parametrize('message', ['SPCX 的均线策略有什么风险？', '如何生成可回测的策略？', '为什么生成策略代码失败了', '先不要生成策略代码，解释原理', "Don't write strategy code yet"])
def test_advice_and_negations_do_not_create_code(message):
    assert not explicit_strategy_creation(message)


def test_weekend_holiday_dst_and_hk_lunch():
    clock = market_clock('USStock', utc('2026-10-03T11:00:00'))
    assert clock['available'] and not clock['is_open']
    assert clock['next_open'] == '2026-10-05T09:30:00-04:00'
    assert not market_clock('USStock', utc('2026-12-25T16:00:00'))['is_open']
    assert market_clock('USStock', utc('2026-11-02T15:00:00'))['is_open']
    assert not market_clock('HKStock', utc('2026-10-05T04:30:00'))['is_open']


def test_regular_session_and_after_close_gates():
    config = normalize_research_config({'market': 'USStock', 'symbol': 'SPCX', 'session_window': 'regular'})
    assert not research_gate(config, now=utc('2026-10-03T11:00:00'))['allowed']
    assert research_gate(config, now=utc('2026-10-02T15:00:00'))['allowed']
    config['session_window'] = 'after_close'
    assert research_gate(config, now=utc('2026-10-02T20:30:00'))['allowed']
    assert not research_gate(config, now=utc('2026-10-03T20:30:00'))['allowed']


def test_price_event_rejects_stale_unfinished_missing_and_nonfinite_data():
    now = utc('2026-10-02T15:00:00')
    config = normalize_research_config({'market': 'USStock', 'symbol': 'SPCX', 'trigger': {'type': 'price_above', 'price': 160}})
    for age, price in [(600, 170), (10, 170), (120, float('nan')), (120, 159)]:
        assert not research_gate(config, now=now, candles=[{'time': now.timestamp() - age, 'close': price}])['allowed']
    assert research_gate(config, now=now, candles=[{'time': now.timestamp() - 120, 'close': 161}])['allowed']
    config['trigger']['type'] = 'price_below'
    assert research_gate(config, now=now, candles=[{'time': now.timestamp() - 120, 'close': 159}])['allowed']


@pytest.mark.parametrize('config', [{'run_interval_minutes': -1}, {'trigger': {'type': 'execute_trade'}}, {'session_window': 'invalid'}, {'market': 'USStock', 'symbol': 'SPCX', 'trigger': {'type': 'price_above', 'price': 'NaN'}}])
def test_invalid_research_config_rejected(config):
    with pytest.raises((ValueError, TypeError)):
        normalize_research_config(config)


def test_research_brief_reaches_analysis_model(monkeypatch):
    from app.services import portfolio_monitor as monitor
    captured = {}
    def analyze(**kwargs):
        captured.update(kwargs)
        return {'decision': 'HOLD', 'summary': 'Waiting for criteria'}
    monkeypatch.setattr(monitor, 'get_fast_analysis_service', lambda: SimpleNamespace(analyze=analyze))
    result = monitor._analyze_single_position({'market': 'USStock', 'symbol': 'SPCX'}, 'zh-CN', 7, 'Only after volume confirmation')
    assert captured['research_brief'] == 'Only after volume confirmation'
    assert result['final_decision'] == 'HOLD'


def test_stock_four_hour_bars_do_not_merge_across_days():
    from app.data_sources.us_stock import USStockDataSource
    bars = []
    for day in ['2026-10-01', '2026-10-02']:
        for hour in range(13, 20):
            bars.append({'time': utc(f'{day}T{hour}:30:00').timestamp(), 'open': 1, 'high': 3, 'low': 1, 'close': 2, 'volume': 10})
    result = USStockDataSource._merge_stock_hours(bars)
    assert len(result) == 4
    assert [x['volume'] for x in result] == [40, 30, 40, 30]


def test_sell_outlooks_are_exit_leads_not_discarded():
    from app.services.research_opportunities import eligible_opportunities
    assert eligible_opportunities({'success': True, 'position_analyses': [{'market': 'USStock', 'symbol': 'SPCX', 'final_decision': 'SELL'}]}) == [('USStock', 'SPCX')]


def test_explicit_strategy_router_bypasses_llm_classifier(monkeypatch):
    from app.routes import ai_chat
    monkeypatch.setattr(ai_chat, 'LLMService', lambda: pytest.fail('Unnecessary model classification'))
    plan = ai_chat._classify_agent_intent('请为 SPCX 生成可回测的策略代码，不要下单', [], {'market': 'USStock', 'symbol': 'SPCX'}, 'zh-CN')
    assert plan['intent'] == 'strategy_build' and plan['should_execute']
    assert plan['workflow'] == 'script_strategy'


def test_unmet_monitor_gate_skips_billing_and_llm(monkeypatch):
    from contextlib import contextmanager
    from app.services import portfolio_monitor as monitor
    config = {'market': 'USStock', 'symbol': 'SPCX', 'session_window': 'regular'}
    class Cursor:
        def execute(self, *args): pass
        def close(self): pass
        def fetchone(self):
            return {'id': 5, 'user_id': 7, 'position_ids': '[1]', 'monitor_type': 'ai', 'config': config}
    @contextmanager
    def db():
        yield SimpleNamespace(cursor=Cursor)
    captured = []
    monkeypatch.setattr(monitor, 'get_db_connection', db)
    monkeypatch.setattr(monitor, '_get_positions_for_monitor', lambda *a, **k: [{'market': 'USStock', 'symbol': 'SPCX'}])
    monkeypatch.setattr(monitor, '_bump_monitor_schedule', lambda *a, **k: captured.append(a))
    monkeypatch.setattr(monitor, 'get_billing_service', lambda: pytest.fail('Gate must precede billing'))
    monkeypatch.setattr('app.services.research_workflow.market_clock', lambda *a, **k: {'available': True, 'is_open': False})
    result = monitor.run_single_monitor(5, user_id=7)
    assert result['skipped'] and captured


def test_generic_consensus_cannot_override_personalized_wait_conditions():
    from app.services.fast_analysis_policy import should_override_with_consensus
    assert should_override_with_consensus('BUY', 90, 15)
    assert not should_override_with_consensus('BUY', 90, 15, 'Wait for confirmed earnings release')


def test_single_python_artifact_is_extracted_without_narration():
    from app.routes.strategy import _strip_code_fence
    assert _strip_code_fence('Here is the repaired code:\n```python\ndef initialize(context):\n    pass\n```\nReview before running.') == 'def initialize(context):\n    pass'


def test_stock_authoring_does_not_inherit_generic_crypto_capabilities():
    from app.services.strategy_ai_capabilities import resolve_strategy_generation_intent
    intent = resolve_strategy_generation_intent(prompt='Target: USStock:SPCX\n日线，只做多，5日均线上穿20日均线买入，止损5%，不加杠杆', context={'market': 'USStock', 'symbol': 'SPCX'})
    assert 'crypto_swap' not in intent.capabilities
    assert 'protection' in intent.capabilities
