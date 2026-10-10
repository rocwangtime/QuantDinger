"""Readiness, loop health and observed-session report semantics."""
from datetime import datetime, timezone

import pytest

from app.services.automation import health, review
from tests.test_agent_automations import config


def test_agent_health_detects_stalled_and_failed_ticks(monkeypatch):
    monkeypatch.setattr(health.time, 'time', lambda: 1000)
    health.started()
    assert health.snapshot(1001)['status'] == 'running'
    assert health.snapshot(1016)['status'] == 'stalled'
    health.tick(False)
    assert health.snapshot(1001)['status'] == 'degraded'
    health.tick(True)
    assert health.snapshot(1001)['last_success_at'] == 1000
    health.stopped()
    assert health.snapshot(1001)['status'] == 'stopped'


def test_observed_sessions_use_exchange_dates_and_do_not_invent_single_mark_return():
    row = {'id': 1, 'name': 'fixture', 'revision': 1, 'active': False, 'config': config(), 'state': {}}
    stamps = [datetime.fromisoformat(s) for s in ['2026-10-05T14:00:00+00:00', '2026-10-05T15:00:00+00:00', '2026-10-06T01:00:00+00:00', '2026-10-06T14:00:00+00:00']]
    samples = [{'sampled_at': s, 'report': {'equity': value}} for s, value in zip(stamps, [1000, 1020, 1030, 990])]
    report = review.assemble(row, [], samples, [], 0, 2000, {'runs': False, 'samples': False, 'orders': False})
    assert len(report['observed_sessions']) == 2
    assert report['observed_sessions'][0]['day'] == '2026-10-05'
    assert report['observed_sessions'][0]['samples'] == 3
    assert report['observed_sessions'][0]['observed_change_pct'] == pytest.approx(3)
    assert report['observed_sessions'][1]['observed_change_pct'] is None
    assert report['coverage']['partial'] is False
    assert 'observed_marks_not_full_session_returns' in report['limitations']


def test_partial_report_preserves_usage_gaps_and_separates_protection():
    row = {'id': 1, 'name': 'fixture', 'revision': 1, 'active': False, 'config': config(), 'state': {}}
    usage = {'input_tokens': 10, 'output_tokens': 20, 'token_source': 'estimated', 'estimated_cost': None}
    runs = [
        {'status': 'completed', 'preview': True, 'has_decision': True, 'items': [{'action': 'BUY'}], 'usage': usage},
        {'status': 'failed', 'has_decision': False, 'calls': [{'usage': usage}]},
        {'status': 'planned', 'source': 'deterministic_protection', 'has_decision': True, 'items': [{'action': 'EXIT'}]},
    ]
    report = review.assemble(row, runs, [], [], 0, 2000, {'runs': True, 'samples': False, 'orders': False})
    assert report['coverage']['partial'] is True
    assert report['decisions']['proposed_actions'] == {'BUY': 1}
    assert report['decisions']['protective_runs'] == 1
    assert report['model_usage']['recorded_calls'] == report['model_usage']['estimated_token_calls'] == report['model_usage']['unpriced_calls'] == 2
    assert report['model_usage']['estimated_cost_by_currency'] == {}
    assert 'token_id' not in report['task']


def test_naive_database_timestamps_are_interpreted_as_utc():
    naive = datetime(2026, 10, 5, 14)
    assert review.timestamp(naive) == naive.replace(tzinfo=timezone.utc).timestamp()


@pytest.mark.parametrize('provider, configured, ready',
                         [('openai', True, True), ('openai', False, False), ('openrouter', True, False)])
def test_model_readiness_checks_supported_provider_without_calling_it(monkeypatch, provider, configured, ready):
    from types import SimpleNamespace
    from app.services.automation import readiness
    service = SimpleNamespace(provider=SimpleNamespace(value=provider),
                              get_default_model=lambda: 'fixture', is_configured=lambda: configured)
    monkeypatch.setattr(readiness, 'LLMService', lambda **_: service)
    monkeypatch.setattr(readiness, 'validate_selection', lambda _: {})
    result = readiness.model_state({})
    assert result['configured'] is ready and 'api_key' not in result
