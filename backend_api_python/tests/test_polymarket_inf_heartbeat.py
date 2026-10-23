"""Non-finite telemetry must never terminate the paper observer."""
import json
import math
from contextlib import nullcontext
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from app.utils.json_safe import sanitize_json


def strict_json(raw):
    def reject(value):
        raise AssertionError(value)
    return json.loads(raw, parse_constant=reject)


@pytest.mark.parametrize('target', ['heartbeat', 'status'])
def test_nonfinite_database_payload(monkeypatch, target):
    from app.services import strategy_command_repository as repo
    from app.services.polymarket import observer
    payload = {'nested': [math.inf, -math.inf, {'value': math.nan}], 'ok': 3}
    cursor = Mock()
    db = Mock(cursor=lambda: cursor)
    if target == 'heartbeat':
        monkeypatch.setattr(repo, 'get_db_connection', lambda: nullcontext(db))
        repo.StrategyCommandRepository().record_worker_heartbeat(worker_id='test', role='test', metadata=payload)
        raw = cursor.execute.call_args.args[1][2]
    else:
        monkeypatch.setattr(observer, 'get_db_connection', lambda: nullcontext(db))
        monkeypatch.setattr(observer, '_DAILY_STATS', Mock())
        monkeypatch.setattr(observer, 'snapshot', lambda: payload)
        observer._persist_status()
        raw = cursor.execute.call_args.args[1][0]
    assert strict_json(raw) == {'nested': [None, None, {'value': None}], 'ok': 3}
    assert math.isinf(payload['nested'][0])
    db.commit.assert_called_once()
    cursor.close.assert_called_once()


def test_status_failure_continues(monkeypatch, caplog):
    from app.services.polymarket import observer
    monkeypatch.setattr(observer, '_DAILY_STATS', Mock())
    monkeypatch.setattr(observer, 'get_db_connection', Mock(side_effect=RuntimeError('offline')))
    observer._persist_status()
    assert 'status persistence failed (RuntimeError)' in caplog.text


@pytest.mark.parametrize('zero_source', ['unowned', 'empty_assignment', 'first_quiet_sample', 'decay'])
def test_merge_snapshot_smoke(monkeypatch, zero_source):
    from app.services.polymarket import observer
    from app.services.polymarket.ws_balance import RateBalancer, ratio
    from app.services.polymarket.ws_group_process import merge_feed_status
    idle = RateBalancer(1)
    idle.set_groups({'a': 'quiet'}, now=idle.sample_at - 60)
    if zero_source != 'unowned':
        idle.assignment = {'a': 0}
    if zero_source == 'empty_assignment':
        idle.assignment = {}
    if zero_source == 'first_quiet_sample':
        idle.sample(idle.sample_at + 60)
    if zero_source == 'decay':
        idle.rates['quiet'] = 1.0
        idle.measured.add('quiet')
        idle.sample(idle.sample_at + 100000)
    assert idle.loads() == [0.0]
    busy = RateBalancer(1)
    busy.set_groups({'b': 'busy'})
    busy.assignment = {'b': 0}
    parts = [dict(key=key, shard_base=i, shard_count=1, status=b.metrics())
             for i, (key, b) in enumerate([('quiet', idle), ('busy', busy)])]
    merged = merge_feed_status(parts)
    assert merged['ws_load_ratio'] is None
    assert math.isinf(ratio(merged['ws_shard_loads']))  # planner semantics preserved
    json.dumps(merged, allow_nan=False)
    monkeypatch.setattr(observer, 'STATUS', merged)
    monkeypatch.setattr(observer, '_PIPELINE', None)
    monkeypatch.setattr(observer, '_REFERENCE', None)
    monkeypatch.setattr(observer, '_WS', None)
    assert strict_json(json.dumps(sanitize_json(observer.snapshot()), allow_nan=False))['ws_load_ratio'] is None


def test_heartbeat_failure_continues(monkeypatch, caplog):
    import app
    from app.commands import polymarket_observer as command
    from app.runtime import process
    from app.services import strategy_command_repository as repo
    from app.services.polymarket import observer
    event = Mock()
    event.is_set.side_effect = [False, False, True]
    repository = Mock()
    repository.acquire_process_lease.return_value = True
    repository.renew_process_lease.return_value = True
    repository.record_worker_heartbeat.side_effect = RuntimeError('offline')
    monkeypatch.setattr(command, 'configure_allocator', lambda: None)
    monkeypatch.setattr(command, 'enable_group_processes', lambda: None)
    monkeypatch.setattr(app, 'create_app', lambda **kw: SimpleNamespace(app_context=nullcontext))
    monkeypatch.setattr(process, 'ShutdownSignal', lambda: SimpleNamespace(event=event, install=lambda: None))
    monkeypatch.setattr(repo, 'StrategyCommandRepository', lambda: repository)
    for name in ('start', 'stop', 'snapshot'):
        monkeypatch.setattr(observer, name, Mock(return_value={}))
    command.main()
    assert repository.record_worker_heartbeat.call_count == 2
    repository.renew_process_lease.assert_called_once()
    observer.stop.assert_called_once()
    assert 'heartbeat persistence failed (RuntimeError)' in caplog.text


def test_outage_sample_preserves_existing_load():
    from app.services.polymarket.ws_balance import RateBalancer
    balancer = RateBalancer(1)
    balancer.set_groups({'a': 'hot'})
    balancer.assignment = {'a': 0}
    balancer.rates['hot'] = 100.0
    balancer.measured.add('hot')
    balancer.sample(balancer.sample_at + 60, unavailable={'hot'})
    assert balancer.loads() == [100.0]
