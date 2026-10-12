"""Observer ownership, heartbeat and shutdown without network/broker access."""
from contextlib import nullcontext
from types import SimpleNamespace

import pytest


@pytest.mark.parametrize('command, owns_observer', [('scheduler', False), ('polymarket_observer', True)])
@pytest.mark.parametrize('acquired', [True, False])
def test_process_boundaries_and_lease_loss(monkeypatch, command, owns_observer, acquired):
    import importlib
    import app
    from app.runtime import process
    from app.services import strategy_command_repository
    from app.services.polymarket import observer
    from app import startup
    from app.services.automation import worker

    calls = []
    beats = []

    class Event:
        ticks = 0

        def is_set(self):
            return self.ticks >= 2

        def wait(self, _):
            self.ticks += 1

    class Repository:
        def acquire_process_lease(self, **kw):
            calls.append(('acquire', kw['lease_key']))
            return acquired

        def renew_process_lease(self, **kw):
            calls.append(('renew', kw['lease_key']))
            return False

        def record_worker_heartbeat(self, **kw):
            beats.append(kw)

        def release_process_lease(self, **kw):
            calls.append(('release', kw['lease_key']))

        def mark_worker_stopped(self, _):
            calls.append(('stopped', None))

    monkeypatch.delenv('POLYMARKET_OBSERVER_IN_SCHEDULER', raising=False)
    monkeypatch.setattr(app, 'create_app', lambda **_: SimpleNamespace(app_context=nullcontext))
    monkeypatch.setattr(process, 'ShutdownSignal', lambda: SimpleNamespace(event=Event(), install=lambda: None))
    monkeypatch.setattr(strategy_command_repository, 'StrategyCommandRepository', Repository)
    monkeypatch.setattr(startup, '_start_scheduler_services', lambda: calls.append(('scheduler_start', None)))
    monkeypatch.setattr(worker, 'stop', lambda: calls.append(('agent_stop', None)))
    monkeypatch.setattr(observer, 'start', lambda: calls.append(('observer_start', None)))
    monkeypatch.setattr(observer, 'stop', lambda: calls.append(('observer_stop', None)))
    monkeypatch.setattr(observer, 'snapshot', lambda: {'running': True})
    importlib.import_module('app.commands.' + command).main()
    assert ('observer_start', None) in calls if owns_observer and acquired else ('observer_start', None) not in calls
    assert ('scheduler_start', None) in calls if not owns_observer and acquired else ('scheduler_start', None) not in calls
    assert ('observer_stop', None) in calls if owns_observer and acquired else ('observer_stop', None) not in calls
    assert calls[-1] == ('stopped', None)
    assert not any(kind == 'release' for kind, _ in calls)  # lost lease must not release a successor's lease
    assert beats[0]['role'] == ('polymarket-observer' if owns_observer else 'scheduler')
    assert beats[0]['metadata']['leader'] is acquired
    if owns_observer:
        assert beats[0]['metadata']['polymarket_observer'] == {'running': True}
