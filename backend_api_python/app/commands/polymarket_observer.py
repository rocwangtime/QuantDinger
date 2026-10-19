"""Paper-only Polymarket observer process entrypoint."""

from __future__ import annotations

import os
import sys


def configure_allocator():
    """Opt-in glibc experiment must take effect before the interpreter starts."""
    value = os.getenv('POLYMARKET_OBSERVER_MALLOC_ARENA_MAX', '').strip()
    if not value:
        return
    if not value.isascii() or not value.isdigit() or not 1 <= int(value) <= 128:
        raise ValueError('POLYMARKET_OBSERVER_MALLOC_ARENA_MAX must be an integer from 1 to 128')
    if os.getenv('MALLOC_ARENA_MAX') != value:
        environment = dict(os.environ, MALLOC_ARENA_MAX=value)
        os.execve(sys.executable, [sys.executable, '-m', 'app.commands.polymarket_observer'], environment)


def enable_group_processes():
    """One feed process per hot group.

    Roll back with POLYMARKET_OBSERVER_PROCESS_PER_GROUP=0. Read process env and
    .env before the default: setdefault('1') first would stick, because
    load_dotenv(override=False) cannot replace a value already written.
    """
    try:
        from pathlib import Path

        from dotenv import load_dotenv
        backend_dir = Path(__file__).resolve().parents[2]
        load_dotenv(backend_dir / '.env', override=False)
        load_dotenv(backend_dir.parent / '.env', override=False)
    except Exception:
        pass
    os.environ.setdefault('POLYMARKET_OBSERVER_PROCESS_PER_GROUP', '1')


def main() -> None:
    configure_allocator()
    os.environ["QD_PROCESS_ROLE"] = "polymarket-observer"
    enable_group_processes()

    from app import create_app
    from app.runtime.process import ShutdownSignal
    from app.services.polymarket.observer import start, stop, snapshot
    from app.services.strategy_command_repository import StrategyCommandRepository
    from app.workers.trading import build_worker_id

    app = create_app(register_http_routes=False)
    shutdown = ShutdownSignal()
    shutdown.install()
    repository = StrategyCommandRepository()
    worker_id = build_worker_id()
    lease_key = "polymarket-observer-global-services"
    lease_seconds = max(10, int(os.getenv("POLYMARKET_OBSERVER_LEASE_SEC", "30")))
    leader = False
    observer_owned = False
    with app.app_context():
        try:
            while not shutdown.event.is_set():
                if not leader:
                    leader = repository.acquire_process_lease(
                        lease_key=lease_key,
                        owner_id=worker_id,
                        lease_seconds=lease_seconds,
                    )
                    if leader:
                        observer_owned = True
                        start()
                else:
                    leader = repository.renew_process_lease(
                        lease_key=lease_key,
                        owner_id=worker_id,
                        lease_seconds=lease_seconds,
                    )
                    if not leader:
                        break
                repository.record_worker_heartbeat(
                    worker_id=worker_id,
                    role="polymarket-observer",
                    metadata={"leader": leader, "polymarket_observer": snapshot()},
                )
                shutdown.event.wait(10)
        finally:
            if observer_owned:
                stop()
            if leader:
                repository.release_process_lease(lease_key=lease_key, owner_id=worker_id)
            repository.mark_worker_stopped(worker_id)


if __name__ == "__main__":
    main()
