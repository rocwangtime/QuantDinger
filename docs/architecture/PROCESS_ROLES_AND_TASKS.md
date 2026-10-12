# Backend Process Roles and Durable Tasks

The production deployment uses one backend image with independent process roles.

| Role | Command | Responsibility |
| --- | --- | --- |
| API | `gunicorn -c gunicorn_config.py run:app` | HTTP, authentication, validation, durable command submission |
| Migration | `python -m app.commands.migrate` | Fail-fast schema application before services start |
| Trading | `python -m app.commands.trading_worker` | Strategy runtimes, pending orders, grid fills, exchange connections |
| Scheduler | `python -m app.commands.scheduler` | Portfolio monitoring, deployment schedules, payment scans, signal alerts |
| Polymarket Observer | `python -m app.commands.polymarket_observer` | Public-market observation and paper simulation; dedicated lease and heartbeat |
| Celery Worker | `celery -A app.celery_app:celery_app worker` | AI, backtests, reports, and maintenance jobs |
| Celery Beat | `celery -A app.celery_app:celery_app beat` | Periodic maintenance dispatch |

## Ownership rules

- HTTP processes never start trading or scheduler threads.
- Strategy start, stop, restart, and reconcile requests use `qd_strategy_commands`.
- Trading workers claim commands with PostgreSQL `SKIP LOCKED` semantics.
- A strategy runtime requires a renewable row in `qd_strategy_runtime_leases`.
- Fencing tokens increase when an expired runtime is taken over by another worker.
- Global exchange pollers and scheduler loops use `qd_process_leases` leader ownership.
- Worker health is recorded in `qd_worker_heartbeats`.

## Celery boundary

Celery owns finite jobs that can be serialized, retried, and observed independently:

- fast AI analysis;
- agent backtests;
- reflection and AI calibration;
- market catalog synchronization;
- runtime metadata cleanup.

Celery must not own long-lived strategy loops, exchange polling, broker sessions, or grid runtime state. Those remain in the trading process because they require renewable ownership, reconciliation, and controlled shutdown.

## Redis separation

The cache Redis instance may use an eviction policy. Celery uses `redis-jobs`, which enables AOF persistence and `noeviction`. Queue state must never share an evictable Redis memory policy.

## Deployment sequence

Docker Compose enforces this order:

1. PostgreSQL and both Redis instances become healthy.
2. The migration process exits successfully.
3. API, trading, scheduler, Polymarket Observer, Celery Worker, and Celery Beat start.
4. Health checks require fresh worker heartbeats.

Use these endpoints for operations:

- `/api/health` for liveness;
- `/api/health/ready` for PostgreSQL and Celery Broker readiness;
- `/api/health/workers` for trading, scheduler, polymarket-observer, and Celery heartbeat summaries.

## HK fast decision capacity (P0 item 2)

Scheduler Agent automation reserves one `AgentFastDecision` thread for non-preview
`HKStock` price-trigger runs, from snapshot/model analysis through immediate execution
and later planned execution. The existing two `AgentDecision` threads serve other
runs. Each lane has a separate admission count and SQL query before `LIMIT`; bulk
backlogs cannot consume the fast slot or hide eligible fast runs. Context preparation,
protection and reconciliation retain their existing pools. Fast capacity is bounded:
additional HK runs can wait and expire under the existing 15-second deadline.
Freshness, revision/generation checks, pause/cancel, authorization, lot validation and
SIMULATE gates remain in the existing runner/order path.

Polymarket observer now runs separately with lease
`polymarket-observer-global-services` and role `polymarket-observer` in
`qd_worker_heartbeats`. Its heartbeat metadata includes the existing observer snapshot;
`/api/health/workers` aggregates this role without changing the API schema. Container
health uses `python -m app.commands.worker_health polymarket-observer`. Observer
configuration, including `POLYMARKET_OBSERVER_ENABLED=false` by default, public feeds,
paper accounting and shutdown flushing, is unchanged. A healthy process heartbeat
alone does not prove feed connectivity; inspect observer status as before.

Deploy all roles from the same pinned backend image. Base and GHCR Compose now start
`polymarket-observer`; production and Futu image overlays include it. Futu scheduler
DB pool maximum is 12 for the additional decision concurrency. Start the new service
when updating existing installations that use explicit service lists. No schema
migration is added by this item. Scheduler observer hosting defaults off via
`POLYMARKET_OBSERVER_IN_SCHEDULER=false`, explicitly pinned by Compose. For a legacy
single-process fallback only, stop the dedicated observer before enabling this flag;
never run both owners. Updating requires restarting scheduler and starting observer.
No deployment or gate enabling is part of this change.

This isolates execution capacity; it does not establish a real-model P95 guarantee.
Measure price receipt to valid decision/skip with long research runs occupying bulk
capacity during SIMULATE acceptance. Model latency, shared account locks, DB capacity,
context preparation and multiple concurrent HK triggers can still affect latency.

Local validation for this item (2026-10-12): 388 selected tests passed on PostgreSQL
18.6 using only the disposable `qd_automation_test` database. This includes Agent,
fast-slot saturation/admission, process ownership, Polymarket unit/integration and
release gates. Ruff over app/scripts/tests, dependency lock, backend structure,
documentation and Compose YAML/wiring checks passed. Docker was unavailable, so
`docker compose config` and container startup were not validated. No real-model,
OpenD, server, deployment or live execution tests were performed. Existing dependency
deprecation warnings remain.
