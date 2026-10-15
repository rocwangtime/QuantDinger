# Paper observer WS and maker inventory stability

Scope: local source inspection, offline tests and the supplied local PostgreSQL
only. Base origin/main 8b2d424. No server access, production profiling, credentials,
wallet/signing integration, real orders, merge or deployment.

## Receive-path findings

Read the observer entrypoint, observer/pipeline, history, recording, rate balancer,
paper account and maker paths, paper lab documentation, and the earlier reports
`/workspace/quantdinger/deploy/pm_book_rca_20261014.md` and
`/workspace/quantdinger/deploy/pm_book_leak_20261015.md`.

The user supplied 257 reconnects, 91 slow-consumer closes on 2026-10-14 UTC,
837/837/340/339 assets per shard, and median/p95 book ages of 11.7/40.5 seconds.
These observations were not independently re-queried.

Confirmed code bottleneck: websocket-client invokes on_message synchronously on
its receive thread. That callback decoded JSON in RateBalancer.receive, scanned
the full asset assignment to rebuild ownership, decoded the same JSON again in
Pipeline.receive, then acquired the shared pipeline lock. While holding it, the
callback filtered events, updated levels, staged recording and updated/replayed
paper history. The evaluator acquires the same lock to copy full depth for
recording and eligible markets. Thus slow processing and lock contention directly
stop socket draining. The threaded regression holds the book lock while delivering
ten frames and verifies callbacks continue, capacity stays bounded and the lost
generation cannot repopulate books.

Freshness/pair-skew gates and opportunity evaluation already run in the evaluator;
maker runs in the account worker, polling recorded SQL; observation SQL writes
run in the writer. Those are not synchronous callback DB writes. They still
compete for CPU/GIL and may indirectly contribute. The old rate balancer could
split paired outcomes and retained prior owners across discovery; it did not
provide an asset-count bound. It optimized measured token rates rather than
ensuring equal counts. Shard imbalance is evidence of uneven subscription counts,
not proof that message rates were equally imbalanced.

The production reports prove backlog/slow consumption, not a unique CPU hotspot.
Without a live profile/raw stream it is not possible to attribute all 257
reconnects to parsing versus history, lock holds, host/network or upstream delay.
The fix removes the demonstrated synchronous bottleneck and bounds local backlog;
it does not claim a measured production throughput improvement.

## Implementation

* Receive callbacks enqueue raw frames only. Each shard has an ordered worker;
  it decodes once and applies updates with original receipt timestamps.
* At capacity, discard the generation, not selected incremental price changes.
  The market loop closes the connection; the worker invalidates books, recording
  timestamps and paper history. More than two seconds in the queue also triggers
  invalidation. Recovery requires fresh snapshots; deltas alone cannot form a book.
  This intentionally loses observations during overload instead of fabricating a
  coherent book from a partial delta stream. Queue depth, drops and high-water
  are exposed per shard; drops accumulate across connection generations.
* Greedy whole-market placement balances asset count (two outcomes together).
  Balanced existing owners are retained to avoid catalogue-wide reconnect churn.
  Four shards remain the default minimum, a 600-asset soft target grows to at most
  eight. Refresh membership changes start a new connection generation to avoid
  the subscribe-before-ownership snapshot race. Legacy per-token rate moves are
  disabled; EWMA rates remain diagnostic.
* Reconnect delays are 5–120 seconds exponential with 0.5–1.5x jitter. A brief
  successful open does not reset the failure streak; a 60-second connection does.
  Existing REST fallback policy remains. Logs include shard IDs. Existing
  reconnect/slow-consumer counters and book-age p50/p95 are retained.
* Maker defaults change from 100 to 20 unpaired shares per market and 500 to 100
  globally. Quotes reserve worst-case global capacity. On a one-leg fill, the
  overfilled side pauses at fill admission as well as refreshed quotation. The
  underfilled side can still pair up to the imbalance even at/above reduced caps.
  Pairing cannot overshoot. Cash reservation remains proportional to active legs.
  No dollar-notional cap was added; the requested optional global restriction is
  the existing global share cap, now lower.

## Environment defaults

Documented in backend_api_python/env.example:

| Variable | Default |
| --- | --- |
| POLYMARKET_OBSERVER_WS_SHARDS | 4 minimum, range 1–8 |
| POLYMARKET_OBSERVER_WS_ASSETS_PER_SHARD | 600 soft target, max 8 shards |
| POLYMARKET_OBSERVER_WS_RECEIVE_QUEUE_MAX | 256 raw frames per shard |
| POLYMARKET_PAPER_MAKER_MARKET_CAP | 20 unpaired shares |
| POLYMARKET_PAPER_MAKER_GLOBAL_CAP | 100 unpaired shares |

The REBALANCE_* controls no longer move individual tokens. Existing
RATE_HALF_LIFE_SECONDS continues to control diagnostic rates.

## Validation

Final source validation, backend_api_python/.venv, with DATABASE_URL,
AUTOMATION_TEST_DATABASE_URL and QD_TEST_POSTGRES_DSN all set to
`postgresql://qd_test:qd_test@127.0.0.1:5432/qd_automation_test`:

* Full backend `pytest -q`: 4,294 passed, 24 skipped (59 warnings).
* Focused observer/updown, queue/balance and maker tests: 122 passed.
* `ruff check backend_api_python`: all checks passed.
* `git diff --check`: passed.

No public-feed soak or production performance measurement was run. Tests use
fake sockets, synthetic frames and local PostgreSQL, with no server operations.
