# Polymarket public-data and paper arbitrage lab

The `/polymarket` frontend workspace observes standard YES/NO binary markets and
tests buying balanced complete sets from actual public order-book depth. This
release has no wallet credentials, signing, live order submission, funding,
or onchain transactions. Futu and the existing virtual strategy accounts are
independent of these experiments.

## Workflow

1. Configure matched shares, independent virtual cash, and minimum net edge.
   Leave market IDs blank to examine up to 100 high-volume catalogue entries,
   or enter up to eight numeric Gamma market IDs.
2. Record 2–30 seconds of REST book samples at intervals of 500–5000 ms.
   At most eight supported markets and the first 50 price levels per side are
   recorded. This is bounded observation, not a continuous trading bot.
3. Inspect acquisition cost, market-specific taker fees, net estimates and
   rejection reasons. Archived quotes are marked; they cannot be used as an
   execution price.
4. Run a paper experiment. The worker refreshes the market identity, fees and
   constraints and obtains a new signal. It captures additional books after
   the configured first-leg delay, inter-leg delay and compensation delay.
5. Inspect simulated fills, merges, cash changes and any remaining exposure.
   Download the frozen evidence or replay the paper result without network reads.

## Financial model and constraints

Only exact YES/NO labels, explicit standard-market metadata (`negRisk=false`),
active order books and known fees/constraints are accepted. CTF/v1 token IDs and
protocol-v2 position IDs are selected according to the market's protocol version;
all books must match the same condition and the corresponding outcome ID.
Negative-risk, augmented negative-risk and team-name outcomes are excluded.

The supported fee curve is the published exponent-1 curve:
`shares × feeRate × price × (1 − price)`. Fees are modeled in collateral and
rounded conservatively upwards per consumed level. Unknown fee flags or other
fee exponents reject the market. No rebates are credited. The current Gamma
minimum-order constraint is treated conservatively as minimum notional.

Estimated net profit is matched collateral payout minus depth-weighted purchase
cost, taker fees, configured merge cost and a profit reserve. Basis points are
measured against the matched payout, not portfolio capital. Buy price caps are
rounded down to the market tick; they never exceed the configured price buffer.
Entry also rejects when the worst permitted prices consume the minimum net edge.

FOK applies to each leg separately. FAK may partially fill. The second leg targets
the actual first-leg quantity; only the smaller balanced quantity is merged.
Missing, stale, crossed or mismatched books never fall back to a reference price.
The modeled merge is a complete-set accounting operation, not proof of a real
onchain merge. Configured merge cost is an explicit assumption, not a live gas quote.

Residual exposure receives at most one compensation attempt against a later
observed bid book, bounded by the configured loss limit including exit fees.
Unfilled or expensive compensation leaves `needs_review` exposure. Realized P&L
excludes the cost basis of unsold inventory; cash change and residual cost basis
are reported separately. Only closed independent paper experiments contribute
to the frontend closed-P&L statistic. Replays and exposed experiments are excluded.
These statistics are not a portfolio equity curve or evidence of risk-free returns.

REST polling can miss brief opportunities and cannot prove queue priority or
live fill quality. Repeated experiments can reuse observed liquidity; they are
independent experiments, not executions from a shared funded account. First/last
profitable observations do not imply a continuously available opportunity.

## Persistence and HTTP API

Authenticated jobs reuse `qd_agent_jobs` and the existing Celery `jobs` queue,
with the bounded thread-pool fallback when `CELERY_TASKS_ENABLED=false`.
Kinds are `polymarket_scan`, `polymarket_paper` and `polymarket_replay`.
No additional database migration or external credentials are required.

- `POST /api/polymarket/scans`: market IDs, recording window and paper settings.
- `POST /api/polymarket/paper-runs`: owned completed `scanJobId`, `marketId`, settings.
- `GET /api/polymarket/jobs?kind=polymarket_paper`: latest experiments including failures.
- `GET /api/polymarket/jobs/{job_id}`: progress and result without full frozen depth.
- `POST /api/polymarket/jobs/{job_id}/cancel`: cooperative cancellation.
- `POST /api/polymarket/jobs/{job_id}/replay`: replay an owned completed paper experiment.
- `GET /api/polymarket/jobs/{job_id}/evidence`: hash-verified frozen observations.

Submission/replay requires `Idempotency-Key` (1–120 characters). Identical retries
recover the same job; different parameters conflict. Human-job idempotency is
serialized in PostgreSQL and scoped to user, kind and key. Concurrent admission
allows at most two active Polymarket jobs per user. Cancelled rows cannot be
overwritten by late worker completion. Worker loss can leave an unfinished job;
cancel that job and start a new recording rather than inventing missing evidence.

Frozen bundles contain constraints, fees, timestamps, depth, settings and an
engine version and implementation-source SHA-256, with a canonical bundle SHA-256.
Replay verifies ownership, job kind,
completion, bundle integrity and engine compatibility. History queries omit the
depth bundle in SQL to avoid loading large recordings for a list.

Live execution is a separate future stage requiring eligible accounts/regions,
wallet signing, order/chain reconciliation, funded-account reservations and
explicit operational limits. Public-data access does not establish trading eligibility.

## Release validation (2026-10-07)

- Model, HTTP/OpenAPI, durable jobs and disposable PostgreSQL integration checks:
  58 passed. Includes concurrent retry/admission, owner isolation, cancellation,
  fee/price constraints, one-leg failure, bounded unwind and frozen replay.
- Frontend: 409 unit tests passed, targeted ESLint passed, production build passed.
- Full backend suite: 3640 passed, 31 skipped, 6 existing failures. All six are
  `test_worker_never_restores_blocked_spot_sell_quantity` cases in
  `test_spot_close_balance_guard.py`; the same six failures reproduce on the
  pre-change commit because the tests patch a removed `build_live_order_context`
  symbol. Those unrelated execution files were not changed by this release.
- Public API and browser smoke checks use actual depth. A scan with no qualifying
  net edge and its rejected paper run are valid results; no profitable fills are
  fabricated to demonstrate the feature.

## References checked for this implementation

- [Market details and fee metadata](https://docs.polymarket.com/market-data/market-details)
- [Market discovery and versioned outcome identifiers](https://docs.polymarket.com/market-data/discover-markets)
- [Order types and independent batch outcomes](https://docs.polymarket.com/trading/place-orders)
- [Position splitting and merging](https://docs.polymarket.com/trading/positions/manage)
- [Geographic restrictions](https://docs.polymarket.com/api-reference/geoblock)

## Eligibility probe and continuous observer

`GET /api/polymarket/eligibility` performs a read-only request to Polymarket's
public geoblock endpoint. The API reports a masked IP only. A blocked response
maps to `blocked`; documented close-only jurisdictions map to `close_only`;
other valid unblocked responses map to `eligible`. Network, timeout, malformed,
and non-200 results map to `unknown`. The endpoint itself has no close-only
field, so close-only comes from Polymarket's published jurisdiction list and may
change. Cache TTL defaults to 15 minutes; manual refresh is rate-limited. Any
future live-entry code must call `require_live_eligibility()` and proceed only
when it returns true. The frontend `canEnableLive()` has the same fail-closed
rule. This probe is informational and never enables trading; the feature stays
paper-only.

The optional observer is disabled by default and starts only in the dedicated `polymarket-observer`
leader process. It discovers standard YES/NO, non-negative-risk markets using
the existing engine validation and fee metadata, then maintains public books
from the documented CLOB market WebSocket using the already locked
`websocket-client` library. It sends application-level PING frames every ten
seconds, subscribes with `assets_ids` and `type: market`, consumes `book` and
`price_change`, and reconnects with backoff. When WebSocket is disabled or
unavailable it uses bounded REST book snapshots. REST fallback has a default
10-second interval. Depth-weighted asks, engine fee calculation, merge-cost
assumption, and net edge are recorded as aggregate opportunity episodes and
hourly near-miss aggregates. Episodes are retained for 30 days by default and
near misses for seven days. This is sampled public data; gaps, API limits, and
latency mean it is not a promise of executable or atomic arbitrage. Crypto Up/Down observation is a separate default-off option described below.
Bounded scans and paper experiments continue to accept YES/NO markets only.

Configuration: `POLYMARKET_OBSERVER_ENABLED`, `POLYMARKET_OBSERVER_MARKET_COUNT`,
`POLYMARKET_OBSERVER_SIZE_SHARES`, `POLYMARKET_OBSERVER_MIN_EDGE`,
`POLYMARKET_OBSERVER_NEAR_MISS_BPS`, `POLYMARKET_OBSERVER_REFRESH_SECONDS`,
`POLYMARKET_OBSERVER_REST_POLL_SECONDS`, `POLYMARKET_OBSERVER_RETENTION_DAYS`,
`POLYMARKET_OBSERVER_NEAR_MISS_RETENTION_DAYS` (default 7 days),
`POLYMARKET_OBSERVER_STALE_GRACE_SECONDS` (default 60 seconds),
`POLYMARKET_OBSERVER_WS_ENABLED`, `POLYMARKET_OBSERVER_MERGE_COST`, and
`POLYMARKET_GEOBLOCK_TTL_SECONDS`. Status, summary, and paginated episodes are
available under authenticated `/api/polymarket/observer/...` endpoints.


## Recurring crypto Up/Down research observer

`POLYMARKET_OBSERVER_UPDOWN_ENABLED=false` is a separate opt-in; the observer
itself also defaults off. With this option off, discovery, quotes and retention
follow the existing YES/NO path. No wallet, credentials, signing, submissions or
onchain actions are introduced.

Live Gamma data checked on 2026-10-11 confirms exact `Up`/`Down` outcomes, two
`clobTokenIds`, `negRisk=false`, `enableOrderBook=true`, `acceptingOrders=true`,
`feesEnabled=true`, and `feeSchedule={rate:0.07, exponent:1, takerOnly:true}`.
The series recurrence identifies 5m, 15m, hourly, 4h and daily windows. BTC, ETH
and XRP expose all five; SOL exposes 5m, 15m and 4h (hourly/daily series absent
at inspection). Short windows now resolve using Chainlink asset/USD 60-second
TWAP streams; inspected hourly/daily markets instead cite Binance asset/USDT.
`eventStartTime` is the window start; `startDate` is the catalogue listing date.
The active BTC 15m events catalogue lists future windows roughly 24 hours ahead
(the embedded series event list itself is truncated). Explicit fee schedules
are used; legacy `makerBaseFee`/`takerBaseFee=1000` are not interpreted as the
current curve rate. Missing, unsupported or conflicting fee schedules reject.

The [official fee page](https://docs.polymarket.com/trading/fees) documents
`shares × feeRate × p × (1-p)` and the crypto rate 0.07. It matches the existing
exponent-1 engine; no alternate guessed crypto curve is introduced. A 100-share
trade at 0.5 costs 1.75 in fees. The engine retains conservative upward rounding
to five decimals per consumed level, which can exceed the documented nearest
rounding; rebates remain excluded.

Discovery resolves exact asset/interval series IDs once per observer process,
then requests current/future events from each verified series. Window duration,
complete-set metadata and fees are validated for every market. Daily windows
allow 23/25 hours for daylight-saving transitions. Resolved/closed or expired
markets are excluded, upcoming starts are bounded by lookahead, and conditions
are deduplicated. Absent series are retried at process restart. Discovery refresh sends incremental subscribe/unsubscribe frames on the open
public WebSocket only when the token set changes, preserves retained depth,
and prunes removed-market books and per-market caches. YES/NO discovery retains its own
existing cadence. Expired Up/Down samples are rejected even between refreshes.
REST fallback remains available.

Episodes and hourly near misses add `market_type`, nullable `asset`/`interval`,
and an `expiry_bucket` measured at each observation. Existing rows default to
`yes_no`, null asset/interval and an empty bucket. Up/Down episodes split at
bucket transitions; near-miss primary keys include a bounded deterministic tag
key in addition to market/hour. Buckets are fixed absolute remaining time:
`>10m` means at least 600 seconds, `5-10m` at least 300, `2-5m` at least 120,
`1-2m` at least 60, `30-60s` at least 30, `<30s` below 30. Longer intervals spend
most of their duration in the first bucket; this is intentional for comparisons.
Episodes/near misses keep their existing configurable retention.

The separate bounded sample table records best Up/Down asks, each token's
bid/ask midpoint (null if a bid is unavailable), market end/expiry tags, net edge,
and underlying reference value/source/time. Public Binance spot asset/USDT
prices come from `api.binance.com/api/v3/ticker/price`, with
`data-api.binance.vision` fallback. A dedicated worker caches prices per asset,
including failed reads, and performs no HTTP in book callbacks. Stale or failed
references are null and never replace order books or become a trading signal.
These are USDT spot observations, not Chainlink USD/TWAP resolution prices.
No numeric price-to-beat was observed; an explicit market `priceToBeat`, if
supplied, is stored without inferring it from spot. Optional Chainlink collection
is deferred: the [official RTDS migration guide](https://docs.polymarket.com/migrate/rtds-to-polybolt)
deprecates the public price topics and requires authentication for replacement
PolyBolt reference-price channels. Under this task's no-credentials constraint,
no price-stream subscription is enabled and `chainlink_price` remains null.
Samples are event-driven, at most once per configured period per market, rather
than guaranteed regular time-series observations. At the default 5-second rate,
40-market cap allows at most 691,200 rows/day and 4,838,400 rows over seven days;
26 continuously updating subscriptions allow at most 449,280 rows/day.

Additive registered migration: `20261011_polymarket_updown.sql`. It uses
`ADD COLUMN IF NOT EXISTS`, preserves legacy rows and replaces the near-miss
primary key transactionally. It is safe to reapply. Apply through the normal
migration command before running upgraded observer/API code.

| Environment variable | Default | Bounds / meaning |
| --- | --- | --- |
| `POLYMARKET_OBSERVER_UPDOWN_ENABLED` | false | Separate paper research opt-in |
| `POLYMARKET_OBSERVER_UPDOWN_ASSETS` | BTC,ETH,SOL,XRP | Allowlisted assets |
| `POLYMARKET_OBSERVER_UPDOWN_INTERVALS` | 5m,15m,1h,4h,1d | Allowlisted intervals; absent series skipped |
| `POLYMARKET_OBSERVER_UPDOWN_LOOKAHEAD_SECONDS` | 1800 | 0–86400; future start horizon |
| `POLYMARKET_OBSERVER_UPDOWN_REFRESH_SECONDS` | 60 | 10–300; discovery cadence; incremental subscriptions |
| `POLYMARKET_OBSERVER_UPDOWN_MAX_MARKETS` | 40 | 1–100; Up/Down cap in addition to YES/NO |
| `POLYMARKET_OBSERVER_UPDOWN_PER_PAIR_LIMIT` | 2 | 1–10; earliest windows per asset/interval |
| `POLYMARKET_OBSERVER_UPDOWN_SAMPLE_SECONDS` | 5 | 1–60; research sample write interval |
| `POLYMARKET_OBSERVER_UPDOWN_SAMPLE_RETENTION_DAYS` | 7 | 1–365; ten-minute cleanup cadence |
| `POLYMARKET_OBSERVER_REFERENCE_CACHE_SECONDS` | 5 | 2–60; shared per-asset cache/worker cadence |

Authenticated observer summary/episodes accept validated `market_type`, `asset`,
`interval` and `expiry_bucket` filters. Summary includes bounded `groups` with
episodes, near-miss samples and best net edge by these tags; observer status
includes current Up/Down subscriptions (ID, asset, interval, end time) and bounded
rejection reasons. The frontend provides filters and grouped statistics in both
English and Simplified Chinese. No public submission capability is added.


## WebSocket receive isolation and bounded writer (2026-10-11)

The market receive thread only parses JSON, applies dictionary book snapshots or
price deltas, marks markets dirty through an asset-to-market index, and updates
counters. It performs no database/HTTP work or opportunity calculations. A
separate evaluator copies dirty books under a short lock every 250 ms and runs
existing depth/fee calculations outside that lock. Markets remain dirty until
their one-second episode/near-miss throttle permits evaluation. Research keeps
its independent `POLYMARKET_OBSERVER_UPDOWN_SAMPLE_SECONDS` throttle; reference
prices come only from the existing worker's cached `ReferencePrices.get()`.

Observation functions record parameterized SQL in an evaluator-local recording
context; direct callers retain synchronous persistence, while scheduler REST
fallback uses the same ordered queue as WS evaluation. The
writer commits ordered transactions, using the PostgreSQL adapter's batched
`executemany` for adjacent identical statements. Episode end/start and expiry
bucket transitions stay indivisible and FIFO, and near-miss upserts retain the
maximum edge and exact accepted observation counts. Failed writer batches stay
in flight ahead of later records and retry; they are never silently discarded.

The bounded FIFO holds atomic observation groups (usually one or two SQL
operations). When full, queued research for the same market is replaced with
the latest value; otherwise oldest queued research is evicted first. New
research can be dropped if only protected episode/near-miss operations remain.
Only the evaluator or discovery worker can wait for protected queue capacity;
the receive thread never accesses the write queue. Pending books coalesce
naturally in memory while evaluation waits. Queue counters `enqueued`/`written`
count SQL operations, while `dropped`/`coalesced` and depth/high-water count
groups. Written operations include updates that affect zero rows and are not a
count of newly inserted rows. A batch respects the configured SQL operation
limit except when a single indivisible transition group exceeds that limit.

Current [Polymarket real-time documentation](https://docs.polymarket.com/market-data/realtime-data)
explicitly supports `{"assets_ids":[...],"operation":"subscribe"}` and
`unsubscribe` on an existing market connection. Unchanged subscriptions cause
no wire update; removals and additions are sent in two bounded frames. Four connections are now the default, with independent receive threads,
subscription sets and ten-second application `PING` keepalives. A local
single-socket smoke still hit one slow-consumer close after receive isolation,
so sharding was selected from that evidence. Unused custom-feature events
are no longer requested. `WS_SHARDS=1` uses one connection; initial placement at 2 separates YES/NO and
Up/Down, while 3–8 use stable CRC32 market-ID assignment. Measured token rates
subsequently determine placement as described below. Reconnects affect only
the failed shard and clear that shard's depth. REST fallback remains available.

Status uses the existing JSON status row and `polymarket-observer` heartbeat, with cumulative
WebSocket messages/rate, age, connections, reconnects, close reason counters,
slow-consumer count, writer queue depth/high-water, operation/drop/coalesce
counts, oldest pending/in-flight lag, last batch time, last write error, evaluator
cycle time and final `flush_complete`. Messages/sec is cumulative since pipeline
start. Subscription and rejection details are capped at 20 entries each; total
`markets_subscribed` remains accurate. websocket-client 1.9 can report close-frame
text through `on_error` and omit it from `on_close`; slow-consumer detection
therefore covers both forms. Stop closes the receive socket, joins workers and
flushes the writer under a shared timeout, then persists final status. A timeout
reports `stopping`/`flush_complete=false`, preserving pending work for retry;
restart is refused while previous workers remain alive. The queue is in-memory:
process termination or a permanent DB outage beyond shutdown cannot guarantee
durable transitions. Inspect flush/error metrics before terminating the worker.

| New environment variable | Default | Bounds / meaning |
| --- | --- | --- |
| `POLYMARKET_OBSERVER_WS_SHARDS` | 4 | 1–8; initial market assignment, then measured token-rate balancing |
| `POLYMARKET_OBSERVER_EVALUATE_MS` | 250 | 50–5000; evaluator cadence |
| `POLYMARKET_OBSERVER_WRITE_QUEUE_MAX` | 10000 | 1–100000; atomic queued groups |
| `POLYMARKET_OBSERVER_WRITE_BATCH_SIZE` | 500 | 1–5000; SQL operations per transaction |
| `POLYMARKET_OBSERVER_WRITE_BATCH_MS` | 1000 | 10–10000; writer idle/retry wait in ms; enqueue wakes writer |

Local-only reproducible probes: run `scripts/polymarket_ws_probe.py load` or
`live --seconds 150` with the approved `QD_TEST_POSTGRES_DSN`. Each uses a
disposable schema, deletes all probe rows in `finally`, and broadens near-miss
threshold to 10000 bps solely to exercise persistence. No wallet, signing,
orders, credentials, additional dependencies or HTTP schema changes are added.

## Continuous paper arbitrage account (2026-10-11)

`POLYMARKET_PAPER_ACCOUNT_ENABLED=false` is a separate default-off gate. Both
this flag and `POLYMARKET_OBSERVER_ENABLED` must be enabled in polymarket-observer.
The independent bounded experiments remain available. The new account is a
single shared virtual account (`account_id=global`), visible to authenticated
users, with no connection to funded strategy accounts. Defaults are 5,000 virtual
collateral units and a 1,000-unit trade cap. No additional dependencies, wallet,
authenticated exchange requests, signing, submissions or chain calls are used.

The evaluator sends bounded book signals to a 256-item paper inbox without
waiting. A dedicated account thread owns state, timed simulation and atomic
PostgreSQL writes. A separate public-read thread measures RTT and checks residual
resolution, so network timeouts do not delay simulated leg scheduling. A dedicated
PostgreSQL session advisory lock prevents two account workers from writing the
global account simultaneously. Database failures pause admissions and retry the
same in-memory transition; the receive threads never perform database work.
Status persists every ten seconds and after transitions. Inbox overflow counts
as dropped signals, never synthetic fills. At most 100 pending/held trades are
allowed; unresolved exposure counts toward this bound.

### Exact sizing and portfolio controls

Sizing walks the union of both tokens' cumulative ask-depth boundaries (top 50
levels). Every matched marginal tranche must retain the configured minimum net
edge after the existing upward-rounded per-level engine fees and its allocation
of the configured fixed merge cost. It stops at the first failing tranche; later
cheap depth cannot subsidize an expensive intermediate tranche. The accepted
full depth determines the signal net edge, measured against matched payout.

The ticket's collateral spend, including both entry fees and merge cost, is the
minimum of eligible depth cost, the trade cap, 20% of current free capital, and
the edge-scaled cap. The latter is `min_ticket` at `min_edge_bps`, increases
linearly to `trade_cap` at `full_edge_bps` (default 100), and clamps at both ends.
Available portfolio deployment also caps it at 60% of current equity. Pending
reservations and the original cost basis of zero-marked residual inventory both
count toward this deployment limit. Size rounds down to `market.sizeStep` when
available, otherwise the configured size step; buy price limits round down to
market tick. Each leg must meet Gamma's minimum order notional, following the
existing engine convention. Worst permitted prices and a 0.001-unit fee rounding
allowance must fit all cash limits and retain the threshold, or size decreases
again / entry is skipped. All cap inputs, the binding cap and rejection decision
are frozen in evidence.

A UTC day's first account tick freezes start-of-day equity. When conservative
equity falls 2% below it, a durable daily-loss-stop event disables new entries for
that UTC day, including when later gains recover the loss. Existing timed exits
and settlements continue. The next UTC day resets the gate. Each attempted
market is reserved against concurrent attempts; the 60-second cooldown is
refreshed after completion or rejection. Repeated signals during cooldown count
as opportunities seen, not additional attempts. Books with no qualifying net
edge do not create trades. Expired markets cannot start new attempts.

### Delayed fill and capital model

The receive path retains timestamped full-book events and deltas in bounded
per-token rings (10 seconds / 2,000 events by default). Pruned events advance a
base book; reconstruction, sorting and fee arithmetic run outside receive.
Removed markets and reconnected shards clear their history. At each step the
worker reconstructs the last book observed at or before the planned arrival
instant. Missing, crossed, too-old or evicted observations cannot fill; future
books cannot replace the required observation. Signal books use this same
historical lookup rather than attributing a new timestamp to cached depth.

RTT is measured with unauthenticated `GET https://clob.polymarket.com/time`,
every five minutes by default, retaining 60 samples. Status exposes median, p90,
measurement time/error, fallback state and delay used. A failed/stale measurement
uses the configured 150 ms fallback. One-way simulated delay is the ceiling of
median RTT multiplied by `latency_multiplier` (default 1, intentionally
conservative). That frozen delay applies to signal→leg1, leg1→leg2, and failed
leg2→unwind. Scheduling does not sleep in evaluator or receive callbacks.

Both entry legs are individually FOK within their planned worst ask plus the
configured absolute per-share slippage allowance (default zero), rounded down
to tick. Competitors remove 50% of the top level on each side before sweeping;
subsequent levels retain all depth by default, with a separate configurable
share. The same assumption applies to the unwind bid sweep. First-leg failure
releases the entire reservation. Second-leg failure triggers one FAK sell of the
first token against later observed bids, charging the same engine taker fees.
Partial unwind retains original proportional cost basis separately and marks
remaining inventory at zero for equity and loss-stop decisions. This deliberately
conservative mark is not a prediction of the winning outcome.

Successful complete sets lock actual acquisition cost, fees and assumed merge
cost until the configured 60-second settlement delay. Payout and realized P&L
are released together. With merging disabled, complete sets remain locked until
`endDate`, an explicit simulation assumption about settlement timing. Residual
inventory is not paid merely because `endDate` passed: public Gamma evidence
must have matching condition, `closed=true`, `umaResolutionStatus=resolved` and
exact binary 0/1 outcome prices. Unknown/disputed/proposed results remain zero
marked and held. The finality field is present in Polymarket's
[official Gamma market model](https://github.com/Polymarket/py-sdk/blob/main/src/polymarket/models/gamma/market.py).
No winner is inferred from reference spot prices. Daily net P&L reflects
conservative equity changes, including residual write-downs; trade realized P&L
excludes unresolved cost basis until actual simulated release. `free + locked +
positions_value == equity` always holds; residual zero marks have a separate
`residual_cost_basis`. Restart cancels a pending first entry, or conservatively
accounts for a previously observed first leg without inventing a missed hedge
or unwind. Historical books are not persisted as a live feed.

### Storage, replay, reports and UI

Registered additive migration: `20261011_polymarket_paper_account.sql`, bootstrap
component `20261011_polymarket_paper_account`. Tables are
`qd_polymarket_paper_accounts`, `qd_polymarket_paper_trades`,
`qd_polymarket_paper_legs`, `qd_polymarket_paper_ledger`, and
`qd_polymarket_paper_daily`. Account/transition/trade/leg/report writes commit as
one transaction. Leg details include requested/filled shares, average prices,
fees and planned arrival / latency timestamps. Trade evidence is zlib-compressed
canonical JSON with SHA-256, model version and model/fee-engine source hash.
Evidence contains signal and per-step top 50 books for both sides of both tokens,
market metadata, sizing inputs, configuration, RTT observations and decision.
Replay verifies its digest and model compatibility, recomputes sizing and fills,
and checks the saved result, including eventual release evidence. No public
reads occur during replay.

Trade, ledger and daily history have configurable 90-day retention. Evidence for
pending or held capital is exempt until closed so it remains auditable. The live
account state retains at most 100 recent event records; ledger snapshots retain
those events durably within the retention window. Loss-stop events are included
in ledger event details. Ledger snapshots are produced at least every ten
seconds while healthy, plus transitions. Reports use UTC days, attempts,
complete-set fills / fill rate, single-leg failures, unwinds, fees, conservative
net P&L, absolute equity max drawdown, average/max locked-capital utilization,
opportunities seen versus attempted, and market-type groups. Average utilization
samples the lightweight account ticks. Detail / exports remain paper labelled.

Authenticated endpoints under `/api/polymarket/paper-account`:

- `GET /status`: balances, positions / cost basis, today's gate/P&L, measured latency.
- `GET /trades`: `status`, `market_type`, UTC `day`, `limit` (1–100), `offset`.
- `GET /trades/{id}`: decoded digest-verified evidence and simulated result.
- `POST /trades/{id}/replay`: immediate offline sizing/fill comparison; no new trade.
- `GET /daily-report`: UTC `day` or last `days` (1–90).
- `GET /export`: same report window, `format=csv|json`, attachment download.

The `/#/polymarket` panel displays paper balance cards, latency, loss stop,
paginated UTC daily reports and filtered trades, evidence drilldown, replay and
CSV/JSON report downloads in en-US and zh-CN. OpenAPI is regenerated.

Every account setting uses the prefix `POLYMARKET_PAPER_ACCOUNT_`; full defaults
are in `backend_api_python/env.example`:

| Suffix | Default |
| --- | --- |
| `ENABLED` | false |
| `INITIAL_CAPITAL`, `TRADE_CAP` | 5000, 1000 |
| `FREE_FRACTION`, `LOCKED_FRACTION`, `LOSS_FRACTION` | 0.20, 0.60, 0.02 |
| `MIN_EDGE_BPS`, `FULL_EDGE_BPS`, `MIN_TICKET` | 10, 100, 10 |
| `SIZE_STEP`, `SLIPPAGE` | 0.01, 0 (absolute price allowance) |
| `COMPETITOR_TOP`, `COMPETITOR_OTHER` | 0.50, 0 |
| `MERGE_COST`, `MERGE_ENABLED`, `SETTLEMENT_SECONDS` | 0.02, true, 60 |
| `COOLDOWN_SECONDS` | 60 |
| `DEFAULT_LATENCY_MS`, `LATENCY_MULTIPLIER`, `LATENCY_REFRESH_SECONDS` | 150, 1, 300 |
| `MAX_BOOK_AGE_MS`, `HISTORY_SECONDS`, `HISTORY_EVENTS` | 2000, 10, 2000 |
| `RETENTION_DAYS` | 90 |

### Observer process isolation follow-up

HK Agent P0 item 2 now implements this process split with
`python -m app.commands.polymarket_observer` and a Compose `polymarket-observer`
service. It owns a dedicated renewable leader lease, publishes observer snapshot
metadata under its own worker heartbeat, and drains through the existing `stop()`
on shutdown or lease loss. Scheduler hosting defaults off and Compose pins
`POLYMARKET_OBSERVER_IN_SCHEDULER=false`. Paper-only behavior and account exclusion
remain unchanged. See [process roles and deployment notes](../architecture/PROCESS_ROLES_AND_TASKS.md).

Further paper research can retain versioned replay implementations and tune competitor shares / history bounds
using measured feed rates; no live execution is part of this account.

Validation for this account release: 43 new account unit/PG tests passed; the
combined Polymarket/scheduler checks passed 188 tests, including 12 PG18
integration tests. The requested full backend selection passed 3,845 tests with
62 deselected; frontend passed 417 tests, production build and changed-file
ESLint. Global Ruff and OpenAPI export passed. The final three-minute isolated
local PG/public-feed smoke observed 83,387 messages, RTT median 107.1 ms / p90
369.7 ms, zero qualifying opportunities or attempts, no worker/writer errors,
one remote-close reconnect and no slow-consumer closes. All smoke rows were
removed. Full commands, file manifest, assumptions and logs are recorded in
`deploy/polymarket_paper_account_notes.md` in the development workspace.


## Measured WS shard balancing and daily health (2026-10-13)

The observer remains public, read-only and paper-only. Each valid incoming frame
counts once for each distinct token named in it, including price-change frames
before a book exists. Only the current owner contributes to that token's EWMA;
brief handover overlap does not inflate its rate. Physical per-shard frame totals
exclude `PONG` and include overlap traffic. Receive callbacks update only memory.

Every `POLYMARKET_OBSERVER_WS_REBALANCE_SECONDS` (default 60), measured counts are
converted to rates using elapsed monotonic time and an EWMA with
`POLYMARKET_OBSERVER_WS_RATE_HALF_LIFE_SECONDS` (default 30). Idle tokens decay.
Largest-first greedy bins balance individual asset/token subscriptions; YES/NO
outcomes may use different shards after a rebalance. The total discovered token
set is preserved. Equivalent bins are matched to existing shard owners to
maximize retained subscriptions, then only a prefix of moves needed to enter
the hysteresis band is applied. This is a churn-reducing heuristic, not a global
minimum-move optimizer.

Imbalance is `(max shard rate - min shard rate) / mean rate`, or zero for an idle
feed. A move requires both current imbalance and its reduction to exceed
`POLYMARKET_OBSERVER_WS_REBALANCE_HYSTERESIS` (default 0.25). Successful rebalances
must be at least `POLYMARKET_OBSERVER_WS_REBALANCE_MIN_SECONDS` apart (default
120). Reconnects request a fresh check as soon as all shards are connected,
bypassing the periodic check deadline but preserving hysteresis and cooldown.
A single indivisibly hot token can leave residual imbalance without causing
repeated reshuffles. Status exposes `ws_shard_loads` (EWMA token frames/sec),
`ws_load_imbalance`, `ws_rebalance_hysteresis` and
`ws_last_rebalance_monotonic` (process-local clock; null before the first move).

All destination subscribe frames are sent before source unsubscribe frames.
A destination send failure aborts source removals. Healthy moves preserve book
and paper history caches; reconnects still invalidate the failed shard's books.
The public stream has no subscription acknowledgement barrier here: send order
reduces gaps where feasible, but cannot guarantee gap-free delivery after a
remote/network failure. Brief duplicate events remain possible during handover.

Migration `20261013_polymarket_ws_daily.sql` is additive, repeatable and registered
in the bootstrap invoked by `python -m app.commands.migrate`. It stores cumulative
UTC-day checkpoints by process session in `qd_polymarket_ws_daily`. The ten-second
status persistence and final shutdown status upsert reconnect, slow-consumer,
rebalance counts and `shard_msg_totals`. Retries overwrite a session checkpoint,
so an ambiguous commit cannot double count; restarted processes use new session
IDs and reports sum their counts. Failed writes retain dirty days for retry.
A hard kill can lose counters since the latest checkpoint; this is not a durable
per-message log. WS daily rows currently have no automatic retention policy.

The existing authenticated `/api/polymarket/paper-account/daily-report` and JSON
or CSV export include `ws_stats` with those four fields, aggregated across
sessions. Days with WS activity but no paper-account row are returned with `day`
and `ws_stats` only; economic/account fields remain absent. CSV serializes
`ws_stats` as JSON. No account enablement is required to collect WS health stats.

Deploy notes: run migrations before updating API/observer, set the shard count
and optional four tuning variables in the observer environment, then restart the
dedicated observer using the existing lease/drain lifecycle. Keep scheduler
hosting disabled. Inspect per-shard load, imbalance, slow-consumer counts and
writer health after restart. No wallet, credentials, signing or orders are added.

Local validation: PostgreSQL 18.6, existing `backend_api_python/.venv`, with
`DATABASE_URL`, `AUTOMATION_TEST_DATABASE_URL` and `QD_TEST_POSTGRES_DSN` all set
to the approved localhost `qd_automation_test` database. From the backend root:

```bash
.venv/bin/python -m pytest tests/test_polymarket*.py tests/integration/test_polymarket*.py tests/test_process_roles.py tests/test_scheduler_sql.py -q
.venv/bin/python -m pytest tests/test_db_bootstrap.py -q
```

Results: 211 and 10 passed respectively; 15 existing `datetime.utcnow()`
deprecation warnings. Changed Python modules/tests passed Ruff; whitespace checks,
OpenAPI export and local migration bootstrap passed. Coverage includes EWMA decay,
LPT load reduction, subscription preservation, hysteresis/cooldown, reconnect
checks, destination failure, subscribe-before-unsubscribe ordering, UTC rollover,
retry retention and idempotent PG upsert/report aggregation across sessions.
No public-feed soak or server deployment was performed for this change.
