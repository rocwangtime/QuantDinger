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

The optional observer is disabled by default and starts only in the scheduler
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
are deduplicated. Absent series are retried at process restart. Every discovery
refresh reconnects the public market WebSocket with the current token set,
clears old depth and prunes per-market caches. YES/NO discovery retains its own
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
| `POLYMARKET_OBSERVER_UPDOWN_REFRESH_SECONDS` | 60 | 10–300; discovery/reconnect cadence |
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
