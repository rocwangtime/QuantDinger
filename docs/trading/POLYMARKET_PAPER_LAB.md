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
