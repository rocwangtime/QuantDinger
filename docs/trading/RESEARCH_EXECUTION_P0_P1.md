# Research and execution reliability: P0/P1

This release extends the existing strategy, job, and trading workers. Desktop
controls are implemented in the separate QuantDinger-Vue repository, alongside
the authenticated human API and its OpenAPI specification.
No broker session, real order, or deployed application is certified by local
tests.

## Desktop controls

The desktop source is maintained in `rocwangtime/QuantDinger-Vue`. The backtest
center's evolution report shows promotion evidence, cumulative attempts,
holdout reuse, and a frozen replay action. The strategy editor supports AI entry
mode, an evidence job ID, and imported covariance models with explicit limits.

Strategy details expose shadow evaluation inside AI decision records, a
portfolio-risk analysis tab, and virtual order groups for signal-mode strategies.
Risk input files contain dated daily returns by asset and optional signed weights;
the resulting model can be downloaded and imported into deployment controls.
Virtual group recovery retains its ID by authenticated user and strategy. A
submission with an unknown response retains its request and idempotency key for
explicit retry after reopening the page. Backend records remain authoritative.

Use matching frontend and backend builds. No server deployment or database
migration is performed by these local code changes.

## Reproducible research

`POST /api/strategy-evolution/run` freezes the authorized script and parameter
schema before queuing. A worker captures complete input frames, including
fundamental columns, membership periods, venue product metadata, and instrument
rules. The compressed bundle is addressed by its content hash.

`POST /api/strategy-evolution/jobs/{jobId}/replay` reuses that job's frozen
request and bundle. Its body cannot override parameters or providers. A replay
fails if its owner, content hash, source, or execution-library fingerprint does
not match. Keep `EVOLUTION_BUNDLE_DIR` on storage shared by API and Celery;
the default `data/evolution_bundles` uses the existing Docker data volume.

Economic results replay against the same data. Research evidence can change:
replays are new studies, contribute to cumulative selection counts, and expose
an already-used holdout. They cannot restore that holdout's independence.

Reports now identify fixed-parameter window validation explicitly. This is not
rolling parameter refitting. DSR uses current attempts, including evaluated
pruned candidates, plus conservative prior reserved attempts for that user's
source ID. Separate copied sources are separate families. PBO uses date-aligned
daily equity returns and eight-partition CSCV, including pruned candidates;
missing dates are intersected and never filled. Early pruning may leave too few
common observations. Fold-return ranking remains a separate diagnostic.

Unavailable checks yield `insufficient_evidence`. Promotion requires adequate
return observations and closed trades, an unused holdout with at least 30 daily
returns, acceptable DSR/PBO, and positive stressed-cost results. These are
research screens, not calibrated profitability probabilities. Zero fee/slippage
assumptions or unavailable persistent research history cannot qualify.

New fundamental observations and corrections are archived in
`qd_fundamental_revisions`. `load_recorded_revision` separates system knowledge
time from public release time. Historical revisions before this migration are
unknown; the baseline is dated at migration time. Bundles preserve the exact
values used by a study, including any corrections present when it was captured.

For deployment, supply `researchEvidenceJobId` through `POST /api/strategies`
or `PUT /api/strategies/{id}`. The linked job must qualify and match the source,
typed parameters, runtime, capital, and leverage. A later study makes prior
evidence stale. Costs are carried into the runtime's evaluation assumptions.
Direct parameter patches cannot keep a mismatched evidence link.
`REQUIRE_RESEARCH_EVIDENCE_FOR_LIVE=true` makes this check mandatory for live
deployments; its default is false for compatibility. Existing deployments are
not retroactively recertified.

## AI entry policies and shadow evaluation

Deployment accepts `aiDecisionMode` and the existing `aiDecisionFilter` toggle.
Specifying a mode enables AI unless the toggle is explicitly false.

| Mode | Valid rejection | Provider/billing unavailable |
| --- | --- | --- |
| `shadow` | Record raw rejection; entry continues | Entry continues; unavailable audit |
| `advisory` | Reject entry | Legacy behavior: entry continues |
| `required` | Reject entry | Reject entry |

Exits bypass AI. Unsupported robot types have no valid AI decision and cannot
pass a required policy. Invalid confidence/probability numbers are rejected.
Required mode also rejects entries if the audit cannot be persisted; a failed
shadow audit is isolated with a savepoint so it cannot poison the order write.

Example deployment additions:

```json
{"aiDecisionFilter": true, "aiDecisionMode": "shadow"}
```

Queue a report with
`POST /api/strategies/{id}/ai-evaluation`:

```json
{"horizonHours": 24, "limit": 100}
```

Read or cancel it at `/api/strategies/ai-evaluation/jobs/{jobId}` and its
`/cancel` action. The report pairs baseline opportunity returns with the shadow
suggestion at a fixed hypothetical exit horizon. Only closed hourly bars are
observed; timestamps are interpreted as bar-open times. Frozen fee/slippage
assumptions and observed data snapshot IDs are included. Horizon gaps and
invalid suggestions remain missing. Blocked losses, missed winners, latency,
and model credits are reported separately.

These are observational counterfactuals, not actual execution gains or a
portfolio equity curve. Overlapping horizons are dependent, missing decisions
can bias the sample, and credits are not converted to dollars without a verified
rate. Confidence is not treated as a market event probability.

## Portfolio risk

`POST /api/portfolio/risk-analysis` accepts daily, dated returns and signed
capital weights in one valuation currency:

```json
{
  "returns": {
    "BTC/USDT": {"2026-09-01": 0.01, "2026-09-02": -0.02},
    "ETH/USDT": {"2026-09-01": 0.02, "2026-09-02": -0.03}
  },
  "weights": {"BTC/USDT": 0.5, "ETH/USDT": -0.5},
  "shrinkage": 0.2,
  "annualPeriods": 365
}
```

This abbreviated example needs at least 30 common daily observations to produce
a model. Output includes shrunk covariance, volatility contributions, empirical
95% VaR/expected shortfall, stress return, and gross/net exposure. Asset keys must
match canonical strategy position symbols, such as `BTC/USDT`.

Opt into entry enforcement by passing the returned `model` through deployment:

```json
{
  "portfolioRisk": {
    "portfolio_model": {"symbols": ["BTC/USDT"], "covariance": [[0.0004]], "as_of": "REPLACE_WITH_ACTUAL_DATA_DATE", "period": "daily"},
    "max_portfolio_daily_volatility": 0.03,
    "portfolio_model_max_age_hours": 96
  }
}
```

The gateway serializes admission by user/account/market type and checks the
projected book. Before fresh broker submission, the dispatcher rechecks current
policy. Unpriced positions, stale/invalid models, uncovered assets, and exceeded
limits fail closed. Active strategies sharing the guarded account must declare
the same valuation currency; unknown or mixed currencies reject because this
guard does not implement FX conversion. Pending entries reserve their full requested notional;
their risk uses a conservative triangle bound, so unfilled hedges do not reduce
exposure. The denominator is allocated strategy capital, not independently
verified account NAV. Reductions and existing broker-identity reconciliation
continue independently. The guard uses stored marks and model data; it fetches
no new history in the order path. Refresh the model through an explicit
deployment update. `portfolioRisk: {}` disables this opt-in guard.

## Virtual multi-leg coordination

Create a dedicated stopped signal-mode strategy with an empty virtual account,
no active executor/start command, and a manifest declaring the permitted legs.
`POST /api/portfolio/order-groups` requires `Idempotency-Key`:

```json
{
  "strategyId": 42,
  "maxGrossNotional": 500,
  "timeoutSeconds": 300,
  "legs": [
    {"symbol": "BTC/USDT", "action": "open_long", "quantity": 1, "referencePrice": 100},
    {"symbol": "ETH/USDT", "action": "open_short", "quantity": 2, "referencePrice": 50}
  ]
}
```

Reference prices above are illustrative simulation marks. Omit `strategyRunId`
to create an isolated `order_group` run without starting the strategy. An
explicit run must belong to the same user and strategy. Spot shorts reject.
Two to eight unique entry legs are supported, as market orders at supplied
marks with the existing virtual costs. This does not simulate order-book
liquidity or simultaneous cross-venue fills.

The pending-order worker advances one leg per group per tick. Ledger fills,
pending-order state, and group state commit together. `group_waiting` orders are
excluded from ordinary dispatch. Duplicate keys reuse the group; a changed
request conflicts. Startup, other signal orders, deployment editing, and
deletion are blocked while the strategy remains reserved.

Read `/api/portfolio/order-groups/{groupId}` for every leg and actual
group-attributed residual quantities/gross/net notionals. Failure or deadline
expiry cancels remaining work. Filled exposure becomes `needs_review`.

- `POST .../{groupId}/cancel` cancels unfilled work.
- `POST .../{groupId}/unwind` with
  `{"referencePrices":{"BTC/USDT":90}}` queues idempotent reductions.
  Failed compensations can be retried without repeating completed reductions.
- `POST .../{groupId}/resolve` with `{"reason":"manual closure reconciled"}`
  verifies the virtual account is flat and has no outstanding work before
  releasing its reservation. Prior exposure remains in the audit.

Foreign fills require manual review. There is no automatic real-account unwind
or new broker authority in this release.

## Verification and rollout

Apply migrations through the existing migration command before starting new
workers. API, trading worker, and Celery must use the same build and shared data
volume. CI includes the new PostgreSQL integration tests alongside transactional
execution accounting and the existing release gates.

With `QD_TEST_POSTGRES_DSN` pointed at a migrated disposable database:

```sh
python -m pytest tests/test_research_execution_policies.py tests/test_research_execution_api.py tests/integration/test_research_order_groups.py tests/release_gate -q
```

Integration cases cover concurrent admission, interrupted ledger writes,
restart, cancellation, incomplete fills, compensation retries, owner isolation,
holdout overlap, audit failure, and fundamental corrections. Existing broker
recovery tests remain separate from actual Futu SIMULATE or other venue
end-to-end acceptance.
