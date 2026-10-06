# Agent paper portfolio dashboard

## Approved scope

The first forward-evaluation milestone extends existing Futu US/HK SIMULATE
tasks with a human task workspace, fill-based reports and model-independent
holding protection. The user approved implementation after the US paper order
flow was demonstrated. Tool-loop research and live execution are later milestones.

Vue lives in the private QuantDinger-Vue repository at
`src/views/agent-tasks/index.vue`, served at `/#/agent-tasks`. Human JWT APIs
remain under `/api/agent-automations`; internal actors are never returned to the
browser. New editor tasks default to research-only, with protection enabled.
Existing saved configurations retain protection disabled until explicitly edited.
The editor's editable 8%/3%/10% defaults are operating parameters, not calibrated
trading recommendations.

## Accounting

`automation/performance.py` replays actual cumulative quantity and average fill
price once per intent, in intent creation order. Partial and cancelled orders
with fills count; unfilled proposals do not. Same-symbol outstanding orders cannot
overlap in the executor, enabling average-cost acquisition/disposal in that order.

Capital is a virtual task allocation, not broker account equity. Adopted holdings
consume it at enrollment market value, which becomes task cost; unrelated broker
holdings are excluded. Initial external quantities are retained: a same-symbol
external trade or split invalidates attribution. Legacy tasks without this inventory
have only the existing minimum-quantity consistency check. Legacy adopted holdings
without enrollment prices show unavailable returns; historical prices are not guessed.

Capital is immutable after paper tracking initializes. Pause/restart, risk reset,
previews and prompt changes never erase fills/samples. The benchmark freezes the
initial equal-weight universe and cash fraction, allows theoretical fractional
shares, and anchors at the first simultaneous eligible observation, timestamped
separately. It is a price-return benchmark, before fees/dividends. No SPY comparison,
cash interest, historical agent backtest, annualized return or win rate is inferred.

Missing fill costs/marks or ownership discrepancies suppress total equity/returns.
Partial positions and the last valid report retain timestamps. Fees/dividends are
unavailable, not zero. Model usage covers completed decisions including previews,
excluding failed/cancelled provider calls. Estimated costs are separated by currency;
unknown prices are counted, not assumed free.

## Independent protection

The scheduler has a separate two-thread monitoring pool, targeting 30-second
observations of active and initialized paused paper tasks. Paused tasks are read-only.
The monitor reads the selected SIMULATE account and uses the existing fresh subscribed
regular-session quote gate. It never calls an LLM. Remote observations older than
30 seconds cannot authorize purchases. Missing/stale/closed-session quotes and
monitor records older than 90 seconds block new buys when protection is enabled.

| Fractional `config.risk` limit | Action |
|---|---|
| `stop_loss_pct` | Mark below task average cost locks purchases in that symbol and plans an exit |
| `max_daily_loss_pct` | Loss from the exchange-local day's first valid observation halts purchases and plans task exits |
| `max_drawdown_pct` | Decline from observed high-water equity halts purchases and plans task exits |

Stops latch until explicit human reset of a paused task; disabling checks does not
implicitly clear an existing latch. Reset restarts daily-loss
and drawdown risk anchors at the last valid equity, retaining historical returns
and maximum drawdown. Positions still below their cost stop trip again. Sampled
monitoring cannot guarantee an execution price or capture between-observation peaks.
Overnight losses enter drawdown; the daily anchor starts at that day's first valid mark.

Protection writes ordinary durable `protection:<revision>:<minute>` planned EXIT
runs, valid for 45 seconds. The existing executor refreshes account/price, sizes
owned long positions with lot/notional limits, and uses the regular Futu gateway.
A price-trigger task's entry threshold does not apply to protective exits. Busy
work and outstanding/unknown orders must finish/reconcile first. Later protection
runs can retry reductions only after the original order is terminal.

Operator arming, hard switch, policy, token, instrument and daily notional limits
still apply to exits. Authorization expiry/limits can block them, displayed in run
history. Protection does not renew authorization or install guaranteed broker stops.

## Concurrency and pause

Monitoring state, human pause/reset and final broker submission share per-user
advisory transaction lock `824112`. `submission_guard` re-reads current protection
for each buy immediately around the broker call. An old model snapshot cannot
bypass a newer halt. A new trip latches before cancelling pending model plans.
The monitor waits for busy work/orders before planning exits. Model latency does
not consume the observation pool.

Pause/cancel prevents later submissions and requests cancellation of outstanding
orders during reconciliation. An accepted order can fill before cancellation is
acknowledged; its final result is reconciled and accounted. Pause does not liquidate.
Removed universe members remain in the internal actor's instruments while owned,
allowing reductions subject to the human broker policy allowlist.

## Storage and HTTP

`20261006_agent_performance.sql` adds `qd_agent_automation_samples` with task
ownership/cascade and a task/time index. Boot migration and scheduler initialization
apply idempotent DDL. Valid samples are written at most once per minute, retained
without automatic deletion. The dashboard returns the latest 1440 valid observations
in ascending order, 30 recent runs, receipts, usage and cached latest/risk state.

| Human JWT endpoint | Contract |
|---|---|
| `GET /api/agent-automations/{task_id}/dashboard` | Owner-scoped cached report, no model/broker side effects |
| `POST /api/agent-automations/{task_id}/risk/reset` | Owner-only, paused-only reset retaining history |

`risk.enabled` is boolean; limits are fractions (0.08 = 8%). Report percentages
are percentage points (8.0 = 8%). `as_of` and `checked_at` are Unix seconds.
Success uses `code: 1`, errors `code: 0`. OpenAPI is generated from route schemas.
Research evidence records config, revision, timestamps, model usage and
`portfolio-json-v2` prompt version. Protection records account/price/risk/config
evidence with `source: deterministic_protection`. Preview never executes.

## Verification

Pure tests cover fills, average cost, adoption, external inventory/splits, unavailable
data, fixed benchmark and risk latches. Disposable PostgreSQL replay covers samples,
model-free protective plans, deduplication, paused observation, history-preserving
reset, final buy guards, ownership and cached HTTP. Existing task/gateway/Futu/
bootstrap/OpenAPI regressions run alongside Vue lint/build/unit tests. Browser
form/save/start/pause/preview uses explicitly labeled synthetic local fixtures.
No live account or actual broker submission is used in these tests.
