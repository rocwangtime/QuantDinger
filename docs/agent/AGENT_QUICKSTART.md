# Agent Gateway Quickstart

QuantDinger exposes a tenant-scoped Agent Gateway at `/api/agent/v1`. Agent tokens are separate from human JWT sessions and enforce capability scopes, market/instrument allowlists, rate limits, expiry, and paper-only restrictions.

The machine-readable contract is [agent-openapi.json](agent-openapi.json). MCP setup is documented in [MCP_SETUP.md](MCP_SETUP.md).

## Authenticate

Create an Agent Token from the human admin UI, store the full token when it is shown once, and send it as a bearer token:

```bash
curl -H "Authorization: Bearer $QUANTDINGER_AGENT_TOKEN" \
  http://localhost:8888/api/agent/v1/whoami
```

Scopes are `R` for reads, `W` for saved artifacts and deployment configuration, `B` for backtests, `N` for notification side effects, and `T` for trade proposals and emergency controls. `C` is admin-only. New tokens must be `paper_only=true`; Agent broker-live execution is disabled regardless of `AGENT_LIVE_TRADING_ENABLED`.

Every mutating W/B/N/T request requires a unique `Idempotency-Key` header. Reuse the same key only when retrying the exact same method, route, query, and body. The gateway atomically reserves the key, returns a stored completed response on replay, and rejects concurrent or mismatched reuse.

## Strategy API V2

Executable strategies use Strategy API V2. Code defines `initialize(context)`, declares its universe and subscriptions, and provides `handle_data`, `on_rebalance`, or a scheduled callback. Markets, instruments, frequencies, dependencies, warmup, and leverage policy come from the compiled manifest.

The Agent Gateway exposes the complete source lifecycle:

1. List starter code with `GET /strategy-sources/templates`.
2. Compile code with `POST /strategy-sources/compile`.
3. Save a private source with `POST /strategy-sources`.
4. Inspect or update it through `/strategy-sources/{source_id}`.
5. Review immutable snapshots through `/strategy-sources/{source_id}/versions`.
6. Create a stopped deployment from the saved source id.

Source restoration requires an explicit `confirm=true` request and creates another immutable snapshot.

Create a stopped deployment from a saved source:

```bash
curl -X POST http://localhost:8888/api/agent/v1/strategies \
  -H "Authorization: Bearer $QUANTDINGER_AGENT_TOKEN" \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: deploy-spy-trend-v1" \
  -d '{
    "name": "spy-trend",
    "sourceId": 12,
    "initialCapital": 10000,
    "executionMode": "signal",
    "leverageEnabled": false,
    "params": {"lookback": 50}
  }'
```

Update the same canonical fields with `PATCH /api/agent/v1/strategies/{id}`. Starting a deployment is intentionally not part of the W-scope configuration endpoint. A T-scope token can stop a running deployment through `/strategies/{id}/stop`.

## Backtests

Backtests accept Strategy API V2 code and run asynchronously:

```bash
curl -X POST http://localhost:8888/api/agent/v1/backtest/run \
  -H "Authorization: Bearer $QUANTDINGER_AGENT_TOKEN" \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: spy-trend-2025" \
  -d '{
    "code": "def initialize(context):\n    g.symbol = \"USStock:SPY\"\n    context.set_universe([g.symbol])\n    context.subscribe(frequency=\"1d\")\n\ndef handle_data(context, data):\n    pass",
    "startDate": "2025-01-01",
    "endDate": "2025-12-31",
    "initialCapital": 10000,
    "leverageEnabled": false,
    "params": {}
  }'
```

Poll `/api/agent/v1/jobs/{job_id}` or consume `/api/agent/v1/jobs/{job_id}/stream`. Reuse an idempotency key when retrying the same submission.

Successful backtests also save a standard Backtest Center history record for the
agent token's user. The completed job's `result.runId` identifies that record;
the initial submission still returns `job_id` while the run is queued. The result
keeps its existing metrics alongside `runId`. A persistence error fails the job
instead of reporting a successful result without saved history. Jobs completed
before this behavior was enabled are not automatically added to history.

Cancel a queued or running job with `POST /jobs/{job_id}/cancel` using B scope, confirmation at the MCP layer, and a new idempotency key. A running worker may finish its local computation, but it cannot overwrite the durable cancelled state.

## Indicators

Indicators are chart-only. Fetch `/indicators/authoring-contract`, validate with `/indicators/validate`, and save through `/indicators`. Indicator code cannot be passed to the backtest endpoint; convert the trading idea to Strategy API V2 first.

## Research, broker observations, and notifications

Research tools expose point-in-time universes, factor metadata, and the tenant watchlist under `/research/*`. Broker observation endpoints under `/trading/*` return safe credential metadata, account snapshots, account/strategy positions, pending orders, and cursor-paginated trade ledgers. They never return decrypted API keys, secrets, passphrases, or encrypted credential blobs.

For an owned Futu US SIMULATE credential,
`GET /trading/accounts/{credential_id}/futu-quote?symbol=SPY` returns a
quote-only OpenD snapshot with provider, exchange timestamp, receipt time and
staleness. Until real-time entitlement and market status can be verified, it
deliberately reports `execution_eligible=false`; no order code consumes it.
`GET /trading/accounts/{credential_id}/futu-quote-status` returns sanitized
OpenD health and subscription quota, while
`GET /trading/accounts/{credential_id}/futu-order-book?symbol=SPY` returns a
subscribed book and symbol market state. These are diagnostics, not execution
authorization; quote permission remains `UNVERIFIED` until independently checked.

N-scope signal-alert endpoints under `/notifications/signal-alerts` reuse the existing indicator notification service. Immediate evaluation requires explicit MCP confirmation because it may deliver a notification.

## Trading policy and immutable intents

`POST /trade-intents` (T scope) records an immutable proposed order. The legacy
`POST /quick-trade/orders` endpoint is an alias. A unique `Idempotency-Key` is
mandatory for each intent; retrying the same key with changed order content is
rejected. Creating an intent is always plan-only, including when `PAPER_AUTO`
is enabled; no paper or broker order is generated by this route.

```bash
curl -X POST http://localhost:8888/api/agent/v1/trade-intents \
  -H "Authorization: Bearer $QUANTDINGER_AGENT_TOKEN" \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: spy-plan-20260929-1' \
  -d '{"broker":"futu","credential_id":2,"market":"USStock","symbol":"SPY","side":"buy","qty":1,"order_type":"limit","limit_price":600,"reason":"test proposal"}'
```

The server verifies that the credential belongs to the token's tenant and is
an explicitly selected US `SIMULATE` account, but a Futu intent is **always a
plan**. It does not use the existing strategy's Futu SIMULATE order path, and
it cannot address a REAL account. Agent tokens can read the effective policy
through `GET /trading-policy` and list/get/cancel unexecuted proposals under
`/trade-intents`; they cannot change policy or approve an order.

Human administrators can view and change policy under
`/api/agent/v1/admin/trading-policy` (human JWT only). `PAPER_AUTO` currently
supports **internal platform paper simulation only**. Enabling it requires an
exact market/symbol allowlist, per-order/daily/count limits, typed
`confirm_mode=PAPER_AUTO`, and an expiry within 24 hours. It does not call
Futu or another broker. The Web Agent Token page shows policy and intents.
An Agent may call `POST /paper-orders/place` to place an **internal platform
paper** order only while that policy is active, using a paper-only T-scope
token and a fresh Idempotency-Key. This explicit execution route cannot select
a Futu credential. Futu SIMULATE Agent direct order execution remains closed
pending broker-order recovery, cancellation, and reconciliation checks.
`LIVE_APPROVAL` and `LIVE_AUTO` are rejected, including when the old server
environment switch is set; they require a separately authorized implementation.

## Runtime and emergency stop

`GET /runtime/overview` returns compact tenant runtime state. Internal paper
simulation uses a research K-line quote labeled `research_kline`, with
`is_realtime=false` and `execution_eligible=false`. This quote is never treated
as a Futu execution quote. The human policy, not an Agent-supplied confirm
flag, controls whether it may write to the internal paper ledger.

The emergency stop at `/quick-trade/kill-switch` persists a global
`EMERGENCY_STOP` before attempting to cancel previously created Agent orders.
It expires pending proposals, cancels open internal paper orders, revokes all
active T-scope tokens for the tenant, and reports any exchange cancellation
attempts for mandatory human review. An accepted exchange cancellation
request is not marked terminal until separate broker reconciliation confirms
it. It **does not liquidate positions**, stop separately deployed strategies,
or sign out of Futu OpenD. The human administrator must explicitly clear the
global stop before new Agent proposals are accepted. If a strategy is already
running, pause it via its own operator controls as a separate action. A global
stop also downgrades every account's Agent `PAPER_AUTO` policy to `PLAN_ONLY`,
so clearing the stop never silently resumes prior Agent automation.

Stopping autonomous submissions, cancelling Agent-owned open orders, and
liquidating a position are distinct actions. The first uses the human policy
`EMERGENCY_STOP`; the second uses `/agent-orders/cancel` (T scope) or
`/admin/agent-orders/cancel` (human JWT). Neither action liquidates. No Agent
force-liquidation endpoint is exposed.

Rate limiting is shared through Redis across API workers and enforces both token and tenant quotas. Responses include `X-RateLimit-Limit`, `X-RateLimit-Remaining`, and `X-RateLimit-Reset`; `429` responses also include `Retry-After`.

Never log tokens or credential material. Treat redacted values as terminal and do not attempt to reconstruct them.
