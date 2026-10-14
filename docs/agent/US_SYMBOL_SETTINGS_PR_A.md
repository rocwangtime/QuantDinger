# US/HK single-symbol settings — PR A

Single-symbol SIMULATE price tasks now support USStock and HKStock settings.
Xiaomi's existing defaults and HKEX tick table are unchanged. US prices use
$0.01 ticks at or above $1 and $0.0001 below $1; sizing uses the broker's
integer lot (default 1) and whole shares. Buying cash remains min(cash, power).

US triggers and daily buy reservations use America/New_York; XNYS sessions
handle DST, holidays and early closes. HK uses XHKG and Asia/Hong_Kong with
its existing mandatory lunch exclusion. Opening blackout is configurable
(default US 5 minutes, legacy HK 0); closing blackout defaults to 5 minutes.
extended_hours defaults false. Explicit US opt-in permits observation
04:00–20:00 ET on session days; broker regular-session execution eligibility
and all existing execution/authorization gates still apply.

Migration 20261014_us_agent_symbol_seed.sql creates an inactive plan_only AAPL
task for each existing US automation user/account without an AAPL price task.
It creates an internal execution actor, no authorization, and is idempotent.
The seed uses budget 15000 USD, order cap 3000, daily buys 6000, position 9000,
20% reserve, ±2% previous-close or 1%/5-minute trigger, 30-minute directional
cooldown, 8 triggers/day, 5-minute opening/closing blackouts, no extended hours,
5% stop loss, 2% daily budget loss, 8% drawdown and deepseek-v4-1-flash.
Reference price remains unset: settings save requires a valid reference price,
and execution independently revalidates price, broker lot and cash.
Accounts without an existing US automation are not discovered or modified.

Validation: 298 targeted backend tests passed against local PostgreSQL 18,
including existing HK settings, pricing, fast-slot, latency, scoring and
broker contracts; 420 frontend tests, changed Vue files' ESLint, and the
production pnpm build passed. Ruff, backend structure, requirements lock,
documentation checks and OpenAPI export passed. Existing calendar deprecation
and build chunk-size warnings remain. No server, model or OpenD acceptance,
activation, push, merge or deployment was performed. Fast-slot/scoring/report
generalization is outside PR A.
