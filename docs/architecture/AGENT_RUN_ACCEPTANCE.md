# Agent paper run readiness and acceptance

The user approved continuing development after bounded research and event reviews.
This scope adds owner-scoped readiness, an explicit read-only Futu connection
probe, scheduler component diagnostics and a bounded forward review export.
It reuses existing tables and broker permission gates; there are no new runtime
dependencies. Probe/readiness/report operations never submit/cancel orders,
invoke an LLM, arm an account or change policy.

Readiness separates configured prerequisites from current runtime conditions.
Start validates model/provider, internal actor, paper policy, operator arming and
instrument scope. Live account checks and final submission guards remain required.
A probe verifies the saved SIMULATE account and eligible session quotes, stores
only sanitized results under the current task revision and rechecks freshness.
Cached reads never silently refresh external data. Results are diagnostic snapshots,
not execution permissions. Daily decision slots and account trading limits have
different time bases and are reported separately.

The Agent loop keeps process-local tick/success timestamps. The existing scheduler
heartbeat publishes that component snapshot without introducing another scheduler
heartbeat identity or weakening worker health. API readers inspect leader metadata.
Stop/start changes a process-local generation. In-flight analysis and final broker submission reject their old generation even after the global stop event is cleared. Planned durable runs may be freshly claimed by the new dispatcher and still require current authorization and prices.

Legacy processes without published metadata report unknown; recent observations
remain visible. Missing component metadata does not imply a healthy Agent loop.

Reports read task/run/sample/receipt rows in one repeatable-read snapshot. They
export gross task results, observed exchange-local sessions, run status/action
counts, model usage coverage, risk state and outstanding orders. Mark-to-mark
session changes do not imply full-session returns. Bounded row limits expose
truncation and mark reports partial. No win rate, inferred historical backtest,
annualization, fees or overnight attribution is invented. Report export is JSON
through the authenticated human API; no report is uploaded elsewhere.

Acceptance uses disposable PostgreSQL plus model/broker stubs for expired/missing
policy, actor scope, probe failures, stale revisions, worker stalls, report isolation,
truncation and preview restrictions. Vue is verified with local synthetic fixtures.
Configured provider/OpenD smoke checks and multiple trading sessions are separate
runtime acceptance; a local build does not establish profitability.

## Local acceptance record (2026-10-06)

- The combined affected backend suite passed 169 tests against disposable
  PostgreSQL and broker/model stubs. Two further regression cases passed:
  unconfigured-model HTTP start blocks before broker I/O while pause works;
  obsolete queued callbacks cannot claim durable work after a restart.
  Related execution/intent regressions and generation checks also passed.
- Existing Vue unit suite: 378 passed. Targeted ESLint and production build pass.
  Vite reports the existing large-chunk advisory; no new build failure.
- Real Vue component/i18n/API adapters were exercised against a loopback fixture:
  blocked preview/start, explicit read-only probe, closed-session waiting, task
  selection, enabled pause, report truncation, single-mark null change and market
  timezone formatting. Browser JSON download was read back from disk and checked
  for schema, selected task, coverage and sanitized output. Screenshot:
  `outputs/agent-run-report.jpg` (synthetic data; not a return claim).
- The inspected worktree default resolves to OpenRouter, which these task streams
  do not support. No configured supported provider was found. Actual model/OpenD
  smoke checks and multiple real paper sessions are **not** marked accepted.
- Changes are local to the attached development worktree and private Vue checkout.
  Existing changes in the user's primary Futu adapter/tests are preserved.
