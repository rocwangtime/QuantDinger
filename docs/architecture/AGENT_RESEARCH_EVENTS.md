# Bounded research and event portfolio reviews

## Scope

The user approved further implementation after the paper dashboard milestone.
This scoped extension adds bounded read-only research, event portfolio reviews,
and visible decision budgets/traces. It reuses Futu SIMULATE execution, existing
cancellation, durable intents and independent protection. No new dependencies,
provider credentials, account permissions or live execution venues are required.

## Research protocol

Existing tasks default to `research.mode=snapshot`. `tool_loop` uses the existing
cancellable JSON streaming transport: a model may return `tool_requests` instead
of a final portfolio decision. The application validates and dispatches these
requests, supplies timestamped results, then asks for a complete decision. This
is an application JSON tool protocol, not native provider function calling.

The only tools are `account`, `quote`, `daily_bars`, and `news`. Account/quote/bars
read the frozen run snapshot; news is fetched only when requested. Symbols must
exist in run evidence. No arbitrary URLs, scripts, SQL or order tools are exposed.
Tool results remain untrusted evidence. Model streams are cancellable; synchronous news adapters check cancellation before/after their provider request and use that provider's network timeout. Their underlying request may finish after cancellation, but cannot advance the run or submit orders. The last
round must produce a complete valid decision. Partial/invalid/over-budget output
cannot execute. Fast 15-second price triggers retain snapshot mode.

Per-run bounds cover model calls, tool requests, output tokens and existing wall
clock deadline. The exchange-local daily decision limit reserves a slot when a
run is created, including previews, failed and cancelled work. Protection runs
are exempt because they never invoke a model. Limits are per task, not global
account limits or exact monetary caps. Input/reasoning/provider billing may add
cost; estimated spend is reported separately from token bounds.

## Event portfolio

`event_portfolio` observes fresh eligible regular-session prices every 30 seconds.
The first valid observation enrolls price/fill baselines and requests a review.
Subsequent triggers are a configured absolute price movement from the last
admitted review or a change in cumulative task fills. Paused, stale, halted or
busy tasks cannot enqueue reviews. Cooldown, daily slot limits, outstanding-order
checks and row locks bound churn. Deferred events retain their baseline and retry
later. Revision changes re-enroll. Protective exits have priority.

Events enqueue ordinary durable runs with a three-minute deadline and explicit
trigger evidence. Analysis completion can execute immediately after the usual
fresh account/quote and submission checks. An expired event never becomes a
late order. Price sampling can miss movement between observations.

Full read-tool payloads are available in owner-scoped run details. List/dashboard run summaries return a tool request count to keep repeated polling bounded; usage queries project only per-call totals.

## Verification

Use pure research/config/event tests and disposable PostgreSQL replay for quota
reservation, busy/paused/protection precedence, event deduplication and cancellation.
Run gateway regressions, OpenAPI export, Vue lint/build/unit checks and local
browser verification. Model and broker calls remain fixture stubs in validation.

## Implemented acceptance

- 147 affected backend tests passed, including disposable PostgreSQL concurrency
  reservations, DST reset boundaries, cumulative fill events, protection priority,
  preview isolation, incomplete/over-budget research and existing gateway regressions.
- Final streaming/API changes passed 40 focused tests; the subsequent slim run-list
  serialization and projected usage read passed four HTTP/tool-loop regressions.
- Existing Vue unit suite: 378 passed. Targeted ESLint, Ruff, OpenAPI export,
  diff whitespace checks and production build passed. The production build retains
  its existing large-bundle warning.
- Local browser fixtures verified legacy snapshot defaults, new tool-loop defaults,
  SIMULATE account filtering, event editor, 2.5% to 0.025 conversion, six daily slots,
  research-only save and readable tool details. Fixed the event template showing
  fast-trigger fields and hidden empty chart initialization. Saved synthetic proof
  at ignored `outputs/agent-events-review.jpg`.
- Provider/broker calls were stubbed during verification. Actual configured-model
  tool-loop interoperability, live OpenD observation timing and multi-session paper
  performance remain deployment acceptance checks; no profitability is established.
