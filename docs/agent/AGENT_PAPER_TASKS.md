# Agent paper portfolio tasks

Open **Agent paper portfolios** at `/#/agent-tasks`. Add a saved Futu US/HK
SIMULATE account, then set a mandate, universe, capital, reserve, order limits and
configured model. Choose daily pre-open review or a single-symbol price trigger.
Preview calls the model but never submits orders. Automatic paper execution
requires current account authorization managed on the broker accounts page.

The workspace combines start/pause/edit, evidence, receipts, holdings, forward
performance and model usage. Independent program monitoring checks position
stops and observed portfolio daily loss/drawdown. Stops latch until explicit
paused-task reset. Reductions use the ordinary authorized gateway; expired
authorization, order limits, outstanding orders or quote availability can block
them. This is sampled protection, not guaranteed broker-side stop orders.

Pause prevents subsequent submissions and requests order cancellation; it does
not close holdings. Accepted orders can fill before cancellation acknowledgement.
Reset retains performance and starts new daily-loss/drawdown risk anchors at the
last valid equity. Positions below their cost stop trip again.

Reports use actual task-owned cumulative fills and allocated virtual cash.
Adopted holdings consume capital at enrollment market value. Unrelated account
equity is excluded. Returns are gross before fees/dividends/interest. Missing
costs or inconsistent holdings suppress full returns. Capital becomes immutable
after tracking initializes. Legacy missing initial values are never guessed.

The benchmark freezes the initial equal-weight universe and reserve, allows
hypothetical fractional shares and starts at the first simultaneous eligible
observation. Its start date is shown separately. Universe edits do not rewrite
it. Historical agent backtests, annualized returns and win rates are not inferred.
Model costs cover completed decisions including previews, excluding failed/
cancelled calls and broker fees. Unknown pricing and currency totals stay explicit.

Update both backend and private Vue client. Boot applies
`20261006_agent_performance.sql`; deployments disabling automatic migration
must apply it first. The scheduler performs ongoing monitoring. Legacy protection
remains disabled until explicitly edited.

See [implementation design](../architecture/AGENT_PAPER_DASHBOARD.md).

## Bounded research and intraday review

New tasks in the editor default to **Bounded tool research**; existing saved tasks
retain **Single snapshot decision**. Only research/automatic SIMULATE modes are
supported. To review a portfolio during the session, choose **Intraday portfolio
review**, configure a price movement fraction (the editor displays percent),
fill review and cooldown, then preview before starting.

The first valid observation requests one review. Subsequent reviews follow price
movement from the last admitted review or cumulative task fill changes. A
30-second observation interval can miss movements between observations. Busy
work, pending orders, missing evidence and latched protection defer reviews.
A three-minute event deadline prevents delayed decisions from becoming late orders.
Protection keeps priority and does not call the model.

The research loop exposes only timestamped run account/quote/bar snapshots and
on-demand news search. It uses a validated application JSON tool protocol over
existing cancellable streaming, rather than provider-native function calling.
For example, a model can request completed daily bars for one allowed symbol,
read the result and then choose BUY/REDUCE/EXIT/HOLD/WAIT. It cannot invoke
arbitrary broker commands, URLs, scripts or SQL. The order service refreshes
account/price and validates every final action. Fast price triggers use a single
snapshot call within their existing 15-second event budget.

| Setting | Default | Meaning |
|---|---|---|
| `research.mode` | `snapshot` on backend/legacy tasks | `snapshot` or `tool_loop`; editor chooses `tool_loop` for new daily/event tasks |
| `research.max_model_calls` | 3 | At most 1–4 rounds; last round must produce a complete decision |
| `research.max_tool_requests` | 6 | At most 1–8 read requests, two per round |
| `research.max_output_tokens` | 7000 | Total reported/estimated output tokens across model rounds, 700–14000 |
| `research.max_decisions_per_day` | 8 | Created non-protection runs per exchange-local day, 1–100 |
| `events.price_move_pct` | 0.02 | Absolute movement from last admitted review, 0.001–0.5 |
| `events.on_fill` | true | Review when cumulative task fills change |

Daily slots include previews, failures and cancellations; editing/restarting does
not erase consumed slots. Slots reset at exchange-local midnight, including DST.
Model calls/output limits bound activity, not monetary spend: inputs and provider
reasoning/billing can add cost. Partial, malformed, over-budget or cancelled output
cannot be executed. Each completed tool round and available failed/cancelled usage
is retained, using estimates when the provider did not return totals. Failed
single-snapshot calls remain outside usage coverage. News services/broker fees are
not part of model cost estimates.

The task page shows quota/reset time, event causes, model/tool counts and a
read-tool history in decision details. **Decision review** covers the latest
30 runs and counts proposed actions separately from protective runs and previews.
It is descriptive; paper outcomes remain actual order receipts and the gross
forward equity curve. No win rate or trading accuracy is inferred.
