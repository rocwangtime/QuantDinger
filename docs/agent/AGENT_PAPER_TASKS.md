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
