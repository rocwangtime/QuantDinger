# US shortlist plans and forward scores

Opt in on an existing **USStock daily_portfolio / plan_only** configuration:

```json
{
  "universe_source": "screener:top_n",
  "top_n": 10,
  "universe_max_age_days": 4
}
```

These fields supplement the usual task configuration. Keep its stored `symbols`;
resolution never edits them. `top_n` is an integer 1–20. The maximum age is an
integer 0–30 calendar days, measured against the screener's `as_of` in New York.
Only completed US screener runs qualify; partial/failed runs never qualify.
The newest qualifying run is ranked by data date, completion time and ID. An
empty or unavailable shortlist completes the agent run with
`skip_reason=no_fresh_completed_us_screener_shortlist` and no decisions.

The resolved shortlist and screener ID are frozen in run evidence. Existing
budget, reserve, per-order, daily and target-weight limits stay in place. The
screener evidence path uses only the screener's rate-limited quote context
(`get_market_snapshot` / `request_history_kline`). Its account snapshot is an
explicitly **hypothetical cash budget**, with no broker account, positions or
open orders read. It does not grant permission to execute or change task state.
Snapshot and tool-loop research can consume this evidence. Config normalization,
store create/edit, symbol-settings materialization, runtime analysis, execution
and final submission guards reject a screener source in an executing mode.

The existing scoring maintenance tick registers non-preview screener decisions
idempotently at `1d` and `5d`. XNYS session calendars handle holidays/weekends.
The anchor is the last completed session at decision time; targets are the
first/fifth subsequent session closes. This is a close-to-close research mark,
not an executable entry price. QFQ daily bars are shared with the screener cache;
missing exact anchor/target bars remain pending without a later-price fallback.
Incomplete caches are refreshed on retry.
All shortlist constituents must have both marks; a missing leg leaves the score
null. Forward jobs are capped at 40 pending scores and 50 seconds per tick, with
ten-minute retries and the adapter's 2.1-second request spacing.

Each outcome stores raw symbol return, action-directional return (BUY/HOLD =
long, REDUCE/EXIT = negative return, WAIT = zero), each baseline return and each
excess return, in percentage points. Baselines are the equal-weight **frozen
resolved shortlist**, SPY and QQQ. Returns are gross, unweighted per-decision
marks; they are not a portfolio simulation or realized account profit.

`GET /api/agent-automations/<task_id>/daily-report?day=YYYY-MM-DD` includes
`forward_scoring`: hit rate (strictly positive excess) and mean excess against
all three baselines at each horizon, scored/pending counts, screener run IDs,
and turnover. Turnover is the fraction of the union of previous/current plan
symbols whose membership or target weight changed. The first plan has unknown
turnover. A decision is fully scored only when both horizons are scored.
The existing task review API includes at most 91 daily forward reports, which
the Vue report dialog displays read-only.

`GET /api/agent-automations/runs/<run_id>/scores` exposes frozen inputs and
nullable outcomes. Owner-scoped `POST .../scores/rescore` resets only `1d`/`5d`
observations for the maintenance tick, preserving inputs and due dates. Cached daily reports are invalidated atomically so pending coverage is rebuilt. No
legacy intraday scores or task/order records are reset. The migration only
extends the score horizon constraint and is registered after the scoring table
migration. It neither modifies nor activates existing tasks.
