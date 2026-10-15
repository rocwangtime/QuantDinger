# US daily research screener

This implements item 1 of the 2026-10-15 selection plan. It produces an independent,
auditable research shortlist. It does not read or change Agent task configurations,
activate tasks, create intents/orders, or import application execution services.
Nothing connects the shortlist to an Agent. No deployment or server access was
performed while building this feature.

## Universe and provenance

`UniverseService.resolve_members(0, system_nasdaq100_id, as_of=day)` is the source.
The catalog's Nasdaq-100 row has `source=provider`, `source_ref=NDX`: it reads
`qd_universe_members`, not the symbol master. The symbol-master branch applies only
to `source=symbol_master`; `USStock:hot:equity` would mean hot equities, not index
constituents. An empty Nasdaq-100 member table therefore resolves to **zero**.
There was no production query: qd-sg was explicitly out of scope.

Two registered, idempotent migrations are provided:

* `20261015_us_screener.sql`: runs, per-symbol observations, daily bar cache and
  short-lived quote cache.
* `20261015_nasdaq100_seed.sql`: seeds only a completely empty system Nasdaq-100.
  It leaves any populated universe untouched and does not backfill historical
  membership. Its `valid_from` is **2026-10-14**.

**The seed is explicitly flagged, not certified as a complete current index.**
[StockTi's public list](https://stockti.com/stocks/nasdaq-100), observed 2026-10-14,
advertises 101 stocks but exposes only 100. Those exposed symbols are recorded,
with WBD replaced by MRNA according to the
[Nasdaq announcement effective 2026-10-09](https://nasdaqinc.gcs-web.com/news-releases/news-release-details/moderna-inc-join-nasdaq-100-indexr-beginning-october-9-2026).
Nasdaq's [official weighting page](https://indexes.nasdaq.com/Index/Weighting/NDX)
requires login for complete components. Other public lists disagreed or were old;
no missing 101st symbol was invented. Universe metadata records both URLs and
`verification=flagged_incomplete_public_list`; each run copies the metadata into
its regime record and the frontend prominently shows the warning. Review/replace
this seed with a fully verified dated index membership before relying on it.

Local PG18 verification: applying the seed twice to an empty universe creates 100
unique members; the real universe resolver returns 100 on 2026-10-14, and zero
before that date. This verifies resolution, not complete index coverage. If
resolution is empty, an explicitly configured `US_SCREENER_MANUAL_SYMBOLS` list
may be used; the run records `universe_fallback=true`. No silent generic-stock
fallback is used. Universes above 200 symbols fail explicitly.

## Filters, features and score

All calculations use completed daily bars through the run's New York session date.
Daily bars are sorted/deduplicated; the latest 61 sessions must be contiguous on
XNYS. Missing/invalid bars are excluded. Price is the completed session close.
Extended-session bid/ask values never enter the spread filter.

* Session close >= USD 5.
* Mean of `daily close × share volume` over 20 sessions >= USD 50 million.
* Regular-session spread <= **10 basis points**, with per-symbol `spread_source`:
  * `rth_snapshot`: valid uncrossed positive bid/ask timestamped inside the exact
    as-of XNYS session (including its calendar-specific early close).
  * `corwin_schultz_daily`: fallback when no valid RTH snapshot exists. For each
    of the last 20 adjacent daily-bar pairs, beta = log(H1/L1)^2 + log(H2/L2)^2,
    gamma = log(max(H1,H2)/min(L1,L2))^2, D = 3−2√2,
    alpha = max(0, (√(2 beta)−√beta)/D−√(gamma/D)). Average
    `2 tanh(alpha/2) × 10,000` across pairs. This Corwin–Schultz (2012)
    high/low estimator removes a two-day volatility component; it is an estimate,
    can floor at zero, and is not an executable spread or a substitute for measured
    quotes in execution. Store this only in `spread_estimate_bps`; `spread_bps` is null. Flag
    `spread_unverified` and permit eligibility by default.
    `US_SCREENER_UNVERIFIED_SPREAD=exclude` excludes unverified symbols.
    The 10 bps cutoff applies only to observed RTH quotes.
* Earnings dates within three XNYS sessions on either side of the as-of date are
  excluded (including the earnings day). Holidays and weekends do not count.
* Unknown earnings: `US_SCREENER_EARNINGS_UNKNOWN=flag` (default) permits
  eligibility with the visible `earnings_unknown` flag; `exclude` also excludes.
  Legacy `open`/`closed` are aliases. Every row has `earnings_status=known|unknown`.

The helper first reads fundamental observations ingested by the as-of cutoff and
refreshed within seven days. If no future calendar exists, current/latest-session
runs query Nasdaq's public expected earnings endpoint:
`https://api.nasdaq.com/api/analyst/{symbol}/earnings-date`.
`reportText` includes expected or estimated reporting dates supplied by Zacks.
Forecast dates can change. Historical runs older than yesterday do not fetch live
forecasts. Past period ends never substitute for a future calendar.

`20261015_us_screener_quality.sql` adds the DB earnings cache with `source` and
`fetched_at`. Positive and empty/error results are cached for 24 hours. Requests
are spaced by at least one second, have a five-second timeout and no retries,
credentials, challenge bypasses or region workarounds. Futu remains read-only;
no trading context is used to fetch earnings. Unknown is never silently unflagged.

Local network verification on 2026-10-15 using the application's User-Agent:
Nasdaq returned HTTP 200 and dates for AAPL (2026-11-02), MSFT (2026-10-28),
NVDA (2026-11-18). Yahoo chart returned 429, quoteSummary 401, and Stock Analysis
403; these are not used. This verifies access from the development environment;
the production server was not accessed. The cached snapshot-only implementation
previously never refreshed `_earnings_dates`, explaining broad unknown coverage.

Feature definitions:

* Momentum 20/60: `close[t]/close[t−20 or 60] − 1` (fraction).
* Realized volatility 20: sample standard deviation of the latest 20 daily log
  returns × sqrt(252) (annualized fraction).
* ATR% 14: arithmetic mean of 14 true ranges / latest close × 100; true range is
  max(high−low, abs(high−previous close), abs(low−previous close)). This version
  uses a simple average, not Wilder smoothing.
* Distance to MA50: `(close/mean(last 50 closes)−1) × 100` (percentage points).
* QQQ regime: risk_on if `close > MA50 > MA200`, otherwise risk_off; missing or
  invalid QQQ coverage produces unknown. Regime is descriptive, not an eligibility
  or trading authorization switch.

Across eligible symbols, score = **0.4 z(momentum20) + 0.4 z(momentum60)
− 0.2 z(volatility20)**. Z uses population standard deviation; a zero-variance
feature contributes zero. Descending score, then ascending symbol breaks ties.
The first ten are included. Others retain their rank/score and `below_top_n`.
Excluded symbols retain computed features, reasons and flags; unavailable values
remain absent/null rather than invented. The run stores the score version.

## Data, execution bounds and persistence

`FutuData` constructs only `OpenQuoteContext`, allowing `get_market_snapshot` and
`request_history_kline`. Quote APIs have no `TrdEnv` argument: SIMULATE is an
account/execution setting, and no trading/account context is created at all.
The existing `FUTU_OPEND_HOST` / `FUTU_OPEND_PORT` configure the quote connection.
No credential, account, position or order endpoint is called.

Quotes are batched by 50; successive remote calls are at least 2.1 seconds apart.
Quote cache TTL is 60 seconds; source timestamps determine RTH eligibility. Daily QFQ bars are cached by symbol/session (300 bars requested over a
420-day calendar window); cache history is retained for 14 days. Pagination is
unneeded for the 61/200-bar windows; insufficient returned coverage fails explicitly.
Broker permissions/history quotas can cause a partial run; there is no provider
fallback and no hidden retry loop. The single admitted pending run across API,
CLI and beat prevents local screener workers multiplying the rate. Other platform
quote consumers have independent budgets; production quota testing is still needed.

Admission uses a screener-specific PostgreSQL transaction advisory lock. Scheduled
runs have a unique `daily:YYYY-MM-DD` key, manual/CLI attempts unique keys. A
conditional claim prevents duplicate Celery deliveries from rerunning a job.
Runs store as_of, frozen params, regime, status, counts, timestamps and error.
Each symbol stores features, score/rank, included, exclusion reasons and flags.
Pending rows are saved before network calls; final writes replace their observations
atomically. A worker interruption retains `not_processed` coverage rather than
silently dropping symbols. `counts.failed` and `counts.coverage_ratio` record
market-data coverage, also copied to regime as `failed_count`/`coverage_ratio`.
Coverage counts valid market observations regardless of eligibility filters.
Runs complete when coverage >= `US_SCREENER_MIN_COVERAGE` (default 0.9, range
(0,1]); below that they remain partial. Failed symbols stay excluded with reasons.
An unavailable descriptive QQQ regime remains explicitly unknown. Item-2 resolves
completed runs only and defensively requires included rows to have no exclusions;
its existing `plan_only` constraints remain in force.

The engine has a 480-second cooperative budget, SDK connection waits are capped,
Celery soft/hard limits are 510/540 seconds, and CLI uses an isolated spawned process
with a 540-second timeout. The beat tick marks queued/running jobs over 15 minutes
old failed (`worker_timeout`) while retaining symbol rows. A failed scheduled run
is not automatically retried that day; an admin can manually retry. A queued job
that cannot be published becomes failed (`queue_unavailable`).

## Schedule and interfaces

Celery beat sends a short maintenance tick every minute. With
`US_SCREENER_ENABLED=true`, the first tick at or after **16:30 America/New_York**
on an XNYS session day admits that day's run. ZoneInfo handles DST; weekends and
market holidays are skipped. Early-close days still run at 16:30. Missed sessions
are not backfilled. Default scheduling is disabled until an operator configures it.

* Authenticated `GET /api/us-screener/latest` (null before any runs).
* Authenticated `GET /api/us-screener/runs/<id>` (404 for missing).
* **Admin-only** `POST /api/us-screener/runs` queues a research run (202;
  409 when another run is pending; 503 for queue failure).
* `python -m app.commands.us_screener` creates and runs a local bounded research
  attempt without Celery. Manual and CLI use the most recent completed session.

The Agent workspace contains an independent read-only US screener section,
including parameters, regime, top ten features/scores and excluded reasons.
It supports zh-CN/en-US and both dark/realdark themes. There is no trigger button
in this version; administrators may use the API or CLI. Refresh never triggers a
new run or changes an Agent task.

## Validation and future deployment notes

Tests cover feature math, filters, earnings/holidays, regime, z-score/tie ranking,
DST scheduling, Futu batching/cache/rate bounds, PostgreSQL persistence and duplicate
admission, real universe resolution/idempotent seed, cached earnings, API auth/admin
roles and execution-import restrictions. Tests use synthetic data and the explicit
local disposable PG18 database; no remote Futu or trading service was exercised.

Future deployment (not performed): run `python -m app.commands.migrate` with the
normal migration process, review the flagged seed, preserve existing secrets and
OpenD state, configure the three `US_SCREENER_*` values in backend `env.example`,
and restart **API, Celery worker (maintenance queue), Celery beat**, plus publish
the matching frontend build. The scheduler-worker and trading-worker have no new
screener responsibilities. Migration/bootstrap has existing unrelated migrations;
review those separately under the normal deployment procedure. Do not activate
Agent tasks or any trading switch as part of enabling this research schedule.

## Regular-session quote capture

`US_SCREENER_ENABLED=true` or `US_SCREENER_RTH_CAPTURE_ENABLED=true` enables a
bounded maintenance task. The minute tick captures once per XNYS session between
close minus 30 minutes and close minus 2 minutes (15:30–15:58 ET normally,
12:30–12:58 on half days). Holidays and DST use the exchange calendar.
The read-only Futu provider batches at most 50 symbols with its existing rate limit.
Only valid RTH bid/ask quotes updated within 120 seconds of capture are persisted.
The cache is keyed by symbol and session, retained for 14 calendar days, and read
for the exact run `as_of`; after-hours snapshots never overwrite it.

Migration `20261015_us_screener_rth.sql` replaces legacy transient cache keys and
adds per-session capture admission. Failed/partial captures remain recorded and
are not retried that session; uncaptured symbols follow the unverified policy.
Manual capture: `python -m app.commands.us_screener --capture-now`, with either
enable flag, inside RTH only. The CLI uses a bounded child process.

A run with zero shortlisted symbols is `partial`, with error and regime reason
`no_eligible_symbols`, regardless of coverage, and cannot supply an automation
shortlist.
