# HK Agent lot and budget validation (P0 item 1)

The single-symbol settings editor requires a positive `reference_price` in HKD
when saving. It is an operator-supplied validation reference, not an executable
quote. Existing settings display an empty reference until the next save; no
historical price is invented. Each of `per_order_cap`, `daily_buy_cap` and
`max_position` must cover `lot_size * reference_price` and must not exceed
`budget * (1 - cash_reserve_pct / 100)`. The existing cap ordering still applies.

Execution uses the fresh eligible SIMULATE quote and broker lot size. Quantities
round down to whole lots; below-lot quantities never create an intent. Per-order
and position limits are checked before intent creation and in the submission
preflight. HK symbol cash availability retains the task's reserve and outstanding
buy reservations even when account cash is lower than the task budget. Daily buy
notional is checked under the existing per-user transaction lock before submission,
including uncertain orders. Existing daily accounting remains conservative: full
requested notional of cancelled/failed orders is retained for the HK day.

Skip records and logs contain reason codes such as `BELOW_ONE_LOT`,
`PER_ORDER_CAP`, `MAX_POSITION`, `CASH_RESERVE` and `DAILY_BUY_CAP`; logs contain
only these codes. Existing authorization, quote freshness, pause, revision,
protection and SIMULATE gates remain in force. Reference-price validation does
not promise that changing market prices will remain affordable. Fees are not
included in these notional limits.

Local verification uses PostgreSQL 18 at `127.0.0.1/qd_automation_test` with
broker/model stubs. No real broker acceptance, deployment or profitability claim
is made by these tests.
