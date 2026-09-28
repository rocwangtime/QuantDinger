# Futu US paper-trading MVP

QuantDinger connects to Futu through a private FutuOpenD gateway. This branch
is intentionally restricted to US stocks/ETFs in `TrdEnv.SIMULATE`: long-only,
limit orders, and an explicitly selected simulated `acc_id`. A Futu live
credential, HK market, unlock password, or market order is rejected by the
backend. Live trading requires a separate review and implementation.

## Key OpenAPI limitation

Futu's [today's deals](https://openapi.futunn.com/futu-api-doc/en/trade/get-order-fill-list.html),
[historical deals](https://openapi.futunn.com/futu-api-doc/en/trade/get-history-order-fill-list.html),
and [deal push](https://openapi.futunn.com/futu-api-doc/en/trade/update-order-fill.html)
are unavailable for paper accounts. Paper fills in QuantDinger are therefore
*inferred* from changes in cumulative filled quantity and average price on
[order updates](https://openapi.futunn.com/futu-api-doc/en/trade/update-order.html).
They are not individual broker execution prints. Reconciliation compares
order-level cumulative quantity/average and account positions; duplicate
callbacks must not increase the recorded quantity. Order history is queried
with the explicit simulated `acc_id`, as Futu [recommends](https://openapi.futunn.com/futu-api-doc/en/trade/get-history-order-list.html).

## Operator setup

1. Install FutuOpenD on the host and log in **outside** this repository. Do not
   place the Futu login/password in code, Git, chat, or QuantDinger logs.
2. Bind OpenD to `127.0.0.1:11111`. Do not open port 11111 to the Internet.
3. For Linux Docker Compose, start the `local-brokers` `opend-relay` profile.
   The relay listens on Docker's private bridge only, normally at
   `host.docker.internal:11112` from the API and trading-worker containers.
4. Set `ALLOW_LOCAL_DESKTOP_BROKERS=true` on this self-hosted instance. Keep
   `FUTU_ALLOW_REMOTE_OPEND=false`.
5. In the Web Account Center, choose Futu, probe OpenD, then select the exact
   US `SIMULATE` account ID. The牛牛登录号 is **not** the OpenAPI `acc_id`.
6. Save only `futu_host`, `futu_port`, `trade_env=demo`, `trade_market=US`,
   `security_firm`, and the selected `acc_id` as the encrypted broker
   credential. No unlock password is accepted for this MVP.

The Account Center shows connection, account balances, orders and positions.
Its “inferred fills” tab shows the local strategy ledger; use Strategy Center
for runtime status and Backtest Center for historical results.

## Acceptance checks

1. Probe lists a US `SIMULATE` account and returns a US quote. Confirm its
   `acc_id` in Futu OpenD before saving the credential.
2. Confirm the one-minute strategy backtest is reasonable; deploy it with a
   small, affordable order limit during regular US hours only.
3. Observe a simulated limit buy fill and subsequent limit sell fill. Compare
   order ID, cumulative filled quantity, average price, and final position
   against the Futu account.
4. Restart the trading worker, disconnect/reconnect OpenD, and replay a
   duplicate cumulative order update. Recorded quantity must not increase.
5. Leave live trading disabled even after paper acceptance.
