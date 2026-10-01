# Futu adapter: US/HK stock SIMULATE only

The backend hard-fences Futu to US or HK **stock** paper trading with an
explicit OpenAPI `acc_id`. Only limit orders and long positions are permitted.
Real trading, unlock passwords, HK options/futures, market orders, and account
auto-selection are rejected. HK orders require the broker's lot size, a fresh
regular-session quote, and a broker-reported `max_cash_buy` check.

Futu does not expose paper deal history or deal push. Execution events are
derived from cumulative order updates and reconciled with simulated order
history and positions. See [deployment and acceptance notes](../../../../docs/architecture/FUTU_OPEND.md).
For HK-specific setup see [HK paper acceptance](../../../../docs/architecture/FUTU_HK_PAPER_ADDENDUM_CN.md).
