# Futu adapter: US SIMULATE only

This branch's backend hard-fences Futu to US paper trading with an explicit
OpenAPI `acc_id`. Only limit orders and long positions are permitted. Real
trading, unlock passwords, HK orders, market orders, and account auto-selection
are rejected.

Futu does not expose paper deal history or deal push. Execution events are
derived from cumulative order updates and reconciled with simulated order
history and positions. See [deployment and acceptance notes](../../../../docs/architecture/FUTU_OPEND.md).
