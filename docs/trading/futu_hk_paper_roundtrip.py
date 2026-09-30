"""Futu HK stock paper roundtrip, one board lot and one attempt per side.

This bounded acceptance strategy uses HK.00700 only after an operator has
selected its HK STOCK SIMULATE account, verified the 100-share lot size and
buying power, and explicitly armed that account. It is not a recommendation.
"""

PERSIST_RUNTIME_STATE = True


def initialize(context):
    g.symbol = "HKStock:00700"
    g.phase = "idle"
    g.held_bars = 0
    context.set_universe([g.symbol])
    context.subscribe(
        frequency="1m",
        fields=["open", "high", "low", "close", "volume"],
    )
    context.set_warmup(3)
    context.set_metadata(direction_mode="long_only")


def _hk_tick_price(price, direction):
    # HK.00700 currently trades in HKD 0.2 ticks near this test range.
    # The operator must reconfirm the broker snapshot before deployment.
    steps = int(price / 0.2)
    if direction == "buy":
        steps += 1
    return round(steps * 0.2, 1)


def handle_data(context, data):
    bars = get_history(3, "1m", "close", g.symbol)
    if len(bars) < 3:
        return

    close = float(bars["close"].iloc[-1])
    if close <= 0:
        return
    shares = float(get_position(g.symbol).amount or 0.0)

    if g.phase == "idle" and shares == 0:
        order_target(
            g.symbol, 100, order_type="limit",
            limit_price=_hk_tick_price(close * 1.01, "buy"),
            reason="hk_paper_roundtrip_buy_once",
        )
        g.phase = "entry_submitted"
    elif g.phase == "entry_submitted" and shares >= 100:
        g.held_bars += 1
        if g.held_bars >= 2:
            order_target(
                g.symbol, 0, order_type="limit",
                limit_price=_hk_tick_price(close * 0.99, "sell"),
                reason="hk_paper_roundtrip_sell_once",
            )
            g.phase = "exit_submitted"
    elif g.phase == "exit_submitted" and shares == 0:
        g.phase = "done"
