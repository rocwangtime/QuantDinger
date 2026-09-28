"""Futu US paper roundtrip (one share, one attempt per side).

Uses completed SPY one-minute bars for a bounded limit buy, then submits one
limit sell after the position has been observed for two completed bars. This is
an acceptance-test strategy, not an investment recommendation.
"""

PERSIST_RUNTIME_STATE = True


def initialize(context):
    g.symbol = "USStock:SPY"
    g.phase = "idle"
    g.held_bars = 0
    context.set_universe([g.symbol])
    context.subscribe(
        frequency="1m",
        fields=["open", "high", "low", "close", "volume"],
    )
    context.set_warmup(3)
    context.set_metadata(direction_mode="long_only")


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
            g.symbol,
            1,
            order_type="limit",
            limit_price=round(close * 1.02, 2),
            reason="paper_roundtrip_buy_once",
        )
        g.phase = "entry_submitted"
    elif g.phase == "entry_submitted" and shares >= 1:
        g.held_bars += 1
        if g.held_bars >= 2:
            order_target(
                g.symbol,
                0,
                order_type="limit",
                limit_price=round(close * 0.98, 2),
                reason="paper_roundtrip_sell_once",
            )
            g.phase = "exit_submitted"
    elif g.phase == "exit_submitted" and shares == 0:
        g.phase = "done"
