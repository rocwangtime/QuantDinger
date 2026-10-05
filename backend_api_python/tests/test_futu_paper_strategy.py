"""The deployable paper acceptance strategy must stay bounded and limit-only."""

from pathlib import Path

import pandas as pd

from app.services.strategy_v2 import StrategyV2BacktestRunner
from app.services.strategy_v2.contract import compile_strategy_v2
from app.services.strategy_v2.service import _instrument_member
from app.services.trading_executor import _bind_live_candidate_exchange


STRATEGY_PATH = Path(__file__).resolve().parents[2] / "docs/trading/futu_us_paper_roundtrip.py"


def test_futu_paper_strategy_compiles_and_has_persistent_state():
    source = STRATEGY_PATH.read_text(encoding="utf-8")
    program = compile_strategy_v2(source)

    assert program.namespace["PERSIST_RUNTIME_STATE"] is True
    assert program.manifest.driving_frequency == "1m"
    assert [item.key for item in program.manifest.universe.instruments] == ["USStock:SPY"]


def test_futu_execution_binding_preserves_strategy_key_for_intents_and_bars():
    program = compile_strategy_v2(STRATEGY_PATH.read_text(encoding="utf-8"))
    member = _instrument_member(program.manifest.universe.instruments[0])
    intent_key = program.manifest.universe.instruments[0].key

    _bind_live_candidate_exchange(member, "futu")

    assert member["exchange_id"] == "futu"
    assert member["key"] == intent_key == "USStock:SPY"


def test_futu_paper_strategy_backtest_submits_only_one_limit_buy_and_sell():
    source = STRATEGY_PATH.read_text(encoding="utf-8")
    index = pd.date_range("2026-09-28 13:30", periods=8, freq="min", tz="UTC")
    frame = pd.DataFrame({
        "open": [600.0] * 8,
        "high": [601.0] * 8,
        "low": [599.0] * 8,
        "close": [600.0] * 8,
        "volume": [1000] * 8,
    }, index=index)
    result = StrategyV2BacktestRunner(
        code=source,
        frames={"USStock:SPY": frame},
        initial_capital=10000,
        commission=0,
        slippage=0,
    ).run()

    trades = result["rawTrades"]
    assert len(trades) == 2
    assert [trade["side"] for trade in trades] == ["buy", "sell"]
    assert all(trade["quantity"] == 1 for trade in trades)
    assert result["holdingSnapshots"][-1]["positions"] == {}
