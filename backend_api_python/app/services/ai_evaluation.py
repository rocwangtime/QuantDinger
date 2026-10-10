"""Paired fixed-horizon shadow evaluation, distinct from portfolio returns."""

import json
from datetime import datetime, timedelta, timezone
from statistics import mean

import pandas as pd

from app.services.portfolio.risk import finite_number
from app.utils.db import get_db_connection


def _object(value):
    return json.loads(value) if isinstance(value, str) else value or {}


def summarize_shadow(rows, *, horizon_hours):
    observed = [row for row in rows if row.get("net_return") is not None]
    deltas = [row["filtered_return"] - row["net_return"] for row in observed]
    rejected = [row for row in observed if not row["raw_allowed"]]
    days = {str(row["decision_at"])[:10] for row in observed}
    return {
        "method": "pairedFixedHorizonCounterfactual", "horizon_hours": horizon_hours,
        "observed": len(observed), "missing": len(rows) - len(observed), "decision_days": len(days),
        "mean_baseline_return": mean(row["net_return"] for row in observed) if observed else None,
        "mean_filtered_return": mean(row["filtered_return"] for row in observed) if observed else None,
        "mean_paired_delta": mean(deltas) if deltas else None,
        "blocked_losses": sum(row["net_return"] < 0 for row in rejected),
        "missed_winners": sum(row["net_return"] > 0 for row in rejected),
        "mean_latency_ms": mean(float(row.get("latency_ms") or 0) for row in rows) if rows else None,
        "model_credits": sum(max(0, float(row.get("charged_credits") or 0)) for row in rows),
        "evidence_status": "observational" if len(days) >= 20 and len(observed) >= 60 else "insufficient_evidence",
        "limitations": ["Opportunity returns are not a portfolio equity curve",
                        "Overlapping horizons and repeated symbols are dependent observations",
                        "Model credits are reported separately; no unverified USD conversion",
                        "Fixed-horizon exits are hypothetical and do not prove live fill quality",
                        "Model confidence is not an event probability"],
        "rows": rows,
    }


def evaluate_strategy_shadow(*, user_id, strategy_id, horizon_hours=24, limit=100, on_progress=None, frame_loader=None):
    from app.routes.strategy_services import get_strategy_service
    from app.services.strategy_v2.market_data import load_strategy_frame
    from app.services.strategy_v2.snapshot import MarketDataSnapshotStore

    horizon = int(horizon_hours)
    if not 1 <= horizon <= 168:
        raise ValueError("aiEvaluation.invalidHorizon")
    strategy = get_strategy_service().get_strategy(int(strategy_id), user_id=int(user_id))
    if not strategy:
        raise ValueError("strategyV2.strategyNotFound")
    with get_db_connection() as db:
        cur = db.cursor()
        cur.execute("SELECT * FROM qd_ai_decisions WHERE user_id=%s AND source_type='strategy' AND source_id=%s "
                    "AND decision IN ('shadow_pass','shadow_reject','shadow_unavailable') ORDER BY created_at,id LIMIT %s",
                    (int(user_id), int(strategy_id), max(1, min(200, int(limit)))))
        decisions = cur.fetchall() or []
        cur.close()
    output, cache = [], {}
    loader = frame_loader or load_strategy_frame
    now = datetime.now(timezone.utc)
    for index, decision in enumerate(decisions):
        if on_progress:
            on_progress({"phase": "observing", "completed": index, "total": len(decisions)})
        state = _object(decision.get("request_snapshot"))
        policy = next((item for item in reversed(_object(decision.get("checks_json")) or [])
                       if isinstance(item, dict) and item.get("name") == "entry_policy"), {})
        stamp = pd.Timestamp(decision["created_at"])
        stamp = stamp.tz_localize("UTC") if stamp.tzinfo is None else stamp.tz_convert("UTC")
        target = stamp.to_pydatetime() + timedelta(hours=horizon)
        billing = _object(decision.get("billing_json"))
        row = {"decision_id": decision["decision_uid"], "decision_at": stamp.isoformat(),
               "raw_allowed": bool(policy.get("raw_allowed", True)), "latency_ms": decision.get("latency_ms", 0),
               "charged_credits": float(billing.get("charged") or 0) - float(billing.get("refunded") or 0),
               "net_return": None, "missing_reason": "horizonNotReached"}
        output.append(row)
        if target > now:
            continue
        try:
            if not policy.get("valid_decision"):
                raise ValueError("validShadowDecisionUnavailable")
            instrument = (state.get("context") or {}).get("instrument") or {}
            market = instrument.get("market") or strategy.get("market_category")
            symbol = str(decision.get("symbol") or "")
            suffix = symbol.split("@", 1)[1] if "@" in symbol else ""
            if ":" in symbol and symbol.split(":", 1)[0] in {"Crypto", "USStock", "HKStock", "CNStock", "Forex"}:
                market, symbol = symbol.split(":", 1)
                symbol = symbol.split("@", 1)[0]
            if market not in {"Crypto", "USStock", "HKStock", "CNStock", "Forex"}:
                raise ValueError("instrumentMarketMissing")
            exchange = suffix.split(":", 1)[0] or instrument.get("exchange_id") or ""
            key = (market, symbol, exchange, decision.get("market_type"), target.isoformat())
            if key not in cache:
                frame = loader(market, symbol, "1h", target - timedelta(hours=2),
                               min(now, target + timedelta(days=4)),
                               market_type=decision.get("market_type"), exchange_id=exchange)
                cache[key] = frame
            frame = cache[key]
            closed_at = pd.to_datetime(frame.index, utc=True) + pd.Timedelta(hours=1)
            visible = frame.loc[(closed_at >= pd.Timestamp(target)) & (closed_at <= pd.Timestamp(now))
                                & (closed_at <= pd.Timestamp(target + timedelta(days=4)))]
            if visible.empty:
                raise ValueError("closedHorizonBarUnavailable")
            price = finite_number(visible.iloc[0]["close"])
            reference = finite_number(state.get("reference_price", 0))
            if reference <= 0 or price <= 0:
                raise ValueError("priceUnavailable")
            action = str(decision.get("action") or "")
            if action not in {"open_long", "add_long", "open_short", "add_short", "buy"}:
                raise ValueError("unsupportedCounterfactualAction")
            config = _object(strategy.get("trading_config"))
            costs = (state.get("context") or {}).get("evaluation_costs") or config
            commission, slippage = finite_number(costs.get("commission", 0.0005)), finite_number(costs.get("slippage", 0.0005))
            if not 0 <= commission < 1 or not 0 <= slippage < 1:
                raise ValueError("invalidCostAssumptions")
            short = action.endswith("_short")
            entry = reference * (1 - slippage if short else 1 + slippage)
            exit_price = price * (1 + slippage if short else 1 - slippage)
            gross = (entry - exit_price if short else exit_price - entry) / reference
            net = gross - commission * (entry + exit_price) / reference
            row.update(net_return=net, filtered_return=net if row["raw_allowed"] else 0.0,
                       observed_price=price, observed_at=closed_at[frame.index.get_loc(visible.index[0])].isoformat(),
                       commission=commission, slippage=slippage, missing_reason=None,
                       data_snapshot=MarketDataSnapshotStore().save(frame), market=market, symbol=symbol)
        except Exception as exc:
            row["missing_reason"] = str(exc)[:200]
    return summarize_shadow(output, horizon_hours=horizon)


def run_shadow_job(payload, on_progress):
    return evaluate_strategy_shadow(user_id=int(payload["__userId"]), strategy_id=int(payload["strategyId"]),
                                    horizon_hours=payload.get("horizonHours", 24), limit=payload.get("limit", 100),
                                    on_progress=on_progress)
