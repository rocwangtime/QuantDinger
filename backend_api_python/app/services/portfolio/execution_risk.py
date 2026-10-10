"""Opt-in covariance entry guard, including conservative queued reservations."""

import json
import math
from contextlib import closing

from app.services.live_trading.account_risk import account_risk_snapshot, _strategy_credential_id
from app.services.live_trading.records import normalize_strategy_symbol
from app.services.portfolio.risk import finite_number, projected_model_risk
from app.utils.db import get_db_connection

ENTRIES = {"open_long", "add_long", "open_short", "add_short"}


def mapping(value):
    return json.loads(value) if isinstance(value, str) else value or {}


def enforce_portfolio_entry(*, user_id, strategy_id, action, symbol, quantity, price, pending_id=0):
    """Caller owns the transaction; serialize admission within the account.

    Pending hedges cannot reduce reported risk until filled. The triangle bound
    deliberately overestimates possible risk when several orders are outstanding.
    """
    if action not in ENTRIES:
        return
    with get_db_connection() as db:
        with closing(db.cursor()) as cur:
            cur.execute("SELECT * FROM qd_strategies_trading WHERE id=%s AND user_id=%s", (strategy_id, user_id))
            strategy = cur.fetchone() or {}
            if not strategy:
                raise ValueError("portfolioRisk.strategyNotFound")
            policy = mapping(strategy.get("trading_config")).get("portfolio_risk") or {}
            if not policy:
                return
            credential = _strategy_credential_id(strategy)
            if not credential:
                raise ValueError("portfolioRisk.credentialRequired")
            market_type = strategy.get("market_type") or "spot"
            cur.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
                        (f"portfolio-risk:{user_id}:{credential}:{market_type}",))
            cur.execute("SELECT s.id,s.exchange_config,s.trading_config FROM qd_strategies_trading s "
                        "WHERE s.user_id=%s AND s.execution_mode='live' AND s.market_type=%s "
                        "AND (s.status='running' OR s.id=%s "
                        "OR EXISTS(SELECT 1 FROM qd_strategy_positions p WHERE p.strategy_id=s.id AND p.size>0) "
                        "OR EXISTS(SELECT 1 FROM pending_orders p WHERE p.strategy_id=s.id "
                        "AND p.status IN ('pending','processing','sent','syncing','reconciling')))",
                        (user_id, market_type, strategy_id))
            currencies = set()
            for row in cur.fetchall() or []:
                if _strategy_credential_id(row) != credential:
                    continue
                currency = str(mapping(row.get("trading_config")).get("quote_currency") or "").strip().upper()
                if not currency:
                    raise ValueError("portfolioRisk.valuationCurrencyMissing")
                currencies.add(currency)
            if len(currencies) != 1:
                raise ValueError("portfolioRisk.mixedValuationCurrencyUnsupported")
            snapshot = account_risk_snapshot(user_id=user_id, credential_id=credential, market_type=market_type,
                                              strategy_id=strategy_id)
            if snapshot["unpriced_position_count"]:
                raise ValueError("portfolioRisk.positionPriceMissing")
            capital = finite_number(snapshot["capital_budget"])
            current = snapshot["symbol_net_notional"]
            projected = dict(current)
            proposed = finite_number(quantity) * finite_number(price)
            if proposed <= 0 or capital <= 0:
                raise ValueError("portfolioRisk.invalidValuation")
            key = normalize_strategy_symbol(symbol)
            projected[key] = projected.get(key, 0) + (-proposed if action.endswith("_short") else proposed)
            model = policy.get("portfolio_model")
            options = {"capital": capital, "max_age_hours": policy.get("portfolio_model_max_age_hours", 96)}
            base = projected_model_risk(model, notionals=current, **options)
            after = projected_model_risk(model, notionals=projected, **options)
            if not base["available"] or not after["available"]:
                raise ValueError("portfolioRisk.modelUnavailable")
            cur.execute("SELECT p.symbol,p.signal_type,p.amount,p.price,s.exchange_config,s.trading_config "
                        "FROM pending_orders p JOIN qd_strategies_trading s ON s.id=p.strategy_id "
                        "WHERE p.user_id=%s AND p.execution_mode='live' AND p.id<>%s "
                        "AND p.status IN ('pending','processing','sent','syncing','reconciling') "
                        "AND p.signal_type IN ('open_long','add_long','open_short','add_short') "
                        "AND s.market_type=%s", (user_id, pending_id, market_type))
            reservations = {}
            for row in cur.fetchall() or []:
                if _strategy_credential_id(row) != credential:
                    continue
                name = normalize_strategy_symbol(row["symbol"])
                if name not in model["symbols"]:
                    raise ValueError("portfolioRisk.pendingAssetUncovered")
                notional = finite_number(row["amount"]) * finite_number(row["price"])
                if notional <= 0:
                    raise ValueError("portfolioRisk.pendingPriceMissing")
                directions = reservations.setdefault(name, {"long": 0, "short": 0})
                directions["short" if row["signal_type"].endswith("_short") else "long"] += notional
            pending_bound = 0.0
            for name, sides in reservations.items():
                index = model["symbols"].index(name)
                variance = finite_number(model["covariance"][index][index])
                pending_bound += max(sides.values()) * math.sqrt(max(0, variance)) / capital
            estimate = max(base["daily_volatility"], after["daily_volatility"]) + pending_bound
            if estimate > finite_number(policy["max_portfolio_daily_volatility"]):
                raise ValueError("portfolioRisk.volatilityExceeded")
            return {"daily_volatility_upper_bound": estimate, "pending_volatility_bound": pending_bound,
                    "method": "covarianceWithPendingTriangleBound", "capital": capital,
                    "valuation_currency": next(iter(currencies))}
