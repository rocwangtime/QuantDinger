"""Dated covariance and empirical tail risk; no normal-return assumption."""

import math
from datetime import datetime, timezone

import numpy as np
import pandas as pd


def finite_number(value):
    number = float(value)
    if not math.isfinite(number):
        raise ValueError("portfolioRisk.nonFiniteInput")
    return number


def analyze_portfolio(*, returns, weights, shrinkage=0.2, annual_periods=252, shocks=None):
    if not isinstance(returns, dict) or not isinstance(weights, dict) or not weights or len(weights) > 100:
        raise ValueError("portfolioRisk.invalidPanel")
    symbols = sorted(weights)
    missing = [symbol for symbol in symbols if symbol not in returns]
    if missing:
        return {"available": False, "reason": "missingAssetReturns", "missing": missing}
    series = {}
    for symbol in symbols:
        values = returns[symbol]
        if not isinstance(values, dict) or len(values) > 5000:
            raise ValueError("portfolioRisk.datedReturnsRequired")
        index = pd.to_datetime(list(values), utc=True, errors="raise")
        if index.normalize().duplicated().any() or any(index > pd.Timestamp.now(tz="UTC")):
            raise ValueError("portfolioRisk.invalidDates")
        series[symbol] = pd.Series([finite_number(v) for v in values.values()], index=index)
    panel = pd.DataFrame(series).sort_index().dropna()
    vector = np.array([finite_number(weights[symbol]) for symbol in symbols])
    if np.abs(vector).sum() > 10:
        raise ValueError("portfolioRisk.weightLimitExceeded")
    if len(panel) < 30:
        return {"available": False, "reason": "insufficientAlignedObservations", "observations": len(panel)}
    coefficient = finite_number(shrinkage)
    if not 0 <= coefficient <= 1 or int(annual_periods) not in {252, 365}:
        raise ValueError("portfolioRisk.invalidConfiguration")
    covariance = panel.cov().to_numpy()
    covariance = (1 - coefficient) * covariance + coefficient * np.diag(np.diag(covariance))
    contributions = vector * (covariance @ vector)
    variance = max(0.0, float(contributions.sum()))
    daily = panel.to_numpy() @ vector
    threshold = float(np.quantile(daily, 0.05))
    tail = daily[daily <= threshold]
    scenario = shocks or {symbol: -0.1 for symbol in symbols}
    stress = sum(finite_number(weights[symbol]) * finite_number(scenario.get(symbol, 0)) for symbol in symbols)
    return {
        "available": True, "observations": len(panel), "as_of": panel.index[-1].isoformat(),
        "gross_weight": float(np.abs(vector).sum()), "net_weight": float(vector.sum()),
        "daily_volatility": math.sqrt(variance), "annualized_volatility": math.sqrt(variance * int(annual_periods)),
        "empirical_var_95": max(0.0, -threshold), "empirical_expected_shortfall_95": max(0.0, -float(tail.mean())),
        "stress_return": stress, "stress_shocks": scenario,
        "risk_contributions": dict(zip(symbols, map(float, contributions))),
        "model": {"symbols": symbols, "covariance": covariance.tolist(), "as_of": panel.index[-1].isoformat(),
                  "shrinkage": coefficient, "period": "daily"},
        "limitations": ["Historical covariance can change", "Caller must supply daily returns in one valuation currency"],
    }


def projected_model_risk(model, *, notionals, capital, max_age_hours=96, now=None):
    capital = finite_number(capital)
    if not model or capital <= 0:
        return {"available": False, "reason": "portfolioModelUnavailable"}
    timestamp = pd.Timestamp(model["as_of"])
    timestamp = timestamp.tz_localize("UTC") if timestamp.tzinfo is None else timestamp.tz_convert("UTC")
    age = ((now or datetime.now(timezone.utc)) - timestamp.to_pydatetime()).total_seconds()
    if age < -2 or age > finite_number(max_age_hours) * 3600:
        return {"available": False, "reason": "portfolioModelStale"}
    symbols = model["symbols"]
    if not symbols or len(symbols) != len(set(symbols)) or len(symbols) > 100:
        raise ValueError("portfolioRisk.invalidModel")
    missing = [symbol for symbol, value in notionals.items() if value and symbol not in symbols]
    if missing:
        return {"available": False, "reason": "portfolioModelCoverageMissing", "missing": missing}
    matrix = np.asarray(model["covariance"], dtype=float)
    if matrix.shape != (len(symbols), len(symbols)) or not np.isfinite(matrix).all():
        raise ValueError("portfolioRisk.invalidModel")
    if model.get("period") != "daily" or not np.allclose(matrix, matrix.T) or np.linalg.eigvalsh(matrix).min() < -1e-10:
        raise ValueError("portfolioRisk.invalidModel")
    vector = np.array([finite_number(notionals.get(symbol, 0)) / capital for symbol in symbols])
    return {"available": True, "daily_volatility": math.sqrt(max(0.0, float(vector @ matrix @ vector))),
            "model_as_of": model["as_of"]}
