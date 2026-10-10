"""Application service that adapts Strategy API V2 to the evolution engine."""

from __future__ import annotations

import copy
import uuid
from datetime import datetime
from typing import Any

from app.services.script_source import get_script_source_service
from app.services.strategy_v2 import StrategyV2BacktestService
from app.services.portfolio.risk import finite_number

from .constraints import discover_parameter_constraints
from .engine import StrategyEvolutionEngine
from .evaluator import PreparedEvolutionEvaluator
from .models import EvolutionConfig, SearchParameter
from .parameter_space import adapt_source_for_parameters, build_parameter_space
from .search import ParameterSampler


class StrategyEvolutionService:
    def __init__(self, *, backtest_service: StrategyV2BacktestService | None = None) -> None:
        self.backtest_service = backtest_service or StrategyV2BacktestService()

    def prepare_submission(self, *, user_id: int, payload: dict[str, Any]) -> dict[str, Any]:
        from .bundles import content_hash
        clean = {key: value for key, value in payload.items() if not key.startswith("__")}
        source_id = _positive_int(clean.get("sourceId"))
        source = get_script_source_service().get_source(source_id, user_id=user_id) if source_id else None
        if not source:
            raise ValueError("strategyEvolution.sourceNotFound")
        frozen = copy.deepcopy(source)
        return {**clean, "__frozenSource": frozen, "__codeHash": content_hash(str(frozen.get("code") or "")),
                "__studyId": str(uuid.uuid4())}

    def run(self, *, user_id: int, payload: dict[str, Any], on_progress=None) -> dict[str, Any]:
        source_id = _positive_int(payload.get("sourceId"))
        if not source_id:
            raise ValueError("strategyEvolution.sourceRequired")
        source = payload.get("__frozenSource") or get_script_source_service().get_source(source_id, user_id=user_id)
        if not source:
            raise ValueError("strategyEvolution.sourceNotFound")
        from .bundles import content_hash
        if payload.get("__codeHash") and payload["__codeHash"] != content_hash(str(source.get("code") or "")):
            raise ValueError("strategyEvolution.frozenSourceHashMismatch")
        code = str(source.get("code") or "").strip()
        if not code:
            raise ValueError("strategyEvolution.sourceCodeRequired")
        start_date = _date(payload.get("startDate"), "strategyEvolution.startDateRequired")
        end_date = _date(payload.get("endDate"), "strategyEvolution.endDateRequired", end_of_day=True)
        config = EvolutionConfig.from_payload(payload.get("config"))
        parameter_rows = self._parameter_rows(source, payload.get("parameterSpace"))
        parameters = [SearchParameter.from_payload(item) for item in parameter_rows]
        code = adapt_source_for_parameters(code, parameter_rows)
        constraints = discover_parameter_constraints(code, {item.name for item in parameters})
        initial_capital = max(10.0, finite_number(payload.get("initialCapital") or 10_000))
        leverage_enabled = bool(payload.get("leverageEnabled", False))
        leverage = max(1.0, finite_number(payload.get("leverage") or 1.0))
        commission = max(0.0, min(1.0, finite_number(payload.get("commission") or 0.0)))
        slippage = max(0.0, min(1.0, finite_number(payload.get("slippage") or 0.0)))

        if isinstance(self.backtest_service, StrategyV2BacktestService):
            evaluate = PreparedEvolutionEvaluator(
                backtest_service=self.backtest_service,
                user_id=user_id,
                code=code,
                start_date=start_date,
                end_date=end_date,
                initial_capital=initial_capital,
                leverage_enabled=leverage_enabled,
                leverage=leverage,
                source_id=source_id,
                strategy_name=str(source.get("name") or ""),
                bundle_id=str(payload.get("__bundleId") or ""),
            )
        else:
            def evaluate(params, segment_start, segment_end, segment_commission, segment_slippage):
                _run_id, segment_result = self.backtest_service.run(
                    user_id=user_id,
                    code=code,
                    start_date=segment_start,
                    end_date=segment_end,
                    initial_capital=initial_capital,
                    leverage_enabled=leverage_enabled,
                    leverage=leverage,
                    commission=segment_commission,
                    slippage=segment_slippage,
                    params=params,
                    persist=False,
                    source_id=source_id,
                    strategy_name=str(source.get("name") or ""),
                )
                return segment_result

        history = None
        previous_trials = 0
        if payload.get("__studyId"):
            from .history import ResearchHistory
            history = ResearchHistory(user_id=user_id, source_id=source_id, study_id=payload["__studyId"])
            previous_trials = history.reserve(code_hash=content_hash(code), trials=config.trials)
            if getattr(evaluate, "bundle_id", ""):
                history.attach_bundle(evaluate.bundle_id)
        result = StrategyEvolutionEngine(evaluate).run(
            parameters=parameters,
            config=config,
            start_date=start_date,
            end_date=end_date,
            commission=commission,
            slippage=slippage,
            constraints=constraints,
            on_progress=on_progress,
            previous_trials=previous_trials,
            holdout_tracker=history.expose_holdout if history else None,
        )
        result["source"] = {"id": source_id, "name": str(source.get("name") or "")}
        result["economicAssumptions"] = {"initialCapital": initial_capital, "leverage": leverage if leverage_enabled else 1.0,
                                         "commission": commission, "slippage": slippage}
        result["reproducibility"] = {**getattr(evaluate, "bundle_metadata", {}),
                                     "sourceHash": content_hash(str(source.get("code") or "")),
                                     "sourceFrozenAtSubmission": bool(payload.get("__frozenSource")),
                                     "historyTracked": history is not None,
                                     "studyId": payload.get("__studyId")}
        if history is None:
            result["promotion"]["eligible"] = False
            result["promotion"]["reasons"].append("researchHistoryUnavailable")
        if commission <= 0 or slippage <= 0:
            result["promotion"]["eligible"] = False
            result["promotion"]["reasons"].append("executionCostsUnverified")
        return result

    @staticmethod
    def parameter_space(*, user_id: int, source_id: int) -> dict[str, Any]:
        source = get_script_source_service().get_source(source_id, user_id=user_id)
        if not source:
            raise ValueError("strategyEvolution.sourceNotFound")
        return build_parameter_space(source)

    @staticmethod
    def estimate(payload: dict[str, Any]) -> dict[str, Any]:
        config = EvolutionConfig.from_payload(payload.get("config"))
        parameters = [SearchParameter.from_payload(item) for item in (payload.get("parameterSpace") or [])]
        parameter_count = len(parameters)
        trial_count = config.trials
        if config.method == "grid" and parameters:
            trial_count = ParameterSampler(parameters, seed=config.seed).grid_size(config.trials)
        fold_runs = trial_count
        stress_runs = len(config.cost_multipliers)
        total_runs = fold_runs + stress_runs + (1 if config.blind_ratio > 0 else 0)
        universe_size = max(1, min(5000, int(payload.get("universeSize") or 1)))
        relative_units = total_runs * universe_size
        tier = "low" if relative_units < 500 else "medium" if relative_units < 5000 else "high"
        return {
            "backtestRuns": total_runs,
            "relativeUnits": relative_units,
            "resourceTier": tier,
            "parameterCount": parameter_count,
            "trials": trial_count,
            "requestedTrials": config.trials,
            "folds": config.folds,
            "walkForwardMode": "singlePass",
            "monteCarloPaths": config.monte_carlo_paths,
            "isUpperBound": config.auto_prune,
        }

    @staticmethod
    def _parameters(source: dict[str, Any], raw: Any) -> list[SearchParameter]:
        return [SearchParameter.from_payload(item) for item in StrategyEvolutionService._parameter_rows(source, raw)]

    @staticmethod
    def _parameter_rows(source: dict[str, Any], raw: Any) -> list[dict[str, Any]]:
        definitions = list(build_parameter_space(source).get("parameters") or [])
        allowed = {str(item.get("name") or ""): item for item in definitions}
        requested = list(
            (item for item in definitions if item.get("optimizable", True))
            if raw is None
            else (raw or [])
        )
        output: list[dict[str, Any]] = []
        for item in requested:
            name = str(item.get("name") or "")
            if name not in allowed:
                raise ValueError("strategyEvolution.parameterNotDeclared")
            declared = dict(allowed[name])
            if not declared.get("optimizable", True):
                raise ValueError("strategyEvolution.parameterNotOptimizable")
            merged = dict(declared)
            kind = str(declared.get("type") or "").lower()
            if kind in {"int", "integer", "float", "number", "percent"}:
                declared_min = declared.get("min")
                declared_max = declared.get("max")
                requested_min = item.get("min", declared_min)
                requested_max = item.get("max", declared_max)
                if declared_min is None or declared_max is None:
                    raise ValueError("strategyEvolution.parameterRangeUndeclared")
                if float(requested_min) < float(declared_min) or float(requested_max) > float(declared_max):
                    raise ValueError("strategyEvolution.parameterRangeOutsideDeclaration")
                merged["min"] = requested_min
                merged["max"] = requested_max
            option_rows = merged.get("options") or merged.get("values")
            if option_rows and not merged.get("choices"):
                merged["choices"] = [option.get("value") if isinstance(option, dict) else option for option in option_rows]
            SearchParameter.from_payload(merged)
            output.append(merged)
        return output


def _date(value: Any, error_code: str, *, end_of_day: bool = False) -> datetime:
    text = str(value or "").strip()
    if not text:
        raise ValueError(error_code)
    result = datetime.strptime(text, "%Y-%m-%d")
    return result.replace(hour=23, minute=59, second=59) if end_of_day else result


def _positive_int(value: Any) -> int | None:
    try:
        result = int(value)
        return result if result > 0 else None
    except (TypeError, ValueError):
        return None
