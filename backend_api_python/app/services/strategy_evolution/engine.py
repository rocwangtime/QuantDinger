"""Optimization engine independent from HTTP and existing backtest routes."""

from __future__ import annotations

from collections import Counter
from collections.abc import Callable
from dataclasses import asdict
from datetime import datetime
from statistics import mean, median
from time import perf_counter
from typing import Any

from .constraints import ParameterConstraint, first_rejection
from .models import EvolutionConfig, SearchParameter
from .search import ParameterSampler
from .statistics import (
    composite_score,
    deflated_sharpe,
    equity_returns,
    metric_snapshot,
    monte_carlo_bootstrap,
    parameter_heatmap,
    probability_of_backtest_overfitting,
    return_matrix_pbo,
    sample_sharpe,
)
from .walk_forward import WalkForwardFold, build_walk_forward_plan


Evaluator = Callable[[dict[str, Any], datetime, datetime, float, float], dict[str, Any]]
ProgressCallback = Callable[[dict[str, Any]], None]


class StrategyEvolutionFailure(ValueError):
    """A user-facing evolution failure with structured diagnostics."""

    def __init__(self, code: str, details: dict[str, Any]) -> None:
        super().__init__(code)
        self.code = code
        self.details = details


class StrategyEvolutionEngine:
    def __init__(self, evaluator: Evaluator) -> None:
        self.evaluator = evaluator

    def run(
        self,
        *,
        parameters: list[SearchParameter],
        config: EvolutionConfig,
        start_date: datetime,
        end_date: datetime,
        commission: float,
        slippage: float,
        constraints: tuple[ParameterConstraint, ...] = (),
        on_progress: ProgressCallback | None = None,
        previous_trials: int = 0,
        holdout_tracker=None,
    ) -> dict[str, Any]:
        started_at = perf_counter()
        if not parameters:
            raise ValueError("strategyEvolution.parametersRequired")
        if len(parameters) > 8:
            raise ValueError("strategyEvolution.parameterLimitExceeded")
        context_provider = getattr(self.evaluator, "walk_forward_context", None)
        walk_forward_context = (
            context_provider(start_date, end_date)
            if callable(context_provider)
            else {}
        )
        plan = build_walk_forward_plan(
            start_date,
            end_date,
            folds=config.folds,
            train_ratio=config.train_ratio,
            blind_ratio=config.blind_ratio,
            observations=walk_forward_context.get("observations"),
            warmup_bars=max(
                int(walk_forward_context.get("warmupBars") or 0),
                _parameter_lookback_bars(parameters),
            ),
            embargo_bars=config.embargo_bars,
        )
        sampler = ParameterSampler(parameters, seed=config.seed)
        grid = sampler.grid(config.trials) if config.method == "grid" else []
        trial_count = len(grid) if config.method == "grid" else config.trials
        history: list[tuple[dict[str, Any], float]] = []
        trials: list[dict[str, Any]] = []
        seen: set[tuple[tuple[str, str], ...]] = set()
        for trial_index in range(trial_count):
            params = self._candidate(config, sampler, grid, history, trial_index)
            identity = tuple(sorted((str(key), repr(value)) for key, value in params.items()))
            rejection = first_rejection(constraints, params)
            if rejection is not None:
                trial = self._skipped_trial(trial_index + 1, params, "parameterConstraint")
                trial["constraint"] = rejection.metadata()
            elif identity in seen:
                trial = self._skipped_trial(trial_index + 1, params, "duplicateCandidate")
            else:
                seen.add(identity)
                trial = self._evaluate_trial(
                    trial_index + 1,
                    params,
                    plan.folds,
                    config,
                    commission,
                    slippage,
                    [float(row[1]) for row in history],
                )
            trials.append(trial)
            if not trial["pruned"]:
                history.append((params, float(trial["score"])))
            if on_progress:
                on_progress({
                    "phase": "search",
                    "completed": trial_index + 1,
                    "total": trial_count,
                    "bestScore": max((row["score"] for row in trials if not row["pruned"]), default=0.0),
                })
        completed = sorted(
            (row for row in trials if not row["pruned"] and row["components"].get("validationActivity", 0) > 0),
            key=lambda row: row["score"],
            reverse=True,
        )
        if not completed:
            raise StrategyEvolutionFailure(
                "strategyEvolution.allTrialsPruned",
                self._failure_diagnostics(trials),
            )
        best = completed[0]
        blind_result = None
        holdout_exposures = 0
        if plan.blind_start and plan.blind_end:
            if holdout_tracker:
                holdout_exposures = holdout_tracker(plan.blind_start, plan.blind_end)
            blind_result = self.evaluator(best["params"], plan.blind_start, plan.blind_end, commission, slippage)
        final_result = blind_result or best["validationResults"][-1]
        diagnostic_results = list(best["validationResults"])
        if blind_result:
            diagnostic_results.append(blind_result)
        returns = [
            value
            for diagnostic_result in diagnostic_results
            for value in equity_returns(diagnostic_result.get("equityCurve") or [])
        ]
        score_matrix = [[float(item["score"]) for item in row["folds"] if "score" in item] for row in completed]
        fold_proxy = probability_of_backtest_overfitting(score_matrix)
        pbo = return_matrix_pbo([row["validationResults"] for row in trials if row.get("folds")])
        pbo["foldRankDiagnostic"] = {**fold_proxy, "method": "foldReturnRankProxy", "selectedCandidatesOnly": True}
        dsr = deflated_sharpe(
            returns,
            observed_sharpe=sample_sharpe(returns),
            trials=max(1, previous_trials + sum(bool(row.get("folds")) for row in trials)),
        )
        dsr.update(trialCountMethod="conservativeCumulativeAttempts", previousTrials=previous_trials,
                   attemptedTrials=sum(bool(row.get("folds")) for row in trials),
                   effectiveTrials=max(1, previous_trials + sum(bool(row.get("folds")) for row in trials)))
        monte_carlo = monte_carlo_bootstrap(
            returns,
            paths=config.monte_carlo_paths,
            block_size=config.block_size,
            seed=config.seed + 991,
        )
        cost_stress = self._cost_stress(
            best["params"],
            plan.blind_start or start_date,
            plan.blind_end or end_date,
            commission,
            slippage,
            config.cost_multipliers,
        )
        robustness = self._robustness_grade(pbo, dsr, monte_carlo, cost_stress)
        evidence_reasons = []
        if robustness["availableChecks"] != robustness["totalChecks"]:
            evidence_reasons.append("missingRobustnessChecks")
        if not blind_result:
            evidence_reasons.append("holdoutUnavailable")
        elif len(equity_returns(blind_result.get("equityCurve") or [])) < 30:
            evidence_reasons.append("insufficientHoldoutObservations")
        if holdout_exposures:
            evidence_reasons.append("holdoutPreviouslyExposed")
        if len(returns) < 60:
            evidence_reasons.append("insufficientReturnObservations")
        if sum(int(row.get("totalTrades") or 0) for row in diagnostic_results) < 20:
            evidence_reasons.append("insufficientClosedTrades")
        if not dsr.get("available") or float(dsr.get("probability") or 0) < 0.95:
            evidence_reasons.append("deflatedSharpeBelowThreshold")
        if not pbo.get("available") or float(pbo.get("probability") or 0) > 0.2:
            evidence_reasons.append("overfittingRisk")
        if not cost_stress or float(cost_stress[-1].get("return") or 0) <= 0:
            evidence_reasons.append("costStressNonPositive")
        return {
            "status": "complete",
            "method": config.method,
            "config": asdict(config),
            "plan": {
                **plan.metadata(),
                "evaluationMethod": "fixedParameterWindowValidation",
                "rollingRefit": False,
                "holdoutPriorExposures": holdout_exposures,
                "frequency": walk_forward_context.get("frequency"),
            },
            "summary": {
                "bestScore": best["score"],
                "robustnessScore": robustness["score"],
                "robustnessGrade": robustness["grade"],
                "availableChecks": robustness["availableChecks"],
                "totalChecks": robustness["totalChecks"],
                "oosReturn": best["components"]["oosReturn"],
                "oosSharpe": best["components"]["oosSharpe"],
                "maxDrawdown": best["components"]["maxDrawdown"],
                "decayRate": best["components"]["decayRate"],
                "completedTrials": len(completed),
                "prunedTrials": sum(1 for row in trials if row["pruned"]),
                "constraintRejectedTrials": sum(1 for row in trials if row.get("reason") == "parameterConstraint"),
                "duplicateTrials": sum(1 for row in trials if row.get("reason") == "duplicateCandidate"),
                "actualBacktestRuns": self._evaluation_count(),
                "elapsedSeconds": round(perf_counter() - started_at, 2),
            },
            "bestParams": best["params"],
            "trials": [self._public_trial(row) for row in trials],
            "topCandidates": [self._public_trial(row) for row in completed[: config.top_candidates]],
            "convergence": self._convergence(trials),
            "heatmap": parameter_heatmap(trials),
            "equityCurve": final_result.get("equityCurve") or [],
            "validation": {
                "pbo": pbo,
                "deflatedSharpe": dsr,
                "monteCarlo": monte_carlo,
                "costStress": cost_stress,
            },
            "promotion": {"eligible": not evidence_reasons, "reasons": evidence_reasons,
                          "minimumObservations": 60, "minimumClosedTrades": 20,
                          "minimumHoldoutObservations": 30,
                          "statisticalApprovalIsNotBrokerApproval": True},
            "constraints": [constraint.metadata() for constraint in constraints],
        }

    def _evaluate_trial(
        self,
        number: int,
        params: dict[str, Any],
        folds: tuple[WalkForwardFold, ...],
        config: EvolutionConfig,
        commission: float,
        slippage: float,
        prior_scores: list[float],
    ) -> dict[str, Any]:
        train_metrics: list[dict[str, float]] = []
        validation_metrics: list[dict[str, float]] = []
        validation_results: list[dict[str, Any]] = []
        fold_rows: list[dict[str, Any]] = []
        pruned = False
        evaluate_plan = getattr(self.evaluator, "evaluate_plan", None)
        prepared_results = evaluate_plan(params, folds, commission, slippage) if callable(evaluate_plan) else None
        for fold_index, fold in enumerate(folds):
            if prepared_results is not None:
                train_result, validation_result = prepared_results[fold_index]
            else:
                train_result = self.evaluator(params, fold.train_start, fold.train_end, commission, slippage)
                validation_result = self.evaluator(params, fold.validation_start, fold.validation_end, commission, slippage)
            train_metric = metric_snapshot(train_result)
            validation_metric = metric_snapshot(validation_result)
            train_metrics.append(train_metric)
            validation_metrics.append(validation_metric)
            validation_results.append(validation_result)
            partial_score, _ = composite_score(train_metrics, validation_metrics, config.weights)
            fold_rows.append({
                **fold.metadata(),
                "train": train_metric,
                "validation": validation_metric,
                "score": validation_metric["return"],
                "selectionScore": partial_score,
            })
            no_activity = (
                train_metric.get("activity", 0) <= 0
                and validation_metric.get("activity", 0) <= 0
            )
            invalid_metrics = train_metric.get("valid", 0) <= 0 or validation_metric.get("valid", 0) <= 0
            if invalid_metrics:
                pruned = True
                break
            if config.auto_prune and no_activity:
                pruned = True
                break
            if config.auto_prune and len(fold_rows) >= 2 and len(prior_scores) >= 6:
                threshold = median(prior_scores) - 8.0
                catastrophic = validation_metric["drawdown"] <= -70.0
                if partial_score < threshold or catastrophic:
                    pruned = True
                    break
        score, components = composite_score(train_metrics, validation_metrics, config.weights)
        reason = (
            "invalidMetrics"
            if any(row.get("valid", 0) <= 0 for row in train_metrics + validation_metrics)
            else "noActivity"
            if components.get("validationActivity", 0) <= 0
            else "autoPruned"
            if pruned
            else ""
        )
        pruned = pruned or components.get("validationActivity", 0) <= 0
        return {
            "number": number,
            "params": params,
            "score": score,
            "components": components,
            "folds": fold_rows,
            "pruned": pruned,
            "reason": reason,
            "validationResults": validation_results,
        }

    @staticmethod
    def _skipped_trial(number: int, params: dict[str, Any], reason: str) -> dict[str, Any]:
        return {
            "number": number,
            "params": params,
            "score": 0.0,
            "components": {
                "oosReturn": 0.0,
                "oosSharpe": 0.0,
                "maxDrawdown": 0.0,
                "decayRate": 0.0,
                "validationActivity": 0.0,
                "activeValidationFolds": 0,
                "activityEvidence": 0.0,
            },
            "folds": [],
            "pruned": True,
            "reason": reason,
            "validationResults": [],
        }

    def _evaluation_count(self) -> int | None:
        stats = getattr(self.evaluator, "stats", None)
        if isinstance(stats, dict):
            return int(stats.get("backtestRuns") or 0)
        return None

    def _failure_diagnostics(self, trials: list[dict[str, Any]]) -> dict[str, Any]:
        reason_counts = Counter(str(row.get("reason") or "unknown") for row in trials)
        evaluated = [
            row for row in trials
            if row.get("reason") not in {"parameterConstraint", "duplicateCandidate"}
        ]
        active_training_trials = sum(
            1 for row in evaluated
            if any(float((fold.get("train") or {}).get("activity") or 0) > 0 for fold in row.get("folds") or [])
        )
        active_validation_trials = sum(
            1 for row in evaluated
            if float((row.get("components") or {}).get("validationActivity") or 0) > 0
        )
        dominant_reason = max(reason_counts, key=reason_counts.get) if reason_counts else "unknown"
        message_keys = {
            "noActivity": "strategyEvolution.failure.noValidationActivity",
            "parameterConstraint": "strategyEvolution.failure.parameterConstraints",
            "invalidMetrics": "strategyEvolution.failure.invalidMetrics",
            "duplicateCandidate": "strategyEvolution.failure.duplicateCandidates",
            "autoPruned": "strategyEvolution.failure.autoPruned",
        }
        return {
            "messageKey": message_keys.get(dominant_reason, "strategyEvolution.allTrialsPruned"),
            "dominantReason": dominant_reason,
            "totalTrials": len(trials),
            "evaluatedTrials": len(evaluated),
            "activeTrainingTrials": active_training_trials,
            "activeValidationTrials": active_validation_trials,
            "actualBacktestRuns": self._evaluation_count(),
            "reasonCounts": dict(reason_counts),
        }

    def _cost_stress(
        self,
        params: dict[str, Any],
        start: datetime,
        end: datetime,
        commission: float,
        slippage: float,
        multipliers: tuple[float, ...],
    ) -> list[dict[str, float]]:
        output = []
        for multiplier in multipliers:
            result = self.evaluator(params, start, end, commission * multiplier, slippage * multiplier)
            metrics = metric_snapshot(result)
            output.append({
                "multiplier": multiplier,
                "commission": commission * multiplier,
                "slippage": slippage * multiplier,
                **metrics,
            })
        if output:
            baseline_return = output[0]["return"]
            for row in output:
                row["returnDelta"] = round(row["return"] - baseline_return, 6)
        return output

    @staticmethod
    def _candidate(config, sampler, grid, history, index):
        if config.method == "grid":
            return grid[index % len(grid)]
        if config.method == "tpe":
            return sampler.tpe_candidate(history)
        return sampler.random_candidate()

    @staticmethod
    def _public_trial(trial: dict[str, Any]) -> dict[str, Any]:
        return {key: value for key, value in trial.items() if key != "validationResults"}

    @staticmethod
    def _convergence(trials: list[dict[str, Any]]) -> list[dict[str, float | int | bool]]:
        best = 0.0
        output = []
        for row in trials:
            if not row["pruned"]:
                best = max(best, float(row["score"]))
            output.append({"trial": row["number"], "score": row["score"], "best": round(best, 4), "pruned": row["pruned"]})
        return output

    @staticmethod
    def _robustness_grade(pbo, dsr, monte_carlo, cost_stress) -> dict[str, Any]:
        checks = []
        if pbo.get("available"):
            checks.append(1.0 - float(pbo.get("probability") or 0.0))
        if dsr.get("available"):
            checks.append(float(dsr.get("probability") or 0.0))
        if monte_carlo.get("available"):
            checks.append(1.0 - float(monte_carlo.get("lossProbability") or 0.0))
        stressed = cost_stress[-1]["return"] if cost_stress else -100.0
        cost_available = bool(cost_stress and any(row.get("activity", 0) > 0 for row in cost_stress))
        if cost_available:
            checks.append(1.0 / (1.0 + pow(2.718281828, -stressed / 10.0)))
        score = round(100.0 * mean(checks), 2) if checks else 0.0
        grade = ("A" if score >= 80 else "B" if score >= 65 else "C" if score >= 50 else "D") if len(checks) == 4 else "insufficient_evidence"
        return {
            "score": score,
            "grade": grade,
            "availableChecks": len(checks),
            "totalChecks": 4,
        }


def _parameter_lookback_bars(parameters: list[SearchParameter]) -> int:
    tokens = ("period", "lookback", "window", "length", "bars")
    values = [
        int(parameter.maximum)
        for parameter in parameters
        if parameter.maximum is not None
        and parameter.maximum > 0
        and any(token in parameter.name.lower() for token in tokens)
    ]
    return max(values, default=0)
