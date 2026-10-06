"""Forward-only portfolio dashboard contract. All monetary P&L is gross."""
from marshmallow import Schema, fields


class AutomationReportSchema(Schema):
    as_of = fields.Float(metadata={'description': 'UTC observation Unix seconds; never a synthetic backtest date'})
    equity = fields.Float(allow_none=True)
    capital = fields.Float()
    virtual_cash = fields.Float(allow_none=True)
    return_pct = fields.Float(allow_none=True, metadata={'description': 'Percentage points, e.g. 3.0 means 3%'})
    realized_pnl = fields.Float(allow_none=True)
    unrealized_pnl = fields.Float(allow_none=True)
    benchmark_return_pct = fields.Float(allow_none=True)
    benchmark_started_at = fields.Float(allow_none=True)
    daily_loss_pct = fields.Float()
    drawdown_pct = fields.Float()
    max_drawdown_pct = fields.Float()
    positions = fields.List(fields.Dict())
    filled_order_count = fields.Integer()
    turnover = fields.Float()
    fees = fields.Float(allow_none=True, metadata={'description': 'Null means unavailable, not zero'})
    pnl_basis = fields.String()
    errors = fields.List(fields.String())
    account_as_of = fields.Float()


class AutomationSampleSchema(Schema):
    sampled_at = fields.DateTime()
    report = fields.Nested(AutomationReportSchema)


class AutomationPerformanceSchema(Schema):
    capital = fields.Float()
    symbols = fields.List(fields.String())
    reserve_ratio = fields.Float()
    started_at = fields.Float()
    benchmark_started_at = fields.Float()
    benchmark_prices = fields.Dict()
    baseline_prices = fields.Dict()
    external_quantities = fields.Dict()
    high_water = fields.Float()
    day = fields.String()
    day_equity = fields.Float()
    max_drawdown_pct = fields.Float()
    last_sample_at = fields.Float()
    latest = fields.Nested(AutomationReportSchema)
    latest_valid = fields.Nested(AutomationReportSchema)


class AutomationDashboardDataSchema(Schema):
    task = fields.Dict(metadata={'description': 'Owner-scoped task, normalized research/event settings, daily decision_budget and event_review baseline'})
    decision_stats = fields.Dict(metadata={'description': 'Descriptive counts for latest 30 runs; proposed actions are not fills or trading accuracy'})
    performance = fields.Nested(AutomationPerformanceSchema)
    risk = fields.Dict(metadata={'description': 'healthy, checked_at (Unix seconds), halted, reasons, stopped_symbols; latched until explicit reset'})
    series = fields.List(fields.Nested(AutomationSampleSchema), metadata={'description': 'Most recent 1440 valid minute observations, oldest first'})
    orders = fields.List(fields.Dict())
    runs = fields.List(fields.Dict())
    model_usage = fields.Dict(metadata={'description': 'Completed snapshot decisions plus recorded tool-loop attempts, including previews and failed/cancelled rounds. Currency totals are estimates; unpriced calls are counted separately.'})


class AutomationDashboardEnvelopeSchema(Schema):
    code = fields.Integer()
    msg = fields.String()
    data = fields.Nested(AutomationDashboardDataSchema)


class AutomationReadinessDataSchema(Schema):
    checked_at = fields.Float()
    task_revision = fields.Integer()
    configuration_ready = fields.Boolean(metadata={'description': 'Configured prerequisites only; never permission to submit an order'})
    model = fields.Dict()
    checks = fields.List(fields.Dict(metadata={'description': 'key, status (pass/blocked/wait/unknown), code and optional details'}))
    decision_budget = fields.Dict()
    account_limits = fields.Dict(allow_none=True)
    scheduler = fields.Dict()
    connection_check = fields.Dict(allow_none=True, metadata={'description': 'Manual read-only probe, same revision, at most 90 seconds old'})


class AutomationReadinessEnvelopeSchema(Schema):
    code = fields.Integer()
    msg = fields.String()
    data = fields.Nested(AutomationReadinessDataSchema)


class AutomationReviewDataSchema(Schema):
    schema_version = fields.String()
    generated_at = fields.Float()
    task = fields.Dict()
    configuration = fields.Dict()
    window = fields.Dict()
    coverage = fields.Dict(metadata={'description': 'Truncation flags; at most 1000 recent runs, 10000 samples and 1000 updated task orders'})
    decisions = fields.Dict()
    model_usage = fields.Dict()
    observed_sessions = fields.List(fields.Dict(metadata={'description': 'First/last observed equity and mark-to-mark change, not full trading-session returns'}))
    latest_valid_task_performance = fields.Dict(allow_none=True)
    current_risk = fields.Dict()
    orders = fields.List(fields.Dict())
    limitations = fields.List(fields.String())


class AutomationReviewEnvelopeSchema(Schema):
    code = fields.Integer()
    msg = fields.String()
    data = fields.Nested(AutomationReviewDataSchema)
