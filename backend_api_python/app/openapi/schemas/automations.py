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
    task = fields.Dict()
    performance = fields.Nested(AutomationPerformanceSchema)
    risk = fields.Dict(metadata={'description': 'healthy, checked_at (Unix seconds), halted, reasons, stopped_symbols; latched until explicit reset'})
    series = fields.List(fields.Nested(AutomationSampleSchema), metadata={'description': 'Most recent 1440 valid minute observations, oldest first'})
    orders = fields.List(fields.Dict())
    runs = fields.List(fields.Dict())
    model_usage = fields.Dict(metadata={'description': 'Completed decisions including previews only. Currency totals are estimates; unpriced calls are counted separately.'})


class AutomationDashboardEnvelopeSchema(Schema):
    code = fields.Integer()
    msg = fields.String()
    data = fields.Nested(AutomationDashboardDataSchema)
