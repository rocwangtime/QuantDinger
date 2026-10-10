"""Inputs for public-data observation and paper experiments; no credentials."""

from marshmallow import Schema, fields, validate


class PaperSettings(Schema):
    quantity = fields.Float(validate=validate.Range(min=0.000001, max=100000))
    budget = fields.Float(validate=validate.Range(min=1, max=1000000))
    minNetEdgeBps = fields.Float(validate=validate.Range(min=0, max=10000))
    slippageBps = fields.Float(validate=validate.Range(min=0, max=1000))
    settlementCost = fields.Float(validate=validate.Range(min=0, max=1000))
    riskReserve = fields.Float(validate=validate.Range(min=0, max=1000))
    latencyMs = fields.Integer(validate=validate.Range(min=0, max=2000))
    legDelayMs = fields.Integer(validate=validate.Range(min=0, max=3000))
    maxBookAgeMs = fields.Integer(validate=validate.Range(min=500, max=10000))
    maxUnhedgedMs = fields.Integer(validate=validate.Range(min=1000, max=30000))
    maxUnwindLoss = fields.Float(validate=validate.Range(min=0, max=1000000))
    orderType = fields.Str(validate=validate.OneOf(["FOK", "FAK"]))


class ScanRequest(Schema):
    marketIds = fields.List(fields.Str(validate=validate.Regexp(r"^[0-9]{1,20}$")),
                           load_default=list, validate=validate.Length(max=8))
    marketLimit = fields.Integer(load_default=8, validate=validate.Range(min=1, max=8))
    durationSeconds = fields.Integer(load_default=15, validate=validate.Range(min=2, max=30))
    sampleIntervalMs = fields.Integer(load_default=1000, validate=validate.Range(min=500, max=5000))
    settings = fields.Nested(PaperSettings, load_default=dict)


class PaperRequest(Schema):
    scanJobId = fields.Str(required=True, validate=validate.Regexp(r"^[a-f0-9]{32}$"))
    marketId = fields.Str(required=True, validate=validate.Regexp(r"^[0-9]{1,20}$"))
    settings = fields.Nested(PaperSettings, load_default=dict)


class JobListQuery(Schema):
    kind = fields.Str(load_default="polymarket_paper", validate=validate.OneOf([
        "polymarket_scan", "polymarket_paper", "polymarket_replay"]))
    limit = fields.Int(load_default=20, validate=validate.Range(min=1, max=50))
