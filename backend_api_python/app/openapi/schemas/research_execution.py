"""Human API inputs for research, risk and isolated virtual coordination."""

from marshmallow import Schema, fields, validate


# Additive documentation for existing deployment handlers, which retain their
# service-layer validation and accept the existing deployment fields as well.
RESEARCH_DEPLOYMENT_DOC = {"requestBody": {
    "required": True,
    "description": "Existing Strategy V2 deployment fields plus opt-in research and risk controls.",
    "content": {"application/json": {"schema": {
        "type": "object", "additionalProperties": True, "properties": {
            "sourceId": {"type": "integer", "minimum": 1},
            "params": {"type": "object", "additionalProperties": True},
            "aiDecisionFilter": {"type": "boolean"},
            "aiDecisionMode": {"type": "string", "enum": ["advisory", "shadow", "required"],
                               "description": "Specifying a mode enables AI unless aiDecisionFilter is explicitly false."},
            "researchEvidenceJobId": {"type": "string", "description": "Owned qualifying evolution job matching this deployment."},
            "portfolioRisk": {"type": "object", "additionalProperties": False,
                              "description": "An empty object disables the opt-in entry guard. Otherwise model and limit are required.",
                              "properties": {
                                  "portfolio_model": {"type": "object", "description": "The model returned by risk-analysis."},
                                  "max_portfolio_daily_volatility": {"type": "number", "minimum": 0, "exclusiveMinimum": True},
                                  "portfolio_model_max_age_hours": {"type": "number", "minimum": 0, "exclusiveMinimum": True, "default": 96},
                              }},
        }
    }}},
}}


class PortfolioRiskRequest(Schema):
    returns = fields.Dict(keys=fields.Str(), values=fields.Dict(), required=True)
    weights = fields.Dict(keys=fields.Str(), values=fields.Float(), required=True)
    shrinkage = fields.Float(load_default=0.2, validate=validate.Range(min=0, max=1))
    annualPeriods = fields.Int(load_default=252, validate=validate.OneOf([252, 365]))
    shocks = fields.Dict(keys=fields.Str(), values=fields.Float(), load_default=None, allow_none=True)


class OrderGroupLeg(Schema):
    symbol = fields.Str(required=True, validate=validate.Length(min=1, max=80))
    action = fields.Str(required=True, validate=validate.OneOf(["open_long", "open_short"]))
    quantity = fields.Float(required=True, validate=validate.Range(min=0, min_inclusive=False))
    referencePrice = fields.Float(required=True, validate=validate.Range(min=0, min_inclusive=False))


class OrderGroupRequest(Schema):
    strategyId = fields.Int(required=True, validate=validate.Range(min=1))
    strategyRunId = fields.Int(load_default=0, validate=validate.Range(min=0),
                               metadata={"description": "Omit to create an isolated order_group run without starting the strategy."})
    executionMode = fields.Str(load_default="signal", validate=validate.OneOf(["signal"]))
    legs = fields.List(fields.Nested(OrderGroupLeg), required=True, validate=validate.Length(min=2, max=8))
    maxGrossNotional = fields.Float(required=True, validate=validate.Range(min=0, min_inclusive=False))
    timeoutSeconds = fields.Int(load_default=300, validate=validate.Range(min=5, max=3600))


class OrderGroupUnwindRequest(Schema):
    referencePrices = fields.Dict(keys=fields.Str(), values=fields.Float(), required=True)


class OrderGroupResolveRequest(Schema):
    reason = fields.Str(required=True, validate=validate.Length(min=1, max=500))


class ShadowEvaluationRequest(Schema):
    horizonHours = fields.Int(load_default=24, validate=validate.Range(min=1, max=168))
    limit = fields.Int(load_default=100, validate=validate.Range(min=1, max=200))
