"""Portfolio risk analysis and virtual order-group control."""

from flask import g, jsonify, request

from app.routes.portfolio import portfolio_blp
from app.openapi.schemas.research_execution import PortfolioRiskRequest, OrderGroupRequest, OrderGroupUnwindRequest, OrderGroupResolveRequest
from app.services import order_groups
from app.services.portfolio.risk import analyze_portfolio
from app.utils.auth import login_required


def _reply(call):
    try:
        return jsonify({"code": 1, "msg": "common.success", "data": call()})
    except (ValueError, TypeError, KeyError) as exc:
        status = 404 if str(exc) == "orderGroup.notFound" else 400
        return jsonify({"code": 0, "msg": str(exc), "data": None}), status


@portfolio_blp.route('/risk-analysis', methods=['POST'])
@login_required
@portfolio_blp.arguments(PortfolioRiskRequest)
def portfolio_risk_analysis(payload):
    """Analyze aligned daily returns in one currency and return a reusable risk model."""
    return _reply(lambda: analyze_portfolio(returns=payload['returns'], weights=payload['weights'],
                  shrinkage=payload['shrinkage'], annual_periods=payload['annualPeriods'], shocks=payload['shocks']))


@portfolio_blp.route('/order-groups', methods=['POST'])
@login_required
@portfolio_blp.doc(parameters=[{'name': 'Idempotency-Key', 'in': 'header', 'required': True,
                               'schema': {'type': 'string', 'minLength': 1, 'maxLength': 120}}])
@portfolio_blp.arguments(OrderGroupRequest)
def create_virtual_order_group(payload):
    """Queue 2–8 virtual legs; Idempotency-Key is required and conflicts reject."""
    return _reply(lambda: order_groups.create_group(user_id=int(g.user_id),
                  idempotency_key=request.headers.get('Idempotency-Key'), payload=payload))


@portfolio_blp.route('/order-groups/<group_id>', methods=['GET'])
@login_required
def get_virtual_order_group(group_id):
    """Read durable leg state and group-attributed residual exposure."""
    return _reply(lambda: order_groups.get_group(user_id=int(g.user_id), group_id=group_id))


@portfolio_blp.route('/order-groups/<group_id>/cancel', methods=['POST'])
@login_required
def cancel_virtual_order_group(group_id):
    """Stop unfilled group work; filled exposure requires an explicit unwind."""
    return _reply(lambda: order_groups.cancel_group(user_id=int(g.user_id), group_id=group_id))


@portfolio_blp.route('/order-groups/<group_id>/unwind', methods=['POST'])
@login_required
@portfolio_blp.arguments(OrderGroupUnwindRequest)
def unwind_virtual_order_group(payload, group_id):
    """Queue idempotent reductions using explicit reference marks, including virtual costs."""
    return _reply(lambda: order_groups.unwind_group(user_id=int(g.user_id), group_id=group_id,
                                                   reference_prices=payload['referencePrices']))


@portfolio_blp.route('/order-groups/<group_id>/resolve', methods=['POST'])
@login_required
@portfolio_blp.arguments(OrderGroupResolveRequest)
def resolve_virtual_order_group(payload, group_id):
    """Acknowledge manual closure after checking the virtual account is flat."""
    return _reply(lambda: order_groups.resolve_group(user_id=int(g.user_id), group_id=group_id, reason=payload['reason']))
