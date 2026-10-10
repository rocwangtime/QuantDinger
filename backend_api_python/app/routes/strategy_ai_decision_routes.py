"""AI decision audit routes for live strategies."""

from flask import g, jsonify, request

from app.routes.strategy_blueprint import strategy_blp
from app.routes.strategy_services import get_strategy_service
from app.services.ai_decision_filter import list_ai_decisions
from app.utils.auth import login_required
from app.openapi.schemas.research_execution import ShadowEvaluationRequest
from app.openapi.schemas.common import HumanSuccessEnvelopeSchema
from app.utils import agent_jobs
from app.services.ai_evaluation import run_shadow_job


@strategy_blp.route('/strategies/ai-decisions', methods=['GET'])
@login_required
def get_strategy_ai_decisions():
    strategy_id = int(request.args.get('id') or 0)
    if strategy_id <= 0:
        return jsonify({'code': 0, 'msg': 'strategyV2.strategyIdRequired'}), 400
    strategy = get_strategy_service().get_strategy(strategy_id, user_id=int(g.user_id))
    if not strategy:
        return jsonify({'code': 0, 'msg': 'strategyV2.strategyNotFound'}), 404
    rows = list_ai_decisions(
        user_id=int(g.user_id),
        source_type='strategy',
        source_id=strategy_id,
        limit=int(request.args.get('limit') or 100),
    )
    return jsonify({'code': 1, 'msg': 'common.success', 'data': rows})


@strategy_blp.route('/strategies/<int:strategy_id>/ai-evaluation', methods=['POST'])
@login_required
@strategy_blp.alt_response(202, schema=HumanSuccessEnvelopeSchema, description='Evaluation queued')
@strategy_blp.arguments(ShadowEvaluationRequest)
def submit_strategy_ai_evaluation(payload, strategy_id):
    """Compare baseline and shadow suggestions at a fixed horizon, net of assumed costs."""
    if not get_strategy_service().get_strategy(strategy_id, user_id=int(g.user_id)):
        return jsonify({'code': 0, 'msg': 'strategyV2.strategyNotFound', 'data': None}), 404
    receipt = agent_jobs.submit_job(user_id=int(g.user_id), agent_token_id=None, kind='ai_evaluation',
        request_payload={**payload, 'strategyId': strategy_id, '__userId': int(g.user_id)}, runner=run_shadow_job)
    return jsonify({'code': 1, 'msg': 'common.success', 'data': _evaluation_job(receipt)}), 202


@strategy_blp.route('/strategies/ai-evaluation/jobs/<job_id>', methods=['GET'])
@login_required
def get_strategy_ai_evaluation(job_id):
    row = agent_jobs.get_job(job_id, user_id=int(g.user_id))
    if not row or row.get('kind') != 'ai_evaluation':
        return jsonify({'code': 0, 'msg': 'aiEvaluation.jobNotFound', 'data': None}), 404
    return jsonify({'code': 1, 'msg': 'common.success', 'data': _evaluation_job(row)})


@strategy_blp.route('/strategies/ai-evaluation/jobs/<job_id>/cancel', methods=['POST'])
@login_required
def cancel_strategy_ai_evaluation(job_id):
    row = agent_jobs.get_job(job_id, user_id=int(g.user_id))
    if not row or row.get('kind') != 'ai_evaluation':
        return jsonify({'code': 0, 'msg': 'aiEvaluation.jobNotFound', 'data': None}), 404
    return jsonify({'code': 1, 'msg': 'common.success', 'data': _evaluation_job(
        agent_jobs.cancel_job(job_id, user_id=int(g.user_id)) or row)})


def _evaluation_job(row):
    from app.routes.strategy_evolution import _public_job
    public = _public_job(row)
    if public:
        public.pop('request', None)
    return public
