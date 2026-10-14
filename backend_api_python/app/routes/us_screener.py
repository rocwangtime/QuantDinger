"""Authenticated research data and admin-only enqueue; no execution APIs."""
from flask import jsonify
from app.openapi.blueprint import HumanBlueprint
from app.openapi.schemas.common import HumanSuccessEnvelopeSchema, HumanErrorEnvelopeSchema
from app.utils.auth import login_required, admin_required
from app.services.us_screener import store

blp = HumanBlueprint('us_screener', __name__)


def public_run(row):
    if row is None:
        return None
    row = dict(row)
    for key in ('as_of', 'created_at', 'finished_at'):
        if row.get(key):
            row[key] = row[key].isoformat()
    return row


@blp.route('/latest', methods=['GET'])
@blp.doc(summary='Read latest independent US research screener run; no market or execution calls')
@login_required
def latest():
    return jsonify(code=1, msg='success', data=public_run(store.read()))


@blp.route('/runs/<int:run_id>', methods=['GET'])
@blp.doc(summary='Read persisted US research features, scores and exclusions')
@blp.alt_response(404, schema=HumanErrorEnvelopeSchema, description='Research run not found')
@login_required
def detail(run_id):
    row = store.read(run_id)
    return jsonify(code=1 if row else 0, msg='success' if row else 'not_found', data=public_run(row)), 200 if row else 404


@blp.route('/runs', methods=['POST'])
@blp.doc(summary='Admin: enqueue an independent research-only US screener', description='Queues read-only Futu market data work. Never changes Agent tasks or creates orders/intents.')
@blp.alt_response(202, schema=HumanSuccessEnvelopeSchema, description='Research run queued')
@blp.alt_response(403, schema=HumanErrorEnvelopeSchema, description='Admin access required')
@blp.alt_response(409, schema=HumanErrorEnvelopeSchema, description='Another screener run is pending')
@blp.alt_response(503, schema=HumanErrorEnvelopeSchema, description='Research queue unavailable')
@login_required
@admin_required
def trigger():
    from app.services.us_screener.service import enqueue
    try:
        row = enqueue()
        if not row:
            return jsonify(code=0, msg='screener_busy', data=None), 409
        return jsonify(code=1, msg='queued', data=row), 202
    except Exception:
        return jsonify(code=0, msg='screener_queue_unavailable', data=None), 503
