"""Authenticated public-data scans and independent paper complete-set trials."""

from flask import g, jsonify, request

from app.openapi.blueprint import HumanBlueprint
from app.openapi.schemas.polymarket import JobListQuery, PaperRequest, ScanRequest
from app.services.billing_service import BillingError
from app.services.polymarket import jobs
from app.services.polymarket.engine import validate_settings
from app.utils import agent_jobs
from app.utils.auth import login_required

blp = HumanBlueprint("polymarket", __name__)
IDEMPOTENCY = {"parameters": [{"name": "Idempotency-Key", "in": "header", "required": True,
                               "schema": {"type": "string", "minLength": 1, "maxLength": 120}}]}


def reply(call, status=200):
    try:
        return jsonify({"code": 1, "msg": "common.success", "data": call()}), status
    except BillingError as exc:
        return jsonify({"code": 0, "msg": str(exc.code), "data": None}), exc.status
    except (ValueError, KeyError, TypeError) as exc:
        message = str(exc)
        return jsonify({"code": 0, "msg": message, "data": None}), 404 if message == "polymarket.notFound" else 400


def submit(kind, payload, runner):
    key = str(request.headers.get("Idempotency-Key") or "").strip()
    if not key or len(key) > 120:
        raise ValueError("polymarket.idempotencyRequired")
    payload = {**payload, "__userId": int(g.user_id)}
    if "settings" in payload:
        payload["settings"] = validate_settings(payload["settings"])
    return jobs.public_job(agent_jobs.submit_job(user_id=int(g.user_id), agent_token_id=None, kind=kind,
                           request_payload=payload, runner=runner, idempotency_key=key))


@blp.route("/scans", methods=["POST"])
@login_required
@blp.doc(**IDEMPOTENCY)
@blp.arguments(ScanRequest)
def scan(payload):
    """Record bounded public depth samples and net complete-set quotes."""
    return reply(lambda: submit("polymarket_scan", payload, jobs.run_scan), 202)


@blp.route("/paper-runs", methods=["POST"])
@login_required
@blp.doc(**IDEMPOTENCY)
@blp.arguments(PaperRequest)
def paper(payload):
    """Refresh an owned scan market and observe delayed paper execution."""
    def create():
        jobs.bundle_from_job(payload["scanJobId"], int(g.user_id), "polymarket_scan")
        return submit("polymarket_paper", payload, jobs.run_paper)
    return reply(create, 202)


@blp.route("/jobs", methods=["GET"])
@login_required
@blp.arguments(JobListQuery, location="query")
def history(query):
    """List this user's experiments, including failed and cancelled attempts."""
    return reply(lambda: jobs.list_jobs(user_id=int(g.user_id), **query))


@blp.route("/jobs/<job_id>", methods=["GET"])
@login_required
def detail(job_id):
    """Read owned progress/results without downloading the full frozen depth."""
    return reply(lambda: jobs.public_job(jobs.owned_job(job_id, int(g.user_id))))


@blp.route("/jobs/<job_id>/cancel", methods=["POST"])
@login_required
def cancel(job_id):
    """Cooperatively cancel observation; no broker orders or wallet operations."""
    def stop():
        jobs.owned_job(job_id, int(g.user_id))
        return jobs.public_job(agent_jobs.cancel_job(job_id, user_id=int(g.user_id)))
    return reply(stop)


@blp.route("/jobs/<job_id>/replay", methods=["POST"])
@login_required
@blp.doc(**IDEMPOTENCY)
def replay(job_id):
    """Repeat the saved paper experiment from frozen observations without network reads."""
    def create():
        jobs.bundle_from_job(job_id, int(g.user_id), "polymarket_paper")
        return submit("polymarket_replay", {"sourceJobId": job_id}, jobs.run_replay)
    return reply(create, 202)


@blp.route("/jobs/<job_id>/evidence", methods=["GET"])
@login_required
def evidence(job_id):
    """Download hash-verified depth and assumptions for an owned experiment."""
    def fetch():
        row = jobs.owned_job(job_id, int(g.user_id))
        bundle = jobs.bundle_from_job(job_id, int(g.user_id), row["kind"])
        return {"bundleHash": jobs.digest(bundle), "bundle": bundle}
    return reply(fetch)
