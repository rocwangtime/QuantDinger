"""Durable, owner-scoped public scans, paper trials, and frozen replay."""

import copy
import time

from app.utils import agent_jobs
from app.utils.db import get_db_connection
from .client import PublicClient
from .engine import ENGINE_VERSION, IMPLEMENTATION_HASH, digest, number, quote, simulate, validate_settings

KINDS = {"polymarket_scan", "polymarket_paper", "polymarket_replay"}


def owned_job(job_id, user_id, *, kind=None):
    row = agent_jobs.get_job(str(job_id), user_id=int(user_id))
    if not row or row.get("kind") not in KINDS or kind and row["kind"] != kind:
        raise ValueError("polymarket.notFound")
    return row


def bundle_from_job(job_id, user_id, kind):
    row = owned_job(job_id, user_id, kind=kind)
    result = row.get("result") or {}
    if row["status"] != "succeeded" or not isinstance(result, dict) or not result.get("bundle"):
        raise ValueError("polymarket.evidenceUnavailable")
    bundle = result["bundle"]
    if digest(bundle) != result.get("bundleHash"):
        raise ValueError("polymarket.evidenceHashMismatch")
    return copy.deepcopy(bundle)


def public_job(row):
    output = {key: row.get(key) for key in ("job_id", "kind", "status", "created_at", "started_at", "finished_at", "progress")}
    if row.get("error"):
        # Persistent runner logs can contain a traceback; display just the reason.
        output["error"] = str(row["error"]).splitlines()[0][:200]
    result = row.get("result")
    if isinstance(result, dict):
        output["result"] = {key: value for key, value in result.items() if key != "bundle"}
    return output


def list_jobs(user_id, kind, limit=20):
    if kind not in KINDS:
        raise ValueError("polymarket.notFound")
    with get_db_connection() as db:
        cur = db.cursor()
        try:
            # Do not load megabytes of frozen depth for a history list.
            cur.execute("SELECT job_id,kind,status,created_at,started_at,finished_at,progress,error,"
                        "result - 'bundle' AS result FROM qd_agent_jobs WHERE user_id=%s AND kind=%s "
                        "ORDER BY id DESC LIMIT %s", (int(user_id), kind, max(1, min(int(limit), 50))))
            return [public_job(row) for row in cur.fetchall() or []]
        finally:
            cur.close()


def _emit(callback, event):
    if callback:
        callback(event)


def _pause(milliseconds, callback):
    end = time.monotonic() + milliseconds / 1000
    while time.monotonic() < end:
        _emit(callback, {"phase": "waiting_for_observation"})
        time.sleep(min(0.2, max(0, end - time.monotonic())))


def run_scan(payload, on_progress=None, *, client=None):
    settings = validate_settings(payload["settings"])
    duration, interval = int(payload.get("durationSeconds", 15)), int(payload.get("sampleIntervalMs", 1000))
    if not 2 <= duration <= 30 or not 500 <= interval <= 5000:
        raise ValueError("polymarket.invalidRecordingWindow")
    client = client or PublicClient()
    try:
        _emit(on_progress, {"phase": "discovering"})
        markets, excluded = client.discover(market_ids=payload.get("marketIds"), limit=payload.get("marketLimit", 8))
        if not markets:
            raise ValueError("polymarket.noSupportedMarkets")
        samples, latest, episodes = [], [], {market["id"]: {"samples": 0, "firstMs": None, "lastMs": None,
                                                          "maxNetProfit": None} for market in markets}
        deadline = time.monotonic() + duration
        while True:
            frames = client.capture(markets)
            latest = []
            for market in markets:
                frame = frames[market["id"]]
                observed = quote(market, frame, settings)
                samples.append({"marketId": market["id"], "frame": frame})
                latest.append(observed)
                if observed["eligible"]:
                    stats = episodes[market["id"]]
                    stats["samples"] += 1
                    stats["firstMs"] = stats["firstMs"] or frame["observedMs"]
                    stats["lastMs"] = frame["observedMs"]
                    if stats["maxNetProfit"] is None or number(observed["netProfit"]) > number(stats["maxNetProfit"]):
                        stats["maxNetProfit"] = observed["netProfit"]
            _emit(on_progress, {"phase": "recording", "sampleCount": len(samples), "rows": latest})
            if time.monotonic() >= deadline:
                break
            _pause(min(interval, max(0, int((deadline - time.monotonic()) * 1000))), None)
        bundle = {"engineVersion": ENGINE_VERSION, "implementationHash": IMPLEMENTATION_HASH,
                  "type": "scan", "settings": settings, "markets": markets,
                  "samples": samples, "sampling": {"durationSeconds": duration, "sampleIntervalMs": interval,
                  "transport": "REST polling", "depthLimit": 50}}
        return {"mode": "observe", "rows": latest, "markets": markets, "excluded": excluded,
                "sampleCount": len(samples), "opportunityObservations": episodes, "bundleHash": digest(bundle), "bundle": bundle,
                "limitations": ["A bounded sample of the 100 highest-volume catalogue entries, not all markets",
                                "REST samples can miss short-lived opportunities; first/last observations are not continuous duration",
                                "The first 50 levels on each side are recorded; deeper liquidity is not assumed"]}
    finally:
        client.close()


def run_paper(payload, on_progress=None, *, client=None):
    settings = validate_settings(payload["settings"])
    scan = bundle_from_job(payload["scanJobId"], payload["__userId"], "polymarket_scan")
    market_id = str(payload["marketId"])
    saved = next((market for market in scan["markets"] if market["id"] == market_id), None)
    if not saved:
        raise ValueError("polymarket.marketOutsideScan")
    client = client or PublicClient()
    try:
        # Refresh constraints/fees and the signal: clicking an old scan cannot
        # buy against its archived price. Both identities must still match.
        market = client.market(market_id)
        if any(market[key] != saved[key] for key in ("conditionId", "yesAssetId", "noAssetId", "version")):
            raise ValueError("polymarket.marketIdentityChanged")
        frames = {"signal": client.capture([market])[market_id]}
        signal = quote(market, frames["signal"], settings)
        _emit(on_progress, {"phase": "signal", "signal": signal})
        if signal["eligible"]:
            _pause(settings["latencyMs"], on_progress)
            frames["leg1"] = client.capture([market])[market_id]
            _emit(on_progress, {"phase": "leg1_observed"})
            _pause(settings["legDelayMs"], on_progress)
            frames["leg2"] = client.capture([market])[market_id]
            _emit(on_progress, {"phase": "leg2_observed"})
            _pause(settings["latencyMs"], on_progress)
            frames["unwind"] = client.capture([market])[market_id]
        bundle = {"engineVersion": ENGINE_VERSION, "implementationHash": IMPLEMENTATION_HASH, "type": "paper", "market": market,
                  "settings": settings, "frames": frames, "scanBundleHash": digest(scan)}
        outcome = simulate(bundle)
        return {**outcome, "market": market, "bundleHash": digest(bundle), "bundle": bundle}
    finally:
        client.close()


def run_replay(payload, on_progress=None):
    bundle = bundle_from_job(payload["sourceJobId"], payload["__userId"], "polymarket_paper")
    _emit(on_progress, {"phase": "replaying"})
    result = simulate(bundle)
    original = owned_job(payload["sourceJobId"], payload["__userId"], kind="polymarket_paper")["result"]
    same = all(original.get(key) == value for key, value in result.items())
    return {**result, "market": bundle["market"], "bundleHash": digest(bundle), "replayMatches": same,
            "sourceJobId": payload["sourceJobId"], "bundle": bundle}
