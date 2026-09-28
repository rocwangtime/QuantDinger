"""
Futu OpenAPI routes — connection diagnostics, account/positions, quote probe.

Strategy live orders still go through pending_orders; these endpoints are for
credential setup and operator health checks only.
"""

from flask import g, jsonify, request
from app.openapi.blueprint import HumanBlueprint as Blueprint
from app.utils.auth import login_required
from app.utils.broker_session import BrokerSessionRegistry
from app.utils.logger import get_logger
from app.utils.db import get_db_connection
from app.utils.local_brokers import desktop_broker_cloud_reject_message, local_desktop_brokers_allowed
from app.services.futu_trading import FutuClient, FutuConfig
from app.services.futu_trading.config import normalize_trade_env, normalize_trade_market

logger = get_logger(__name__)

futu_blp = Blueprint("futu", __name__)
_sessions = BrokerSessionRegistry("futu")


def _placeholder_status():
    return {
        "connected": False,
        "host": "",
        "port": 0,
        "trade_env": "demo",
        "trade_market": "US",
        "acc_id": None,
        "worker_streams": [],
    }


def _worker_streams_for_user() -> list[dict]:
    try:
        with get_db_connection() as db:
            cur = db.cursor()
            cur.execute(
                """
                SELECT h.credential_id, h.state, h.last_event_at,
                       h.last_connected_at, h.reconnect_count, h.updated_at
                FROM qd_execution_stream_health h
                JOIN qd_exchange_credentials c
                  ON c.id = h.credential_id
                WHERE c.user_id = %s AND LOWER(c.exchange_id) = 'futu'
                ORDER BY h.updated_at DESC
                """,
                (int(g.user_id),),
            )
            rows = [dict(row) for row in (cur.fetchall() or [])]
            cur.close()
        return rows
    except Exception:
        logger.warning("Futu worker stream status unavailable")
        return []


def _require_connected_client():
    client = _sessions.get()
    if client is None or not client.connected:
        return None, (jsonify({"success": False, "error": "Not connected to FutuOpenD"}), 400)
    return client, None


def _config_from_request(data: dict) -> FutuConfig:
    env = normalize_trade_env(
        data.get("trade_env") or data.get("environment") or data.get("tradeEnv") or "demo",
        default="demo",
    )
    market = normalize_trade_market(
        data.get("trade_market") or data.get("tradeMarket") or data.get("market"),
        market_category="USStock",
    )
    encrypt_raw = data.get("is_encrypt")
    if encrypt_raw is None:
        encrypt_raw = data.get("isEncrypt")
    is_encrypt = None if encrypt_raw in (None, "") else bool(encrypt_raw)
    return FutuConfig(
        host=str(data.get("host") or data.get("futu_host") or "127.0.0.1").strip(),
        port=int(data.get("port") or data.get("futu_port") or 11111),
        trade_env=env,
        trade_market=market,
        security_firm=str(data.get("security_firm") or data.get("securityFirm") or "FUTUSECURITIES"),
        acc_id=int(data.get("acc_id") or data.get("accId") or 0),
        unlock_password=str(data.get("unlock_password") or data.get("unlockPassword") or ""),
        is_encrypt=is_encrypt,
        market_category="USStock" if market == "US" else "HKStock",
    )


@futu_blp.route("/status", methods=["GET"])
@login_required
def get_status():
    """Get FutuOpenD connection status for the current user session."""
    try:
        client = _sessions.get()
        if client is None:
            status = _placeholder_status()
        else:
            status = client.get_connection_status()
        status["worker_streams"] = _worker_streams_for_user()
        return jsonify({"success": True, "data": status})
    except Exception as e:
        logger.error("Futu get status failed: %s", e)
        return jsonify({"success": False, "error": str(e)}), 500


@futu_blp.route("/connect", methods=["POST"])
@login_required
def connect():
    """
    Connect to FutuOpenD (diagnostics only — no orders placed).

    Body: host, port, trade_env=demo, trade_market=US,
    security_firm, acc_id. Unlock passwords and live trading are rejected.
    """
    try:
        if not local_desktop_brokers_allowed():
            return jsonify({
                "success": False,
                "error": desktop_broker_cloud_reject_message("futu"),
            }), 403

        data = request.get_json() or {}
        config = _config_from_request(data)
        client = FutuClient(config)
        if not client.connect():
            return jsonify({
                "success": False,
                "error": "Connection failed. Ensure FutuOpenD is running and reachable.",
            }), 400

        _sessions.set(client)
        return jsonify({
            "success": True,
            "message": "Connected successfully",
            "data": client.get_connection_status(),
        })
    except ImportError:
        return jsonify({
            "success": False,
            "error": "futu-api not installed. Run: pip install futu-api",
        }), 500
    except Exception as e:
        logger.error("Futu connection failed: %s", e)
        return jsonify({"success": False, "error": str(e)}), 500


@futu_blp.route("/disconnect", methods=["POST"])
@login_required
def disconnect():
    try:
        _sessions.disconnect_current()
        return jsonify({"success": True, "message": "Disconnected"})
    except Exception as e:
        logger.error("Futu disconnect failed: %s", e)
        return jsonify({"success": False, "error": str(e)}), 500


@futu_blp.route("/probe", methods=["POST"])
@login_required
def probe():
    """Connect (or reuse session) and return permissions / account probe (no orders)."""
    try:
        if not local_desktop_brokers_allowed():
            return jsonify({
                "success": False,
                "error": desktop_broker_cloud_reject_message("futu"),
            }), 403

        data = request.get_json() or {}
        # A probe must reflect the host/firm currently entered in the form,
        # not a previously cached connection for this user.
        config = _config_from_request(data)
        client = FutuClient(config)
        if not client.connect():
            return jsonify({
                "success": False,
                "error": "Connection failed. Ensure FutuOpenD is running.",
            }), 400
        _sessions.set(client)

        probe_data = client.probe_permissions()
        return jsonify({
            "success": True,
            "data": {
                "status": client.get_connection_status(),
                "probe": probe_data,
            },
        })
    except ImportError:
        return jsonify({
            "success": False,
            "error": "futu-api not installed. Run: pip install futu-api",
        }), 500
    except Exception as e:
        logger.error("Futu probe failed: %s", e)
        return jsonify({"success": False, "error": str(e)}), 500


@futu_blp.route("/account", methods=["GET"])
@login_required
def get_account():
    try:
        client, err = _require_connected_client()
        if err is not None:
            return err
        summary = client.get_account_summary()
        if not summary.get("success"):
            return jsonify({"success": False, "error": "FUTU_ACCOUNT_QUERY_FAILED"}), 502
        return jsonify({"success": True, "data": summary})
    except Exception as e:
        logger.error("Futu get account failed: %s", e)
        return jsonify({"success": False, "error": str(e)}), 500


@futu_blp.route("/positions", methods=["GET"])
@login_required
def get_positions():
    try:
        client, err = _require_connected_client()
        if err is not None:
            return err
        return jsonify({"success": True, "data": client.get_positions()})
    except Exception as e:
        logger.error("Futu get positions failed: %s", e)
        return jsonify({"success": False, "error": str(e)}), 500


@futu_blp.route("/orders", methods=["GET"])
@login_required
def get_orders():
    try:
        client, err = _require_connected_client()
        if err is not None:
            return err
        return jsonify({"success": True, "data": client.get_recent_orders()})
    except Exception as e:
        logger.error("Futu get orders failed: %s", e)
        return jsonify({"success": False, "error": str(e)}), 500


@futu_blp.route("/fills", methods=["GET"])
@login_required
def get_inferred_fills():
    """Platform-recorded fills inferred from paper order cumulative updates."""
    try:
        with get_db_connection() as db:
            cur = db.cursor()
            cur.execute(
                """
                SELECT t.id, t.strategy_id, t.symbol, t.type AS side, t.amount AS quantity,
                       t.price, t.exchange_order_id, t.created_at, t.fill_source,
                       t.commission, t.commission_ccy
                FROM qd_strategy_trades t
                JOIN qd_exchange_credentials c
                  ON c.id = t.credential_id AND c.user_id = t.user_id
                WHERE t.user_id = %s AND LOWER(c.exchange_id) = 'futu'
                ORDER BY t.id DESC LIMIT 100
                """,
                (int(g.user_id),),
            )
            rows = [dict(row) for row in (cur.fetchall() or [])]
            cur.close()
        for row in rows:
            row["source_label"] = "inferred_from_paper_order"
        return jsonify({"success": True, "data": rows})
    except Exception:
        logger.exception("Futu inferred fill query failed")
        return jsonify({"success": False, "error": "FUTU_FILL_QUERY_FAILED"}), 500


@futu_blp.route("/quote", methods=["GET"])
@login_required
def get_quote():
    """Get a snapshot quote (query: symbol, marketType=HKStock|USStock)."""
    try:
        client, err = _require_connected_client()
        if err is not None:
            return err
        symbol = request.args.get("symbol")
        market_type = request.args.get("marketType") or request.args.get("market_type") or "HKStock"
        if not symbol:
            return jsonify({"success": False, "error": "Missing symbol"}), 400
        return jsonify(client.get_quote(symbol, market_type))
    except Exception as e:
        logger.error("Futu get quote failed: %s", e)
        return jsonify({"success": False, "error": str(e)}), 500


# openapi-compat: legacy import name
futu_bp = futu_blp
