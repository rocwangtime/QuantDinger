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
from app.services.futu_trading.operator_control import (
    ensure_saved_account_credential, saved_account_credential, pause_account,
)
from app.services.futu_trading.operator_gate import (
    arm as arm_automation, begin_pause, finish_pause, hard_switch_enabled,
    state_for_user,
)

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
        market_category=str(data.get("market_category") or data.get("marketCategory") or "USStock"),
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


def _pause_selected_account(acc_id: int) -> dict:
    user_id = int(g.user_id)
    state = next((row for row in state_for_user(user_id) if int(row["acc_id"]) == acc_id), None)
    credential_id = int(state["credential_id"]) if state else 0
    try:
        credential_id, config = saved_account_credential(
            user_id, acc_id, credential_id=credential_id,
        )
    except Exception as exc:
        if state:
            begin_pause(user_id, credential_id, acc_id)
            finish_pause(user_id, acc_id, confirmed=False, error=str(exc))
            return {"state": "unconfirmed", "enabled": False, "error": str(exc)}
        raise
    return pause_account(user_id, credential_id, acc_id, config)


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
        status["credential_id"] = int(getattr(client, "saved_credential_id", 0) or 0) if client else None
        status["worker_streams"] = _worker_streams_for_user()
        status["automation"] = state_for_user(int(g.user_id))
        status["automation_hard_switch"] = hard_switch_enabled()
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
        if str(data.get("confirm_acc_id") or "").strip() != str(config.acc_id) or config.acc_id <= 0:
            return jsonify({"success": False, "error": "FUTU_ACCOUNT_CONFIRMATION_REQUIRED"}), 400
        for row in state_for_user(int(g.user_id)):
            if int(row["acc_id"]) != config.acc_id and row["state"] in {"armed", "stopping", "unconfirmed"}:
                return jsonify({"success": False, "error": "FUTU_PAUSE_OTHER_ACCOUNT_FIRST"}), 409
        client = FutuClient(config)
        if not client.connect():
            return jsonify({
                "success": False,
                "error": "Connection failed. Ensure FutuOpenD is running and reachable.",
            }), 400

        try:
            credential_id = ensure_saved_account_credential(int(g.user_id), config)
        except Exception:
            client.disconnect()
            raise
        client.saved_credential_id = credential_id
        _sessions.set(client)
        status = client.get_connection_status()
        status["credential_id"] = credential_id
        return jsonify({
            "success": True,
            "message": "Connected successfully",
            "data": status,
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
        client = _sessions.get()
        account_ids = {int(row["acc_id"]) for row in state_for_user(int(g.user_id))}
        if client is not None and int(client.config.acc_id or 0) > 0:
            selected_id = int(client.config.acc_id)
            if selected_id not in account_ids:
                try:
                    saved_account_credential(int(g.user_id), selected_id)
                except ValueError:
                    pass  # No saved strategy credential means no order permission.
                else:
                    account_ids.add(selected_id)
        for acc_id in sorted(account_ids):
            stopped = _pause_selected_account(acc_id)
            if stopped["state"] != "paused":
                return jsonify({"success": False, "data": stopped,
                                "error": "FUTU_STOP_NOT_CONFIRMED"}), 409
        _sessions.disconnect_current()
        return jsonify({"success": True, "message": "Disconnected"})
    except Exception as e:
        logger.error("Futu disconnect failed: %s", e)
        return jsonify({"success": False, "error": str(e)}), 500


@futu_blp.route("/automation", methods=["GET"])
@login_required
def automation_status():
    return jsonify({
        "success": True,
        "data": {"hard_switch_enabled": hard_switch_enabled(),
                 "accounts": state_for_user(int(g.user_id))},
    })


@futu_blp.route("/automation/arm", methods=["POST"])
@login_required
def automation_arm():
    """Explicitly arm only the connected, saved stock SIMULATE account."""
    try:
        if not hard_switch_enabled():
            return jsonify({"success": False, "error": "FUTU_PAPER_AUTOTRADE_HARD_DISABLED"}), 403
        client, error = _require_connected_client()
        if error is not None:
            return error
        status = client.get_connection_status()
        acc_id = int(status.get("acc_id") or 0)
        supplied = str((request.get_json(silent=True) or {}).get("confirm_acc_id") or "").strip()
        if not status.get("connected") or not status.get("account_ready") or supplied != str(acc_id):
            return jsonify({"success": False, "error": "FUTU_ACCOUNT_CONFIRMATION_REQUIRED"}), 400
        credential_id, config = saved_account_credential(
            int(g.user_id), acc_id, connected=client.config,
            credential_id=int(getattr(client, "saved_credential_id", 0) or 0),
        )
        previous = next(
            (row for row in state_for_user(int(g.user_id)) if int(row["acc_id"]) == acc_id),
            None,
        )
        if previous and int(previous["credential_id"]) != credential_id:
            previous_stop = _pause_selected_account(acc_id)
            if previous_stop["state"] != "paused":
                return jsonify({"success": False, "data": previous_stop,
                                "error": "FUTU_PREVIOUS_CREDENTIAL_STOP_UNCONFIRMED"}), 409
        preflight = pause_account(int(g.user_id), credential_id, acc_id, config)
        if preflight["state"] != "paused":
            return jsonify({"success": False, "data": preflight,
                            "error": "FUTU_PRE_ARM_STOP_UNCONFIRMED"}), 409
        arm_automation(int(g.user_id), credential_id, acc_id)
        return jsonify({"success": True, "data": {"state": "armed", "acc_id": acc_id,
                                                "credential_id": credential_id}})
    except ValueError as exc:
        return jsonify({"success": False, "error": str(exc)}), 400
    except Exception:
        logger.exception("Futu automation arm failed")
        return jsonify({"success": False, "error": "FUTU_ARM_FAILED"}), 500


@futu_blp.route("/automation/pause", methods=["POST"])
@login_required
def automation_pause():
    """Disarm first, then cancel only platform-owned queued/open paper orders."""
    try:
        data = request.get_json(silent=True) or {}
        acc_id = int(data.get("acc_id") or 0)
        if acc_id <= 0:
            client = _sessions.get()
            acc_id = int(client.config.acc_id or 0) if client else 0
        if acc_id <= 0:
            return jsonify({"success": False, "error": "FUTU_ACCOUNT_SELECTION_REQUIRED"}), 400
        result = _pause_selected_account(acc_id)
        return jsonify({"success": result["state"] == "paused", "data": result}), (
            200 if result["state"] == "paused" else 409
        )
    except ValueError as exc:
        return jsonify({"success": False, "error": str(exc)}), 400
    except Exception:
        logger.exception("Futu automation pause failed")
        return jsonify({"success": False, "error": "FUTU_PAUSE_FAILED"}), 500


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
        # Probe the form's host/firm without replacing an armed account's
        # connected session. Selecting a new account still requires /connect.
        config = _config_from_request(data)
        client = FutuClient(config)
        if not client.connect():
            return jsonify({
                "success": False,
                "error": "Connection failed. Ensure FutuOpenD is running.",
            }), 400
        try:
            probe_data = client.probe_permissions()
            status = client.get_connection_status()
        finally:
            client.disconnect()
        return jsonify({
            "success": True,
            "data": {
                "status": status,
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
