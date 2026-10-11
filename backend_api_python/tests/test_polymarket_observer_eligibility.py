import ast
import importlib
import subprocess
import sys
from pathlib import Path

from app.services.polymarket.eligibility import derive
from app.services.polymarket.observer import opportunity, ws_apply_event


def test_eligibility_derivation_and_masking():
    assert derive({"blocked": True, "country": "SG", "ip": "43.159.12.34"})["status"] == "blocked"
    allowed = derive({"blocked": False, "country": "US", "region": "NY", "ip": "43.159.12.34"})
    assert allowed["status"] == "close_only"
    assert allowed["ip"] == "43.159.x.x"
    assert derive({"blocked": False, "country": "CH", "ip": "203.0.113.1"})["status"] == "eligible"
    assert derive(None, error="timeout")["status"] == "unknown"


def test_websocket_book_and_delta_parsing():
    books = {}
    assert ws_apply_event({"event_type": "book", "asset_id": "a", "bids": [{"price": "0.3", "size": "2"}], "asks": [{"price": "0.4", "size": "3"}]}, books) == ["a"]
    assert ws_apply_event({"event_type": "price_change", "price_changes": [{"asset_id": "a", "price": "0.4", "size": "0", "side": "SELL"}]}, books) == ["a"]
    assert books["a"]["asks"] == {}


def test_retention_runs_at_most_every_ten_minutes_and_uses_near_miss_setting(monkeypatch):
    from app.services.polymarket import observer

    statements = []

    class Cursor:
        def execute(self, sql, params=()):
            statements.append((sql, params))

        def close(self):
            pass

    class DB:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            pass

        def cursor(self):
            return Cursor()

        def commit(self):
            pass

    monkeypatch.setattr(observer, "get_db_connection", DB)
    monkeypatch.setenv("POLYMARKET_OBSERVER_NEAR_MISS_RETENTION_DAYS", "12")
    observer._RETENTION_LAST[0] = 0
    observer._run_retention(1000)
    observer._run_retention(1599)
    assert len(statements) == 2
    assert "qd_polymarket_observer_near_misses" in statements[1][0]
    assert statements[1][1] == (12,)
    observer._run_retention(1600)
    assert len(statements) == 4


def test_websocket_status_transitions_clear_error_and_preserve_mode(monkeypatch):
    from app.services.polymarket import observer

    monkeypatch.setattr(observer, "STATUS", {"mode": "rest", "connected": True, "last_error": "old"})
    observer._ws_connected()
    assert observer.snapshot() == {"mode": "ws", "connected": True, "last_error": None}
    observer._ws_error("socket failed\ntraceback")
    assert observer.snapshot()["last_error"] == "socket failed"
    observer._ws_disconnected()
    assert observer.snapshot()["connected"] is False


def test_observer_opportunity_uses_engine_fee_model():
    market = {"yesAssetId": "y", "noAssetId": "n", "feeRate": "0.02"}
    result = opportunity(market, {"y": {"asks": {"0.4": "10"}}, "n": {"asks": {"0.5": "10"}}}, 5, 0)
    assert result["size_available"] == 5
    assert result["net_edge"] > 0
    assert result["net_edge_bps"] == result["net_edge"] / 5 * 10000


def test_polymarket_package_has_no_wallet_signing_or_order_dependencies():
    package = Path(__file__).parents[1] / "app" / "services" / "polymarket"
    forbidden = {"py_clob_client", "eth_account", "web3", "eth_keys"}
    for path in package.glob("*.py"):
        tree = ast.parse(path.read_text())
        for node in ast.walk(tree):
            if isinstance(node, (ast.Import, ast.ImportFrom)):
                names = [alias.name for alias in node.names] if isinstance(node, ast.Import) else [node.module or ""]
                assert not any(name.split(".")[0] in forbidden for name in names), path
            if isinstance(node, ast.Constant) and isinstance(node.value, str):
                assert not any(term in node.value.lower() for term in ("/order", "/orders")), path
    script = "import app.services.polymarket.client, app.services.polymarket.engine, app.services.polymarket.jobs, app.services.polymarket.eligibility, app.services.polymarket.observer; import sys; assert not ({'py_clob_client','eth_account','web3','eth_keys'} & set(sys.modules))"
    subprocess.run([sys.executable, "-c", script], check=True)


def test_scheduler_observer_stays_off_by_default(monkeypatch):
    from app.services import polymarket
    monkeypatch.setenv("POLYMARKET_OBSERVER_ENABLED", "false")
    assert polymarket.observer.start() is False
    assert polymarket.observer.snapshot()["running"] is False
    assert polymarket.observer.snapshot()["mode"] == "disabled"


def test_geoblock_cache_ttl_and_refresh_rate_limit(monkeypatch):
    from app.services.polymarket import eligibility

    class Response:
        status_code = 200

        @staticmethod
        def json():
            return {"blocked": False, "country": "CH", "ip": "203.0.113.5"}

    class Session:
        calls = 0

        @classmethod
        def get(cls, *_args, **_kwargs):
            cls.calls += 1
            return Response()

    eligibility._CACHE = None
    eligibility._LAST_REFRESH = 0
    monkeypatch.setenv("POLYMARKET_GEOBLOCK_TTL_SECONDS", "60")
    now = [100.0]
    assert eligibility.check(session=Session, clock=lambda: now[0])["cached"] is False
    now[0] += 2
    assert eligibility.check(session=Session, clock=lambda: now[0])["cached"] is True
    assert Session.calls == 1
    now[0] += 61
    assert eligibility.check(session=Session, clock=lambda: now[0])["cached"] is False
    assert Session.calls == 2
