import inspect
import json
from contextlib import contextmanager

from flask import Flask, g

from app.routes import portfolio
from app.services import portfolio_monitor
from app.services.research_opportunities import eligible_opportunities, public_analysis


RESULT = {
    "success": True,
    "position_analyses": [
        {"market": "USStock", "symbol": "spy", "final_decision": "BUY", "confidence": 76,
         "reasoning": "Study the setup", "suggested_entry": 600},
        {"market": "USStock", "symbol": "SPY", "final_decision": "BUY"},
        {"market": "HKStock", "symbol": "00700", "final_decision": "BUY"},
        {"market": "USStock", "symbol": "MSFT", "final_decision": "HOLD"},
        {"market": "Crypto", "symbol": "BTC/USDT", "final_decision": "BUY"},
        {"market": "USStock", "symbol": "A;DROP", "final_decision": "BUY"},
        {"market": "USStock", "symbol": "AAPL", "final_decision": "BUY", "error": "missing data"},
    ],
}


def test_only_deduplicated_completed_stock_buy_outlooks_enter_inbox():
    assert eligible_opportunities(RESULT) == [("HKStock", "00700"), ("USStock", "SPY")]
    assert eligible_opportunities({**RESULT, "success": False}) == []
    assert eligible_opportunities({"success": True, "position_analyses": "not-a-list"}) == []
    analysis = public_analysis(RESULT, "USStock", "SPY")
    assert analysis["symbol"] == "spy"
    assert analysis["reasoning"] == "Study the setup"
    assert "analysis" not in analysis


def test_monitor_run_and_opportunity_insert_share_one_transaction(monkeypatch):
    calls = []

    class Cursor:
        def execute(self, sql, params):
            calls.append((sql, params))

        def fetchone(self):
            return {"id": 91, "user_id": 7}

        def close(self):
            pass

    class Connection:
        def cursor(self):
            return Cursor()

        def commit(self):
            calls.append(("COMMIT", ()))

    @contextmanager
    def connection():
        yield Connection()

    monkeypatch.setattr(portfolio_monitor, "get_db_connection", connection)
    portfolio_monitor._bump_monitor_schedule(5, 60, RESULT)
    inserts = [(sql, args) for sql, args in calls if "INSERT INTO qd_research_opportunities" in sql]
    assert len(inserts) == 2
    assert inserts[0][1][:3] == (5, 7, 91)
    assert calls[-1][0] == "COMMIT"


def test_opportunity_routes_scope_reads_and_updates_to_owner(monkeypatch):
    calls = []

    class Cursor:
        def execute(self, sql, params):
            calls.append((sql, params))

        def fetchall(self):
            return [{"id": 3, "monitor_id": 5, "run_id": 91, "market": "USStock",
                     "symbol": "SPY", "status": "new", "created_at": None,
                     "run_created_at": None, "result_json": json.dumps(RESULT)}]

        def fetchone(self):
            return {"id": 3, "status": "reviewed"}

        def close(self):
            pass

    class Connection:
        def cursor(self):
            return Cursor()

        def commit(self):
            pass

    @contextmanager
    def connection():
        yield Connection()

    monkeypatch.setattr(portfolio, "get_db_connection", connection)
    app = Flask(__name__)
    with app.test_request_context("/opportunities?status=new&limit=999"):
        g.user_id = 7
        response = inspect.unwrap(portfolio.get_research_opportunities)()
    assert response.json["data"][0]["analysis"]["reasoning"] == "Study the setup"
    assert calls[0][1] == (7, "new", 100)
    with app.test_request_context("/opportunities/3", method="PATCH", json={"status": "reviewed"}):
        g.user_id = 7
        response = inspect.unwrap(portfolio.update_research_opportunity)(3)
    assert response.json["data"]["status"] == "reviewed"
    assert calls[1][1] == ("reviewed", 3, 7)


def test_invalid_opportunity_status_never_touches_database():
    app = Flask(__name__)
    with app.test_request_context("/opportunities?status=armed"):
        g.user_id = 7
        response, status = inspect.unwrap(portfolio.get_research_opportunities)()
    assert status == 400
    with app.test_request_context("/opportunities/3", method="PATCH", json={"status": "armed"}):
        g.user_id = 7
        response, status = inspect.unwrap(portfolio.update_research_opportunity)(3)
    assert status == 400
