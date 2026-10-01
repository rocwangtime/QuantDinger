import inspect
from contextlib import contextmanager

from flask import Flask, g

from app.routes import portfolio


def test_monitor_runs_are_scoped_to_owner(monkeypatch):
    calls = []

    class Cursor:
        def execute(self, sql, params):
            calls.append((sql, params))

        def fetchone(self):
            return {"exists": 1}

        def fetchall(self):
            return [{"id": 12, "status": "completed", "result_json": '{"success":true}', "created_at": None}]

        def close(self):
            pass

    class Connection:
        def cursor(self):
            return Cursor()

    @contextmanager
    def connection():
        yield Connection()

    monkeypatch.setattr(portfolio, "get_db_connection", connection)
    app = Flask(__name__)
    with app.test_request_context("/monitors/5/runs?limit=999"):
        g.user_id = 7
        response = inspect.unwrap(portfolio.get_monitor_runs)(5)
    assert response.json["data"][0]["result"]["success"] is True
    assert calls[0][1] == (5, 7)
    assert calls[1][1] == (5, 7, 50)


def test_monitor_runs_hide_other_users(monkeypatch):
    class Cursor:
        def execute(self, sql, params):
            pass

        def fetchone(self):
            return None

        def close(self):
            pass

    class Connection:
        def cursor(self):
            return Cursor()

    @contextmanager
    def connection():
        yield Connection()

    monkeypatch.setattr(portfolio, "get_db_connection", connection)
    app = Flask(__name__)
    with app.test_request_context("/monitors/5/runs"):
        g.user_id = 8
        response, status = inspect.unwrap(portfolio.get_monitor_runs)(5)
    assert status == 404
    assert response.json["data"] is None
