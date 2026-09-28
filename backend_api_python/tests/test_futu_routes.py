"""Futu account setup routes must not reuse stale diagnostic sessions."""

import inspect
from types import SimpleNamespace

from flask import g

from app.routes import futu
from app.utils.broker_session import BrokerSessionRegistry


def test_probe_replaces_connection_when_operator_changes_opend_host(app, monkeypatch):
    created = []

    class FakeClient:
        def __init__(self, config):
            self.config = config
            self.connected = False
            self.disconnected = False
            created.append(self)

        def connect(self):
            self.connected = True
            return True

        def disconnect(self):
            self.disconnected = True

        def get_connection_status(self):
            return {"opend_connected": self.connected, "host": self.config.host}

        def probe_permissions(self):
            return {"accounts": [], "trade_env": "demo"}

    monkeypatch.setattr(futu, "FutuClient", FakeClient)
    monkeypatch.setattr(futu, "_sessions", BrokerSessionRegistry("futu"))
    monkeypatch.setattr(futu, "local_desktop_brokers_allowed", lambda: True)

    for host in ("127.0.0.1", "host.docker.internal"):
        with app.test_request_context("/api/futu/probe", method="POST", json={"host": host}):
            g.user_id = 17
            response = inspect.unwrap(futu.probe)()
            assert response.get_json()["data"]["status"]["host"] == host

    assert len(created) == 2
    assert created[0].disconnected is True
    assert created[1].disconnected is False


def test_account_route_does_not_report_failed_broker_query_as_success(app, monkeypatch):
    client = SimpleNamespace(get_account_summary=lambda: {
        "success": False,
        "error": "OpenD disconnected",
    })
    monkeypatch.setattr(futu, "_require_connected_client", lambda: (client, None))

    with app.test_request_context("/api/futu/account"):
        response, code = inspect.unwrap(futu.get_account)()

    assert code == 502
    assert response.get_json() == {"success": False, "error": "FUTU_ACCOUNT_QUERY_FAILED"}
