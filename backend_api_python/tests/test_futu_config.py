import inspect
import json
from unittest.mock import MagicMock

import pytest
from flask import g
from marshmallow import ValidationError

from app.openapi.schemas.high_risk import CredentialCreateRequestSchema
from app.services.exchange_execution import resolve_exchange_config, safe_exchange_config_for_log
from app.services.futu_trading.config import (
    FutuConfig,
    config_from_exchange_config,
    is_local_or_private_opend_host,
    normalize_security_firm,
    normalize_trade_env,
    normalize_trade_market,
    validate_opend_host,
)


def test_normalize_trade_env():
    assert normalize_trade_env("demo") == "demo"
    assert normalize_trade_env("paper") == "demo"
    assert normalize_trade_env("simulate") == "demo"
    assert normalize_trade_env("live") == "live"
    assert normalize_trade_env("REAL") == "live"
    assert normalize_trade_env("") == "demo"


def test_normalize_trade_market():
    assert normalize_trade_market("HK") == "HK"
    assert normalize_trade_market("", market_category="HKStock") == "HK"
    assert normalize_trade_market("", market_category="USStock") == "US"
    assert normalize_trade_market("USStock") == "US"


def test_normalize_security_firm():
    assert normalize_security_firm("") == "FUTUSECURITIES"
    assert normalize_security_firm("futu") == "FUTUSECURITIES"
    assert normalize_security_firm("FUTUINC") == "FUTUINC"


def test_config_from_exchange_config_demo_default():
    cfg = config_from_exchange_config({
        "futu_host": "host.docker.internal",
        "futu_port": 11111,
        "environment": "demo",
        "trade_market": "US",
    })
    assert isinstance(cfg, FutuConfig)
    assert cfg.host == "host.docker.internal"
    assert cfg.is_simulate is True
    assert cfg.trade_market == "US"
    redacted = cfg.redacted_dict()
    assert "unlock_password" not in redacted
    assert redacted["has_unlock_password"] is False


def test_config_rejects_live_and_unlock_password():
    with pytest.raises(ValueError, match="FUTU_SIMULATE_ONLY"):
        config_from_exchange_config({"trade_env": "live", "trade_market": "US"})
    with pytest.raises(ValueError, match="FUTU_UNLOCK_PASSWORD_NOT_ACCEPTED"):
        config_from_exchange_config({"trade_env": "demo", "trade_market": "US", "unlock_password": "secret"})


def test_opend_host_validation_accepts_only_local_and_private_ranges():
    assert is_local_or_private_opend_host("127.0.0.1")
    assert is_local_or_private_opend_host("localhost")
    assert is_local_or_private_opend_host("host.docker.internal")
    assert is_local_or_private_opend_host("10.1.2.3")
    assert is_local_or_private_opend_host("172.16.0.1")
    assert is_local_or_private_opend_host("172.31.255.254")
    assert is_local_or_private_opend_host("192.168.1.2")
    assert is_local_or_private_opend_host("fd12::1")
    assert not is_local_or_private_opend_host("172.15.255.255")
    assert not is_local_or_private_opend_host("172.32.0.1")
    assert not is_local_or_private_opend_host("8.8.8.8")
    assert not is_local_or_private_opend_host("example.com")


def test_validate_opend_host_rejects_remote_by_default(monkeypatch):
    monkeypatch.delenv("FUTU_ALLOW_REMOTE_OPEND", raising=False)
    with pytest.raises(ValueError, match="private LAN"):
        validate_opend_host("169.254.169.254")


def test_validate_opend_host_allows_remote_only_with_explicit_opt_in(monkeypatch):
    monkeypatch.setenv("FUTU_ALLOW_REMOTE_OPEND", "true")
    assert validate_opend_host("203.0.113.8") == "203.0.113.8"


def test_futu_credential_requires_and_cross_validates_market():
    schema = CredentialCreateRequestSchema()
    with pytest.raises(ValidationError, match="trade_market"):
        schema.load({"exchange_id": "futu"})
    with pytest.raises(ValidationError, match="FUTU_US_MARKET_ONLY"):
        schema.load({
            "exchange_id": "futu",
            "trade_market": "US",
            "market_category": "HKStock",
            "acc_id": 99,
        })
    with pytest.raises(ValidationError, match="FUTU_US_MARKET_ONLY"):
        schema.load({"exchange_id": "futu", "trade_market": "unsupported", "acc_id": 99})
    loaded = schema.load({
        "exchange_id": "futu",
        "trade_market": "US",
        "market_category": "USStock",
        "acc_id": 99,
    })
    assert loaded["trade_market"] == "US"


def test_futu_web_endpoint_is_saved_without_schema_defaults_overriding_it(app, monkeypatch):
    from app.routes import credentials

    payload = CredentialCreateRequestSchema().load({
        "exchange_id": "futu",
        "name": "US paper",
        "host": "host.docker.internal",
        "port": 11112,
        "trade_env": "demo",
        "trade_market": "US",
        "market_category": "USStock",
        "acc_id": 99,
    })
    captured = {}
    def capture_config(plain):
        captured["config"] = json.loads(plain)
        return "encrypted"

    monkeypatch.setattr(
        "app.utils.local_brokers.local_desktop_brokers_allowed", lambda: True,
    )
    monkeypatch.setattr(credentials, "encrypt_credential_blob", capture_config)
    connection = MagicMock()
    connection.cursor.return_value.fetchone.return_value = {"id": 7}
    context = MagicMock()
    context.__enter__.return_value = connection
    monkeypatch.setattr(credentials, "get_db_connection", lambda: context)

    with app.test_request_context("/api/credentials/create", method="POST"):
        g.user_id = 1
        response = inspect.unwrap(credentials.create_credential)(payload)

    assert response.get_json()["data"]["id"] == 7
    assert captured["config"]["futu_host"] == "host.docker.internal"
    assert captured["config"]["futu_port"] == 11112
    assert captured["config"]["acc_id"] == 99


def test_futu_strategy_cannot_override_saved_simulate_account(monkeypatch):
    monkeypatch.setattr(
        "app.services.exchange_execution._load_credential_config",
        lambda *_args, **_kwargs: {
            "exchange_id": "futu",
            "futu_host": "host.docker.internal",
            "futu_port": 11112,
            "trade_env": "demo",
            "trade_market": "US",
            "security_firm": "FUTUSECURITIES",
            "acc_id": 99,
        },
    )

    resolved = resolve_exchange_config({
        "credential_id": 7,
        "futu_host": "10.0.0.8",
        "futu_port": 11111,
        "trade_env": "live",
        "trade_market": "HK",
        "security_firm": "FUTUINC",
        "acc_id": 100,
        "market_type": "USStock",
    })

    assert resolved["futu_host"] == "host.docker.internal"
    assert resolved["futu_port"] == 11112
    assert resolved["trade_env"] == "demo"
    assert resolved["trade_market"] == "US"
    assert resolved["security_firm"] == "FUTUSECURITIES"
    assert resolved["acc_id"] == 99
    assert resolved["market_type"] == "USStock"
    assert resolved["_operator_credential_id"] == 7


def test_futu_inline_or_missing_credential_cannot_gain_operator_permission(monkeypatch):
    monkeypatch.setattr(
        "app.services.exchange_execution._load_credential_config",
        lambda *_args, **_kwargs: {},
    )
    resolved = resolve_exchange_config({
        "exchange_id": "futu", "credential_id": 7,
        "trade_env": "demo", "trade_market": "US", "acc_id": 99,
    }, user_id=1)
    assert resolved.get("_operator_credential_id", 0) == 0


def test_safe_exchange_config_masks_futu_unlock_password():
    safe = safe_exchange_config_for_log({
        "exchange_id": "futu",
        "unlock_password": "super-secret-password",
        "unlockPassword": "second-secret-password",
    })
    assert "super-secret-password" not in str(safe)
    assert "second-secret-password" not in str(safe)
