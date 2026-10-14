from datetime import datetime, timedelta, timezone
from decimal import Decimal
import pytest
from app.services.polymarket import backtest as b


@pytest.fixture
def evidence():
    start = datetime(2026, 10, 10, tzinfo=timezone.utc)
    end = start + timedelta(minutes=5)
    at = end - timedelta(seconds=40)
    window = dict(
        market_id="m",
        asset="BTC",
        interval="5m",
        start_time=start,
        end_time=end,
        price_to_beat=100,
        start_twap_at=start,
        settlement_twap=101,
        settlement_twap_at=end,
        official_outcome="Up",
        up_token_id="u",
        down_token_id="d",
    )
    book = dict(recording_id="book", token_id="u", exchange_at=at, received_at=at, asks=[dict(price=".5", size="10")])
    tick = dict(exchange_at=at, received_at=at, price=101)
    return window, book, tick, dict(allow_proxy=False, cap=1000, max_book_age_seconds=1)


def test_exact_boundary_and_outcome(evidence):
    w, _, _, _ = evidence
    assert b.baseline(w, [], False) == (Decimal(100), "chainlink_twap")
    w["start_twap_at"] -= timedelta(milliseconds=1)
    assert b.baseline(w, [], False)[0] is None
    w["start_twap_at"] = w["start_time"]
    w.update(official_outcome=None, settlement_twap=100)
    assert b.outcome(w, [], False) == ("Up", "chainlink_twap")
    w["official_outcome"] = "Down"
    assert b.outcome(w, [], False) == ("Down", "official_resolution")


def test_entry_fee_depth_window(evidence):
    w, book, tick, c = evidence
    trade, counts = b.evaluate(w, [], [book, book], [tick], c, 60, 3)
    assert (trade["shares"], trade["fee"], trade["cost"], trade["pnl"]) == (10, 0.175, 5.175, 4.825)
    assert counts == {}
    assert b.evaluate(w, [], [book], [tick], c, 30, 3)[0] is None
    book["received_at"] = tick["received_at"] = w["end_time"]
    assert b.evaluate(w, [], [book], [tick], c, 60, 3)[0] is None


@pytest.mark.parametrize(
    "latency,age,expected",
    [(0, 0, True), (1, 1, True), (1.001, 0, False), (0, 1.001, False), (-1, 0, False), (0, -1, False)],
)
def test_two_stale_clocks(evidence, latency, age, expected):
    _, book, _, _ = evidence
    at = book["received_at"]
    book["exchange_at"] = at - timedelta(seconds=latency)
    assert b.fresh_book(book, at + timedelta(seconds=age), 1) is expected
    book["exchange_at"] = None
    assert not b.fresh_book(book, at, 1)


def test_stale_filtered_count(evidence):
    w, book, tick, c = evidence
    book["exchange_at"] -= timedelta(seconds=2)
    trade, counts = b.evaluate(w, [], [book], [tick], c, 60, 3)
    assert trade is None and counts["stale_book_events"] == 1


def test_cap_fee_and_depth():
    assert b.fee(Decimal(".5")) == Decimal(".0175")
    p, q, f = b.size_at_ask([dict(price=".9", size="10000"), dict(price=".8", size="10000")], Decimal(1000))
    assert p == Decimal(".8") and q <= 10000 and q * p + f <= 1000
    assert b.size_at_ask([], Decimal(1000)) is None


def test_proxy_opt_in_and_labels(evidence):
    w, book, _, c = evidence
    w.update(price_to_beat=None, settlement_twap=None, official_outcome=None)
    samples = [
        dict(observed_at=at, reference_observed_at=at, reference_price=p, reference_source="api.binance.com")
        for at, p in [(w["start_time"], 100), (book["received_at"], 101), (w["end_time"], 102)]
    ]
    assert b.evaluate(w, samples, [book], [], c, 60, 3)[0] is None
    c["allow_proxy"] = True
    trade, _ = b.evaluate(w, samples, [book], [], c, 60, 3)
    assert set(trade["evidence_source"].values()) == {"binance_proxy"}
    w["official_outcome"] = "Down"
    trade, _ = b.evaluate(w, samples, [book], [], c, 60, 3)
    assert trade["evidence_source"]["settlement"] == "official_resolution"
    assert trade["pnl"] == -trade["cost"]


def test_no_lookahead_missing_identity(evidence):
    w, book, tick, c = evidence
    tick["received_at"] += timedelta(seconds=2)
    assert b.evaluate(w, [], [book], [tick], c, 60, 3)[0] is None
    w["up_token_id"] = None
    assert b.evaluate(w, [], [book], [tick], c, 60, 3)[1]["missing_token_windows"] == 1


def test_statistics():
    trades = [
        dict(
            pnl=p,
            cost=10,
            won=p > 0,
            ask=0.5,
            end_time=str(i),
            market_id=str(i),
            evidence_source={"reference": "chainlink_twap"},
        )
        for i, p in enumerate([5, -10, 3])
    ]
    stats = b.statistics(trades, 100)
    assert stats["max_drawdown"] == 10 and stats["net_pnl"] == -2 and stats["return_on_cost"] == -2 / 30
    assert stats == b.statistics(trades, 100)
    assert b.statistics([], 100)["return_ci_95"] is None


def test_bounds():
    c = dict(start="2026-10-01T00:00:00+00:00", end="2026-10-02T00:00:00+00:00")
    assert b.validate(c)["seconds"] == [60]
    for changes in [
        dict(cap=1001),
        dict(end="2026-11-01T00:00:00+00:00"),
        dict(start="2026-10-01"),
        dict(allow_proxy="false"),
        dict(seconds=[0]),
    ]:
        with pytest.raises(ValueError):
            b.validate({**c, **changes})


def test_invalid_duration_and_proxy_freshness(evidence):
    w, book, tick, c = evidence
    w["end_time"] += timedelta(seconds=1)
    assert b.evaluate(w, [], [book], [tick], c, 60, 3)[1]["invalid_window_windows"] == 1
    at = book["received_at"]
    samples = [
        dict(
            observed_at=at,
            reference_observed_at=at - timedelta(seconds=11),
            reference_source="data-api.binance.vision",
            reference_price=100,
        )
    ]
    assert b.proxy(samples, at) is None
    samples[0]["reference_observed_at"] = at
    assert b.proxy(samples, at) == 100
    assert b.proxy(samples, at - timedelta(seconds=1)) is None


def test_report_auth_and_openapi(client, app):
    from app.openapi import get_openapi_api

    path = "/api/polymarket/backtests/report"
    assert client.get(path).status_code == 401
    assert path in get_openapi_api(app).spec.to_dict()["paths"]


def test_optional_task_bounds(monkeypatch):
    from app.tasks.polymarket_backtest import polymarket_backtest

    assert polymarket_backtest.soft_time_limit == 240
    assert polymarket_backtest.time_limit == 300
    with pytest.raises(ValueError):
        polymarket_backtest.run(dict(start="2026-10-01T00:00:00Z", end="2026-10-10T00:00:00Z"))


def test_proxy_fallback_uses_coherent_pair(evidence):
    w, book, _, c = evidence
    samples = [
        dict(observed_at=at, reference_observed_at=at, reference_price=p, reference_source="api.binance.com")
        for at, p in [(w["start_time"], 200), (book["received_at"], 202)]
    ]
    c["allow_proxy"] = True
    trade, _ = b.evaluate(w, samples, [book], [], c, 60, 3)
    assert trade["price_to_beat"] == 200
    assert trade["evidence_source"]["price_to_beat"] == "binance_proxy"
    assert trade["evidence_source"]["reference"] == "binance_proxy"


def test_best_ask_only_and_boundary_size():
    p, q, _ = b.size_at_ask([dict(price='.5', size='2'), dict(price='.6', size='100')], Decimal(1000))
    assert p == Decimal('.5') and q == 2
    assert b.size_at_ask([dict(price='1', size='2')], Decimal(1000)) == (Decimal(1), Decimal(2), Decimal(0))


def test_tiny_ask_fee_rounding_is_bounded():
    p, q, f = b.size_at_ask([dict(price='0.000000001', size='1000000000000')], Decimal('1000'))
    assert q > 0 and q*p+f <= 1000
