import json
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path
from uuid import uuid4
from app.services.polymarket import backtest
from tests.integration.test_polymarket_recording import database, market  # noqa: F401


def test_persistence_and_repeatable_migration(database, monkeypatch):
    db, cur = database

    @contextmanager
    def connect():
        cur.execute("SELECT 1")  # Reproduce the real pool checkout health probe.
        yield db

    monkeypatch.setattr(backtest, "get_db_connection", connect)
    m = market()
    start, end = datetime.fromisoformat(m["eventStartTime"]), datetime.fromisoformat(m["endDate"])
    at = end - timedelta(seconds=40)
    cur.execute(
        "INSERT INTO qd_polymarket_observer_windows(market_id,condition_id,asset,interval,start_time,end_time,price_to_beat,start_twap_at,official_outcome,up_token_id,down_token_id) VALUES ('123','c','BTC','15m',%s,%s,100,%s,'Up','u','d')",
        (start, end, start),
    )
    cur.execute(
        "INSERT INTO qd_polymarket_observer_books VALUES (%s,'123','u',%s,%s,'ws','[]'::jsonb,%s::jsonb)",
        (str(uuid4()), at, at, json.dumps([dict(price=".5", size="10")])),
    )
    cur.execute("INSERT INTO qd_polymarket_observer_twap VALUES ('BTC',60,%s,%s,101,'chainlink')", (at, at))
    db.commit()
    result = backtest.run(dict(start=start.isoformat(), end=(end + timedelta(seconds=1)).isoformat(), bootstrap=100))
    row = next(r for r in result["groups"] if r["asset"] == "BTC" and r["interval"] == "15m")
    assert row["trades"] == 1
    assert backtest.reports()[0]["result"] == result
    assert backtest.markdown(result).startswith("# Polymarket")
    sql = (Path(__file__).parents[2] / "migrations/20261014_polymarket_backtest.sql").read_text()
    cur.execute(sql)
    cur.execute(sql)
    db.commit()
    assert backtest.reports()[0]["result"] == result
