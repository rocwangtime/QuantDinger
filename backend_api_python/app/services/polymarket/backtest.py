"""Bounded offline late-window study. Never imports an execution or network client."""

from bisect import bisect_right
from collections import Counter
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_DOWN, ROUND_UP
import json
import random
from uuid import uuid4

from app.utils.db import get_db_connection

VERSION = "late_window_v1"
MAX_ROWS = 100000


def number(value):
    try:
        value = Decimal(str(value))
        return value if value.is_finite() and value > 0 else None
    except Exception:
        return None


def fee(price):
    return Decimal(".07") * price * (1 - price)


def size_at_ask(asks, cap):
    """Only best ask liquidity; fee-inclusive cost never exceeds the cash cap."""
    levels = [(number(r.get("price")), number(r.get("size"))) for r in asks]
    levels = [(p, q) for p, q in levels if p and q and p <= 1]
    if not levels:
        return None
    price = min(p for p, _ in levels)
    depth = sum(q for p, q in levels if p == price)
    shares = min(depth, cap / (price + fee(price))).quantize(Decimal(".00000001"), rounding=ROUND_DOWN)
    charge = (shares * fee(price)).quantize(Decimal(".00000001"), rounding=ROUND_UP)
    if shares * price + charge > cap:
        # Reserve one fee rounding quantum in one step, even for tiny asks.
        shares = min(depth, (cap - Decimal(".00000001")) / (price + fee(price))).quantize(
            Decimal(".00000001"), rounding=ROUND_DOWN
        )
        charge = (shares * fee(price)).quantize(Decimal(".00000001"), rounding=ROUND_UP)
    return (price, shares, charge) if shares > 0 else None


def fresh_book(book, now, max_age):
    if not book or book.get("exchange_at") is None:
        return False
    latency = (book["received_at"] - book["exchange_at"]).total_seconds()
    age = (now - book["received_at"]).total_seconds()
    return 0 <= latency <= max_age and 0 <= age <= max_age


def latest(rows, at, field):
    index = bisect_right(rows, at, key=lambda r: r[field]) - 1
    return rows[index] if index >= 0 else None


def proxy(rows, at):
    index = bisect_right(rows, at, key=lambda r: r["observed_at"]) - 1
    while index >= 0:
        row = rows[index]
        if (at - row["observed_at"]).total_seconds() > 10:
            break
        observed = row.get("reference_observed_at")
        if (
            row.get("reference_source") in ("api.binance.com", "data-api.binance.vision")
            and observed
            and observed <= row["observed_at"]
            and 0 <= (at - observed).total_seconds() <= 10
        ):
            return number(row.get("reference_price"))
        index -= 1
    return None


def baseline(window, samples, allow_proxy):
    # Only exact recorded boundary TWAP establishes Chainlink price-to-beat.
    price = number(window.get("price_to_beat"))
    if price and window.get("start_twap_at") == window["start_time"]:
        return price, "chainlink_twap"
    return (proxy(samples, window["start_time"]), "binance_proxy") if allow_proxy else (None, "unavailable")


def outcome(window, samples, allow_proxy):
    if window.get("official_outcome") in ("Up", "Down"):
        return window["official_outcome"], "official_resolution"
    start, source = baseline(window, samples, False)
    end = number(window.get("settlement_twap"))
    if start and end and window.get("settlement_twap_at") == window["end_time"]:
        return ("Up" if end >= start else "Down"), source
    if allow_proxy:
        start, end = proxy(samples, window["start_time"]), proxy(samples, window["end_time"])
        if start and end:
            return ("Up" if end >= start else "Down"), "binance_proxy"
    return None, "unavailable"


def reference(ticks, samples, at, allow_proxy):
    # Receive-time ordering prevents replay lookahead, even for delayed exchange ticks.
    tick = latest(ticks, at, "received_at")
    if tick and 0 <= (at - tick["exchange_at"]).total_seconds() <= 5:
        return number(tick["price"]), "chainlink_twap"
    return (proxy(samples, at), "binance_proxy") if allow_proxy else (None, "unavailable")


def evaluate(window, samples, books, ticks, config, seconds, threshold):
    counts = Counter()
    expected = {"5m": 300, "15m": 900}.get(window.get("interval"))
    if (
        window.get("asset") not in ("BTC", "ETH")
        or expected is None
        or (window["end_time"] - window["start_time"]).total_seconds() != expected
    ):
        counts["invalid_window_windows"] += 1
        return None, counts
    start, start_source = baseline(window, samples, config["allow_proxy"])
    winner, settlement_source = outcome(window, samples, config["allow_proxy"])
    if not start or not winner:
        counts["missing_evidence_windows"] += 1
        return None, counts
    if not window.get("up_token_id") or not window.get("down_token_id"):
        counts["missing_token_windows"] += 1
        return None, counts
    begin = max(window["start_time"], window["end_time"] - timedelta(seconds=seconds))
    # Evaluate on every recorded book/reference/sample update. A book-only event
    # can admit a fresh quote without pretending samples are continuous.
    times = sorted(
        {r["received_at"] for r in books + ticks if begin <= r["received_at"] < window["end_time"]}
        | {r["observed_at"] for r in samples if begin <= r["observed_at"] < window["end_time"]}
    )
    by_token = {
        token: sorted([b for b in books if b["token_id"] == token], key=lambda b: b["received_at"])
        for token in (window["up_token_id"], window["down_token_id"])
    }
    for at in times:
        entry_start, entry_source = start, start_source
        ref, ref_source = reference(ticks, samples, at, False)
        # Prefer a complete Chainlink pair. When either half is unavailable,
        # explicit proxy mode may use a complete Binance pair instead.
        if not ref or entry_source != ref_source:
            if config["allow_proxy"]:
                entry_start = proxy(samples, window["start_time"])
                ref = proxy(samples, at)
                entry_source = ref_source = "binance_proxy"
            else:
                ref = None
        if not ref or not entry_start:
            counts["missing_reference_events"] += 1
            continue
        delta = (ref / entry_start - 1) * 10000
        if abs(delta) < threshold or delta == 0:
            continue
        side = "Up" if delta > 0 else "Down"
        token = window["up_token_id"] if side == "Up" else window["down_token_id"]
        book = latest(by_token[token], at, "received_at")
        if not fresh_book(book, at, config["max_book_age_seconds"]):
            counts["stale_book_events"] += 1
            continue
        fill = size_at_ask(book["asks"], Decimal(str(config["cap"])))
        if not fill:
            counts["missing_depth_events"] += 1
            continue
        price, shares, charge = fill
        cost = shares * price + charge
        payout = shares if side == winner else Decimal(0)
        trade = dict(
            market_id=window["market_id"],
            entry_at=at.isoformat(),
            end_time=window["end_time"].isoformat(),
            side=side,
            outcome=winner,
            won=side == winner,
            ask=float(price),
            shares=float(shares),
            fee=float(charge),
            cost=float(cost),
            pnl=float(payout - cost),
            price_to_beat=float(entry_start),
            reference=float(ref),
            book_id=str(book["recording_id"]),
            evidence_source=dict(price_to_beat=entry_source, reference=ref_source, settlement=settlement_source),
        )
        return trade, counts
    counts["no_entry_windows"] += 1
    return None, counts


def statistics(trades, repetitions):
    trades = sorted(trades, key=lambda t: (t["end_time"], t["market_id"]))
    pnl, cost = sum(t["pnl"] for t in trades), sum(t["cost"] for t in trades)
    equity = peak = drawdown = 0
    for trade in trades:
        equity += trade["pnl"]
        peak = max(peak, equity)
        drawdown = max(drawdown, peak - equity)
    rng = random.Random(0)
    returns = []
    for _ in range(repetitions if trades else 0):
        sample = rng.choices(trades, k=len(trades))
        returns.append(sum(t["pnl"] for t in sample) / sum(t["cost"] for t in sample))
    returns.sort()
    mix = Counter(json.dumps(t["evidence_source"], sort_keys=True) for t in trades)
    return dict(
        trades=len(trades),
        win_rate=sum(t["won"] for t in trades) / len(trades) if trades else None,
        avg_ask=sum(t["ask"] for t in trades) / len(trades) if trades else None,
        net_pnl=pnl,
        cost=cost,
        return_on_cost=pnl / cost if cost else None,
        max_drawdown=drawdown,
        return_ci_95=[returns[int((len(returns) - 1) * q)] for q in (0.025, 0.975)] if returns else None,
        evidence_source_mix=dict(mix),
    )


def validate(config):
    config = dict(config)
    for key in ("start", "end"):
        value = config[key]
        value = datetime.fromisoformat(value.replace("Z", "+00:00")) if isinstance(value, str) else value
        if value.tzinfo is None:
            raise ValueError("Time bounds require timezone")
        config[key] = value.astimezone(timezone.utc).isoformat()
    start, end = (datetime.fromisoformat(config[k]) for k in ("start", "end"))
    if not timedelta(0) < end - start <= timedelta(days=7) or end > datetime.now(timezone.utc):
        raise ValueError("Require past range of at most seven days")
    defaults = dict(
        allow_proxy=False,
        cap=1000,
        max_book_age_seconds=1,
        seconds=[60],
        thresholds=[3],
        max_windows=1000,
        bootstrap=1000,
    )
    config = {**defaults, **config}
    if type(config["allow_proxy"]) is not bool:
        raise ValueError("allow_proxy must be boolean")
    for key, low, high in [
        ("cap", 0.01, 1000),
        ("max_book_age_seconds", 0.001, 60),
        ("max_windows", 1, 1000),
        ("bootstrap", 100, 2000),
    ]:
        if (key in ("max_windows", "bootstrap") and type(config[key]) is not int) or not low <= config[key] <= high:
            raise ValueError("Invalid bounded setting: " + key)
    for key, high in [("seconds", 900), ("thresholds", 10000)]:
        if (
            not isinstance(config[key], list)
            or not 1 <= len(config[key]) <= 9
            or any(not 0 < v <= high for v in config[key])
        ):
            raise ValueError("Invalid grid: " + key)
        config[key] = sorted(set(config[key]))
    if len(config["seconds"]) * len(config["thresholds"]) * config["max_windows"] * config["bootstrap"] > 18000000:
        raise ValueError("Bootstrap work budget exceeded")
    return config


def fetch(cur, sql, params, limit=MAX_ROWS):
    cur.execute(sql + " LIMIT %s", (*params, limit + 1))
    rows = cur.fetchall() or []
    if len(rows) > limit:
        raise ValueError("Recording bound exceeded; narrow the date range")
    return rows


def run(config):
    config = validate(config)
    groups = {
        (interval, asset, t, d): ([], Counter())
        for interval in ("5m", "15m")
        for asset in ("BTC", "ETH")
        for t in config["seconds"]
        for d in config["thresholds"]
    }
    with get_db_connection() as db:
        # Pool checkout health probes can already have started a transaction.
        # This connection is owned by this job; discard the probe transaction.
        db.rollback()
        cur = db.cursor()
        try:
            # One consistent recording snapshot; concurrent observer writes cannot
            # produce a book/reference mismatch inside a study.
            cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            cur.execute("SET LOCAL statement_timeout='30s'")
            windows = fetch(
                cur,
                "SELECT * FROM qd_polymarket_observer_windows WHERE end_time >= %s AND end_time < %s AND asset IN ('BTC','ETH') AND interval IN ('5m','15m') ORDER BY end_time,market_id",
                (config["start"], config["end"]),
                config["max_windows"],
            )
            total_rows = 0
            for window in windows:
                bounds = (window["market_id"], window["start_time"] - timedelta(seconds=10), window["end_time"])
                samples = fetch(
                    cur,
                    "SELECT * FROM qd_polymarket_observer_samples WHERE market_id=%s AND observed_at >= %s AND observed_at <= %s ORDER BY observed_at,id",
                    bounds,
                )
                books = fetch(
                    cur,
                    "SELECT * FROM qd_polymarket_observer_books WHERE market_id=%s AND received_at >= %s AND received_at < %s ORDER BY received_at,recording_id",
                    bounds,
                )
                ticks = fetch(
                    cur,
                    "SELECT * FROM qd_polymarket_observer_twap WHERE asset=%s AND window_seconds=60 AND received_at >= %s AND received_at < %s ORDER BY received_at,exchange_at",
                    (window["asset"], bounds[1], bounds[2]),
                )
                total_rows += len(samples) + len(books) + len(ticks)
                if total_rows > MAX_ROWS:
                    raise ValueError("Total recording bound exceeded; narrow the date range")
                for t in config["seconds"]:
                    for d in config["thresholds"]:
                        trades, counts = groups[(window["interval"], window["asset"], t, d)]
                        trade, filtered = evaluate(window, samples, books, ticks, config, t, d)
                        counts.update(filtered)
                        if trade:
                            trades.append(trade)
            rows = [
                dict(
                    interval=i,
                    asset=a,
                    T=t,
                    D=d,
                    **statistics(trades, config["bootstrap"]),
                    filtered_counts=dict(counts),
                    entries=trades,
                )
                for (i, a, t, d), (trades, counts) in groups.items()
            ]
            identity = str(uuid4())
            result = dict(
                id=identity, engine=VERSION, windows=len(windows), recording_rows=total_rows, config=config, groups=rows
            )
            cur.execute(
                "INSERT INTO qd_polymarket_backtests(id,config,result) VALUES (%s,%s::jsonb,%s::jsonb)",
                (identity, json.dumps(config), json.dumps(result)),
            )
            db.commit()
            return result
        finally:
            cur.close()


def reports(limit=20):
    if not 1 <= limit <= 100:
        raise ValueError("limit must be 1..100")
    with get_db_connection() as db:
        cur = db.cursor()
        try:
            cur.execute(
                "SELECT id,created_at,config,result FROM qd_polymarket_backtests ORDER BY created_at DESC LIMIT %s",
                (limit,),
            )
            return cur.fetchall() or []
        finally:
            cur.close()


def markdown(result):
    lines = [
        "# Polymarket late-window offline paper backtest",
        "",
        f"Run `{result['id']}`; {result['windows']} recorded windows. Proxy allowed: {result['config']['allow_proxy']}.",
        "Fees included in cost. Drawdown is realized settlement PnL in USD, not funded portfolio drawdown.",
        "Bootstrap: seeded independent window resampling, 95% percentile CI; sparse/correlated windows limit inference.",
        "",
        "| Interval | Asset | T | D bps | Trades | Win rate | Avg ask | Net PnL | Return/cost | 95% CI | Max DD | Filtered counts | Evidence mix |",
        "|---|---|---|---|---|---|---|---|---|---|---|---|---|",
    ]
    for row in result["groups"]:
        values = [
            row[k]
            for k in (
                "interval",
                "asset",
                "T",
                "D",
                "trades",
                "win_rate",
                "avg_ask",
                "net_pnl",
                "return_on_cost",
                "return_ci_95",
                "max_drawdown",
                "filtered_counts",
                "evidence_source_mix",
            )
        ]
        lines.append("| " + " | ".join(str(v) if v is not None else "unknown" for v in values) + " |")
    return "\n".join(lines) + "\n"
