"""Deterministic complete-set quotes and paper fills from frozen depth.

Prices/quantities stay Decimal internally. This model charges the published
exponent-1 taker fee in collateral, assumes no queue priority, and earns no
rebates. It is not a broker gateway or an atomic two-leg fill simulator.
"""

import hashlib
import json
from decimal import Decimal, InvalidOperation, ROUND_CEILING, ROUND_DOWN
from pathlib import Path

ENGINE_VERSION = "polymarket-paper-1"
IMPLEMENTATION_HASH = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
ZERO = Decimal("0")
ONE = Decimal("1")
EPS = Decimal("0.000001")


def number(value):
    if isinstance(value, bool):
        raise ValueError("polymarket.invalidNumber")
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        raise ValueError("polymarket.invalidNumber") from None
    if not result.is_finite() or abs(result) > Decimal("1000000000"):
        raise ValueError("polymarket.invalidNumber")
    return result


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"),
                                      ensure_ascii=False, allow_nan=False).encode()).hexdigest()


def public(value):
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, dict):
        return {key: public(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [public(item) for item in value]
    return value


def _array(value):
    try:
        return json.loads(value) if isinstance(value, str) else value
    except (ValueError, TypeError):
        return None


def market_from_gamma(row):
    """Only exact YES/NO standard binary markets with verified fee metadata."""
    if not isinstance(row, dict):
        raise ValueError("polymarket.invalidMarket")
    if row.get("active") is not True or row.get("closed") is not False or row.get("archived") is True:
        raise ValueError("polymarket.marketClosed")
    if row.get("acceptingOrders") is not True or row.get("enableOrderBook") is not True:
        raise ValueError("polymarket.ordersUnavailable")
    if row.get("negRisk") is not False or any(event.get("negRiskAugmented") is True
                                             for event in row.get("events", []) if isinstance(event, dict)):
        raise ValueError("polymarket.negativeRiskExcluded")
    outcomes = _array(row.get("outcomes"))
    if not isinstance(outcomes, list) or len(outcomes) != 2 or {str(x).lower() for x in outcomes} != {"yes", "no"}:
        raise ValueError("polymarket.standardBinaryRequired")
    version = row.get("version") or "v1"
    if version not in {"v1", "v2"}:
        raise ValueError("polymarket.unsupportedVersion")
    ids = _array(row.get("positionIds") if version == "v2" else row.get("clobTokenIds"))
    if not isinstance(ids, list) or len(ids) != 2 or any(not str(x).isdigit() or len(str(x)) > 80 for x in ids) or str(ids[0]) == str(ids[1]):
        raise ValueError("polymarket.outcomeIdsMissing")
    condition = str(row.get("conditionId") or "")
    if not condition.startswith("0x") or len(condition) not in {64, 66}:
        raise ValueError("polymarket.conditionMissing")
    try:
        int(condition[2:], 16)
    except ValueError:
        raise ValueError("polymarket.conditionMissing") from None
    enabled, schedule = row.get("feesEnabled"), row.get("feeSchedule")
    if enabled is False:
        rate = ZERO
    elif enabled is True and isinstance(schedule, dict) and schedule.get("exponent") == 1:
        rate = number(schedule.get("rate"))
    else:
        raise ValueError("polymarket.feeUnknown")
    tick, minimum = number(row.get("orderPriceMinTickSize")), number(row.get("orderMinSize"))
    if not ZERO <= rate <= ONE or not ZERO < tick <= Decimal("0.1") or minimum <= ZERO:
        raise ValueError("polymarket.marketConstraintsUnknown")
    yes = next(i for i, label in enumerate(outcomes) if str(label).lower() == "yes")
    return public({"id": str(row["id"]), "question": str(row.get("question") or "")[:1000],
                   "slug": str(row.get("slug") or "")[:300], "conditionId": condition,
                   "version": version, "yesAssetId": str(ids[yes]), "noAssetId": str(ids[1 - yes]),
                   "feeRate": rate, "feeExponent": 1, "feesEnabled": enabled,
                   "tickSize": tick, "minOrderNotional": minimum, "collateral": "pUSD",
                   "description": str(row.get("description") or "")[:12000], "endDate": row.get("endDate")})


def normalize_book(raw, market, asset_id, observed_ms):
    if not isinstance(raw, dict) or str(raw.get("asset_id")) != asset_id or raw.get("market") != market["conditionId"]:
        raise ValueError("polymarket.bookIdentityMismatch")
    sides = {}
    for side in ("bids", "asks"):
        rows = raw.get(side)
        if not isinstance(rows, list) or len(rows) > 10000:
            raise ValueError("polymarket.invalidBook")
        levels = {}
        for row in rows:
            price, size = number(row.get("price")), number(row.get("size"))
            if not ZERO < price < ONE or size < ZERO or price in levels:
                raise ValueError("polymarket.invalidBook")
            if size:
                levels[price] = size
        sorted_levels = sorted(levels, reverse=side == "bids")
        sides[side] = [{"price": price, "size": levels[price]} for price in sorted_levels[:50]]
    if sides["bids"] and sides["asks"] and sides["bids"][0]["price"] >= sides["asks"][0]["price"]:
        raise ValueError("polymarket.crossedBook")
    return public({"assetId": asset_id, "conditionId": market["conditionId"], "observedMs": int(observed_ms),
                   "exchangeTimestamp": raw.get("timestamp"), "exchangeHash": str(raw.get("hash") or ""),
                   "depthLimit": 50, **sides})


def fee(quantity, price, rate):
    # Round upwards conservatively; do not credit unearned rebates.
    return (quantity * rate * price * (ONE - price)).quantize(Decimal("0.00001"), rounding=ROUND_CEILING)


def sweep(book, side, quantity, rate, *, limit=None, order_type="FAK"):
    remaining, fills = quantity, []
    for level in book["asks" if side == "BUY" else "bids"]:
        price = number(level["price"])
        if limit is not None and (price > limit if side == "BUY" else price < limit):
            break
        take = min(remaining, number(level["size"]))
        if take > ZERO:
            fills.append({"price": price, "quantity": take})
            remaining -= take
        if remaining <= ZERO:
            break
    if order_type == "FOK" and remaining > EPS:
        fills = []
    filled = sum((x["quantity"] for x in fills), ZERO)
    notional = sum((x["quantity"] * x["price"] for x in fills), ZERO)
    fees = fee(filled, notional / filled, rate) if filled and len(fills) == 1 else sum((fee(x["quantity"], x["price"], rate) for x in fills), ZERO)
    return {"side": side, "requested": quantity, "filled": filled, "notional": notional, "fee": fees,
            "averagePrice": notional / filled if filled else None, "worstPrice": fills[-1]["price"] if fills else None,
            "fills": fills, "status": "filled" if filled >= quantity - EPS else "partial" if filled else "unfilled"}


def validate_settings(payload):
    defaults = {"quantity": "100", "budget": "200", "minNetEdgeBps": "20", "slippageBps": "10",
                "settlementCost": "0.02", "riskReserve": "0.05", "latencyMs": 150, "legDelayMs": 250,
                "maxBookAgeMs": 5000, "maxUnhedgedMs": 10000, "maxUnwindLoss": "5", "orderType": "FOK"}
    settings = {**defaults, **payload}
    if set(settings) != set(defaults):
        raise ValueError("polymarket.unknownSetting")
    bounds = {"quantity": (EPS, 100000), "budget": (ONE, 1000000), "minNetEdgeBps": (ZERO, 10000),
              "slippageBps": (ZERO, 1000), "settlementCost": (ZERO, 1000), "riskReserve": (ZERO, 1000),
              "maxUnwindLoss": (ZERO, 1000000)}
    for key, (low, high) in bounds.items():
        value = number(settings[key])
        if not low <= value <= number(high):
            raise ValueError("polymarket.invalidSetting")
        settings[key] = format(value, "f")
    for key, low, high in [("latencyMs", 0, 2000), ("legDelayMs", 0, 3000), ("maxBookAgeMs", 500, 10000),
                           ("maxUnhedgedMs", 1000, 30000)]:
        value = number(settings[key])
        if value != int(value) or not low <= value <= high:
            raise ValueError("polymarket.invalidSetting")
        settings[key] = int(value)
    if settings["orderType"] not in {"FOK", "FAK"}:
        raise ValueError("polymarket.invalidOrderType")
    return settings


def books_at(frame, market, settings, now_ms):
    if frame.get("error"):
        raise ValueError(frame["error"])
    books = frame.get("books") or {}
    answer = []
    for asset in (market["yesAssetId"], market["noAssetId"]):
        book = books.get(asset)
        if not book or book["conditionId"] != market["conditionId"] or book["assetId"] != asset:
            raise ValueError("polymarket.bookIdentityMismatch")
        age = now_ms - int(book["observedMs"])
        if age < 0 or age > settings["maxBookAgeMs"]:
            raise ValueError("polymarket.staleBook")
        answer.append(book)
    return answer


def price_limits(legs, market, settings):
    tick = number(market["tickSize"])
    limits = []
    for leg in legs:
        buffered = number(leg["worstPrice"]) * (ONE + number(settings["slippageBps"]) / 10000)
        # Round down: rounding upwards would exceed the requested price cap.
        rounded = (buffered / tick).to_integral_value(rounding=ROUND_DOWN) * tick
        limits.append(min(ONE - tick, rounded))
    return limits


def quote(market, frame, settings):
    base = {"marketId": market["id"], "question": market["question"], "observedMs": frame["observedMs"],
            "eligible": False, "reason": None}
    try:
        books = books_at(frame, market, settings, frame["observedMs"])
        qty, budget, rate = number(settings["quantity"]), number(settings["budget"]), number(market["feeRate"])
        legs = [sweep(book, "BUY", qty, rate, order_type="FOK") for book in books]
        if any(leg["filled"] < qty - EPS for leg in legs):
            raise ValueError("polymarket.insufficientDepth")
        acquisition = sum((x["notional"] for x in legs), ZERO)
        fees = sum((x["fee"] for x in legs), ZERO)
        cost = acquisition + fees + number(settings["settlementCost"])
        net = qty - cost - number(settings["riskReserve"])
        edge = net / qty * 10000
        limits = price_limits(legs, market, settings)
        upper_cost = (sum((qty * p + fee(qty, p, rate) for p in limits), ZERO)
                      + number(settings["settlementCost"]) + Decimal("0.001"))
        worst_net = qty - upper_cost - number(settings["riskReserve"])
        reason = ("polymarket.belowMinimum" if any(x["notional"] < number(market["minOrderNotional"]) for x in legs)
                  else "polymarket.budgetExceeded" if cost > budget
                  else "polymarket.noNetEdge" if net <= ZERO or edge < number(settings["minNetEdgeBps"])
                  else "polymarket.priceBufferConsumesEdge" if worst_net <= ZERO or worst_net / qty * 10000 < number(settings["minNetEdgeBps"])
                  else None)
        base.update(eligible=reason is None, reason=reason, quantity=qty, acquisitionCost=acquisition, takerFees=fees,
                    settlementCost=number(settings["settlementCost"]), riskReserve=number(settings["riskReserve"]),
                    grossProfit=qty - acquisition, netProfit=net, netEdgeBps=edge, legs=legs,
                    entryPriceLimits=limits, worstCaseNetProfit=worst_net,
                    yesAsk=books[0]["asks"][0]["price"], noAsk=books[1]["asks"][0]["price"])
    except ValueError as exc:
        base["reason"] = str(exc)
    return public(base)


def simulate(bundle):
    """Replay exactly the observed frames, never fabricate a missing fill."""
    if bundle.get("engineVersion") != ENGINE_VERSION or bundle.get("implementationHash") != IMPLEMENTATION_HASH:
        raise ValueError("polymarket.engineVersionMismatch")
    market, settings, frames = bundle["market"], validate_settings(bundle["settings"]), bundle["frames"]
    signal = quote(market, frames["signal"], settings)
    cash, rate, qty = number(settings["budget"]), number(market["feeRate"]), number(settings["quantity"])
    result = {"mode": "paper", "engineVersion": ENGINE_VERSION, "implementationHash": IMPLEMENTATION_HASH,
              "signal": signal, "status": "rejected",
              "fills": [], "mergedQuantity": ZERO, "settlementCost": ZERO, "residuals": [], "realizedPnl": ZERO,
              "cash": cash, "initialCash": cash, "cashChange": ZERO, "residualCostBasis": ZERO,
              "reason": signal["reason"], "maxUnhedgedObservedMs": 0,
              "limitations": ["Depth snapshots do not prove live execution or queue priority",
                              "Buy and sell fees are conservatively modeled in collateral; no rebates",
                              "Runs are independent experiments, not a shared portfolio equity curve",
                              "Merge is modeled, not an onchain transaction; configured settlement cost is an assumption"]}
    if not signal["eligible"]:
        return public(result)
    tick = number(market["tickSize"])
    limits = [number(value) for value in signal["entryPriceLimits"]]
    # Reserve the worst possible two-leg spend before exposing the first leg.
    upper_cost = (sum((qty * p + fee(qty, p, rate) for p in limits), ZERO)
                  + number(settings["settlementCost"]) + Decimal("0.001"))  # per-level fee rounding bound
    if upper_cost > cash:
        result["reason"] = "polymarket.budgetExceeded"
        return public(result)
    held = [ZERO, ZERO]
    entry_cost = [ZERO, ZERO]
    first_at = None
    result["status"] = "unfilled"
    for index, role in enumerate(("leg1", "leg2")):
        target = qty if index == 0 else held[0]
        frame = frames.get(role) or {"error": "polymarket.observationMissing"}
        if frame.get("error"):
            result["reason"] = frame["error"]
            break
        at = int(frame.get("observedMs", 0))
        if first_at is not None and (at - first_at > settings["maxUnhedgedMs"] or at < first_at):
            result["reason"] = "polymarket.exposureTimeout"
            break
        try:
            if at < frames["signal"]["observedMs"] + settings["latencyMs"]:
                raise ValueError("polymarket.observationTooEarly")
            if index and at < first_at + settings["legDelayMs"]:
                raise ValueError("polymarket.observationTooEarly")
            book = books_at(frame, market, settings, at)[index]
            fill = sweep(book, "BUY", target, rate, limit=limits[index], order_type=settings["orderType"])
            if fill["notional"] and fill["notional"] < number(market["minOrderNotional"]):
                fill = sweep(book, "BUY", target, rate, limit=ZERO, order_type="FOK")
            result["fills"].append({"role": role, "assetId": book["assetId"], "observedMs": at, **fill})
            held[index] = fill["filled"]
            entry_cost[index] = fill["notional"] + fill["fee"]
            cash -= entry_cost[index]
            if index == 0:
                if not held[0]:
                    result["reason"] = "polymarket.firstLegUnfilled"
                    break
                first_at = at
            elif fill["filled"] < target - EPS:
                result["reason"] = "polymarket.secondLegUnfilled" if not fill["filled"] else "polymarket.partialFill"
        except ValueError as exc:
            result["reason"] = str(exc)
            break
    merged = min(held)
    settlement = number(settings["settlementCost"]) if merged else ZERO
    cash += merged - settlement
    held = [x - merged for x in held]
    result.update(mergedQuantity=merged, settlementCost=settlement)
    if first_at is not None:
        result["maxUnhedgedObservedMs"] = max(0, int(frames.get("leg2", {}).get("observedMs", first_at)) - first_at)
    # One bounded compensation attempt. A missing or expensive exit remains exposure.
    for index, remaining in enumerate(held):
        if remaining <= EPS:
            continue
        frame = frames.get("unwind") or {"error": "polymarket.observationMissing"}
        try:
            if frame.get("error"):
                raise ValueError(frame["error"])
            at = int(frame.get("observedMs", 0))
            if at < (first_at or 0):
                raise ValueError("polymarket.observationTooEarly")
            book = books_at(frame, market, settings, at)[index]
            result["maxUnhedgedObservedMs"] = max(result["maxUnhedgedObservedMs"], at - (first_at or at))
            average_cost = entry_cost[index] / (remaining + merged)
            min_price = max(tick, average_cost - number(settings["maxUnwindLoss"]) / remaining)
            fill = sweep(book, "SELL", remaining, rate, limit=min_price, order_type="FAK")
            # Include exit fees in the loss cap, not just the bid price.
            loss = average_cost * fill["filled"] - (fill["notional"] - fill["fee"])
            if loss > number(settings["maxUnwindLoss"]) or ZERO < fill["notional"] < number(market["minOrderNotional"]):
                fill = sweep(book, "SELL", remaining, rate, limit=ONE, order_type="FOK")
            result["fills"].append({"role": "unwind", "assetId": book["assetId"], "observedMs": at, **fill})
            cash += fill["notional"] - fill["fee"]
            held[index] -= fill["filled"]
            if held[index] > EPS:
                result["residuals"].append({"assetId": book["assetId"], "outcome": "YES" if index == 0 else "NO",
                                            "quantity": held[index], "costBasis": average_cost * held[index]})
        except ValueError as exc:
            result["reason"] = str(exc)
            result["residuals"].append({"assetId": market["yesAssetId" if index == 0 else "noAssetId"],
                                        "outcome": "YES" if index == 0 else "NO", "quantity": remaining,
                                        "costBasis": entry_cost[index] * remaining / (remaining + merged)})
    residual_cost = sum((x["costBasis"] for x in result["residuals"]), ZERO)
    result.update(cash=cash, realizedPnl=cash + residual_cost - number(settings["budget"]),
                  cashChange=cash - number(settings["budget"]), residualCostBasis=residual_cost)
    if result["residuals"]:
        result["status"] = "needs_review"
    elif merged:
        result["status"] = "merged" if merged >= qty - EPS else "partial_merged"
        result["reason"] = None
    elif result["fills"] and any(number(x["filled"]) for x in result["fills"]):
        result["status"] = "unwound"
    return public(result)
