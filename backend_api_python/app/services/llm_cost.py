"""Display-only per-response LLM cost estimates (not billing records).

Rates are per million tokens, verified against the linked provider price pages.
Unknown models deliberately have no estimated price.
"""
from __future__ import annotations

from datetime import datetime, time, timezone
from zoneinfo import ZoneInfo


VOLC_PRICE_URL = "https://docs.volcengine.com/docs/ark/model-pricing?lang=zh"
DEEPSEEK_PRICE_URL = "https://api-docs.deepseek.com/quick_start/pricing/"
OPENAI_PRICE_URL = "https://developers.openai.com/api/docs/pricing"


def _is_peak(provider: str, now: datetime) -> bool:
    if provider == "volcengine":
        local = now.astimezone(ZoneInfo("Asia/Shanghai"))
        clock = local.time()
        return local.weekday() < 5 and (time(9) <= clock < time(12) or time(14) <= clock < time(18))
    local = now.astimezone(timezone.utc)
    clock = local.time()
    return local.weekday() < 5 and (time(1) <= clock < time(4) or time(6) <= clock < time(10))


def _price(provider: str, model: str, now: datetime):
    model = model.lower().strip().replace("_", "-")
    if provider == "volcengine":
        if model in {"deepseek-v4-1-flash", "deepseek-v4.1-flash", "deepseek-v4-1-flash-260910"}:
            return (2.0, 0.04, 8.0, "CNY", VOLC_PRICE_URL) if _is_peak(provider, now) else (1.0, 0.02, 4.0, "CNY", VOLC_PRICE_URL)
        if model == "deepseek-v4-pro":
            return 9.0, 0.30, 27.0, "CNY", VOLC_PRICE_URL
        if model == "deepseek-v4-flash":
            return 3.0, 0.10, 9.0, "CNY", VOLC_PRICE_URL
    if provider == "deepseek":
        if model in {"deepseek-flash", "deepseek-v4-flash"}:
            return (0.3, 0.006, 1.2, "USD", DEEPSEEK_PRICE_URL) if _is_peak(provider, now) else (0.15, 0.003, 0.6, "USD", DEEPSEEK_PRICE_URL)
        if model == "deepseek-v4-pro":
            return (1.32, 0.044, 3.96, "USD", DEEPSEEK_PRICE_URL) if _is_peak(provider, now) else (0.66, 0.022, 1.98, "USD", DEEPSEEK_PRICE_URL)
    if provider == "openai":
        rates = {
            "gpt-5.4": (2.5, 0.25, 15.0),
            "gpt-5.2": (1.75, 0.175, 14.0),
            "gpt-6-astra": (10.0, 1.0, 50.0),
            "gpt-6.1-sol": (2.0, 0.10, 10.0),
            "gpt-6-luna": (0.10, 0.01, 0.50),
        }
        if model in rates:
            return *rates[model], "USD", OPENAI_PRICE_URL
    return None


def build_usage_display(*, provider: str, model: str, usage: dict | None,
                        estimated_input_tokens: int, estimated_output_tokens: int,
                        now: datetime | None = None) -> dict:
    """Return a safe, honest usage summary; never infer a price for unknown routes."""
    usage = usage if isinstance(usage, dict) else {}
    actual = isinstance(usage.get("prompt_tokens"), int) and isinstance(usage.get("completion_tokens"), int)
    input_tokens = max(0, usage["prompt_tokens"] if actual else int(estimated_input_tokens or 0))
    output_tokens = max(0, usage["completion_tokens"] if actual else int(estimated_output_tokens or 0))
    details = usage.get("prompt_tokens_details") or {}
    cached_tokens = max(0, min(input_tokens, int(details.get("cached_tokens") or usage.get("prompt_cache_hit_tokens") or 0))) if actual else 0
    normalized_provider = str(provider or "").strip().lower()
    normalized_model = str(model or "").strip()
    when = now or datetime.now(timezone.utc)
    rate = _price(normalized_provider, normalized_model, when)
    result = {
        "provider": normalized_provider,
        "model": normalized_model,
        "reasoning_effort": usage.get('reasoning_effort', 'default'),
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "total_tokens": input_tokens + output_tokens,
        "cached_input_tokens": cached_tokens,
        "token_source": "provider" if actual else "estimated",
        "estimated_cost": None,
        "currency": None,
        "price_source": None,
    }
    if rate:
        input_rate, cached_rate, output_rate, currency, source = rate
        result["estimated_cost"] = round(((input_tokens - cached_tokens) * input_rate + cached_tokens * cached_rate + output_tokens * output_rate) / 1_000_000, 8)
        result["currency"] = currency
        result["price_source"] = source
    return result


def aggregate_usage_display(events: list[dict]) -> dict | None:
    """Aggregate a multi-call generation without hiding repair/retry token costs."""
    summaries = [build_usage_display(
        provider=event.get("provider", ""), model=event.get("model", ""),
        usage=event.get("usage"), estimated_input_tokens=0, estimated_output_tokens=0,
    ) for event in events if isinstance(event, dict)]
    if not summaries:
        return None
    providers = {item["provider"] for item in summaries}
    models = {item["model"] for item in summaries}
    currencies = {item["currency"] for item in summaries}
    result = {
        "provider": summaries[0]["provider"] if len(providers) == 1 else "multiple",
        "model": summaries[0]["model"] if len(models) == 1 else "multiple",
        "input_tokens": sum(item["input_tokens"] for item in summaries),
        "output_tokens": sum(item["output_tokens"] for item in summaries),
        "total_tokens": sum(item["total_tokens"] for item in summaries),
        "cached_input_tokens": sum(item["cached_input_tokens"] for item in summaries),
        "token_source": "provider",
        "estimated_cost": None,
        "currency": None,
        "price_source": None,
        "request_count": len(summaries),
        "reasoning_effort": summaries[0]['reasoning_effort'] if len({item['reasoning_effort'] for item in summaries}) == 1 else 'multiple',
    }
    if len(currencies) == 1 and None not in currencies and all(item["estimated_cost"] is not None for item in summaries):
        result["estimated_cost"] = round(sum(item["estimated_cost"] for item in summaries), 8)
        result["currency"] = summaries[0]["currency"]
        sources = {item["price_source"] for item in summaries}
        result["price_source"] = summaries[0]["price_source"] if len(sources) == 1 else None
    return result
