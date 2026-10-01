from datetime import datetime, timezone

from app.services.llm_cost import aggregate_usage_display, build_usage_display


def test_volcengine_flash_peak_price_uses_cached_input():
    result = build_usage_display(
        provider="volcengine",
        model="deepseek-v4-1-flash",
        usage={"prompt_tokens": 1000, "completion_tokens": 500, "prompt_tokens_details": {"cached_tokens": 200}},
        estimated_input_tokens=1,
        estimated_output_tokens=1,
        now=datetime(2026, 10, 1, 2, 0, tzinfo=timezone.utc),
    )
    assert result["token_source"] == "provider"
    assert result["total_tokens"] == 1500
    assert result["cached_input_tokens"] == 200
    assert result["currency"] == "CNY"
    assert result["estimated_cost"] == round((800 * 2 + 200 * .04 + 500 * 8) / 1_000_000, 8)


def test_unknown_route_has_tokens_but_not_invented_price():
    result = build_usage_display(
        provider="custom", model="ep-unknown",
        usage=None, estimated_input_tokens=123, estimated_output_tokens=45,
    )
    assert result["token_source"] == "estimated"
    assert result["total_tokens"] == 168
    assert result["estimated_cost"] is None


def test_deepseek_official_price_is_not_volcengine_price():
    result = build_usage_display(
        provider="deepseek", model="deepseek-flash",
        usage={"prompt_tokens": 1_000_000, "completion_tokens": 1_000_000},
        estimated_input_tokens=0, estimated_output_tokens=0,
        now=datetime(2026, 10, 3, 0, 0, tzinfo=timezone.utc),
    )
    assert result["currency"] == "USD"
    assert result["estimated_cost"] == .75


def test_multi_call_strategy_generation_includes_repair_tokens():
    result = aggregate_usage_display([
        {"provider": "volcengine", "model": "deepseek-v4-1-flash", "usage": {"prompt_tokens": 100, "completion_tokens": 50}},
        {"provider": "volcengine", "model": "deepseek-v4-1-flash", "usage": {"prompt_tokens": 200, "completion_tokens": 80}},
    ])
    assert result["request_count"] == 2
    assert result["total_tokens"] == 430
    assert result["currency"] == "CNY"
