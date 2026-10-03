"""Cross-worker cancellation for an interactive AI generation.

Only an authenticated owner can address a request. A short-lived Redis marker
lets the SSE worker stop after an in-flight data/provider operation returns.
"""
from __future__ import annotations

from functools import lru_cache
from uuid import UUID

import redis

from app.config.redis_urls import cache_redis_url


def valid_request_id(value: str) -> str | None:
    try:
        return str(UUID(str(value)))
    except (TypeError, ValueError, AttributeError):
        return None


@lru_cache(maxsize=1)
def _client():
    return redis.Redis.from_url(cache_redis_url(), socket_connect_timeout=2, socket_timeout=2)


def _key(user_id: int, request_id: str) -> str:
    return f"quantdinger:ai-generation:cancel:{int(user_id)}:{request_id}"


def cancel_generation(user_id: int, request_id: str) -> bool:
    normalized = valid_request_id(request_id)
    if not normalized:
        return False
    _client().set(_key(user_id, normalized), "1", ex=600)
    return True


def generation_cancelled(user_id: int, request_id: str | None) -> bool:
    if not request_id:
        return False
    return bool(_client().exists(_key(user_id, request_id)))
