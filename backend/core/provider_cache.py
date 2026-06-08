"""DB-backed TTL cache for TMDB/TVDB/Skyhook responses.

Self-manages its own session so provider getters need no `db` param.
Best-effort: errors are swallowed, never block a real fetch.
"""
import hashlib
import json
from datetime import datetime, timedelta
from typing import Awaitable, Callable

from db import AsyncSessionLocal
from models.provider_cache import ProviderCache

TTL_SHOW = 6 * 3600
TTL_SEASON = 6 * 3600
TTL_EPISODE = 6 * 3600
TTL_MOVIE = 24 * 3600
TTL_IMAGES = 24 * 3600
TTL_SKYHOOK = 12 * 3600
TTL_IDS = 7 * 24 * 3600
TTL_TRENDING = 30 * 60
TTL_LIST = 6 * 3600       # popular/top-rated/now-playing/upcoming/on-air/recommendations/discover
TTL_SEARCH = 6 * 3600     # search results
TTL_CONFIG = 7 * 24 * 3600  # genres, languages, countries, provider list — rarely change


def _key(provider: str, endpoint: str, params: dict) -> str:
    raw = f"{provider}|{endpoint}|{json.dumps(params, sort_keys=True, default=str)}"
    return hashlib.sha256(raw.encode()).hexdigest()


async def get(provider: str, endpoint: str, params: dict) -> dict | None:
    try:
        key = _key(provider, endpoint, params)
        async with AsyncSessionLocal() as s:
            row = await s.get(ProviderCache, key)
            if row and row.expires_at > datetime.utcnow():
                return row.value
    except Exception:
        pass
    return None


async def set(provider: str, endpoint: str, params: dict, value: dict, ttl: int) -> None:
    try:
        key = _key(provider, endpoint, params)
        expires = datetime.utcnow() + timedelta(seconds=ttl)
        async with AsyncSessionLocal() as s:
            row = await s.get(ProviderCache, key)
            if row:
                row.value = value
                row.expires_at = expires
            else:
                s.add(ProviderCache(cache_key=key, value=value, expires_at=expires))
            await s.commit()
    except Exception:
        pass


async def cached(
    provider: str,
    endpoint: str,
    params: dict,
    ttl: int,
    fetch: Callable[[], Awaitable[dict]],
) -> dict:
    """Return cached value, else fetch + store. Only truthy results are cached."""
    hit = await get(provider, endpoint, params)
    if hit is not None:
        return hit
    value = await fetch()
    if value:
        await set(provider, endpoint, params, value, ttl)
    return value
