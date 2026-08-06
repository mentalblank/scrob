import os
import unittest
from unittest.mock import AsyncMock, patch

import httpx

os.environ.setdefault("SECRET_KEY", "test-secret")
os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://test:test@localhost/test")

from core import tmdb


def _response(status: int, payload: dict | None = None) -> httpx.Response:
    return httpx.Response(
        status,
        json=payload if payload is not None else {"status_code": 43},
        request=httpx.Request("GET", "https://api.themoviedb.org/3/movie/9480"),
    )


class _FakeClient:
    """Stands in for httpx.AsyncClient, replaying a queued list of responses."""

    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = 0

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def get(self, url, headers=None, params=None):
        self.calls += 1
        return self.responses.pop(0)


def _patched(responses):
    client = _FakeClient(responses)
    return client, patch.object(tmdb.httpx, "AsyncClient", lambda *a, **k: client), \
        patch.object(tmdb.asyncio, "sleep", AsyncMock())


class RetryTests(unittest.IsolatedAsyncioTestCase):
    async def test_transient_5xx_is_retried_until_it_succeeds(self) -> None:
        """TMDB answers 502 for its own upstream blips; the same request works
        moments later, so a single bad gateway must not fail enrichment."""
        client, client_patch, sleep_patch = _patched(
            [_response(502), _response(502), _response(200, {"id": 9480})]
        )
        with client_patch, sleep_patch:
            data = await tmdb._get("https://api.themoviedb.org/3/movie/9480")

        self.assertEqual(data, {"id": 9480})
        self.assertEqual(client.calls, 3)

    async def test_5xx_raises_once_the_retries_are_spent(self) -> None:
        client, client_patch, sleep_patch = _patched([_response(503)] * 4)
        with client_patch, sleep_patch:
            with self.assertRaises(httpx.HTTPStatusError):
                await tmdb._get("https://api.themoviedb.org/3/movie/9480")

        self.assertEqual(client.calls, 4)  # first attempt + 3 retries

    async def test_4xx_is_not_retried(self) -> None:
        client, client_patch, sleep_patch = _patched([_response(404), _response(200, {"id": 1})])
        with client_patch, sleep_patch:
            with self.assertRaises(httpx.HTTPStatusError):
                await tmdb._get("https://api.themoviedb.org/3/movie/9480")

        self.assertEqual(client.calls, 1)


class ValidateApiKeyTests(unittest.IsolatedAsyncioTestCase):
    async def test_rejected_key_is_reported_as_invalid(self) -> None:
        client, client_patch, sleep_patch = _patched([_response(401)])
        with client_patch, sleep_patch:
            self.assertFalse(await tmdb.validate_api_key("bad-key"))

    async def test_outage_is_not_reported_as_an_invalid_key(self) -> None:
        client, client_patch, sleep_patch = _patched([_response(502)] * 4)
        with client_patch, sleep_patch:
            with self.assertRaises(tmdb.TMDBUnavailable):
                await tmdb.validate_api_key("good-key")

    async def test_accepted_key_is_valid(self) -> None:
        client, client_patch, sleep_patch = _patched([_response(200, {"success": True})])
        with client_patch, sleep_patch:
            self.assertTrue(await tmdb.validate_api_key("good-key"))
