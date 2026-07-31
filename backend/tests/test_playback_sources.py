import os
import unittest
from types import SimpleNamespace
from unittest.mock import patch

os.environ.setdefault("SECRET_KEY", "test-secret")
os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://test:test@localhost/test")

from models.base import CollectionSource
from routers.media import _media_server_web_url


def _conn(url="http://jelly.lan:8096", external=None):
    return SimpleNamespace(id=1, url=url, token="tok", external_server_url=external)


def _file(source, source_id="abc123"):
    return SimpleNamespace(source=CollectionSource(source), source_id=source_id)


class MediaServerWebUrlTests(unittest.IsolatedAsyncioTestCase):
    """The "server webpage" playback target opens these links in the user's browser."""

    async def test_jellyfin_link_points_at_item_details(self) -> None:
        url = await _media_server_web_url(_conn(), _file("jellyfin"), "srv9")
        self.assertEqual(url, "http://jelly.lan:8096/web/index.html#/details?id=abc123&serverId=srv9")

    async def test_emby_link_uses_its_own_route(self) -> None:
        url = await _media_server_web_url(_conn(), _file("emby"), None)
        self.assertEqual(url, "http://jelly.lan:8096/web/index.html#!/item?id=abc123")

    async def test_external_url_wins_over_internal_url(self) -> None:
        # The browser opening the link may not reach the server's LAN address.
        conn = _conn(external="https://media.example.com/")
        url = await _media_server_web_url(conn, _file("jellyfin"), None)
        self.assertEqual(url, "https://media.example.com/web/index.html#/details?id=abc123")

    async def test_no_link_without_a_source_id(self) -> None:
        self.assertIsNone(await _media_server_web_url(_conn(), _file("jellyfin", None), None))

    async def test_plex_link_needs_the_machine_identifier(self) -> None:
        async def fake_machine_id(url, token):
            return "machine-1"

        cache: dict[int, str | None] = {}
        with patch("core.plex.get_machine_identifier", fake_machine_id):
            url = await _media_server_web_url(_conn(), _file("plex", "5150"), None, cache)
        self.assertEqual(
            url,
            "http://jelly.lan:8096/web/index.html#!/server/machine-1/details"
            "?key=%2Flibrary%2Fmetadata%2F5150",
        )
        self.assertEqual(cache, {1: "machine-1"})

    async def test_plex_machine_identifier_is_looked_up_once_per_connection(self) -> None:
        calls = []

        async def fake_machine_id(url, token):
            calls.append(url)
            return "machine-1"

        cache: dict[int, str | None] = {}
        with patch("core.plex.get_machine_identifier", fake_machine_id):
            await _media_server_web_url(_conn(), _file("plex", "1"), None, cache)
            await _media_server_web_url(_conn(), _file("plex", "2"), None, cache)
        self.assertEqual(len(calls), 1)


if __name__ == "__main__":
    unittest.main()
