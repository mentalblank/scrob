import os
import unittest
from types import SimpleNamespace
from unittest.mock import patch

os.environ.setdefault("SECRET_KEY", "test-secret")
os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://test:test@localhost/test")

from sqlalchemy.dialects import postgresql

from models.base import CollectionSource, MediaType
from routers.media import (
    _media_row_match,
    _media_server_web_url,
    movie_media_match,
    movie_tmdb_key,
)


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


class ConvertedMovieMatchTests(unittest.TestCase):
    """An episode re-filed as a movie stays an episode row, reachable as the movie
    only through the linked_movie_tmdb_id it records."""

    def _compiled(self, clause) -> str:
        return str(clause.compile(
            dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}
        ))

    def test_movie_match_covers_both_real_movies_and_converted_episodes(self) -> None:
        sql = self._compiled(movie_media_match([46687]))
        self.assertIn("media.media_type", sql)
        self.assertIn("linked_movie_tmdb_id", sql)
        # Either branch alone is enough to match a row.
        self.assertIn(" OR ", sql)

    def test_movie_key_reports_the_film_a_converted_row_stands_for(self) -> None:
        sql = self._compiled(movie_tmdb_key())
        self.assertIn("CASE WHEN", sql)
        self.assertIn("linked_movie_tmdb_id", sql)

    def test_episode_lookups_are_left_alone(self) -> None:
        # Only movies follow conversions; an episode id must still match exactly.
        sql = self._compiled(_media_row_match(MediaType.episode, 1234))
        self.assertNotIn("linked_movie_tmdb_id", sql)
        self.assertNotIn(" OR ", sql)


if __name__ == "__main__":
    unittest.main()
