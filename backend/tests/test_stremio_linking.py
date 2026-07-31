import os
import unittest
from unittest import IsolatedAsyncioTestCase

os.environ.setdefault("SECRET_KEY", "test-secret")
os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://test:test@localhost/test")

from types import SimpleNamespace

from models.base import MediaType
from models.events import WatchEvent
from models.show import Show
from routers.stremio import (
    _describe_media,
    _merge_stremio_prefs,
    _imdb_numeric,
    _log_watch_event,
    _parse_link_target,
    _start_playback_session,
    _unmatched_html,
    media_to_stremio_meta,
)


class ImdbNumericTests(unittest.TestCase):
    """MediaURI ids are digits only, so the tt prefix has to come off cleanly."""

    def test_strips_prefix_and_normalises_case(self):
        self.assertEqual(_imdb_numeric("tt7587890"), "7587890")
        self.assertEqual(_imdb_numeric("TT7587890"), "7587890")
        self.assertEqual(_imdb_numeric(" tt7587890 "), "7587890")


class ParseLinkTargetTests(unittest.TestCase):
    """What the user types into the link page, normalised to (kind, value)."""

    def test_bare_imdb_id(self):
        self.assertEqual(_parse_link_target("tt7587890"), ("imdb", "tt7587890"))
        self.assertEqual(_parse_link_target(" TT7587890 "), ("imdb", "tt7587890"))

    def test_prefixed_provider_ids(self):
        self.assertEqual(_parse_link_target("imdb:TT7587890"), ("imdb", "tt7587890"))
        self.assertEqual(_parse_link_target("tmdb:79744"), ("tmdb", "79744"))
        self.assertEqual(_parse_link_target("tvdb:350665"), ("tvdb", "350665"))

    def test_bare_number_is_read_as_tmdb(self):
        self.assertEqual(_parse_link_target("79744"), ("tmdb", "79744"))

    def test_library_search_result_is_passed_through(self):
        self.assertEqual(_parse_link_target("scrob:show:12"), ("scrob", "scrob:show:12"))
        self.assertEqual(_parse_link_target("scrob:media:34"), ("scrob", "scrob:media:34"))


class StremioMetaTypeTests(unittest.TestCase):
    """A catalog entry's type must come from the item, not from the catalog it was
    requested under, or movies show up in series lists and refuse to play."""

    def test_movie_stays_a_movie(self):
        movie = SimpleNamespace(
            id=1, media_type=MediaType.movie, tmdb_id=550, uri_id="tmdb:m:550",
            title="Fight Club", overview=None, poster_path=None, release_date="1999-10-15",
            tmdb_rating=8.4, tmdb_data=None,
        )
        self.assertEqual(media_to_stremio_meta(movie)["type"], "movie")

    def test_show_is_a_series(self):
        show = Show(tmdb_id=79744, title="The Rookie", first_air_date="2018-10-16")
        show.id = 2
        self.assertEqual(media_to_stremio_meta(show)["type"], "series")

    def test_episode_counts_as_a_series(self):
        episode = SimpleNamespace(
            id=3, media_type=MediaType.episode, tmdb_id=12345, uri_id="tmdb:e:12345",
            title="Life and Death", overview=None, poster_path=None, release_date="2024-02-20",
            tmdb_rating=None, tmdb_data=None,
        )
        self.assertEqual(media_to_stremio_meta(episode)["type"], "series")


class UnmatchedPageTests(unittest.TestCase):
    def test_page_reports_404_and_offers_linking(self):
        res = _unmatched_html("key123", "tt7587890:6:6", "series", "mark-watched")
        body = res.body.decode()
        self.assertEqual(res.status_code, 404)
        self.assertIn("tt7587890:6:6", body)
        self.assertIn('const kind = "series";', body)
        self.assertIn('const action = "mark-watched";', body)

    def test_api_key_and_id_are_json_encoded_not_interpolated_raw(self):
        # Both land inside a <script> block, so they must not be able to break out.
        res = _unmatched_html('a"b', '</script>', "movie", "mark-watched")
        body = res.body.decode()
        self.assertIn(r'const apiKey = "a\"b";', body)
        self.assertIn(r'const itemId = "\u003c/script\u003e";', body)
        self.assertNotIn("</script>\";", body)
        self.assertNotIn("<p><code></script></code>", body)


class _ScalarResult:
    def __init__(self, value):
        self._value = value

    def scalar_one_or_none(self):
        return self._value


class _FakeDB:
    """Fakes just enough of AsyncSession for the watch/session helpers: every
    execute() returns the next queued scalar_one_or_none() value and records the
    statement, and add() is captured so tests can see what would be written."""

    def __init__(self, queued_scalars=()):
        self._queued = list(queued_scalars)
        self.added = []
        self.executed = []

    async def execute(self, stmt):
        self.executed.append(stmt)
        value = self._queued.pop(0) if self._queued else None
        return _ScalarResult(value)

    def add(self, obj):
        self.added.append(obj)


class LogWatchEventDedupTests(IsolatedAsyncioTestCase):
    """Stremio can fire the same externalUrl twice for one click, which used to
    put two rows in history for a single tap."""

    async def test_first_call_records_the_event(self):
        db = _FakeDB(queued_scalars=[None])  # no recent WatchEvent found
        self.assertTrue(await _log_watch_event(db, user_id=1, media_id=2))
        self.assertEqual(len(db.added), 1)
        self.assertTrue(db.added[0].completed)

    async def test_repeat_within_the_window_is_ignored(self):
        db = _FakeDB(queued_scalars=[123])  # a recent WatchEvent id is found
        self.assertFalse(await _log_watch_event(db, user_id=1, media_id=2))
        self.assertEqual(db.added, [])


class StartPlaybackSessionTests(IsolatedAsyncioTestCase):
    async def test_opens_a_manual_session_and_writes_no_history(self):
        db = _FakeDB()
        media = SimpleNamespace(id=7, runtime=42)
        await _start_playback_session(db, user_id=3, media=media)

        self.assertEqual(len(db.added), 1)
        session = db.added[0]
        self.assertEqual(session.session_key, "manual-3-7")
        # "manual" is what auto_complete_manual_sessions sweeps, so the session
        # completes itself once the runtime has elapsed.
        self.assertEqual(session.source, "manual")
        self.assertEqual(session.state, "playing")
        self.assertNotIsInstance(session, WatchEvent)


class DescribeMediaTests(IsolatedAsyncioTestCase):
    async def test_episode_shows_show_title_and_episode_code(self):
        show = Show(id=1, title="Rick and Morty")
        episode = SimpleNamespace(
            id=9, media_type=MediaType.episode, show_id=1, custom_title=None,
            title="Solaricks", season_number=6, episode_number=6,
            release_date="2022-09-04", tmdb_data=None, poster_path=None,
        )
        db = _FakeDB(queued_scalars=[show])
        details = await _describe_media(db, episode)

        self.assertEqual(details["kind"], "Episode")
        self.assertEqual(details["heading"], "Rick and Morty")
        self.assertEqual(details["subtitle"], "S06E06 · Solaricks")

    async def test_movie_shows_title_and_year(self):
        movie = SimpleNamespace(
            id=4, media_type=MediaType.movie, show_id=None, custom_title=None,
            title="Dune", season_number=None, episode_number=None,
            release_date="2021-10-22", tmdb_data=None, poster_path=None,
        )
        details = await _describe_media(_FakeDB(), movie)

        self.assertEqual(details["kind"], "Movie")
        self.assertEqual(details["heading"], "Dune")
        self.assertEqual(details["subtitle"], "2021")

    async def test_title_is_escaped_for_the_confirmation_page(self):
        movie = SimpleNamespace(
            id=5, media_type=MediaType.movie, show_id=None, custom_title=None,
            title="<script>x</script>", season_number=None, episode_number=None,
            release_date=None, tmdb_data=None, poster_path=None,
        )
        details = await _describe_media(_FakeDB(), movie)
        self.assertEqual(details["heading"], "&lt;script&gt;x&lt;/script&gt;")


class StremioPrefsTests(unittest.TestCase):
    """Addon configuration, merged over the defaults so a newly added option
    appears for users who saved a config before it existed."""

    def test_missing_config_is_all_defaults(self):
        prefs = _merge_stremio_prefs(None)
        self.assertTrue(all(prefs["catalogs"].values()))
        self.assertTrue(all(prefs["types"].values()))
        self.assertTrue(all(prefs["actions"].values()))
        self.assertIsNone(prefs["enabled_list_ids"])
        self.assertEqual(prefs["catalog_limit"], 50)

    def test_stored_values_win_and_gaps_fall_back(self):
        prefs = _merge_stremio_prefs({"catalogs": {"history": False}, "types": {"movie": False}})
        self.assertFalse(prefs["catalogs"]["history"])
        self.assertTrue(prefs["catalogs"]["watchlist"])
        self.assertFalse(prefs["types"]["movie"])
        self.assertTrue(prefs["types"]["series"])

    def test_list_selection_round_trips(self):
        self.assertEqual(_merge_stremio_prefs({"enabled_list_ids": [3, 7]})["enabled_list_ids"], [3, 7])
        # Every list unticked is a real choice, not "unset".
        self.assertEqual(_merge_stremio_prefs({"enabled_list_ids": []})["enabled_list_ids"], [])

    def test_out_of_range_catalog_limit_is_ignored(self):
        self.assertEqual(_merge_stremio_prefs({"catalog_limit": 0})["catalog_limit"], 50)
        self.assertEqual(_merge_stremio_prefs({"catalog_limit": 500})["catalog_limit"], 50)
        self.assertEqual(_merge_stremio_prefs({"catalog_limit": 25})["catalog_limit"], 25)


if __name__ == "__main__":
    unittest.main()
