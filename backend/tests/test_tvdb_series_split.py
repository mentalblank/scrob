"""TVDB keeps a continuation or an anthology as further seasons of one series;
TMDB gives each its own show. The two catalogues only link at episode level, so
a TVDB page has to gather state from several local shows."""
import inspect
import os
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

os.environ.setdefault("SECRET_KEY", "test-secret")
os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://test:test@localhost/test")

from core.episode_order import resolve_tvdb_series_id, tmdb_shows_for_tvdb_series
from models.base import MediaType
from models.media import Media
from models.show import Show as ShowModel
from routers.shows import get_tvdb_season

from tests.test_episode_order import _EmptyResult, _ExistingResult, _ScalarOneResult

# TVDB 405535 season 3 is TMDB show 323411 season 1, and TMDB gives that show no
# series-level TVDB id at all.
TVDB_SERIES = 500
PARENT_TMDB = 999
MEMBER_TMDB = 888


class ResolveTvdbSeriesIdTests(unittest.IsolatedAsyncioTestCase):
    async def test_series_level_id_is_used_when_tmdb_has_one(self) -> None:
        series_id = await resolve_tvdb_series_id(
            PARENT_TMDB, "tmdb-key", "tvdb-key",
            show_data={"external_ids": {"tvdb_id": TVDB_SERIES}},
        )
        self.assertEqual(series_id, TVDB_SERIES)

    async def test_split_off_show_is_resolved_through_one_of_its_episodes(self) -> None:
        """The show has no TVDB id of its own, but its episodes do, and a TVDB
        episode names the series it belongs to."""
        with (
            patch("core.episode_order.tmdb.get_episode_external_ids",
                  AsyncMock(return_value={"tvdb_id": 11414890})),
            patch("core.episode_order.tvdb.get_episode",
                  AsyncMock(return_value={"id": 11414890, "seriesId": TVDB_SERIES})) as get_episode,
        ):
            series_id = await resolve_tvdb_series_id(
                MEMBER_TMDB, "tmdb-key", "tvdb-key",
                show_data={"external_ids": {}, "seasons": [{"season_number": 1}]},
            )

        self.assertEqual(series_id, TVDB_SERIES)
        get_episode.assert_awaited_once_with(11414890, "tvdb-key")

    async def test_show_with_no_episode_link_resolves_to_nothing(self) -> None:
        with patch("core.episode_order.tmdb.get_episode_external_ids",
                   AsyncMock(return_value={"tvdb_id": None})):
            series_id = await resolve_tvdb_series_id(
                MEMBER_TMDB, "tmdb-key", "tvdb-key",
                show_data={"external_ids": {}, "seasons": [{"season_number": 1}]},
            )
        self.assertIsNone(series_id)


class TmdbShowsForTvdbSeriesTests(unittest.IsolatedAsyncioTestCase):
    """Neither catalogue links seasons to each other. A TVDB episode carries an
    IMDb id, and TMDB's /find reports which show that episode belongs to."""

    async def test_each_season_resolves_to_its_own_tmdb_show(self) -> None:
        tvdb_series = {
            "episodes": [
                {"id": 7974291, "seasonNumber": 1, "number": 1},
                {"id": 7974292, "seasonNumber": 1, "number": 2},
                {"id": 10649169, "seasonNumber": 2, "number": 1},
                {"id": 900, "seasonNumber": 0, "number": 1},
            ],
        }
        episodes = {
            7974291: {"remoteIds": [{"sourceName": "IMDB", "id": "tt13299456"}]},
            10649169: {"remoteIds": [{"sourceName": "IMDB", "id": "tt27655087"}]},
        }
        found = {
            "tt13299456": {"tv_episode_results": [{"show_id": 113988}]},
            "tt27655087": {"tv_episode_results": [{"show_id": 225634}]},
        }

        with (
            patch("core.episode_order.tvdb.get_episode",
                  AsyncMock(side_effect=lambda eid, key: episodes[eid])),
            patch("core.episode_order.tmdb.find_by_external_id",
                  AsyncMock(side_effect=lambda imdb, source, api_key=None: found[imdb])),
        ):
            owners = await tmdb_shows_for_tvdb_series(
                389492, "tvdb-key", "tmdb-key", tvdb_series=tvdb_series
            )

        # Specials are skipped: season 0 is not a show of its own.
        self.assertEqual(owners, {1: 113988, 2: 225634})

    async def test_a_season_tmdb_does_not_carry_is_left_out(self) -> None:
        tvdb_series = {"episodes": [{"id": 5301, "seasonNumber": 4, "number": 1}]}
        with (
            patch("core.episode_order.tvdb.get_episode",
                  AsyncMock(return_value={"remoteIds": []})),
            patch("core.episode_order.tmdb.find_by_external_id", AsyncMock()) as find,
        ):
            owners = await tmdb_shows_for_tvdb_series(
                389492, "tvdb-key", "tmdb-key", tvdb_series=tvdb_series
            )

        self.assertEqual(owners, {})
        find.assert_not_awaited()


class _stack:
    """Enter a tuple of patches as one context manager."""

    def __init__(self, patches):
        self.patches = patches

    def __enter__(self):
        for p in self.patches:
            p.start()
        return self

    def __exit__(self, *exc):
        for p in self.patches:
            p.stop()
        return False


def _member_season_db(episode_rows, *, add=None):
    """A season page for a TVDB season owned by a different TMDB show."""
    parent = ShowModel(id=42, tvdb_id=TVDB_SERIES, tmdb_id=PARENT_TMDB)
    member = ShowModel(id=77, tmdb_id=MEMBER_TMDB)
    mapping = SimpleNamespace(
        series_tmdb_id=MEMBER_TMDB,
        tvdb_series_id=TVDB_SERIES,
        tvdb_id=5301,
        tvdb_season_number=3,
        tvdb_episode_number=1,
        tmdb_season_number=1,
        tmdb_episode_number=1,
        tmdb_episode_id=9101,
    )
    watched = [(row.id,) for row in episode_rows]
    return SimpleNamespace(
        execute=AsyncMock(
            side_effect=[
                _ScalarOneResult(parent),               # show_result
                _EmptyResult(),                         # parent show's own mappings — none
                _ExistingResult([mapping]),             # the TVDB series' mappings
                _ExistingResult([parent, member]),      # member shows
                _ExistingResult(episode_rows),          # ep_result across both shows
                _EmptyResult(),                         # season overrides
                _EmptyResult(),                         # episode overrides
                _ExistingResult(watched),               # watched_q
                _ExistingResult(watched),               # collected_q
                _ExistingResult([]),                    # episode_ratings_q
                _ScalarOneResult(None),                 # show_media_result
            ]
            if episode_rows
            else [
                _ScalarOneResult(parent),
                _EmptyResult(),
                _ExistingResult([mapping]),
                _ExistingResult([parent, member]),
                _EmptyResult(),
                _EmptyResult(),
                _EmptyResult(),
                _ScalarOneResult(None),
            ]
        ),
        add=add or MagicMock(),
        flush=AsyncMock(),
        commit=AsyncMock(),
    )


_RAW_SERIES = {
    "id": TVDB_SERIES,
    "name": "Test Show",
    "remoteIds": [{"sourceName": "TheMovieDB", "id": str(PARENT_TMDB)}],
    "seasons": [{"number": 3, "type": {"type": "official"}, "id": 113}],
}
_RAW_EPISODES = [
    {"id": 5301, "seasonNumber": 3, "number": 1, "name": "Detroit", "aired": "2026-06-07"},
]


def _patches():
    return (
        patch("routers.shows.get_user_tvdb_key", AsyncMock(return_value="tvdb-key")),
        patch("routers.shows.get_user_metadata_language", AsyncMock(return_value=None)),
        patch("routers.shows.get_user_tmdb_key", AsyncMock(return_value=None)),
        patch("routers.shows.tvdb_client.get_series", AsyncMock(return_value=_RAW_SERIES)),
        patch("routers.shows.tvdb_client.get_series_episodes", AsyncMock(return_value=_RAW_EPISODES)),
        patch("routers.shows.tvdb_client.get_season", AsyncMock(return_value={})),
    )


class TvdbSeasonOwnedByAnotherShowTests(unittest.IsolatedAsyncioTestCase):
    async def test_state_comes_from_the_show_that_owns_the_season(self) -> None:
        """The episode is stored under the split-off TMDB show, not under the
        show the TVDB series id resolves to."""
        episode = Media(
            id=301, show_id=77, media_type=MediaType.episode,
            season_number=1, episode_number=1, tmdb_id=9101,
        )
        db = _member_season_db([episode])

        with _stack(_patches()):
            result = await get_tvdb_season(
                tvdb_id=TVDB_SERIES, season_number=3, db=db,
                current_user=SimpleNamespace(id=7),
            )

        row = result["episodes"][0]
        self.assertEqual(row["id"], 301)
        self.assertTrue(row["watched"])
        self.assertTrue(row["in_library"])
        # The card has to link to the show that actually holds the episode.
        self.assertEqual(row["show_tmdb_id"], MEMBER_TMDB)
        self.assertFalse(row["unmatched"])

    async def test_a_missing_episode_is_reported_not_invented(self) -> None:
        """Creating a row under the parent show would invent an episode TMDB
        never had at that position."""
        add = MagicMock()
        db = _member_season_db([], add=add)

        with _stack(_patches()):
            result = await get_tvdb_season(
                tvdb_id=TVDB_SERIES, season_number=3, db=db,
                current_user=SimpleNamespace(id=7),
            )

        self.assertTrue(result["episodes"][0]["unmatched"])
        self.assertIsNone(result["episodes"][0]["id"])
        add.assert_not_called()


def test_sync_matches_an_episode_filed_under_a_different_show():
    """The pre-load that catches an episode already stored elsewhere must not be
    limited to rows with no show at all — the split-off show has one, and
    restricting to orphans recreated every episode of it on each sync."""
    from routers import sync as sync_router

    source = inspect.getsource(sync_router.sync_items)
    start = source.index("ep_tmdb_ids: set[int] = set()")
    block = source[start:start + 1200]
    assert "Media.tmdb_id.in_(chunk)" in block
    assert "Media.show_id.is_(None)" not in block


def test_same_position_duplicates_merge_without_an_order_mapping():
    """The mapping only protects TVDB browsing when the merge moves an episode.
    Two rows at the same position take nothing with them, and requiring one
    stranded every duplicate on shows neither catalogue can reconcile."""
    from routers.sync import _group_needs_mapping

    same_position = {
        "keep": {"season": 1, "episode": 1},
        "merge": [{"season": 1, "episode": 1}],
    }
    renumbered = {
        "keep": {"season": 1, "episode": 9},
        "merge": [{"season": 2, "episode": 1}],
    }
    assert _group_needs_mapping(same_position) is False
    assert _group_needs_mapping(renumbered) is True


def test_mapping_build_does_not_roll_back_the_request_session():
    """A rollback expires every object loaded from the session, including the
    authenticated user — whose next attribute access then attempts lazy IO from
    outside the greenlet and 500s the page."""
    from routers import shows

    source = inspect.getsource(shows._mappings_for)
    assert "build_session" in source
    assert "ensure_episode_order_mapping(\n                build_db" in source
    assert "db.rollback()" not in source.replace("build_db.rollback()", "")


class KnownSeriesIsTrustedTests(unittest.IsolatedAsyncioTestCase):
    """A caller arriving from the TVDB side already knows the series. Deriving it
    again from TMDB's episode ids can only fail for the shows that have none —
    which are exactly the ones that need title-and-date matching."""

    async def test_supplied_series_id_skips_resolution(self) -> None:
        from core import episode_order

        with (
            patch.object(episode_order.tmdb, "get_show",
                         AsyncMock(return_value={"external_ids": {}, "seasons": []})),
            patch.object(episode_order, "resolve_tvdb_series_id", AsyncMock()) as resolve,
            patch.object(episode_order.tvdb, "get_series", AsyncMock(return_value={})),
            patch.object(episode_order.tvdb, "get_series_episodes", AsyncMock(return_value=[])),
        ):
            db = AsyncMock()
            db.execute.return_value = _EmptyResult()
            with self.assertRaises(ValueError):
                await episode_order.ensure_episode_order_mapping(
                    db, 330285, "tmdb-key", "tvdb-key", tvdb_series_id=79076
                )

        resolve.assert_not_awaited()
