import os
import unittest

os.environ.setdefault("SECRET_KEY", "test-secret")
os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://test:test@localhost/test")

from models.episode_order import EpisodeOrderMapping, UserShowEpisodeOrder
from routers.media import (
    _attach_episode_order_fields,
    _effective_episode_order,
    _resolve_add_overrides,
    RequestOverrides,
)


def _pref(series_tmdb_id, episode_order="tvdb"):
    return UserShowEpisodeOrder(user_id=1, series_tmdb_id=series_tmdb_id, episode_order=episode_order)


def _mapping(series_tmdb_id, tmdb_season, tmdb_episode, tvdb_season, tvdb_episode):
    return EpisodeOrderMapping(
        series_tmdb_id=series_tmdb_id,
        tmdb_season_number=tmdb_season,
        tmdb_episode_number=tmdb_episode,
        tmdb_episode_id=1,
        tvdb_id=1,
        tvdb_season_number=tvdb_season,
        tvdb_episode_number=tvdb_episode,
        match_method="external_id",
    )


class AttachEpisodeOrderFieldsTests(unittest.TestCase):
    """Regression tests for #186: episode/season links must route to
    /show/tvdb/... using translated numbers when the user has switched a
    show to TVDB numbering - previously every link builder only checked
    tvdb_sourced (no TMDB counterpart at all), which has nothing to do with
    this preference and could 404 if TMDB renumbered since the show was
    matched."""

    def test_episode_with_tvdb_preference_and_mapping_gets_translated_position(self) -> None:
        item = {"type": "episode", "show_tmdb_id": 100, "season_number": 4, "episode_number": 12}
        episode_orders = {100: _pref(100)}
        tmdb_to_tvdb = {(100, 4, 12): _mapping(100, 4, 12, 3, 8)}

        _attach_episode_order_fields(item, episode_orders, tmdb_to_tvdb)

        self.assertEqual(item["show_episode_order"], "tvdb")
        self.assertEqual(item["tvdb_season_number"], 3)
        self.assertEqual(item["tvdb_episode_number"], 8)

    def test_tvdb_sourced_episode_never_gets_a_translated_position(self) -> None:
        # tvdb_sourced episodes (no TMDB counterpart) already store TVDB-
        # native numbers in season_number/episode_number - a coincidental
        # numeric match against tmdb_to_tvdb (keyed by real TMDB positions,
        # both small integers) must not attach an unrelated episode's
        # translated position.
        item = {
            "type": "episode", "show_tmdb_id": 100,
            "season_number": 4, "episode_number": 12, "tvdb_sourced": True,
        }
        episode_orders = {100: _pref(100)}
        # A real mapping row that coincidentally shares this episode's raw
        # TVDB-as-season/episode numbers as its TMDB position.
        tmdb_to_tvdb = {(100, 4, 12): _mapping(100, 4, 12, 9, 99)}

        _attach_episode_order_fields(item, episode_orders, tmdb_to_tvdb)

        self.assertEqual(item["show_episode_order"], "tvdb")
        self.assertNotIn("tvdb_season_number", item)
        self.assertNotIn("tvdb_episode_number", item)

    def test_episode_with_tvdb_preference_but_no_mapping_only_sets_preference(self) -> None:
        item = {"type": "episode", "show_tmdb_id": 100, "season_number": 4, "episode_number": 12}
        episode_orders = {100: _pref(100)}

        _attach_episode_order_fields(item, episode_orders, {})

        self.assertEqual(item["show_episode_order"], "tvdb")
        self.assertNotIn("tvdb_season_number", item)
        self.assertNotIn("tvdb_episode_number", item)

    def test_episode_with_tmdb_preference_is_a_noop(self) -> None:
        item = {"type": "episode", "show_tmdb_id": 100, "season_number": 4, "episode_number": 12}
        episode_orders = {100: _pref(100, episode_order="tmdb")}
        tmdb_to_tvdb = {(100, 4, 12): _mapping(100, 4, 12, 3, 8)}

        _attach_episode_order_fields(item, episode_orders, tmdb_to_tvdb)

        self.assertNotIn("show_episode_order", item)
        self.assertNotIn("tvdb_season_number", item)

    def test_episode_with_no_preference_row_is_a_noop(self) -> None:
        item = {"type": "episode", "show_tmdb_id": 100, "season_number": 4, "episode_number": 12}
        _attach_episode_order_fields(item, {}, {})
        self.assertNotIn("show_episode_order", item)

    def test_season_list_item_picks_lowest_episode_numbers_tvdb_season(self) -> None:
        item = {"type": "series", "tmdb_id": 100, "season_number": 4}
        episode_orders = {100: _pref(100)}
        tmdb_to_tvdb = {
            (100, 4, 5): _mapping(100, 4, 5, 3, 1),
            (100, 4, 1): _mapping(100, 4, 1, 3, 9),
            (100, 5, 1): _mapping(100, 5, 1, 4, 1),
        }

        _attach_episode_order_fields(item, episode_orders, tmdb_to_tvdb)

        self.assertEqual(item["show_episode_order"], "tvdb")
        # Lowest tmdb_episode_number in season 4 is episode 1 -> tvdb season 3.
        self.assertEqual(item["tvdb_season_number"], 3)
        self.assertNotIn("tvdb_episode_number", item)

    def test_whole_show_item_sets_preference_without_season_fields(self) -> None:
        item = {"type": "series", "tmdb_id": 100}
        episode_orders = {100: _pref(100)}
        tmdb_to_tvdb = {(100, 4, 12): _mapping(100, 4, 12, 3, 8)}

        _attach_episode_order_fields(item, episode_orders, tmdb_to_tvdb)

        self.assertEqual(item["show_episode_order"], "tvdb")
        self.assertNotIn("tvdb_season_number", item)

    def test_movie_item_is_a_noop(self) -> None:
        item = {"type": "movie", "tmdb_id": 550}
        _attach_episode_order_fields(item, {550: _pref(550)}, {})
        self.assertNotIn("show_episode_order", item)


class AccountDefaultEpisodeOrderTests(unittest.TestCase):
    """A per-show row is an override, not the only way to ask for TVDB
    numbering: an account whose primary metadata source is TVDB has asked for
    it everywhere it hasn't been overridden. Without this the card's link
    carried the TVDB show id and TMDB's position - Re:ZERO's TMDB S01E79 is
    TVDB's S04E13, so the link opened a real but unrelated episode."""

    def test_missing_row_follows_the_account_default(self) -> None:
        self.assertEqual(_effective_episode_order(None, "tvdb"), "tvdb")
        self.assertEqual(_effective_episode_order(None, "tmdb"), "tmdb")

    def test_per_show_row_outranks_the_account_default(self) -> None:
        self.assertEqual(
            _effective_episode_order(_pref(100, episode_order="tmdb"), "tvdb"), "tmdb"
        )
        self.assertEqual(
            _effective_episode_order(_pref(100, episode_order="tvdb"), "tmdb"), "tvdb"
        )

    def test_episode_translates_under_the_account_default_alone(self) -> None:
        item = {"type": "episode", "show_tmdb_id": 100, "season_number": 1, "episode_number": 79}
        tmdb_to_tvdb = {(100, 1, 79): _mapping(100, 1, 79, 4, 13)}

        _attach_episode_order_fields(item, {}, tmdb_to_tvdb, "tvdb")

        self.assertEqual(item["show_episode_order"], "tvdb")
        self.assertEqual(item["tvdb_season_number"], 4)
        self.assertEqual(item["tvdb_episode_number"], 13)

    def test_show_pinned_to_tmdb_stays_tmdb_under_a_tvdb_account(self) -> None:
        item = {"type": "episode", "show_tmdb_id": 100, "season_number": 1, "episode_number": 79}
        tmdb_to_tvdb = {(100, 1, 79): _mapping(100, 1, 79, 4, 13)}

        _attach_episode_order_fields(
            item, {100: _pref(100, episode_order="tmdb")}, tmdb_to_tvdb, "tvdb"
        )

        self.assertNotIn("show_episode_order", item)
        self.assertNotIn("tvdb_season_number", item)


class ResolveAddOverridesTests(unittest.TestCase):
    """The customize-on-add popup lets an admin override the root folder/
    quality profile/tags/season-folder for a single Radarr/Sonarr add - this
    must only ever apply for an admin, regardless of the *_customize_on_add
    settings (those only control whether the frontend shows the picker)."""

    def test_admin_overrides_are_honored(self) -> None:
        overrides = RequestOverrides(root_folder="/movies-4k")
        self.assertIs(_resolve_add_overrides(overrides, True), overrides)

    def test_non_admin_overrides_are_ignored(self) -> None:
        overrides = RequestOverrides(root_folder="/movies-4k")
        self.assertIsNone(_resolve_add_overrides(overrides, False))

    def test_no_overrides_sent_is_a_noop_for_an_admin(self) -> None:
        self.assertIsNone(_resolve_add_overrides(None, True))

    def test_no_overrides_sent_is_a_noop_for_a_non_admin(self) -> None:
        self.assertIsNone(_resolve_add_overrides(None, False))


if __name__ == "__main__":
    unittest.main()
