import os
import unittest

os.environ.setdefault("SECRET_KEY", "test-secret")
os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://test:test@localhost/test")

from core.image_overrides import NO_NUMBER, apply_overrides, override_key, public_path


def _overrides(*entries):
    return {override_key(*key): value for key, value in entries}


class PublicPathTests(unittest.TestCase):
    """Every emitted value must work as a raw <img src>, not only via tmdbImageUrl.

    Client-rendered lists (the navbar search dropdown, the calendar, the
    connections tables) drop the field straight into the tag.
    """

    def test_tmdb_path_becomes_a_sized_tmdb_url(self):
        self.assertEqual(public_path("tmdb", "/abc.jpg", "poster"), "https://image.tmdb.org/t/p/w500/abc.jpg")

    def test_tmdb_size_follows_the_kind_being_replaced(self):
        self.assertEqual(public_path("tmdb", "/a.jpg", "backdrop"), "https://image.tmdb.org/t/p/w1280/a.jpg")
        self.assertEqual(public_path("tmdb", "/a.jpg", "still"), "https://image.tmdb.org/t/p/w780/a.jpg")

    def test_tvdb_path_becomes_an_artwork_url(self):
        self.assertEqual(
            public_path("tvdb", "/banners/v4/series/1/posters/x.jpg"),
            "https://artworks.thetvdb.com/banners/v4/series/1/posters/x.jpg",
        )

    def test_external_path_becomes_this_servers_own_image_route(self):
        self.assertEqual(
            public_path("external", "/deadbeef.png"),
            "/api/proxy/media/image/ext/deadbeef.png",
        )


class ApplyOverridesTests(unittest.TestCase):
    def test_show_poster_replaced_on_a_grid_card(self):
        overrides = _overrides((("tmdb:s:1396", NO_NUMBER, NO_NUMBER, "poster"), "/new.jpg"))
        payload = {"results": [{"type": "series", "tmdb_id": 1396, "poster_path": "/old.jpg"}]}
        apply_overrides(payload, overrides)
        self.assertEqual(payload["results"][0]["poster_path"], "/new.jpg")

    def test_override_set_on_tmdb_id_also_applies_to_the_tvdb_addressed_card(self):
        # load_overrides expands a show's row across both provider ids.
        overrides = _overrides(
            (("tmdb:s:1396", NO_NUMBER, NO_NUMBER, "poster"), "/new.jpg"),
            (("tvdb:s:81189", NO_NUMBER, NO_NUMBER, "poster"), "/new.jpg"),
        )
        payload = {"type": "series", "tvdb_id": 81189, "poster_path": "/old.jpg"}
        apply_overrides(payload, overrides)
        self.assertEqual(payload["poster_path"], "/new.jpg")

    def test_season_poster_inherits_the_show_from_its_parent(self):
        overrides = _overrides((("tmdb:s:1396", 2, NO_NUMBER, "poster"), "/s2.jpg"))
        payload = {
            "type": "series",
            "tmdb_id": 1396,
            "poster_path": "/show.jpg",
            "seasons_meta": [
                {"season_number": 1, "poster_path": "/s1.jpg"},
                {"season_number": 2, "poster_path": "/old-s2.jpg"},
            ],
        }
        apply_overrides(payload, overrides)
        self.assertEqual(payload["poster_path"], "/show.jpg")
        self.assertEqual(payload["seasons_meta"][0]["poster_path"], "/s1.jpg")
        self.assertEqual(payload["seasons_meta"][1]["poster_path"], "/s2.jpg")

    def test_episode_still_replaces_both_fields_that_carry_it(self):
        overrides = _overrides((("tmdb:s:1396", 1, 3, "still"), "/still.jpg"))
        payload = {
            "type": "episode",
            "show_tmdb_id": 1396,
            "season_number": 1,
            "episode_number": 3,
            "still_path": "/old-still.jpg",
            "poster_path": "/old-still.jpg",
        }
        apply_overrides(payload, overrides)
        self.assertEqual(payload["still_path"], "/still.jpg")
        self.assertEqual(payload["poster_path"], "/still.jpg")

    def test_episode_card_show_poster_uses_the_show_override(self):
        overrides = _overrides((("tmdb:s:1396", NO_NUMBER, NO_NUMBER, "poster"), "/new.jpg"))
        payload = {
            "type": "episode",
            "show_uri_id": "tmdb:s:1396",
            "season_number": 1,
            "episode_number": 3,
            "show_poster_path": "/old.jpg",
            "poster_path": "/still.jpg",
        }
        apply_overrides(payload, overrides)
        self.assertEqual(payload["show_poster_path"], "/new.jpg")
        # The episode's own still is untouched by a show-poster override.
        self.assertEqual(payload["poster_path"], "/still.jpg")

    def test_nested_episodes_inherit_the_show_through_the_seasons_map(self):
        overrides = _overrides((("tmdb:s:1396", 1, 2, "still"), "/still.jpg"))
        payload = {
            "type": "series",
            "tmdb_id": 1396,
            "seasons": {"season_1": [
                {"season_number": 1, "episode_number": 1, "still_path": "/e1.jpg"},
                {"season_number": 1, "episode_number": 2, "still_path": "/e2.jpg"},
            ]},
        }
        apply_overrides(payload, overrides)
        self.assertEqual(payload["seasons"]["season_1"][0]["still_path"], "/e1.jpg")
        self.assertEqual(payload["seasons"]["season_1"][1]["still_path"], "/still.jpg")

    def test_movie_poster_and_backdrop_are_independent(self):
        overrides = _overrides((("tmdb:m:550", NO_NUMBER, NO_NUMBER, "backdrop"), "/bd.jpg"))
        payload = {"type": "movie", "uri_id": "tmdb:m:550", "poster_path": "/p.jpg", "backdrop_path": "/old.jpg"}
        apply_overrides(payload, overrides)
        self.assertEqual(payload["poster_path"], "/p.jpg")
        self.assertEqual(payload["backdrop_path"], "/bd.jpg")

    def test_a_shows_override_never_leaks_onto_a_movie_of_the_same_id(self):
        overrides = _overrides((("tmdb:s:550", NO_NUMBER, NO_NUMBER, "poster"), "/show.jpg"))
        payload = {"type": "movie", "tmdb_id": 550, "poster_path": "/movie.jpg"}
        apply_overrides(payload, overrides)
        self.assertEqual(payload["poster_path"], "/movie.jpg")

    def test_a_recommendation_row_keeps_its_own_identity(self):
        overrides = _overrides((("tmdb:s:1396", NO_NUMBER, NO_NUMBER, "poster"), "/new.jpg"))
        payload = {
            "type": "series",
            "tmdb_id": 1396,
            "poster_path": "/old.jpg",
            "recommendations": [{"type": "series", "tmdb_id": 999, "poster_path": "/other.jpg"}],
        }
        apply_overrides(payload, overrides)
        self.assertEqual(payload["poster_path"], "/new.jpg")
        self.assertEqual(payload["recommendations"][0]["poster_path"], "/other.jpg")

    def test_empty_override_map_leaves_the_payload_alone(self):
        payload = {"type": "series", "tmdb_id": 1396, "poster_path": "/old.jpg"}
        apply_overrides(payload, {})
        self.assertEqual(payload["poster_path"], "/old.jpg")




class PrefixedArtworkTests(unittest.TestCase):
    """Artwork of a related subject, carried under a prefix beside its own ids."""

    def test_remap_row_source_and_target_posters_are_keyed_separately(self):
        overrides = _overrides(
            (("tmdb:s:100", NO_NUMBER, NO_NUMBER, "poster"), "/source.jpg"),
            (("tmdb:s:200", NO_NUMBER, NO_NUMBER, "poster"), "/target.jpg"),
        )
        payload = {
            "source_show_uri_id": "tmdb:s:100",
            "source_show_tmdb_id": 100,
            "source_show_poster_path": "/old-source.jpg",
            "target_show_uri_id": "tmdb:s:200",
            "target_show_tmdb_id": 200,
            "target_show_poster_path": "/old-target.jpg",
        }
        apply_overrides(payload, overrides)
        self.assertEqual(payload["source_show_poster_path"], "/source.jpg")
        self.assertEqual(payload["target_show_poster_path"], "/target.jpg")

    def test_conversion_row_movie_poster_uses_the_movie_override(self):
        overrides = _overrides((("tmdb:m:550", NO_NUMBER, NO_NUMBER, "poster"), "/new.jpg"))
        payload = {"movie_tmdb_id": 550, "movie_poster_path": "/old.jpg"}
        apply_overrides(payload, overrides)
        self.assertEqual(payload["movie_poster_path"], "/new.jpg")

    def test_a_movie_override_does_not_touch_a_show_of_the_same_id(self):
        overrides = _overrides((("tmdb:m:550", NO_NUMBER, NO_NUMBER, "poster"), "/movie.jpg"))
        payload = {"source_show_tmdb_id": 550, "source_show_poster_path": "/show.jpg"}
        apply_overrides(payload, overrides)
        self.assertEqual(payload["source_show_poster_path"], "/show.jpg")


if __name__ == "__main__":
    unittest.main()
