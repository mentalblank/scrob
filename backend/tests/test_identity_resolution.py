"""Identity resolution: uris, provider ids and duplicate-tolerant lookups."""
import unittest

from routers.sync import _canonical_duplicate
from utils.media_uri import MediaURI


class _Row:
    def __init__(self, id: int, season_number: int, episode_number: int):
        self.id = id
        self.season_number = season_number
        self.episode_number = episode_number


class MediaURIParsing(unittest.TestCase):
    def test_parses_each_provider(self):
        for raw, provider, prefix, ident in [
            ("tmdb:s:1396", "tmdb", "s", "1396"),
            ("tvdb:s:81189", "tvdb", "s", "81189"),
            ("tmdb:m:27205", "tmdb", "m", "27205"),
            ("imdb:m:903747", "imdb", "m", "903747"),
        ]:
            uri = MediaURI.parse(raw)
            self.assertEqual(uri.provider, provider)
            self.assertEqual(uri.type_prefix, prefix)
            self.assertEqual(uri.id, ident)

    def test_rejects_malformed(self):
        for raw in ["tmdb:1396", "", "tmdb::1396", "nonsense"]:
            with self.assertRaises(ValueError):
                MediaURI.parse(raw)


class CanonicalDuplicateChoice(unittest.TestCase):
    """The row matching TMDB's own numbering survives a merge."""

    SEASONS = [{"season_number": 1, "episode_count": 25}]

    def test_prefers_row_inside_metadata(self):
        tmdb_numbered = _Row(id=99, season_number=1, episode_number=14)
        tvdb_numbered = _Row(id=10, season_number=2, episode_number=1)
        keeper = _canonical_duplicate([tvdb_numbered, tmdb_numbered], self.SEASONS)
        self.assertEqual(keeper.id, tmdb_numbered.id)

    def test_episode_beyond_season_length_is_not_canonical(self):
        beyond = _Row(id=5, season_number=1, episode_number=40)
        inside = _Row(id=6, season_number=1, episode_number=3)
        keeper = _canonical_duplicate([beyond, inside], self.SEASONS)
        self.assertEqual(keeper.id, inside.id)

    def test_falls_back_to_earliest_season_then_oldest_row(self):
        later = _Row(id=2, season_number=9, episode_number=1)
        earlier = _Row(id=7, season_number=8, episode_number=1)
        keeper = _canonical_duplicate([later, earlier], [])
        self.assertEqual(keeper.id, earlier.id)


if __name__ == "__main__":
    unittest.main()
