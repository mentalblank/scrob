import os
import unittest
from types import SimpleNamespace

os.environ.setdefault("SECRET_KEY", "test-secret")
os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://test:test@localhost/test")

from models.base import MediaType
from models.media import Media, stamp_media_uri
from routers.sync import media_server_enrichment_enabled
from routers.webhooks import parse_jellyfin_payload, parse_plex_payload


def _settings(prefs):
    return SimpleNamespace(preferences=prefs)


def test_media_server_enrichment_defaults_to_on():
    assert media_server_enrichment_enabled(None) is True
    assert media_server_enrichment_enabled(_settings(None)) is True
    assert media_server_enrichment_enabled(_settings({})) is True
    assert media_server_enrichment_enabled(_settings({"media_server_enrichment": True})) is True


def test_media_server_enrichment_is_off_only_when_explicitly_false():
    assert media_server_enrichment_enabled(_settings({"media_server_enrichment": False})) is False


def test_uri_falls_back_through_tvdb_then_imdb():
    tvdb_only = Media(media_type=MediaType.episode, title="t")
    stamp_media_uri(tvdb_only, tvdb_id="349232", imdb_id="tt0959621")
    assert tvdb_only.uri_id == "tvdb:e:349232"

    imdb_only = Media(media_type=MediaType.movie, title="t")
    stamp_media_uri(imdb_only, imdb_id="tt0137523")
    assert imdb_only.uri_id == "imdb:m:0137523"

    tmdb_wins = Media(tmdb_id=550, media_type=MediaType.movie, title="t")
    stamp_media_uri(tmdb_wins, tvdb_id="1", imdb_id="tt2")
    assert tmdb_wins.uri_id == "tmdb:m:550"

    unidentified = Media(media_type=MediaType.movie, title="t")
    stamp_media_uri(unidentified)
    assert unidentified.uri_id is None


def test_jellyfin_payload_keeps_tvdb_and_imdb_ids():
    nested = parse_jellyfin_payload({
        "NotificationType": "PlaybackStop",
        "Item": {
            "Id": "1", "Name": "Pilot", "Type": "Episode",
            "ParentIndexNumber": 1, "IndexNumber": 1,
            "ProviderIds": {"Tmdb": "62085", "Tvdb": "349232", "Imdb": "tt0959621"},
            "SeriesProviderIds": {"Tmdb": "1396", "Tvdb": "81189", "Imdb": "tt0903747"},
        },
        "Session": {"Id": "s"},
    })
    assert nested["tvdb_id"] == "349232"
    assert nested["imdb_id"] == "tt0959621"
    assert nested["series_tvdb_id"] == "81189"
    assert nested["series_imdb_id"] == "tt0903747"

    flat = parse_jellyfin_payload({
        "NotificationType": "PlaybackStop",
        "ItemType": "Movie",
        "Name": "Test Movie",
        "Provider_tmdb": "550",
        "Provider_tvdb": "12345",
        "Provider_imdb": "tt0137523",
    })
    assert flat["tvdb_id"] == "12345"
    assert flat["imdb_id"] == "tt0137523"


def test_plex_payload_keeps_item_and_series_ids():
    data = parse_plex_payload({
        "event": "media.scrobble",
        "Metadata": {
            "type": "episode", "title": "Pilot", "parentIndex": 1, "index": 1,
            "Guid": [{"id": "tmdb://62085"}, {"id": "tvdb://349232"}, {"id": "imdb://tt0959621"}],
            "grandparentGuid": "tvdb://81189",
        },
    })
    assert (data["tmdb_id"], data["tvdb_id"], data["imdb_id"]) == ("62085", "349232", "tt0959621")
    assert data["grandparent_tvdb_id"] == "81189"


def test_jellyfin_requests_the_file_path_field():
    """CollectionFile.file_path comes from Item.Path, which Jellyfin only returns
    when it is asked for by name."""
    import inspect

    from core import jellyfin

    for fn in (jellyfin.get_movies, jellyfin.get_episodes, jellyfin.get_items_by_ids):
        source = inspect.getsource(fn)
        assert "Path" in source, fn.__name__


class TvdbArtworkStorageTests(unittest.IsolatedAsyncioTestCase):
    """TVDB artwork sits beside TMDB's; a media row is shared by every user, so
    one viewer's provider preference must not overwrite another's images."""

    async def test_series_keeps_tmdb_artwork_and_records_tvdb_alongside(self) -> None:
        from unittest.mock import AsyncMock, patch

        from core import enrichment
        from models.media import Media

        media = Media(tmdb_id=37854, media_type=MediaType.series, title="t")

        async def fake_get_show(tmdb_id, api_key=None, **kwargs):
            return {
                "name": "Test Show", "poster_path": "/tmdb-poster.jpg",
                "backdrop_path": "/tmdb-backdrop.jpg", "genres": [], "credits": {},
            }

        async def fake_artwork(series_tvdb_id, tvdb_api_key, tvdb_lang="eng"):
            return ("https://artworks.thetvdb.com/p.jpg", "https://artworks.thetvdb.com/b.jpg")

        with patch.object(enrichment.tmdb, "get_show", fake_get_show), \
             patch.object(enrichment, "_tvdb_series_artwork", fake_artwork):
            await enrichment.enrich_media(
                media, api_key="k", is_tvdb=True,
                tvdb_api_key="tk", series_tvdb_id=81797,
            )

        self.assertIn("image.tmdb.org", media.poster_path)
        self.assertEqual(media.tvdb_data["poster_path"], "https://artworks.thetvdb.com/p.jpg")
        self.assertEqual(media.tvdb_data["backdrop_path"], "https://artworks.thetvdb.com/b.jpg")


def test_tvdb_language_helper_exists_under_the_name_callers_use():
    """A helper referenced by a name that doesn't exist only fails when the code
    finally runs — which is how this one shipped."""
    import inspect

    from core import tvdb

    for module_name in ("routers.sync", "core.enrichment"):
        module = __import__(module_name, fromlist=["*"])
        source = inspect.getsource(module)
        for call in ("tvdb_client.tvdb_language(", "tvdb.tvdb_language("):
            if call in source:
                break
        assert "to_three_letter_lang" not in source, module_name
    assert callable(tvdb.tvdb_language)
    assert tvdb.tvdb_language(None) == "eng"


def test_switching_to_tvdb_order_builds_the_mapping_even_when_the_show_is_linked():
    """A TVDB id says the show can be rendered; only the mapping says which
    episode a play belongs to, and episodes are stored at TMDB positions."""
    import inspect

    from routers import shows

    source = inspect.getsource(shows.set_show_episode_order)
    assert "needs_mapping = body.force_refresh or not existing" in source
    assert "not existing and not tvdb_id" not in source


def test_clearing_the_database_also_clears_rows_keyed_on_media_ids():
    """A table holding a media or show primary key must be truncated with it —
    a surviving alias points at a deleted row and its unique constraint stops
    the next import from replacing it."""
    import inspect

    from routers import admin

    source = inspect.getsource(admin.clear_database)
    for table in (
        "media_aliases",
        "media_translations",
        "show_translations",
        "episode_movie_conversions",
        "show_season_overrides",
        "show_episode_overrides",
    ):
        assert f'"{table}"' in source, table


def test_tvdb_pages_build_the_mapping_on_first_view():
    """Reaching a TVDB page through the global default-order setting must map
    the show too — only the per-show switch used to."""
    import inspect

    from routers import shows

    helper = inspect.getsource(shows._mappings_for)
    assert "ensure_episode_order_mapping" in helper
    for fn in (shows.get_tvdb_show, shows.get_tvdb_season):
        assert "_mappings_for(" in inspect.getsource(fn), fn.__name__


def test_identity_is_recorded_before_the_long_enrichment_phase():
    """Enrichment and mapping take minutes of network calls and can fail. Ids the
    source already handed over must be committed first, and an item already in
    the library keeps recording them on a later sync."""
    import inspect

    from routers import sync as sync_router

    source = inspect.getsource(sync_router.sync_items)
    assert source.index("record_provider_ids_bulk") < source.index("Batch enrich newly created media")
    # The already-collected branch records ids too, not just newly created rows.
    assert source.count("source_ids_by_media[") >= 2
