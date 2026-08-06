import asyncio
import pytest
from core.providers import (
    TMDBAdapter,
    TVDBAdapter,
    MDBListAdapter,
    UnifiedShow,
    UnifiedEpisode,
    UnifiedMovie,
)


def test_tmdb_adapter_parse_show():
    adapter = TMDBAdapter()
    raw_data = {
        "id": 1396,
        "name": "Breaking Bad",
        "original_name": "Breaking Bad",
        "first_air_date": "2008-01-20",
        "status": "Ended",
        "overview": "A chemistry teacher diagnosed with lung cancer...",
        "poster_path": "/path/to/poster.jpg",
        "backdrop_path": "/path/to/backdrop.jpg",
        "external_ids": {"imdb_id": "tt0959621", "tvdb_id": 81189},
    }

    unified = adapter.parse_show(raw_data)
    assert isinstance(unified, UnifiedShow)
    assert unified.provider == "tmdb"
    assert unified.external_id == "1396"
    assert unified.title == "Breaking Bad"
    assert unified.all_external_ids == {"tmdb": "1396", "imdb": "tt0959621", "tvdb": "81189"}


def test_tmdb_adapter_parse_episode():
    adapter = TMDBAdapter()
    raw_data = {
        "id": 62085,
        "name": "Pilot",
        "season_number": 1,
        "episode_number": 1,
        "air_date": "2008-01-20",
        "overview": "Walter White turns 50...",
        "still_path": "/path/to/still.jpg",
        "runtime": 58,
        "external_ids": {"imdb_id": "tt0959621_1_1", "tvdb_id": 348530},
    }

    unified = adapter.parse_episode(raw_data, show_external_id="1396")
    assert isinstance(unified, UnifiedEpisode)
    assert unified.provider == "tmdb"
    assert unified.external_id == "62085"
    assert unified.show_external_id == "1396"
    assert unified.season_number == 1
    assert unified.episode_number == 1
    assert unified.title == "Pilot"
    assert unified.all_external_ids["tvdb"] == "348530"


def test_tvdb_adapter_parse_show():
    adapter = TVDBAdapter()
    raw_data = {
        "id": 81189,
        "name": "Breaking Bad",
        "firstAired": "2008-01-20",
        "overview": "TVDB Breaking Bad overview",
        "image": "https://art.tvdb/poster.jpg",
        "remoteIds": [
            {"type": 2, "id": "tt0959621"},
            {"type": 12, "id": "1396"},
        ],
    }

    unified = adapter.parse_show(raw_data)
    assert isinstance(unified, UnifiedShow)
    assert unified.provider == "tvdb"
    assert unified.external_id == "81189"
    assert unified.title == "Breaking Bad"
    assert unified.all_external_ids == {"tvdb": "81189", "imdb": "tt0959621", "tmdb": "1396"}


def test_mdblist_adapter_parse_item():
    adapter = MDBListAdapter()
    raw_show = {
        "id": 999,
        "title": "Succession",
        "mediatype": "show",
        "tmdbid": 76331,
        "tvdbid": 338186,
        "imdbid": "tt7660850",
        "year": 2018,
    }

    unified = adapter.parse_item(raw_show)
    assert isinstance(unified, UnifiedShow)
    assert unified.provider == "mdblist"
    assert unified.title == "Succession"
    assert unified.all_external_ids["tmdb"] == "76331"
    assert unified.all_external_ids["tvdb"] == "338186"
    assert unified.all_external_ids["imdb"] == "tt7660850"


def test_async_fetch_fallbacks():
    tmdb = TMDBAdapter()
    tvdb = TVDBAdapter()
    mdblist = MDBListAdapter(api_key="mock_key")

    show_tmdb = asyncio.run(tmdb.fetch_show_details("1396"))
    show_tvdb = asyncio.run(tvdb.fetch_show_details("81189"))
    show_mdb = asyncio.run(mdblist.fetch_show_details("999"))

    assert show_tmdb.provider == "tmdb"
    assert show_tvdb.provider == "tvdb"
    assert show_mdb.provider == "mdblist"
