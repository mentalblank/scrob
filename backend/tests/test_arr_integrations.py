import asyncio
import pytest
from integrations import SonarrClient, RadarrClient


def test_sonarr_client_add_and_get_series():
    client = SonarrClient(base_url="http://localhost:8989", api_key="test_sonarr_key")

    add_res = asyncio.run(
        client.add_series(
            tvdb_id=81189,
            title="Breaking Bad",
            quality_profile_id=1,
            root_folder_path="/tv",
            season_folder=True,
        )
    )

    assert add_res["tvdbId"] == 81189
    assert add_res["qualityProfileId"] == 1
    assert add_res["status"] == "added"

    get_res = asyncio.run(client.get_series(tvdb_id=81189))
    assert get_res is not None
    assert get_res["tvdbId"] == 81189


def test_radarr_client_add_and_get_movie():
    client = RadarrClient(base_url="http://localhost:7878", api_key="test_radarr_key")

    add_res = asyncio.run(
        client.add_movie(
            tmdb_id=27205,
            title="Inception",
            quality_profile_id=2,
            root_folder_path="/movies",
        )
    )

    assert add_res["tmdbId"] == 27205
    assert add_res["qualityProfileId"] == 2
    assert add_res["status"] == "added"

    get_res = asyncio.run(client.get_movie(tmdb_id=27205))
    assert get_res is not None
    assert get_res["tmdbId"] == 27205
