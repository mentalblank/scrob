import logging
from typing import Any

logger = logging.getLogger(__name__)


class SonarrClient:
    def __init__(self, base_url: str, api_key: str):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key

    async def add_series(
        self,
        tvdb_id: int,
        title: str,
        quality_profile_id: int,
        root_folder_path: str,
        season_folder: bool = True,
    ) -> dict[str, Any]:
        logger.info("Adding series '%s' (TVDB %s) to Sonarr", title, tvdb_id)
        # Mock/simulated Sonarr v3/v4 API response
        return {
            "id": 101,
            "title": title,
            "tvdbId": tvdb_id,
            "qualityProfileId": quality_profile_id,
            "path": f"{root_folder_path}/{title}",
            "seasonFolder": season_folder,
            "monitored": True,
            "status": "added",
        }

    async def get_series(self, tvdb_id: int) -> dict[str, Any] | None:
        return {
            "id": 101,
            "title": "Sample Series",
            "tvdbId": tvdb_id,
            "status": "existing",
        }


class RadarrClient:
    def __init__(self, base_url: str, api_key: str):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key

    async def add_movie(
        self,
        tmdb_id: int,
        title: str,
        quality_profile_id: int,
        root_folder_path: str,
    ) -> dict[str, Any]:
        logger.info("Adding movie '%s' (TMDB %s) to Radarr", title, tmdb_id)
        # Mock/simulated Radarr v3/v4 API response
        return {
            "id": 202,
            "title": title,
            "tmdbId": tmdb_id,
            "qualityProfileId": quality_profile_id,
            "path": f"{root_folder_path}/{title}",
            "monitored": True,
            "status": "added",
        }

    async def get_movie(self, tmdb_id: int) -> dict[str, Any] | None:
        return {
            "id": 202,
            "title": "Sample Movie",
            "tmdbId": tmdb_id,
            "status": "existing",
        }
