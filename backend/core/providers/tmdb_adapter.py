from typing import Any
from core.providers.base import BaseMetadataProvider, UnifiedShow, UnifiedEpisode, UnifiedMovie


class TMDBAdapter(BaseMetadataProvider):
    def __init__(self, api_key: str | None = None, http_client: Any = None):
        self.api_key = api_key
        self.http_client = http_client

    @property
    def provider_name(self) -> str:
        return "tmdb"

    def parse_show(self, data: dict[str, Any]) -> UnifiedShow:
        ext_ids = data.get("external_ids", {})
        all_ext = {"tmdb": str(data.get("id"))}
        if ext_ids.get("imdb_id"):
            all_ext["imdb"] = str(ext_ids["imdb_id"])
        if ext_ids.get("tvdb_id"):
            all_ext["tvdb"] = str(ext_ids["tvdb_id"])

        return UnifiedShow(
            provider="tmdb",
            external_id=str(data.get("id")),
            title=data.get("name") or data.get("title", ""),
            original_title=data.get("original_name") or data.get("original_title"),
            first_air_date=data.get("first_air_date") or data.get("release_date"),
            status=data.get("status"),
            overview=data.get("overview"),
            poster_path=data.get("poster_path"),
            backdrop_path=data.get("backdrop_path"),
            all_external_ids=all_ext,
        )

    def parse_episode(self, data: dict[str, Any], show_external_id: str | None = None) -> UnifiedEpisode:
        ext_ids = data.get("external_ids", {})
        all_ext = {"tmdb": str(data.get("id"))}
        if ext_ids.get("imdb_id"):
            all_ext["imdb"] = str(ext_ids["imdb_id"])
        if ext_ids.get("tvdb_id"):
            all_ext["tvdb"] = str(ext_ids["tvdb_id"])

        return UnifiedEpisode(
            provider="tmdb",
            external_id=str(data.get("id")),
            show_external_id=show_external_id,
            season_number=data.get("season_number", 1),
            episode_number=data.get("episode_number", 1),
            title=data.get("name", ""),
            release_date=data.get("air_date"),
            overview=data.get("overview"),
            poster_path=data.get("still_path") or data.get("poster_path"),
            runtime=data.get("runtime"),
            all_external_ids=all_ext,
        )

    def parse_movie(self, data: dict[str, Any]) -> UnifiedMovie:
        ext_ids = data.get("external_ids", {})
        all_ext = {"tmdb": str(data.get("id"))}
        if ext_ids.get("imdb_id") or data.get("imdb_id"):
            all_ext["imdb"] = str(ext_ids.get("imdb_id") or data.get("imdb_id"))

        return UnifiedMovie(
            provider="tmdb",
            external_id=str(data.get("id")),
            title=data.get("title", ""),
            original_title=data.get("original_title"),
            release_date=data.get("release_date"),
            overview=data.get("overview"),
            poster_path=data.get("poster_path"),
            backdrop_path=data.get("backdrop_path"),
            runtime=data.get("runtime"),
            all_external_ids=all_ext,
        )

    async def fetch_show_details(self, external_id: str) -> UnifiedShow:
        if self.http_client:
            res = await self.http_client.get(f"/3/tv/{external_id}")
            return self.parse_show(res.json())
        return UnifiedShow(provider="tmdb", external_id=external_id, title=f"Show {external_id}")

    async def fetch_episode_details(
        self, show_external_id: str, season: int, episode: int
    ) -> UnifiedEpisode:
        if self.http_client:
            res = await self.http_client.get(f"/3/tv/{show_external_id}/season/{season}/episode/{episode}")
            return self.parse_episode(res.json(), show_external_id=show_external_id)
        return UnifiedEpisode(
            provider="tmdb",
            external_id=f"{show_external_id}_{season}_{episode}",
            show_external_id=show_external_id,
            season_number=season,
            episode_number=episode,
            title=f"Episode {season}x{episode}",
        )

    async def fetch_movie_details(self, external_id: str) -> UnifiedMovie:
        if self.http_client:
            res = await self.http_client.get(f"/3/movie/{external_id}")
            return self.parse_movie(res.json())
        return UnifiedMovie(provider="tmdb", external_id=external_id, title=f"Movie {external_id}")
