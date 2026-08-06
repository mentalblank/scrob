from typing import Any
from core.providers.base import BaseMetadataProvider, UnifiedShow, UnifiedEpisode, UnifiedMovie


class TVDBAdapter(BaseMetadataProvider):
    def __init__(self, api_key: str | None = None, http_client: Any = None):
        self.api_key = api_key
        self.http_client = http_client

    @property
    def provider_name(self) -> str:
        return "tvdb"

    def parse_show(self, data: dict[str, Any]) -> UnifiedShow:
        remote_ids = data.get("remoteIds", [])
        all_ext = {"tvdb": str(data.get("id"))}
        for rid in remote_ids:
            if rid.get("type") == 2:  # IMDB
                all_ext["imdb"] = str(rid.get("id"))
            elif rid.get("type") == 12:  # TMDB
                all_ext["tmdb"] = str(rid.get("id"))

        return UnifiedShow(
            provider="tvdb",
            external_id=str(data.get("id")),
            title=data.get("name", ""),
            original_title=data.get("originalName"),
            first_air_date=data.get("firstAired"),
            status=data.get("status", {}).get("name") if isinstance(data.get("status"), dict) else data.get("status"),
            overview=data.get("overview"),
            poster_path=data.get("image"),
            backdrop_path=data.get("artworks", [{}])[0].get("image") if data.get("artworks") else None,
            all_external_ids=all_ext,
        )

    def parse_episode(self, data: dict[str, Any], show_external_id: str | None = None) -> UnifiedEpisode:
        all_ext = {"tvdb": str(data.get("id"))}
        return UnifiedEpisode(
            provider="tvdb",
            external_id=str(data.get("id")),
            show_external_id=show_external_id or str(data.get("seriesId")),
            season_number=data.get("seasonNumber", 1),
            episode_number=data.get("number", 1),
            title=data.get("name", ""),
            release_date=data.get("aired"),
            overview=data.get("overview"),
            poster_path=data.get("image"),
            runtime=data.get("runtime"),
            all_external_ids=all_ext,
        )

    def parse_movie(self, data: dict[str, Any]) -> UnifiedMovie:
        all_ext = {"tvdb": str(data.get("id"))}
        return UnifiedMovie(
            provider="tvdb",
            external_id=str(data.get("id")),
            title=data.get("name", ""),
            release_date=data.get("released"),
            overview=data.get("overview"),
            poster_path=data.get("image"),
            runtime=data.get("runtime"),
            all_external_ids=all_ext,
        )

    async def fetch_show_details(self, external_id: str) -> UnifiedShow:
        if self.http_client:
            res = await self.http_client.get(f"/v4/series/{external_id}/extended")
            return self.parse_show(res.json().get("data", {}))
        return UnifiedShow(provider="tvdb", external_id=external_id, title=f"TVDB Show {external_id}")

    async def fetch_episode_details(
        self, show_external_id: str, season: int, episode: int
    ) -> UnifiedEpisode:
        if self.http_client:
            res = await self.http_client.get(f"/v4/episodes/{show_external_id}")
            return self.parse_episode(res.json().get("data", {}), show_external_id=show_external_id)
        return UnifiedEpisode(
            provider="tvdb",
            external_id=f"{show_external_id}_{season}_{episode}",
            show_external_id=show_external_id,
            season_number=season,
            episode_number=episode,
            title=f"TVDB Episode {season}x{episode}",
        )

    async def fetch_movie_details(self, external_id: str) -> UnifiedMovie:
        if self.http_client:
            res = await self.http_client.get(f"/v4/movies/{external_id}")
            return self.parse_movie(res.json().get("data", {}))
        return UnifiedMovie(provider="tvdb", external_id=external_id, title=f"TVDB Movie {external_id}")
