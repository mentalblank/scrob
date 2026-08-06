from typing import Any
from core.providers.base import BaseMetadataProvider, UnifiedShow, UnifiedEpisode, UnifiedMovie


class MDBListAdapter(BaseMetadataProvider):
    def __init__(self, api_key: str | None = None, http_client: Any = None):
        self.api_key = api_key
        self.http_client = http_client

    @property
    def provider_name(self) -> str:
        return "mdblist"

    def parse_item(self, data: dict[str, Any]) -> UnifiedShow | UnifiedMovie:
        all_ext = {}
        if data.get("id"):
            all_ext["mdblist"] = str(data["id"])
        if data.get("tmdbid"):
            all_ext["tmdb"] = str(data["tmdbid"])
        if data.get("tvdbid"):
            all_ext["tvdb"] = str(data["tvdbid"])
        if data.get("imdbid"):
            all_ext["imdb"] = str(data["imdbid"])
        if data.get("traktid"):
            all_ext["trakt"] = str(data["traktid"])

        mediatype = data.get("mediatype", "show")
        if mediatype == "movie":
            return UnifiedMovie(
                provider="mdblist",
                external_id=str(data.get("id") or data.get("tmdbid")),
                title=data.get("title", ""),
                release_date=str(data.get("year")) if data.get("year") else None,
                overview=data.get("description"),
                poster_path=data.get("poster"),
                runtime=data.get("runtime"),
                all_external_ids=all_ext,
            )
        else:
            return UnifiedShow(
                provider="mdblist",
                external_id=str(data.get("id") or data.get("tvdbid") or data.get("tmdbid")),
                title=data.get("title", ""),
                first_air_date=str(data.get("year")) if data.get("year") else None,
                status=data.get("status"),
                overview=data.get("description"),
                poster_path=data.get("poster"),
                all_external_ids=all_ext,
            )

    async def fetch_show_details(self, external_id: str) -> UnifiedShow:
        if self.http_client:
            res = await self.http_client.get(f"/?apikey={self.api_key}&i={external_id}")
            item = self.parse_item(res.json())
            if isinstance(item, UnifiedShow):
                return item
        return UnifiedShow(provider="mdblist", external_id=external_id, title=f"MDBList Show {external_id}")

    async def fetch_episode_details(
        self, show_external_id: str, season: int, episode: int
    ) -> UnifiedEpisode:
        return UnifiedEpisode(
            provider="mdblist",
            external_id=f"{show_external_id}_{season}_{episode}",
            show_external_id=show_external_id,
            season_number=season,
            episode_number=episode,
            title=f"MDBList Episode {season}x{episode}",
        )

    async def fetch_movie_details(self, external_id: str) -> UnifiedMovie:
        if self.http_client:
            res = await self.http_client.get(f"/?apikey={self.api_key}&i={external_id}")
            item = self.parse_item(res.json())
            if isinstance(item, UnifiedMovie):
                return item
        return UnifiedMovie(provider="mdblist", external_id=external_id, title=f"MDBList Movie {external_id}")
