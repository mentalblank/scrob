from abc import ABC, abstractmethod
from typing import Any
from pydantic import BaseModel, Field


class UnifiedShow(BaseModel):
    provider: str
    external_id: str
    title: str
    original_title: str | None = None
    first_air_date: str | None = None
    status: str | None = None
    overview: str | None = None
    poster_path: str | None = None
    backdrop_path: str | None = None
    all_external_ids: dict[str, str] = Field(default_factory=dict)


class UnifiedEpisode(BaseModel):
    provider: str
    external_id: str
    show_external_id: str | None = None
    season_number: int
    episode_number: int
    title: str
    release_date: str | None = None
    overview: str | None = None
    poster_path: str | None = None
    runtime: int | None = None
    all_external_ids: dict[str, str] = Field(default_factory=dict)


class UnifiedMovie(BaseModel):
    provider: str
    external_id: str
    title: str
    original_title: str | None = None
    release_date: str | None = None
    overview: str | None = None
    poster_path: str | None = None
    backdrop_path: str | None = None
    runtime: int | None = None
    all_external_ids: dict[str, str] = Field(default_factory=dict)


class BaseMetadataProvider(ABC):
    @property
    @abstractmethod
    def provider_name(self) -> str:
        pass

    @abstractmethod
    async def fetch_show_details(self, external_id: str) -> UnifiedShow:
        pass

    @abstractmethod
    async def fetch_episode_details(
        self, show_external_id: str, season: int, episode: int
    ) -> UnifiedEpisode:
        pass

    @abstractmethod
    async def fetch_movie_details(self, external_id: str) -> UnifiedMovie:
        pass
