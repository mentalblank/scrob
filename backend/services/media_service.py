from typing import Any
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session
from models.domain.show import Show
from models.domain.season import Season
from models.domain.episode import Episode


class MediaService:
    @staticmethod
    async def get_show_details(db: AsyncSession | Session, show_id: int) -> dict[str, Any] | None:
        stmt = select(Show).where(Show.id == show_id)
        show = await MediaService._exec_scalar(db, stmt)
        if not show:
            return None
        return {
            "id": show.id,
            "canonical_title": show.canonical_title,
            "original_title": show.original_title,
            "first_air_date": show.first_air_date,
            "status": show.status,
            "overview": show.overview,
            "poster_path": show.poster_path,
            "backdrop_path": show.backdrop_path,
        }

    @staticmethod
    async def get_season_details(
        db: AsyncSession | Session, show_id: int, season_num: int
    ) -> dict[str, Any] | None:
        stmt = select(Season).where(Season.show_id == show_id, Season.season_number == season_num)
        season = await MediaService._exec_scalar(db, stmt)
        if not season:
            return None
        return {
            "id": season.id,
            "show_id": season.show_id,
            "season_number": season.season_number,
            "canonical_title": season.canonical_title,
            "overview": season.overview,
            "poster_path": season.poster_path,
            "episode_count": season.episode_count,
        }

    @staticmethod
    async def get_episode_details(
        db: AsyncSession | Session, show_id: int, season_num: int, ep_num: int
    ) -> dict[str, Any] | None:
        stmt = select(Episode).where(
            Episode.show_id == show_id,
            Episode.season_number == season_num,
            Episode.episode_number == ep_num,
        )
        episode = await MediaService._exec_scalar(db, stmt)
        if not episode:
            return None
        return {
            "id": episode.id,
            "show_id": episode.show_id,
            "season_id": episode.season_id,
            "season_number": episode.season_number,
            "episode_number": episode.episode_number,
            "canonical_title": episode.canonical_title,
            "release_date": episode.release_date,
            "overview": episode.overview,
            "poster_path": episode.poster_path,
            "runtime": episode.runtime,
        }

    @staticmethod
    async def _exec_scalar(db: AsyncSession | Session, stmt: Any) -> Any:
        if isinstance(db, AsyncSession):
            res = await db.execute(stmt)
            return res.scalar_one_or_none()
        else:
            res = db.execute(stmt)
            return res.scalar_one_or_none()
