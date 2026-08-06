import difflib
from typing import Any
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session

from models.domain.show import Show
from models.domain.season import Season
from models.domain.episode import Episode
from services.external_id_registry import ExternalIDRegistryService


class AssetResolver:
    @staticmethod
    async def resolve_show(
        db: AsyncSession | Session,
        show_ref: str | int,
        provider: str = "tmdb",
        title: str | None = None,
    ) -> Show | None:
        # 1. Direct integer primary key match if show_ref is integer or numeric string
        if isinstance(show_ref, int) or (isinstance(show_ref, str) and show_ref.isdigit()):
            show_id = int(show_ref)
            stmt = select(Show).where(Show.id == show_id)
            show = await AssetResolver._exec_scalar(db, stmt)
            if show:
                return show

        # 2. Check external ID registry
        resolved_id = await ExternalIDRegistryService.resolve_asset_id(
            db, provider=provider, external_id=str(show_ref), asset_type="show"
        )
        if resolved_id:
            stmt = select(Show).where(Show.id == resolved_id)
            show = await AssetResolver._exec_scalar(db, stmt)
            if show:
                return show

        # 3. Fuzzy title match if title is provided
        if title:
            stmt = select(Show)
            shows = await AssetResolver._exec_scalars(db, stmt)
            for s in shows:
                if s.canonical_title.lower() == title.lower() or (
                    s.original_title and s.original_title.lower() == title.lower()
                ):
                    return s
                ratio = difflib.SequenceMatcher(None, s.canonical_title.lower(), title.lower()).ratio()
                if ratio >= 0.9:
                    return s

        return None

    @staticmethod
    async def resolve_episode(
        db: AsyncSession | Session,
        show_ref: str | int,
        season: int,
        episode: int,
        provider: str = "tmdb",
        external_ep_id: str | None = None,
        episode_title: str | None = None,
        release_date: str | None = None,
    ) -> Episode | None:
        # Step 1: Resolve show entity first
        show = await AssetResolver.resolve_show(db, show_ref=show_ref, provider=provider)
        if not show:
            return None

        # Step 2: Check exact match in ExternalIDRegistry by external_ep_id if present
        if external_ep_id:
            ep_id = await ExternalIDRegistryService.resolve_asset_id(
                db, provider=provider, external_id=str(external_ep_id), asset_type="episode"
            )
            if ep_id:
                stmt = select(Episode).where(Episode.id == ep_id)
                ep = await AssetResolver._exec_scalar(db, stmt)
                if ep:
                    return ep

        # Step 3: Check TVDB position mapping if provider is tvdb
        if provider == "tvdb":
            ep_id = await ExternalIDRegistryService.get_tvdb_position_mapping(
                db, show_id=show.id, tvdb_season=season, tvdb_ep=episode
            )
            if ep_id:
                stmt = select(Episode).where(Episode.id == ep_id)
                ep = await AssetResolver._exec_scalar(db, stmt)
                if ep:
                    return ep

        # Step 4: Check canonical (show_id, season_number, episode_number) in episodes table
        stmt = select(Episode).where(
            Episode.show_id == show.id,
            Episode.season_number == season,
            Episode.episode_number == episode,
        )
        ep = await AssetResolver._exec_scalar(db, stmt)
        if ep:
            return ep

        # Step 5: Fuzzy title / release date match among show's episodes
        stmt = select(Episode).where(Episode.show_id == show.id)
        show_episodes = await AssetResolver._exec_scalars(db, stmt)

        if episode_title:
            for e in show_episodes:
                if e.canonical_title.lower() == episode_title.lower():
                    return e
                ratio = difflib.SequenceMatcher(None, e.canonical_title.lower(), episode_title.lower()).ratio()
                if ratio >= 0.85:
                    return e

        if release_date:
            for e in show_episodes:
                if e.release_date == release_date:
                    return e

        return None

    @staticmethod
    async def _exec_scalar(db: AsyncSession | Session, stmt: Any) -> Any:
        if isinstance(db, AsyncSession):
            res = await db.execute(stmt)
            return res.scalar_one_or_none()
        else:
            res = db.execute(stmt)
            return res.scalar_one_or_none()

    @staticmethod
    async def _exec_scalars(db: AsyncSession | Session, stmt: Any) -> list[Any]:
        if isinstance(db, AsyncSession):
            res = await db.execute(stmt)
            return list(res.scalars().all())
        else:
            res = db.execute(stmt)
            return list(res.scalars().all())
