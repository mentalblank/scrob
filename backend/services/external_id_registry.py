from typing import Any
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session
from models.domain.external_id import ExternalID


class ExternalIDRegistryService:
    @staticmethod
    async def register_external_id(
        db: AsyncSession | Session,
        asset_type: str,
        asset_id: int,
        provider: str,
        external_id: str,
        provider_season: int | None = None,
        provider_episode: int | None = None,
    ) -> ExternalID:
        ext = ExternalID(
            asset_type=asset_type,
            asset_id=asset_id,
            provider=provider,
            external_id=str(external_id),
            provider_season=provider_season,
            provider_episode=provider_episode,
        )
        db.add(ext)
        if isinstance(db, AsyncSession):
            await db.flush()
        else:
            db.flush()
        return ext

    @staticmethod
    async def resolve_asset_id(
        db: AsyncSession | Session,
        provider: str,
        external_id: str,
        asset_type: str,
    ) -> int | None:
        stmt = select(ExternalID.asset_id).where(
            ExternalID.provider == provider,
            ExternalID.external_id == str(external_id),
            ExternalID.asset_type == asset_type,
        )
        if isinstance(db, AsyncSession):
            res = await db.execute(stmt)
            return res.scalar_one_or_none()
        else:
            res = db.execute(stmt)
            return res.scalar_one_or_none()

    @staticmethod
    async def get_tvdb_position_mapping(
        db: AsyncSession | Session,
        show_id: int,
        tvdb_season: int,
        tvdb_ep: int,
    ) -> int | None:
        stmt = select(ExternalID.asset_id).where(
            ExternalID.asset_id == show_id,
            ExternalID.provider == "tvdb",
            ExternalID.provider_season == tvdb_season,
            ExternalID.provider_episode == tvdb_ep,
            ExternalID.asset_type == "episode",
        )
        if isinstance(db, AsyncSession):
            res = await db.execute(stmt)
            return res.scalar_one_or_none()
        else:
            res = db.execute(stmt)
            return res.scalar_one_or_none()

    @staticmethod
    async def get_external_ids_for_asset(
        db: AsyncSession | Session,
        asset_type: str,
        asset_id: int,
    ) -> dict[str, str]:
        stmt = select(ExternalID.provider, ExternalID.external_id).where(
            ExternalID.asset_type == asset_type,
            ExternalID.asset_id == asset_id,
        )
        if isinstance(db, AsyncSession):
            res = await db.execute(stmt)
            rows = res.all()
        else:
            rows = db.execute(stmt).all()
        return {r[0]: r[1] for r in rows}
