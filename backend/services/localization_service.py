from typing import Any
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session
from models.domain.show import Show
from models.domain.user_override import UserOverride


class LocalizationService:
    @staticmethod
    async def set_user_override(
        db: AsyncSession | Session,
        user_id: int,
        asset_type: str,
        asset_id: int,
        custom_title: str | None = None,
        custom_overview: str | None = None,
        custom_poster_path: str | None = None,
        notes: str | None = None,
    ) -> UserOverride:
        stmt = select(UserOverride).where(
            UserOverride.user_id == user_id,
            UserOverride.asset_type == asset_type,
            UserOverride.asset_id == asset_id,
        )
        if isinstance(db, AsyncSession):
            res = await db.execute(stmt)
            existing = res.scalar_one_or_none()
        else:
            existing = db.execute(stmt).scalar_one_or_none()

        if existing:
            if custom_title is not None:
                existing.custom_title = custom_title
            if custom_overview is not None:
                existing.custom_overview = custom_overview
            if custom_poster_path is not None:
                existing.custom_poster_path = custom_poster_path
            if notes is not None:
                existing.notes = notes
            override_obj = existing
        else:
            override_obj = UserOverride(
                user_id=user_id,
                asset_type=asset_type,
                asset_id=asset_id,
                custom_title=custom_title,
                custom_overview=custom_overview,
                custom_poster_path=custom_poster_path,
                notes=notes,
            )
            db.add(override_obj)

        if isinstance(db, AsyncSession):
            await db.flush()
        else:
            db.flush()
        return override_obj

    @staticmethod
    async def get_localized_show_details(
        db: AsyncSession | Session, user_id: int, show_id: int
    ) -> dict[str, Any] | None:
        # SQL-native projection using COALESCE
        stmt = (
            select(
                Show.id,
                func.coalesce(UserOverride.custom_title, Show.canonical_title).label("title"),
                func.coalesce(UserOverride.custom_overview, Show.overview).label("overview"),
                func.coalesce(UserOverride.custom_poster_path, Show.poster_path).label("poster_path"),
            )
            .outerjoin(
                UserOverride,
                (UserOverride.asset_id == Show.id)
                & (UserOverride.asset_type == "show")
                & (UserOverride.user_id == user_id),
            )
            .where(Show.id == show_id)
        )

        if isinstance(db, AsyncSession):
            res = await db.execute(stmt)
            row = res.one_or_none()
        else:
            row = db.execute(stmt).one_or_none()

        if not row:
            return None

        return {
            "id": row.id,
            "title": row.title,
            "overview": row.overview,
            "poster_path": row.poster_path,
        }
