from typing import Any
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session
from models.domain.rating import Rating


class RatingService:
    @staticmethod
    async def set_rating(
        db: AsyncSession | Session,
        user_id: int,
        asset_type: str,
        asset_id: int,
        rating_value: int,
    ) -> Rating:
        if not (1 <= rating_value <= 10):
            raise ValueError("Rating value must be an integer between 1 and 10.")

        # Check existing rating
        stmt = select(Rating).where(
            Rating.user_id == user_id,
            Rating.asset_type == asset_type,
            Rating.asset_id == asset_id,
        )
        if isinstance(db, AsyncSession):
            res = await db.execute(stmt)
            existing = res.scalar_one_or_none()
        else:
            existing = db.execute(stmt).scalar_one_or_none()

        if existing:
            existing.rating = float(rating_value)
            rating_obj = existing
        else:
            rating_obj = Rating(
                user_id=user_id,
                asset_type=asset_type,
                asset_id=asset_id,
                rating=float(rating_value),
            )
            db.add(rating_obj)

        if isinstance(db, AsyncSession):
            await db.flush()
        else:
            db.flush()
        return rating_obj

    @staticmethod
    async def get_user_ratings(
        db: AsyncSession | Session, user_id: int, limit: int = 50, offset: int = 0
    ) -> list[Rating]:
        stmt = (
            select(Rating)
            .where(Rating.user_id == user_id)
            .order_by(Rating.rated_at.desc())
            .offset(offset)
            .limit(limit)
        )
        if isinstance(db, AsyncSession):
            res = await db.execute(stmt)
            return list(res.scalars().all())
        else:
            res = db.execute(stmt)
            return list(res.scalars().all())
