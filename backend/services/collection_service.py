from typing import Any
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session
from models.domain.collection import Collection, CollectionFile


class CollectionService:
    @staticmethod
    async def add_to_collection(
        db: AsyncSession | Session,
        user_id: int,
        media_id: int,
        source: str = "manual",
    ) -> Collection:
        item = Collection(user_id=user_id, asset_type="show", asset_id=media_id)
        db.add(item)
        if isinstance(db, AsyncSession):
            await db.flush()
        else:
            db.flush()
        return item

    @staticmethod
    async def get_user_collection(
        db: AsyncSession | Session, user_id: int
    ) -> list[Collection]:
        stmt = select(Collection).where(Collection.user_id == user_id)
        if isinstance(db, AsyncSession):
            res = await db.execute(stmt)
            return list(res.scalars().all())
        else:
            res = db.execute(stmt).scalars().all()
