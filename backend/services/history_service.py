from datetime import datetime, timezone
from typing import Any
from sqlalchemy import select, delete
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session
from models.domain.watch_event import WatchEvent


class HistoryService:
    @staticmethod
    async def record_watch_event(
        db: AsyncSession | Session,
        user_id: int,
        asset_type: str,
        asset_id: int,
        watched_at: datetime | None = None,
    ) -> WatchEvent:
        event = WatchEvent(
            user_id=user_id,
            asset_type=asset_type,
            asset_id=asset_id,
            watched_at=watched_at or datetime.now(timezone.utc),
        )
        db.add(event)
        if isinstance(db, AsyncSession):
            await db.flush()
        else:
            db.flush()
        return event

    @staticmethod
    async def delete_watch_event(
        db: AsyncSession | Session, user_id: int, event_id: int
    ) -> bool:
        stmt = delete(WatchEvent).where(WatchEvent.id == event_id, WatchEvent.user_id == user_id)
        if isinstance(db, AsyncSession):
            res = await db.execute(stmt)
            return res.rowcount > 0
        else:
            res = db.execute(stmt)
            return res.rowcount > 0

    @staticmethod
    async def get_user_history_feed(
        db: AsyncSession | Session, user_id: int, limit: int = 50, offset: int = 0
    ) -> list[WatchEvent]:
        stmt = (
            select(WatchEvent)
            .where(WatchEvent.user_id == user_id)
            .order_by(WatchEvent.watched_at.desc())
            .offset(offset)
            .limit(limit)
        )
        if isinstance(db, AsyncSession):
            res = await db.execute(stmt)
            return list(res.scalars().all())
        else:
            res = db.execute(stmt)
            return list(res.scalars().all())
