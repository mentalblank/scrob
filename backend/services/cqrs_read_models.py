from typing import Any
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session
from models.domain.show import Show
from models.domain.episode import Episode
from models.domain.watch_event import WatchEvent


class CQRSReadModels:
    @staticmethod
    async def get_show_progress_summary(
        db: AsyncSession | Session, user_id: int, show_id: int
    ) -> dict[str, Any]:
        # 1. Fetch total aired episodes count in a single query
        stmt_ep_count = select(func.count(Episode.id)).where(Episode.show_id == show_id)
        if isinstance(db, AsyncSession):
            total_episodes = (await db.execute(stmt_ep_count)).scalar() or 0
        else:
            total_episodes = db.execute(stmt_ep_count).scalar() or 0

        # 2. Fetch total watched episodes count for this show in a single query
        stmt_watched_count = (
            select(func.count(func.distinct(WatchEvent.asset_id)))
            .join(Episode, Episode.id == WatchEvent.asset_id)
            .where(WatchEvent.user_id == user_id, Episode.show_id == show_id)
        )
        if isinstance(db, AsyncSession):
            watched_episodes = (await db.execute(stmt_watched_count)).scalar() or 0
        else:
            watched_episodes = db.execute(stmt_watched_count).scalar() or 0

        progress_percent = (
            round((watched_episodes / total_episodes) * 100, 1) if total_episodes > 0 else 0.0
        )

        return {
            "show_id": show_id,
            "user_id": user_id,
            "total_episodes": total_episodes,
            "watched_episodes": watched_episodes,
            "unwatched_episodes": max(0, total_episodes - watched_episodes),
            "progress_percent": progress_percent,
            "is_completed": total_episodes > 0 and watched_episodes >= total_episodes,
        }

    @staticmethod
    async def get_dashboard_feed(
        db: AsyncSession | Session, user_id: int, limit: int = 20, cursor: int | None = None
    ) -> dict[str, Any]:
        stmt = (
            select(WatchEvent)
            .where(WatchEvent.user_id == user_id)
            .order_by(WatchEvent.id.desc())
            .limit(limit)
        )
        if cursor:
            stmt = stmt.where(WatchEvent.id < cursor)

        if isinstance(db, AsyncSession):
            res = await db.execute(stmt)
            recent_events = list(res.scalars().all())
        else:
            res = db.execute(stmt)
            recent_events = list(res.scalars().all())

        next_cursor = recent_events[-1].id if recent_events else None

        return {
            "user_id": user_id,
            "limit": limit,
            "next_cursor": next_cursor,
            "has_more": len(recent_events) == limit,
            "items": [
                {
                    "event_id": ev.id,
                    "asset_id": ev.asset_id,
                    "watched_at": ev.watched_at.isoformat() if ev.watched_at else None,
                }
                for ev in recent_events
            ],
        }
