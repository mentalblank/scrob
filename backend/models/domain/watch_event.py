from datetime import datetime, timezone
from typing import Optional
from sqlalchemy import DateTime, ForeignKey, Index, Integer, String, Float, func
from sqlalchemy.orm import Mapped, mapped_column, relationship
from models.domain.base import DomainBase


class WatchEvent(DomainBase):
    __tablename__ = "watch_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    asset_type: Mapped[str] = mapped_column(String(20), default="episode", nullable=False)  # 'episode', 'movie'
    asset_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    media_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)  # Backwards compat alias for asset_id
    provider: Mapped[Optional[str]] = mapped_column(String(50))  # 'plex', 'jellyfin', 'emby'
    progress_percent: Mapped[float] = mapped_column(Float, default=100.0, nullable=False)
    watched_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), nullable=False)

    __table_args__ = (
        Index("idx_watch_events_user_watched", "user_id", "watched_at"),
    )

    def __init__(self, **kwargs):
        if "media_id" in kwargs and "asset_id" not in kwargs:
            kwargs["asset_id"] = kwargs["media_id"]
        elif "asset_id" in kwargs and "media_id" not in kwargs:
            kwargs["media_id"] = kwargs["asset_id"]
        super().__init__(**kwargs)
