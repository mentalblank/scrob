from datetime import datetime
from typing import Optional
from sqlalchemy import Integer, String, Text, Boolean, ForeignKey, DateTime, func, Index
from sqlalchemy.orm import Mapped, mapped_column, relationship
from .base import Base


class Comment(Base):
    __tablename__ = "comments"
    __table_args__ = (
        Index("idx_comments_uri", "media_type", "uri_id", "season_number", "episode_number"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)

    media_type: Mapped[str] = mapped_column(String(50), nullable=False)
    # URI of the show/movie/episode (e.g. tmdb:s:123, tmdb:m:456).
    # For season/episode comments this is the SHOW's uri_id.
    uri_id: Mapped[str] = mapped_column(String(50), nullable=False)
    season_number: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    episode_number: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    content: Mapped[str] = mapped_column(Text, nullable=False)
    is_spoiler: Mapped[bool] = mapped_column(Boolean, default=False, server_default='false', nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), onupdate=func.now())

    user: Mapped["User"] = relationship()
