from datetime import datetime
from typing import Optional

from sqlalchemy import DateTime, ForeignKey, Integer, String, UniqueConstraint, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .base import Base


class EpisodeMovieConversion(Base):
    """Record of an episode row that was re-filed as a movie.

    Conversion mutates the media row in place so collection and watch history
    follow it, which means the original coordinates would otherwise be lost.
    Keeping them here makes the change visible in the UI and reversible.
    """

    __tablename__ = "episode_movie_conversions"
    __table_args__ = (
        UniqueConstraint("user_id", "media_id", name="uq_episode_movie_conversion"),
    )

    id                      : Mapped[int]            = mapped_column(Integer, primary_key=True)
    user_id                 : Mapped[int]            = mapped_column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    media_id                : Mapped[int]            = mapped_column(Integer, ForeignKey("media.id", ondelete="CASCADE"), nullable=False)
    original_show_id        : Mapped[Optional[int]]  = mapped_column(Integer, ForeignKey("shows.id", ondelete="SET NULL"), nullable=True)
    original_season_number  : Mapped[Optional[int]]  = mapped_column(Integer, nullable=True)
    original_episode_number : Mapped[Optional[int]]  = mapped_column(Integer, nullable=True)
    original_title          : Mapped[Optional[str]]  = mapped_column(String(500), nullable=True)
    original_tmdb_id        : Mapped[Optional[int]]  = mapped_column(Integer, nullable=True)
    movie_tmdb_id           : Mapped[int]            = mapped_column(Integer, nullable=False)
    created_at              : Mapped[datetime]       = mapped_column(DateTime, server_default=func.now(), nullable=False)

    media        : Mapped["Media"] = relationship("Media", foreign_keys=[media_id])
    original_show: Mapped[Optional["Show"]] = relationship("Show", foreign_keys=[original_show_id])
