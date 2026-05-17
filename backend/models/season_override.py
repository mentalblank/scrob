from datetime import datetime
from sqlalchemy import DateTime, ForeignKey, Integer, UniqueConstraint, func
from sqlalchemy.orm import Mapped, mapped_column
from .base import Base


class ShowSeasonOverride(Base):
    __tablename__ = "show_season_overrides"
    __table_args__ = (
        UniqueConstraint("user_id", "source_show_tmdb_id", "source_season_number", name="uq_season_override"),
    )

    id                   : Mapped[int]      = mapped_column(Integer, primary_key=True)
    user_id              : Mapped[int]      = mapped_column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    source_show_tmdb_id  : Mapped[int]      = mapped_column(Integer, nullable=False)
    source_season_number : Mapped[int]      = mapped_column(Integer, nullable=False)
    target_show_tmdb_id  : Mapped[int]      = mapped_column(Integer, nullable=False)
    target_season_number : Mapped[int]      = mapped_column(Integer, nullable=False)
    created_at           : Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), nullable=False)


class ShowEpisodeOverride(Base):
    __tablename__ = "show_episode_overrides"
    __table_args__ = (
        UniqueConstraint("user_id", "source_show_tmdb_id", "source_season_number", "source_episode_number", name="uq_episode_override"),
    )

    id                    : Mapped[int]      = mapped_column(Integer, primary_key=True)
    user_id               : Mapped[int]      = mapped_column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    source_show_tmdb_id   : Mapped[int]      = mapped_column(Integer, nullable=False)
    source_season_number  : Mapped[int]      = mapped_column(Integer, nullable=False)
    source_episode_number : Mapped[int]      = mapped_column(Integer, nullable=False)
    target_show_tmdb_id   : Mapped[int]      = mapped_column(Integer, nullable=False)
    target_season_number  : Mapped[int]      = mapped_column(Integer, nullable=False)
    target_episode_number : Mapped[int]      = mapped_column(Integer, nullable=False)
    created_at            : Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), nullable=False)
