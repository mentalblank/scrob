from datetime import datetime
from typing import TYPE_CHECKING
from sqlalchemy import DateTime, ForeignKey, Integer, String, Text, UniqueConstraint, func
from sqlalchemy.orm import Mapped, mapped_column, relationship
from models.domain.base import DomainBase

if TYPE_CHECKING:
    from .show import Show
    from .episode import Episode


class Season(DomainBase):
    __tablename__ = "seasons"
    __table_args__ = (
        UniqueConstraint("show_id", "season_number", name="uq_seasons_show_season"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    show_id: Mapped[int] = mapped_column(Integer, ForeignKey("shows.id", ondelete="CASCADE"), nullable=False)
    season_number: Mapped[int] = mapped_column(Integer, nullable=False)
    canonical_title: Mapped[str | None] = mapped_column(String(500))
    overview: Mapped[str | None] = mapped_column(Text)
    poster_path: Mapped[str | None] = mapped_column(String(500))
    episode_count: Mapped[int | None] = mapped_column(Integer, default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())

    show: Mapped["Show"] = relationship("Show", back_populates="seasons")
    episodes: Mapped[list["Episode"]] = relationship("Episode", back_populates="season", cascade="all, delete-orphan")
