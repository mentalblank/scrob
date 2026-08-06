from datetime import datetime
from typing import TYPE_CHECKING
from sqlalchemy import DateTime, ForeignKey, Integer, String, Text, UniqueConstraint, func
from sqlalchemy.orm import Mapped, mapped_column, relationship
from models.domain.base import DomainBase

if TYPE_CHECKING:
    from .show import Show
    from .season import Season


class Episode(DomainBase):
    __tablename__ = "episodes"
    __table_args__ = (
        UniqueConstraint("show_id", "season_number", "episode_number", name="uq_episodes_show_season_ep"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    show_id: Mapped[int] = mapped_column(Integer, ForeignKey("shows.id", ondelete="CASCADE"), nullable=False)
    season_id: Mapped[int | None] = mapped_column(Integer, ForeignKey("seasons.id", ondelete="CASCADE"))
    season_number: Mapped[int] = mapped_column(Integer, nullable=False)
    episode_number: Mapped[int] = mapped_column(Integer, nullable=False)
    canonical_title: Mapped[str] = mapped_column(String(500), nullable=False)
    release_date: Mapped[str | None] = mapped_column(String(20))
    overview: Mapped[str | None] = mapped_column(Text)
    poster_path: Mapped[str | None] = mapped_column(String(500))
    runtime: Mapped[int | None] = mapped_column(Integer)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())

    show: Mapped["Show"] = relationship("Show", back_populates="episodes")
    season: Mapped["Season | None"] = relationship("Season", back_populates="episodes")
