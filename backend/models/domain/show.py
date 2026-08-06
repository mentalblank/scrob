from datetime import datetime
from typing import TYPE_CHECKING
from sqlalchemy import DateTime, Integer, String, Text, func
from sqlalchemy.orm import Mapped, mapped_column, relationship
from models.domain.base import DomainBase

if TYPE_CHECKING:
    from .season import Season
    from .episode import Episode


class Show(DomainBase):
    __tablename__ = "shows"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    canonical_title: Mapped[str] = mapped_column(String(500), nullable=False)
    original_title: Mapped[str | None] = mapped_column(String(500))
    first_air_date: Mapped[str | None] = mapped_column(String(20))
    status: Mapped[str | None] = mapped_column(String(100))
    overview: Mapped[str | None] = mapped_column(Text)
    poster_path: Mapped[str | None] = mapped_column(String(500))
    backdrop_path: Mapped[str | None] = mapped_column(String(500))
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())

    seasons: Mapped[list["Season"]] = relationship("Season", back_populates="show", cascade="all, delete-orphan")
    episodes: Mapped[list["Episode"]] = relationship("Episode", back_populates="show", cascade="all, delete-orphan")
