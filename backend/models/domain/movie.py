from datetime import datetime
from typing import Optional
from sqlalchemy import DateTime, Integer, String, Text, func
from sqlalchemy.orm import Mapped, mapped_column
from models.domain.base import DomainBase


class Movie(DomainBase):
    __tablename__ = "movies"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    canonical_title: Mapped[str] = mapped_column(String(500), nullable=False, index=True)
    original_title: Mapped[Optional[str]] = mapped_column(String(500))
    release_date: Mapped[Optional[str]] = mapped_column(String(20))
    overview: Mapped[Optional[str]] = mapped_column(Text)
    poster_path: Mapped[Optional[str]] = mapped_column(String(500))
    backdrop_path: Mapped[Optional[str]] = mapped_column(String(500))
    runtime: Mapped[Optional[int]] = mapped_column(Integer)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), nullable=False)
