from datetime import datetime
from typing import Optional

from sqlalchemy import DateTime, Float, Integer, String, Text, event, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import insert as _pg_insert
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .base import Base, MediaType
from .media_alias import MediaAlias


class Show(Base):
    __tablename__ = "shows"

    id                   : Mapped[int]             = mapped_column(Integer, primary_key=True)
    tmdb_id              : Mapped[Optional[int]]   = mapped_column(Integer, unique=True, nullable=True)
    tvdb_id              : Mapped[Optional[int]]   = mapped_column(Integer, unique=True, nullable=True)
    uri_id               : Mapped[Optional[str]]   = mapped_column(String(50), nullable=True, index=True)
    title                : Mapped[str]             = mapped_column(String(500), nullable=False)
    original_title       : Mapped[Optional[str]]   = mapped_column(String(500))
    overview             : Mapped[Optional[str]]   = mapped_column(Text)
    poster_path          : Mapped[Optional[str]]   = mapped_column(String(500))
    backdrop_path        : Mapped[Optional[str]]   = mapped_column(String(500))
    tmdb_rating          : Mapped[Optional[float]] = mapped_column(Float)
    status               : Mapped[Optional[str]]   = mapped_column(String(100))
    tagline              : Mapped[Optional[str]]   = mapped_column(Text)
    first_air_date       : Mapped[Optional[str]]   = mapped_column(String(20))
    last_air_date        : Mapped[Optional[str]]   = mapped_column(String(20))
    tmdb_data            : Mapped[Optional[dict]]  = mapped_column(JSONB)
    # User-defined rename overrides (propagated everywhere in the UI)
    custom_title         : Mapped[Optional[str]]   = mapped_column(String(500))
    custom_season_names  : Mapped[Optional[dict]]  = mapped_column(JSONB)  # {season_number: custom_name}
    created_at           : Mapped[datetime]        = mapped_column(DateTime, server_default=func.now(), nullable=False)
    updated_at           : Mapped[datetime]        = mapped_column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)

    episodes : Mapped[list["Media"]] = relationship(back_populates="show")


def _seed_show_aliases(mapper, connection, target: "Show") -> None:
    rows = []
    if target.tvdb_id:
        rows.append({"internal_id": target.id, "media_type": MediaType.series,
                     "provider": "tvdb", "external_id": str(target.tvdb_id)})
    if target.tmdb_id:
        rows.append({"internal_id": target.id, "media_type": MediaType.series,
                     "provider": "tmdb", "external_id": str(target.tmdb_id)})
    for r in rows:
        connection.execute(
            _pg_insert(MediaAlias.__table__)
            .values(**r)
            .on_conflict_do_nothing(constraint="uq_media_aliases_provider_external_type")
        )


event.listen(Show, "after_insert", _seed_show_aliases)