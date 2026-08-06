from datetime import datetime
from sqlalchemy import DateTime, Index, Integer, String, UniqueConstraint, func
from sqlalchemy.orm import Mapped, mapped_column
from models.domain.base import DomainBase


class ExternalID(DomainBase):
    __tablename__ = "external_ids"
    __table_args__ = (
        UniqueConstraint("provider", "external_id", "asset_type", name="uq_external_ids_provider_ext_type"),
        Index("idx_ext_ids_provider_lookup", "provider", "external_id"),
        Index("idx_ext_ids_asset_lookup", "asset_type", "asset_id"),
        Index("idx_ext_ids_tvdb_position", "asset_id", "provider", "provider_season", "provider_episode"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    asset_type: Mapped[str] = mapped_column(String(20), nullable=False)  # 'show', 'season', 'episode', 'movie'
    asset_id: Mapped[int] = mapped_column(Integer, nullable=False)
    provider: Mapped[str] = mapped_column(String(50), nullable=False)  # 'tmdb', 'tvdb', 'imdb', 'trakt', 'anidb'
    external_id: Mapped[str] = mapped_column(String(100), nullable=False)
    provider_season: Mapped[int | None] = mapped_column(Integer)
    provider_episode: Mapped[int | None] = mapped_column(Integer)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
