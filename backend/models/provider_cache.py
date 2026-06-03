from datetime import datetime

from sqlalchemy import DateTime, Index, String, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


class ProviderCache(Base):
    """TTL cache for external provider responses (TMDB / TVDB / Skyhook).

    Keyed by a hash of provider + endpoint + params. Cuts repeat API calls
    for detail/season/episode/image fetches that change rarely.
    """
    __tablename__ = "provider_cache"
    __table_args__ = (
        Index("idx_provider_cache_expires", "expires_at"),
    )

    cache_key  : Mapped[str]      = mapped_column(String(64), primary_key=True)
    value      : Mapped[dict]     = mapped_column(JSONB, nullable=False)
    expires_at : Mapped[datetime] = mapped_column(DateTime, nullable=False)
    created_at : Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), nullable=False)
