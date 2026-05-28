from datetime import datetime

from sqlalchemy import Boolean, DateTime, Enum, Index, Integer, String, UniqueConstraint, func
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base, MediaType


class MediaAlias(Base):
    __tablename__ = "media_aliases"
    __table_args__ = (
        UniqueConstraint("provider", "external_id", "media_type", name="uq_media_aliases_provider_external_type"),
        Index("idx_media_aliases_internal_type", "internal_id", "media_type"),
    )

    id          : Mapped[int]       = mapped_column(Integer, primary_key=True)
    internal_id : Mapped[int]       = mapped_column(Integer, nullable=False)
    media_type  : Mapped[MediaType] = mapped_column(Enum(MediaType), nullable=False)
    provider    : Mapped[str]       = mapped_column(String(50), nullable=False)
    external_id : Mapped[str]       = mapped_column(String(100), nullable=False)
    is_manual   : Mapped[bool]      = mapped_column(Boolean, server_default="false", nullable=False)
    created_at  : Mapped[datetime]  = mapped_column(DateTime, server_default=func.now(), nullable=False)
