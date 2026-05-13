from datetime import datetime
from typing import Optional

from sqlalchemy import ForeignKey, Integer, String, Enum, DateTime, func, Index, Boolean
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .base import Base, MediaType

class BlocklistItem(Base):
    __tablename__ = "blocklist_items"
    __table_args__ = (
        Index("idx_blocklist_user_tmdb_type", "user_id", "tmdb_id", "media_type", unique=True),
    )

    id         : Mapped[int]       = mapped_column(Integer, primary_key=True)
    user_id    : Mapped[int]       = mapped_column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    tmdb_id    : Mapped[int]       = mapped_column(Integer, nullable=False)
    media_type : Mapped[MediaType] = mapped_column(Enum(MediaType), nullable=False)
    is_dropped : Mapped[bool]      = mapped_column(Boolean, server_default='0', nullable=False)
    created_at : Mapped[datetime]  = mapped_column(DateTime, server_default=func.now(), nullable=False)

    user : Mapped["User"] = relationship()
