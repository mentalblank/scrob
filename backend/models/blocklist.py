from datetime import datetime
from typing import Optional

from sqlalchemy import ForeignKey, Integer, String, Enum, DateTime, func, Index, Boolean, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .base import Base, MediaType


class BlocklistItem(Base):
    __tablename__ = "blocklist_items"
    __table_args__ = (
        UniqueConstraint("user_id", "uri_id", name="uq_blocklist_user_uri"),
    )

    id         : Mapped[int]            = mapped_column(Integer, primary_key=True)
    user_id    : Mapped[int]            = mapped_column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    uri_id     : Mapped[str]            = mapped_column(String(50), nullable=False, index=True)
    media_type : Mapped[MediaType]      = mapped_column(Enum(MediaType), nullable=False)
    is_dropped : Mapped[bool]           = mapped_column(Boolean, server_default='0', nullable=False)
    created_at : Mapped[datetime]       = mapped_column(DateTime, server_default=func.now(), nullable=False)

    user : Mapped["User"] = relationship()
