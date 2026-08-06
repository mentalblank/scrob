from datetime import datetime
from enum import Enum as PyEnum
from typing import Optional
from sqlalchemy import DateTime, ForeignKey, Integer, String, Text, UniqueConstraint, func
from sqlalchemy import Enum as SQLEnum
from sqlalchemy.orm import Mapped, mapped_column, relationship
from models.domain.base import DomainBase


class PrivacyLevel(str, PyEnum):
    public = "public"
    private = "private"
    unlisted = "unlisted"


class List(DomainBase):
    __tablename__ = "lists"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text)
    privacy_level: Mapped[PrivacyLevel] = mapped_column(
        SQLEnum(PrivacyLevel, native_enum=False),
        default=PrivacyLevel.public,
        server_default="public",
        nullable=False,
    )
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now(), onupdate=func.now(), nullable=False
    )

    items: Mapped[list["ListItem"]] = relationship(
        back_populates="list", cascade="all, delete-orphan"
    )


class ListItem(DomainBase):
    __tablename__ = "list_items"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    list_id: Mapped[int] = mapped_column(ForeignKey("lists.id", ondelete="CASCADE"), nullable=False, index=True)
    asset_type: Mapped[str] = mapped_column(String(20), default="show", nullable=False)  # 'show', 'movie'
    asset_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    media_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)  # Alias for asset_id
    sort_order: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    notes: Mapped[Optional[str]] = mapped_column(Text)
    added_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), nullable=False)

    list: Mapped["List"] = relationship(back_populates="items")

    __table_args__ = (
        UniqueConstraint("list_id", "asset_type", "asset_id", name="uq_list_item_asset"),
    )

    def __init__(self, **kwargs):
        if "media_id" in kwargs and "asset_id" not in kwargs:
            kwargs["asset_id"] = kwargs["media_id"]
        elif "asset_id" in kwargs and "media_id" not in kwargs:
            kwargs["media_id"] = kwargs["asset_id"]
        super().__init__(**kwargs)
