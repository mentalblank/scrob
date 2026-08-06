from datetime import datetime
from typing import Optional
from sqlalchemy import DateTime, ForeignKey, Index, Integer, String, UniqueConstraint, func
from sqlalchemy.orm import Mapped, mapped_column, relationship
from models.domain.base import DomainBase


class Collection(DomainBase):
    __tablename__ = "collections"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    asset_type: Mapped[str] = mapped_column(String(20), default="show", nullable=False)  # 'show', 'movie'
    asset_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    media_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)  # Alias for asset_id
    added_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), nullable=False)

    files: Mapped[list["CollectionFile"]] = relationship(
        back_populates="collection", cascade="all, delete-orphan"
    )

    __table_args__ = (
        UniqueConstraint("user_id", "asset_type", "asset_id", name="uq_collection_user_asset"),
    )

    def __init__(self, **kwargs):
        if "media_id" in kwargs and "asset_id" not in kwargs:
            kwargs["asset_id"] = kwargs["media_id"]
        elif "asset_id" in kwargs and "media_id" not in kwargs:
            kwargs["media_id"] = kwargs["asset_id"]
        super().__init__(**kwargs)


class CollectionFile(DomainBase):
    __tablename__ = "collection_files"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    collection_id: Mapped[int] = mapped_column(ForeignKey("collections.id", ondelete="CASCADE"), nullable=False, index=True)
    source: Mapped[str] = mapped_column(String(50), default="manual", nullable=False)  # 'plex', 'jellyfin', 'manual'
    source_id: Mapped[Optional[str]] = mapped_column(String(255))
    file_path: Mapped[Optional[str]] = mapped_column(String(1000))
    resolution: Mapped[Optional[str]] = mapped_column(String(50))
    video_codec: Mapped[Optional[str]] = mapped_column(String(50))
    audio_codec: Mapped[Optional[str]] = mapped_column(String(50))
    added_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), nullable=False)

    collection: Mapped["Collection"] = relationship(back_populates="files")
