from datetime import datetime
from typing import Optional
from sqlalchemy import DateTime, Float, ForeignKey, Integer, String, Text, UniqueConstraint, func
from sqlalchemy.orm import Mapped, mapped_column
from models.domain.base import DomainBase


class Rating(DomainBase):
    __tablename__ = "ratings"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    asset_type: Mapped[str] = mapped_column(String(20), default="show", nullable=False)  # 'show', 'season', 'episode', 'movie'
    asset_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    media_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)  # Alias for asset_id
    rating: Mapped[float] = mapped_column(Float, nullable=False)
    review: Mapped[Optional[str]] = mapped_column(Text)
    rated_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), nullable=False)

    __table_args__ = (
        UniqueConstraint("user_id", "asset_type", "asset_id", name="uq_rating_user_asset"),
    )

    def __init__(self, **kwargs):
        if "media_id" in kwargs and "asset_id" not in kwargs:
            kwargs["asset_id"] = kwargs["media_id"]
        elif "asset_id" in kwargs and "media_id" not in kwargs:
            kwargs["media_id"] = kwargs["asset_id"]
        super().__init__(**kwargs)
