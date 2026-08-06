from datetime import datetime
from sqlalchemy import DateTime, Integer, String, Text, UniqueConstraint, func
from sqlalchemy.orm import Mapped, mapped_column
from models.domain.base import DomainBase


class UserOverride(DomainBase):
    __tablename__ = "user_metadata_overrides"
    __table_args__ = (
        UniqueConstraint("user_id", "asset_type", "asset_id", name="uq_user_override_asset"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(Integer, nullable=False)
    asset_type: Mapped[str] = mapped_column(String(20), nullable=False)  # 'show', 'season', 'episode', 'movie'
    asset_id: Mapped[int] = mapped_column(Integer, nullable=False)
    custom_title: Mapped[str | None] = mapped_column(String(500))
    custom_overview: Mapped[str | None] = mapped_column(Text)
    custom_poster_path: Mapped[str | None] = mapped_column(String(500))
    notes: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
