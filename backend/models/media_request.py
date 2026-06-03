import enum
from datetime import datetime
from typing import Optional

from sqlalchemy import DateTime, Enum, Integer, String, ForeignKey, func
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


class RequestStatus(str, enum.Enum):
    pending  = "pending"
    approved = "approved"
    rejected = "rejected"


class MediaRequest(Base):
    __tablename__ = "media_requests"

    id          : Mapped[int]            = mapped_column(Integer, primary_key=True)
    user_id     : Mapped[int]            = mapped_column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    uri_id      : Mapped[str]            = mapped_column(String(50), nullable=False, index=True)
    media_type  : Mapped[str]            = mapped_column(String(10), nullable=False)  # "movie" | "series"
    title       : Mapped[str]            = mapped_column(String(500), nullable=False, server_default="")
    poster_path : Mapped[Optional[str]]  = mapped_column(String(500))
    status      : Mapped[RequestStatus]  = mapped_column(Enum(RequestStatus), nullable=False, default=RequestStatus.pending)
    season_number: Mapped[Optional[int]]  = mapped_column(Integer, nullable=True)
    episode_number: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    reviewed_by : Mapped[Optional[int]]  = mapped_column(Integer, ForeignKey("users.id", ondelete="SET NULL"), nullable=True)
    created_at  : Mapped[datetime]       = mapped_column(DateTime, server_default=func.now(), nullable=False)
    updated_at  : Mapped[datetime]       = mapped_column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)
