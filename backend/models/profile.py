from typing import Optional

from sqlalchemy import ForeignKey, Integer, JSON, String, Enum as SQLEnum
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .base import Base, PrivacyLevel


class UserProfileData(Base):
    __tablename__ = "user_profiles"

    id                  : Mapped[int]                  = mapped_column(Integer, primary_key=True)
    user_id             : Mapped[int]                  = mapped_column(Integer, ForeignKey("users.id", ondelete="CASCADE"), unique=True, nullable=False, index=True)
    display_name        : Mapped[Optional[str]]        = mapped_column(String(64))
    bio                 : Mapped[Optional[str]]        = mapped_column(String(280))
    country             : Mapped[Optional[str]]        = mapped_column(String(2))
    movie_genres        : Mapped[Optional[list[str]]]  = mapped_column(JSON)
    show_genres         : Mapped[Optional[list[str]]]  = mapped_column(JSON)
    streaming_services  : Mapped[Optional[list[str]]]  = mapped_column(JSON)
    content_language    : Mapped[Optional[str]]        = mapped_column(String(10))
    privacy_level       : Mapped[PrivacyLevel]         = mapped_column(SQLEnum(PrivacyLevel), default=PrivacyLevel.private, nullable=False, server_default=PrivacyLevel.private.value)
    avatar_path         : Mapped[Optional[str]]        = mapped_column(String(255))
    pagination_type     : Mapped[str]                  = mapped_column(String(20), default="infinite_scroll", server_default="infinite_scroll")

    user: Mapped["User"] = relationship(back_populates="profile")
