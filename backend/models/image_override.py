from datetime import datetime
from typing import Optional

from sqlalchemy import DateTime, ForeignKey, Index, Integer, String, Text, UniqueConstraint, func
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base

# Season/episode are part of the uniqueness key, and Postgres treats NULLs as
# distinct, so "no season" is a sentinel rather than NULL - otherwise a show
# could collect unlimited duplicate show-level rows.
NO_NUMBER = -1

IMAGE_KINDS = {"poster", "backdrop", "still"}
IMAGE_SOURCES = {"tmdb", "tvdb", "external"}


class MediaImageOverride(Base):
    """One user's replacement artwork for a show, season, episode, or movie."""

    __tablename__ = "media_image_overrides"
    __table_args__ = (
        UniqueConstraint(
            "user_id", "subject_uri", "season_number", "episode_number", "image_kind",
            name="uq_media_image_override",
        ),
        Index("idx_media_image_overrides_user", "user_id"),
    )

    id             : Mapped[int]           = mapped_column(Integer, primary_key=True)
    user_id        : Mapped[int]           = mapped_column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    # Provider identity of the show (for show/season/episode) or the movie.
    subject_uri    : Mapped[str]           = mapped_column(String(64), nullable=False)
    # Set when the subject resolves to a local row, so the loader can expand the
    # override across both of the show's provider ids.
    show_id        : Mapped[Optional[int]] = mapped_column(Integer, ForeignKey("shows.id", ondelete="CASCADE"), nullable=True)
    media_id       : Mapped[Optional[int]] = mapped_column(Integer, ForeignKey("media.id", ondelete="CASCADE"), nullable=True)
    season_number  : Mapped[int]           = mapped_column(Integer, nullable=False, server_default=str(NO_NUMBER))
    episode_number : Mapped[int]           = mapped_column(Integer, nullable=False, server_default=str(NO_NUMBER))
    image_kind     : Mapped[str]           = mapped_column(String(16), nullable=False)
    # Cache-key path: a TMDB path, a TVDB artwork path, or "/<sha>.<ext>" for an
    # external image already downloaded into the "ext" cache bucket.
    image_path     : Mapped[str]           = mapped_column(String(500), nullable=False)
    source         : Mapped[str]           = mapped_column(String(16), nullable=False)
    # Original URL for an external image, kept so the picker can show its origin.
    source_url     : Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at     : Mapped[datetime]      = mapped_column(DateTime, server_default=func.now(), nullable=False)
    updated_at     : Mapped[datetime]      = mapped_column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)
