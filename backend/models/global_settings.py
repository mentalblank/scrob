from typing import Optional

from sqlalchemy import Boolean, Float, Integer, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


class GlobalSettings(Base):
    __tablename__ = "global_settings"

    id                     : Mapped[int]           = mapped_column(Integer, primary_key=True)
    tmdb_api_key           : Mapped[Optional[str]] = mapped_column(String(255))
    radarr_url             : Mapped[Optional[str]] = mapped_column(String(500))
    radarr_token           : Mapped[Optional[str]] = mapped_column(String(500))
    radarr_root_folder     : Mapped[Optional[str]] = mapped_column(String(500))
    radarr_quality_profile : Mapped[Optional[int]] = mapped_column(Integer)
    radarr_tags            : Mapped[Optional[list]] = mapped_column(JSONB)
    sonarr_url             : Mapped[Optional[str]] = mapped_column(String(500))
    sonarr_token           : Mapped[Optional[str]] = mapped_column(String(500))
    sonarr_root_folder     : Mapped[Optional[str]] = mapped_column(String(500))
    sonarr_quality_profile : Mapped[Optional[int]] = mapped_column(Integer)
    sonarr_tags            : Mapped[Optional[list]] = mapped_column(JSONB)
    sonarr_season_folder         : Mapped[bool]          = mapped_column(Boolean, nullable=False, server_default="true")
    radarr_require_approval      : Mapped[bool]          = mapped_column(Boolean, nullable=False, server_default="false")
    sonarr_require_approval      : Mapped[bool]          = mapped_column(Boolean, nullable=False, server_default="false")
    radarr_customize_on_add      : Mapped[bool]          = mapped_column(Boolean, nullable=False, server_default="false")
    sonarr_customize_on_add      : Mapped[bool]          = mapped_column(Boolean, nullable=False, server_default="false")
    tvdb_api_key                 : Mapped[Optional[str]] = mapped_column(String(255))
    tvdb_subscriber_pin          : Mapped[Optional[str]] = mapped_column(String(255))
    image_cache_enabled          : Mapped[bool]          = mapped_column(Boolean, nullable=False, server_default="false")
    image_cache_limit_gb         : Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    # Days before a cached image is evicted regardless of size limit. NULL/0 = never expire.
    image_cache_expiry_days      : Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    # NULL = fall back to the ENABLE_REGISTRATIONS / REGISTRATION_MAX_ALLOWED_USERS env vars.
    enable_registrations         : Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)
    registration_max_allowed_users: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    # Admin setup wizard completed. New rows default False (needs setup);
    # existing rows backfill True via server_default so current installs aren't interrupted.
    setup_completed               : Mapped[bool]          = mapped_column(Boolean, nullable=False, default=False, server_default="true")

    # Restored: the merge dropped these three from the model while leaving the
    # columns in the database and their readers in the code.
    enable_logged_out_navigation : Mapped[bool]          = mapped_column(Boolean, nullable=False, server_default="false")
    disable_comments             : Mapped[bool]          = mapped_column(Boolean, nullable=False, server_default="false")
    disable_user_ratings         : Mapped[bool]          = mapped_column(Boolean, nullable=False, server_default="false")
    plex_client_identifier       : Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
