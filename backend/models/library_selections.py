from sqlalchemy import Integer, String, ForeignKey, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


class JellyfinLibrarySelection(Base):
    __tablename__ = "jellyfin_library_selections"
    __table_args__ = (UniqueConstraint("user_id", "library_id"),)

    id           : Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id      : Mapped[int] = mapped_column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    library_id   : Mapped[str] = mapped_column(String(255), nullable=False)
    library_name : Mapped[str] = mapped_column(String(500), nullable=False)


class EmbyLibrarySelection(Base):
    __tablename__ = "emby_library_selections"
    __table_args__ = (UniqueConstraint("user_id", "library_id"),)

    id           : Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id      : Mapped[int] = mapped_column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    library_id   : Mapped[str] = mapped_column(String(255), nullable=False)
    library_name : Mapped[str] = mapped_column(String(500), nullable=False)


class PlexLibrarySelection(Base):
    __tablename__ = "plex_library_selections"
    __table_args__ = (UniqueConstraint("user_id", "library_key"),)

    id           : Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id      : Mapped[int] = mapped_column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    library_key  : Mapped[str] = mapped_column(String(255), nullable=False)
    library_name : Mapped[str] = mapped_column(String(500), nullable=False)
