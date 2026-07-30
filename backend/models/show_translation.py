from datetime import datetime
from sqlalchemy import DateTime, ForeignKey, Integer, String, Text, UniqueConstraint, func
from sqlalchemy.orm import Mapped, mapped_column
from .base import Base


class ShowTranslation(Base):
    __tablename__ = "show_translations"
    __table_args__ = (UniqueConstraint("show_id", "language", name="uq_show_translations_show_language"),)

    id          : Mapped[int]           = mapped_column(Integer, primary_key=True)
    show_id     : Mapped[int]           = mapped_column(Integer, ForeignKey("shows.id", ondelete="CASCADE"), nullable=False, index=True)
    language    : Mapped[str]           = mapped_column(String(10), nullable=False)
    title       : Mapped[str | None]    = mapped_column(String(500))
    overview    : Mapped[str | None]    = mapped_column(Text)
    tagline     : Mapped[str | None]    = mapped_column(Text)
    poster_path : Mapped[str | None]    = mapped_column(String(500))
    fetched_at  : Mapped[datetime]      = mapped_column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)
