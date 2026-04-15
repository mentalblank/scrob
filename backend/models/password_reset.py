from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, Integer, String, func
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


class PasswordResetToken(Base):
    __tablename__ = "password_reset_tokens"

    id         : Mapped[int]      = mapped_column(Integer, primary_key=True)
    user_id    : Mapped[int]      = mapped_column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    token      : Mapped[str]      = mapped_column(String(64), unique=True, nullable=False)
    created_at : Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), nullable=False)
