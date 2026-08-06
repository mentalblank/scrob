from abc import ABC, abstractmethod
from typing import Any
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session


class SyncResult(BaseModel):
    provider: str
    user_id: int
    success: bool
    items_processed: int = 0
    items_added: int = 0
    items_updated: int = 0
    errors: list[str] = Field(default_factory=list)


class ScrobbleResult(BaseModel):
    provider: str
    user_id: int
    success: bool
    action: str  # 'scrobble', 'pause', 'stop'
    asset_id: int | None = None
    asset_type: str | None = None  # 'episode', 'movie'
    detail: str | None = None


class BaseSyncProvider(ABC):
    @property
    @abstractmethod
    def provider_name(self) -> str:
        pass

    @abstractmethod
    async def sync_library(self, db: AsyncSession | Session, user_id: int) -> SyncResult:
        pass

    @abstractmethod
    async def process_scrobble(
        self, db: AsyncSession | Session, user_id: int, event: dict[str, Any]
    ) -> ScrobbleResult:
        pass
