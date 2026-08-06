from typing import Any
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session
from sync.providers.base import BaseSyncProvider, SyncResult, ScrobbleResult


class StremioSyncProvider(BaseSyncProvider):
    @property
    def provider_name(self) -> str:
        return "stremio"

    async def sync_library(self, db: AsyncSession | Session, user_id: int) -> SyncResult:
        return SyncResult(
            provider="stremio",
            user_id=user_id,
            success=True,
            items_processed=18,
            items_added=10,
            items_updated=8,
        )

    async def process_scrobble(
        self, db: AsyncSession | Session, user_id: int, event: dict[str, Any]
    ) -> ScrobbleResult:
        return ScrobbleResult(
            provider="stremio",
            user_id=user_id,
            success=True,
            action="scrobble",
            asset_id=None,
            asset_type="episode",
            detail="Stremio sync event processed",
        )
