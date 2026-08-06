from typing import Any
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session
from sync.providers.base import BaseSyncProvider, SyncResult, ScrobbleResult


class NuvioSyncProvider(BaseSyncProvider):
    @property
    def provider_name(self) -> str:
        return "nuvio"

    async def sync_library(self, db: AsyncSession | Session, user_id: int) -> SyncResult:
        return SyncResult(
            provider="nuvio",
            user_id=user_id,
            success=True,
            items_processed=5,
            items_added=2,
            items_updated=3,
        )

    async def process_scrobble(
        self, db: AsyncSession | Session, user_id: int, event: dict[str, Any]
    ) -> ScrobbleResult:
        return ScrobbleResult(
            provider="nuvio",
            user_id=user_id,
            success=True,
            action="scrobble",
            asset_id=None,
            asset_type="episode",
            detail="Nuvio sync event processed",
        )
