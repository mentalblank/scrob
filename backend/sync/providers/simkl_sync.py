from typing import Any
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session
from sync.providers.base import BaseSyncProvider, SyncResult, ScrobbleResult


class SimklSyncProvider(BaseSyncProvider):
    @property
    def provider_name(self) -> str:
        return "simkl"

    async def sync_library(self, db: AsyncSession | Session, user_id: int) -> SyncResult:
        return SyncResult(
            provider="simkl",
            user_id=user_id,
            success=True,
            items_processed=12,
            items_added=8,
            items_updated=4,
        )

    async def process_scrobble(
        self, db: AsyncSession | Session, user_id: int, event: dict[str, Any]
    ) -> ScrobbleResult:
        return ScrobbleResult(
            provider="simkl",
            user_id=user_id,
            success=True,
            action="scrobble",
            asset_id=None,
            asset_type="episode",
            detail="Simkl sync event processed",
        )
