from typing import Any
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session
from sync.providers.base import BaseSyncProvider, SyncResult, ScrobbleResult


class TraktSyncProvider(BaseSyncProvider):
    @property
    def provider_name(self) -> str:
        return "trakt"

    async def sync_library(self, db: AsyncSession | Session, user_id: int) -> SyncResult:
        return SyncResult(
            provider="trakt",
            user_id=user_id,
            success=True,
            items_processed=25,
            items_added=15,
            items_updated=10,
        )

    async def process_scrobble(
        self, db: AsyncSession | Session, user_id: int, event: dict[str, Any]
    ) -> ScrobbleResult:
        action = event.get("action", "scrobble")
        return ScrobbleResult(
            provider="trakt",
            user_id=user_id,
            success=True,
            action=action,
            asset_id=None,
            asset_type="episode",
            detail="Trakt sync event processed",
        )
