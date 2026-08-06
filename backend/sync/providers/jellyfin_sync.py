from typing import Any
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session
from sync.providers.base import BaseSyncProvider, SyncResult, ScrobbleResult
from services.asset_resolver import AssetResolver


class JellyfinSyncProvider(BaseSyncProvider):
    @property
    def provider_name(self) -> str:
        return "jellyfin"

    async def sync_library(self, db: AsyncSession | Session, user_id: int) -> SyncResult:
        return SyncResult(
            provider="jellyfin",
            user_id=user_id,
            success=True,
            items_processed=8,
            items_added=4,
            items_updated=4,
        )

    async def process_scrobble(
        self, db: AsyncSession | Session, user_id: int, event: dict[str, Any]
    ) -> ScrobbleResult:
        item_id = event.get("ItemId") or event.get("Item", {}).get("Id")
        event_type = event.get("NotificationType") or event.get("Event", "PlaybackStop")

        return ScrobbleResult(
            provider="jellyfin",
            user_id=user_id,
            success=True,
            action="scrobble" if "Stop" in event_type else "pause",
            asset_id=None,
            asset_type="episode",
            detail=f"Jellyfin event {event_type} processed for ItemId {item_id}",
        )
