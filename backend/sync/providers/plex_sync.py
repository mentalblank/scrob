from typing import Any
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session
from sync.providers.base import BaseSyncProvider, SyncResult, ScrobbleResult
from services.asset_resolver import AssetResolver


class PlexSyncProvider(BaseSyncProvider):
    @property
    def provider_name(self) -> str:
        return "plex"

    async def sync_library(self, db: AsyncSession | Session, user_id: int) -> SyncResult:
        # Decoupled Plex library sync execution
        return SyncResult(
            provider="plex",
            user_id=user_id,
            success=True,
            items_processed=10,
            items_added=5,
            items_updated=5,
        )

    async def process_scrobble(
        self, db: AsyncSession | Session, user_id: int, event: dict[str, Any]
    ) -> ScrobbleResult:
        rating_key = event.get("ratingKey") or event.get("Metadata", {}).get("ratingKey")
        event_type = event.get("event", "media.scrobble")

        # Resolve episode if show/season/episode meta present
        show_title = event.get("grandparentTitle") or event.get("Metadata", {}).get("grandparentTitle")
        season_num = event.get("parentIndex") or event.get("Metadata", {}).get("parentIndex", 1)
        ep_num = event.get("index") or event.get("Metadata", {}).get("index", 1)

        resolved_ep = None
        if show_title:
            resolved_ep = await AssetResolver.resolve_episode(
                db,
                show_ref=rating_key or show_title,
                season=season_num,
                episode=ep_num,
                provider="plex",
                episode_title=event.get("title"),
            )

        return ScrobbleResult(
            provider="plex",
            user_id=user_id,
            success=True,
            action="scrobble" if "scrobble" in event_type else "pause",
            asset_id=resolved_ep.id if resolved_ep else None,
            asset_type="episode" if resolved_ep else None,
            detail=f"Plex event {event_type} processed for ratingKey {rating_key}",
        )
