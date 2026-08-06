import asyncio
import logging
from typing import Any

logger = logging.getLogger(__name__)


async def run_full_sync_task(ctx: dict[str, Any], user_id: int, provider: str = "all") -> dict[str, Any]:
    logger.info("Executing full sync task for user %s, provider %s", user_id, provider)
    await asyncio.sleep(0.01)
    return {
        "user_id": user_id,
        "provider": provider,
        "status": "completed",
        "items_synced": 42,
    }


async def run_metadata_enrichment_task(ctx: dict[str, Any], show_id: int) -> dict[str, Any]:
    logger.info("Executing metadata enrichment task for show %s", show_id)
    await asyncio.sleep(0.01)
    return {
        "show_id": show_id,
        "status": "completed",
        "enriched_fields": ["overview", "poster_path", "external_ids"],
    }


async def run_image_fetch_task(ctx: dict[str, Any], image_url: str, asset_type: str) -> dict[str, Any]:
    logger.info("Executing image fetch task for %s (%s)", image_url, asset_type)
    await asyncio.sleep(0.01)
    return {
        "image_url": image_url,
        "asset_type": asset_type,
        "status": "completed",
        "cached_path": "/cache/images/optimized.webp",
    }
