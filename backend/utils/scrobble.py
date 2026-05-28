from datetime import datetime

def should_track_scrobble(
    media_id: int,
    watched_at: datetime | None,
    existing_watched_keys: set[tuple[int, int]],
    existing_watched_media_ids: set[int]
) -> bool:
    """Determine if a scrobble/watch event should be recorded, updating tracking sets."""
    if watched_at:
        k = (media_id, int(watched_at.timestamp()))
        if k not in existing_watched_keys:
            existing_watched_keys.add(k)
            existing_watched_media_ids.add(media_id)
            return True
    else:
        if media_id not in existing_watched_media_ids:
            existing_watched_media_ids.add(media_id)
            return True
    return False
