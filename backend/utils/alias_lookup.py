"""Database helpers for resolving aliases between provider IDs and internal PKs."""
from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from models.media_alias import MediaAlias
from utils.media_uri import MediaURI


async def get_internal_id_for_uri(db: AsyncSession, uri_id: str) -> int | None:
    """Return the internal DB PK for a URI string, or None if not found."""
    try:
        uri = MediaURI.parse(uri_id)
    except ValueError:
        return None

    row = await db.execute(
        select(MediaAlias.internal_id).where(
            MediaAlias.provider == uri.provider,
            MediaAlias.external_id == uri.id,
            MediaAlias.media_type == uri.media_type,
        )
    )
    result = row.scalar_one_or_none()
    return result


async def get_provider_id_for_uri(
    db: AsyncSession,
    uri_id: str,
    target_provider: str,
) -> str | None:
    """Translate a URI string to a specific provider's external ID.

    Example: get_provider_id_for_uri(db, 'tmdb:s:1396', 'tvdb') -> '81189'
    Returns None if the alias or the target-provider translation doesn't exist.
    """
    internal_id = await get_internal_id_for_uri(db, uri_id)
    if internal_id is None:
        return None

    try:
        uri = MediaURI.parse(uri_id)
    except ValueError:
        return None

    row = await db.execute(
        select(MediaAlias.external_id).where(
            MediaAlias.internal_id == internal_id,
            MediaAlias.media_type == uri.media_type,
            MediaAlias.provider == target_provider,
        )
    )
    return row.scalar_one_or_none()


async def find_show_by_provider_id(
    db: AsyncSession,
    provider: str,
    external_id: str,
) -> "Show | None":
    """Return the Show ORM row linked to a provider external ID via media_aliases.

    Falls back gracefully if the media_aliases table doesn't exist yet.
    """
    try:
        from models.show import Show
        from models.base import MediaType as MT

        alias_q = await db.execute(
            select(MediaAlias.internal_id).where(
                MediaAlias.provider == provider,
                MediaAlias.external_id == str(external_id),
                MediaAlias.media_type == MT.series,
            )
        )
        internal_id = alias_q.scalar_one_or_none()
        if internal_id is None:
            return None
        show_q = await db.execute(select(Show).where(Show.id == internal_id))
        return show_q.scalar_one_or_none()
    except Exception:
        return None


async def upsert_alias(
    db: AsyncSession,
    internal_id: int,
    media_type: str,
    provider: str,
    external_id: str,
    is_manual: bool = False,
) -> None:
    """Insert an alias if it doesn't already exist. Idempotent."""
    from sqlalchemy.dialects.postgresql import insert as pg_insert
    from models.base import MediaType as MT

    mt = MT(media_type)
    stmt = (
        pg_insert(MediaAlias)
        .values(
            internal_id=internal_id,
            media_type=mt,
            provider=provider,
            external_id=str(external_id),
            is_manual=is_manual,
        )
        .on_conflict_do_nothing(constraint="uq_media_aliases_provider_external_type")
    )
    await db.execute(stmt)
