"""Database helpers for resolving aliases between provider IDs and internal PKs."""
from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from models.media_alias import MediaAlias
from utils.media_uri import MediaURI


async def get_internal_id_for_uri(db: AsyncSession, uri_id: str) -> int | None:
    """Return the internal DB PK for a URI string, or None if not found.

    media_aliases is consulted first, then the uri_id stored on the row itself,
    then the provider id columns. Only the alias table was checked before, and
    it holds a handful of rows, so almost every lookup missed.
    """
    try:
        uri = MediaURI.parse(uri_id)
    except ValueError:
        return None

    from models.base import MediaType
    from models.media import Media
    from models.show import Show

    row = await db.execute(
        select(MediaAlias.internal_id).where(
            MediaAlias.provider == uri.provider,
            MediaAlias.external_id == uri.id,
            MediaAlias.media_type == uri.media_type,
        )
    )
    result = row.scalars().first()
    if result is not None:
        return result

    model = Show if uri.media_type == MediaType.series else Media
    row = await db.execute(select(model.id).where(model.uri_id == uri_id))
    result = row.scalars().first()
    if result is not None:
        return result

    try:
        provider_id = int(uri.id)
    except (TypeError, ValueError):
        return None

    if uri.provider == "tmdb":
        column = model.tmdb_id
    elif uri.provider == "tvdb" and model is Show:
        column = Show.tvdb_id
    else:
        return None

    conditions = [column == provider_id]
    if model is Media:
        conditions.append(Media.media_type == uri.media_type)
    row = await db.execute(select(model.id).where(*conditions))
    return row.scalars().first()


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
    alias = row.scalars().first()
    if alias is not None:
        return alias

    # Shows keep both provider ids on the row, so a missing alias is not a
    # missing link.
    from models.base import MediaType
    from models.show import Show

    if uri.media_type != MediaType.series:
        return None
    show_q = await db.execute(select(Show).where(Show.id == internal_id))
    show = show_q.scalars().first()
    if show is None:
        return None
    value = show.tvdb_id if target_provider == "tvdb" else show.tmdb_id
    return str(value) if value else None


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
        internal_id = alias_q.scalars().first()
        if internal_id is not None:
            show_q = await db.execute(select(Show).where(Show.id == internal_id))
            return show_q.scalars().first()

        # No alias row — fall back to the ids stored on the show itself.
        show_q = await db.execute(select(Show).where(Show.uri_id == f"{provider}:s:{external_id}"))
        show = show_q.scalars().first()
        if show is not None:
            return show
        try:
            provider_id = int(external_id)
        except (TypeError, ValueError):
            return None
        column = Show.tvdb_id if provider == "tvdb" else Show.tmdb_id
        show_q = await db.execute(select(Show).where(column == provider_id))
        return show_q.scalars().first()
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


async def record_provider_ids(
    db: AsyncSession,
    internal_id: int,
    media_type: str,
    *,
    tmdb_id: int | str | None = None,
    tvdb_id: int | str | None = None,
    imdb_id: str | None = None,
) -> None:
    """Record every provider id known for a row, whatever the source gave us.

    Identity is not metadata: a title keeps its ids even when enrichment is off,
    so nothing has to be re-resolved later to reconcile it across providers.
    """
    # IMDb ids are stored numeric — MediaURI ids are digits only.
    imdb_numeric = str(imdb_id).strip().lower().lstrip("t") if imdb_id else None
    for provider, external_id in (("tmdb", tmdb_id), ("tvdb", tvdb_id), ("imdb", imdb_numeric)):
        if external_id in (None, ""):
            continue
        await upsert_alias(db, internal_id, media_type, provider, str(external_id))


async def record_provider_ids_bulk(
    db: AsyncSession,
    entries: list[tuple[int, str, dict]],
) -> None:
    """Record provider ids for many rows in one statement per chunk.

    A library sync resolves tens of thousands of episodes, and a round trip per
    id took longer than the metadata fetches it followed.
    """
    from sqlalchemy.dialects.postgresql import insert as pg_insert

    from models.base import MediaType as MT

    rows: list[dict] = []
    seen: set[tuple[str, str, str]] = set()
    for internal_id, media_type, ids in entries:
        imdb_id = ids.get("imdb_id")
        imdb_numeric = str(imdb_id).strip().lower().lstrip("t") if imdb_id else None
        for provider, external_id in (
            ("tmdb", ids.get("tmdb_id")),
            ("tvdb", ids.get("tvdb_id")),
            ("imdb", imdb_numeric),
        ):
            if external_id in (None, ""):
                continue
            # The unique constraint is on (provider, external_id, media_type), so
            # a chunk carrying the same pair twice would abort the statement.
            key = (provider, str(external_id), media_type)
            if key in seen:
                continue
            seen.add(key)
            rows.append({
                "internal_id": internal_id,
                "media_type": MT(media_type),
                "provider": provider,
                "external_id": str(external_id),
                "is_manual": False,
            })

    for i in range(0, len(rows), 1000):
        stmt = (
            pg_insert(MediaAlias)
            .values(rows[i : i + 1000])
            .on_conflict_do_nothing(constraint="uq_media_aliases_provider_external_type")
        )
        await db.execute(stmt)


async def link_show_provider_ids(
    db: AsyncSession,
    show,
    *,
    external_ids: dict | None = None,
    tvdb_id: int | str | None = None,
    imdb_id: str | None = None,
) -> None:
    """Set a Show's tvdb_id from whatever the caller already resolved, and record
    every provider id it now has as an alias.

    A show should not end up holding only a TMDB id because of which door it
    came through — a Trakt import, a Simkl import, a webhook and a library sync
    all have TMDB's external_ids in hand at the point they build the row. Safe
    to call on an existing row: it fills a missing tvdb_id and never overwrites
    one already set, and never steals a tvdb_id another show has claimed, since
    the column is unique.
    """
    from models.show import Show

    external_ids = external_ids or {}
    tvdb_id = tvdb_id or external_ids.get("tvdb_id")
    imdb_id = imdb_id or external_ids.get("imdb_id")

    if tvdb_id and not show.tvdb_id:
        try:
            tvdb_numeric = int(tvdb_id)
        except (TypeError, ValueError):
            tvdb_numeric = None
        if tvdb_numeric:
            taken = await db.execute(select(Show.id).where(Show.tvdb_id == tvdb_numeric))
            if not taken.scalars().first():
                show.tvdb_id = tvdb_numeric

    if show.id is None:
        await db.flush()

    await record_provider_ids(
        db,
        show.id,
        "series",
        tmdb_id=show.tmdb_id,
        tvdb_id=show.tvdb_id or tvdb_id,
        imdb_id=imdb_id,
    )
