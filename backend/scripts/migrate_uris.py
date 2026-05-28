"""Idempotent URI migration script."""
from __future__ import annotations

import asyncio
import sys
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

DRY_RUN = "--dry-run" in sys.argv


async def _run(db: "AsyncSession") -> None:
    from sqlalchemy import select, update
    from sqlalchemy.dialects.postgresql import insert as pg_insert

    from models.show import Show
    from models.media import Media
    from models.media_alias import MediaAlias
    from models.base import MediaType

    shows_updated = 0
    media_updated = 0
    aliases_upserted = 0
    collision_shows: list[int] = []

    # ── 1. Shows ────────────────────────────────────────────────────────────────
    result = await db.execute(select(Show).where(Show.uri_id.is_(None)))
    shows = result.scalars().all()

    for show in shows:
        if show.tmdb_id is not None:
            uri = f"tmdb:s:{show.tmdb_id}"
        elif show.tvdb_id is not None:
            uri = f"tvdb:s:{show.tvdb_id}"
        else:
            print(f"  SKIP show id={show.id} title={show.title!r} — no provider ID")
            continue

        # Collision check: another show already has this uri_id
        existing = await db.execute(
            select(Show.id).where(Show.uri_id == uri, Show.id != show.id)
        )
        if existing.scalar_one_or_none() is not None:
            collision_shows.append(show.id)
            print(f"  COLLISION show id={show.id} uri={uri} — skipping")
            continue

        if not DRY_RUN:
            show.uri_id = uri
        shows_updated += 1

    # ── 2. Media ────────────────────────────────────────────────────────────────
    media_result = await db.execute(select(Media).where(Media.uri_id.is_(None), Media.tmdb_id.isnot(None)))
    medias = media_result.scalars().all()

    TYPE_PREFIX = {
        MediaType.movie: "m",
        MediaType.series: "s",
        MediaType.episode: "e",
        MediaType.season: "s",
        MediaType.person: "p",
    }
    for m in medias:
        pfx = TYPE_PREFIX.get(m.media_type, "x")
        uri = f"tmdb:{pfx}:{m.tmdb_id}"
        if not DRY_RUN:
            m.uri_id = uri
        media_updated += 1

    # ── 3. media_aliases — seed from Show external IDs ─────────────────────────
    all_shows_result = await db.execute(
        select(Show).where(Show.id.isnot(None))
    )
    all_shows = all_shows_result.scalars().all()

    def _alias_vals(internal_id: int, provider: str, ext_id: str | int, mtype: str = "series") -> dict:
        return {
            "internal_id": internal_id,
            "media_type": mtype,
            "provider": provider,
            "external_id": str(ext_id),
            "is_manual": False,
        }

    async def _upsert(v: dict) -> None:
        stmt = pg_insert(MediaAlias).values(**v).on_conflict_do_nothing(
            constraint="uq_media_aliases_provider_external_type"
        )
        await db.execute(stmt)

    for show in all_shows:
        vals: list[dict] = []
        if show.tmdb_id:
            vals.append(_alias_vals(show.id, "tmdb", show.tmdb_id))
        if show.tvdb_id:
            vals.append(_alias_vals(show.id, "tvdb", show.tvdb_id))
        # Cross-IDs from tmdb_data
        tmdb_data = show.tmdb_data or {}
        ext_ids = tmdb_data.get("external_ids") or {}
        if ext_ids.get("tvdb_id") and not show.tvdb_id:
            vals.append(_alias_vals(show.id, "tvdb", ext_ids["tvdb_id"]))
        if ext_ids.get("imdb_id"):
            vals.append(_alias_vals(show.id, "imdb", ext_ids["imdb_id"]))

        for v in vals:
            if not DRY_RUN:
                await _upsert(v)
            aliases_upserted += 1

    # ── 4. media_aliases — seed from Media rows (movies + episodes) ─────────────
    MEDIA_MTYPE = {
        MediaType.movie: "movie",
        MediaType.episode: "episode",
        MediaType.series: "series",
    }
    all_media_result = await db.execute(
        select(Media).where(Media.tmdb_id.isnot(None))
    )
    for m in all_media_result.scalars().all():
        mtype_str = MEDIA_MTYPE.get(m.media_type)
        if not mtype_str:
            continue
        v = _alias_vals(m.id, "tmdb", m.tmdb_id, mtype=mtype_str)
        if not DRY_RUN:
            await _upsert(v)
        aliases_upserted += 1

    if not DRY_RUN:
        await db.commit()

    mode = "DRY-RUN" if DRY_RUN else "APPLIED"
    print(f"\n[{mode}] Shows with uri_id populated: {shows_updated}")
    print(f"[{mode}] Media rows with uri_id populated: {media_updated}")
    print(f"[{mode}] Aliases seeded (shows + media): {aliases_upserted}")
    if collision_shows:
        print(f"[{mode}] Collisions skipped (show IDs): {collision_shows}")
    print(f"\nNext steps:")
    print(f"  Verify: SELECT COUNT(*) FROM shows WHERE uri_id IS NULL;")
    print(f"  Verify: SELECT COUNT(*) FROM media WHERE uri_id IS NULL AND tmdb_id IS NOT NULL;")


async def main() -> None:
    import sys
    import os
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

    from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
    from sqlalchemy.orm import sessionmaker

    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        from core.config import settings
        db_url = settings.db_url

    engine = create_async_engine(db_url)
    async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    async with async_session() as session:
        await _run(session)

    await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
