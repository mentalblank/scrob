"""Fill in TVDB series artwork for shows that have a tvdb_id but no stored art.

Enrichment only writes tvdb_data artwork when it happens to run with TVDB as
the active source, so a library built up under TMDB ends up with TVDB stills on
its episodes and nothing on its shows. Media pages then fall back to TMDB
posters even when the user's primary metadata source is TVDB.

Only shows are touched: _tvdb_series_artwork resolves series artwork, and TVDB
has no equivalent path for movies here.

    python -m scripts.backfill_tvdb_artwork --user-id 1 --dry-run
    python -m scripts.backfill_tvdb_artwork --user-id 1
"""

import argparse
import asyncio
import sys

from sqlalchemy import select

from core.enrichment import _tvdb_series_artwork
from db import AsyncSessionLocal
from models.show import Show


async def _resolve_key(db, user_id: int) -> str | None:
    from routers.shows import get_user_tvdb_key

    return await get_user_tvdb_key(db, user_id)


async def _language(db, user_id: int) -> str:
    from core import tvdb as tvdb_client
    from core.translations import get_user_metadata_language

    return tvdb_client.tvdb_language(await get_user_metadata_language(db, user_id)) or "eng"


async def run(user_id: int, limit: int | None, dry_run: bool, force: bool) -> int:
    async with AsyncSessionLocal() as db:
        api_key = await _resolve_key(db, user_id)
        if not api_key:
            print(f"No TVDB API key for user {user_id} (and none set globally).")
            return 1
        lang = await _language(db, user_id)

        rows = (await db.execute(select(Show).where(Show.tvdb_id.isnot(None)))).scalars().all()
        pending = [
            s for s in rows
            if force or not (s.tvdb_data or {}).get("poster_path")
        ]
        if limit:
            pending = pending[:limit]

        print(f"{len(rows)} shows have a tvdb_id; {len(pending)} need artwork (language {lang}).")
        if dry_run:
            for s in pending[:20]:
                print(f"  would fetch  tvdb:{s.tvdb_id}  {s.title}")
            if len(pending) > 20:
                print(f"  ... and {len(pending) - 20} more")
            return 0

        filled = missing = failed = 0
        for i, show in enumerate(pending, 1):
            try:
                poster, backdrop = await _tvdb_series_artwork(int(show.tvdb_id), api_key, lang)
            except Exception as exc:  # one bad series must not end the run
                failed += 1
                print(f"  [{i}/{len(pending)}] {show.title}: {exc}")
                continue

            if not poster and not backdrop:
                missing += 1
            else:
                show.tvdb_data = {
                    **(show.tvdb_data or {}),
                    "tvdb_id": show.tvdb_id,
                    "poster_path": poster,
                    "backdrop_path": backdrop,
                }
                filled += 1

            # Commit in batches so an interrupted run keeps what it fetched.
            if i % 25 == 0:
                await db.commit()
                print(f"  [{i}/{len(pending)}] filled={filled} no-art={missing} failed={failed}")

        await db.commit()
        print(f"Done. filled={filled} no-art={missing} failed={failed}")
        return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--user-id", type=int, required=True, help="whose TVDB key and language to use")
    ap.add_argument("--limit", type=int, default=None, help="stop after this many shows")
    ap.add_argument("--dry-run", action="store_true", help="list what would be fetched, write nothing")
    ap.add_argument("--force", action="store_true", help="refetch shows that already have artwork")
    args = ap.parse_args()
    return asyncio.run(run(args.user_id, args.limit, args.dry_run, args.force))


if __name__ == "__main__":
    sys.exit(main())
