import asyncio
from fastapi import APIRouter, Depends, Query, HTTPException, BackgroundTasks
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy import select, update, delete, func, cast, String
from sqlalchemy.dialects.postgresql import insert

from db import get_db, engine
from models.media import Media
from models.show import Show
from models.collection import Collection, CollectionFile
from models.users import User, UserSettings
from models.connections import MediaServerConnection
from models.sync import SyncJob, SyncStatus
from models.events import WatchEvent
from models.ratings import Rating
from models.library_selections import JellyfinLibrarySelection, EmbyLibrarySelection, PlexLibrarySelection
from datetime import datetime, timezone
from dateutil import parser
from models.base import MediaType, CollectionSource
from models.global_settings import GlobalSettings
from core import jellyfin, emby, plex, tmdb
import core.trakt as trakt_client
from core.enrichment import enrich_media

from dependencies import get_current_user


async def _get_effective_tmdb_key(db: AsyncSession, user_settings: UserSettings | None) -> str | None:
    if user_settings and user_settings.tmdb_api_key:
        return user_settings.tmdb_api_key
    gs_result = await db.execute(select(GlobalSettings).where(GlobalSettings.id == 1))
    gs = gs_result.scalar_one_or_none()
    return gs.tmdb_api_key if gs else None

router = APIRouter()

# Global semaphore — at most one sync running at a time across all users
_sync_semaphore = asyncio.Semaphore(1)

BATCH_SIZE = 500
TMDB_CONCURRENCY = 5  # Max concurrent TMDB requests
# asyncpg hard limit is 32767 parameters per query; stay well under it
_MAX_IN_PARAMS = 30_000


async def _select_in_chunks(db: AsyncSession, stmt_builder, ids: list):
    """Execute a select statement using chunked IN clauses to avoid the 32767-parameter limit.
    stmt_builder(chunk) should return a SQLAlchemy select() statement for that chunk of IDs.
    Returns a flat list of all rows."""
    results = []
    for i in range(0, len(ids), _MAX_IN_PARAMS):
        chunk = ids[i : i + _MAX_IN_PARAMS]
        res = await db.execute(stmt_builder(chunk))
        results.extend(res.scalars().all())
    return results


def extract_watch_state(item: dict, source: CollectionSource) -> dict:
    state = {"completed": False, "last_played": None, "play_count": 0, "user_rating": None}

    if source in (CollectionSource.jellyfin, CollectionSource.emby):
        user_data = item.get("UserData", {})
        state["completed"] = user_data.get("Played", False)
        state["play_count"] = user_data.get("PlayCount", 1 if state["completed"] else 0)
        lp = user_data.get("LastPlayedDate")
        if lp:
            dt = parser.isoparse(lp)
            if dt.tzinfo:
                dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
            state["last_played"] = dt
        r = user_data.get("Rating")
        if r is not None:
            state["user_rating"] = float(r)
    else:  # Plex
        state["play_count"] = int(item.get("viewCount", 0))
        state["completed"] = state["play_count"] > 0
        lp = item.get("lastViewedAt") or item.get("viewedAt")
        if lp:
            state["last_played"] = datetime.fromtimestamp(lp, tz=timezone.utc).replace(tzinfo=None)
        if state["play_count"] == 0 and item.get("viewedAt"):
            state["play_count"] = 1
            state["completed"] = True
        r = item.get("userRating")
        if r is not None:
            state["user_rating"] = float(r)

    return state


def get_jellyfin_tmdb_id(provider_ids: dict) -> int | None:
    tid = provider_ids.get("Tmdb") or provider_ids.get("tmdb")
    return int(tid) if tid else None


def extract_jellyfin_quality(item: dict) -> dict:
    from core.jellyfin import extract_quality
    quality = extract_quality(item.get("MediaStreams", []))
    quality["file_path"] = item.get("Path")
    return quality


async def sync_shows_batch(
    series_tmdb_map: dict,  # source_series_id → tmdb_id
    db: AsyncSession,
    api_key: str = None,
) -> tuple[dict, dict]:
    """
    Fetch and insert all shows in parallel (up to TMDB_CONCURRENCY concurrent requests).
    Returns (show_map: source_id→show.id, show_id_to_tmdb: show.id→series_tmdb_id).
    """
    all_tmdb_ids = list({tid for tid in series_tmdb_map.values() if tid})

    # Bulk load already-known shows (chunked to stay under asyncpg's 32767-param limit)
    existing_shows: dict[int, Show] = {}
    if all_tmdb_ids:
        shows_loaded = await _select_in_chunks(
            db,
            lambda chunk: select(Show).where(Show.tmdb_id.in_(chunk)),
            all_tmdb_ids,
        )
        for s in shows_loaded:
            existing_shows[s.tmdb_id] = s

    missing = [tid for tid in all_tmdb_ids if tid not in existing_shows]
    print(f"    {len(existing_shows)} shows in DB, fetching {len(missing)} from TMDB in parallel...")

    semaphore = asyncio.Semaphore(TMDB_CONCURRENCY)
    fetched: dict[int, dict] = {}

    async def fetch_show(tmdb_id: int):
        async with semaphore:
            try:
                fetched[tmdb_id] = await tmdb.get_show(tmdb_id, api_key=api_key)
            except Exception as e:
                print(f"  Failed to fetch show tmdb={tmdb_id}: {e}")

    if missing:
        await asyncio.gather(*[fetch_show(tid) for tid in missing])

    if fetched:
        values = []
        for tmdb_id, d in fetched.items():
            values.append({
                "tmdb_id": tmdb_id,
                "title": d.get("name"),
                "original_title": d.get("original_name"),
                "overview": d.get("overview"),
                "poster_path": tmdb.poster_url(d.get("poster_path")),
                "backdrop_path": tmdb.poster_url(d.get("backdrop_path"), size="w1280"),
                "tmdb_rating": d.get("vote_average"),
                "status": d.get("status"),
                "tagline": d.get("tagline"),
                "first_air_date": d.get("first_air_date"),
                "last_air_date": d.get("last_air_date"),
                "tmdb_data": {
                    "genres": [g["name"] for g in d.get("genres", [])],
                    "external_ids": d.get("external_ids", {}),
                    "original_language": d.get("original_language"),
                    "seasons": [
                        {
                            "season_number": s["season_number"],
                            "poster_path": tmdb.poster_url(s.get("poster_path")),
                            "episode_count": s["episode_count"],
                            "name": s["name"],
                        }
                        for s in d.get("seasons", [])
                    ],
                },
            })

        stmt = insert(Show).values(values)
        stmt = stmt.on_conflict_do_update(
            index_elements=["tmdb_id"],
            set_={
                k: getattr(stmt.excluded, k)
                for k in values[0].keys()
                if k != "tmdb_id"
            }
        )
        stmt = stmt.returning(Show)
        res = await db.execute(stmt)
        for s in res.scalars().all():
            existing_shows[s.tmdb_id] = s

    show_map: dict[str, int] = {}
    show_id_to_tmdb: dict[int, int] = {}
    for source_id, tmdb_id in series_tmdb_map.items():
        show = existing_shows.get(tmdb_id)
        if show:
            show_map[str(source_id)] = show.id
            show_id_to_tmdb[show.id] = show.tmdb_id

    return show_map, show_id_to_tmdb


async def batch_enrich_items(
    items: list[tuple],  # (Media, series_tmdb_id | None)
    api_key: str = None,
    show_title_map: dict[int, str] | None = None,
) -> list[dict]:
    """
    Parallel enrichment for newly created media.
    Episodes: one TMDB /season/{n} call per unique season (3865 calls vs 45k).
    Movies: parallel /movie/{id} calls.
    Returns a list of warning dicts for seasons/items that couldn't be enriched.
    """
    semaphore = asyncio.Semaphore(TMDB_CONCURRENCY)
    if show_title_map is None:
        show_title_map = {}

    movies = [m for (m, _) in items if m.media_type == MediaType.movie]
    episodes = [(m, stid) for (m, stid) in items if m.media_type == MediaType.episode and stid]

    # ── Movies: parallel enrichment ──────────────────────────────────────────
    async def enrich_movie(media: Media):
        async with semaphore:
            await enrich_media(media, api_key=api_key)

    if movies:
        await asyncio.gather(*[enrich_movie(m) for m in movies], return_exceptions=True)

    # ── Episodes: one TMDB call per unique (series, season) ──────────────────
    season_to_eps: dict[tuple, list[Media]] = {}
    for media, stid in episodes:
        if media.season_number is not None:
            season_to_eps.setdefault((stid, media.season_number), []).append(media)

    season_data: dict[tuple, dict[int, dict]] = {}
    failed_season_keys: set[tuple] = set()

    async def fetch_season(stid: int, sn: int):
        async with semaphore:
            try:
                d = await tmdb.get_season(stid, sn, api_key=api_key)
                season_data[(stid, sn)] = {ep["episode_number"]: ep for ep in d.get("episodes", [])}
            except Exception as e:
                show_title = show_title_map.get(stid, f"ID {stid}")
                print(f"  Failed to fetch '{show_title}' Season {sn} from TMDB (404 Not Found). This usually means your local season numbering doesn't match TMDB.")
                season_data[(stid, sn)] = {}
                failed_season_keys.add((stid, sn))

    if season_to_eps:
        print(f"    Fetching {len(season_to_eps)} seasons from TMDB...")
        await asyncio.gather(
            *[fetch_season(stid, sn) for (stid, sn) in season_to_eps],
            return_exceptions=True,
        )

    for (stid, sn), ep_list in season_to_eps.items():
        ep_map = season_data.get((stid, sn), {})
        for media in ep_list:
            ep = ep_map.get(media.episode_number)
            if not ep:
                continue
            media.tmdb_id = ep.get("id") or media.tmdb_id
            media.title = ep.get("name") or media.title
            media.overview = ep.get("overview")
            media.poster_path = tmdb.poster_url(ep.get("still_path"), size="w500")
            media.release_date = ep.get("air_date")
            media.tmdb_rating = ep.get("vote_average")
            media.tmdb_data = {"runtime": ep.get("runtime"), "cast": []}

    # Build per-show warning entries (group failed seasons by show)
    show_to_failed: dict[int, list[int]] = {}
    show_to_ep_count: dict[int, int] = {}
    for (stid, sn) in failed_season_keys:
        show_to_failed.setdefault(stid, []).append(sn)
        show_to_ep_count[stid] = show_to_ep_count.get(stid, 0) + len(season_to_eps.get((stid, sn), []))

    warnings: list[dict] = []
    for stid, failed_seasons in show_to_failed.items():
        warnings.append({
            "show": show_title_map.get(stid, f"TMDB show #{stid}"),
            "tmdb_id": stid,
            "seasons": sorted(failed_seasons),
            "affected_episodes": show_to_ep_count.get(stid, 0),
            "reason": "Season not found on TMDB — the show may be split into separate series on TMDB",
        })

    return warnings


async def _fan_out_changes_to_other_connections(
    db: AsyncSession,
    user_id: int,
    exclude_connection_id: int | None,
    new_watched_ids: set[int],
    new_ratings: dict[int, float],
    settings: "UserSettings | None" = None,
) -> None:
    """After an inbound sync, push the items that actually changed to every OTHER
    connection (media servers + Trakt) that has push_watched / push_ratings enabled.

    Only the delta (what was added to Scrob during this sync) is pushed, so we
    never blast unchanged history at the target server.

    pass exclude_connection_id=None when syncing from Trakt (no MediaServerConnection to skip).
    """
    if not new_watched_ids and not new_ratings:
        return

    all_changed_ids = set(new_watched_ids) | set(new_ratings.keys())

    # ── Media server fan-out ─────────────────────────────────────────────────
    conns_filter = [MediaServerConnection.user_id == user_id]
    if exclude_connection_id is not None:
        conns_filter.append(MediaServerConnection.id != exclude_connection_id)
    other_conns_result = await db.execute(
        select(MediaServerConnection).where(*conns_filter)
    )
    other_conns = other_conns_result.scalars().all()
    push_candidates = [c for c in other_conns if c.push_watched or c.push_ratings]

    push_tasks = []

    if push_candidates:
        files_result = await db.execute(
            select(CollectionFile.source_id, CollectionFile.source, Collection.media_id)
            .join(Collection, Collection.id == CollectionFile.collection_id)
            .where(
                Collection.user_id == user_id,
                Collection.media_id.in_(all_changed_ids),
                CollectionFile.source_id.isnot(None),
            )
        )
        # (source_type, media_id) → [source_id]
        source_ids_map: dict[tuple[CollectionSource, int], list[str]] = {}
        for source_id, source_type, media_id in files_result.all():
            source_ids_map.setdefault((source_type, media_id), []).append(source_id)

        for conn in push_candidates:
            conn_source = CollectionSource(conn.type)
            if conn.push_watched:
                for mid in new_watched_ids:
                    for sid in source_ids_map.get((conn_source, mid), []):
                        if conn.type == "plex":
                            push_tasks.append(plex.mark_watched(conn.url, conn.token, sid))
                        elif conn.type == "jellyfin":
                            push_tasks.append(jellyfin.mark_watched(conn.url, conn.token, conn.server_user_id, sid))
                        elif conn.type == "emby":
                            push_tasks.append(emby.mark_watched(conn.url, conn.token, conn.server_user_id, sid))
            if conn.push_ratings:
                for mid, rating in new_ratings.items():
                    for sid in source_ids_map.get((conn_source, mid), []):
                        if conn.type == "plex":
                            push_tasks.append(plex.set_rating(conn.url, conn.token, sid, rating))
                        elif conn.type == "jellyfin":
                            push_tasks.append(jellyfin.set_rating(conn.url, conn.token, conn.server_user_id, sid, rating))
                        elif conn.type == "emby":
                            push_tasks.append(emby.set_rating(conn.url, conn.token, conn.server_user_id, sid, rating))

    # ── Trakt fan-out ────────────────────────────────────────────────────────
    push_trakt_watched = settings and settings.trakt_push_watched and settings.trakt_access_token and settings.trakt_client_id
    push_trakt_ratings = settings and settings.trakt_push_ratings and settings.trakt_access_token and settings.trakt_client_id

    if (push_trakt_watched or push_trakt_ratings) and all_changed_ids:
        media_res = await db.execute(
            select(Media).where(Media.id.in_(all_changed_ids))
        )
        media_items = media_res.scalars().all()
        media_by_id: dict[int, Media] = {m.id: m for m in media_items}

        # Load shows for episode tmdb_id lookups
        show_ids = {m.show_id for m in media_items if m.show_id}
        shows_by_id: dict[int, "Show"] = {}
        if show_ids:
            shows_res = await db.execute(select(Show).where(Show.id.in_(show_ids)))
            shows_by_id = {s.id: s for s in shows_res.scalars().all()}

        if push_trakt_watched:
            for mid in new_watched_ids:
                media = media_by_id.get(mid)
                if not media or not media.tmdb_id:
                    continue
                if media.media_type == MediaType.movie:
                    push_tasks.append(trakt_client.add_movie_to_history(settings.trakt_client_id, settings.trakt_access_token, media.tmdb_id))
                elif media.media_type == MediaType.episode and media.show_id and media.season_number is not None and media.episode_number is not None:
                    show = shows_by_id.get(media.show_id)
                    if show and show.tmdb_id:
                        push_tasks.append(trakt_client.add_episode_to_history(settings.trakt_client_id, settings.trakt_access_token, show.tmdb_id, media.season_number, media.episode_number))

        if push_trakt_ratings:
            for mid, rating in new_ratings.items():
                media = media_by_id.get(mid)
                if not media or not media.tmdb_id:
                    continue
                if media.media_type == MediaType.movie:
                    push_tasks.append(trakt_client.set_movie_rating(settings.trakt_client_id, settings.trakt_access_token, media.tmdb_id, rating))
                elif media.media_type in (MediaType.series, MediaType.episode):
                    push_tasks.append(trakt_client.set_show_rating(settings.trakt_client_id, settings.trakt_access_token, media.tmdb_id, rating))

    if push_tasks:
        target_count = len(push_candidates) + (1 if (push_trakt_watched or push_trakt_ratings) else 0)
        print(f"  Fanning out {len(push_tasks)} changes to {target_count} other connection(s) (incl. Trakt)...")
        results = await asyncio.gather(*push_tasks, return_exceptions=True)
        failed = sum(1 for r in results if isinstance(r, Exception))
        if failed:
            print(f"  {failed}/{len(push_tasks)} fan-out push tasks failed (non-fatal)")


async def sync_items(
    items: list,
    media_type: MediaType,
    source: CollectionSource,
    db: AsyncSession,
    stats: dict,
    user_id: int,
    job_id: int = None,
    show_map: dict = {},
    api_key: str = None,
    show_id_to_tmdb: dict = {},  # show.id → series tmdb_id, for episode enrichment
    sync_collection: bool = True,
    sync_watched: bool = True,
    sync_ratings: bool = True,
    new_watched_ids: set[int] | None = None,  # accumulated across calls; mutated in-place
    new_ratings: dict[int, float] | None = None,  # accumulated across calls; mutated in-place
    connection_id: int | None = None,
) -> list[dict]:  # returns warnings
    print(f"  Syncing {len(items)} {media_type.value}s from {source.value}...")

    # ── Phase 1: Pre-load existing data (replaces all N+1 queries) ────────────

    # All existing CollectionFiles for this user+source: source_id → (CollectionFile, media_id, Media)
    files_q = await db.execute(
        select(CollectionFile, Collection.media_id, Media)
        .join(Collection, Collection.id == CollectionFile.collection_id)
        .join(Media, Media.id == Collection.media_id)
        .where(Collection.user_id == user_id, CollectionFile.source == source)
    )
    files_rows = files_q.all()
    existing_files: dict[str, tuple[CollectionFile, int, Media]] = {
        f.source_id: (f, media_id, m) for f, media_id, m in files_rows
    }
    # (media_id, source) → CollectionFile — to detect webhook-vs-sync source_id mismatches
    files_by_media_source: dict[tuple[int, CollectionSource], CollectionFile] = {
        (media_id, f.source): f for f, media_id, _ in files_rows
    }

    # All existing Collections for this user: media_id → Collection.id
    # Used to attach new CollectionFiles to existing Collections (multi-source items)
    colls_q = await db.execute(
        select(Collection.id, Collection.media_id).where(Collection.user_id == user_id)
    )
    existing_coll_by_media_id: dict[int, int] = {
        media_id: coll_id for coll_id, media_id in colls_q.all()
    }

    # All relevant media, keyed for O(1) lookup
    media_by_episode: dict[tuple, Media] = {}   # (show_id, season, ep) → Media
    media_by_tmdb: dict[tuple, Media] = {}       # (tmdb_id, media_type) → Media

    if media_type == MediaType.episode:
        show_ids = list(set(show_map.values()))
        if show_ids:
            episodes = await _select_in_chunks(
                db,
                lambda chunk: select(Media).where(Media.media_type == MediaType.episode, Media.show_id.in_(chunk)),
                show_ids,
            )
            for m in episodes:
                media_by_episode[(m.show_id, m.season_number, m.episode_number)] = m
        # Also pre-load orphaned episode rows (show_id=None, created by webhook before first sync)
        # so they can be deduplicated by TMDB ID instead of creating a second row.
        ep_tmdb_ids: set[int] = set()
        for item in items:
            tid = (
                get_jellyfin_tmdb_id(item.get("ProviderIds", {}))
                if source in (CollectionSource.jellyfin, CollectionSource.emby)
                else plex.extract_tmdb_id(item.get("Guid", []))
            )
            if tid:
                ep_tmdb_ids.add(tid)
        if ep_tmdb_ids:
            orphans = await _select_in_chunks(
                db,
                lambda chunk: select(Media).where(
                    Media.media_type == MediaType.episode,
                    Media.tmdb_id.in_(chunk),
                    Media.show_id.is_(None),
                ),
                list(ep_tmdb_ids),
            )
            for m in orphans:
                media_by_tmdb[(m.tmdb_id, m.media_type)] = m
    else:
        tmdb_ids: set[int] = set()
        for item in items:
            tid = (
                get_jellyfin_tmdb_id(item.get("ProviderIds", {}))
                if source in (CollectionSource.jellyfin, CollectionSource.emby)
                else plex.extract_tmdb_id(item.get("Guid", []))
            )
            if tid:
                tmdb_ids.add(tid)
        if tmdb_ids:
            medias = await _select_in_chunks(
                db,
                lambda chunk: select(Media).where(Media.media_type == media_type, Media.tmdb_id.in_(chunk)),
                list(tmdb_ids),
            )
            for m in medias:
                media_by_tmdb[(m.tmdb_id, m.media_type)] = m

    # Reverse lookup: media.id → Media object (for healing unenriched items in skipped branch)
    media_by_id: dict[int, Media] = {m.id: m for _, _, m in files_rows}
    for m in list(media_by_episode.values()) + list(media_by_tmdb.values()):
        media_by_id[m.id] = m

    # Existing watch event media_ids (only need the int, not the ORM object)
    we_res = await db.execute(select(WatchEvent.media_id).where(WatchEvent.user_id == user_id))
    existing_watched: set[int] = {row[0] for row in we_res}

    # Existing ratings: media_id → Rating
    rat_res = await db.execute(select(Rating).where(Rating.user_id == user_id))
    existing_ratings: dict[int, Rating] = {r.media_id: r for r in rat_res.scalars()}

    # ── Phase 2: Main sync loop (no N+1 queries, savepoints for error isolation) ──
    new_media_for_enrichment: list[tuple] = []  # (Media, series_tmdb_id | None)
    skipped_warnings: list[dict] = []

    for i, item in enumerate(items):
        new_media: Media | None = None
        try:
            async with db.begin_nested():
                if source in (CollectionSource.jellyfin, CollectionSource.emby):
                    source_id = str(item.get("Id"))
                    quality = extract_jellyfin_quality(item)
                    tmdb_id = get_jellyfin_tmdb_id(item.get("ProviderIds", {}))
                    parent_id = item.get("SeriesId")
                    name = item.get("Name")
                    season_num = item.get("ParentIndexNumber")
                    episode_num = item.get("IndexNumber")
                else:  # Plex
                    source_id = str(item.get("ratingKey"))
                    quality = plex.extract_quality(item.get("Media", []))
                    tmdb_id = plex.extract_tmdb_id(item.get("Guid", []))
                    parent_id = item.get("grandparentRatingKey")
                    name = item.get("title")
                    season_num = item.get("parentIndex")
                    episode_num = item.get("index")

                file_entry = existing_files.get(source_id)
                media_id_for_watch: int | None = None

                # Detect re-match: same Plex ratingKey but TMDB ID changed.
                # Evict the stale CollectionFile so the item is re-processed below.
                if file_entry and tmdb_id and sync_collection:
                    _, _existing_media_id, _existing_media = file_entry
                    if _existing_media.tmdb_id is not None and _existing_media.tmdb_id != tmdb_id:
                        stale_file = file_entry[0]
                        stale_collection_id = stale_file.collection_id
                        await db.delete(stale_file)
                        await db.flush()
                        remaining_q = await db.execute(
                            select(func.count(CollectionFile.id)).where(
                                CollectionFile.collection_id == stale_collection_id
                            )
                        )
                        if remaining_q.scalar() == 0:
                            stale_coll = await db.get(Collection, stale_collection_id)
                            if stale_coll:
                                await db.delete(stale_coll)
                                existing_coll_by_media_id.pop(_existing_media_id, None)
                        existing_files.pop(source_id, None)
                        files_by_media_source.pop((_existing_media_id, source), None)
                        file_entry = None

                if file_entry:
                    existing_file, existing_media_id, existing_media_obj = file_entry
                    if sync_collection:
                        # Update quality metadata in-place on the CollectionFile.
                        # Never overwrite language lists with empty — bulk endpoints (e.g. Plex
                        # /library/sections/all) often omit Part.Stream data, so an empty result
                        # means "not available here", not "no languages".
                        existing_file.resolution = quality.get("resolution")
                        existing_file.video_codec = quality.get("video_codec")
                        existing_file.audio_codec = quality.get("audio_codec")
                        existing_file.audio_channels = quality.get("audio_channels")
                        if quality.get("audio_languages"):
                            existing_file.audio_languages = quality["audio_languages"]
                        if quality.get("subtitle_languages"):
                            existing_file.subtitle_languages = quality["subtitle_languages"]
                        existing_file.file_path = quality.get("file_path")
                        if connection_id is not None:
                            existing_file.connection_id = connection_id
                    stats["skipped"] += 1
                    media_id_for_watch = existing_media_id

                    # Heal missing TMDB ID for movies
                    if media_type == MediaType.movie and existing_media_obj.tmdb_id is None and tmdb_id is not None:
                        existing_media_obj.tmdb_id = tmdb_id
                        if not any(m is existing_media_obj for m, _ in new_media_for_enrichment):
                            new_media_for_enrichment.append((existing_media_obj, None))

                    # Heal unenriched episodes: webhook may have created a Media row
                    # without show_id/poster_path before the first sync ran.
                    if media_type == MediaType.episode:
                        show_id = show_map.get(str(parent_id)) if parent_id else None
                        if show_id:
                            if existing_media_obj and (
                                existing_media_obj.show_id is None
                                or (existing_media_obj.poster_path is None and not existing_media_obj.tmdb_data)
                            ):
                                ep_series_tmdb_id = show_id_to_tmdb.get(show_id)
                                if ep_series_tmdb_id:
                                    existing_media_obj.show_id = show_id
                                    # Also fill in season/episode numbers if the webhook
                                    # created the row without them — required for enrichment.
                                    if existing_media_obj.season_number is None and season_num is not None:
                                        existing_media_obj.season_number = season_num
                                    if existing_media_obj.episode_number is None and episode_num is not None:
                                        existing_media_obj.episode_number = episode_num
                                    if not any(m is existing_media_obj for m, _ in new_media_for_enrichment):
                                        new_media_for_enrichment.append((existing_media_obj, ep_series_tmdb_id))
                else:
                    show_id = show_map.get(str(parent_id)) if media_type == MediaType.episode else None

                    # Look up existing media from pre-loaded dicts (O(1), no DB query)
                    if media_type == MediaType.episode and show_id:
                        media = media_by_episode.get((show_id, season_num, episode_num))
                        if not media and tmdb_id:
                            # Fallback: catch orphaned rows created by webhook without show_id
                            media = media_by_tmdb.get((tmdb_id, media_type))
                            if media:
                                # Backfill missing show_id so future lookups work correctly
                                media.show_id = show_id
                                media_by_episode[(show_id, season_num, episode_num)] = media
                    elif tmdb_id:
                        media = media_by_tmdb.get((tmdb_id, media_type))
                    else:
                        media = None

                    if media and (media.id, source) in files_by_media_source:
                        # Media has a CollectionFile for this source but a different source_id
                        # (e.g., webhook ratingKey differs from sync ratingKey for the same item).
                        # Update the existing CollectionFile in-place instead of inserting a duplicate.
                        if sync_collection:
                            existing_alt_file = files_by_media_source[(media.id, source)]
                            existing_alt_file.source_id = source_id
                            existing_alt_file.resolution = quality.get("resolution")
                            existing_alt_file.video_codec = quality.get("video_codec")
                            existing_alt_file.audio_codec = quality.get("audio_codec")
                            existing_alt_file.audio_channels = quality.get("audio_channels")
                            if quality.get("audio_languages"):
                                existing_alt_file.audio_languages = quality["audio_languages"]
                            if quality.get("subtitle_languages"):
                                existing_alt_file.subtitle_languages = quality["subtitle_languages"]
                            existing_alt_file.file_path = quality.get("file_path")
                            if connection_id is not None:
                                existing_alt_file.connection_id = connection_id
                            # Keep in-memory maps consistent
                            old_source_id = existing_alt_file.source_id
                            existing_files.pop(old_source_id, None)
                            existing_files[source_id] = (existing_alt_file, media.id, tmdb_id)
                            files_by_media_source[(media.id, source)] = existing_alt_file
                        stats["skipped"] += 1
                        media_id_for_watch = media.id
                    else:
                        if not media:
                            if not tmdb_id:
                                # TV episodes belonging to a known show can still be tracked and
                                # enriched later even without an individual episode TMDB ID (e.g.
                                # Jellyfin hasn't finished fetching episode metadata yet).
                                # Everything else (movies, episodes without show context) is skipped.
                                if not (
                                    media_type == MediaType.episode
                                    and show_id
                                    and season_num is not None
                                    and episode_num is not None
                                ):
                                    skipped_warnings.append({
                                        "title": name,
                                        "media_type": media_type.value,
                                        "source_id": source_id,
                                        "reason": "Unmatched on source — no TMDB ID available",
                                    })
                                    stats["skipped"] += 1
                                    raise Exception("Skip this item (unmatched)") # Triggers rollback of the nested transaction

                            media = Media(
                                tmdb_id=tmdb_id,
                                media_type=media_type,
                                title=name,
                                show_id=show_id,
                                season_number=season_num,
                                episode_number=episode_num,
                            )
                            db.add(media)
                            await db.flush()  # Get generated ID
                            new_media = media  # Cache updated after savepoint commits below

                            ep_series_tmdb_id = show_id_to_tmdb.get(show_id) if show_id else None
                            if tmdb_id or ep_series_tmdb_id:
                                new_media_for_enrichment.append((media, ep_series_tmdb_id))

                        if sync_collection:
                            coll_id = existing_coll_by_media_id.get(media.id)
                            if coll_id is None:
                                # Upsert: ON CONFLICT DO NOTHING guards against races
                                # between concurrent webhooks / savepoint rollbacks that
                                # desynchronise the in-memory dict from the DB.
                                coll_stmt = insert(Collection).values(user_id=user_id, media_id=media.id)
                                coll_stmt = coll_stmt.on_conflict_do_nothing(constraint="uq_collection_user_media")
                                await db.execute(coll_stmt)
                                await db.flush()
                                coll_result = await db.execute(
                                    select(Collection.id).where(
                                        Collection.user_id == user_id,
                                        Collection.media_id == media.id,
                                    )
                                )
                                coll_id = coll_result.scalar_one()
                                existing_coll_by_media_id[media.id] = coll_id
                                stats["movies" if media_type == MediaType.movie else "episodes"] += 1
                            # else: collection already exists from another source — just add the file
                            db.add(CollectionFile(
                                collection_id=coll_id,
                                connection_id=connection_id,
                                source=source,
                                source_id=source_id,
                                file_path=quality.get("file_path"),
                                resolution=quality.get("resolution"),
                                video_codec=quality.get("video_codec"),
                                audio_codec=quality.get("audio_codec"),
                                audio_channels=quality.get("audio_channels"),
                                audio_languages=quality.get("audio_languages"),
                                subtitle_languages=quality.get("subtitle_languages"),
                            ))
                        media_id_for_watch = media.id

                if media_id_for_watch is not None:
                    watch_state = extract_watch_state(item, source)
                    if sync_watched and (watch_state["completed"] or watch_state["play_count"] > 0) and media_id_for_watch not in existing_watched:
                        db.add(WatchEvent(
                            user_id=user_id,
                            media_id=media_id_for_watch,
                            watched_at=watch_state["last_played"] or datetime.now(timezone.utc).replace(tzinfo=None),
                            completed=watch_state["completed"],
                            play_count=max(1, watch_state["play_count"]),
                            progress_percent=1.0 if watch_state["completed"] else 0.0,
                        ))
                        existing_watched.add(media_id_for_watch)
                        if new_watched_ids is not None:
                            new_watched_ids.add(media_id_for_watch)

                    if sync_ratings and watch_state["user_rating"] is not None:
                        existing_r = existing_ratings.get(media_id_for_watch)
                        if existing_r:
                            existing_r.rating = watch_state["user_rating"]
                        else:
                            new_r = Rating(user_id=user_id, media_id=media_id_for_watch, rating=watch_state["user_rating"])
                            db.add(new_r)
                            existing_ratings[media_id_for_watch] = new_r
                        if new_ratings is not None:
                            new_ratings[media_id_for_watch] = watch_state["user_rating"]

            # Savepoint committed — update pre-loaded caches so duplicates within the
            # same sync batch reuse the newly created media instead of creating another.
            if new_media:
                if media_type == MediaType.episode and new_media.show_id:
                    media_by_episode[(new_media.show_id, new_media.season_number, new_media.episode_number)] = new_media
                elif new_media.tmdb_id:
                    media_by_tmdb[(new_media.tmdb_id, new_media.media_type)] = new_media

        except Exception as e:
            if str(e) == "Skip this item (unmatched)":
                continue
            # Savepoint already rolled back — remove the enrichment entry we may have queued
            if new_media and new_media_for_enrichment and new_media_for_enrichment[-1][0] is new_media:
                new_media_for_enrichment.pop()
            stats["errors"] += 1
            print(f"    Error syncing item {i}: {e}")

        if (i + 1) % BATCH_SIZE == 0:
            await db.commit()
            if job_id:
                await db.execute(
                    update(SyncJob)
                    .where(SyncJob.id == job_id)
                    .values(processed_items=SyncJob.processed_items + BATCH_SIZE, updated_at=func.now())
                )
                await db.commit()
            print(f"    Processed {i+1}/{len(items)} items...")

    await db.commit()
    processed_remainder = len(items) % BATCH_SIZE
    if job_id and processed_remainder > 0:
        await db.execute(
            update(SyncJob)
            .where(SyncJob.id == job_id)
            .values(processed_items=SyncJob.processed_items + processed_remainder, updated_at=func.now())
        )
        await db.commit()

    # ── Phase 3: Batch enrich newly created media ─────────────────────────────
    warnings: list[dict] = []
    if new_media_for_enrichment:
        unique_seasons = len({(stid, m.season_number) for m, stid in new_media_for_enrichment if m.media_type == MediaType.episode and stid})
        print(f"  Enriching {len(new_media_for_enrichment)} new items ({unique_seasons} unique seasons)...")

        # Build series_tmdb_id → source title map so warnings can name the show
        series_title_map: dict[int, str] = {}
        if media_type == MediaType.episode:
            for item in items:
                if source in (CollectionSource.jellyfin, CollectionSource.emby):
                    parent_id = str(item.get("SeriesId", ""))
                    title = item.get("SeriesName")
                else:
                    parent_id = str(item.get("grandparentRatingKey", ""))
                    title = item.get("grandparentTitle")
                if parent_id and title:
                    show_id = show_map.get(parent_id)
                    if show_id:
                        series_tmdb_id = show_id_to_tmdb.get(show_id)
                        if series_tmdb_id:
                            series_title_map[series_tmdb_id] = title

        warnings = await batch_enrich_items(new_media_for_enrichment, api_key=api_key, show_title_map=series_title_map)
        await db.commit()

    all_warnings = skipped_warnings + warnings
    print(f"  Finished syncing {media_type.value}s. Stats: {stats}")
    return all_warnings


async def run_jellyfin_sync(user_id: int, job_id: int, movie_limit: int, show_limit: int, connection_id: int | None = None, partial: bool = False):
    async with _sync_semaphore:
        await _run_jellyfin_sync(user_id, job_id, movie_limit, show_limit, connection_id, partial)


async def _run_jellyfin_sync(user_id: int, job_id: int, movie_limit: int, show_limit: int, connection_id: int | None = None, partial: bool = False):
    print(f"Starting Jellyfin sync for user {user_id}, job {job_id}")
    async_session = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
    async with async_session() as db:
        try:
            await db.execute(update(SyncJob).where(SyncJob.id == job_id).values(status=SyncStatus.running, processed_items=0, total_items=0))
            await db.commit()

            settings_result = await db.execute(select(UserSettings).where(UserSettings.user_id == user_id))
            settings = settings_result.scalar_one_or_none()
            tmdb_api_key = await _get_effective_tmdb_key(db, settings)

            # Load the specific connection (or oldest jellyfin connection for this user)
            conn_q = select(MediaServerConnection).where(
                MediaServerConnection.user_id == user_id,
                MediaServerConnection.type == "jellyfin",
            )
            if connection_id:
                conn_q = conn_q.where(MediaServerConnection.id == connection_id)
            else:
                conn_q = conn_q.order_by(MediaServerConnection.id.asc()).limit(1)
            conn_result = await db.execute(conn_q)
            conn = conn_result.scalar_one_or_none()

            if not conn or not tmdb_api_key:
                err = "Missing Jellyfin connection or TMDB API key"
                await db.execute(update(SyncJob).where(SyncJob.id == job_id).values(status=SyncStatus.failed, error_message=err))
                await db.commit()
                return

            j_url, j_token, j_user = conn.url, conn.token, conn.server_user_id

            print(f"  Fetching libraries from {j_url}")
            libraries = await jellyfin.get_libraries(j_url, j_token, j_user)

            sel_result = await db.execute(
                select(JellyfinLibrarySelection).where(JellyfinLibrarySelection.connection_id == conn.id)
            )
            selected_ids = {row.library_id for row in sel_result.scalars().all()}
            if selected_ids:
                libraries = [lib for lib in libraries if lib.get("Id") in selected_ids]

            print(f"  Found {len(libraries)} libraries to sync")
            stats = {"movies": 0, "episodes": 0, "skipped": 0, "errors": 0}
            all_warnings: list[dict] = []
            total_discovered = 0
            _new_watched: set[int] = set()
            _new_ratings: dict[int, float] = {}

            # Determine min_date for partial sync
            min_date = None
            if partial:
                last_sync = conn.last_partial_sync or conn.last_full_sync
                if last_sync:
                    # Jellyfin expects ISO 8601
                    min_date = last_sync.isoformat()

            for lib in libraries:
                lib_type = (lib.get("CollectionType") or "").lower()
                lib_id = lib.get("Id")
                lib_name = lib.get("Name")
                print(f"  Processing library: {lib_name} ({lib_type})")

                if lib_type == "movies":
                    items = await jellyfin.get_movies(lib_id, j_url, j_token, j_user, min_date=min_date)

                    if movie_limit:
                        items = items[:movie_limit]

                    movies_without_tmdb = [
                        m for m in items
                        if not get_jellyfin_tmdb_id(m.get("ProviderIds", {}))
                        and (m.get("ProviderIds", {}).get("Imdb") or m.get("Name"))
                    ]
                    if movies_without_tmdb:
                        print(f"    Resolving {len(movies_without_tmdb)} movies via IMDb/title fallback...")
                        semaphore = asyncio.Semaphore(TMDB_CONCURRENCY)

                        async def resolve_movie_tmdb_id(m: dict) -> None:
                            async with semaphore:
                                pids = m.get("ProviderIds", {})
                                imdb_id = pids.get("Imdb") or pids.get("imdb")
                                try:
                                    if imdb_id:
                                        res = await tmdb.find_by_external_id(imdb_id, "imdb_id", api_key=tmdb_api_key)
                                        if res.get("movie_results"):
                                            tid = res["movie_results"][0]["id"]
                                            m.setdefault("ProviderIds", {})["Tmdb"] = str(tid)
                                            return
                                    title = m.get("Name")
                                    year = m.get("ProductionYear")
                                    if title:
                                        res = await tmdb.search_movies(title, year=year, api_key=tmdb_api_key)
                                        if res.get("results"):
                                            best = res["results"][0]
                                            for r in res["results"]:
                                                if r.get("title", "").lower() == title.lower():
                                                    best = r
                                                    break
                                            tid = best["id"]
                                            m.setdefault("ProviderIds", {})["Tmdb"] = str(tid)
                                except Exception as e:
                                    print(f"    Could not resolve movie '{m.get('Name')}': {e}")

                        await asyncio.gather(*[resolve_movie_tmdb_id(m) for m in movies_without_tmdb])

                    total_discovered += len(items)
                    await db.execute(update(SyncJob).where(SyncJob.id == job_id).values(total_items=total_discovered))
                    await db.commit()

                    w = await sync_items(items, MediaType.movie, CollectionSource.jellyfin, db, stats, user_id, job_id, api_key=tmdb_api_key,
                        sync_collection=conn.sync_collection, sync_watched=conn.sync_watched, sync_ratings=conn.sync_ratings,
                        new_watched_ids=_new_watched, new_ratings=_new_ratings, connection_id=conn.id)
                    all_warnings.extend(w)

                elif lib_type in ("tvshows", "tv"):
                    if partial:
                        print(f"    Fetching recently updated shows and episodes...")
                        shows = await jellyfin.get_shows(lib_id, j_url, j_token, j_user, min_date=min_date)
                        items = await jellyfin.get_episodes(lib_id, j_url, j_token, j_user, min_date=min_date)
                        
                        needed_show_ids = {str(e.get("SeriesId")) for e in items if e.get("SeriesId")}
                        existing_show_ids = {str(s.get("Id")) for s in shows}
                        missing_show_ids = list(needed_show_ids - existing_show_ids)
                        if missing_show_ids:
                            print(f"    Fetching {len(missing_show_ids)} parent shows for updated episodes...")
                            extra_shows = await jellyfin.get_items_by_ids(j_url, j_token, j_user, missing_show_ids)
                            shows.extend(extra_shows)
                    else:
                        shows = await jellyfin.get_shows(lib_id, j_url, j_token, j_user)
                        if show_limit:
                            shows = shows[:show_limit]
                        items = None # Fetch later

                    series_tmdb_map = {
                        s.get("Id"): get_jellyfin_tmdb_id(s.get("ProviderIds", {}))
                        for s in shows if get_jellyfin_tmdb_id(s.get("ProviderIds", {}))
                    }

                    total_discovered += len(series_tmdb_map)
                    await db.execute(update(SyncJob).where(SyncJob.id == job_id).values(total_items=total_discovered))
                    await db.commit()

                    print(f"    Mapping {len(series_tmdb_map)} shows to TMDB...")
                    show_map, show_id_to_tmdb = await sync_shows_batch(series_tmdb_map, db, api_key=tmdb_api_key)
                    unmatched_shows = [s for s in shows if str(s.get("Id")) not in show_map]
                    for s in unmatched_shows:
                        all_warnings.append({
                            "title": s.get("Name"),
                            "media_type": "series",
                            "source_id": str(s.get("Id")),
                            "reason": "Unmatched on source — no TMDB ID available for the series",
                        })

                    if items is None:
                        items = await jellyfin.get_episodes(lib_id, j_url, j_token, j_user, min_date=min_date)
                    filtered_episodes = [e for e in items if str(e.get("SeriesId")) in show_map]

                    total_discovered = total_discovered - len(series_tmdb_map) + len(filtered_episodes)
                    await db.execute(update(SyncJob).where(SyncJob.id == job_id).values(total_items=total_discovered))
                    await db.commit()

                    w = await sync_items(
                        filtered_episodes, MediaType.episode, CollectionSource.jellyfin,
                        db, stats, user_id, job_id, show_map,
                        api_key=tmdb_api_key, show_id_to_tmdb=show_id_to_tmdb,
                        sync_collection=conn.sync_collection, sync_watched=conn.sync_watched, sync_ratings=conn.sync_ratings,
                        new_watched_ids=_new_watched, new_ratings=_new_ratings, connection_id=conn.id,
                    )
                    all_warnings.extend(w)

            print(f"Jellyfin sync job {job_id} completed. Stats: {stats}")
            if partial:
                conn.last_partial_sync = datetime.utcnow()
            else:
                conn.last_full_sync = datetime.utcnow()
                conn.last_partial_sync = conn.last_full_sync
            await _fan_out_changes_to_other_connections(db, user_id, conn.id, _new_watched, _new_ratings, settings=settings)
            await db.execute(update(SyncJob).where(SyncJob.id == job_id).values(status=SyncStatus.completed, stats=stats, warnings=all_warnings or None))
            await db.commit()
        except Exception as e:
            print(f"Jellyfin sync job {job_id} failed: {e}")
            import traceback
            traceback.print_exc()
            await db.rollback()
            await db.execute(update(SyncJob).where(SyncJob.id == job_id).values(status=SyncStatus.failed, error_message=str(e)[:900]))
            await db.commit()


async def run_emby_sync(user_id: int, job_id: int, movie_limit: int, show_limit: int, connection_id: int | None = None, partial: bool = False):
    async with _sync_semaphore:
        await _run_emby_sync(user_id, job_id, movie_limit, show_limit, connection_id, partial)


async def _run_emby_sync(user_id: int, job_id: int, movie_limit: int, show_limit: int, connection_id: int | None = None, partial: bool = False):
    print(f"Starting Emby sync for user {user_id}, job {job_id}")
    async_session = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
    async with async_session() as db:
        try:
            await db.execute(update(SyncJob).where(SyncJob.id == job_id).values(status=SyncStatus.running, processed_items=0, total_items=0))
            await db.commit()

            if connection_id is not None:
                conn_result = await db.execute(
                    select(MediaServerConnection).where(
                        MediaServerConnection.id == connection_id,
                        MediaServerConnection.user_id == user_id,
                        MediaServerConnection.type == "emby",
                    )
                )
            else:
                conn_result = await db.execute(
                    select(MediaServerConnection).where(
                        MediaServerConnection.user_id == user_id,
                        MediaServerConnection.type == "emby",
                    ).order_by(MediaServerConnection.id.asc()).limit(1)
                )
            conn = conn_result.scalar_one_or_none()

            settings_result = await db.execute(select(UserSettings).where(UserSettings.user_id == user_id))
            settings = settings_result.scalar_one_or_none()
            tmdb_api_key = await _get_effective_tmdb_key(db, settings)

            if not conn or not conn.url or not conn.token or not conn.server_user_id:
                err = "Missing Emby connection (URL, Token, or User ID)"
                await db.execute(update(SyncJob).where(SyncJob.id == job_id).values(status=SyncStatus.failed, error_message=err))
                await db.commit()
                return

            e_url = conn.url
            e_token = conn.token
            e_user = conn.server_user_id

            print(f"  Fetching libraries from {e_url}")
            libraries = await emby.get_libraries(e_url, e_token, e_user)

            sel_result = await db.execute(
                select(EmbyLibrarySelection).where(EmbyLibrarySelection.connection_id == conn.id)
            )
            selected_ids = {row.library_id for row in sel_result.scalars().all()}
            if selected_ids:
                libraries = [lib for lib in libraries if lib.get("Id") in selected_ids]

            print(f"  Found {len(libraries)} libraries to sync")
            stats = {"movies": 0, "episodes": 0, "skipped": 0, "errors": 0}
            all_warnings: list[dict] = []
            total_discovered = 0
            _new_watched: set[int] = set()
            _new_ratings: dict[int, float] = {}

            # Determine min_date for partial sync
            min_date = None
            if partial:
                last_sync = conn.last_partial_sync or conn.last_full_sync
                if last_sync:
                    min_date = last_sync.isoformat()

            for lib in libraries:
                lib_type = (lib.get("CollectionType") or "").lower()
                lib_id = lib.get("Id")
                lib_name = lib.get("Name")
                print(f"  Processing library: {lib_name} ({lib_type})")

                if lib_type == "movies":
                    items = await emby.get_movies(lib_id, e_url, e_token, e_user, min_date=min_date)

                    if movie_limit:
                        items = items[:movie_limit]

                    movies_without_tmdb = [
                        m for m in items
                        if not get_jellyfin_tmdb_id(m.get("ProviderIds", {}))
                        and (m.get("ProviderIds", {}).get("Imdb") or m.get("Name"))
                    ]
                    if movies_without_tmdb:
                        print(f"    Resolving {len(movies_without_tmdb)} movies via IMDb/title fallback...")
                        semaphore = asyncio.Semaphore(TMDB_CONCURRENCY)

                        async def resolve_emby_movie_tmdb_id(m: dict) -> None:
                            async with semaphore:
                                pids = m.get("ProviderIds", {})
                                imdb_id = pids.get("Imdb") or pids.get("imdb")
                                try:
                                    if imdb_id:
                                        res = await tmdb.find_by_external_id(imdb_id, "imdb_id", api_key=tmdb_api_key)
                                        if res.get("movie_results"):
                                            tid = res["movie_results"][0]["id"]
                                            m.setdefault("ProviderIds", {})["Tmdb"] = str(tid)
                                            return
                                    title = m.get("Name")
                                    year = m.get("ProductionYear")
                                    if title:
                                        res = await tmdb.search_movies(title, year=year, api_key=tmdb_api_key)
                                        if res.get("results"):
                                            best = res["results"][0]
                                            for r in res["results"]:
                                                if r.get("title", "").lower() == title.lower():
                                                    best = r
                                                    break
                                            tid = best["id"]
                                            m.setdefault("ProviderIds", {})["Tmdb"] = str(tid)
                                except Exception as e:
                                    print(f"    Could not resolve movie '{m.get('Name')}': {e}")

                        await asyncio.gather(*[resolve_emby_movie_tmdb_id(m) for m in movies_without_tmdb])

                    total_discovered += len(items)
                    await db.execute(update(SyncJob).where(SyncJob.id == job_id).values(total_items=total_discovered))
                    await db.commit()

                    w = await sync_items(items, MediaType.movie, CollectionSource.emby, db, stats, user_id, job_id, api_key=tmdb_api_key,
                        sync_collection=conn.sync_collection, sync_watched=conn.sync_watched, sync_ratings=conn.sync_ratings,
                        new_watched_ids=_new_watched, new_ratings=_new_ratings, connection_id=conn.id)
                    all_warnings.extend(w)

                elif lib_type in ("tvshows", "tv"):
                    if partial:
                        print(f"    Fetching recently updated shows and episodes...")
                        shows = await emby.get_shows(lib_id, e_url, e_token, e_user, min_date=min_date)
                        items = await emby.get_episodes(lib_id, e_url, e_token, e_user, min_date=min_date)
                        
                        needed_show_ids = {str(e.get("SeriesId")) for e in items if e.get("SeriesId")}
                        existing_show_ids = {str(s.get("Id")) for s in shows}
                        missing_show_ids = list(needed_show_ids - existing_show_ids)
                        if missing_show_ids:
                            print(f"    Fetching {len(missing_show_ids)} parent shows for updated episodes...")
                            extra_shows = await emby.get_items_by_ids(e_url, e_token, e_user, missing_show_ids)
                            shows.extend(extra_shows)
                    else:
                        shows = await emby.get_shows(lib_id, e_url, e_token, e_user)
                        if show_limit:
                            shows = shows[:show_limit]
                        items = None # Fetch later

                    series_tmdb_map = {
                        s.get("Id"): get_jellyfin_tmdb_id(s.get("ProviderIds", {}))
                        for s in shows if get_jellyfin_tmdb_id(s.get("ProviderIds", {}))
                    }

                    total_discovered += len(series_tmdb_map)
                    await db.execute(update(SyncJob).where(SyncJob.id == job_id).values(total_items=total_discovered))
                    await db.commit()

                    print(f"    Mapping {len(series_tmdb_map)} shows to TMDB...")
                    show_map, show_id_to_tmdb = await sync_shows_batch(
                        series_tmdb_map, db, api_key=tmdb_api_key
                    )
                    unmatched_shows = [s for s in shows if str(s.get("Id")) not in show_map]
                    for s in unmatched_shows:
                        all_warnings.append({
                            "title": s.get("Name"),
                            "media_type": "series",
                            "source_id": str(s.get("Id")),
                            "reason": "Unmatched on source — no TMDB ID available for the series",
                        })

                    if items is None:
                        items = await emby.get_episodes(lib_id, e_url, e_token, e_user, min_date=min_date)
                    filtered_episodes = [e for e in items if str(e.get("SeriesId")) in show_map]

                    total_discovered = total_discovered - len(series_tmdb_map) + len(filtered_episodes)
                    await db.execute(update(SyncJob).where(SyncJob.id == job_id).values(total_items=total_discovered))
                    await db.commit()

                    w = await sync_items(
                        filtered_episodes, MediaType.episode, CollectionSource.emby,
                        db, stats, user_id, job_id, show_map,
                        api_key=tmdb_api_key, show_id_to_tmdb=show_id_to_tmdb,
                        sync_collection=conn.sync_collection, sync_watched=conn.sync_watched, sync_ratings=conn.sync_ratings,
                        new_watched_ids=_new_watched, new_ratings=_new_ratings, connection_id=conn.id,
                    )
                    all_warnings.extend(w)

            print(f"Emby sync job {job_id} completed. Stats: {stats}")
            if partial:
                conn.last_partial_sync = datetime.utcnow()
            else:
                conn.last_full_sync = datetime.utcnow()
                conn.last_partial_sync = conn.last_full_sync
            await _fan_out_changes_to_other_connections(db, user_id, conn.id, _new_watched, _new_ratings, settings=settings)
            await db.execute(update(SyncJob).where(SyncJob.id == job_id).values(status=SyncStatus.completed, stats=stats, warnings=all_warnings or None))
            await db.commit()
        except Exception as e:
            print(f"Emby sync job {job_id} failed: {e}")
            import traceback
            traceback.print_exc()
            await db.rollback()
            await db.execute(update(SyncJob).where(SyncJob.id == job_id).values(status=SyncStatus.failed, error_message=str(e)[:900]))
            await db.commit()


async def _get_recently_updated_plex_items(p_url, p_token, lib_key, fetcher_func, min_timestamp):
    all_items = []
    offset = 0
    limit = 100
    while True:
        items = await fetcher_func(p_url, p_token, lib_key, sort="updatedAt:desc", offset=offset, limit=limit)
        if not items:
            break
        
        reached_end = False
        for item in items:
            updated_at = item.get("updatedAt")
            if updated_at and updated_at >= min_timestamp:
                all_items.append(item)
            else:
                reached_end = True
                break
        
        if reached_end or len(items) < limit:
            break
        offset += limit
    return all_items


async def _backfill_plex_languages(db: AsyncSession, user_id: int, connection_id: int, p_url: str, p_token: str) -> int:
    """Fetch full item detail from Plex for any CollectionFiles that have no language data yet."""
    result = await db.execute(
        select(CollectionFile)
        .join(Collection, Collection.id == CollectionFile.collection_id)
        .where(
            Collection.user_id == user_id,
            CollectionFile.source == CollectionSource.plex,
            CollectionFile.connection_id == connection_id,
            CollectionFile.source_id.isnot(None),
            (CollectionFile.audio_languages.is_(None)) | (cast(CollectionFile.audio_languages, String) == '[]'),
        )
    )
    files = result.scalars().all()
    if not files:
        return 0

    sem = asyncio.Semaphore(10)

    async def _fetch(cf: CollectionFile):
        async with sem:
            item = await plex.get_item(p_url, p_token, cf.source_id)
            if not item:
                return
            quality = plex.extract_quality(item.get("Media", []))
            if quality.get("audio_languages"):
                cf.audio_languages = quality["audio_languages"]
            if quality.get("subtitle_languages"):
                cf.subtitle_languages = quality["subtitle_languages"]

    await asyncio.gather(*[_fetch(cf) for cf in files])
    await db.commit()
    return len(files)


async def run_plex_sync(user_id: int, job_id: int, movie_limit: int, show_limit: int, connection_id: int | None = None, partial: bool = False):
    async with _sync_semaphore:
        await _run_plex_sync(user_id, job_id, movie_limit, show_limit, connection_id, partial)


async def _run_plex_sync(user_id: int, job_id: int, movie_limit: int, show_limit: int, connection_id: int | None = None, partial: bool = False):
    print(f"Starting Plex sync for user {user_id}, job {job_id}")
    async_session = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
    async with async_session() as db:
        try:
            await db.execute(update(SyncJob).where(SyncJob.id == job_id).values(status=SyncStatus.running, processed_items=0, total_items=0))
            await db.commit()

            if connection_id is not None:
                conn_result = await db.execute(
                    select(MediaServerConnection).where(
                        MediaServerConnection.id == connection_id,
                        MediaServerConnection.user_id == user_id,
                        MediaServerConnection.type == "plex",
                    )
                )
            else:
                conn_result = await db.execute(
                    select(MediaServerConnection).where(
                        MediaServerConnection.user_id == user_id,
                        MediaServerConnection.type == "plex",
                    ).order_by(MediaServerConnection.id.asc()).limit(1)
                )
            conn = conn_result.scalar_one_or_none()

            settings_result = await db.execute(select(UserSettings).where(UserSettings.user_id == user_id))
            settings = settings_result.scalar_one_or_none()
            tmdb_api_key = await _get_effective_tmdb_key(db, settings)

            if not conn or not conn.url or not conn.token:
                err = "Missing Plex connection (URL or Token)"
                await db.execute(update(SyncJob).where(SyncJob.id == job_id).values(status=SyncStatus.failed, error_message=err))
                await db.commit()
                return

            p_url = conn.url
            p_token = conn.token

            print(f"  Fetching Plex libraries...")
            libraries = await plex.get_libraries(p_url, p_token)

            sel_result = await db.execute(
                select(PlexLibrarySelection).where(PlexLibrarySelection.connection_id == conn.id)
            )
            selected_keys = {row.library_key for row in sel_result.scalars().all()}
            if selected_keys:
                libraries = [lib for lib in libraries if lib.get("key") in selected_keys]

            print(f"  Found {len(libraries)} libraries to sync")
            stats = {"movies": 0, "episodes": 0, "skipped": 0, "errors": 0}
            all_warnings: list[dict] = []
            total_discovered = 0
            _new_watched: set[int] = set()
            _new_ratings: dict[int, float] = {}

            # Determine min_timestamp for partial sync
            min_timestamp = None
            history_keys = set()
            if partial:
                last_sync = conn.last_partial_sync or conn.last_full_sync
                if last_sync:
                    min_timestamp = int(last_sync.timestamp())
                    print(f"  Fetching Plex history since {last_sync}...")
                    history = await plex.get_history(p_url, p_token, min_timestamp)
                    history_keys = {str(i["ratingKey"]) for i in history if i.get("ratingKey")}
                    print(f"  Found {len(history_keys)} unique items in history.")

            for lib in libraries:
                lib_type = lib.get("type")
                lib_key = lib.get("key")
                lib_title = lib.get("title")
                print(f"  Processing library: {lib_title} ({lib_type})")

                if lib_type == "movie":
                    if partial:
                        print(f"    Fetching recently updated movies...")
                        items = await _get_recently_updated_plex_items(p_url, p_token, lib_key, plex.get_movies, min_timestamp)
                        # Add items found in history that might not have been "updated" (just watched)
                        movie_history_keys = [] # We don't know which history keys are movies yet
                        # But we can just fetch all history keys from this library section
                        in_lib_history_keys = list(history_keys) # Fetching them all is fine, get_items_by_ids handles it
                        if in_lib_history_keys:
                            history_items = await plex.get_items_by_ids(p_url, p_token, lib_key, in_lib_history_keys)
                            # Merge them
                            existing_keys = {str(m["ratingKey"]) for m in items}
                            for hi in history_items:
                                if str(hi["ratingKey"]) not in existing_keys:
                                    items.append(hi)
                    else:
                        items = await plex.get_movies(p_url, p_token, lib_key)

                    if movie_limit:
                        items = items[:movie_limit]

                    movies_without_tmdb = [
                        m for m in items
                        if not plex.extract_tmdb_id(m.get("Guid", []))
                        and (plex.extract_imdb_id(m.get("Guid", [])) or m.get("title"))
                    ]
                    if movies_without_tmdb:
                        print(f"    Resolving {len(movies_without_tmdb)} movies via IMDb/title fallback...")
                        semaphore = asyncio.Semaphore(TMDB_CONCURRENCY)

                        async def resolve_movie_tmdb_id(m: dict) -> None:
                            async with semaphore:
                                guids = m.get("Guid", [])
                                imdb_id = plex.extract_imdb_id(guids)
                                try:
                                    if imdb_id:
                                        res = await tmdb.find_by_external_id(imdb_id, "imdb_id", api_key=tmdb_api_key)
                                        if res.get("movie_results"):
                                            tid = res["movie_results"][0]["id"]
                                            m.setdefault("Guid", []).append({"id": f"tmdb://{tid}"})
                                            return
                                    title = m.get("title")
                                    year = m.get("year")
                                    if title:
                                        res = await tmdb.search_movies(title, year=year, api_key=tmdb_api_key)
                                        if res.get("results"):
                                            best = res["results"][0]
                                            for r in res["results"]:
                                                if r.get("title", "").lower() == title.lower():
                                                    best = r
                                                    break
                                            tid = best["id"]
                                            m.setdefault("Guid", []).append({"id": f"tmdb://{tid}"})
                                except Exception as e:
                                    print(f"    Could not resolve movie '{m.get('title')}': {e}")

                        await asyncio.gather(*[resolve_movie_tmdb_id(m) for m in movies_without_tmdb])

                    total_discovered += len(items)
                    await db.execute(update(SyncJob).where(SyncJob.id == job_id).values(total_items=total_discovered))
                    await db.commit()

                    w = await sync_items(items, MediaType.movie, CollectionSource.plex, db, stats, user_id, job_id, api_key=tmdb_api_key,
                        sync_collection=conn.sync_collection, sync_watched=conn.sync_watched, sync_ratings=conn.sync_ratings,
                        new_watched_ids=_new_watched, new_ratings=_new_ratings, connection_id=conn.id)
                    all_warnings.extend(w)

                elif lib_type == "show":
                    if partial:
                        print(f"    Fetching recently updated shows and episodes...")
                        shows = await _get_recently_updated_plex_items(p_url, p_token, lib_key, plex.get_shows, min_timestamp)
                        episodes = await _get_recently_updated_plex_items(p_url, p_token, lib_key, plex.get_episodes, min_timestamp)
                        
                        # Add items from history
                        in_lib_history_keys = list(history_keys)
                        if in_lib_history_keys:
                            h_items = await plex.get_items_by_ids(p_url, p_token, lib_key, in_lib_history_keys)
                            existing_show_keys = {str(s["ratingKey"]) for s in shows}
                            existing_ep_keys = {str(e["ratingKey"]) for e in episodes}
                            for hi in h_items:
                                h_type = hi.get("type")
                                rk = str(hi["ratingKey"])
                                if h_type == "show" and rk not in existing_show_keys:
                                    shows.append(hi)
                                elif h_type == "episode" and rk not in existing_ep_keys:
                                    episodes.append(hi)

                        needed_show_keys = {str(e.get("grandparentRatingKey")) for e in episodes if e.get("grandparentRatingKey")}
                        existing_show_keys = {str(s.get("ratingKey")) for s in shows}
                        missing_show_keys = list(needed_show_keys - existing_show_keys)
                        if missing_show_keys:
                            print(f"    Fetching {len(missing_show_keys)} parent shows for updated episodes...")
                            extra_shows = await plex.get_items_by_ids(p_url, p_token, lib_key, missing_show_keys)
                            shows.extend(extra_shows)
                        items = episodes
                    else:
                        shows = await plex.get_shows(p_url, p_token, lib_key)
                        if show_limit:
                            shows = shows[:show_limit]
                        items = None # Fetch later

                    series_tmdb_map = {
                        s.get("ratingKey"): plex.extract_tmdb_id(s.get("Guid", []))
                        for s in shows if plex.extract_tmdb_id(s.get("Guid", []))
                    }

                    shows_without_tmdb = [
                        s for s in shows
                        if s.get("ratingKey") not in series_tmdb_map
                        and (plex.extract_tvdb_id(s.get("Guid", [])) or plex.extract_imdb_id(s.get("Guid", [])))
                    ]
                    if shows_without_tmdb:
                        print(f"    Resolving {len(shows_without_tmdb)} shows via TVDB/IMDb fallback...")
                        semaphore = asyncio.Semaphore(TMDB_CONCURRENCY)

                        async def resolve_show_tmdb_id(s: dict) -> None:
                            async with semaphore:
                                guids = s.get("Guid", [])
                                tvdb_id = plex.extract_tvdb_id(guids)
                                imdb_id = plex.extract_imdb_id(guids)
                                try:
                                    if tvdb_id:
                                        res = await tmdb.find_by_external_id(tvdb_id, "tvdb_id", api_key=tmdb_api_key)
                                        if res.get("tv_results"):
                                            series_tmdb_map[s["ratingKey"]] = res["tv_results"][0]["id"]
                                            return
                                    if imdb_id:
                                        res = await tmdb.find_by_external_id(imdb_id, "imdb_id", api_key=tmdb_api_key)
                                        if res.get("tv_results"):
                                            series_tmdb_map[s["ratingKey"]] = res["tv_results"][0]["id"]
                                            return
                                    title = s.get("title") or s.get("titleSort")
                                    if title:
                                        res = await tmdb.search_shows(title, api_key=tmdb_api_key)
                                        if res.get("results"):
                                            series_tmdb_map[s["ratingKey"]] = res["results"][0]["id"]
                                except Exception as e:
                                    print(f"    Could not resolve show '{s.get('title')}': {e}")

                        await asyncio.gather(*[resolve_show_tmdb_id(s) for s in shows_without_tmdb])

                    total_discovered += len(series_tmdb_map)
                    await db.execute(update(SyncJob).where(SyncJob.id == job_id).values(total_items=total_discovered))
                    await db.commit()

                    print(f"    Mapping {len(series_tmdb_map)} shows to TMDB...")
                    show_map, show_id_to_tmdb = await sync_shows_batch(
                        series_tmdb_map, db, api_key=tmdb_api_key
                    )
                    print(f"    Mapped {len(show_map)}/{len(series_tmdb_map)} shows.")

                    unmatched_shows = [s for s in shows if str(s.get("ratingKey")) not in show_map]
                    for s in unmatched_shows:
                        all_warnings.append({
                            "title": s.get("title"),
                            "media_type": "series",
                            "source_id": str(s.get("ratingKey")),
                            "reason": "Unmatched on source — no TMDB ID available for the series",
                        })

                    print(f"    Fetching episodes for {lib_title}...")
                    if items is None:
                        items = await plex.get_episodes(p_url, p_token, lib_key, min_timestamp=min_timestamp)
                    filtered_episodes = [i for i in items if str(i.get("grandparentRatingKey")) in show_map]

                    total_discovered = total_discovered - len(series_tmdb_map) + len(filtered_episodes)
                    await db.execute(update(SyncJob).where(SyncJob.id == job_id).values(total_items=total_discovered))
                    await db.commit()

                    w = await sync_items(
                        filtered_episodes, MediaType.episode, CollectionSource.plex,
                        db, stats, user_id, job_id, show_map,
                        api_key=tmdb_api_key, show_id_to_tmdb=show_id_to_tmdb,
                        sync_collection=conn.sync_collection, sync_watched=conn.sync_watched, sync_ratings=conn.sync_ratings,
                        new_watched_ids=_new_watched, new_ratings=_new_ratings, connection_id=conn.id,
                    )
                    all_warnings.extend(w)

            backfilled = await _backfill_plex_languages(db, user_id, conn.id, p_url, p_token)
            if backfilled:
                print(f"Plex sync job {job_id}: backfilled language data for {backfilled} file(s).")
            print(f"Plex sync job {job_id} completed. Stats: {stats}")
            if partial:
                conn.last_partial_sync = datetime.utcnow()
            else:
                conn.last_full_sync = datetime.utcnow()
                conn.last_partial_sync = conn.last_full_sync
            await _fan_out_changes_to_other_connections(db, user_id, conn.id, _new_watched, _new_ratings, settings=settings)
            await db.execute(update(SyncJob).where(SyncJob.id == job_id).values(status=SyncStatus.completed, stats=stats, warnings=all_warnings or None))
            await db.commit()
        except Exception as e:
            print(f"Plex sync job {job_id} failed: {e}")
            import traceback
            traceback.print_exc()
            await db.rollback()
            await db.execute(update(SyncJob).where(SyncJob.id == job_id).values(status=SyncStatus.failed, error_message=str(e)[:900]))
            await db.commit()


class LibrarySelectionBody(BaseModel):
    library_ids: list[str]


class PlexLibrarySelectionBody(BaseModel):
    library_keys: list[str]


async def _get_connection_or_404(db: AsyncSession, connection_id: int, user_id: int) -> MediaServerConnection:
    result = await db.execute(
        select(MediaServerConnection).where(
            MediaServerConnection.id == connection_id,
            MediaServerConnection.user_id == user_id,
        )
    )
    conn = result.scalar_one_or_none()
    if not conn:
        raise HTTPException(status_code=404, detail="Connection not found")
    return conn


@router.get("/connection/{connection_id}/libraries")
async def get_connection_libraries(
    connection_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    conn = await _get_connection_or_404(db, connection_id, current_user.id)

    try:
        if conn.type == "jellyfin":
            available = await jellyfin.get_libraries(conn.url, conn.token, conn.server_user_id)
            sel_result = await db.execute(
                select(JellyfinLibrarySelection).where(JellyfinLibrarySelection.connection_id == conn.id)
            )
            selected_ids = {row.library_id for row in sel_result.scalars().all()}
            libraries = [
                {"id": lib["Id"], "name": lib["Name"], "type": lib.get("CollectionType"), "selected": lib["Id"] in selected_ids}
                for lib in available if lib.get("CollectionType") in ("movies", "tvshows", "tv")
            ]
            return {"libraries": libraries, "all_selected": len(selected_ids) == 0}

        elif conn.type == "emby":
            available = await emby.get_libraries(conn.url, conn.token, conn.server_user_id)
            sel_result = await db.execute(
                select(EmbyLibrarySelection).where(EmbyLibrarySelection.connection_id == conn.id)
            )
            selected_ids = {row.library_id for row in sel_result.scalars().all()}
            libraries = [
                {"id": lib["Id"], "name": lib["Name"], "type": lib.get("CollectionType"), "selected": lib["Id"] in selected_ids}
                for lib in available if lib.get("CollectionType") in ("movies", "tvshows", "tv")
            ]
            return {"libraries": libraries, "all_selected": len(selected_ids) == 0}

        elif conn.type == "plex":
            available = await plex.get_libraries(conn.url, conn.token)
            sel_result = await db.execute(
                select(PlexLibrarySelection).where(PlexLibrarySelection.connection_id == conn.id)
            )
            selected_keys = {row.library_key for row in sel_result.scalars().all()}
            libraries = [
                {"key": lib["key"], "name": lib["title"], "type": lib.get("type"), "selected": lib["key"] in selected_keys}
                for lib in available if lib.get("type") in ("movie", "show")
            ]
            return {"libraries": libraries, "all_selected": len(selected_keys) == 0}

        else:
            raise HTTPException(status_code=400, detail=f"Unknown connection type: {conn.type}")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Could not reach server: {e}")


@router.post("/connection/{connection_id}/scan")
async def trigger_library_scan(
    connection_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    conn = await _get_connection_or_404(db, connection_id, current_user.id)

    try:
        if conn.type in ("jellyfin", "emby"):
            client = jellyfin if conn.type == "jellyfin" else emby
            ok = await client.scan_libraries(conn.url, conn.token)
        elif conn.type == "plex":
            sel_result = await db.execute(
                select(PlexLibrarySelection).where(PlexLibrarySelection.connection_id == conn.id)
            )
            selected_keys = [row.library_key for row in sel_result.scalars().all()]
            ok = await plex.scan_libraries(conn.url, conn.token, selected_keys)
        else:
            raise HTTPException(status_code=400, detail=f"Unknown connection type: {conn.type}")

        if not ok:
            raise HTTPException(status_code=502, detail="Library scan request failed")
        return {"status": "ok", "message": "Library scan triggered successfully"}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Could not reach server: {e}")


@router.put("/connection/{connection_id}/libraries")
async def save_connection_libraries(
    connection_id: int,
    body: dict,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    conn = await _get_connection_or_404(db, connection_id, current_user.id)

    try:
        if conn.type == "jellyfin":
            library_ids: list[str] = body.get("library_ids", [])
            available = await jellyfin.get_libraries(conn.url, conn.token, conn.server_user_id)
            name_map = {lib["Id"]: lib["Name"] for lib in available}
            await db.execute(delete(JellyfinLibrarySelection).where(JellyfinLibrarySelection.connection_id == conn.id))
            for lid in library_ids:
                if lid in name_map:
                    db.add(JellyfinLibrarySelection(user_id=current_user.id, connection_id=conn.id, library_id=lid, library_name=name_map[lid]))
            await db.commit()
            return {"saved": len(library_ids)}

        elif conn.type == "emby":
            library_ids = body.get("library_ids", [])
            available = await emby.get_libraries(conn.url, conn.token, conn.server_user_id)
            name_map = {lib["Id"]: lib["Name"] for lib in available}
            await db.execute(delete(EmbyLibrarySelection).where(EmbyLibrarySelection.connection_id == conn.id))
            for lid in library_ids:
                if lid in name_map:
                    db.add(EmbyLibrarySelection(user_id=current_user.id, connection_id=conn.id, library_id=lid, library_name=name_map[lid]))
            await db.commit()
            return {"saved": len(library_ids)}

        elif conn.type == "plex":
            library_keys: list[str] = body.get("library_keys", [])
            available = await plex.get_libraries(conn.url, conn.token)
            name_map = {lib["key"]: lib["title"] for lib in available}
            await db.execute(delete(PlexLibrarySelection).where(PlexLibrarySelection.connection_id == conn.id))
            for key in library_keys:
                if key in name_map:
                    db.add(PlexLibrarySelection(user_id=current_user.id, connection_id=conn.id, library_key=key, library_name=name_map[key]))
            await db.commit()
            return {"saved": len(library_keys)}

        else:
            raise HTTPException(status_code=400, detail=f"Unknown connection type: {conn.type}")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Could not reach server: {e}")


@router.post("/connection/{connection_id}")
async def sync_connection(
    connection_id: int,
    background_tasks: BackgroundTasks,
    partial: bool = Query(default=False),
    movie_limit: int = Query(default=0),
    show_limit: int = Query(default=0),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    conn = await _get_connection_or_404(db, connection_id, current_user.id)

    settings_result = await db.execute(select(UserSettings).where(UserSettings.user_id == current_user.id))
    settings = settings_result.scalar_one_or_none()
    if not await _get_effective_tmdb_key(db, settings):
        raise HTTPException(status_code=400, detail="TMDB API key required")

    source_map = {"jellyfin": CollectionSource.jellyfin, "emby": CollectionSource.emby, "plex": CollectionSource.plex}
    source = source_map.get(conn.type)
    if not source:
        raise HTTPException(status_code=400, detail=f"Unknown connection type: {conn.type}")

    job = SyncJob(user_id=current_user.id, source=source, status=SyncStatus.pending)
    db.add(job)
    await db.commit()
    await db.refresh(job)

    runner_map = {"jellyfin": run_jellyfin_sync, "emby": run_emby_sync, "plex": run_plex_sync}
    background_tasks.add_task(runner_map[conn.type], current_user.id, job.id, movie_limit, show_limit, connection_id, partial)
    return {"status": "started", "job_id": job.id, "message": f"{conn.type.capitalize()} sync is running in the background"}


async def _run_full_push(user_id: int, connection_id: int) -> None:
    async_session = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
    async with async_session() as db:
        conn_result = await db.execute(
            select(MediaServerConnection).where(
                MediaServerConnection.id == connection_id,
                MediaServerConnection.user_id == user_id,
            )
        )
        conn = conn_result.scalar_one_or_none()
        if not conn:
            return

        conn_source = CollectionSource(conn.type)
        watched_ids: set[int] = set()
        ratings_map: dict[int, float] = {}

        if conn.push_watched:
            watched_result = await db.execute(
                select(WatchEvent.media_id).where(WatchEvent.user_id == user_id).distinct()
            )
            watched_ids = {row[0] for row in watched_result.all()}

        if conn.push_ratings:
            ratings_result = await db.execute(
                select(Rating.media_id, Rating.rating).where(
                    Rating.user_id == user_id,
                    Rating.rating.isnot(None),
                )
            )
            ratings_map = {row[0]: row[1] for row in ratings_result.all()}

        all_media_ids = watched_ids | set(ratings_map.keys())
        if not all_media_ids:
            print(f"Full push for connection {connection_id}: nothing to push")
            return

        files_result = await db.execute(
            select(CollectionFile.source_id, Collection.media_id)
            .join(Collection, Collection.id == CollectionFile.collection_id)
            .where(
                Collection.user_id == user_id,
                Collection.media_id.in_(all_media_ids),
                CollectionFile.source == conn_source,
                CollectionFile.source_id.isnot(None),
            )
        )
        source_ids_map: dict[int, list[str]] = {}
        for source_id, media_id in files_result.all():
            source_ids_map.setdefault(media_id, []).append(source_id)

        push_tasks = []

        if conn.push_watched:
            for mid in watched_ids:
                for sid in source_ids_map.get(mid, []):
                    if conn.type == "plex":
                        push_tasks.append(plex.mark_watched(conn.url, conn.token, sid))
                    elif conn.type == "jellyfin":
                        push_tasks.append(jellyfin.mark_watched(conn.url, conn.token, conn.server_user_id, sid))
                    elif conn.type == "emby":
                        push_tasks.append(emby.mark_watched(conn.url, conn.token, conn.server_user_id, sid))

        if conn.push_ratings:
            for mid, rating in ratings_map.items():
                for sid in source_ids_map.get(mid, []):
                    if conn.type == "plex":
                        push_tasks.append(plex.set_rating(conn.url, conn.token, sid, rating))
                    elif conn.type == "jellyfin":
                        push_tasks.append(jellyfin.set_rating(conn.url, conn.token, conn.server_user_id, sid, rating))
                    elif conn.type == "emby":
                        push_tasks.append(emby.set_rating(conn.url, conn.token, conn.server_user_id, sid, rating))

        if not push_tasks:
            print(f"Full push for connection {connection_id}: no items found in collection for this server")
            return

        print(f"Full push for connection {connection_id}: pushing {len(push_tasks)} items...")
        results = await asyncio.gather(*push_tasks, return_exceptions=True)
        failed = sum(1 for r in results if isinstance(r, Exception))
        print(f"Full push for connection {connection_id}: {len(push_tasks) - failed}/{len(push_tasks)} succeeded")


@router.post("/connection/{connection_id}/push")
async def push_upstream(
    connection_id: int,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    conn = await _get_connection_or_404(db, connection_id, current_user.id)
    if not conn.push_watched and not conn.push_ratings:
        raise HTTPException(status_code=400, detail="Enable 'Scrob → Server' push flags for this connection first")
    background_tasks.add_task(_run_full_push, current_user.id, connection_id)
    return {"status": "started", "message": "Full upstream sync is running in the background"}


@router.post("/jellyfin")
async def sync_jellyfin(
    background_tasks: BackgroundTasks,
    partial: bool = Query(default=False),
    movie_limit: int = Query(default=0),
    show_limit: int = Query(default=0),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    settings_result = await db.execute(select(UserSettings).where(UserSettings.user_id == current_user.id))
    settings = settings_result.scalar_one_or_none()
    if not await _get_effective_tmdb_key(db, settings):
        raise HTTPException(status_code=400, detail="TMDB API key required")

    conn_result = await db.execute(
        select(MediaServerConnection).where(
            MediaServerConnection.user_id == current_user.id,
            MediaServerConnection.type == "jellyfin",
        ).order_by(MediaServerConnection.id.asc()).limit(1)
    )
    if not conn_result.scalar_one_or_none():
        raise HTTPException(status_code=400, detail="No Jellyfin connection configured")

    job = SyncJob(user_id=current_user.id, source=CollectionSource.jellyfin, status=SyncStatus.pending)
    db.add(job)
    await db.commit()
    await db.refresh(job)

    background_tasks.add_task(run_jellyfin_sync, current_user.id, job.id, movie_limit, show_limit, None, partial)
    return {"status": "started", "job_id": job.id, "message": "Jellyfin sync is running in the background"}


@router.post("/emby")
async def sync_emby(
    background_tasks: BackgroundTasks,
    partial: bool = Query(default=False),
    movie_limit: int = Query(default=0),
    show_limit: int = Query(default=0),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    settings_result = await db.execute(select(UserSettings).where(UserSettings.user_id == current_user.id))
    settings = settings_result.scalar_one_or_none()
    if not await _get_effective_tmdb_key(db, settings):
        raise HTTPException(status_code=400, detail="TMDB API key required")

    conn_result = await db.execute(
        select(MediaServerConnection).where(
            MediaServerConnection.user_id == current_user.id,
            MediaServerConnection.type == "emby",
        ).order_by(MediaServerConnection.id.asc()).limit(1)
    )
    if not conn_result.scalar_one_or_none():
        raise HTTPException(status_code=400, detail="No Emby connection configured")

    job = SyncJob(user_id=current_user.id, source=CollectionSource.emby, status=SyncStatus.pending)
    db.add(job)
    await db.commit()
    await db.refresh(job)

    background_tasks.add_task(run_emby_sync, current_user.id, job.id, movie_limit, show_limit, None, partial)
    return {"status": "started", "job_id": job.id, "message": "Emby sync is running in the background"}


@router.post("/plex")
async def sync_plex(
    background_tasks: BackgroundTasks,
    partial: bool = Query(default=False),
    movie_limit: int = Query(default=0),
    show_limit: int = Query(default=0),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    settings_result = await db.execute(select(UserSettings).where(UserSettings.user_id == current_user.id))
    settings = settings_result.scalar_one_or_none()
    if not await _get_effective_tmdb_key(db, settings):
        raise HTTPException(status_code=400, detail="TMDB API key required")

    conn_result = await db.execute(
        select(MediaServerConnection).where(
            MediaServerConnection.user_id == current_user.id,
            MediaServerConnection.type == "plex",
        ).order_by(MediaServerConnection.id.asc()).limit(1)
    )
    if not conn_result.scalar_one_or_none():
        raise HTTPException(status_code=400, detail="No Plex connection configured")

    job = SyncJob(user_id=current_user.id, source=CollectionSource.plex, status=SyncStatus.pending)
    db.add(job)
    await db.commit()
    await db.refresh(job)

    background_tasks.add_task(run_plex_sync, current_user.id, job.id, movie_limit, show_limit, None, partial)
    return {"status": "started", "job_id": job.id, "message": "Plex sync is running in the background"}


@router.get("/status")
async def get_sync_status(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    query = select(SyncJob).where(SyncJob.user_id == current_user.id).order_by(SyncJob.created_at.desc()).limit(5)
    result = await db.execute(query)
    jobs = result.scalars().all()
    return jobs


@router.post("/heal")
async def heal_metadata(
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Re-enrich all collection items that are missing poster/date metadata."""
    result = await db.execute(select(UserSettings).where(UserSettings.user_id == current_user.id))
    settings = result.scalar_one_or_none()
    if not await _get_effective_tmdb_key(db, settings):
        raise HTTPException(status_code=400, detail="TMDB API key required")

    background_tasks.add_task(run_heal, current_user.id, settings.tmdb_api_key)
    return {"status": "started", "message": "Metadata heal is running in the background"}


async def run_heal(user_id: int, api_key: str):
    from models.show import Show
    async_session = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
    async with async_session() as db:
        try:
            # Load all collection media missing poster_path
            coll_q = await db.execute(
                select(Media)
                .join(Collection, Collection.media_id == Media.id)
                .where(
                    Collection.user_id == user_id,
                    Media.poster_path.is_(None),
                )
            )
            items = coll_q.scalars().all()

            movies = [m for m in items if m.media_type == MediaType.movie and m.tmdb_id]
            episodes = [m for m in items if m.media_type == MediaType.episode and m.show_id and m.season_number is not None and m.episode_number is not None]

            if not movies and not episodes:
                print(f"Heal: nothing to fix for user {user_id}")
                return

            print(f"Heal: {len(movies)} movies, {len(episodes)} episodes to re-enrich for user {user_id}")

            # Load show tmdb_ids for episodes
            show_ids = list({m.show_id for m in episodes})
            show_tmdb_map: dict[int, int] = {}
            if show_ids:
                shows_q = await db.execute(select(Show).where(Show.id.in_(show_ids)))
                for s in shows_q.scalars().all():
                    if s.tmdb_id:
                        show_tmdb_map[s.id] = s.tmdb_id

            to_enrich = [(m, None) for m in movies] + [
                (m, show_tmdb_map[m.show_id]) for m in episodes if m.show_id in show_tmdb_map
            ]

            await batch_enrich_items(to_enrich, api_key=api_key)
            await db.commit()
            print(f"Heal complete for user {user_id}: processed {len(to_enrich)} items")
        except Exception as e:
            print(f"Heal failed for user {user_id}: {e}")
            import traceback
            traceback.print_exc()


@router.post("/abort")
async def abort_sync(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Aborts any pending or running sync jobs for the current user."""
    await db.execute(
        update(SyncJob)
        .where(SyncJob.user_id == current_user.id)
        .where(SyncJob.status.in_([SyncStatus.pending, SyncStatus.running]))
        .values(status=SyncStatus.failed, error_message="Aborted by user", updated_at=func.now())
    )
    await db.commit()
    return {"status": "ok", "message": "All active sync jobs have been marked as aborted"}
