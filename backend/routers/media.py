import asyncio
import httpx
import re
import urllib.parse
import uuid
from typing import Optional
from pydantic import BaseModel
from fastapi import APIRouter, Depends, Query, HTTPException, Request
from fastapi.responses import StreamingResponse, Response
from starlette.background import BackgroundTask
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, or_, and_, func, cast as sa_cast, Text, delete
from sqlalchemy.orm import joinedload
from sqlalchemy.orm.attributes import flag_modified

from db import get_db, AsyncSessionLocal
from models.media import Media
from models.collection import Collection, CollectionFile
from models.connections import MediaServerConnection
from models.events import WatchEvent
from models.ratings import Rating
from models.playback_progress import PlaybackProgress
from models.base import MediaType, CollectionSource
from models.lists import List as UserList, ListItem
from models.media_request import MediaRequest, RequestStatus
from models.profile import UserProfileData
from core import tmdb
from dependencies import get_current_user
from models.users import User, UserSettings
from models.show import Show as ShowModel
from models.global_settings import GlobalSettings
from models.blocklist import BlocklistItem

router = APIRouter()


from utils.media_uri import MediaURI


class SessionReportRequest(BaseModel):
    connection_id: int
    state: str  # "playing" | "progress" | "paused" | "stopped"
    position_ms: int = 0
    duration_ms: int = 0
    file_id: int | None = None
    plex_session_id: str | None = None  # Plex Universal Transcoder session ID for keepalive pings


# Simple TTL cache for the /for-you endpoint — keyed by user_id
import time as _time
_FOR_YOU_CACHE: dict[int, tuple[float, dict]] = {}
_FOR_YOU_TTL = 900  # 15 minutes

# TMDB genre name → ID mappings (used to convert filter names to discover API IDs)
MOVIE_GENRE_IDS: dict[str, int] = {
    "Action": 28, "Adventure": 12, "Animation": 16, "Comedy": 35,
    "Crime": 80, "Documentary": 99, "Drama": 18, "Family": 10751,
    "Fantasy": 14, "History": 36, "Horror": 27, "Music": 10402,
    "Mystery": 9648, "Romance": 10749, "Science Fiction": 878,
    "Thriller": 53, "War": 10752, "Western": 37,
}

TV_GENRE_IDS: dict[str, int] = {
    "Action & Adventure": 10759, "Animation": 16, "Comedy": 35,
    "Crime": 80, "Documentary": 99, "Drama": 18, "Family": 10751,
    "Kids": 10762, "Mystery": 9648, "News": 10763, "Reality": 10764,
    "Sci-Fi & Fantasy": 10765, "Soap": 10766, "Talk": 10767,
    "War & Politics": 10768, "Western": 37,
}

MOVIE_GENRE_NAMES: dict[int, str] = {v: k for k, v in MOVIE_GENRE_IDS.items()}
TV_GENRE_NAMES: dict[int, str] = {v: k for k, v in TV_GENRE_IDS.items()}


def _genre_weight(genre_ids: list[int], liked: set[str], disliked: set[str], name_map: dict[int, str]) -> float:
    """Weighted random score for an item based on user genre preferences."""
    if not liked and not disliked:
        return 1.0
    score = 1.0
    for gid in genre_ids:
        name = name_map.get(gid)
        if name in liked:
            score += 2.0
        elif name in disliked:
            score -= 1.5
    return max(0.05, score)


def _filter_disliked(
    results: list[dict],
    disliked: set[str],
    liked: set[str],
    name_map: dict[int, str],
) -> list[dict]:
    """Drop items whose only genres are disliked and none are liked."""
    if not disliked:
        return results
    out = []
    for r in results:
        gids = r.get("genre_ids", [])
        names = {name_map.get(gid) for gid in gids} - {None}
        has_liked = bool(names & liked)
        has_only_disliked = bool(names) and names <= disliked
        if not has_only_disliked or has_liked:
            out.append(r)
    return out


TV_STATUS_IDS: dict[str, int] = {
    "Returning Series": 0, "Planned": 1, "In Production": 2,
    "Ended": 3, "Canceled": 4,
}

_URI_TYPE_PREFIX: dict[str, str] = {"series": "s", "movie": "m", "episode": "e"}


async def resolve_uris_to_internal_ids(
    db: AsyncSession,
    uris: list[str],
) -> dict[str, int]:
    """Resolve URI strings to internal DB primary keys via media_aliases.

    Returns mapping uri_string -> internal_id. URIs with no alias are omitted.
    """
    if not uris:
        return {}

    from models.media_alias import MediaAlias

    parsed: list[tuple[str, MediaURI]] = []
    for uri_str in uris:
        try:
            parsed.append((uri_str, MediaURI.parse(uri_str)))
        except ValueError:
            pass

    if not parsed:
        return {}

    conditions = [
        and_(
            MediaAlias.provider == uri.provider,
            MediaAlias.external_id == uri.id,
            MediaAlias.media_type == uri.media_type,
        )
        for _, uri in parsed
    ]

    q = await db.execute(
        select(
            MediaAlias.provider,
            MediaAlias.external_id,
            MediaAlias.media_type,
            MediaAlias.internal_id,
        ).where(or_(*conditions))
    )

    result: dict[str, int] = {}
    for provider, ext_id, media_type, internal_id in q.all():
        prefix = _URI_TYPE_PREFIX.get(media_type.value, "")
        if prefix:
            result[f"{provider}:{prefix}:{ext_id}"] = internal_id

    return result


async def enrich_with_state(
    db: AsyncSession,
    user_id: int,
    items: list[dict],
    apply_tvdb_metadata: bool = True,
) -> list[dict]:
    """Add watched, in_lists, collection_pct, and is_monitored fields to a list of media items.

    apply_tvdb_metadata: when True and user prefers TVDB, overwrite TMDB-sourced title/poster
    with locally-stored TVDB data for shows already in the DB. Default True for discovery/search
    callers. Pass False for callers that serve local DB items (history, library, lists) whose
    metadata is already correct.
    """
    # 0. Preliminaries & inputs lookup for TVDB/TMDB shows
    show_tmdb_ids_for_lookup = []
    show_tvdb_only_ids_for_lookup = []
    for i in items:
        t = i.get("type")
        tid = i.get("tmdb_id")
        if t in ("series", "season") and tid:
            show_tmdb_ids_for_lookup.append(tid)
        elif t == "episode":
            s_tmdb = i.get("show_tmdb_id")
            if s_tmdb:
                show_tmdb_ids_for_lookup.append(s_tmdb)
            elif i.get("show_tvdb_id"):
                show_tvdb_only_ids_for_lookup.append(int(i["show_tvdb_id"]))

    # Load local shows for URI backfilling
    input_tmdb_to_local_show_pre = {}
    tvdb_only_show_map_pre = {}
    if show_tmdb_ids_for_lookup:
        _int_ids = [k for k in show_tmdb_ids_for_lookup if isinstance(k, int)]
        if _int_ids:
            local_shows_q = await db.execute(select(ShowModel).where(ShowModel.tmdb_id.in_(_int_ids)))
            input_tmdb_to_local_show_pre = {s.tmdb_id: s for s in local_shows_q.scalars().all()}
    if show_tvdb_only_ids_for_lookup:
        tvdb_only_q = await db.execute(select(ShowModel).where(ShowModel.tvdb_id.in_(show_tvdb_only_ids_for_lookup)))
        tvdb_only_show_map_pre = {s.tvdb_id: s for s in tvdb_only_q.scalars().all()}

    # Backfill uri_id and show_uri_id on all items first
    for item in items:
        tid = item.get("tmdb_id")
        t = item.get("type")
        if not item.get("uri_id"):
            tvdb_id = item.get("tvdb_id")
            if tid:
                if t == "movie":
                    item["uri_id"] = f"tmdb:m:{tid}"
                elif t in ("series", "season"):
                    local_s = input_tmdb_to_local_show_pre.get(tid)
                    item["uri_id"] = (local_s.uri_id if local_s else None) or f"tmdb:s:{tid}"
                elif t == "episode":
                    item["uri_id"] = f"tmdb:e:{tid}"
                elif t == "person":
                    item["uri_id"] = f"tmdb:p:{tid}"
            elif tvdb_id and t in ("series", "season"):
                local_s = tvdb_only_show_map_pre.get(tvdb_id)
                item["uri_id"] = (local_s.uri_id if local_s else None) or f"tvdb:s:{tvdb_id}"

        if t == "episode" and not item.get("show_uri_id"):
            s_tmdb = item.get("show_tmdb_id")
            local_s = input_tmdb_to_local_show_pre.get(s_tmdb)
            if local_s and local_s.uri_id:
                item["show_uri_id"] = local_s.uri_id
            elif s_tmdb:
                item["show_uri_id"] = f"tmdb:s:{s_tmdb}"

    # --- URI resolution (hybrid mode) ---
    # When items carry uri_id fields, resolve them to internal DB PKs via media_aliases.
    # Resolved internal_id is stored as _internal_id on each item for downstream use.
    raw_uris = [i["uri_id"] for i in items if i.get("uri_id")]
    if raw_uris:
        uri_to_internal = await resolve_uris_to_internal_ids(db, raw_uris)
        for item in items:
            if item.get("uri_id") and item["uri_id"] in uri_to_internal:
                item["_internal_id"] = uri_to_internal[item["uri_id"]]

    movie_tmdb_ids = [i["tmdb_id"] for i in items if i.get("type") == "movie" and i.get("tmdb_id")]
    show_tmdb_ids  = [i["tmdb_id"] for i in items if i.get("type") == "series" and i.get("tmdb_id")]
    # TVDB-only show IDs referenced by episode items (no tmdb counterpart)
    show_tvdb_only_ids: list[int] = []
    for i in items:
        if i.get("type") == "episode":
            s_tmdb = i.get("show_tmdb_id")
            if s_tmdb:
                show_tmdb_ids.append(s_tmdb)
            elif i.get("show_tvdb_id"):
                show_tvdb_only_ids.append(int(i["show_tvdb_id"]))
    ep_tmdb_ids    = [i["tmdb_id"] for i in items if i.get("type") == "episode" and i.get("tmdb_id")]
    all_tmdb_ids   = [i["tmdb_id"] for i in items if i.get("tmdb_id")]

    # URI-based show lookups (series items resolved via media_aliases but with no tmdb_id)
    uri_resolved_show_ids: list[int] = [
        i["_internal_id"] for i in items
        if i.get("type") == "series" and i.get("_internal_id") and not i.get("tmdb_id")
    ]

    if not all_tmdb_ids and not uri_resolved_show_ids and not show_tvdb_only_ids:
        return items

    # --- Radarr / Sonarr state (Request button logic) ---
    settings_q = await db.execute(select(UserSettings).where(UserSettings.user_id == user_id))
    settings = settings_q.scalar_one_or_none()
    include_specials = False
    primary_source = "tmdb"
    if settings and settings.preferences:
        include_specials = settings.preferences.get("include_specials", False)
        primary_source = settings.preferences.get("primary_metadata_source", "tmdb")
    gs = await _get_global_settings(db)

    monitored_status = {} # tmdb_id -> bool (currently unpopulated; is_monitored set by show detail endpoint)
    request_enabled_map: dict = {}  # (tmdb_id or uri_id) -> bool

    radarr_cfg = _effective_radarr(settings, gs)
    sonarr_cfg = _effective_sonarr(settings, gs)
    if radarr_cfg or sonarr_cfg:
        radarr_ready = radarr_cfg is not None
        sonarr_ready = sonarr_cfg is not None
        for item in items:
            tid = item.get("tmdb_id")
            t = item.get("type")
            key = tid if tid is not None else item.get("uri_id")
            if t == "movie":
                request_enabled_map[key] = radarr_ready
            elif t in ("series", "episode"):
                request_enabled_map[key] = sonarr_ready

    # --- Pending/rejected request state ---
    request_status_map: dict[int | str, str] = {}
    lookup_keys: dict[tuple[str, str], list[int | str]] = {}
    
    for item in items:
        tid = item.get("tmdb_id")
        t = item.get("type")
        if not t:
            continue
            
        lookup_tid = tid
        lookup_type = t
        if t in ("episode", "season"):
            lookup_tid = item.get("show_tmdb_id")
            lookup_type = "series"

        _req_uri = None
        if t in ("episode", "season"):
            _req_uri = item.get("show_uri_id")
            if not _req_uri and item.get("show_tvdb_id"):
                _req_uri = f"tvdb:s:{item.get('show_tvdb_id')}"
            if not _req_uri and t == "season":
                _req_uri = item.get("uri_id")
        else:
            _req_uri = item.get("uri_id")

        if not _req_uri and lookup_type in ("movie", "series") and lookup_tid:
            _req_uri = f"tmdb:{'m' if lookup_type == 'movie' else 's'}:{lookup_tid}"

        if lookup_type in ("movie", "series") and _req_uri:
            item_key = tid if tid is not None else item.get("uri_id")
            if item_key is not None:
                lookup_keys.setdefault((_req_uri, lookup_type), []).append(item_key)

    if lookup_keys:
        uri_ids = [uri for uri, _ in lookup_keys.keys()]
        req_q = await db.execute(
            select(MediaRequest)
            .where(
                MediaRequest.user_id == user_id,
                MediaRequest.uri_id.in_(uri_ids),
                MediaRequest.status.in_([RequestStatus.pending, RequestStatus.rejected]),
            )
            .order_by(MediaRequest.updated_at.desc())
        )
        seen_requests = {}
        for req in req_q.scalars().all():
            key_pair = (req.uri_id, req.media_type)
            if key_pair not in seen_requests:
                seen_requests[key_pair] = req
                
        for (uri_id, media_type), req in seen_requests.items():
            for item_key in lookup_keys.get((uri_id, media_type), []):
                request_status_map[item_key] = req.status.value

    # --- Watched state ---
    watched_movies: set[int] = set()
    if movie_tmdb_ids:
        q = await db.execute(
            select(Media.tmdb_id)
            .join(WatchEvent, WatchEvent.media_id == Media.id)
            .where(WatchEvent.user_id == user_id, Media.tmdb_id.in_(movie_tmdb_ids), Media.media_type == MediaType.movie)
            .distinct()
        )
        watched_movies = {r[0] for r in q.all()}

    watched_shows: set[int] = set()
    show_watched_count_map: dict[int, int] = {}
    collected_map: dict[int, int] = {}
    overrides_by_show = {}
    show_seasons_map: dict[int, list] = {}
    total_map: dict[int, int] = {}
    show_status_map: dict[int, str] = {}
    adjusted_show_season_ep_counts = {}
    input_tmdb_to_local_show = {}
    resolved_tvdb_ids = {}

    # For shows resolved via URI aliases (no tmdb_id), inject into the shared maps.
    # Key: tmdb_id integer when available, otherwise show.uri_id string.
    # uri_id strings never collide with integer TMDB IDs and never produce negative values.
    if uri_resolved_show_ids:
        uri_shows_q = await db.execute(
            select(ShowModel).where(ShowModel.id.in_(uri_resolved_show_ids))
        )
        for s in uri_shows_q.scalars().all():
            if s.tmdb_id is not None:
                key: int | str = s.tmdb_id
            elif s.uri_id:
                key = s.uri_id
            else:
                import logging as _log
                _log.getLogger(__name__).warning("URI-resolved show id=%s has no tmdb_id and no uri_id — skipping", s.id)
                continue
            input_tmdb_to_local_show[key] = s
            if key not in show_tmdb_ids:
                show_tmdb_ids.append(key)

    # TVDB-only show lookup (for episode items that reference shows by tvdb_id with no tmdb counterpart)
    tvdb_only_show_map: dict[int, ShowModel] = {}
    if show_tvdb_only_ids:
        tvdb_only_q = await db.execute(
            select(ShowModel).where(ShowModel.tvdb_id.in_(show_tvdb_only_ids))
        )
        tvdb_only_show_map = {s.tvdb_id: s for s in tvdb_only_q.scalars().all()}

    if show_tmdb_ids:
        # 1. Look up any existing local shows in the DB by their positive TMDB ID.
        # show_tmdb_ids may contain uri_id strings for TVDB-only shows — exclude them.
        _int_show_tmdb_ids = [k for k in show_tmdb_ids if isinstance(k, int)]
        local_shows_q = await db.execute(
            select(ShowModel).where(ShowModel.tmdb_id.in_(_int_show_tmdb_ids)) if _int_show_tmdb_ids else
            select(ShowModel).where(False)
        )
        local_shows_by_tmdb_id = {s.tmdb_id: s for s in local_shows_q.scalars().all()}

        # 1b. For IDs not found by direct tmdb_id match, try media_aliases.
        # This resolves the cross-provider case: TMDB search result → TVDB-only library show.
        unmatched_tids = [tid for tid in show_tmdb_ids if isinstance(tid, int) and tid > 0 and tid not in local_shows_by_tmdb_id]
        if unmatched_tids:
            try:
                from models.media_alias import MediaAlias
                alias_q = await db.execute(
                    select(MediaAlias.external_id, MediaAlias.internal_id).where(
                        MediaAlias.provider == "tmdb",
                        MediaAlias.external_id.in_([str(t) for t in unmatched_tids]),
                        MediaAlias.media_type == "series",
                    )
                )
                alias_rows = alias_q.all()
                if alias_rows:
                    alias_internal_ids = [r[1] for r in alias_rows]
                    alias_shows_q = await db.execute(
                        select(ShowModel).where(ShowModel.id.in_(alias_internal_ids))
                    )
                    alias_shows_by_id = {s.id: s for s in alias_shows_q.scalars().all()}
                    for ext_id_str, internal_id in alias_rows:
                        show_row = alias_shows_by_id.get(internal_id)
                        if show_row:
                            local_shows_by_tmdb_id[int(ext_id_str)] = show_row
            except Exception:
                pass

        # 2. Track resolved TVDB IDs and check for missing ones (integer keys only)
        for tid in show_tmdb_ids:
            if not isinstance(tid, int):
                continue
            if tid in local_shows_by_tmdb_id and local_shows_by_tmdb_id[tid].tvdb_id:
                resolved_tvdb_ids[tid] = local_shows_by_tmdb_id[tid].tvdb_id

        # Fetch external IDs for shows we don't have local mappings for.
        # Step 1: try alias table (tmdb provider → tvdb provider for same internal show).
        # Step 2: fall back to TMDB API only for IDs not in alias table.
        missing_tvdb_ids = [tid for tid in show_tmdb_ids if isinstance(tid, int) and tid > 0 and tid not in resolved_tvdb_ids]
        if missing_tvdb_ids:
            try:
                from models.media_alias import MediaAlias as _MA
                alias_tvdb_q = await db.execute(
                    select(_MA.external_id.label("tmdb_ext"), _MA.internal_id)
                    .where(
                        _MA.provider == "tmdb",
                        _MA.external_id.in_([str(t) for t in missing_tvdb_ids]),
                        _MA.media_type == "series",
                    )
                )
                alias_tmdb_rows = alias_tvdb_q.all()
                if alias_tmdb_rows:
                    internal_ids_needed = [r[1] for r in alias_tmdb_rows]
                    tvdb_alias_q = await db.execute(
                        select(_MA.internal_id, _MA.external_id)
                        .where(
                            _MA.provider == "tvdb",
                            _MA.internal_id.in_(internal_ids_needed),
                            _MA.media_type == "series",
                        )
                    )
                    internal_to_tvdb = {r[0]: int(r[1]) for r in tvdb_alias_q.all()}
                    for tmdb_ext_str, internal_id in alias_tmdb_rows:
                        if internal_id in internal_to_tvdb:
                            resolved_tvdb_ids[int(tmdb_ext_str)] = internal_to_tvdb[internal_id]
            except Exception:
                pass

            still_missing = [tid for tid in missing_tvdb_ids if isinstance(tid, int) and tid not in resolved_tvdb_ids]
            if still_missing:
                tmdb_key = await get_user_tmdb_key(db, user_id)
                if check_tmdb_key(tmdb_key):
                    async def fetch_ext_ids(tid: int):
                        try:
                            ext = await tmdb.get_external_ids(tid, "tv", api_key=tmdb_key)
                            return tid, ext.get("tvdb_id")
                        except Exception:
                            return tid, None

                    ext_results = await asyncio.gather(*[fetch_ext_ids(tid) for tid in still_missing])
                    for tid, tvdb_id_val in ext_results:
                        if tvdb_id_val:
                            try:
                                resolved_tvdb_ids[tid] = int(tvdb_id_val)
                            except (ValueError, TypeError):
                                pass

        # 3. Query local shows where TVDB ID matches (handles TVDB-only shows stored with negative TMDB IDs)
        tvdb_only_shows_by_tvdb_id = {}
        if resolved_tvdb_ids:
            tvdb_shows_q = await db.execute(
                select(ShowModel).where(ShowModel.tvdb_id.in_(list(resolved_tvdb_ids.values())))
            )
            tvdb_only_shows_by_tvdb_id = {s.tvdb_id: s for s in tvdb_shows_q.scalars().all()}

        # 4. Map the input TMDB ID to the local ShowModel row and local DB ID
        input_tmdb_to_local_show = {}
        local_show_id_to_input_tmdb_id = {}
        for tid in show_tmdb_ids:
            show_row = None
            if tid in local_shows_by_tmdb_id:
                show_row = local_shows_by_tmdb_id[tid]
            elif resolved_tvdb_ids.get(tid) in tvdb_only_shows_by_tvdb_id:
                show_row = tvdb_only_shows_by_tvdb_id[resolved_tvdb_ids[tid]]

            if show_row:
                input_tmdb_to_local_show[tid] = show_row
                local_show_id_to_input_tmdb_id[show_row.id] = tid

        from models.season_override import ShowSeasonOverride
        # Query overrides using the FK (source_show_id / target_show_id)
        local_show_ids = list(local_show_id_to_input_tmdb_id.keys())
        overrides_q = await db.execute(
            select(ShowSeasonOverride).where(
                ShowSeasonOverride.user_id == user_id,
                or_(
                    ShowSeasonOverride.source_show_id.in_(local_show_ids),
                    ShowSeasonOverride.target_show_id.in_(local_show_ids),
                )
            )
        ) if local_show_ids else (await db.execute(select(ShowSeasonOverride).where(False)))
        overrides = overrides_q.scalars().all()

        for override in overrides:
            src_tid = local_show_id_to_input_tmdb_id.get(override.source_show_id, override.source_show_id)
            tgt_tid = local_show_id_to_input_tmdb_id.get(override.target_show_id, override.target_show_id) if override.target_show_id else None
            overrides_by_show.setdefault(src_tid, []).append(override)
            if tgt_tid:
                overrides_by_show.setdefault(tgt_tid, []).append(override)

        # Build sets of watched/collected episodes per show
        show_watched_eps = {tid: set() for tid in show_tmdb_ids}
        show_collected_eps = {tid: set() for tid in show_tmdb_ids}

        # 1. Fetch raw watched episodes linked directly
        if local_show_id_to_input_tmdb_id:
            watched_eps_q = await db.execute(
                select(ShowModel.id, Media.season_number, Media.episode_number)
                .join(WatchEvent, WatchEvent.media_id == Media.id)
                .join(ShowModel, ShowModel.id == Media.show_id)
                .where(
                    WatchEvent.user_id == user_id,
                    Media.media_type == MediaType.episode,
                    Media.season_number.isnot(None),
                    Media.episode_number.isnot(None),
                    ShowModel.id.in_(list(local_show_id_to_input_tmdb_id.keys())),
                )
            )
            for show_id, sn, en in watched_eps_q.all():
                show_tmdb_id = local_show_id_to_input_tmdb_id[show_id]
                if include_specials or sn != 0:
                    show_watched_eps[show_tmdb_id].add((sn, en))

        # 2. Fetch raw collected episodes linked directly
        if local_show_id_to_input_tmdb_id:
            collected_eps_q = await db.execute(
                select(ShowModel.id, Media.season_number, Media.episode_number)
                .join(Collection, Collection.media_id == Media.id)
                .join(ShowModel, ShowModel.id == Media.show_id)
                .where(
                    Collection.user_id == user_id,
                    Media.media_type == MediaType.episode,
                    Media.season_number.isnot(None),
                    Media.episode_number.isnot(None),
                    ShowModel.id.in_(list(local_show_id_to_input_tmdb_id.keys())),
                )
            )
            for show_id, sn, en in collected_eps_q.all():
                show_tmdb_id = local_show_id_to_input_tmdb_id[show_id]
                if include_specials or sn != 0:
                    show_collected_eps[show_tmdb_id].add((sn, en))

        # 3. Merge overrides
        async def merge_override(this_show_tmdb_id, this_season, other_show_id, other_season):
            other_show_q = await db.execute(
                select(ShowModel).where(ShowModel.id == other_show_id)
            )
            other_show = other_show_q.scalar_one_or_none()
            if other_show:
                # Watched episodes
                other_watched_q = await db.execute(
                    select(Media.episode_number)
                    .join(WatchEvent, WatchEvent.media_id == Media.id)
                    .where(
                        Media.show_id == other_show.id,
                        WatchEvent.user_id == user_id,
                        Media.media_type == MediaType.episode,
                        Media.season_number == other_season,
                        Media.episode_number.isnot(None),
                    )
                )
                for (en,) in other_watched_q.all():
                    show_watched_eps[this_show_tmdb_id].add((this_season, en))

                # Collected episodes
                other_coll_q = await db.execute(
                    select(Media.episode_number)
                    .join(Collection, Collection.media_id == Media.id)
                    .where(
                        Media.show_id == other_show.id,
                        Collection.user_id == user_id,
                        Media.media_type == MediaType.episode,
                        Media.season_number == other_season,
                        Media.episode_number.isnot(None),
                    )
                )
                for (en,) in other_coll_q.all():
                    show_collected_eps[this_show_tmdb_id].add((this_season, en))

        for override in overrides:
            src_tid = local_show_id_to_input_tmdb_id.get(override.source_show_id)
            tgt_tid = local_show_id_to_input_tmdb_id.get(override.target_show_id) if override.target_show_id else None
            if src_tid is not None and src_tid in show_tmdb_ids and override.target_show_id:
                await merge_override(src_tid, override.source_season_number, override.target_show_id, override.target_season_number)
            if tgt_tid is not None and tgt_tid in show_tmdb_ids and override.source_show_id:
                await merge_override(tgt_tid, override.target_season_number, override.source_show_id, override.source_season_number)

        show_watched_count_map = {tid: len(eps) for tid, eps in show_watched_eps.items()}
        collected_map = {tid: len(eps) for tid, eps in show_collected_eps.items()}

    watched_episodes: set[int] = set()
    if ep_tmdb_ids:
        q = await db.execute(
            select(Media.tmdb_id)
            .join(WatchEvent, WatchEvent.media_id == Media.id)
            .where(WatchEvent.user_id == user_id, Media.tmdb_id.in_(ep_tmdb_ids), Media.media_type == MediaType.episode)
            .distinct()
        )
        watched_episodes = {r[0] for r in q.all()}

    # --- List membership ---
    user_list_ids_q = await db.execute(select(UserList.id).where(UserList.user_id == user_id))
    user_list_ids = [r[0] for r in user_list_ids_q.all()]

    list_membership_by_key: dict[tuple[str, Optional[int], Optional[int]], list[int]] = {}
    all_uris = [i["uri_id"] for i in items if i.get("uri_id")]
    if user_list_ids and all_uris:
        q_lists = await db.execute(
            select(Media.uri_id, Media.season_number, Media.episode_number, ListItem.list_id)
            .join(ListItem, ListItem.media_id == Media.id)
            .where(ListItem.list_id.in_(user_list_ids), Media.uri_id.in_(all_uris))
            .distinct()
        )
        for uri, sn, en, list_id in q_lists.all():
            if uri:
                list_membership_by_key.setdefault((uri, sn, en), []).append(list_id)

    # --- Collection pct and watched status for shows ---
    show_pct: dict[int, int] = {}
    show_aired_count: dict[int, int] = {}
    show_tvdb_ids: dict[int, int] = {}
    if show_tmdb_ids:
        # Total episodes from TMDB metadata.
        # Check local DB first for existing show rows.
        total_map: dict[int, int] = {}
        show_status_map: dict[int, str] = {}
        show_seasons_map: dict[int, list] = {}
        show_ep_tmdb_ids: dict[int, set[int]] = {} # show_tmdb_id -> {ep_tmdb_id, ...}

        # Populate show_tvdb_ids with already resolved TVDB IDs
        show_tvdb_ids.update(resolved_tvdb_ids)

        for tid, show_row in input_tmdb_to_local_show.items():
            tmdb_data = show_row.tmdb_data
            status = show_row.status
            tvdb_id = show_row.tvdb_id
            
            seasons = (tmdb_data or {}).get("seasons", [])
            show_status_map[tid] = status or ""
            show_seasons_map[tid] = seasons
            if tvdb_id:
                show_tvdb_ids[tid] = tvdb_id
            elif tmdb_data:
                ext_tvdb = (tmdb_data.get("external_ids") or {}).get("tvdb_id")
                if ext_tvdb:
                    try:
                        show_tvdb_ids[tid] = int(ext_tvdb)
                    except (ValueError, TypeError):
                        pass
            total_map[tid] = sum(
                s.get("episode_count", 0) for s in seasons if (include_specials or s.get("season_number", 0) != 0)
            )

        # 2. For shows not in local DB (or to ensure accuracy), fetch details from TMDB
        # Only positive integer keys are valid TMDB IDs; uri_id string keys are TVDB-only.
        missing_show_ids = [tid for tid in show_tmdb_ids if isinstance(tid, int) and tid > 0 and tid not in total_map]
        
        # We also need to get ALL episode TMDB IDs for these shows to correctly identify
        # watched episodes that might not have a show_id link.
        # This is expensive, so we only do it if the user has watched episodes.
        tmdb_key = await get_user_tmdb_key(db, user_id)
        if show_tmdb_ids and check_tmdb_key(tmdb_key):
            async def fetch_show_and_seasons(tid: int):
                try:
                    data = await tmdb.get_show(tid, api_key=tmdb_key)
                    ep_ids = set()
                    # Fallback match by show_id or TMDB ID. mark_show_watched usually sets show_id.
                    return tid, data, ep_ids
                except Exception:
                    return tid, None, set()

            if missing_show_ids:
                missing_results = await asyncio.gather(*[fetch_show_and_seasons(tid) for tid in missing_show_ids])
                for tid, data, _ in missing_results:
                    if data:
                        seasons = data.get("seasons", [])
                        show_status_map[tid] = data.get("status", "")
                        show_seasons_map[tid] = seasons
                        tv_id = data.get("external_ids", {}).get("tvdb_id")
                        if tv_id:
                            show_tvdb_ids[tid] = int(tv_id)
                        total_map[tid] = sum(
                            s.get("episode_count", 0) for s in seasons if (include_specials or s.get("season_number", 0) != 0)
                        )

        for tmdb_id in show_tmdb_ids:
            # Get native seasons
            seasons = show_seasons_map.get(tmdb_id, [])
            show_season_ep_counts = {
                s["season_number"]: s.get("episode_count", 0)
                for s in seasons if (include_specials or s.get("season_number", 0) != 0)
            }

            # Apply overrides (keyed by show.id FK)
            this_show = input_tmdb_to_local_show.get(tmdb_id)
            show_overrides = overrides_by_show.get(tmdb_id, [])
            for override in show_overrides:
                src_show_id = override.source_show_id
                tgt_show_id = override.target_show_id
                is_source = this_show and this_show.id == src_show_id
                is_target = this_show and this_show.id == tgt_show_id
                if is_source:
                    # Remapped away! Exclude this season's episodes
                    if override.source_season_number in show_season_ep_counts:
                        del show_season_ep_counts[override.source_season_number]
                elif is_target and tgt_show_id:
                    # Remapped to! Include the source season's episodes
                    src_show_q = await db.execute(
                        select(ShowModel).where(ShowModel.id == src_show_id)
                    )
                    src_show = src_show_q.scalar_one_or_none()
                    ep_count = 0
                    if src_show and src_show.tmdb_data:
                        src_seasons = src_show.tmdb_data.get("seasons", [])
                        src_season_meta = next((s for s in src_seasons if s.get("season_number") == override.source_season_number), None)
                        if src_season_meta:
                            ep_count = src_season_meta.get("episode_count", 0)
                    show_season_ep_counts[override.target_season_number] = ep_count

            adjusted_show_season_ep_counts[tmdb_id] = show_season_ep_counts
            # Now update total_map!
            total_map[tmdb_id] = sum(show_season_ep_counts.values())

        for tmdb_id in show_tmdb_ids:
            total = total_map.get(tmdb_id, 0)
            collected = collected_map.get(tmdb_id, 0)
            show_pct[tmdb_id] = min(100, int((collected / total) * 100)) if total > 0 else 0

        # --- Aired counts for 'watched' logic ---
        show_aired_count = {tid: total_map.get(tid, 0) for tid in show_tmdb_ids}

        # Active shows count unaired episodes; fetch last_episode_to_air to count aired only,
        # unless prefetched in _last_episode_to_air.
        prefetched: dict[int, dict] = {}
        if primary_source != "tvdb":
            prefetched = {
                item["tmdb_id"]: item["_last_episode_to_air"]
                for item in items
                if item.get("type") == "series" and item.get("_last_episode_to_air")
            }
        FINAL_STATUSES = {"Ended", "Canceled"}
        needs_live_call = []
        if primary_source != "tvdb":
            needs_live_call = [
                tid for tid in show_tmdb_ids
                if isinstance(tid, int) and tid > 0  # uri_id string keys are not TMDB IDs
                and tid not in prefetched
                and (0 < show_pct.get(tid, 0) < 100 or 0 < show_watched_count_map.get(tid, 0))
                and show_status_map.get(tid, "") not in FINAL_STATUSES
            ]
        if needs_live_call:
            tmdb_key = await get_user_tmdb_key(db, user_id)
            if check_tmdb_key(tmdb_key):
                async def fetch_last_aired(tid: int) -> tuple[int, dict | None]:
                    try:
                        return tid, await tmdb.get_show_light(tid, api_key=tmdb_key)
                    except Exception:
                        return tid, None

                live_results = await asyncio.gather(*[fetch_last_aired(tid) for tid in needs_live_call])
                for tid, data in live_results:
                    if data and data.get("last_episode_to_air"):
                        prefetched[tid] = data["last_episode_to_air"]

        if primary_source != "tvdb":
            for tid, last_ep in prefetched.items():
                if not last_ep:
                    continue
                last_season = last_ep.get("season_number", 0)
                last_ep_num = last_ep.get("episode_number", 0)
                
                show_season_ep_counts = adjusted_show_season_ep_counts.get(tid, {})
                # Sum completed seasons before the current airing season, plus episodes aired so far in it.
                aired_total = sum(
                    show_season_ep_counts.get(s_num, 0)
                    for s_num in show_season_ep_counts
                    if (s_num < last_season if include_specials else 0 < s_num < last_season)
                ) + last_ep_num
                show_aired_count[tid] = aired_total
                collected = collected_map.get(tid, 0)
                show_pct[tid] = min(100, int((collected / aired_total) * 100)) if aired_total > 0 else 0

        # Now we can accurately set watched_shows
        watched_shows = {
            tid for tid in show_tmdb_ids
            if show_watched_count_map.get(tid, 0) > 0
            and show_watched_count_map.get(tid, 0) >= show_aired_count.get(tid, 0)
        }

    # --- Collection state for movies/episodes ---
    collected_movie_ids: set[int] = set()
    if movie_tmdb_ids:
        coll_q = await db.execute(
            select(Media.tmdb_id)
            .join(Collection, Collection.media_id == Media.id)
            .where(Collection.user_id == user_id, Media.tmdb_id.in_(movie_tmdb_ids), Media.media_type == MediaType.movie)
            .distinct()
        )
        collected_movie_ids = {r[0] for r in coll_q.all()}

    collected_ep_ids: set[int] = set()
    if ep_tmdb_ids:
        coll_q = await db.execute(
            select(Media.tmdb_id)
            .join(Collection, Collection.media_id == Media.id)
            .where(Collection.user_id == user_id, Media.tmdb_id.in_(ep_tmdb_ids), Media.media_type == MediaType.episode)
            .distinct()
        )
        collected_ep_ids = {r[0] for r in coll_q.all()}

    # --- User ratings ---
    # Only fetch show/movie-level ratings (season_number IS NULL); season-specific ratings
    # are fetched separately in the show detail endpoints.
    user_ratings: dict[tuple, float] = {}
    user_ratings_by_uri: dict[str, float] = {}
    if all_tmdb_ids:
        ratings_q = await db.execute(
            select(Media.tmdb_id, Media.media_type, func.max(Rating.rating))
            .join(Rating, Rating.media_id == Media.id)
            .where(
                Rating.user_id == user_id,
                Media.tmdb_id.in_(all_tmdb_ids),
                Rating.season_number.is_(None),
            )
            .group_by(Media.tmdb_id, Media.media_type)
        )
        for tmdb_id, media_type, rating_val in ratings_q.all():
            user_ratings[(tmdb_id, media_type.value)] = rating_val
    # Supplement: ratings for TVDB-only items (no tmdb_id, have uri_id)
    _uri_no_tmdb = [i["uri_id"] for i in items if i.get("uri_id") and not i.get("tmdb_id")]
    if _uri_no_tmdb:
        ratings_q2 = await db.execute(
            select(Media.uri_id, func.max(Rating.rating))
            .join(Rating, Rating.media_id == Media.id)
            .where(
                Rating.user_id == user_id,
                Media.uri_id.in_(_uri_no_tmdb),
                Rating.season_number.is_(None),
            )
            .group_by(Media.uri_id)
        )
        for row_uri, rating_val in ratings_q2.all():
            user_ratings_by_uri[row_uri] = rating_val

    # --- Blocked state (URI-based) ---
    blocked_uris: set[str] = set()
    dropped_uris: set[str] = set()

    # Collect all URIs to look up in the blocklist
    blocklist_lookup_uris: set[str] = set()
    for item in items:
        tid = item.get("tmdb_id")
        t = item.get("type")
        if t == "movie" and tid:
            uri = item.get("uri_id") or f"tmdb:m:{tid}"
            blocklist_lookup_uris.add(uri)
        elif t == "series":
            local = input_tmdb_to_local_show.get(tid)
            if local and local.uri_id:
                blocklist_lookup_uris.add(local.uri_id)
            elif tid:
                blocklist_lookup_uris.add(item.get("uri_id") or f"tmdb:s:{tid}")
        elif t == "episode":
            s_tmdb = item.get("show_tmdb_id")
            s_tvdb = item.get("show_tvdb_id")
            local = input_tmdb_to_local_show.get(s_tmdb) or tvdb_only_show_map.get(s_tvdb)
            if local and local.uri_id:
                blocklist_lookup_uris.add(local.uri_id)
            elif s_tmdb:
                blocklist_lookup_uris.add(f"tmdb:s:{s_tmdb}")
            elif s_tvdb:
                blocklist_lookup_uris.add(f"tvdb:s:{s_tvdb}")

    if blocklist_lookup_uris:
        block_q = await db.execute(
            select(BlocklistItem.uri_id, BlocklistItem.media_type, BlocklistItem.is_dropped)
            .where(
                BlocklistItem.user_id == user_id,
                BlocklistItem.uri_id.in_(blocklist_lookup_uris),
            )
        )
        for b_uri, _mtype, b_dropped in block_q.all():
            if b_dropped:
                dropped_uris.add(b_uri)
            else:
                blocked_uris.add(b_uri)

    # --- Playback progress ---
    progress_map: dict[tuple[int, str], float] = {}
    if all_tmdb_ids:
        progress_q = await db.execute(
            select(Media.tmdb_id, Media.media_type, PlaybackProgress.progress_percent)
            .join(PlaybackProgress, PlaybackProgress.media_id == Media.id)
            .where(
                PlaybackProgress.user_id == user_id,
                Media.tmdb_id.in_(all_tmdb_ids)
            )
        )
        for tmdb_id, media_type, prog_pct in progress_q.all():
            progress_map[(tmdb_id, media_type.value)] = min(100, max(0, int(prog_pct * 100)))

    # --- Play count (detail view only) ---
    play_count_map: dict[int, int] = {}
    if len(items) == 1:
        item0 = items[0]
        tid0 = item0.get("tmdb_id")
        t0 = item0.get("type")
        if tid0 and t0 in ("movie", "episode"):
            mt0 = MediaType.movie if t0 == "movie" else MediaType.episode
            pc_q = await db.execute(
                select(func.count(WatchEvent.id))
                .join(Media, Media.id == WatchEvent.media_id)
                .where(
                    WatchEvent.user_id == user_id,
                    Media.tmdb_id == tid0,
                    Media.media_type == mt0,
                )
            )
            play_count_map[tid0] = pc_q.scalar() or 0

    cf = await _get_content_filters(db, user_id)

    # --- Apply to items ---
    for item in items:
        tid = item.get("tmdb_id")
        t = item.get("type")

        # Backfill uri_id if missing
        if not item.get("uri_id"):
            tvdb_id = item.get("tvdb_id")
            if tid:
                if t == "movie":
                    item["uri_id"] = f"tmdb:m:{tid}"
                elif t == "series":
                    local_s = input_tmdb_to_local_show.get(tid)
                    item["uri_id"] = (local_s.uri_id if local_s else None) or f"tmdb:s:{tid}"
                elif t == "episode":
                    item["uri_id"] = f"tmdb:e:{tid}"
                elif t == "person":
                    item["uri_id"] = f"tmdb:p:{tid}"
            elif tvdb_id and t == "series":
                local_s = tvdb_only_show_map.get(tvdb_id)
                item["uri_id"] = (local_s.uri_id if local_s else None) or f"tvdb:s:{tvdb_id}"

        # Backfill show_uri_id for episode items
        if t == "episode" and not item.get("show_uri_id"):
            s_tmdb = item.get("show_tmdb_id")
            local_s = input_tmdb_to_local_show.get(s_tmdb)
            if local_s and local_s.uri_id:
                item["show_uri_id"] = local_s.uri_id
            elif s_tmdb:
                item["show_uri_id"] = f"tmdb:s:{s_tmdb}"

        if t == "movie":
            item["watched"] = tid in watched_movies
            in_lib = tid in collected_movie_ids
            item["in_library"] = in_lib
            item["collection_pct"] = 100 if in_lib else 0
            item["progress_percent"] = progress_map.get((tid, "movie"))
        elif t == "series":
            # TVDB-only series (no tmdb_id): look up by uri_id string, matching the key
            # injected into show_tmdb_ids / input_tmdb_to_local_show above.
            _show_key = tid if tid is not None else item.get("uri_id")
            item["watched"] = _show_key in watched_shows if _show_key is not None else False
            pct = show_pct.get(_show_key, 0) if _show_key is not None else 0
            item["collection_pct"] = pct
            item["in_library"] = pct > 0
            watched_count = show_watched_count_map.get(_show_key, 0) if _show_key is not None else 0
            aired_count = show_aired_count.get(_show_key, 0) if _show_key is not None else 0
            item["watched_episodes_count"] = watched_count
            item["total_episodes_count"] = aired_count
            item["watch_pct"] = min(100, int((watched_count / aired_count) * 100)) if aired_count > 0 else 0
            if _show_key is not None and _show_key in show_tvdb_ids and show_tvdb_ids[_show_key]:
                item["tvdb_id"] = show_tvdb_ids[_show_key]

            if apply_tvdb_metadata and primary_source == "tvdb":
                _local_show = input_tmdb_to_local_show.get(tid) or (
                    input_tmdb_to_local_show.get(_show_key) if _show_key is not None else None
                )
                if _local_show:
                    item["title"] = _local_show.title
                    if _local_show.poster_path:
                        item["poster_path"] = _local_show.poster_path
                    if _local_show.backdrop_path:
                        item["backdrop_path"] = _local_show.backdrop_path
                    if _local_show.first_air_date:
                        item["release_date"] = _local_show.first_air_date
        elif t == "episode":
            item["watched"] = tid in watched_episodes
            in_lib = tid in collected_ep_ids
            item["in_library"] = in_lib
            item["collection_pct"] = 100 if in_lib else 0
            item["progress_percent"] = progress_map.get((tid, "episode"))
        else:
            item["watched"] = False
            item["collection_pct"] = 0
            item["in_library"] = False

        # Lookup list membership
        sn_key = item.get("season_number") if t in ("season", "episode") else None
        en_key = item.get("episode_number") if t == "episode" else None
        item["in_lists"] = []
        if item.get("uri_id"):
            item["in_lists"] = list_membership_by_key.get((item["uri_id"], sn_key, en_key), [])

        item["is_monitored"] = monitored_status.get(tid, False)
        _req_key = tid if tid is not None else item.get("uri_id")
        item["request_enabled"] = request_enabled_map.get(_req_key, False)
        item["request_status"] = request_status_map.get(tid) or (
            request_status_map.get(item.get("uri_id")) if tid is None else None
        )
        item["user_rating"] = user_ratings.get((tid, t))
        item["play_count"] = play_count_map.get(tid, 0)
        # URI-based fallbacks for TVDB-only items (tid is None)
        if tid is None and item.get("uri_id"):
            _uri = item["uri_id"]
            if item["user_rating"] is None:
                item["user_rating"] = user_ratings_by_uri.get(_uri)

        # Blocked status: URI-based lookup
        def _check_uri_blocked(check_uris: set[str]) -> tuple[bool, bool]:
            check = check_uris - {None}
            return bool(check & blocked_uris), bool(check & dropped_uris)

        if t == "movie":
            _b, _d = _check_uri_blocked({item.get("uri_id"), f"tmdb:m:{tid}" if tid else None})
        elif t == "series":
            local = input_tmdb_to_local_show.get(tid)
            _b, _d = _check_uri_blocked({
                item.get("uri_id"),
                local.uri_id if local else None,
                f"tmdb:s:{tid}" if tid else None,
            })
        elif t == "episode":
            s_tmdb = item.get("show_tmdb_id")
            s_tvdb = item.get("show_tvdb_id")
            local = input_tmdb_to_local_show.get(s_tmdb) or tvdb_only_show_map.get(s_tvdb)
            _b, _d = _check_uri_blocked({
                local.uri_id if local else None,
                f"tmdb:s:{s_tmdb}" if s_tmdb else None,
                f"tvdb:s:{s_tvdb}" if s_tvdb else None,
            })
        else:
            _b, _d = False, False

        is_blocked = _b or _is_content_filtered(item, *cf)
        item["is_blocked"] = is_blocked
        item["is_dropped"] = _d

    return items


async def _get_global_settings(db: AsyncSession) -> GlobalSettings | None:
    if "global_settings" not in db.info:
        result = await db.execute(select(GlobalSettings).where(GlobalSettings.id == 1))
        db.info["global_settings"] = result.scalar_one_or_none()
    return db.info["global_settings"]


async def get_user_tmdb_key(db: AsyncSession, user_id: int) -> str | None:
    cache_key = f"tmdb_key_{user_id}"
    if cache_key not in db.info:
        result = await db.execute(select(UserSettings).where(UserSettings.user_id == user_id))
        settings_row = result.scalar_one_or_none()
        if settings_row and settings_row.tmdb_api_key:
            db.info[cache_key] = settings_row.tmdb_api_key
        else:
            gs = await _get_global_settings(db)
            db.info[cache_key] = gs.tmdb_api_key if gs else None
    return db.info[cache_key]


async def get_user_content_language(db: AsyncSession, user_id: int) -> str:
    """Return the user's preferred content language for TMDB image selection.
    Defaults to 'en' if none is configured in profile settings."""
    result = await db.execute(
        select(UserProfileData).where(UserProfileData.user_id == user_id)
    )
    profile = result.scalar_one_or_none()
    lang = getattr(profile, "content_language", None) if profile else None
    return lang or "en"


def _effective_radarr(user_settings: UserSettings | None, global_settings: GlobalSettings | None):
    """Return the settings object whose Radarr config is fully configured, user first."""
    for s in (user_settings, global_settings):
        if s and all([s.radarr_url, s.radarr_token, s.radarr_root_folder, s.radarr_quality_profile]):
            return s
    return None


def _effective_sonarr(user_settings: UserSettings | None, global_settings: GlobalSettings | None):
    """Return the settings object whose Sonarr config is fully configured, user first."""
    for s in (user_settings, global_settings):
        if s and all([s.sonarr_url, s.sonarr_token, s.sonarr_root_folder, s.sonarr_quality_profile]):
            return s
    return None


def check_tmdb_key(api_key: str | None) -> bool:
    if api_key:
        return True
    return bool(getattr(tmdb.settings, "tmdb_api_key", None))


def _extract_movie_certification(data: dict, country: str = "US") -> str | None:
    for entry in data.get("release_dates", {}).get("results", []):
        if entry.get("iso_3166_1") == country:
            for rd in entry.get("release_dates", []):
                cert = rd.get("certification", "").strip()
                if cert:
                    return cert
    return None


def _extract_movie_release_dates(data: dict, country: str = "US") -> dict:
    results = data.get("release_dates", {}).get("results", [])
    us_entry = next((e for e in results if e.get("iso_3166_1") == country), None)
    digital = physical = None
    if us_entry:
        for rd in us_entry.get("release_dates", []):
            t = rd.get("type")
            d = (rd.get("release_date") or "")[:10] or None
            if t == 4 and not digital:
                digital = d
            elif t == 5 and not physical:
                physical = d
    return {"digital": digital, "physical": physical}


def _extract_show_content_rating(data: dict, country: str = "US") -> str | None:
    for entry in data.get("content_ratings", {}).get("results", []):
        if entry.get("iso_3166_1") == country:
            rating = entry.get("rating", "").strip()
            if rating:
                return rating
    return None


def format_media(media: Media) -> dict:
    cast = []
    raw_cast = (media.tmdb_data or {}).get("cast", [])
    for c in raw_cast:
        cast.append(
            {
                "tmdb_id": c.get("id"),
                "name": c.get("name"),
                "character": c.get("character"),
                "profile_path": tmdb.poster_url(c.get("profile_path"))
                if c.get("profile_path")
                else None,
            }
        )

    return {
        "id": media.id,
        "tmdb_id": media.tmdb_id,
        "uri_id": media.uri_id,
        "type": media.media_type,
        "title": media.custom_title or media.title,
        "tmdb_title": media.title,
        "custom_title": media.custom_title,
        "original_title": media.original_title,
        "overview": media.overview,
        "poster_path": media.poster_path,
        "backdrop_path": media.backdrop_path,
        "release_date": media.release_date,
        "runtime": media.runtime,
        "tmdb_rating": media.tmdb_rating,
        "tagline": media.tagline,
        "status": media.status,
        "season_number": media.season_number,
        "season_name": (media.show.custom_season_names or {}).get(str(media.season_number)) if (media.show and media.season_number is not None) else None,
        "episode_number": media.episode_number,
        "show_title": (media.show.custom_title or media.show.title) if media.show else None,
        "show_uri_id": media.show.uri_id if media.show else None,
        "show_tmdb_id": media.show.tmdb_id if media.show else None,
        "show_tvdb_id": (
            media.show.tvdb_id if (media.show and media.show.tvdb_id) else (
                int(media.show.tmdb_data.get("external_ids", {}).get("tvdb_id"))
                if (media.show and media.show.tmdb_data and media.show.tmdb_data.get("external_ids", {}).get("tvdb_id"))
                else None
            )
        ) if media.show else None,
        "show_poster_path": media.show.poster_path if media.show else None,
        "show_backdrop_path": media.show.backdrop_path if media.show else None,
        "genres": (media.tmdb_data or {}).get("genres", []),
        "original_language": (media.tmdb_data or {}).get("original_language"),
        "cast": cast[:12],
        "collection": (media.tmdb_data or {}).get("collection"),
        "adult": media.adult,
    }


@router.get("")
async def list_media(
    type: MediaType | None = Query(None),
    sort: str = Query(default="created_at"),
    page: int = Query(1, ge=1),
    page_size: int = Query(30, ge=1, le=100),
    genre: str | None = Query(None),
    year: int | None = Query(None),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    offset = (page - 1) * page_size

    filters = [Collection.user_id == current_user.id]
    if type:
        filters.append(Media.media_type == type)
    if genre:
        filters.append(sa_cast(Media.tmdb_data["genres"], Text).contains(f'"{genre}"'))
    if year:
        filters.append(Media.release_date.like(f'{year}%'))

    base_query = (
        select(Media)
        .options(joinedload(Media.show))
        .join(Collection, Collection.media_id == Media.id)
        .where(*filters)
    )

    # Count total
    count_query = (
        select(func.count())
        .select_from(Media)
        .join(Collection, Collection.media_id == Media.id)
        .where(*filters)
    )
    total_result = await db.execute(count_query)
    total_count = total_result.scalar_one()
    total_pages = (total_count + page_size - 1) // page_size

    # Sort and Paginate
    if sort == "last_watched":
        last_watched_sq = (
            select(WatchEvent.media_id, func.max(WatchEvent.watched_at).label("last_watched_at"))
            .where(WatchEvent.user_id == current_user.id)
            .group_by(WatchEvent.media_id)
            .subquery()
        )
        query = (
            base_query
            .outerjoin(last_watched_sq, last_watched_sq.c.media_id == Media.id)
            .order_by(last_watched_sq.c.last_watched_at.desc().nulls_last())
            .offset(offset).limit(page_size)
        )
    else:
        sort_map = {
            "rating": Media.tmdb_rating.desc().nulls_last(),
            "release_date": Media.release_date.desc().nulls_last(),
            "title": func.lower(Media.title).asc(),
            "created_at": Collection.added_at.desc(),
        }
        order = sort_map.get(sort, Collection.added_at.desc())
        query = base_query.order_by(order).offset(offset).limit(page_size)
    result = await db.execute(query)
    items = result.scalars().all()

    results = [format_media(m) for m in items]
    await enrich_with_state(db, current_user.id, results, apply_tvdb_metadata=False)
    return {
        "page": page,
        "page_size": page_size,
        "total_results": total_count,
        "total_pages": total_pages,
        "results": results,
    }


def _dedupe_search_series(items: list[dict]) -> list[dict]:
    """Collapse duplicate series search results for the same show (by title and year)."""
    def _norm(s) -> str:
        return (s or "").strip().lower()

    def _year(item: dict) -> str:
        rd = item.get("release_date") or ""
        return rd[:4] if rd else ""

    best_by_key: dict[tuple, int] = {}  # (title, year) -> index in result
    result: list[dict] = []
    for item in items:
        if item.get("type") != "series" or not item.get("title"):
            result.append(item)
            continue
        key = (_norm(item.get("title")), _year(item))
        if key not in best_by_key:
            best_by_key[key] = len(result)
            result.append(item)
            continue
        # Duplicate — decide whether the new one is better than the kept one.
        idx = best_by_key[key]
        kept = result[idx]
        new_score = (1 if item.get("in_library") else 0, 1 if item.get("poster_path") else 0)
        kept_score = (1 if kept.get("in_library") else 0, 1 if kept.get("poster_path") else 0)
        if new_score > kept_score:
            result[idx] = item
    return result


@router.get("/search")
async def search_media(
    q: str = Query(..., min_length=2),
    type: str | None = Query(None),
    year: int | None = Query(None),
    page: int = Query(1, ge=1),
    in_library: bool = Query(False),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    valid_types = {m.value for m in MediaType} | {"person", "collection"}
    if type is not None and type not in valid_types:
        type = None

    # Collection search: TMDB only, no local DB
    if type == "collection":
        tmdb_key = await get_user_tmdb_key(db, current_user.id)
        if not check_tmdb_key(tmdb_key):
            return {"page": page, "total_pages": 1, "total_results": 0, "results": []}
        try:
            data = await tmdb.search_collection(q, page=page, api_key=tmdb_key)
            collections = [
                {
                    "id": None,
                    "tmdb_id": c.get("id"),
                    "type": "collection",
                    "title": c.get("name"),
                    "poster_path": tmdb.poster_url(c.get("poster_path")),
                    "backdrop_path": tmdb.poster_url(c.get("backdrop_path"), size="w1280"),
                    "overview": c.get("overview"),
                    "in_library": False,
                }
                for c in data.get("results", [])
            ]
        except Exception as e:
            print(f"TMDB collection search error: {e}")
            collections = []
            data = {}
        return {
            "page": page,
            "total_pages": data.get("total_pages", 1),
            "total_results": data.get("total_results", 0),
            "results": collections,
        }

    # People search: TMDB only, no local DB
    if type == "person":
        tmdb_key = await get_user_tmdb_key(db, current_user.id)
        if not check_tmdb_key(tmdb_key):
            return {"page": page, "total_pages": 1, "total_results": 0, "results": []}
        try:
            data = await tmdb.search_people(q, page=page, api_key=tmdb_key)
            people = [
                {
                    "id": None,
                    "tmdb_id": p.get("id"),
                    "type": "person",
                    "title": p.get("name"),
                    "poster_path": tmdb.poster_url(p.get("profile_path")),
                    "known_for_department": p.get("known_for_department"),
                    "in_library": False,
                }
                for p in data.get("results", [])
            ]
        except Exception as e:
            print(f"TMDB people search error: {e}")
            people = []
            data = {}
        return {
            "page": page,
            "total_pages": data.get("total_pages", 1),
            "total_results": data.get("total_results", 0),
            "results": people,
        }

    # Episode search: local DB only (TMDB has no episode search endpoint)
    if type == MediaType.episode:
        db_query = (
            select(Media)
            .options(joinedload(Media.show))
            .where(or_(Media.title.ilike(f"%{q}%"), Media.original_title.ilike(f"%{q}%")))
            .where(Media.media_type == MediaType.episode)
            .limit(50)
        )
        result = await db.execute(db_query)
        items = result.scalars().all()
        formatted = [format_media(m) for m in items]
        for item in formatted:
            item["in_library"] = True
        return {"page": 1, "total_pages": 1, "total_results": len(formatted), "results": formatted}

    # Collection-only filter: search local DB, skip TMDB entirely
    if in_library:
        PAGE_SIZE = 24
        lib_q = (
            select(Media)
            .options(joinedload(Media.show))
            .join(Collection, Collection.media_id == Media.id)
            .where(
                Collection.user_id == current_user.id,
                or_(Media.title.ilike(f"%{q}%"), Media.original_title.ilike(f"%{q}%")),
            )
        )
        if type and type in {m.value for m in MediaType}:
            lib_q = lib_q.where(Media.media_type == type)
        else:
            lib_q = lib_q.where(Media.media_type != MediaType.episode)
        count_result = await db.execute(select(func.count()).select_from(lib_q.subquery()))
        total = count_result.scalar_one()
        lib_q = lib_q.order_by(Media.title).offset((page - 1) * PAGE_SIZE).limit(PAGE_SIZE)
        items_result = await db.execute(lib_q)
        items = items_result.scalars().all()
        formatted = [format_media(m) for m in items]
        for item in formatted:
            item["in_library"] = True
        return {
            "page": page,
            "total_pages": max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE),
            "total_results": total,
            "results": formatted,
        }

    tmdb_key = await get_user_tmdb_key(db, current_user.id)

    # No TMDB key: fall back to local title search
    if not check_tmdb_key(tmdb_key):
        db_query = (
            select(Media)
            .options(joinedload(Media.show))
            .where(or_(Media.title.ilike(f"%{q}%"), Media.original_title.ilike(f"%{q}%")))
            .limit(30)
        )
        if type:
            db_query = db_query.where(Media.media_type == type)
        else:
            db_query = db_query.where(Media.media_type != MediaType.episode)
        result = await db.execute(db_query)
        items = result.scalars().all()
        formatted = [format_media(m) for m in items]
        for item in formatted:
            item["in_library"] = True
        return {"page": 1, "total_pages": 1, "total_results": len(formatted), "results": formatted}

    # 1. Determine primary metadata source preference
    settings_res = await db.execute(select(UserSettings).where(UserSettings.user_id == current_user.id))
    settings_row = settings_res.scalar_one_or_none()
    preferences = settings_row.preferences if settings_row else None
    primary_source = preferences.get("primary_metadata_source") if preferences else "tmdb"

    # Search TMDB/TVDB
    raw_results = []
    total_pages = 1
    total_results = 0
    
    try:
        if type == MediaType.movie:
            data = await tmdb.search_movies(q, page=page, year=year, api_key=tmdb_key)
            raw_results = data.get("results", [])
            for res in raw_results:
                res["media_type"] = "movie"
            total_pages = data.get("total_pages", 1)
            total_results = data.get("total_results", 0)
        elif type == MediaType.series:
            tvdb_api_key = None
            if primary_source == "tvdb":
                from routers.shows import get_user_tvdb_key
                tvdb_api_key = await get_user_tvdb_key(db, current_user.id)
            
            tvdb_results = []
            if tvdb_api_key:
                from core import tvdb as tvdb_client
                lang_code = await get_user_content_language(db, current_user.id)
                tvdb_lang = tvdb_client.to_three_letter_lang(lang_code)
                try:
                    tvdb_results = await tvdb_client.search_series(q, tvdb_api_key, lang=tvdb_lang)
                except Exception as e:
                    print(f"TVDB search error: {e}")
            
            if primary_source == "tvdb" and not tvdb_results and check_tmdb_key(tmdb_key):
                tvdb_api_key = None
            
            if not tvdb_api_key:
                data = await tmdb.search_shows(q, page=page, year=year, api_key=tmdb_key)
                raw_results = data.get("results", [])
                for res in raw_results:
                    res["media_type"] = "tv"
                total_pages = data.get("total_pages", 1)
                total_results = data.get("total_results", 0)
            else:
                PAGE_SIZE = 20
                total_results = len(tvdb_results)
                total_pages = max(1, (total_results + PAGE_SIZE - 1) // PAGE_SIZE)
                start_idx = (page - 1) * PAGE_SIZE
                raw_results = tvdb_results[start_idx : start_idx + PAGE_SIZE]
                mapped_results = []
                for res in raw_results:
                    mapped_results.append({
                        "media_type": "tv",
                        "id": None,
                        "tvdb_id": res.get("tvdb_id"),
                        "name": res.get("title"),
                        "original_name": res.get("title"),
                        "overview": res.get("overview"),
                        "poster_path": res.get("image_url"),
                        "backdrop_path": None,
                        "first_air_date": f"{res['year']}-01-01" if res.get("year") else None,
                    })
                raw_results = mapped_results
        else:
            # "All": movies + shows + people, interleaved by popularity score
            tvdb_api_key = None
            if primary_source == "tvdb":
                from routers.shows import get_user_tvdb_key
                tvdb_api_key = await get_user_tvdb_key(db, current_user.id)
            
            movie_task = tmdb.search_movies(q, page=page, api_key=tmdb_key) if check_tmdb_key(tmdb_key) else None
            people_task = tmdb.search_people(q, page=page, api_key=tmdb_key) if check_tmdb_key(tmdb_key) else None
            
            tvdb_results = []
            if tvdb_api_key:
                from core import tvdb as tvdb_client
                lang_code = await get_user_content_language(db, current_user.id)
                tvdb_lang = tvdb_client.to_three_letter_lang(lang_code)
                try:
                    tvdb_results = await tvdb_client.search_series(q, tvdb_api_key, lang=tvdb_lang)
                except Exception as e:
                    print(f"TVDB search error: {e}")
            
            show_results = []
            movie_data = await movie_task if movie_task else {}
            people_data = await people_task if people_task else {}
            
            movie_results = movie_data.get("results", [])
            for res in movie_results:
                res["media_type"] = "movie"
            
            people_results = people_data.get("results", [])
            for res in people_results:
                res["media_type"] = "person"
            
            tmdb_show_total_pages = 1
            tmdb_show_total_results = 0
            
            if primary_source == "tvdb" and not tvdb_results and check_tmdb_key(tmdb_key):
                try:
                    tmdb_show_data = await tmdb.search_shows(q, page=page, api_key=tmdb_key)
                    tmdb_show_results = tmdb_show_data.get("results", [])
                    for res in tmdb_show_results:
                        res["media_type"] = "tv"
                    show_results = tmdb_show_results
                    tmdb_show_total_pages = tmdb_show_data.get("total_pages", 1)
                    tmdb_show_total_results = tmdb_show_data.get("total_results", 0)
                except Exception as e:
                    print(f"TMDB shows search fallback error: {e}")
            elif tvdb_results:
                max_tmdb_pop = max([r.get("popularity", 0.0) for r in movie_results + people_results] + [100.0])
                for i, r in enumerate(tvdb_results):
                    show_results.append({
                        "media_type": "tv",
                        "id": None,
                        "tvdb_id": r["tvdb_id"],
                        "name": r["title"],
                        "original_name": r["title"],
                        "overview": r.get("overview"),
                        "poster_path": r.get("image_url"),
                        "backdrop_path": None,
                        "first_air_date": f"{r['year']}-01-01" if r.get("year") else None,
                        "vote_average": 0.0,
                        "popularity": max(1.0, max_tmdb_pop * (1.0 - i * 0.05)),
                    })
            else:
                try:
                    tmdb_show_data = await tmdb.search_shows(q, page=page, api_key=tmdb_key)
                    tmdb_show_results = tmdb_show_data.get("results", [])
                    for res in tmdb_show_results:
                        res["media_type"] = "tv"
                    show_results = tmdb_show_results
                    tmdb_show_total_pages = tmdb_show_data.get("total_pages", 1)
                    tmdb_show_total_results = tmdb_show_data.get("total_results", 0)
                except Exception as e:
                    print(f"TMDB shows search error: {e}")
            
            raw_results = sorted(
                movie_results + show_results + people_results,
                key=lambda x: x.get("popularity", 0),
                reverse=True,
            )
            
            if tvdb_results:
                total_pages = max(
                    movie_data.get("total_pages", 1),
                    (len(show_results) + 20 - 1) // 20,
                    people_data.get("total_pages", 1),
                )
                total_results = (
                    movie_data.get("total_results", 0)
                    + len(show_results)
                    + people_data.get("total_results", 0)
                )
            else:
                total_pages = max(
                    movie_data.get("total_pages", 1),
                    tmdb_show_total_pages,
                    people_data.get("total_pages", 1),
                )
                total_results = (
                    movie_data.get("total_results", 0)
                    + tmdb_show_total_results
                    + people_data.get("total_results", 0)
                )
    except Exception as e:
        print(f"Search error: {e}")

    # Filter out blocked items
    blocked_movies = await _get_blocked_ids(db, current_user.id, MediaType.movie)
    blocked_series = await _get_blocked_ids(db, current_user.id, MediaType.series)
    cf = await _get_content_filters(db, current_user.id)
    raw_results = [
        res for res in raw_results
        if not (res.get("media_type") == "movie" and res.get("id") in blocked_movies)
        and not (res.get("media_type") == "tv" and (
            res.get("id") in blocked_series 
            or (res.get("tvdb_id") and (res.get("tvdb_id") in blocked_series or -res.get("tvdb_id") in blocked_series))
        ))
        and not _is_content_filtered(res, *cf)
    ]

    tmdb_ids_on_page = [res.get("id") for res in raw_results if res.get("id")]
    local_map: dict[tuple[int, str], Media] = {}
    local_show_map: dict[int, ShowModel] = {}
    if tmdb_ids_on_page:
        local_q = (
            select(Media)
            .options(joinedload(Media.show))
            .where(Media.tmdb_id.in_(tmdb_ids_on_page))
        )
        if type == MediaType.movie:
            local_q = local_q.where(Media.media_type == MediaType.movie)
        elif type != MediaType.series:
            local_q = local_q.where(Media.media_type == MediaType.movie)
        local_result = await db.execute(local_q)
        local_map = {(m.tmdb_id, m.media_type.value): m for m in local_result.scalars().all()}
        
        shows_q = select(ShowModel).where(ShowModel.tmdb_id.in_(tmdb_ids_on_page))
        shows_res = await db.execute(shows_q)
        local_show_map = {s.tmdb_id: s for s in shows_res.scalars().all() if s.tmdb_id}

    # For TVDB series, query local database by tvdb_id
    tvdb_ids_on_page = [res.get("tvdb_id") for res in raw_results if res.get("tvdb_id")]
    local_tvdb_show_map: dict[int, ShowModel] = {}
    if tvdb_ids_on_page:
        shows_q = select(ShowModel).where(ShowModel.tvdb_id.in_(tvdb_ids_on_page))
        shows_res = await db.execute(shows_q)
        shows_list = shows_res.scalars().all()
        local_tvdb_show_map = {s.tvdb_id: s for s in shows_list if s.tvdb_id}

    # 3. Build enriched list preserving relevance order
    enriched = []
    seen_tmdb_ids = set()
    seen_tvdb_ids = set()
    
    for res in raw_results:
        media_type = res.get("media_type")
        if media_type == "tv":
            media_type = "series"
        if media_type not in ("movie", "series", "person"):
            continue

        if media_type == "person":
            tmdb_id = res.get("id")
            seen_tmdb_ids.add(tmdb_id)
            enriched.append({
                "id": None,
                "uri_id": f"tmdb:p:{tmdb_id}" if tmdb_id else None,
                "tmdb_id": tmdb_id,
                "type": "person",
                "title": res.get("name"),
                "poster_path": tmdb.poster_url(res.get("profile_path")),
                "known_for_department": res.get("known_for_department"),
                "in_library": False,
            })
            continue

        # TVDB Series formatting path
        if media_type == "series" and res.get("tvdb_id") is not None:
            tvdb_id = res["tvdb_id"]
            seen_tvdb_ids.add(tvdb_id)
            local_show = local_tvdb_show_map.get(tvdb_id)
            if local_show:
                item = {
                    "id": local_show.id,
                    "uri_id": local_show.uri_id,
                    "tmdb_id": local_show.tmdb_id,
                    "tvdb_id": tvdb_id,
                    "show_tvdb_id": tvdb_id,
                    "type": "series",
                    "title": local_show.custom_title or local_show.title or res.get("name") or res.get("title"),
                    "original_title": local_show.original_title or res.get("original_name") or res.get("title"),
                    "overview": local_show.overview or res.get("overview"),
                    "poster_path": local_show.poster_path or res.get("poster_path") or res.get("image_url"),
                    "backdrop_path": local_show.backdrop_path,
                    "release_date": res.get("first_air_date") or (f"{res['year']}-01-01" if res.get("year") else None),
                    "tmdb_rating": local_show.tmdb_rating or 0.0,
                    "in_library": True,
                    "adult": False,
                }
            else:
                item = {
                    "id": None,
                    "tmdb_id": None,
                    "tvdb_id": tvdb_id,
                    "show_tvdb_id": tvdb_id,
                    "type": "series",
                    "title": res.get("name") or res.get("title"),
                    "original_title": res.get("original_name") or res.get("title"),
                    "overview": res.get("overview"),
                    "poster_path": res.get("poster_path") or res.get("image_url"),
                    "backdrop_path": None,
                    "release_date": res.get("first_air_date") or (f"{res['year']}-01-01" if res.get("year") else None),
                    "tmdb_rating": 0.0,
                    "in_library": False,
                    "adult": False,
                }
            enriched.append(item)
            continue

        # Standard TMDB Movie/Series formatting path
        tmdb_id = res.get("id")
        seen_tmdb_ids.add(tmdb_id)
        local = local_map.get((tmdb_id, media_type))
        local_show = local_show_map.get(tmdb_id) if media_type == "series" else None
        
        if local:
            item = format_media(local)
            item["type"] = media_type
            item["in_library"] = True
            if not item.get("poster_path"):
                item["poster_path"] = tmdb.poster_url(res.get("poster_path"))
            if not item.get("release_date"):
                item["release_date"] = res.get("release_date") or res.get("first_air_date")
            if not item.get("title"):
                item["title"] = res.get("title") or res.get("name")
        elif local_show:
            item = {
                "id": local_show.id,
                "uri_id": local_show.uri_id,
                "tmdb_id": local_show.tmdb_id,
                "tvdb_id": local_show.tvdb_id,
                "show_tvdb_id": local_show.tvdb_id,
                "type": "series",
                "title": local_show.custom_title or local_show.title or res.get("title") or res.get("name"),
                "original_title": local_show.original_title or res.get("original_title") or res.get("original_name"),
                "overview": local_show.overview or res.get("overview"),
                "poster_path": local_show.poster_path or tmdb.poster_url(res.get("poster_path")),
                "backdrop_path": local_show.backdrop_path or tmdb.poster_url(res.get("backdrop_path"), size="w1280"),
                "release_date": res.get("release_date") or res.get("first_air_date"),
                "tmdb_rating": local_show.tmdb_rating or res.get("vote_average"),
                "in_library": True,
                "adult": res.get("adult", False),
            }
        else:
            item = {
                "id": None,
                "tmdb_id": tmdb_id,
                "type": media_type,
                "title": res.get("title") or res.get("name"),
                "original_title": res.get("original_title") or res.get("original_name"),
                "overview": res.get("overview"),
                "poster_path": tmdb.poster_url(res.get("poster_path")),
                "backdrop_path": tmdb.poster_url(res.get("backdrop_path"), size="w1280"),
                "release_date": res.get("release_date") or res.get("first_air_date"),
                "tmdb_rating": res.get("vote_average"),
                "in_library": False,
                "adult": res.get("adult", False),
            }
        enriched.append(item)

    # 4. On page 1, append local library items that TMDB/TVDB didn't return
    if page == 1:
        fallback_q = (
            select(Media)
            .options(joinedload(Media.show))
            .where(or_(Media.title.ilike(f"%{q}%"), Media.original_title.ilike(f"%{q}%")))
            .limit(20)
        )
        if type:
            fallback_q = fallback_q.where(Media.media_type == type)
        else:
            fallback_q = fallback_q.where(Media.media_type != MediaType.episode)
            
        fallback_result = await db.execute(fallback_q)
        for m in fallback_result.scalars().all():
            if m.media_type == MediaType.movie and m.tmdb_id in seen_tmdb_ids:
                continue
            if m.media_type == MediaType.series:
                if m.tmdb_id and m.tmdb_id in seen_tmdb_ids:
                    continue
                if m.show and m.show.tvdb_id and m.show.tvdb_id in seen_tvdb_ids:
                    continue
            item = format_media(m)
            item["in_library"] = True
            enriched.append(item)

    enriched = _dedupe_search_series(enriched)
    await enrich_with_state(db, current_user.id, enriched, apply_tvdb_metadata=False)
    return {
        "page": page,
        "total_pages": total_pages,
        "total_results": total_results,
        "results": enriched,
    }


async def _sync_trending(
    type: MediaType,
    page: int = 1,
    api_key: str | None = None,
):
    """Fetch trending data from TMDB."""
    if not check_tmdb_key(api_key):
        return {"results": [], "page": 1, "total_pages": 1, "total_results": 0}

    try:
        if type == MediaType.movie:
            data = await tmdb.get_trending_movies(page=page, api_key=api_key)
        else:
            data = await tmdb.get_trending_shows(page=page, api_key=api_key)
        return data
    except Exception as e:
        print(f"Error fetching trending from TMDB: {e}")
        return {"results": [], "page": 1, "total_pages": 1, "total_results": 0}


async def _get_blocked_ids(db: AsyncSession, user_id: int, media_type: MediaType) -> set[int]:
    """Return blocked TMDB integer IDs for pre-enrichment filtering of raw TMDB results.
    Only includes tmdb-provider entries since TVDB-only shows don't appear in TMDB results.
    """
    _prefix_map = {
        MediaType.movie:   "tmdb:m:",
        MediaType.series:  "tmdb:s:",
        MediaType.episode: "tmdb:e:",
    }
    prefix = _prefix_map.get(media_type, "tmdb:s:")
    query = select(BlocklistItem.uri_id).where(
        BlocklistItem.user_id == user_id,
        BlocklistItem.media_type == media_type,
        BlocklistItem.uri_id.like(f"{prefix}%"),
    )
    result = await db.execute(query)
    ids: set[int] = set()
    for (uri,) in result.all():
        if uri:
            try:
                ids.add(int(uri[len(prefix):]))
            except ValueError:
                pass
    return ids


# ── Content-filter helpers ───────────────────────────────────────────────────

# Combined genre name → TMDB ID map (movie + TV)
_ALL_GENRE_ID_MAP: dict[str, int] = {**MOVIE_GENRE_IDS, **TV_GENRE_IDS}
# Reverse: numeric genre ID → name, used when filtering raw TMDB list results
_GENRE_ID_TO_NAME: dict[int, str] = {v: k for k, v in _ALL_GENRE_ID_MAP.items()}


async def _get_content_filters(db: AsyncSession, user_id: int, lower: bool = True) -> tuple:
    """Return (blocked_genre_names, blocked_keywords, blocked_regexes, filter_languages, language_filter_mode) from user preferences."""
    q = await db.execute(select(UserSettings).where(UserSettings.user_id == user_id))
    settings = q.scalar_one_or_none()
    prefs: dict = (settings.preferences or {}) if settings else {}
    cf = prefs.get("content_filters", {})

    genres = cf.get("blocked_genres", [])
    keywords = cf.get("blocked_keywords", [])
    regexes = cf.get("blocked_regexes", [])
    languages = cf.get("filter_languages", [])
    mode = cf.get("language_filter_mode", "blacklist")

    if lower:
        return (
            {g.lower() for g in genres},
            [k.lower() for k in keywords],
            regexes,
            [lang.lower() for lang in languages],
            mode.lower(),
        )
    return (genres, keywords, regexes, languages, mode)


def _is_content_filtered(
    item: dict,
    blocked_genres: set[str],
    blocked_keywords: list[str],
    blocked_regexes: list[str],
    filter_languages: list[str],
    language_filter_mode: str,
) -> bool:
    """Return True if item should be hidden due to a content filter rule."""
    import re as _re

    if filter_languages:
        itype = item.get("type")
        if itype is None or itype in ("movie", "series"):
            raw_lang = (item.get("original_language") or "").lower()
            from core.tvdb import to_two_letter_lang
            item_lang = to_two_letter_lang(raw_lang) or raw_lang
            
            if language_filter_mode == "blacklist":
                if item_lang in filter_languages or raw_lang in filter_languages:
                    return True
            elif language_filter_mode == "whitelist":
                if item_lang and item_lang not in filter_languages and raw_lang not in filter_languages:
                    return True

    title = (item.get("title") or item.get("name") or "").lower()

    # Genre check: TMDB list results use numeric genre_ids; detail views may use string genres
    if blocked_genres:
        item_genre_ids: list[int] = item.get("genre_ids") or []
        resolved: set[str] = {_GENRE_ID_TO_NAME[gid].lower() for gid in item_genre_ids if gid in _GENRE_ID_TO_NAME}
        raw_genres = item.get("genres") or []
        for g in raw_genres:
            if isinstance(g, dict):
                resolved.add((g.get("name") or "").lower())
            elif isinstance(g, str):
                resolved.add(g.lower())
        if resolved & blocked_genres:
            return True

    # Keyword check (case-insensitive substring)
    for kw in blocked_keywords:
        if kw and kw in title:
            return True

    # Regex check (invalid patterns are silently skipped)
    for pat in blocked_regexes:
        try:
            if pat and _re.search(pat, title, _re.IGNORECASE):
                return True
        except _re.error:
            pass

    return False


@router.get("/trending/movies")
async def trending_movies(
    page: int = Query(1, ge=1),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    tmdb_key = await get_user_tmdb_key(db, current_user.id)
    data = await _sync_trending(MediaType.movie, page, api_key=tmdb_key)
    tmdb_results = data.get("results", [])

    if not tmdb_results:
        return {"page": page, "total_pages": 1, "total_results": 0, "results": []}

    blocked_ids = await _get_blocked_ids(db, current_user.id, MediaType.movie)
    cf = await _get_content_filters(db, current_user.id)
    tmdb_results = [res for res in tmdb_results if res.get("id") not in blocked_ids and not _is_content_filtered(res, *cf)]

    if not tmdb_results:
        return {"page": page, "total_pages": 1, "total_results": 0, "results": []}

    tmdb_ids = [res["id"] for res in tmdb_results]
    query = (
        select(Media)
        .options(joinedload(Media.show))
        .where(Media.tmdb_id.in_(tmdb_ids), Media.media_type == MediaType.movie)
    )
    result = await db.execute(query)
    local_map = {m.tmdb_id: m for m in result.scalars().all()}

    enriched = []
    for res in tmdb_results:
        tmdb_id = res["id"]
        if tmdb_id in local_map:
            enriched.append({**format_media(local_map[tmdb_id]), "in_library": True})
        else:
            enriched.append(
                {
                    "id": None,
                    "tmdb_id": tmdb_id,
                    "type": MediaType.movie,
                    "title": res.get("title"),
                    "poster_path": tmdb.poster_url(res.get("poster_path")),
                    "release_date": res.get("release_date"),
                    "tmdb_rating": res.get("vote_average"),
                    "in_library": False,
                    "adult": res.get("adult", False),
                }
            )
    await enrich_with_state(db, current_user.id, enriched)
    return {
        "page": data.get("page", 1),
        "total_pages": data.get("total_pages", 1),
        "total_results": data.get("total_results", 0),
        "results": enriched,
    }


@router.get("/trending/shows")
async def trending_shows(
    page: int = Query(1, ge=1),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    tmdb_key = await get_user_tmdb_key(db, current_user.id)
    data = await _sync_trending(MediaType.series, page, api_key=tmdb_key)
    tmdb_results = data.get("results", [])

    if not tmdb_results:
        return {"page": page, "total_pages": 1, "total_results": 0, "results": []}

    blocked_ids = await _get_blocked_ids(db, current_user.id, MediaType.series)
    cf = await _get_content_filters(db, current_user.id)
    tmdb_results = [res for res in tmdb_results if res.get("id") not in blocked_ids and not _is_content_filtered(res, *cf)]

    if not tmdb_results:
        return {"page": page, "total_pages": 1, "total_results": 0, "results": []}

    tmdb_ids = [res["id"] for res in tmdb_results]

    # Collect which of these show TMDB IDs the user has in their library
    collected_q = await db.execute(
        select(ShowModel.tmdb_id)
        .join(Media, Media.show_id == ShowModel.id)
        .join(Collection, Collection.media_id == Media.id)
        .where(Collection.user_id == current_user.id, ShowModel.tmdb_id.in_(tmdb_ids))
        .distinct()
    )
    in_library: set[int] = {row[0] for row in collected_q.all()}

    enriched = []
    for res in tmdb_results:
        tmdb_id = res["id"]
        enriched.append(
            {
                "id": None,
                "tmdb_id": tmdb_id,
                "type": MediaType.series,
                "title": res.get("name"),
                "poster_path": tmdb.poster_url(res.get("poster_path")),
                "backdrop_path": tmdb.poster_url(res.get("backdrop_path"), size="w1280"),
                "release_date": res.get("first_air_date"),
                "tmdb_rating": res.get("vote_average"),
                "in_library": tmdb_id in in_library,
                "adult": res.get("adult", False),
            }
        )
    await enrich_with_state(db, current_user.id, enriched)
    return {
        "page": data.get("page", 1),
        "total_pages": data.get("total_pages", 1),
        "total_results": data.get("total_results", 0),
        "results": enriched,
    }


@router.get("/on-air-today")
async def on_air_today(
    page: int = Query(default=1, ge=1),
    db: AsyncSession = Depends(get_db), current_user: User = Depends(get_current_user)
):
    tmdb_key = await get_user_tmdb_key(db, current_user.id)
    if not check_tmdb_key(tmdb_key):
        return {"results": [], "page": 1, "total_pages": 1, "total_results": 0}
    data = await tmdb.get_on_air_today(page=page, api_key=tmdb_key)
    tmdb_results = data.get("results", [])

    blocked_ids = await _get_blocked_ids(db, current_user.id, MediaType.series)
    cf = await _get_content_filters(db, current_user.id)
    tmdb_results = [res for res in tmdb_results if res.get("id") not in blocked_ids and not _is_content_filtered(res, *cf)]

    results = [
        {
            "id": None,
            "tmdb_id": s.get("id"),
            "type": "series",
            "title": s.get("name"),
            "poster_path": tmdb.poster_url(s.get("poster_path")),
            "backdrop_path": tmdb.poster_url(s.get("backdrop_path"), size="w780"),
            "tmdb_rating": s.get("vote_average"),
            "release_date": s.get("first_air_date"),
        }
        for s in tmdb_results
    ]
    await enrich_with_state(db, current_user.id, results)
    return {
        "results": results,
        "page": data.get("page", page),
        "total_pages": data.get("total_pages", 1),
        "total_results": data.get("total_results", 0),
    }


@router.get("/airing-today/collected")
async def airing_today_collected(
    page: int = Query(default=1, ge=1),
    timezone: str = Query(default="UTC"),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Return shows airing today on TMDB that the user has in their collection."""
    tmdb_key = await get_user_tmdb_key(db, current_user.id)
    if not check_tmdb_key(tmdb_key):
        return {"results": [], "page": 1, "total_pages": 1, "total_results": 0}

    from datetime import datetime
    from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

    try:
        user_tz = ZoneInfo(timezone)
    except (ZoneInfoNotFoundError, KeyError):
        user_tz = ZoneInfo("UTC")

    today = datetime.now(user_tz).date().isoformat()

    # Collect the user's show TMDB IDs in one query
    collected_q = await db.execute(
        select(ShowModel.tmdb_id)
        .join(Media, Media.show_id == ShowModel.id)
        .join(Collection, Collection.media_id == Media.id)
        .where(Collection.user_id == current_user.id)
        .distinct()
    )
    collected_tmdb_ids: set[int] = {row[0] for row in collected_q.all()}
    
    blocked_ids = await _get_blocked_ids(db, current_user.id, MediaType.series)
    cf = await _get_content_filters(db, current_user.id)
    
    # Filter the library IDs first (if they are blocked, don't even bother)
    collected_tmdb_ids = {tid for tid in collected_tmdb_ids if tid not in blocked_ids}

    if not collected_tmdb_ids:
        return {"results": [], "page": 1, "total_pages": 1, "total_results": 0}

    # Fetch all shows airing today to filter against collection
    # We fetch up to 20 pages (400 shows) to ensure we find enough collected shows
    try:
        first = await tmdb.get_on_air_today(page=1, api_key=tmdb_key)
    except Exception as e:
        print(f"Error fetching airing-today from TMDB: {e}")
        return {"results": [], "page": 1, "total_pages": 1, "total_results": 0}
        
    total_tmdb_pages = min(first.get("total_pages", 1), 20)
    all_tmdb_shows = list(first.get("results", []))

    if total_tmdb_pages > 1:
        pages_data = await asyncio.gather(
            *[tmdb.get_on_air_today(page=p, api_key=tmdb_key) for p in range(2, total_tmdb_pages + 1)],
            return_exceptions=True,
        )
        for page_data in pages_data:
            if isinstance(page_data, Exception):
                continue
            all_tmdb_shows.extend(page_data.get("results", []))

    # Keep only shows in the user's collection and not blocked/filtered
    collected_shows = [
        s for s in all_tmdb_shows 
        if s.get("id") in collected_tmdb_ids 
        and not _is_content_filtered(s, *cf)
    ]

    if not collected_shows:
        return {"results": [], "page": 1, "total_pages": 1, "total_results": 0}

    # Paginate the collected shows locally
    page_size = 20
    total_results = len(collected_shows)
    total_pages = (total_results + page_size - 1) // page_size
    
    start_idx = (page - 1) * page_size
    end_idx = start_idx + page_size
    page_shows = collected_shows[start_idx:end_idx]

    if not page_shows:
        return {"results": [], "page": page, "total_pages": total_pages, "total_results": total_results}

    semaphore = asyncio.Semaphore(10)

    async def fetch_episode(show: dict) -> dict | None:
        async with semaphore:
            try:
                detail = await tmdb.get_show_light(show["id"], api_key=tmdb_key)
            except Exception:
                detail = {}

        episode: dict | None = None
        for candidate in (detail.get("last_episode_to_air"), detail.get("next_episode_to_air")):
            if candidate and candidate.get("air_date") == today:
                episode = candidate
                break

        show_name = show.get("name")
        if episode:
            return {
                "id": None,
                "tmdb_id": episode.get("id") or show["id"],
                "uri_id": f"tmdb:e:{episode.get('id')}" if episode.get("id") else None,
                "type": "episode",
                "title": episode.get("name") or show_name,
                "show_title": show_name,
                "show_uri_id": f"tmdb:s:{show['id']}",
                "show_tmdb_id": show["id"],
                "season_number": episode.get("season_number"),
                "episode_number": episode.get("episode_number"),
                "poster_path": tmdb.poster_url(episode.get("still_path"), size="w780")
                    or tmdb.poster_url(show.get("backdrop_path"), size="w780"),
                "backdrop_path": tmdb.poster_url(show.get("backdrop_path"), size="w780"),
                "tmdb_rating": show.get("vote_average"),
                "release_date": episode.get("air_date"),
                "adult": show.get("adult", False),
            }
        return {
            "id": None,
            "tmdb_id": show["id"],
            "type": "series",
            "title": show_name,
            "poster_path": tmdb.poster_url(show.get("poster_path")),
            "backdrop_path": tmdb.poster_url(show.get("backdrop_path"), size="w780"),
            "tmdb_rating": show.get("vote_average"),
            "release_date": show.get("first_air_date"),
            "adult": show.get("adult", False),
        }

    results = list(await asyncio.gather(*[fetch_episode(s) for s in page_shows]))
    await enrich_with_state(db, current_user.id, results, apply_tvdb_metadata=False)
    
    # Filter out dropped shows
    filtered = [i for i in results if not i.get("is_dropped")]
    return {
        "results": filtered,
        "page": page,
        "total_pages": total_pages,
        "total_results": total_results
    }


@router.get("/recently-added")
async def recently_added(
    type: MediaType | None = Query(None),
    limit: int = Query(default=20, le=100),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    # Subquery: latest added_at per media for this user (deduplicates movies in both Plex+Jellyfin)
    coll_subq = (
        select(Collection.media_id, func.max(Collection.added_at).label("max_added"))
        .where(Collection.user_id == current_user.id)
        .group_by(Collection.media_id)
        .subquery()
    )
    media_filters = [
        # Exclude episodes missing season/episode numbers — they are unidentifiable
        # orphans (created by webhook before the show was synced) and cannot be displayed.
        or_(
            Media.media_type != MediaType.episode,
            and_(Media.season_number.isnot(None), Media.episode_number.isnot(None)),
        )
    ]
    if type:
        media_filters.append(Media.media_type == type)
    query = (
        select(Media)
        .join(coll_subq, coll_subq.c.media_id == Media.id)
        .options(joinedload(Media.show))
        .where(*media_filters)
        .order_by(coll_subq.c.max_added.desc())
        .limit(limit)
    )
    result = await db.execute(query)
    items = [format_media(m) for m in result.scalars().all()]
    await enrich_with_state(db, current_user.id, items, apply_tvdb_metadata=False)
    # Filter out blocked and dropped
    filtered = [i for i in items if not i.get("is_blocked") and not i.get("is_dropped")]
    return {"results": filtered}



_PERSON_PAGE_SIZE = 20

@router.get("/person/{person_id}")
async def get_person_details(
    person_id: str,
    page: int = Query(1, ge=1),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    # Accept URI ("tmdb:p:X" / "tvdb:p:Y") or raw integer. Dispatch TVDB via existing handler.
    if ":" in person_id:
        parts = person_id.split(":", 2)
        if len(parts) == 3 and parts[1] == "p":
            if parts[0] == "tvdb":
                return await get_person_details_tvdb(int(parts[2]), page, db, current_user)
            person_id = parts[2]
    try:
        person_id_int = int(person_id)
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Invalid person ID: {person_id!r}")
    try:
        tmdb_key = await get_user_tmdb_key(db, current_user.id)
        if not check_tmdb_key(tmdb_key):
            raise HTTPException(status_code=404, detail="TMDB API Key not configured")
        data = await tmdb.get_person(person_id_int, api_key=tmdb_key)
        credits = data.get("combined_credits", {})
        cast_credits = credits.get("cast", [])
        formatted_credits = []
        for c in cast_credits:
            m_type = "movie" if c.get("media_type") == "movie" else "series"
            popularity = c.get("popularity", 0)
            # Role weight: how significant was this person's role?
            # Movies: billing order (0 = lead, higher = smaller part)
            # TV: episode count (more episodes = regular cast, not a guest)
            if c.get("media_type") == "tv":
                episode_count = c.get("episode_count") or 0
                role_weight = min(episode_count, 20) / 20.0
            else:
                order = c.get("order") or 0
                role_weight = max(0.05, 1.0 - order * 0.05)
            formatted_credits.append(
                {
                    "tmdb_id": c.get("id"),
                    "type": m_type,
                    "title": c.get("title") or c.get("name"),
                    "poster_path": tmdb.poster_url(c.get("poster_path")),
                    "release_date": c.get("release_date") or c.get("first_air_date"),
                    "character": c.get("character"),
                    "popularity": popularity,
                    "adult": c.get("adult", False),
                    "genre_ids": c.get("genre_ids"),
                    "overview": c.get("overview"),
                    "_score": popularity * max(role_weight, 0.05),
                }
            )
        # Deduplicate by tmdb_id — a person may appear in multiple episodes of the
        # same show; keep the entry with the highest score.
        seen: dict[int, int] = {}  # tmdb_id -> index in formatted_credits
        deduped: list[dict] = []
        for credit in formatted_credits:
            tid = credit["tmdb_id"]
            if tid in seen:
                if credit["_score"] > deduped[seen[tid]]["_score"]:
                    deduped[seen[tid]] = credit
            else:
                seen[tid] = len(deduped)
                deduped.append(credit)

        # Enrichment happens above
        non_blocked = [c for c in deduped if not c.get("is_blocked")]

        # Sort by release date (most recent first), items without date at the end
        non_blocked.sort(key=lambda x: x.get("release_date") or "", reverse=True)
        for credit in non_blocked:
            if "_score" in credit:
                del credit["_score"]

        total_credits = len(non_blocked)
        start = (page - 1) * _PERSON_PAGE_SIZE
        top_credits = non_blocked[start:start + _PERSON_PAGE_SIZE]

        # Which of the user's lists contain this person?
        user_list_ids_q = await db.execute(select(UserList.id).where(UserList.user_id == current_user.id))
        user_list_ids = [r[0] for r in user_list_ids_q.all()]
        person_in_lists: list[int] = []
        if user_list_ids:
            li_q = await db.execute(
                select(ListItem.list_id)
                .join(Media, Media.id == ListItem.media_id)
                .where(
                    ListItem.list_id.in_(user_list_ids),
                    Media.tmdb_id == person_id_int,
                    Media.media_type == MediaType.person,
                )
            )
            person_in_lists = [r[0] for r in li_q.all()]

        _pid = data.get("id")
        return {
            "uri_id": f"tmdb:p:{_pid}" if _pid else None,
            "tmdb_id": _pid,
            "name": data.get("name"),
            "biography": data.get("biography"),
            "profile_path": tmdb.poster_url(data.get("profile_path"), size="h632"),
            "birthday": data.get("birthday"),
            "place_of_birth": data.get("place_of_birth"),
            "known_for_department": data.get("known_for_department"),
            "credits": top_credits,
            "total_credits": total_credits,
            "page": page,
            "page_size": _PERSON_PAGE_SIZE,
            "in_lists": person_in_lists,
        }
    except Exception as e:
        if isinstance(e, HTTPException):
            raise e
        raise HTTPException(status_code=404, detail=f"Person not found: {e}")


# Internal handler — invoked by `/person/{id}` URI dispatcher when provider=tvdb.
async def get_person_details_tvdb(
    person_id: int,
    page: int = Query(1, ge=1),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    try:
        # Retrieve TVDB API Key
        api_key = None
        user_settings_q = await db.execute(
            select(UserSettings).where(UserSettings.user_id == current_user.id)
        )
        user_settings = user_settings_q.scalar_one_or_none()
        if user_settings and user_settings.tvdb_api_key:
            api_key = user_settings.tvdb_api_key
        else:
            gs_q = await db.execute(select(GlobalSettings).where(GlobalSettings.id == 1))
            gs = gs_q.scalar_one_or_none()
            if gs:
                api_key = gs.tvdb_api_key
        
        if not api_key:
            raise HTTPException(status_code=404, detail="TVDB API Key not configured")
        
        # User language preference
        user_lang = await get_user_content_language(db, current_user.id)
        from core import tvdb as tvdb_client
        tvdb_lang = tvdb_client.to_three_letter_lang(user_lang)

        # Fetch TVDB person details (extended includes characters)
        data = await tvdb_client.get_person(person_id, api_key=api_key)
        if not data:
            raise HTTPException(status_code=404, detail="Person not found on TVDB")
        
        # Normalize fields
        biographies = data.get("biographies") or []
        biography = None
        for bio_entry in biographies:
            if bio_entry.get("language") == tvdb_lang:
                biography = bio_entry.get("biography")
                break
        if not biography and tvdb_lang != "eng":
            for bio_entry in biographies:
                if bio_entry.get("language") == "eng":
                    biography = bio_entry.get("biography")
                    break
        if not biography and biographies:
            biography = biographies[0].get("biography")

        birthday = data.get("birth")
        place_of_birth = data.get("birthPlace")
        profile_path = tvdb_client._image_url(data.get("image"))

        # Format characters (credits)
        characters = data.get("characters") or []
        formatted_credits = []

        tmdb_key = await get_user_tmdb_key(db, current_user.id)
        has_tmdb = check_tmdb_key(tmdb_key)

        for c in characters:
            series_info = c.get("series")
            movie_info = c.get("movie")
            
            series_id = c.get("seriesId") or (series_info.get("id") if series_info else None)
            movie_id = c.get("movieId") or (movie_info.get("id") if movie_info else None)
            
            if not series_id and not movie_id:
                continue

            char_name = c.get("name")

            if series_id:
                title = series_info.get("name") if series_info else None
                if not title:
                    title = f"Series {series_id}"
                poster_path = tvdb_client._image_url(series_info.get("image")) if series_info else None
                year = series_info.get("year") if series_info else None
                
                sort_order = c.get("sort") or 0
                role_weight = max(0.05, 1.0 - sort_order * 0.05)
                
                formatted_credits.append(
                    {
                        "tvdb_id": series_id,
                        "tmdb_id": None,
                        "type": "series",
                        "title": title,
                        "poster_path": poster_path,
                        "release_date": str(year) if year else None,
                        "character": char_name,
                        "popularity": 0,
                        "adult": False,
                        "genre_ids": [],
                        "overview": None,
                        "_score": role_weight,
                    }
                )
            elif movie_id:
                movie_title = movie_info.get("name") if movie_info else None
                if not movie_title:
                    movie_title = f"Movie {movie_id}"
                movie_year = movie_info.get("year") if movie_info else None
                
                year_val = None
                if movie_year:
                    match = re.search(r'\b\d{4}\b', str(movie_year))
                    if match:
                        year_val = int(match.group(0))

                tmdb_movie_id = None
                poster_path = tvdb_client._image_url(movie_info.get("image")) if movie_info else None
                overview = None
                popularity = 0
                
                if has_tmdb and movie_title:
                    try:
                        search_res = await tmdb.search_movies(movie_title, year=year_val, api_key=tmdb_key)
                        results = search_res.get("results") or []
                        if results:
                            best_match = results[0]
                            tmdb_movie_id = best_match.get("id")
                            poster_path = tmdb.poster_url(best_match.get("poster_path"))
                            overview = best_match.get("overview")
                            popularity = best_match.get("popularity") or 0
                    except Exception as e:
                        print(f"Failed to resolve TVDB movie {movie_id} to TMDB: {e}")

                sort_order = c.get("sort") or 0
                role_weight = max(0.05, 1.0 - sort_order * 0.05)

                formatted_credits.append(
                    {
                        "tmdb_id": tmdb_movie_id,
                        "tvdb_id": movie_id,
                        "type": "movie",
                        "title": movie_title,
                        "poster_path": poster_path,
                        "release_date": str(movie_year) if movie_year else None,
                        "character": char_name,
                        "popularity": popularity,
                        "adult": False,
                        "genre_ids": [],
                        "overview": overview,
                        "_score": role_weight,
                    }
                )

        # Deduplicate credits: key by type + id, keeping highest score
        seen: dict[str, int] = {}
        deduped: list[dict] = []
        for credit in formatted_credits:
            key = f"{credit['type']}_{credit['tmdb_id'] or credit['tvdb_id']}"
            if key in seen:
                if credit["_score"] > deduped[seen[key]]["_score"]:
                    deduped[seen[key]] = credit
            else:
                seen[key] = len(deduped)
                deduped.append(credit)

        # Get blocked / dropped state and content filters
        blocked_series = await _get_blocked_ids(db, current_user.id, MediaType.series)
        cf = await _get_content_filters(db, current_user.id)
        
        tvdb_series_ids = [c["tvdb_id"] for c in deduped if c["type"] == "series" and c["tvdb_id"]]
        
        local_shows_map = {}
        blocked_db = {}
        if tvdb_series_ids:
            local_shows_q = await db.execute(
                select(ShowModel).where(ShowModel.tvdb_id.in_(tvdb_series_ids))
            )
            for show in local_shows_q.scalars().all():
                local_shows_map[show.tvdb_id] = show

            # Build URI set to query: tvdb:s:X for each tvdb_id, plus tmdb:s:Y for shows with TMDB cross-ref
            block_uris: list[str] = [f"tvdb:s:{tvdb_id}" for tvdb_id in tvdb_series_ids]
            for show in local_shows_map.values():
                if show.uri_id and show.uri_id not in block_uris:
                    block_uris.append(show.uri_id)

            block_q = await db.execute(
                select(BlocklistItem.uri_id, BlocklistItem.is_dropped)
                .where(
                    BlocklistItem.user_id == current_user.id,
                    BlocklistItem.uri_id.in_(block_uris),
                )
            )
            for b_uri, is_dropped in block_q.all():
                # Map uri back to numeric ID key for downstream tvdb_id check
                parts = b_uri.split(":", 2)
                if len(parts) == 3:
                    try:
                        blocked_db[int(parts[2])] = is_dropped
                    except ValueError:
                        pass

        enrichable_items = []
        for credit in deduped:
            if credit["type"] == "movie":
                if credit["tmdb_id"]:
                    enrichable_items.append(credit)
            elif credit["type"] == "series":
                tvdb_id = credit["tvdb_id"]
                show_obj = local_shows_map.get(tvdb_id)
                if show_obj:
                    credit["id"] = show_obj.id
                    if show_obj.tmdb_id:
                        credit["tmdb_id"] = show_obj.tmdb_id
                        enrichable_items.append(credit)
                    else:
                        # TVDB-only show in database, calculate metadata manually
                        watched_q = await db.execute(
                            select(func.count(func.distinct(Media.episode_number)))
                            .join(WatchEvent, WatchEvent.media_id == Media.id)
                            .where(Media.show_id == show_obj.id, WatchEvent.user_id == current_user.id, Media.media_type == MediaType.episode)
                        )
                        watched_count = watched_q.scalar() or 0
                        
                        collected_q = await db.execute(
                            select(func.count(func.distinct(Media.episode_number)))
                            .join(Collection, Collection.media_id == Media.id)
                            .where(Media.show_id == show_obj.id, Collection.user_id == current_user.id, Media.media_type == MediaType.episode)
                        )
                        collected_count = collected_q.scalar() or 0
                        
                        total_episodes = sum(s.get("episode_count", 0) for s in (show_obj.tmdb_data or {}).get("seasons", []) if s.get("season_number") != 0)
                        
                        is_blocked = tvdb_id in blocked_db or _is_content_filtered(credit, *cf)
                        
                        credit["watched"] = watched_count >= total_episodes if total_episodes > 0 else False
                        credit["in_library"] = collected_count > 0
                        credit["collection_pct"] = min(100, int((collected_count / total_episodes) * 100)) if total_episodes > 0 else 0
                        credit["watched_episodes_count"] = watched_count
                        credit["total_episodes_count"] = total_episodes
                        credit["in_lists"] = []
                        credit["is_monitored"] = False
                        credit["request_enabled"] = False
                        credit["request_status"] = None
                        credit["user_rating"] = None
                        credit["play_count"] = 0
                        credit["is_blocked"] = is_blocked
                        credit["is_dropped"] = blocked_db.get(tvdb_id, False)
                else:
                    # TVDB show not in database
                    is_blocked = tvdb_id in blocked_db or _is_content_filtered(credit, *cf)
                    credit["watched"] = False
                    credit["in_library"] = False
                    credit["collection_pct"] = 0
                    credit["watched_episodes_count"] = 0
                    credit["total_episodes_count"] = 0
                    credit["in_lists"] = []
                    credit["is_monitored"] = False
                    credit["request_enabled"] = False
                    credit["request_status"] = None
                    credit["user_rating"] = None
                    credit["play_count"] = 0
                    credit["is_blocked"] = is_blocked
                    credit["is_dropped"] = blocked_db.get(tvdb_id, False)

        # Enrich the rest (movies and mapped series)
        if enrichable_items:
            await enrich_with_state(db, current_user.id, enrichable_items)

        # Filter out blocked credits
        non_blocked = [c for c in deduped if not c.get("is_blocked")]

        # Sort by release date (most recent first), items without date at the end
        non_blocked.sort(key=lambda x: x.get("release_date") or "", reverse=True)

        for credit in non_blocked:
            if "_score" in credit:
                del credit["_score"]

        total_credits = len(non_blocked)
        start = (page - 1) * _PERSON_PAGE_SIZE
        top_credits = non_blocked[start:start + _PERSON_PAGE_SIZE]

        # Check list membership for the person
        user_list_ids_q = await db.execute(select(UserList.id).where(UserList.user_id == current_user.id))
        user_list_ids = [r[0] for r in user_list_ids_q.all()]
        person_in_lists: list[int] = []
        if user_list_ids:
            li_q = await db.execute(
                select(ListItem.list_id)
                .join(Media, Media.id == ListItem.media_id)
                .where(
                    ListItem.list_id.in_(user_list_ids),
                    Media.tmdb_id == person_id,
                    Media.media_type == MediaType.person,
                )
            )
            person_in_lists = [r[0] for r in li_q.all()]

        _tvdb_pid = data.get("id")
        return {
            "uri_id": f"tvdb:p:{_tvdb_pid}" if _tvdb_pid else None,
            "tmdb_id": None,
            "tvdb_id": _tvdb_pid,
            "name": data.get("name"),
            "biography": biography,
            "profile_path": profile_path,
            "birthday": birthday,
            "place_of_birth": place_of_birth,
            "known_for_department": "Acting",
            "credits": top_credits,
            "total_credits": total_credits,
            "page": page,
            "page_size": _PERSON_PAGE_SIZE,
            "in_lists": person_in_lists,
        }
    except Exception as e:
        if isinstance(e, HTTPException):
            raise e
        raise HTTPException(status_code=404, detail=f"Person not found: {e}")


@router.get("/collection/{collection_id}")
async def get_collection_details(
    collection_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    try:
        tmdb_key = await get_user_tmdb_key(db, current_user.id)
        if not check_tmdb_key(tmdb_key):
            raise HTTPException(status_code=404, detail="TMDB API Key not configured")

        data, genre_data = await asyncio.gather(
            tmdb.get_collection(collection_id, api_key=tmdb_key),
            tmdb.get_genre_list(api_key=tmdb_key),
        )

        genre_map = {g["id"]: g["name"] for g in genre_data.get("genres", [])}
        parts_data = sorted(data.get("parts", []), key=lambda x: x.get("release_date") or "")

        # Fetch credits for all parts in parallel (cap at 15 to avoid long waits)
        credit_results = await asyncio.gather(
            *[tmdb.get_movie_credits(p["id"], api_key=tmdb_key) for p in parts_data[:15]],
            return_exceptions=True,
        )

        # Aggregate unique genres from all parts
        all_genre_ids: set[int] = set()
        for p in parts_data:
            all_genre_ids.update(p.get("genre_ids", []))
        genres = [genre_map[gid] for gid in all_genre_ids if gid in genre_map]

        # Aggregate cast: rank by number of appearances across films, then popularity
        person_data: dict[int, dict] = {}
        for credits in credit_results:
            if isinstance(credits, Exception):
                continue
            for person in credits.get("cast", [])[:20]:
                pid = person.get("id")
                if pid not in person_data:
                    person_data[pid] = {
                        "tmdb_id": pid,
                        "name": person.get("name"),
                        "profile_path": tmdb.poster_url(person.get("profile_path"), size="w185"),
                        "appearances": 0,
                        "popularity": person.get("popularity", 0),
                    }
                person_data[pid]["appearances"] += 1

        cast = sorted(
            person_data.values(),
            key=lambda x: (-x["appearances"], -x["popularity"]),
        )[:15]

        parts = [
            {
                "tmdb_id": p.get("id"),
                "type": "movie",
                "title": p.get("title"),
                "poster_path": tmdb.poster_url(p.get("poster_path")),
                "backdrop_path": tmdb.poster_url(p.get("backdrop_path"), size="w1280"),
                "release_date": p.get("release_date"),
                "tmdb_rating": p.get("vote_average"),
                "overview": p.get("overview"),
            }
            for p in parts_data
        ]
        await enrich_with_state(db, current_user.id, parts)
        parts = [p for p in parts if not p.get("is_blocked")]

        return {
            "id": data.get("id"),
            "name": data.get("name"),
            "overview": data.get("overview"),
            "poster_path": tmdb.poster_url(data.get("poster_path")),
            "backdrop_path": tmdb.poster_url(data.get("backdrop_path"), size="w1280"),
            "genres": genres,
            "cast": cast,
            "parts": parts,
        }

    except Exception as e:
        if isinstance(e, HTTPException):
            raise e
        raise HTTPException(status_code=404, detail=f"Collection not found: {e}")


@router.get("/tmdb/list")
async def get_tmdb_list(
    type: MediaType = Query(...),
    category: str = Query("popular"),
    page: int = Query(1, ge=1),
    genre: str | None = Query(None),
    year: int | None = Query(None),
    min_rating: float | None = Query(None),
    status: str | None = Query(None),
    provider_id: int | None = Query(None),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    try:
        tmdb_key = await get_user_tmdb_key(db, current_user.id)
        if not check_tmdb_key(tmdb_key):
            return {"page": page, "total_pages": 1, "total_results": 0, "results": []}

        category_sort_map = {
            "popular": "popularity.desc",
            "top_rated": "vote_average.desc",
            "trending": "popularity.desc",
        }
        has_filters = bool(genre or year or min_rating or status or provider_id)

        if has_filters:
            sort_by = category_sort_map.get(category, "popularity.desc")
            
            # Use user's region if available
            region = "US"
            profile_q = await db.execute(select(UserProfileData).where(UserProfileData.user_id == current_user.id))
            profile = profile_q.scalar_one_or_none()
            if profile and profile.country:
                region = profile.country

            if type == MediaType.movie:
                genre_id = MOVIE_GENRE_IDS.get(genre) if genre else None
                data = await tmdb.discover_movies(
                    page=page, genre_id=genre_id, year=year,
                    min_rating=min_rating, sort_by=sort_by, api_key=tmdb_key,
                    watch_provider_id=provider_id, watch_region=region
                )
            else:
                genre_id = TV_GENRE_IDS.get(genre) if genre else None
                status_id = TV_STATUS_IDS.get(status) if status else None
                data = await tmdb.discover_shows(
                    page=page, genre_id=genre_id, year=year,
                    min_rating=min_rating, sort_by=sort_by,
                    status=status_id, api_key=tmdb_key,
                    watch_provider_id=provider_id, watch_region=region
                )
        elif type == MediaType.movie:
            if category == "top_rated":
                data = await tmdb.get_top_rated_movies(page=page, api_key=tmdb_key)
            elif category == "trending":
                data = await tmdb.get_trending_movies(page=page, api_key=tmdb_key)
            else:
                data = await tmdb.get_popular_movies(page=page, api_key=tmdb_key)
        else:  # series/episode
            if category == "top_rated":
                data = await tmdb.get_top_rated_shows(page=page, api_key=tmdb_key)
            elif category == "trending":
                data = await tmdb.get_trending_shows(page=page, api_key=tmdb_key)
            else:
                data = await tmdb.get_popular_shows(page=page, api_key=tmdb_key)

        results = data.get("results", [])

        blocked_ids = await _get_blocked_ids(db, current_user.id, type)
        cf = await _get_content_filters(db, current_user.id)
        results = [r for r in results if r["id"] not in blocked_ids and not _is_content_filtered(r, *cf)]

        tmdb_ids = [res["id"] for res in results]

        # Check local library
        if type == MediaType.series:
            # Match against Show.tmdb_id — never use episode tmdb_ids here,
            # as TMDB IDs across shows and episodes share the same number space
            # and collide (causing episodes to appear in show listings).
            show_q = (
                select(ShowModel.tmdb_id)
                .join(Media, Media.show_id == ShowModel.id)
                .join(Collection, Collection.media_id == Media.id)
                .where(
                    Collection.user_id == current_user.id,
                    ShowModel.tmdb_id.in_(tmdb_ids),
                )
                .distinct()
            )
            show_result = await db.execute(show_q)
            library_tmdb_ids = {row[0] for row in show_result.all()}
        else:
            query = (
                select(Media)
                .where(Media.tmdb_id.in_(tmdb_ids), Media.media_type == MediaType.movie)
            )
            result = await db.execute(query)
            library_tmdb_ids = {m.tmdb_id for m in result.scalars().all()}

        enriched = []
        for res in results:
            tmdb_id = res["id"]
            enriched.append(
                {
                    "id": None,
                    "tmdb_id": tmdb_id,
                    "type": type,
                    "title": res.get("title") or res.get("name"),
                    "poster_path": tmdb.poster_url(res.get("poster_path")),
                    "release_date": res.get("release_date") or res.get("first_air_date"),
                    "tmdb_rating": res.get("vote_average"),
                    "in_library": tmdb_id in library_tmdb_ids,
                    "adult": res.get("adult", False),
                }
            )
        await enrich_with_state(db, current_user.id, enriched)
        return {
            "page": data.get("page", 1),
            "total_pages": data.get("total_pages", 1),
            "total_results": data.get("total_results", 0),
            "results": enriched,
        }
    except Exception as e:
        print(f"Error fetching TMDB list: {e}")
        return {"page": page, "total_pages": 1, "total_results": 0, "results": []}


from sqlalchemy import delete as sa_delete
from pydantic import BaseModel as PydanticModel


class CollectRequest(PydanticModel):
    uri_id: str                              # REQUIRED — media URI
    media_type: MediaType
    # Episode context — required when collecting an episode that doesn't exist in the DB yet
    show_uri_id: Optional[str] = None
    season_number: Optional[int] = None
    episode_number: Optional[int] = None


class CollectSeasonRequest(PydanticModel):
    show_uri_id: str                          # REQUIRED — show URI
    season_number: int


def _enrich_movie_list(results: list[dict], library_ids: set[int]) -> list[dict]:
    return [
        {
            "id": None,
            "tmdb_id": r["id"],
            "type": MediaType.movie,
            "title": r.get("title"),
            "poster_path": tmdb.poster_url(r.get("poster_path")),
            "backdrop_path": tmdb.poster_url(r.get("backdrop_path"), size="w1280"),
            "release_date": r.get("release_date"),
            "tmdb_rating": r.get("vote_average"),
            "in_library": r["id"] in library_ids,
            "adult": r.get("adult", False),
        }
        for r in results if r.get("id")
    ]


def _enrich_show_list(results: list[dict], library_ids: set[int]) -> list[dict]:
    return [
        {
            "id": None,
            "tmdb_id": r["id"],
            "type": MediaType.series,
            "title": r.get("name"),
            "poster_path": tmdb.poster_url(r.get("poster_path")),
            "backdrop_path": tmdb.poster_url(r.get("backdrop_path"), size="w1280"),
            "release_date": r.get("first_air_date"),
            "tmdb_rating": r.get("vote_average"),
            "in_library": r["id"] in library_ids,
            "adult": r.get("adult", False),
        }
        for r in results if r.get("id")
    ]


async def _movie_library_ids(db: AsyncSession, user_id: int, tmdb_ids: list[int]) -> set[int]:
    q = await db.execute(
        select(Media.tmdb_id)
        .join(Collection, Collection.media_id == Media.id)
        .where(Collection.user_id == user_id, Media.tmdb_id.in_(tmdb_ids), Media.media_type == MediaType.movie)
        .distinct()
    )
    return {row[0] for row in q.all()}


async def _show_library_ids(db: AsyncSession, user_id: int, tmdb_ids: list[int]) -> set[int]:
    q = await db.execute(
        select(ShowModel.tmdb_id)
        .join(Media, Media.show_id == ShowModel.id)
        .join(Collection, Collection.media_id == Media.id)
        .where(Collection.user_id == user_id, ShowModel.tmdb_id.in_(tmdb_ids))
        .distinct()
    )
    return {row[0] for row in q.all()}


@router.get("/now-playing")
async def now_playing(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    tmdb_key = await get_user_tmdb_key(db, current_user.id)
    if not check_tmdb_key(tmdb_key):
        return {"results": []}
    try:
        data = await tmdb.get_now_playing(api_key=tmdb_key)
        results = data.get("results", [])

        blocked_ids = await _get_blocked_ids(db, current_user.id, MediaType.movie)
        cf = await _get_content_filters(db, current_user.id)
        results = [res for res in results if res.get("id") not in blocked_ids and not _is_content_filtered(res, *cf)]

        ids = [r["id"] for r in results if r.get("id")]
        lib = await _movie_library_ids(db, current_user.id, ids)
        items = _enrich_movie_list(results, lib)
        await enrich_with_state(db, current_user.id, items)
        return {"results": items}
    except Exception:
        return {"results": []}


@router.get("/trending/trailers")
async def trending_trailers(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    tmdb_key = await get_user_tmdb_key(db, current_user.id)
    if not check_tmdb_key(tmdb_key):
        return {"results": []}
    try:
        data = await tmdb.get_trending_movies(time_window="week", api_key=tmdb_key)
        movies = data.get("results", [])

        blocked_ids = await _get_blocked_ids(db, current_user.id, MediaType.movie)
        cf = await _get_content_filters(db, current_user.id)
        movies = [m for m in movies if m.get("id") not in blocked_ids and not _is_content_filtered(m, *cf)][:16]

        async def fetch_trailer(movie: dict) -> dict | None:
            try:
                vdata = await tmdb.get_movie_videos(movie["id"], api_key=tmdb_key)
                videos = vdata.get("results", [])
                trailer = next(
                    (v for v in videos if v.get("site") == "YouTube" and v.get("type") == "Trailer" and v.get("official")),
                    next((v for v in videos if v.get("site") == "YouTube" and v.get("type") == "Trailer"), None),
                )
                if not trailer:
                    return None
                return {
                    "tmdb_id": movie["id"],
                    "title": movie.get("title") or movie.get("name"),
                    "poster_path": tmdb.poster_url(movie.get("poster_path")),
                    "backdrop_path": tmdb.poster_url(movie.get("backdrop_path"), size="w780"),
                    "release_date": movie.get("release_date"),
                    "trailer_key": trailer["key"],
                    "trailer_name": trailer.get("name", ""),
                }
            except Exception:
                return None

        results_raw = await asyncio.gather(*[fetch_trailer(m) for m in movies])
        results = [r for r in results_raw if r is not None]
        return {"results": results}
    except Exception:
        return {"results": []}

async def _filter_and_enrich_discover_results(
    db: AsyncSession,
    user_id: int,
    results: list[dict],
    media_type: MediaType,
) -> list[dict]:
    blocked_ids = await _get_blocked_ids(db, user_id, media_type)
    cf = await _get_content_filters(db, user_id)
    filtered = [res for res in results if res.get("id") not in blocked_ids and not _is_content_filtered(res, *cf)]
    
    ids = [r["id"] for r in filtered if r.get("id")]
    if media_type == MediaType.movie:
        lib = await _movie_library_ids(db, user_id, ids)
        items = _enrich_movie_list(filtered, lib)
    else:
        lib = await _show_library_ids(db, user_id, ids)
        items = _enrich_show_list(filtered, lib)
        
    await enrich_with_state(db, user_id, items)
    return items


@router.get("/upcoming")
async def upcoming_movies(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    tmdb_key = await get_user_tmdb_key(db, current_user.id)
    if not check_tmdb_key(tmdb_key):
        return {"results": []}
    try:
        data = await tmdb.get_upcoming_movies(api_key=tmdb_key)
        results = data.get("results", [])

        items = await _filter_and_enrich_discover_results(db, current_user.id, results, MediaType.movie)
        return {"results": items}
    except Exception:
        return {"results": []}


@router.get("/on-air-this-week")
async def on_air_this_week(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    tmdb_key = await get_user_tmdb_key(db, current_user.id)
    if not check_tmdb_key(tmdb_key):
        return {"results": []}
    try:
        data = await tmdb.get_on_air_this_week(api_key=tmdb_key)
        results = data.get("results", [])

        items = await _filter_and_enrich_discover_results(db, current_user.id, results, MediaType.series)
        return {"results": items}
    except Exception:
        return {"results": []}


@router.get("/hidden-gems")
async def hidden_gems(
    type: MediaType = Query(MediaType.movie),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    import random
    tmdb_key = await get_user_tmdb_key(db, current_user.id)
    if not check_tmdb_key(tmdb_key):
        return {"results": []}
    try:
        page = random.randint(1, 5)
        if type == MediaType.movie:
            data = await tmdb.discover_movies(
                page=page, sort_by="vote_average.desc",
                min_rating=7.5, vote_count_min=150, vote_count_max=3000,
                api_key=tmdb_key,
            )
            results = data.get("results", [])

            items = await _filter_and_enrich_discover_results(db, current_user.id, results, MediaType.movie)
            return {"results": items}
        else:
            data = await tmdb.discover_shows(
                page=page, sort_by="vote_average.desc",
                min_rating=7.5, vote_count_min=150, vote_count_max=3000,
                api_key=tmdb_key,
            )
            results = data.get("results", [])

            items = await _filter_and_enrich_discover_results(db, current_user.id, results, MediaType.series)
            return {"results": items}
    except Exception:
        return {"results": []}


@router.get("/top-rated-movies")
async def top_rated_movies(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    tmdb_key = await get_user_tmdb_key(db, current_user.id)
    if not check_tmdb_key(tmdb_key):
        return {"results": []}
    try:
        data = await tmdb.get_top_rated_movies(api_key=tmdb_key)
        results = data.get("results", [])

        items = await _filter_and_enrich_discover_results(db, current_user.id, results, MediaType.movie)
        return {"results": items}
    except Exception:
        return {"results": []}


@router.get("/top-rated-shows")
async def top_rated_shows(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    tmdb_key = await get_user_tmdb_key(db, current_user.id)
    if not check_tmdb_key(tmdb_key):
        return {"results": []}
    try:
        data = await tmdb.get_top_rated_shows(api_key=tmdb_key)
        results = data.get("results", [])

        items = await _filter_and_enrich_discover_results(db, current_user.id, results, MediaType.series)
        return {"results": items}
    except Exception:
        return {"results": []}


# TMDB watch provider IDs for reference:
# Netflix=8, Amazon Prime=9, Apple TV+=350, Disney+=337, Max=1899, Hulu=15
@router.get("/for-you")
async def for_you(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    import random
    from models.profile import UserProfileData

    cached = _FOR_YOU_CACHE.get(current_user.id)
    if cached and (_time.monotonic() - cached[0]) < _FOR_YOU_TTL:
        return cached[1]

    tmdb_key = await get_user_tmdb_key(db, current_user.id)
    if not check_tmdb_key(tmdb_key):
        return {"results": []}

    profile_q = await db.execute(
        select(UserProfileData).where(UserProfileData.user_id == current_user.id)
    )
    profile = profile_q.scalar_one_or_none()

    if not profile:
        return {"results": []}

    liked_genres = profile.liked_genres or []
    disliked_genres: set[str] = set(profile.disliked_genres or [])
    language: str | None = getattr(profile, "content_language", None)

    if not liked_genres:
        return {"results": []}

    selected_genres = random.sample(liked_genres, min(4, len(liked_genres)))

    movie_coros = []
    show_coros = []

    for genre_name in selected_genres:
        # Check if it's a movie genre
        movie_genre_id = MOVIE_GENRE_IDS.get(genre_name)
        if movie_genre_id:
            movie_coros.append(tmdb.discover_movies(
                genre_id=movie_genre_id,
                sort_by="popularity.desc",
                with_original_language=language,
                api_key=tmdb_key,
            ))
        
        # Check if it's a TV genre
        tv_genre_id = TV_GENRE_IDS.get(genre_name)
        if tv_genre_id:
            show_coros.append(tmdb.discover_shows(
                genre_id=tv_genre_id,
                sort_by="popularity.desc",
                with_original_language=language,
                api_key=tmdb_key,
            ))

    if not movie_coros and not show_coros:
        return {"results": []}

    all_results = await asyncio.gather(*(movie_coros + show_coros), return_exceptions=True)

    num_movie_coros = len(movie_coros)
    movie_raw: list[dict] = []
    show_raw: list[dict] = []

    for i, res in enumerate(all_results):
        if isinstance(res, Exception):
            continue
        raw = res.get("results", [])[:8]
        if i < num_movie_coros:
            movie_raw.extend(raw)
        else:
            show_raw.extend(raw)

    movie_liked_set = set(liked_genres)
    show_liked_set = set(liked_genres)

    seen: set[int] = set()
    unique_movies: list[dict] = []
    for r in _filter_disliked(movie_raw, disliked_genres, movie_liked_set, MOVIE_GENRE_NAMES):
        rid = r.get("id")
        if rid and rid not in seen:
            seen.add(rid)
            unique_movies.append(r)

    seen2: set[int] = set()
    unique_shows: list[dict] = []
    for r in _filter_disliked(show_raw, disliked_genres, show_liked_set, TV_GENRE_NAMES):
        rid = r.get("id")
        if rid and rid not in seen2:
            seen2.add(rid)
            unique_shows.append(r)

    movie_ids = [r["id"] for r in unique_movies]
    show_ids = [r["id"] for r in unique_shows]

    movie_lib = await _movie_library_ids(db, current_user.id, movie_ids) if movie_ids else set()
    show_lib = await _show_library_ids(db, current_user.id, show_ids) if show_ids else set()

    # Filter out blocked items and content
    blocked_movies = await _get_blocked_ids(db, current_user.id, MediaType.movie)
    blocked_series = await _get_blocked_ids(db, current_user.id, MediaType.series)
    cf = await _get_content_filters(db, current_user.id)

    movie_items = _enrich_movie_list(unique_movies, movie_lib)
    show_items = _enrich_show_list(unique_shows, show_lib)

    combined = []
    for item in movie_items:
        if item["tmdb_id"] not in blocked_movies and not _is_content_filtered(item, *cf):
            combined.append(item)
    for item in show_items:
        if item["tmdb_id"] not in blocked_series and not _is_content_filtered(item, *cf):
            combined.append(item)

    random.shuffle(combined)

    await enrich_with_state(db, current_user.id, combined)
    unwatched = [item for item in combined if not item.get("watched")]
    result = {"results": unwatched[:20]}
    _FOR_YOU_CACHE[current_user.id] = (_time.monotonic(), result)
    return result


@router.get("/streaming")
async def streaming(
    provider_id: int,
    type: MediaType = Query(MediaType.movie),
    watch_region: str = Query("US"),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    tmdb_key = await get_user_tmdb_key(db, current_user.id)
    if not check_tmdb_key(tmdb_key):
        return {"results": []}
    try:
        if type == MediaType.movie:
            data = await tmdb.discover_movies(
                watch_provider_id=provider_id,
                watch_region=watch_region,
                api_key=tmdb_key,
            )
        else:
            data = await tmdb.discover_shows(
                watch_provider_id=provider_id,
                watch_region=watch_region,
                api_key=tmdb_key,
            )
        results = data.get("results", [])
        items = await _filter_and_enrich_discover_results(db, current_user.id, results, type)
        return {"results": items}
    except Exception:
        return {"results": []}




@router.get("/genres")
async def get_genres(
    type: str = Query("movie"),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    tmdb_key = await get_user_tmdb_key(db, current_user.id)
    if not check_tmdb_key(tmdb_key):
        return {"genres": []}
    try:
        if type == "movie":
            return await tmdb.get_genre_list(api_key=tmdb_key)
        else:
            return await tmdb.get_tv_genre_list(api_key=tmdb_key)
    except Exception:
        return {"genres": []}


@router.get("/languages")
async def get_languages(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    tmdb_key = await get_user_tmdb_key(db, current_user.id)
    if not check_tmdb_key(tmdb_key):
        return []
    try:
        return await tmdb.get_languages(api_key=tmdb_key)
    except Exception:
        return []


@router.get("/countries")
async def get_countries(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    tmdb_key = await get_user_tmdb_key(db, current_user.id)
    if not check_tmdb_key(tmdb_key):
        return []
    try:
        return await tmdb.get_countries(api_key=tmdb_key)
    except Exception:
        return []

@router.get("/watch-providers")
async def get_watch_providers(
    type: str = Query("movie"),
    region: str = Query("US"),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    tmdb_key = await get_user_tmdb_key(db, current_user.id)
    if not check_tmdb_key(tmdb_key):
        return {"results": []}
    try:
        providers = await tmdb.get_watch_providers(type=type, region=region, api_key=tmdb_key)
        # Normalize logo paths
        for p in providers:
            if p.get("logo_path"):
                p["logo_path"] = tmdb.poster_url(p["logo_path"], size="w154")
        return {"results": providers}
    except Exception:
        return {"results": []}

@router.get("/new-episodes")
async def new_episodes(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    tmdb_key = await get_user_tmdb_key(db, current_user.id)
    if not check_tmdb_key(tmdb_key):
        return {"results": []}
    try:
        data = await tmdb.get_on_air_this_week(api_key=tmdb_key)
        results = data.get("results", [])
        ids = [r["id"] for r in results if r.get("id")]
        lib = await _show_library_ids(db, current_user.id, ids)
        items = _enrich_show_list(results, lib)
        await enrich_with_state(db, current_user.id, items)
        # Only return shows the user has in their library and not blocked
        library_items = [i for i in items if i.get("in_library") and not i.get("is_blocked")]
        return {"results": library_items}

    except Exception:
        return {"results": []}



async def recommended(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    import random
    from models.profile import UserProfileData
    tmdb_key = await get_user_tmdb_key(db, current_user.id)
    if not check_tmdb_key(tmdb_key):
        return {"results": []}

    profile_q = await db.execute(select(UserProfileData).where(UserProfileData.user_id == current_user.id))
    _rec_profile = profile_q.scalar_one_or_none()
    _rec_disliked: set[str] = set(_rec_profile.disliked_genres or []) if _rec_profile else set()
    _rec_liked: set[str] = set(_rec_profile.liked_genres or []) if _rec_profile else set()

    # Bulk-load all collected IDs for filtering later
    all_movie_ids_q = await db.execute(
        select(Media.tmdb_id)
        .join(Collection, Collection.media_id == Media.id)
        .where(Collection.user_id == current_user.id, Media.media_type == MediaType.movie)
        .distinct()
    )
    all_collected_movie_ids: set[int] = {row[0] for row in all_movie_ids_q.all()}

    all_show_ids_q = await db.execute(
        select(ShowModel.tmdb_id)
        .join(Media, Media.show_id == ShowModel.id)
        .join(Collection, Collection.media_id == Media.id)
        .where(Collection.user_id == current_user.id)
        .distinct()
    )
    all_collected_show_ids: set[int] = {row[0] for row in all_show_ids_q.all()}
    
    blocked_movies = await _get_blocked_ids(db, current_user.id, MediaType.movie)
    blocked_series = await _get_blocked_ids(db, current_user.id, MediaType.series)
    cf = await _get_content_filters(db, current_user.id)


    if not all_collected_movie_ids and not all_collected_show_ids:
        return {"results": []}

    # Sample seed items from most recently added
    recent_movies_q = await db.execute(
        select(Media.tmdb_id)
        .join(Collection, Collection.media_id == Media.id)
        .where(Collection.user_id == current_user.id, Media.media_type == MediaType.movie)
        .order_by(Collection.added_at.desc())
        .limit(10)
    )
    recent_movie_ids = [row[0] for row in recent_movies_q.all()]

    recent_shows_q = await db.execute(
        select(ShowModel.tmdb_id)
        .join(Media, Media.show_id == ShowModel.id)
        .join(Collection, Collection.media_id == Media.id)
        .where(Collection.user_id == current_user.id)
        .order_by(Collection.added_at.desc())
        .distinct()
        .limit(5)
    )
    recent_show_ids = [row[0] for row in recent_shows_q.all()]

    seed_movies = random.sample(recent_movie_ids, min(3, len(recent_movie_ids)))
    seed_shows = random.sample(recent_show_ids, min(2, len(recent_show_ids)))

    semaphore = asyncio.Semaphore(10)

    async def fetch_recs(tmdb_id: int, is_show: bool) -> list[dict]:
        async with semaphore:
            try:
                if is_show:
                    data = await tmdb.get_show_recommendations(tmdb_id, api_key=tmdb_key)
                else:
                    data = await tmdb.get_movie_recommendations(tmdb_id, api_key=tmdb_key)
                return data.get("results", [])
            except Exception:
                return []

    all_results = await asyncio.gather(
        *[fetch_recs(mid, False) for mid in seed_movies],
        *[fetch_recs(sid, True) for sid in seed_shows],
    )

    seen: set[int] = set()
    enriched: list[dict] = []
    n_movies = len(seed_movies)

    for i, batch in enumerate(all_results):
        is_show = i >= n_movies
        name_map = TV_GENRE_NAMES if is_show else MOVIE_GENRE_NAMES
        liked_set = _rec_liked
        filtered_batch = _filter_disliked(batch, _rec_disliked, liked_set, name_map)
        for item in filtered_batch:
            tmdb_id = item.get("id")
            if not tmdb_id or tmdb_id in seen:
                continue
            seen.add(tmdb_id)
            if is_show and tmdb_id in all_collected_show_ids:
                continue
            if not is_show and tmdb_id in all_collected_movie_ids:
                continue
            
            # Blocklist and Content Filter checks
            if is_show and tmdb_id in blocked_series:
                continue
            if not is_show and tmdb_id in blocked_movies:
                continue
            if _is_content_filtered(item, *cf):
                continue

            if is_show:
                enriched.append({
                    "id": None,
                    "tmdb_id": tmdb_id,
                    "type": MediaType.series,
                    "title": item.get("name"),
                    "poster_path": tmdb.poster_url(item.get("poster_path")),
                    "release_date": item.get("first_air_date"),
                    "tmdb_rating": item.get("vote_average"),
                    "in_library": False,
                    "adult": item.get("adult", False),
                })
            else:
                enriched.append({
                    "id": None,
                    "tmdb_id": tmdb_id,
                    "type": MediaType.movie,
                    "title": item.get("title"),
                    "poster_path": tmdb.poster_url(item.get("poster_path")),
                    "release_date": item.get("release_date"),
                    "tmdb_rating": item.get("vote_average"),
                    "in_library": False,
                    "adult": item.get("adult", False),
                })

    random.shuffle(enriched)
    final = enriched[:20]
    await enrich_with_state(db, current_user.id, final)
    return {"results": final}


@router.post("/collect")
async def manually_collect(
    body: CollectRequest,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Manually add a movie/episode to the user's collection."""
    tmdb_key = await get_user_tmdb_key(db, current_user.id)

    # Parse primary URI — required
    try:
        primary_uri = MediaURI.parse(body.uri_id)
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Invalid uri_id: {body.uri_id!r}")
    primary_tmdb_id = int(primary_uri.id) if primary_uri.provider == "tmdb" else None

    media_q = await db.execute(
        select(Media).where(Media.uri_id == body.uri_id, Media.media_type == body.media_type)
    )
    media = media_q.scalars().first()

    # Resolve show via show_uri_id
    async def _resolve_show_link() -> "ShowModel | None":
        from models.show import Show as ShowModel
        if not body.show_uri_id:
            return None
        try:
            _u = MediaURI.parse(body.show_uri_id)
        except ValueError:
            return None
        col = ShowModel.tvdb_id if _u.provider == "tvdb" else ShowModel.tmdb_id
        q = await db.execute(select(ShowModel).where(col == int(_u.id)))
        return q.scalar_one_or_none()

    if media and body.media_type == MediaType.episode and not media.show_id:
        show_link = await _resolve_show_link()
        if show_link:
            media.show_id = show_link.id

    if not media:
        # is_tvdb from show_uri_id provider
        is_tvdb = bool(body.show_uri_id and body.show_uri_id.startswith("tvdb:"))
        if is_tvdb:
            from routers.shows import get_user_tvdb_key
            tvdb_api_key = await get_user_tvdb_key(db, current_user.id)
            if not tvdb_api_key:
                raise HTTPException(status_code=400, detail="TVDB API key required to collect this TVDB episode")
        else:
            if not check_tmdb_key(tmdb_key):
                raise HTTPException(status_code=404, detail="Media not found and no TMDB key configured")

        try:
            from core.enrichment import enrich_media
            if body.media_type == MediaType.movie:
                if primary_tmdb_id is None:
                    raise HTTPException(status_code=400, detail="Movie collect requires TMDB URI")
                data = await tmdb.get_movie(primary_tmdb_id, api_key=tmdb_key)
                title = data.get("title", "")
                media = Media(
                    tmdb_id=primary_tmdb_id,
                    uri_id=body.uri_id,
                    media_type=body.media_type,
                    title=title,
                )
                db.add(media)
                await db.flush()
                await enrich_media(media, api_key=tmdb_key)
            elif body.media_type == MediaType.episode:
                if not body.show_uri_id or body.season_number is None or body.episode_number is None:
                    raise HTTPException(
                        status_code=400,
                        detail="show_uri_id, season_number, and episode_number are required to collect a new episode",
                    )

                if is_tvdb:
                    from routers.shows import get_user_tvdb_key
                    from routers.media import get_user_content_language
                    from core import tvdb as tvdb_client
                    tvdb_api_key = await get_user_tvdb_key(db, current_user.id)
                    lang_code = await get_user_content_language(db, current_user.id)
                    tvdb_lang = tvdb_client.to_three_letter_lang(lang_code)

                    _uri = MediaURI.parse(body.show_uri_id)
                    tvdb_id = int(_uri.id)
                    show_q = await db.execute(
                        select(ShowModel).where(ShowModel.tvdb_id == tvdb_id)
                    )
                    show = show_q.scalar_one_or_none()
                    if not show:
                        show_data = await tvdb_client.get_series(tvdb_id, tvdb_api_key, lang=tvdb_lang)
                        formatted_show = tvdb_client.format_series(show_data, lang=tvdb_lang)
                        _tcross = formatted_show.get("tmdb_id_cross")
                        show = ShowModel(
                            tvdb_id=tvdb_id,
                            tmdb_id=_tcross,
                            uri_id=f"tmdb:s:{_tcross}" if _tcross else f"tvdb:s:{tvdb_id}",
                            title=formatted_show.get("title", ""),
                            poster_path=formatted_show.get("poster_path"),
                            backdrop_path=formatted_show.get("backdrop_path"),
                            tmdb_rating=None,
                            status=formatted_show.get("status"),
                            first_air_date=formatted_show.get("first_air_date"),
                            last_air_date=formatted_show.get("last_air_date"),
                            tmdb_data={
                                "genres": formatted_show.get("genres", []),
                                "seasons": [
                                    {
                                        "season_number": s["season_number"],
                                        "poster_path": s.get("poster_path"),
                                        "episode_count": s.get("episode_count", 0),
                                        "name": s.get("name"),
                                    }
                                    for s in formatted_show.get("seasons", [])
                                ],
                                "source": "tvdb",
                            },
                        )
                        db.add(show)
                        await db.flush()

                    raw_eps = await tvdb_client.get_series_episodes(tvdb_id, body.season_number, tvdb_api_key, lang=tvdb_lang)
                    ep = next((e for e in raw_eps if e.get("number") == body.episode_number), None)
                    ep_title = ep.get("name") if ep else f"Episode {body.episode_number}"
                    media = Media(
                        tmdb_id=primary_tmdb_id,
                        uri_id=body.uri_id,
                        media_type=MediaType.episode,
                        title=ep_title,
                        season_number=body.season_number,
                        episode_number=body.episode_number,
                        show_id=show.id,
                    )
                    db.add(media)
                    await db.flush()
                    await enrich_media(
                        media,
                        api_key=tmdb_key,
                        is_tvdb=True,
                        tvdb_api_key=tvdb_api_key,
                        tvdb_lang=tvdb_lang,
                        series_tvdb_id=tvdb_id,
                    )
                else:
                    # Link to parent show
                    # Derive TMDB show id from show_uri_id
                    _suri = MediaURI.parse(body.show_uri_id)
                    if _suri.provider != "tmdb":
                        raise HTTPException(status_code=400, detail="Episode collect via show_uri_id requires tmdb provider")
                    _show_tmdb_id = int(_suri.id)
                    from models.show import Show as ShowModel
                    show_q = await db.execute(
                        select(ShowModel).where(ShowModel.tmdb_id == _show_tmdb_id)
                    )
                    show = show_q.scalar_one_or_none()
                    if not show:
                        show_data = await tmdb.get_show(_show_tmdb_id, api_key=tmdb_key)
                        show = ShowModel(
                            tmdb_id=_show_tmdb_id,
                            uri_id=f"tmdb:s:{_show_tmdb_id}",
                            title=show_data.get("name", ""),
                            poster_path=tmdb.poster_url(show_data.get("poster_path")),
                            backdrop_path=tmdb.poster_url(show_data.get("backdrop_path"), size="w1280"),
                            tmdb_rating=show_data.get("vote_average"),
                            status=show_data.get("status"),
                            first_air_date=show_data.get("first_air_date"),
                            last_air_date=show_data.get("last_air_date"),
                            tmdb_data={
                                "genres": [g["name"] for g in show_data.get("genres", [])],
                                "seasons": [
                                    {
                                        "season_number": s["season_number"],
                                        "poster_path": tmdb.poster_url(s.get("poster_path")),
                                        "episode_count": s["episode_count"],
                                        "name": s["name"],
                                    }
                                    for s in show_data.get("seasons", [])
                                ]
                            }
                        )
                        db.add(show)
                        await db.flush()

                    ep_data = await tmdb.get_episode(
                        _show_tmdb_id, body.season_number, body.episode_number, api_key=tmdb_key
                    )
                    media = Media(
                        tmdb_id=primary_tmdb_id,
                        uri_id=body.uri_id,
                        media_type=MediaType.episode,
                        title=ep_data.get("name", ""),
                        season_number=body.season_number,
                        episode_number=body.episode_number,
                        show_id=show.id,
                    )
                    db.add(media)
                    await db.flush()
                    await enrich_media(media, api_key=tmdb_key, series_tmdb_id=_show_tmdb_id)
            else:
                raise HTTPException(status_code=400, detail=f"Manual collection not supported for type: {body.media_type}")
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(status_code=404, detail=f"Lookup failed: {e}")

    # Check for existing collection entry
    existing_q = await db.execute(
        select(Collection).where(
            Collection.user_id == current_user.id,
            Collection.media_id == media.id,
        )
    )
    if existing_q.scalars().first():
        return {"status": "ok", "message": "Already in collection"}

    from sqlalchemy.dialects.postgresql import insert as pg_insert
    coll_stmt = pg_insert(Collection).values(user_id=current_user.id, media_id=media.id)
    coll_stmt = coll_stmt.on_conflict_do_nothing(constraint="uq_collection_user_media")
    result = await db.execute(coll_stmt)
    await db.flush()
    coll_q = await db.execute(
        select(Collection).where(Collection.user_id == current_user.id, Collection.media_id == media.id)
    )
    coll = coll_q.scalar_one()
    db.add(CollectionFile(
        collection_id=coll.id,
        source=CollectionSource.manual,
        source_id=body.uri_id,
    ))
    await db.commit()
    return {"status": "ok", "message": "Added to collection"}


@router.delete("/collect")
async def manually_uncollect(
    media_type: MediaType = Query(...),
    uri_id: str | None = Query(None),
    media_id: int | None = Query(None, alias="id"),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    if not uri_id and not media_id:
        raise HTTPException(status_code=400, detail="Provide uri_id or id")

    if uri_id:
        media_q = await db.execute(
            select(Media).where(Media.uri_id == uri_id, Media.media_type == media_type)
        )
    else:
        media_q = await db.execute(
            select(Media).where(Media.id == media_id, Media.media_type == media_type)
        )
    
    media = media_q.scalars().first()
    if not media:
        return {"status": "ok"}

    await db.execute(
        sa_delete(Collection).where(
            Collection.user_id == current_user.id,
            Collection.media_id == media.id,
        )
    )
    await db.commit()
    return {"status": "ok", "message": "Removed from collection"}


async def _resolve_season_episodes(
    db: AsyncSession, show: "ShowModel", series_tmdb_id: int, season_number: int, tmdb_key: str | None
) -> list:
    """Return all Media rows for a season, creating or adopting rows as needed.

    Always uses TMDB as the authoritative episode list so that:
    - shows with no Media rows yet get them created
    - orphaned episodes (show_id=NULL) get adopted
    - already-linked episodes are returned as-is
    """
    if not check_tmdb_key(tmdb_key):
        q = await db.execute(
            select(Media).where(
                Media.show_id == show.id,
                Media.media_type == MediaType.episode,
                Media.season_number == season_number,
            )
        )
        return q.scalars().all()

    try:
        season_data = await tmdb.get_season(series_tmdb_id, season_number, api_key=tmdb_key)
    except Exception:
        q = await db.execute(
            select(Media).where(
                Media.show_id == show.id,
                Media.media_type == MediaType.episode,
                Media.season_number == season_number,
            )
        )
        return q.scalars().all()

    tmdb_episodes = season_data.get("episodes", [])
    if not tmdb_episodes:
        return []

    tmdb_ids = [ep["id"] for ep in tmdb_episodes if ep.get("id")]
    existing_q = await db.execute(
        select(Media).where(
            Media.tmdb_id.in_(tmdb_ids),
            Media.media_type == MediaType.episode,
        )
    )
    existing_by_tmdb: dict[int, Media] = {m.tmdb_id: m for m in existing_q.scalars().all()}

    result: list[Media] = []
    for ep in tmdb_episodes:
        tid = ep.get("id")
        if not tid:
            continue
        media = existing_by_tmdb.get(tid)
        if media:
            if not media.show_id:
                media.show_id = show.id
        else:
            media = Media(
                tmdb_id=tid,
                uri_id=f"tmdb:e:{tid}" if tid else None,
                media_type=MediaType.episode,
                title=ep.get("name", ""),
                season_number=season_number,
                episode_number=ep.get("episode_number"),
                show_id=show.id,
                overview=ep.get("overview"),
                release_date=ep.get("air_date"),
                tmdb_rating=ep.get("vote_average"),
                poster_path=tmdb.poster_url(ep.get("still_path"), size="w500"),
            )
            db.add(media)
        result.append(media)

    await db.flush()
    return result


async def _resolve_season_episodes_tvdb(
    db: AsyncSession, show: "ShowModel", series_tvdb_id: int, season_number: int, tvdb_api_key: str | None, lang: str = "eng"
) -> list:
    """Return all Media rows for a TVDB season, creating or adopting rows as needed."""
    from core import tvdb as tvdb_client
    if not tvdb_api_key:
        q = await db.execute(
            select(Media).where(
                Media.show_id == show.id,
                Media.media_type == MediaType.episode,
                Media.season_number == season_number,
            )
        )
        return q.scalars().all()

    try:
        tvdb_episodes = await tvdb_client.get_series_episodes(series_tvdb_id, season_number, tvdb_api_key, lang=lang)
    except Exception:
        q = await db.execute(
            select(Media).where(
                Media.show_id == show.id,
                Media.media_type == MediaType.episode,
                Media.season_number == season_number,
            )
        )
        return q.scalars().all()

    if not tvdb_episodes:
        return []

    tvdb_ids = [ep["id"] for ep in tvdb_episodes if ep.get("id")]
    existing_q = await db.execute(
        select(Media).where(
            Media.tmdb_id.in_(tvdb_ids),
            Media.media_type == MediaType.episode,
        )
    )
    existing_by_tvdb: dict[int, Media] = {m.tmdb_id: m for m in existing_q.scalars().all()}

    result: list[Media] = []
    for ep in tvdb_episodes:
        tid = ep.get("id")
        if not tid:
            continue
        media = existing_by_tvdb.get(tid)
        if media:
            if not media.show_id:
                media.show_id = show.id
        else:
            media = Media(
                tmdb_id=tid,
                uri_id=f"tvdb:e:{tid}" if tid else None,
                media_type=MediaType.episode,
                title=ep.get("name") or f"Episode {ep.get('number')}",
                season_number=season_number,
                episode_number=ep.get("number"),
                show_id=show.id,
                overview=ep.get("overview"),
                release_date=ep.get("aired"),
                tmdb_data={
                    "runtime": ep.get("runtime"),
                    "tvdb_episode_id": tid,
                    "source": "tvdb",
                },
                poster_path=tvdb_client._image_url(ep.get("image")),
            )
            db.add(media)
        result.append(media)

    await db.flush()
    return result


async def _collect_episodes(db: AsyncSession, user_id: int, episodes: list) -> int:
    """Insert Collection + CollectionFile(manual) for each episode, skipping existing ones."""
    from sqlalchemy.dialects.postgresql import insert as pg_insert
    added = 0
    for ep in episodes:
        coll_stmt = pg_insert(Collection).values(user_id=user_id, media_id=ep.id)
        coll_stmt = coll_stmt.on_conflict_do_nothing(constraint="uq_collection_user_media")
        await db.execute(coll_stmt)
        await db.flush()
        coll_q = await db.execute(
            select(Collection).where(Collection.user_id == user_id, Collection.media_id == ep.id)
        )
        coll = coll_q.scalar_one_or_none()
        if not coll:
            continue
        existing_file_q = await db.execute(
            select(CollectionFile).where(
                CollectionFile.collection_id == coll.id,
                CollectionFile.source == CollectionSource.manual,
            )
        )
        if not existing_file_q.scalars().first():
            db.add(CollectionFile(
                collection_id=coll.id,
                source=CollectionSource.manual,
                source_id=str(ep.tmdb_id or ep.id),
            ))
            added += 1
    return added


@router.post("/collect-season")
async def collect_season(
    body: CollectSeasonRequest,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Manually add all episodes in a season to the user's collection."""
    from sqlalchemy.dialects.postgresql import insert as pg_insert

    tmdb_key = await get_user_tmdb_key(db, current_user.id)

    try:
        _suri = MediaURI.parse(body.show_uri_id)
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Invalid show_uri_id: {body.show_uri_id!r}")

    series_tmdb_id: int | None = None
    if _suri.provider == "tmdb":
        series_tmdb_id = int(_suri.id)
        show_q = await db.execute(select(ShowModel).where(ShowModel.tmdb_id == series_tmdb_id))
        show = show_q.scalar_one_or_none()
    else:
        tvdb_id_cs = int(_suri.id)
        show_q = await db.execute(select(ShowModel).where(ShowModel.tvdb_id == tvdb_id_cs))
        show = show_q.scalar_one_or_none()
        if not show:
            raise HTTPException(status_code=404, detail="TVDB show not found in local DB; sync first")
        if show.tmdb_id:
            series_tmdb_id = show.tmdb_id
        elif show.uri_id:
            from utils.alias_lookup import get_provider_id_for_uri
            alias = await get_provider_id_for_uri(db, show.uri_id, "tmdb")
            if alias:
                series_tmdb_id = int(alias)
        if not series_tmdb_id:
            raise HTTPException(status_code=400, detail="No TMDB cross-reference for TVDB show; alias lookup failed")

    if not show:
        if not check_tmdb_key(tmdb_key):
            raise HTTPException(status_code=404, detail="Show not found and no TMDB key configured")
        show_data = await tmdb.get_show(series_tmdb_id, api_key=tmdb_key)
        show = ShowModel(
            tmdb_id=series_tmdb_id,
            uri_id=f"tmdb:s:{series_tmdb_id}",
            title=show_data.get("name", ""),
            poster_path=tmdb.poster_url(show_data.get("poster_path")),
            backdrop_path=tmdb.poster_url(show_data.get("backdrop_path"), size="w1280"),
            tmdb_rating=show_data.get("vote_average"),
            status=show_data.get("status"),
            first_air_date=show_data.get("first_air_date"),
            last_air_date=show_data.get("last_air_date"),
            tmdb_data={
                "genres": [g["name"] for g in show_data.get("genres", [])],
                "seasons": [
                    {
                        "season_number": s["season_number"],
                        "poster_path": tmdb.poster_url(s.get("poster_path")),
                        "episode_count": s["episode_count"],
                        "name": s["name"],
                    }
                    for s in show_data.get("seasons", [])
                ],
            },
        )
        db.add(show)
        await db.flush()

    episodes = await _resolve_season_episodes(db, show, series_tmdb_id, body.season_number, tmdb_key)
    if not episodes:
        return {"status": "ok", "count": 0}

    added = await _collect_episodes(db, current_user.id, episodes)
    await db.commit()
    return {"status": "ok", "count": added}


@router.post("/collect-show")
async def collect_show(
    body: CollectRequest,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Manually collect all aired seasons/episodes for a show."""
    tmdb_key = await get_user_tmdb_key(db, current_user.id)
    if not check_tmdb_key(tmdb_key):
        raise HTTPException(status_code=400, detail="TMDB key required to collect a show")

    # URI required; TMDB only
    try:
        _uri = MediaURI.parse(body.uri_id)
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Invalid uri_id: {body.uri_id!r}")
    show_tmdb_id: int | None = None
    if _uri.provider == "tmdb":
        show_tmdb_id = int(_uri.id)
        show_q = await db.execute(select(ShowModel).where(ShowModel.tmdb_id == show_tmdb_id))
        show = show_q.scalar_one_or_none()
    else:
        # TVDB URI: find local show, try alias → TMDB cross-ref
        tvdb_id = int(_uri.id)
        show_q = await db.execute(select(ShowModel).where(ShowModel.tvdb_id == tvdb_id))
        show = show_q.scalar_one_or_none()
        if show and show.tmdb_id:
            show_tmdb_id = show.tmdb_id
        elif show and show.uri_id:
            from utils.alias_lookup import get_provider_id_for_uri
            alias = await get_provider_id_for_uri(db, show.uri_id, "tmdb")
            if alias:
                show_tmdb_id = int(alias)
        if not show:
            raise HTTPException(status_code=404, detail="TVDB show not found in local DB; sync first")
        if not show_tmdb_id:
            raise HTTPException(status_code=400, detail="No TMDB cross-reference for TVDB show; alias lookup failed")

    if not show and show_tmdb_id:
        show_data = await tmdb.get_show(show_tmdb_id, api_key=tmdb_key)
        show = ShowModel(
            tmdb_id=show_tmdb_id,
            uri_id=f"tmdb:s:{show_tmdb_id}",
            title=show_data.get("name", ""),
            poster_path=tmdb.poster_url(show_data.get("poster_path")),
            backdrop_path=tmdb.poster_url(show_data.get("backdrop_path"), size="w1280"),
            tmdb_rating=show_data.get("vote_average"),
            status=show_data.get("status"),
            first_air_date=show_data.get("first_air_date"),
            last_air_date=show_data.get("last_air_date"),
            tmdb_data={
                "genres": [g["name"] for g in show_data.get("genres", [])],
                "seasons": [
                    {
                        "season_number": s["season_number"],
                        "poster_path": tmdb.poster_url(s.get("poster_path")),
                        "episode_count": s["episode_count"],
                        "name": s["name"],
                    }
                    for s in show_data.get("seasons", [])
                ],
            },
        )
        db.add(show)
        await db.flush()

    season_numbers = [
        s["season_number"]
        for s in (show.tmdb_data or {}).get("seasons", [])
        if s.get("season_number", 0) != 0
    ]

    total_added = 0
    for sn in season_numbers:
        episodes = await _resolve_season_episodes(db, show, show_tmdb_id, sn, tmdb_key)
        total_added += await _collect_episodes(db, current_user.id, episodes)

    await db.commit()
    return {"status": "ok", "count": total_added}


@router.delete("/collect-show")
async def uncollect_show(
    uri_id: str = Query(...),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    try:
        uri = MediaURI.parse(uri_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid uri_id")
    if uri.provider == "tvdb":
        show_q = await db.execute(select(ShowModel).where(ShowModel.tvdb_id == int(uri.id)))
    else:
        show_q = await db.execute(select(ShowModel).where(ShowModel.tmdb_id == int(uri.id)))
    show = show_q.scalar_one_or_none()
    if not show:
        return {"status": "ok"}

    episode_ids_q = await db.execute(
        select(Media.id).where(
            Media.show_id == show.id,
            Media.media_type == MediaType.episode,
        )
    )
    episode_ids = [r[0] for r in episode_ids_q.all()]
    if episode_ids:
        await db.execute(
            sa_delete(Collection).where(
                Collection.user_id == current_user.id,
                Collection.media_id.in_(episode_ids),
            )
        )
        await db.commit()
    return {"status": "ok"}


@router.delete("/collect-season")
async def uncollect_season(
    season_number: int = Query(...),
    show_uri_id: str = Query(...),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    from models.show import Show as ShowModel
    try:
        uri = MediaURI.parse(show_uri_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid show_uri_id")
    if uri.provider == "tvdb":
        show_q = await db.execute(select(ShowModel).where(ShowModel.tvdb_id == int(uri.id)))
    else:
        show_q = await db.execute(select(ShowModel).where(ShowModel.tmdb_id == int(uri.id)))
    show = show_q.scalar_one_or_none()
    if not show:
        return {"status": "ok"}

    episodes_q = await db.execute(
        select(Media.id).where(
            Media.show_id == show.id,
            Media.media_type == MediaType.episode,
            Media.season_number == season_number,
        )
    )
    episode_ids = [r[0] for r in episodes_q.all()]
    if not episode_ids:
        return {"status": "ok"}

    await db.execute(
        sa_delete(Collection).where(
            Collection.user_id == current_user.id,
            Collection.media_id.in_(episode_ids),
        )
    )
    await db.commit()
    return {"status": "ok"}


@router.get("/request-status")
async def get_request_status(
    media_type: MediaType = Query(...),
    uri_id: str = Query(...),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    try:
        uri = MediaURI.parse(uri_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid uri_id")
    if uri.provider == "tmdb":
        tmdb_id = int(uri.id)
    else:
        from utils.alias_lookup import get_provider_id_for_uri
        alias_tmdb = await get_provider_id_for_uri(db, uri_id, "tmdb")
        if not alias_tmdb:
            raise HTTPException(status_code=404, detail="No TMDB mapping for URI")
        tmdb_id = int(alias_tmdb)
    settings_q = await db.execute(select(UserSettings).where(UserSettings.user_id == current_user.id))
    settings = settings_q.scalar_one_or_none()
    gs = await _get_global_settings(db)

    monitored = False

    try:
        if media_type == MediaType.movie:
            radarr_cfg = _effective_radarr(settings, gs)
            if not radarr_cfg:
                raise HTTPException(status_code=503, detail="Radarr not configured")
            url = radarr_cfg.radarr_url.rstrip("/")
            async with httpx.AsyncClient(timeout=5.0) as client:
                res = await client.get(
                    f"{url}/api/v3/movie/lookup",
                    params={"apiKey": radarr_cfg.radarr_token, "term": f"tmdb:{tmdb_id}"},
                )
                if res.status_code == 200:
                    for entry in res.json():
                        if entry.get("id"):
                            monitored = True
                            break

        elif media_type == MediaType.series:
            sonarr_cfg = _effective_sonarr(settings, gs)
            if not sonarr_cfg:
                raise HTTPException(status_code=503, detail="Sonarr not configured")
            tvdb_id: int | None = None
            show_q = await db.execute(select(ShowModel).where(ShowModel.tmdb_id == tmdb_id))
            show_row = show_q.scalar_one_or_none()
            if show_row and show_row.tmdb_data:
                tvdb_id = (show_row.tmdb_data.get("external_ids") or {}).get("tvdb_id")
            if not tvdb_id:
                from core import tmdb as tmdb_core
                tmdb_key = await get_user_tmdb_key(db, current_user.id)
                ext_ids = await tmdb_core.get_external_ids(tmdb_id, "tv", api_key=tmdb_key)
                tvdb_id = ext_ids.get("tvdb_id")
            if tvdb_id:
                url = sonarr_cfg.sonarr_url.rstrip("/")
                async with httpx.AsyncClient(timeout=5.0) as client:
                    res = await client.get(
                        f"{url}/api/v3/series/lookup",
                        params={"apiKey": sonarr_cfg.sonarr_token, "term": f"tvdb:{tvdb_id}"},
                    )
                    if res.status_code == 200:
                        for entry in res.json():
                            if entry.get("id"):
                                monitored = True
                                break

    except HTTPException:
        raise
    except Exception:
        raise HTTPException(status_code=503, detail="Service unavailable")

    return {"monitored": monitored}


@router.post("/{type}/{media_id_or_uri}/request")
async def request_media(
    type: MediaType,
    media_id_or_uri: str,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    tmdb_id = _parse_tmdb_id(media_id_or_uri)
    """Request a movie (Radarr) or series (Sonarr)."""
    settings_q = await db.execute(
        select(UserSettings).where(UserSettings.user_id == current_user.id)
    )
    settings = settings_q.scalar_one_or_none()
    gs = await _get_global_settings(db)

    async def _upsert_request(media_type_str: str, title: str, poster_path: str | None) -> dict:
        """Create or update a pending media request, return 202 response."""
        uri_prefix = "m" if media_type_str == "movie" else "s"
        request_uri = f"tmdb:{uri_prefix}:{tmdb_id}"
        existing_q = await db.execute(
            select(MediaRequest).where(
                MediaRequest.user_id == current_user.id,
                MediaRequest.uri_id == request_uri,
            )
        )
        existing = existing_q.scalar_one_or_none()
        if existing:
            if existing.status == RequestStatus.approved:
                raise HTTPException(status_code=409, detail="Already approved and added")
            existing.status = RequestStatus.pending
            existing.updated_at = func.now()
        else:
            db.add(MediaRequest(
                user_id=current_user.id,
                uri_id=request_uri,
                media_type=media_type_str,
                title=title,
                poster_path=poster_path,
                status=RequestStatus.pending,
            ))
        await db.commit()
        return {"status": "pending_approval", "message": "Request submitted for admin approval"}

    if type == MediaType.movie:
        radarr_cfg = _effective_radarr(settings, gs)
        if not radarr_cfg:
            raise HTTPException(status_code=400, detail="Radarr not configured in settings")

        uses_global = gs and radarr_cfg is gs and not current_user.is_admin
        if uses_global and gs.radarr_require_approval:
            tmdb_key = await get_user_tmdb_key(db, current_user.id)
            title, poster = "", None
            try:
                from core import tmdb as tmdb_core
                movie_data = await tmdb_core.get_movie(tmdb_id, api_key=tmdb_key)
                title = movie_data.get("title") or ""
                poster = tmdb_core.poster_url(movie_data.get("poster_path")) if movie_data.get("poster_path") else None
            except Exception: pass
            return await _upsert_request("movie", title, poster)

        from core import radarr
        try:
            res = await radarr.add_movie(
                url=radarr_cfg.radarr_url,
                token=radarr_cfg.radarr_token,
                tmdb_id=tmdb_id,
                title="",
                root_folder=radarr_cfg.radarr_root_folder,
                quality_profile_id=radarr_cfg.radarr_quality_profile,
                tags=radarr_cfg.radarr_tags
            )
            return res
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Radarr error: {e}")

    elif type == MediaType.series:
        sonarr_cfg = _effective_sonarr(settings, gs)
        if not sonarr_cfg:
            raise HTTPException(status_code=400, detail="Sonarr not configured in settings")

        uses_global = gs and sonarr_cfg is gs and not current_user.is_admin
        if uses_global and gs.sonarr_require_approval:
            tmdb_key = await get_user_tmdb_key(db, current_user.id)
            title, poster = "", None
            try:
                from core import tmdb as tmdb_core
                show_data = await tmdb_core.get_show(tmdb_id, api_key=tmdb_key)
                title = show_data.get("name") or ""
                poster = tmdb_core.poster_url(show_data.get("poster_path")) if show_data.get("poster_path") else None
            except Exception: pass
            return await _upsert_request("series", title, poster)

        from core import sonarr, tmdb
        try:
            tmdb_key = await get_user_tmdb_key(db, current_user.id)

            # Try alias registry first — avoids an unnecessary TMDB API call for known shows
            tvdb_id = None
            show_row_q = await db.execute(
                select(ShowModel).where(ShowModel.tmdb_id == tmdb_id)
            )
            local_show = show_row_q.scalar_one_or_none()
            if local_show and local_show.tvdb_id:
                tvdb_id = local_show.tvdb_id
            elif local_show and local_show.uri_id:
                from utils.alias_lookup import get_provider_id_for_uri
                alias = await get_provider_id_for_uri(db, local_show.uri_id, "tvdb")
                if alias:
                    try:
                        tvdb_id = int(alias)
                    except (ValueError, TypeError):
                        pass
            else:
                # Also try alias table by tmdb:s:{tmdb_id} URI
                from utils.alias_lookup import get_provider_id_for_uri
                alias = await get_provider_id_for_uri(db, f"tmdb:s:{tmdb_id}", "tvdb")
                if alias:
                    try:
                        tvdb_id = int(alias)
                    except (ValueError, TypeError):
                        pass

            if not tvdb_id:
                ext_ids = await tmdb.get_external_ids(tmdb_id, "tv", api_key=tmdb_key)
                tvdb_id = ext_ids.get("tvdb_id")

            if not tvdb_id:
                raise HTTPException(status_code=400, detail="Could not find TVDB ID for this show")

            res = await sonarr.add_series(
                url=sonarr_cfg.sonarr_url,
                token=sonarr_cfg.sonarr_token,
                tvdb_id=tvdb_id,
                root_folder=sonarr_cfg.sonarr_root_folder,
                quality_profile_id=sonarr_cfg.sonarr_quality_profile,
                tags=sonarr_cfg.sonarr_tags,
                season_folder=sonarr_cfg.sonarr_season_folder if sonarr_cfg.sonarr_season_folder is not None else True,
            )
            return res
        except Exception as e:
            if isinstance(e, HTTPException): raise e
            raise HTTPException(status_code=500, detail=f"Sonarr error: {e}")

    else:
        raise HTTPException(status_code=400, detail="Can only request movies or series")


async def refresh_technical_data(db: AsyncSession, media_ids: list[int], user_id: int) -> None:
    """For every CollectionFile the user has for the given media IDs, fetch fresh
    technical data (resolution, codecs, languages) from Plex, Jellyfin, or Emby.
    Manual entries are upgraded to the real source by searching all connections."""
    import core.plex as plex_client
    import core.jellyfin as jellyfin_client
    import core.emby as emby_client
    from models.show import Show as ShowModel

    # Load all connections for this user, grouped by type
    conns_result = await db.execute(
        select(MediaServerConnection).where(MediaServerConnection.user_id == user_id)
    )
    all_conns = conns_result.scalars().all()
    conns_by_id: dict[int, MediaServerConnection] = {c.id: c for c in all_conns}
    plex_conns    = [c for c in all_conns if c.type == "plex"]
    jellyfin_conns = [c for c in all_conns if c.type == "jellyfin"]
    emby_conns    = [c for c in all_conns if c.type == "emby"]

    if not all_conns:
        return

    files_result = await db.execute(
        select(CollectionFile, Collection, Media)
        .join(Collection, Collection.id == CollectionFile.collection_id)
        .join(Media, Media.id == Collection.media_id)
        .where(
            Collection.user_id == user_id,
            Collection.media_id.in_(media_ids),
        )
    )
    rows = files_result.all()
    if not rows:
        return

    # Pre-load shows for any episode rows
    show_ids = {media.show_id for _, _, media in rows if media.show_id is not None}
    show_tmdb_map: dict[int, int] = {}
    if show_ids:
        shows_result = await db.execute(select(ShowModel).where(ShowModel.id.in_(show_ids)))
        for s in shows_result.scalars().all():
            show_tmdb_map[s.id] = s.tmdb_id

    for cf, coll, media in rows:
        quality: dict = {}
        new_source: Optional[CollectionSource] = None
        new_source_id: Optional[str] = None
        new_connection_id: Optional[int] = None

        # Resolve the connection for this file (non-manual sources have connection_id set)
        conn = conns_by_id.get(cf.connection_id) if cf.connection_id else None

        if cf.source == CollectionSource.plex and conn and cf.source_id:
            item = await plex_client.get_item(conn.url, conn.token, cf.source_id)
            if item:
                quality = plex_client.extract_quality(item.get("Media", []))

        elif cf.source in (CollectionSource.jellyfin, CollectionSource.emby) and conn and cf.source_id:
            client_mod = jellyfin_client if cf.source == CollectionSource.jellyfin else emby_client
            item = await client_mod.get_item(conn.url, conn.token, cf.source_id, user_id=conn.server_user_id)
            if item:
                quality = client_mod.extract_quality(item.get("MediaStreams", []))
                if not quality.get("file_path") and item.get("Path"):
                    quality["file_path"] = item["Path"]

        elif cf.source == CollectionSource.manual and media.tmdb_id:
            # Try to find the item across all connections by TMDB metadata
            item = None
            if media.media_type == MediaType.movie:
                for c in plex_conns:
                    item = await plex_client.find_movie_by_tmdb_id(c.url, c.token, media.tmdb_id)
                    if item:
                        new_source = CollectionSource.plex
                        new_source_id = str(item.get("ratingKey", ""))
                        new_connection_id = c.id
                        quality = plex_client.extract_quality(item.get("Media", []))
                        break
                if not item:
                    for c in jellyfin_conns:
                        item = await jellyfin_client.find_movie_by_tmdb_id(c.url, c.token, media.tmdb_id)
                        if item:
                            new_source = CollectionSource.jellyfin
                            new_source_id = item.get("Id", "")
                            new_connection_id = c.id
                            quality = jellyfin_client.extract_quality(item.get("MediaStreams", []))
                            if not quality.get("file_path") and item.get("Path"):
                                quality["file_path"] = item["Path"]
                            break
                if not item:
                    for c in emby_conns:
                        item = await emby_client.find_movie_by_tmdb_id(c.url, c.token, media.tmdb_id)
                        if item:
                            new_source = CollectionSource.emby
                            new_source_id = item.get("Id", "")
                            new_connection_id = c.id
                            quality = emby_client.extract_quality(item.get("MediaStreams", []))
                            if not quality.get("file_path") and item.get("Path"):
                                quality["file_path"] = item["Path"]
                            break

            elif media.media_type == MediaType.episode and media.season_number is not None and media.episode_number is not None:
                series_tmdb_id = show_tmdb_map.get(media.show_id) if media.show_id else None
                if series_tmdb_id:
                    for c in plex_conns:
                        item = await plex_client.find_episode_by_ids(
                            c.url, c.token, series_tmdb_id, media.season_number, media.episode_number,
                        )
                        if item:
                            new_source = CollectionSource.plex
                            new_source_id = str(item.get("ratingKey", ""))
                            new_connection_id = c.id
                            quality = plex_client.extract_quality(item.get("Media", []))
                            break
                    if not item:
                        for c in jellyfin_conns:
                            item = await jellyfin_client.find_episode_by_ids(
                                c.url, c.token, series_tmdb_id, media.season_number, media.episode_number,
                            )
                            if item:
                                new_source = CollectionSource.jellyfin
                                new_source_id = item.get("Id", "")
                                new_connection_id = c.id
                                quality = jellyfin_client.extract_quality(item.get("MediaStreams", []))
                                if not quality.get("file_path") and item.get("Path"):
                                    quality["file_path"] = item["Path"]
                                break
                    if not item:
                        for c in emby_conns:
                            item = await emby_client.find_episode_by_ids(
                                c.url, c.token, series_tmdb_id, media.season_number, media.episode_number,
                            )
                            if item:
                                new_source = CollectionSource.emby
                                new_source_id = item.get("Id", "")
                                new_connection_id = c.id
                                quality = emby_client.extract_quality(item.get("MediaStreams", []))
                                if not quality.get("file_path") and item.get("Path"):
                                    quality["file_path"] = item["Path"]
                                break

        if not quality.get("resolution"):
            continue

        # Upgrade manual entry to the real source so future syncs work
        if new_source and new_source_id:
            cf.source = new_source
            cf.source_id = new_source_id
        if new_connection_id:
            cf.connection_id = new_connection_id

        if quality.get("resolution"):    cf.resolution         = quality["resolution"]
        if quality.get("video_codec"):   cf.video_codec        = quality["video_codec"]
        if quality.get("audio_codec"):   cf.audio_codec        = quality["audio_codec"]
        if quality.get("audio_channels"): cf.audio_channels    = quality["audio_channels"]
        if quality.get("audio_languages") is not None: cf.audio_languages    = quality["audio_languages"]
        if quality.get("subtitle_languages") is not None: cf.subtitle_languages = quality["subtitle_languages"]
        if quality.get("file_path"):     cf.file_path          = quality["file_path"]


@router.post("/movie/{media_id_or_uri}/refresh")
async def refresh_movie_metadata(
    media_id_or_uri: str,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Re-fetch TMDB metadata for a movie the user has in their library."""
    from core.enrichment import enrich_media

    tmdb_id = _parse_tmdb_id(media_id_or_uri)

    result = await db.execute(
        select(Media).where(Media.tmdb_id == tmdb_id, Media.media_type == MediaType.movie)
    )
    media = result.scalar_one_or_none()
    if not media:
        raise HTTPException(status_code=404, detail="Movie not found")

    coll_result = await db.execute(
        select(Collection).where(Collection.user_id == current_user.id, Collection.media_id == media.id)
    )
    if not coll_result.scalar_one_or_none():
        raise HTTPException(status_code=403, detail="Movie not in your library")

    tmdb_key = await get_user_tmdb_key(db, current_user.id)
    await enrich_media(media, api_key=tmdb_key)

    await refresh_technical_data(db, [media.id], current_user.id)

    await db.commit()
    return {"message": "Metadata refreshed successfully"}


async def get_where_to_watch(
    db: AsyncSession,
    user_id: int,
    tmdb_id: int,
    media_type: MediaType,
    media: "Media | None" = None,
    show: "ShowModel | None" = None,
    tmdb_key: str | None = None,
) -> list[dict]:
    """Return a deduplicated list of local servers and streaming services where this title is available."""
    sources: list[dict] = []
    seen_names: set[str] = set()

    def _add(entry: dict) -> None:
        key = (entry["type"], entry["name"])
        if key not in seen_names:
            seen_names.add(key)
            sources.append(entry)

    # ── Local media servers ───────────────────────────────────────────────────
    if media_type == MediaType.movie and media:
        files_q = await db.execute(
            select(CollectionFile, MediaServerConnection)
            .join(Collection, Collection.id == CollectionFile.collection_id)
            .outerjoin(MediaServerConnection, MediaServerConnection.id == CollectionFile.connection_id)
            .where(Collection.media_id == media.id, Collection.user_id == user_id)
        )
        for cf, conn in files_q.all():
            name = conn.name if conn else cf.source.value.title()
            _add({"type": cf.source.value, "name": name, "logo": None, "category": "local", "is_subscribed": True, "connection_id": conn.id if conn else None})

    elif media_type == MediaType.series and show:
        files_q = await db.execute(
            select(CollectionFile.connection_id, CollectionFile.source, MediaServerConnection.name)
            .distinct()
            .join(Collection, Collection.id == CollectionFile.collection_id)
            .join(Media, Media.id == Collection.media_id)
            .outerjoin(MediaServerConnection, MediaServerConnection.id == CollectionFile.connection_id)
            .where(
                Media.show_id == show.id,
                Collection.user_id == user_id,
                Media.media_type == MediaType.episode,
            )
        )
        for cid, src, conn_name in files_q.all():
            name = conn_name if conn_name else src.value.title()
            _add({"type": src.value, "name": name, "logo": None, "category": "local", "is_subscribed": True, "connection_id": cid})

    # ── TMDB streaming providers ──────────────────────────────────────────────
    if tmdb_key and check_tmdb_key(tmdb_key):
        try:
            profile_q = await db.execute(
                select(UserProfileData).where(UserProfileData.user_id == user_id)
            )
            profile = profile_q.scalar_one_or_none()
            country = (profile.country if profile and profile.country else None) or "US"
            user_streaming_ids = (
                {int(s) for s in (profile.streaming_services or [])} if profile else set()
            )

            if media_type == MediaType.movie:
                providers_data = await tmdb.get_movie_watch_providers(tmdb_id, api_key=tmdb_key)
            else:
                providers_data = await tmdb.get_show_watch_providers(tmdb_id, api_key=tmdb_key)

            country_data = (providers_data.get("results") or {}).get(country, {})
            
            # Combine all available categories
            for category in ["flatrate", "buy", "rent"]:
                for p in country_data.get(category, []):
                    pid = p.get("provider_id")
                    logo = tmdb.poster_url(p.get("logo_path"), size="w92") if p.get("logo_path") else None
                    
                    # If user has NOT selected any services, treat all flatrate as 'subscribed'
                    is_subscribed = False
                    if category == "flatrate":
                        if not user_streaming_ids or pid in user_streaming_ids:
                            is_subscribed = True
                    elif user_streaming_ids and pid in user_streaming_ids:
                        # Even for buy/rent, if it's one of their main services, mark it
                        is_subscribed = True

                    _add({
                        "type": "streaming", 
                        "name": p.get("provider_name"), 
                        "logo": logo,
                        "category": category,
                        "is_subscribed": is_subscribed,
                        "url": country_data.get("link")
                    })
        except Exception:
            pass

    return sources


def _srt_to_vtt(srt: str) -> str:
    vtt = re.sub(r"(\d{2}:\d{2}:\d{2}),(\d{3})", r"\1.\2", srt)
    return "WEBVTT\n\n" + vtt.strip()


async def _streaming_resolve_show_id(media_ref: str, db: AsyncSession) -> Optional[int]:
    """Return Show.id for a media_ref that is either a URI string or a legacy tmdb_id integer string."""
    from models.show import Show as _Show
    if ':' in media_ref:
        from utils.media_uri import MediaURI
        from utils.alias_lookup import get_internal_id_for_uri
        try:
            MediaURI.parse(media_ref)
        except ValueError:
            return None
        show_row = await db.execute(select(_Show.id).where(_Show.uri_id == media_ref))
        show_id = show_row.scalar_one_or_none()
        if show_id is not None:
            return show_id
        return await get_internal_id_for_uri(db, media_ref)
    try:
        tmdb_id = int(media_ref)
    except ValueError:
        return None
    show_row = await db.execute(select(_Show.id).where(_Show.tmdb_id == tmdb_id))
    return show_row.scalar_one_or_none()


async def _streaming_resolve_media(media_ref: str, media_type: MediaType, db: AsyncSession) -> Optional[Media]:
    """Return Media row for a media_ref that is either a URI string or a legacy tmdb_id integer string."""
    if ':' in media_ref:
        from utils.alias_lookup import get_internal_id_for_uri
        media_row = await db.execute(
            select(Media).where(Media.uri_id == media_ref, Media.media_type == media_type)
        )
        media = media_row.scalars().first()
        if media is not None:
            return media
        internal_id = await get_internal_id_for_uri(db, media_ref)
        if internal_id is None:
            return None
        media_row = await db.execute(select(Media).where(Media.id == internal_id))
        return media_row.scalars().first()
    try:
        tmdb_id = int(media_ref)
    except ValueError:
        return None
    media_row = await db.execute(
        select(Media).where(Media.tmdb_id == tmdb_id, Media.media_type == media_type)
    )
    return media_row.scalars().first()


async def _get_streaming_source(
    db: AsyncSession,
    user_id: int,
    media_ref: str,
    media_type: MediaType,
    connection_id: int,
) -> tuple[CollectionFile, MediaServerConnection]:
    media = await _streaming_resolve_media(media_ref, media_type, db)
    if not media:
        raise HTTPException(404, "Not in library")

    cf_q = await db.execute(
        select(CollectionFile, MediaServerConnection)
        .join(Collection, Collection.id == CollectionFile.collection_id)
        .outerjoin(MediaServerConnection, MediaServerConnection.id == CollectionFile.connection_id)
        .where(
            Collection.media_id == media.id,
            Collection.user_id == user_id,
            CollectionFile.connection_id == connection_id,
        )
    )
    row = cf_q.first()
    if not row:
        raise HTTPException(404, "Source not found")

    cf, conn = row
    if not conn or not cf.source_id:
        raise HTTPException(400, "No valid connection configured")
    return cf, conn


def _parse_tmdb_id(media_id_or_uri: str) -> int:
    if ":" in media_id_or_uri:
        try:
            return int(MediaURI.parse(media_id_or_uri).id)
        except (ValueError, AttributeError):
            raise HTTPException(status_code=400, detail=f"Invalid media ID: {media_id_or_uri!r}")
    else:
        try:
            return int(media_id_or_uri)
        except ValueError:
            raise HTTPException(status_code=400, detail=f"Invalid media ID: {media_id_or_uri!r}")


@router.get("/playback/{type}/{media_ref:path}")
async def get_playback_sources(
    type: str,
    media_ref: str,
    season_number: Optional[int] = Query(None),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Return available local-server playback sources for a movie, episode, show, or season.
    media_ref accepts either a URI string (tmdb:s:123, tvdb:s:456) or a legacy tmdb_id integer."""
    if type not in ("movie", "episode", "series", "season"):
        raise HTTPException(400, "Only movie/episode/series/season streaming supported")

    from models.show import Show

    # Base query for files
    q = select(CollectionFile, MediaServerConnection)\
        .join(Collection, Collection.id == CollectionFile.collection_id)\
        .join(Media, Media.id == Collection.media_id)\
        .outerjoin(MediaServerConnection, MediaServerConnection.id == CollectionFile.connection_id)\
        .where(
            Collection.user_id == current_user.id,
            CollectionFile.source.in_([CollectionSource.jellyfin, CollectionSource.emby, CollectionSource.plex]),
            CollectionFile.connection_id.isnot(None),
            CollectionFile.source_id.isnot(None),
        )

    if type in ("movie", "episode"):
        media = await _streaming_resolve_media(media_ref, MediaType(type), db)
        if not media:
            return []
        q = q.where(Media.id == media.id)
    elif type == "season":
        show_id = await _streaming_resolve_show_id(media_ref, db)
        if show_id is None:
            return []
        q = q.join(Show, Show.id == Media.show_id)\
            .where(Show.id == show_id, Media.media_type == "episode")
        if season_number is not None:
            q = q.where(Media.season_number == season_number)
        q = q.order_by(Media.episode_number.asc()).limit(1)
    elif type == "series":
        show_id = await _streaming_resolve_show_id(media_ref, db)
        if show_id is None:
            return []
        q = q.join(Show, Show.id == Media.show_id).where(Show.id == show_id)
        if season_number is not None:
            q = q.where(Media.season_number == season_number)
        q = q.order_by(Media.season_number.asc(), Media.episode_number.asc()).limit(1)

    result = await db.execute(q)
    files = result.all()

    if not files:
        return []

    from core import jellyfin as jellyfin_core
    from core import plex as plex_core

    sources = []
    for cf, conn in files:
        if not conn:
            continue
        resolution = cf.resolution
        subtitles: list[dict] = []
        audio_tracks: list[dict] = []
        external_url: Optional[str] = None

        if cf.source.value in ("jellyfin", "emby") and cf.source_id:
            try:
                item = await jellyfin_core.get_item(conn.url, conn.token, cf.source_id, user_id=conn.server_user_id)
                if item:
                    if conn.external_server_url:
                        base = conn.external_server_url.rstrip("/")
                        target_key = cf.source_id
                        if type == "series" and "SeriesId" in item:
                            target_key = item["SeriesId"]
                        elif type == "season" and "SeasonId" in item:
                            target_key = item["SeasonId"]
                        external_url = f"{base}/web/index.html#!/details?id={target_key}"

                    if resolution is None:
                        q = jellyfin_core.extract_quality(item.get("MediaStreams", []))
                        resolution = q.get("resolution")
                    for stream in item.get("MediaStreams", []):
                        if stream.get("Type") == "Subtitle":
                            codec = (stream.get("Codec") or "").lower()
                            # Skip image-based subtitle formats — they cannot be served as VTT
                            if codec in {"hdmv_pgs_subtitle", "pgssub", "dvd_subtitle", "dvbsub", "dvb_subtitle"}:
                                continue
                            lang = stream.get("Language") or None
                            label = stream.get("DisplayTitle") or stream.get("Title") or lang or "Subtitle"
                            subtitles.append({
                                "index": stream.get("Index"),
                                "language": lang,
                                "label": label,
                                "codec": codec,
                            })
                        elif stream.get("Type") == "Audio":
                            lang = stream.get("Language") or None
                            label = stream.get("DisplayTitle") or stream.get("Title") or lang or "Audio"
                            audio_tracks.append({
                                "index": stream.get("Index"),
                                "language": lang,
                                "label": label,
                                "codec": stream.get("Codec"),
                            })
            except Exception:
                pass

        elif cf.source.value == "plex" and cf.source_id:
            _plex_image_codecs = {"pgssub", "vobsub", "dvd_subtitle", "dvbsub"}
            try:
                item = await plex_core.get_item(conn.url, conn.token, cf.source_id)
                if item:
                    # Construct external URL if provided
                    if conn.external_server_url:
                        base = conn.external_server_url.rstrip("/")
                        target_key = cf.source_id
                        if type == "series" and "grandparentRatingKey" in item:
                            target_key = item["grandparentRatingKey"]
                        elif type == "season" and "parentRatingKey" in item:
                            target_key = item["parentRatingKey"]
                        machine_id = await plex_core.get_machine_identifier(conn.url, conn.token)
                        if machine_id:
                            external_url = f"{base}/web/index.html#!/server/{machine_id}/details?key=%2Flibrary%2Fmetadata%2F{target_key}"
                        else:
                            external_url = f"{base}/web/index.html#!/details?key=%2Flibrary%2Fmetadata%2F{target_key}"

                    media_list = item.get("Media", [])
                    if media_list and media_list[0].get("Part"):
                        for stream in media_list[0]["Part"][0].get("Stream", []):
                            stype = stream.get("streamType")
                            if stype == 3 and stream.get("key"):
                                codec = (stream.get("codec") or "").lower()
                                if codec in _plex_image_codecs:
                                    continue
                                lang = stream.get("languageCode") or stream.get("languageTag") or None
                                label = stream.get("displayTitle") or stream.get("title") or lang or "Subtitle"
                                subtitles.append({
                                    "index": stream.get("id"),
                                    "language": lang,
                                    "label": label,
                                    "codec": codec,
                                })
                            elif stype == 2:
                                lang = stream.get("languageCode") or stream.get("languageTag") or None
                                label = stream.get("displayTitle") or stream.get("title") or lang or "Audio"
                                audio_tracks.append({
                                    "index": stream.get("id"),
                                    "language": lang,
                                    "label": label,
                                    "codec": stream.get("codec"),
                                })
            except Exception:
                pass

        sources.append({
            "connection_id": cf.connection_id,
            "source": cf.source.value,
            "name": conn.name or cf.source.value.title(),
            "resolution": resolution,
            "subtitles": subtitles,
            "audio_tracks": audio_tracks,
            "external_url": external_url,
        })

    return sources


@router.get("/subtitles/{type}/{media_ref:path}")
async def get_subtitle(
    type: MediaType,
    media_ref: str,
    connection_id: int,
    stream_index: int,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Proxy a subtitle track as WebVTT from a Jellyfin, Emby, or Plex server."""
    if type not in (MediaType.movie, MediaType.episode):
        raise HTTPException(400, "Only movie/episode subtitles supported")

    cf, conn = await _get_streaming_source(db, current_user.id, media_ref, type, connection_id)

    cache_headers = {"Cache-Control": "private, max-age=3600"}

    if cf.source.value in ("jellyfin", "emby"):
        sub_url = (
            f"{conn.url.rstrip('/')}/Videos/{cf.source_id}"
            f"/{cf.source_id}/Subtitles/{stream_index}/0/Stream.vtt"
        )
        async with httpx.AsyncClient(timeout=httpx.Timeout(30.0)) as client:
            res = await client.get(sub_url, headers={"X-Emby-Token": conn.token})
        if res.status_code >= 400:
            raise HTTPException(502, "Subtitle not available")
        return Response(content=res.content, media_type="text/vtt", headers=cache_headers)

    elif cf.source.value == "plex":
        from core import plex as plex_core
        item = await plex_core.get_item(conn.url, conn.token, cf.source_id)
        if not item:
            raise HTTPException(502, "Could not fetch item from Plex")
        sub_key = None
        sub_codec = ""
        media_list = item.get("Media", [])
        if media_list and media_list[0].get("Part"):
            for stream in media_list[0]["Part"][0].get("Stream", []):
                if stream.get("streamType") == 3 and stream.get("id") == stream_index:
                    sub_key = stream.get("key")
                    sub_codec = (stream.get("codec") or "").lower()
                    break
        if not sub_key:
            raise HTTPException(404, "Subtitle track not found")
        sub_url = f"{conn.url.rstrip('/')}{sub_key}"
        async with httpx.AsyncClient(timeout=httpx.Timeout(30.0)) as client:
            res = await client.get(sub_url, headers={"X-Plex-Token": conn.token})
        if res.status_code >= 400:
            raise HTTPException(502, "Subtitle not available from Plex")
        content = res.text
        if sub_codec in ("subrip", "srt") or not content.strip().startswith("WEBVTT"):
            content = _srt_to_vtt(content)
        return Response(content=content.encode("utf-8"), media_type="text/vtt", headers=cache_headers)

    else:
        raise HTTPException(400, f"Subtitles not supported for source: {cf.source.value}")


@router.get("/stream/{type}/{media_ref:path}")
async def stream_media(
    type: MediaType,
    media_ref: str,
    connection_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Proxy a direct video stream from a Plex server (Jellyfin/Emby use the HLS endpoint)."""
    if type not in (MediaType.movie, MediaType.episode):
        raise HTTPException(400, "Only movie/episode streaming supported")

    cf, conn = await _get_streaming_source(db, current_user.id, media_ref, type, connection_id)

    upstream_headers: dict[str, str] = {}
    range_header = request.headers.get("Range")
    if range_header:
        upstream_headers["Range"] = range_header

    if cf.source.value in ("jellyfin", "emby"):
        upstream_headers["X-Emby-Token"] = conn.token
        stream_url = f"{conn.url.rstrip('/')}/Videos/{cf.source_id}/stream"
        params: dict = {"Static": "true"}
    elif cf.source.value == "plex":
        from core import plex as plex_core
        item = await plex_core.get_item(conn.url, conn.token, cf.source_id)
        if not item:
            raise HTTPException(502, "Could not fetch item from Plex")
        media_list = item.get("Media", [])
        if not media_list or not media_list[0].get("Part"):
            raise HTTPException(502, "No media part found in Plex")
        part_key = media_list[0]["Part"][0]["key"]
        stream_url = f"{conn.url.rstrip('/')}{part_key}"
        upstream_headers["X-Plex-Token"] = conn.token
        params = {}
    else:
        raise HTTPException(400, f"Streaming not supported for source: {cf.source.value}")

    try:
        client = httpx.AsyncClient(timeout=httpx.Timeout(None))
        upstream_req = client.build_request("GET", stream_url, headers=upstream_headers, params=params)
        upstream_res = await client.send(upstream_req, stream=True)
    except Exception as e:
        raise HTTPException(502, f"Could not connect to media server: {e}")

    res_headers: dict[str, str] = {"Accept-Ranges": "bytes"}
    for h in ("Content-Type", "Content-Length", "Content-Range"):
        v = upstream_res.headers.get(h.lower())
        if v:
            res_headers[h] = v

    async def cleanup() -> None:
        await upstream_res.aclose()
        await client.aclose()

    return StreamingResponse(
        upstream_res.aiter_bytes(65536),
        status_code=upstream_res.status_code,
        headers=res_headers,
        background=BackgroundTask(cleanup),
    )


def _rewrite_m3u8_urls(content: str, connection_id: int, request_path: str, inherit_qs: str = "") -> str:
    """Rewrite URL lines in an M3U8 manifest to route through our HLS segment proxy."""
    base_dir = request_path.rsplit("/", 1)[0] if "/" in request_path else ""
    lines = content.splitlines()
    out = []
    for line in lines:
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            out.append(line)
            continue
        # Resolve absolute vs relative path
        if stripped.startswith("http://") or stripped.startswith("https://"):
            parsed = urllib.parse.urlparse(stripped)
            path_qs = parsed.path + ("?" + parsed.query if parsed.query else "")
        elif stripped.startswith("/"):
            path_qs = stripped
        else:
            # Relative — resolve against the directory of the current manifest and
            # inherit the parent manifest's session query params (DeviceId, PlaySessionId, api_key).
            path_qs = base_dir + "/" + stripped
            if inherit_qs and "?" not in path_qs:
                path_qs += "?" + inherit_qs
        encoded = urllib.parse.quote(path_qs, safe="")
        out.append(f"/api/proxy/media/hls-segment?connection_id={connection_id}&path={encoded}")
    return "\n".join(out)


async def _get_conn_for_user(connection_id: int, user_id: int, db: AsyncSession) -> MediaServerConnection:
    """Fetch a MediaServerConnection and verify it belongs to the given user."""
    result = await db.execute(
        select(MediaServerConnection).where(
            MediaServerConnection.id == connection_id,
            MediaServerConnection.user_id == user_id,
        )
    )
    conn = result.scalars().first()
    if not conn:
        raise HTTPException(404, "Connection not found")
    return conn


@router.get("/hls/{type}/{media_ref:path}")
async def hls_master_manifest(
    type: MediaType,
    media_ref: str,
    connection_id: int,
    audio_stream_index: int | None = None,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Fetch and rewrite an HLS master M3U8 from Emby/Jellyfin so all segment URLs
    are proxied through this server. Creates a proper transcoding session on the server."""
    if type not in (MediaType.movie, MediaType.episode):
        raise HTTPException(400, "Only movie/episode HLS supported")

    cf, conn = await _get_streaming_source(db, current_user.id, media_ref, type, connection_id)
    if cf.source.value not in ("jellyfin", "emby"):
        raise HTTPException(400, "HLS streaming is only supported for Jellyfin/Emby sources")

    manifest_path = f"/Videos/{cf.source_id}/master.m3u8"
    manifest_url = f"{conn.url.rstrip('/')}{manifest_path}"

    # Generate a PlaySessionId so Emby/Jellyfin can track the transcoding session.
    # Emby requires POST /PlaybackInfo with this ID to initialise the session before
    # the first segment is requested; without it Emby throws "Value cannot be null (key)".
    play_session_id = uuid.uuid4().hex
    tag = ""
    media_source_id = cf.source_id
    try:
        info_url = f"{conn.url.rstrip('/')}/Items/{cf.source_id}/PlaybackInfo"
        common_headers = {"X-Emby-Token": conn.token, "Content-Type": "application/json"}
        async with httpx.AsyncClient(timeout=10) as info_client:
            if conn.type == "emby":
                # Emby requires POST to initialise the transcoding session and bind PlaySessionId
                info_body: dict = {
                    "DeviceId": "scrob",
                    "PlaySessionId": play_session_id,
                    "MaxStreamingBitrate": 140_000_000,
                }
                if conn.server_user_id:
                    info_body["UserId"] = conn.server_user_id
                info_res = await info_client.post(info_url, json=info_body, headers=common_headers)
            else:
                # Jellyfin accepts GET; POST also works but GET is simpler
                info_params: dict = {}
                if conn.server_user_id:
                    info_params["UserId"] = conn.server_user_id
                info_res = await info_client.get(info_url, params=info_params, headers=common_headers)
        info_data = info_res.json()
        # Server may echo back or generate a session ID — use whichever is set
        play_session_id = info_data.get("PlaySessionId") or play_session_id
        media_sources = info_data.get("MediaSources", [])
        source_meta = next((s for s in media_sources if s.get("Id") == cf.source_id), None)
        if source_meta is None and media_sources:
            source_meta = media_sources[0]
        if source_meta:
            tag = source_meta.get("ETag", "")
            media_source_id = source_meta.get("Id", cf.source_id)
    except Exception:
        pass  # proceed without Tag; non-Emby servers may not require it

    params: dict[str, str] = {
        "VideoCodec": "copy",
        "MediaSourceId": media_source_id,
        "DeviceId": "scrob",
        "PlaySessionId": play_session_id,
        "api_key": conn.token,
    }
    if audio_stream_index is not None:
        params["AudioStreamIndex"] = str(audio_stream_index)
    if tag:
        params["Tag"] = tag
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            res = await client.get(
                manifest_url,
                params=params,
                headers={"X-Emby-Token": conn.token},
            )
        if res.status_code != 200:
            raise HTTPException(502, f"Media server returned {res.status_code} for HLS manifest — body: {res.text[:300]}")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(502, f"Could not fetch HLS manifest: {e}")

    rewritten = _rewrite_m3u8_urls(res.text, connection_id, manifest_path)
    return Response(
        content=rewritten,
        media_type="application/vnd.apple.mpegurl",
        headers={"Cache-Control": "no-store"},
    )


@router.get("/hls-segment")
async def hls_segment_proxy(
    connection_id: int,
    path: str,
    request: Request,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Proxy HLS sub-manifests and TS segments from Emby/Jellyfin.
    For M3U8 responses, rewrites embedded URLs before returning."""
    conn = await _get_conn_for_user(connection_id, current_user.id, db)

    # path is the decoded absolute path (+ query string) on the media server.
    # Emby/Jellyfin HLS sub-playlists often use relative segment paths with no auth
    # query params. Always inject api_key so the session lookup on the server succeeds.
    segment_url = f"{conn.url.rstrip('/')}{path}"
    if conn.type in ("jellyfin", "emby") and "api_key=" not in path:
        sep = "&" if "?" in path else "?"
        segment_url += f"{sep}api_key={conn.token}"
    try:
        client = httpx.AsyncClient(timeout=httpx.Timeout(None))
        upstream_req = client.build_request(
            "GET", segment_url, headers={"X-Emby-Token": conn.token}
        )
        upstream_res = await client.send(upstream_req, stream=True)
    except Exception as e:
        raise HTTPException(502, f"Could not fetch HLS segment: {e}")

    content_type = upstream_res.headers.get("content-type", "")
    is_manifest = "mpegurl" in content_type or path.split("?")[0].endswith(".m3u8")

    if is_manifest:
        # Read the full body, rewrite URLs, return as text
        body = await upstream_res.aread()
        await upstream_res.aclose()
        await client.aclose()
        # Strip query string for path resolution, but pass it as inherit_qs so that
        # bare relative segment paths (e.g. "0.ts") get DeviceId/PlaySessionId/api_key appended.
        base_path = path.split("?")[0]
        inherit_qs = path.split("?", 1)[1] if "?" in path else ""
        rewritten = _rewrite_m3u8_urls(body.decode("utf-8"), connection_id, base_path, inherit_qs)
        return Response(
            content=rewritten,
            media_type="application/vnd.apple.mpegurl",
            headers={"Cache-Control": "no-store"},
        )

    # Binary segment (TS/fMP4) — stream through
    res_headers: dict[str, str] = {}
    for h in ("Content-Type", "Content-Length"):
        v = upstream_res.headers.get(h.lower())
        if v:
            res_headers[h] = v

    async def cleanup() -> None:
        await upstream_res.aclose()
        await client.aclose()

    return StreamingResponse(
        upstream_res.aiter_bytes(65536),
        status_code=upstream_res.status_code,
        headers=res_headers,
        background=BackgroundTask(cleanup),
    )


@router.post("/session/report/{type}/{media_ref:path}")
async def report_session(
    type: MediaType,
    media_ref: str,
    body: SessionReportRequest,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Report playback state to the media server so the session appears in its Now Playing dashboard."""
    if type not in (MediaType.movie, MediaType.episode):
        return {"ok": False}

    media = await _streaming_resolve_media(media_ref, type, db)
    if not media:
        return {"ok": False}

    session_cf_filters = [
        Collection.media_id == media.id,
        Collection.user_id == current_user.id,
        CollectionFile.connection_id == body.connection_id,
    ]
    if body.file_id is not None:
        session_cf_filters.append(CollectionFile.id == body.file_id)

    cf_q = await db.execute(
        select(CollectionFile, MediaServerConnection)
        .join(Collection, Collection.id == CollectionFile.collection_id)
        .outerjoin(MediaServerConnection, MediaServerConnection.id == CollectionFile.connection_id)
        .where(*session_cf_filters)
    )
    row = cf_q.first()
    if not row:
        return {"ok": False}

    cf, conn = row
    if not conn or not cf.source_id:
        return {"ok": False}

    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(10.0)) as client:
            if cf.source.value in ("jellyfin", "emby"):
                pos_ticks = body.position_ms * 10_000  # Jellyfin uses 100-nanosecond ticks
                # X-Emby-Authorization is required for Jellyfin/Emby to associate the
                # playback report with a client session and show it in "Now Playing".
                device_id = f"scrob-{current_user.id}"
                auth_header = (
                    f'MediaBrowser Token="{conn.token}", Device="Scrob",'
                    f' DeviceId="{device_id}", Version="1.0.0"'
                )
                headers = {
                    "X-Emby-Token": conn.token,
                    "X-Emby-Authorization": auth_header,
                    "Content-Type": "application/json",
                }
                base = conn.url.rstrip("/")
                if body.state == "playing":
                    await client.post(
                        f"{base}/Sessions/Playing",
                        json={
                            "ItemId": cf.source_id,
                            "PositionTicks": pos_ticks,
                            "CanSeek": True,
                            "IsPaused": False,
                            "IsMuted": False,
                            "PlayMethod": "DirectPlay",
                        },
                        headers=headers,
                    )
                elif body.state in ("progress", "paused"):
                    await client.post(
                        f"{base}/Sessions/Playing/Progress",
                        json={
                            "ItemId": cf.source_id,
                            "PositionTicks": pos_ticks,
                            "IsPaused": body.state == "paused",
                            "IsMuted": False,
                        },
                        headers=headers,
                    )
                elif body.state == "stopped":
                    await client.post(
                        f"{base}/Sessions/Playing/Stopped",
                        json={"ItemId": cf.source_id, "PositionTicks": pos_ticks},
                        headers=headers,
                    )
            elif cf.source.value == "plex":
                plex_state = "stopped" if body.state == "stopped" else ("paused" if body.state == "paused" else "playing")
                if body.plex_session_id:
                    try:
                        await client.get(
                            f"{conn.url.rstrip('/')}/video/:/transcode/universal/ping",
                            params={"session": body.plex_session_id},
                            headers={"X-Plex-Token": conn.token},
                        )
                    except Exception:
                        pass
                await client.get(
                    f"{conn.url.rstrip('/')}/:/timeline",
                    params={
                        "ratingKey": cf.source_id,
                        "key": f"/library/metadata/{cf.source_id}",
                        "state": plex_state,
                        "time": str(body.position_ms),
                        "duration": str(body.duration_ms),
                        "identifier": "tv.plex.providers.library",
                        "X-Plex-Token": conn.token,
                    },
                    headers={"Accept": "application/json"},
                )
    except Exception:
        pass  # Best-effort; non-critical

    return {"ok": True}




# ── Blocklist ───────────────────────────────────────────────────────────────

@router.get("/blocklist")
async def get_blocklist(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Get all blocked and dropped items for the current user."""
    q = select(BlocklistItem).where(BlocklistItem.user_id == current_user.id)
    result = await db.execute(q)
    blocked = result.scalars().all()
    return [{"uri_id": b.uri_id, "tmdb_id": b.tmdb_id, "media_type": b.media_type, "is_dropped": b.is_dropped} for b in blocked]


@router.get("/blocklist/enriched")
async def get_blocklist_enriched(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Get all blocked and dropped items for the current user, fully enriched with metadata."""
    q = select(BlocklistItem).where(BlocklistItem.user_id == current_user.id)
    result = await db.execute(q)
    blocked = result.scalars().all()

    if not blocked:
        return []

    tmdb_api_key = await get_user_tmdb_key(db, current_user.id)
    
    tvdb_api_key = None
    user_settings_q = await db.execute(
        select(UserSettings).where(UserSettings.user_id == current_user.id)
    )
    user_settings = user_settings_q.scalar_one_or_none()
    if user_settings and user_settings.tvdb_api_key:
        tvdb_api_key = user_settings.tvdb_api_key
    else:
        gs_q = await db.execute(select(GlobalSettings).where(GlobalSettings.id == 1))
        gs = gs_q.scalar_one_or_none()
        if gs:
            tvdb_api_key = gs.tvdb_api_key

    primary_metadata_source = "tmdb"
    if user_settings and user_settings.preferences:
        primary_metadata_source = user_settings.preferences.get("primary_metadata_source", "tmdb")

    from core import tvdb as tvdb_client
    user_lang = await get_user_content_language(db, current_user.id)
    tvdb_lang = tvdb_client.to_three_letter_lang(user_lang)
    
    from models.show import Show

    sem = asyncio.Semaphore(20)

    async def fetch_by_uri(b: BlocklistItem):
        uri = b.uri_id
        if not uri:
            return None
        parts = uri.split(":", 2)
        if len(parts) != 3:
            return None
        provider, type_prefix, ext_id = parts

        if type_prefix == "m":
            # Movie
            local_q = await db.execute(
                select(Media).where(Media.media_type == MediaType.movie, Media.uri_id == uri)
            )
            local = local_q.scalar_one_or_none()
            if local:
                return {"id": local.id, "uri_id": uri, "tmdb_id": local.tmdb_id, "type": "movie",
                        "title": local.title, "release_date": local.release_date,
                        "poster_path": local.poster_path, "backdrop_path": local.backdrop_path,
                        "overview": local.overview, "tmdb_rating": local.tmdb_rating}
            if provider == "tmdb":
                async with sem:
                    try:
                        data = await tmdb.get_movie_light(int(ext_id), api_key=tmdb_api_key)
                        return {"uri_id": uri, "tmdb_id": int(ext_id), "type": "movie",
                                "title": data.get("title"), "release_date": data.get("release_date"),
                                "poster_path": tmdb.poster_url(data.get("poster_path")),
                                "backdrop_path": tmdb.poster_url(data.get("backdrop_path"), size="w1280"),
                                "overview": data.get("overview"), "tmdb_rating": data.get("vote_average")}
                    except Exception:
                        return None

        elif type_prefix == "s":
            # Series
            local_q = await db.execute(select(Show).where(Show.uri_id == uri))
            local = local_q.scalar_one_or_none()
            if local:
                result = {"id": local.id, "uri_id": uri, "tmdb_id": local.tmdb_id, "tvdb_id": local.tvdb_id,
                        "type": "series", "title": local.title, "release_date": local.first_air_date,
                        "poster_path": local.poster_path, "backdrop_path": local.backdrop_path,
                        "overview": local.overview, "tmdb_rating": local.tmdb_rating}
                
                if primary_metadata_source == "tvdb" and local.tvdb_id and tvdb_api_key:
                    async with sem:
                        try:
                            raw = await tvdb_client.get_series(local.tvdb_id, tvdb_api_key, lang=tvdb_lang)
                            show_fmt = tvdb_client.format_series(raw, lang=tvdb_lang)
                            result["title"] = show_fmt.get("title") or result["title"]
                            result["release_date"] = show_fmt.get("first_air_date") or result["release_date"]
                            result["poster_path"] = show_fmt.get("poster_path") or result["poster_path"]
                            result["backdrop_path"] = show_fmt.get("backdrop_path") or result["backdrop_path"]
                            result["overview"] = show_fmt.get("overview") or result["overview"]
                        except Exception:
                            pass
                return result
            if provider == "tmdb":
                async with sem:
                    try:
                        data = await tmdb.get_show_light(int(ext_id), api_key=tmdb_api_key)
                        result_item = {"uri_id": uri, "tmdb_id": int(ext_id), "type": "series",
                                "title": data.get("name"), "release_date": data.get("first_air_date"),
                                "poster_path": tmdb.poster_url(data.get("poster_path")),
                                "backdrop_path": tmdb.poster_url(data.get("backdrop_path"), size="w1280"),
                                "overview": data.get("overview"), "tmdb_rating": data.get("vote_average")}
                        
                        if primary_metadata_source == "tvdb" and tvdb_api_key:
                            ext_data = await tmdb.get_show(int(ext_id), api_key=tmdb_api_key)
                            tvdb_id = ext_data.get("external_ids", {}).get("tvdb_id")
                            if tvdb_id:
                                try:
                                    raw = await tvdb_client.get_series(tvdb_id, tvdb_api_key, lang=tvdb_lang)
                                    show_fmt = tvdb_client.format_series(raw, lang=tvdb_lang)
                                    result_item["title"] = show_fmt.get("title") or result_item["title"]
                                    result_item["release_date"] = show_fmt.get("first_air_date") or result_item["release_date"]
                                    result_item["poster_path"] = show_fmt.get("poster_path") or result_item["poster_path"]
                                    result_item["backdrop_path"] = show_fmt.get("backdrop_path") or result_item["backdrop_path"]
                                    result_item["overview"] = show_fmt.get("overview") or result_item["overview"]
                                except Exception:
                                    pass
                        return result_item
                    except Exception:
                        return None
            if provider == "tvdb" and tvdb_api_key:
                async with sem:
                    try:
                        raw = await tvdb_client.get_series(int(ext_id), tvdb_api_key, lang=tvdb_lang)
                        show_fmt = tvdb_client.format_series(raw, lang=tvdb_lang)
                        return {"uri_id": uri, "tvdb_id": int(ext_id),
                                "tmdb_id": show_fmt.get("tmdb_id_cross"), "type": "series",
                                "title": show_fmt.get("title"), "release_date": show_fmt.get("first_air_date"),
                                "poster_path": show_fmt.get("poster_path"),
                                "backdrop_path": show_fmt.get("backdrop_path"),
                                "overview": show_fmt.get("overview"), "tmdb_rating": None}
                    except Exception:
                        return None
        return None

    tasks = [(b, fetch_by_uri(b)) for b in blocked if b.media_type in (MediaType.movie, MediaType.series)]
    results_raw = await asyncio.gather(*(t[1] for t in tasks))

    output = []
    seen_uris: set[str] = set()
    for (b, _), data in zip(tasks, results_raw):
        if not data:
            continue
        if b.uri_id in seen_uris:
            continue
        seen_uris.add(b.uri_id)
        data["is_dropped"] = b.is_dropped
        output.append(data)

    return output


class BlockRequest(BaseModel):
    uri_id: str
    media_type: MediaType
    is_dropped: bool = False


@router.post("/blocklist")
async def block_item(
    req: BlockRequest,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Block or drop a media item."""
    canonical_uri = req.uri_id
    if not canonical_uri:
        raise HTTPException(status_code=400, detail="uri_id cannot be empty")

    existing_q = await db.execute(
        select(BlocklistItem).where(
            BlocklistItem.user_id == current_user.id,
            BlocklistItem.uri_id == canonical_uri,
        )
    )
    existing = existing_q.scalar_one_or_none()
    if existing:
        existing.is_dropped = req.is_dropped
        await db.commit()
        return {"status": "updated", "is_dropped": req.is_dropped}

    db.add(BlocklistItem(
        user_id=current_user.id,
        uri_id=canonical_uri,
        media_type=req.media_type,
        is_dropped=req.is_dropped,
    ))
    await db.commit()
    return {"status": "ok", "is_dropped": req.is_dropped}


@router.delete("/blocklist")
async def unblock_item(
    media_type: MediaType,
    uri_id: str = Query(...),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Unblock or undrop a media item."""
    canonical_uri = uri_id

    q = select(BlocklistItem).where(
        BlocklistItem.user_id == current_user.id,
        BlocklistItem.uri_id == canonical_uri,
    )
    result = await db.execute(q)
    block = result.scalar_one_or_none()
    if not block:
        return {"status": "not blocked"}

    await db.delete(block)
    await db.commit()
    return {"status": "unblocked"}


@router.get("/{type}/{media_id_or_uri}")
async def get_media_details(
    type: MediaType,
    media_id_or_uri: str,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Unified endpoint for Movies and Episodes details. Accepts URI (tmdb:m:123) or integer ID."""
    tmdb_id = _parse_tmdb_id(media_id_or_uri)

    tmdb_key = await get_user_tmdb_key(db, current_user.id)
    if not check_tmdb_key(tmdb_key):
        raise HTTPException(status_code=404, detail="TMDB API Key not configured")

    user_lang = await get_user_content_language(db, current_user.id)

    try:
        # 1. Fetch from TMDB
        if type == MediaType.movie:
            try:
                data = await tmdb.get_movie(tmdb_id, api_key=tmdb_key)
            except Exception as e:
                # TMDB fetch failed — try to fall back to local DB
                local_media_q = await db.execute(
                    select(Media).where(Media.tmdb_id == tmdb_id, Media.media_type == MediaType.movie)
                )
                local_media = local_media_q.scalars().first()
                if not local_media:
                    raise
                
                data = local_media.tmdb_data or {}
                data.setdefault("title", local_media.title)
                data.setdefault("original_title", local_media.original_title or local_media.title)
                data.setdefault("overview", local_media.overview)
                data.setdefault("poster_path", local_media.poster_path)
                data.setdefault("backdrop_path", local_media.backdrop_path)
                data.setdefault("release_date", local_media.release_date)
                data.setdefault("runtime", local_media.runtime)
                data.setdefault("vote_average", local_media.tmdb_rating or 0.0)
                data.setdefault("tagline", local_media.tagline)
                data.setdefault("status", local_media.status)
                data.setdefault("belongs_to_collection", None)
                data.setdefault("production_companies", [])
                data.setdefault("genres", [])
                data.setdefault("credits", {"cast": []})
                data.setdefault("adult", local_media.adult)

            # Videos and images are supplemental — a 404 on them should not kill the page
            try:
                videos_data = await tmdb.get_movie_videos(tmdb_id, api_key=tmdb_key)
            except Exception:
                videos_data = {}
            try:
                images_data = await tmdb.get_movie_images(tmdb_id, api_key=tmdb_key)
            except Exception:
                images_data = {}
            trailer_youtube_id = tmdb.extract_trailer(videos_data)
            # Pick best backdrop + logo using No Language → user lang → any priority
            picked_backdrop = tmdb.pick_image(images_data.get("backdrops", []), preferred_lang=user_lang, size="original")
            picked_logo = tmdb.pick_image(images_data.get("logos", []), preferred_lang=user_lang, size="w500")
        elif type == MediaType.episode:
            # Look up the episode in the local DB to find its show context
            ep_result = await db.execute(
                select(Media).where(Media.tmdb_id == tmdb_id, Media.media_type == MediaType.episode)
            )
            local_ep = ep_result.scalars().first()

            if local_ep is None or local_ep.show_id is None:
                raise HTTPException(
                    status_code=404,
                    detail="Episode not found — play it via Plex/Jellyfin first so it can be enriched with show context",
                )

            if local_ep.season_number is None or local_ep.episode_number is None:
                raise HTTPException(
                    status_code=404,
                    detail="Episode is missing season/episode numbers — try refreshing metadata from the show page",
                )

            show_result = await db.execute(select(ShowModel).where(ShowModel.id == local_ep.show_id))
            show = show_result.scalar_one_or_none()
            if show is None:
                raise HTTPException(status_code=404, detail="Show not found for this episode")

            try:
                ep_data, ep_show_images = await asyncio.gather(
                    tmdb.get_episode(
                        show.tmdb_id, local_ep.season_number, local_ep.episode_number, api_key=tmdb_key
                    ),
                    tmdb.get_tv_images(show.tmdb_id, api_key=tmdb_key),
                )
            except Exception:
                # Fall back to local episode data
                ep_data = local_ep.tmdb_data or {}
                ep_data.setdefault("name", local_ep.title)
                ep_data.setdefault("overview", local_ep.overview)
                ep_data.setdefault("still_path", local_ep.poster_path)
                ep_data.setdefault("air_date", local_ep.release_date)
                ep_data.setdefault("runtime", local_ep.runtime)
                ep_data.setdefault("vote_average", local_ep.tmdb_rating or 0.0)
                ep_data.setdefault("credits", {"cast": []})
                ep_show_images = {}
            ep_state: dict = {"tmdb_id": tmdb_id, "type": "episode"}
            await enrich_with_state(db, current_user.id, [ep_state])

            # Pick best backdrop + logo for the parent show
            ep_backdrop = tmdb.pick_image(ep_show_images.get("backdrops", []), preferred_lang=user_lang, size="original") or show.backdrop_path
            ep_logo = tmdb.pick_image(ep_show_images.get("logos", []), preferred_lang=user_lang, size="w500")

            # Check local info for library tags
            library_info = None
            if local_ep:
                coll_q = (
                    select(CollectionFile)
                    .join(Collection, Collection.id == CollectionFile.collection_id)
                    .where(Collection.media_id == local_ep.id, Collection.user_id == current_user.id)
                    .order_by(CollectionFile.added_at.desc())
                )
                coll_res = await db.execute(coll_q)
                coll_file = coll_res.scalars().first()
                if coll_file:
                    library_info = {
                        "resolution": coll_file.resolution,
                        "video_codec": coll_file.video_codec,
                        "audio_codec": coll_file.audio_codec,
                        "audio_channels": coll_file.audio_channels,
                        "audio_languages": coll_file.audio_languages,
                        "subtitle_languages": coll_file.subtitle_languages,
                    }

            return {
                "id": local_ep.id,
                "uri_id": local_ep.uri_id or (f"tmdb:e:{tmdb_id}" if tmdb_id else None),
                "tmdb_id": tmdb_id,
                "type": "episode",
                "title": ep_data.get("name") or local_ep.title,
                "overview": ep_data.get("overview"),
                "poster_path": tmdb.poster_url(ep_data.get("still_path"), size="w780"),
                "backdrop_path": ep_backdrop,
                "logo_path": ep_logo,
                "release_date": ep_data.get("air_date"),
                "tmdb_rating": ep_data.get("vote_average"),
                "runtime": ep_data.get("runtime"),
                "season_number": local_ep.season_number,
                "episode_number": local_ep.episode_number,
                "show_title": show.title,
                "show_uri_id": show.uri_id,
                "show_tmdb_id": show.tmdb_id,
                "show_poster_path": show.poster_path,
                "show_backdrop_path": ep_backdrop,
                "directors": [
                    {"tmdb_id": c.get("id"), "name": c.get("name")}
                    for c in (ep_data.get("credits") or {}).get("crew", [])
                    if c.get("job") == "Director"
                ],
                "cast": [
                    {
                        "tmdb_id": c.get("id"),
                        "name": c.get("name"),
                        "character": c.get("character"),
                        "profile_path": tmdb.poster_url(c.get("profile_path"), size="w185"),
                    }
                    for c in (ep_data.get("credits") or {}).get("cast", [])[:12]
                ],
                "genres": (show.tmdb_data or {}).get("genres", []),
                "in_library": ep_state.get("in_library", False),
                "watched": ep_state.get("watched", False),
                "in_lists": ep_state.get("in_lists", []),
                "user_rating": ep_state.get("user_rating"),
                "play_count": ep_state.get("play_count", 0),
                "is_blocked": ep_state.get("is_blocked", False),
                "is_dropped": ep_state.get("is_dropped", False),
                "library": library_info,
            }
        else:
            raise HTTPException(
                status_code=400, detail="Use /shows/{tmdb_id} for series"
            )

        # 2. Check local info
        query = select(Media).where(Media.tmdb_id == tmdb_id, Media.media_type == type)
        result = await db.execute(query)
        media = result.scalars().first()

        local_info = {"in_library": False, "library": None, "id": None}
        if media:
            local_info["in_library"] = True
            local_info["id"] = media.id
            if not media.adult and data.get("adult", False):
                media.adult = True
                await db.commit()
            coll_q = (
                select(CollectionFile)
                .join(Collection, Collection.id == CollectionFile.collection_id)
                .where(Collection.media_id == media.id, Collection.user_id == current_user.id)
                .order_by(CollectionFile.added_at.desc())
            )
            coll_res = await db.execute(coll_q)
            coll_file = coll_res.scalars().first()
            if coll_file:
                local_info["library"] = {
                    "resolution": coll_file.resolution,
                    "video_codec": coll_file.video_codec,
                    "audio_codec": coll_file.audio_codec,
                    "audio_channels": coll_file.audio_channels,
                    "audio_languages": coll_file.audio_languages,
                    "subtitle_languages": coll_file.subtitle_languages,
                }

        # 3. Format Merged Response

        production_companies = [
            {
                "id": c["id"],
                "name": c["name"],
                "logo_path": tmdb.poster_url(c.get("logo_path"), size="w500")
                if c.get("logo_path")
                else None,
                "origin_country": c.get("origin_country"),
            }
            for c in data.get("production_companies", [])
        ]

        # Gather: collection fetch + state enrichment + where-to-watch in parallel
        state_item: dict = {"tmdb_id": tmdb_id, "type": type.value}
        raw_coll = data.get("belongs_to_collection") if type == MediaType.movie else None

        gather_coros = [
            enrich_with_state(db, current_user.id, [state_item]),
            get_where_to_watch(db, current_user.id, tmdb_id, MediaType.movie, media=media, tmdb_key=tmdb_key),
        ]
        if raw_coll:
            gather_coros.append(tmdb.get_collection(raw_coll["id"], api_key=tmdb_key))

        gather_results = await asyncio.gather(*gather_coros)
        where_to_watch = gather_results[1]
        coll_data = gather_results[2] if raw_coll else None

        collection = None
        if coll_data:
            collection = {
                "id": coll_data.get("id"),
                "name": coll_data.get("name"),
                "poster_path": tmdb.poster_url(coll_data.get("poster_path")),
                "backdrop_path": tmdb.poster_url(
                    coll_data.get("backdrop_path"), size="original"
                ),
                "parts": [
                    {
                        "tmdb_id": p.get("id"),
                        "title": p.get("title"),
                        "type": MediaType.movie,
                        "poster_path": tmdb.poster_url(p.get("poster_path")),
                        "release_date": p.get("release_date"),
                        "overview": p.get("overview"),
                        "adult": p.get("adult", False),
                    }
                    for p in coll_data.get("parts", [])
                ],
            }

        if collection and collection.get("parts"):
            await enrich_with_state(db, current_user.id, collection["parts"])

        return {
            **local_info,
            "tmdb_id": tmdb_id,
            "type": type,
            "watched": state_item.get("watched", False),
            "in_lists": state_item.get("in_lists", []),
            "user_rating": state_item.get("user_rating"),
            "play_count": state_item.get("play_count", 0),
            "is_blocked": state_item.get("is_blocked", False),
            "in_library": state_item.get("in_library", local_info["in_library"]),
            "collection_pct": state_item.get("collection_pct", 100 if local_info["in_library"] else 0),
            "is_monitored": state_item.get("is_monitored", False),
            "request_enabled": state_item.get("request_enabled", False),
            "request_status": state_item.get("request_status"),
            "title": data.get("title") or data.get("name"),
            "original_title": data.get("original_title") or data.get("original_name"),
            "overview": data.get("overview"),
            "poster_path": tmdb.poster_url(data.get("poster_path")),
            "backdrop_path": picked_backdrop or tmdb.poster_url(data.get("backdrop_path"), size="original"),
            "logo_path": picked_logo,
            "release_date": data.get("release_date") or data.get("first_air_date"),
            "tmdb_rating": data.get("vote_average"),
            "tagline": data.get("tagline"),
            "runtime": data.get("runtime"),
            "status": data.get("status"),
            "genres": [g["name"] for g in data.get("genres", [])],
            "original_language": data.get("original_language"),
            "age_rating": _extract_movie_certification(data),
            "release_dates": _extract_movie_release_dates(data),
            "imdb_id": data.get("imdb_id"),
            "adult": data.get("adult", False),
            "collection": collection,
            "production_companies": production_companies,
            "directors": [
                {"tmdb_id": c["id"], "name": c["name"]}
                for c in data.get("credits", {}).get("crew", [])
                if c.get("job") == "Director"
            ],
            "cast": [
                {
                    "tmdb_id": c["id"],
                    "name": c["name"],
                    "character": c["character"],
                    "profile_path": tmdb.poster_url(c["profile_path"]),
                }
                for c in data.get("credits", {}).get("cast", [])[:12]
            ],
            "where_to_watch": where_to_watch,
            "trailer_youtube_id": trailer_youtube_id,
        }
    except Exception as e:
        if isinstance(e, HTTPException):
            raise e
        raise HTTPException(status_code=404, detail=f"TMDB Media not found: {e}")


@router.get("/{type}/{media_id_or_uri}/recommendations")
async def get_media_recommendations(
    type: MediaType,
    media_id_or_uri: str,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    tmdb_id = _parse_tmdb_id(media_id_or_uri)
    """Fetch movie/series recommendations from TMDB and enrich with state."""
    tmdb_key = await get_user_tmdb_key(db, current_user.id)
    if not check_tmdb_key(tmdb_key):
        return {"results": []}

    try:
        if type == MediaType.movie:
            data = await tmdb.get_movie(tmdb_id, api_key=tmdb_key)
        else:
            data = await tmdb.get_show(tmdb_id, api_key=tmdb_key)
        
        recs_raw = data.get("recommendations", {}).get("results", [])[:12]
        
        blocked_ids = await _get_blocked_ids(db, current_user.id, type)
        cf = await _get_content_filters(db, current_user.id)
        recs_raw = [res for res in recs_raw if res.get("id") not in blocked_ids and not _is_content_filtered(res, *cf)]

        recommendations = [
            {
                "id": None,
                "tmdb_id": r["id"],
                "type": type.value,
                "title": r.get("title") or r.get("name"),
                "original_title": r.get("original_title") or r.get("original_name"),
                "overview": r.get("overview"),
                "poster_path": tmdb.poster_url(r.get("poster_path")),
                "backdrop_path": tmdb.poster_url(r.get("backdrop_path"), size="w1280"),
                "release_date": r.get("release_date") or r.get("first_air_date"),
                "tmdb_rating": r.get("vote_average"),
                "adult": r.get("adult", False),
            }
            for r in recs_raw
        ]
        await enrich_with_state(db, current_user.id, recommendations)
        return {"results": recommendations}
    except Exception:
        return {"results": []}


def _normalize_path(path: str | None, size: str = "w500") -> str | None:
    if not path:
        return None
    if path.startswith("http"):
        return path
    return tmdb.poster_url(path, size=size)


@router.get("/pick")
async def pick_for_me(
    type: str = Query("movie"),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    import random
    from models.profile import UserProfileData

    if type not in ("movie", "series"):
        raise HTTPException(status_code=400, detail="type must be 'movie' or 'series'")

    tmdb_key = await get_user_tmdb_key(db, current_user.id)

    profile_q = await db.execute(select(UserProfileData).where(UserProfileData.user_id == current_user.id))
    profile = profile_q.scalar_one_or_none()

    conns_q = await db.execute(select(MediaServerConnection).where(MediaServerConnection.user_id == current_user.id))
    connections = conns_q.scalars().all()

    streaming_ids = [int(s) for s in (profile.streaming_services or [])] if profile else []
    region = (profile.country if profile and profile.country else None) or "US"
    has_media_server = bool(connections)

    if not streaming_ids and not has_media_server:
        raise HTTPException(status_code=400, detail="no_sources")

    # ── Watched IDs ────────────────────────────────────────────────────────
    if type == "movie":
        wq = await db.execute(
            select(Media.tmdb_id)
            .join(WatchEvent, WatchEvent.media_id == Media.id)
            .where(WatchEvent.user_id == current_user.id, Media.media_type == MediaType.movie)
            .distinct()
        )
    else:
        wq = await db.execute(
            select(ShowModel.tmdb_id)
            .join(Media, Media.show_id == ShowModel.id)
            .join(WatchEvent, WatchEvent.media_id == Media.id)
            .where(WatchEvent.user_id == current_user.id)
            .distinct()
        )
    watched_ids: set[int] = {r[0] for r in wq.all()}
    
    media_type = MediaType.movie if type == "movie" else MediaType.series
    blocked_ids = await _get_blocked_ids(db, current_user.id, media_type)
    cf = await _get_content_filters(db, current_user.id)


    # ── Collection pool (unwatched) ────────────────────────────────────────
    collection_items: list[dict] = []
    if has_media_server:
        if type == "movie":
            cq = await db.execute(
                select(Media.tmdb_id, Media.title, Media.poster_path, Media.backdrop_path,
                       Media.release_date, Media.tmdb_rating, Media.overview)
                .join(Collection, Collection.media_id == Media.id)
                .where(Collection.user_id == current_user.id, Media.media_type == MediaType.movie)
                .where(Media.tmdb_id.notin_(watched_ids))
                .distinct()
            )
            collection_items = [
                {
                    "tmdb_id": r[0], "type": "movie", "title": r[1],
                    "poster_path": _normalize_path(r[2]),
                    "backdrop_path": _normalize_path(r[3], "w1280"),
                    "release_date": r[4], "tmdb_rating": r[5],
                    "overview": r[6], "in_library": True,
                }
                for r in cq.all() if r[0] and r[0] not in watched_ids and r[0] not in blocked_ids
            ]
            # Apply content filtering
            collection_items = [i for i in collection_items if not _is_content_filtered(i, *cf)]

        else:
            cq = await db.execute(
                select(ShowModel.tmdb_id, ShowModel.title, ShowModel.poster_path,
                       ShowModel.backdrop_path, ShowModel.first_air_date,
                       ShowModel.tmdb_rating, ShowModel.overview, ShowModel.tvdb_id)
                .join(Media, Media.show_id == ShowModel.id)
                .join(Collection, Collection.media_id == Media.id)
                .where(Collection.user_id == current_user.id)
                .where(ShowModel.tmdb_id.notin_(watched_ids))
                .distinct()
            )
            collection_items = [
                {
                    "tmdb_id": r[0], "type": "series", "title": r[1],
                    "poster_path": _normalize_path(r[2]),
                    "backdrop_path": _normalize_path(r[3], "w1280"),
                    "release_date": r[4], "tmdb_rating": r[5],
                    "overview": r[6], "in_library": True,
                    "tvdb_id": r[7],
                }
                for r in cq.all() if r[0] and r[0] not in watched_ids and r[0] not in blocked_ids
            ]
            # Apply content filtering
            collection_items = [i for i in collection_items if not _is_content_filtered(i, *cf)]


    # ── Streaming pool (progressive fallback) ─────────────────────────────
    streaming_candidates: list[dict] = []
    if streaming_ids and check_tmdb_key(tmdb_key):
        disliked: set[str] = set(profile.disliked_genres or []) if profile else set()
        user_genres = (profile.liked_genres or []) if profile else []
        genre_map = MOVIE_GENRE_IDS if type == "movie" else TV_GENRE_IDS
        genre_ids = [genre_map[g] for g in user_genres if g in genre_map]

        tiers = [
            {"genre_ids": genre_ids[:3], "min_rating": 6.0},
            {"genre_ids": [], "min_rating": 6.0},
            {"genre_ids": [], "min_rating": None},
        ]

        for tier in tiers:
            if len(streaming_candidates) + len(collection_items) >= 15:
                break
            coros = []
            for pid in streaming_ids:
                kwargs: dict = dict(watch_provider_id=pid, watch_region=region, api_key=tmdb_key)
                if tier["min_rating"]:
                    kwargs["min_rating"] = tier["min_rating"]
                if tier["genre_ids"]:
                    for gid in tier["genre_ids"]:
                        fn = tmdb.discover_movies if type == "movie" else tmdb.discover_shows
                        coros.append(fn(genre_id=gid, **kwargs))
                else:
                    fn = tmdb.discover_movies if type == "movie" else tmdb.discover_shows
                    coros.append(fn(**kwargs))

            if coros:
                results_list = await asyncio.gather(*coros, return_exceptions=True)
                for res in results_list:
                    if isinstance(res, Exception):
                        continue
                    for r in res.get("results", []):
                        tid = r.get("id")
                        if tid and tid not in watched_ids and tid not in blocked_ids and not _is_content_filtered(r, *cf):
                            streaming_candidates.append(r)


    # ── Combine & deduplicate ──────────────────────────────────────────────
    genre_id_map = {v: k for k, v in (MOVIE_GENRE_IDS if type == "movie" else TV_GENRE_IDS).items()}

    seen: set[int] = set()
    all_candidates: list[dict] = []

    for item in collection_items:
        if item["tmdb_id"] not in seen:
            seen.add(item["tmdb_id"])
            all_candidates.append(item)

    for r in streaming_candidates:
        tid = r.get("id")
        if tid and tid not in seen and tid not in watched_ids:
            seen.add(tid)
            all_candidates.append({
                "tmdb_id": tid,
                "type": type,
                "title": r.get("title") if type == "movie" else r.get("name"),
                "poster_path": tmdb.poster_url(r.get("poster_path")),
                "backdrop_path": tmdb.poster_url(r.get("backdrop_path"), size="w1280"),
                "release_date": r.get("release_date") if type == "movie" else r.get("first_air_date"),
                "tmdb_rating": r.get("vote_average"),
                "overview": r.get("overview"),
                "in_library": False,
                "genres": [genre_id_map[gid] for gid in r.get("genre_ids", []) if gid in genre_id_map],
                "adult": r.get("adult", False),
            })

    if not all_candidates:
        raise HTTPException(status_code=404, detail="no_results")

    liked_set = set(user_genres)
    if disliked or liked_set:
        weights = []
        for item in all_candidates:
            item_genres: list[str] = item.get("genres") or []
            score = 1.0
            for g in item_genres:
                if g in liked_set:
                    score += 2.0
                elif g in disliked:
                    score -= 1.5
            weights.append(max(0.05, score))
        pick = random.choices(all_candidates, weights=weights, k=1)[0]
    else:
        pick = random.choice(all_candidates)

    # ── Fetch local object for the picked item (to find local sources) ─────
    local_media = None
    local_show = None
    if type == "movie":
        m_res = await db.execute(
            select(Media).where(Media.tmdb_id == pick["tmdb_id"], Media.media_type == MediaType.movie)
        )
        local_media = m_res.scalars().first()
    else:
        s_res = await db.execute(select(ShowModel).where(ShowModel.tmdb_id == pick["tmdb_id"]))
        local_show = s_res.scalar_one_or_none()

    # Use local data for overview/genres if missing
    if local_media:
        if not pick.get("overview"): pick["overview"] = local_media.overview
        if not pick.get("genres") and local_media.tmdb_data:
            pick["genres"] = local_media.tmdb_data.get("genres", [])
    elif local_show:
        if not pick.get("overview"): pick["overview"] = local_show.overview
        if not pick.get("genres") and local_show.tmdb_data:
            pick["genres"] = local_show.tmdb_data.get("genres", [])

    # ── Enrich pick: overview + genres + watch providers ───────────────────
    if check_tmdb_key(tmdb_key):
        try:
            if not pick.get("overview") or not pick.get("genres"):
                if type == "movie":
                    details = await tmdb.get_movie(pick["tmdb_id"], api_key=tmdb_key)
                else:
                    details = await tmdb.get_show(pick["tmdb_id"], api_key=tmdb_key)
                
                if not pick.get("overview"):
                    pick["overview"] = details.get("overview")
                if not pick.get("genres"):
                    pick["genres"] = [g["name"] for g in details.get("genres", [])]

            pick["sources"] = await get_where_to_watch(
                db, current_user.id, pick["tmdb_id"],
                MediaType.movie if type == "movie" else MediaType.series,
                media=local_media,
                show=local_show,
                tmdb_key=tmdb_key
            )
        except Exception:
            pick["sources"] = []
    else:
        # No TMDB key, but we might still have local sources
        pick["sources"] = await get_where_to_watch(
            db, current_user.id, pick["tmdb_id"],
            MediaType.movie if type == "movie" else MediaType.series,
            media=local_media,
            show=local_show
        )

    if type == "series":
        if local_show and local_show.tvdb_id:
            pick["tvdb_id"] = local_show.tvdb_id
        elif check_tmdb_key(tmdb_key):
            try:
                details_for_id = await tmdb.get_show(pick["tmdb_id"], api_key=tmdb_key)
                tv_id = details_for_id.get("external_ids", {}).get("tvdb_id")
                if tv_id:
                    pick["tvdb_id"] = int(tv_id)
            except Exception:
                pass

    return pick





# ── Content Filters (genre / keyword / regex) ───────────────────────────────

@router.get("/content-filters")
async def get_content_filters(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Return the user's active content filter rules and the known genre list."""
    blocked_genres, blocked_keywords, blocked_regexes, filter_languages, language_filter_mode = await _get_content_filters(db, current_user.id, lower=False)
    return {
        "blocked_genres": sorted(blocked_genres),
        "blocked_keywords": blocked_keywords,
        "blocked_regexes": blocked_regexes,
        "filter_languages": filter_languages,
        "language_filter_mode": language_filter_mode,
        "available_genres": sorted(set(list(MOVIE_GENRE_IDS.keys()) + list(TV_GENRE_IDS.keys()))),
    }


class GenreFilterRequest(BaseModel):
    genres: list[str]

class KeywordFilterRequest(BaseModel):
    keywords: list[str]

class RegexFilterRequest(BaseModel):
    regexes: list[str]

class LanguageFilterRequest(BaseModel):
    languages: list[str]
    mode: str


async def _patch_content_filters(db: AsyncSession, user_id: int, update: dict) -> None:
    """Merge `update` into the user's preferences.content_filters, creating settings row if absent."""
    q = await db.execute(select(UserSettings).where(UserSettings.user_id == user_id))
    settings = q.scalar_one_or_none()
    if not settings:
        settings = UserSettings(user_id=user_id, preferences={})
        db.add(settings)
    prefs = dict(settings.preferences or {})
    cf = dict(prefs.get("content_filters", {}))
    cf.update(update)
    prefs["content_filters"] = cf
    settings.preferences = prefs
    flag_modified(settings, "preferences")
    await db.commit()


@router.put("/content-filters/genres")
async def update_genre_filters(
    req: GenreFilterRequest,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Replace the blocked genre list."""
    # Validate against known genres
    known = set(list(MOVIE_GENRE_IDS.keys()) + list(TV_GENRE_IDS.keys()))
    valid = [g for g in req.genres if g in known]
    await _patch_content_filters(db, current_user.id, {"blocked_genres": valid})
    return {"status": "ok", "blocked_genres": valid}


@router.put("/content-filters/keywords")
async def update_keyword_filters(
    req: KeywordFilterRequest,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Replace the blocked keyword list."""
    cleaned = [k.strip() for k in req.keywords if k.strip()]
    await _patch_content_filters(db, current_user.id, {"blocked_keywords": cleaned})
    return {"status": "ok", "blocked_keywords": cleaned}


@router.put("/content-filters/regexes")
async def update_regex_filters(
    req: RegexFilterRequest,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Replace the blocked regex list. Invalid patterns are rejected with a 422."""
    import re as _re
    validated: list[str] = []
    for pat in req.regexes:
        pat = pat.strip()
        if not pat:
            continue
        try:
            _re.compile(pat)
            validated.append(pat)
        except _re.error as e:
            raise HTTPException(status_code=422, detail=f"Invalid regex '{pat}': {e}")
    await _patch_content_filters(db, current_user.id, {"blocked_regexes": validated})
    return {"status": "ok", "blocked_regexes": validated}


@router.put("/content-filters/languages")
async def update_language_filters(
    req: LanguageFilterRequest,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Replace the language filter list and mode."""
    cleaned = [lang.strip().lower() for lang in req.languages if lang.strip()]
    mode = req.mode.strip().lower()
    if mode not in ("blacklist", "whitelist"):
        mode = "blacklist"
    await _patch_content_filters(db, current_user.id, {
        "filter_languages": cleaned,
        "language_filter_mode": mode
    })
    return {"status": "ok", "filter_languages": cleaned, "language_filter_mode": mode}
