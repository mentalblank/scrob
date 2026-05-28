import asyncio
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import and_, or_, select, desc, func, delete
from sqlalchemy.orm import selectinload
from db import get_db
from models.media import Media
from models.show import Show
from models.events import WatchEvent
from models.playback_session import PlaybackSession
from models.playback_progress import PlaybackProgress
from models.collection import Collection, CollectionFile
from models.base import MediaType, CollectionSource
from models.users import UserSettings
from models.connections import MediaServerConnection
from routers.media import format_media, enrich_with_state, get_user_tmdb_key, check_tmdb_key

from dependencies import get_current_user
from models.users import User
import core.plex as plex_client
import core.jellyfin as jellyfin_client
import core.emby as emby_client
import core.trakt as trakt_client

router = APIRouter()


async def _push_watch_state(
    db: AsyncSession,
    user_id: int,
    media_ids: list[int],
    watched: bool,
) -> None:
    """Fan-out watched/unwatched state to all connections with push_watched enabled."""
    if not media_ids:
        return

    conns_result = await db.execute(
        select(MediaServerConnection).where(
            MediaServerConnection.user_id == user_id,
            MediaServerConnection.push_watched == True,
        )
    )
    connections = conns_result.scalars().all()

    settings_result = await db.execute(select(UserSettings).where(UserSettings.user_id == user_id))
    settings = settings_result.scalar_one_or_none()
    push_trakt = settings and settings.trakt_push_watched and settings.trakt_access_token

    tasks = []

    if connections:
        files_result = await db.execute(
            select(CollectionFile)
            .join(Collection, Collection.id == CollectionFile.collection_id)
            .where(
                Collection.user_id == user_id,
                Collection.media_id.in_(media_ids),
            )
        )
        coll_files = files_result.scalars().all()

        conn_by_type: dict[str, list[MediaServerConnection]] = {}
        for conn in connections:
            conn_by_type.setdefault(conn.type, []).append(conn)

        for coll_file in coll_files:
            if not coll_file.source_id:
                continue
            source_type = coll_file.source.value if hasattr(coll_file.source, "value") else str(coll_file.source)
            for conn in conn_by_type.get(source_type, []):
                if coll_file.source == CollectionSource.plex:
                    if watched:
                        tasks.append(plex_client.mark_watched(conn.url, conn.token, coll_file.source_id))
                    else:
                        tasks.append(plex_client.mark_unwatched(conn.url, conn.token, coll_file.source_id))
                elif coll_file.source == CollectionSource.jellyfin:
                    if watched:
                        tasks.append(jellyfin_client.mark_watched(conn.url, conn.token, conn.server_user_id, coll_file.source_id))
                    else:
                        tasks.append(jellyfin_client.mark_unwatched(conn.url, conn.token, conn.server_user_id, coll_file.source_id))
                elif coll_file.source == CollectionSource.emby:
                    if watched:
                        tasks.append(emby_client.mark_watched(conn.url, conn.token, conn.server_user_id, coll_file.source_id))
                    else:
                        tasks.append(emby_client.mark_unwatched(conn.url, conn.token, conn.server_user_id, coll_file.source_id))

    push_simkl = settings and settings.simkl_push_watched and settings.simkl_access_token
    if push_simkl and settings.simkl_client_id:
        from core import simkl as simkl_client
        simkl_media_res = await db.execute(select(Media).where(Media.id.in_(media_ids)))
        simkl_media_items = simkl_media_res.scalars().all()
        for media in simkl_media_items:
            if media.media_type == MediaType.movie:
                movie_tmdb_id = media.tmdb_id
                if not movie_tmdb_id and media.uri_id:
                    from utils.alias_lookup import get_provider_id_for_uri
                    try:
                        alias = await get_provider_id_for_uri(db, media.uri_id, "tmdb")
                        movie_tmdb_id = int(alias) if alias else None
                    except Exception:
                        pass
                if not movie_tmdb_id:
                    continue
                if watched:
                    tasks.append(simkl_client.add_movie_to_history(settings.simkl_client_id, settings.simkl_access_token, movie_tmdb_id))
                else:
                    tasks.append(simkl_client.remove_movie_from_history(settings.simkl_client_id, settings.simkl_access_token, movie_tmdb_id))
            elif media.media_type == MediaType.episode and media.show_id and media.season_number is not None and media.episode_number is not None:
                show_res = await db.execute(select(Show).where(Show.id == media.show_id))
                show = show_res.scalar_one_or_none()
                if not show:
                    continue
                show_tmdb_id = show.tmdb_id
                if not show_tmdb_id and show.uri_id:
                    from utils.alias_lookup import get_provider_id_for_uri
                    try:
                        alias = await get_provider_id_for_uri(db, show.uri_id, "tmdb")
                        show_tmdb_id = int(alias) if alias else None
                    except Exception:
                        pass
                if not show_tmdb_id:
                    continue
                if watched:
                    tasks.append(simkl_client.add_episode_to_history(settings.simkl_client_id, settings.simkl_access_token, show_tmdb_id, media.season_number, media.episode_number))
                else:
                    tasks.append(simkl_client.remove_episode_from_history(settings.simkl_client_id, settings.simkl_access_token, show_tmdb_id, media.season_number, media.episode_number))

    if push_trakt and settings.trakt_client_id:
        media_res = await db.execute(
            select(Media).where(Media.id.in_(media_ids))
        )
        media_items = media_res.scalars().all()
        for media in media_items:
            if media.media_type == MediaType.movie:
                # Use alias registry to find the TMDB ID (handles cross-provider movies)
                movie_tmdb_id = media.tmdb_id
                if not movie_tmdb_id and media.uri_id:
                    from utils.alias_lookup import get_provider_id_for_uri
                    try:
                        alias = await get_provider_id_for_uri(db, media.uri_id, "tmdb")
                        movie_tmdb_id = int(alias) if alias else None
                    except Exception:
                        pass
                if not movie_tmdb_id:
                    continue
                if watched:
                    tasks.append(trakt_client.add_movie_to_history(settings.trakt_client_id, settings.trakt_access_token, movie_tmdb_id))
                else:
                    tasks.append(trakt_client.remove_movie_from_history(settings.trakt_client_id, settings.trakt_access_token, movie_tmdb_id))
            elif media.media_type == MediaType.episode and media.show_id and media.season_number is not None and media.episode_number is not None:
                show_res = await db.execute(select(Show).where(Show.id == media.show_id))
                show = show_res.scalar_one_or_none()
                if not show:
                    continue
                # Use positive TMDB ID from alias registry for TVDB-only shows
                show_tmdb_id = show.tmdb_id
                if not show_tmdb_id and show.uri_id:
                    from utils.alias_lookup import get_provider_id_for_uri
                    try:
                        alias = await get_provider_id_for_uri(db, show.uri_id, "tmdb")
                        show_tmdb_id = int(alias) if alias else None
                    except Exception:
                        pass
                if not show_tmdb_id:
                    continue
                if watched:
                    tasks.append(trakt_client.add_episode_to_history(settings.trakt_client_id, settings.trakt_access_token, show_tmdb_id, media.season_number, media.episode_number))
                else:
                    tasks.append(trakt_client.remove_episode_from_history(settings.trakt_client_id, settings.trakt_access_token, show_tmdb_id, media.season_number, media.episode_number))

    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)


def format_event(event: WatchEvent | PlaybackProgress, media: Media) -> dict:
    # Handle both WatchEvent (history) and PlaybackProgress (continue watching)
    watched_at = getattr(event, "watched_at", None) or getattr(event, "updated_at", datetime.utcnow())
    
    data = {
        "id": event.id,
        "media": format_media(media),
        "user_id": event.user_id,
        "watched_at": watched_at.isoformat(),
        "progress_seconds": event.progress_seconds,
        "progress_percent": event.progress_percent,
        "completed": getattr(event, "completed", False),
        "play_count": getattr(event, "play_count", 1),
    }

    if media.media_type == MediaType.episode and media.show:
        data["media"]["show_title"] = media.show.title
        data["media"]["show_uri_id"] = media.show.uri_id
        data["media"]["show_poster_path"] = media.show.poster_path
        data["media"]["show_tmdb_id"] = media.show.tmdb_id
        data["media"]["show_tvdb_id"] = media.show.tvdb_id if media.show.tvdb_id else (
            int(media.show.tmdb_data.get("external_ids", {}).get("tvdb_id"))
            if (media.show.tmdb_data and media.show.tmdb_data.get("external_ids", {}).get("tvdb_id"))
            else None
        )
    return data


@router.get("")
async def get_history(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    type: str | None = Query(None),
    start_date: str | None = Query(None),
    end_date: str | None = Query(None),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    from datetime import datetime, time
    
    offset = (page - 1) * page_size

    base_query = (
        select(func.count())
        .select_from(WatchEvent)
        .join(Media, Media.id == WatchEvent.media_id)
        .where(WatchEvent.user_id == current_user.id)
        .where(WatchEvent.completed == True)
    )
    if type and type in ("movie", "episode"):
        base_query = base_query.where(Media.media_type == type)

    query = (
        select(WatchEvent, Media)
        .join(Media, Media.id == WatchEvent.media_id)
        .options(selectinload(WatchEvent.media).selectinload(Media.show))
        .where(WatchEvent.user_id == current_user.id)
        .where(WatchEvent.completed == True)
        .order_by(desc(WatchEvent.watched_at))
    )
    if type and type in ("movie", "episode"):
        query = query.where(Media.media_type == type)

    if start_date:
        try:
            start_dt = datetime.strptime(start_date, "%Y-%m-%d")
            base_query = base_query.where(WatchEvent.watched_at >= start_dt)
            query = query.where(WatchEvent.watched_at >= start_dt)
        except ValueError:
            pass

    if end_date:
        try:
            end_dt = datetime.combine(datetime.strptime(end_date, "%Y-%m-%d"), time(23, 59, 59, 999999))
            base_query = base_query.where(WatchEvent.watched_at <= end_dt)
            query = query.where(WatchEvent.watched_at <= end_dt)
        except ValueError:
            pass

    total_result = await db.execute(base_query)
    total_count = total_result.scalar_one()
    total_pages = max(1, (total_count + page_size - 1) // page_size)

    query = query.offset(offset).limit(page_size)

    result = await db.execute(query)
    rows = result.all()
    
    events = [format_event(e, m) for e, m in rows]
    if events:
        await enrich_with_state(db, current_user.id, [e["media"] for e in events], False)

    return {
        "page": page,
        "page_size": page_size,
        "total_results": total_count,
        "total_pages": total_pages,
        "results": events,
    }


@router.get("/now-playing")
async def get_now_playing(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Active playback sessions for the current user."""
    result = await db.execute(
        select(PlaybackSession, Media)
        .join(Media, Media.id == PlaybackSession.media_id)
        .options(selectinload(Media.show))
        .where(PlaybackSession.user_id == current_user.id)
        .order_by(desc(PlaybackSession.updated_at))
    )
    rows = result.all()
    sessions = []
    for session, media in rows:
        sessions.append({
            "session_key": session.session_key,
            "source": session.source,
            "state": session.state,
            "progress_percent": session.progress_percent,
            "progress_seconds": session.progress_seconds,
            "started_at": session.started_at.isoformat(),
            "updated_at": session.updated_at.isoformat(),
            "media": format_media(media),
        })
    return {"now_playing": sessions}


@router.delete("/sessions")
async def clear_now_playing_sessions(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Delete all active playback sessions for the current user."""
    await db.execute(
        delete(PlaybackSession).where(PlaybackSession.user_id == current_user.id)
    )
    await db.commit()
    return {"status": "ok"}


@router.get("/continue-watching")
async def get_continue_watching(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Items currently in progress."""
    # Step 0: Find dropped and blocked shows to exclude (URI-based)
    from models.blocklist import BlocklistItem
    dropped_q = await db.execute(
        select(BlocklistItem.uri_id)
        .where(BlocklistItem.user_id == current_user.id)
    )
    dropped_uris = {r[0] for r in dropped_q.all() if r[0]}

    base_query = (
        select(func.count(PlaybackProgress.id))
        .select_from(PlaybackProgress)
        .join(Media, Media.id == PlaybackProgress.media_id)
        .outerjoin(Show, Show.id == Media.show_id)
        .where(PlaybackProgress.user_id == current_user.id)
    )

    query = (
        select(PlaybackProgress, Media)
        .join(Media, Media.id == PlaybackProgress.media_id)
        .outerjoin(Show, Show.id == Media.show_id)
        .options(selectinload(PlaybackProgress.media).selectinload(Media.show))
        .where(PlaybackProgress.user_id == current_user.id)
    )

    if dropped_uris:
        exclude_filter = or_(
            Media.media_type != MediaType.episode,
            Show.uri_id.not_in(dropped_uris),
            Show.uri_id.is_(None),
        )
        base_query = base_query.where(exclude_filter)
        query = query.where(exclude_filter)

    total_result = await db.execute(base_query)
    total_count = total_result.scalar_one()
    total_pages = max(1, (total_count + page_size - 1) // page_size)

    offset = (page - 1) * page_size
    result = await db.execute(
        query
        .order_by(desc(PlaybackProgress.updated_at))
        .offset(offset)
        .limit(page_size)
    )
    rows = result.all()
    items = [format_event(e, m) for e, m in rows]
    if items:
        await enrich_with_state(db, current_user.id, [i["media"] for i in items], False)

    return {
        "page": page,
        "page_size": page_size,
        "total_results": total_count,
        "total_pages": total_pages,
        "continue_watching": items,
    }


@router.delete("/continue-watching")
async def delete_continue_watching(
    uri_id: Optional[str] = Query(None),
    media_type: Optional[str] = Query(None),
    media_id: Optional[int] = Query(None),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    if media_id is not None:
        await db.execute(
            delete(PlaybackProgress).where(
                PlaybackProgress.user_id == current_user.id,
                PlaybackProgress.media_id == media_id,
            )
        )
        await db.commit()
        return {"status": "success"}

    if uri_id is not None and media_type is not None:
        media_q = await db.execute(
            select(Media.id).where(Media.uri_id == uri_id, Media.media_type == media_type)
        )
        resolved_media_id = media_q.scalars().first()
        if not resolved_media_id:
            raise HTTPException(status_code=404, detail="Media not found")

        await db.execute(
            delete(PlaybackProgress).where(
                PlaybackProgress.user_id == current_user.id,
                PlaybackProgress.media_id == resolved_media_id
            )
        )
        await db.commit()
        return {"status": "success"}

    raise HTTPException(status_code=400, detail="Must provide either media_id or uri_id+media_type")


def _format_media_item(media: Media) -> dict:
    data = format_media(media)
    data["show_id"] = media.show_id
    if media.show:
        data["show_tmdb_id"] = media.show.tmdb_id
        data["show_tvdb_id"] = media.show.tvdb_id
    return data


@router.get("/next-up")
async def get_next_up(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
    limit: int | None = None,
    include_hidden: bool = Query(False),
):
    """Next unwatched episode for each show the user is actively watching, sorted by most recent activity."""
    # Step 0: Find dropped and blocked shows to exclude (URI-based)
    from models.blocklist import BlocklistItem
    dropped_q = await db.execute(
        select(BlocklistItem.uri_id)
        .where(BlocklistItem.user_id == current_user.id)
    )
    dropped_uris = {r[0] for r in dropped_q.all() if r[0]}

    # Step 1: Find the last watched / significantly-viewed episode per show
    query = (
        select(Media.show_id, Media.season_number, Media.episode_number, func.max(WatchEvent.watched_at).label("last_watched_at"))
        .join(WatchEvent, WatchEvent.media_id == Media.id)
        .join(Show, Show.id == Media.show_id)
        .where(
            WatchEvent.user_id == current_user.id,
            Media.media_type == MediaType.episode,
            Media.show_id.isnot(None),
            or_(WatchEvent.completed == True, WatchEvent.progress_percent >= 0.5),
        )
    )
    if dropped_uris and not include_hidden:
        query = query.where(
            or_(Show.uri_id.is_(None), Show.uri_id.not_in(dropped_uris))
        )

    result = await db.execute(
        query
        .group_by(Media.show_id, Media.season_number, Media.episode_number)
        .order_by(Media.show_id, desc(Media.season_number), desc(Media.episode_number))
    )
    rows = result.all()

    # Keep only the furthest episode per show and track most recent activity date
    last_per_show: dict[int, tuple[int, int]] = {}
    show_last_watched: dict[int, object] = {}  # show_id -> most recent watched_at
    for show_id, season, episode, last_watched_at in rows:
        if show_id not in last_per_show:
            last_per_show[show_id] = (season, episode)
        # Track the most recent watch date across all episodes for this show
        if show_id not in show_last_watched or (last_watched_at and last_watched_at > show_last_watched[show_id]):
            show_last_watched[show_id] = last_watched_at

    if not last_per_show:
        return {"next_up": []}

    # Step 2: Candidate next episodes (anything after the last watched one, per show)
    show_filters = [
        and_(
            Media.show_id == show_id,
            or_(
                Media.season_number > season,
                and_(Media.season_number == season, Media.episode_number > episode),
            ),
        )
        for show_id, (season, episode) in last_per_show.items()
    ]

    candidates_result = await db.execute(
        select(Media)
        .options(selectinload(Media.show))
        .where(Media.media_type == MediaType.episode, or_(*show_filters))
        .order_by(Media.show_id, Media.season_number, Media.episode_number)
    )
    candidates = candidates_result.scalars().all()

    # Take only the immediately next episode per show
    next_per_show: dict[int, Media] = {}
    for media in candidates:
        if media.show_id not in next_per_show:
            next_per_show[media.show_id] = media

    if not next_per_show:
        return {"next_up": []}

    # Remove episodes the user has already completed
    completed_result = await db.execute(
        select(WatchEvent.media_id)
        .where(
            WatchEvent.user_id == current_user.id,
            WatchEvent.completed == True,
            WatchEvent.media_id.in_([m.id for m in next_per_show.values()]),
        )
    )
    completed_ids = {row[0] for row in completed_result.all()}

    def is_hidden(m: Media) -> bool:
        if not m.show:
            return False
        # Calculate effective show URI matching the logic below
        show_uri = m.show.uri_id
        if not show_uri and m.show.tmdb_id:
            show_uri = f"tmdb:s:{m.show.tmdb_id}"
        if not show_uri and getattr(m.show, 'tvdb_id', None):
            show_uri = f"tvdb:s:{m.show.tvdb_id}"
        return bool(show_uri and show_uri in dropped_uris)

    next_up = [
        m for m in next_per_show.values()
        if m.id not in completed_ids
        and (include_hidden or not is_hidden(m))
    ]
    next_up.sort(
        key=lambda m: show_last_watched.get(m.show_id) or datetime.min,
        reverse=True,
    )
    if limit is not None:
        next_up = next_up[:limit]

    items = [_format_media_item(m) for m in next_up]
    for item in items:
        show_uri = (
            item.get("show_uri_id")
            or (f"tmdb:s:{item['show_tmdb_id']}" if item.get("show_tmdb_id") else None)
            or (f"tvdb:s:{item['show_tvdb_id']}" if item.get("show_tvdb_id") else None)
        )
        item["next_up_hidden"] = bool(show_uri and show_uri in dropped_uris)
    if items:
        await enrich_with_state(db, current_user.id, items, False)

    return {"next_up": items}


import schemas
from core import tmdb
from core.enrichment import enrich_media
from datetime import datetime
from fastapi import HTTPException
from pydantic import BaseModel


class SeasonWatchRequest(BaseModel):
    show_uri_id: str
    season_number: int


class ShowWatchRequest(BaseModel):
    show_uri_id: str


@router.post("", response_model=dict)
async def mark_as_watched(
    event_in: schemas.WatchEventCreate,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    # 1. Check if Media exists locally
    media = None
    if event_in.media_id:
        media = await db.get(Media, event_in.media_id)

    if not media and event_in.uri_id:
        query = select(Media).where(
            Media.uri_id == event_in.uri_id, Media.media_type == event_in.media_type
        )
        result = await db.execute(query)
        media = result.scalars().first()

    # 1b. Episode with show context — resolve/create the episode Media row from the
    # parent show + season + episode. Handles episodes not in the library yet,
    # for BOTH TMDB and TVDB shows (provider-agnostic, unlike the session helper).
    if not media and event_in.media_type == MediaType.episode and event_in.show_uri_id \
            and event_in.season_number is not None and event_in.episode_number is not None:
        from utils.media_uri import MediaURI
        from utils.alias_lookup import get_internal_id_for_uri
        show = None
        try:
            _suri = MediaURI.parse(event_in.show_uri_id)
            col = Show.tvdb_id if _suri.provider == "tvdb" else Show.tmdb_id
            show_q = await db.execute(select(Show).where(col == int(_suri.id)))
            show = show_q.scalar_one_or_none()
        except (ValueError, TypeError):
            show = None
        if show is None:
            internal_id = await get_internal_id_for_uri(db, event_in.show_uri_id)
            if internal_id is not None:
                show_q = await db.execute(select(Show).where(Show.id == internal_id))
                show = show_q.scalar_one_or_none()
        if show is not None:
            ep_q = await db.execute(
                select(Media).where(
                    Media.show_id == show.id,
                    Media.media_type == MediaType.episode,
                    Media.season_number == event_in.season_number,
                    Media.episode_number == event_in.episode_number,
                )
            )
            media = ep_q.scalars().first()
            if media is None:
                media = Media(
                    uri_id=event_in.uri_id,
                    media_type=MediaType.episode,
                    show_id=show.id,
                    season_number=event_in.season_number,
                    episode_number=event_in.episode_number,
                    title=f"Episode {event_in.episode_number}",
                )
                db.add(media)
                await db.flush()

    # 2. Create Media row from TMDB if URI is TMDB-based
    if not media:
        if not event_in.uri_id:
            raise HTTPException(status_code=400, detail="uri_id (or episode show context) required")
        from utils.media_uri import MediaURI
        try:
            uri = MediaURI.parse(event_in.uri_id)
        except (ValueError, TypeError):
            raise HTTPException(status_code=400, detail=f"Invalid uri_id: {event_in.uri_id!r}")
        if uri.provider != "tmdb":
            raise HTTPException(status_code=400, detail="Cannot create new media for non-TMDB URI; sync first")
        tmdb_id_int = int(uri.id)
        from routers.media import get_user_tmdb_key
        api_key = await get_user_tmdb_key(db, current_user.id)
        try:
            if event_in.media_type == MediaType.movie:
                data = await tmdb.get_movie(tmdb_id_int, api_key=api_key)
                title = data.get("title")
            else:
                data = await tmdb.get_show(tmdb_id_int, api_key=api_key)
                title = data.get("name")
            media = Media(
                tmdb_id=tmdb_id_int,
                uri_id=event_in.uri_id,
                media_type=event_in.media_type,
                title=title,
            )
            db.add(media)
            await db.flush()
            await enrich_media(media, api_key=api_key)
        except Exception as e:
            raise HTTPException(status_code=404, detail=f"TMDB Media not found: {e}")

    # 3. Create WatchEvent
    event = WatchEvent(
        user_id=current_user.id,
        media_id=media.id,
        watched_at=(event_in.watched_at.replace(tzinfo=None) if event_in.watched_at else datetime.utcnow()),
        completed=event_in.completed,
        play_count=1,
        progress_percent=1.0 if event_in.completed else 0.0,
    )
    db.add(event)
    if event_in.completed:
        await db.execute(
            delete(PlaybackProgress).where(
                PlaybackProgress.user_id == current_user.id,
                PlaybackProgress.media_id == media.id,
            )
        )
    await db.commit()

    # 4. Push to media servers if outbound push is enabled
    if event_in.completed:
        await _push_watch_state(db, current_user.id, [media.id], watched=True)

    return {"status": "ok", "message": f"Marked {media.title} as watched"}


@router.get("/item-events")
async def get_item_events(
    media_type: MediaType = Query(...),
    uri_id: str = Query(...),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    media_q = await db.execute(select(Media).where(Media.uri_id == uri_id, Media.media_type == media_type))
    media = media_q.scalar_one_or_none()
    if not media:
        return []

    query = (
        select(WatchEvent)
        .join(Media, Media.id == WatchEvent.media_id)
        .where(
            WatchEvent.user_id == current_user.id,
            WatchEvent.completed == True,
            Media.id == media.id,
        )
        .order_by(desc(WatchEvent.watched_at))
    )
    result = await db.execute(query)
    events = result.scalars().all()
    return [{"id": e.id, "watched_at": e.watched_at.isoformat()} for e in events]


@router.delete("/event/{event_id}")
async def delete_single_event(
    event_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Delete a single watch event by its ID."""
    result = await db.execute(
        select(WatchEvent).where(
            WatchEvent.id == event_id,
            WatchEvent.user_id == current_user.id,
        )
    )
    event = result.scalar_one_or_none()
    if not event:
        raise HTTPException(status_code=404, detail="Event not found")

    media_id = event.media_id
    await db.execute(
        delete(WatchEvent).where(
            WatchEvent.id == event_id,
            WatchEvent.user_id == current_user.id,
        )
    )
    await db.commit()

    # Only push "unwatched" to connected services if no events remain for this media
    remaining = await db.execute(
        select(func.count()).where(
            WatchEvent.user_id == current_user.id,
            WatchEvent.media_id == media_id,
        )
    )
    if remaining.scalar() == 0:
        await _push_watch_state(db, current_user.id, [media_id], watched=False)

    return {"status": "ok"}


@router.delete("")
async def clear_history(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    await db.execute(delete(WatchEvent).where(WatchEvent.user_id == current_user.id))
    await db.commit()
    return {"status": "ok", "message": "Watch history cleared"}


@router.delete("/item")
async def unwatch_item(
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
            select(Media.id).where(Media.uri_id == uri_id, Media.media_type == media_type)
        )
        media_ids = list(media_q.scalars().all())
    else:
        media_ids = [media_id]
    
    if not media_ids:
        return {"status": "ok", "count": 0}

    await db.execute(
        delete(WatchEvent).where(
            WatchEvent.user_id == current_user.id,
            WatchEvent.media_id.in_(media_ids),
        )
    )
    await db.commit()
    await _push_watch_state(db, current_user.id, media_ids, watched=False)
    return {"status": "ok"}


@router.get("/item/events")
async def get_item_watch_events(
    media_type: MediaType = Query(...),
    uri_id: Optional[str] = Query(None),
    id: Optional[int] = Query(None, alias="id"),
    show_uri_id: Optional[str] = Query(None),
    season_number: Optional[int] = Query(None),
    episode_number: Optional[int] = Query(None),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    if id is not None:
        media_ids = [id]
    elif uri_id:
        media_q = await db.execute(
            select(Media.id).where(Media.uri_id == uri_id, Media.media_type == media_type)
        )
        media_ids = list(media_q.scalars().all())
    elif media_type == MediaType.episode and show_uri_id and season_number is not None and episode_number is not None:
        # Resolve the episode via its parent show + season + episode.
        # Mirror mark_as_watched: try the Show provider-id column first, then the alias table.
        # (TVDB shows are often not in media_aliases, so alias-only lookup misses them.)
        from utils.media_uri import MediaURI
        from utils.alias_lookup import get_internal_id_for_uri
        show_internal_id = None
        try:
            _suri = MediaURI.parse(show_uri_id)
            col = Show.tvdb_id if _suri.provider == "tvdb" else Show.tmdb_id
            sid_q = await db.execute(select(Show.id).where(col == int(_suri.id)))
            show_internal_id = sid_q.scalar_one_or_none()
        except (ValueError, TypeError):
            show_internal_id = None
        if show_internal_id is None:
            show_internal_id = await get_internal_id_for_uri(db, show_uri_id)
        if show_internal_id is None:
            return {"events": []}
        ep_q = await db.execute(
            select(Media.id).where(
                Media.show_id == show_internal_id,
                Media.media_type == MediaType.episode,
                Media.season_number == season_number,
                Media.episode_number == episode_number,
            )
        )
        media_ids = list(ep_q.scalars().all())
    else:
        # No usable identifier — nothing logged yet. Return empty so the modal opens.
        return {"events": []}

    if not media_ids:
        return {"events": []}

    events_q = await db.execute(
        select(WatchEvent)
        .where(WatchEvent.user_id == current_user.id, WatchEvent.media_id.in_(media_ids))
        .order_by(desc(WatchEvent.watched_at))
    )
    events = events_q.scalars().all()
    
    return {
        "events": [
            {
                "id": e.id,
                "watched_at": e.watched_at.isoformat(),
                "progress_seconds": e.progress_seconds,
                "progress_percent": e.progress_percent,
                "completed": e.completed,
                "play_count": e.play_count,
            }
            for e in events
        ]
    }



@router.delete("/event/{event_id}")
async def delete_watch_event(
    event_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Delete a specific watch event by ID."""
    from fastapi import HTTPException
    event_q = await db.execute(
        select(WatchEvent).where(WatchEvent.id == event_id, WatchEvent.user_id == current_user.id)
    )
    event = event_q.scalar_one_or_none()
    if not event:
        raise HTTPException(status_code=404, detail="Watch event not found")

    media_id = event.media_id
    
    await db.delete(event)
    await db.commit()

    # Check if any watch events remain for this media item
    remaining_q = await db.execute(
        select(func.count(WatchEvent.id))
        .where(WatchEvent.user_id == current_user.id, WatchEvent.media_id == media_id)
    )
    remaining_count = remaining_q.scalar() or 0

    if remaining_count == 0:
        await _push_watch_state(db, current_user.id, [media_id], watched=False)

    return {"status": "ok", "remaining_count": remaining_count}


@router.post("/season")
async def mark_season_watched(
    body: SeasonWatchRequest,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Mark all aired episodes of a season as watched, fetching from TMDB if needed."""
    from utils.media_uri import MediaURI
    try:
        uri = MediaURI.parse(body.show_uri_id)
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Invalid show_uri_id: {body.show_uri_id!r}")

    col = Show.tvdb_id if uri.provider == "tvdb" else Show.tmdb_id
    show_q = await db.execute(select(Show).where(col == int(uri.id)))
    show = show_q.scalar_one_or_none()

    # Effective TMDB ID for downstream TMDB API calls (TVDB-only shows need tmdb fallback via show row)
    effective_tmdb_id = (show.tmdb_id if show else None) or (int(uri.id) if uri.provider == "tmdb" else None)

    api_key = await get_user_tmdb_key(db, current_user.id)
    if not show:
        if not check_tmdb_key(api_key) or not effective_tmdb_id:
            raise HTTPException(status_code=404, detail="Show not found and TMDB key not configured")
        data = await tmdb.get_show(effective_tmdb_id, api_key=api_key)
        show = Show(
            tmdb_id=effective_tmdb_id,
            uri_id=f"tmdb:s:{effective_tmdb_id}",
            title=data.get("name") or "Unknown",
            poster_path=tmdb.poster_url(data.get("poster_path")),
            backdrop_path=tmdb.poster_url(data.get("backdrop_path"), size="w1280"),
            tmdb_rating=data.get("vote_average"),
            status=data.get("status"),
            first_air_date=data.get("first_air_date"),
            tmdb_data={
                "genres": [g["name"] for g in data.get("genres", [])],
                "seasons": [
                    {
                        "season_number": s["season_number"],
                        "episode_count": s["episode_count"],
                        "name": s["name"],
                    } for s in data.get("seasons", [])
                ]
            }
        )
        db.add(show)
        await db.flush()

    # Try alias lookup for TMDB cross-ref if show has no tmdb_id
    if not effective_tmdb_id and show and show.uri_id:
        from utils.alias_lookup import get_provider_id_for_uri
        alias = await get_provider_id_for_uri(db, show.uri_id, "tmdb")
        effective_tmdb_id = int(alias) if alias else None

    # 2. Fetch season episodes from TMDB to ensure we know about all of them
    if not effective_tmdb_id:
        raise HTTPException(status_code=400, detail="Cannot mark season watched without TMDB ID or alias")
    try:
        season_data = await tmdb.get_season(effective_tmdb_id, body.season_number, api_key=api_key)
    except Exception as e:
        raise HTTPException(status_code=404, detail=f"Season not found: {e}")

    # 3. Ensure Media rows exist for all aired episodes in this season
    now = datetime.now()
    today = now.date()
    
    # Get existing episodes for this season
    existing_q = await db.execute(
        select(Media).where(
            Media.show_id == show.id,
            Media.media_type == MediaType.episode,
            Media.season_number == body.season_number
        )
    )
    existing_map = {m.episode_number: m for m in existing_q.scalars().all()}
    
    all_season_episodes = []
    for ep in season_data.get("episodes", []):
        air_date_str = ep.get("air_date")
        if not air_date_str: continue
        try:
            air_date = datetime.strptime(air_date_str, "%Y-%m-%d").date()
            if air_date > today: continue # Skip unaired
        except Exception: continue
        
        ep_num = ep["episode_number"]
        if ep_num in existing_map:
            all_season_episodes.append(existing_map[ep_num])
        else:
            new_ep = Media(
                show_id=show.id,
                tmdb_id=ep["id"],
                uri_id=f"tmdb:e:{ep['id']}" if ep.get("id") else None,
                media_type=MediaType.episode,
                title=ep.get("name") or f"Episode {ep_num}",
                season_number=body.season_number,
                episode_number=ep_num,
                poster_path=tmdb.poster_url(ep.get("still_path"), size="w500"),
                release_date=air_date_str,
                tmdb_rating=ep.get("vote_average"),
            )
            db.add(new_ep)
            all_season_episodes.append(new_ep)
    
    await db.flush() # Get IDs for new episodes
    
    # 4. Mark all as watched
    if not all_season_episodes:
        return {"status": "ok", "count": 0}

    already_q = await db.execute(
        select(WatchEvent.media_id).where(
            WatchEvent.user_id == current_user.id,
            WatchEvent.media_id.in_([ep.id for ep in all_season_episodes]),
            WatchEvent.completed == True
        )
    )
    already_watched = {r[0] for r in already_q.all()}
    
    newly_watched = []
    for ep in all_season_episodes:
        if ep.id not in already_watched:
            db.add(WatchEvent(
                user_id=current_user.id,
                media_id=ep.id,
                watched_at=now,
                completed=True,
                play_count=1,
                progress_percent=1.0,
            ))
            newly_watched.append(ep.id)
            
    if newly_watched:
        await db.execute(
            delete(PlaybackProgress).where(
                PlaybackProgress.user_id == current_user.id,
                PlaybackProgress.media_id.in_(newly_watched),
            )
        )
    await db.commit()
    await _push_watch_state(db, current_user.id, newly_watched, watched=True)
    return {"status": "ok", "count": len(newly_watched)}


@router.delete("/season")
async def unwatch_season(
    season_number: int = Query(...),
    show_uri_id: str = Query(...),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Remove all watch events for a season."""
    from utils.media_uri import MediaURI
    try:
        uri = MediaURI.parse(show_uri_id)
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Invalid show_uri_id: {show_uri_id!r}")
    col = Show.tvdb_id if uri.provider == "tvdb" else Show.tmdb_id
    show_q = await db.execute(select(Show).where(col == int(uri.id)))
    show = show_q.scalar_one_or_none()
    if not show:
        return {"status": "ok", "count": 0}

    episodes_q = await db.execute(
        select(Media.id).where(
            Media.show_id == show.id,
            Media.media_type == MediaType.episode,
            Media.season_number == season_number,
        )
    )
    episode_ids = [r[0] for r in episodes_q.all()]
    if not episode_ids:
        return {"status": "ok", "count": 0}

    result = await db.execute(
        delete(WatchEvent).where(
            WatchEvent.user_id == current_user.id,
            WatchEvent.media_id.in_(episode_ids),
        )
    )
    await db.commit()
    await _push_watch_state(db, current_user.id, episode_ids, watched=False)
    return {"status": "ok", "count": result.rowcount}


@router.post("/show-all")
async def mark_show_watched(
    body: ShowWatchRequest,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Mark all aired episodes of all seasons as watched."""
    from utils.media_uri import MediaURI
    try:
        uri = MediaURI.parse(body.show_uri_id)
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Invalid show_uri_id: {body.show_uri_id!r}")

    col = Show.tvdb_id if uri.provider == "tvdb" else Show.tmdb_id
    show_q = await db.execute(select(Show).where(col == int(uri.id)))
    show = show_q.scalar_one_or_none()

    series_tmdb_id: int | None = int(uri.id) if uri.provider == "tmdb" else (show.tmdb_id if show else None)
    if not series_tmdb_id and show and show.uri_id:
        from utils.alias_lookup import get_provider_id_for_uri
        alias = await get_provider_id_for_uri(db, show.uri_id, "tmdb")
        series_tmdb_id = int(alias) if alias else None

    api_key = await get_user_tmdb_key(db, current_user.id)
    if not show:
        if series_tmdb_id is None or not check_tmdb_key(api_key):
            raise HTTPException(status_code=404, detail="Show not found and TMDB key not configured")
        data = await tmdb.get_show(series_tmdb_id, api_key=api_key)
        show = Show(
            tmdb_id=series_tmdb_id,
            uri_id=f"tmdb:s:{series_tmdb_id}",
            title=data.get("name") or "Unknown",
            poster_path=tmdb.poster_url(data.get("poster_path")),
            backdrop_path=tmdb.poster_url(data.get("backdrop_path"), size="w1280"),
            tmdb_rating=data.get("vote_average"),
            status=data.get("status"),
            first_air_date=data.get("first_air_date"),
            tmdb_data={
                "genres": [g["name"] for g in data.get("genres", [])],
                "seasons": [
                    {
                        "season_number": s["season_number"],
                        "episode_count": s["episode_count"],
                        "name": s["name"],
                    } for s in data.get("seasons", [])
                ]
            }
        )
        db.add(show)
        await db.flush()
    else:
        # We need TMDB data for season/episode counts
        if not show.tmdb_data or "seasons" not in show.tmdb_data:
            data = await tmdb.get_show(show.tmdb_id or series_tmdb_id, api_key=api_key)
            show.tmdb_data = {
                "genres": [g["name"] for g in data.get("genres", [])],
                "seasons": [
                    {
                        "season_number": s["season_number"],
                        "episode_count": s["episode_count"],
                        "name": s["name"],
                    } for s in data.get("seasons", [])
                ]
            }
            await db.flush()

    # 2. For each season, fetch episodes and ensure they exist + mark watched
    seasons = [s["season_number"] for s in show.tmdb_data["seasons"] if s["season_number"] > 0]
    all_newly_watched_ids = []
    
    now = datetime.now()
    today = now.date()

    for sn in seasons:
        try:
            season_data = await tmdb.get_season(show.tmdb_id or series_tmdb_id, sn, api_key=api_key)
        except Exception: continue # Skip failed seasons

        existing_q = await db.execute(
            select(Media).where(
                Media.show_id == show.id,
                Media.media_type == MediaType.episode,
                Media.season_number == sn
            )
        )
        existing_map = {m.episode_number: m for m in existing_q.scalars().all()}
        
        season_eps_to_watch = []
        for ep in season_data.get("episodes", []):
            air_date_str = ep.get("air_date")
            if not air_date_str: continue
            try:
                air_date = datetime.strptime(air_date_str, "%Y-%m-%d").date()
                if air_date > today: continue
            except Exception: continue
            
            ep_num = ep["episode_number"]
            if ep_num in existing_map:
                season_eps_to_watch.append(existing_map[ep_num])
            else:
                new_ep = Media(
                    show_id=show.id,
                    tmdb_id=ep["id"],
                    uri_id=f"tmdb:e:{ep['id']}" if ep.get("id") else None,
                    media_type=MediaType.episode,
                    title=ep.get("name") or f"Episode {ep_num}",
                    season_number=sn,
                    episode_number=ep_num,
                    poster_path=tmdb.poster_url(ep.get("still_path"), size="w500"),
                    release_date=air_date_str,
                    tmdb_rating=ep.get("vote_average"),
                )
                db.add(new_ep)
                season_eps_to_watch.append(new_ep)
        
        await db.flush()
        
        if not season_eps_to_watch: continue

        already_q = await db.execute(
            select(WatchEvent.media_id).where(
                WatchEvent.user_id == current_user.id,
                WatchEvent.media_id.in_([ep.id for ep in season_eps_to_watch]),
                WatchEvent.completed == True
            )
        )
        already_watched = {r[0] for r in already_q.all()}
        
        for ep in season_eps_to_watch:
            if ep.id not in already_watched:
                db.add(WatchEvent(
                    user_id=current_user.id,
                    media_id=ep.id,
                    watched_at=now,
                    completed=True,
                    play_count=1,
                    progress_percent=1.0,
                ))
                all_newly_watched_ids.append(ep.id)

    if all_newly_watched_ids:
        await db.execute(
            delete(PlaybackProgress).where(
                PlaybackProgress.user_id == current_user.id,
                PlaybackProgress.media_id.in_(all_newly_watched_ids),
            )
        )
    await db.commit()
    await _push_watch_state(db, current_user.id, all_newly_watched_ids, watched=True)
    return {"status": "ok", "count": len(all_newly_watched_ids)}


@router.delete("/show-all")
async def unwatch_show(
    show_uri_id: str = Query(...),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Remove all watch events for all episodes of a show."""
    from utils.media_uri import MediaURI
    try:
        uri = MediaURI.parse(show_uri_id)
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Invalid show_uri_id: {show_uri_id!r}")
    col = Show.tvdb_id if uri.provider == "tvdb" else Show.tmdb_id
    show_q = await db.execute(select(Show).where(col == int(uri.id)))
    show = show_q.scalar_one_or_none()
    if not show:
        return {"status": "ok", "count": 0}

    episodes_q = await db.execute(
        select(Media.id).where(
            Media.show_id == show.id,
            Media.media_type == MediaType.episode,
        )
    )
    episode_ids = [r[0] for r in episodes_q.all()]
    if not episode_ids:
        return {"status": "ok", "count": 0}

    result = await db.execute(
        delete(WatchEvent).where(
            WatchEvent.user_id == current_user.id,
            WatchEvent.media_id.in_(episode_ids),
        )
    )
    await db.commit()
    await _push_watch_state(db, current_user.id, episode_ids, watched=False)
    return {"status": "ok", "count": result.rowcount}


@router.delete("/now-playing")
async def clear_all_now_playing_sessions(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Force-clear all now-playing sessions for the current user."""
    await db.execute(
        delete(PlaybackSession).where(PlaybackSession.user_id == current_user.id)
    )
    await db.commit()
    return {"status": "ok"}


# ---------------------------------------------------------------------------
# Manual scrobble session endpoints
# ---------------------------------------------------------------------------

async def _get_or_create_media_for_session(
    db: AsyncSession,
    body: schemas.ManualSessionStart,
    user_id: int,
) -> Media:
    if body.media_id:
        result = await db.execute(select(Media).where(Media.id == body.media_id))
        media = result.scalar_one_or_none()
        if media:
            return media

    result = await db.execute(
        select(Media).where(Media.uri_id == body.uri_id, Media.media_type == body.media_type)
    )
    media = result.scalar_one_or_none()
    if media:
        return media

    from utils.media_uri import MediaURI
    try:
        uri = MediaURI.parse(body.uri_id)
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Invalid uri_id: {body.uri_id!r}")

    if uri.provider != "tmdb":
        raise HTTPException(status_code=400, detail="Only TMDB URIs supported for new session media; sync first")

    tmdb_id_int = int(uri.id)
    api_key = await get_user_tmdb_key(db, user_id)

    if body.media_type == MediaType.movie:
        if not check_tmdb_key(api_key):
            raise HTTPException(status_code=404, detail="Movie not in library and TMDB key not configured")
        try:
            data = await tmdb.get_movie(tmdb_id_int, api_key=api_key)
            title = data.get("title") or body.title or "Unknown"
        except Exception:
            title = body.title or "Unknown"
        media = Media(
            tmdb_id=tmdb_id_int,
            uri_id=body.uri_id,
            media_type=body.media_type,
            title=title,
        )
        db.add(media)
        await db.flush()
        try:
            await enrich_media(media, api_key=api_key)
        except Exception:
            pass
    else:
        media = Media(
            tmdb_id=tmdb_id_int,
            uri_id=body.uri_id,
            media_type=body.media_type,
            title=body.title or "Unknown",
            runtime=body.runtime,
            season_number=body.season_number,
            episode_number=body.episode_number,
        )
        if body.show_uri_id:
            try:
                _suri = MediaURI.parse(body.show_uri_id)
                if _suri.provider == "tvdb":
                    show_q = await db.execute(select(Show).where(Show.tvdb_id == int(_suri.id)))
                else:
                    show_q = await db.execute(select(Show).where(Show.tmdb_id == int(_suri.id)))
                show = show_q.scalar_one_or_none()
                if show:
                    media.show_id = show.id
            except ValueError:
                pass
        db.add(media)
        await db.flush()

    return media


@router.post("/session/start")
async def start_manual_session(
    body: schemas.ManualSessionStart,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Start a manual scrobble session for any movie or episode."""
    media = await _get_or_create_media_for_session(db, body, current_user.id)

    if media.runtime is None and body.runtime:
        media.runtime = body.runtime

    session_key = f"manual-{current_user.id}-{media.id}"

    await db.execute(delete(PlaybackSession).where(PlaybackSession.session_key == session_key))
    session = PlaybackSession(
        user_id=current_user.id,
        media_id=media.id,
        session_key=session_key,
        source="manual",
        state="playing",
        progress_seconds=0,
        progress_percent=0.0,
    )
    db.add(session)
    await db.commit()

    return {"session_key": session_key, "media_id": media.id, "runtime": media.runtime}


@router.patch("/session/{session_key}")
async def update_manual_session(
    session_key: str,
    body: schemas.ManualSessionUpdate,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Heartbeat / pause / resume for a manual session."""
    result = await db.execute(
        select(PlaybackSession).where(
            PlaybackSession.session_key == session_key,
            PlaybackSession.user_id == current_user.id,
        )
    )
    session = result.scalar_one_or_none()
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")

    media_q = await db.execute(select(Media).where(Media.id == session.media_id))
    media = media_q.scalar_one_or_none()

    runtime_seconds = (media.runtime * 60) if (media and media.runtime) else 0
    progress_pct = (body.progress_seconds / runtime_seconds) if runtime_seconds > 0 else 0.0
    progress_pct = min(1.0, max(0.0, progress_pct))

    session.progress_seconds = body.progress_seconds
    session.progress_percent = progress_pct
    if body.state in ("playing", "paused"):
        session.state = body.state
    session.updated_at = datetime.utcnow()

    if 0.05 <= progress_pct < 0.90:
        prog_q = await db.execute(
            select(PlaybackProgress).where(
                PlaybackProgress.user_id == current_user.id,
                PlaybackProgress.media_id == session.media_id,
            )
        )
        prog = prog_q.scalar_one_or_none()
        if prog:
            prog.progress_seconds = body.progress_seconds
            prog.progress_percent = progress_pct
        else:
            db.add(PlaybackProgress(
                user_id=current_user.id,
                media_id=session.media_id,
                progress_seconds=body.progress_seconds,
                progress_percent=progress_pct,
            ))
    await db.commit()
    return {"status": "ok"}


@router.delete("/now-playing/{session_key}")
async def delete_now_playing_session(
    session_key: str,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Force-clear a specific 'stuck' now-playing session."""
    await db.execute(
        delete(PlaybackSession).where(
            PlaybackSession.session_key == session_key,
            PlaybackSession.user_id == current_user.id
        )
    )
    await db.commit()
    return {"status": "ok"}


@router.delete("/session/{session_key}")
async def stop_manual_session(
    session_key: str,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Stop and discard a manual session without marking as watched."""
    result = await db.execute(
        select(PlaybackSession).where(
            PlaybackSession.session_key == session_key,
            PlaybackSession.user_id == current_user.id,
        )
    )
    session = result.scalar_one_or_none()
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")

    media_id = session.media_id
    await db.execute(delete(PlaybackSession).where(PlaybackSession.session_key == session_key))
    await db.execute(
        delete(PlaybackProgress).where(
            PlaybackProgress.user_id == current_user.id,
            PlaybackProgress.media_id == media_id,
        )
    )
    await db.commit()
    return {"status": "ok"}
@router.post("/session/{session_key}/complete")
async def complete_manual_session(
    session_key: str,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Mark as fully watched and end the session."""
    result = await db.execute(
        select(PlaybackSession).where(
            PlaybackSession.session_key == session_key,
            PlaybackSession.user_id == current_user.id,
        )
    )
    session = result.scalar_one_or_none()
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")

    media_id = session.media_id
    await db.execute(delete(PlaybackSession).where(PlaybackSession.session_key == session_key))
    await db.execute(
        delete(PlaybackProgress).where(
            PlaybackProgress.user_id == current_user.id,
            PlaybackProgress.media_id == media_id,
        )
    )

    db.add(WatchEvent(
        user_id=current_user.id,
        media_id=media_id,
        watched_at=datetime.now(),
        completed=True,
        play_count=1,
        progress_percent=1.0,
    ))
    await db.commit()

    await _push_watch_state(db, current_user.id, [media_id], watched=True)
    return {"status": "ok"}
