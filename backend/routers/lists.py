import logging
from fastapi import APIRouter, Depends, HTTPException, Response
from datetime import datetime
from pydantic import BaseModel
from typing import Optional
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func
from sqlalchemy.orm import selectinload

from db import get_db
from models.lists import List as ListModel, ListItem
from models.media import Media
from models.base import MediaType, PrivacyLevel
from models.show import Show as ShowModel
from models.users import UserSettings
from dependencies import get_current_user
from models.users import User
from routers.media import enrich_with_state
from core.config import settings as app_settings
from xml.sax.saxutils import escape

logger = logging.getLogger(__name__)

router = APIRouter()


class ListCreate(BaseModel):
    name: str
    description: Optional[str] = None
    privacy_level: PrivacyLevel = PrivacyLevel.private
    
    # Radarr integration
    radarr_auto_add: bool = False
    radarr_root_folder: Optional[str] = None
    radarr_quality_profile: Optional[int] = None
    radarr_tags: Optional[list[int]] = None
    radarr_monitor: Optional[str] = None

    # Sonarr integration
    sonarr_auto_add: bool = False
    sonarr_root_folder: Optional[str] = None
    sonarr_quality_profile: Optional[int] = None
    sonarr_tags: Optional[list[int]] = None
    sonarr_series_type: Optional[str] = None
    sonarr_season_folder: bool = True
    sonarr_monitor: Optional[str] = None


class ListUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    privacy_level: Optional[PrivacyLevel] = None
    
    # Radarr integration
    radarr_auto_add: Optional[bool] = None
    radarr_root_folder: Optional[str] = None
    radarr_quality_profile: Optional[int] = None
    radarr_tags: Optional[list[int]] = None
    radarr_monitor: Optional[str] = None

    # Sonarr integration
    sonarr_auto_add: Optional[bool] = None
    sonarr_root_folder: Optional[str] = None
    sonarr_quality_profile: Optional[int] = None
    sonarr_tags: Optional[list[int]] = None
    sonarr_series_type: Optional[str] = None
    sonarr_season_folder: Optional[bool] = None
    sonarr_monitor: Optional[str] = None


class ListItemAdd(BaseModel):
    uri_id: str
    media_type: MediaType
    show_uri_id: Optional[str] = None
    season_number: Optional[int] = None
    episode_number: Optional[int] = None


def _format_list(lst: ListModel) -> dict:
    preview_posters: list[dict] = []
    # Only attempt to get posters if items relationship is loaded and not empty
    if "items" in lst.__dict__ and lst.items:
        for item in sorted(lst.items, key=lambda x: (x.sort_order, x.added_at)):
            if len(preview_posters) >= 3:
                break
            try:
                # Safer check for media relationship
                if "media" in item.__dict__:
                    poster = item.media.poster_path
                    if not poster and "show" in item.media.__dict__ and item.media.show:
                        poster = item.media.show.poster_path
                    if poster:
                        preview_posters.append({"url": poster, "adult": item.media.adult})
            except Exception:
                pass
    return {
        "id": lst.id,
        "name": lst.name,
        "description": lst.description,
        "privacy_level": lst.privacy_level,
        "item_count": len(lst.items) if "items" in lst.__dict__ else 0,
        "created_at": lst.created_at.isoformat() if lst.created_at else datetime.now().isoformat(),
        "updated_at": lst.updated_at.isoformat() if lst.updated_at else datetime.now().isoformat(),
        "preview_posters": preview_posters,
        
        "radarr_auto_add": lst.radarr_auto_add,
        "radarr_root_folder": lst.radarr_root_folder,
        "radarr_quality_profile": lst.radarr_quality_profile,
        "radarr_tags": lst.radarr_tags,
        "radarr_monitor": lst.radarr_monitor,
        
        "sonarr_auto_add": lst.sonarr_auto_add,
        "sonarr_root_folder": lst.sonarr_root_folder,
        "sonarr_quality_profile": lst.sonarr_quality_profile,
        "sonarr_tags": lst.sonarr_tags,
        "sonarr_series_type": lst.sonarr_series_type,
        "sonarr_season_folder": lst.sonarr_season_folder,
        "sonarr_monitor": lst.sonarr_monitor,
    }


def _format_item(item: ListItem) -> dict:
    media = item.media
    data: dict = {
        "id": item.id,
        "list_id": item.list_id,
        "added_at": item.added_at.isoformat(),
        "sort_order": item.sort_order,
        "notes": item.notes,
        "media": {
            "id": media.id,
            "uri_id": media.uri_id,
            "tmdb_id": media.tmdb_id,
            "type": media.media_type,
            "title": media.title,
            "poster_path": media.poster_path,
            "backdrop_path": media.backdrop_path,
            "release_date": media.release_date,
            "tmdb_rating": media.tmdb_rating,
            "season_number": media.season_number,
            "episode_number": media.episode_number,
            "adult": media.adult,
            "library": None,
            "in_library": False,
        },
    }
    if media.media_type == MediaType.episode and media.show:
        data["media"]["show_title"] = media.show.title
        data["media"]["show_uri_id"] = media.show.uri_id
        data["media"]["show_poster_path"] = media.show.poster_path
        data["media"]["show_tmdb_id"] = media.show.tmdb_id
    return data


@router.get("/public")
async def get_public_lists(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    result = await db.execute(
        select(ListModel, User.username)
        .join(User, User.id == ListModel.user_id)
        .options(selectinload(ListModel.items).selectinload(ListItem.media).selectinload(Media.show))
        .where(ListModel.privacy_level == PrivacyLevel.public, ListModel.user_id != current_user.id)
        .order_by(func.random())
        .limit(3)
    )
    rows = result.all()
    return {
        "lists": [
            {
                **_format_list(lst),
                "username": username,
            }
            for lst, username in rows
        ]
    }


@router.get("")
async def get_lists(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    result = await db.execute(
        select(ListModel)
        .options(selectinload(ListModel.items).selectinload(ListItem.media).selectinload(Media.show))
        .where(ListModel.user_id == current_user.id)
        .order_by(ListModel.updated_at.desc())
    )
    lists = result.scalars().all()
    return {"lists": [_format_list(lst) for lst in lists]}


@router.post("", status_code=201)
async def create_list(
    body: ListCreate,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    lst = ListModel(
        user_id=current_user.id,
        name=body.name,
        description=body.description,
        privacy_level=body.privacy_level,
        radarr_auto_add=body.radarr_auto_add,
        radarr_root_folder=body.radarr_root_folder,
        radarr_quality_profile=body.radarr_quality_profile,
        radarr_tags=body.radarr_tags,
        radarr_monitor=body.radarr_monitor,
        sonarr_auto_add=body.sonarr_auto_add,
        sonarr_root_folder=body.sonarr_root_folder,
        sonarr_quality_profile=body.sonarr_quality_profile,
        sonarr_tags=body.sonarr_tags,
        sonarr_series_type=body.sonarr_series_type,
        sonarr_season_folder=body.sonarr_season_folder,
        sonarr_monitor=body.sonarr_monitor,
    )
    db.add(lst)
    await db.commit()
    await db.refresh(lst)
    return _format_list(lst)


@router.get("/{list_id}")
async def get_list(
    list_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    from sqlalchemy import and_, or_, exists, extract, cast as sa_cast, Text
    from models.events import WatchEvent
    

    result = await db.execute(
        select(ListModel)
        .options(
            selectinload(ListModel.items)
            .selectinload(ListItem.media)
            .selectinload(Media.show)
        )
        .where(ListModel.id == list_id)
    )
    lst = result.scalar_one_or_none()
    if not lst:
        raise HTTPException(status_code=404, detail="List not found")
    if lst.user_id != current_user.id and lst.privacy_level == PrivacyLevel.private:
        raise HTTPException(status_code=403, detail="Access denied")

    items_sorted = sorted(lst.items, key=lambda x: (x.sort_order, x.added_at))
    formatted_items = [_format_item(i) for i in items_sorted]

    # Fill in missing poster/release_date for series items from the Show table
    series_items_needing_data = [
        item for item in formatted_items
        if item["media"].get("type") in (MediaType.series, "series")
        and (not item["media"].get("poster_path") or not item["media"].get("release_date"))
    ]
    if series_items_needing_data:
        series_tmdb_ids = [m["media"]["tmdb_id"] for m in series_items_needing_data if m["media"].get("tmdb_id")]
        series_uri_ids = [
            m["media"]["uri_id"] for m in series_items_needing_data
            if not m["media"].get("tmdb_id") and m["media"].get("uri_id")
        ]
        show_map: dict = {}
        if series_tmdb_ids:
            shows_result = await db.execute(
                select(ShowModel).where(ShowModel.tmdb_id.in_(series_tmdb_ids))
            )
            for s in shows_result.scalars().all():
                show_map[("tmdb", s.tmdb_id)] = s
        if series_uri_ids:
            shows_result2 = await db.execute(
                select(ShowModel).where(ShowModel.uri_id.in_(series_uri_ids))
            )
            for s in shows_result2.scalars().all():
                show_map[("uri", s.uri_id)] = s
        for item in formatted_items:
            m = item["media"]
            if m.get("type") not in (MediaType.series, "series"):
                continue
            show = show_map.get(("tmdb", m.get("tmdb_id"))) or show_map.get(("uri", m.get("uri_id")))
            if show:
                if not m.get("poster_path") and show.poster_path:
                    m["poster_path"] = show.poster_path
                if not m.get("release_date") and show.first_air_date:
                    m["release_date"] = show.first_air_date
                if not m.get("title") and show.title:
                    m["title"] = show.title

    media_dicts = [item["media"] for item in formatted_items]
    await enrich_with_state(db, current_user.id, media_dicts, False)

    return {
        **_format_list(lst),
        "items": formatted_items,
        "is_owner": lst.user_id == current_user.id,
    }


@router.patch("/{list_id}")
async def update_list(
    list_id: int,
    body: ListUpdate,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    result = await db.execute(
        select(ListModel).where(ListModel.id == list_id, ListModel.user_id == current_user.id)
    )
    lst = result.scalar_one_or_none()
    if not lst:
        raise HTTPException(status_code=404, detail="List not found")

    if body.name is not None:
        lst.name = body.name
    if body.description is not None:
        lst.description = body.description
    if body.privacy_level is not None:
        lst.privacy_level = body.privacy_level
        
    if body.radarr_auto_add is not None:
        lst.radarr_auto_add = body.radarr_auto_add
    if body.radarr_root_folder is not None:
        lst.radarr_root_folder = body.radarr_root_folder
    if body.radarr_quality_profile is not None:
        lst.radarr_quality_profile = body.radarr_quality_profile
    if body.radarr_tags is not None:
        lst.radarr_tags = body.radarr_tags
    if body.radarr_monitor is not None:
        lst.radarr_monitor = body.radarr_monitor
        
    if body.sonarr_auto_add is not None:
        lst.sonarr_auto_add = body.sonarr_auto_add
    if body.sonarr_root_folder is not None:
        lst.sonarr_root_folder = body.sonarr_root_folder
    if body.sonarr_quality_profile is not None:
        lst.sonarr_quality_profile = body.sonarr_quality_profile
    if body.sonarr_tags is not None:
        lst.sonarr_tags = body.sonarr_tags
    if body.sonarr_series_type is not None:
        lst.sonarr_series_type = body.sonarr_series_type
    if body.sonarr_season_folder is not None:
        lst.sonarr_season_folder = body.sonarr_season_folder
    if body.sonarr_monitor is not None:
        lst.sonarr_monitor = body.sonarr_monitor

    await db.commit()

    result = await db.execute(
        select(ListModel)
        .options(selectinload(ListModel.items))
        .where(ListModel.id == list_id)
    )
    lst = result.scalar_one()
    return _format_list(lst)


@router.delete("/{list_id}")
async def delete_list(
    list_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    result = await db.execute(
        select(ListModel).where(ListModel.id == list_id, ListModel.user_id == current_user.id)
    )
    lst = result.scalar_one_or_none()
    if not lst:
        raise HTTPException(status_code=404, detail="List not found")
    await db.delete(lst)
    await db.commit()
    return {"message": "List deleted"}


def _trakt_media_type(media_type: MediaType) -> Optional[str]:
    if media_type == MediaType.movie:
        return "movies"
    if media_type == MediaType.series:
        return "shows"
    return None


async def _push_list_item_to_trakt(
    db: AsyncSession,
    user_id: int,
    list_trakt_slug: str,
    media: Media,
    remove: bool = False,
) -> None:
    trakt_type = _trakt_media_type(media.media_type)
    if not trakt_type:
        return

    push_tmdb_id = media.tmdb_id
    if not push_tmdb_id and media.uri_id:
        from utils.alias_lookup import get_provider_id_for_uri
        alias = await get_provider_id_for_uri(db, media.uri_id, "tmdb")
        push_tmdb_id = int(alias) if alias else None
    if not push_tmdb_id:
        return

    settings_result = await db.execute(select(UserSettings).where(UserSettings.user_id == user_id))
    settings = settings_result.scalar_one_or_none()
    if (
        not settings
        or not settings.trakt_push_lists
        or not settings.trakt_access_token
        or not settings.trakt_client_id
    ):
        return

    from core import trakt as trakt_client
    try:
        if list_trakt_slug in ("__watchlist__", "__watchlist_movies__", "__watchlist_shows__"):
            if remove:
                await trakt_client.remove_from_watchlist(
                    settings.trakt_client_id, settings.trakt_access_token,
                    trakt_type, push_tmdb_id,
                )
            else:
                await trakt_client.add_to_watchlist(
                    settings.trakt_client_id, settings.trakt_access_token,
                    trakt_type, push_tmdb_id,
                )
        else:
            if remove:
                await trakt_client.remove_from_list(
                    settings.trakt_client_id, settings.trakt_access_token,
                    list_trakt_slug, trakt_type, push_tmdb_id,
                )
            else:
                await trakt_client.add_to_list(
                    settings.trakt_client_id, settings.trakt_access_token,
                    list_trakt_slug, trakt_type, push_tmdb_id,
                )
    except Exception as exc:
        logger.warning("Failed to push list item to Trakt (slug=%s, remove=%s): %s", list_trakt_slug, remove, exc)


async def _resolve_or_create_episode(db, current_user, body) -> "Media | None":
    from utils.media_uri import MediaURI
    from utils.alias_lookup import get_internal_id_for_uri

    if not (body.show_uri_id and body.season_number is not None and body.episode_number is not None):
        return None

    show = None
    try:
        _suri = MediaURI.parse(body.show_uri_id)
        col = ShowModel.tvdb_id if _suri.provider == "tvdb" else ShowModel.tmdb_id
        show_q = await db.execute(select(ShowModel).where(col == int(_suri.id)))
        show = show_q.scalar_one_or_none()
    except (ValueError, TypeError):
        show = None
    if show is None:
        internal_id = await get_internal_id_for_uri(db, body.show_uri_id)
        if internal_id is not None:
            show_q = await db.execute(select(ShowModel).where(ShowModel.id == internal_id))
            show = show_q.scalar_one_or_none()
    if show is None:
        return None

    ep_q = await db.execute(
        select(Media).where(
            Media.show_id == show.id,
            Media.media_type == MediaType.episode,
            Media.season_number == body.season_number,
            Media.episode_number == body.episode_number,
        )
    )
    media = ep_q.scalars().first()
    if media is None:
        media = Media(
            uri_id=body.uri_id,
            media_type=MediaType.episode,
            show_id=show.id,
            season_number=body.season_number,
            episode_number=body.episode_number,
            title=f"Episode {body.episode_number}",
        )
        db.add(media)
        await db.commit()
        await db.refresh(media)
    return media


@router.post("/{list_id}/items", status_code=201)
async def add_list_item(
    list_id: int,
    body: ListItemAdd,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    list_result = await db.execute(
        select(ListModel).where(ListModel.id == list_id, ListModel.user_id == current_user.id)
    )
    list_obj = list_result.scalar_one_or_none()
    if not list_obj:
        raise HTTPException(status_code=404, detail="List not found")

    from utils.media_uri import MediaURI
    try:
        uri = MediaURI.parse(body.uri_id)
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Invalid uri_id: {body.uri_id!r}")
        

    media_result = await db.execute(
        select(Media)
        .options(selectinload(Media.show))
        .where(Media.uri_id == body.uri_id, Media.media_type == body.media_type)
    )
    media = media_result.scalar_one_or_none()

    from routers.media import get_user_tmdb_key
    from core import tmdb

    api_key = await get_user_tmdb_key(db, current_user.id)

    if not media:
        from routers.shows import get_show_by_uri
        from routers.media import get_media_details
        try:
            if body.media_type == MediaType.series:
                show_data = await get_show_by_uri(body.uri_id, db=db, current_user=current_user)
                # Ensure a Media row exists for this series
                media_result = await db.execute(
                    select(Media).where(Media.uri_id == body.uri_id, Media.media_type == MediaType.series)
                )
                media = media_result.scalar_one_or_none()
                if not media:
                    # Create a dummy Media row for the series so lists can reference it
                    media = Media(
                        uri_id=body.uri_id,
                        tmdb_id=show_data.get("tmdb_id_cross") or show_data.get("tmdb_id"),
                        media_type=MediaType.series,
                        title=show_data.get("title") or show_data.get("name") or "Unknown Series",
                        poster_path=show_data.get("poster_path"),
                        release_date=show_data.get("first_air_date"),
                    )
                    db.add(media)
                    await db.commit()
                    await db.refresh(media)
            elif body.media_type == MediaType.movie:
                movie_data = await get_media_details(body.media_type, body.uri_id, db=db, current_user=current_user)
                media_result = await db.execute(
                    select(Media).where(Media.uri_id == body.uri_id, Media.media_type == MediaType.movie)
                )
                media = media_result.scalar_one_or_none()
                if not media:
                    media = Media(
                        uri_id=body.uri_id,
                        tmdb_id=movie_data.get("id") or movie_data.get("tmdb_id"),
                        media_type=MediaType.movie,
                        title=movie_data.get("title") or "Unknown Movie",
                        poster_path=movie_data.get("poster_path"),
                        release_date=movie_data.get("release_date"),
                        overview=movie_data.get("overview"),
                    )
                    db.add(media)
                    await db.commit()
                    await db.refresh(media)
            elif body.media_type == MediaType.episode:
                media = await _resolve_or_create_episode(db, current_user, body)
                if media is None:
                    raise Exception(
                        "Episode requires show_uri_id, season_number and episode_number "
                        "(and a known parent show) to add to a list"
                    )
            else:
                raise Exception("Cannot add unmatched items of this media type to lists")
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Failed to sync media item to lists: {e}")
    elif not media.adult and body.media_type in (MediaType.movie, MediaType.series) and media.tmdb_id:
        try:
            if body.media_type == MediaType.movie:
                data = await tmdb.get_movie(media.tmdb_id, api_key=api_key)
            else:
                data = await tmdb.get_show(media.tmdb_id, api_key=api_key)
            if data.get("adult", False):
                media.adult = True
        except Exception:
            pass

    existing = await db.execute(
        select(ListItem).where(ListItem.list_id == list_id, ListItem.media_id == media.id)
    )
    if existing.scalar_one_or_none():
        raise HTTPException(status_code=409, detail="Item already in list")

    item = ListItem(list_id=list_id, media_id=media.id)
    db.add(item)
    await db.commit()

    # --- Sonarr/Radarr Auto-Add ---
    approval_enqueued = False
    # Resolve TMDB integer ID for Sonarr/Radarr APIs (alias lookup for TVDB-only items)
    tmdb_id_int = media.tmdb_id
    if not tmdb_id_int and media.uri_id:
        from utils.alias_lookup import get_provider_id_for_uri as _get_provider_id
        _alias = await _get_provider_id(db, media.uri_id, "tmdb")
        tmdb_id_int = int(_alias) if _alias else None
    if body.media_type in (MediaType.movie, MediaType.series):
        from models.users import UserSettings
        from models.global_settings import GlobalSettings
        from routers.media import _effective_radarr, _effective_sonarr, _get_global_settings

        settings_q = await db.execute(select(UserSettings).where(UserSettings.user_id == current_user.id))
        settings = settings_q.scalar_one_or_none()
        gs = await _get_global_settings(db)

        if body.media_type == MediaType.movie and list_obj.radarr_auto_add:
            radarr_cfg = _effective_radarr(settings, gs)
            if radarr_cfg:
                uses_global = gs and radarr_cfg is gs and not current_user.is_admin
                if uses_global and gs.radarr_require_approval:
                    approval_enqueued = True
                    from models.media_request import MediaRequest, RequestStatus
                    movie_uri = body.uri_id
                    existing_q = await db.execute(
                        select(MediaRequest).where(
                            MediaRequest.user_id == current_user.id,
                            MediaRequest.uri_id == movie_uri,
                        )
                    )
                    existing = existing_q.scalar_one_or_none()
                    if existing:
                        if existing.status != RequestStatus.approved:
                            existing.status = RequestStatus.pending
                            existing.updated_at = func.now()
                    else:
                        db.add(MediaRequest(
                            user_id=current_user.id,
                            uri_id=movie_uri,
                            media_type="movie",
                            title=media.title or "",
                            poster_path=media.poster_path,
                            status=RequestStatus.pending,
                        ))
                    await db.commit()
                else:
                    from core import radarr
                    try:
                        await radarr.add_movie(
                            url=radarr_cfg.radarr_url,
                            token=radarr_cfg.radarr_token,
                            tmdb_id=tmdb_id_int,
                            title=media.title,
                            root_folder=list_obj.radarr_root_folder or radarr_cfg.radarr_root_folder,
                            quality_profile_id=list_obj.radarr_quality_profile or radarr_cfg.radarr_quality_profile,
                            tags=list_obj.radarr_tags,
                            monitored=list_obj.radarr_monitor != "none" if list_obj.radarr_monitor else True,
                            monitor=list_obj.radarr_monitor or "movieOnly",
                        )
                    except Exception as e:
                        print(f"Radarr auto-add failed: {e}")

        elif body.media_type == MediaType.series and list_obj.sonarr_auto_add:
            sonarr_cfg = _effective_sonarr(settings, gs)
            if sonarr_cfg:
                uses_global = gs and sonarr_cfg is gs and not current_user.is_admin
                if uses_global and gs.sonarr_require_approval:
                    approval_enqueued = True
                    from models.media_request import MediaRequest, RequestStatus
                    series_uri = body.uri_id
                    existing_q = await db.execute(
                        select(MediaRequest).where(
                            MediaRequest.user_id == current_user.id,
                            MediaRequest.uri_id == series_uri,
                        )
                    )
                    existing = existing_q.scalar_one_or_none()
                    if existing:
                        if existing.status != RequestStatus.approved:
                            existing.status = RequestStatus.pending
                            existing.updated_at = func.now()
                    else:
                        db.add(MediaRequest(
                            user_id=current_user.id,
                            uri_id=series_uri,
                            media_type="series",
                            title=media.title or "",
                            poster_path=media.poster_path,
                            status=RequestStatus.pending,
                        ))
                    await db.commit()
                else:
                    from core import sonarr
                    try:
                        tvdb_id = (media.tmdb_data or {}).get("external_ids", {}).get("tvdb_id")
                        if not tvdb_id and media.uri_id:
                            from utils.alias_lookup import get_provider_id_for_uri as _get_tvdb
                            tvdb_id = await _get_tvdb(db, media.uri_id, "tvdb")
                        if not tvdb_id and tmdb_id_int:
                            from core import tmdb as tmdb_core
                            ext_ids = await tmdb_core.get_external_ids(tmdb_id_int, "tv", api_key=api_key)
                            tvdb_id = ext_ids.get("tvdb_id")

                        if tvdb_id:
                            await sonarr.add_series(
                                url=sonarr_cfg.sonarr_url,
                                token=sonarr_cfg.sonarr_token,
                                tvdb_id=tvdb_id,
                                root_folder=list_obj.sonarr_root_folder or sonarr_cfg.sonarr_root_folder,
                                quality_profile_id=list_obj.sonarr_quality_profile or sonarr_cfg.sonarr_quality_profile,
                                tags=list_obj.sonarr_tags,
                                monitored=list_obj.sonarr_monitor != "none" if list_obj.sonarr_monitor else True,
                                season_folder=list_obj.sonarr_season_folder,
                                series_type=list_obj.sonarr_series_type or "standard",
                                monitor=list_obj.sonarr_monitor or "all",
                            )
                    except Exception as e:
                        print(f"Sonarr auto-add failed: {e}")

    # --- Trakt Sync Push ---
    if list_obj.trakt_slug:
        await _push_list_item_to_trakt(db, current_user.id, list_obj.trakt_slug, media, remove=False)

    item_result = await db.execute(
        select(ListItem)
        .options(selectinload(ListItem.media).selectinload(Media.show))
        .where(ListItem.list_id == list_id, ListItem.media_id == media.id)
    )
    formatted = _format_item(item_result.scalar_one())
    await enrich_with_state(db, current_user.id, [formatted["media"]], False)
    if approval_enqueued:
        formatted["auto_add_status"] = "pending_approval"
    return formatted


@router.delete("/{list_id}/items/{item_id}")
async def remove_list_item(
    list_id: int,
    item_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    result = await db.execute(
        select(ListItem)
        .options(selectinload(ListItem.media))
        .join(ListModel, ListModel.id == ListItem.list_id)
        .where(
            ListItem.id == item_id,
            ListItem.list_id == list_id,
            ListModel.user_id == current_user.id,
        )
    )
    item = result.scalar_one_or_none()
    if not item:
        raise HTTPException(status_code=404, detail="Item not found")

    list_result = await db.execute(
        select(ListModel).where(ListModel.id == list_id)
    )
    lst = list_result.scalar_one_or_none()
    media = item.media

    await db.delete(item)
    await db.commit()

    if lst and lst.trakt_slug and media:
        await _push_list_item_to_trakt(db, current_user.id, lst.trakt_slug, media, remove=True)

    return {"message": "Item removed"}


@router.post("/{list_id}/items/cleanup-collection")
async def cleanup_collection_items(
    list_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    result = await db.execute(
        select(ListModel)
        .options(selectinload(ListModel.items).selectinload(ListItem.media).selectinload(Media.show))
        .where(ListModel.id == list_id, ListModel.user_id == current_user.id)
    )
    lst = result.scalar_one_or_none()
    if not lst:
        raise HTTPException(status_code=404, detail="List not found")

    if not lst.items:
        return {"removed_count": 0}

    # Prepare items for enrichment check
    items_to_check = []
    item_map = {}
    for li in lst.items:
        mtype = li.media.media_type
        items_to_check.append({
            "uri_id": li.media.uri_id,
            "tmdb_id": li.media.tmdb_id,
            "type": mtype.value if hasattr(mtype, "value") else mtype,
            "_list_item_db_id": li.media.id,
        })
        item_map[li.media.id] = li

    # Use existing enrich_with_state logic to determine library status
    enriched = await enrich_with_state(db, current_user.id, items_to_check, False)

    to_delete = []
    for item in enriched:
        if item.get("in_library"):
            internal_id = item.get("_internal_id") or item.get("_list_item_db_id")
            li = item_map.get(internal_id)
            if li:
                to_delete.append(li)

    if not to_delete:
        return {"removed_count": 0}

    count = len(to_delete)
    for li in to_delete:
        # Handle Trakt sync if the list is linked
        if lst.trakt_slug:
            await _push_list_item_to_trakt(db, current_user.id, lst.trakt_slug, li.media, remove=True)
        await db.delete(li)
    
    await db.commit()
    
    return {"removed_count": count}



