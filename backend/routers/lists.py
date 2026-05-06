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
    tmdb_id: int
    media_type: MediaType


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
    series_tmdb_ids = [
        item["media"]["tmdb_id"]
        for item in formatted_items
        if item["media"].get("type") in (MediaType.series, "series")
        and (not item["media"].get("poster_path") or not item["media"].get("release_date"))
        and item["media"].get("tmdb_id")
    ]
    if series_tmdb_ids:
        shows_result = await db.execute(
            select(ShowModel).where(ShowModel.tmdb_id.in_(series_tmdb_ids))
        )
        show_map = {s.tmdb_id: s for s in shows_result.scalars().all()}
        for item in formatted_items:
            m = item["media"]
            if m.get("type") not in (MediaType.series, "series"):
                continue
            show = show_map.get(m.get("tmdb_id"))
            if show:
                if not m.get("poster_path") and show.poster_path:
                    m["poster_path"] = show.poster_path
                if not m.get("release_date") and show.first_air_date:
                    m["release_date"] = show.first_air_date
                if not m.get("title") and show.title:
                    m["title"] = show.title

    media_dicts = [item["media"] for item in formatted_items]
    await enrich_with_state(db, current_user.id, media_dicts)

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
    if not trakt_type or not media.tmdb_id:
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
        if list_trakt_slug == "__watchlist__":
            if remove:
                await trakt_client.remove_from_watchlist(
                    settings.trakt_client_id, settings.trakt_access_token,
                    trakt_type, media.tmdb_id,
                )
            else:
                await trakt_client.add_to_watchlist(
                    settings.trakt_client_id, settings.trakt_access_token,
                    trakt_type, media.tmdb_id,
                )
        else:
            if remove:
                await trakt_client.remove_from_list(
                    settings.trakt_client_id, settings.trakt_access_token,
                    list_trakt_slug, trakt_type, media.tmdb_id,
                )
            else:
                await trakt_client.add_to_list(
                    settings.trakt_client_id, settings.trakt_access_token,
                    list_trakt_slug, trakt_type, media.tmdb_id,
                )
    except Exception as exc:
        logger.warning("Failed to push list item to Trakt (slug=%s, remove=%s): %s", list_trakt_slug, remove, exc)


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

    media_result = await db.execute(
        select(Media)
        .options(selectinload(Media.show))
        .where(Media.tmdb_id == body.tmdb_id, Media.media_type == body.media_type)
    )
    media = media_result.scalar_one_or_none()

    from routers.media import get_user_tmdb_key
    from core import tmdb

    api_key = await get_user_tmdb_key(db, current_user.id)

    if not media:
        try:
            if body.media_type == MediaType.movie:
                data = await tmdb.get_movie(body.tmdb_id, api_key=api_key)
                media = Media(
                    tmdb_id=body.tmdb_id,
                    media_type=MediaType.movie,
                    title=data.get("title", "Unknown"),
                    poster_path=tmdb.poster_url(data.get("poster_path")),
                    backdrop_path=tmdb.poster_url(data.get("backdrop_path"), size="w1280"),
                    release_date=data.get("release_date"),
                    tmdb_rating=data.get("vote_average"),
                    overview=data.get("overview"),
                    adult=data.get("adult", False),
                )
            elif body.media_type == MediaType.person:
                data = await tmdb.get_person(body.tmdb_id, api_key=api_key)
                media = Media(
                    tmdb_id=body.tmdb_id,
                    media_type=MediaType.person,
                    title=data.get("name", "Unknown"),
                    poster_path=tmdb.poster_url(data.get("profile_path"), size="w185"),
                    overview=data.get("biography"),
                )
            else:
                data = await tmdb.get_show(body.tmdb_id, api_key=api_key)
                media = Media(
                    tmdb_id=body.tmdb_id,
                    media_type=MediaType.series,
                    title=data.get("name", "Unknown"),
                    poster_path=tmdb.poster_url(data.get("poster_path")),
                    backdrop_path=tmdb.poster_url(data.get("backdrop_path"), size="w1280"),
                    release_date=data.get("first_air_date"),
                    tmdb_rating=data.get("vote_average"),
                    overview=data.get("overview"),
                    adult=data.get("adult", False),
                )
            db.add(media)
            await db.flush()
        except Exception as e:
            raise HTTPException(status_code=404, detail=f"Media not found: {e}")
    elif not media.adult and body.media_type in (MediaType.movie, MediaType.series):
        # Existing record may pre-date the adult flag — refresh from TMDB
        try:
            if body.media_type == MediaType.movie:
                data = await tmdb.get_movie(body.tmdb_id, api_key=api_key)
            else:
                data = await tmdb.get_show(body.tmdb_id, api_key=api_key)
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
                from core import radarr
                try:
                    await radarr.add_movie(
                        url=radarr_cfg.radarr_url,
                        token=radarr_cfg.radarr_token,
                        tmdb_id=body.tmdb_id,
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
                from core import sonarr
                try:
                    tvdb_id = (media.tmdb_data or {}).get("external_ids", {}).get("tvdb_id")
                    if not tvdb_id:
                        from core import tmdb as tmdb_core
                        ext_ids = await tmdb_core.get_external_ids(body.tmdb_id, "tv", api_key=api_key)
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
    await enrich_with_state(db, current_user.id, [formatted["media"]])
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


@router.get("/{list_id}/rss")
async def get_list_rss(
    list_id: int,
    apikey: Optional[str] = None,
    db: AsyncSession = Depends(get_db),
):
    # Authentication (optional for public lists, required for others)
    user = None
    if apikey:
        result = await db.execute(select(User).where(User.api_key == apikey))
        user = result.scalar_one_or_none()

    # Get list with items and media
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

    # Access control
    if lst.privacy_level != PrivacyLevel.public:
        if not user or user.id != lst.user_id:
            raise HTTPException(status_code=403, detail="Access denied")

    # Generate RSS items
    items_sorted = sorted(lst.items, key=lambda x: (x.sort_order, x.added_at), reverse=True)
    
    rss_items = []
    for item in items_sorted:
        m = item.media
        if m.media_type == MediaType.person:
            continue  # Sonarr/Radarr don't care about people
            
        title = m.title
        year = m.release_date[:4] if m.release_date else ""
        
        if m.media_type == MediaType.episode and m.show:
            title = f"{m.show.title} - {m.season_number}x{m.episode_number:02d} - {m.title}"
        elif year:
            title = f"{title} ({year})"
            
        # Sonarr/Radarr can often use TMDB/IMDB IDs if provided in the description or as a custom tag
        # For now, we'll provide them in the description as it's widely compatible
        description = f"Type: {m.media_type.value}\n"
        if m.tmdb_id:
            description += f"TMDB: {m.tmdb_id}\n"
        if m.overview:
            description += f"\n{m.overview}"
            
        link = f"{app_settings.server_url}/list/{lst.id}"
        guid = f"scrob:{m.media_type.value}:{m.tmdb_id or m.id}"
        
        rss_items.append(f"""
        <item>
            <title>{escape(title)}</title>
            <link>{escape(link)}</link>
            <description>{escape(description)}</description>
            <pubDate>{item.added_at.strftime("%a, %d %b %Y %H:%M:%S GMT")}</pubDate>
            <guid isPermaLink="false">{escape(guid)}</guid>
        </item>""")

    rss_xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0" xmlns:atom="http://www.w3.org/2005/Atom">
    <channel>
        <title>Scrob List: {escape(lst.name)}</title>
        <description>{escape(lst.description or "")}</description>
        <link>{escape(app_settings.server_url)}/list/{lst.id}</link>
        {"".join(rss_items)}
    </channel>
</rss>"""

    return Response(content=rss_xml, media_type="application/rss+xml")
