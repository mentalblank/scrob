"""Artwork pickers and per-user image overrides.

The options endpoint merges what TMDB and TVDB hold for one subject; the
override endpoints record which of those (or which external URL) a user wants
used in place of the provider's default.
"""

import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from core import image_overrides, tmdb, tvdb
from core.image_cache import store_external_image
from db import get_db
from dependencies import get_current_user
from models.base import MediaType
from models.image_override import IMAGE_KINDS, NO_NUMBER, MediaImageOverride
from models.media import Media
from models.show import Show
from models.users import User, UserSettings
from utils.media_uri import MediaURI

logger = logging.getLogger(__name__)

router = APIRouter()

# Which artwork kinds each subject can have replaced.
KINDS_BY_TARGET = {
    "show": ("poster", "backdrop"),
    "season": ("poster",),
    "episode": ("still",),
    "movie": ("poster", "backdrop"),
}


class ImageOverrideBody(BaseModel):
    subject_uri: str
    kind: str
    source: str
    season_number: Optional[int] = None
    episode_number: Optional[int] = None
    # A provider path for source "tmdb"/"tvdb"; ignored for "external".
    path: Optional[str] = None
    # The image URL for source "external"; ignored otherwise.
    url: Optional[str] = None


async def _provider_keys(db: AsyncSession, user_id: int) -> tuple[Optional[str], Optional[str]]:
    """The user's effective TMDB and TVDB keys, falling back to the global ones."""
    from models.global_settings import GlobalSettings

    user_settings = (await db.execute(
        select(UserSettings).where(UserSettings.user_id == user_id)
    )).scalar_one_or_none()
    global_settings = (await db.execute(
        select(GlobalSettings).where(GlobalSettings.id == 1)
    )).scalar_one_or_none()

    tmdb_key = (user_settings.tmdb_api_key if user_settings else None) or (global_settings.tmdb_api_key if global_settings else None)
    tvdb_key = (user_settings.tvdb_api_key if user_settings else None) or (global_settings.tvdb_api_key if global_settings else None)
    return tmdb_key, tvdb_key


def _parse_subject(subject_uri: str) -> MediaURI:
    try:
        uri = MediaURI.parse(subject_uri)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    if uri.type_prefix not in ("s", "m"):
        raise HTTPException(status_code=400, detail="subject_uri must identify a show (…:s:…) or a movie (…:m:…)")
    return uri


async def _resolve_subject(db: AsyncSession, uri: MediaURI) -> tuple[Optional[Show], Optional[Media], Optional[int], Optional[int]]:
    """Local rows plus the subject's TMDB/TVDB ids, however it was addressed."""
    external_id = int(uri.id)

    if uri.type_prefix == "s":
        show = (await db.execute(select(Show).where(Show.uri_id == str(uri)))).scalar_one_or_none()
        if not show:
            column = Show.tmdb_id if uri.provider == "tmdb" else Show.tvdb_id
            show = (await db.execute(select(Show).where(column == external_id))).scalar_one_or_none()
        if show:
            return show, None, show.tmdb_id, show.tvdb_id
        # Not in the library yet: only the provider it was addressed by is known.
        return None, None, (external_id if uri.provider == "tmdb" else None), (external_id if uri.provider == "tvdb" else None)

    media = (await db.execute(select(Media).where(Media.uri_id == str(uri)))).scalar_one_or_none()
    if not media and uri.provider == "tmdb":
        media = (await db.execute(
            select(Media).where(Media.tmdb_id == external_id, Media.media_type == MediaType.movie)
        )).scalar_one_or_none()
    tmdb_id = (media.tmdb_id if media else None) or (external_id if uri.provider == "tmdb" else None)
    return None, media, tmdb_id, None


def _target_of(uri: MediaURI, season_number: Optional[int], episode_number: Optional[int]) -> str:
    if uri.type_prefix == "m":
        return "movie"
    if episode_number is not None:
        return "episode"
    if season_number is not None:
        return "season"
    return "show"


def _tmdb_option(kind: str, entry: dict) -> dict:
    return {
        "source": "tmdb",
        "kind": kind,
        "path": entry.get("file_path"),
        "url": entry.get("file_path"),
        "language": entry.get("iso_639_1"),
        "width": entry.get("width"),
        "height": entry.get("height"),
        "votes": entry.get("vote_count"),
    }


def _tvdb_kind(type_name: str) -> Optional[str]:
    name = (type_name or "").lower()
    if "poster" in name:
        return "poster"
    if "background" in name or "fanart" in name:
        return "backdrop"
    return None


async def _tvdb_options(artworks: list[dict], wanted_kind: str, tvdb_key: str) -> list[dict]:
    """Classify TVDB artworks by its numeric type table, keeping the wanted kind."""
    try:
        type_names = {t.get("id"): t.get("name") for t in await tvdb.get_artwork_types(tvdb_key)}
    except Exception:
        logger.warning("TVDB artwork type table unavailable; artwork kinds cannot be classified", exc_info=True)
        return []

    options = []
    for artwork in artworks:
        image = artwork.get("image")
        if not image or _tvdb_kind(type_names.get(artwork.get("type"), "")) != wanted_kind:
            continue
        path = image if image.startswith("/") else "/" + image.split("thetvdb.com", 1)[-1].lstrip("/")
        options.append({
            "source": "tvdb",
            "kind": wanted_kind,
            "path": path,
            "url": tvdb._image_url(image),
            "language": artwork.get("language"),
            "width": artwork.get("width"),
            "height": artwork.get("height"),
            "votes": (artwork.get("score") or None),
        })
    return options


@router.get("/options")
async def list_image_options(
    subject_uri: str = Query(...),
    kind: str = Query(...),
    season_number: Optional[int] = Query(None),
    episode_number: Optional[int] = Query(None),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Artwork TMDB and TVDB hold for one show, season, episode, or movie."""
    uri = _parse_subject(subject_uri)
    target = _target_of(uri, season_number, episode_number)
    if kind not in KINDS_BY_TARGET[target]:
        raise HTTPException(status_code=400, detail=f"A {target} has no {kind!r} artwork")

    _, _, tmdb_id, tvdb_id = await _resolve_subject(db, uri)
    tmdb_key, tvdb_key = await _provider_keys(db, current_user.id)

    options: list[dict] = []

    if tmdb_key and tmdb_id:
        try:
            if target == "movie":
                data = await tmdb.get_movie_images(tmdb_id, api_key=tmdb_key)
            elif target == "show":
                data = await tmdb.get_show_images(tmdb_id, api_key=tmdb_key)
            elif target == "season":
                data = await tmdb.get_season_images(tmdb_id, season_number, api_key=tmdb_key)
            else:
                data = await tmdb.get_episode_images(tmdb_id, season_number, episode_number, api_key=tmdb_key)
            bucket = {"poster": "posters", "backdrop": "backdrops", "still": "stills"}[kind]
            options.extend(_tmdb_option(kind, entry) for entry in (data.get(bucket) or []))
        except Exception:
            logger.warning("TMDB artwork lookup failed for %s", subject_uri, exc_info=True)

    if tvdb_key and tvdb_id:
        try:
            if target == "movie":
                options.extend(await _tvdb_options(await tvdb.get_movie_artworks(tvdb_id, tvdb_key), kind, tvdb_key))
            elif target == "show":
                options.extend(await _tvdb_options(await tvdb.get_series_artworks(tvdb_id, tvdb_key), kind, tvdb_key))
            elif target == "season":
                season_id = await tvdb.find_season_id(tvdb_id, season_number, tvdb_key)
                if season_id:
                    options.extend(await _tvdb_options(await tvdb.get_season_artworks(season_id, tvdb_key), kind, tvdb_key))
            else:
                # TVDB keeps no still gallery - an episode has exactly one image.
                for episode in await tvdb.get_series_episodes(tvdb_id, season_number, tvdb_key):
                    if episode.get("number") == episode_number and episode.get("image"):
                        options.append({
                            "source": "tvdb",
                            "kind": "still",
                            "path": "/" + str(episode["image"]).split("thetvdb.com", 1)[-1].lstrip("/"),
                            "url": tvdb._image_url(episode["image"]),
                            "language": None, "width": None, "height": None, "votes": None,
                        })
                        break
        except Exception:
            logger.warning("TVDB artwork lookup failed for %s", subject_uri, exc_info=True)

    return {"target": target, "kind": kind, "options": options}


@router.get("/overrides")
async def list_image_overrides(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Every artwork this user has replaced, newest first."""
    rows = (await db.execute(
        select(MediaImageOverride)
        .where(MediaImageOverride.user_id == current_user.id)
        .order_by(MediaImageOverride.updated_at.desc())
    )).scalars().all()

    return {"results": [
        {
            "id": row.id,
            "subject_uri": row.subject_uri,
            "season_number": None if row.season_number == NO_NUMBER else row.season_number,
            "episode_number": None if row.episode_number == NO_NUMBER else row.episode_number,
            "kind": row.image_kind,
            "source": row.source,
            "source_url": row.source_url,
            "url": image_overrides.public_path(row.source, row.image_path, row.image_kind),
        }
        for row in rows
    ]}


@router.put("/override")
async def set_image_override(
    body: ImageOverrideBody,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Replace one artwork for this user, from a provider option or an external URL."""
    uri = _parse_subject(body.subject_uri)
    target = _target_of(uri, body.season_number, body.episode_number)
    if body.kind not in IMAGE_KINDS or body.kind not in KINDS_BY_TARGET[target]:
        raise HTTPException(status_code=400, detail=f"A {target} has no {body.kind!r} artwork")
    if body.source not in ("tmdb", "tvdb", "external"):
        raise HTTPException(status_code=400, detail="source must be tmdb, tvdb, or external")

    source_url = None
    if body.source == "external":
        if not body.url:
            raise HTTPException(status_code=400, detail="An external override needs a url")
        image_path, _ = await store_external_image(db, body.url)
        source_url = body.url
    else:
        if not body.path or not body.path.startswith("/") or ".." in body.path:
            raise HTTPException(status_code=400, detail="path must be a provider image path")
        if len(body.path) > 500:
            raise HTTPException(status_code=400, detail="path is too long to store")
        image_path = body.path

    show, media, _, _ = await _resolve_subject(db, uri)
    season = body.season_number if body.season_number is not None else NO_NUMBER
    episode = body.episode_number if body.episode_number is not None else NO_NUMBER

    existing = (await db.execute(
        select(MediaImageOverride).where(
            MediaImageOverride.user_id == current_user.id,
            MediaImageOverride.subject_uri == str(uri),
            MediaImageOverride.season_number == season,
            MediaImageOverride.episode_number == episode,
            MediaImageOverride.image_kind == body.kind,
        )
    )).scalar_one_or_none()

    if existing:
        existing.image_path = image_path
        existing.source = body.source
        existing.source_url = source_url
        existing.show_id = show.id if show else None
        existing.media_id = media.id if media else None
        override = existing
    else:
        override = MediaImageOverride(
            user_id=current_user.id,
            subject_uri=str(uri),
            show_id=show.id if show else None,
            media_id=media.id if media else None,
            season_number=season,
            episode_number=episode,
            image_kind=body.kind,
            image_path=image_path,
            source=body.source,
            source_url=source_url,
        )
        db.add(override)

    await db.commit()
    image_overrides.invalidate(current_user.id)

    return {
        "status": "ok",
        "subject_uri": str(uri),
        "kind": body.kind,
        "source": body.source,
        "url": image_overrides.public_path(body.source, image_path, body.kind),
    }


@router.delete("/override")
async def clear_image_override(
    subject_uri: str = Query(...),
    kind: str = Query(...),
    season_number: Optional[int] = Query(None),
    episode_number: Optional[int] = Query(None),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Drop one override, restoring the provider's own artwork."""
    uri = _parse_subject(subject_uri)
    existing = (await db.execute(
        select(MediaImageOverride).where(
            MediaImageOverride.user_id == current_user.id,
            MediaImageOverride.subject_uri == str(uri),
            MediaImageOverride.season_number == (season_number if season_number is not None else NO_NUMBER),
            MediaImageOverride.episode_number == (episode_number if episode_number is not None else NO_NUMBER),
            MediaImageOverride.image_kind == kind,
        )
    )).scalar_one_or_none()

    if existing:
        await db.delete(existing)
        await db.commit()
        image_overrides.invalidate(current_user.id)

    return {"status": "ok", "cleared": existing is not None}
