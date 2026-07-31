import html as html_lib
import json
import logging
import urllib.parse
from datetime import datetime, timedelta
from typing import Optional
from fastapi import APIRouter, Depends, Response, Request
from pydantic import BaseModel
from fastapi.responses import JSONResponse, HTMLResponse, RedirectResponse
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, delete, desc, or_
from sqlalchemy.orm import selectinload

from db import get_db
from models.users import User, UserSettings
from models.media import Media
from models.collection import Collection
from models.show import Show
from models.events import WatchEvent
from models.lists import List as ListModel, ListItem
from models.playback_progress import PlaybackProgress
from models.playback_session import PlaybackSession
from models.base import MediaType
from core import tmdb
from core.enrichment import enrich_media

router = APIRouter()
logger = logging.getLogger(__name__)


def add_cors_headers(response: Response) -> Response:
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
    response.headers["Access-Control-Allow-Headers"] = "*"
    return response


from core.config import settings


def get_base_url(request: Request) -> str:
    # 1. Default to configured server_url from environment/settings
    configured_base = settings.server_url.rstrip("/")

    # 2. Extract forwarded or request headers dynamically
    forwarded_host = request.headers.get("x-forwarded-host")
    forwarded_proto = request.headers.get("x-forwarded-proto")
    req_host = request.headers.get("host")

    host_candidate = forwarded_host or req_host

    # If header contains a valid external public domain (not internal container/localhost address)
    if (
        host_candidate
        and "localhost" not in host_candidate
        and "127.0.0.1" not in host_candidate
        and ":7331" not in host_candidate
        and ":7330" not in host_candidate
    ):
        proto = (forwarded_proto.split(",")[0].strip() if forwarded_proto else request.url.scheme) or "https"
        return f"{proto}://{host_candidate}"

    # 3. Fall back dynamically to configured settings.server_url
    return configured_base


async def get_user_from_key(api_key: str, db: AsyncSession) -> Optional[User]:
    res = await db.execute(select(User).where(User.api_key == api_key))
    return res.scalar_one_or_none()


def media_to_stremio_meta(item) -> Optional[dict]:
    if not item:
        return None

    media_type = getattr(item, "media_type", None)
    is_show = isinstance(item, Show) or media_type in (MediaType.series, "series")
    is_episode = media_type in (MediaType.episode, "episode")
    
    uri_id = getattr(item, "uri_id", "") or ""
    imdb_id = getattr(item, "imdb_id", None)
    tmdb_id = getattr(item, "tmdb_id", None)
    
    tmdb_data = getattr(item, "tmdb_data", None) or {}
    if not imdb_id and isinstance(tmdb_data, dict):
        imdb_id = tmdb_data.get("imdb_id") or (tmdb_data.get("external_ids") or {}).get("imdb_id")

    if "imdb:" in uri_id:
        val = uri_id.split(":")[-1]
        stremio_id = f"tt{val.lstrip('t')}"
    elif imdb_id:
        val = str(imdb_id).strip()
        stremio_id = f"tt{val.lstrip('t')}" if not val.startswith("tt") else val
    elif tmdb_id:
        stremio_id = f"tmdb:{tmdb_id}"
    elif "tmdb:" in uri_id:
        stremio_id = f"tmdb:{uri_id.split(':')[-1]}"
    else:
        stremio_id = f"scrob:{item.id}"

    poster_path = getattr(item, "poster_path", None)
    if poster_path and poster_path.startswith("/"):
        poster_url = f"https://image.tmdb.org/t/p/w500{poster_path}"
    else:
        poster_url = poster_path

    release_year = None
    rel_date = getattr(item, "release_date", None) or getattr(item, "first_air_date", None)
    if rel_date:
        release_year = str(rel_date)[:4]

    title = getattr(item, "title", None) or getattr(item, "name", "Unknown Title")
    overview = getattr(item, "overview", None)
    rating = getattr(item, "tmdb_rating", None) or getattr(item, "vote_average", None)

    return {
        "id": stremio_id,
        # Derived from the item itself: a movie must never be announced as a series
        # just because it turned up in a series catalog (Stremio then can't play it).
        "type": "series" if (is_show or is_episode) else "movie",
        "name": title,
        "poster": poster_url,
        "description": overview,
        "releaseInfo": release_year,
        "imdbRating": str(rating) if rating else None
    }


async def _tmdb_key_for_user(db: AsyncSession, user: Optional[User]) -> Optional[str]:
    if not user:
        return None
    from routers.media import get_user_tmdb_key, check_tmdb_key

    key = await get_user_tmdb_key(db, user.id)
    return key if check_tmdb_key(key) else None


async def _tvdb_key_for_user(db: AsyncSession, user: Optional[User]) -> Optional[str]:
    if not user:
        return None
    from routers.shows import get_user_tvdb_key

    return await get_user_tvdb_key(db, user.id)


DEFAULT_STREMIO_PREFS: dict = {
    "catalogs": {"watchlist": True, "history": True, "nextup": True, "lists": True},
    "types": {"movie": True, "series": True},
    "actions": {"mark_watched": True, "scrobble": True},
    # None means every list; an explicit array picks individual ones.
    "enabled_list_ids": None,
    "catalog_limit": 50,
}


def _merge_stremio_prefs(stored: dict | None) -> dict:
    """Stored config over the defaults, so a new option appears without a migration."""
    prefs = {
        "catalogs": dict(DEFAULT_STREMIO_PREFS["catalogs"]),
        "types": dict(DEFAULT_STREMIO_PREFS["types"]),
        "actions": dict(DEFAULT_STREMIO_PREFS["actions"]),
        "enabled_list_ids": DEFAULT_STREMIO_PREFS["enabled_list_ids"],
        "catalog_limit": DEFAULT_STREMIO_PREFS["catalog_limit"],
    }
    if not isinstance(stored, dict):
        return prefs

    for group in ("catalogs", "types", "actions"):
        values = stored.get(group)
        if isinstance(values, dict):
            for key in prefs[group]:
                if key in values:
                    prefs[group][key] = bool(values[key])

    list_ids = stored.get("enabled_list_ids")
    if isinstance(list_ids, list):
        prefs["enabled_list_ids"] = [int(v) for v in list_ids if str(v).isdigit()]

    limit = stored.get("catalog_limit")
    if isinstance(limit, int) and 1 <= limit <= 200:
        prefs["catalog_limit"] = limit

    return prefs


async def _stremio_prefs(db: AsyncSession, user: Optional[User]) -> dict:
    if not user:
        return _merge_stremio_prefs(None)
    res = await db.execute(select(UserSettings).where(UserSettings.user_id == user.id))
    row = res.scalar_one_or_none()
    return _merge_stremio_prefs(((row.preferences or {}) if row else {}).get("stremio"))


async def _save_stremio_prefs(db: AsyncSession, user: User, prefs: dict) -> None:
    from sqlalchemy.orm.attributes import flag_modified

    res = await db.execute(select(UserSettings).where(UserSettings.user_id == user.id))
    row = res.scalar_one_or_none()
    if not row:
        row = UserSettings(user_id=user.id, preferences={})
        db.add(row)
    preferences = dict(row.preferences or {})
    preferences["stremio"] = prefs
    row.preferences = preferences
    flag_modified(row, "preferences")
    await db.commit()


async def _resolve_runtime(db: AsyncSession, user: Optional[User], media: Media) -> Optional[int]:
    """Fill in a missing runtime from TMDB, then TVDB, and remember it on the row.

    A scrobble session only auto-completes once the runtime has elapsed, so a title
    without one can never finish on its own.
    """
    if media.runtime:
        return media.runtime

    tmdb_key = await _tmdb_key_for_user(db, user)
    show = None
    if media.show_id:
        show_res = await db.execute(select(Show).where(Show.id == media.show_id))
        show = show_res.scalar_one_or_none()

    runtime = None
    if tmdb_key:
        try:
            if media.media_type == MediaType.episode:
                if show and show.tmdb_id and media.season_number is not None and media.episode_number is not None:
                    data = await tmdb.get_episode(
                        show.tmdb_id, media.season_number, media.episode_number, api_key=tmdb_key
                    )
                    runtime = data.get("runtime")
            elif media.tmdb_id:
                data = await tmdb.get_movie(media.tmdb_id, api_key=tmdb_key)
                runtime = data.get("runtime")
        except Exception:
            runtime = None

    if not runtime and show and show.tvdb_id and media.media_type == MediaType.episode:
        tvdb_key = await _tvdb_key_for_user(db, user)
        if tvdb_key:
            try:
                from core import tvdb as tvdb_client

                episodes = await tvdb_client.get_series_episodes(
                    show.tvdb_id, media.season_number, tvdb_key
                )
                for raw in episodes:
                    formatted = tvdb_client.format_episode(raw)
                    if formatted.get("episode_number") == media.episode_number:
                        runtime = formatted.get("runtime")
                        break
            except Exception:
                runtime = None

    # TVDB reports a show-level average runtime, which beats having none at all.
    if not runtime and show and (show.tmdb_data or {}).get("episode_run_time"):
        run_times = (show.tmdb_data or {}).get("episode_run_time") or []
        runtime = run_times[0] if run_times else None

    if runtime:
        media.runtime = int(runtime)
    return media.runtime


def _js_literal(value: str) -> str:
    """JSON-encode a value for embedding in an inline <script> block.

    json.dumps alone is not enough: it leaves "</script>" intact, which ends the
    script element during HTML parsing regardless of the surrounding quotes.
    """
    return (
        json.dumps(value)
        .replace("<", "\\u003c")
        .replace(">", "\\u003e")
        .replace("&", "\\u0026")
    )


def _imdb_numeric(imdb_id: str) -> str:
    """tt7587890 -> 7587890. MediaURI ids are digits only."""
    return imdb_id.strip().lower().lstrip("t")


STREMIO_ALIAS_PROVIDER = "stremio"


async def _pinned_media(db: AsyncSession, item_id: str) -> Optional[Media]:
    """The Media row a user pinned to this exact Stremio id, if any."""
    from models.media_alias import MediaAlias

    res = await db.execute(
        select(Media)
        .join(MediaAlias, MediaAlias.internal_id == Media.id)
        .where(
            MediaAlias.provider == STREMIO_ALIAS_PROVIDER,
            MediaAlias.external_id == item_id,
            MediaAlias.media_type == Media.media_type,
        )
        .limit(1)
    )
    return res.scalars().first()


async def _pin_media(db: AsyncSession, item_id: str, media: Media) -> None:
    """Pin a Stremio id to a Media row, replacing whatever it pointed at before.

    upsert_alias only inserts, so it can't re-point an existing alias — and
    re-pointing is the whole purpose of a correction.
    """
    from models.media_alias import MediaAlias

    await db.execute(
        delete(MediaAlias).where(
            MediaAlias.provider == STREMIO_ALIAS_PROVIDER,
            MediaAlias.external_id == item_id,
        )
    )
    db.add(MediaAlias(
        internal_id=media.id,
        media_type=media.media_type,
        provider=STREMIO_ALIAS_PROVIDER,
        external_id=item_id,
        is_manual=True,
    ))


async def _remember_imdb_alias(db: AsyncSession, internal_id: int, media_type: str, imdb_id: str) -> None:
    """Record the IMDb -> internal mapping so the next Stremio call resolves instantly."""
    from utils.alias_lookup import upsert_alias

    try:
        await upsert_alias(db, internal_id, media_type, "imdb", _imdb_numeric(imdb_id))
    except Exception:
        pass


async def _show_from_imdb_id(db: AsyncSession, imdb_id: str, api_key: Optional[str]) -> Optional[Show]:
    """Resolve an IMDb id (tt7587890) to a local Show row.

    Stremio identifies everything by IMDb id, which is the one namespace Scrob does
    not key shows on, so this walks: locally cached external_ids, then TMDB's find
    endpoint, matching the resulting show on tmdb_id and tvdb_id before creating a
    new row — otherwise a TVDB-sourced show would be duplicated.
    """
    from utils.alias_lookup import find_show_by_provider_id

    show = await find_show_by_provider_id(db, "imdb", _imdb_numeric(imdb_id))
    if show:
        return show

    local = await db.execute(
        select(Show)
        .where(Show.tmdb_data["external_ids"]["imdb_id"].astext == imdb_id)
        .limit(1)
    )
    show = local.scalars().first()
    if show:
        await _remember_imdb_alias(db, show.id, "series", imdb_id)
        return show

    if not api_key:
        return None

    try:
        res = await tmdb.find_by_external_id(imdb_id, "imdb_id", api_key=api_key)
    except Exception:
        return None

    series_tmdb_id = None
    if res.get("tv_results"):
        series_tmdb_id = res["tv_results"][0].get("id")
    elif res.get("tv_episode_results"):
        series_tmdb_id = res["tv_episode_results"][0].get("show_id")
    if not series_tmdb_id:
        return None

    by_tmdb = await db.execute(select(Show).where(Show.tmdb_id == series_tmdb_id).limit(1))
    show = by_tmdb.scalars().first()
    if show:
        await _remember_imdb_alias(db, show.id, "series", imdb_id)
        return show

    try:
        show_data = await tmdb.get_show(series_tmdb_id, api_key=api_key)
        tvdb_id = (show_data.get("external_ids") or {}).get("tvdb_id")
    except Exception:
        tvdb_id = None
    if tvdb_id:
        by_tvdb = await db.execute(select(Show).where(Show.tvdb_id == int(tvdb_id)).limit(1))
        show = by_tvdb.scalars().first()
        if show:
            if not show.tmdb_id:
                show.tmdb_id = series_tmdb_id
            await _remember_imdb_alias(db, show.id, "series", imdb_id)
            return show

    from routers.webhooks import _find_or_create_show

    try:
        show = await _find_or_create_show(db, series_tmdb_id, api_key)
    except Exception:
        return None
    if show:
        await _remember_imdb_alias(db, show.id, "series", imdb_id)
    return show


async def _movie_from_imdb_id(db: AsyncSession, imdb_id: str, api_key: Optional[str]) -> Optional[Media]:
    """Resolve an IMDb id to a movie Media row, creating and enriching it if needed."""
    from utils.alias_lookup import get_internal_id_for_uri

    internal_id = await get_internal_id_for_uri(db, f"imdb:m:{_imdb_numeric(imdb_id)}")
    if internal_id:
        by_alias = await db.execute(select(Media).where(Media.id == internal_id).limit(1))
        media = by_alias.scalars().first()
        if media:
            return media

    local = await db.execute(
        select(Media)
        .where(Media.media_type == MediaType.movie, Media.uri_id == f"imdb:m:{_imdb_numeric(imdb_id)}")
        .limit(1)
    )
    media = local.scalars().first()
    if media:
        return media

    if not api_key:
        return None

    try:
        res = await tmdb.find_by_external_id(imdb_id, "imdb_id", api_key=api_key)
    except Exception:
        return None
    results = res.get("movie_results") or []
    if not results:
        return None
    movie_tmdb_id = results[0].get("id")
    if not movie_tmdb_id:
        return None

    by_tmdb = await db.execute(
        select(Media)
        .where(Media.tmdb_id == movie_tmdb_id, Media.media_type == MediaType.movie)
        .limit(1)
    )
    media = by_tmdb.scalars().first()
    if media:
        await _remember_imdb_alias(db, media.id, "movie", imdb_id)
        return media

    media = Media(
        media_type=MediaType.movie,
        tmdb_id=movie_tmdb_id,
        uri_id=f"tmdb:m:{movie_tmdb_id}",
        title=results[0].get("title") or f"Movie {imdb_id}",
    )
    db.add(media)
    await db.flush()
    try:
        await enrich_media(media, api_key=api_key)
    except Exception:
        pass
    await _remember_imdb_alias(db, media.id, "movie", imdb_id)
    return media


async def _episode_for_show(
    db: AsyncSession,
    show: Show,
    season_num: int,
    episode_num: int,
    api_key: Optional[str],
    tvdb_key: Optional[str] = None,
) -> Optional[Media]:
    """Find (or create and enrich) the Media row for one episode of a known show."""
    existing = await db.execute(
        select(Media)
        .where(
            Media.media_type == MediaType.episode,
            Media.show_id == show.id,
            Media.season_number == season_num,
            Media.episode_number == episode_num,
        )
        .limit(1)
    )
    media = existing.scalars().first()
    if media:
        return media

    # Without a usable provider id there is nothing to enrich a new row from, and a
    # bare "Episode 6x6" placeholder in the history is worse than no entry at all.
    use_tvdb = bool(show.tvdb_id and tvdb_key and not show.tmdb_id)
    if not show.tmdb_id and not use_tvdb:
        return None

    placeholder = f"Episode {season_num}x{episode_num}"
    media = Media(
        media_type=MediaType.episode,
        show_id=show.id,
        season_number=season_num,
        episode_number=episode_num,
        title=placeholder,
    )
    db.add(media)
    await db.flush()
    try:
        await enrich_media(
            media,
            api_key=api_key,
            series_tmdb_id=show.tmdb_id,
            series_tvdb_id=show.tvdb_id,
            is_tvdb=use_tvdb,
            tvdb_api_key=tvdb_key,
            db=db,
        )
    except Exception:
        pass

    if media.title == placeholder:
        # The episode doesn't exist upstream (bad season/episode numbers, or the
        # provider doesn't carry it). Drop the row rather than leaving a nameless
        # entry behind, and let the caller offer manual linking instead.
        await db.delete(media)
        await db.flush()
        return None
    return media


async def resolve_media_item(
    item_id: str, media_type: str, db: AsyncSession, user: Optional[User] = None
) -> Optional[Media]:
    """Find target Media object in database from Stremio item ID (supporting series episodes tt1234:S:E)."""
    if not item_id:
        return None

    # A correction made from the result page pins this exact Stremio id, which is
    # the only way to fix a catalogue that numbers an episode differently.
    pinned = await _pinned_media(db, item_id)
    if pinned:
        return pinned

    parts = item_id.split(":")
    base_id = parts[0]
    season_num = int(parts[1]) if len(parts) >= 3 and parts[1].isdigit() else None
    episode_num = int(parts[2]) if len(parts) >= 3 and parts[2].isdigit() else None

    api_key = await _tmdb_key_for_user(db, user)
    tvdb_key = await _tvdb_key_for_user(db, user)

    # Handle IMDb IDs (e.g. tt7587890 or tt7587890:6:6)
    if base_id.startswith("tt"):
        if season_num is not None and episode_num is not None:
            show = await _show_from_imdb_id(db, base_id, api_key)
            if show:
                episode = await _episode_for_show(db, show, season_num, episode_num, api_key, tvdb_key)
                if episode:
                    return episode
            return None

        return await _movie_from_imdb_id(db, base_id, api_key)

    # Handle TMDB IDs (e.g. tmdb:10223 or tmdb:10223:6:6)
    elif item_id.startswith("tmdb:"):
        try:
            tmdb_val = int(parts[1])
            if len(parts) >= 4 and parts[2].isdigit() and parts[3].isdigit():
                season_num = int(parts[2])
                episode_num = int(parts[3])

            if season_num is not None and episode_num is not None:
                show_res = await db.execute(select(Show).where(Show.tmdb_id == tmdb_val).limit(1))
                show = show_res.scalars().first()
                if show:
                    episode = await _episode_for_show(db, show, season_num, episode_num, api_key, tvdb_key)
                    if episode:
                        return episode
                return None

            res = await db.execute(
                select(Media)
                .where(Media.tmdb_id == tmdb_val, Media.media_type == MediaType.movie)
                .limit(1)
            )
            found = res.scalars().first()
            if found:
                return found
        except (ValueError, IndexError):
            pass

    elif item_id.startswith("scrob:"):
        try:
            media_val = int(parts[1])
            res = await db.execute(select(Media).where(Media.id == media_val).limit(1))
            return res.scalars().first()
        except (ValueError, IndexError):
            pass

    return None


# ── Configuration & Options Endpoints ──────────────────────────────────────────

@router.get("")
@router.get("/")
@router.get("/configure")
async def stremio_configure_root():
    # No key in the path means there is nothing to configure yet — the manifest URL
    # that carries the key is issued from the app's own integrations settings.
    return RedirectResponse(url="/settings#tab-integrations", status_code=302)


class StremioConfigBody(BaseModel):
    catalogs: dict[str, bool] | None = None
    types: dict[str, bool] | None = None
    actions: dict[str, bool] | None = None
    enabled_list_ids: list[int] | None = None
    catalog_limit: int | None = None


@router.get("/{api_key}/configure")
async def stremio_configure(request: Request, api_key: str, db: AsyncSession = Depends(get_db)):
    """The addon's own configuration page, opened by Stremio's Configure button."""
    user = await get_user_from_key(api_key, db)
    if not user:
        return HTMLResponse(content="<h2>Unauthorized API Key</h2>", status_code=401)

    prefs = await _stremio_prefs(db, user)
    base_url = get_base_url(request)
    lists_res = await db.execute(
        select(ListModel).where(ListModel.user_id == user.id).order_by(ListModel.name)
    )
    user_lists = lists_res.scalars().all()
    enabled_ids = prefs["enabled_list_ids"]

    def toggle(name: str, label: str, desc: str, checked: bool) -> str:
        return (
            '<label class="toggle">'
            f'<input type="checkbox" data-key="{name}"{" checked" if checked else ""} />'
            f'<span class="toggle-text"><strong>{html_lib.escape(label)}</strong>'
            f'<small>{html_lib.escape(desc)}</small></span></label>'
        )

    catalog_rows = "".join([
        toggle("catalogs.watchlist", "Watchlist", "Everything in your Scrob library.", prefs["catalogs"]["watchlist"]),
        toggle("catalogs.history", "History", "Recently watched, newest first.", prefs["catalogs"]["history"]),
        toggle("catalogs.nextup", "Next Up", "The next unwatched episode of shows in progress.", prefs["catalogs"]["nextup"]),
        toggle("catalogs.lists", "Custom lists", "Your own Scrob lists, one catalog each.", prefs["catalogs"]["lists"]),
    ])
    type_rows = "".join([
        toggle("types.movie", "Movies", "Offer the movie version of each catalog.", prefs["types"]["movie"]),
        toggle("types.series", "Series", "Offer the series version of each catalog.", prefs["types"]["series"]),
    ])
    action_rows = "".join([
        toggle("actions.mark_watched", "Mark watched", "Adds a stream entry that logs a play immediately.", prefs["actions"]["mark_watched"]),
        toggle("actions.scrobble", "Scrobble session", "Adds a stream entry that starts a timed session.", prefs["actions"]["scrobble"]),
    ])

    list_rows = "".join(
        '<label class="toggle">'
        f'<input type="checkbox" data-list-id="{cl.id}"'
        f'{" checked" if (enabled_ids is None or cl.id in enabled_ids) else ""} />'
        f'<span class="toggle-text"><strong>{html_lib.escape(cl.name)}</strong></span></label>'
        for cl in user_lists
    ) or '<p class="empty">No custom lists yet.</p>'

    manifest_url = f"{base_url}/stremio/{api_key}/manifest.json"
    install_url = manifest_url.replace("https://", "stremio://").replace("http://", "stremio://")

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover" />
    <title>Scrob — Configure Addon</title>
    <meta name="color-scheme" content="dark light" />
    <style>{_PAGE_CSS}
        .shell {{ max-width: 560px; }}
        h1 {{ margin-bottom: 4px; }}
        .group {{ margin-top: 22px; }}
        .group > label.section {{ display: block; color: var(--text-muted); font-size: 0.66rem;
          font-weight: 800; text-transform: uppercase; letter-spacing: 0.14em; margin-bottom: 8px; }}
        .toggle {{ display: flex; align-items: flex-start; gap: 11px; padding: 12px 13px;
          background: var(--field-bg); border: 1px solid var(--glass-border);
          border-radius: 14px; margin-bottom: 7px; cursor: pointer;
          -webkit-tap-highlight-color: transparent; }}
        .toggle input {{ margin-top: 1px; width: 18px; height: 18px; flex-shrink: 0;
          accent-color: var(--accent); cursor: pointer; }}
        .toggle-text {{ min-width: 0; }}
        .toggle-text strong {{ display: block; font-size: 0.88rem; font-weight: 650; overflow-wrap: anywhere; }}
        .toggle-text small {{ display: block; color: var(--text-muted); font-size: 0.76rem; margin-top: 2px; }}
        .empty {{ color: var(--text-dim); font-size: 0.8rem; text-align: left; }}
        .limit {{ display: flex; align-items: center; flex-wrap: wrap; gap: 10px; padding: 11px 13px;
          background: var(--field-bg); border: 1px solid var(--glass-border); border-radius: 14px; }}
        .limit input {{ width: 92px; min-height: 40px; background: var(--bg-page);
          border: 1px solid var(--glass-border); color: var(--text); border-radius: 10px;
          padding: 7px 10px; font-family: inherit; font-size: 16px; }}
        .limit span {{ font-size: 0.84rem; color: var(--text-muted); }}
        .url {{ display: flex; flex-wrap: wrap; gap: 8px; }}
        .url input {{ flex: 1 1 200px; min-width: 0; min-height: 44px; background: var(--field-bg);
          border: 1px solid var(--glass-border); color: var(--text-muted); border-radius: 12px;
          padding: 10px 12px; font-size: 0.74rem; font-family: ui-monospace, SFMono-Regular, monospace; }}
        .url .btn {{ flex: 0 0 auto; }}
        .status {{ margin-top: 14px; font-size: 0.84rem; text-align: center; min-height: 1.2em; }}
        .status.ok {{ color: var(--ok); }}
        .status.err {{ color: #f87171; }}
        @media (max-width: 420px) {{
          .group {{ margin-top: 18px; }}
          .url .btn {{ flex: 1 1 100%; }}
        }}
    </style>
    {_THEME_BOOTSTRAP}
</head>
<body>
    <div class="shell">
        <div class="glass-card">
            {_BRAND_MARK}
            <h1>Configure Addon</h1>
            <p>Pick what Scrob puts into Stremio. Changes apply the next time Stremio reloads the addon.</p>

            <div class="group"><label class="section">Catalogs</label>{catalog_rows}</div>
            <div class="group"><label class="section">Types</label>{type_rows}</div>
            <div class="group"><label class="section">Stream actions</label>{action_rows}</div>
            <div class="group"><label class="section">Custom lists</label>{list_rows}</div>
            <div class="group"><label class="section">Catalog size</label>
                <div class="limit">
                    <input id="limit" type="number" min="1" max="200" value="{prefs['catalog_limit']}" />
                    <span>items per catalog (1–200)</span>
                </div>
            </div>

            <div class="group"><label class="section">Manifest URL</label>
                <div class="url">
                    <input id="manifest" readonly value="{html_lib.escape(manifest_url)}" />
                    <button class="btn" type="button" id="copy-btn">Copy</button>
                </div>
            </div>

            <div class="actions">
                <button class="btn primary" type="button" id="save-btn">Save</button>
                <a class="btn" href="{html_lib.escape(install_url)}">Install in Stremio</a>
            </div>
            <div class="status" id="status"></div>
            {_nav_actions(base_url)}
        </div>
    </div>
    <script>
        const apiKey = {_js_literal(api_key)};
        const statusEl = document.getElementById('status');

        function setStatus(msg, cls) {{
            statusEl.textContent = msg;
            statusEl.className = 'status ' + (cls || '');
        }}

        document.getElementById('copy-btn').addEventListener('click', () => {{
            const field = document.getElementById('manifest');
            field.select();
            navigator.clipboard.writeText(field.value);
            setStatus('Manifest URL copied.', 'ok');
        }});

        document.getElementById('save-btn').addEventListener('click', async () => {{
            const payload = {{ catalogs: {{}}, types: {{}}, actions: {{}} }};
            document.querySelectorAll('input[data-key]').forEach(el => {{
                const [group, key] = el.dataset.key.split('.');
                payload[group][key] = el.checked;
            }});

            const listInputs = Array.from(document.querySelectorAll('input[data-list-id]'));
            // All ticked means "every list", including ones added later.
            payload.enabled_list_ids = listInputs.every(el => el.checked)
                ? null
                : listInputs.filter(el => el.checked).map(el => Number(el.dataset.listId));

            payload.catalog_limit = Number(document.getElementById('limit').value) || 50;

            setStatus('Saving...');
            try {{
                const res = await fetch(`/stremio/${{apiKey}}/config`, {{
                    method: 'POST',
                    headers: {{ 'Content-Type': 'application/json' }},
                    body: JSON.stringify(payload),
                }});
                if (!res.ok) {{
                    setStatus('Could not save those settings.', 'err');
                    return;
                }}
                setStatus('Saved. Reload the addon in Stremio to pick up catalog changes.', 'ok');
            }} catch (e) {{
                setStatus('Request failed.', 'err');
            }}
        }});
    </script>
</body>
</html>"""
    return add_cors_headers(HTMLResponse(content=html))


@router.post("/{api_key}/config")
async def save_stremio_config(api_key: str, body: StremioConfigBody, db: AsyncSession = Depends(get_db)):
    user = await get_user_from_key(api_key, db)
    if not user:
        return JSONResponse(content={"detail": "Unauthorized API key"}, status_code=401)

    prefs = _merge_stremio_prefs(body.model_dump(exclude_none=False))
    await _save_stremio_prefs(db, user, prefs)
    return add_cors_headers(JSONResponse(content={"status": "ok", "config": prefs}))


@router.options("/{full_path:path}")
async def stremio_options(full_path: str):
    response = Response(status_code=200)
    return add_cors_headers(response)


# ── Manifest Endpoints ─────────────────────────────────────────────────────────

@router.get("/manifest.json")
@router.get("/{api_key}/manifest.json")
async def get_stremio_manifest(request: Request, api_key: Optional[str] = None, db: AsyncSession = Depends(get_db)):
    user = await get_user_from_key(api_key, db) if api_key else None
    prefs = await _stremio_prefs(db, user)
    enabled_types = [t for t in ("movie", "series") if prefs["types"][t]] or ["movie", "series"]

    catalogs: list[dict] = []
    for key, name in (("watchlist", "Scrob Watchlist"), ("history", "Scrob History"), ("nextup", "Scrob Next Up")):
        if not prefs["catalogs"][key]:
            continue
        for media_type in enabled_types:
            catalogs.append({"type": media_type, "id": f"scrob_{key}", "name": name})

    manifest = {
        "id": "net.deltahub.scrob.stremio",
        "version": "1.0.0",
        "name": "Scrob",
        "description": "Sync your personal Scrob watchlists, history, and custom lists into Stremio",
        "resources": ["catalog", "meta", "stream"],
        "types": enabled_types,
        "catalogs": catalogs,
        "behaviorHints": {
            "configurable": True,
            "configurationRequired": False if api_key else True,
            # Stremio opens this from its own origin, so it has to be absolute.
            "configurationURL": (
                f"{get_base_url(request)}/stremio/{api_key}/configure" if api_key
                else f"{get_base_url(request)}/settings#tab-integrations"
            ),
        }
    }

    if user and prefs["catalogs"]["lists"]:
        custom_lists_res = await db.execute(select(ListModel).where(ListModel.user_id == user.id))
        enabled_ids = prefs["enabled_list_ids"]
        for cl in custom_lists_res.scalars().all():
            if enabled_ids is not None and cl.id not in enabled_ids:
                continue
            for media_type in enabled_types:
                manifest["catalogs"].append({
                    "type": media_type,
                    "id": f"scrob_list_{cl.id}",
                    "name": f"Scrob: {cl.name}"
                })

    response = JSONResponse(content=manifest)
    return add_cors_headers(response)


# ── Catalog Endpoints ──────────────────────────────────────────────────────────

@router.get("/{api_key}/catalog/{type}/{catalog_id}.json")
@router.get("/{api_key}/catalog/{type}/{catalog_id}/skip={skip}.json")
async def get_stremio_catalog(
    api_key: str,
    type: str,
    catalog_id: str,
    skip: int = 0,
    db: AsyncSession = Depends(get_db)
):
    user = await get_user_from_key(api_key, db)
    if not user:
        response = JSONResponse(status_code=401, content={"metas": [], "error": "Invalid API key"})
        return add_cors_headers(response)

    prefs = await _stremio_prefs(db, user)
    metas = []
    limit = prefs["catalog_limit"]

    try:
        if catalog_id == "scrob_history":
            query = select(WatchEvent).where(
                WatchEvent.user_id == user.id
            ).options(selectinload(WatchEvent.media).selectinload(Media.show)).order_by(desc(WatchEvent.watched_at)).offset(skip).limit(limit * 2)

            res = await db.execute(query)
            events = res.scalars().all()

            seen_ids = set()
            for e in events:
                if not e.media:
                    continue
                
                target_item = e.media
                if type == "series":
                    if e.media.show:
                        target_item = e.media.show
                    elif e.media.media_type != MediaType.series:
                        # An episode with no show row has no series-level id to
                        # hand Stremio, so there is nothing useful to list.
                        continue
                elif e.media.media_type != MediaType.movie:
                    continue

                meta = media_to_stremio_meta(target_item)
                if meta and meta["type"] == type and meta["id"] not in seen_ids:
                    seen_ids.add(meta["id"])
                    metas.append(meta)
                    if len(metas) >= limit:
                        break

        elif catalog_id.startswith("scrob_list_"):
            try:
                list_id = int(catalog_id.replace("scrob_list_", ""))
                # A Scrob list holds both kinds, but each Stremio catalog is typed —
                # filter in SQL so paging stays correct instead of dropping rows after
                # the limit has already been applied.
                wanted_types = (
                    [MediaType.series, MediaType.episode]
                    if type == "series"
                    else [MediaType.movie]
                )
                query = (
                    select(ListItem)
                    .join(Media, Media.id == ListItem.media_id)
                    .where(ListItem.list_id == list_id, Media.media_type.in_(wanted_types))
                    .options(selectinload(ListItem.media).selectinload(Media.show))
                    .order_by(desc(ListItem.added_at))
                    .offset(skip)
                    .limit(limit)
                )

                res = await db.execute(query)
                items = res.scalars().all()
                seen_ids = set()
                for item in items:
                    if not item.media:
                        continue
                    # Episodes are represented by their show — several episodes of the
                    # same show collapse into one catalog entry. An episode with no
                    # show row has no series-level id to offer, so it is skipped.
                    if type == "series":
                        if item.media.show:
                            target_item = item.media.show
                        elif item.media.media_type == MediaType.series:
                            target_item = item.media
                        else:
                            continue
                    else:
                        target_item = item.media
                    meta = media_to_stremio_meta(target_item)
                    if meta and meta["type"] == type and meta["id"] not in seen_ids:
                        seen_ids.add(meta["id"])
                        metas.append(meta)
            except ValueError:
                pass

        else:
            # Watchlist / General catalog for requested type. Scoped to this user's
            # own collection — the API key identifies one user, so a catalog must
            # never expose another account's library.
            if type == "series":
                query = (
                    select(Show)
                    .join(Media, Media.show_id == Show.id)
                    .join(Collection, Collection.media_id == Media.id)
                    .where(Collection.user_id == user.id)
                    .distinct()
                    .order_by(desc(Show.updated_at))
                    .offset(skip)
                    .limit(limit)
                )
                res = await db.execute(query)
                shows = res.scalars().all()
                metas = [media_to_stremio_meta(s) for s in shows if s]
            else:
                query = (
                    select(Media)
                    .join(Collection, Collection.media_id == Media.id)
                    .where(Collection.user_id == user.id, Media.media_type == MediaType.movie)
                    .order_by(desc(Media.updated_at))
                    .offset(skip)
                    .limit(limit)
                )
                res = await db.execute(query)
                movies = res.scalars().all()
                metas = [media_to_stremio_meta(m) for m in movies if m]

            metas = [m for m in metas if m and m["type"] == type]

    except Exception as e:
        logger.error(f"Error fetching catalog {catalog_id}: {e}", exc_info=True)

    response = JSONResponse(content={"metas": metas})
    return add_cors_headers(response)


# ── Meta Endpoints ─────────────────────────────────────────────────────────────

@router.get("/{api_key}/meta/{type}/{id}.json")
async def get_stremio_meta(request: Request, api_key: str, type: str, id: str, db: AsyncSession = Depends(get_db)):
    user = await get_user_from_key(api_key, db)
    base_url = get_base_url(request)
    
    item = None
    if id.startswith("tt"):
        clean_imdb = id.split(":")[0].lstrip("t")
        if type == "series":
            res = await db.execute(select(Show).where(Show.uri_id == f"imdb:s:{clean_imdb}").limit(1))
            item = res.scalar_one_or_none()
        if not item:
            res = await db.execute(select(Media).where(Media.uri_id == f"imdb:m:{clean_imdb}").limit(1))
            item = res.scalar_one_or_none()
    elif id.startswith("tmdb:"):
        try:
            tmdb_id = int(id.split(":")[1])
            if type == "series":
                res = await db.execute(select(Show).where(Show.tmdb_id == tmdb_id).limit(1))
                item = res.scalar_one_or_none()
            if not item:
                res = await db.execute(select(Media).where(Media.tmdb_id == tmdb_id).limit(1))
                item = res.scalar_one_or_none()
        except (ValueError, IndexError):
            pass

    if item:
        meta = media_to_stremio_meta(item)
    else:
        meta = {
            "id": id,
            "type": type,
            "name": f"Title {id}"
        }

    meta["links"] = [
        {
            "name": "View on Scrob",
            "category": "web",
            "url": f"{base_url}/media/{id}"
        },
        {
            "name": "⚡ Mark Watched on Scrob",
            "category": "web",
            "url": f"{base_url}/stremio/{api_key}/mark-watched?id={id}&type={type}"
        }
    ]

    response = JSONResponse(content={"meta": meta})
    return add_cors_headers(response)


# ── Stream Endpoints ────────────────────────────────────────────────────────────

@router.get("/{api_key}/stream/{type}/{id}.json")
async def get_stremio_stream(request: Request, api_key: str, type: str, id: str, db: AsyncSession = Depends(get_db)):
    user = await get_user_from_key(api_key, db)
    base_url = get_base_url(request)
    streams = []

    if user:
        prefs = await _stremio_prefs(db, user)
        if prefs["actions"]["mark_watched"]:
            action_title = "⚡ Mark Episode Watched on Scrob" if (type == "series" or ":" in id) else "⚡ Mark Watched on Scrob"
            streams.append({
                "name": "Scrob",
                "title": action_title,
                "externalUrl": f"{base_url}/stremio/{api_key}/mark-watched?id={id}&type={type}"
            })
        if prefs["actions"]["scrobble"]:
            streams.append({
                "name": "Scrob",
                "title": "▶ Start Scrobbling on Scrob",
                "externalUrl": f"{base_url}/stremio/{api_key}/scrobble-session?id={id}&type={type}"
            })

    response = JSONResponse(content={"streams": streams})
    return add_cors_headers(response)


def _stremio_session_key(user_id: int, media_id: int) -> str:
    # Reuses the "manual" source so the existing auto-complete sweep in
    # routers.history picks these sessions up once the runtime has elapsed.
    return f"manual-{user_id}-{media_id}"


async def _log_watch_event(db: AsyncSession, user_id: int, media_id: int) -> bool:
    """Record a completed play, ignoring a repeat of one just logged.

    Stremio (and the browser it hands the externalUrl to) can fire the same link
    more than once for a single click, so without this guard one tap on "mark
    watched" lands two WatchEvent rows. Returns False when the call was a repeat.
    """
    recent_cutoff = datetime.utcnow() - timedelta(minutes=5)
    existing = await db.execute(
        select(WatchEvent.id).where(
            WatchEvent.user_id == user_id,
            WatchEvent.media_id == media_id,
            # NULL >= cutoff is false in SQL, so an unknown-dated event needs an
            # explicit OR to still count as "already logged".
            or_(WatchEvent.watched_at.is_(None), WatchEvent.watched_at >= recent_cutoff),
        ).limit(1)
    )
    if existing.scalar_one_or_none() is not None:
        return False

    db.add(WatchEvent(
        user_id=user_id,
        media_id=media_id,
        watched_at=datetime.utcnow(),
        completed=True,
        play_count=1,
        progress_percent=1.0,
    ))
    # The play is done, so it no longer belongs in Continue Watching or Now Playing.
    await db.execute(
        delete(PlaybackProgress).where(
            PlaybackProgress.user_id == user_id,
            PlaybackProgress.media_id == media_id,
        )
    )
    await db.execute(
        delete(PlaybackSession).where(
            PlaybackSession.user_id == user_id,
            PlaybackSession.media_id == media_id,
        )
    )
    return True


async def _start_playback_session(db: AsyncSession, user_id: int, media: Media) -> None:
    """Open (or restart) a playback session for a title being watched in Stremio.

    Nothing is written to history here — the session shows up under Now Playing and
    is completed by ``auto_complete_manual_sessions`` once the runtime has elapsed.
    """
    session_key = _stremio_session_key(user_id, media.id)
    await db.execute(delete(PlaybackSession).where(PlaybackSession.session_key == session_key))
    db.add(PlaybackSession(
        user_id=user_id,
        media_id=media.id,
        session_key=session_key,
        source="manual",
        state="playing",
        progress_seconds=0,
        progress_percent=0.0,
    ))


async def _describe_media(db: AsyncSession, media: Media) -> dict:
    """HTML-escaped display strings for the confirmation pages.

    Episodes are shown as the show plus SxxEyy plus the episode title, since the
    bare episode title on its own rarely says what was actually logged.
    """
    title = media.custom_title or media.title
    year = (media.release_date or "")[:4]

    if media.media_type == MediaType.episode:
        show_title = None
        show_poster = None
        if media.show_id:
            show_res = await db.execute(select(Show).where(Show.id == media.show_id))
            show = show_res.scalar_one_or_none()
            if show:
                show_title = show.custom_title or show.title
                show_poster = show.poster_path
        show_title = show_title or (media.tmdb_data or {}).get("show_title")

        code = ""
        if media.season_number is not None and media.episode_number is not None:
            code = f"S{media.season_number:02d}E{media.episode_number:02d}"

        heading = show_title or title
        subtitle_parts = [p for p in (code, title if title != heading else None) if p]
        return {
            "kind": "Episode",
            "heading": html_lib.escape(heading),
            "subtitle": html_lib.escape(" · ".join(subtitle_parts)),
            # The show poster reads better at card size than an episode still.
            "poster": show_poster or media.poster_path,
        }

    return {
        "kind": "Series" if media.media_type == MediaType.series else "Movie",
        "heading": html_lib.escape(title),
        "subtitle": html_lib.escape(year),
        "poster": media.poster_path,
    }


# Mirrors the app's own tokens (frontend/src/styles/global.css) so these pages,
# which Stremio opens in a plain browser tab outside the Astro app, still look
# like Scrob rather than a bare error page.
_PAGE_CSS = """
    :root {
      --bg-page: #09090b; --bg-card: #18181b;
      --glass-bg: rgba(24, 24, 27, 0.4); --glass-border: rgba(255, 255, 255, 0.05);
      --text: #f4f4f5; --text-muted: #a1a1aa; --text-dim: #71717a;
      --accent: #3b82f6; --ok: #10b981; --warn: #f59e0b;
      --field-bg: rgba(9, 9, 11, 0.6);
    }
    /* Dark is the default; the bootstrap script adds .light when the user has
       chosen the light theme in the app, which is the same signal Base.astro uses. */
    :root.light {
      --bg-page: #e7e9ed; --bg-card: #f4f5f7;
      --glass-bg: rgba(244, 245, 247, 0.55); --glass-border: rgba(0, 0, 0, 0.08);
      --text: #18181b; --text-muted: #52525b; --text-dim: #71717a;
      --field-bg: rgba(255, 255, 255, 0.7);
    }
    * { box-sizing: border-box; }
    html { -webkit-text-size-adjust: 100%; }
    body {
      font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', sans-serif;
      background: var(--bg-page); color: var(--text);
      display: flex;
      min-height: 100vh;
      /* Dynamic units so mobile browser chrome doesn't cut the card off. */
      min-height: 100dvh;
      margin: 0;
      padding: max(16px, env(safe-area-inset-top)) max(16px, env(safe-area-inset-right))
               max(16px, env(safe-area-inset-bottom)) max(16px, env(safe-area-inset-left));
      background-image:
        radial-gradient(circle at 15% 0%, rgba(59, 130, 246, 0.18), transparent 45%),
        radial-gradient(circle at 85% 100%, rgba(139, 92, 246, 0.14), transparent 45%);
      background-attachment: fixed;
    }
    /* auto margins rather than align-items:center — a card taller than the
       viewport still scrolls to its top instead of being clipped. */
    .shell { width: 100%; max-width: 460px; margin: auto; }
    .glass-card {
      background-color: var(--glass-bg); border: 1px solid var(--glass-border);
      backdrop-filter: blur(12px); -webkit-backdrop-filter: blur(12px);
      box-shadow: 0 4px 30px rgba(0, 0, 0, 0.25);
      border-radius: 24px; padding: 28px;
    }
    .brand { display: flex; align-items: center; gap: 8px; justify-content: center; margin-bottom: 18px; }
    .brand span { font-size: 0.7rem; font-weight: 800; letter-spacing: 0.22em; text-transform: uppercase; color: var(--text-dim); }
    h1 { font-size: 1.3rem; margin: 0 0 6px; text-align: center; letter-spacing: -0.01em; }
    h1.ok { color: var(--ok); }
    h1.warn { color: var(--warn); }
    p { color: var(--text-muted); font-size: 0.9rem; line-height: 1.5; margin: 6px 0; text-align: center; }
    .media { display: flex; gap: 14px; align-items: center; text-align: left;
      background: var(--field-bg); border: 1px solid var(--glass-border);
      border-radius: 16px; padding: 12px; margin: 18px 0; }
    .poster { width: 58px; height: 87px; border-radius: 10px; object-fit: cover; flex-shrink: 0;
      border: 1px solid var(--glass-border); background: var(--bg-card); }
    .poster.empty { display: flex; align-items: center; justify-content: center;
      font-size: 0.5rem; font-weight: 800; color: var(--text-dim); letter-spacing: 0.05em; }
    .media-text { min-width: 0; }
    .kind { color: var(--text-dim); font-size: 0.62rem; font-weight: 800; text-transform: uppercase; letter-spacing: 0.12em; }
    .heading { font-size: 1rem; font-weight: 700; margin: 3px 0 2px; overflow-wrap: anywhere; }
    .subtitle { color: var(--text-muted); font-size: 0.85rem; margin: 0; overflow-wrap: anywhere; }
    .actions { display: flex; flex-wrap: wrap; gap: 8px; margin-top: 20px; }
    .btn { flex: 1 1 140px; display: inline-flex; align-items: center; justify-content: center; gap: 6px;
      min-height: 44px; padding: 11px 14px; border-radius: 14px; font-size: 0.85rem; font-weight: 700;
      border: 1px solid var(--glass-border); background: var(--field-bg); color: var(--text);
      text-decoration: none; cursor: pointer; transition: all 0.2s ease; font-family: inherit;
      -webkit-tap-highlight-color: transparent; }
    .btn.primary { background: var(--accent); border-color: transparent; color: #fff; }
    .btn:disabled { opacity: 0.5; cursor: not-allowed; }
    /* Hover lift only where there is a real pointer — on touch it sticks after a tap. */
    @media (hover: hover) {
      .btn:hover { border-color: rgba(59, 130, 246, 0.4); transform: translateY(-1px); }
      .btn.primary:hover { background: #60a5fa; }
      .btn:disabled:hover { transform: none; }
    }
    .btn:active { transform: scale(0.98); }
    .fixup { margin-top: 14px; text-align: center; }
    .fixup a { color: var(--text-dim); font-size: 0.76rem; text-decoration: none; border-bottom: 1px dashed var(--text-dim); }
    .fixup a:hover { color: var(--accent); border-color: var(--accent); }

    @media (max-width: 420px) {
      .glass-card { padding: 20px 18px; border-radius: 20px; }
      h1 { font-size: 1.15rem; }
      p { font-size: 0.86rem; }
      .media { gap: 11px; padding: 10px; margin: 14px 0; }
      .poster { width: 48px; height: 72px; border-radius: 8px; }
      .heading { font-size: 0.94rem; }
      .subtitle { font-size: 0.8rem; }
      /* Below this width two buttons side by side start truncating their labels. */
      .btn { flex: 1 1 100%; }
    }
    @media (min-width: 640px) {
      .glass-card { padding: 32px; }
      h1 { font-size: 1.4rem; }
    }
    @media (prefers-reduced-motion: reduce) {
      .btn, .btn:hover, .btn:active { transition: none; transform: none; }
    }
"""

# The app's own mark (frontend/public/scrob.svg), inlined so the page renders it
# whichever origin it is opened from.
_BRAND_MARK = (
    '<div class="brand">'
    '<svg width="20" height="22" viewBox="0 0 419 454" aria-hidden="true">'
    '<defs>'
    '<linearGradient id="ringGrad" gradientUnits="userSpaceOnUse" x1="0" y1="0" x2="0" y2="454">'
    '<stop offset="0%" stop-color="#5B34D6"/><stop offset="50%" stop-color="#9E3BC1"/>'
    '<stop offset="100%" stop-color="#C147D8"/></linearGradient>'
    '<linearGradient id="dotGrad" gradientUnits="objectBoundingBox" x1="0" y1="1" x2="0" y2="0">'
    '<stop offset="0%" stop-color="#5B34D6"/><stop offset="100%" stop-color="#C147D8"/>'
    '</linearGradient>'
    '</defs>'
    '<path d="M 394.09 73.88 A 226.5 226.5 0 1 0 332.74 427.26 L 287.64 358.22 '
    'A 144.6 144.6 0 1 1 334.56 130.14 Z" fill="url(#ringGrad)"/>'
    '<circle cx="368.97" cy="347.2" r="48.29" fill="url(#dotGrad)"/>'
    '</svg>'
    '<span>Scrob</span></div>'
)

# Runs before paint. Matches Base.astro: an explicit choice in localStorage wins,
# and anything else falls back to dark.
_THEME_BOOTSTRAP = (
    "<script>(function(){try{"
    "if(localStorage.getItem('theme')==='light')"
    "document.documentElement.classList.add('light');"
    "}catch(e){}})();</script>"
)


def _poster_markup(poster_url: Optional[str]) -> str:
    if poster_url:
        return f'<img class="poster" src="{html_lib.escape(poster_url)}" alt="" loading="lazy" />'
    return '<div class="poster empty">NO ART</div>'


def _details_block(details: dict) -> str:
    subtitle = f'<p class="subtitle">{details["subtitle"]}</p>' if details["subtitle"] else ""
    return (
        '<div class="media">'
        f'{_poster_markup(details.get("poster"))}'
        '<div class="media-text">'
        f'<div class="kind">{details["kind"]}</div>'
        f'<div class="heading">{details["heading"]}</div>'
        f'{subtitle}'
        '</div></div>'
    )


def _nav_actions(base_url: str) -> str:
    """Somewhere to go next: Stremio drops the user in a bare tab with no chrome.

    No close button — window.close() is refused for a tab the page did not open
    itself, so it silently did nothing.
    """
    return (
        '<div class="actions">'
        f'<a class="btn primary" href="{html_lib.escape(base_url)}/">Open Scrob</a>'
        '</div>'
    )


def _fixup_link(base_url: str, api_key: str, item_id: str, media_type: str, action: str) -> str:
    """Escape hatch for a wrong automatic match."""
    href = (
        f"{base_url}/stremio/{api_key}/relink"
        f"?id={urllib.parse.quote(item_id)}&type={urllib.parse.quote(media_type)}"
        f"&action={urllib.parse.quote(action)}"
    )
    return f'<div class="fixup"><a href="{html_lib.escape(href)}">Wrong title? Match it manually</a></div>'


def _result_page(
    *,
    title: str,
    heading: str,
    heading_class: str,
    note: str,
    details: dict,
    base_url: str,
    fixup: str = "",
) -> str:
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover" />
    <meta name="color-scheme" content="dark light" />
    <title>{html_lib.escape(title)}</title>
    <style>{_PAGE_CSS}</style>
    {_THEME_BOOTSTRAP}
</head>
<body>
    <div class="shell">
        <div class="glass-card">
            {_BRAND_MARK}
            <h1 class="{heading_class}">{heading}</h1>
            {_details_block(details)}
            <p>{note}</p>
            {_nav_actions(base_url)}
            {fixup}
        </div>
    </div>
</body>
</html>"""


def _unmatched_html(
    api_key: str,
    item_id: str,
    media_type: str,
    action: str,
    base_url: str = "",
    current: Optional[dict] = None,
    status_code: int = 404,
) -> HTMLResponse:
    """Offer to link the title by hand, either because nothing matched or because
    the automatic match was wrong.

    Stremio only ever hands us an IMDb id, and Scrob keys shows on TMDB/TVDB, so a
    miss here is usually a missing cross-reference rather than a missing title. The
    page lets the user search their own library or paste an IMDb/TMDB/TVDB id; the
    link is remembered as a media alias so this only has to be done once per title.
    """
    kind = "series" if (media_type == "series" or ":" in item_id) else "movie"
    # The id comes straight off a Stremio URL, so escape it for HTML and hand the
    # script its values as JSON literals rather than interpolating them raw.
    item_id_text = html_lib.escape(item_id)
    id_parts = item_id.split(":")
    detected_season = id_parts[1] if len(id_parts) >= 3 and id_parts[1].isdigit() else ""
    detected_episode = id_parts[2] if len(id_parts) >= 3 and id_parts[2].isdigit() else ""
    js_api_key = _js_literal(api_key)
    js_item_id = _js_literal(item_id)
    js_kind = _js_literal(kind)
    js_action = _js_literal(action)
    heading = "Wrong title?" if current else "Couldn't match this title"
    lead = (
        "Search your library or paste an IMDb, TMDB or TVDB id to point this Stremio "
        f"entry at the right {kind} instead."
        if current
        else f"<code>{item_id_text}</code> isn't linked to anything in Scrob yet, so nothing was logged."
    )
    current_block = _details_block(current) if current else ""

    # Catalogues disagree about episode numbering, so an id can land one episode
    # off inside the right show. That is a different fix from linking a title.
    episode_fix_block = ""
    if detected_season and detected_episode:
        episode_fix_block = f'''
            <label for="fix-season">Wrong episode of this show?</label>
            <div class="row">
                <input id="fix-season" type="number" min="0" value="{detected_season}" aria-label="Season" />
                <input id="fix-episode" type="number" min="1" value="{detected_episode}" aria-label="Episode" />
                <button class="btn primary" id="fix-btn" type="button">Use this</button>
            </div>
            <div class="hint">Pins this Stremio entry to the episode you pick and undoes the wrong log.</div>'''
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover" />
    <title>Scrob — Link This Title</title>
    <meta name="color-scheme" content="dark light" />
    <style>{_PAGE_CSS}
        .shell {{ max-width: 480px; }}
        h1 {{ color: var(--warn); }}
        label {{ display: block; color: var(--text-muted); font-size: 0.66rem; font-weight: 800;
          margin: 18px 0 6px; text-transform: uppercase; letter-spacing: 0.14em; }}
        input {{ width: 100%; min-width: 0; min-height: 44px; background: var(--field-bg);
          border: 1px solid var(--glass-border); color: var(--text); border-radius: 12px;
          padding: 11px 13px; font-size: 16px; font-family: inherit; }}
        input:focus {{ outline: none; border-color: var(--accent); }}
        code {{ color: var(--text); font-size: 0.85rem; overflow-wrap: anywhere; }}
        .row {{ display: flex; gap: 8px; align-items: stretch; }}
        .row input {{ flex: 1 1 auto; }}
        .row .btn {{ flex: 0 0 auto; }}
        .results {{ margin-top: 10px; max-height: 240px; overflow-y: auto;
          -webkit-overflow-scrolling: touch; }}
        .result {{ display: flex; justify-content: space-between; align-items: center; gap: 10px;
          background: var(--field-bg); border: 1px solid var(--glass-border);
          border-radius: 12px; padding: 10px 12px; margin-bottom: 8px; }}
        .result > span {{ font-size: 0.86rem; min-width: 0; flex: 1; overflow-wrap: anywhere; }}
        .result-poster {{ width: 34px; height: 51px; border-radius: 7px; object-fit: cover;
          flex-shrink: 0; border: 1px solid var(--glass-border); background: var(--bg-card); }}
        .result-poster.empty {{ display: flex; align-items: center; justify-content: center;
          font-size: 0.42rem; font-weight: 800; color: var(--text-dim); }}
        .result small {{ color: var(--text-dim); display: block; }}
        .result .btn {{ flex: 0 0 auto; min-height: 36px; padding: 7px 12px; font-size: 0.74rem; }}
        .status {{ margin-top: 14px; font-size: 0.84rem; text-align: center; min-height: 1.2em; }}
        .status.ok {{ color: var(--ok); }}
        .status.err {{ color: #f87171; }}
        .hint {{ color: var(--text-dim); font-size: 0.74rem; margin-top: 6px; text-align: left; }}
        @media (max-width: 420px) {{
          /* The search/link rows keep their own layout — a full-width button under
             a full-width field reads better than a squeezed pair. */
          .row {{ flex-wrap: wrap; }}
          .row input {{ flex: 1 1 100%; }}
          .row .btn {{ flex: 1 1 100%; }}
        }}
    </style>
    {_THEME_BOOTSTRAP}
</head>
<body>
    <div class="shell">
        <div class="glass-card">
            {_BRAND_MARK}
            <h1>{heading}</h1>
            {current_block}
            <p>{lead}</p>

            {episode_fix_block}

            <label for="q">Search Scrob</label>
            <div class="row">
                <input id="q" type="text" placeholder="Title..." autocomplete="off" />
                <button class="btn primary" id="search-btn" type="button">Search</button>
            </div>
            <div class="results" id="results"></div>

            <label for="ext">Or provide an id</label>
            <div class="row">
                <input id="ext" type="text" placeholder="tt7587890, tmdb:79744, tvdb:350665" autocomplete="off" />
                <button class="btn primary" id="link-btn" type="button">Link</button>
            </div>
            <div class="hint">A bare number is read as a TMDB id.</div>

            <div class="status" id="status"></div>
            {_nav_actions(base_url)}
        </div>
        </div>
        <script>
            const apiKey = {js_api_key};
            const itemId = {js_item_id};
            const kind = {js_kind};
            const action = {js_action};
            const statusEl = document.getElementById('status');
            const resultsEl = document.getElementById('results');

            function setStatus(msg, cls) {{
                statusEl.textContent = msg;
                statusEl.className = 'status ' + (cls || '');
            }}

            async function submitTarget(target) {{
                setStatus('Linking...');
                try {{
                    const res = await fetch(`/stremio/${{apiKey}}/link`, {{
                        method: 'POST',
                        headers: {{ 'Content-Type': 'application/json' }},
                        body: JSON.stringify({{ id: itemId, type: kind, action: action, target: target }}),
                    }});
                    const data = await res.json();
                    if (!res.ok) {{
                        setStatus(data.detail || 'Could not link that id.', 'err');
                        return;
                    }}
                    setStatus(`Linked to ${{data.title}} and it ${{data.message}}.`, 'ok');
                    document.getElementById('link-btn').disabled = true;
                    document.getElementById('search-btn').disabled = true;
                }} catch (e) {{
                    setStatus('Request failed.', 'err');
                }}
            }}

            async function runSearch() {{
                const q = document.getElementById('q').value.trim();
                if (!q) return;
                resultsEl.innerHTML = '';
                setStatus('Searching...');
                try {{
                    const url = `/stremio/${{apiKey}}/link/search?q=${{encodeURIComponent(q)}}&type=${{kind}}`;
                    const res = await fetch(url);
                    const data = await res.json();
                    const items = data.results || [];
                    if (!items.length) {{
                        setStatus('Nothing found in your library. Try an id instead.', 'err');
                        return;
                    }}
                    setStatus('');
                    for (const item of items) {{
                        const row = document.createElement('div');
                        row.className = 'result';
                        if (item.poster) {{
                            const art = document.createElement('img');
                            art.className = 'result-poster';
                            art.src = item.poster;
                            art.loading = 'lazy';
                            art.alt = '';
                            row.appendChild(art);
                        }} else {{
                            const art = document.createElement('div');
                            art.className = 'result-poster empty';
                            art.textContent = 'NO ART';
                            row.appendChild(art);
                        }}
                        const label = document.createElement('span');
                        label.textContent = item.title;
                        const sub = document.createElement('small');
                        sub.textContent = item.subtitle || '';
                        label.appendChild(sub);
                        const btn = document.createElement('button');
                        btn.type = 'button';
                        btn.className = 'btn primary';
                        btn.textContent = 'Link';
                        btn.addEventListener('click', () => submitTarget(item.value));
                        row.appendChild(label);
                        row.appendChild(btn);
                        resultsEl.appendChild(row);
                    }}
                }} catch (e) {{
                    setStatus('Search failed.', 'err');
                }}
            }}

            const fixBtn = document.getElementById('fix-btn');
            if (fixBtn) {{
                fixBtn.addEventListener('click', async () => {{
                    const season = Number(document.getElementById('fix-season').value);
                    const episode = Number(document.getElementById('fix-episode').value);
                    if (!Number.isFinite(season) || !Number.isFinite(episode)) return;
                    setStatus('Fixing...');
                    try {{
                        const res = await fetch(`/stremio/${{apiKey}}/fix-episode`, {{
                            method: 'POST',
                            headers: {{ 'Content-Type': 'application/json' }},
                            body: JSON.stringify({{ id: itemId, season: season, episode: episode, action: action }}),
                        }});
                        const data = await res.json();
                        if (!res.ok) {{
                            setStatus(data.detail || 'Could not use that episode.', 'err');
                            return;
                        }}
                        setStatus(`Pinned to ${{data.code}} — ${{data.title}}, which ${{data.message}}.`, 'ok');
                        fixBtn.disabled = true;
                    }} catch (e) {{
                        setStatus('Request failed.', 'err');
                    }}
                }});
            }}

            document.getElementById('search-btn').addEventListener('click', runSearch);
            document.getElementById('q').addEventListener('keydown', (e) => {{ if (e.key === 'Enter') runSearch(); }});
            document.getElementById('link-btn').addEventListener('click', () => {{
                const value = document.getElementById('ext').value.trim();
                if (value) submitTarget(value);
            }});
            document.getElementById('ext').addEventListener('keydown', (e) => {{
                if (e.key === 'Enter') document.getElementById('link-btn').click();
            }});
        </script>
</body>
</html>"""
    return HTMLResponse(content=html, status_code=status_code)


@router.get("/{api_key}/scrobble-session")
async def start_scrobble_session(request: Request, api_key: str, id: str, type: str = "movie", db: AsyncSession = Depends(get_db)):
    user = await get_user_from_key(api_key, db)
    if not user:
        return HTMLResponse(content="<h2>Unauthorized API Key</h2>", status_code=401)

    target_media = await resolve_media_item(id, type, db, user)
    if not target_media:
        return add_cors_headers(_unmatched_html(api_key, id, type, "scrobble-session", get_base_url(request)))

    details = await _describe_media(db, target_media)
    base_url = get_base_url(request)

    # A session can only auto-complete once its runtime has elapsed, so look the
    # runtime up from TMDB/TVDB before giving up on scrobbling this title.
    runtime = await _resolve_runtime(db, user, target_media)

    if not runtime:
        logged = await _log_watch_event(db, user.id, target_media.id)
        await db.commit()
        heading = "✓ Marked as Watched"
        heading_class = "ok"
        note = (
            "No runtime is available for this title from Scrob, TMDB or TVDB, so it went "
            "straight into your history instead of being scrobbled."
            if logged
            else "This was already logged a moment ago, so nothing was added twice."
        )
    else:
        await _start_playback_session(db, user.id, target_media)
        await db.commit()
        heading = "▶ Now Playing"
        heading_class = "ok"
        note = (
            "Scrobbling to your Scrob account — it lands in your history once the "
            f"{runtime}-minute runtime has elapsed."
        )

    html = _result_page(
        title="Scrob — Scrobbling",
        heading=heading,
        heading_class=heading_class,
        note=note,
        details=details,
        base_url=base_url,
        fixup=_fixup_link(base_url, api_key, id, type, "scrobble-session"),
    )
    return add_cors_headers(HTMLResponse(content=html))


# ── Action Endpoints ───────────────────────────────────────────────────────────

@router.get("/{api_key}/mark-watched")
async def mark_watched_action(request: Request, api_key: str, id: str, type: str = "movie", db: AsyncSession = Depends(get_db)):
    user = await get_user_from_key(api_key, db)
    if not user:
        return HTMLResponse(content="<h2>Unauthorized API Key</h2>", status_code=401)

    target_media = await resolve_media_item(id, type, db, user)
    if not target_media:
        return add_cors_headers(_unmatched_html(api_key, id, type, "mark-watched", get_base_url(request)))

    logged = await _log_watch_event(db, user.id, target_media.id)
    await db.commit()

    details = await _describe_media(db, target_media)
    base_url = get_base_url(request)
    note = (
        "Logged to your Scrob watch history."
        if logged
        else "This was already logged a moment ago, so nothing was added twice."
    )

    html = _result_page(
        title="Scrob — Marked Watched",
        heading="✓ Marked as Watched",
        heading_class="ok",
        note=note,
        details=details,
        base_url=base_url,
        fixup=_fixup_link(base_url, api_key, id, type, "mark-watched"),
    )
    return add_cors_headers(HTMLResponse(content=html))


@router.get("/{api_key}/relink")
async def relink_page(
    request: Request,
    api_key: str,
    id: str,
    type: str = "movie",
    action: str = "mark-watched",
    db: AsyncSession = Depends(get_db),
):
    """Correct a wrong automatic match, reached from the result pages.

    The same page the unmatched path shows, except it also names what the id is
    currently pointing at so the user can see what they are overriding.
    """
    user = await get_user_from_key(api_key, db)
    if not user:
        return HTMLResponse(content="<h2>Unauthorized API Key</h2>", status_code=401)

    current = None
    matched = await resolve_media_item(id, type, db, user)
    if matched:
        current = await _describe_media(db, matched)
        await db.commit()

    return add_cors_headers(_unmatched_html(
        api_key, id, type, action, get_base_url(request), current=current, status_code=200
    ))


# ── Manual Linking ─────────────────────────────────────────────────────────────

class LinkRequest(BaseModel):
    id: str
    type: str = "movie"
    action: str = "mark-watched"
    target: str


@router.get("/{api_key}/link/search")
async def search_for_link(api_key: str, q: str, type: str = "movie", db: AsyncSession = Depends(get_db)):
    """Search the user's own Scrob library for a title to link a Stremio id to."""
    user = await get_user_from_key(api_key, db)
    if not user:
        return JSONResponse(content={"detail": "Unauthorized API key"}, status_code=401)

    term = f"%{q.strip()}%"
    results = []
    if type == "series":
        rows = await db.execute(select(Show).where(Show.title.ilike(term)).order_by(Show.title).limit(15))
        for show in rows.scalars().all():
            year = (show.first_air_date or "")[:4]
            results.append({
                "value": f"scrob:show:{show.id}",
                "title": show.custom_title or show.title,
                "poster": show.poster_path,
                "subtitle": " · ".join(filter(None, [year, f"TMDB {show.tmdb_id}" if show.tmdb_id else None,
                                                     f"TVDB {show.tvdb_id}" if show.tvdb_id else None])),
            })
    else:
        rows = await db.execute(
            select(Media)
            .where(Media.media_type == MediaType.movie, Media.title.ilike(term))
            .order_by(Media.title)
            .limit(15)
        )
        for media in rows.scalars().all():
            year = (media.release_date or "")[:4]
            results.append({
                "value": f"scrob:media:{media.id}",
                "title": media.custom_title or media.title,
                "poster": media.poster_path,
                "subtitle": " · ".join(filter(None, [year, f"TMDB {media.tmdb_id}" if media.tmdb_id else None])),
            })

    return add_cors_headers(JSONResponse(content={"results": results}))


def _parse_link_target(target: str) -> tuple[str, str]:
    """Normalise what the user typed into (kind, value).

    Accepts "tt7587890", "imdb:tt7587890", "tmdb:79744", "tvdb:350665",
    "scrob:show:12", "scrob:media:34", and a bare number (read as TMDB).
    """
    value = target.strip()
    lowered = value.lower()
    if lowered.startswith("scrob:"):
        return "scrob", value
    for prefix in ("imdb:", "tmdb:", "tvdb:"):
        if lowered.startswith(prefix):
            rest = value.split(":", 1)[1].strip()
            return prefix[:-1], rest.lower() if prefix == "imdb:" else rest
    if lowered.startswith("tt"):
        return "imdb", lowered
    return "tmdb", value


async def _show_for_link_target(
    db: AsyncSession, kind: str, value: str, api_key: Optional[str]
) -> Optional[Show]:
    if kind == "scrob":
        parts = value.split(":")
        if len(parts) != 3 or parts[1] != "show" or not parts[2].isdigit():
            return None
        return (await db.execute(select(Show).where(Show.id == int(parts[2])))).scalars().first()

    if kind == "imdb":
        return await _show_from_imdb_id(db, value if value.startswith("tt") else f"tt{value}", api_key)

    if kind == "tvdb":
        if not value.isdigit():
            return None
        show = (await db.execute(select(Show).where(Show.tvdb_id == int(value)))).scalars().first()
        if show:
            return show
        if not api_key:
            return None
        try:
            res = await tmdb.find_by_external_id(value, "tvdb_id", api_key=api_key)
        except Exception:
            return None
        tv = res.get("tv_results") or []
        if not tv:
            return None
        value = str(tv[0].get("id"))
        kind = "tmdb"

    if kind == "tmdb":
        if not value.isdigit():
            return None
        show = (await db.execute(select(Show).where(Show.tmdb_id == int(value)))).scalars().first()
        if show:
            return show
        if not api_key:
            return None
        from routers.webhooks import _find_or_create_show

        try:
            return await _find_or_create_show(db, int(value), api_key)
        except Exception:
            return None
    return None


async def _movie_for_link_target(
    db: AsyncSession, kind: str, value: str, api_key: Optional[str]
) -> Optional[Media]:
    if kind == "scrob":
        parts = value.split(":")
        if len(parts) != 3 or parts[1] != "media" or not parts[2].isdigit():
            return None
        return (await db.execute(select(Media).where(Media.id == int(parts[2])))).scalars().first()

    if kind == "imdb":
        return await _movie_from_imdb_id(db, value if value.startswith("tt") else f"tt{value}", api_key)

    if kind == "tvdb":
        if not (value.isdigit() and api_key):
            return None
        try:
            res = await tmdb.find_by_external_id(value, "tvdb_id", api_key=api_key)
        except Exception:
            return None
        movies = res.get("movie_results") or []
        if not movies:
            return None
        value = str(movies[0].get("id"))
        kind = "tmdb"

    if kind == "tmdb":
        if not value.isdigit():
            return None
        media = (
            await db.execute(
                select(Media)
                .where(Media.tmdb_id == int(value), Media.media_type == MediaType.movie)
                .limit(1)
            )
        ).scalars().first()
        if media:
            return media
        if not api_key:
            return None
        media = Media(
            media_type=MediaType.movie,
            tmdb_id=int(value),
            uri_id=f"tmdb:m:{value}",
            title=f"Movie {value}",
        )
        db.add(media)
        await db.flush()
        try:
            await enrich_media(media, api_key=api_key)
        except Exception:
            pass
        return media
    return None


@router.post("/{api_key}/link")
async def link_stremio_item(api_key: str, body: LinkRequest, db: AsyncSession = Depends(get_db)):
    """Link an unmatched Stremio id to a title, then log the watch the user asked for.

    The IMDb id is stored as a media alias, so every later call for the same show or
    movie resolves without the user having to do this again.
    """
    user = await get_user_from_key(api_key, db)
    if not user:
        return JSONResponse(content={"detail": "Unauthorized API key"}, status_code=401)

    parts = body.id.split(":")
    base_id = parts[0]
    season_num = int(parts[1]) if len(parts) >= 3 and parts[1].isdigit() else None
    episode_num = int(parts[2]) if len(parts) >= 3 and parts[2].isdigit() else None
    is_episode = body.type == "series" or (season_num is not None and episode_num is not None)

    tmdb_key = await _tmdb_key_for_user(db, user)
    tvdb_key = await _tvdb_key_for_user(db, user)
    kind, value = _parse_link_target(body.target)

    if is_episode:
        if season_num is None or episode_num is None:
            return JSONResponse(
                content={"detail": "This Stremio id has no season/episode to log."}, status_code=400
            )
        show = await _show_for_link_target(db, kind, value, tmdb_key)
        if not show:
            return JSONResponse(content={"detail": "Couldn't resolve that id to a show."}, status_code=404)
        if base_id.startswith("tt"):
            await _remember_imdb_alias(db, show.id, "series", base_id)
        target_media = await _episode_for_show(db, show, season_num, episode_num, tmdb_key, tvdb_key)
        if not target_media:
            return JSONResponse(
                content={"detail": f"Linked to {show.title}, but S{season_num}E{episode_num} couldn't be fetched."},
                status_code=404,
            )
    else:
        target_media = await _movie_for_link_target(db, kind, value, tmdb_key)
        if not target_media:
            return JSONResponse(content={"detail": "Couldn't resolve that id to a movie."}, status_code=404)
        if base_id.startswith("tt"):
            await _remember_imdb_alias(db, target_media.id, "movie", base_id)

    # Honour whichever action the user came in on, so linking from the scrobble
    # entry starts a session rather than silently marking the title watched.
    if body.action == "scrobble-session" and target_media.runtime:
        await _start_playback_session(db, user.id, target_media)
        status_text = "is now scrobbling"
    else:
        await _log_watch_event(db, user.id, target_media.id)
        status_text = "was logged to your history"
    await db.commit()

    return add_cors_headers(JSONResponse(
        content={"status": "ok", "title": target_media.title, "message": status_text}
    ))


class FixEpisodeBody(BaseModel):
    """Point a Stremio id at a different episode of the show it already matched."""
    id: str
    season: int
    episode: int
    action: str = "mark-watched"


async def _undo_stremio_action(db: AsyncSession, user_id: int, media_id: int) -> None:
    """Roll back what the wrong match just recorded, so a correction doesn't leave
    the mis-identified episode sitting in history."""
    recent_cutoff = datetime.utcnow() - timedelta(hours=6)
    await db.execute(
        delete(WatchEvent).where(
            WatchEvent.user_id == user_id,
            WatchEvent.media_id == media_id,
            or_(WatchEvent.watched_at.is_(None), WatchEvent.watched_at >= recent_cutoff),
        )
    )
    await db.execute(
        delete(PlaybackSession).where(
            PlaybackSession.user_id == user_id,
            PlaybackSession.media_id == media_id,
        )
    )
    await db.execute(
        delete(PlaybackProgress).where(
            PlaybackProgress.user_id == user_id,
            PlaybackProgress.media_id == media_id,
        )
    )


@router.post("/{api_key}/fix-episode")
async def fix_stremio_episode(api_key: str, body: FixEpisodeBody, db: AsyncSession = Depends(get_db)):
    """Correct the episode a Stremio id resolves to.

    Catalogues disagree about episode numbering often enough that an id like
    tt7587890:6:6 can land one episode off. The chosen episode is pinned to this
    exact id, and whatever the wrong match just logged is undone.
    """
    user = await get_user_from_key(api_key, db)
    if not user:
        return JSONResponse(content={"detail": "Unauthorized API key"}, status_code=401)

    previous = await resolve_media_item(body.id, "series", db, user)
    show = None
    if previous is not None and previous.show_id:
        show_res = await db.execute(select(Show).where(Show.id == previous.show_id))
        show = show_res.scalar_one_or_none()
    if show is None:
        base_id = body.id.split(":")[0]
        if base_id.startswith("tt"):
            show = await _show_from_imdb_id(db, base_id, await _tmdb_key_for_user(db, user))
    if show is None:
        return JSONResponse(
            content={"detail": "Couldn't work out which show this belongs to — link it first."},
            status_code=404,
        )

    target_media = await _episode_for_show(
        db, show, body.season, body.episode,
        await _tmdb_key_for_user(db, user),
        await _tvdb_key_for_user(db, user),
    )
    if not target_media:
        return JSONResponse(
            content={"detail": f"{show.title} has no S{body.season:02d}E{body.episode:02d}."},
            status_code=404,
        )

    if previous is not None and previous.id != target_media.id:
        await _undo_stremio_action(db, user.id, previous.id)

    await _pin_media(db, body.id, target_media)

    runtime = await _resolve_runtime(db, user, target_media)
    if body.action == "scrobble-session" and runtime:
        await _start_playback_session(db, user.id, target_media)
        status_text = "is now scrobbling"
    else:
        await _log_watch_event(db, user.id, target_media.id)
        status_text = "was logged to your history"
    await db.commit()

    return add_cors_headers(JSONResponse(content={
        "status": "ok",
        "title": target_media.custom_title or target_media.title,
        "code": f"S{body.season:02d}E{body.episode:02d}",
        "message": status_text,
    }))
