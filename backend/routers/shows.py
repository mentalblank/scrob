import asyncio
from datetime import datetime, date
from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, case, cast as sa_cast, Text, or_, and_
from sqlalchemy.orm import aliased

from models.events import WatchEvent
from models.collection import Collection, CollectionFile
from models.ratings import Rating
from models.lists import List as UserList, ListItem
from models.playback_progress import PlaybackProgress

from db import get_db
from models.media import Media
from models.season_override import ShowSeasonOverride, ShowEpisodeOverride
from models.collection import Collection, CollectionFile
from models.base import MediaType
from models.show import Show as ShowModel
from models.users import User, UserSettings
from routers.media import (
    format_media, get_user_tmdb_key, get_user_content_language, check_tmdb_key,
    enrich_with_state, refresh_technical_data, _extract_show_content_rating,
    get_where_to_watch, _get_blocked_ids, _get_content_filters, _is_content_filtered,
    _effective_sonarr, _get_global_settings
)

from dependencies import get_current_user
from core import tmdb
from core import tvdb as tvdb_client

router = APIRouter()


async def get_user_tvdb_key(db: AsyncSession, user_id: int) -> str | None:
    from models.global_settings import GlobalSettings
    result = await db.execute(select(UserSettings).where(UserSettings.user_id == user_id))
    s = result.scalar_one_or_none()
    if s and s.tvdb_api_key:
        return s.tvdb_api_key
    gs_result = await db.execute(select(GlobalSettings).where(GlobalSettings.id == 1))
    gs = gs_result.scalar_one_or_none()
    return gs.tvdb_api_key if gs else None


def format_show(show: ShowModel) -> dict:
    # Merge custom season names into seasons_meta so they propagate everywhere
    raw_seasons = (show.tmdb_data or {}).get("seasons", [])
    custom_season_names: dict = show.custom_season_names or {}
    if custom_season_names:
        seasons_meta = [
            {
                **s,
                "name": custom_season_names.get(str(s.get("season_number")), s.get("name")),
                "custom_name": custom_season_names.get(str(s.get("season_number"))),
            }
            for s in raw_seasons
        ]
    else:
        seasons_meta = raw_seasons

    return {
        "id": show.id,
        "tmdb_id": show.tmdb_id,
        "tvdb_id": show.tvdb_id if show.tvdb_id else (
            int(show.tmdb_data.get("external_ids", {}).get("tvdb_id"))
            if (show.tmdb_data and show.tmdb_data.get("external_ids", {}).get("tvdb_id"))
            else None
        ),
        "type": "series",
        "title": show.custom_title or show.title,
        "tmdb_title": show.title,
        "custom_title": show.custom_title,
        "original_title": show.original_title,
        "overview": show.overview,
        "poster_path": show.poster_path,
        "backdrop_path": show.backdrop_path,
        "tmdb_rating": show.tmdb_rating,
        "status": show.status,
        "tagline": show.tagline,
        "first_air_date": show.first_air_date,
        "last_air_date": show.last_air_date,
        "genres": (show.tmdb_data or {}).get("genres", []),
        "seasons_meta": seasons_meta,
        "custom_season_names": custom_season_names,
        "original_language": (show.tmdb_data or {}).get("original_language"),
        "adult": (show.tmdb_data or {}).get("adult", False),
    }


async def get_enriched_show_info(db: AsyncSession, current_user_id: int, series_tmdb_id: int, show_info: dict) -> dict:
    overrides_res = await db.execute(
        select(ShowSeasonOverride).where(
            ShowSeasonOverride.user_id == current_user_id,
            ShowSeasonOverride.target_show_tmdb_id == series_tmdb_id
        )
    )
    season_overrides = overrides_res.scalars().all()
    
    existing_snums = {s.get("season_number") for s in show_info.get("seasons_meta", [])}
    seasons_meta = list(show_info.get("seasons_meta", []))
    
    for override in season_overrides:
        s_num = override.target_season_number
        if s_num not in existing_snums:
            poster_path = None
            overview = None
            air_date = None
            name = f"Season {s_num}"
            episode_count = 0
            
            src_show_q = await db.execute(
                select(ShowModel).where(ShowModel.tmdb_id == override.source_show_tmdb_id)
            )
            src_show = src_show_q.scalar_one_or_none()
            if src_show and src_show.tmdb_data:
                src_seasons = src_show.tmdb_data.get("seasons", [])
                src_season_meta = next((s for s in src_seasons if s.get("season_number") == override.source_season_number), None)
                if src_season_meta:
                    poster_path = src_season_meta.get("poster_path")
                    overview = src_season_meta.get("overview")
                    air_date = src_season_meta.get("air_date")
                    name = src_season_meta.get("name") or name
                    episode_count = src_season_meta.get("episode_count", 0)
                    
            seasons_meta.append({
                "season_number": s_num,
                "name": name,
                "episode_count": episode_count,
                "overview": overview,
                "poster_path": poster_path,
                "air_date": air_date
            })

    # Re-apply custom season names to the updated seasons_meta list
    custom_season_names = show_info.get("custom_season_names") or {}
    for s in seasons_meta:
        s_num_str = str(s.get("season_number"))
        if s_num_str in custom_season_names:
            s["name"] = custom_season_names[s_num_str]
            s["custom_name"] = custom_season_names[s_num_str]

    seasons_meta.sort(key=lambda x: x.get("season_number", 0))
    show_info["seasons_meta"] = seasons_meta
    return show_info


@router.get("")
async def list_shows(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
    sort: str = Query(default="title"),
    page: int = Query(1, ge=1),
    page_size: int = Query(30, ge=1, le=100),
    genre: str | None = Query(None),
    year: int | None = Query(None),
    status: str | None = Query(None),
):
    offset = (page - 1) * page_size

    user_settings_q = await db.execute(select(UserSettings).where(UserSettings.user_id == current_user.id))
    user_settings = user_settings_q.scalar_one_or_none()
    include_specials = False
    if user_settings and user_settings.preferences:
        include_specials = user_settings.preferences.get("include_specials", False)

    # A show is "in the user's collection" if they have at least one countable episode
    # collected — same criteria used by enrich_with_state for the percentage calculation.
    show_ids_q = (
        select(Media.show_id)
        .join(Collection, Collection.media_id == Media.id)
        .where(
            Collection.user_id == current_user.id,
            Media.show_id.isnot(None),
            Media.media_type == MediaType.episode,
            Media.season_number.isnot(None),
            Media.episode_number.isnot(None),
        )
    )
    if not include_specials:
        show_ids_q = show_ids_q.where(Media.season_number != 0)

    user_show_ids = show_ids_q.distinct().subquery()

    base_query = select(ShowModel).where(ShowModel.id.in_(select(user_show_ids)))
    if genre:
        base_query = base_query.where(sa_cast(ShowModel.tmdb_data["genres"], Text).contains(f'"{genre}"'))
    if year:
        base_query = base_query.where(ShowModel.first_air_date.like(f'{year}%'))
    if status:
        base_query = base_query.where(ShowModel.status == status)

    # Count total
    count_query = select(func.count()).select_from(base_query.subquery())
    total_result = await db.execute(count_query)
    total_count = total_result.scalar_one()
    total_pages = (total_count + page_size - 1) // page_size

    # Sort and Paginate
    if sort == "rating":
        q = base_query.order_by(ShowModel.tmdb_rating.desc().nulls_last())
    elif sort == "release_date":
        q = base_query.order_by(ShowModel.first_air_date.desc().nulls_last())
    elif sort == "created_at":
        q = base_query.order_by(ShowModel.created_at.desc().nulls_last())
    elif sort == "last_watched":
        last_watched_sq = (
            select(Media.show_id, func.max(WatchEvent.watched_at).label("last_watched_at"))
            .join(WatchEvent, WatchEvent.media_id == Media.id)
            .where(WatchEvent.user_id == current_user.id, Media.show_id.isnot(None))
            .group_by(Media.show_id)
            .subquery()
        )
        q = (
            base_query
            .outerjoin(last_watched_sq, last_watched_sq.c.show_id == ShowModel.id)
            .order_by(last_watched_sq.c.last_watched_at.desc().nulls_last())
        )
    else:
        q = base_query.order_by(func.lower(ShowModel.title).asc())

    result = await db.execute(q.limit(page_size).offset(offset))
    results = [format_show(s) for s in result.scalars().all()]
    await enrich_with_state(db, current_user.id, results)
    return {
        "page": page,
        "page_size": page_size,
        "total_results": total_count,
        "total_pages": total_pages,
        "results": results,
    }


@router.get("/{series_tmdb_id}")
async def get_show(
    series_tmdb_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    user_settings_q = await db.execute(select(UserSettings).where(UserSettings.user_id == current_user.id))
    user_settings = user_settings_q.scalar_one_or_none()
    include_specials = False
    if user_settings and user_settings.preferences:
        include_specials = user_settings.preferences.get("include_specials", False)

    # 1. Try to find locally
    show_result = await db.execute(
        select(ShowModel).where(ShowModel.tmdb_id == series_tmdb_id)
    )
    show = show_result.scalar_one_or_none()

    if show:
        # Fetch local episodes
        episodes_result = await db.execute(
            select(Media)
            .where(Media.media_type == MediaType.episode)
            .where(Media.show_id == show.id)
            .order_by(Media.season_number.asc(), Media.episode_number.asc())
        )
        episodes = episodes_result.scalars().all()

        seasons_meta = {
            s["season_number"]: s for s in (show.tmdb_data or {}).get("seasons", [])
        }

        seasons: dict = {}
        all_ep_formatted = []
        for ep in episodes:
            s_num = ep.season_number or 0
            season_poster = (
                seasons_meta.get(s_num, {}).get("poster_path") or show.poster_path
            )
            ep_formatted = format_media(ep)
            ep_formatted["poster_path"] = ep.poster_path or season_poster
            seasons.setdefault(s_num, []).append(ep_formatted)
            all_ep_formatted.append(ep_formatted)

        if all_ep_formatted:
            await enrich_with_state(db, current_user.id, all_ep_formatted)

        # Fetch networks + recommendations from TMDB if key is available
        networks = []
        recommendations = []
        cast = []
        tmdb_extra: dict | None = None
        tv_images: dict = {}  # Will be populated if TMDB key is available
        api_key = await get_user_tmdb_key(db, current_user.id)
        user_lang = await get_user_content_language(db, current_user.id)
        trailer_youtube_id: str | None = None
        if check_tmdb_key(api_key):
            try:
                tmdb_extra, videos_data, tv_images = await asyncio.gather(
                    tmdb.get_show(series_tmdb_id, api_key=api_key),
                    tmdb.get_tv_videos(series_tmdb_id, api_key=api_key),
                    tmdb.get_tv_images(series_tmdb_id, api_key=api_key),
                )
                # Extract first official YouTube trailer
                trailer_youtube_id = next(
                    (
                        v["key"]
                        for v in videos_data.get("results", [])
                        if v.get("type") == "Trailer" and v.get("site") == "YouTube" and v.get("official")
                    ),
                    next(
                        (
                            v["key"]
                            for v in videos_data.get("results", [])
                            if v.get("type") == "Trailer" and v.get("site") == "YouTube"
                        ),
                        None,
                    ),
                )
                networks = [
                    {
                        "id": n["id"],
                        "name": n["name"],
                        "logo_path": tmdb.poster_url(n.get("logo_path"), size="w500")
                        if n.get("logo_path")
                        else None,
                        "origin_country": n.get("origin_country"),
                    }
                    for n in tmdb_extra.get("networks", [])
                ]
                recommendations = [
                    {
                        "id": None,
                        "tmdb_id": r["id"],
                        "type": "series",
                        "title": r.get("name") or r.get("title"),
                        "original_title": r.get("original_name")
                        or r.get("original_title"),
                        "overview": r.get("overview"),
                        "poster_path": tmdb.poster_url(r.get("poster_path")),
                        "backdrop_path": tmdb.poster_url(
                            r.get("backdrop_path"), size="w1280"
                        ),
                        "release_date": r.get("first_air_date")
                        or r.get("release_date"),
                        "tmdb_rating": r.get("vote_average"),
                        "adult": r.get("adult", False),
                    }
                    for r in tmdb_extra.get("recommendations", {}).get("results", [])[
                        :12
                    ]
                ]
                await enrich_with_state(db, current_user.id, recommendations)
                cast = [
                    {
                        "tmdb_id": c.get("id"),
                        "name": c.get("name"),
                        "character": c.get("character"),
                        "profile_path": tmdb.poster_url(c.get("profile_path")),
                    }
                    for c in tmdb_extra.get("credits", {}).get("cast", [])[:12]
                ]
            except Exception:
                pass

        # Pick best backdrop + logo using No Language → user lang → any priority
        picked_backdrop = tmdb.pick_image(tv_images.get("backdrops", []), preferred_lang=user_lang, size="original")
        picked_logo = tmdb.pick_image(tv_images.get("logos", []), preferred_lang=user_lang, size="w500")

        state_item: dict = {"tmdb_id": series_tmdb_id, "type": "series"}
        # Pass last_episode_to_air from the already-fetched tmdb_extra so enrich_with_state
        # doesn't make a redundant second TMDB call for the same data.
        if tmdb_extra and tmdb_extra.get("last_episode_to_air"):
            state_item["_last_episode_to_air"] = tmdb_extra["last_episode_to_air"]
        await enrich_with_state(db, current_user.id, [state_item])

        # --- Per-season states ---
        season_states: dict = {}

        # Get season overrides where this show is the target or the source
        overrides_all_q = await db.execute(
            select(ShowSeasonOverride).where(
                ShowSeasonOverride.user_id == current_user.id,
                or_(
                    ShowSeasonOverride.source_show_tmdb_id == series_tmdb_id,
                    ShowSeasonOverride.target_show_tmdb_id == series_tmdb_id
                )
            )
        )
        overrides_all = overrides_all_q.scalars().all()

        coll_eps_by_season = {}
        watched_eps_by_season = {}

        # Collected episodes per season
        coll_eps_q = await db.execute(
            select(Media.season_number, Media.episode_number)
            .join(Collection, Collection.media_id == Media.id)
            .where(
                Media.show_id == show.id,
                Media.media_type == MediaType.episode,
                Media.season_number.isnot(None),
                Media.episode_number.isnot(None),
            )
        )
        for sn, en in coll_eps_q.all():
            coll_eps_by_season.setdefault(sn, set()).add(en)

        # Watched episodes per season (including those not in collection)
        watched_eps_q = await db.execute(
            select(Media.season_number, Media.episode_number)
            .join(WatchEvent, WatchEvent.media_id == Media.id)
            .where(
                Media.show_id == show.id,
                WatchEvent.user_id == current_user.id,
                Media.media_type == MediaType.episode,
                Media.season_number.isnot(None),
                Media.episode_number.isnot(None),
            )
        )
        for sn, en in watched_eps_q.all():
            watched_eps_by_season.setdefault(sn, set()).add(en)

        # Merge remapped seasons from overrides
        for override in overrides_all:
            if override.source_show_tmdb_id == series_tmdb_id:
                this_season = override.source_season_number
                other_show_tmdb_id = override.target_show_tmdb_id
                other_season = override.target_season_number
            else:
                this_season = override.target_season_number
                other_show_tmdb_id = override.source_show_tmdb_id
                other_season = override.source_season_number

            other_show_q = await db.execute(
                select(ShowModel).where(ShowModel.tmdb_id == other_show_tmdb_id)
            )
            other_show = other_show_q.scalar_one_or_none()
            if other_show:
                # Fetch other show's collected episodes for other_season
                other_coll_q = await db.execute(
                    select(Media.episode_number)
                    .join(Collection, Collection.media_id == Media.id)
                    .where(
                        Media.show_id == other_show.id,
                        Collection.user_id == current_user.id,
                        Media.media_type == MediaType.episode,
                        Media.season_number == other_season,
                        Media.episode_number.isnot(None),
                    )
                )
                for (en,) in other_coll_q.all():
                    coll_eps_by_season.setdefault(this_season, set()).add(en)

                # Fetch other show's watched episodes for other_season
                other_watched_q = await db.execute(
                    select(Media.episode_number)
                    .join(WatchEvent, WatchEvent.media_id == Media.id)
                    .where(
                        Media.show_id == other_show.id,
                        WatchEvent.user_id == current_user.id,
                        Media.media_type == MediaType.episode,
                        Media.season_number == other_season,
                        Media.episode_number.isnot(None),
                    )
                )
                for (en,) in other_watched_q.all():
                    watched_eps_by_season.setdefault(this_season, set()).add(en)

        coll_per_season = {sn: len(eps) for sn, eps in coll_eps_by_season.items()}
        watched_per_season = {sn: len(eps) for sn, eps in watched_eps_by_season.items()}

        # Season user ratings (stored against the show's Media row with season_number)
        show_media_q = await db.execute(
            select(Media).where(Media.tmdb_id == series_tmdb_id, Media.media_type == MediaType.series)
        )
        show_media = show_media_q.scalars().first()
        if show_media and not show_media.adult and (tmdb_extra or {}).get("adult", False):
            show_media.adult = True
            await db.commit()
        season_ratings: dict = {}
        if show_media:
            ratings_q = await db.execute(
                select(Rating.season_number, func.max(Rating.rating))
                .where(
                    Rating.media_id == show_media.id,
                    Rating.user_id == current_user.id,
                    Rating.season_number.isnot(None),
                )
                .group_by(Rating.season_number)
            )
            season_ratings = dict(ratings_q.all())

        # Episode counts per season from stored TMDB metadata
        season_ep_counts: dict = {
            s["season_number"]: s.get("episode_count", 0)
            for s in (show.tmdb_data or {}).get("seasons", [])
        }

        # Apply overrides to season_ep_counts
        for override in overrides_all:
            if override.source_show_tmdb_id == series_tmdb_id:
                # Remapped away! Exclude this season from counts (intra-show only)
                if override.source_show_tmdb_id == override.target_show_tmdb_id:
                    if override.source_season_number in season_ep_counts:
                        del season_ep_counts[override.source_season_number]
            elif override.target_show_tmdb_id == series_tmdb_id:
                # Remapped to! Include the source season's episode count
                src_show_q = await db.execute(
                    select(ShowModel).where(ShowModel.tmdb_id == override.source_show_tmdb_id)
                )
                src_show = src_show_q.scalar_one_or_none()
                ep_count = 0
                if src_show and src_show.tmdb_data:
                    src_seasons = src_show.tmdb_data.get("seasons", [])
                    src_season_meta = next((s for s in src_seasons if s.get("season_number") == override.source_season_number), None)
                    if src_season_meta:
                        ep_count = src_season_meta.get("episode_count", 0)
                season_ep_counts[override.target_season_number] = ep_count

        # For shows that are still airing, adjust the counts to only include aired episodes
        last_ep = (tmdb_extra or show.tmdb_data or {}).get("last_episode_to_air")
        if last_ep:
            last_sn = last_ep.get("season_number")
            last_en = last_ep.get("episode_number")
            if last_sn in season_ep_counts:
                # Capped at the last aired episode number for the current season
                season_ep_counts[last_sn] = last_en
            # Future seasons have 0 aired episodes
            for sn in season_ep_counts:
                if sn > last_sn:
                    season_ep_counts[sn] = 0

        # Build season states for all known seasons
        for sn in set(list(coll_per_season.keys()) + list(season_ep_counts.keys())):
            collected = coll_per_season.get(sn, 0)
            watched = watched_per_season.get(sn, 0)
            total = season_ep_counts.get(sn, 0)
            
            # Use distinct (season, episode) from user's collection for calculation
            # to be consistent with how total is calculated (unique episodes in season).
            season_states[sn] = {
                "in_library": collected > 0,
                "collection_pct": min(100, int((collected / total) * 100)) if total > 0 else 0,
                "watched": watched >= total if total > 0 else False,
                "watch_pct": min(100, int((watched / total) * 100)) if total > 0 else 0,
                "user_rating": season_ratings.get(sn),
                "watched_episodes_count": watched,
                "total_episodes_count": total,
            }

        # Enhance seasons_meta with live TMDB data (id, rating, overview, air_date)
        tmdb_season_map: dict = {}
        if tmdb_extra:
            tmdb_season_map = {s["season_number"]: s for s in tmdb_extra.get("seasons", [])}
        
        # Remove any seasons that are source of a season-level override (intra-show only)
        source_seasons_to_remove = {
            o.source_season_number for o in overrides_all
            if o.source_show_tmdb_id == series_tmdb_id and o.source_show_tmdb_id == o.target_show_tmdb_id
        }
        base_seasons_meta = [
            s for s in (show.tmdb_data or {}).get("seasons", [])
            if s.get("season_number") not in source_seasons_to_remove
        ]
        
        existing_snums = {s.get("season_number") for s in base_seasons_meta}
        season_overrides = {o.target_season_number: o for o in overrides_all if o.target_show_tmdb_id == series_tmdb_id}

        for s_num in seasons.keys():
            if s_num not in existing_snums:
                override = season_overrides.get(s_num)
                poster_path = None
                overview = None
                air_date = None
                name = f"Season {s_num}"
                
                if override:
                    src_show_q = await db.execute(
                        select(ShowModel).where(ShowModel.tmdb_id == override.source_show_tmdb_id)
                    )
                    src_show = src_show_q.scalar_one_or_none()
                    if src_show and src_show.tmdb_data:
                        src_seasons = src_show.tmdb_data.get("seasons", [])
                        src_season_meta = next((s for s in src_seasons if s.get("season_number") == override.source_season_number), None)
                        if src_season_meta:
                            poster_path = src_season_meta.get("poster_path")
                            overview = src_season_meta.get("overview")
                            air_date = src_season_meta.get("air_date")
                            name = src_season_meta.get("name") or name

                base_seasons_meta.append({
                    "season_number": s_num,
                    "name": name,
                    "episode_count": len(seasons[s_num]),
                    "overview": overview,
                    "poster_path": poster_path,
                    "air_date": air_date
                })
        base_seasons_meta.sort(key=lambda x: x.get("season_number", 0))

        custom_names = show.custom_season_names or {}
        enhanced_seasons_meta = [
            {
                **s,
                "name": custom_names.get(str(s["season_number"]), s.get("name")),
                "tmdb_season_id": tmdb_season_map.get(s["season_number"], {}).get("id"),
                "tmdb_rating": tmdb_season_map.get(s["season_number"], {}).get("vote_average"),
                # Fill overview/air_date from live TMDB if not stored in DB
                "overview": s.get("overview") or tmdb_season_map.get(s["season_number"], {}).get("overview"),
                "air_date": s.get("air_date") or tmdb_season_map.get(s["season_number"], {}).get("air_date"),
            }
            for s in base_seasons_meta
        ]

        where_to_watch = await get_where_to_watch(
            db, current_user.id, series_tmdb_id, MediaType.series, show=show, tmdb_key=api_key
        )

        return {
            **format_show(show),
            "tvdb_id": show.tvdb_id or (
                int((tmdb_extra or show.tmdb_data or {}).get("external_ids", {}).get("tvdb_id"))
                if (tmdb_extra or show.tmdb_data or {}).get("external_ids", {}).get("tvdb_id")
                else None
            ),
            "backdrop_path": picked_backdrop or show.backdrop_path,
            "logo_path": picked_logo,
            "seasons_meta": enhanced_seasons_meta,
            "original_language": (show.tmdb_data or {}).get("original_language") or (tmdb_extra or {}).get("original_language"),
            "age_rating": _extract_show_content_rating(tmdb_extra) if tmdb_extra else None,
            "imdb_id": (tmdb_extra or show.tmdb_data or {}).get("external_ids", {}).get("imdb_id"),
            "adult": (tmdb_extra or show.tmdb_data or {}).get("adult", False),
            "in_library": state_item.get("collection_pct", 0) > 0 if state_item else False,
            "watched": state_item.get("watched", False) if state_item else False,
            "in_lists": state_item.get("in_lists", []),
            "collection_pct": state_item.get("collection_pct", 0),
            "watch_pct": state_item.get("watch_pct", 0),
            "is_monitored": state_item.get("is_monitored", False),
            "request_enabled": state_item.get("request_enabled", False),
            "is_blocked": state_item.get("is_blocked", False) if state_item else False,
            "is_dropped": state_item.get("is_dropped", False) if state_item else False,
            "request_status": state_item.get("request_status"),
            "user_rating": state_item.get("user_rating"),
            "season_states": season_states,
            "seasons": {f"season_{k}": v for k, v in sorted(seasons.items())},
            "cast": cast,
            "networks": networks,
            "where_to_watch": where_to_watch,
            "trailer_youtube_id": trailer_youtube_id,
            "include_specials": include_specials,
            "watched_episodes_count": state_item.get("watched_episodes_count", 0) if state_item else 0,
            "total_episodes_count": state_item.get("total_episodes_count", 0) if state_item else 0,
        }

    # 2. If not local, fetch from TMDB
    api_key = await get_user_tmdb_key(db, current_user.id)
    if not check_tmdb_key(api_key):
        raise HTTPException(
            status_code=404, detail="Show not found and TMDB key not configured"
        )

    user_lang = await get_user_content_language(db, current_user.id)

    try:
        data, videos_data, tv_images = await asyncio.gather(
            tmdb.get_show(series_tmdb_id, api_key=api_key),
            tmdb.get_tv_videos(series_tmdb_id, api_key=api_key),
            tmdb.get_tv_images(series_tmdb_id, api_key=api_key),
        )
        trailer_youtube_id_tmdb = next(
            (
                v["key"]
                for v in videos_data.get("results", [])
                if v.get("type") == "Trailer" and v.get("site") == "YouTube" and v.get("official")
            ),
            next(
                (
                    v["key"]
                    for v in videos_data.get("results", [])
                    if v.get("type") == "Trailer" and v.get("site") == "YouTube"
                ),
                None,
            ),
        )

        # Pick best backdrop + logo using No Language → user lang → any priority
        picked_backdrop = tmdb.pick_image(tv_images.get("backdrops", []), preferred_lang=user_lang, size="original")
        picked_logo = tmdb.pick_image(tv_images.get("logos", []), preferred_lang=user_lang, size="w500")

        cast = [
            {
                "tmdb_id": c.get("id"),
                "name": c.get("name"),
                "character": c.get("character"),
                "profile_path": tmdb.poster_url(c.get("profile_path")),
            }
            for c in data.get("credits", {}).get("cast", [])[:12]
        ]

        networks = [
            {
                "id": n["id"],
                "name": n["name"],
                "logo_path": tmdb.poster_url(n.get("logo_path"), size="w500")
                if n.get("logo_path")
                else None,
                "origin_country": n.get("origin_country"),
            }
            for n in data.get("networks", [])
        ]

        state_item_tmdb: dict = {"tmdb_id": series_tmdb_id, "type": "series"}
        await enrich_with_state(db, current_user.id, [state_item_tmdb])

        where_to_watch = await get_where_to_watch(
            db, current_user.id, series_tmdb_id, MediaType.series, tmdb_key=api_key
        )

        active_seasons = [s for s in data.get("seasons", []) if s.get("season_number", 0) > 0 and s.get("episode_count", 0) > 0]
        seasons_data = {}
        if len(active_seasons) == 1:
            single_season = active_seasons[0]
            s_num = single_season["season_number"]
            try:
                s_data = await tmdb.get_season(series_tmdb_id, s_num, api_key=api_key)
                episodes_list = []
                for ep in s_data.get("episodes", []):
                    episodes_list.append({
                        "id": None,
                        "tmdb_id": ep.get("id"),
                        "type": "episode",
                        "title": ep.get("name"),
                        "overview": ep.get("overview"),
                        "poster_path": ep.get("still_path"),
                        "release_date": ep.get("air_date"),
                        "runtime": ep.get("runtime"),
                        "tmdb_rating": ep.get("vote_average"),
                        "season_number": s_num,
                        "episode_number": ep.get("episode_number"),
                        "in_library": False,
                        "watched": False,
                        "in_lists": [],
                        "collection_pct": 0,
                        "progress_percent": 0,
                    })
                seasons_data[f"season_{s_num}"] = episodes_list
            except Exception as e:
                pass

        return {
            "id": None,
            "tmdb_id": series_tmdb_id,
            "tvdb_id": (
                int(data.get("external_ids", {}).get("tvdb_id"))
                if data.get("external_ids", {}).get("tvdb_id")
                else None
            ),
            "title": data.get("name"),
            "original_title": data.get("original_name"),
            "overview": data.get("overview"),
            "poster_path": tmdb.poster_url(data.get("poster_path")),
            "backdrop_path": picked_backdrop or tmdb.poster_url(data.get("backdrop_path"), size="original"),
            "logo_path": picked_logo,
            "tmdb_rating": data.get("vote_average"),
            "status": data.get("status"),
            "tagline": data.get("tagline"),
            "first_air_date": data.get("first_air_date"),
            "last_air_date": data.get("last_air_date"),
            "genres": [g["name"] for g in data.get("genres", [])],
            "original_language": data.get("original_language"),
            "age_rating": _extract_show_content_rating(data),
            "imdb_id": data.get("external_ids", {}).get("imdb_id"),
            "adult": data.get("adult", False),
            "in_library": state_item_tmdb.get("collection_pct", 0) > 0,
            "watched": state_item_tmdb.get("watched", False),
            "in_lists": state_item_tmdb.get("in_lists", []),
            "collection_pct": state_item_tmdb.get("collection_pct", 0),
            "is_monitored": state_item_tmdb.get("is_monitored", False),
            "request_enabled": state_item_tmdb.get("request_enabled", False),
            "is_blocked": state_item_tmdb.get("is_blocked", False),
            "is_dropped": state_item_tmdb.get("is_dropped", False),
            "request_status": state_item_tmdb.get("request_status"),
            "user_rating": state_item_tmdb.get("user_rating"),
            "cast": cast,
            "networks": networks,
            "seasons_meta": [
                {
                    "season_number": s["season_number"],
                    "poster_path": tmdb.poster_url(s.get("poster_path")),
                    "episode_count": s["episode_count"],
                    "name": s["name"],
                    "air_date": s.get("air_date"),
                    "overview": s.get("overview"),
                    "tmdb_rating": s.get("vote_average"),
                }
                for s in data.get("seasons", [])
            ],
            "seasons": seasons_data,
            "season_states": {},
            "where_to_watch": where_to_watch,
            "trailer_youtube_id": trailer_youtube_id_tmdb,
            "include_specials": include_specials,
            "watched_episodes_count": state_item_tmdb.get("watched_episodes_count", 0) if state_item_tmdb else 0,
            "total_episodes_count": state_item_tmdb.get("total_episodes_count", 0) if state_item_tmdb else 0,
        }
    except Exception as e:
        raise HTTPException(status_code=404, detail=f"TMDB Show not found: {e}")



@router.get("/{series_tmdb_id}/recommendations")
async def get_show_recommendations(
    series_tmdb_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Fetch series recommendations from TMDB and enrich with state."""
    tmdb_key = await get_user_tmdb_key(db, current_user.id)
    if not check_tmdb_key(tmdb_key):
        return {"results": []}

    try:
        data = await tmdb.get_show(series_tmdb_id, api_key=tmdb_key)
        recs_raw = data.get("recommendations", {}).get("results", [])[:12]
        
        blocked_ids = await _get_blocked_ids(db, current_user.id, MediaType.series)
        cf = await _get_content_filters(db, current_user.id)
        recs_raw = [res for res in recs_raw if res.get("id") not in blocked_ids and not _is_content_filtered(res, *cf)]

        recommendations = [
            {
                "id": None,
                "tmdb_id": r["id"],
                "type": "series",
                "title": r.get("name") or r.get("title"),
                "original_title": r.get("original_name") or r.get("original_title"),
                "overview": r.get("overview"),
                "poster_path": tmdb.poster_url(r.get("poster_path")),
                "backdrop_path": tmdb.poster_url(r.get("backdrop_path"), size="w1280"),
                "release_date": r.get("first_air_date") or r.get("release_date"),
                "tmdb_rating": r.get("vote_average"),
            }
            for r in recs_raw
        ]
        await enrich_with_state(db, current_user.id, recommendations)
        return {"results": recommendations}
    except Exception:
        return {"results": []}


@router.get("/{series_tmdb_id}/season/{season_number}")
async def get_show_season(
    series_tmdb_id: int,
    season_number: int,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    # Check for active season override
    override_result = await db.execute(
        select(ShowSeasonOverride).where(
            ShowSeasonOverride.user_id == current_user.id,
            ShowSeasonOverride.target_show_tmdb_id == series_tmdb_id,
            ShowSeasonOverride.target_season_number == season_number
        )
    )
    season_override = override_result.scalar_one_or_none()

    query_show_tmdb_id = series_tmdb_id
    query_season_number = season_number
    if season_override:
        query_show_tmdb_id = season_override.source_show_tmdb_id
        query_season_number = season_override.source_season_number

    # 1. Try to find show and local episodes for this season
    show_result = await db.execute(
        select(ShowModel).where(ShowModel.tmdb_id == series_tmdb_id)
    )
    show = show_result.scalar_one_or_none()

    local_episodes = []
    if show:
        ep_result = await db.execute(
            select(Media)
            .where(Media.media_type == MediaType.episode)
            .where(Media.show_id == show.id)
            .where(Media.season_number == season_number)
            .order_by(Media.episode_number.asc())
        )
        local_episodes = ep_result.scalars().all()

    # 2. Always fetch full season data from TMDB for consistent metadata
    api_key = await get_user_tmdb_key(db, current_user.id)

    try:
        if check_tmdb_key(api_key):
            import asyncio

            # Fetch season and show info (if not local)
            if not show:
                try:
                    tmdb_show_data = await tmdb.get_show(series_tmdb_id, api_key=api_key)
                except Exception:
                    tmdb_show_data = {}

                try:
                    tmdb_data = await tmdb.get_season(query_show_tmdb_id, query_season_number, api_key=api_key)
                except Exception:
                    tmdb_data = {
                        "id": None,
                        "season_number": season_number,
                        "name": f"Season {season_number}",
                        "overview": "No overview available.",
                        "poster_path": None,
                        "backdrop_path": None,
                        "air_date": None,
                        "vote_average": 0.0,
                        "episodes": []
                    }

                show_info = {
                    "id": None,
                    "tmdb_id": series_tmdb_id,
                    "tvdb_id": (
                        int(tmdb_show_data.get("external_ids", {}).get("tvdb_id"))
                        if tmdb_show_data.get("external_ids", {}).get("tvdb_id")
                        else None
                    ),
                    "title": tmdb_show_data.get("name"),
                    "poster_path": tmdb.poster_url(tmdb_show_data.get("poster_path")),
                    "backdrop_path": tmdb.poster_url(
                        tmdb_show_data.get("backdrop_path"), size="w1280"
                    ),
                    "seasons_meta": [
                        {
                            "season_number": s["season_number"],
                            "name": s.get("name"),
                            "overview": s.get("overview"),
                            "poster_path": tmdb.poster_url(s.get("poster_path")),
                            "episode_count": s.get("episode_count"),
                            "air_date": s.get("air_date"),
                        }
                        for s in tmdb_show_data.get("seasons", [])
                    ]
                }
            else:
                try:
                    tmdb_data = await tmdb.get_season(
                        query_show_tmdb_id, query_season_number, api_key=api_key
                    )
                except Exception:
                    tmdb_data = {
                        "id": None,
                        "season_number": season_number,
                        "name": f"Season {season_number}",
                        "overview": "No overview available.",
                        "poster_path": None,
                        "backdrop_path": None,
                        "air_date": None,
                        "vote_average": 0.0,
                        "episodes": []
                    }
                show_info = format_show(show)

            show_info = await get_enriched_show_info(db, current_user.id, series_tmdb_id, show_info)



            if season_override:
                tmdb_data = dict(tmdb_data)
                tmdb_data["season_number"] = season_number
                if tmdb_data.get("name") == f"Season {query_season_number}":
                    tmdb_data["name"] = f"Season {season_number}"
                
                mapped_episodes = []
                for ep in tmdb_data.get("episodes", []):
                    ep_copy = dict(ep)
                    ep_copy["season_number"] = season_number
                    mapped_episodes.append(ep_copy)
                tmdb_data["episodes"] = mapped_episodes

            # Bulk fetch watched state and ratings for episodes in this season
            tmdb_episodes = tmdb_data.get("episodes", [])
            total_in_season = len(tmdb_episodes)
            today_str = date.today().isoformat()
            total_aired_in_season = sum(
                1 for ep in tmdb_episodes
                if ep.get("air_date") and ep["air_date"] <= today_str
            )
            season_ep_tmdb_ids = [
                ep.get("id") for ep in tmdb_episodes if ep.get("id")
            ]

            # Fetch all local episodes for both shows and seasons
            show_ids = []
            if show:
                show_ids.append(show.id)
            
            other_show = None
            if season_override:
                other_show_q = await db.execute(
                    select(ShowModel).where(ShowModel.tmdb_id == query_show_tmdb_id)
                )
                other_show = other_show_q.scalar_one_or_none()
                if other_show and other_show.id not in show_ids:
                    show_ids.append(other_show.id)

            local_media_list = []
            
            # Query by show and season
            if show_ids:
                conditions = []
                if show:
                    conditions.append(and_(Media.show_id == show.id, Media.season_number == season_number))
                if other_show:
                    conditions.append(and_(Media.show_id == other_show.id, Media.season_number == query_season_number))
                
                local_by_show_q = await db.execute(
                    select(Media).where(
                        Media.media_type == MediaType.episode,
                        or_(*conditions)
                    )
                )
                local_media_list.extend(local_by_show_q.scalars().all())

            # Query by TMDB IDs
            if season_ep_tmdb_ids:
                local_by_tmdb_q = await db.execute(
                    select(Media).where(
                        Media.media_type == MediaType.episode,
                        Media.tmdb_id.in_(season_ep_tmdb_ids)
                    )
                )
                local_media_list.extend(local_by_tmdb_q.scalars().all())

            # De-duplicate by Python set/id
            seen_ids = set()
            local_media = []
            for m in local_media_list:
                if m.id not in seen_ids:
                    seen_ids.add(m.id)
                    local_media.append(m)

            local_media_by_ep: dict[int, list[Media]] = {}
            for m in local_media:
                if m.episode_number is not None:
                    local_media_by_ep.setdefault(m.episode_number, []).append(m)

            local_media_by_tmdb_id: dict[int, list[Media]] = {}
            for m in local_media:
                if m.tmdb_id is not None:
                    local_media_by_tmdb_id.setdefault(m.tmdb_id, []).append(m)

            local_media_ids = list(seen_ids)
            watched_media_ids = set()
            if local_media_ids:
                watched_q = await db.execute(
                    select(WatchEvent.media_id).where(
                        WatchEvent.user_id == current_user.id,
                        WatchEvent.media_id.in_(local_media_ids)
                    ).distinct()
                )
                watched_media_ids = {r[0] for r in watched_q.all()}

            episode_ratings = {}
            if local_media_ids:
                ratings_q = await db.execute(
                    select(Rating.media_id, Rating.rating).where(
                        Rating.user_id == current_user.id,
                        Rating.media_id.in_(local_media_ids),
                        Rating.season_number.is_(None)
                    )
                )
                episode_ratings = {r[0]: r[1] for r in ratings_q.all()}

            collected_media_ids = set()
            if local_media_ids:
                coll_q = await db.execute(
                    select(Collection.media_id).where(
                        Collection.user_id == current_user.id,
                        Collection.media_id.in_(local_media_ids)
                    ).distinct()
                )
                collected_media_ids = {r[0] for r in coll_q.all()}

            # Fetch list membership per episode
            episode_in_lists: dict[int, list[int]] = {}
            if local_media_ids:
                user_lists_q = await db.execute(
                    select(UserList.id).where(UserList.user_id == current_user.id)
                )
                user_list_ids = [r[0] for r in user_lists_q.all()]
                if user_list_ids:
                    ep_lists_q = await db.execute(
                        select(Media.tmdb_id, ListItem.list_id)
                        .join(ListItem, ListItem.media_id == Media.id)
                        .where(
                            Media.id.in_(local_media_ids),
                            ListItem.list_id.in_(user_list_ids),
                        )
                        .distinct()
                    )
                    for ep_tmdb_id, list_id in ep_lists_q.all():
                        episode_in_lists.setdefault(ep_tmdb_id, []).append(list_id)

            # Fetch active playback progress per episode
            episode_progress: dict[int, float] = {}
            if local_media_ids:
                progress_q = await db.execute(
                    select(PlaybackProgress.media_id, PlaybackProgress.progress_percent).where(
                        PlaybackProgress.user_id == current_user.id,
                        PlaybackProgress.media_id.in_(local_media_ids)
                    )
                )
                episode_progress = {r[0]: r[1] for r in progress_q.all()}

            episodes = []
            seen_ep_nums = set()
            for ep in tmdb_episodes:
                ep_num = ep.get("episode_number")
                ep_tmdb_id = ep.get("id")
                seen_ep_nums.add(ep_num)

                # Find all corresponding local Media rows
                eps_for_num = local_media_by_ep.get(ep_num, [])
                eps_for_tmdb = local_media_by_tmdb_id.get(ep_tmdb_id, []) if ep_tmdb_id else []
                matching_media = list({m.id: m for m in (eps_for_num + eps_for_tmdb)}.values())

                is_watched = any(m.id in watched_media_ids for m in matching_media)
                is_in_library = any(m.id in collected_media_ids for m in matching_media)

                local_media_id = matching_media[0].id if matching_media else None
                local_ep = matching_media[0] if matching_media else None

                user_rating = None
                for m in matching_media:
                    if m.id in episode_ratings:
                        user_rating = episode_ratings[m.id]
                        break

                user_progress = None
                for m in matching_media:
                    if m.id in episode_progress:
                        user_progress = min(100, max(0, int(episode_progress[m.id] * 100)))
                        break

                episodes.append(
                    {
                        "id": local_media_id,
                        "tmdb_id": ep_tmdb_id,
                        "type": "episode",
                        "title": (local_ep.custom_title or ep.get("name")) if local_ep else ep.get("name"),
                        "overview": ep.get("overview"),
                        "poster_path": tmdb.poster_url(
                            ep.get("still_path"), size="w500"
                        ),
                        "air_date": ep.get("air_date"),
                        "episode_number": ep_num,
                        "season_number": season_number,
                        "tmdb_rating": ep.get("vote_average"),
                        "in_library": is_in_library,
                        "runtime": ep.get("runtime"),
                        "watched": is_watched,
                        "user_rating": user_rating,
                        "in_lists": episode_in_lists.get(ep_tmdb_id, []) if ep_tmdb_id else [],
                        "progress_percent": user_progress,
                    }
                )

            # Merge any local episodes that aren't in TMDB episodes list (e.g. custom remapped/extra episodes)
            for local_ep in local_media:
                ep_num = local_ep.episode_number
                if ep_num in seen_ep_nums:
                    continue
                seen_ep_nums.add(ep_num)
                local_media_id = local_ep.id
                is_watched = local_media_id in watched_media_ids
                is_in_library = local_media_id in collected_media_ids
                user_rating = episode_ratings.get(local_media_id)

                user_progress = None
                if local_media_id in episode_progress:
                    user_progress = min(100, max(0, int(episode_progress[local_media_id] * 100)))

                episodes.append(
                    {
                        "id": local_media_id,
                        "tmdb_id": local_ep.tmdb_id,
                        "type": "episode",
                        "title": local_ep.custom_title or local_ep.title or f"Episode {ep_num}",
                        "overview": local_ep.overview or "No overview available.",
                        "poster_path": tmdb.poster_url(local_ep.poster_path, size="w500") if local_ep.poster_path else None,
                        "air_date": local_ep.release_date,
                        "episode_number": ep_num,
                        "season_number": season_number,
                        "tmdb_rating": local_ep.tmdb_rating or 0.0,
                        "in_library": is_in_library,
                        "runtime": local_ep.runtime,
                        "watched": is_watched,
                        "user_rating": user_rating,
                        "in_lists": [],
                        "progress_percent": user_progress,
                    }
                )

            show_state: dict = {"tmdb_id": series_tmdb_id, "type": "series"}
            await enrich_with_state(db, current_user.id, [show_state])

            collected_in_season = sum(1 for ep in episodes if ep.get("in_library"))
            watched_count = sum(1 for ep in episodes if ep.get("watched"))

            season_in_library = collected_in_season > 0
            aired_denom = total_aired_in_season if total_aired_in_season > 0 else total_in_season
            season_collection_pct = min(100, int((collected_in_season / aired_denom) * 100)) if aired_denom > 0 else 0
            season_watched = watched_count >= aired_denom if aired_denom > 0 else False
            season_watch_pct = min(100, int((watched_count / aired_denom) * 100)) if aired_denom > 0 else 0

            # Season user rating (stored against show's Media row with season_number)
            season_user_rating = None
            show_media_q = await db.execute(
                select(Media).where(Media.tmdb_id == series_tmdb_id, Media.media_type == MediaType.series)
            )
            show_media = show_media_q.scalars().first()
            if show_media:
                rating_q = await db.execute(
                    select(Rating.rating).where(
                        Rating.media_id == show_media.id,
                        Rating.user_id == current_user.id,
                        Rating.season_number == season_number,
                    )
                )
                season_user_rating = rating_q.scalar_one_or_none()

            return {
                "id": tmdb_data.get("id"),
                "tmdb_id": series_tmdb_id,
                "season_number": season_number,
                "name": (show.custom_season_names or {}).get(str(season_number)) or tmdb_data.get("name") if show else tmdb_data.get("name"),
                "overview": tmdb_data.get("overview"),
                "poster_path": tmdb.poster_url(tmdb_data.get("poster_path")),
                "backdrop_path": tmdb.poster_url(
                    tmdb_data.get("backdrop_path"), size="w1280"
                ),
                "air_date": tmdb_data.get("air_date"),
                "tmdb_rating": tmdb_data.get("vote_average"),
                "episodes": episodes,
                "show": show_info,
                "is_dropped": show_state.get("is_dropped", False),
                "show_watched": show_state.get("watched", False),
                "season_watched": season_watched,
                "season_watch_pct": season_watch_pct,
                "season_in_library": season_in_library,
                "season_collection_pct": season_collection_pct,
                "season_user_rating": season_user_rating,
                "show_in_lists": show_state.get("in_lists", []),
                "show_in_library": show_state.get("collection_pct", 0) > 0,
                "show_collection_pct": show_state.get("collection_pct", 0),
                "show_request_enabled": show_state.get("request_enabled", False),
                "show_is_monitored": show_state.get("is_monitored", False),
            }
        elif show:
            # Fallback to local only if no TMDB key
            return {
                "season_number": season_number,
                "name": f"Season {season_number}",
                "episodes": [format_media(ep) for ep in local_episodes],
                "show": format_show(show),
            }
        else:
            raise HTTPException(status_code=404, detail="Show not found")
    except Exception as e:
        if isinstance(e, HTTPException):
            raise e
        raise HTTPException(status_code=404, detail=f"Season not found: {e}")


@router.get("/{series_tmdb_id}/season/{season_number}/{episode_number}")
async def get_episode_detail(
    series_tmdb_id: int,
    season_number: int,
    episode_number: int,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    api_key = await get_user_tmdb_key(db, current_user.id)
    if not check_tmdb_key(api_key):
        raise HTTPException(status_code=404, detail="TMDB API Key not configured")

    # Check for active episode or season override
    ep_override_result = await db.execute(
        select(ShowEpisodeOverride).where(
            ShowEpisodeOverride.user_id == current_user.id,
            ShowEpisodeOverride.target_show_tmdb_id == series_tmdb_id,
            ShowEpisodeOverride.target_season_number == season_number,
            ShowEpisodeOverride.target_episode_number == episode_number
        )
    )
    ep_override = ep_override_result.scalar_one_or_none()

    query_show_tmdb_id = series_tmdb_id
    query_season_number = season_number
    query_episode_number = episode_number
    season_override = None

    if ep_override:
        query_show_tmdb_id = ep_override.source_show_tmdb_id
        query_season_number = ep_override.source_season_number
        query_episode_number = ep_override.source_episode_number
    else:
        season_override_result = await db.execute(
            select(ShowSeasonOverride).where(
                ShowSeasonOverride.user_id == current_user.id,
                ShowSeasonOverride.target_show_tmdb_id == series_tmdb_id,
                ShowSeasonOverride.target_season_number == season_number
            )
        )
        season_override = season_override_result.scalar_one_or_none()
        if season_override:
            query_show_tmdb_id = season_override.source_show_tmdb_id
            query_season_number = season_override.source_season_number

    try:
        show_result = await db.execute(
            select(ShowModel).where(ShowModel.tmdb_id == series_tmdb_id)
        )
        show = show_result.scalar_one_or_none()

        local_ep = None
        library_info = None
        if show:
            local_result = await db.execute(
                select(Media)
                .where(Media.media_type == MediaType.episode)
                .where(Media.show_id == show.id)
                .where(Media.season_number == season_number)
                .where(Media.episode_number == episode_number)
            )
            local_ep = local_result.scalars().first()

        ep_data = None
        if show:
            try:
                ep_data = await tmdb.get_episode(
                    query_show_tmdb_id, query_season_number, query_episode_number, api_key=api_key
                )
            except Exception:
                ep_data = {
                    "id": local_ep.tmdb_id if (local_ep and local_ep.tmdb_id) else None,
                    "season_number": season_number,
                    "episode_number": episode_number,
                    "name": local_ep.custom_title or local_ep.title if local_ep else f"Episode {episode_number}",
                    "overview": local_ep.overview if (local_ep and local_ep.overview) else "No overview available.",
                    "air_date": local_ep.release_date if local_ep else None,
                    "still_path": local_ep.poster_path if local_ep else None,
                    "vote_average": local_ep.tmdb_rating if local_ep else 0.0,
                    "runtime": local_ep.runtime if local_ep else 0,
                    "guest_stars": [],
                    "credits": {"cast": [], "crew": []}
                }
            show_info = format_show(show)
        else:
            try:
                ep_data = await tmdb.get_episode(
                    query_show_tmdb_id, query_season_number, query_episode_number, api_key=api_key
                )
            except Exception:
                ep_data = {
                    "id": None,
                    "season_number": season_number,
                    "episode_number": episode_number,
                    "name": f"Episode {episode_number}",
                    "overview": "No overview available.",
                    "air_date": None,
                    "still_path": None,
                    "vote_average": 0.0,
                    "runtime": 0,
                    "guest_stars": [],
                    "credits": {"cast": [], "crew": []}
                }
            show_tmdb = await tmdb.get_show(series_tmdb_id, api_key=api_key)
            show_info = {
                "id": None,
                "tmdb_id": series_tmdb_id,
                "tvdb_id": (
                    int(show_tmdb.get("external_ids", {}).get("tvdb_id"))
                    if show_tmdb.get("external_ids", {}).get("tvdb_id")
                    else None
                ),
                "title": show_tmdb.get("name"),
                "poster_path": tmdb.poster_url(show_tmdb.get("poster_path")),
                "backdrop_path": tmdb.poster_url(
                    show_tmdb.get("backdrop_path"), size="w1280"
                ),
                "seasons_meta": [
                    {
                        "season_number": s["season_number"],
                        "name": s.get("name"),
                        "overview": s.get("overview"),
                        "poster_path": tmdb.poster_url(s.get("poster_path")),
                        "episode_count": s.get("episode_count"),
                        "air_date": s.get("air_date"),
                    }
                    for s in show_tmdb.get("seasons", [])
                ]
            }

        show_info = await get_enriched_show_info(db, current_user.id, series_tmdb_id, show_info)

        if ep_override or season_override:
            ep_data = dict(ep_data)
            ep_data["season_number"] = season_number
            ep_data["episode_number"] = episode_number
            


        ep_tmdb_id = ep_data.get("id")
            
        if not local_ep and ep_tmdb_id:
            local_result_by_tmdb = await db.execute(
                select(Media)
                .where(Media.media_type == MediaType.episode)
                .where(Media.tmdb_id == ep_tmdb_id)
            )
            local_ep = local_result_by_tmdb.scalars().first()

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

        credits = ep_data.get("credits", {})
        cast = [
            {
                "tmdb_id": c.get("id"),
                "name": c.get("name"),
                "character": c.get("character"),
                "profile_path": tmdb.poster_url(c.get("profile_path")),
            }
            for c in credits.get("cast", [])[:12]
        ]
        guest_stars = [
            {
                "tmdb_id": c.get("id"),
                "name": c.get("name"),
                "character": c.get("character"),
                "profile_path": tmdb.poster_url(c.get("profile_path")),
            }
            for c in ep_data.get("guest_stars", [])[:6]
        ]

        # Get all episodes in this season for navigation (querying target season_number to match page being viewed)
        try:
            season_tmdb = await tmdb.get_season(
                series_tmdb_id, season_number, api_key=api_key
            )
        except Exception:
            season_tmdb = {"episodes": []}
        
        # Fetch any custom names for these navigation episodes
        nav_ep_tmdb_ids = [ep.get("id") for ep in season_tmdb.get("episodes", []) if ep.get("id")]
        nav_custom_titles = {}
        if nav_ep_tmdb_ids:
            nav_q = await db.execute(
                select(Media.tmdb_id, Media.custom_title)
                .where(Media.media_type == MediaType.episode)
                .where(Media.tmdb_id.in_(nav_ep_tmdb_ids))
                .where(Media.custom_title.isnot(None))
            )
            nav_custom_titles = {r[0]: r[1] for r in nav_q.all()}

        episodes_nav = [
            {
                "episode_number": ep.get("episode_number"),
                "title": nav_custom_titles.get(ep.get("id")) or ep.get("name"),
            }
            for ep in season_tmdb.get("episodes", [])
        ]

        ep_state: dict = {"tmdb_id": ep_tmdb_id, "type": "episode"}
        await enrich_with_state(db, current_user.id, [ep_state])

        return {
            "id": local_ep.id if local_ep else None,
            "tmdb_id": ep_tmdb_id,
            "in_library": ep_state.get("in_library", local_ep is not None),
            "watched": ep_state.get("watched", False),
            "in_lists": ep_state.get("in_lists", []),
            "collection_pct": ep_state.get("collection_pct", 100 if local_ep else 0),
            "user_rating": ep_state.get("user_rating"),
            "play_count": ep_state.get("play_count", 0),
            "progress_percent": ep_state.get("progress_percent"),
            "episode_number": episode_number,
            "season_number": season_number,
            "title": local_ep.custom_title or ep_data.get("name") if local_ep else ep_data.get("name"),
            "overview": ep_data.get("overview"),
            "still_path": tmdb.poster_url(ep_data.get("still_path"), size="w780"),
            "air_date": ep_data.get("air_date"),
            "runtime": ep_data.get("runtime"),
            "tmdb_rating": ep_data.get("vote_average"),
            "cast": cast,
            "guest_stars": guest_stars,
            "show": show_info,
            "episodes": episodes_nav,
            "library": library_info,
        }
    except Exception as e:
        if isinstance(e, HTTPException):
            raise e
        raise HTTPException(status_code=404, detail=f"Episode not found: {e}")


@router.post("/{series_tmdb_id}/refresh")
async def refresh_show_metadata(
    series_tmdb_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    api_key = await get_user_tmdb_key(db, current_user.id)
    if not check_tmdb_key(api_key):
        raise HTTPException(status_code=400, detail="TMDB API key not configured")

    show_result = await db.execute(
        select(ShowModel).where(ShowModel.tmdb_id == series_tmdb_id)
    )
    show = show_result.scalar_one_or_none()
    if not show:
        raise HTTPException(status_code=404, detail="Show not found in local library")

    try:
        data = await tmdb.get_show(series_tmdb_id, api_key=api_key)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"TMDB fetch failed: {e}")

    # Update show-level fields
    show.title = data.get("name") or show.title
    show.original_title = data.get("original_name")
    show.overview = data.get("overview")
    show.poster_path = tmdb.poster_url(data.get("poster_path"))
    show.backdrop_path = tmdb.poster_url(data.get("backdrop_path"), size="w1280")
    show.tmdb_rating = data.get("vote_average")
    show.status = data.get("status")
    show.tagline = data.get("tagline")
    show.first_air_date = data.get("first_air_date")
    show.last_air_date = data.get("last_air_date")
    show.tmdb_data = {
        "genres": [g["name"] for g in data.get("genres", [])],
        "external_ids": data.get("external_ids", {}),
        "original_language": data.get("original_language"),
        "seasons": [
            {
                "season_number": s["season_number"],
                "poster_path": tmdb.poster_url(s.get("poster_path")),
                "episode_count": s["episode_count"],
                "name": s["name"],
                "air_date": s.get("air_date"),
                "overview": s.get("overview"),
            }
            for s in data.get("seasons", [])
        ],
    }

    # Re-enrich all local episodes linked to this show
    ep_result = await db.execute(
        select(Media)
        .where(Media.media_type == MediaType.episode)
        .where(Media.show_id == show.id)
    )
    episodes = ep_result.scalars().all()

    # Also find orphaned episodes (show_id = null) in this user's collection
    # whose (season_number, episode_number) match TMDB data for this show.
    # This happens when the show's TMDB lookup failed during the original sync,
    # leaving episodes with no show_id even though they belong to this show.
    user_media_ids_sq = select(Collection.media_id).where(Collection.user_id == current_user.id)
    orphan_result = await db.execute(
        select(Media)
        .where(
            Media.media_type == MediaType.episode,
            Media.show_id.is_(None),
            Media.id.in_(user_media_ids_sq),
        )
    )
    orphans = orphan_result.scalars().all()

    semaphore = asyncio.Semaphore(10)
    season_data: dict[int, dict[int, dict]] = {}

    # Collect seasons needed: linked episodes + all show seasons (for orphan matching)
    linked_seasons = {ep.season_number for ep in episodes if ep.season_number is not None}
    tmdb_seasons = {s["season_number"] for s in data.get("seasons", [])}
    orphan_seasons = {ep.season_number for ep in orphans if ep.season_number is not None}
    # Only fetch seasons that could contain orphans belonging to this show
    needed_seasons = linked_seasons | (tmdb_seasons & orphan_seasons)

    async def fetch_season(sn: int) -> None:
        async with semaphore:
            try:
                d = await tmdb.get_season(series_tmdb_id, sn, api_key=api_key)
                season_data[sn] = {ep["episode_number"]: ep for ep in d.get("episodes", [])}
            except Exception:
                season_data[sn] = {}

    if needed_seasons:
        await asyncio.gather(*[fetch_season(sn) for sn in needed_seasons])

    def apply_episode_data(media: Media, ep: dict) -> None:
        media.show_id = show.id
        media.tmdb_id = ep.get("id") or media.tmdb_id
        media.title = ep.get("name") or media.title
        media.overview = ep.get("overview")
        media.poster_path = tmdb.poster_url(ep.get("still_path"), size="w500")
        media.release_date = ep.get("air_date")
        media.tmdb_rating = ep.get("vote_average")
        media.tmdb_data = {"runtime": ep.get("runtime"), "cast": []}

    for media in episodes:
        if media.season_number is None:
            continue
        ep = season_data.get(media.season_number, {}).get(media.episode_number)
        if ep:
            apply_episode_data(media, ep)

    # Adopt orphans whose (season, episode) has exactly one candidate in this show's TMDB data
    for media in orphans:
        if media.season_number is None or media.episode_number is None:
            continue
        ep = season_data.get(media.season_number, {}).get(media.episode_number)
        if ep:
            apply_episode_data(media, ep)

    all_media_ids = [ep.id for ep in episodes] + [ep.id for ep in orphans]
    await refresh_technical_data(db, all_media_ids, current_user.id)

    await db.commit()
    return {"message": "Metadata refreshed successfully"}


# ── TVDB Show Endpoints ─────────────────────────────────────────────────────


@router.get("/tvdb/{tvdb_id}")
async def get_tvdb_show(
    tvdb_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    settings_q = await db.execute(select(UserSettings).where(UserSettings.user_id == current_user.id))
    settings = settings_q.scalar_one_or_none()

    api_key = await get_user_tvdb_key(db, current_user.id)
    if not api_key:
        raise HTTPException(status_code=400, detail="TVDB API key not configured")

    user_lang = await get_user_content_language(db, current_user.id)
    tvdb_lang = tvdb_client.to_three_letter_lang(user_lang)

    try:
        raw = await tvdb_client.get_series(tvdb_id, api_key, lang=tvdb_lang)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"TVDB fetch failed: {e}")

    show_data = tvdb_client.format_series(raw, lang=tvdb_lang)
    cast = tvdb_client.format_cast(raw)

    # Look up local Show row by tvdb_id
    show_result = await db.execute(select(ShowModel).where(ShowModel.tvdb_id == tvdb_id))
    show = show_result.scalar_one_or_none()

    # Collect per-season library state if we have a local show
    season_states: dict = {}
    if show:
        coll_per_season_q = await db.execute(
            select(Media.season_number, func.count(func.distinct(Media.episode_number)))
            .join(Collection, Collection.media_id == Media.id)
            .where(
                Media.show_id == show.id,
                Collection.user_id == current_user.id,
                Media.media_type == MediaType.episode,
                Media.season_number.isnot(None),
                Media.episode_number.isnot(None),
            )
            .group_by(Media.season_number)
        )
        coll_per_season: dict = dict(coll_per_season_q.all())

        watched_per_season_q = await db.execute(
            select(Media.season_number, func.count(func.distinct(Media.episode_number)))
            .join(WatchEvent, WatchEvent.media_id == Media.id)
            .where(
                Media.show_id == show.id,
                WatchEvent.user_id == current_user.id,
                Media.media_type == MediaType.episode,
                Media.season_number.isnot(None),
                Media.episode_number.isnot(None),
            )
            .group_by(Media.season_number)
        )
        watched_per_season: dict = dict(watched_per_season_q.all())

        season_ep_counts = {s["season_number"]: s.get("episode_count", 0) for s in show_data["seasons"]}

        for sn in set(list(coll_per_season.keys()) + list(season_ep_counts.keys())):
            collected = coll_per_season.get(sn, 0)
            watched = watched_per_season.get(sn, 0)
            total = season_ep_counts.get(sn, 0)
            # When TVDB reports 0 episodes (common for web series), use collected count as denominator
            effective_total = total if total > 0 else collected
            season_states[sn] = {
                "in_library": collected > 0,
                "collection_pct": min(100, int((collected / effective_total) * 100)) if effective_total > 0 else 0,
                "watched": watched >= effective_total if effective_total > 0 else False,
                "watch_pct": min(100, int((watched / effective_total) * 100)) if effective_total > 0 else 0,
                "user_rating": None,
            }

    in_library = bool(season_states and any(v["in_library"] for v in season_states.values()))

    collection_pct = 0
    watch_pct = 0
    watched_overall = False
    include_specials = False

    if show:
        include_specials = settings.preferences.get("include_specials", False) if (settings and settings.preferences) else False
        lib_seasons = [v for sn, v in season_states.items() if (include_specials or sn != 0) and v["in_library"]]
        watched_overall = bool(lib_seasons) and all(v["watched"] for v in lib_seasons)

        total_eps = sum(count for sn, count in season_ep_counts.items() if (include_specials or sn != 0))
        total_coll = sum(coll_per_season.get(sn, 0) for sn in coll_per_season if (include_specials or sn != 0))
        total_watched = sum(watched_per_season.get(sn, 0) for sn in watched_per_season if (include_specials or sn != 0))

        collection_pct = min(100, int((total_coll / total_eps) * 100)) if total_eps > 0 else 0
        watch_pct = min(100, int((total_watched / total_eps) * 100)) if total_eps > 0 else 0

    # Sonarr state
    gs = await _get_global_settings(db)
    sonarr_cfg = _effective_sonarr(settings, gs)
    is_monitored = False
    request_enabled = sonarr_cfg is not None
    if sonarr_cfg:
        try:
            import httpx as _httpx
            url = sonarr_cfg.sonarr_url.rstrip("/")
            async with _httpx.AsyncClient(timeout=5.0) as client:
                res = await client.get(
                    f"{url}/api/v3/series/lookup",
                    params={"apiKey": sonarr_cfg.sonarr_token, "term": f"tvdb:{tvdb_id}"},
                )
                if res.status_code == 200:
                    for entry in res.json():
                        if entry.get("id"):
                            is_monitored = True
                            break
        except Exception:
            pass

    # Where to watch (local media servers only; no TMDB streaming for TVDB-only shows)
    where_to_watch = await get_where_to_watch(db, current_user.id, tvdb_id, MediaType.series, show=show) if show else []

    # Networks (name only; TVDB doesn't provide logos)
    networks = [{"id": None, "name": n.get("name"), "logo_path": None, "origin_country": None}
                for n in (raw.get("networks") or []) if n.get("name")]

    # Blocklist and drop status
    from models.blocklist import BlocklistItem
    is_blocked = False
    is_dropped = False
    block_ids = [-tvdb_id]
    if show_data.get("tmdb_id_cross"):
        block_ids.append(show_data["tmdb_id_cross"])

    block_q = await db.execute(
        select(BlocklistItem.is_dropped)
        .where(
            BlocklistItem.user_id == current_user.id,
            BlocklistItem.media_type == MediaType.series,
            BlocklistItem.tmdb_id.in_(block_ids),
        )
    )
    block_row = block_q.first()
    if block_row is not None:
        is_dropped = block_row[0]
        is_blocked = not is_dropped

    if not is_blocked:
        cf = await _get_content_filters(db, current_user.id)
        temp_item = {
            "type": "series",
            "title": show_data.get("title"),
            "genres": show_data.get("genres", []),
            "original_language": show_data.get("original_language"),
            "age_rating": show_data.get("age_rating"),
        }
        is_blocked = _is_content_filtered(temp_item, *cf)

    return {
        **show_data,
        "id": show.id if show else None,
        "tmdb_id": None,
        "type": "series",
        "tagline": None,
        "tmdb_rating": None,
        "imdb_id": show_data.get("imdb_id"),
        "tmdb_id_cross": show_data.get("tmdb_id_cross"),
        "adult": False,
        "in_library": in_library,
        "watched": watched_overall,
        "in_lists": [],
        "collection_pct": collection_pct,
        "watch_pct": watch_pct,
        "is_monitored": is_monitored,
        "request_enabled": request_enabled,
        "is_blocked": is_blocked,
        "is_dropped": is_dropped,
        "request_status": None,
        "user_rating": None,
        "season_states": season_states,
        "seasons": {},
        "seasons_meta": show_data["seasons"],
        "cast": cast,
        "networks": networks,
        "where_to_watch": where_to_watch,
        "include_specials": include_specials,
    }


@router.get("/tvdb/{tvdb_id}/season/{season_number}")
async def get_tvdb_season(
    tvdb_id: int,
    season_number: int,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    api_key = await get_user_tvdb_key(db, current_user.id)
    if not api_key:
        raise HTTPException(status_code=400, detail="TVDB API key not configured")

    user_lang = await get_user_content_language(db, current_user.id)
    tvdb_lang = tvdb_client.to_three_letter_lang(user_lang)

    try:
        raw_series, raw_episodes = await asyncio.gather(
            tvdb_client.get_series(tvdb_id, api_key, lang=tvdb_lang),
            tvdb_client.get_series_episodes(tvdb_id, season_number, api_key, lang=tvdb_lang),
        )
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"TVDB fetch failed: {e}")

    show_data = tvdb_client.format_series(raw_series, lang=tvdb_lang)
    
    seen_eps = set()
    eps = []
    for e in raw_episodes:
        if e.get("id") not in seen_eps:
            seen_eps.add(e.get("id"))
            eps.append(tvdb_client.format_episode(e))

    # Look up local Show row and episode states
    show_result = await db.execute(select(ShowModel).where(ShowModel.tvdb_id == tvdb_id))
    show = show_result.scalar_one_or_none()

    watched_ep_ids: set = set()
    episode_ratings: dict = {}
    user_collected_eps: set = set()

    if show:
        ep_result = await db.execute(
            select(Media)
            .where(
                Media.show_id == show.id,
                Media.media_type == MediaType.episode,
                Media.season_number == season_number,
            )
        )
        local_eps = ep_result.scalars().all()
        local_media_ids = [m.id for m in local_eps]

        if local_media_ids:
            watched_q = await db.execute(
                select(WatchEvent.media_id)
                .where(WatchEvent.user_id == current_user.id, WatchEvent.media_id.in_(local_media_ids))
                .distinct()
            )
            watched_ep_ids = {r[0] for r in watched_q.all()}

            ep_ratings_q = await db.execute(
                select(Rating.media_id, Rating.rating)
                .where(
                    Rating.user_id == current_user.id,
                    Rating.media_id.in_(local_media_ids),
                    Rating.season_number.is_(None),
                )
            )
            episode_ratings = {r[0]: r[1] for r in ep_ratings_q.all()}

            coll_q = await db.execute(
                select(Media.episode_number)
                .join(Collection, Collection.media_id == Media.id)
                .where(
                    Media.show_id == show.id,
                    Media.season_number == season_number,
                    Collection.user_id == current_user.id,
                    Media.media_type == MediaType.episode,
                    Media.episode_number.isnot(None),
                )
                .distinct()
            )
            user_collected_eps = {r[0] for r in coll_q.all()}

        # Build episode_number → local Media map
        local_ep_map = {m.episode_number: m for m in local_eps}
    else:
        local_ep_map = {}

    enriched_eps = []
    for ep in eps:
        ep_num = ep.get("episode_number")
        local_m = local_ep_map.get(ep_num)
        enriched_eps.append({
            **ep,
            "id": local_m.id if local_m else None,
            "in_library": ep_num in user_collected_eps,
            "watched": local_m.id in watched_ep_ids if local_m else False,
            "user_rating": episode_ratings.get(local_m.id) if local_m else None,
            "in_lists": [],
        })

    season_meta = next((s for s in show_data["seasons"] if s["season_number"] == season_number), {})

    # Compute season-level stats
    season_in_library = bool(user_collected_eps)
    total_eps = len(eps)
    effective_total = total_eps if total_eps > 0 else len(user_collected_eps)
    season_collection_pct = min(100, int((len(user_collected_eps) / effective_total) * 100)) if effective_total > 0 else 0
    season_watched = bool(local_eps) and len(watched_ep_ids) >= len(local_eps) if show else False
    season_watch_pct = min(100, int((len(watched_ep_ids) / effective_total) * 100)) if effective_total > 0 else 0

    return {
        "tvdb_id": tvdb_id,
        "season_number": season_number,
        "name": season_meta.get("name") or f"Season {season_number}",
        "overview": season_meta.get("overview"),
        "poster_path": season_meta.get("poster_path"),
        "backdrop_path": show_data["backdrop_path"],
        "air_date": season_meta.get("air_date"),
        "episodes": enriched_eps,
        "season_in_library": season_in_library,
        "season_watched": season_watched,
        "season_watch_pct": season_watch_pct,
        "season_collection_pct": season_collection_pct,
        "season_user_rating": None,
        "show_in_library": show is not None,
        "show": {
            "id": show.id if show else None,
            "tvdb_id": tvdb_id,
            "tmdb_id_cross": show_data.get("tmdb_id_cross"),
            "title": show_data["title"],
            "poster_path": show_data["poster_path"],
            "backdrop_path": show_data["backdrop_path"],
            "seasons_meta": show_data["seasons"],
        },
    }


@router.get("/tvdb/{tvdb_id}/season/{season_number}/episode/{episode_number}")
async def get_tvdb_episode(
    tvdb_id: int,
    season_number: int,
    episode_number: int,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    api_key = await get_user_tvdb_key(db, current_user.id)
    if not api_key:
        raise HTTPException(status_code=400, detail="TVDB API key not configured")

    user_lang = await get_user_content_language(db, current_user.id)
    tvdb_lang = tvdb_client.to_three_letter_lang(user_lang)

    try:
        raw_series, raw_episodes = await asyncio.gather(
            tvdb_client.get_series(tvdb_id, api_key, lang=tvdb_lang),
            tvdb_client.get_series_episodes(tvdb_id, season_number, api_key, lang=tvdb_lang),
        )
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"TVDB fetch failed: {e}")

    show_data = tvdb_client.format_series(raw_series, lang=tvdb_lang)
    
    seen_eps = set()
    eps = []
    for e in raw_episodes:
        if e.get("id") not in seen_eps:
            seen_eps.add(e.get("id"))
            eps.append(tvdb_client.format_episode(e))
    ep_data = next((e for e in eps if e.get("episode_number") == episode_number), None)
    if not ep_data:
        raise HTTPException(status_code=404, detail="Episode not found")

    show_result = await db.execute(select(ShowModel).where(ShowModel.tvdb_id == tvdb_id))
    show = show_result.scalar_one_or_none()

    in_library = False
    watched = False
    user_rating = None
    local_ep_id = None
    library_info = None
    play_count = 0
    progress_percent = None

    if show:
        local_ep_q = await db.execute(
            select(Media).where(
                Media.show_id == show.id,
                Media.media_type == MediaType.episode,
                Media.season_number == season_number,
                Media.episode_number == episode_number,
            )
        )
        local_ep = local_ep_q.scalars().first()
        if local_ep:
            local_ep_id = local_ep.id
            coll_q = await db.execute(
                select(func.count()).select_from(Collection).where(
                    Collection.media_id == local_ep.id,
                    Collection.user_id == current_user.id,
                )
            )
            in_library = coll_q.scalar_one() > 0
            watched_q = await db.execute(
                select(func.count()).select_from(WatchEvent).where(
                    WatchEvent.media_id == local_ep.id,
                    WatchEvent.user_id == current_user.id,
                )
            )
            play_count = watched_q.scalar_one()
            watched = play_count > 0
            rating_q = await db.execute(
                select(Rating.rating).where(
                    Rating.media_id == local_ep.id,
                    Rating.user_id == current_user.id,
                    Rating.season_number.is_(None),
                )
            )
            user_rating = rating_q.scalar_one_or_none()

            progress_q = await db.execute(
                select(PlaybackProgress.progress_percent).where(
                    PlaybackProgress.media_id == local_ep.id,
                    PlaybackProgress.user_id == current_user.id
                )
            )
            progress_pct = progress_q.scalar_one_or_none()
            progress_percent = min(100, max(0, int(progress_pct * 100))) if progress_pct is not None else None

            if in_library:
                coll_file_q = await db.execute(
                    select(CollectionFile)
                    .join(Collection, Collection.id == CollectionFile.collection_id)
                    .where(
                        Collection.media_id == local_ep.id,
                        Collection.user_id == current_user.id,
                    )
                    .order_by(CollectionFile.added_at.desc())
                )
                coll_file = coll_file_q.scalars().first()
                if coll_file:
                    library_info = {
                        "resolution": coll_file.resolution,
                        "video_codec": coll_file.video_codec,
                        "audio_codec": coll_file.audio_codec,
                        "audio_channels": coll_file.audio_channels,
                        "audio_languages": coll_file.audio_languages,
                        "subtitle_languages": coll_file.subtitle_languages,
                    }

    cast = tvdb_client.format_cast(raw_series)
    season_meta = next((s for s in show_data["seasons"] if s["season_number"] == season_number), {})

    return {
        **ep_data,
        "id": local_ep_id,
        "in_library": in_library,
        "watched": watched,
        "user_rating": user_rating,
        "play_count": play_count,
        "progress_percent": progress_percent,
        "in_lists": [],
        "library": library_info,
        "cast": cast,
        "episodes": [{"episode_number": e["episode_number"], "name": e["name"]} for e in eps],
        "show": {
            "id": show.id if show else None,
            "tvdb_id": tvdb_id,
            "tmdb_id_cross": show_data.get("tmdb_id_cross"),
            "title": show_data["title"],
            "poster_path": show_data["poster_path"],
            "backdrop_path": show_data["backdrop_path"],
            "seasons_meta": show_data["seasons"],
        },
        "season": {
            "name": season_meta.get("name") or f"Season {season_number}",
            "season_number": season_number,
            "poster_path": season_meta.get("poster_path"),
        },
    }


@router.post("/tvdb/{tvdb_id}/refresh")
async def refresh_tvdb_show_metadata(
    tvdb_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    api_key = await get_user_tvdb_key(db, current_user.id)
    if not api_key:
        raise HTTPException(status_code=400, detail="TVDB API key not configured")

    show_result = await db.execute(select(ShowModel).where(ShowModel.tvdb_id == tvdb_id))
    show = show_result.scalar_one_or_none()
    if not show:
        raise HTTPException(status_code=404, detail="Show not found in local library")

    user_lang = await get_user_content_language(db, current_user.id)
    tvdb_lang = tvdb_client.to_three_letter_lang(user_lang)

    try:
        raw = await tvdb_client.get_series(tvdb_id, api_key, lang=tvdb_lang)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"TVDB fetch failed: {e}")

    show_fmt = tvdb_client.format_series(raw, lang=tvdb_lang)
    show.title = show_fmt["title"] or show.title
    show.original_title = show_fmt.get("original_title")
    show.overview = show_fmt.get("overview")
    show.poster_path = show_fmt.get("poster_path")
    show.backdrop_path = show_fmt.get("backdrop_path")
    show.status = show_fmt.get("status")
    show.first_air_date = show_fmt.get("first_air_date")
    show.last_air_date = show_fmt.get("last_air_date")
    show.tmdb_data = {
        **(show.tmdb_data or {}),
        "seasons": show_fmt.get("seasons", []),
        "genres": show_fmt.get("genres", []),
        "source": "tvdb",
    }
    await db.commit()
    return {"message": "Metadata refreshed successfully"}
