import inspect
import logging

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from core import tmdb
from core import tvdb as tvdb_client
from models.media import Media, MediaType

logger = logging.getLogger(__name__)


def tmdb_season_covers(show_tmdb_data: dict | None, season_number: int, episode_number: int) -> bool:
    """True if the show's cached TMDB season list (shape: [{season_number,
    episode_count, name}, ...], as stored on Show.tmdb_data) plausibly has
    this season/episode position. Used to decide whether a TVDB episode with
    no local mapping genuinely has no TMDB counterpart (confidently absent,
    safe to enrich from TVDB directly) versus might still exist on TMDB but
    hasn't been matched yet (ambiguous — don't guess)."""
    if not show_tmdb_data:
        return False
    for season in show_tmdb_data.get("seasons", []):
        if season.get("season_number") == season_number:
            return episode_number <= (season.get("episode_count") or 0)
    return False


def is_unmapped_tvdb_episode(media: Media) -> bool:
    """True if this episode Media row was created from TVDB data because it
    has no TMDB counterpart. Such rows must be excluded from anything sent to
    services that expect real TMDB identifiers (Trakt, Simkl, MDBList)."""
    return (
        media.media_type == MediaType.episode
        and isinstance(media.tmdb_data, dict)
        and media.tmdb_data.get("source") == "tvdb"
    )


async def enrich_episode_from_tvdb(media: Media, tvdb_episode_data: dict) -> None:
    """Populate a bare episode Media record from TVDB data (shape: core.tvdb's
    format_episode output) for an episode that has no TMDB counterpart.

    The TVDB episode id lands in both uri_id ("tvdb:e:<id>", the canonical
    identifier every URL builder reads) and tmdb_id, which older tmdb_id-keyed
    consumers still index on. tmdb_data.source is tagged "tvdb" so
    is_unmapped_tvdb_episode can keep these rows out of outbound TMDB pushes.
    """
    tvdb_episode_id = tvdb_episode_data.get("tvdb_id")
    if tvdb_episode_id:
        media.tmdb_id = tvdb_episode_id
        if not media.uri_id:
            media.uri_id = f"tvdb:e:{tvdb_episode_id}"
    # TVDB sometimes has an episode with no name at all (#173) - media.title is
    # NOT NULL, so a brand-new row (title still unset) needs a fallback rather
    # than failing the insert.
    episode_number = tvdb_episode_data.get("episode_number", media.episode_number)
    fallback_title = f"Episode {episode_number}" if episode_number is not None else "Untitled Episode"
    media.title = tvdb_episode_data.get("name") or media.title or fallback_title
    media.overview = tvdb_episode_data.get("overview")
    if tvdb_episode_data.get("image_url"):
        media.poster_path = tvdb_episode_data["image_url"]
    media.release_date = tvdb_episode_data.get("air_date")
    media.runtime = tvdb_episode_data.get("runtime") or media.runtime
    media.tmdb_data = {
        "runtime": tvdb_episode_data.get("runtime"),
        "tvdb_episode_id": tvdb_episode_id,
        "source": "tvdb",
    }


def extract_movie_certification(data: dict, country: str = "US") -> str | None:
    """Age certification from a TMDB movie payload, for `country` then US then any."""
    results = (data.get("release_dates") or {}).get("results", [])

    def _for(code: str) -> str | None:
        for entry in results:
            if entry.get("iso_3166_1") != code:
                continue
            for rd in entry.get("release_dates", []):
                cert = (rd.get("certification") or "").strip()
                if cert:
                    return cert
        return None

    for preferred in (country, "US"):
        cert = _for(preferred)
        if cert:
            return cert
    for entry in results:
        for rd in entry.get("release_dates", []):
            cert = (rd.get("certification") or "").strip()
            if cert:
                return cert
    return None


def extract_show_content_rating(data: dict, country: str = "US") -> str | None:
    """US content rating (TV-Y/TV-G/TV-14/TV-MA…) from a TMDB show detail payload.

    Falls back to the first rating of any country so non-US-rated titles still
    get a value to filter on.
    """
    results = (data.get("content_ratings") or {}).get("results", [])
    for preferred in (country, "US"):
        for entry in results:
            if entry.get("iso_3166_1") == preferred:
                rating = (entry.get("rating") or "").strip()
                if rating:
                    return rating
    for entry in results:
        rating = (entry.get("rating") or "").strip()
        if rating:
            return rating
    return None


def _extract_release_dates(results: list) -> dict:
    us_entry = next((e for e in results if e.get("iso_3166_1") == "US"), None)
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


async def enrich_media(
    media: Media,
    api_key: str = None,
    series_tmdb_id: int = None,
    is_tvdb: bool = False,
    tvdb_api_key: str = None,
    tvdb_lang: str = "eng",
    series_tvdb_id: int = None,
    series_uri_id: str | None = None,
    db=None,
    bypass_cache: bool = False,
) -> None:
    """Fetch TMDB/TVDB metadata and update the media record in place.

    series_uri_id (e.g. 'tvdb:s:81189', 'tmdb:s:1396') resolves series context without
    requiring the caller to extract integer IDs first. Provide db for cross-provider alias lookup.
    """
    # bypass_cache: skip the shared TMDB response cache - set for a user-initiated
    # refresh, which must not be answered from a response fetched moments earlier.
    cache_ttl = None if bypass_cache else tmdb.DEFAULT_CACHE_TTL

    # Resolve series_uri_id → integer params when caller uses URI instead of raw ints
    if series_uri_id and not (series_tmdb_id or series_tvdb_id):
        try:
            from utils.media_uri import MediaURI
            _suri = MediaURI.parse(series_uri_id)
            if _suri.provider == "tvdb":
                series_tvdb_id = int(_suri.id)
                is_tvdb = True
            elif _suri.provider == "tmdb":
                series_tmdb_id = int(_suri.id)
            if db and not series_tvdb_id and series_tmdb_id:
                from utils.alias_lookup import get_provider_id_for_uri
                _tvdb = await get_provider_id_for_uri(db, series_uri_id, "tvdb")
                if _tvdb:
                    series_tvdb_id = int(_tvdb)
        except (ValueError, Exception):
            pass

    if media.media_type == MediaType.movie and not media.tmdb_id:
        if media.uri_id and media.uri_id.startswith("tmdb:m:"):
            try:
                media.tmdb_id = int(media.uri_id.split(":")[2])
            except (IndexError, ValueError):
                pass
        if not media.tmdb_id:
            return
    if media.media_type == MediaType.episode and not series_tmdb_id and not series_tvdb_id:
        return

    try:
        if media.media_type == MediaType.movie:
            data = await tmdb.get_movie(media.tmdb_id, api_key=api_key, cache_ttl=cache_ttl)
            media.title = data.get("title") or media.title
            media.original_title = data.get("original_title")
            media.overview = data.get("overview")
            media.poster_path = tmdb.poster_url(data.get("poster_path"))
            media.backdrop_path = tmdb.poster_url(data.get("backdrop_path"), size="w1280")
            media.release_date = data.get("release_date")
            media.tmdb_rating = data.get("vote_average")
            media.runtime = data.get("runtime") or media.runtime
            media.tmdb_data = {
                "runtime": data.get("runtime"),
                "genres": [g["name"] for g in data.get("genres", [])],
                "external_ids": data.get("external_ids", {}),
                "cast": [
                    {"name": c["name"], "character": c["character"], "profile_path": tmdb.poster_url(c.get("profile_path"), size="w185")}
                    for c in data.get("credits", {}).get("cast", [])[:10]
                ],
                "tagline": data.get("tagline"),
                "status": data.get("status"),
                "adult": data.get("adult", False),
                "release_dates": _extract_release_dates(data.get("release_dates", {}).get("results", [])),
                # #319: the detail page's mid/post-credits badge. Stored on every
                # enrichment so it is not left to the webhook backfill alone.
                **tmdb.credits_stinger_fields(data),
            }
            media.content_rating = extract_movie_certification(data)
            media.adult = data.get("adult", False)

        elif media.media_type == MediaType.series:
            if not media.tmdb_id:
                return
            data = await tmdb.get_show(media.tmdb_id, api_key=api_key, cache_ttl=cache_ttl)
            media.title = data.get("name") or media.title
            media.original_title = data.get("original_name")
            media.overview = data.get("overview")
            media.poster_path = tmdb.poster_url(data.get("poster_path"))
            media.backdrop_path = tmdb.poster_url(data.get("backdrop_path"), size="w1280")
            media.release_date = data.get("first_air_date")
            media.tmdb_rating = data.get("vote_average")
            media.tmdb_data = {
                "genres": [g["name"] for g in data.get("genres", [])],
                "cast": [
                    {"name": c["name"], "character": c.get("character", ""), "profile_path": tmdb.poster_url(c.get("profile_path"), size="w185")}
                    for c in data.get("credits", {}).get("cast", [])[:10]
                ],
                "tagline": data.get("tagline"),
                "status": data.get("status"),
                "adult": data.get("adult", False),
                # A show carries the same community stinger keywords a movie
                # does, so the badge should not be movie-only (#319).
                **tmdb.credits_stinger_fields(data),
            }
            media.content_rating = extract_show_content_rating(data)
            media.adult = data.get("adult", False)

            if series_tvdb_id and tvdb_api_key:
                tvdb_poster, tvdb_backdrop = await _tvdb_series_artwork(series_tvdb_id, tvdb_api_key, tvdb_lang)
                if tvdb_poster or tvdb_backdrop:
                    media.tvdb_data = {
                        **(media.tvdb_data or {}),
                        "tvdb_id": series_tvdb_id,
                        "poster_path": tvdb_poster,
                        "backdrop_path": tvdb_backdrop,
                    }

        elif media.media_type == MediaType.episode:
            if media.season_number is None or media.episode_number is None:
                return

            # A show with no TMDB match at all still has to enrich - go straight
            # to its TVDB counterpart rather than falling through to a TMDB call
            # there is no series id for.
            if not series_tmdb_id and series_tvdb_id and tvdb_api_key:
                try:
                    raw_eps = await tvdb_client.get_series_episodes(
                        series_tvdb_id, media.season_number, tvdb_api_key, language=tvdb_lang
                    )
                    ep = next(
                        (e for e in raw_eps if e.get("number") == media.episode_number), None
                    )
                    if ep:
                        await enrich_episode_from_tvdb(
                            media, tvdb_client.format_episode(ep, tvdb_lang)
                        )
                        return
                except Exception as e:
                    print(f"  TVDB-only episode enrich failed: {e}")

            enriched = False
            if is_tvdb and tvdb_api_key and series_tvdb_id:
                try:
                    raw_eps = await tvdb_client.get_series_episodes(series_tvdb_id, media.season_number, tvdb_api_key, language=tvdb_lang)
                    ep = next((e for e in raw_eps if e.get("number") == media.episode_number), None)
                    if ep:
                        tvdb_ep_id = ep.get("id")
                        if tvdb_ep_id:
                            if not media.uri_id:
                                media.uri_id = f"tvdb:e:{tvdb_ep_id}"
                            # Do NOT set media.tmdb_id to a TVDB ID — different namespace
                        media.title = media.title or ep.get("name")
                        media.overview = media.overview or ep.get("overview")
                        media.release_date = media.release_date or ep.get("aired")
                        media.runtime = media.runtime or ep.get("runtime")
                        # Ids belong in media_aliases; tvdb_data carries artwork.
                        media.tvdb_data = {
                            **(media.tvdb_data or {}),
                            "poster_path": tvdb_client._image_url(ep["image"]) if ep.get("image") else None,
                        }
                        # TMDB still names the row; only fall back to TVDB for a
                        # title when TMDB has nothing to say about this episode.
                        enriched = not series_tmdb_id
                except Exception as e:
                    print(f"  TVDB enrich failed for episode: {e}, falling back to TMDB")

            if not enriched and series_tmdb_id:
                try:
                    data = await tmdb.get_episode(series_tmdb_id, media.season_number, media.episode_number, api_key=api_key, cache_ttl=cache_ttl)
                except Exception:
                    data = {}
                # A row created from a media server, Trakt or a scrobble carries
                # the source's numbering. When it also carries a TMDB episode id
                # and that id isn't what sits at this position, the position is
                # the source's, not TMDB's — so re-file the row where TMDB puts
                # it. Costs one extra read, and only for a show the catalogues
                # actually disagree about.
                if media.tmdb_id and data.get("id") != media.tmdb_id:
                    from core.episode_order import tmdb_episode_index

                    try:
                        placed = (await tmdb_episode_index(series_tmdb_id, api_key)).get(media.tmdb_id)
                    except Exception:
                        placed = None
                    if placed:
                        media.season_number, media.episode_number, data = placed
                if not data:
                    # TMDB has no episode at this position - some shows' season
                    # structures only line up under TVDB's numbering (#162, #186),
                    # so fall back to the show's TVDB match before giving up.
                    if series_tvdb_id and tvdb_api_key:
                        try:
                            raw_eps = await tvdb_client.get_series_episodes(
                                series_tvdb_id, media.season_number, tvdb_api_key, language=tvdb_lang
                            )
                            ep = next(
                                (e for e in raw_eps if e.get("number") == media.episode_number), None
                            )
                            if ep:
                                await enrich_episode_from_tvdb(
                                    media, tvdb_client.format_episode(ep, tvdb_lang)
                                )
                                return
                        except Exception as e:
                            print(f"  TVDB episode fallback failed: {e}")
                    # Nothing from either provider. Mark the attempt with an empty
                    # dict rather than leaving tmdb_data None, so the sync
                    # heal-check doesn't retry this row forever.
                    if media.tmdb_data is None:
                        media.tmdb_data = {}
                    return
                ep_id = data.get("id") or media.tmdb_id
                media.tmdb_id = ep_id
                if ep_id and not media.uri_id:
                    media.uri_id = f"tmdb:e:{ep_id}"
                media.title = data.get("name") or media.title
                media.overview = data.get("overview")
                media.poster_path = tmdb.poster_url(data.get("still_path"), size="w500")
                media.release_date = data.get("air_date")
                media.tmdb_rating = data.get("vote_average")
                media.runtime = data.get("runtime") or media.runtime
                media.tmdb_data = {
                    "runtime": data.get("runtime"),
                    "cast": [
                        {
                            "name": c["name"],
                            "character": c["character"],
                            "profile_path": tmdb.poster_url(c.get("profile_path"), size="w185")
                        }
                        for c in data.get("credits", {}).get("cast", [])[:10]
                    ],
                }

    except Exception as e:
        # Don't let failures break webhook processing.
        # Set tmdb_data to an empty dict (not None) so the sync heal-check knows
        # enrichment was already attempted and won't retry it indefinitely.
        if media.tmdb_data is None:
            media.tmdb_data = {}
        from httpx import HTTPStatusError
        if isinstance(e, HTTPStatusError) and e.response.status_code == 404:
            print(f"  Enrich SKIPPED for {media.title}: not found on provider (id={media.tmdb_id})")
        else:
            import traceback
            print(f"  Enrich FAILED for {media.title}: {e}")
            traceback.print_exc()


async def _tvdb_series_meta(series_tvdb_id: int, tvdb_api_key: str, tvdb_lang: str = "eng") -> dict:
    """Resolve a series' TVDB name and artwork, falling back to Skyhook for the
    images. Returns {} when neither provider has anything to say.

    The name matters as much as the artwork: a card under a TVDB-primary
    account showing TMDB's title for the show ("Re:ZERO -Starting Life in
    Another World-" where TVDB says "Re: ZERO, Starting Life in Another
    World") reads as the wrong provider's data even when everything else on
    the card is right."""
    name = poster = backdrop = None
    try:
        # get_series takes no language; format_series names the parameter
        # "language". Both were called with lang=, inside a bare except, so
        # this fell through to (None, None) every time.
        raw = await tvdb_client.get_series(series_tvdb_id, tvdb_api_key)
        if raw:
            fmt = tvdb_client.format_series(raw, language=tvdb_lang)
            name = fmt.get("title") or fmt.get("name")
            poster = fmt.get("poster_path")
            backdrop = fmt.get("backdrop_path")
    except Exception:
        pass
    if not poster or not backdrop:
        try:
            from core import skyhook
            sky = await skyhook.get_show(series_tvdb_id)
            if sky:
                imgs = skyhook.extract_images(sky)
                poster = poster or imgs.get("poster")
                backdrop = backdrop or imgs.get("fanart")
        except Exception:
            pass
    if not (name or poster or backdrop):
        return {}
    return {"name": name, "poster_path": poster, "backdrop_path": backdrop}


async def _tvdb_series_artwork(series_tvdb_id: int, tvdb_api_key: str, tvdb_lang: str = "eng") -> tuple[str | None, str | None]:
    """Resolve series (poster, backdrop) from TVDB, then Skyhook. (None, None) on failure."""
    meta = await _tvdb_series_meta(series_tvdb_id, tvdb_api_key, tvdb_lang)
    return meta.get("poster_path"), meta.get("backdrop_path")


async def tvdb_series_artwork_cached(
    series_tvdb_id: int, tvdb_api_key: str | None, tvdb_lang: str = "eng"
) -> tuple[str | None, str | None]:
    """_tvdb_series_artwork behind the shared provider cache.

    Browse listings need artwork for series that have no local row to read it
    from — trending, search, recommendations — so the same handful of series
    would otherwise be refetched on every page view.
    """
    from core import provider_cache

    data = await tvdb_series_meta_cached(series_tvdb_id, tvdb_api_key, tvdb_lang)
    return data.get("poster_path"), data.get("backdrop_path")


async def tvdb_series_meta_cached(
    series_tvdb_id: int, tvdb_api_key: str | None, tvdb_lang: str = "eng"
) -> dict:
    """_tvdb_series_meta behind the shared provider cache.

    Browse listings need name and artwork for series that have no local row to
    read them from - trending, search, recommendations - so the same handful of
    series would otherwise be refetched on every page view.

    Cached under its own endpoint rather than the artwork one it replaced: an
    entry written before names were stored has no "name" key, and reading it
    back here looks exactly like "TVDB has no name for this series", so those
    cards would keep the TMDB title until the old entry expired.
    """
    from core import provider_cache

    async def _fetch() -> dict:
        return await _tvdb_series_meta(series_tvdb_id, tvdb_api_key, tvdb_lang)

    return await provider_cache.cached(
        "tvdb",
        "series_meta",
        {"id": int(series_tvdb_id), "lang": tvdb_lang},
        provider_cache.TTL_IMAGES,
        _fetch,
    ) or {}


async def tvdb_series_episode_meta_cached(
    series_tvdb_id: int, tvdb_api_key: str | None, tvdb_lang: str = "eng"
) -> dict[str, dict]:
    """Every TVDB episode of a series, keyed by TVDB episode id (as a string,
    since this round-trips through JSON in the provider cache).

    Listings hold rows enriched from TMDB, so a viewer whose primary source is
    TVDB still sees TMDB titles and stills on episode cards. The mapping table
    (episode_order_mappings) knows which TVDB episode each row is, but not what
    that episode is called or what it looks like - this fills that in with one
    cached round trip per series rather than one per episode.
    """
    from core import provider_cache

    async def _fetch() -> dict:
        try:
            raw_eps = await tvdb_client.get_series_episodes(
                series_tvdb_id, None, tvdb_api_key, language=tvdb_lang
            )
        except Exception:
            return {}
        out: dict[str, dict] = {}
        for raw in raw_eps:
            ep_id = raw.get("id")
            if not ep_id:
                continue
            fmt = tvdb_client.format_episode(raw, tvdb_lang)
            out[str(ep_id)] = {
                "title": fmt.get("title"),
                "overview": fmt.get("overview"),
                "still_path": fmt.get("still_path"),
                "season_number": fmt.get("season_number"),
                "episode_number": fmt.get("episode_number"),
            }
        return out

    return await provider_cache.cached(
        "tvdb",
        "series_episode_meta",
        {"id": int(series_tvdb_id), "lang": tvdb_lang},
        provider_cache.TTL_EPISODE,
        _fetch,
    ) or {}


async def enrich_for_user(
    db,
    user_id: int,
    media: Media,
    *,
    series_uri_id: str | None = None,
    series_tmdb_id: int | None = None,
    series_tvdb_id: int | None = None,
) -> None:
    """Resolve the user's metadata-source pref, keys, and TVDB series id, then
    call enrich_media. Single entry point so call sites don't re-derive routing.
    """
    from sqlalchemy import select
    from models.users import UserSettings

    settings = (await db.execute(select(UserSettings).where(UserSettings.user_id == user_id))).scalar_one_or_none()
    prefs = settings.preferences if settings and settings.preferences else {}
    is_tvdb = prefs.get("primary_metadata_source", "tmdb") == "tvdb"

    if media.media_type == MediaType.series and not series_tmdb_id:
        series_tmdb_id = media.tmdb_id

    from routers.media import get_user_tmdb_key
    api_key = await get_user_tmdb_key(db, user_id)

    tvdb_api_key = None
    tvdb_lang = "eng"
    if is_tvdb:
        from routers.shows import get_user_tvdb_key
        tvdb_api_key = await get_user_tvdb_key(db, user_id)
        from core.translations import get_user_metadata_language
        tvdb_lang = tvdb_client.tvdb_language(await get_user_metadata_language(db, user_id))

        if not series_tvdb_id:
            if series_uri_id:
                try:
                    from utils.alias_lookup import get_provider_id_for_uri
                    _tvdb = await get_provider_id_for_uri(db, series_uri_id, "tvdb")
                    if _tvdb:
                        series_tvdb_id = int(_tvdb)
                except Exception:
                    pass
            if not series_tvdb_id and series_tmdb_id:
                try:
                    res = await tmdb.get_external_ids(series_tmdb_id, "tv", api_key=api_key)
                    if res.get("tvdb_id"):
                        series_tvdb_id = int(res["tvdb_id"])
                except Exception:
                    pass

    use_tvdb = is_tvdb and bool(tvdb_api_key) and bool(series_tvdb_id)
    await enrich_media(
        media,
        api_key=api_key,
        series_tmdb_id=series_tmdb_id,
        is_tvdb=use_tvdb,
        tvdb_api_key=tvdb_api_key,
        tvdb_lang=tvdb_lang,
        series_tvdb_id=series_tvdb_id,
        series_uri_id=series_uri_id,
        db=db,
    )


async def create_media_safely(
    db: AsyncSession, tmdb_id: int | None, media_type: MediaType, **fields
) -> tuple[Media, bool]:
    """Add a new Media row, tolerating a concurrent insert of the same
    (tmdb_id, media_type) racing with this one. Returns (media, created) -
    created is False when the row returned is an existing one from a lost
    race, not the one just constructed from **fields.

    Dozens of call sites across the app each do their own "check for an
    existing row, then create if missing" - that check-then-create isn't
    atomic, so two concurrent calls for the same episode (e.g. a webhook
    delivered twice, or two connected media servers reporting the same play
    close together) could both decide it's missing and both insert, creating
    a silent duplicate. A unique index on (tmdb_id, media_type) now makes the
    loser of that race fail with IntegrityError instead - this catches that
    and returns whichever row actually won the race, so callers never see the
    difference between "created" and "lost the race, here's the real one"
    (see #157, whose crash was a downstream symptom of this exact problem).
    """
    media = Media(tmdb_id=tmdb_id, media_type=media_type, **fields)
    if tmdb_id is None:
        # No unique index applies to a null tmdb_id - nothing to race on.
        db.add(media)
        await db.flush()
        return media, True
    try:
        # add() happens *inside* the savepoint, not before it - add()-then-flush
        # must be a single unit the savepoint fully owns, or rolling back on
        # conflict leaves the session's flush-error state stuck even though the
        # SQL-level SAVEPOINT itself rolled back, poisoning every other pending
        # change in the session (verified against a real Postgres instance,
        # not just mocks - see this function's tests).
        async with db.begin_nested():
            db.add(media)
            await db.flush()
    except IntegrityError:
        result = await db.execute(
            select(Media)
            .where(Media.tmdb_id == tmdb_id, Media.media_type == media_type)
            .order_by(Media.id)
        )
        existing = result.scalars().first()
        if not existing:
            raise
        if media_type == MediaType.episode and (
            fields.get("season_number"), fields.get("episode_number")
        ) != (existing.season_number, existing.episode_number):
            # Same tmdb_id but disagreeing season/episode - almost certainly
            # TMDB having re-numbered this episode between two resolutions
            # (or, rarely, a genuine id collision between two unrelated
            # episodes) rather than a same-episode race. There's no safe way
            # to insert a second row for this tmdb_id, so the existing row is
            # still returned, but this is worth surfacing rather than masking.
            logger.warning(
                "Media tmdb_id=%s media_type=episode: existing row's season/episode "
                "(%s, %s) disagrees with the one just attempted (%s, %s) - reusing "
                "the existing row",
                tmdb_id,
                existing.season_number, existing.episode_number,
                fields.get("season_number"), fields.get("episode_number"),
            )
        return existing, False
    return media, True


async def apply_media_change_safely(db: AsyncSession, media: Media, mutate) -> Media:
    """Run `mutate()` against an *already-persisted* Media row and flush the
    result, tolerating a conflict if it just reassigned media.tmdb_id to a
    value some other row (of the same media_type) already claims.

    Several flows mutate an existing row's tmdb_id after the fact - most
    commonly a stub row (created with tmdb_id=None because it couldn't be
    matched yet) finally getting resolved by a later sync, a manual "match
    unmatched movie/episode" action, or an orphan-healing pass. Sharing a
    tmdb_id is possible (TMDB re-numbering, or a genuine coincidence) even
    though it's rare, and it must not crash the whole request/batch when it
    happens - this returns the pre-existing row instead.

    `mutate` may be sync or async, and MUST be the thing that actually changes
    media.tmdb_id - it needs to run *inside* this function's savepoint, not
    before calling this function, or the recovery below can't work (same
    lesson as create_media_safely's add()-inside-savepoint requirement: a
    state change made outside the savepoint's scope leaves the session's
    flush-error state stuck even after the SQL-level SAVEPOINT rolls back,
    poisoning every other pending change in the session - verified against a
    real Postgres instance, not just mocks).

    A media row still being created in the *same* flush cycle (media.id is
    None) doesn't need any of this - that creation's own savepoint already
    covers it, so `mutate` just runs directly.
    """
    if media.id is None:
        result = mutate()
        if inspect.isawaitable(result):
            await result
        return media

    # Captured up front: after a savepoint rolls back, SQLAlchemy expires the
    # touched object's attributes, and reading one back triggers an implicit
    # (sync) reload that isn't safe under AsyncSession outside an awaited
    # context (raises MissingGreenlet) - so nothing on `media` gets read again
    # once the except block below is reached. Everything needed there is
    # captured into plain local variables first instead.
    original_tmdb_id = media.tmdb_id
    original_media_id = media.id
    original_media_type = media.media_type
    resolved_tmdb_id: int | None = None
    try:
        async with db.begin_nested():
            result = mutate()
            if inspect.isawaitable(result):
                await result
            resolved_tmdb_id = media.tmdb_id  # read before the flush, never after
            await db.flush()
    except IntegrityError:
        if resolved_tmdb_id is None or resolved_tmdb_id == original_tmdb_id:
            # Not actually about the id we changed - re-raise rather than mask
            # an unrelated conflict (e.g. on some other dirty row in this flush).
            raise
        result = await db.execute(
            select(Media)
            .where(Media.tmdb_id == resolved_tmdb_id, Media.media_type == original_media_type)
            .order_by(Media.id)
        )
        existing = result.scalars().first()
        if not existing or existing.id == original_media_id:
            raise
        logger.warning(
            "Media.tmdb_id=%s just resolved for media.id=%s but media.id=%s already "
            "has it - discarding the change and using the existing row instead",
            resolved_tmdb_id, original_media_id, existing.id,
        )
        return existing
    return media


async def enrich_media_safely(
    db: AsyncSession,
    media: Media,
    api_key: str = None,
    series_tmdb_id: int = None,
    tvdb_id: int | None = None,
    tvdb_api_key: str | None = None,
    tvdb_lang: str | None = None,
) -> Media:
    """enrich_media(), wrapped so a newly-resolved episode tmdb_id that
    collides with another row returns the pre-existing row instead of
    crashing. Use this instead of a plain enrich_media() call anywhere the
    media passed in already has an id from an earlier create_media_safely()
    call - see apply_media_change_safely for the full explanation.

    tvdb_id here is the *series* TVDB id, which enrich_media now takes as
    series_tvdb_id; the parameter name is kept so existing callers are
    unchanged.
    """
    return await apply_media_change_safely(
        db,
        media,
        lambda: enrich_media(
            media,
            api_key=api_key,
            series_tmdb_id=series_tmdb_id,
            series_tvdb_id=tvdb_id,
            tvdb_api_key=tvdb_api_key,
            tvdb_lang=tvdb_lang if tvdb_lang is not None else "eng",
        ),
    )
