from core import tmdb
from models.media import Media, MediaType


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
) -> None:
    """Fetch TMDB/TVDB metadata and update the media record in place.

    series_uri_id (e.g. 'tvdb:s:81189', 'tmdb:s:1396') resolves series context without
    requiring the caller to extract integer IDs first. Provide db for cross-provider alias lookup.
    """
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
            data = await tmdb.get_movie(media.tmdb_id, api_key=api_key)
            media.title = data.get("title") or media.title
            media.original_title = data.get("original_title")
            media.overview = data.get("overview")
            media.poster_path = tmdb.poster_url(data.get("poster_path"))
            media.backdrop_path = tmdb.poster_url(data.get("backdrop_path"), size="w1280")
            media.release_date = data.get("release_date")
            media.tmdb_rating = data.get("vote_average")
            media.tmdb_data = {
                "runtime": data.get("runtime"),
                "genres": [g["name"] for g in data.get("genres", [])],
                "cast": [
                    {"name": c["name"], "character": c["character"], "profile_path": tmdb.poster_url(c.get("profile_path"), size="w185")}
                    for c in data.get("credits", {}).get("cast", [])[:10]
                ],
                "tagline": data.get("tagline"),
                "status": data.get("status"),
                "adult": data.get("adult", False),
                "release_dates": _extract_release_dates(data.get("release_dates", {}).get("results", [])),
            }
            media.adult = data.get("adult", False)

        elif media.media_type == MediaType.series:
            if not media.tmdb_id:
                return
            data = await tmdb.get_show(media.tmdb_id, api_key=api_key)
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
            }
            media.adult = data.get("adult", False)

            # Pref=TVDB: override artwork only, keep TMDB metadata
            if is_tvdb and series_tvdb_id and tvdb_api_key:
                tvdb_poster, tvdb_backdrop = await _tvdb_series_artwork(series_tvdb_id, tvdb_api_key, tvdb_lang)
                if tvdb_poster:
                    media.poster_path = tvdb_poster
                if tvdb_backdrop:
                    media.backdrop_path = tvdb_backdrop

        elif media.media_type == MediaType.episode:
            if media.season_number is None or media.episode_number is None:
                return

            enriched = False
            if is_tvdb and tvdb_api_key and series_tvdb_id:
                try:
                    from core import tvdb as tvdb_client
                    raw_eps = await tvdb_client.get_series_episodes(series_tvdb_id, media.season_number, tvdb_api_key, lang=tvdb_lang)
                    ep = next((e for e in raw_eps if e.get("number") == media.episode_number), None)
                    if ep:
                        tvdb_ep_id = ep.get("id")
                        if tvdb_ep_id:
                            if not media.uri_id:
                                media.uri_id = f"tvdb:e:{tvdb_ep_id}"
                            # Do NOT set media.tmdb_id to a TVDB ID — different namespace
                        media.title = ep.get("name") or media.title
                        media.overview = ep.get("overview")
                        if ep.get("image"):
                            media.poster_path = tvdb_client._image_url(ep["image"])
                        media.release_date = ep.get("aired")
                        media.tmdb_data = {
                            "runtime": ep.get("runtime"),
                            "tvdb_episode_id": tvdb_ep_id,
                            "source": "tvdb",
                        }
                        enriched = True
                except Exception as e:
                    print(f"  TVDB enrich failed for episode: {e}, falling back to TMDB")

            if not enriched and series_tmdb_id:
                data = await tmdb.get_episode(series_tmdb_id, media.season_number, media.episode_number, api_key=api_key)
                ep_id = data.get("id") or media.tmdb_id
                media.tmdb_id = ep_id
                if ep_id and not media.uri_id:
                    media.uri_id = f"tmdb:e:{ep_id}"
                media.title = data.get("name") or media.title
                media.overview = data.get("overview")
                media.poster_path = tmdb.poster_url(data.get("still_path"), size="w500")
                media.release_date = data.get("air_date")
                media.tmdb_rating = data.get("vote_average")
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


async def _tvdb_series_artwork(series_tvdb_id: int, tvdb_api_key: str, tvdb_lang: str = "eng") -> tuple[str | None, str | None]:
    """Resolve series (poster, backdrop) from TVDB, then Skyhook. (None, None) on failure."""
    poster = backdrop = None
    try:
        from core import tvdb as tvdb_client
        raw = await tvdb_client.get_series(series_tvdb_id, tvdb_api_key, lang=tvdb_lang)
        if raw:
            fmt = tvdb_client.format_series(raw, lang=tvdb_lang)
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
    return poster, backdrop


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

    from routers.media import get_user_tmdb_key, get_user_content_language
    api_key = await get_user_tmdb_key(db, user_id)

    tvdb_api_key = None
    tvdb_lang = "eng"
    if is_tvdb:
        from routers.shows import get_user_tvdb_key
        from core import tvdb as tvdb_client
        tvdb_api_key = await get_user_tvdb_key(db, user_id)
        tvdb_lang = tvdb_client.to_three_letter_lang(await get_user_content_language(db, user_id))

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