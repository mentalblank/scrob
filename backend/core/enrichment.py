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
) -> None:
    """Fetch TMDB/TVDB metadata and update the media record in place."""
    if media.media_type == MediaType.movie and not media.tmdb_id:
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
                            media.tmdb_id = tvdb_ep_id
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
                media.tmdb_id = data.get("id") or media.tmdb_id
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