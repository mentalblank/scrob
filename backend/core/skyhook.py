"""Skyhook client — Sonarr's public TVDB metadata proxy."""
import httpx

SKYHOOK_BASE = "https://skyhook.sonarr.tv/v1/tvdb/shows"


async def get_show(tvdb_id: int, lang: str = "en") -> dict | None:
    """Fetch a show from Skyhook. Returns the raw dict, or None on 404/error.

    Never raises — Skyhook is a best-effort supplement; callers degrade gracefully.
    """
    from core import provider_cache

    async def _fetch() -> dict | None:
        try:
            async with httpx.AsyncClient(timeout=httpx.Timeout(15.0)) as client:
                r = await client.get(
                    f"{SKYHOOK_BASE}/{lang}/{int(tvdb_id)}",
                    headers={"Accept": "application/json"},
                )
                if r.status_code == 404:
                    return None
                r.raise_for_status()
                return r.json()
        except Exception:
            return None

    return await provider_cache.cached(
        "skyhook", "show", {"id": int(tvdb_id), "lang": lang}, provider_cache.TTL_SKYHOOK, _fetch
    )


def extract_cross_ids(raw: dict) -> dict:
    """Return {"tmdb": int|None, "imdb": str|None} from a Skyhook response."""
    tmdb_id = None
    try:
        if raw.get("tmdbId"):
            tmdb_id = int(raw["tmdbId"])
    except (TypeError, ValueError):
        tmdb_id = None
    imdb_id = raw.get("imdbId") or None
    return {"tmdb": tmdb_id, "imdb": imdb_id}


def _first_image(images: list, cover_type: str) -> str | None:
    """First URL whose coverType matches (case-insensitive)."""
    for img in images or []:
        if (img.get("coverType") or "").lower() == cover_type.lower():
            return img.get("url")
    return None


def extract_images(raw: dict) -> dict:
    """Return show-level artwork keyed by type. Missing types are omitted."""
    images = raw.get("images") or []
    out = {
        "clearlogo": _first_image(images, "clearlogo"),
        "fanart": _first_image(images, "fanart"),
        "poster": _first_image(images, "poster"),
        "banner": _first_image(images, "banner"),
    }
    return {k: v for k, v in out.items() if v}


def extract_season_images(raw: dict) -> dict:
    """Return {season_number: {"poster": url, "name": name|None}} for seasons
    that carry artwork or a name."""
    out: dict = {}
    for s in raw.get("seasons") or []:
        sn = s.get("seasonNumber")
        if sn is None:
            continue
        poster = _first_image(s.get("images"), "poster")
        name = s.get("name")
        if poster or name:
            out[int(sn)] = {"poster": poster, "name": name}
    return out


def format_episodes(raw: dict) -> list[dict]:
    """Normalise Skyhook episodes[] to scrob's episode shape."""
    out: list[dict] = []
    for e in raw.get("episodes") or []:
        sn = e.get("seasonNumber")
        en = e.get("episodeNumber")
        if sn is None or en is None:
            continue
        out.append({
            "tvdb_id": e.get("tvdbId"),
            "season_number": sn,
            "episode_number": en,
            "absolute_episode_number": e.get("absoluteEpisodeNumber"),
            "title": e.get("title"),
            "overview": e.get("overview"),
            "image_url": e.get("image"),
            "air_date": e.get("airDate"),
            "runtime": e.get("runtime"),
        })
    return out
