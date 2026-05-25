"""TVDB v4 API client.

Token-based auth: POST /login returns a 30-day Bearer token.
We cache the token in memory (module-level) and refresh it when it expires.
"""
import asyncio
import time
import httpx

TVDB_BASE = "https://api4.thetvdb.com/v4"

# In-memory token cache keyed by api_key
_token_cache: dict[str, tuple[str, float]] = {}  # api_key -> (token, expires_at)
_token_lock = asyncio.Lock()

TVDB_IMAGE_BASE = "https://artworks.thetvdb.com"


def _image_url(path: str | None) -> str | None:
    if not path:
        return None
    if path.startswith("http"):
        return path
    return f"{TVDB_IMAGE_BASE}{path}"


async def _get_token(api_key: str) -> str:
    """Return a valid TVDB Bearer token, refreshing if necessary."""
    async with _token_lock:
        cached = _token_cache.get(api_key)
        if cached:
            token, expires_at = cached
            # Refresh 1 hour before expiry
            if time.time() < expires_at - 3600:
                return token

        async with httpx.AsyncClient(timeout=httpx.Timeout(30.0)) as client:
            r = await client.post(
                f"{TVDB_BASE}/login",
                json={"apikey": api_key},
                headers={"Content-Type": "application/json", "Accept": "application/json"},
            )
            r.raise_for_status()
            data = r.json()

        token = data["data"]["token"]
        # TVDB tokens last 30 days; cache for 29 days
        expires_at = time.time() + 29 * 86400
        _token_cache[api_key] = (token, expires_at)
        return token


async def _get(path: str, api_key: str, params: dict | None = None, lang: str | None = None) -> dict:
    token = await _get_token(api_key)
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
    }
    if lang:
        headers["Accept-Language"] = lang

    async with httpx.AsyncClient(timeout=httpx.Timeout(30.0)) as client:
        r = await client.get(
            f"{TVDB_BASE}{path}",
            headers=headers,
            params=params or {},
        )
        r.raise_for_status()
        return r.json()


def to_three_letter_lang(lang_code: str | None) -> str:
    """Map a 2-letter language code (ISO 639-1) to 3-letter code (ISO 639-2/T).
    Defaults to 'eng' if not found or empty."""
    if not lang_code:
        return "eng"
    lang_code = lang_code.lower().strip()
    if len(lang_code) == 3:
        return lang_code
    mapping = {
        "aa": "aar", "ab": "abk", "ae": "ave", "af": "afr", "ak": "aka",
        "am": "amh", "an": "arg", "ar": "ara", "as": "asm", "av": "ava",
        "ay": "aym", "az": "aze", "ba": "bak", "be": "bel", "bg": "bul",
        "bh": "bih", "bi": "bis", "bm": "bam", "bn": "ben", "bo": "bod",
        "br": "bre", "bs": "bos", "ca": "cat", "ce": "che", "ch": "cha",
        "co": "cos", "cr": "cre", "cs": "ces", "cu": "chu", "cv": "chv",
        "cy": "cym", "da": "dan", "de": "deu", "dv": "div", "dz": "dzo",
        "ee": "ewe", "el": "ell", "en": "eng", "eo": "epo", "es": "spa",
        "et": "est", "eu": "eus", "fa": "fas", "ff": "ful", "fi": "fin",
        "fj": "fij", "fo": "fao", "fr": "fra", "fy": "fry", "ga": "gle",
        "gd": "gla", "gl": "glg", "gn": "grn", "gu": "guj", "gv": "glv",
        "ha": "hau", "he": "heb", "hi": "hin", "ho": "hmo", "hr": "hrv",
        "ht": "hat", "hu": "hun", "hy": "hye", "hz": "her", "ia": "ina",
        "id": "ind", "ie": "ile", "ig": "ibo", "ii": "iii", "ik": "ipk",
        "io": "ido", "is": "isl", "it": "ita", "iu": "iku", "ja": "jpn",
        "jv": "jav", "ka": "kat", "kg": "kon", "ki": "kik", "kj": "kua",
        "kk": "kaz", "kl": "kal", "km": "khm", "kn": "kan", "ko": "kor",
        "kr": "kau", "ks": "kas", "kv": "kom", "kw": "cor", "ky": "kir",
        "la": "lat", "lb": "ltz", "lg": "lug", "li": "lim", "ln": "lin",
        "lo": "lao", "lt": "lit", "lu": "lub", "lv": "lav", "mg": "mlg",
        "mh": "mah", "mi": "mri", "mk": "mkd", "ml": "mal", "mn": "mon",
        "mr": "mar", "ms": "msa", "mt": "mlt", "my": "mya", "na": "nau",
        "nb": "nob", "nd": "nde", "ne": "nep", "ng": "ndo", "nl": "nld",
        "nn": "nno", "no": "nor", "nr": "nbl", "nv": "nav", "ny": "nya",
        "oc": "oci", "oj": "oji", "om": "orm", "or": "ori", "os": "oss",
        "pa": "pan", "pi": "pli", "pl": "pol", "ps": "pus", "pt": "por",
        "qu": "que", "rm": "roh", "rn": "run", "ro": "ron", "ru": "rus",
        "rw": "kin", "sa": "san", "sc": "srd", "sd": "snd", "se": "sme",
        "sg": "sag", "sh": "hbs", "si": "sin", "sk": "slk", "sl": "slv",
        "sm": "smo", "sn": "sna", "so": "som", "sq": "sqi", "sr": "srp",
        "ss": "ssw", "st": "sot", "su": "sun", "sv": "swe", "sw": "swa",
        "ta": "tam", "te": "tel", "tg": "tgk", "th": "tha", "ti": "tir",
        "tk": "tuk", "tl": "tgl", "tn": "tsn", "to": "ton", "tr": "tur",
        "ts": "tso", "tt": "tat", "tw": "twi", "ty": "tah", "ug": "uig",
        "uk": "ukr", "ur": "urd", "uz": "uzb", "ve": "ven", "vi": "vie",
        "vo": "vol", "wa": "wln", "wo": "wol", "xh": "xho", "yi": "yid",
        "yo": "yor", "za": "zha", "zh": "zho", "zu": "zul"
    }
    return mapping.get(lang_code, "eng")


async def validate_api_key(api_key: str) -> bool:
    if not api_key:
        return False
    try:
        await _get_token(api_key)
        return True
    except Exception:
        return False


async def search_series(query: str, api_key: str, lang: str | None = None) -> list[dict]:
    """Search for TV series by title. Returns list of simplified series dicts."""
    data = await _get("/search", api_key, params={"query": query, "type": "series"}, lang=lang)
    results = []
    for item in data.get("data") or []:
        tvdb_id_str = item.get("tvdb_id") or item.get("id") or ""
        try:
            tvdb_id = int(str(tvdb_id_str).lstrip("series-"))
        except (ValueError, TypeError):
            continue
        
        # Try to find a translated title/overview in search results
        title = item.get("name")
        overview = item.get("overview")
        
        # TVDB search results sometimes have a 'translations' dict for the title
        if not title and "translations" in item:
            title = item["translations"].get(lang or "eng") or item["translations"].get("eng")
        
        results.append({
            "tvdb_id": tvdb_id,
            "title": title or "",
            "overview": overview,
            "year": item.get("year"),
            "image_url": _image_url(item.get("image_url") or item.get("thumbnail")),
            "status": item.get("status"),
            "network": item.get("network"),
        })
    return results


async def get_series(tvdb_id: int, api_key: str, lang: str | None = None) -> dict:
    """Fetch series extended info including episodes for accurate per-season counts."""
    data = await _get(f"/series/{tvdb_id}/extended", api_key, params={"meta": "translations,episodes"}, lang=lang)
    return data.get("data") or {}


async def get_series_episodes(tvdb_id: int, season_number: int, api_key: str, lang: str = "eng") -> list[dict]:
    """Fetch episodes for a specific season (season_type=official) in the specified language."""
    episodes = []
    page = 0
    try:
        while True:
            data = await _get(
                f"/series/{tvdb_id}/episodes/official/{lang}",
                api_key,
                params={"page": page, "season": season_number},
                lang=lang
            )
            batch = (data.get("data") or {}).get("episodes") or []
            if not batch:
                break
            
            # Client-side filter to ensure we only get the requested season, 
            # in case the API ignores the season param when lang is provided
            batch = [e for e in batch if e.get("seasonNumber") == season_number]
            
            episodes.extend(batch)
            # TVDB paginates at 500; if we got fewer, we're done
            if len(batch) < 500:
                break
            page += 1
    except Exception:
        # Fall back to english if custom language lookup fails
        if lang != "eng":
            return await get_series_episodes(tvdb_id, season_number, api_key, lang="eng")
        else:
            raise
    return episodes


def get_season_label(lang: str) -> str:
    """Return a localized 'Season' label for common languages."""
    labels = {
        "eng": "Season",
        "spa": "Temporada",
        "fra": "Saison",
        "deu": "Staffel",
        "ita": "Stagione",
        "por": "Temporada",
        "nld": "Seizoen",
        "rus": "Сезон",
        "zho": "季",
        "jpn": "シーズン",
        "kor": "시즌",
        "ara": "الموسم",
        "pol": "Sezon",
        "tur": "Sezon",
        "dan": "Sæson",
        "fin": "Kausi",
        "nor": "Sesong",
        "swe": "Säsong",
    }
    return labels.get(lang, "Season")


def get_specials_label(lang: str) -> str:
    """Return a localized 'Specials' label for common languages."""
    labels = {
        "eng": "Specials",
        "spa": "Especiales",
        "fra": "Hors-série",
        "deu": "Specials",
        "ita": "Speciali",
        "por": "Especiais",
        "nld": "Specials",
        "rus": "Спецвыпуски",
        "zho": "特别篇",
        "jpn": "スペシャル",
        "kor": "스페셜",
    }
    return labels.get(lang, "Specials")


def format_series(raw: dict, lang: str = "eng") -> dict:
    """Normalise TVDB extended series data into a frontend-friendly dict, translating name and overview if possible."""
    image = raw.get("image") or ""
    poster = _image_url(image) if image else None

    translations = raw.get("translations") or {}
    
    # Try preferred overview translation
    overview = None
    for t in translations.get("overviewTranslations") or []:
        if isinstance(t, dict) and t.get("language") == lang:
            overview = t.get("overview")
            break
            
    # Fallback to English overview if preferred translation not found
    if not overview and lang != "eng":
        for t in translations.get("overviewTranslations") or []:
            if isinstance(t, dict) and t.get("language") == "eng":
                overview = t.get("overview")
                break
                
    if not overview:
        overview = raw.get("overview")

    # Try preferred title translation
    title = None
    for t in translations.get("nameTranslations") or []:
        if isinstance(t, dict) and t.get("language") == lang:
            title = t.get("name")
            break
            
    # Fallback to English title if preferred translation not found
    if not title and lang != "eng":
        for t in translations.get("nameTranslations") or []:
            if isinstance(t, dict) and t.get("language") == "eng":
                title = t.get("name")
                break
                
    if not title:
        title = raw.get("name")

    genres = [g.get("name") for g in (raw.get("genres") or []) if g.get("name")]

    # Count episodes per season and derive premiere dates from embedded episodes
    episode_counts: dict[int, int] = {}
    season_premiere_dates: dict[int, str] = {}
    seen_episodes = set()
    for ep in raw.get("episodes") or []:
        ep_id = ep.get("id")
        if ep_id in seen_episodes:
            continue
        seen_episodes.add(ep_id)
        
        sn = ep.get("seasonNumber")
        if sn is None:
            continue
        episode_counts[sn] = episode_counts.get(sn, 0) + 1
        if ep.get("number") == 1 and ep.get("aired") and sn not in season_premiere_dates:
            season_premiere_dates[sn] = ep["aired"]

    seasons = []
    season_label = get_season_label(lang)
    specials_label = get_specials_label(lang)
    for s in raw.get("seasons") or []:
        if s.get("type", {}).get("type") == "official":
            sn = s.get("number")
            count = episode_counts.get(sn) if sn in episode_counts else (s.get("episodeCount") or 0)
            
            # Use season name if provided (should be localized now via Accept-Language)
            # otherwise fallback to localized "Season X"
            name = s.get("name")
            if not name:
                name = f"{season_label} {sn}" if sn != 0 else specials_label

            seasons.append({
                "season_number": sn,
                "name": name,
                "overview": s.get("overview"),
                "poster_path": _image_url(s.get("image")),
                "episode_count": count,
                "air_date": s.get("premiereDate") or season_premiere_dates.get(sn),
                "id": s.get("id"),
            })
    seasons.sort(key=lambda x: x["season_number"] or 0)

    network = None
    for n in raw.get("networks") or []:
        if n.get("primaryLanguage") == "eng" or not network:
            network = n.get("name")

    age_rating = None
    for cr in raw.get("contentRatings") or []:
        if cr.get("country") == "usa" and cr.get("contentType") == "TV":
            age_rating = cr.get("name")
            break
    if not age_rating:
        for cr in raw.get("contentRatings") or []:
            age_rating = cr.get("name")
            break

    imdb_id = None
    tmdb_id_cross = None
    for rid in raw.get("remoteIds") or []:
        source = (rid.get("sourceName") or "").upper()
        if source == "IMDB" and not imdb_id:
            imdb_id = rid.get("id")
        elif "MOVIEDB" in source and not tmdb_id_cross:
            try:
                tmdb_id_cross = int(rid.get("id"))
            except (TypeError, ValueError):
                pass

    return {
        "tvdb_id": raw.get("id"),
        "title": title,
        "original_title": raw.get("originalName"),
        "overview": overview,
        "poster_path": poster,
        "backdrop_path": _image_url(raw.get("artworks", [{}])[0].get("image") if raw.get("artworks") else None),
        "first_air_date": raw.get("firstAired"),
        "last_air_date": raw.get("lastAired"),
        "status": (raw.get("status") or {}).get("name"),
        "genres": genres,
        "network": network,
        "seasons": seasons,
        "original_language": raw.get("originalLanguage"),
        "age_rating": age_rating,
        "imdb_id": imdb_id,
        "tmdb_id_cross": tmdb_id_cross,
    }


def format_cast(raw: dict) -> list[dict]:
    """Extract actor list from TVDB extended series data."""
    characters = [c for c in (raw.get("characters") or []) if c.get("type") == 3]
    characters.sort(key=lambda x: x.get("sort") or 999)
    return [
        {
            "tmdb_id": None,
            "person_id": c.get("personId"),
            "name": c.get("personName") or "",
            "character": c.get("name") or "",
            "profile_path": _image_url(c.get("image")),
        }
        for c in characters[:12]
        if c.get("personName")
    ]


def format_episode(raw: dict) -> dict:
    return {
        "tvdb_id": raw.get("id"),
        "season_number": raw.get("seasonNumber"),
        "episode_number": raw.get("number"),
        "name": raw.get("name"),
        "overview": raw.get("overview"),
        "air_date": raw.get("aired"),
        "runtime": raw.get("runtime"),
        "image_url": _image_url(raw.get("image")),
    }


async def get_series_episodes_by_type(tvdb_id: int, api_key: str, season_type: str = "official", lang: str = "eng") -> list[dict]:
    """Fetch all episodes for a series, paginated, using specific season_type and lang."""
    episodes = []
    page = 0
    while True:
        data = await _get(
            f"/series/{tvdb_id}/episodes/{season_type}/{lang}",
            api_key,
            params={"page": page},
            lang=lang
        )
        batch = (data.get("data") or {}).get("episodes") or []
        if not batch:
            break
        episodes.extend(batch)
        if len(batch) < 100:
            break
        page += 1
    return episodes

