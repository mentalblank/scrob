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

# BCP 47 (metadata_language) → ISO 639-3 used by TVDB
_TVDB_LANG: dict[str, str] = {
    "en":    "eng",
    "fr":    "fra",
    "de":    "deu",
    "es":    "spa",
    "es-MX": "spa",
    "it":    "ita",
    "pt-BR": "por",
    "pt-PT": "por",
    "ja":    "jpn",
    "ko":    "kor",
    "zh-CN": "zho",
    "zh-TW": "zho",
    "hi":    "hin",
    "ar":    "ara",
    "ru":    "rus",
    "nl":    "nld",
    "pl":    "pol",
    "tr":    "tur",
    "sv":    "swe",
    "cs":    "ces",
    "hu":    "hun",
    "hr":    "hrv",
    "sr":    "srp",
}


def tvdb_language(metadata_language: str | None) -> str | None:
    """Convert a BCP 47 metadata_language code to the ISO 639-3 code TVDB expects."""
    if not metadata_language:
        return "eng"
    if metadata_language in _TVDB_LANG:
        return _TVDB_LANG[metadata_language]
    short_lang = metadata_language.split("-")[0]
    return _TVDB_LANG.get(short_lang, "eng")


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


async def _get(path: str, api_key: str, params: dict | None = None) -> dict:
    token = await _get_token(api_key)
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    if params and params.get("language"):
        headers["Accept-Language"] = str(params["language"])
    async with httpx.AsyncClient(timeout=httpx.Timeout(30.0)) as client:
        r = await client.get(
            f"{TVDB_BASE}{path}",
            headers=headers,
            params=params or {},
        )
        r.raise_for_status()
        return r.json()


async def validate_api_key(api_key: str) -> bool:
    if not api_key:
        return False
    try:
        await _get_token(api_key)
        return True
    except Exception:
        return False


async def search_series(query: str, api_key: str) -> list[dict]:
    """Search for TV series by title. Returns list of simplified series dicts."""
    data = await _get("/search", api_key, params={"query": query, "type": "series"})
    results = []
    for item in data.get("data") or []:
        tvdb_id_str = item.get("tvdb_id") or item.get("id") or ""
        try:
            tvdb_id = int(str(tvdb_id_str).lstrip("series-"))
        except (ValueError, TypeError):
            continue
        results.append({
            "tvdb_id": tvdb_id,
            "title": item.get("name") or item.get("translations", {}).get("eng", ""),
            "overview": item.get("overview") or item.get("overviews", {}).get("eng"),
            "year": item.get("year"),
            "image_url": _image_url(item.get("image_url") or item.get("thumbnail")),
            "status": item.get("status"),
            "network": item.get("network"),
        })
    return results


async def get_series(tvdb_id: int, api_key: str) -> dict:
    """Fetch series extended info including episodes for accurate per-season counts."""
    data = await _get(f"/series/{tvdb_id}/extended", api_key, params={"meta": "translations,episodes"})
    return data.get("data") or {}


async def get_season(season_id: int, api_key: str) -> dict:
    """Fetch extended season metadata, including translated names and overviews."""
    data = await _get(
        f"/seasons/{season_id}/extended",
        api_key,
        params={"meta": "translations"},
    )
    return data.get("data") or {}


def format_season(raw: dict, language: str | None = None) -> dict:
    """Normalise extended TVDB season metadata."""
    translations = raw.get("translations") or {}

    def _pick(key: str, field: str) -> str | None:
        entries = translations.get(key) or []
        fallback = None
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            if language and entry.get("language") == language:
                return entry.get(field) or None
            if entry.get("language") == "eng":
                fallback = entry.get(field) or None
        return fallback

    return {
        "season_number": raw.get("number"),
        "name": _pick("nameTranslations", "name") or raw.get("name"),
        "overview": _pick("overviewTranslations", "overview") or raw.get("overview"),
        "poster_path": _image_url(raw.get("image")),
        "air_date": raw.get("premiereDate"),
        "id": raw.get("id"),
    }


async def get_series_episodes(tvdb_id: int, season_number: int | None, api_key: str, language: str | None = None) -> list[dict]:
    """Fetch episodes for a series (season_type=official), optionally filtered to one season.

    TVDB v4 ignores the ``season`` query parameter unless ``episodeNumber`` is also
    supplied, so it always returns every episode of the series. Filter client-side
    instead, otherwise callers see specials and other seasons mixed in.
    """
    episodes = []
    page = 0
    lang = language or "eng"
    while True:
        data = await _get(
            f"/series/{tvdb_id}/episodes/official/{lang}",
            api_key,
            params={"page": page},
        )
        batch = (data.get("data") or {}).get("episodes") or []
        if not batch:
            break
        episodes.extend(batch)
        if not (data.get("links") or {}).get("next"):
            break
        page += 1
    if season_number is not None:
        episodes = [e for e in episodes if e.get("seasonNumber") == season_number]
    return episodes


_COUNTRY_TO_TVDB = {
    "US": "usa", "AU": "aus", "GB": "gbr", "CA": "can", "NZ": "nzl",
    "DE": "deu", "FR": "fra", "ES": "esp", "IT": "ita", "NL": "nld",
    "SE": "swe", "NO": "nor", "DK": "dnk", "FI": "fin", "IE": "irl",
    "BR": "bra", "MX": "mex", "JP": "jpn", "KR": "kor",
}


def format_series(raw: dict, language: str | None = None, country: str | None = None) -> dict:
    """Normalise TVDB extended series data into a frontend-friendly dict."""
    image = raw.get("image") or ""
    poster = _image_url(image) if image else None

    translations = raw.get("translations") or {}

    def _pick(key: str, field: str) -> str | None:
        entries = translations.get(key) or []
        result = None
        for t in entries:
            if not isinstance(t, dict):
                continue
            if language and t.get("language") == language:
                return t.get(field) or None  # preferred language found
            if t.get("language") == "eng":
                result = t.get(field) or None  # English fallback
        return result

    translated_title = _pick("nameTranslations", "name")
    eng_overview = _pick("overviewTranslations", "overview")

    genres = [g.get("name") for g in (raw.get("genres") or []) if g.get("name")]

    # Count episodes per season and derive premiere dates from embedded episodes
    episode_counts: dict[int, int] = {}
    season_premiere_dates: dict[int, str] = {}
    for ep in raw.get("episodes") or []:
        sn = ep.get("seasonNumber")
        if sn is None:
            continue
        episode_counts[sn] = episode_counts.get(sn, 0) + 1
        if ep.get("number") == 1 and ep.get("aired") and sn not in season_premiere_dates:
            season_premiere_dates[sn] = ep["aired"]

    seasons = []
    for s in raw.get("seasons") or []:
        if s.get("type", {}).get("type") == "official":
            sn = s.get("number")
            count = episode_counts.get(sn) if sn in episode_counts else (s.get("episodeCount") or 0)
            seasons.append({
                "season_number": sn,
                "name": s.get("name") or f"Season {sn}",
                "overview": None,
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

    # Prefer the viewer's own region, then US, then whatever exists — ratings are
    # region-specific, so an AU user should get MA15+ where a US user gets TV-MA.
    age_rating = None
    ratings = raw.get("contentRatings") or []
    preferred_codes = [c for c in (_COUNTRY_TO_TVDB.get((country or "").upper()), "usa") if c]
    for code in preferred_codes:
        for cr in ratings:
            if cr.get("country") == code and cr.get("name"):
                age_rating = cr["name"]
                break
        if age_rating:
            break
    if not age_rating:
        for cr in ratings:
            if cr.get("name"):
                age_rating = cr["name"]
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
        "title": translated_title or raw.get("name"),
        "original_title": raw.get("originalName") or raw.get("name"),
        "overview": eng_overview or raw.get("overview"),
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
            "tvdb_id": c.get("peopleId") or c.get("personId"),
            "person_id": c.get("peopleId") or c.get("personId"),
            "id": c.get("peopleId") or c.get("personId"),
            "name": c.get("personName") or "",
            "character": c.get("name") or "",
            "profile_path": _image_url(c.get("image")),
        }
        for c in characters[:12]
        if c.get("personName")
    ]


def format_episode(raw: dict, language: str | None = None) -> dict:
    translations = raw.get("translations") or {}
    name_trans = translations.get("nameTranslations") or []
    overview_trans = translations.get("overviewTranslations") or []

    translated_name = None
    translated_overview = None

    if language:
        for t in name_trans:
            if isinstance(t, dict) and t.get("language") == language:
                translated_name = t.get("name")
                break
        for t in overview_trans:
            if isinstance(t, dict) and t.get("language") == language:
                translated_overview = t.get("overview")
                break

    if not translated_name:
        for t in name_trans:
            if isinstance(t, dict) and t.get("language") == "eng":
                translated_name = t.get("name")
                break

    if not translated_overview:
        for t in overview_trans:
            if isinstance(t, dict) and t.get("language") == "eng":
                translated_overview = t.get("overview")
                break

    ep_name = translated_name or raw.get("name")
    ep_overview = translated_overview or raw.get("overview")

    img_url = _image_url(raw.get("image"))
    return {
        "tvdb_id": raw.get("id"),
        "season_number": raw.get("seasonNumber"),
        "episode_number": raw.get("number"),
        "name": ep_name,
        "title": ep_name,
        "overview": ep_overview,
        "air_date": raw.get("aired"),
        "runtime": raw.get("runtime"),
        "image_url": img_url,
        "still_path": img_url,
    }


_THREE_TO_TWO = {
    "aar": "aa", "abk": "ab", "ave": "ae", "afr": "af", "aka": "ak",
    "amh": "am", "arg": "an", "ara": "ar", "asm": "as", "ava": "av",
    "aym": "ay", "aze": "az", "bak": "ba", "bel": "be", "bul": "bg",
    "bih": "bh", "bis": "bi", "bam": "bm", "ben": "bn", "bod": "bo",
    "bre": "br", "bos": "bs", "cat": "ca", "che": "ce", "cha": "ch",
    "cos": "co", "cre": "cr", "ces": "cs", "chu": "cu", "chv": "cv",
    "cym": "cy", "dan": "da", "deu": "de", "div": "dv", "dzo": "dz",
    "ewe": "ee", "ell": "el", "eng": "en", "epo": "eo", "spa": "es",
    "est": "et", "eus": "eu", "fas": "fa", "ful": "ff", "fin": "fi",
    "fij": "fj", "fao": "fo", "fra": "fr", "fry": "fy", "gle": "ga",
    "gla": "gd", "glg": "gl", "grn": "gn", "guj": "gu", "glv": "gv",
    "hau": "ha", "heb": "he", "hin": "hi", "hmo": "ho", "hrv": "hr",
    "hat": "ht", "hun": "hu", "hye": "hy", "her": "hz", "ina": "ia",
    "ind": "id", "ile": "ie", "ibo": "ig", "iii": "ii", "ipk": "ik",
    "ido": "io", "isl": "is", "ita": "it", "iku": "iu", "jpn": "ja",
    "jav": "jv", "kat": "ka", "kon": "kg", "kik": "ki", "kua": "kj",
    "kaz": "kk", "kal": "kl", "khm": "km", "kan": "kn", "kor": "ko",
    "kau": "kr", "kas": "ks", "kom": "kv", "cor": "kw", "kir": "ky",
    "lat": "la", "ltz": "lb", "lug": "lg", "lim": "li", "lin": "ln",
    "lao": "lo", "lit": "lt", "lub": "lu", "lav": "lv", "mlg": "mg",
    "mah": "mh", "mri": "mi", "mkd": "mk", "mal": "ml", "mon": "mn",
    "mar": "mr", "msa": "ms", "mlt": "mt", "mya": "my", "nau": "na",
    "nob": "nb", "nde": "nd", "nep": "ne", "ndo": "ng", "nld": "nl",
    "nno": "nn", "nor": "no", "nbl": "nr", "nav": "nv", "nya": "ny",
    "oci": "oc", "oji": "oj", "orm": "om", "ori": "or", "oss": "os",
    "pan": "pa", "pli": "pi", "pol": "pl", "pus": "ps", "por": "pt",
    "que": "qu", "roh": "rm", "run": "rn", "ron": "ro", "rus": "ru",
    "kin": "rw", "san": "sa", "srd": "sc", "snd": "sd", "sme": "se",
    "sag": "sg", "hbs": "sh", "sin": "si", "slk": "sk", "slv": "sl",
    "smo": "sm", "sna": "sn", "som": "so", "sqi": "sq", "srp": "sr",
    "ssw": "ss", "sot": "st", "sun": "su", "swe": "sv", "swa": "sw",
    "tam": "ta", "tel": "te", "tgk": "tg", "tha": "th", "tir": "ti",
    "tuk": "tk", "tgl": "tl", "tsn": "tn", "ton": "to", "tur": "tr",
    "tso": "ts", "tat": "tt", "twi": "tw", "tah": "ty", "uig": "ug",
    "ukr": "uk", "urd": "ur", "uzb": "uz", "ven": "ve", "vie": "vi",
    "vol": "vo", "wln": "wa", "wol": "wo", "xho": "xh", "yid": "yi",
    "yor": "yo", "zha": "za", "zho": "zh", "zul": "zu"
}


def to_two_letter_lang(lang_code: str | None) -> str | None:
    """Map a 3-letter language code (ISO 639-2/T) back to 2-letter code (ISO 639-1)."""
    if not lang_code:
        return None
    lang_code = lang_code.lower().strip()
    if len(lang_code) == 2:
        return lang_code
    return _THREE_TO_TWO.get(lang_code)
