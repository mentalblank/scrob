import asyncio
import time
import httpx
from core.config import settings

TMDB_BASE = "https://api.themoviedb.org/3"
TMDB_IMAGE_BASE = "https://image.tmdb.org/t/p"

# Errors that are worth retrying (transient). 404/4xx are permanent — don't retry.
_RETRYABLE = (httpx.ConnectError, httpx.TimeoutException, httpx.RemoteProtocolError)

DEFAULT_CACHE_TTL = 1800  # 30 minutes — TMDB metadata/discovery results don't need to be fresher than this

# Request-path budget. Kept deliberately tight: these calls sit inside
# page-render fan-outs (home page enrich_with_state, Next Up, etc.), so a wide
# retry window turns "TMDB is unreachable" into a multi-minute hang and a proxy
# 504 instead of a quick fall-back to locally-stored data. The circuit breaker
# below then makes every call after the first failure return instantly.
_HTTP_TIMEOUT = 8.0
_MAX_RETRIES = 1
_BACKOFF_BASE = 1  # seconds; sleep is _BACKOFF_BASE * 2**attempt between tries

# Circuit breaker: after _BREAKER_THRESHOLD consecutive retryable failures,
# short-circuit every _get for _BREAKER_COOLDOWN seconds. _fail_count is not
# reset when the cooldown lapses, so the first probe request that fails again
# re-opens the breaker immediately; a single success resets everything.
_BREAKER_THRESHOLD = 3
_BREAKER_COOLDOWN = 45.0
_breaker_fail_count = 0
_breaker_open_until = 0.0


class TMDBUnavailable(RuntimeError):
    """TMDB could not be reached — says nothing about whether a key is valid.

    Also raised by _get, with no HTTP attempt at all, while the circuit breaker
    is open. Callers that already tolerate a missing TMDB response - the
    metadata enrichers, trending rows - catch this like any other fetch error.
    """


def _breaker_blocked() -> bool:
    return _breaker_open_until > time.monotonic()


def _breaker_record_success() -> None:
    global _breaker_fail_count, _breaker_open_until
    _breaker_fail_count = 0
    _breaker_open_until = 0.0


def _breaker_record_failure() -> None:
    global _breaker_fail_count, _breaker_open_until
    _breaker_fail_count += 1
    if _breaker_fail_count >= _BREAKER_THRESHOLD:
        _breaker_open_until = time.monotonic() + _BREAKER_COOLDOWN


class _TTLCache:
    """Minimal bounded in-process cache: TTL expiry checked lazily on read, oldest
    entry evicted on overflow (dict insertion order). No shared/multi-worker
    guarantees — fine here since scrob runs a single uvicorn process; this just
    avoids re-hitting TMDB for identical requests within the TTL window, which is
    what was actually making every click slow for users far from TMDB's servers.

    Distinct from core/provider_cache, which is the database-backed cache shared
    across processes and restarts: this one exists so a single page render does
    not fan out the same request a dozen times, and so a caller can bypass it
    with cache_ttl=None when a user explicitly asked to refresh."""

    def __init__(self, maxsize: int = 2000):
        self._store: dict[tuple, tuple[float, dict]] = {}
        self._maxsize = maxsize

    def get(self, key: tuple):
        entry = self._store.get(key)
        if entry is None:
            return None
        expires_at, value = entry
        if time.monotonic() >= expires_at:
            del self._store[key]
            return None
        return value

    def set(self, key: tuple, value: dict, ttl: float) -> None:
        if key not in self._store and len(self._store) >= self._maxsize:
            self._store.pop(next(iter(self._store)))
        self._store[key] = (time.monotonic() + ttl, value)


_cache = _TTLCache()


def get_headers(api_key: str = None) -> dict:
    key = api_key or getattr(settings, 'tmdb_api_key', None)
    if not key:
        return {}
    return {
        "Authorization": f"Bearer {key}",
        "accept": "application/json",
    }


async def _get(
    url: str,
    *,
    headers: dict = None,
    params: dict = None,
    max_retries: int = _MAX_RETRIES,
    cache_ttl: float | None = DEFAULT_CACHE_TTL,
) -> dict:
    """Shared GET helper with retry + exponential backoff for transient failures.

    cache_ttl: seconds to cache the response for, keyed by (url, params) — the
    api_key in `headers` is auth-only and doesn't change TMDB's response content,
    so it's deliberately excluded from the cache key to share hits across users/
    jobs. Pass cache_ttl=None to bypass caching (e.g. validate_api_key, where the
    response genuinely depends on which key was used, or a user-initiated
    refresh that must not be served from a response fetched moments earlier).

    Raises TMDBUnavailable immediately (no HTTP attempt) while the circuit
    breaker is open — see the _breaker_* helpers above.
    """
    cache_key = None
    if cache_ttl is not None:
        cache_key = (url, tuple(sorted((params or {}).items())))
        cached = _cache.get(cache_key)
        if cached is not None:
            return cached

    if _breaker_blocked():
        raise TMDBUnavailable("TMDB circuit breaker open")

    last_exc: Exception | None = None
    for attempt in range(max_retries + 1):
        try:
            async with httpx.AsyncClient(timeout=httpx.Timeout(_HTTP_TIMEOUT)) as client:
                r = await client.get(url, headers=headers or {}, params=params)
                if r.status_code == 429:
                    wait = int(r.headers.get("Retry-After", 2 ** (attempt + 1)))
                    last_exc = httpx.HTTPStatusError(
                        "429 Too Many Requests", request=r.request, response=r
                    )
                    await asyncio.sleep(wait)
                    continue
                r.raise_for_status()
                data = r.json()
                if cache_key is not None:
                    _cache.set(cache_key, data, cache_ttl)
                _breaker_record_success()
                return data
        except _RETRYABLE as e:
            last_exc = e
            if attempt < max_retries:
                await asyncio.sleep(_BACKOFF_BASE * 2 ** attempt)
        except httpx.HTTPStatusError as e:
            # TMDB answers 5xx for its own upstream blips (status 43,
            # "Couldn't connect to the backend server") — the same request
            # succeeds moments later, so retry those. 4xx is our fault and
            # never fixes itself.
            if e.response.status_code < 500:
                raise
            last_exc = e
            if attempt >= max_retries:
                raise
            await asyncio.sleep(_BACKOFF_BASE * 2 ** attempt)
    # Only a genuine connectivity/timeout failure trips the breaker; a 429 or a
    # 5xx means TMDB is up and answering.
    if isinstance(last_exc, _RETRYABLE):
        _breaker_record_failure()
    raise last_exc


async def validate_api_key(api_key: str) -> bool:
    """True if TMDB accepts the key, False if it rejects it.

    Raises TMDBUnavailable when TMDB can't answer — an outage is not a verdict
    on the key, and reporting one as the other sends people to regenerate a key
    that was fine all along.
    """
    if not api_key:
        return False
    try:
        await _get(f"{TMDB_BASE}/authentication", headers=get_headers(api_key), cache_ttl=None)
        return True
    except httpx.HTTPStatusError as e:
        if e.response.status_code < 500:
            return False
        raise TMDBUnavailable(str(e)) from e
    except Exception as e:
        raise TMDBUnavailable(str(e)) from e


async def get_movie(tmdb_id: int, api_key: str = None, language: str | None = None, cache_ttl: float | None = DEFAULT_CACHE_TTL) -> dict:
    params: dict = {"append_to_response": "credits,release_dates,recommendations,external_ids,keywords"}
    if language:
        params["language"] = language
    return await _get(
        f"{TMDB_BASE}/movie/{tmdb_id}",
        headers=get_headers(api_key),
        params=params,
        cache_ttl=cache_ttl,
    )


async def get_show(tmdb_id: int, api_key: str = None, language: str | None = None, cache_ttl: float | None = DEFAULT_CACHE_TTL) -> dict:
    params: dict = {"append_to_response": "credits,content_ratings,recommendations,external_ids,keywords"}
    if language:
        params["language"] = language
    return await _get(
        f"{TMDB_BASE}/tv/{tmdb_id}",
        headers=get_headers(api_key),
        params=params,
        cache_ttl=cache_ttl,
    )


async def get_season(tmdb_id: int, season_number: int, api_key: str = None, language: str | None = None, cache_ttl: float | None = DEFAULT_CACHE_TTL) -> dict:
    params: dict = {}
    if language:
        params["language"] = language
    return await _get(
        f"{TMDB_BASE}/tv/{tmdb_id}/season/{season_number}",
        headers=get_headers(api_key),
        params=params or None,
        cache_ttl=cache_ttl,
    )


async def get_episode(tmdb_id: int, season_number: int, episode_number: int, api_key: str = None, language: str | None = None, cache_ttl: float | None = DEFAULT_CACHE_TTL) -> dict:
    params: dict = {"append_to_response": "credits"}
    if language:
        params["language"] = language
    return await _get(
        f"{TMDB_BASE}/tv/{tmdb_id}/season/{season_number}/episode/{episode_number}",
        headers=get_headers(api_key),
        params=params,
        cache_ttl=cache_ttl,
    )


async def get_episode_external_ids(
    tmdb_id: int,
    season_number: int,
    episode_number: int,
    api_key: str = None,
) -> dict:
    return await _get(
        f"{TMDB_BASE}/tv/{tmdb_id}/season/{season_number}/episode/{episode_number}/external_ids",
        headers=get_headers(api_key),
    )


async def get_movie_images(tmdb_id: int, api_key: str = None, language: str | None = None) -> dict:
    """All artwork TMDB holds for a movie: posters, backdrops, logos.

    include_image_language keeps the language-less artwork ("null"), which is
    most of the textless backdrops, alongside the requested language.
    """
    return await _get(
        f"{TMDB_BASE}/movie/{tmdb_id}/images",
        headers=get_headers(api_key),
        params={"include_image_language": f"{(language or 'en').split('-')[0]},null"},
    )


async def get_show_images(tmdb_id: int, api_key: str = None, language: str | None = None) -> dict:
    """All artwork TMDB holds for a show: posters, backdrops, logos."""
    return await _get(
        f"{TMDB_BASE}/tv/{tmdb_id}/images",
        headers=get_headers(api_key),
        params={"include_image_language": f"{(language or 'en').split('-')[0]},null"},
    )


async def get_season_images(tmdb_id: int, season_number: int, api_key: str = None, language: str | None = None) -> dict:
    """Season posters."""
    return await _get(
        f"{TMDB_BASE}/tv/{tmdb_id}/season/{season_number}/images",
        headers=get_headers(api_key),
        params={"include_image_language": f"{(language or 'en').split('-')[0]},null"},
    )


async def get_episode_images(tmdb_id: int, season_number: int, episode_number: int, api_key: str = None) -> dict:
    """Episode stills. TMDB does not language-filter these."""
    return await _get(
        f"{TMDB_BASE}/tv/{tmdb_id}/season/{season_number}/episode/{episode_number}/images",
        headers=get_headers(api_key),
    )


async def get_trending_movies(time_window: str = "day", page: int = 1, api_key: str = None, language: str | None = None) -> dict:
    params: dict = {"page": page}
    if language:
        params["language"] = language
    return await _get(f"{TMDB_BASE}/trending/movie/{time_window}", headers=get_headers(api_key), params=params)


async def get_trending_shows(time_window: str = "day", page: int = 1, api_key: str = None, language: str | None = None) -> dict:
    params: dict = {"page": page}
    if language:
        params["language"] = language
    return await _get(f"{TMDB_BASE}/trending/tv/{time_window}", headers=get_headers(api_key), params=params)


async def get_show_light(tmdb_id: int, api_key: str = None, language: str | None = None) -> dict:
    """Fetch base show details (includes last_episode_to_air / next_episode_to_air)."""
    params: dict = {}
    if language:
        params["language"] = language
    return await _get(f"{TMDB_BASE}/tv/{tmdb_id}", headers=get_headers(api_key), params=params or None)


async def get_movie_light(tmdb_id: int, api_key: str = None, language: str | None = None) -> dict:
    """Fetch base movie details without append_to_response (cheaper, used for translation backfill)."""
    params: dict = {}
    if language:
        params["language"] = language
    return await _get(f"{TMDB_BASE}/movie/{tmdb_id}", headers=get_headers(api_key), params=params or None)


async def get_on_air_today(page: int = 1, api_key: str = None, timezone: str = "UTC") -> dict:
    return await _get(f"{TMDB_BASE}/tv/airing_today", headers=get_headers(api_key), params={"page": page, "timezone": timezone})


async def get_popular_movies(page: int = 1, api_key: str = None, language: str | None = None) -> dict:
    params: dict = {"page": page}
    if language:
        params["language"] = language
    return await _get(f"{TMDB_BASE}/movie/popular", headers=get_headers(api_key), params=params)


async def get_top_rated_movies(page: int = 1, api_key: str = None, language: str | None = None) -> dict:
    params: dict = {"page": page}
    if language:
        params["language"] = language
    return await _get(f"{TMDB_BASE}/movie/top_rated", headers=get_headers(api_key), params=params)


async def get_popular_shows(page: int = 1, api_key: str = None, language: str | None = None) -> dict:
    params: dict = {"page": page}
    if language:
        params["language"] = language
    return await _get(f"{TMDB_BASE}/tv/popular", headers=get_headers(api_key), params=params)


async def get_top_rated_shows(page: int = 1, api_key: str = None, language: str | None = None) -> dict:
    params: dict = {"page": page}
    if language:
        params["language"] = language
    return await _get(f"{TMDB_BASE}/tv/top_rated", headers=get_headers(api_key), params=params)


async def search_multi(q: str, page: int = 1, api_key: str = None, language: str | None = None) -> dict:
    params: dict = {"query": q, "include_adult": "false", "page": page}
    if language:
        params["language"] = language
    return await _get(f"{TMDB_BASE}/search/multi", headers=get_headers(api_key), params=params)


async def search_movies(q: str, page: int = 1, year: int | None = None, api_key: str = None, language: str | None = None) -> dict:
    params: dict = {"query": q, "include_adult": "false", "page": page}
    if year:
        params["primary_release_year"] = year
    if language:
        params["language"] = language
    return await _get(f"{TMDB_BASE}/search/movie", headers=get_headers(api_key), params=params)


async def search_shows(q: str, page: int = 1, year: int | None = None, api_key: str = None, language: str | None = None) -> dict:
    params: dict = {"query": q, "include_adult": "false", "page": page}
    if year:
        params["first_air_date_year"] = year
    if language:
        params["language"] = language
    return await _get(f"{TMDB_BASE}/search/tv", headers=get_headers(api_key), params=params)


async def search_collection(q: str, page: int = 1, api_key: str = None) -> dict:
    return await _get(f"{TMDB_BASE}/search/collection", headers=get_headers(api_key), params={"query": q, "include_adult": "false", "page": page})


async def search_people(q: str, page: int = 1, api_key: str = None) -> dict:
    return await _get(f"{TMDB_BASE}/search/person", headers=get_headers(api_key), params={"query": q, "include_adult": "false", "page": page})


def poster_url(path: str, size: str = "w500") -> str | None:
    if not path:
        return None
    return f"{TMDB_IMAGE_BASE}/{size}{path}"


async def get_person(person_id: int, api_key: str = None) -> dict:
    return await _get(f"{TMDB_BASE}/person/{person_id}", headers=get_headers(api_key), params={"append_to_response": "combined_credits"})


async def get_movie_credits(movie_id: int, api_key: str = None) -> dict:
    return await _get(f"{TMDB_BASE}/movie/{movie_id}/credits", headers=get_headers(api_key))


async def get_genre_list(api_key: str = None) -> dict:
    return await _get(f"{TMDB_BASE}/genre/movie/list", headers=get_headers(api_key))


async def get_tv_genre_list(api_key: str = None) -> dict:
    return await _get(f"{TMDB_BASE}/genre/tv/list", headers=get_headers(api_key))


async def get_languages(api_key: str = None) -> list[dict]:
    return await _get(f"{TMDB_BASE}/configuration/languages", headers=get_headers(api_key))


async def get_countries(api_key: str = None) -> list[dict]:
    return await _get(f"{TMDB_BASE}/configuration/countries", headers=get_headers(api_key))


async def get_watch_providers(type: str = "movie", region: str = "US", api_key: str = None) -> list[dict]:
    """Fetch available watch providers for a specific region from TMDB."""
    path = "movie" if type == "movie" else "tv"
    res = await _get(
        f"{TMDB_BASE}/watch/providers/{path}",
        headers=get_headers(api_key),
        params={"watch_region": region},
    )
    return res.get("results", [])


async def get_now_playing(page: int = 1, api_key: str = None) -> dict:
    return await _get(f"{TMDB_BASE}/movie/now_playing", headers=get_headers(api_key), params={"page": page})


async def get_upcoming_movies(page: int = 1, api_key: str = None) -> dict:
    return await _get(f"{TMDB_BASE}/movie/upcoming", headers=get_headers(api_key), params={"page": page})


async def get_on_air_this_week(page: int = 1, api_key: str = None) -> dict:
    return await _get(f"{TMDB_BASE}/tv/on_the_air", headers=get_headers(api_key), params={"page": page})


async def get_movie_recommendations(movie_id: int, api_key: str = None) -> dict:
    return await _get(f"{TMDB_BASE}/movie/{movie_id}/recommendations", headers=get_headers(api_key))


async def get_show_recommendations(show_id: int, api_key: str = None) -> dict:
    return await _get(f"{TMDB_BASE}/tv/{show_id}/recommendations", headers=get_headers(api_key))


async def discover_movies(
    page: int = 1,
    genre_id: int | None = None,
    genre_ids: list[int] | None = None,  # OR'd via TMDB's "|" syntax; takes priority over genre_id if both given
    year: int | None = None,
    min_rating: float | None = None,
    vote_count_min: int | None = None,
    vote_count_max: int | None = None,
    sort_by: str = "popularity.desc",
    watch_provider_id: int | None = None,
    watch_region: str = "US",
    with_original_language: str | None = None,
    language: str | None = None,
    api_key: str = None,
) -> dict:
    params: dict = {
        "page": page,
        "sort_by": sort_by,
        "include_adult": "false",
        "vote_count.gte": vote_count_min if vote_count_min is not None else 50,
    }
    if genre_ids:
        params["with_genres"] = "|".join(str(g) for g in genre_ids)
    elif genre_id:
        params["with_genres"] = genre_id
    if year:
        params["primary_release_year"] = year
    if min_rating:
        params["vote_average.gte"] = min_rating
    if vote_count_max is not None:
        params["vote_count.lte"] = vote_count_max
    if watch_provider_id is not None:
        params["with_watch_providers"] = watch_provider_id
        params["watch_region"] = watch_region
    if with_original_language:
        params["with_original_language"] = with_original_language
    if language:
        params["language"] = language
    async with httpx.AsyncClient() as client:
        r = await client.get(
            f"{TMDB_BASE}/discover/movie",
            headers=get_headers(api_key),
            params=params,
        )
        r.raise_for_status()
        return r.json()


async def discover_shows(
    page: int = 1,
    genre_id: int | None = None,
    genre_ids: list[int] | None = None,  # OR'd via TMDB's "|" syntax; takes priority over genre_id if both given
    year: int | None = None,
    min_rating: float | None = None,
    vote_count_min: int | None = None,
    vote_count_max: int | None = None,
    sort_by: str = "popularity.desc",
    status: int | None = None,
    watch_provider_id: int | None = None,
    watch_region: str = "US",
    with_original_language: str | None = None,
    language: str | None = None,
    api_key: str = None,
) -> dict:
    params: dict = {
        "page": page,
        "sort_by": sort_by,
        "include_adult": "false",
        "vote_count.gte": vote_count_min if vote_count_min is not None else 50,
    }
    if genre_ids:
        params["with_genres"] = "|".join(str(g) for g in genre_ids)
    elif genre_id:
        params["with_genres"] = genre_id
    if year:
        params["first_air_date_year"] = year
    if min_rating:
        params["vote_average.gte"] = min_rating
    if vote_count_max is not None:
        params["vote_count.lte"] = vote_count_max
    if status is not None:
        params["with_status"] = status
    if watch_provider_id is not None:
        params["with_watch_providers"] = watch_provider_id
        params["watch_region"] = watch_region
    if with_original_language:
        params["with_original_language"] = with_original_language
    if language:
        params["language"] = language
    async with httpx.AsyncClient() as client:
        r = await client.get(
            f"{TMDB_BASE}/discover/tv",
            headers=get_headers(api_key),
            params=params,
        )
        r.raise_for_status()
        return r.json()


async def get_collection(collection_id: int, api_key: str = None) -> dict:
    return await _get(f"{TMDB_BASE}/collection/{collection_id}", headers=get_headers(api_key))


async def get_movie_videos(tmdb_id: int, api_key: str = None) -> dict:
    return await _get(f"{TMDB_BASE}/movie/{tmdb_id}/videos", headers=get_headers(api_key))


async def find_by_external_id(external_id: str, source: str, api_key: str = None) -> dict:
    """Find a movie or TV show by an external ID (imdb_id, tvdb_id, etc.)."""
    return await _get(f"{TMDB_BASE}/find/{external_id}", headers=get_headers(api_key), params={"external_source": source})


async def get_external_ids(tmdb_id: int, type: str, api_key: str = None) -> dict:
    """Fetch external IDs (IMDB, TVDB, etc.) for a movie or TV show."""
    path = "movie" if type == "movie" else "tv"
    return await _get(f"{TMDB_BASE}/{path}/{tmdb_id}/external_ids", headers=get_headers(api_key))


async def get_movie_watch_providers(movie_id: int, api_key: str = None) -> dict:
    return await _get(f"{TMDB_BASE}/movie/{movie_id}/watch/providers", headers=get_headers(api_key))


async def get_show_watch_providers(show_id: int, api_key: str = None) -> dict:
    return await _get(f"{TMDB_BASE}/tv/{show_id}/watch/providers", headers=get_headers(api_key))


def credits_stinger_fields(data: dict) -> dict:
    """The two stinger flags as tmdb_data keys, for spreading into a snapshot.

    Written together and always both present, so their presence is what marks
    a row as already checked - see webhooks._backfill_credits_stingers.
    """
    has_mid, has_post = extract_credits_stingers(data)
    return {"has_mid_credits_scene": has_mid, "has_post_credits_scene": has_post}


def extract_credits_stingers(data: dict) -> tuple[bool, bool]:
    """(has_mid_credits_scene, has_post_credits_scene) for a movie or show payload.

    TMDB has no dedicated field for this - it is carried by the
    community-maintained keywords 'duringcreditsstinger' and
    'aftercreditsstinger' on an append_to_response=keywords payload (#319).
    The two endpoints disagree on the shape: a movie nests the list under
    "keywords", a show under "results".
    """
    block = (data or {}).get("keywords") or {}
    keywords = block.get("keywords") or block.get("results") or []
    names = {str(k.get("name", "")).lower() for k in keywords if isinstance(k, dict)}
    return "duringcreditsstinger" in names, "aftercreditsstinger" in names
