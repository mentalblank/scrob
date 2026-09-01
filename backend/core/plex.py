import asyncio
import logging
import re
import httpx
import xmltodict
from datetime import datetime, timezone
from typing import Optional, List, Dict
from urllib.parse import urlencode

logger = logging.getLogger(__name__)

TIMEOUT = httpx.Timeout(120.0)

async def _get(url: str, token: str, params: Optional[Dict] = None) -> Dict:
    async with httpx.AsyncClient(timeout=TIMEOUT, follow_redirects=False) as client:
        headers = {
            "X-Plex-Token": token,
            "Accept": "application/json"
        }
        res = await client.get(url, headers=headers, params=params)
        res.raise_for_status()
        return res.json()

def get_guids(item: Dict) -> List[Dict]:
    """Return a normalised Guid list for a Plex item.

    Modern Plex returns a 'Guid' array: [{"id": "tmdb://123"}, ...].
    Legacy items may have an empty/missing 'Guid' but a single lowercase 'guid'
    string like 'com.plexapp.agents.thetvdb://73762/1/1'.
    HAMA items have a guid string like 'com.plexapp.agents.hama://tvdb-73762/1/1'.
    """
    guids = item.get("Guid") or []
    if not guids:
        legacy = item.get("guid", "")
        if legacy:
            guids = [{"id": legacy}]
    return guids


# Plex exposes a source id in a Guid two ways:
#   * the plain scheme - "tmdb://123", or the legacy agent form
#     "com.plexapp.agents.themoviedb://123/1/1"
#   * the HAMA / Absolute Series Scanner agent, which packs the real source
#     behind its own scheme: "com.plexapp.agents.hama://tvdb-73762/1/1"
#     (and "tvdb2-", "tvdb3-" ... for TheTVDB's alternate episode orders).
# Anchoring each alternative on the "://" boundary keeps a stray "tmdb-123"
# elsewhere in the string from being read as an id, and \d+ stops at the
# season/episode path and any "?lang=" suffix on its own.
_TMDB_GUID_RE = re.compile(r"(?:tmdb|themoviedb)://(\d+)|://tmdb-(\d+)")
_TVDB_GUID_RE = re.compile(r"tvdb://(\d+)|://tvdb[2-9]?-(\d+)")
_IMDB_GUID_RE = re.compile(r"imdb://(tt\d+)|://imdb-(tt\d+)")


def _first_guid_match(guids: List[Dict], pattern: "re.Pattern[str]") -> Optional[str]:
    for guid in guids or []:
        match = pattern.search(guid.get("id", "") or "")
        if match:
            return next((g for g in match.groups() if g), None)
    return None


def extract_tmdb_id(guids: List[Dict]) -> Optional[int]:
    raw = _first_guid_match(guids, _TMDB_GUID_RE)
    return int(raw) if raw is not None else None


def extract_tvdb_id(guids: List[Dict]) -> Optional[str]:
    ids = extract_all_tvdb_ids(guids)
    return ids[0] if ids else None


# The HAMA agent writes the series id inline instead of as a scheme -
# "com.plexapp.agents.hama://tvdb-73762/1/1" - and uses tvdb2-/tvdb3-... for
# TheTVDB's alternate episode orders, where the series id is still the part we
# want. Matching on the prefix alone missed every one of those.
_TVDB_GUID_RE = re.compile(r"tvdb://(\d+)|://tvdb[2-9]?-(\d+)")


def extract_all_tvdb_ids(guids: List[Dict]) -> List[str]:
    """Return ALL TVDB IDs from Plex GUIDs."""
    ids: List[str] = []
    for guid in guids or []:
        match = _TVDB_GUID_RE.search(guid.get("id", "") or "")
        if not match:
            continue
        val = (match.group(1) or match.group(2) or "").strip()
        if val and val not in ids:
            ids.append(val)
    return ids


def extract_imdb_id(guids: List[Dict]) -> Optional[str]:
    return _first_guid_match(guids, _IMDB_GUID_RE)

def extract_quality(media_list: List[Dict]) -> Dict:
    if not media_list:
        return {}
    
    # Plex usually has multiple 'Media' objects for different versions, we take the first
    m = media_list[0]
    h = m.get("height", 0)
    w = m.get("width", 0)

    # Prefer Plex's own videoResolution label (e.g. "1080", "720", "4k") when available.
    plex_res = str(m.get("videoResolution", "")).lower()
    if plex_res in ("4k", "2160"):
        resolution = "4K"
    elif plex_res == "1080":
        resolution = "1080p"
    elif plex_res == "720":
        resolution = "720p"
    elif plex_res in ("480", "sd"):
        resolution = "480p"
    elif plex_res:
        resolution = f"{plex_res}p"
    else:
        # Fallback using both width and height so cinemascope encodes like
        # 1920x800 (2.40:1) are not misclassified — width is the reliable dimension.
        if w >= 3200 or h >= 2000:
            resolution = "4K"
        elif w >= 1700 or h >= 800:
            resolution = "1080p"
        elif w >= 1100 or h >= 540:
            resolution = "720p"
        else:
            resolution = f"{h}p"

    quality = {
        "resolution": resolution,
        "video_codec": m.get("videoCodec"),
        "audio_codec": m.get("audioCodec"),
        "audio_channels": f"{m.get('audioChannels', 0)}.0" if m.get("audioChannels") else None,
        "audio_languages": [],
        "subtitle_languages": [],
    }
    
    # Plex JSON doesn't always have deep stream info in the list view, 
    # but we can try to extract from the first Part
    parts = m.get("Part", [])
    if parts:
        p = parts[0]
        quality["file_path"] = p.get("file")
        
        # Extract languages from streams if available
        streams = p.get("Stream", [])
        for s in streams:
            stream_type = s.get("streamType")
            # Plex uses language (e.g. "English") or languageCode (e.g. "en") or languageTag (e.g. "en")
            lang = s.get("languageTag") or s.get("languageCode") or s.get("language")
            
            if not lang:
                continue
                
            if stream_type == 2: # Audio
                if lang not in quality["audio_languages"]:
                    quality["audio_languages"].append(lang)
            elif stream_type == 3: # Subtitle
                if lang not in quality["subtitle_languages"]:
                    quality["subtitle_languages"].append(lang)
        
    return quality

async def get_item(url: str, token: str, rating_key: str) -> Optional[Dict]:
    """Fetch full metadata for a single item by ratingKey, including Media/Part/Stream detail."""
    try:
        data = await _get(
            f"{url.rstrip('/')}/library/metadata/{rating_key}",
            token,
            params={"includeGuids": 1},
        )
        items = data.get("MediaContainer", {}).get("Metadata", [])
        return items[0] if items else None
    except Exception:
        return None


async def find_movie_by_tmdb_id(url: str, token: str, tmdb_id: int) -> Optional[Dict]:
    """Search all Plex libraries for a movie by TMDB ID. Returns the item (with Media/Part detail) or None."""
    try:
        data = await _get(
            f"{url.rstrip('/')}/library/all",
            token,
            params={"type": 1, "guid": f"tmdb://{tmdb_id}", "includeGuids": 1},
        )
        items = data.get("MediaContainer", {}).get("Metadata", [])
        if not items:
            return None
        # Fetch the full item with Media/Part/Stream detail
        return await get_item(url, token, str(items[0]["ratingKey"]))
    except Exception:
        return None


async def find_episode_by_ids(url: str, token: str, series_tmdb_id: int, season: int, episode: int) -> Optional[Dict]:
    """Search all Plex libraries for an episode by series TMDB ID + season + episode number."""
    try:
        # Try filtering by grandparent GUID and indexes (supported on modern Plex)
        data = await _get(
            f"{url.rstrip('/')}/library/all",
            token,
            params={
                "type": 4,
                "grandparentGuid": f"tmdb://{series_tmdb_id}",
                "parentIndex": season,
                "index": episode,
                "includeGuids": 1,
            },
        )
        items = data.get("MediaContainer", {}).get("Metadata", [])
        if items:
            return await get_item(url, token, str(items[0]["ratingKey"]))
        return None
    except Exception:
        return None


async def validate_connection(url: str, token: str) -> bool:
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(10.0), follow_redirects=False) as client:
            headers = {
                "X-Plex-Token": token,
                "Accept": "application/json"
            }
            # Simple endpoint to check connection
            r = await client.get(f"{url.rstrip('/')}/", headers=headers)
            return r.status_code == 200
    except Exception:
        return False

async def get_machine_identifier(url: str, token: str) -> Optional[str]:
    """Return the server's machineIdentifier from the Plex root endpoint."""
    try:
        data = await _get(f"{url.rstrip('/')}/", token)
        return data.get("MediaContainer", {}).get("machineIdentifier")
    except Exception:
        return None

async def get_libraries(url: str, token: str) -> List[Dict]:
    data = await _get(f"{url.rstrip('/')}/library/sections", token)
    return data.get("MediaContainer", {}).get("Directory", [])

async def get_history(url: str, token: str, mindate: int) -> List[Dict]:
    """Fetch playback history from Plex since a given epoch timestamp."""
    data = await _get(
        f"{url.rstrip('/')}/status/sessions/history/all",
        token,
        params={"mindate": mindate}
    )
    return data.get("MediaContainer", {}).get("Metadata", [])

async def _get_all_plex_items(
    url: str,
    token: str,
    section_id: str,
    plex_type: int,
    sort: Optional[str] = None,
    offset: int = 0,
    limit: Optional[int] = None
) -> List[Dict]:
    all_items = []
    current_offset = offset
    page_size = 500 if limit is None else min(limit, 500)
    while True:
        params = {"type": plex_type, "includeGuids": 1}
        if sort:
            params["sort"] = sort
        params["X-Plex-Container-Start"] = current_offset
        params["X-Plex-Container-Size"] = page_size

        data = await _get(f"{url.rstrip('/')}/library/sections/{section_id}/all", token, params=params)
        mc = data.get("MediaContainer", {})
        items = mc.get("Metadata", [])
        all_items.extend(items)

        total = mc.get("totalSize", 0)
        current_offset += len(items)

        if limit is not None and len(all_items) >= limit:
            all_items = all_items[:limit]
            break
        if not items or current_offset >= total:
            break

    return all_items


async def get_movies(url: str, token: str, section_id: str, sort: Optional[str] = None, offset: int = 0, limit: Optional[int] = None) -> List[Dict]:
    return await _get_all_plex_items(url, token, section_id, 1, sort, offset, limit)


async def get_shows(url: str, token: str, section_id: str, sort: Optional[str] = None, offset: int = 0, limit: Optional[int] = None) -> List[Dict]:
    return await _get_all_plex_items(url, token, section_id, 2, sort, offset, limit)


async def get_episodes(url: str, token: str, section_id: str, sort: Optional[str] = None, offset: int = 0, limit: Optional[int] = None) -> List[Dict]:
    return await _get_all_plex_items(url, token, section_id, 4, sort, offset, limit)

async def get_items_by_ids(url: str, token: str, section_id: str, rating_keys: List[str], chunk_size: int = 200) -> List[Dict]:
    if not rating_keys:
        return []

    all_items: List[Dict] = []
    base_url = f"{url.rstrip('/')}/library/sections/{section_id}/all"

    for i in range(0, len(rating_keys), chunk_size):
        chunk = rating_keys[i : i + chunk_size]
        params = {"includeGuids": 1, "id": ",".join(chunk)}
        data = await _get(base_url, token, params=params)
        all_items.extend(data.get("MediaContainer", {}).get("Metadata", []))

    return all_items

async def get_seasons(url: str, token: str, section_id: str) -> List[Dict]:
    """Fetch season metadata, including user ratings, from a TV library."""
    params = {"type": 3, "includeGuids": 1}
    data = await _get(f"{url.rstrip('/')}/library/sections/{section_id}/all", token, params=params)
    return data.get("MediaContainer", {}).get("Metadata", [])

async def get_recently_added(url: str, token: str, section_id: str, media_type: int, limit: int = 50) -> List[Dict]:
    """Fetch the most recently-added items from a library section.

    media_type: 1 = movie, 4 = episode
    Returns items with full Guid/Media/Part detail.
    """
    try:
        data = await _get(
            f"{url.rstrip('/')}/library/sections/{section_id}/recentlyAdded",
            token,
            params={"type": media_type, "includeGuids": 1, "X-Plex-Container-Size": limit},
        )
        return data.get("MediaContainer", {}).get("Metadata", [])
    except Exception:
        return []


async def get_history(url: str, token: str, since: Optional[datetime] = None) -> List[Dict]:
    """Fetch per-play viewing history from this server (one row per actual play,
    unlike get_movies/get_shows/get_episodes which only expose the aggregate
    viewCount/lastViewedAt fields).

    since: if given, only plays at or after this time are returned (Plex's
    viewedAt> filter). Paginated the same way as get_watchlist.
    Returns [] rather than raising on failure (old server, missing endpoint,
    insufficient token permission) so a sync can just skip the backfill.
    """
    items: List[Dict] = []
    start = 0
    params: Dict = {"sort": "viewedAt:asc"}
    if since is not None:
        # The cursor is stored as naive UTC; .timestamp() on a naive datetime
        # interprets it as *local* time, shifting the window by the host's UTC
        # offset. West of UTC that starts the fetch hours too late, and since
        # the cursor still advances, those plays are then skipped forever (#126).
        if since.tzinfo is None:
            since = since.replace(tzinfo=timezone.utc)
        params["viewedAt>"] = int(since.timestamp())
    try:
        while True:
            data = await _get(
                f"{url.rstrip('/')}/status/sessions/history/all",
                token,
                params={**params, "X-Plex-Container-Start": start},
            )
            container = data.get("MediaContainer", {})
            batch = container.get("Metadata", [])
            items.extend(batch)
            total = container.get("totalSize", 0) or len(items)
            start += len(batch)
            if not batch or start >= total:
                break
        return items
    except Exception:
        logger.warning("Could not fetch Plex play history from %s", url)
        return []


async def get_account_id(url: str, token: str, username: str) -> Optional[int]:
    """Resolve a server-local Plex username (MediaServerConnection.server_username)
    to its numeric account id, for scoping history to one user on a shared server."""
    try:
        data = await _get(f"{url.rstrip('/')}/accounts", token)
        accounts = data.get("MediaContainer", {}).get("Account", [])
        for account in accounts:
            name = account.get("name") or account.get("title") or ""
            if name.strip().lower() == username.strip().lower():
                account_id = account.get("id")
                return int(account_id) if account_id is not None else None
    except Exception:
        logger.warning("Could not resolve Plex account id for username=%s", username)
    return None


METADATA_BASE = "https://metadata.provider.plex.tv"
DISCOVER_BASE = "https://discover.provider.plex.tv"
PLEX_TV_BASE  = "https://plex.tv"
APP_AUTH_BASE = "https://app.plex.tv/auth"
COMMUNITY_BASE = "https://community.plex.tv"

# ── Plex account SSO (plex.tv PIN OAuth) ───────────────────────────────────────
PLEX_AUTH_CLIENT_ID = "scrob-login"
PLEX_AUTH_PRODUCT = "Scrob"
_AUTH_HEADERS = {
    "Accept": "application/json",
    "X-Plex-Product": PLEX_AUTH_PRODUCT,
    "X-Plex-Client-Identifier": PLEX_AUTH_CLIENT_ID,
}


async def create_auth_pin() -> Optional[Dict]:
    """Create a strong Plex auth PIN. Returns {id, code} or None."""
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(15.0), follow_redirects=True) as client:
            res = await client.post(
                f"{PLEX_TV_BASE}/api/v2/pins",
                headers=_AUTH_HEADERS,
                params={"strong": "true"},
            )
            res.raise_for_status()
            data = res.json()
            return {"id": data["id"], "code": data["code"]}
    except Exception:
        return None


def build_auth_url(code: str, forward_url: str) -> str:
    """Build the app.plex.tv URL the browser is sent to for authentication."""
    params = {
        "clientID": PLEX_AUTH_CLIENT_ID,
        "code": code,
        "forwardUrl": forward_url,
        "context[device][product]": PLEX_AUTH_PRODUCT,
    }
    return f"https://app.plex.tv/auth#?{urlencode(params)}"


async def check_auth_pin(pin_id: str) -> Optional[str]:
    """Poll a PIN; return the authToken once the user has authorised, else None."""
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(15.0), follow_redirects=True) as client:
            res = await client.get(
                f"{PLEX_TV_BASE}/api/v2/pins/{pin_id}",
                headers=_AUTH_HEADERS,
            )
            res.raise_for_status()
            return res.json().get("authToken") or None
    except Exception:
        return None


async def get_account(auth_token: str) -> Optional[Dict]:
    """Fetch the Plex account for an authToken. Returns {id, username, email, thumb} or None."""
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(15.0), follow_redirects=True) as client:
            res = await client.get(
                f"{PLEX_TV_BASE}/api/v2/user",
                headers={**_AUTH_HEADERS, "X-Plex-Token": auth_token},
            )
            res.raise_for_status()
            data = res.json()
            if not data.get("id"):
                return None
            return {
                "id": str(data["id"]),
                "username": data.get("username") or data.get("title") or "",
                "email": data.get("email") or "",
                "thumb": data.get("thumb"),
            }
    except Exception:
        return None


def _connection_rank(conn: Dict) -> tuple:
    """Sort key for a server connection: non-relay https remote first, relay last."""
    relay = 1 if conn.get("relay") else 0
    uri = str(conn.get("uri", ""))
    https = 0 if (conn.get("protocol") == "https" or uri.startswith("https")) else 1
    local = 1 if conn.get("local") else 0
    return (relay, https, local)


async def get_servers(auth_token: str) -> List[Dict]:
    """List reachable Plex servers as [{name, client_identifier, token, url, owned}].

    url is the first connection that responds; token is the per-server access token.
    """
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(20.0), follow_redirects=True) as client:
            res = await client.get(
                f"{PLEX_TV_BASE}/api/v2/resources",
                headers={**_AUTH_HEADERS, "X-Plex-Token": auth_token},
                params={"includeHttps": 1, "includeRelay": 1},
            )
            res.raise_for_status()
            devices = res.json()
    except Exception:
        return []

    servers: List[Dict] = []
    for dev in devices:
        if "server" not in (dev.get("provides") or "").split(","):
            continue
        token = dev.get("accessToken")
        if not token:
            continue
        for conn in sorted(dev.get("connections") or [], key=_connection_rank):
            uri = conn.get("uri")
            if uri and await validate_connection(uri, token):
                servers.append({
                    "name": dev.get("name") or "Plex",
                    "client_identifier": dev.get("clientIdentifier"),
                    "token": token,
                    "url": uri,
                    "owned": bool(dev.get("owned")),
                })
                break
    return servers

_CLOUD_HEADERS = {
    "Accept": "application/json",
    "X-Plex-Product": "Scrob",
    "X-Plex-Client-Identifier": "scrob-watchlist",
}


async def _post_graphql(token: str, query: str, variables: Optional[Dict] = None) -> Dict:
    """POST a GraphQL query to the Plex community API."""
    payload: Dict = {"query": query}
    if variables:
        payload["variables"] = variables
    async with httpx.AsyncClient(timeout=TIMEOUT, follow_redirects=True) as client:
        res = await client.post(
            f"{COMMUNITY_BASE}/api",
            headers={"X-Plex-Token": token, "Content-Type": "application/json", "Accept": "application/json"},
            json=payload,
        )
        res.raise_for_status()
        return res.json()


async def get_all_friends(token: str) -> List[Dict]:
    """Return all Plex friends (server users) visible to this token via the community GraphQL API."""
    try:
        data = await _post_graphql(token, """
            query GetAllFriends {
              allFriendsV2 {
                user { id username displayName }
              }
            }
        """)
        friends = []
        for entry in (data.get("data") or {}).get("allFriendsV2") or []:
            user = entry.get("user", {})
            if user.get("id"):
                friends.append({
                    "watchlist_id": user["id"],
                    "username": user.get("username", ""),
                    "display_name": user.get("displayName", ""),
                })
        return friends
    except Exception:
        return []


async def get_friend_watchlist(token: str, watchlist_id: str) -> List[Dict]:
    """Fetch all watchlist items for a friend via the community GraphQL API.
    Returns list of {id (plex metadata id), title, type} — no GUIDs; enrich separately.
    """
    items = []
    cursor = None
    query = """
        query GetWatchlist($user: UserInput!, $first: PaginationInt!, $after: String) {
          userV2(user: $user) {
            ... on User {
              watchlist(first: $first, after: $after) {
                nodes { id title type }
                pageInfo { hasNextPage endCursor }
              }
            }
          }
        }
    """
    while True:
        try:
            data = await _post_graphql(
                token, query,
                variables={"user": {"id": watchlist_id}, "first": 100, "after": cursor},
            )
        except Exception:
            break
        watchlist = (data.get("data") or {}).get("userV2", {}).get("watchlist", {})
        nodes = watchlist.get("nodes", [])
        items.extend(nodes)
        page_info = watchlist.get("pageInfo", {})
        if not page_info.get("hasNextPage") or not page_info.get("endCursor"):
            break
        cursor = page_info["endCursor"]
    return items


async def enrich_plex_item(token: str, plex_id: str) -> Optional[Dict]:
    """Fetch full metadata for a Plex community item to get GUIDs (TMDB/TVDB/IMDB IDs)."""
    try:
        data = await _get(
            f"{DISCOVER_BASE}/library/metadata/{plex_id}",
            token,
            params={"includeGuids": 1},
        )
        items = data.get("MediaContainer", {}).get("Metadata", [])
        return items[0] if items else None
    except Exception:
        return None


_RESOLVE_CANDIDATE_CHECK_LIMIT = 10


async def resolve_tmdb_ratingkey(token: str, tmdb_id: int, media_type: str, title: str) -> str | None:
    """Return the Plex Discover ratingKey for an item identified by TMDB ID.

    Discover has no endpoint that filters its global catalog by external
    guid directly - `/library/sections/computer/all?guid=...` (the original
    approach here) isn't a real Discover section and always came back empty.
    The approach actually used by Plex's own clients (and python-plexapi's
    MyPlexAccount.searchDiscover) is a title search against `/library/search`.

    That search's results do NOT include each candidate's external Guid list
    despite `includeMetadata=1` (confirmed live against issue #119/#83 - every
    candidate came back with Guid=None even for an exact title match), so
    title text alone can't be trusted to pick the right candidate on its own
    (remakes, unrelated titles that share a name). Each candidate's ratingKey
    is verified against enrich_plex_item's full metadata (which does carry
    Guid) until the exact tmdb match is found, checking only the closest
    _RESOLVE_CANDIDATE_CHECK_LIMIT results since Discover search is already
    relevance-ranked and the right item is normally at or near the top.

    media_type must be 'movie' or 'show'.
    Returns None if the item cannot be found.
    """
    libtype = "movies" if media_type == "movie" else "tv"
    want_guid = f"tmdb://{tmdb_id}"
    try:
        async with httpx.AsyncClient(timeout=TIMEOUT, follow_redirects=True) as client:
            res = await client.get(
                f"{DISCOVER_BASE}/library/search",
                headers={"X-Plex-Token": token, "Accept": "application/json"},
                params={
                    "X-Plex-Token": token,
                    "query": title,
                    "limit": 30,
                    "searchTypes": libtype,
                    "searchProviders": "discover",
                    "includeMetadata": 1,
                },
            )
            if res.status_code >= 400:
                logger.warning(
                    "Discover search failed for tmdb_id=%s title=%r: HTTP %s - %s",
                    tmdb_id, title, res.status_code, res.text[:500],
                )
                return None
            data = res.json()
        search_results = data.get("MediaContainer", {}).get("SearchResults", [])
        external = next(
            (s.get("SearchResult", []) for s in search_results if s.get("id") == "external"),
            [],
        )
        candidates = [
            rk for r in external
            if (rk := r.get("Metadata", {}).get("ratingKey"))
        ][:_RESOLVE_CANDIDATE_CHECK_LIMIT]

        checked = []
        for rating_key in candidates:
            full = await enrich_plex_item(token, rating_key)
            guids = (full or {}).get("Guid", []) or []
            checked.append({"ratingKey": rating_key, "guids": guids})
            if any(g.get("id") == want_guid for g in guids):
                return rating_key

        logger.warning(
            "No Discover match for tmdb_id=%s title=%r (want %s): %s section(s), "
            "%s external candidate(s), checked=%s",
            tmdb_id, title, want_guid, len(search_results), len(external), checked,
        )
        return None
    except Exception:
        logger.exception("Discover search errored for tmdb_id=%s title=%r", tmdb_id, title)
        return None


async def resolve_season_rating_key(
    url: str,
    token: str,
    show_tmdb_id: int,
    season_number: int,
) -> str | None:
    """Resolve a local Plex season ratingKey from a TMDB show ID and season number."""
    try:
        data = await _get(
            f"{url.rstrip('/')}/library/sections/all",
            token,
            params={"type": 2, "guid": f"tmdb://{show_tmdb_id}", "includeGuids": 1},
        )
        shows = data.get("MediaContainer", {}).get("Metadata", [])
        show = next(
            (
                item
                for item in shows
                if extract_tmdb_id(get_guids(item)) == show_tmdb_id
            ),
            None,
        )
        if not show or not show.get("ratingKey"):
            return None
        children = await _get(
            f"{url.rstrip('/')}/library/metadata/{show['ratingKey']}/children",
            token,
        )
        seasons = children.get("MediaContainer", {}).get("Metadata", [])
        match = next(
            (
                item
                for item in seasons
                if item.get("type") == "season" and item.get("index") == season_number
            ),
            None,
        )
        return str(match["ratingKey"]) if match and match.get("ratingKey") else None
    except Exception:
        return None


async def add_to_watchlist(token: str, rating_key: str) -> bool:
    """Add a Plex item to the user's watchlist by its Discover ratingKey."""
    try:
        async with httpx.AsyncClient(timeout=PUSH_TIMEOUT, follow_redirects=True) as client:
            r = await client.put(
                f"{DISCOVER_BASE}/actions/addToWatchlist",
                headers={"X-Plex-Token": token, "Accept": "application/json"},
                params={"X-Plex-Token": token, "ratingKey": rating_key},
            )
            return r.status_code < 400
    except Exception:
        return False


async def remove_from_watchlist(token: str, rating_key: str) -> bool:
    """Remove a Plex item from the user's watchlist by its Discover ratingKey."""
    try:
        async with httpx.AsyncClient(timeout=PUSH_TIMEOUT, follow_redirects=True) as client:
            r = await client.delete(
                f"{DISCOVER_BASE}/actions/removeFromWatchlist",
                headers={"X-Plex-Token": token, "Accept": "application/json"},
                params={"X-Plex-Token": token, "ratingKey": rating_key},
            )
            return r.status_code < 400
    except Exception:
        return False


async def get_watchlist(token: str) -> List[Dict]:
    """Fetch all items from the user's Plex watchlist via the Plex Discover API."""
    items: List[Dict] = []
    start = 0
    url = f"{DISCOVER_BASE}/library/sections/watchlist/all"
    while True:
        async with httpx.AsyncClient(timeout=TIMEOUT, follow_redirects=True) as client:
            res = await client.get(
                url,
                headers={"X-Plex-Token": token, "Accept": "application/json"},
                params={"X-Plex-Token": token, "X-Plex-Container-Start": start, "includeGuids": 1},
            )
            res.raise_for_status()
            data = res.json()
        container = data.get("MediaContainer", {})
        batch = container.get("Metadata", [])
        items.extend(batch)
        total = container.get("totalSize", 0) or len(items)
        start += len(batch)
        if not batch or start >= total:
            break
    return items


PUSH_TIMEOUT = httpx.Timeout(15.0)

async def mark_watched(url: str, token: str, rating_key: str, client: httpx.AsyncClient | None = None) -> bool:
    """Scrobble a media item as watched on Plex."""
    headers = {"X-Plex-Token": token, "Accept": "application/json"}
    params = {"key": rating_key, "identifier": "com.plexapp.plugins.library"}
    try:
        if client:
            r = await client.get(f"{url.rstrip('/')}/:/scrobble", headers=headers, params=params)
            return r.status_code < 400
        async with httpx.AsyncClient(timeout=PUSH_TIMEOUT, follow_redirects=False) as c:
            r = await c.get(f"{url.rstrip('/')}/:/scrobble", headers=headers, params=params)
            return r.status_code < 400
    except Exception:
        return False

async def mark_unwatched(url: str, token: str, rating_key: str) -> bool:
    """Unscrobble a media item on Plex."""
    try:
        async with httpx.AsyncClient(timeout=TIMEOUT, follow_redirects=False) as client:
            headers = {"X-Plex-Token": token, "Accept": "application/json"}
            r = await client.get(
                f"{url.rstrip('/')}/:/unscrobble",
                headers=headers,
                params={"key": rating_key, "identifier": "com.plexapp.plugins.library"},
            )
            return r.status_code < 400
    except Exception:
        return False

async def scan_libraries(url: str, token: str, section_keys: list[str]) -> bool:
    """Trigger a library scan on the given section keys. Scans all sections if list is empty."""
    try:
        if not section_keys:
            libraries = await get_libraries(url, token)
            section_keys = [lib["key"] for lib in libraries]
        async with httpx.AsyncClient(timeout=TIMEOUT, follow_redirects=False) as client:
            headers = {"X-Plex-Token": token, "Accept": "application/json"}
            for key in section_keys:
                r = await client.get(
                    f"{url.rstrip('/')}/library/sections/{key}/refresh",
                    headers=headers,
                )
                if r.status_code >= 400:
                    return False
        return True
    except Exception:
        return False


async def set_rating(url: str, token: str, rating_key: str, rating: float, client: httpx.AsyncClient | None = None) -> bool:
    """Set a star rating on a Plex item (0–10 scale)."""
    headers = {"X-Plex-Token": token, "Accept": "application/json"}
    params = {"key": rating_key, "identifier": "com.plexapp.plugins.library", "rating": rating}
    try:
        if client:
            r = await client.put(f"{url.rstrip('/')}/:/rate", headers=headers, params=params)
            return r.status_code < 400
        async with httpx.AsyncClient(timeout=PUSH_TIMEOUT, follow_redirects=False) as c:
            r = await c.put(f"{url.rstrip('/')}/:/rate", headers=headers, params=params)
            return r.status_code < 400
    except Exception:
        return False
