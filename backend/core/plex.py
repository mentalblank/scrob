import httpx
import xmltodict
from typing import Optional, List, Dict

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

def extract_tmdb_id(guids: List[Dict]) -> Optional[int]:
    if not guids:
        return None
    for guid in guids:
        id_str = guid.get("id", "")
        if id_str.startswith("tmdb://"):
            try:
                return int(id_str.replace("tmdb://", ""))
            except ValueError:
                continue
    return None


def extract_tvdb_id(guids: List[Dict]) -> Optional[str]:
    """Extract TVDB ID string from a Plex Guid list, e.g. 'tvdb://73762' → '73762'."""
    if not guids:
        return None
    for guid in guids:
        id_str = guid.get("id", "")
        if id_str.startswith("tvdb://"):
            val = id_str.replace("tvdb://", "").strip()
            return val if val else None
    return None


def extract_imdb_id(guids: List[Dict]) -> Optional[str]:
    """Extract IMDb ID string from a Plex Guid list, e.g. 'imdb://tt0944947' → 'tt0944947'."""
    if not guids:
        return None
    for guid in guids:
        id_str = guid.get("id", "")
        if id_str.startswith("imdb://"):
            val = id_str.replace("imdb://", "").strip()
            return val if val else None
    return None

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
    elif plex_res == "480":
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

async def get_libraries(url: str, token: str) -> List[Dict]:
    data = await _get(f"{url.rstrip('/')}/library/sections", token)
    return data.get("MediaContainer", {}).get("Directory", [])

async def get_movies(url: str, token: str, section_id: str) -> List[Dict]:
    params = {"includeGuids": 1}
    data = await _get(f"{url.rstrip('/')}/library/sections/{section_id}/all", token, params=params)
    return data.get("MediaContainer", {}).get("Metadata", [])

async def get_shows(url: str, token: str, section_id: str) -> List[Dict]:
    params = {"includeGuids": 1}
    data = await _get(f"{url.rstrip('/')}/library/sections/{section_id}/all", token, params=params)
    return data.get("MediaContainer", {}).get("Metadata", [])

async def get_episodes(url: str, token: str, section_id: str) -> List[Dict]:
    params = {"type": 4, "includeGuids": 1}
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


async def mark_watched(url: str, token: str, rating_key: str) -> bool:
    """Scrobble a media item as watched on Plex."""
    try:
        async with httpx.AsyncClient(timeout=TIMEOUT, follow_redirects=False) as client:
            headers = {"X-Plex-Token": token, "Accept": "application/json"}
            r = await client.get(
                f"{url.rstrip('/')}/:/scrobble",
                headers=headers,
                params={"key": rating_key, "identifier": "com.plexapp.plugins.library"},
            )
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


async def set_rating(url: str, token: str, rating_key: str, rating: float) -> bool:
    """Set a star rating on a Plex item (0–10 scale)."""
    try:
        async with httpx.AsyncClient(timeout=TIMEOUT, follow_redirects=False) as client:
            headers = {"X-Plex-Token": token, "Accept": "application/json"}
            r = await client.put(
                f"{url.rstrip('/')}/library/metadata/{rating_key}/userRating",
                headers=headers,
                params={"rating": rating},
            )
            return r.status_code < 400
    except Exception:
        return False
