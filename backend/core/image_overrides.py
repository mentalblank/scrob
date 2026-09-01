"""Per-user artwork overrides, and the response rewriting that applies them.

Image paths are emitted from roughly 340 places across the routers, so an
override is applied once, on the way out, by walking the JSON body rather than
by joining this table into every query that selects a poster.
"""

import logging
import time
from typing import Any, Optional

from sqlalchemy import select

from models.image_override import IMAGE_KINDS, NO_NUMBER, MediaImageOverride
from models.media import Media
from models.show import Show

logger = logging.getLogger(__name__)

TMDB_IMAGE_BASE = "https://image.tmdb.org/t/p"
TVDB_IMAGE_BASE = "https://artworks.thetvdb.com"

# Route that serves the external-image bucket. An override value has to work as
# a raw <img src> as well as through the frontend's tmdbImageUrl - several
# client-rendered lists (the navbar search dropdown, the calendar, the
# connections tables) drop the field straight into the tag.
EXTERNAL_URL_PREFIX = "/api/proxy/media/image/ext"

# Size baked into a TMDB override, per kind. These match what the routers
# already bake into the field being replaced, so an override arrives in the
# same shape - and lands in the same cache bucket - as the artwork it displaces.
_TMDB_SIZE_BY_KIND = {"poster": "w500", "backdrop": "w1280", "still": "w780"}

# Fields carrying artwork for the node itself, by media kind.
_SHOW_FIELDS = {"poster": ("poster_path",), "backdrop": ("backdrop_path",)}
# Episodes store their still in poster_path (enrichment writes the TMDB still
# there) as well as in still_path on the endpoints that expose both.
_EPISODE_STILL_FIELDS = ("still_path", "poster_path")

_OverrideMap = dict[tuple[str, int, int, str], str]

# Loaded maps are small (one row per artwork a user has replaced) and read on
# every response, so they are held briefly in-process and dropped on write.
_CACHE_TTL = 30.0
_cache: dict[int, tuple[float, _OverrideMap]] = {}


def override_key(subject_uri: str, season: int, episode: int, kind: str) -> tuple[str, int, int, str]:
    return (subject_uri, season, episode, kind)


def public_path(source: str, image_path: str, kind: str = "poster") -> str:
    """The value written into a response for a stored override.

    Every form is directly usable as an <img src>, and every form is one the
    frontend's tmdbImageUrl routes to the right image-cache bucket: a sized TMDB
    URL, a TVDB artwork URL, or this server's own path for a user-supplied image
    already downloaded into the external bucket.
    """
    if source == "external":
        return f"{EXTERNAL_URL_PREFIX}{image_path}"
    if source == "tvdb":
        return f"{TVDB_IMAGE_BASE}{image_path}"
    return f"{TMDB_IMAGE_BASE}/{_TMDB_SIZE_BY_KIND.get(kind, 'w500')}{image_path}"


def invalidate(user_id: int) -> None:
    _cache.pop(user_id, None)


async def load_overrides(db, user_id: int) -> _OverrideMap:
    """Every override this user has, keyed for lookup during response rewriting.

    A show reachable under both a TMDB and a TVDB id gets an entry under each,
    so an override set from one provider's page still applies on the other's.
    """
    cached = _cache.get(user_id)
    if cached and time.monotonic() - cached[0] < _CACHE_TTL:
        return cached[1]

    rows = (await db.execute(
        select(MediaImageOverride, Show.tmdb_id, Show.tvdb_id, Media.tmdb_id, Media.uri_id)
        .outerjoin(Show, MediaImageOverride.show_id == Show.id)
        .outerjoin(Media, MediaImageOverride.media_id == Media.id)
        .where(MediaImageOverride.user_id == user_id)
    )).all()

    overrides: _OverrideMap = {}
    for override, show_tmdb_id, show_tvdb_id, media_tmdb_id, media_uri_id in rows:
        value = public_path(override.source, override.image_path, override.image_kind)
        subjects = {override.subject_uri}
        if show_tmdb_id:
            subjects.add(f"tmdb:s:{show_tmdb_id}")
        if show_tvdb_id:
            subjects.add(f"tvdb:s:{show_tvdb_id}")
        if media_tmdb_id:
            subjects.add(f"tmdb:m:{media_tmdb_id}")
        if media_uri_id:
            subjects.add(media_uri_id)
        for subject in subjects:
            overrides[override_key(subject, override.season_number, override.episode_number, override.image_kind)] = value

    _cache[user_id] = (time.monotonic(), overrides)
    return overrides


def _as_id(value: Any) -> Optional[str]:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return str(value)
    if isinstance(value, str) and value.isdigit():
        return value
    return None


def _prefixed_keys(node: dict, prefix: str) -> list[str]:
    """Subject uris for a prefixed sibling group - show_*, source_show_*, movie_*."""
    infix = "m" if prefix.split("_")[-1] == "movie" else "s"
    keys: list[str] = []
    uri = node.get(f"{prefix}_uri_id")
    if isinstance(uri, str) and f":{infix}:" in uri:
        keys.append(uri)
    for field, provider in ((f"{prefix}_tmdb_id", "tmdb"), (f"{prefix}_tvdb_id", "tvdb")):
        external_id = _as_id(node.get(field))
        if external_id:
            keys.append(f"{provider}:{infix}:{external_id}")
    return keys


def _show_keys(node: dict) -> list[str]:
    return _prefixed_keys(node, "show")


def _own_keys(node: dict, prefix: str) -> list[str]:
    """Provider uris for the node's own subject, for a series ('s') or movie ('m')."""
    keys: list[str] = []
    uri = node.get("uri_id")
    if isinstance(uri, str) and f":{prefix}:" in uri:
        keys.append(uri)
    for field, provider in (("tmdb_id", "tmdb"), ("tvdb_id", "tvdb")):
        external_id = _as_id(node.get(field))
        if external_id:
            keys.append(f"{provider}:{prefix}:{external_id}")
    return keys


def _number(value: Any) -> int:
    return value if isinstance(value, int) and not isinstance(value, bool) else NO_NUMBER


def _lookup(overrides: _OverrideMap, keys: list[str], season: int, episode: int, kind: str) -> Optional[str]:
    for key in keys:
        value = overrides.get(override_key(key, season, episode, kind))
        if value is not None:
            return value
    return None


def _node_type(node: dict, show_keys: list[str], season: int, episode: int) -> Optional[str]:
    node_type = node.get("type")
    if node_type in ("series", "season", "episode", "movie"):
        return node_type
    uri = node.get("uri_id")
    if isinstance(uri, str):
        if ":m:" in uri:
            return "movie"
        if ":e:" in uri:
            return "episode"
        if ":s:" in uri and season == NO_NUMBER:
            return "series"
    # Bare nested records - a seasons_meta entry, an episode inside a season
    # payload - carry only their numbers and inherit the show from their parent.
    if show_keys and episode != NO_NUMBER:
        return "episode"
    if show_keys and season != NO_NUMBER:
        return "season"
    return None


def _apply_node(node: dict, overrides: _OverrideMap, inherited_show_keys: list[str]) -> list[str]:
    """Rewrite one node's artwork fields; return the show keys its children inherit."""
    show_keys = _show_keys(node) or inherited_show_keys
    season = _number(node.get("season_number"))
    episode = _number(node.get("episode_number"))
    node_type = _node_type(node, show_keys, season, episode)

    if node_type == "series":
        show_keys = _own_keys(node, "s") or show_keys

    # Artwork belonging to a *related* subject travels under a prefix -
    # show_poster_path on an episode card, source_show_poster_path on a remap,
    # movie_poster_path on a conversion - always beside that subject's own ids.
    for field in list(node.keys()):
        for kind, suffix in (("poster", "_poster_path"), ("backdrop", "_backdrop_path")):
            if not field.endswith(suffix) or field == suffix[1:]:
                continue
            prefix = field[: -len(suffix)]
            keys = _prefixed_keys(node, prefix)
            if not keys and prefix == "show":
                keys = show_keys
            replacement = _lookup(overrides, keys, NO_NUMBER, NO_NUMBER, kind)
            if replacement:
                node[field] = replacement

    if node_type == "series":
        for kind, fields in _SHOW_FIELDS.items():
            replacement = _lookup(overrides, show_keys, NO_NUMBER, NO_NUMBER, kind)
            if replacement:
                for field in fields:
                    if field in node:
                        node[field] = replacement

    elif node_type == "season" and season != NO_NUMBER:
        replacement = _lookup(overrides, show_keys, season, NO_NUMBER, "poster")
        if replacement and "poster_path" in node:
            node["poster_path"] = replacement

    elif node_type == "episode" and season != NO_NUMBER and episode != NO_NUMBER:
        replacement = _lookup(overrides, show_keys, season, episode, "still")
        if replacement:
            for field in _EPISODE_STILL_FIELDS:
                if field in node:
                    node[field] = replacement

    elif node_type == "movie":
        movie_keys = _own_keys(node, "m")
        for kind, fields in _SHOW_FIELDS.items():
            replacement = _lookup(overrides, movie_keys, NO_NUMBER, NO_NUMBER, kind)
            if replacement:
                for field in fields:
                    if field in node:
                        node[field] = replacement

    return show_keys


def apply_overrides(payload: Any, overrides: _OverrideMap, inherited_show_keys: Optional[list[str]] = None) -> Any:
    """Rewrite every artwork field in a decoded JSON body, in place."""
    if not overrides:
        return payload
    inherited = inherited_show_keys or []
    if isinstance(payload, dict):
        child_keys = _apply_node(payload, overrides, inherited)
        for value in payload.values():
            if isinstance(value, (dict, list)):
                apply_overrides(value, overrides, child_keys)
    elif isinstance(payload, list):
        for entry in payload:
            if isinstance(entry, (dict, list)):
                apply_overrides(entry, overrides, inherited)
    return payload


__all__ = [
    "IMAGE_KINDS",
    "NO_NUMBER",
    "apply_overrides",
    "invalidate",
    "load_overrides",
    "override_key",
    "public_path",
]
