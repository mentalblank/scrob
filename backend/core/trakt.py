"""Trakt.tv API client.

Uses the Device Authentication flow — no redirect URI needed.
Trakt uses TMDB IDs natively, so no external ID mapping is required.

Rate limits: 1000 requests per 5 minutes per user.
"""

import asyncio
import logging
from datetime import datetime
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

TRAKT_BASE = "https://api.trakt.tv"
TIMEOUT = 30.0


def _headers(client_id: str, access_token: Optional[str] = None) -> dict:
    h = {
        "Content-Type": "application/json",
        "trakt-api-version": "2",
        "trakt-api-key": client_id,
    }
    if access_token:
        h["Authorization"] = f"Bearer {access_token}"
    return h


# ── Device Authentication ─────────────────────────────────────────────────────

async def start_device_auth(client_id: str) -> dict:
    """Start the device authentication flow.

    Returns: {device_code, user_code, verification_url, expires_in, interval}
    """
    async with httpx.AsyncClient(timeout=TIMEOUT) as client:
        resp = await client.post(
            f"{TRAKT_BASE}/oauth/device/code",
            json={"client_id": client_id},
            headers={"Content-Type": "application/json"},
        )
        resp.raise_for_status()
        return resp.json()


async def poll_device_token(client_id: str, client_secret: str, device_code: str) -> Optional[dict]:
    """Poll for the device token.

    Returns token dict on success, None if still pending (authorization_pending / slow_down).
    Raises httpx.HTTPStatusError on permanent failure (expired / denied).
    """
    async with httpx.AsyncClient(timeout=TIMEOUT) as client:
        resp = await client.post(
            f"{TRAKT_BASE}/oauth/device/token",
            json={
                "code": device_code,
                "client_id": client_id,
                "client_secret": client_secret,
            },
            headers={"Content-Type": "application/json"},
        )
        if resp.status_code == 200:
            return resp.json()
        if resp.status_code in (400, 429):
            # 400 = authorization_pending / slow_down — keep polling
            return None
        resp.raise_for_status()
        return None


async def refresh_access_token(client_id: str, client_secret: str, refresh_token: str) -> dict:
    """Exchange a refresh token for a new access token."""
    async with httpx.AsyncClient(timeout=TIMEOUT) as client:
        resp = await client.post(
            f"{TRAKT_BASE}/oauth/token",
            json={
                "refresh_token": refresh_token,
                "client_id": client_id,
                "client_secret": client_secret,
                "redirect_uri": "urn:ietf:wg:oauth:2.0:oob",
                "grant_type": "refresh_token",
            },
            headers={"Content-Type": "application/json"},
        )
        resp.raise_for_status()
        return resp.json()


async def revoke_token(client_id: str, client_secret: str, access_token: str) -> None:
    """Revoke an access token (disconnect)."""
    try:
        async with httpx.AsyncClient(timeout=TIMEOUT) as client:
            await client.post(
                f"{TRAKT_BASE}/oauth/revoke",
                json={
                    "token": access_token,
                    "client_id": client_id,
                    "client_secret": client_secret,
                },
                headers={"Content-Type": "application/json"},
            )
    except Exception as exc:
        logger.warning("Failed to revoke Trakt token: %s", exc)


async def validate_token(client_id: str, access_token: str) -> bool:
    """Return True if the token is valid."""
    try:
        async with httpx.AsyncClient(timeout=TIMEOUT) as client:
            resp = await client.get(
                f"{TRAKT_BASE}/users/me",
                headers=_headers(client_id, access_token),
            )
            return resp.status_code == 200
    except Exception:
        return False


# ── User Data Fetching ────────────────────────────────────────────────────────

async def get_watched_movies(client_id: str, access_token: str) -> list[dict]:
    """Fetch all watched movies.

    Returns list of: {plays, last_watched_at, movie: {title, ids: {tmdb, ...}}}
    """
    async with httpx.AsyncClient(timeout=60.0) as client:
        resp = await client.get(
            f"{TRAKT_BASE}/sync/watched/movies",
            headers=_headers(client_id, access_token),
        )
        resp.raise_for_status()
        return resp.json()


async def get_watched_shows(client_id: str, access_token: str) -> list[dict]:
    """Fetch all watched shows with episode-level detail.

    Returns list of: {show: {title, ids: {tmdb, ...}}, seasons: [{number, episodes: [{number, plays, last_watched_at}]}]}
    """
    async with httpx.AsyncClient(timeout=120.0) as client:
        resp = await client.get(
            f"{TRAKT_BASE}/sync/watched/shows",
            headers=_headers(client_id, access_token),
        )
        resp.raise_for_status()
        return resp.json()


async def get_ratings(client_id: str, access_token: str) -> dict:
    """Fetch all user ratings.

    Returns: {movies: [{rated_at, rating, movie: {ids: {tmdb}}}],
              shows: [{rated_at, rating, show: {ids: {tmdb}}}]}
    """
    async def _fetch(path: str) -> list:
        async with httpx.AsyncClient(timeout=60.0) as client:
            resp = await client.get(f"{TRAKT_BASE}{path}", headers=_headers(client_id, access_token))
            resp.raise_for_status()
            return resp.json()

    movies, shows = await asyncio.gather(
        _fetch("/sync/ratings/movies"),
        _fetch("/sync/ratings/shows"),
    )
    return {"movies": movies, "shows": shows}


async def get_history(
    client_id: str, access_token: str, start_at: Optional[datetime] = None
) -> list[dict]:
    """Fetch user's playback history.
    
    If start_at is provided, only items watched after that date are returned.
    Returns list of: {id, watched_at, action, type, movie: {...}, show: {...}, season: {...}, episode: {...}}
    """
    params = {"limit": 100}
    if start_at:
        # Trakt expects ISO 8601 with Z for UTC
        params["start_at"] = start_at.isoformat() + "Z"
    
    async with httpx.AsyncClient(timeout=60.0) as client:
        resp = await client.get(
            f"{TRAKT_BASE}/sync/history",
            headers=_headers(client_id, access_token),
            params=params,
        )
        resp.raise_for_status()
        return resp.json()


# ── Outbound Push ─────────────────────────────────────────────────────────────

async def add_movie_to_history(client_id: str, access_token: str, tmdb_id: int) -> None:
    """Mark a movie as watched on Trakt."""
    async with httpx.AsyncClient(timeout=TIMEOUT) as client:
        resp = await client.post(
            f"{TRAKT_BASE}/sync/history",
            json={"movies": [{"ids": {"tmdb": tmdb_id}}]},
            headers=_headers(client_id, access_token),
        )
        resp.raise_for_status()


async def remove_movie_from_history(client_id: str, access_token: str, tmdb_id: int) -> None:
    """Mark a movie as unwatched on Trakt."""
    async with httpx.AsyncClient(timeout=TIMEOUT) as client:
        resp = await client.post(
            f"{TRAKT_BASE}/sync/history/remove",
            json={"movies": [{"ids": {"tmdb": tmdb_id}}]},
            headers=_headers(client_id, access_token),
        )
        resp.raise_for_status()


async def add_episode_to_history(
    client_id: str,
    access_token: str,
    show_tmdb_id: int,
    season_number: int,
    episode_number: int,
) -> None:
    """Mark an episode as watched on Trakt."""
    async with httpx.AsyncClient(timeout=TIMEOUT) as client:
        resp = await client.post(
            f"{TRAKT_BASE}/sync/history",
            json={
                "shows": [{
                    "ids": {"tmdb": show_tmdb_id},
                    "seasons": [{
                        "number": season_number,
                        "episodes": [{"number": episode_number}],
                    }],
                }]
            },
            headers=_headers(client_id, access_token),
        )
        resp.raise_for_status()


async def remove_episode_from_history(
    client_id: str,
    access_token: str,
    show_tmdb_id: int,
    season_number: int,
    episode_number: int,
) -> None:
    """Mark an episode as unwatched on Trakt."""
    async with httpx.AsyncClient(timeout=TIMEOUT) as client:
        resp = await client.post(
            f"{TRAKT_BASE}/sync/history/remove",
            json={
                "shows": [{
                    "ids": {"tmdb": show_tmdb_id},
                    "seasons": [{
                        "number": season_number,
                        "episodes": [{"number": episode_number}],
                    }],
                }]
            },
            headers=_headers(client_id, access_token),
        )
        resp.raise_for_status()


async def set_movie_rating(
    client_id: str, access_token: str, tmdb_id: int, rating: float
) -> None:
    """Rate a movie on Trakt (1–10 scale)."""
    trakt_rating = max(1, min(10, round(rating)))
    async with httpx.AsyncClient(timeout=TIMEOUT) as client:
        resp = await client.post(
            f"{TRAKT_BASE}/sync/ratings",
            json={"movies": [{"rating": trakt_rating, "ids": {"tmdb": tmdb_id}}]},
            headers=_headers(client_id, access_token),
        )
        resp.raise_for_status()


async def remove_movie_rating(client_id: str, access_token: str, tmdb_id: int) -> None:
    """Remove a movie rating on Trakt."""
    async with httpx.AsyncClient(timeout=TIMEOUT) as client:
        resp = await client.post(
            f"{TRAKT_BASE}/sync/ratings/remove",
            json={"movies": [{"ids": {"tmdb": tmdb_id}}]},
            headers=_headers(client_id, access_token),
        )
        resp.raise_for_status()


async def get_user_lists(client_id: str, access_token: str) -> list[dict]:
    """Fetch the authenticated user's personal lists.

    Returns list of: {name, description, slug, item_count, ...}
    """
    async with httpx.AsyncClient(timeout=TIMEOUT) as client:
        resp = await client.get(
            f"{TRAKT_BASE}/users/me/lists",
            headers=_headers(client_id, access_token),
        )
        resp.raise_for_status()
        return resp.json()


async def get_list_items(client_id: str, access_token: str, list_slug: str) -> list[dict]:
    """Fetch items in a user's personal list.

    Returns list of: {type, movie: {title, ids: {tmdb}}, show: {title, ids: {tmdb}}}
    """
    async with httpx.AsyncClient(timeout=60.0) as client:
        resp = await client.get(
            f"{TRAKT_BASE}/users/me/lists/{list_slug}/items",
            headers=_headers(client_id, access_token),
        )
        resp.raise_for_status()
        return resp.json()


async def get_watchlist(client_id: str, access_token: str) -> list[dict]:
    """Fetch the user's watchlist (movies + shows combined).

    Returns list of: {type, movie: {title, ids: {tmdb}}, show: {title, ids: {tmdb}}}
    """
    async def _fetch(kind: str) -> list:
        async with httpx.AsyncClient(timeout=60.0) as client:
            resp = await client.get(
                f"{TRAKT_BASE}/sync/watchlist/{kind}",
                headers=_headers(client_id, access_token),
            )
            resp.raise_for_status()
            return resp.json()

    movies, shows = await asyncio.gather(_fetch("movies"), _fetch("shows"))
    return movies + shows


async def add_to_watchlist(client_id: str, access_token: str, media_type: str, tmdb_id: int) -> None:
    """Add a movie or show to the user's watchlist. media_type must be 'movies' or 'shows'."""
    async with httpx.AsyncClient(timeout=TIMEOUT) as client:
        resp = await client.post(
            f"{TRAKT_BASE}/sync/watchlist",
            json={media_type: [{"ids": {"tmdb": tmdb_id}}]},
            headers=_headers(client_id, access_token),
        )
        resp.raise_for_status()


async def remove_from_watchlist(client_id: str, access_token: str, media_type: str, tmdb_id: int) -> None:
    """Remove a movie or show from the user's watchlist. media_type must be 'movies' or 'shows'."""
    async with httpx.AsyncClient(timeout=TIMEOUT) as client:
        resp = await client.post(
            f"{TRAKT_BASE}/sync/watchlist/remove",
            json={media_type: [{"ids": {"tmdb": tmdb_id}}]},
            headers=_headers(client_id, access_token),
        )
        resp.raise_for_status()


async def add_to_list(client_id: str, access_token: str, list_slug: str, media_type: str, tmdb_id: int) -> None:
    """Add a movie or show to a Trakt list.

    media_type must be 'movies' or 'shows'.
    """
    async with httpx.AsyncClient(timeout=TIMEOUT) as client:
        resp = await client.post(
            f"{TRAKT_BASE}/users/me/lists/{list_slug}/items",
            json={media_type: [{"ids": {"tmdb": tmdb_id}}]},
            headers=_headers(client_id, access_token),
        )
        resp.raise_for_status()


async def remove_from_list(client_id: str, access_token: str, list_slug: str, media_type: str, tmdb_id: int) -> None:
    """Remove a movie or show from a Trakt list.

    media_type must be 'movies' or 'shows'.
    """
    async with httpx.AsyncClient(timeout=TIMEOUT) as client:
        resp = await client.post(
            f"{TRAKT_BASE}/users/me/lists/{list_slug}/items/remove",
            json={media_type: [{"ids": {"tmdb": tmdb_id}}]},
            headers=_headers(client_id, access_token),
        )


async def set_show_rating(
    client_id: str, access_token: str, tmdb_id: int, rating: float
) -> None:
    """Rate a show on Trakt."""
    trakt_rating = max(1, min(10, round(rating)))
    async with httpx.AsyncClient(timeout=TIMEOUT) as client:
        resp = await client.post(
            f"{TRAKT_BASE}/sync/ratings",
            json={"shows": [{"rating": trakt_rating, "ids": {"tmdb": tmdb_id}}]},
            headers=_headers(client_id, access_token),
        )
        resp.raise_for_status()


async def remove_show_rating(client_id: str, access_token: str, tmdb_id: int) -> None:
    """Remove a show rating on Trakt."""
    async with httpx.AsyncClient(timeout=TIMEOUT) as client:
        resp = await client.post(
            f"{TRAKT_BASE}/sync/ratings/remove",
            json={"shows": [{"ids": {"tmdb": tmdb_id}}]},
            headers=_headers(client_id, access_token),
        )
        resp.raise_for_status()
