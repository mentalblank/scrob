import asyncio
import re
import unicodedata
from collections.abc import Awaitable, Callable, Iterable
from datetime import date

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from core import tmdb, tvdb
from models.episode_order import EpisodeOrderMapping, UserShowEpisodeOrder


_VALID_ORDERS = {"tmdb", "tvdb"}


def _normalise_title(value: str | None) -> str:
    text = unicodedata.normalize("NFKD", value or "").encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z0-9]+", "", text.lower())


def _date_distance(left: str | None, right: str | None) -> int | None:
    if not left or not right:
        return None
    try:
        return abs((date.fromisoformat(left) - date.fromisoformat(right)).days)
    except ValueError:
        return None


async def get_episode_order(
    db: AsyncSession,
    user_id: int,
    series_tmdb_id: int,
) -> UserShowEpisodeOrder | None:
    result = await db.execute(
        select(UserShowEpisodeOrder).where(
            UserShowEpisodeOrder.user_id == user_id,
            UserShowEpisodeOrder.series_tmdb_id == series_tmdb_id,
        )
    )
    return result.scalar_one_or_none()


async def get_mappings_for_tvdb_season(
    db: AsyncSession,
    series_tmdb_id: int,
    tvdb_season_number: int,
) -> list[EpisodeOrderMapping]:
    result = await db.execute(
        select(EpisodeOrderMapping)
        .where(
            EpisodeOrderMapping.series_tmdb_id == series_tmdb_id,
            EpisodeOrderMapping.tvdb_season_number == tvdb_season_number,
        )
        .order_by(EpisodeOrderMapping.tvdb_episode_number)
    )
    return list(result.scalars().all())


async def get_mapping_by_tvdb_position(
    db: AsyncSession,
    series_tmdb_id: int,
    tvdb_season_number: int,
    tvdb_episode_number: int,
) -> EpisodeOrderMapping | None:
    result = await db.execute(
        select(EpisodeOrderMapping).where(
            EpisodeOrderMapping.series_tmdb_id == series_tmdb_id,
            EpisodeOrderMapping.tvdb_season_number == tvdb_season_number,
            EpisodeOrderMapping.tvdb_episode_number == tvdb_episode_number,
        )
    )
    return result.scalar_one_or_none()


async def resolve_tvdb_series_id(
    series_tmdb_id: int,
    tmdb_api_key: str,
    tvdb_api_key: str,
    *,
    show_data: dict | None = None,
) -> int | None:
    """Find the TVDB series a TMDB show's episodes actually live in.

    TVDB keeps an anthology or a continuation as extra seasons of one series
    while TMDB splits it into separate shows, so the split-off show carries no
    series-level TVDB id at all. Its episodes still do, and a TVDB episode names
    its series — that is the only link between the two catalogues.
    """
    if show_data is None:
        show_data = await tmdb.get_show(series_tmdb_id, api_key=tmdb_api_key)
    direct = (show_data.get("external_ids") or {}).get("tvdb_id")
    if direct:
        return int(direct)

    season_numbers = sorted(
        {
            int(season["season_number"])
            for season in show_data.get("seasons") or []
            if season.get("season_number")
        }
    )
    for season_number in season_numbers[:2]:
        for episode_number in (1, 2):
            try:
                ids = await tmdb.get_episode_external_ids(
                    series_tmdb_id, season_number, episode_number, api_key=tmdb_api_key
                )
            except Exception:
                continue
            episode_tvdb_id = ids.get("tvdb_id")
            if not episode_tvdb_id:
                continue
            try:
                episode = await tvdb.get_episode(int(episode_tvdb_id), tvdb_api_key)
            except Exception:
                continue
            if episode.get("seriesId"):
                return int(episode["seriesId"])
    return None


async def tmdb_shows_for_tvdb_series(
    tvdb_series_id: int,
    tvdb_api_key: str,
    tmdb_api_key: str,
    *,
    tvdb_series: dict | None = None,
) -> dict[int, int]:
    """Map each official TVDB season number to the TMDB show that holds it.

    A TVDB series can span several TMDB shows. Neither catalogue links seasons
    to each other, but a TVDB episode carries an IMDb id and TMDB's /find
    reports which show that episode belongs to.
    """
    if tvdb_series is None:
        tvdb_series = await tvdb.get_series(tvdb_series_id, tvdb_api_key)

    first_episode_by_season: dict[int, int] = {}
    for episode in tvdb_series.get("episodes") or []:
        season_number = episode.get("seasonNumber")
        if season_number in (None, 0) or episode.get("id") is None:
            continue
        current = first_episode_by_season.get(int(season_number))
        if current is None or (episode.get("number") or 0) < current[1]:
            first_episode_by_season[int(season_number)] = (int(episode["id"]), episode.get("number") or 0)

    async def resolve(season_number: int, episode_id: int) -> tuple[int, int] | None:
        try:
            episode = await tvdb.get_episode(episode_id, tvdb_api_key)
            imdb_id = tvdb.remote_id(episode, "IMDB")
            if not imdb_id:
                return None
            found = await tmdb.find_by_external_id(imdb_id, "imdb_id", api_key=tmdb_api_key)
        except Exception:
            return None
        for result in found.get("tv_episode_results") or []:
            if result.get("show_id"):
                return season_number, int(result["show_id"])
        return None

    resolved = await asyncio.gather(
        *(
            resolve(season_number, entry[0])
            for season_number, entry in sorted(first_episode_by_season.items())
        )
    )
    return {season: show_id for season, show_id in (r for r in resolved if r)}


async def ensure_episode_order_mapping(
    db: AsyncSession,
    series_tmdb_id: int,
    tmdb_api_key: str,
    tvdb_api_key: str,
    *,
    force: bool = False,
    tvdb_series_id: int | None = None,
    on_progress: Callable[[int, int], Awaitable[None]] | None = None,
) -> dict:
    existing_result = await db.execute(
        select(EpisodeOrderMapping).where(
            EpisodeOrderMapping.series_tmdb_id == series_tmdb_id
        )
    )
    existing = list(existing_result.scalars().all())
    known_series_id = next(
        (row.tvdb_series_id for row in existing if row.tvdb_series_id), None
    )
    if existing and not force and known_series_id:
        return {
            "tvdb_id": known_series_id,
            "matched": len(existing),
            "tmdb_episodes": len(existing),
            "unmatched": 0,
        }

    show_data = await tmdb.get_show(series_tmdb_id, api_key=tmdb_api_key)
    # A caller coming from the TVDB side already knows the series. Trusting it
    # is what lets a show TMDB holds no TVDB ids for still map, on title and air
    # date — re-deriving the series from those missing ids can only fail.
    tvdb_id = tvdb_series_id or await resolve_tvdb_series_id(
        series_tmdb_id, tmdb_api_key, tvdb_api_key, show_data=show_data
    )
    if not tvdb_id:
        raise ValueError("TMDB does not expose a TVDB identifier for this show")

    if existing and not force:
        # Rows predate the column — stamp them rather than rebuilding the show.
        for row in existing:
            row.tvdb_series_id = tvdb_id
        await db.flush()
        return {
            "tvdb_id": tvdb_id,
            "matched": len(existing),
            "tmdb_episodes": len(existing),
            "unmatched": 0,
        }

    tmdb_season_numbers = sorted(
        {
            int(season["season_number"])
            for season in show_data.get("seasons") or []
            if season.get("season_number") is not None
        }
    )
    tvdb_series = await tvdb.get_series(tvdb_id, tvdb_api_key)
    tvdb_season_numbers = sorted(
        {
            int(season.get("number"))
            for season in tvdb_series.get("seasons") or []
            if season.get("number") is not None
            and (season.get("type") or {}).get("type") in (None, "official")
        }
    )
    if not tvdb_season_numbers:
        tvdb_season_numbers = sorted(
            {
                int(episode.get("seasonNumber"))
                for episode in tvdb_series.get("episodes") or []
                if episode.get("seasonNumber") is not None
            }
        )

    # One un-filtered fetch covers every season — TVDB returns the whole series
    # per request, so asking per-season would just repeat the same download.
    tmdb_seasons, all_tvdb_episodes = await asyncio.gather(
        asyncio.gather(
            *(tmdb.get_season(series_tmdb_id, number, api_key=tmdb_api_key) for number in tmdb_season_numbers)
        ),
        tvdb.get_series_episodes(tvdb_id, None, tvdb_api_key),
    )

    tmdb_episodes = [
        episode
        for season in tmdb_seasons
        for episode in season.get("episodes") or []
        if episode.get("season_number") is not None and episode.get("episode_number") is not None
    ]
    wanted_seasons = set(tvdb_season_numbers)
    tvdb_episodes = [
        episode for episode in all_tvdb_episodes
        if episode.get("seasonNumber") in wanted_seasons
    ]
    tvdb_by_id = {
        int(episode["id"]): episode
        for episode in tvdb_episodes
        if episode.get("id") is not None
    }

    # Neither side has anything to match against — fail immediately instead of
    # burning API calls on a per-episode loop that's guaranteed to find nothing.
    if not tmdb_episodes or not tvdb_episodes:
        raise ValueError("No TMDB episodes could be matched to TVDB")

    async def _would_match(episode: dict) -> bool:
        ids = await tmdb.get_episode_external_ids(
            series_tmdb_id,
            int(episode["season_number"]),
            int(episode["episode_number"]),
            api_key=tmdb_api_key,
        )
        external_tvdb_id = ids.get("tvdb_id")
        if external_tvdb_id and tvdb_by_id.get(int(external_tvdb_id)):
            return True
        title = _normalise_title(episode.get("name"))
        if not title:
            return False
        return any(
            _normalise_title(candidate.get("name")) == title
            and (_date_distance(episode.get("air_date"), candidate.get("aired")) or 0) <= 1
            for candidate in tvdb_episodes
        )

    # A show-wide numbering/title mismatch (the usual cause of a total match
    # failure) shows up regardless of which episode is checked — probe a small
    # sample spread across the whole series (not just season 1, which is the
    # most likely place for one-off numbering quirks) so a doomed mapping fails
    # in a handful of requests instead of after fetching external IDs for every
    # episode.
    PROBE_SIZE = 10
    if len(tmdb_episodes) > PROBE_SIZE:
        sample_indices = sorted({(i * len(tmdb_episodes)) // PROBE_SIZE for i in range(PROBE_SIZE)})
        probe_matches = await asyncio.gather(*(_would_match(tmdb_episodes[i]) for i in sample_indices))
        if not any(probe_matches):
            raise ValueError("No TMDB episodes could be matched to TVDB")

    total_episodes = len(tmdb_episodes)
    if on_progress:
        await on_progress(0, total_episodes)

    semaphore = asyncio.Semaphore(5)
    progress_lock = asyncio.Lock()
    completed = 0

    async def load_external_ids(episode: dict) -> tuple[dict, dict]:
        nonlocal completed
        async with semaphore:
            ids = await tmdb.get_episode_external_ids(
                series_tmdb_id,
                int(episode["season_number"]),
                int(episode["episode_number"]),
                api_key=tmdb_api_key,
            )
        if on_progress:
            async with progress_lock:
                completed += 1
                snapshot = completed
            if snapshot % 10 == 0 or snapshot == total_episodes:
                await on_progress(snapshot, total_episodes)
        return episode, ids

    external_rows = await asyncio.gather(
        *(load_external_ids(episode) for episode in tmdb_episodes)
    )

    used_tvdb_ids: set[int] = set()
    mappings: list[EpisodeOrderMapping] = []
    unmatched: list[dict] = []

    for episode, external_ids in external_rows:
        external_tvdb_id = external_ids.get("tvdb_id")
        match = tvdb_by_id.get(int(external_tvdb_id)) if external_tvdb_id else None
        method = "external_id"
        if match is None:
            title = _normalise_title(episode.get("name"))
            candidates = [
                candidate
                for candidate in tvdb_episodes
                if candidate.get("id") not in used_tvdb_ids
                and title
                and _normalise_title(candidate.get("name")) == title
                and (_date_distance(episode.get("air_date"), candidate.get("aired")) or 0) <= 1
            ]
            if len(candidates) == 1:
                match = candidates[0]
                method = "title_date"

        if match is None or match.get("seasonNumber") is None or match.get("number") is None:
            unmatched.append(episode)
            continue

        mapped_tvdb_id = int(match["id"])
        # Two TMDB episodes can carry the same TVDB external id — a split or a
        # merged entry on one side. A TVDB episode maps to exactly one position,
        # so the first claim wins; letting both through aborts the whole show's
        # mapping on the unique constraint and leaves it with none at all.
        if mapped_tvdb_id in used_tvdb_ids:
            unmatched.append(episode)
            continue
        used_tvdb_ids.add(mapped_tvdb_id)
        mappings.append(
            EpisodeOrderMapping(
                series_tmdb_id=series_tmdb_id,
                tmdb_season_number=int(episode["season_number"]),
                tmdb_episode_number=int(episode["episode_number"]),
                tmdb_episode_id=int(episode["id"]),
                tvdb_series_id=tvdb_id,
                tvdb_id=mapped_tvdb_id,
                tvdb_season_number=int(match["seasonNumber"]),
                tvdb_episode_number=int(match["number"]),
                match_method=method,
            )
        )

    if not mappings:
        raise ValueError("No TMDB episodes could be matched to TVDB")

    await db.execute(
        delete(EpisodeOrderMapping).where(
            EpisodeOrderMapping.series_tmdb_id == series_tmdb_id
        )
    )
    db.add_all(mappings)
    await db.flush()

    return {
        "tvdb_id": tvdb_id,
        "matched": len(mappings),
        "tmdb_episodes": len(tmdb_episodes),
        "unmatched": len(unmatched),
    }


def validate_episode_order(value: str) -> str:
    if value not in _VALID_ORDERS:
        raise ValueError(f"Unsupported episode order: {value}")
    return value


async def tmdb_episode_index(
    series_tmdb_id: int,
    api_key: str | None,
    *,
    season_numbers: Iterable[int] | None = None,
    concurrency: int = 8,
) -> dict[int, tuple[int, int, dict]]:
    """Map each TMDB episode id of a show to the position TMDB gives it.

    A media server can number a show its own way — TVDB seasons, absolute order,
    story arcs — while still naming every episode by its TMDB id. The id is what
    identifies an episode; the source's numbers only describe that server.

    Returns {episode_tmdb_id: (season_number, episode_number, episode_payload)}.
    """
    if season_numbers is None:
        show = await tmdb.get_show_light(series_tmdb_id, api_key=api_key)
        season_numbers = [
            s["season_number"]
            for s in show.get("seasons", [])
            if s.get("season_number") is not None
        ]

    semaphore = asyncio.Semaphore(concurrency)
    index: dict[int, tuple[int, int, dict]] = {}

    async def load(season_number: int) -> None:
        async with semaphore:
            try:
                data = await tmdb.get_season(series_tmdb_id, season_number, api_key=api_key)
            except Exception:
                return
            for episode in data.get("episodes", []):
                if episode.get("id") and episode.get("episode_number") is not None:
                    index[episode["id"]] = (
                        season_number,
                        episode["episode_number"],
                        episode,
                    )

    await asyncio.gather(*[load(sn) for sn in sorted(set(season_numbers))])
    return index
