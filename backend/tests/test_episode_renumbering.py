import os
import unittest
from unittest.mock import AsyncMock, patch

os.environ.setdefault("SECRET_KEY", "test-secret")
os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://test:test@localhost/test")

from models.base import MediaType
from models.media import Media
from routers import sync as sync_router

SHOW_TMDB = 37854

# TMDB numbers this show continuously; the media server splits it into TVDB
# seasons, so the same episode ids sit at different positions on each side.
TMDB_SEASONS = {
    1: [
        {"id": 852692, "episode_number": 1, "name": "Episode 1", "runtime": 24},
        {"id": 852691, "episode_number": 8, "name": "Episode 8", "runtime": 24},
        {"id": 1152978, "episode_number": 9, "name": "Episode 9", "runtime": 24},
    ],
    2: [
        {"id": 1146416, "episode_number": 62, "name": "Episode 62", "runtime": 24},
    ],
}


def _episode(tmdb_id, season_number, episode_number):
    return Media(
        tmdb_id=tmdb_id,
        media_type=MediaType.episode,
        title="from the server",
        season_number=season_number,
        episode_number=episode_number,
    )


async def _fake_get_season(series_tmdb_id, season_number, api_key=None, **kwargs):
    if season_number not in TMDB_SEASONS:
        raise RuntimeError("404 season not found")
    return {"episodes": TMDB_SEASONS[season_number]}


async def _fake_get_show_light(series_tmdb_id, api_key=None, **kwargs):
    return {"seasons": [{"season_number": n} for n in TMDB_SEASONS]}


class BatchEnrichRenumberingTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.patches = [
            patch.object(sync_router.tmdb, "get_season", _fake_get_season),
            patch("core.episode_order.tmdb.get_season", _fake_get_season),
            patch("core.episode_order.tmdb.get_show_light", _fake_get_show_light),
        ]
        for p in self.patches:
            p.start()

    async def asyncTearDown(self) -> None:
        for p in self.patches:
            p.stop()

    async def test_episode_is_filed_where_tmdb_puts_it_not_where_the_server_did(self) -> None:
        """The server calls this episode s2e1; TMDB calls it s1e9. The TMDB
        episode id is the same on both sides, so the id decides."""
        media = _episode(1152978, season_number=2, episode_number=1)

        await sync_router.batch_enrich_items([(media, SHOW_TMDB)], api_key="k")

        self.assertEqual((media.season_number, media.episode_number), (1, 9))
        self.assertEqual(media.title, "Episode 9")

    async def test_an_episode_already_in_the_right_place_is_left_alone(self) -> None:
        media = _episode(852692, season_number=1, episode_number=1)

        await sync_router.batch_enrich_items([(media, SHOW_TMDB)], api_key="k")

        self.assertEqual((media.season_number, media.episode_number), (1, 1))
        self.assertEqual(media.title, "Episode 1")

    async def test_episode_without_a_tmdb_id_keeps_the_position_it_was_given(self) -> None:
        media = _episode(None, season_number=1, episode_number=8)

        await sync_router.batch_enrich_items([(media, SHOW_TMDB)], api_key="k")

        self.assertEqual((media.season_number, media.episode_number), (1, 8))
        self.assertEqual(media.title, "Episode 8")

    async def test_a_season_tmdb_does_not_have_does_not_strand_its_episodes(self) -> None:
        """The server's season 9 doesn't exist on TMDB, but its episodes carry
        ids TMDB knows — they must still land in the right place."""
        media = _episode(1146416, season_number=9, episode_number=3)

        warnings = await sync_router.batch_enrich_items([(media, SHOW_TMDB)], api_key="k")

        self.assertEqual((media.season_number, media.episode_number), (2, 62))
        self.assertEqual(warnings, [])


class TvdbStillMatchingTests(unittest.IsolatedAsyncioTestCase):
    """A still is matched on the row's own TVDB episode id. The two catalogues
    number plenty of shows differently, so matching on position would hang the
    wrong image on an episode."""

    async def asyncSetUp(self) -> None:
        self.patches = [
            patch.object(sync_router.tmdb, "get_season", _fake_get_season),
            patch("core.episode_order.tmdb.get_season", _fake_get_season),
            patch("core.episode_order.tmdb.get_show_light", _fake_get_show_light),
            patch("core.enrichment._tvdb_series_artwork",
                  AsyncMock(return_value=("https://artworks.thetvdb.com/p.jpg",
                                          "https://artworks.thetvdb.com/b.jpg"))),
            patch("core.tvdb.get_series_episodes", AsyncMock(return_value=[
                {"id": 900001, "image": "/still-a.jpg", "seasonNumber": 2, "number": 1},
                {"id": 900002, "image": "/still-b.jpg", "seasonNumber": 1, "number": 1},
            ])),
        ]
        for p in self.patches:
            p.start()

    async def asyncTearDown(self) -> None:
        for p in self.patches:
            p.stop()

    async def test_still_follows_the_tvdb_id_the_caller_supplies(self) -> None:
        media = _episode(852692, season_number=1, episode_number=1)
        media.id = 77

        await sync_router.batch_enrich_items(
            [(media, SHOW_TMDB)],
            api_key="k",
            tvdb_api_key="tk",
            show_tvdb_ids={SHOW_TMDB: 81797},
            tvdb_episode_ids={77: 900001},
        )

        self.assertIn("still-a.jpg", media.tvdb_data["poster_path"])
        # TMDB artwork is untouched — the row is shared between viewers.
        self.assertEqual(media.title, "Episode 1")

    async def test_row_without_a_tvdb_id_gets_no_still_rather_than_a_guess(self) -> None:
        media = _episode(852692, season_number=1, episode_number=1)
        media.id = 78

        await sync_router.batch_enrich_items(
            [(media, SHOW_TMDB)],
            api_key="k",
            tvdb_api_key="tk",
            show_tvdb_ids={SHOW_TMDB: 81797},
            tvdb_episode_ids={},
        )

        self.assertIsNone(media.tvdb_data)


class RenumberedShowReportingTests(unittest.IsolatedAsyncioTestCase):
    """The sync builds an episode-order mapping for the shows it had to
    renumber, so browsing in TVDB order is right from the first pull rather
    than on first view."""

    async def asyncSetUp(self) -> None:
        self.patches = [
            patch.object(sync_router.tmdb, "get_season", _fake_get_season),
            patch("core.episode_order.tmdb.get_season", _fake_get_season),
            patch("core.episode_order.tmdb.get_show_light", _fake_get_show_light),
        ]
        for p in self.patches:
            p.start()

    async def asyncTearDown(self) -> None:
        for p in self.patches:
            p.stop()

    async def test_a_moved_episode_reports_its_show(self) -> None:
        moved = _episode(1152978, season_number=2, episode_number=1)
        renumbered: set[int] = set()

        await sync_router.batch_enrich_items(
            [(moved, SHOW_TMDB)], api_key="k", renumbered_shows=renumbered
        )

        self.assertEqual(renumbered, {SHOW_TMDB})

    async def test_a_show_already_in_the_right_order_reports_nothing(self) -> None:
        settled = _episode(852692, season_number=1, episode_number=1)
        renumbered: set[int] = set()

        await sync_router.batch_enrich_items(
            [(settled, SHOW_TMDB)], api_key="k", renumbered_shows=renumbered
        )

        self.assertEqual(renumbered, set())


class ScrobbleAndTrackerNumberingTests(unittest.IsolatedAsyncioTestCase):
    """A webhook, a Trakt import or a scrobble creates an episode row from the
    numbers its source uses. When the row carries a TMDB episode id, that id
    decides where it belongs — otherwise a play lands on the wrong episode."""

    async def asyncSetUp(self) -> None:
        async def fake_get_episode(series_tmdb_id, season_number, episode_number, api_key=None, **kw):
            for sn, eps in TMDB_SEASONS.items():
                if sn == season_number:
                    for ep in eps:
                        if ep["episode_number"] == episode_number:
                            return ep
            raise RuntimeError("404 episode not found")

        from core import enrichment

        self.patches = [
            patch.object(enrichment.tmdb, "get_episode", fake_get_episode),
            patch("core.episode_order.tmdb.get_season", _fake_get_season),
            patch("core.episode_order.tmdb.get_show_light", _fake_get_show_light),
        ]
        for p in self.patches:
            p.start()

    async def asyncTearDown(self) -> None:
        for p in self.patches:
            p.stop()

    async def test_scrobbled_episode_is_refiled_onto_tmdb_numbering(self) -> None:
        from core import enrichment

        # The server calls this s2e1; TMDB calls the same episode id s1e9.
        media = _episode(1152978, season_number=2, episode_number=1)

        await enrichment.enrich_media(media, api_key="k", series_tmdb_id=SHOW_TMDB)

        self.assertEqual((media.season_number, media.episode_number), (1, 9))
        self.assertEqual(media.tmdb_id, 1152978)
        self.assertEqual(media.title, "Episode 9")

    async def test_episode_already_at_the_tmdb_position_is_untouched(self) -> None:
        from core import enrichment

        media = _episode(852692, season_number=1, episode_number=1)

        await enrichment.enrich_media(media, api_key="k", series_tmdb_id=SHOW_TMDB)

        self.assertEqual((media.season_number, media.episode_number), (1, 1))
        self.assertEqual(media.title, "Episode 1")


def test_mapping_build_does_not_share_the_sync_session():
    """Building a mapping can fail on data the catalogues disagree about. On the
    sync's own session that failure poisoned the transaction and took the whole
    library sync down with it."""
    import inspect

    from routers import sync as sync_router

    source = inspect.getsource(sync_router.sync_items)
    start = source.index("Could not map episode order")
    block = source[max(0, start - 1600):start]
    assert "mapping_session" in block
    assert "ensure_episode_order_mapping(map_db" in block
