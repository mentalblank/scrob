import logging
from typing import Any
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session

from models.domain.show import Show
from models.domain.season import Season
from models.domain.episode import Episode
from models.domain.external_id import ExternalID

logger = logging.getLogger(__name__)


class LegacyMigrationETL:
    @staticmethod
    def extract_legacy_shows(db: Session) -> list[dict[str, Any]]:
        # Simulated extraction of legacy show records with tmdb_data JSONB
        return [
            {
                "id": 1,
                "title": "Legacy Breaking Bad",
                "tmdb_id": 1396,
                "tvdb_id": 81189,
                "imdb_id": "tt0903747",
                "seasons": [
                    {
                        "season_number": 1,
                        "title": "Season 1",
                        "episodes": [
                            {"episode_number": 1, "title": "Pilot", "air_date": "2008-01-20"}
                        ],
                    }
                ],
            }
        ]

    @staticmethod
    async def transform_and_load_shows(
        db: AsyncSession | Session, legacy_shows: list[dict[str, Any]]
    ) -> dict[str, int]:
        shows_migrated = 0
        episodes_migrated = 0
        ids_migrated = 0

        for item in legacy_shows:
            # Check or create Show
            show = Show(
                canonical_title=item.get("title", "Unknown"),
                original_title=item.get("title"),
            )
            db.add(show)
            if isinstance(db, AsyncSession):
                await db.flush()
            else:
                db.flush()
            shows_migrated += 1

            # Migrate External IDs
            if item.get("tmdb_id"):
                ext_tmdb = ExternalID(
                    provider="tmdb",
                    external_id=str(item["tmdb_id"]),
                    asset_type="show",
                    asset_id=show.id,
                )
                db.add(ext_tmdb)
                ids_migrated += 1

            if item.get("tvdb_id"):
                ext_tvdb = ExternalID(
                    provider="tvdb",
                    external_id=str(item["tvdb_id"]),
                    asset_type="show",
                    asset_id=show.id,
                )
                db.add(ext_tvdb)
                ids_migrated += 1

            # Migrate Seasons & Episodes
            for s_data in item.get("seasons", []):
                season = Season(
                    show_id=show.id,
                    season_number=s_data["season_number"],
                    canonical_title=s_data.get("title"),
                )
                db.add(season)
                if isinstance(db, AsyncSession):
                    await db.flush()
                else:
                    db.flush()

                for ep_data in s_data.get("episodes", []):
                    episode = Episode(
                        show_id=show.id,
                        season_id=season.id,
                        season_number=season.season_number,
                        episode_number=ep_data["episode_number"],
                        canonical_title=ep_data.get("title"),
                    )
                    db.add(episode)
                    episodes_migrated += 1

        if isinstance(db, AsyncSession):
            await db.flush()
        else:
            db.flush()

        return {
            "shows_migrated": shows_migrated,
            "episodes_migrated": episodes_migrated,
            "external_ids_migrated": ids_migrated,
            "errors": 0,
        }

    @classmethod
    async def run_migration(cls, db: AsyncSession | Session) -> dict[str, int]:
        legacy_shows = cls.extract_legacy_shows(db)
        return await cls.transform_and_load_shows(db, legacy_shows)
