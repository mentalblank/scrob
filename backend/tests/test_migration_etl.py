import asyncio
import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, sessionmaker

from models.domain.base import DomainBase
from models.domain.show import Show
from models.domain.season import Season
from models.domain.episode import Episode
from models.domain.external_id import ExternalID
from scripts.migrate_legacy_data import LegacyMigrationETL


@pytest.fixture
def db_session():
    engine = create_engine("sqlite:///:memory:", echo=False)
    DomainBase.metadata.create_all(engine)
    SessionLocal = sessionmaker(bind=engine)
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()


def test_legacy_migration_etl(db_session: Session):
    res = asyncio.run(LegacyMigrationETL.run_migration(db_session))
    db_session.commit()

    assert res["shows_migrated"] == 1
    assert res["episodes_migrated"] == 1
    assert res["external_ids_migrated"] == 2
    assert res["errors"] == 0

    shows = db_session.execute(select(Show)).scalars().all()
    assert len(shows) == 1
    assert shows[0].canonical_title == "Legacy Breaking Bad"

    ext_ids = db_session.execute(select(ExternalID)).scalars().all()
    assert len(ext_ids) == 2
    providers = {e.provider for e in ext_ids}
    assert "tmdb" in providers
    assert "tvdb" in providers
