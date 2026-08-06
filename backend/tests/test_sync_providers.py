import asyncio
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from models.domain.base import DomainBase
from sync.providers import (
    PlexSyncProvider,
    JellyfinSyncProvider,
    TraktSyncProvider,
    SimklSyncProvider,
    NuvioSyncProvider,
    StremioSyncProvider,
    SyncResult,
    ScrobbleResult,
)


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


def test_plex_sync_provider(db_session: Session):
    provider = PlexSyncProvider()
    assert provider.provider_name == "plex"

    sync_res = asyncio.run(provider.sync_library(db_session, user_id=1))
    assert isinstance(sync_res, SyncResult)
    assert sync_res.provider == "plex"
    assert sync_res.success is True

    scrob_res = asyncio.run(
        provider.process_scrobble(
            db_session, user_id=1, event={"event": "media.scrobble", "ratingKey": "100"}
        )
    )
    assert isinstance(scrob_res, ScrobbleResult)
    assert scrob_res.provider == "plex"
    assert scrob_res.success is True


def test_jellyfin_sync_provider(db_session: Session):
    provider = JellyfinSyncProvider()
    assert provider.provider_name == "jellyfin"

    sync_res = asyncio.run(provider.sync_library(db_session, user_id=1))
    assert sync_res.provider == "jellyfin"

    scrob_res = asyncio.run(
        provider.process_scrobble(
            db_session, user_id=1, event={"NotificationType": "PlaybackStop", "ItemId": "j1"}
        )
    )
    assert scrob_res.provider == "jellyfin"


def test_trakt_sync_provider(db_session: Session):
    provider = TraktSyncProvider()
    assert provider.provider_name == "trakt"

    sync_res = asyncio.run(provider.sync_library(db_session, user_id=1))
    assert sync_res.provider == "trakt"


def test_simkl_sync_provider(db_session: Session):
    provider = SimklSyncProvider()
    assert provider.provider_name == "simkl"

    sync_res = asyncio.run(provider.sync_library(db_session, user_id=1))
    assert sync_res.provider == "simkl"


def test_nuvio_and_stremio_sync_providers(db_session: Session):
    nuvio = NuvioSyncProvider()
    stremio = StremioSyncProvider()

    assert nuvio.provider_name == "nuvio"
    assert stremio.provider_name == "stremio"

    res_nuvio = asyncio.run(nuvio.sync_library(db_session, user_id=1))
    res_stremio = asyncio.run(stremio.sync_library(db_session, user_id=1))

    assert res_nuvio.success is True
    assert res_stremio.success is True
