import asyncio
import pytest
from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, sessionmaker

from models.domain.base import DomainBase
from models.domain.show import Show
from models.domain.episode import Episode
from models.domain.external_id import ExternalID
from services.external_id_registry import ExternalIDRegistryService


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


def test_register_and_resolve_external_id(db_session: Session):
    show = Show(canonical_title="Breaking Bad")
    db_session.add(show)
    db_session.commit()

    # Register TMDB, TVDB, and IMDB external IDs for the show
    asyncio.run(ExternalIDRegistryService.register_external_id(
        db_session, asset_type="show", asset_id=show.id, provider="tmdb", external_id="1396"
    ))
    asyncio.run(ExternalIDRegistryService.register_external_id(
        db_session, asset_type="show", asset_id=show.id, provider="tvdb", external_id="81189"
    ))
    asyncio.run(ExternalIDRegistryService.register_external_id(
        db_session, asset_type="show", asset_id=show.id, provider="imdb", external_id="tt0959621"
    ))
    db_session.commit()

    # Resolve asset_id via each provider
    tmdb_resolved = asyncio.run(ExternalIDRegistryService.resolve_asset_id(
        db_session, provider="tmdb", external_id="1396", asset_type="show"
    ))
    tvdb_resolved = asyncio.run(ExternalIDRegistryService.resolve_asset_id(
        db_session, provider="tvdb", external_id="81189", asset_type="show"
    ))
    imdb_resolved = asyncio.run(ExternalIDRegistryService.resolve_asset_id(
        db_session, provider="imdb", external_id="tt0959621", asset_type="show"
    ))

    assert tmdb_resolved == show.id
    assert tvdb_resolved == show.id
    assert imdb_resolved == show.id


def test_tvdb_position_mapping(db_session: Session):
    show = Show(canonical_title="Futurama")
    db_session.add(show)
    db_session.commit()

    ep = Episode(
        show_id=show.id,
        season_number=1,
        episode_number=1,
        canonical_title="Space Pilot 3000",
    )
    db_session.add(ep)
    db_session.commit()

    # Register TVDB position mapping (e.g. TVDB S01E01 mapped to canonical ep.id)
    asyncio.run(ExternalIDRegistryService.register_external_id(
        db_session,
        asset_type="episode",
        asset_id=ep.id,
        provider="tvdb",
        external_id="172821",
        provider_season=1,
        provider_episode=1,
    ))
    db_session.commit()

    mapped_ep_id = asyncio.run(ExternalIDRegistryService.get_tvdb_position_mapping(
        db_session, show_id=ep.id, tvdb_season=1, tvdb_ep=1
    ))
    assert mapped_ep_id == ep.id


def test_get_all_external_ids_for_asset(db_session: Session):
    show = Show(canonical_title="The Office")
    db_session.add(show)
    db_session.commit()

    asyncio.run(ExternalIDRegistryService.register_external_id(
        db_session, asset_type="show", asset_id=show.id, provider="tmdb", external_id="2316"
    ))
    asyncio.run(ExternalIDRegistryService.register_external_id(
        db_session, asset_type="show", asset_id=show.id, provider="trakt", external_id="1234"
    ))
    db_session.commit()

    ids = asyncio.run(ExternalIDRegistryService.get_external_ids_for_asset(
        db_session, asset_type="show", asset_id=show.id
    ))

    assert ids == {"tmdb": "2316", "trakt": "1234"}


def test_duplicate_external_id_rejection(db_session: Session):
    asyncio.run(ExternalIDRegistryService.register_external_id(
        db_session, asset_type="show", asset_id=1, provider="tmdb", external_id="500"
    ))
    db_session.commit()

    with pytest.raises(IntegrityError):
        asyncio.run(ExternalIDRegistryService.register_external_id(
            db_session, asset_type="show", asset_id=2, provider="tmdb", external_id="500"
        ))
