import asyncio
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from models.domain.base import DomainBase
from models.domain.show import Show
from models.domain.season import Season
from models.domain.episode import Episode
from services.external_id_registry import ExternalIDRegistryService
from services.asset_resolver import AssetResolver


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


def test_resolve_show_by_id_and_external_ids(db_session: Session):
    show = Show(canonical_title="Breaking Bad", original_title="Breaking Bad")
    db_session.add(show)
    db_session.commit()

    asyncio.run(ExternalIDRegistryService.register_external_id(
        db_session, asset_type="show", asset_id=show.id, provider="tmdb", external_id="1396"
    ))
    asyncio.run(ExternalIDRegistryService.register_external_id(
        db_session, asset_type="show", asset_id=show.id, provider="tvdb", external_id="81189"
    ))
    db_session.commit()

    # Resolve by primary key
    resolved_pk = asyncio.run(AssetResolver.resolve_show(db_session, show_ref=show.id))
    assert resolved_pk is not None
    assert resolved_pk.id == show.id

    # Resolve by TMDB ID
    resolved_tmdb = asyncio.run(AssetResolver.resolve_show(db_session, show_ref="1396", provider="tmdb"))
    assert resolved_tmdb is not None
    assert resolved_tmdb.id == show.id

    # Resolve by TVDB ID
    resolved_tvdb = asyncio.run(AssetResolver.resolve_show(db_session, show_ref="81189", provider="tvdb"))
    assert resolved_tvdb is not None
    assert resolved_tvdb.id == show.id


def test_resolve_show_by_fuzzy_title(db_session: Session):
    show = Show(canonical_title="Game of Thrones")
    db_session.add(show)
    db_session.commit()

    resolved = asyncio.run(AssetResolver.resolve_show(
        db_session, show_ref="unmapped_id", provider="tmdb", title="Game of Thrones"
    ))
    assert resolved is not None
    assert resolved.id == show.id


def test_resolve_episode_exact_and_external_id(db_session: Session):
    show = Show(canonical_title="Stranger Things")
    db_session.add(show)
    db_session.commit()

    season = Season(show_id=show.id, season_number=1, canonical_title="Season 1")
    db_session.add(season)
    db_session.commit()

    episode = Episode(
        show_id=show.id,
        season_id=season.id,
        season_number=1,
        episode_number=1,
        canonical_title="Chapter One: The Vanishing of Will Byers",
        release_date="2016-07-15",
    )
    db_session.add(episode)
    db_session.commit()

    # Register TMDB episode external ID
    asyncio.run(ExternalIDRegistryService.register_external_id(
        db_session, asset_type="episode", asset_id=episode.id, provider="tmdb", external_id="1198305"
    ))
    # Register show TMDB ID
    asyncio.run(ExternalIDRegistryService.register_external_id(
        db_session, asset_type="show", asset_id=show.id, provider="tmdb", external_id="66732"
    ))
    db_session.commit()

    # Resolve by external_ep_id
    resolved_ext = asyncio.run(AssetResolver.resolve_episode(
        db_session, show_ref="66732", season=1, episode=1, provider="tmdb", external_ep_id="1198305"
    ))
    assert resolved_ext is not None
    assert resolved_ext.id == episode.id

    # Resolve by canonical season + episode
    resolved_canonical = asyncio.run(AssetResolver.resolve_episode(
        db_session, show_ref=show.id, season=1, episode=1
    ))
    assert resolved_canonical is not None
    assert resolved_canonical.id == episode.id


def test_resolve_episode_tvdb_position_mapping(db_session: Session):
    show = Show(canonical_title="Money Heist")
    db_session.add(show)
    db_session.commit()

    # Canonical TMDB S01E01
    episode = Episode(
        show_id=show.id,
        season_number=1,
        episode_number=1,
        canonical_title="Efectuar lo acordado",
    )
    db_session.add(episode)
    db_session.commit()

    # Register TVDB position mapping (TVDB S01E01 mapped to canonical episode.id)
    asyncio.run(ExternalIDRegistryService.register_external_id(
        db_session,
        asset_type="episode",
        asset_id=episode.id,
        provider="tvdb",
        external_id="6115383",
        provider_season=1,
        provider_episode=1,
    ))
    asyncio.run(ExternalIDRegistryService.register_external_id(
        db_session, asset_type="show", asset_id=show.id, provider="tvdb", external_id="326943"
    ))
    db_session.commit()

    resolved_tvdb = asyncio.run(AssetResolver.resolve_episode(
        db_session, show_ref="326943", season=1, episode=1, provider="tvdb"
    ))
    assert resolved_tvdb is not None
    assert resolved_tvdb.id == episode.id


def test_resolve_episode_fuzzy_title(db_session: Session):
    show = Show(canonical_title="The Mandalorian")
    db_session.add(show)
    db_session.commit()

    episode = Episode(
        show_id=show.id,
        season_number=1,
        episode_number=1,
        canonical_title="Chapter 1: The Mandalorian",
    )
    db_session.add(episode)
    db_session.commit()

    resolved = asyncio.run(AssetResolver.resolve_episode(
        db_session,
        show_ref=show.id,
        season=99,
        episode=99,
        episode_title="Chapter 1: The Mandalorian",
    ))
    assert resolved is not None
    assert resolved.id == episode.id
