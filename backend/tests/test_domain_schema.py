import pytest
from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, sessionmaker

from models.domain.base import DomainBase
from models.domain.show import Show
from models.domain.season import Season
from models.domain.episode import Episode


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


def test_create_canonical_show_season_episode(db_session: Session):
    show = Show(
        canonical_title="Breaking Bad",
        original_title="Breaking Bad",
        first_air_date="2008-01-20",
        status="Ended",
        overview="A high school chemistry teacher diagnosed with inoperable lung cancer...",
    )
    db_session.add(show)
    db_session.commit()

    assert show.id is not None

    season = Season(
        show_id=show.id,
        season_number=1,
        canonical_title="Season 1",
        overview="First season of Breaking Bad",
        episode_count=7,
    )
    db_session.add(season)
    db_session.commit()

    assert season.id is not None

    episode = Episode(
        show_id=show.id,
        season_id=season.id,
        season_number=1,
        episode_number=1,
        canonical_title="Pilot",
        release_date="2008-01-20",
        overview="Walter White turns 50 and learns he has cancer...",
        runtime=58,
    )
    db_session.add(episode)
    db_session.commit()

    assert episode.id is not None
    assert episode.show.canonical_title == "Breaking Bad"
    assert episode.season.canonical_title == "Season 1"
    assert len(show.episodes) == 1
    assert show.episodes[0].canonical_title == "Pilot"


def test_unique_constraint_episodes(db_session: Session):
    show = Show(canonical_title="Better Call Saul")
    db_session.add(show)
    db_session.commit()

    ep1 = Episode(
        show_id=show.id,
        season_number=1,
        episode_number=1,
        canonical_title="Uno",
    )
    db_session.add(ep1)
    db_session.commit()

    ep2 = Episode(
        show_id=show.id,
        season_number=1,
        episode_number=1,
        canonical_title="Duplicate Uno",
    )
    db_session.add(ep2)

    with pytest.raises(IntegrityError):
        db_session.commit()


def test_unique_constraint_seasons(db_session: Session):
    show = Show(canonical_title="The Wire")
    db_session.add(show)
    db_session.commit()

    s1_a = Season(show_id=show.id, season_number=1, canonical_title="Season 1")
    db_session.add(s1_a)
    db_session.commit()

    s1_b = Season(show_id=show.id, season_number=1, canonical_title="Duplicate Season 1")
    db_session.add(s1_b)

    with pytest.raises(IntegrityError):
        db_session.commit()


def test_cascade_delete_show(db_session: Session):
    show = Show(canonical_title="Fargo")
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
        canonical_title="The Crocodile's Dilemma",
    )
    db_session.add(episode)
    db_session.commit()

    db_session.delete(show)
    db_session.commit()

    assert db_session.query(Show).count() == 0
    assert db_session.query(Season).count() == 0
    assert db_session.query(Episode).count() == 0
