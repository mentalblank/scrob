import asyncio
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from models.domain import (
    DomainBase,
    Show,
    Season,
    Episode,
    User,
    WatchEvent,
)
from services.cqrs_read_models import CQRSReadModels


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


def test_show_progress_summary_cqrs(db_session: Session):
    user = User(username="alice", email="alice@example.com", api_key="key_alice")
    db_session.add(user)
    db_session.commit()

    show = Show(canonical_title="Breaking Bad")
    db_session.add(show)
    db_session.commit()

    season = Season(show_id=show.id, season_number=1, canonical_title="Season 1")
    db_session.add(season)
    db_session.commit()

    # Add 4 episodes
    episodes = []
    for i in range(1, 5):
        ep = Episode(
            show_id=show.id,
            season_id=season.id,
            season_number=1,
            episode_number=i,
            canonical_title=f"Episode {i}",
        )
        db_session.add(ep)
        episodes.append(ep)
    db_session.commit()

    # Watch 2 episodes
    we1 = WatchEvent(user_id=user.id, asset_type="episode", asset_id=episodes[0].id)
    we2 = WatchEvent(user_id=user.id, asset_type="episode", asset_id=episodes[1].id)
    db_session.add_all([we1, we2])
    db_session.commit()

    summary = asyncio.run(CQRSReadModels.get_show_progress_summary(db_session, user_id=user.id, show_id=show.id))

    assert summary["show_id"] == show.id
    assert summary["total_episodes"] == 4
    assert summary["watched_episodes"] == 2
    assert summary["unwatched_episodes"] == 2
    assert summary["progress_percent"] == 50.0
    assert summary["is_completed"] is False


def test_dashboard_feed_cursor_pagination(db_session: Session):
    user = User(username="bob", email="bob@example.com", api_key="key_bob")
    db_session.add(user)
    db_session.commit()

    # Add 5 watch events
    events = []
    for i in range(1, 6):
        we = WatchEvent(user_id=user.id, asset_type="episode", asset_id=i)
        db_session.add(we)
        events.append(we)
    db_session.commit()

    feed_page1 = asyncio.run(CQRSReadModels.get_dashboard_feed(db_session, user_id=user.id, limit=2))
    assert len(feed_page1["items"]) == 2
    assert feed_page1["has_more"] is True
    assert feed_page1["next_cursor"] is not None

    feed_page2 = asyncio.run(
        CQRSReadModels.get_dashboard_feed(
            db_session, user_id=user.id, limit=2, cursor=feed_page1["next_cursor"]
        )
    )
    assert len(feed_page2["items"]) == 2
    assert feed_page2["has_more"] is True
