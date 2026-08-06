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
    Rating,
    Collection,
    List,
    ListItem,
)
from services.media_service import MediaService
from services.history_service import HistoryService
from services.rating_service import RatingService
from services.collection_service import CollectionService
from services.list_service import ListService


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


def test_media_service(db_session: Session):
    show = Show(canonical_title="The Wire", overview="Baltimore drug scene...")
    db_session.add(show)
    db_session.commit()

    season = Season(show_id=show.id, season_number=1, canonical_title="Season 1")
    db_session.add(season)
    db_session.commit()

    episode = Episode(show_id=show.id, season_id=season.id, season_number=1, episode_number=1, canonical_title="The Target")
    db_session.add(episode)
    db_session.commit()

    show_details = asyncio.run(MediaService.get_show_details(db_session, show.id))
    assert show_details is not None
    assert show_details["canonical_title"] == "The Wire"

    season_details = asyncio.run(MediaService.get_season_details(db_session, show.id, 1))
    assert season_details is not None
    assert season_details["canonical_title"] == "Season 1"

    ep_details = asyncio.run(MediaService.get_episode_details(db_session, show.id, 1, 1))
    assert ep_details is not None
    assert ep_details["canonical_title"] == "The Target"


def test_history_service(db_session: Session):
    user = User(username="alice", email="alice@example.com", api_key="key_alice")
    db_session.add(user)
    db_session.commit()

    event = asyncio.run(HistoryService.record_watch_event(db_session, user_id=user.id, asset_type="episode", asset_id=101))
    db_session.commit()

    assert event.id is not None
    feed = asyncio.run(HistoryService.get_user_history_feed(db_session, user_id=user.id))
    assert len(feed) == 1
    assert feed[0].asset_id == 101

    deleted = asyncio.run(HistoryService.delete_watch_event(db_session, user_id=user.id, event_id=event.id))
    db_session.commit()
    assert deleted is True


def test_rating_service(db_session: Session):
    user = User(username="bob", email="bob@example.com", api_key="key_bob")
    db_session.add(user)
    db_session.commit()

    rating = asyncio.run(RatingService.set_rating(db_session, user_id=user.id, asset_type="show", asset_id=200, rating_value=9))
    db_session.commit()

    assert rating.rating == 9
    ratings = asyncio.run(RatingService.get_user_ratings(db_session, user_id=user.id))
    assert len(ratings) == 1

    with pytest.raises(ValueError):
        asyncio.run(RatingService.set_rating(db_session, user_id=user.id, asset_type="show", asset_id=200, rating_value=15))


def test_collection_and_list_service(db_session: Session):
    user = User(username="charlie", email="charlie@example.com", api_key="key_charlie")
    db_session.add(user)
    db_session.commit()

    coll_item = asyncio.run(CollectionService.add_to_collection(db_session, user_id=user.id, media_id=300))
    db_session.commit()
    assert coll_item.id is not None

    custom_list = asyncio.run(ListService.create_custom_list(db_session, user_id=user.id, name="Favorites"))
    db_session.commit()
    assert custom_list.id is not None

    list_item = asyncio.run(ListService.add_item_to_list(db_session, list_id=custom_list.id, media_id=300))
    db_session.commit()
    assert list_item.id is not None
