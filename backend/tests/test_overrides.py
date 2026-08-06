import asyncio
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from models.domain.base import DomainBase
from models.domain.show import Show
from models.domain.user_override import UserOverride
from services.localization_service import LocalizationService


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


def test_sql_coalesce_user_override(db_session: Session):
    show = Show(
        canonical_title="Original Show Title",
        overview="Original Overview",
        poster_path="/original_poster.jpg",
    )
    db_session.add(show)
    db_session.commit()

    # User 1 has no overrides -> default title/overview returned
    user1_details = asyncio.run(LocalizationService.get_localized_show_details(db_session, user_id=1, show_id=show.id))
    assert user1_details["title"] == "Original Show Title"
    assert user1_details["overview"] == "Original Overview"
    assert user1_details["poster_path"] == "/original_poster.jpg"

    # User 2 sets a custom title override
    asyncio.run(
        LocalizationService.set_user_override(
            db_session,
            user_id=2,
            asset_type="show",
            asset_id=show.id,
            custom_title="User 2 Custom Title",
        )
    )
    db_session.commit()

    # User 2 gets custom title via SQL COALESCE projection
    user2_details = asyncio.run(LocalizationService.get_localized_show_details(db_session, user_id=2, show_id=show.id))
    assert user2_details["title"] == "User 2 Custom Title"
    assert user2_details["overview"] == "Original Overview"  # COALESCE falls back to show.overview

    # User 1 still gets default title
    user1_details_after = asyncio.run(LocalizationService.get_localized_show_details(db_session, user_id=1, show_id=show.id))
    assert user1_details_after["title"] == "Original Show Title"
