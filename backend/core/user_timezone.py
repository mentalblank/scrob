"""Per-user display timezone.

Every timestamp column in this app is naive UTC (see db.py) and stays that
way - this module only decides which zone those instants are *rendered* and
*bucketed* into. Resolution order is the user's own setting, then whatever
zone the caller forwarded (the frontend sends the browser's), then the
server's own TZ. A user who never picks one still gets sensible dates, and
one user's pick never moves anyone else's clock.
"""

from datetime import date, datetime
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError, available_timezones

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from core.config import settings
from models.users import UserSettings

UTC = ZoneInfo("UTC")


def is_valid(name: str | None) -> bool:
    if not name:
        return False
    try:
        ZoneInfo(name)
    except (ZoneInfoNotFoundError, KeyError, ValueError):
        return False
    return True


def known_timezones() -> list[str]:
    return sorted(available_timezones())


def server_tz() -> ZoneInfo:
    try:
        return ZoneInfo(settings.tz)
    except (ZoneInfoNotFoundError, KeyError, ValueError):
        return UTC


def resolve(*candidates: str | None) -> ZoneInfo:
    """First usable zone among the candidates, else the server's own."""
    for name in candidates:
        if is_valid(name):
            return ZoneInfo(name)
    return server_tz()


async def get_user_tz(db: AsyncSession, user_id: int, fallback: str | None = None) -> ZoneInfo:
    result = await db.execute(
        select(UserSettings.timezone).where(UserSettings.user_id == user_id)
    )
    return resolve(result.scalar_one_or_none(), fallback)


async def user_today(db: AsyncSession, user_id: int, fallback: str | None = None) -> date:
    """The user's current calendar date - the boundary "has this aired yet",
    "what's on today" and day-grouped history all have to agree on."""
    return datetime.now(await get_user_tz(db, user_id, fallback)).date()
