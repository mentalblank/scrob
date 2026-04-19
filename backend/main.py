import asyncio
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from contextlib import asynccontextmanager
from sqlalchemy.ext.asyncio import AsyncSession
from db import engine, Base
import models # noqa: F401
from routers import webhooks, media, history, ratings, sync, shows, auth, lists, oidc, profile, trakt, comments

from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.middleware import SlowAPIMiddleware
from core.limiter import limiter

from sqlalchemy import select, update, or_
from models.sync import SyncJob, SyncStatus
from models.base import CollectionSource


async def _auto_sync_scheduler():
    from db import async_sessionmaker
    from models.users import UserSettings
    from routers.sync import run_jellyfin_sync, run_emby_sync, run_plex_sync
    from datetime import datetime, timezone

    CHECK_INTERVAL = 300  # seconds between scheduler ticks

    while True:
        await asyncio.sleep(CHECK_INTERVAL)
        try:
            async_session = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
            async with async_session() as db:
                result = await db.execute(
                    select(UserSettings).where(
                        or_(
                            UserSettings.jellyfin_auto_sync_interval.isnot(None),
                            UserSettings.emby_auto_sync_interval.isnot(None),
                            UserSettings.plex_auto_sync_interval.isnot(None),
                        )
                    )
                )
                all_settings = result.scalars().all()

                now = datetime.now(timezone.utc).replace(tzinfo=None)

                for user_settings in all_settings:
                    user_id = user_settings.user_id
                    sources = [
                        (CollectionSource.jellyfin, user_settings.jellyfin_auto_sync_interval, run_jellyfin_sync),
                        (CollectionSource.emby,     user_settings.emby_auto_sync_interval,     run_emby_sync),
                        (CollectionSource.plex,     user_settings.plex_auto_sync_interval,     run_plex_sync),
                    ]
                    for source, interval, run_fn in sources:
                        if not interval:
                            continue

                        # Skip if a sync is already pending or running for this user+source
                        active_q = await db.execute(
                            select(SyncJob).where(
                                SyncJob.user_id == user_id,
                                SyncJob.source == source,
                                SyncJob.status.in_([SyncStatus.pending, SyncStatus.running]),
                            )
                        )
                        if active_q.scalar_one_or_none():
                            continue

                        # Find the last completed or failed sync for this user+source
                        last_q = await db.execute(
                            select(SyncJob).where(
                                SyncJob.user_id == user_id,
                                SyncJob.source == source,
                                SyncJob.status.in_([SyncStatus.completed, SyncStatus.failed]),
                            ).order_by(SyncJob.updated_at.desc()).limit(1)
                        )
                        last_job = last_q.scalar_one_or_none()

                        if last_job:
                            elapsed_hours = (now - last_job.updated_at).total_seconds() / 3600
                            if elapsed_hours < interval:
                                continue

                        job = SyncJob(user_id=user_id, source=source, status=SyncStatus.pending)
                        db.add(job)
                        await db.flush()
                        job_id = job.id
                        await db.commit()

                        print(f"Auto-sync: queuing {source.value} sync for user {user_id} (job {job_id})")
                        asyncio.create_task(run_fn(user_id, job_id, 0, 0))

        except Exception as e:
            print(f"Auto-sync scheduler error: {e}")
            import traceback
            traceback.print_exc()


@asynccontextmanager
async def lifespan(app: FastAPI):
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    # Clean up stuck sync jobs on startup
    from db import async_sessionmaker
    async_session = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
    async with async_session() as db:
        await db.execute(
            update(SyncJob)
            .where(SyncJob.status.in_([SyncStatus.pending, SyncStatus.running]))
            .values(status=SyncStatus.failed, error_message="Aborted due to server restart")
        )
        await db.commit()

    scheduler_task = asyncio.create_task(_auto_sync_scheduler())

    yield

    scheduler_task.cancel()
    try:
        await scheduler_task
    except asyncio.CancelledError:
        pass

from core.config import settings

# Rate limiter — keyed by client IP, in-memory storage (suitable for single-instance deploy).
app = FastAPI(title="Scrob", version="0.1.0", lifespan=lifespan)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
app.add_middleware(SlowAPIMiddleware)

# The backend is internal-only (localhost), but lock CORS to the configured
# frontend origin as defence-in-depth. The backend uses Bearer token auth only
# (no cookies), so allow_credentials is not needed.
app.add_middleware(
    CORSMiddleware,
    allow_origins=[settings.server_url],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(auth.router, prefix="/auth", tags=["auth"])
app.include_router(oidc.router, prefix="/auth/oidc", tags=["oidc"])
app.include_router(webhooks.router, prefix="/webhooks", tags=["webhooks"])
app.include_router(media.router, prefix="/media", tags=["media"])
app.include_router(history.router, prefix="/history", tags=["history"])
app.include_router(ratings.router, prefix="/ratings", tags=["ratings"])
app.include_router(sync.router, prefix="/sync", tags=["sync"])
app.include_router(shows.router, prefix="/shows", tags=["shows"])
app.include_router(lists.router, prefix="/lists", tags=["lists"])
app.include_router(profile.router, prefix="/profile", tags=["profile"])
app.include_router(trakt.router, prefix="/trakt", tags=["trakt"])
app.include_router(comments.router, prefix="/comments", tags=["comments"])

@app.get("/health")
async def health():
    return {"status": "ok", "app": "Scrob"}