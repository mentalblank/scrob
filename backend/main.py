import asyncio
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from contextlib import asynccontextmanager
from sqlalchemy.ext.asyncio import AsyncSession
from db import engine, Base
import models # noqa: F401
from routers import webhooks, media, history, ratings, sync, shows, auth, lists, oidc, profile, trakt, comments, admin

from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.middleware import SlowAPIMiddleware
from core.limiter import limiter

from sqlalchemy import select, update
from models.sync import SyncJob, SyncStatus
from models.base import CollectionSource


async def _auto_sync_scheduler():
    from db import async_sessionmaker
    from models.connections import MediaServerConnection
    from models.users import UserSettings
    from routers.sync import run_jellyfin_sync, run_emby_sync, run_plex_sync
    from routers.trakt import run_trakt_sync
    from datetime import datetime, timezone

    CHECK_INTERVAL = 300  # seconds between scheduler ticks

    ms_source_map = {"jellyfin": CollectionSource.jellyfin, "emby": CollectionSource.emby, "plex": CollectionSource.plex}
    ms_runner_map = {"jellyfin": run_jellyfin_sync, "emby": run_emby_sync, "plex": run_plex_sync}

    while True:
        await asyncio.sleep(CHECK_INTERVAL)
        try:
            async_session = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
            async with async_session() as db:
                now = datetime.utcnow()

                # ── Media Server Connections ─────────────────────────────────
                res = await db.execute(
                    select(MediaServerConnection).where(
                        (MediaServerConnection.auto_sync_interval.isnot(None)) |
                        (MediaServerConnection.partial_sync_interval.isnot(None))
                    )
                )
                connections = res.scalars().all()

                for conn in connections:
                    user_id = conn.user_id
                    source = ms_source_map.get(conn.type)
                    run_fn = ms_runner_map.get(conn.type)
                    if not source or not run_fn: continue

                    # Check for active job
                    active_q = await db.execute(select(SyncJob).where(
                        SyncJob.user_id == user_id, SyncJob.source == source,
                        SyncJob.status.in_([SyncStatus.pending, SyncStatus.running])
                    ))
                    if active_q.scalar_one_or_none(): continue

                    do_full = False
                    do_partial = False

                    if conn.auto_sync_interval:
                        last = conn.last_full_sync
                        if not last or (now - last).total_seconds() / 3600 >= conn.auto_sync_interval:
                            do_full = True
                    
                    if not do_full and conn.partial_sync_interval:
                        last = conn.last_partial_sync or conn.last_full_sync
                        if not last or (now - last).total_seconds() / 3600 >= conn.partial_sync_interval:
                            do_partial = True

                    if do_full or do_partial:
                        job = SyncJob(user_id=user_id, source=source, status=SyncStatus.pending)
                        db.add(job)
                        await db.commit()
                        print(f"Auto-sync: queuing {'full' if do_full else 'partial'} {conn.type} sync for user {user_id} (job {job.id})")
                        asyncio.create_task(run_fn(user_id, job.id, 0, 0, conn.id, partial=do_partial))

                # ── Trakt Sync ───────────────────────────────────────────────
                res = await db.execute(
                    select(UserSettings).where(
                        (UserSettings.trakt_full_sync_interval.isnot(None)) |
                        (UserSettings.trakt_partial_sync_interval.isnot(None))
                    )
                )
                settings_list = res.scalars().all()

                for settings in settings_list:
                    if not settings.trakt_access_token: continue
                    user_id = settings.user_id
                    
                    # Check for active Trakt job
                    active_q = await db.execute(select(SyncJob).where(
                        SyncJob.user_id == user_id, SyncJob.source == CollectionSource.trakt,
                        SyncJob.status.in_([SyncStatus.pending, SyncStatus.running])
                    ))
                    if active_q.scalar_one_or_none(): continue

                    do_full = False
                    do_partial = False

                    if settings.trakt_full_sync_interval:
                        last = settings.last_trakt_full_sync
                        if not last or (now - last).total_seconds() / 3600 >= settings.trakt_full_sync_interval:
                            do_full = True
                    
                    if not do_full and settings.trakt_partial_sync_interval:
                        last = settings.last_trakt_partial_sync or settings.last_trakt_full_sync
                        if not last or (now - last).total_seconds() / 3600 >= settings.trakt_partial_sync_interval:
                            do_partial = True

                    if do_full or do_partial:
                        job = SyncJob(user_id=user_id, source=CollectionSource.trakt, status=SyncStatus.pending)
                        db.add(job)
                        await db.commit()
                        print(f"Auto-sync: queuing {'full' if do_full else 'partial'} Trakt sync for user {user_id} (job {job.id})")
                        asyncio.create_task(run_trakt_sync(user_id, job.id, partial=do_partial))

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
app.include_router(admin.router, prefix="/admin", tags=["admin"])

@app.get("/health")
async def health():
    return {"status": "ok", "app": "Scrob"}