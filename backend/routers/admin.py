from datetime import datetime

from fastapi import APIRouter, BackgroundTasks, Depends, File, HTTPException, UploadFile, status
from fastapi.responses import Response
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.future import select
from sqlalchemy import delete, func, update, text

from db import get_db, engine
from models.users import User
from models.global_settings import GlobalSettings
from models.media import Media
from models.collection import Collection
from models.show import Show
from models.sync import SyncJob, SyncStatus
from models.base import CollectionSource, MediaType
from models.media_request import MediaRequest, RequestStatus
from models.users import UserSettings
from dependencies import require_admin
from core.url_validator import validate_service_url
from core.backup import pg_dump, pg_restore
import schemas

router = APIRouter()


async def _get_or_create_global_settings(db: AsyncSession) -> GlobalSettings:
    result = await db.execute(select(GlobalSettings).where(GlobalSettings.id == 1))
    gs = result.scalar_one_or_none()
    if not gs:
        gs = GlobalSettings(id=1)
        db.add(gs)
        await db.flush()
    return gs


@router.get("/settings", response_model=schemas.GlobalSettings)
async def get_global_settings(
    db: AsyncSession = Depends(get_db),
    _: User = Depends(require_admin),
):
    return await _get_or_create_global_settings(db)


@router.patch("/settings", response_model=schemas.GlobalSettings)
async def update_global_settings(
    body: schemas.GlobalSettings,
    db: AsyncSession = Depends(get_db),
    _: User = Depends(require_admin),
):
    gs = await _get_or_create_global_settings(db)

    update_data = body.model_dump(exclude_unset=True)

    url_fields = {"radarr_url": "Radarr URL", "sonarr_url": "Sonarr URL"}
    for field, label in url_fields.items():
        if field in update_data and update_data[field]:
            update_data[field] = await validate_service_url(update_data[field], label)

    for field, value in update_data.items():
        if hasattr(gs, field):
            setattr(gs, field, value)

    await db.commit()
    await db.refresh(gs)
    return gs


@router.get("/users", response_model=list[schemas.AdminUser])
async def list_users(
    db: AsyncSession = Depends(get_db),
    _: User = Depends(require_admin),
):
    result = await db.execute(select(User).order_by(User.created_at.asc()))
    return result.scalars().all()


@router.patch("/users/{user_id}/toggle-admin", response_model=schemas.AdminUser)
async def toggle_admin(
    user_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(require_admin),
):
    result = await db.execute(select(User).where(User.id == user_id))
    target = result.scalar_one_or_none()
    if not target:
        raise HTTPException(status_code=404, detail="User not found")

    # Prevent removing your own admin if you're the only one
    if target.id == current_user.id and target.is_admin:
        count_result = await db.execute(
            select(func.count()).select_from(User).where(User.is_admin.is_(True))
        )
        if count_result.scalar_one() <= 1:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="You are the sole admin. Promote another user before removing your own admin rights.",
            )

    target.is_admin = not target.is_admin
    await db.commit()
    await db.refresh(target)
    return target


@router.delete("/users/{user_id}")
async def delete_user(
    user_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(require_admin),
):
    if user_id == current_user.id:
        count_result = await db.execute(select(func.count()).select_from(User))
        if count_result.scalar_one() > 1:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="You cannot delete your own account while other users exist.",
            )

    result = await db.execute(select(User).where(User.id == user_id))
    target = result.scalar_one_or_none()
    if not target:
        raise HTTPException(status_code=404, detail="User not found")

    await db.execute(delete(User).where(User.id == user_id))
    await db.commit()
    return {"status": "deleted"}


@router.get("/backup")
async def backup_database(_: User = Depends(require_admin)):
    """Full database backup via pg_dump (schema + data + sequences)."""
    try:
        payload = await pg_dump()
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))

    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    filename = f"scrob_backup_{timestamp}.pgdump"
    return Response(
        content=payload,
        media_type="application/octet-stream",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
            "Content-Length": str(len(payload)),
        },
    )


@router.post("/maintenance/heal")
async def admin_heal_metadata(
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(require_admin),
):
    """Re-enrich all collection items server-wide that are missing poster/date metadata."""
    gs = await _get_or_create_global_settings(db)
    if not gs.tmdb_api_key:
        raise HTTPException(status_code=400, detail="A global TMDB API key is required for server-wide heal")
    job = SyncJob(user_id=current_user.id, source=CollectionSource.tmdb, job_type="heal", status=SyncStatus.pending)
    db.add(job)
    await db.commit()
    await db.refresh(job)
    background_tasks.add_task(run_admin_heal, gs.tmdb_api_key, current_user.id, job.id)
    return {"status": "started", "message": "Server-wide metadata heal is running in the background"}


async def run_admin_heal(api_key: str, user_id: int | None = None, job_id: int | None = None):
    from models.show import Show
    from routers.sync import batch_enrich_items
    async_session = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
    async with async_session() as db:
        async def _update_job(**kwargs):
            if job_id is None:
                return
            await db.execute(update(SyncJob).where(SyncJob.id == job_id).values(updated_at=func.now(), **kwargs))
            await db.commit()

        try:
            await _update_job(status=SyncStatus.running)

            coll_q = await db.execute(
                select(Media)
                .where(
                    Media.poster_path.is_(None),
                    Media.id.in_(select(Collection.media_id).distinct()),
                )
            )
            items = coll_q.scalars().all()

            movies = [m for m in items if m.media_type == MediaType.movie and m.tmdb_id]
            episodes = [m for m in items if m.media_type == MediaType.episode and m.show_id and m.season_number is not None and m.episode_number is not None]

            if not movies and not episodes:
                print("Admin heal: nothing to fix server-wide")
                await _update_job(status=SyncStatus.completed, total_items=0, stats={"healed": True})
                return

            print(f"Admin heal: {len(movies)} movies, {len(episodes)} episodes to re-enrich")

            show_ids = list({m.show_id for m in episodes})
            show_tmdb_map: dict[int, int] = {}
            if show_ids:
                shows_q = await db.execute(select(Show).where(Show.id.in_(show_ids)))
                for s in shows_q.scalars().all():
                    if s.tmdb_id:
                        show_tmdb_map[s.id] = s.tmdb_id

            to_enrich = [(m, None) for m in movies] + [
                (m, show_tmdb_map[m.show_id]) for m in episodes if m.show_id in show_tmdb_map
            ]

            await _update_job(total_items=len(to_enrich), processed_items=0)
            await batch_enrich_items(to_enrich, api_key=api_key)
            await db.commit()
            await _update_job(processed_items=len(to_enrich), status=SyncStatus.completed, stats={"healed": True})
            print(f"Admin heal complete: processed {len(to_enrich)} items")
        except Exception as e:
            print(f"Admin heal failed: {e}")
            import traceback
            traceback.print_exc()
            await _update_job(status=SyncStatus.failed, error_message=str(e)[:900])


@router.post("/restore")
async def restore_database(
    file: UploadFile = File(...),
    db: AsyncSession = Depends(get_db),
    _: User = Depends(require_admin),
):
    """Restore database from a pg_dump custom-format backup file."""
    fname = file.filename or ""
    if not (fname.endswith(".pgdump") or fname.endswith(".bak")):
        raise HTTPException(status_code=400, detail="Only .pgdump backup files are accepted.")

    content = await file.read()
    if not content:
        raise HTTPException(status_code=400, detail="Uploaded file is empty.")

    # Release SQLAlchemy session before pg_restore takes exclusive locks.
    await db.rollback()

    try:
        await pg_restore(content)
    except (ValueError, RuntimeError) as e:
        raise HTTPException(status_code=400, detail=str(e))

    return {"status": "restored"}

@router.delete("/database")
async def clear_database(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(require_admin),
):
    """
    Resets all media, collections, history, lists, and comments globally.
    User settings and admin settings are preserved.
    """
    tables_to_truncate = [
        "shows",
        "media",
        "collections",
        "collection_files",
        "watch_events",
        "ratings",
        "lists",
        "list_items",
        "sync_jobs",
        "playback_sessions",
        "playback_progress",
        "follows",
        "blocklist_items",
        "comments"
    ]
    
    query = text(f"TRUNCATE {', '.join(tables_to_truncate)} CASCADE")
    await db.execute(query)
    await db.commit()
    
    return {"status": "ok", "message": "Database cleared successfully."}


# ── Media requests (approval queue) ──────────────────────────────────────────

@router.get("/requests/pending-count")
async def pending_requests_count(
    db: AsyncSession = Depends(get_db),
    _: User = Depends(require_admin),
):
    result = await db.execute(
        select(func.count()).select_from(MediaRequest).where(MediaRequest.status == RequestStatus.pending)
    )
    return {"pending": result.scalar_one()}


@router.get("/requests")
async def list_requests(
    db: AsyncSession = Depends(get_db),
    _: User = Depends(require_admin),
):
    result = await db.execute(
        select(MediaRequest, User)
        .join(User, User.id == MediaRequest.user_id)
        .order_by(MediaRequest.updated_at.desc())
    )
    rows = result.all()
    return [
        {
            "id":          req.id,
            "uri_id":      req.uri_id,
            "media_type":  req.media_type,
            "title":       req.title,
            "poster_path": req.poster_path,
            "status":      req.status.value,
            "reviewed_by": req.reviewed_by,
            "created_at":  req.created_at,
            "updated_at":  req.updated_at,
            "user": {
                "id":           user.id,
                "username":     user.username,
                "display_name": user.username,
            },
        }
        for req, user in rows
    ]


@router.post("/requests/{request_id}/approve")
async def approve_request(
    request_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(require_admin),
):
    req_q = await db.execute(select(MediaRequest).where(MediaRequest.id == request_id))
    req = req_q.scalar_one_or_none()
    if not req:
        raise HTTPException(status_code=404, detail="Request not found")

    gs = await _get_or_create_global_settings(db)
    settings_q = await db.execute(select(UserSettings).where(UserSettings.user_id == req.user_id))
    settings = settings_q.scalar_one_or_none()

    # Derive provider + numeric ID from uri_id
    from utils.media_uri import MediaURI
    try:
        _uri = MediaURI.parse(req.uri_id)
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Invalid uri_id on request: {req.uri_id}")
    _req_tmdb_id = int(_uri.id) if _uri.provider == "tmdb" else None

    if req.media_type == "movie":
        from routers.media import _effective_radarr
        from core import radarr as radarr_core
        radarr_cfg = _effective_radarr(settings, gs)
        if not radarr_cfg:
            raise HTTPException(status_code=400, detail="Radarr not configured")
        if not _req_tmdb_id:
            raise HTTPException(status_code=400, detail="Radarr requires TMDB ID; request is not TMDB-based")
        try:
            await radarr_core.add_movie(
                url=radarr_cfg.radarr_url,
                token=radarr_cfg.radarr_token,
                tmdb_id=_req_tmdb_id,
                title=req.title or "",
                root_folder=radarr_cfg.radarr_root_folder,
                quality_profile_id=radarr_cfg.radarr_quality_profile,
                tags=radarr_cfg.radarr_tags,
            )
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Radarr error: {e}")

    elif req.media_type == "series":
        from routers.media import _effective_sonarr, get_user_tmdb_key
        from core import sonarr as sonarr_core, tmdb as tmdb_core
        sonarr_cfg = _effective_sonarr(settings, gs)
        if not sonarr_cfg:
            raise HTTPException(status_code=400, detail="Sonarr not configured")
        try:
            tmdb_key = await get_user_tmdb_key(db, current_user.id)
            # If URI is TVDB-based, use that ID directly. Otherwise try alias table then TMDB API.
            if _uri.provider == "tvdb":
                tvdb_id = int(_uri.id)
            else:
                tvdb_id = None
                if req.uri_id:
                    from utils.alias_lookup import get_provider_id_for_uri as _get_tvdb_admin
                    _tvdb_str = await _get_tvdb_admin(db, req.uri_id, "tvdb")
                    tvdb_id = int(_tvdb_str) if _tvdb_str else None
                if not tvdb_id and _req_tmdb_id:
                    ext_ids = await tmdb_core.get_external_ids(_req_tmdb_id, "tv", api_key=tmdb_key)
                    tvdb_id = ext_ids.get("tvdb_id")
            if not tvdb_id:
                raise HTTPException(status_code=400, detail="Could not find TVDB ID")
            await sonarr_core.add_series(
                url=sonarr_cfg.sonarr_url,
                token=sonarr_cfg.sonarr_token,
                tvdb_id=tvdb_id,
                root_folder=sonarr_cfg.sonarr_root_folder,
                quality_profile_id=sonarr_cfg.sonarr_quality_profile,
                tags=sonarr_cfg.sonarr_tags,
                season_folder=sonarr_cfg.sonarr_season_folder if sonarr_cfg.sonarr_season_folder is not None else True,
            )
        except Exception as e:
            if isinstance(e, HTTPException): raise e
            raise HTTPException(status_code=500, detail=f"Sonarr error: {e}")

    req.status = RequestStatus.approved
    req.reviewed_by = current_user.id
    req.updated_at = func.now()
    await db.commit()
    return {"status": "approved"}


@router.post("/requests/{request_id}/reject")
async def reject_request(
    request_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(require_admin),
):
    req_q = await db.execute(select(MediaRequest).where(MediaRequest.id == request_id))
    req = req_q.scalar_one_or_none()
    if not req:
        raise HTTPException(status_code=404, detail="Request not found")

    req.status = RequestStatus.rejected
    req.reviewed_by = current_user.id
    req.updated_at = func.now()
    await db.commit()
    return {"status": "rejected"}
