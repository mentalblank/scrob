from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy import delete, func

from db import get_db
from models.users import User
from models.global_settings import GlobalSettings
from dependencies import require_admin
from core.url_validator import validate_service_url
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
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="You cannot delete your own account from the admin panel.",
        )

    result = await db.execute(select(User).where(User.id == user_id))
    target = result.scalar_one_or_none()
    if not target:
        raise HTTPException(status_code=404, detail="User not found")

    await db.execute(delete(User).where(User.id == user_id))
    await db.commit()
    return {"status": "deleted"}
