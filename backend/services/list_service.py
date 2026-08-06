from typing import Any
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session
from models.domain.list import List, ListItem, PrivacyLevel


class ListService:
    @staticmethod
    async def create_custom_list(
        db: AsyncSession | Session,
        user_id: int,
        name: str,
        description: str | None = None,
        privacy_level: PrivacyLevel = PrivacyLevel.public,
    ) -> List:
        custom_list = List(
            user_id=user_id,
            name=name,
            description=description,
            privacy_level=privacy_level,
        )
        db.add(custom_list)
        if isinstance(db, AsyncSession):
            await db.flush()
        else:
            db.flush()
        return custom_list

    @staticmethod
    async def add_item_to_list(
        db: AsyncSession | Session,
        list_id: int,
        media_id: int,
        notes: str | None = None,
    ) -> ListItem:
        item = ListItem(list_id=list_id, asset_type="show", asset_id=media_id, notes=notes)
        db.add(item)
        if isinstance(db, AsyncSession):
            await db.flush()
        else:
            db.flush()
        return item
