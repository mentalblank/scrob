"""add timezone to user_settings

Revision ID: 3ac71e0d9f42
Revises: 0f562a85b987
Create Date: 2026-09-05 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = '3ac71e0d9f42'
down_revision: Union[str, Sequence[str], None] = '0f562a85b987'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('user_settings', sa.Column('timezone', sa.String(length=64), nullable=True))


def downgrade() -> None:
    op.drop_column('user_settings', 'timezone')
