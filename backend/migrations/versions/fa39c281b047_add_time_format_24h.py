"""add time_format_24h to user_settings

Revision ID: fa39c281b047
Revises: e5f6a7b8c9d0
Create Date: 2026-05-18 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = 'fa39c281b047'
down_revision: Union[str, Sequence[str], None] = 'e5f6a7b8c9d0'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('user_settings', sa.Column('time_format_24h', sa.Boolean(), server_default='false', nullable=False))


def downgrade() -> None:
    op.drop_column('user_settings', 'time_format_24h')
