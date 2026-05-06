"""add trakt list sync columns

Revision ID: e1f2a3b4c5d6
Revises: 5dbe314a6255
Create Date: 2026-05-05 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = 'e1f2a3b4c5d6'
down_revision: Union[str, Sequence[str], None] = 'd4e5f6a7b8c9'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('user_settings', sa.Column('trakt_sync_lists', sa.Boolean(), server_default='false', nullable=False))
    op.add_column('user_settings', sa.Column('trakt_push_lists', sa.Boolean(), server_default='false', nullable=False))
    op.add_column('lists', sa.Column('trakt_slug', sa.String(255), nullable=True))


def downgrade() -> None:
    op.drop_column('lists', 'trakt_slug')
    op.drop_column('user_settings', 'trakt_push_lists')
    op.drop_column('user_settings', 'trakt_sync_lists')
