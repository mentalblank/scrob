"""Add auto sync interval columns to user_settings

Revision ID: 0003_auto_sync_intervals
Revises: 0002_nullable_password_hash
Create Date: 2026-04-19
"""
from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa

revision: str = '0003_auto_sync_intervals'
down_revision: Union[str, Sequence[str], None] = '0002_nullable_password_hash'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('user_settings', sa.Column('jellyfin_auto_sync_interval', sa.Integer(), nullable=True))
    op.add_column('user_settings', sa.Column('emby_auto_sync_interval', sa.Integer(), nullable=True))
    op.add_column('user_settings', sa.Column('plex_auto_sync_interval', sa.Integer(), nullable=True))


def downgrade() -> None:
    op.drop_column('user_settings', 'plex_auto_sync_interval')
    op.drop_column('user_settings', 'emby_auto_sync_interval')
    op.drop_column('user_settings', 'jellyfin_auto_sync_interval')
