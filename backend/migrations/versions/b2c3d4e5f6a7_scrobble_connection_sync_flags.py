"""add sync flags to scrobble_connections

Revision ID: b2c3d4e5f6a7
Revises: a1b2c3d4e5f6
Create Date: 2026-05-08 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = 'b2c3d4e5f6a7'
down_revision: Union[str, Sequence[str], None] = 'a1b2c3d4e5f6'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('scrobble_connections', sa.Column('sync_collection', sa.Boolean(), server_default='true', nullable=False))
    op.add_column('scrobble_connections', sa.Column('sync_watched',    sa.Boolean(), server_default='true', nullable=False))
    op.add_column('scrobble_connections', sa.Column('sync_playback',   sa.Boolean(), server_default='true', nullable=False))


def downgrade() -> None:
    op.drop_column('scrobble_connections', 'sync_playback')
    op.drop_column('scrobble_connections', 'sync_watched')
    op.drop_column('scrobble_connections', 'sync_collection')
