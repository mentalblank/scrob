"""add plex watchlist auto-request to connections

Revision ID: c1d2e3f4a5b6
Revises: b1c2d3e4f5a6
Create Date: 2026-05-15 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = 'c1d2e3f4a5b6'
down_revision: Union[str, Sequence[str], None] = 'b1c2d3e4f5a6'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('media_server_connections', sa.Column('watchlist_to_radarr', sa.Boolean(), server_default='false', nullable=False))
    op.add_column('media_server_connections', sa.Column('watchlist_to_sonarr', sa.Boolean(), server_default='false', nullable=False))
    op.add_column('media_server_connections', sa.Column('watchlist_synced_ids', sa.JSON(), nullable=True))


def downgrade() -> None:
    op.drop_column('media_server_connections', 'watchlist_synced_ids')
    op.drop_column('media_server_connections', 'watchlist_to_sonarr')
    op.drop_column('media_server_connections', 'watchlist_to_radarr')
