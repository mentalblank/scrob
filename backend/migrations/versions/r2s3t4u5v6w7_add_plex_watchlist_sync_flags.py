"""Add plex_sync_watchlist and plex_push_watchlist to media_server_connections

Revision ID: r2s3t4u5v6w7
Revises: q1r2s3t4u5v6
Create Date: 2026-06-27
"""
from alembic import op
import sqlalchemy as sa

revision = 'r2s3t4u5v6w7'
down_revision = 'q1r2s3t4u5v6'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column('media_server_connections', sa.Column('plex_sync_watchlist', sa.Boolean(), nullable=False, server_default='false'))
    op.add_column('media_server_connections', sa.Column('plex_push_watchlist', sa.Boolean(), nullable=False, server_default='false'))


def downgrade() -> None:
    op.drop_column('media_server_connections', 'plex_push_watchlist')
    op.drop_column('media_server_connections', 'plex_sync_watchlist')
