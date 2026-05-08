"""Add tiered sync scheduling

Revision ID: 99fbc5954be6
Revises: cb25424a7781
Create Date: 2026-05-09 00:37:41.300152

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '99fbc5954be6'
down_revision: Union[str, Sequence[str], None] = 'cb25424a7781'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # UserSettings (Trakt)
    op.add_column('user_settings', sa.Column('trakt_full_sync_interval', sa.Integer(), nullable=True))
    op.add_column('user_settings', sa.Column('trakt_partial_sync_interval', sa.Integer(), nullable=True))
    op.add_column('user_settings', sa.Column('last_trakt_full_sync', sa.DateTime(), nullable=True))
    op.add_column('user_settings', sa.Column('last_trakt_partial_sync', sa.DateTime(), nullable=True))

    # MediaServerConnection
    op.add_column('media_server_connections', sa.Column('partial_sync_interval', sa.Integer(), nullable=True))
    op.add_column('media_server_connections', sa.Column('last_full_sync', sa.DateTime(), nullable=True))
    op.add_column('media_server_connections', sa.Column('last_partial_sync', sa.DateTime(), nullable=True))


def downgrade() -> None:
    # MediaServerConnection
    op.drop_column('media_server_connections', 'last_partial_sync')
    op.drop_column('media_server_connections', 'last_full_sync')
    op.drop_column('media_server_connections', 'partial_sync_interval')

    # UserSettings
    op.drop_column('user_settings', 'last_trakt_partial_sync')
    op.drop_column('user_settings', 'last_trakt_full_sync')
    op.drop_column('user_settings', 'trakt_partial_sync_interval')
    op.drop_column('user_settings', 'trakt_full_sync_interval')
