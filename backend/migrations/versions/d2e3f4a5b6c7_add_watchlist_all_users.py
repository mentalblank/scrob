"""add watchlist_all_users to connections

Revision ID: d2e3f4a5b6c7
Revises: c1d2e3f4a5b6
Create Date: 2026-05-15 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = 'd2e3f4a5b6c7'
down_revision: Union[str, Sequence[str], None] = 'c1d2e3f4a5b6'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('media_server_connections', sa.Column('watchlist_all_users', sa.Boolean(), server_default='false', nullable=False))


def downgrade() -> None:
    op.drop_column('media_server_connections', 'watchlist_all_users')
