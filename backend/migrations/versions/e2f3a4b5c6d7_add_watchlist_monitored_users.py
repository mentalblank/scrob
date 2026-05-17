"""add watchlist_monitored_users to connections

Revision ID: e2f3a4b5c6d7
Revises: d2e3f4a5b6c7
Create Date: 2026-05-15 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = 'e2f3a4b5c6d7'
down_revision: Union[str, Sequence[str], None] = 'd2e3f4a5b6c7'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('media_server_connections', sa.Column('watchlist_monitored_users', sa.JSON(), nullable=True))


def downgrade() -> None:
    op.drop_column('media_server_connections', 'watchlist_monitored_users')
