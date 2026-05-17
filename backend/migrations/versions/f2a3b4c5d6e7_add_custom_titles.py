"""add custom title columns for show season and episode renaming

Revision ID: f2a3b4c5d6e7
Revises: b1c2d3e4f5a6
Create Date: 2026-05-17 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = 'f2a3b4c5d6e7'
down_revision: Union[str, Sequence[str], None] = 'merge_fork_upstream'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Add custom_title and custom_season_names to shows
    op.add_column('shows', sa.Column('custom_title', sa.String(500), nullable=True))
    op.add_column('shows', sa.Column('custom_season_names', sa.dialects.postgresql.JSONB(), nullable=True))
    # Add custom_title to media (episodes)
    op.add_column('media', sa.Column('custom_title', sa.String(500), nullable=True))


def downgrade() -> None:
    op.drop_column('shows', 'custom_title')
    op.drop_column('shows', 'custom_season_names')
    op.drop_column('media', 'custom_title')
