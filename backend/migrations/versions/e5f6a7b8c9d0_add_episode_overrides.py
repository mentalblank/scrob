"""add show episode overrides table

Revision ID: e5f6a7b8c9d0
Revises: f2a3b4c5d6e7
Create Date: 2026-05-17 01:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = 'e5f6a7b8c9d0'
down_revision: Union[str, Sequence[str], None] = 'f2a3b4c5d6e7'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        'show_episode_overrides',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('user_id', sa.Integer(), nullable=False),
        sa.Column('source_show_tmdb_id', sa.Integer(), nullable=False),
        sa.Column('source_season_number', sa.Integer(), nullable=False),
        sa.Column('source_episode_number', sa.Integer(), nullable=False),
        sa.Column('target_show_tmdb_id', sa.Integer(), nullable=False),
        sa.Column('target_season_number', sa.Integer(), nullable=False),
        sa.Column('target_episode_number', sa.Integer(), nullable=False),
        sa.Column('created_at', sa.DateTime(), server_default=sa.text('now()'), nullable=False),
        sa.ForeignKeyConstraint(['user_id'], ['users.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('user_id', 'source_show_tmdb_id', 'source_season_number', 'source_episode_number', name='uq_episode_override')
    )


def downgrade() -> None:
    op.drop_table('show_episode_overrides')
