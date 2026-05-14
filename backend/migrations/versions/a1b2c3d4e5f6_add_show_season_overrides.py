"""add show season overrides table

Revision ID: b1c2d3e4f5a6
Revises: c4d5e6f7a8b9
Create Date: 2026-05-14 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = 'b1c2d3e4f5a6'
down_revision: Union[str, Sequence[str], None] = 'c4d5e6f7a8b9'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        'show_season_overrides',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('user_id', sa.Integer(), sa.ForeignKey('users.id', ondelete='CASCADE'), nullable=False),
        sa.Column('source_show_tmdb_id', sa.Integer(), nullable=False),
        sa.Column('source_season_number', sa.Integer(), nullable=False),
        sa.Column('target_show_tmdb_id', sa.Integer(), nullable=False),
        sa.Column('target_season_number', sa.Integer(), nullable=False),
        sa.Column('created_at', sa.DateTime(), server_default=sa.func.now(), nullable=False),
        sa.UniqueConstraint('user_id', 'source_show_tmdb_id', 'source_season_number', name='uq_season_override'),
    )


def downgrade() -> None:
    op.drop_table('show_season_overrides')
