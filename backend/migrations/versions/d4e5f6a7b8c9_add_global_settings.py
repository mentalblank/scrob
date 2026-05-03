"""add_global_settings

Revision ID: d4e5f6a7b8c9
Revises: c3a1b2d4e5f6
Create Date: 2026-05-03 00:01:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'd4e5f6a7b8c9'
down_revision: Union[str, Sequence[str], None] = 'c3a1b2d4e5f6'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        'global_settings',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('tmdb_api_key', sa.String(255), nullable=True),
        sa.Column('radarr_url', sa.String(500), nullable=True),
        sa.Column('radarr_token', sa.String(500), nullable=True),
        sa.Column('radarr_root_folder', sa.String(500), nullable=True),
        sa.Column('radarr_quality_profile', sa.Integer(), nullable=True),
        sa.Column('radarr_tags', sa.JSON(), nullable=True),
        sa.Column('sonarr_url', sa.String(500), nullable=True),
        sa.Column('sonarr_token', sa.String(500), nullable=True),
        sa.Column('sonarr_root_folder', sa.String(500), nullable=True),
        sa.Column('sonarr_quality_profile', sa.Integer(), nullable=True),
        sa.Column('sonarr_tags', sa.JSON(), nullable=True),
        sa.Column('sonarr_season_folder', sa.Boolean(), server_default='true', nullable=False),
        sa.PrimaryKeyConstraint('id'),
    )


def downgrade() -> None:
    op.drop_table('global_settings')
