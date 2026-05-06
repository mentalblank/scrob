"""add list integrations

Revision ID: 0011_add_list_integrations
Revises: 0010_add_blocklist_items
Create Date: 2026-05-06 15:55:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '0011_add_list_integrations'
down_revision: Union[str, Sequence[str], None] = '0010_add_blocklist_items'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Radarr integration
    op.add_column('lists', sa.Column('radarr_auto_add', sa.Boolean(), server_default='false', nullable=False))
    op.add_column('lists', sa.Column('radarr_root_folder', sa.String(length=500), nullable=True))
    op.add_column('lists', sa.Column('radarr_quality_profile', sa.Integer(), nullable=True))
    op.add_column('lists', sa.Column('radarr_tags', sa.JSON(), nullable=True))
    op.add_column('lists', sa.Column('radarr_monitor', sa.String(length=50), nullable=True))

    # Sonarr integration
    op.add_column('lists', sa.Column('sonarr_auto_add', sa.Boolean(), server_default='false', nullable=False))
    op.add_column('lists', sa.Column('sonarr_root_folder', sa.String(length=500), nullable=True))
    op.add_column('lists', sa.Column('sonarr_quality_profile', sa.Integer(), nullable=True))
    op.add_column('lists', sa.Column('sonarr_tags', sa.JSON(), nullable=True))
    op.add_column('lists', sa.Column('sonarr_series_type', sa.String(length=50), nullable=True))
    op.add_column('lists', sa.Column('sonarr_season_folder', sa.Boolean(), server_default='true', nullable=False))
    op.add_column('lists', sa.Column('sonarr_monitor', sa.String(length=50), nullable=True))


def downgrade() -> None:
    op.drop_column('lists', 'sonarr_monitor')
    op.drop_column('lists', 'sonarr_season_folder')
    op.drop_column('lists', 'sonarr_series_type')
    op.drop_column('lists', 'sonarr_tags')
    op.drop_column('lists', 'sonarr_quality_profile')
    op.drop_column('lists', 'sonarr_root_folder')
    op.drop_column('lists', 'sonarr_auto_add')
    op.drop_column('lists', 'radarr_monitor')
    op.drop_column('lists', 'radarr_tags')
    op.drop_column('lists', 'radarr_quality_profile')
    op.drop_column('lists', 'radarr_root_folder')
    op.drop_column('lists', 'radarr_auto_add')
