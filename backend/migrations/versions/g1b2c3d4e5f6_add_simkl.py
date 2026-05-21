"""add simkl integration fields

Revision ID: g1b2c3d4e5f6
Revises: fa39c281b047
Create Date: 2026-05-18
"""
from alembic import op
import sqlalchemy as sa

revision = 'g1b2c3d4e5f6'
down_revision = 'fa39c281b047'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column('user_settings', sa.Column('simkl_client_id',    sa.String(255),  nullable=True))
    op.add_column('user_settings', sa.Column('simkl_access_token', sa.String(2000), nullable=True))
    op.add_column('user_settings', sa.Column('simkl_device_code',  sa.String(255),  nullable=True))
    op.add_column('user_settings', sa.Column('simkl_sync_watched', sa.Boolean(), nullable=False, server_default='true'))
    op.add_column('user_settings', sa.Column('simkl_sync_ratings', sa.Boolean(), nullable=False, server_default='true'))
    op.add_column('user_settings', sa.Column('simkl_sync_lists',   sa.Boolean(), nullable=False, server_default='true'))
    op.add_column('user_settings', sa.Column('simkl_push_watched', sa.Boolean(), nullable=False, server_default='false'))
    op.add_column('user_settings', sa.Column('simkl_push_ratings', sa.Boolean(), nullable=False, server_default='false'))

    # Add simkl to the collectionsource enum used by sync_jobs
    op.execute("ALTER TYPE collectionsource ADD VALUE IF NOT EXISTS 'simkl'")


def downgrade() -> None:
    op.drop_column('user_settings', 'simkl_push_ratings')
    op.drop_column('user_settings', 'simkl_push_watched')
    op.drop_column('user_settings', 'simkl_sync_lists')
    op.drop_column('user_settings', 'simkl_sync_ratings')
    op.drop_column('user_settings', 'simkl_sync_watched')
    op.drop_column('user_settings', 'simkl_device_code')
    op.drop_column('user_settings', 'simkl_access_token')
    op.drop_column('user_settings', 'simkl_client_id')
