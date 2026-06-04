"""Add playback_target to user_settings

Revision ID: v6w7x8y9z0a1
Revises: u5v6w7x8y9z0
Create Date: 2026-06-04
"""
from alembic import op
import sqlalchemy as sa

revision = 'v6w7x8y9z0a1'
down_revision = 'u5v6w7x8y9z0'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column('user_settings', sa.Column('playback_target', sa.String(length=20), nullable=False, server_default='web'))


def downgrade() -> None:
    op.drop_column('user_settings', 'playback_target')
