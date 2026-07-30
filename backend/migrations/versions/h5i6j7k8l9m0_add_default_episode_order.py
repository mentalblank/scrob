"""Add missing default_episode_order column to user_settings

The user_settings.default_episode_order field has existed on the
SQLAlchemy model since the TVDB/episode-ordering merge, but no
migration ever actually added the column — it only surfaced as a
runtime UndefinedColumnError, never caught by import-time checks
or the mocked-session test suite.

Revision ID: h5i6j7k8l9m0
Revises: g4h5i6j7k8l9
Create Date: 2026-07-30
"""
from alembic import op
import sqlalchemy as sa

revision = 'h5i6j7k8l9m0'
down_revision = 'g4h5i6j7k8l9'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        'user_settings',
        sa.Column('default_episode_order', sa.String(length=20), nullable=False, server_default='tmdb'),
    )


def downgrade() -> None:
    op.drop_column('user_settings', 'default_episode_order')
