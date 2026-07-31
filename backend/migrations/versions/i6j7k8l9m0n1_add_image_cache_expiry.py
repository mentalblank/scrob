"""Add image_cache_expiry_days to global_settings

Lets admins cap how long cached TMDB/TVDB artwork is kept, independent of the
size limit. NULL / 0 means entries are only ever evicted by the size limit.

Revision ID: i6j7k8l9m0n1
Revises: h5i6j7k8l9m0
Create Date: 2026-07-31
"""
from alembic import op
import sqlalchemy as sa

revision = 'i6j7k8l9m0n1'
down_revision = 'h5i6j7k8l9m0'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        'global_settings',
        sa.Column('image_cache_expiry_days', sa.Integer(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column('global_settings', 'image_cache_expiry_days')
