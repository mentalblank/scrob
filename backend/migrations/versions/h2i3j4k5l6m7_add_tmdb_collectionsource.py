"""add tmdb to collectionsource enum

Revision ID: h2i3j4k5l6m7
Revises: g1b2c3d4e5f6
Create Date: 2026-05-20
"""
from alembic import op

revision = 'h2i3j4k5l6m7'
down_revision = 'g1b2c3d4e5f6'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("ALTER TYPE collectionsource ADD VALUE IF NOT EXISTS 'tmdb'")


def downgrade() -> None:
    pass
