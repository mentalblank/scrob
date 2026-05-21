"""add disliked_genres to user_profiles

Revision ID: j4k5l6m7n8o9
Revises: i3j4k5l6m7n8
Create Date: 2026-05-20
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = 'j4k5l6m7n8o9'
down_revision = 'i3j4k5l6m7n8'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column('user_profiles', sa.Column('disliked_genres', postgresql.JSONB(), nullable=True))


def downgrade() -> None:
    op.drop_column('user_profiles', 'disliked_genres')
