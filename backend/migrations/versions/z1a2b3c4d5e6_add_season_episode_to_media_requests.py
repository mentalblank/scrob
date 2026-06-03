"""Add season and episode numbers to media requests

Revision ID: z1a2b3c4d5e6
Revises: n8o9p0q1r2s3
Create Date: 2026-06-03
"""
from alembic import op
import sqlalchemy as sa

revision = 'z1a2b3c4d5e6'
down_revision = 'n8o9p0q1r2s3'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column('media_requests', sa.Column('season_number', sa.Integer(), nullable=True))
    op.add_column('media_requests', sa.Column('episode_number', sa.Integer(), nullable=True))


def downgrade() -> None:
    op.drop_column('media_requests', 'episode_number')
    op.drop_column('media_requests', 'season_number')
