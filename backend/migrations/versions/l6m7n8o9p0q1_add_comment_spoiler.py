"""Add is_spoiler column to comments

Revision ID: l6m7n8o9p0q1
Revises: k5l6m7n8o9p0
Create Date: 2026-05-22
"""
from alembic import op
import sqlalchemy as sa

revision = 'l6m7n8o9p0q1'
down_revision = 'k5l6m7n8o9p0'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column('comments', sa.Column('is_spoiler', sa.Boolean(), nullable=False, server_default='false'))


def downgrade() -> None:
    op.drop_column('comments', 'is_spoiler')
