"""Add provider_cache table for TTL-cached TMDB/TVDB/Skyhook responses.

Revision ID: u5v6w7x8y9z0
Revises: z1a2b3c4d5e6
Create Date: 2026-06-03
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision: str = 'u5v6w7x8y9z0'
down_revision: Union[str, Sequence[str], None] = 'z1a2b3c4d5e6'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        'provider_cache',
        sa.Column('cache_key', sa.String(length=64), nullable=False),
        sa.Column('value', postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column('expires_at', sa.DateTime(), nullable=False),
        sa.Column('created_at', sa.DateTime(), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint('cache_key'),
    )
    op.create_index('idx_provider_cache_expires', 'provider_cache', ['expires_at'])


def downgrade() -> None:
    op.drop_index('idx_provider_cache_expires', table_name='provider_cache')
    op.drop_table('provider_cache')
