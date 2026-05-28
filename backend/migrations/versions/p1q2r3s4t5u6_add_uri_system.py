"""Add URI system: media_aliases table, uri_id on shows and media

Revision ID: p1q2r3s4t5u6
Revises: 20260527_genres
Create Date: 2026-05-28

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision: str = 'p1q2r3s4t5u6'
down_revision: Union[str, Sequence[str], None] = '20260527_genres'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        'media_aliases',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('internal_id', sa.Integer(), nullable=False),
        sa.Column('media_type', postgresql.ENUM(name='mediatype', create_type=False), nullable=False),
        sa.Column('provider', sa.String(50), nullable=False),
        sa.Column('external_id', sa.String(100), nullable=False),
        sa.Column('is_manual', sa.Boolean(), server_default='false', nullable=False),
        sa.Column('created_at', sa.DateTime(), server_default=sa.text('now()'), nullable=False),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('provider', 'external_id', 'media_type',
                            name='uq_media_aliases_provider_external_type'),
    )
    op.create_index('idx_media_aliases_internal_type', 'media_aliases', ['internal_id', 'media_type'])

    op.add_column('shows', sa.Column('uri_id', sa.String(50), nullable=True))
    op.create_index('ix_shows_uri_id', 'shows', ['uri_id'])

    op.add_column('media', sa.Column('uri_id', sa.String(50), nullable=True))
    op.create_index('ix_media_uri_id', 'media', ['uri_id'])


def downgrade() -> None:
    op.drop_index('ix_media_uri_id', 'media')
    op.drop_column('media', 'uri_id')

    op.drop_index('ix_shows_uri_id', 'shows')
    op.drop_column('shows', 'uri_id')

    op.drop_index('idx_media_aliases_internal_type', 'media_aliases')
    op.drop_table('media_aliases')
