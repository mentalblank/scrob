"""Add uri_id to media_requests, make tmdb_id nullable.

Revision ID: s3t4u5v6w7x8
Revises: r2s3t4u5v6w7
Create Date: 2026-05-29

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = 's3t4u5v6w7x8'
down_revision: Union[str, Sequence[str], None] = 'r2s3t4u5v6w7'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('media_requests', sa.Column('uri_id', sa.String(50), nullable=True))
    op.create_index('ix_media_requests_uri_id', 'media_requests', ['uri_id'])

    # Backfill uri_id from tmdb_id + media_type
    op.execute("""
        UPDATE media_requests
        SET uri_id = CASE
            WHEN media_type = 'movie'  AND tmdb_id >= 0 THEN 'tmdb:m:' || tmdb_id::text
            WHEN media_type = 'series' AND tmdb_id >= 0 THEN 'tmdb:s:' || tmdb_id::text
            WHEN media_type = 'series' AND tmdb_id < 0  THEN 'tvdb:s:' || ABS(tmdb_id)::text
            ELSE NULL
        END
        WHERE uri_id IS NULL AND tmdb_id IS NOT NULL
    """)

    op.alter_column('media_requests', 'tmdb_id', nullable=True)


def downgrade() -> None:
    op.alter_column('media_requests', 'tmdb_id', nullable=False)
    op.drop_index('ix_media_requests_uri_id', 'media_requests')
    op.drop_column('media_requests', 'uri_id')
