"""Remove legacy integer ID hacks: fix negative tmdb_id shows, migrate blocklist/comments to uri_id.

Revision ID: q1r2s3t4u5v6
Revises: p1q2r3s4t5u6
Create Date: 2026-05-28

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = 'q1r2s3t4u5v6'
down_revision: Union[str, Sequence[str], None] = 'p1q2r3s4t5u6'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ── 1. Fix shows with negative tmdb_id (TVDB-only hack) ─────────────────────
    # Shows stored as tmdb_id = -tvdb_id: move the value to tvdb_id column and null out tmdb_id.
    op.execute("""
        UPDATE shows
        SET tvdb_id = ABS(tmdb_id),
            tmdb_id = NULL
        WHERE tmdb_id < 0 AND tvdb_id IS NULL
    """)
    # For rows where tvdb_id was already set (from the k5l6m7n8o9p0 migration),
    # the negative tmdb_id is just redundant — null it out.
    op.execute("UPDATE shows SET tmdb_id = NULL WHERE tmdb_id < 0")

    # ── 2. blocklist_items: add uri_id, migrate data, drop legacy unique index ───
    op.add_column('blocklist_items', sa.Column('uri_id', sa.String(50), nullable=True))

    op.execute("""
        UPDATE blocklist_items
        SET uri_id = CASE
            WHEN media_type::text = 'movie'   THEN 'tmdb:m:' || tmdb_id::text
            WHEN media_type::text = 'series'  AND tmdb_id >= 0 THEN 'tmdb:s:' || tmdb_id::text
            WHEN media_type::text = 'series'  AND tmdb_id < 0  THEN 'tvdb:s:' || ABS(tmdb_id)::text
            WHEN media_type::text = 'episode' THEN 'tmdb:e:' || tmdb_id::text
            ELSE 'tmdb:s:' || tmdb_id::text
        END
        WHERE tmdb_id IS NOT NULL AND uri_id IS NULL
    """)

    # Make uri_id NOT NULL now that all rows are populated
    op.alter_column('blocklist_items', 'uri_id', nullable=False)

    # Make tmdb_id nullable (deprecated)
    op.alter_column('blocklist_items', 'tmdb_id', nullable=True)

    # Drop old unique index, create new one keyed on uri_id
    op.drop_index('idx_blocklist_user_tmdb_type', table_name='blocklist_items')
    op.create_unique_constraint('uq_blocklist_user_uri', 'blocklist_items', ['user_id', 'uri_id'])
    op.create_index('ix_blocklist_items_uri_id', 'blocklist_items', ['uri_id'])

    # ── 3. comments: add uri_id, migrate data, drop legacy index ────────────────
    op.add_column('comments', sa.Column('uri_id', sa.String(50), nullable=True))

    op.execute("""
        UPDATE comments
        SET uri_id = CASE
            WHEN media_type = 'movie'   THEN 'tmdb:m:' || tmdb_id::text
            WHEN media_type IN ('series', 'season', 'episode') THEN 'tmdb:s:' || tmdb_id::text
            ELSE 'tmdb:s:' || tmdb_id::text
        END
        WHERE tmdb_id IS NOT NULL AND uri_id IS NULL
    """)

    op.alter_column('comments', 'uri_id', nullable=False)
    op.alter_column('comments', 'tmdb_id', nullable=True)

    op.drop_index('idx_comments_media', table_name='comments')
    op.create_index('idx_comments_uri', 'comments', ['media_type', 'uri_id', 'season_number', 'episode_number'])


def downgrade() -> None:
    op.drop_index('idx_comments_uri', 'comments')
    op.create_index('idx_comments_media', 'comments', ['media_type', 'tmdb_id', 'season_number', 'episode_number'])
    op.alter_column('comments', 'tmdb_id', nullable=False)
    op.alter_column('comments', 'uri_id', nullable=True)
    op.drop_column('comments', 'uri_id')

    op.drop_index('ix_blocklist_items_uri_id', 'blocklist_items')
    op.drop_constraint('uq_blocklist_user_uri', 'blocklist_items', type_='unique')
    op.alter_column('blocklist_items', 'tmdb_id', nullable=False)
    op.alter_column('blocklist_items', 'uri_id', nullable=True)
    op.create_index('idx_blocklist_user_tmdb_type', 'blocklist_items', ['user_id', 'tmdb_id', 'media_type'], unique=True)
    op.drop_column('blocklist_items', 'uri_id')

    # Cannot safely reverse the shows negative-id fix (data was destructive)
