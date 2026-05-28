"""Drop deprecated legacy tmdb_id columns from blocklist_items, comments, overrides, media_requests.

Revision ID: t4u5v6w7x8y9
Revises: s3t4u5v6w7x8
Create Date: 2026-05-29

WARNING: Destructive. Backfill must be complete + verified before running.
Run `python -m backend.scripts.migrate_uris` first, ensure `uri_id` populated everywhere.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = 't4u5v6w7x8y9'
down_revision: Union[str, Sequence[str], None] = 's3t4u5v6w7x8'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Safety: refuse to run if any row still missing uri_id
    op.execute("""
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM blocklist_items WHERE uri_id IS NULL) THEN
                RAISE EXCEPTION 'blocklist_items has rows with NULL uri_id — run migrate_uris first';
            END IF;
            IF EXISTS (SELECT 1 FROM comments WHERE uri_id IS NULL) THEN
                RAISE EXCEPTION 'comments has rows with NULL uri_id';
            END IF;
        END $$;
    """)

    # blocklist_items.tmdb_id
    op.drop_column('blocklist_items', 'tmdb_id')

    # comments.tmdb_id
    op.drop_column('comments', 'tmdb_id')

    # show_season_overrides legacy columns
    op.drop_column('show_season_overrides', 'source_show_tmdb_id')
    op.drop_column('show_season_overrides', 'target_show_tmdb_id')

    # show_episode_overrides legacy columns
    op.drop_column('show_episode_overrides', 'source_show_tmdb_id')
    op.drop_column('show_episode_overrides', 'target_show_tmdb_id')

    # media_requests: make uri_id NOT NULL, drop tmdb_id
    op.execute("""
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM media_requests WHERE uri_id IS NULL) THEN
                RAISE EXCEPTION 'media_requests has rows with NULL uri_id';
            END IF;
        END $$;
    """)
    op.alter_column('media_requests', 'uri_id', nullable=False)
    op.drop_column('media_requests', 'tmdb_id')


def downgrade() -> None:
    # Re-add columns as nullable. Manual backfill required if needed.
    op.add_column('media_requests', sa.Column('tmdb_id', sa.Integer(), nullable=True))
    op.alter_column('media_requests', 'uri_id', nullable=True)

    op.add_column('show_episode_overrides', sa.Column('target_show_tmdb_id', sa.Integer(), nullable=True))
    op.add_column('show_episode_overrides', sa.Column('source_show_tmdb_id', sa.Integer(), nullable=True))

    op.add_column('show_season_overrides', sa.Column('target_show_tmdb_id', sa.Integer(), nullable=True))
    op.add_column('show_season_overrides', sa.Column('source_show_tmdb_id', sa.Integer(), nullable=True))

    op.add_column('comments', sa.Column('tmdb_id', sa.Integer(), nullable=True))
    op.add_column('blocklist_items', sa.Column('tmdb_id', sa.Integer(), nullable=True))
