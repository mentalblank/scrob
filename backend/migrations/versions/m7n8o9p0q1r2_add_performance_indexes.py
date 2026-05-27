"""Add performance indexes

Revision ID: m7n8o9p0q1r2
Revises: l6m7n8o9p0q1
Create Date: 2026-05-23
"""
from alembic import op

revision = 'm7n8o9p0q1r2'
down_revision = 'l6m7n8o9p0q1'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # collections: join key used in every collection lookup across all routers
    op.create_index('idx_collections_media_id', 'collections', ['media_id'])

    # collection_files: FK join used in history, ratings, shows, webhooks, media routers
    op.create_index('idx_collection_files_collection_id', 'collection_files', ['collection_id'])

    # watch_events: covers the history listing query
    # (WHERE user_id=X AND completed=TRUE ORDER BY watched_at DESC)
    op.create_index(
        'idx_watch_events_user_completed_watched_at',
        'watch_events',
        ['user_id', 'completed', 'watched_at'],
        postgresql_ops={'watched_at': 'DESC'},
    )

    # media: list pages filter by type and sort by date or rating
    op.create_index('idx_media_type_release_date', 'media', ['media_type', 'release_date'])
    op.create_index('idx_media_type_tmdb_rating', 'media', ['media_type', 'tmdb_rating'])


def downgrade() -> None:
    op.drop_index('idx_media_type_tmdb_rating', table_name='media')
    op.drop_index('idx_media_type_release_date', table_name='media')
    op.drop_index('idx_watch_events_user_completed_watched_at', table_name='watch_events')
    op.drop_index('idx_collection_files_collection_id', table_name='collection_files')
    op.drop_index('idx_collections_media_id', table_name='collections')
