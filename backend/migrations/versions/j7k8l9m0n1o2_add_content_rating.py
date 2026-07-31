"""Add content_rating to shows and media

Stores the age/content certification (TV-MA, R, PG-13, …) so listings can be
filtered by it. TMDB only returns certifications on detail fetches, never on
list endpoints, so the value has to be persisted at enrichment time.

Revision ID: j7k8l9m0n1o2
Revises: i6j7k8l9m0n1
Create Date: 2026-07-31
"""
from alembic import op
import sqlalchemy as sa

revision = 'j7k8l9m0n1o2'
down_revision = 'i6j7k8l9m0n1'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column('shows', sa.Column('content_rating', sa.String(length=16), nullable=True))
    op.add_column('media', sa.Column('content_rating', sa.String(length=16), nullable=True))
    op.create_index('idx_media_content_rating', 'media', ['content_rating'])
    op.create_index('idx_shows_content_rating', 'shows', ['content_rating'])


def downgrade() -> None:
    op.drop_index('idx_shows_content_rating', table_name='shows')
    op.drop_index('idx_media_content_rating', table_name='media')
    op.drop_column('media', 'content_rating')
    op.drop_column('shows', 'content_rating')
