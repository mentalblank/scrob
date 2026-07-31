"""Track episode -> movie conversions so they can be listed and reverted

Converting an episode into a movie mutates the media row in place, which left no
record of what had been changed or how to undo it. Keep the original coordinates
so the remaps UI can show the conversion and put it back.

Revision ID: k8l9m0n1o2p3
Revises: j7k8l9m0n1o2
Create Date: 2026-07-31
"""
from alembic import op
import sqlalchemy as sa

revision = 'k8l9m0n1o2p3'
down_revision = 'j7k8l9m0n1o2'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        'episode_movie_conversions',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('user_id', sa.Integer(), sa.ForeignKey('users.id', ondelete='CASCADE'), nullable=False),
        sa.Column('media_id', sa.Integer(), sa.ForeignKey('media.id', ondelete='CASCADE'), nullable=False),
        sa.Column('original_show_id', sa.Integer(), sa.ForeignKey('shows.id', ondelete='SET NULL'), nullable=True),
        sa.Column('original_season_number', sa.Integer(), nullable=True),
        sa.Column('original_episode_number', sa.Integer(), nullable=True),
        sa.Column('original_title', sa.String(length=500), nullable=True),
        sa.Column('original_tmdb_id', sa.Integer(), nullable=True),
        sa.Column('movie_tmdb_id', sa.Integer(), nullable=False),
        sa.Column('created_at', sa.DateTime(), server_default=sa.func.now(), nullable=False),
        sa.UniqueConstraint('user_id', 'media_id', name='uq_episode_movie_conversion'),
    )
    op.create_index('idx_emc_user', 'episode_movie_conversions', ['user_id'])


def downgrade() -> None:
    op.drop_index('idx_emc_user', table_name='episode_movie_conversions')
    op.drop_table('episode_movie_conversions')
