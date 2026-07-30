"""Add metadata_language to user_profiles and media/show translation tables

Revision ID: p0q1r2s3t4u5
Revises: o9p0q1r2s3t4
Create Date: 2026-06-09
"""
from alembic import op
import sqlalchemy as sa

revision = 'p0q1r2s3t4u5'
down_revision = 'o9p0q1r2s3t4'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # 1. Add metadata_language to user_profiles
    op.add_column('user_profiles', sa.Column('metadata_language', sa.String(10), nullable=True))

    # 2. Create media_translations table
    op.create_table(
        'media_translations',
        sa.Column('id', sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column('media_id', sa.Integer(), sa.ForeignKey('media.id', ondelete='CASCADE'), nullable=False),
        sa.Column('language', sa.String(10), nullable=False),
        sa.Column('title', sa.String(500), nullable=True),
        sa.Column('overview', sa.Text(), nullable=True),
        sa.Column('tagline', sa.Text(), nullable=True),
        sa.Column('poster_path', sa.String(500), nullable=True),
        sa.Column('fetched_at', sa.DateTime(), server_default=sa.func.now(), nullable=False),
    )
    op.create_index('idx_media_translations_media_id', 'media_translations', ['media_id'])
    op.create_index(
        'uq_media_translations_media_language',
        'media_translations', ['media_id', 'language'],
        unique=True,
    )

    # 3. Create show_translations table
    op.create_table(
        'show_translations',
        sa.Column('id', sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column('show_id', sa.Integer(), sa.ForeignKey('shows.id', ondelete='CASCADE'), nullable=False),
        sa.Column('language', sa.String(10), nullable=False),
        sa.Column('title', sa.String(500), nullable=True),
        sa.Column('overview', sa.Text(), nullable=True),
        sa.Column('tagline', sa.Text(), nullable=True),
        sa.Column('poster_path', sa.String(500), nullable=True),
        sa.Column('fetched_at', sa.DateTime(), server_default=sa.func.now(), nullable=False),
    )
    op.create_index('idx_show_translations_show_id', 'show_translations', ['show_id'])
    op.create_index(
        'uq_show_translations_show_language',
        'show_translations', ['show_id', 'language'],
        unique=True,
    )


def downgrade() -> None:
    op.drop_index('uq_show_translations_show_language', table_name='show_translations')
    op.drop_index('idx_show_translations_show_id', table_name='show_translations')
    op.drop_table('show_translations')

    op.drop_index('uq_media_translations_media_language', table_name='media_translations')
    op.drop_index('idx_media_translations_media_id', table_name='media_translations')
    op.drop_table('media_translations')

    op.drop_column('user_profiles', 'metadata_language')
