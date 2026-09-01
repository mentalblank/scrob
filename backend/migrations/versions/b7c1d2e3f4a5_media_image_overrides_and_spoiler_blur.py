"""per-user image overrides and unwatched-spoiler blur settings

Revision ID: b7c1d2e3f4a5
Revises: 3ac71e0d9f42
Create Date: 2026-09-05 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = 'b7c1d2e3f4a5'
down_revision: Union[str, Sequence[str], None] = '3ac71e0d9f42'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


_BLUR_COLUMNS = (
    'blur_unwatched_episode_images',
    'blur_unwatched_episode_overviews',
    'blur_unwatched_movie_images',
    'blur_unwatched_movie_overviews',
)


def upgrade() -> None:
    op.create_table(
        'media_image_overrides',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('user_id', sa.Integer(), nullable=False),
        sa.Column('subject_uri', sa.String(length=64), nullable=False),
        sa.Column('show_id', sa.Integer(), nullable=True),
        sa.Column('media_id', sa.Integer(), nullable=True),
        sa.Column('season_number', sa.Integer(), nullable=False, server_default='-1'),
        sa.Column('episode_number', sa.Integer(), nullable=False, server_default='-1'),
        sa.Column('image_kind', sa.String(length=16), nullable=False),
        sa.Column('image_path', sa.String(length=500), nullable=False),
        sa.Column('source', sa.String(length=16), nullable=False),
        sa.Column('source_url', sa.Text(), nullable=True),
        sa.Column('created_at', sa.DateTime(), server_default=sa.text('now()'), nullable=False),
        sa.Column('updated_at', sa.DateTime(), server_default=sa.text('now()'), nullable=False),
        sa.ForeignKeyConstraint(['user_id'], ['users.id'], ondelete='CASCADE'),
        sa.ForeignKeyConstraint(['show_id'], ['shows.id'], ondelete='CASCADE'),
        sa.ForeignKeyConstraint(['media_id'], ['media.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint(
            'user_id', 'subject_uri', 'season_number', 'episode_number', 'image_kind',
            name='uq_media_image_override',
        ),
    )
    op.create_index('idx_media_image_overrides_user', 'media_image_overrides', ['user_id'])

    for column in _BLUR_COLUMNS:
        op.add_column(
            'user_settings',
            sa.Column(column, sa.Boolean(), nullable=False, server_default='false'),
        )


def downgrade() -> None:
    for column in _BLUR_COLUMNS:
        op.drop_column('user_settings', column)
    op.drop_index('idx_media_image_overrides_user', table_name='media_image_overrides')
    op.drop_table('media_image_overrides')
