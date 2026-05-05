"""add blocklist items

Revision ID: 0010_add_blocklist_items
Revises: d4e5f6a7b8c9
Create Date: 2026-05-05 13:08:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '0010_add_blocklist_items'
down_revision: Union[str, Sequence[str], None] = 'd4e5f6a7b8c9'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table('blocklist_items',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('user_id', sa.Integer(), nullable=False),
        sa.Column('tmdb_id', sa.Integer(), nullable=False),
        sa.Column('media_type', postgresql.ENUM('movie', 'series', 'episode', 'person', name='mediatype', create_type=False), nullable=False),
        sa.Column('created_at', sa.DateTime(), server_default=sa.text('now()'), nullable=False),
        sa.ForeignKeyConstraint(['user_id'], ['users.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index('idx_blocklist_user_tmdb_type', 'blocklist_items', ['user_id', 'tmdb_id', 'media_type'], unique=True)


def downgrade() -> None:
    op.drop_index('idx_blocklist_user_tmdb_type', table_name='blocklist_items')
    op.drop_table('blocklist_items')
