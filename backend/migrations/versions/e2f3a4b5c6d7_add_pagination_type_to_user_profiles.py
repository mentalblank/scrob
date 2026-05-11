"""Add pagination_type to user_profiles

Revision ID: e2f3a4b5c6d7
Revises: 99fbc5954be6
Create Date: 2026-05-12 01:25:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'e2f3a4b5c6d7'
down_revision: Union[str, Sequence[str], None] = '99fbc5954be6'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('user_profiles', sa.Column('pagination_type', sa.String(length=20), server_default='infinite_scroll', nullable=False))


def downgrade() -> None:
    op.drop_column('user_profiles', 'pagination_type')
