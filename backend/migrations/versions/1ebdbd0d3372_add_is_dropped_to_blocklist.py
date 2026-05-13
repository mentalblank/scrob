"""add is_dropped to blocklist

Revision ID: 1ebdbd0d3372
Revises: e2f3a4b5c6d7
Create Date: 2026-05-13 19:26:02.468982

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '1ebdbd0d3372'
down_revision: Union[str, Sequence[str], None] = 'e2f3a4b5c6d7'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column('blocklist_items', sa.Column('is_dropped', sa.Boolean(), server_default='0', nullable=False))


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column('blocklist_items', 'is_dropped')
