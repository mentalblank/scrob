"""add_is_admin_to_users

Revision ID: c3a1b2d4e5f6
Revises: af55228ac709
Create Date: 2026-05-03 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'c3a1b2d4e5f6'
down_revision: Union[str, Sequence[str], None] = '0005_cf_connection_id'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('users', sa.Column('is_admin', sa.Boolean(), server_default='false', nullable=False))
    op.execute(
        "UPDATE users SET is_admin = true WHERE id = (SELECT MIN(id) FROM users)"
    )


def downgrade() -> None:
    op.drop_column('users', 'is_admin')
