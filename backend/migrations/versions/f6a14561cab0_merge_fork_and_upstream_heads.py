"""merge fork and upstream heads

Revision ID: f6a14561cab0
Revises: 8c3d0e5f7a2b, l9m0n1o2p3q4
Create Date: 2026-08-02 18:16:42.344875

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'f6a14561cab0'
down_revision: Union[str, Sequence[str], None] = ('8c3d0e5f7a2b', 'l9m0n1o2p3q4')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
