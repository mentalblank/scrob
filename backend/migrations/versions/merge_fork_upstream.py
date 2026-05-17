"""merge fork and upstream heads

Revision ID: merge_fork_upstream
Revises: ('1ebdbd0d3372', 'eb9cc9663187')
Create Date: 2026-05-17 10:15:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'merge_fork_upstream'
down_revision: Union[str, Sequence[str], None] = ('1ebdbd0d3372', 'eb9cc9663187')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
