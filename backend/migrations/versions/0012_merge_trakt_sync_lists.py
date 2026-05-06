"""merge trakt sync lists with fork chain

Revision ID: 0012_merge_trakt_sync_lists
Revises: 0011_add_list_integrations, e1f2a3b4c5d6
Create Date: 2026-05-06

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '0012_merge_trakt_sync_lists'
down_revision: Union[str, Sequence[str], None] = ('0011_add_list_integrations', 'e1f2a3b4c5d6')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
