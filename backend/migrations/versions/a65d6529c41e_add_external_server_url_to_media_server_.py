"""Add external_server_url to media_server_connections

Revision ID: a65d6529c41e
Revises: j4k5l6m7n8o9
Create Date: 2026-05-23 14:25:45.665459

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = 'a65d6529c41e'
down_revision: Union[str, Sequence[str], None] = 'j4k5l6m7n8o9'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column('media_server_connections', sa.Column('external_server_url', sa.String(length=500), nullable=True))


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column('media_server_connections', 'external_server_url')
