"""merge local and upstream migration heads

Both branches independently generated new migrations using the same
deterministic revision-id scheme after their common ancestor
(n8o9p0q1r2s3), producing 7 colliding revision ids. The upstream side
of that colliding stretch was renamed (suffixed with "u") to resolve
the collision; this migration reconciles the two now-divergent chains
back into a single head.

Revision ID: f3a4b5c6d7e8
Revises: w7x8y9z0a1b2, c7d8e9f0a1b2
Create Date: 2026-07-30
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = 'f3a4b5c6d7e8'
down_revision: Union[str, Sequence[str], None] = ('w7x8y9z0a1b2', 'c7d8e9f0a1b2')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
