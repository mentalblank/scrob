"""Backfill show uri_id from the provider ids already stored

Revision ID: c3e4f5a6b7d8
Revises: b2d3f4a5c6e7
Create Date: 2026-08-02

"""
from typing import Sequence, Union

from alembic import op


revision: str = "c3e4f5a6b7d8"
down_revision: Union[str, Sequence[str], None] = "b2d3f4a5c6e7"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Only the TVDB resolution path ever set uri_id, so lookups keyed on it
    # missed almost every show.
    op.execute(
        """
        UPDATE shows
        SET uri_id = 'tmdb:s:' || tmdb_id
        WHERE uri_id IS NULL AND tmdb_id IS NOT NULL
        """
    )
    op.execute(
        """
        UPDATE shows
        SET uri_id = 'tvdb:s:' || tvdb_id
        WHERE uri_id IS NULL AND tvdb_id IS NOT NULL
        """
    )


def downgrade() -> None:
    pass
