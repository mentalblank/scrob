"""Backfill media.runtime from the cached TMDB payload

Revision ID: b2d3f4a5c6e7
Revises: a1c2e3f4b5d6
Create Date: 2026-08-02

"""
from typing import Sequence, Union

from alembic import op


revision: str = "b2d3f4a5c6e7"
down_revision: Union[str, Sequence[str], None] = "a1c2e3f4b5d6"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Enrichment stored runtime only inside tmdb_data, so the column every
    # consumer reads was left null on rows written before this release.
    op.execute(
        """
        UPDATE media
        SET runtime = (tmdb_data->>'runtime')::int
        WHERE runtime IS NULL
          AND tmdb_data ? 'runtime'
          AND tmdb_data->>'runtime' ~ '^[0-9]+$'
          AND (tmdb_data->>'runtime')::int > 0
        """
    )


def downgrade() -> None:
    pass
