"""Backfill media uri_id from stored TMDB ids

Revision ID: d4f5a6b7c8e9
Revises: c3e4f5a6b7d8
Create Date: 2026-08-02

"""
from typing import Sequence, Union

from alembic import op


revision: str = "d4f5a6b7c8e9"
down_revision: Union[str, Sequence[str], None] = "c3e4f5a6b7d8"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # The URI refactor added the column without backfilling it. Episodes created
    # from TVDB keep a TVDB id in tmdb_id (tmdb_data.source = 'tvdb'), so they
    # get a tvdb uri instead of a wrong tmdb one.
    op.execute(
        """
        UPDATE media
        SET uri_id = 'tmdb:m:' || tmdb_id
        WHERE uri_id IS NULL AND tmdb_id IS NOT NULL AND media_type = 'movie'
        """
    )
    op.execute(
        """
        UPDATE media
        SET uri_id = 'tmdb:e:' || tmdb_id
        WHERE uri_id IS NULL AND tmdb_id IS NOT NULL AND media_type = 'episode'
          AND (tmdb_data->>'source' IS DISTINCT FROM 'tvdb')
        """
    )
    op.execute(
        """
        UPDATE media
        SET uri_id = 'tvdb:e:' || tmdb_id
        WHERE uri_id IS NULL AND tmdb_id IS NOT NULL AND media_type = 'episode'
          AND tmdb_data->>'source' = 'tvdb'
        """
    )
    op.execute(
        """
        UPDATE media
        SET uri_id = 'tmdb:s:' || tmdb_id
        WHERE uri_id IS NULL AND tmdb_id IS NOT NULL AND media_type = 'series'
        """
    )


def downgrade() -> None:
    pass
