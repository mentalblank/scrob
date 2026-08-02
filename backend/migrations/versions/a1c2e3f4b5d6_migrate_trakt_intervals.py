"""Carry fork Trakt sync intervals into the shared cloud auto-sync fields

Revision ID: a1c2e3f4b5d6
Revises: f6a14561cab0
Create Date: 2026-08-02

"""
from typing import Sequence, Union

from alembic import op


revision: str = "a1c2e3f4b5d6"
down_revision: Union[str, Sequence[str], None] = "f6a14561cab0"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # The fork scheduled Trakt off trakt_full_sync_interval / trakt_partial_sync_interval;
    # upstream's scheduler reads trakt_auto_sync_interval instead. Without this copy,
    # every user with Trakt auto-sync configured silently stops syncing after the merge.
    op.execute(
        """
        UPDATE user_settings
        SET trakt_auto_sync_interval = COALESCE(
            trakt_partial_sync_interval::double precision,
            trakt_full_sync_interval::double precision
        )
        WHERE trakt_auto_sync_interval IS NULL
          AND (trakt_partial_sync_interval IS NOT NULL OR trakt_full_sync_interval IS NOT NULL)
        """
    )


def downgrade() -> None:
    pass
