"""add scrobble_connections table, revert nullable url/token

Revision ID: a1b2c3d4e5f6
Revises: f1a2b3c4d5e6
Create Date: 2026-05-08 00:00:00.000000

This migration supersedes the abandoned nullable_connection_url_token migration
(same revision ID). It cleans up any connections that were created without a URL
or token, reverts those columns to NOT NULL, and creates the scrobble_connections
table for webhook-only connections.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = 'a1b2c3d4e5f6'
down_revision: Union[str, Sequence[str], None] = 'f1a2b3c4d5e6'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Remove any connections left with NULL url/token from the abandoned stash migration
    op.execute("DELETE FROM media_server_connections WHERE url IS NULL OR token IS NULL")

    # Revert columns to NOT NULL in case the stash migration made them nullable
    op.alter_column('media_server_connections', 'url',   existing_type=sa.String(500), nullable=False)
    op.alter_column('media_server_connections', 'token', existing_type=sa.String(500), nullable=False)

    op.create_table(
        'scrobble_connections',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('user_id', sa.Integer(), sa.ForeignKey('users.id', ondelete='CASCADE'), nullable=False, index=True),
        sa.Column('type', sa.String(50), nullable=False),
        sa.Column('name', sa.String(255), nullable=False),
        sa.Column('server_user_id', sa.String(255), nullable=True),
        sa.Column('server_username', sa.String(255), nullable=True),
        sa.Column('created_at', sa.DateTime(), server_default=sa.func.now(), nullable=False),
    )


def downgrade() -> None:
    op.drop_table('scrobble_connections')
    op.alter_column('media_server_connections', 'url',   existing_type=sa.String(500), nullable=True)
    op.alter_column('media_server_connections', 'token', existing_type=sa.String(500), nullable=True)
