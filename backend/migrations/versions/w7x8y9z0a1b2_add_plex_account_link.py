"""Add plex_account_id / plex_username to users (Plex SSO link)

Revision ID: w7x8y9z0a1b2
Revises: v6w7x8y9z0a1
Create Date: 2026-06-08
"""
from alembic import op
import sqlalchemy as sa

revision = 'w7x8y9z0a1b2'
down_revision = 'v6w7x8y9z0a1'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column('users', sa.Column('plex_account_id', sa.String(length=64), nullable=True))
    op.add_column('users', sa.Column('plex_username', sa.String(length=255), nullable=True))
    op.create_unique_constraint('uq_users_plex_account_id', 'users', ['plex_account_id'])
    op.create_index('ix_users_plex_account_id', 'users', ['plex_account_id'])


def downgrade() -> None:
    op.drop_index('ix_users_plex_account_id', table_name='users')
    op.drop_constraint('uq_users_plex_account_id', 'users', type_='unique')
    op.drop_column('users', 'plex_username')
    op.drop_column('users', 'plex_account_id')
