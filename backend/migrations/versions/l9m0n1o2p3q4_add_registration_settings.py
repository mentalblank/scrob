"""Add registration controls to global_settings

Registration was only configurable via ENABLE_REGISTRATIONS /
REGISTRATION_MAX_ALLOWED_USERS, which meant a restart to change. These columns
let an admin toggle it from the UI. NULL means "fall back to the env var", so
existing deployments keep behaving exactly as configured until an admin changes it.

Revision ID: l9m0n1o2p3q4
Revises: k8l9m0n1o2p3
Create Date: 2026-07-31
"""
from alembic import op
import sqlalchemy as sa

revision = 'l9m0n1o2p3q4'
down_revision = 'k8l9m0n1o2p3'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column('global_settings', sa.Column('enable_registrations', sa.Boolean(), nullable=True))
    op.add_column('global_settings', sa.Column('registration_max_allowed_users', sa.Integer(), nullable=True))


def downgrade() -> None:
    op.drop_column('global_settings', 'registration_max_allowed_users')
    op.drop_column('global_settings', 'enable_registrations')
