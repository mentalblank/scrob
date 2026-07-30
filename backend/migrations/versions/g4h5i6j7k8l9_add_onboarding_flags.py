"""Add onboarding flags: user_settings.onboarded, global_settings.setup_completed

Revision ID: g4h5i6j7k8l9
Revises: f3a4b5c6d7e8
Create Date: 2026-07-30
"""
from alembic import op
import sqlalchemy as sa

revision = 'g4h5i6j7k8l9'
down_revision = 'f3a4b5c6d7e8'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column('user_settings', sa.Column('onboarded', sa.Boolean(), nullable=False, server_default='true'))
    op.add_column('global_settings', sa.Column('setup_completed', sa.Boolean(), nullable=False, server_default='true'))


def downgrade() -> None:
    op.drop_column('global_settings', 'setup_completed')
    op.drop_column('user_settings', 'onboarded')
