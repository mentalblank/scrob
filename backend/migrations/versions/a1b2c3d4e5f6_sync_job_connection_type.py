"""add connection_id and job_type to sync_jobs

Revision ID: c4d5e6f7a8b9
Revises: b2c3d4e5f6a7
Create Date: 2026-05-13 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = 'c4d5e6f7a8b9'
down_revision: Union[str, Sequence[str], None] = 'b2c3d4e5f6a7'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('sync_jobs', sa.Column('connection_id', sa.Integer(), nullable=True))
    op.add_column('sync_jobs', sa.Column('job_type', sa.String(20), nullable=False, server_default='pull'))
    op.create_foreign_key(
        'fk_sync_jobs_connection_id',
        'sync_jobs', 'media_server_connections',
        ['connection_id'], ['id'],
        ondelete='SET NULL',
    )


def downgrade() -> None:
    op.drop_constraint('fk_sync_jobs_connection_id', 'sync_jobs', type_='foreignkey')
    op.drop_column('sync_jobs', 'job_type')
    op.drop_column('sync_jobs', 'connection_id')
