"""add media_requests table and approval flags

Revision ID: i3j4k5l6m7n8
Revises: h2i3j4k5l6m7
Create Date: 2026-05-20
"""
from alembic import op
import sqlalchemy as sa

revision = 'i3j4k5l6m7n8'
down_revision = 'h2i3j4k5l6m7'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("CREATE TYPE requeststatus AS ENUM ('pending', 'approved', 'rejected')")
    op.execute("""
        CREATE TABLE media_requests (
            id          SERIAL PRIMARY KEY,
            user_id     INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            tmdb_id     INTEGER NOT NULL,
            media_type  VARCHAR(10) NOT NULL,
            title       VARCHAR(500) NOT NULL DEFAULT '',
            poster_path VARCHAR(500),
            status      requeststatus NOT NULL DEFAULT 'pending',
            reviewed_by INTEGER REFERENCES users(id) ON DELETE SET NULL,
            created_at  TIMESTAMP NOT NULL DEFAULT NOW(),
            updated_at  TIMESTAMP NOT NULL DEFAULT NOW()
        )
    """)
    op.execute("CREATE INDEX ix_media_requests_user_tmdb ON media_requests (user_id, tmdb_id, media_type)")
    op.add_column('global_settings', sa.Column('radarr_require_approval', sa.Boolean(), nullable=False, server_default='false'))
    op.add_column('global_settings', sa.Column('sonarr_require_approval', sa.Boolean(), nullable=False, server_default='false'))


def downgrade() -> None:
    op.drop_column('global_settings', 'sonarr_require_approval')
    op.drop_column('global_settings', 'radarr_require_approval')
    op.drop_table('media_requests')
    op.execute("DROP TYPE requeststatus")
