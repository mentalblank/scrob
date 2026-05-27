"""Migrate next_up_hidden_shows to blocklist_items and drop column

Revision ID: n1o2p3q4r5s6
Revises: 938835b238ae
Create Date: 2026-05-27 10:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = 'n1o2p3q4r5s6'
down_revision: Union[str, Sequence[str], None] = '11f34ec77054'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Migrate existing next_up_hidden_shows data into blocklist_items as dropped shows.
    # The next_up_hidden_shows column stores internal show DB IDs (show.id).
    # We need to map these to tmdb_id from the shows table (which can be negative for TVDB-only shows).
    bind = op.get_bind()

    # Fetch all users with non-empty next_up_hidden_shows
    rows = bind.execute(
        sa.text("""
            SELECT us.user_id, us.next_up_hidden_shows
            FROM user_settings us
            WHERE us.next_up_hidden_shows IS NOT NULL
              AND jsonb_array_length(us.next_up_hidden_shows) > 0
        """)
    ).fetchall()

    for user_id, hidden_show_ids in rows:
        if not hidden_show_ids:
            continue
        # Resolve internal show IDs to tmdb_ids
        show_rows = bind.execute(
            sa.text(
                "SELECT id, tmdb_id FROM shows WHERE id = ANY(:ids)"
            ),
            {"ids": list(hidden_show_ids)},
        ).fetchall()

        for show_id, tmdb_id in show_rows:
            if tmdb_id is None:
                continue
            # Insert as dropped show in blocklist_items (ignore conflicts — already blocked)
            bind.execute(
                sa.text("""
                    INSERT INTO blocklist_items (user_id, tmdb_id, media_type, is_dropped, created_at)
                    VALUES (:user_id, :tmdb_id, 'series', true, NOW())
                    ON CONFLICT (user_id, tmdb_id, media_type) DO UPDATE
                      SET is_dropped = true
                """),
                {"user_id": user_id, "tmdb_id": tmdb_id},
            )

    # Drop the now-redundant column
    op.drop_column('user_settings', 'next_up_hidden_shows')


def downgrade() -> None:
    op.add_column(
        'user_settings',
        sa.Column(
            'next_up_hidden_shows',
            postgresql.JSONB(astext_type=sa.Text()),
            server_default='[]',
            nullable=True,
        ),
    )
    # Note: data restoration from blocklist_items is not attempted on downgrade
    # since we can't distinguish migrated entries from manually blocked shows.
