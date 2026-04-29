"""Introduce media_server_connections table; migrate per-user server settings out of user_settings

Revision ID: 0004_media_server_connections
Revises: 0003_auto_sync_intervals
Create Date: 2026-04-29
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = '0004_media_server_connections'
down_revision: Union[str, Sequence[str], None] = 'af55228ac709'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ── 1. Create media_server_connections ──────────────────────────────────────
    op.create_table(
        'media_server_connections',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('user_id', sa.Integer(), nullable=False),
        sa.Column('type', sa.String(50), nullable=False),
        sa.Column('name', sa.String(255), nullable=False),
        sa.Column('url', sa.String(500), nullable=False),
        sa.Column('token', sa.String(500), nullable=False),
        sa.Column('server_user_id', sa.String(255), nullable=True),
        sa.Column('server_username', sa.String(255), nullable=True),
        sa.Column('sync_collection', sa.Boolean(), nullable=False, server_default='true'),
        sa.Column('sync_watched',    sa.Boolean(), nullable=False, server_default='true'),
        sa.Column('sync_ratings',    sa.Boolean(), nullable=False, server_default='true'),
        sa.Column('sync_playback',   sa.Boolean(), nullable=False, server_default='true'),
        sa.Column('push_watched',    sa.Boolean(), nullable=False, server_default='false'),
        sa.Column('push_ratings',    sa.Boolean(), nullable=False, server_default='false'),
        sa.Column('auto_sync_interval', sa.Integer(), nullable=True),
        sa.Column('created_at', sa.DateTime(), server_default=sa.text('now()'), nullable=False),
        sa.ForeignKeyConstraint(['user_id'], ['users.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
        sa.CheckConstraint("type IN ('plex', 'jellyfin', 'emby')", name='ck_msc_type'),
    )
    op.create_index('ix_media_server_connections_user_id', 'media_server_connections', ['user_id'])

    # ── 2. Migrate existing settings into connection rows ────────────────────────
    conn = op.get_bind()

    conn.execute(sa.text("""
        INSERT INTO media_server_connections
            (user_id, type, name, url, token, server_user_id,
             sync_collection, sync_watched, sync_ratings, sync_playback,
             push_watched, push_ratings, auto_sync_interval, created_at)
        SELECT
            user_id, 'jellyfin', 'Jellyfin',
            jellyfin_url, jellyfin_token, jellyfin_user_id,
            COALESCE(jellyfin_sync_collection, TRUE),
            COALESCE(jellyfin_sync_watched, TRUE),
            COALESCE(jellyfin_sync_ratings, TRUE),
            COALESCE(jellyfin_sync_playback, TRUE),
            COALESCE(jellyfin_push_watched, FALSE),
            COALESCE(jellyfin_push_ratings, FALSE),
            jellyfin_auto_sync_interval,
            NOW()
        FROM user_settings
        WHERE jellyfin_url IS NOT NULL AND jellyfin_token IS NOT NULL
    """))

    conn.execute(sa.text("""
        INSERT INTO media_server_connections
            (user_id, type, name, url, token, server_user_id,
             sync_collection, sync_watched, sync_ratings, sync_playback,
             push_watched, push_ratings, auto_sync_interval, created_at)
        SELECT
            user_id, 'emby', 'Emby',
            emby_url, emby_token, emby_user_id,
            COALESCE(emby_sync_collection, TRUE),
            COALESCE(emby_sync_watched, TRUE),
            COALESCE(emby_sync_ratings, TRUE),
            COALESCE(emby_sync_playback, TRUE),
            COALESCE(emby_push_watched, FALSE),
            COALESCE(emby_push_ratings, FALSE),
            emby_auto_sync_interval,
            NOW()
        FROM user_settings
        WHERE emby_url IS NOT NULL AND emby_token IS NOT NULL
    """))

    conn.execute(sa.text("""
        INSERT INTO media_server_connections
            (user_id, type, name, url, token, server_username,
             sync_collection, sync_watched, sync_ratings, sync_playback,
             push_watched, push_ratings, auto_sync_interval, created_at)
        SELECT
            user_id, 'plex', 'Plex',
            plex_url, plex_token, plex_username,
            COALESCE(plex_sync_collection, TRUE),
            COALESCE(plex_sync_watched, TRUE),
            COALESCE(plex_sync_ratings, TRUE),
            COALESCE(plex_sync_playback, TRUE),
            COALESCE(plex_push_watched, FALSE),
            COALESCE(plex_push_ratings, FALSE),
            plex_auto_sync_interval,
            NOW()
        FROM user_settings
        WHERE plex_url IS NOT NULL AND plex_token IS NOT NULL
    """))

    # ── 3. Add connection_id FK to library selection tables ──────────────────────
    op.add_column('jellyfin_library_selections',
        sa.Column('connection_id', sa.Integer(), nullable=True))
    op.create_foreign_key(
        'fk_jls_connection', 'jellyfin_library_selections',
        'media_server_connections', ['connection_id'], ['id'], ondelete='CASCADE')

    op.add_column('emby_library_selections',
        sa.Column('connection_id', sa.Integer(), nullable=True))
    op.create_foreign_key(
        'fk_els_connection', 'emby_library_selections',
        'media_server_connections', ['connection_id'], ['id'], ondelete='CASCADE')

    op.add_column('plex_library_selections',
        sa.Column('connection_id', sa.Integer(), nullable=True))
    op.create_foreign_key(
        'fk_pls_connection', 'plex_library_selections',
        'media_server_connections', ['connection_id'], ['id'], ondelete='CASCADE')

    # ── 4. Populate connection_id on library selection rows ──────────────────────
    conn.execute(sa.text("""
        UPDATE jellyfin_library_selections jls
        SET connection_id = msc.id
        FROM media_server_connections msc
        WHERE msc.user_id = jls.user_id AND msc.type = 'jellyfin'
    """))

    conn.execute(sa.text("""
        UPDATE emby_library_selections els
        SET connection_id = msc.id
        FROM media_server_connections msc
        WHERE msc.user_id = els.user_id AND msc.type = 'emby'
    """))

    conn.execute(sa.text("""
        UPDATE plex_library_selections pls
        SET connection_id = msc.id
        FROM media_server_connections msc
        WHERE msc.user_id = pls.user_id AND msc.type = 'plex'
    """))

    # Delete orphaned selections (user had library selections but no matching server configured)
    conn.execute(sa.text("DELETE FROM jellyfin_library_selections WHERE connection_id IS NULL"))
    conn.execute(sa.text("DELETE FROM emby_library_selections WHERE connection_id IS NULL"))
    conn.execute(sa.text("DELETE FROM plex_library_selections WHERE connection_id IS NULL"))

    # Make connection_id NOT NULL now that data is populated
    op.alter_column('jellyfin_library_selections', 'connection_id', nullable=False)
    op.alter_column('emby_library_selections', 'connection_id', nullable=False)
    op.alter_column('plex_library_selections', 'connection_id', nullable=False)

    # ── 5. Replace unique constraints on library selections ──────────────────────
    op.drop_constraint('jellyfin_library_selections_user_id_library_id_key', 'jellyfin_library_selections', type_='unique')
    op.create_unique_constraint('uq_jls_conn_lib', 'jellyfin_library_selections', ['connection_id', 'library_id'])

    op.drop_constraint('uq_emby_library_user', 'emby_library_selections', type_='unique')
    op.create_unique_constraint('uq_els_conn_lib', 'emby_library_selections', ['connection_id', 'library_id'])

    op.drop_constraint('plex_library_selections_user_id_library_key_key', 'plex_library_selections', type_='unique')
    op.create_unique_constraint('uq_pls_conn_lib', 'plex_library_selections', ['connection_id', 'library_key'])

    # ── 6. Drop old server credential + sync columns from user_settings ──────────
    op.drop_column('user_settings', 'jellyfin_url')
    op.drop_column('user_settings', 'jellyfin_token')
    op.drop_column('user_settings', 'jellyfin_user_id')
    op.drop_column('user_settings', 'emby_url')
    op.drop_column('user_settings', 'emby_token')
    op.drop_column('user_settings', 'emby_user_id')
    op.drop_column('user_settings', 'plex_url')
    op.drop_column('user_settings', 'plex_token')
    op.drop_column('user_settings', 'plex_username')
    op.drop_column('user_settings', 'jellyfin_sync_collection')
    op.drop_column('user_settings', 'jellyfin_sync_watched')
    op.drop_column('user_settings', 'jellyfin_sync_ratings')
    op.drop_column('user_settings', 'jellyfin_sync_playback')
    op.drop_column('user_settings', 'emby_sync_collection')
    op.drop_column('user_settings', 'emby_sync_watched')
    op.drop_column('user_settings', 'emby_sync_ratings')
    op.drop_column('user_settings', 'emby_sync_playback')
    op.drop_column('user_settings', 'plex_sync_collection')
    op.drop_column('user_settings', 'plex_sync_watched')
    op.drop_column('user_settings', 'plex_sync_ratings')
    op.drop_column('user_settings', 'plex_sync_playback')
    op.drop_column('user_settings', 'jellyfin_push_watched')
    op.drop_column('user_settings', 'jellyfin_push_ratings')
    op.drop_column('user_settings', 'emby_push_watched')
    op.drop_column('user_settings', 'emby_push_ratings')
    op.drop_column('user_settings', 'plex_push_watched')
    op.drop_column('user_settings', 'plex_push_ratings')
    op.drop_column('user_settings', 'jellyfin_auto_sync_interval')
    op.drop_column('user_settings', 'emby_auto_sync_interval')
    op.drop_column('user_settings', 'plex_auto_sync_interval')

    # ── 7. Drop unused app_settings table ───────────────────────────────────────
    op.drop_table('app_settings')


def downgrade() -> None:
    # Restore app_settings
    op.create_table(
        'app_settings',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('jellyfin_url', sa.String(500), nullable=True),
        sa.Column('jellyfin_token', sa.String(500), nullable=True),
        sa.Column('jellyfin_user_id', sa.String(255), nullable=True),
        sa.Column('emby_url', sa.String(500), nullable=True),
        sa.Column('emby_token', sa.String(500), nullable=True),
        sa.Column('emby_user_id', sa.String(255), nullable=True),
        sa.Column('plex_url', sa.String(500), nullable=True),
        sa.Column('plex_token', sa.String(500), nullable=True),
        sa.Column('updated_at', sa.DateTime(), server_default=sa.text('now()'), nullable=False),
        sa.PrimaryKeyConstraint('id'),
    )

    # Restore columns to user_settings
    for col, typ in [
        ('jellyfin_url', sa.String(500)), ('jellyfin_token', sa.String(500)),
        ('jellyfin_user_id', sa.String(255)),
        ('emby_url', sa.String(500)), ('emby_token', sa.String(500)),
        ('emby_user_id', sa.String(255)),
        ('plex_url', sa.String(500)), ('plex_token', sa.String(500)),
        ('plex_username', sa.String(255)),
    ]:
        op.add_column('user_settings', sa.Column(col, typ, nullable=True))

    for col in [
        'jellyfin_sync_collection', 'jellyfin_sync_watched', 'jellyfin_sync_ratings', 'jellyfin_sync_playback',
        'emby_sync_collection', 'emby_sync_watched', 'emby_sync_ratings', 'emby_sync_playback',
        'plex_sync_collection', 'plex_sync_watched', 'plex_sync_ratings', 'plex_sync_playback',
        'jellyfin_push_watched', 'jellyfin_push_ratings',
        'emby_push_watched', 'emby_push_ratings',
        'plex_push_watched', 'plex_push_ratings',
    ]:
        op.add_column('user_settings', sa.Column(col, sa.Boolean(), nullable=False, server_default='true' if 'sync' in col else 'false'))

    for col in ['jellyfin_auto_sync_interval', 'emby_auto_sync_interval', 'plex_auto_sync_interval']:
        op.add_column('user_settings', sa.Column(col, sa.Integer(), nullable=True))

    # Restore data from connections back into user_settings (one per type per user)
    conn = op.get_bind()
    for src_type, prefix, user_id_col in [
        ('jellyfin', 'jellyfin', 'server_user_id'),
        ('emby',     'emby',     'server_user_id'),
        ('plex',     'plex',     'server_username'),
    ]:
        if src_type in ('jellyfin', 'emby'):
            conn.execute(sa.text(f"""
                UPDATE user_settings us
                SET {prefix}_url = msc.url,
                    {prefix}_token = msc.token,
                    {prefix}_user_id = msc.server_user_id,
                    {prefix}_sync_collection = msc.sync_collection,
                    {prefix}_sync_watched = msc.sync_watched,
                    {prefix}_sync_ratings = msc.sync_ratings,
                    {prefix}_sync_playback = msc.sync_playback,
                    {prefix}_push_watched = msc.push_watched,
                    {prefix}_push_ratings = msc.push_ratings,
                    {prefix}_auto_sync_interval = msc.auto_sync_interval
                FROM (
                    SELECT DISTINCT ON (user_id) *
                    FROM media_server_connections
                    WHERE type = '{src_type}'
                    ORDER BY user_id, id ASC
                ) msc
                WHERE us.user_id = msc.user_id
            """))
        else:
            conn.execute(sa.text(f"""
                UPDATE user_settings us
                SET plex_url = msc.url,
                    plex_token = msc.token,
                    plex_username = msc.server_username,
                    plex_sync_collection = msc.sync_collection,
                    plex_sync_watched = msc.sync_watched,
                    plex_sync_ratings = msc.sync_ratings,
                    plex_sync_playback = msc.sync_playback,
                    plex_push_watched = msc.push_watched,
                    plex_push_ratings = msc.push_ratings,
                    plex_auto_sync_interval = msc.auto_sync_interval
                FROM (
                    SELECT DISTINCT ON (user_id) *
                    FROM media_server_connections
                    WHERE type = 'plex'
                    ORDER BY user_id, id ASC
                ) msc
                WHERE us.user_id = msc.user_id
            """))

    # Restore library selection unique constraints
    op.drop_constraint('uq_jls_conn_lib', 'jellyfin_library_selections', type_='unique')
    op.create_unique_constraint(
        'jellyfin_library_selections_user_id_library_id_key',
        'jellyfin_library_selections', ['user_id', 'library_id'])

    op.drop_constraint('uq_els_conn_lib', 'emby_library_selections', type_='unique')
    op.create_unique_constraint(
        'uq_emby_library_user',
        'emby_library_selections', ['user_id', 'library_id'])

    op.drop_constraint('uq_pls_conn_lib', 'plex_library_selections', type_='unique')
    op.create_unique_constraint(
        'plex_library_selections_user_id_library_key_key',
        'plex_library_selections', ['user_id', 'library_key'])

    # Drop connection_id columns from library selections
    op.drop_constraint('fk_jls_connection', 'jellyfin_library_selections', type_='foreignkey')
    op.drop_column('jellyfin_library_selections', 'connection_id')
    op.drop_constraint('fk_els_connection', 'emby_library_selections', type_='foreignkey')
    op.drop_column('emby_library_selections', 'connection_id')
    op.drop_constraint('fk_pls_connection', 'plex_library_selections', type_='foreignkey')
    op.drop_column('plex_library_selections', 'connection_id')

    # Drop the connections table
    op.drop_index('ix_media_server_connections_user_id', 'media_server_connections')
    op.drop_table('media_server_connections')
