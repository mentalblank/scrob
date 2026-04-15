"""Initial schema

Revision ID: 0001_initial_schema
Revises:
Create Date: 2026-04-15
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision: str = '0001_initial_schema'
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ── Enums ──────────────────────────────────────────────────────────────────
    postgresql.ENUM('admin', 'user', name='userrole').create(op.get_bind())
    postgresql.ENUM('movie', 'series', 'episode', 'person', name='mediatype').create(op.get_bind())
    postgresql.ENUM('jellyfin', 'emby', 'plex', 'trakt', 'manual', name='collectionsource').create(op.get_bind())
    postgresql.ENUM('public', 'friends_only', 'private', name='privacylevel').create(op.get_bind())
    postgresql.ENUM('pending', 'running', 'completed', 'failed', name='syncstatus').create(op.get_bind())

    # ── Users ──────────────────────────────────────────────────────────────────
    op.create_table('users',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('email', sa.String(255), nullable=False),
        sa.Column('username', sa.String(100), nullable=False),
        sa.Column('password_hash', sa.String(255), nullable=False),
        sa.Column('api_key', sa.String(64), nullable=False),
        sa.Column('role', postgresql.ENUM(name='userrole', create_type=False), nullable=False),
        sa.Column('email_confirmed', sa.Boolean(), nullable=False, server_default='true'),
        sa.Column('totp_enabled', sa.Boolean(), nullable=False, server_default='false'),
        sa.Column('totp_secret', sa.String(255), nullable=True),
        sa.Column('created_at', sa.DateTime(), server_default=sa.text('now()'), nullable=False),
        sa.Column('updated_at', sa.DateTime(), server_default=sa.text('now()'), nullable=False),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('email'),
        sa.UniqueConstraint('username'),
        sa.UniqueConstraint('api_key'),
    )

    op.create_table('totp_backup_codes',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('user_id', sa.Integer(), nullable=False),
        sa.Column('code', sa.String(20), nullable=False),
        sa.Column('used', sa.Boolean(), nullable=False, server_default='false'),
        sa.ForeignKeyConstraint(['user_id'], ['users.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_index('ix_totp_backup_codes_user_id', 'totp_backup_codes', ['user_id'])

    op.create_table('user_profiles',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('user_id', sa.Integer(), nullable=False),
        sa.Column('display_name', sa.String(64), nullable=True),
        sa.Column('bio', sa.String(280), nullable=True),
        sa.Column('country', sa.String(2), nullable=True),
        sa.Column('movie_genres', sa.JSON(), nullable=True),
        sa.Column('show_genres', sa.JSON(), nullable=True),
        sa.Column('streaming_services', sa.JSON(), nullable=True),
        sa.Column('content_language', sa.String(10), nullable=True),
        sa.Column('privacy_level', postgresql.ENUM(name='privacylevel', create_type=False), nullable=False, server_default='private'),
        sa.Column('avatar_path', sa.String(255), nullable=True),
        sa.ForeignKeyConstraint(['user_id'], ['users.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('user_id'),
    )
    op.create_index('ix_user_profiles_user_id', 'user_profiles', ['user_id'])

    op.create_table('email_activations',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('user_id', sa.Integer(), nullable=False),
        sa.Column('email', sa.String(255), nullable=False),
        sa.Column('token', sa.String(64), nullable=False),
        sa.Column('created_at', sa.DateTime(), server_default=sa.text('now()'), nullable=False),
        sa.ForeignKeyConstraint(['user_id'], ['users.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('token'),
    )
    op.create_index('ix_email_activations_user_id', 'email_activations', ['user_id'])

    op.create_table('password_reset_tokens',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('user_id', sa.Integer(), nullable=False),
        sa.Column('token', sa.String(64), nullable=False),
        sa.Column('created_at', sa.DateTime(), server_default=sa.text('now()'), nullable=False),
        sa.ForeignKeyConstraint(['user_id'], ['users.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('token'),
    )
    op.create_index('ix_password_reset_tokens_user_id', 'password_reset_tokens', ['user_id'])

    # ── Global settings ────────────────────────────────────────────────────────
    op.create_table('app_settings',
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

    op.create_table('user_settings',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('user_id', sa.Integer(), nullable=False),
        sa.Column('tmdb_api_key', sa.String(255), nullable=True),
        # Jellyfin
        sa.Column('jellyfin_url', sa.String(500), nullable=True),
        sa.Column('jellyfin_token', sa.String(500), nullable=True),
        sa.Column('jellyfin_user_id', sa.String(255), nullable=True),
        # Emby
        sa.Column('emby_url', sa.String(500), nullable=True),
        sa.Column('emby_token', sa.String(500), nullable=True),
        sa.Column('emby_user_id', sa.String(255), nullable=True),
        # Plex
        sa.Column('plex_url', sa.String(500), nullable=True),
        sa.Column('plex_token', sa.String(500), nullable=True),
        sa.Column('plex_username', sa.String(255), nullable=True),
        # Radarr
        sa.Column('radarr_url', sa.String(500), nullable=True),
        sa.Column('radarr_token', sa.String(500), nullable=True),
        sa.Column('radarr_root_folder', sa.String(500), nullable=True),
        sa.Column('radarr_quality_profile', sa.Integer(), nullable=True),
        sa.Column('radarr_tags', sa.JSON(), nullable=True),
        # Sonarr
        sa.Column('sonarr_url', sa.String(500), nullable=True),
        sa.Column('sonarr_token', sa.String(500), nullable=True),
        sa.Column('sonarr_root_folder', sa.String(500), nullable=True),
        sa.Column('sonarr_quality_profile', sa.Integer(), nullable=True),
        sa.Column('sonarr_tags', sa.JSON(), nullable=True),
        sa.Column('sonarr_season_folder', sa.Boolean(), nullable=False, server_default='true'),
        # Inbound sync flags
        sa.Column('plex_sync_collection', sa.Boolean(), nullable=False, server_default='true'),
        sa.Column('plex_sync_watched', sa.Boolean(), nullable=False, server_default='true'),
        sa.Column('plex_sync_ratings', sa.Boolean(), nullable=False, server_default='true'),
        sa.Column('plex_sync_playback', sa.Boolean(), nullable=False, server_default='true'),
        sa.Column('jellyfin_sync_collection', sa.Boolean(), nullable=False, server_default='true'),
        sa.Column('jellyfin_sync_watched', sa.Boolean(), nullable=False, server_default='true'),
        sa.Column('jellyfin_sync_ratings', sa.Boolean(), nullable=False, server_default='true'),
        sa.Column('jellyfin_sync_playback', sa.Boolean(), nullable=False, server_default='true'),
        sa.Column('emby_sync_collection', sa.Boolean(), nullable=False, server_default='true'),
        sa.Column('emby_sync_watched', sa.Boolean(), nullable=False, server_default='true'),
        sa.Column('emby_sync_ratings', sa.Boolean(), nullable=False, server_default='true'),
        sa.Column('emby_sync_playback', sa.Boolean(), nullable=False, server_default='true'),
        # Outbound push flags
        sa.Column('plex_push_watched', sa.Boolean(), nullable=False, server_default='false'),
        sa.Column('plex_push_ratings', sa.Boolean(), nullable=False, server_default='false'),
        sa.Column('jellyfin_push_watched', sa.Boolean(), nullable=False, server_default='false'),
        sa.Column('jellyfin_push_ratings', sa.Boolean(), nullable=False, server_default='false'),
        sa.Column('emby_push_watched', sa.Boolean(), nullable=False, server_default='false'),
        sa.Column('emby_push_ratings', sa.Boolean(), nullable=False, server_default='false'),
        # Trakt
        sa.Column('trakt_client_id', sa.String(255), nullable=True),
        sa.Column('trakt_client_secret', sa.String(255), nullable=True),
        sa.Column('trakt_access_token', sa.String(2000), nullable=True),
        sa.Column('trakt_refresh_token', sa.String(2000), nullable=True),
        sa.Column('trakt_token_expires_at', sa.BigInteger(), nullable=True),
        sa.Column('trakt_device_code', sa.String(255), nullable=True),
        sa.Column('trakt_sync_watched', sa.Boolean(), nullable=False, server_default='true'),
        sa.Column('trakt_sync_ratings', sa.Boolean(), nullable=False, server_default='true'),
        sa.Column('trakt_push_watched', sa.Boolean(), nullable=False, server_default='false'),
        sa.Column('trakt_push_ratings', sa.Boolean(), nullable=False, server_default='false'),
        sa.Column('preferences', sa.JSON(), nullable=True),
        sa.ForeignKeyConstraint(['user_id'], ['users.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('user_id'),
    )

    # ── Media catalogue ────────────────────────────────────────────────────────
    op.create_table('shows',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('tmdb_id', sa.Integer(), nullable=False),
        sa.Column('title', sa.String(500), nullable=False),
        sa.Column('original_title', sa.String(500), nullable=True),
        sa.Column('overview', sa.Text(), nullable=True),
        sa.Column('poster_path', sa.String(500), nullable=True),
        sa.Column('backdrop_path', sa.String(500), nullable=True),
        sa.Column('tmdb_rating', sa.Float(), nullable=True),
        sa.Column('status', sa.String(100), nullable=True),
        sa.Column('tagline', sa.Text(), nullable=True),
        sa.Column('first_air_date', sa.String(20), nullable=True),
        sa.Column('last_air_date', sa.String(20), nullable=True),
        sa.Column('tmdb_data', sa.JSON(), nullable=True),
        sa.Column('created_at', sa.DateTime(), server_default=sa.text('now()'), nullable=False),
        sa.Column('updated_at', sa.DateTime(), server_default=sa.text('now()'), nullable=False),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('tmdb_id'),
    )

    op.create_table('media',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('tmdb_id', sa.Integer(), nullable=True),
        sa.Column('media_type', postgresql.ENUM(name='mediatype', create_type=False), nullable=False),
        sa.Column('title', sa.String(500), nullable=False),
        sa.Column('original_title', sa.String(500), nullable=True),
        sa.Column('overview', sa.Text(), nullable=True),
        sa.Column('poster_path', sa.String(500), nullable=True),
        sa.Column('backdrop_path', sa.String(500), nullable=True),
        sa.Column('release_date', sa.String(20), nullable=True),
        sa.Column('runtime', sa.Integer(), nullable=True),
        sa.Column('tmdb_rating', sa.Float(), nullable=True),
        sa.Column('tagline', sa.Text(), nullable=True),
        sa.Column('status', sa.String(100), nullable=True),
        sa.Column('tmdb_data', sa.JSON(), nullable=True),
        sa.Column('show_id', sa.Integer(), nullable=True),
        sa.Column('season_number', sa.Integer(), nullable=True),
        sa.Column('episode_number', sa.Integer(), nullable=True),
        sa.Column('created_at', sa.DateTime(), server_default=sa.text('now()'), nullable=False),
        sa.Column('updated_at', sa.DateTime(), server_default=sa.text('now()'), nullable=False),
        sa.ForeignKeyConstraint(['show_id'], ['shows.id'], ondelete='SET NULL'),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_index('idx_media_tmdb_type', 'media', ['tmdb_id', 'media_type'])
    op.create_index('idx_media_show_season_episode', 'media', ['show_id', 'season_number', 'episode_number'])

    # ── Collections ────────────────────────────────────────────────────────────
    op.create_table('collections',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('user_id', sa.Integer(), nullable=False),
        sa.Column('media_id', sa.Integer(), nullable=False),
        sa.Column('added_at', sa.DateTime(), server_default=sa.text('now()'), nullable=False),
        sa.ForeignKeyConstraint(['user_id'], ['users.id'], ondelete='CASCADE'),
        sa.ForeignKeyConstraint(['media_id'], ['media.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('user_id', 'media_id', name='uq_collection_user_media'),
    )

    op.create_table('collection_files',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('collection_id', sa.Integer(), nullable=False),
        sa.Column('source', postgresql.ENUM(name='collectionsource', create_type=False), nullable=False),
        sa.Column('source_id', sa.String(255), nullable=True),
        sa.Column('resolution', sa.String(50), nullable=True),
        sa.Column('video_codec', sa.String(50), nullable=True),
        sa.Column('audio_codec', sa.String(50), nullable=True),
        sa.Column('audio_channels', sa.String(20), nullable=True),
        sa.Column('audio_languages', sa.JSON(), nullable=True),
        sa.Column('subtitle_languages', sa.JSON(), nullable=True),
        sa.Column('file_path', sa.String(1000), nullable=True),
        sa.Column('added_at', sa.DateTime(), server_default=sa.text('now()'), nullable=False),
        sa.ForeignKeyConstraint(['collection_id'], ['collections.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('collection_id', 'source', 'source_id', name='uq_collection_file_source'),
    )

    # ── Watch events & ratings ─────────────────────────────────────────────────
    op.create_table('watch_events',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('user_id', sa.Integer(), nullable=False),
        sa.Column('media_id', sa.Integer(), nullable=False),
        sa.Column('watched_at', sa.DateTime(), nullable=False),
        sa.Column('progress_seconds', sa.Integer(), nullable=True),
        sa.Column('progress_percent', sa.Float(), nullable=True),
        sa.Column('completed', sa.Boolean(), nullable=False, server_default='false'),
        sa.Column('play_count', sa.Integer(), nullable=False, server_default='1'),
        sa.ForeignKeyConstraint(['user_id'], ['users.id'], ondelete='CASCADE'),
        sa.ForeignKeyConstraint(['media_id'], ['media.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_index('idx_watch_events_user_media', 'watch_events', ['user_id', 'media_id'])

    op.create_table('ratings',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('user_id', sa.Integer(), nullable=False),
        sa.Column('media_id', sa.Integer(), nullable=False),
        sa.Column('season_number', sa.Integer(), nullable=True),
        sa.Column('rating', sa.Float(), nullable=True),
        sa.Column('review', sa.Text(), nullable=True),
        sa.Column('rated_at', sa.DateTime(), server_default=sa.text('now()'), nullable=False),
        sa.ForeignKeyConstraint(['user_id'], ['users.id'], ondelete='CASCADE'),
        sa.ForeignKeyConstraint(['media_id'], ['media.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
    )
    # Expression index — NULL season_number is treated as a distinct singleton via COALESCE
    op.execute(
        "CREATE UNIQUE INDEX uq_rating_user_media_season "
        "ON ratings (user_id, media_id, COALESCE(season_number, -1))"
    )

    # ── Lists ──────────────────────────────────────────────────────────────────
    op.create_table('lists',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('user_id', sa.Integer(), nullable=False),
        sa.Column('name', sa.String(255), nullable=False),
        sa.Column('description', sa.Text(), nullable=True),
        sa.Column('privacy_level', postgresql.ENUM(name='privacylevel', create_type=False), nullable=False, server_default='private'),
        sa.Column('created_at', sa.DateTime(), server_default=sa.text('now()'), nullable=False),
        sa.Column('updated_at', sa.DateTime(), server_default=sa.text('now()'), nullable=False),
        sa.ForeignKeyConstraint(['user_id'], ['users.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
    )

    op.create_table('list_items',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('list_id', sa.Integer(), nullable=False),
        sa.Column('media_id', sa.Integer(), nullable=False),
        sa.Column('added_at', sa.DateTime(), server_default=sa.text('now()'), nullable=False),
        sa.Column('sort_order', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('notes', sa.Text(), nullable=True),
        sa.ForeignKeyConstraint(['list_id'], ['lists.id'], ondelete='CASCADE'),
        sa.ForeignKeyConstraint(['media_id'], ['media.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('list_id', 'media_id', name='uq_list_item'),
    )

    # ── Sync ───────────────────────────────────────────────────────────────────
    op.create_table('sync_jobs',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('user_id', sa.Integer(), nullable=False),
        sa.Column('source', postgresql.ENUM(name='collectionsource', create_type=False), nullable=False),
        sa.Column('status', postgresql.ENUM(name='syncstatus', create_type=False), nullable=False, server_default='pending'),
        sa.Column('total_items', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('processed_items', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('errors', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('error_message', sa.String(1000), nullable=True),
        sa.Column('stats', sa.JSON(), nullable=True),
        sa.Column('warnings', sa.JSON(), nullable=True),
        sa.Column('created_at', sa.DateTime(), server_default=sa.text('now()'), nullable=False),
        sa.Column('updated_at', sa.DateTime(), server_default=sa.text('now()'), nullable=False),
        sa.ForeignKeyConstraint(['user_id'], ['users.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
    )

    # ── Library selections ─────────────────────────────────────────────────────
    op.create_table('jellyfin_library_selections',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('user_id', sa.Integer(), nullable=False),
        sa.Column('library_id', sa.String(255), nullable=False),
        sa.Column('library_name', sa.String(500), nullable=False),
        sa.ForeignKeyConstraint(['user_id'], ['users.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('user_id', 'library_id'),
    )

    op.create_table('emby_library_selections',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('user_id', sa.Integer(), nullable=False),
        sa.Column('library_id', sa.String(255), nullable=False),
        sa.Column('library_name', sa.String(500), nullable=False),
        sa.ForeignKeyConstraint(['user_id'], ['users.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('user_id', 'library_id', name='uq_emby_library_user'),
    )

    op.create_table('plex_library_selections',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('user_id', sa.Integer(), nullable=False),
        sa.Column('library_key', sa.String(255), nullable=False),
        sa.Column('library_name', sa.String(500), nullable=False),
        sa.ForeignKeyConstraint(['user_id'], ['users.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('user_id', 'library_key'),
    )

    # ── Playback ───────────────────────────────────────────────────────────────
    op.create_table('playback_sessions',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('user_id', sa.Integer(), nullable=False),
        sa.Column('media_id', sa.Integer(), nullable=False),
        sa.Column('session_key', sa.String(255), nullable=False),
        sa.Column('source', sa.String(16), nullable=False),
        sa.Column('state', sa.String(16), nullable=False, server_default='playing'),
        sa.Column('progress_percent', sa.Float(), nullable=False, server_default='0'),
        sa.Column('progress_seconds', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('started_at', sa.DateTime(), nullable=False),
        sa.Column('updated_at', sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(['user_id'], ['users.id'], ondelete='CASCADE'),
        sa.ForeignKeyConstraint(['media_id'], ['media.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('session_key'),
    )
    op.create_index('idx_playback_sessions_session_key', 'playback_sessions', ['session_key'])

    op.create_table('playback_progress',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('user_id', sa.Integer(), nullable=False),
        sa.Column('media_id', sa.Integer(), nullable=False),
        sa.Column('progress_percent', sa.Float(), nullable=False, server_default='0'),
        sa.Column('progress_seconds', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('updated_at', sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(['user_id'], ['users.id'], ondelete='CASCADE'),
        sa.ForeignKeyConstraint(['media_id'], ['media.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('user_id', 'media_id', name='uq_playback_progress_user_media'),
    )
    op.create_index('idx_playback_progress_user_media', 'playback_progress', ['user_id', 'media_id'])

    # ── Social ─────────────────────────────────────────────────────────────────
    op.create_table('follows',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('follower_id', sa.Integer(), nullable=False),
        sa.Column('following_id', sa.Integer(), nullable=False),
        sa.Column('created_at', sa.DateTime(), server_default=sa.text('now()'), nullable=False),
        sa.ForeignKeyConstraint(['follower_id'], ['users.id'], ondelete='CASCADE'),
        sa.ForeignKeyConstraint(['following_id'], ['users.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('follower_id', 'following_id', name='uq_follow'),
    )
    op.create_index('ix_follows_follower_id', 'follows', ['follower_id'])
    op.create_index('ix_follows_following_id', 'follows', ['following_id'])

    op.create_table('comments',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('user_id', sa.Integer(), nullable=False),
        sa.Column('media_type', sa.String(50), nullable=False),
        sa.Column('tmdb_id', sa.Integer(), nullable=False),
        sa.Column('season_number', sa.Integer(), nullable=True),
        sa.Column('episode_number', sa.Integer(), nullable=True),
        sa.Column('content', sa.Text(), nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(['user_id'], ['users.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_index('idx_comments_media', 'comments', ['media_type', 'tmdb_id', 'season_number', 'episode_number'])
    op.create_index('ix_comments_user_id', 'comments', ['user_id'])


def downgrade() -> None:
    op.drop_table('comments')
    op.drop_table('follows')
    op.drop_table('playback_progress')
    op.drop_table('playback_sessions')
    op.drop_table('plex_library_selections')
    op.drop_table('emby_library_selections')
    op.drop_table('jellyfin_library_selections')
    op.drop_table('sync_jobs')
    op.drop_table('list_items')
    op.drop_table('lists')
    op.drop_table('ratings')
    op.drop_table('watch_events')
    op.drop_table('collection_files')
    op.drop_table('collections')
    op.drop_table('media')
    op.drop_table('shows')
    op.drop_table('password_reset_tokens')
    op.drop_table('email_activations')
    op.drop_table('user_settings')
    op.drop_table('app_settings')
    op.drop_table('user_profiles')
    op.drop_table('totp_backup_codes')
    op.drop_table('users')

    postgresql.ENUM(name='syncstatus').drop(op.get_bind())
    postgresql.ENUM(name='privacylevel').drop(op.get_bind())
    postgresql.ENUM(name='collectionsource').drop(op.get_bind())
    postgresql.ENUM(name='mediatype').drop(op.get_bind())
    postgresql.ENUM(name='userrole').drop(op.get_bind())
