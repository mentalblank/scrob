"""Replace ShowSeasonOverride/ShowEpisodeOverride tmdb_id columns with shows.id FK.

Revision ID: r2s3t4u5v6w7
Revises: q1r2s3t4u5v6
Create Date: 2026-05-28

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = 'r2s3t4u5v6w7'
down_revision: Union[str, Sequence[str], None] = 'q1r2s3t4u5v6'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ── show_season_overrides ────────────────────────────────────────────────────
    op.add_column('show_season_overrides', sa.Column('source_show_id', sa.Integer(), nullable=True))
    op.add_column('show_season_overrides', sa.Column('target_show_id', sa.Integer(), nullable=True))
    op.alter_column('show_season_overrides', 'source_show_tmdb_id', nullable=True)
    op.alter_column('show_season_overrides', 'target_show_tmdb_id', nullable=True)

    # Populate source_show_id from positive tmdb_id
    op.execute("""
        UPDATE show_season_overrides o
        SET source_show_id = (SELECT s.id FROM shows s WHERE s.tmdb_id = o.source_show_tmdb_id LIMIT 1)
        WHERE o.source_show_tmdb_id IS NOT NULL AND o.source_show_tmdb_id >= 0
    """)
    # Populate source_show_id from negative (tvdb) tmdb_id
    op.execute("""
        UPDATE show_season_overrides o
        SET source_show_id = (SELECT s.id FROM shows s WHERE s.tvdb_id = ABS(o.source_show_tmdb_id) LIMIT 1)
        WHERE o.source_show_tmdb_id IS NOT NULL AND o.source_show_tmdb_id < 0 AND o.source_show_id IS NULL
    """)
    # Populate target_show_id from positive tmdb_id
    op.execute("""
        UPDATE show_season_overrides o
        SET target_show_id = (SELECT s.id FROM shows s WHERE s.tmdb_id = o.target_show_tmdb_id LIMIT 1)
        WHERE o.target_show_tmdb_id IS NOT NULL AND o.target_show_tmdb_id >= 0
    """)
    op.execute("""
        UPDATE show_season_overrides o
        SET target_show_id = (SELECT s.id FROM shows s WHERE s.tvdb_id = ABS(o.target_show_tmdb_id) LIMIT 1)
        WHERE o.target_show_tmdb_id IS NOT NULL AND o.target_show_tmdb_id < 0 AND o.target_show_id IS NULL
    """)

    # Delete overrides where source show couldn't be resolved (orphans)
    op.execute("DELETE FROM show_season_overrides WHERE source_show_id IS NULL")

    op.alter_column('show_season_overrides', 'source_show_id', nullable=False)

    op.create_foreign_key('fk_season_override_source_show', 'show_season_overrides',
                          'shows', ['source_show_id'], ['id'], ondelete='CASCADE')
    op.create_foreign_key('fk_season_override_target_show', 'show_season_overrides',
                          'shows', ['target_show_id'], ['id'], ondelete='SET NULL')

    # Update unique constraint
    op.drop_constraint('uq_season_override', 'show_season_overrides', type_='unique')
    op.create_unique_constraint('uq_season_override', 'show_season_overrides',
                                ['user_id', 'source_show_id', 'source_season_number'])

    # ── show_episode_overrides ───────────────────────────────────────────────────
    op.add_column('show_episode_overrides', sa.Column('source_show_id', sa.Integer(), nullable=True))
    op.add_column('show_episode_overrides', sa.Column('target_show_id', sa.Integer(), nullable=True))
    op.alter_column('show_episode_overrides', 'source_show_tmdb_id', nullable=True)
    op.alter_column('show_episode_overrides', 'target_show_tmdb_id', nullable=True)

    op.execute("""
        UPDATE show_episode_overrides o
        SET source_show_id = (SELECT s.id FROM shows s WHERE s.tmdb_id = o.source_show_tmdb_id LIMIT 1)
        WHERE o.source_show_tmdb_id IS NOT NULL AND o.source_show_tmdb_id >= 0
    """)
    op.execute("""
        UPDATE show_episode_overrides o
        SET source_show_id = (SELECT s.id FROM shows s WHERE s.tvdb_id = ABS(o.source_show_tmdb_id) LIMIT 1)
        WHERE o.source_show_tmdb_id IS NOT NULL AND o.source_show_tmdb_id < 0 AND o.source_show_id IS NULL
    """)
    op.execute("""
        UPDATE show_episode_overrides o
        SET target_show_id = (SELECT s.id FROM shows s WHERE s.tmdb_id = o.target_show_tmdb_id LIMIT 1)
        WHERE o.target_show_tmdb_id IS NOT NULL AND o.target_show_tmdb_id >= 0
    """)
    op.execute("""
        UPDATE show_episode_overrides o
        SET target_show_id = (SELECT s.id FROM shows s WHERE s.tvdb_id = ABS(o.target_show_tmdb_id) LIMIT 1)
        WHERE o.target_show_tmdb_id IS NOT NULL AND o.target_show_tmdb_id < 0 AND o.target_show_id IS NULL
    """)

    op.execute("DELETE FROM show_episode_overrides WHERE source_show_id IS NULL")

    op.alter_column('show_episode_overrides', 'source_show_id', nullable=False)

    op.create_foreign_key('fk_episode_override_source_show', 'show_episode_overrides',
                          'shows', ['source_show_id'], ['id'], ondelete='CASCADE')
    op.create_foreign_key('fk_episode_override_target_show', 'show_episode_overrides',
                          'shows', ['target_show_id'], ['id'], ondelete='SET NULL')

    op.drop_constraint('uq_episode_override', 'show_episode_overrides', type_='unique')
    op.create_unique_constraint('uq_episode_override', 'show_episode_overrides',
                                ['user_id', 'source_show_id', 'source_season_number', 'source_episode_number'])


def downgrade() -> None:
    op.drop_constraint('uq_episode_override', 'show_episode_overrides', type_='unique')
    op.create_unique_constraint('uq_episode_override', 'show_episode_overrides',
                                ['user_id', 'source_show_tmdb_id', 'source_season_number', 'source_episode_number'])
    op.drop_constraint('fk_episode_override_target_show', 'show_episode_overrides', type_='foreignkey')
    op.drop_constraint('fk_episode_override_source_show', 'show_episode_overrides', type_='foreignkey')
    op.alter_column('show_episode_overrides', 'source_show_tmdb_id', nullable=False)
    op.alter_column('show_episode_overrides', 'target_show_tmdb_id', nullable=False)
    op.drop_column('show_episode_overrides', 'target_show_id')
    op.drop_column('show_episode_overrides', 'source_show_id')

    op.drop_constraint('uq_season_override', 'show_season_overrides', type_='unique')
    op.create_unique_constraint('uq_season_override', 'show_season_overrides',
                                ['user_id', 'source_show_tmdb_id', 'source_season_number'])
    op.drop_constraint('fk_season_override_target_show', 'show_season_overrides', type_='foreignkey')
    op.drop_constraint('fk_season_override_source_show', 'show_season_overrides', type_='foreignkey')
    op.alter_column('show_season_overrides', 'source_show_tmdb_id', nullable=False)
    op.alter_column('show_season_overrides', 'target_show_tmdb_id', nullable=False)
    op.drop_column('show_season_overrides', 'target_show_id')
    op.drop_column('show_season_overrides', 'source_show_id')
