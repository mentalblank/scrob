"""Merge movie_genres and show_genres into liked_genres

Revision ID: 20260527_genres
Revises: n1o2p3q4r5s6
Create Date: 2026-05-27 21:15:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = '20260527_genres'
down_revision: Union[str, Sequence[str], None] = 'n1o2p3q4r5s6'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Add liked_genres column
    op.add_column('user_profiles', sa.Column('liked_genres', postgresql.JSONB(astext_type=sa.Text()), nullable=True))
    
    # Migrate data: merge movie_genres and show_genres
    op.execute("""
        UPDATE user_profiles
        SET liked_genres = (
            SELECT COALESCE(jsonb_agg(DISTINCT x), '[]'::jsonb)
            FROM (
                SELECT jsonb_array_elements_text(COALESCE(movie_genres, '[]'::jsonb)) AS x
                UNION
                SELECT jsonb_array_elements_text(COALESCE(show_genres, '[]'::jsonb)) AS x
            ) AS t
            WHERE x IS NOT NULL
        )
        WHERE movie_genres IS NOT NULL OR show_genres IS NOT NULL
    """)

    # Drop old columns
    op.drop_column('user_profiles', 'movie_genres')
    op.drop_column('user_profiles', 'show_genres')


def downgrade() -> None:
    # Add old columns back
    op.add_column('user_profiles', sa.Column('show_genres', postgresql.JSONB(astext_type=sa.Text()), nullable=True))
    op.add_column('user_profiles', sa.Column('movie_genres', postgresql.JSONB(astext_type=sa.Text()), nullable=True))
    
    # Restore data (best effort: both get the same liked_genres)
    op.execute("UPDATE user_profiles SET movie_genres = liked_genres, show_genres = liked_genres")
    
    # Drop liked_genres column
    op.drop_column('user_profiles', 'liked_genres')
