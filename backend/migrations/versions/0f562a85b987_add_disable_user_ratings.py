"""add disable_user_ratings global setting

Revision ID: 0f562a85b987
Revises: 01bd56990a60
Create Date: 2026-09-04
"""

from alembic import op
import sqlalchemy as sa


revision = "0f562a85b987"
down_revision = "01bd56990a60"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "global_settings",
        sa.Column("disable_user_ratings", sa.Boolean(), nullable=False, server_default="false"),
    )


def downgrade() -> None:
    op.drop_column("global_settings", "disable_user_ratings")
