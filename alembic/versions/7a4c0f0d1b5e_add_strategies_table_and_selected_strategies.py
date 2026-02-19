"""Add strategies table and selected_strategies on scans

Revision ID: 7a4c0f0d1b5e
Revises: 2b1f4d8c9e10
Create Date: 2026-01-30 00:00:00.000000
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "7a4c0f0d1b5e"
down_revision: Union[str, None] = "2b1f4d8c9e10"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "strategies",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("name", sa.String(length=100), nullable=False),
        sa.Column("key", sa.String(length=50), nullable=False),
        sa.Column("enabled", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("config", sa.JSON(), nullable=False, server_default=sa.text("'{}'::json")),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
        sa.UniqueConstraint("key", name="uq_strategies_key"),
    )
    op.create_index("ix_strategies_key", "strategies", ["key"])

    op.add_column("scans", sa.Column("selected_strategies", sa.JSON(), nullable=True))

    # Clear server defaults that were only needed for creation
    op.alter_column("strategies", "enabled", server_default=None)
    op.alter_column("strategies", "config", server_default=None)


def downgrade() -> None:
    op.drop_column("scans", "selected_strategies")
    op.drop_index("ix_strategies_key", table_name="strategies")
    op.drop_table("strategies")

