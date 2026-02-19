"""Add portfolio_size setting

Revision ID: 6d5f7a1c4a2e
Revises: 8c6a2d5b1f32
Create Date: 2026-01-30 00:00:00.000000
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "6d5f7a1c4a2e"
down_revision: Union[str, None] = "8c6a2d5b1f32"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # settings: add portfolio_size
    op.add_column(
        "settings",
        sa.Column("portfolio_size", sa.Float(), nullable=False, server_default="350000.0"),
    )
    op.execute("UPDATE settings SET portfolio_size = COALESCE(portfolio_size, 350000.0)")
    op.alter_column("settings", "portfolio_size", server_default=None)

    # scans: store portfolio_size per scan
    op.add_column(
        "scans",
        sa.Column("portfolio_size", sa.Numeric(precision=12, scale=2), nullable=False, server_default="350000.00"),
    )
    op.execute("UPDATE scans SET portfolio_size = COALESCE(portfolio_size, 350000.00)")
    op.alter_column("scans", "portfolio_size", server_default=None)


def downgrade() -> None:
    op.drop_column("scans", "portfolio_size")
    op.drop_column("settings", "portfolio_size")

