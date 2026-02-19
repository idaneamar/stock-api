"""Add unified analysis fields

Revision ID: 3f1d9a3bb2c1
Revises: 0c7a0bbd958e
Create Date: 2026-01-30 00:00:00.000000
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "3f1d9a3bb2c1"
down_revision: Union[str, None] = "0c7a0bbd958e"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # scans: unified analysis fields
    op.add_column("scans", sa.Column("analysis_results", sa.JSON(), nullable=True))
    op.add_column("scans", sa.Column("analysis_suggestions", sa.JSON(), nullable=True))
    op.add_column("scans", sa.Column("analysis_status", sa.String(length=50), nullable=True))
    op.add_column("scans", sa.Column("analysis_progress", sa.Integer(), nullable=False, server_default="0"))
    op.add_column("scans", sa.Column("analyzed_at", sa.DateTime(timezone=True), nullable=True))
    op.alter_column("scans", "analysis_progress", server_default=None)

    # settings: unified analysis suggestions (keep legacy column)
    op.add_column(
        "settings",
        sa.Column("analysis_stock_suggestions", sa.Integer(), nullable=False, server_default="10"),
    )
    op.execute(
        "UPDATE settings SET analysis_stock_suggestions = COALESCE(analysis_stock_suggestions, limited_analysis_stock_suggestions, 10)"
    )
    op.alter_column("settings", "analysis_stock_suggestions", server_default=None)


def downgrade() -> None:
    op.drop_column("settings", "analysis_stock_suggestions")

    op.drop_column("scans", "analyzed_at")
    op.drop_column("scans", "analysis_progress")
    op.drop_column("scans", "analysis_status")
    op.drop_column("scans", "analysis_suggestions")
    op.drop_column("scans", "analysis_results")

