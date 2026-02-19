"""Drop legacy full/limited analysis fields

Revision ID: 8c6a2d5b1f32
Revises: 3f1d9a3bb2c1
Create Date: 2026-01-30 00:00:00.000000
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect


revision: str = "8c6a2d5b1f32"
down_revision: Union[str, None] = "3f1d9a3bb2c1"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


_SCAN_LEGACY_COLUMNS = (
    "full_analysis_results",
    "full_analysis_status",
    "full_analysis_progress",
    "full_analyzed_at",
    "limited_analysis_results",
    "limited_analysis_status",
    "limited_analysis_progress",
    "limited_analyzed_at",
)


def _existing_columns(table_name: str) -> set[str]:
    bind = op.get_bind()
    inspector = inspect(bind)
    return {col["name"] for col in inspector.get_columns(table_name)}


def upgrade() -> None:
    scan_cols = _existing_columns("scans")
    for col in _SCAN_LEGACY_COLUMNS:
        if col in scan_cols:
            op.drop_column("scans", col)

    settings_cols = _existing_columns("settings")
    if "limited_analysis_stock_suggestions" in settings_cols:
        op.drop_column("settings", "limited_analysis_stock_suggestions")


def downgrade() -> None:
    settings_cols = _existing_columns("settings")
    if "limited_analysis_stock_suggestions" not in settings_cols:
        op.add_column(
            "settings",
            sa.Column(
                "limited_analysis_stock_suggestions",
                sa.Integer(),
                nullable=False,
                server_default="10",
            ),
        )
        op.alter_column("settings", "limited_analysis_stock_suggestions", server_default=None)

    scan_cols = _existing_columns("scans")
    if "full_analysis_results" not in scan_cols:
        op.add_column("scans", sa.Column("full_analysis_results", sa.JSON(), nullable=True))
    if "full_analysis_status" not in scan_cols:
        op.add_column("scans", sa.Column("full_analysis_status", sa.String(length=50), nullable=True))
    if "full_analysis_progress" not in scan_cols:
        op.add_column("scans", sa.Column("full_analysis_progress", sa.Integer(), nullable=False, server_default="0"))
        op.alter_column("scans", "full_analysis_progress", server_default=None)
    if "full_analyzed_at" not in scan_cols:
        op.add_column("scans", sa.Column("full_analyzed_at", sa.DateTime(timezone=True), nullable=True))

    if "limited_analysis_results" not in scan_cols:
        op.add_column("scans", sa.Column("limited_analysis_results", sa.JSON(), nullable=True))
    if "limited_analysis_status" not in scan_cols:
        op.add_column("scans", sa.Column("limited_analysis_status", sa.String(length=50), nullable=True))
    if "limited_analysis_progress" not in scan_cols:
        op.add_column(
            "scans",
            sa.Column("limited_analysis_progress", sa.Integer(), nullable=False, server_default="0"),
        )
        op.alter_column("scans", "limited_analysis_progress", server_default=None)
    if "limited_analyzed_at" not in scan_cols:
        op.add_column("scans", sa.Column("limited_analyzed_at", sa.DateTime(timezone=True), nullable=True))

