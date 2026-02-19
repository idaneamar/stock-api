"""Drop analysis_suggestions column

Revision ID: b7c9d2e4f6a8
Revises: 8c6a2d5b1f32
Create Date: 2026-02-02 00:00:00.000000
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect


revision: str = "b7c9d2e4f6a8"
down_revision: Union[str, None] = "8c6a2d5b1f32"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _existing_columns(table_name: str) -> set[str]:
    bind = op.get_bind()
    inspector = inspect(bind)
    return {col["name"] for col in inspector.get_columns(table_name)}


def upgrade() -> None:
    scan_cols = _existing_columns("scans")
    if "analysis_suggestions" in scan_cols:
        op.drop_column("scans", "analysis_suggestions")


def downgrade() -> None:
    scan_cols = _existing_columns("scans")
    if "analysis_suggestions" not in scan_cols:
        op.add_column("scans", sa.Column("analysis_suggestions", sa.JSON(), nullable=True))

