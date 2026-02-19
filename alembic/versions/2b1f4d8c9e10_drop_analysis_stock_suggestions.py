"""Drop analysis_stock_suggestions setting

Revision ID: 2b1f4d8c9e10
Revises: 6d5f7a1c4a2e
Create Date: 2026-01-30 00:00:00.000000
"""

from typing import Sequence, Union

from alembic import op
from sqlalchemy import inspect


revision: str = "2b1f4d8c9e10"
down_revision: Union[str, None] = "6d5f7a1c4a2e"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = inspect(bind)
    cols = {c["name"] for c in inspector.get_columns("settings")}
    if "analysis_stock_suggestions" in cols:
        op.drop_column("settings", "analysis_stock_suggestions")


def downgrade() -> None:
    # We intentionally do not restore the setting; previous migrations already introduce it.
    pass

