"""Drop strategy key column

Revision ID: b7d4c2a1e9f0
Revises: 7a4c0f0d1b5e
Create Date: 2026-02-02 00:00:00.000000
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "b7d4c2a1e9f0"
down_revision: Union[str, None] = "7a4c0f0d1b5e"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # The 'key' column was previously indexed/unique. Dropping the column will implicitly
    # remove dependent objects on most DBs, but we also drop the named index defensively.
    try:
        op.drop_index("ix_strategies_key", table_name="strategies", if_exists=True)
    except TypeError:
        # Older Alembic versions do not support if_exists.
        try:
            op.drop_index("ix_strategies_key", table_name="strategies")
        except Exception:
            pass

    with op.batch_alter_table("strategies") as batch_op:
        batch_op.drop_column("key")


def downgrade() -> None:
    # Re-introduce the column as nullable; restoring uniqueness safely requires backfilling.
    with op.batch_alter_table("strategies") as batch_op:
        batch_op.add_column(sa.Column("key", sa.String(length=50), nullable=True))

    try:
        op.create_index("ix_strategies_key", "strategies", ["key"])
    except Exception:
        pass
