"""rename active trades to open trades

Revision ID: cfc2c618edb5
Revises: 0c7a0bbd958e
Create Date: 2025-09-26 14:00:48.887200

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'cfc2c618edb5'
down_revision: Union[str, None] = '0c7a0bbd958e'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)

    has_active = inspector.has_table('active_trades')
    has_open = inspector.has_table('open_trades')

    if has_active and not has_open:
        op.rename_table('active_trades', 'open_trades')
        return

    if has_active and has_open:
        op.execute(sa.text(
            """
            INSERT INTO open_trades (
                symbol,
                action,
                quantity,
                entry_price,
                entry_date,
                stop_loss,
                take_profit,
                target_date,
                scan_id,
                analysis_type,
                strategy,
                updated_at
            )
            SELECT
                at.symbol,
                at.action,
                at.quantity,
                at.entry_price,
                at.entry_date,
                at.stop_loss,
                at.take_profit,
                at.target_date,
                at.scan_id,
                at.analysis_type,
                at.strategy,
                at.updated_at
            FROM active_trades at
            WHERE NOT EXISTS (
                SELECT 1 FROM open_trades ot WHERE ot.symbol = at.symbol
            )
            """
        ))

        op.drop_table('active_trades')


def downgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)

    has_open = inspector.has_table('open_trades')
    has_active = inspector.has_table('active_trades')

    if has_open and not has_active:
        op.rename_table('open_trades', 'active_trades')
        return

    if has_open and has_active:
        op.execute(sa.text(
            """
            INSERT INTO active_trades (
                symbol,
                action,
                quantity,
                entry_price,
                entry_date,
                stop_loss,
                take_profit,
                target_date,
                scan_id,
                analysis_type,
                strategy,
                updated_at
            )
            SELECT
                ot.symbol,
                ot.action,
                ot.quantity,
                ot.entry_price,
                ot.entry_date,
                ot.stop_loss,
                ot.take_profit,
                ot.target_date,
                ot.scan_id,
                ot.analysis_type,
                ot.strategy,
                ot.updated_at
            FROM open_trades ot
            WHERE NOT EXISTS (
                SELECT 1 FROM active_trades at WHERE at.symbol = ot.symbol
            )
            """
        ))

        op.drop_table('open_trades')
