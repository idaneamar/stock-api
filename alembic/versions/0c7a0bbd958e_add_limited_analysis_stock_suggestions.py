"""Add limited analysis stock suggestions setting

Revision ID: 0c7a0bbd958e
Revises: 41809e99215a
Create Date: 2025-09-12 15:30:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '0c7a0bbd958e'
down_revision: Union[str, None] = '41809e99215a'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        'settings',
        sa.Column('limited_analysis_stock_suggestions', sa.Integer(), nullable=False, server_default='10')
    )
    op.execute(
        "UPDATE settings SET limited_analysis_stock_suggestions = 10 WHERE limited_analysis_stock_suggestions IS NULL"
    )
    op.alter_column(
        'settings',
        'limited_analysis_stock_suggestions',
        server_default=None,
    )


def downgrade() -> None:
    op.drop_column('settings', 'limited_analysis_stock_suggestions')
