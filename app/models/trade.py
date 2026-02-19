from sqlalchemy import Column, Integer, String, Date, DateTime, Numeric
from sqlalchemy.sql import func

from app.models.database import Base


class OpenTrade(Base):
    """Tracks currently open positions placed with the broker."""

    __tablename__ = "open_trades"

    id = Column(Integer, primary_key=True, index=True)
    symbol = Column(String(32), unique=True, nullable=False, index=True)
    action = Column(String(10), nullable=False)
    quantity = Column(Integer, nullable=False)
    entry_price = Column(Numeric(12, 4), nullable=False)
    entry_date = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    stop_loss = Column(Numeric(12, 4), nullable=True)
    take_profit = Column(Numeric(12, 4), nullable=True)
    target_date = Column(Date, nullable=True)
    scan_id = Column(Integer, nullable=True)
    analysis_type = Column(String(20), nullable=True)
    strategy = Column(String(50), nullable=True)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)


class ClosedTrade(Base):
    """Stores completed trades for history and reporting."""

    __tablename__ = "closed_trades"

    id = Column(Integer, primary_key=True, index=True)
    symbol = Column(String(32), nullable=False, index=True)
    action = Column(String(10), nullable=False)
    quantity = Column(Integer, nullable=False)
    entry_price = Column(Numeric(12, 4), nullable=False)
    exit_price = Column(Numeric(12, 4), nullable=True)
    entry_date = Column(DateTime(timezone=True), nullable=True)
    exit_date = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    close_reason = Column(String(100), nullable=True)
    strategy = Column(String(50), nullable=True)
    scan_id = Column(Integer, nullable=True)
    analysis_type = Column(String(20), nullable=True)
    stop_loss = Column(Numeric(12, 4), nullable=True)
    take_profit = Column(Numeric(12, 4), nullable=True)
    target_date = Column(Date, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
