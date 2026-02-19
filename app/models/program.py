from sqlalchemy import Column, Integer, String, Boolean, DateTime, JSON
from sqlalchemy.sql import func

from app.models.database import Base


class Program(Base):
    """Saved preset ("Plan/Program") that controls strategies + rules."""

    __tablename__ = "programs"

    id = Column(Integer, primary_key=True, index=True)
    program_id = Column(String(100), nullable=False, unique=True, index=True)
    name = Column(String(200), nullable=False)
    is_baseline = Column(Boolean, nullable=False, default=False)

    # Full program configuration as JSON (single source of truth)
    # Example:
    # {
    #   "enabled_strategy_names": ["Trend", "Momentum", "VWAP"],
    #   "rules": {"strict_rules": true, "adx_min": 30, "volume_spike_required": true},
    #   "market": {"use_vix_filter": true},
    #   "risk": {"daily_loss_limit_pct": 0.02}
    # }
    config = Column(JSON, nullable=False, default=dict)

    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
