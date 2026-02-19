from sqlalchemy import Column, Integer, String, DateTime, JSON, Text, Numeric
from sqlalchemy.sql import func
from app.models.database import Base

class ScanRecord(Base):
    __tablename__ = "scans"

    id = Column(Integer, primary_key=True, index=True)
    status = Column(String(50), default="in_progress", index=True)  # "in_progress", "completed", "failed", "analyzing", "analyzed"
    scan_progress = Column(Integer, default=0)  # Progress percentage (0-100)
    criteria = Column(JSON)  # Store scan criteria as JSON
    stock_symbols = Column(JSON, nullable=True)  # Store detailed stock symbol data as JSON array of objects
    total_found = Column(Integer, nullable=True)
    error_message = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    completed_at = Column(DateTime(timezone=True), nullable=True)

    # Unified analysis fields (preferred)
    analysis_results = Column(JSON, nullable=True)  # All analysis results as JSON list
    analysis_status = Column(String(50), nullable=True)  # "analyzing", "completed", "failed"
    analysis_progress = Column(Integer, default=0)  # Progress percentage (0-100)
    analyzed_at = Column(DateTime(timezone=True), nullable=True)

    # Strategy selection (optional): list of strategy IDs chosen by the user for this scan
    selected_strategies = Column(JSON, nullable=True)

    # Investment fields
    daily_investment = Column(Numeric(10, 2), default=5000.00)  # Daily investment amount with default 5000
    portfolio_size = Column(Numeric(12, 2), default=350000.00)  # Total portfolio size (USD)
