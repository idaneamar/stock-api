from typing import Optional, List

from pydantic import BaseModel, Field


class ActiveTradeUpdatePayload(BaseModel):
    """Payload describing updates for a single trade."""

    symbol: str = Field(..., description="Ticker symbol of the trade to update")
    recommendation: Optional[str] = Field(
        default=None,
        description="Updated recommendation value (e.g., 'Buy' or 'Sell')",
    )
    entry_price: Optional[float] = Field(
        default=None,
        description="Updated entry price for the trade",
    )
    stop_loss: Optional[float] = Field(
        default=None,
        description="Updated stop-loss value",
    )
    take_profit: Optional[float] = Field(
        default=None,
        description="Updated take-profit value",
    )
    position_size: Optional[int] = Field(
        default=None,
        ge=0,
        description="Updated position size for the trade",
    )
    risk_reward_ratio: Optional[float] = Field(
        default=None,
        description="Updated risk/reward ratio",
    )
    entry_date: Optional[str] = Field(
        default=None,
        description="Updated entry date in ISO format",
    )
    exit_date: Optional[str] = Field(
        default=None,
        description="Updated exit date in ISO format",
    )
    strategy: Optional[str] = Field(
        default=None,
        description="Updated strategy label",
    )

    class Config:
        extra = "forbid"


class ActiveTradesUpdateRequest(BaseModel):
    """Request body for updating one or more active trades."""

    updates: List[ActiveTradeUpdatePayload] = Field(
        ..., description="List of per-trade updates to apply"
    )

    class Config:
        extra = "forbid"
