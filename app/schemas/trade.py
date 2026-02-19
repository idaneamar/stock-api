from datetime import date, datetime
from decimal import Decimal
from typing import Optional, Union

from pydantic import BaseModel, Field


class OpenTradeUpdateRequest(BaseModel):
    """Payload to update the target date of an existing open trade."""

    target_date: Optional[date] = Field(
        default=None,
        description="Updated target exit date. Send null to clear the target date.",
    )

    class Config:
        extra = "forbid"
        json_schema_extra = {
            "example": {
                "target_date": "2024-12-31",
            }
        }


class ClosedTradeCreate(BaseModel):
    """Payload to create a closed trade record."""

    symbol: str = Field(..., description="Ticker symbol for the trade", min_length=1)
    entry_price: Decimal = Field(..., description="Executed entry price")
    exit_price: Optional[Decimal] = Field(default=None, description="Executed exit price")
    entry_date: Optional[Union[date, datetime]] = Field(
        default=None,
        description="Date or timestamp when the position was opened",
    )
    exit_date: Union[date, datetime] = Field(..., description="Date or timestamp when the position was closed")
    close_reason: Optional[str] = Field(default=None, description="Reason the trade was closed")
    quantity: int = Field(..., gt=0, description="Number of shares executed")
    action: str = Field(..., description="Buy or Sell action at exit", min_length=1)
    strategy: Optional[str] = Field(default=None, description="Trading strategy identifier")
    scan_id: Optional[int] = Field(default=None, description="Originating scan identifier")
    analysis_type: Optional[str] = Field(default=None, description="Associated analysis type")
    stop_loss: Optional[Decimal] = Field(default=None, description="Stop loss level when the trade was open")
    take_profit: Optional[Decimal] = Field(default=None, description="Take profit level when the trade was open")
    target_date: Optional[date] = Field(default=None, description="Target date originally set for the trade")

    class Config:
        extra = "forbid"
        json_schema_extra = {
            "example": {
                "symbol": "SPTN",
                "entry_price": 26.52,
                "exit_price": 26.59,
                "entry_date": "2025-08-18",
                "exit_date": "2025-08-21",
                "close_reason": "Target date reached",
                "quantity": 125,
                "action": "Sell",
            }
        }


class OpenTradeCreate(BaseModel):
    """Payload to create an open trade record."""

    symbol: Optional[str] = Field(default=None, description="Ticker symbol for the trade", min_length=1)
    quantity: int = Field(..., gt=0, description="Number of shares in the position")
    entry_price: Decimal = Field(..., description="Executed entry price")
    entry_date: Optional[Union[date, datetime]] = Field(
        default=None,
        description="Date or timestamp when the position was opened",
    )
    action: str = Field(..., description="Buy or Sell action opening the position", min_length=1)
    stop_loss: Optional[Decimal] = Field(default=None, description="Configured stop loss level")
    take_profit: Optional[Decimal] = Field(default=None, description="Configured take profit level")
    target_date: Optional[date] = Field(default=None, description="Target exit date")
    strategy: Optional[str] = Field(default=None, description="Trading strategy identifier")
    scan_id: Optional[int] = Field(default=None, description="Originating scan identifier")
    analysis_type: Optional[str] = Field(default=None, description="Associated analysis type")

    class Config:
        extra = "forbid"
        json_schema_extra = {
            "example": {
                "quantity": 430,
                "entry_price": 7.75,
                "entry_date": "2025-08-18",
                "action": "Sell",
                "stop_loss": 12.65,
                "take_profit": 6.48,
                "target_date": "2025-10-08",
            }
        }
