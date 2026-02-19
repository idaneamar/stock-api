from pydantic import BaseModel, Field
from typing import Optional, List

class StockScannerRequest(BaseModel):
    """Request schema for stock scanner endpoint"""
    min_market_cap: Optional[float] = Field(default=150e6, description="Minimum market cap in USD")
    max_market_cap: Optional[float] = Field(default=160e6, description="Maximum market cap in USD")
    min_avg_volume: Optional[float] = Field(default=15000, description="Minimum average daily volume (shares)")
    min_avg_transaction_value: Optional[float] = Field(default=150000, description="Minimum average transaction value (USD)")
    min_volatility: Optional[float] = Field(default=0.4, description="Minimum annualized volatility")
    min_price: Optional[float] = Field(default=2.0, description="Minimum stock price (USD)")
    top_n_stocks: Optional[int] = Field(default=20, description="Number of top stocks to return")
    ignore_vix: Optional[bool] = Field(
        default=False,
        description="Disable the VIX-based market condition filter (true = ignore VIX check)",
    )
    selected_strategies: Optional[List[int]] = Field(
        default=None,
        description="Optional list of strategy IDs to run for this scan (null/[] = use all enabled strategies)",
    )

    # --- Program / Rules (Plans) ---
    program_id: Optional[str] = Field(
        default=None,
        description="Optional program/preset id (e.g., 'str_code_9'). When provided, the backend applies its rules/strategies for this scan.",
    )
    strict_rules: Optional[bool] = Field(
        default=True,
        description="If true, mandatory rules are enforced as hard filters (no Buy/Sell when a mandatory rule fails).",
    )
    adx_min: Optional[float] = Field(
        default=None,
        description="Optional global minimum ADX required to allow a Buy/Sell recommendation.",
    )
    volume_spike_required: Optional[bool] = Field(
        default=None,
        description="If true, Volume Spike must be true for any Buy/Sell recommendation (hard filter).",
    )
    daily_loss_limit_pct: Optional[float] = Field(
        default=None,
        description="Optional daily max loss limit as a fraction (e.g., 0.02 = 2%). Overrides the default 0.02 for this scan.",
    )
    allow_intraday_prices: Optional[bool] = Field(
        default=False,
        description="If true, the backend will try to use intraday/real-time price as the 'close' reference when available.",
    )

    class Config:
        json_schema_extra = {
            "example": {
                "min_market_cap": 150000000,
                "max_market_cap": 160000000,
                "min_avg_volume": 15000,
                "min_avg_transaction_value": 150000,
                "min_volatility": 0.4,
                "min_price": 2.0,
                "top_n_stocks": 20
            }
        }
