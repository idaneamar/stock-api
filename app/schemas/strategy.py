from typing import Any, Dict, Optional, List

from pydantic import BaseModel, Field


class StrategyRule(BaseModel):
    indicator: str = Field(..., description="Left-hand indicator name (e.g. rsi, ema20, close)")
    operator: str = Field(..., description="Comparison operator: >, <, >=, <=, ==, !=")
    value: Optional[float] = Field(default=None, description="Fixed value to compare against")
    compare_to: Optional[str] = Field(default=None, description="Right-hand indicator name to compare against")
    expression: Optional[str] = Field(default=None, description="Expression evaluated using indicator names")

    class Config:
        extra = "forbid"


class StrategyRiskConfig(BaseModel):
    stop_loss_atr_mult: float = Field(default=1.5, ge=0.1, description="ATR multiple for stop-loss")
    take_profit_atr_mult: float = Field(default=3.0, ge=0.1, description="ATR multiple for take-profit")
    min_risk_reward: float = Field(default=1.5, ge=0.0, description="Minimum RR required to accept a signal")

    class Config:
        extra = "forbid"


class StrategyConfig(BaseModel):
    pre_filters: Optional[List[StrategyRule]] = Field(
        default=None,
        description="Optional rules that must pass before buy/sell rules are evaluated (e.g. volume_spike == 1, adx > 30)",
    )
    buy_rules: Optional[List[StrategyRule]] = Field(default=None, description="Rules that trigger a BUY signal")
    sell_rules: Optional[List[StrategyRule]] = Field(default=None, description="Rules that trigger a SELL signal")
    # Backward-compatible single rules list (treated as buy_rules)
    rules: Optional[List[StrategyRule]] = Field(default=None, description="Alias for buy_rules")
    risk: Optional[StrategyRiskConfig] = Field(default=None, description="Risk config for stops/targets")

    class Config:
        extra = "allow"


class StrategyCreateRequest(BaseModel):
    name: str = Field(..., min_length=1, description="Human-friendly strategy name")
    enabled: bool = Field(default=True, description="Whether the strategy is enabled globally")
    config: Dict[str, Any] = Field(default_factory=dict, description="Strategy config (rules + risk)")

    class Config:
        extra = "forbid"


class StrategyUpdateRequest(BaseModel):
    name: Optional[str] = Field(default=None, min_length=1)
    enabled: Optional[bool] = None
    config: Optional[Dict[str, Any]] = None

    class Config:
        extra = "forbid"


class StrategyResponse(BaseModel):
    id: int
    name: str
    enabled: bool
    config: Dict[str, Any]
