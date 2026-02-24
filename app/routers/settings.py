import logging
from typing import Optional
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel, Field
from app.models.database import SessionLocal, get_db
from app.models.settings import Settings
from app.schemas.response import StandardResponse
from sqlalchemy.orm import Session
from app.services.stock_scanner import StockScanner
from app.services.trade_service import TradeService

router = APIRouter(
    prefix="/settings",
    tags=["Settings"],
    responses={404: {"description": "Not found"}},
)

stock_scanner = StockScanner()
trade_service = TradeService()


def _setting_to_dict(s: Settings) -> dict:
    """Serialize a Settings row to a response-safe dict."""
    return {
        "portfolio_size": s.portfolio_size,
        "strict_rules": bool(s.strict_rules) if s.strict_rules is not None else True,
        "volume_spike_required": bool(s.volume_spike_required) if s.volume_spike_required is not None else False,
        "use_intraday": bool(s.use_intraday) if s.use_intraday is not None else False,
        "daily_loss_limit_pct": float(s.daily_loss_limit_pct) if s.daily_loss_limit_pct is not None else 0.02,
    }


class UpdateSettingsRequest(BaseModel):
    portfolio_size: float = Field(
        ...,
        ge=1000,
        description="Total portfolio size in USD (minimum 1,000)",
    )
    strict_rules: Optional[bool] = Field(None, description="Enforce buy/sell rules as hard filters")
    volume_spike_required: Optional[bool] = Field(None, description="Require volume spike for any signal")
    use_intraday: Optional[bool] = Field(None, description="Use intraday/real-time price when available")
    daily_loss_limit_pct: Optional[float] = Field(None, description="Daily max loss limit as fraction (e.g. 0.02)")


@router.get("/", response_model=StandardResponse)
async def get_settings(db: Session = Depends(get_db)):
    """Get all current settings including global engine toggles."""
    try:
        setting = Settings.get_settings(db)
        return StandardResponse(
            success=True,
            status=200,
            message="Settings retrieved successfully",
            data=_setting_to_dict(setting),
        )
    except Exception as e:
        logging.error(f"Error retrieving settings: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error retrieving settings: {str(e)}")


@router.put("/", response_model=StandardResponse)
async def update_settings(request: UpdateSettingsRequest, db: Session = Depends(get_db)):
    """Update portfolio size and global engine toggles."""
    try:
        setting = Settings.get_settings(db)
        setting.portfolio_size = request.portfolio_size

        # Engine toggles – only update fields the client explicitly sent
        if request.strict_rules is not None:
            setting.strict_rules = request.strict_rules
        if request.volume_spike_required is not None:
            setting.volume_spike_required = request.volume_spike_required
        if request.use_intraday is not None:
            setting.use_intraday = request.use_intraday
        if request.daily_loss_limit_pct is not None:
            setting.daily_loss_limit_pct = request.daily_loss_limit_pct

        db.commit()
        db.refresh(setting)

        return StandardResponse(
            success=True,
            status=200,
            message="Settings updated successfully",
            data=_setting_to_dict(setting),
        )
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Error updating settings: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error updating settings: {str(e)}")


@router.post("/reset-all", response_model=StandardResponse)
async def reset_all():
    """
    Delete all scans and trade records from the system.
    
    Intended for administrative use when a full data reset is required.
    """
    try:
        scans_deleted = stock_scanner.delete_all_scans()
        open_deleted = trade_service.delete_all_open_trades()
        closed_deleted = trade_service.delete_all_closed_trades()

        message = (
            "Reset completed with no records removed"
            if (scans_deleted + open_deleted + closed_deleted) == 0
            else (
                "Reset completed. "
                f"Removed {scans_deleted} scans, {open_deleted} open trades, and {closed_deleted} closed trades."
            )
        )

        return StandardResponse(
            success=True,
            status=200,
            message=message,
            data={
                "scans_deleted": scans_deleted,
                "open_trades_deleted": open_deleted,
                "closed_trades_deleted": closed_deleted,
            },
        )
    except Exception as e:
        logging.error(f"Error resetting all data: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error resetting all data: {str(e)}"
        )
