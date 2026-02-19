import logging
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

class UpdateSettingsRequest(BaseModel):
    portfolio_size: float = Field(
        ...,
        ge=1000,
        description="Total portfolio size in USD (minimum 1,000)",
    )


class SettingsResponse(BaseModel):
    portfolio_size: float

@router.get("/", response_model=StandardResponse)
async def get_settings(db: Session = Depends(get_db)):
    """
    Get all current settings.
    
    Returns all settings configured in the system.
    """
    try:
        # This will create a default record if none exists
        setting = Settings.get_settings(db)

        settings_data = {
            "portfolio_size": setting.portfolio_size,
        }
        
        return StandardResponse(
            success=True,
            status=200,
            message="Settings retrieved successfully",
            data=settings_data
        )
        
    except Exception as e:
        logging.error(f"Error retrieving settings: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error retrieving settings: {str(e)}"
        )

@router.put("/", response_model=StandardResponse)
async def update_settings(
    request: UpdateSettingsRequest, 
    db: Session = Depends(get_db)
):
    """
    Update all settings.
    
    Args:
        portfolio_size: Total portfolio size in USD (minimum 1,000)
    
    Updates the settings used by the trading analysis system.
    The minimum portfolio size is 1,000.
    """
    try:
        updated_setting = Settings.update_settings(
            db,
            portfolio_size=request.portfolio_size,
        )

        settings_data = {
            "portfolio_size": updated_setting.portfolio_size,
        }
        
        return StandardResponse(
            success=True,
            status=200,
            message="Settings updated successfully",
            data=settings_data
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Error updating settings: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error updating settings: {str(e)}"
        )


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
