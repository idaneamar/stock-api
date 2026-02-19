from pydantic import BaseModel, Field
from typing import Any, Optional, List
from datetime import datetime

class StandardResponse(BaseModel):
    """Standard API response model"""
    success: bool = Field(description="Whether the request was successful")
    status: int = Field(description="HTTP status code")
    message: str = Field(description="Human-readable message")
    data: Optional[Any] = Field(default=None, description="Response data payload")

    class Config:
        json_schema_extra = {
            "example": {
                "success": True,
                "status": 200,
                "message": "Request completed successfully",
                "data": {"result": "example data"}
            }
        }

class ModifiedOrderRequest(BaseModel):
    """Schema for modified order request"""
    symbol: str = Field(description="Stock symbol")
    position_size: int = Field(description="Modified position size (shares)", gt=0, le=500)

    class Config:
        json_schema_extra = {
            "example": {
                "symbol": "AAPL",
                "position_size": 150
            }
        }

class PlaceModifiedOrdersRequest(BaseModel):
    """Schema for placing modified orders request"""
    scan_id: int = Field(description="Scan ID")
    modified_orders: List[ModifiedOrderRequest] = Field(description="List of modified orders")

    class Config:
        json_schema_extra = {
            "example": {
                "scan_id": 123,
                "modified_orders": [
                    {"symbol": "AAPL", "position_size": 150},
                    {"symbol": "GOOGL", "position_size": 75}
                ]
            }
        }

class ErrorResponse(BaseModel):
    """Standard error response model"""
    success: bool = Field(default=False, description="Whether the request was successful (always False)")
    status: int = Field(description="HTTP status code")
    message: str = Field(description="Error message")

    class Config:
        json_schema_extra = {
            "example": {
                "success": False,
                "status": 400,
                "message": "Invalid request. Missing required field: email."
            }
        }

class ScanRecord(BaseModel):
    """Schema for scan record with progress tracking"""
    id: int = Field(description="Unique scan ID")
    status: str = Field(description="Scan status: 'in_progress', 'completed', 'failed'")
    scan_progress: int = Field(description="Scan progress percentage (0-100)", ge=0, le=100)

    analysis_status: Optional[str] = Field(description="Analysis status: 'analyzing', 'completed', 'failed', or None")
    analysis_progress: int = Field(description="Analysis progress percentage (0-100)", ge=0, le=100)
    
    criteria: dict = Field(description="Original scan criteria used")
    stock_symbols: Optional[List[str]] = Field(description="List of stock symbols found in scan")
    total_found: Optional[int] = Field(description="Total number of stocks found")
    error_message: Optional[str] = Field(description="Error message if scan failed")
    portfolio_size: float = Field(description="Total portfolio size for sizing (USD)", default=350000.00)
    
    created_at: Optional[str] = Field(description="Scan creation timestamp (ISO format)")
    completed_at: Optional[str] = Field(description="Scan completion timestamp (ISO format)")
    analyzed_at: Optional[str] = Field(description="Analysis completion timestamp (ISO format)")

    class Config:
        json_schema_extra = {
            "example": {
                "id": 123,
                "status": "completed",
                "scan_progress": 100,
                "analysis_status": "analyzing",
                "analysis_progress": 65,
                "criteria": {
                    "min_market_cap": 150000000,
                    "max_market_cap": 160000000,
                    "min_avg_volume": 15000
                },
                "stock_symbols": ["AAPL", "MSFT", "GOOGL"],
                "total_found": 3,
                "error_message": None,
                "portfolio_size": 350000.00,
                "created_at": "2025-01-15T10:30:00.123456",
                "completed_at": "2025-01-15T10:35:00.789012",
                "analyzed_at": "2025-01-15T10:45:00.123456"
            }
        }
