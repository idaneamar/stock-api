import logging
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from app.core.config import settings as config_settings
from app.schemas.response import ErrorResponse
from app.models.init_db import create_tables
from app.routers import scans, analysis, websocket, settings, trades, strategies, programs

# Configure logging as early as possible
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
    ],
    force=True  # This forces reconfiguration even if logging was already configured
)

app = FastAPI(
    title=config_settings.PROJECT_NAME,
    version=config_settings.VERSION,
    description=f"""{config_settings.DESCRIPTION}

## Key Endpoints

### Active Trades Endpoint
**URL**: `/analysis/active-trades?start_date=2024-01-01&end_date=2024-12-31`
- Get combined active trades from all scans with optional date filtering
- Each trade includes scan_id and original trade fields

## WebSocket Endpoint

**WebSocket URL**: `ws://localhost:8000/listen_events`

Connect to this WebSocket endpoint to receive real-time event notifications:

### Completion Events:
- **scan_completed**: Sent when a stock scan is completed
- **analysis_completed**: Sent when analysis is completed

### Progress Events:
- **scan_progress**: Real-time progress updates during stock scanning (0-100%)
- **analysis_progress**: Real-time progress updates during analysis (0-100%)

### Event Format:
```json
{{"event": "event_type", "id": "scan_id"}}
```

### Progress Event Format:
```json
{{"event": "scan_progress", "id": "scan_id", "progress": 45}}
```

### Connection Welcome Message:
```json
{{
  "event": "connected",
  "message": "Successfully connected to Stock API events",
  "timestamp": "2024-XX-XXTXX:XX:XX.XXXXXX"
}}
```
"""
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=config_settings.ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize database tables
try:
    create_tables()
except Exception as e:
    logging.error(f"Failed to initialize database: {e}")

# Include routers
app.include_router(scans.router)
app.include_router(analysis.router)
app.include_router(websocket.router)
app.include_router(settings.router)
app.include_router(strategies.router)
app.include_router(programs.router)
app.include_router(trades.router)

# Global exception handler for consistent error responses
@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    return JSONResponse(
        status_code=exc.status_code,
        content=ErrorResponse(
            success=False,
            status=exc.status_code,
            message=exc.detail
        ).dict()
    )

@app.exception_handler(Exception)
async def general_exception_handler(request: Request, exc: Exception):
    logging.error(f"Unhandled exception: {str(exc)}")
    return JSONResponse(
        status_code=500,
        content=ErrorResponse(
            success=False,
            status=500,
            message="Internal server error"
        ).dict()
    )

# Handle Pydantic validation errors
from pydantic import ValidationError
from fastapi.exceptions import RequestValidationError
from starlette.exceptions import HTTPException as StarletteHTTPException

@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    return JSONResponse(
        status_code=422,
        content=ErrorResponse(
            success=False,
            status=422,
            message="Validation error. Please check your request data."
        ).dict()
    )

# Handle 404 and other Starlette HTTP exceptions
@app.exception_handler(StarletteHTTPException)
async def starlette_http_exception_handler(request: Request, exc: StarletteHTTPException):
    return JSONResponse(
        status_code=exc.status_code,
        content=ErrorResponse(
            success=False,
            status=exc.status_code,
            message=exc.detail
        ).dict()
    )




if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=config_settings.PORT, reload=config_settings.RELOAD)
