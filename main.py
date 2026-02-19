import os
import logging
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from app.core.config import settings as config_settings
from app.schemas.response import ErrorResponse
from app.models.init_db import create_tables
from app.routers import scans, analysis, websocket, settings, trades, strategies, programs

# -----------------------------
# Logging (configure early)
# -----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
    force=True,
)

logger = logging.getLogger("main")

# -----------------------------
# Public URLs (for docs display)
# -----------------------------
# In Render set: PUBLIC_BASE_URL = https://stock-api-1-jhsa.onrender.com
PUBLIC_BASE_URL = os.getenv("PUBLIC_BASE_URL", "").rstrip("/")
if not PUBLIC_BASE_URL:
    PUBLIC_BASE_URL = "https://stock-api-1-jhsa.onrender.com"

PUBLIC_WS_URL = os.getenv("PUBLIC_WS_URL", "").rstrip("/")
if not PUBLIC_WS_URL:
    if PUBLIC_BASE_URL.startswith("https://"):
        PUBLIC_WS_URL = PUBLIC_BASE_URL.replace("https://", "wss://")
    elif PUBLIC_BASE_URL.startswith("http://"):
        PUBLIC_WS_URL = PUBLIC_BASE_URL.replace("http://", "ws://")
    else:
        PUBLIC_WS_URL = "wss://stock-api-1-jhsa.onrender.com"

# -----------------------------
# FastAPI App
# -----------------------------
app = FastAPI(
    title=config_settings.PROJECT_NAME,
    version=config_settings.VERSION,
    description=f"""{config_settings.DESCRIPTION}

## Key Endpoints

### Health Check
**URL**: `/health`

### DB Check
**URL**: `/db-check`

### Active Trades Endpoint
**URL**: `/analysis/active-trades?start_date=2024-01-01&end_date=2024-12-31`

## WebSocket Endpoint
**WebSocket URL**: `{PUBLIC_WS_URL}/listen_events`
"""
)

# -----------------------------
# CORS
# -----------------------------
app.add_middleware(
    CORSMiddleware,
    allow_origins=config_settings.ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# -----------------------------
# Simple endpoints
# -----------------------------
@app.get("/")
def root():
    return {"message": "Stock API is running", "docs": f"{PUBLIC_BASE_URL}/docs"}

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/db-check")
def db_check():
    try:
        create_tables()
        return {"db": "ok"}
    except Exception as e:
        logger.error(f"DB check failed: {e}")
        return JSONResponse(status_code=500, content={"db": "error", "detail": str(e)})

# -----------------------------
# Initialize database tables
# -----------------------------
try:
    create_tables()
    logger.info("Database initialization OK")
except Exception as e:
    logger.error(f"Failed to initialize database: {e}")

# -----------------------------
# Routers
# -----------------------------
app.include_router(scans.router)
app.include_router(analysis.router)
app.include_router(websocket.router)
app.include_router(settings.router)
app.include_router(strategies.router)
app.include_router(programs.router)
app.include_router(trades.router)

# -----------------------------
# Exception handlers
# -----------------------------
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
    logger.error(f"Unhandled exception: {str(exc)}")
    return JSONResponse(
        status_code=500,
        content=ErrorResponse(
            success=False,
            status=500,
            message="Internal server error"
        ).dict()
    )

from fastapi.exceptions import RequestValidationError
from pydantic import ValidationError
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

@app.exception_handler(ValidationError)
async def pydantic_validation_exception_handler(request: Request, exc: ValidationError):
    return JSONResponse(
        status_code=422,
        content=ErrorResponse(
            success=False,
            status=422,
            message="Validation error. Please check your request data."
        ).dict()
    )

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
