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
    # fallback (works in Render, but better to set PUBLIC_BASE_URL explicitly)
    PUBLIC_BASE_URL = "https://stock-api-1-jhsa.onrender.com"

PUBLIC_WS_URL = os.getenv("PUBLIC_WS_URL", "").rstrip("/")
if not PUBLIC_WS_URL:
    # If base is https -> ws should be wss
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
- Quick check that the API is running

### DB Check
**URL**: `/db-check`
- Verifies the API can reach the database (requires correct DATABASE_URL in env)

### Active Trades Endpoint
**URL**: `/analysis/active-trades?start_date=2024-01-01&end_date=2024-12-31`
- Get combined active trades from all scans with optional date filtering
- Each trade includes scan_id and original trade fields

## WebSocket Endpoint

**WebSocket URL**: `{PUBLIC_WS_URL}/listen_events`

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
