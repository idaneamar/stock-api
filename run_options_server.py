#!/usr/bin/env python3
"""
run_options_server.py
---------------------
Standalone FastAPI server for the Iron Condor options system.

Runs on port 8001, completely separate from the main cloud backend on Render.
This server MUST run on the machine that has the OptionSys data directory
(the Extreme Pro drive at /Volumes/Extreme Pro/OptionSys).

Usage
-----
    cd /Volumes/Extreme\ Pro/App\ gpt/stock_api-main_updated

    # Normal start (scheduler wakes at 9:30 ET Monday and 16:30 ET Mon-Fri):
    python run_options_server.py

    # Also fire the Monday-open jobs right now (catch-up after missing them):
    python run_options_server.py --run-now

    # Custom port:
    python run_options_server.py --port 8002

    # Set a custom data directory (if drive is mounted elsewhere):
    STOCK_BASE_DIR=/path/to/OptionSys python run_options_server.py

Environment variables
---------------------
    STOCK_BASE_DIR   Path to the OptionSys data directory. Defaults to
                     /Volumes/Extreme Pro/OptionSys
    OPENAI_API_KEY   Required for the AI chat endpoint.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import threading
from contextlib import asynccontextmanager
from pathlib import Path

# ── Make sure we can import the app package ───────────────────────────────────
HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

# ── Load .env file BEFORE anything reads os.environ ───────────────────────────
try:
    from dotenv import load_dotenv
    load_dotenv(HERE / ".env")
except ImportError:
    pass  # python-dotenv not installed — rely on shell env vars

# ── Default data directory ─────────────────────────────────────────────────────
_DEFAULT_DATA_DIR = "/Volumes/Extreme Pro/OptionSys"
if not os.environ.get("STOCK_BASE_DIR"):
    os.environ["STOCK_BASE_DIR"] = _DEFAULT_DATA_DIR

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("options-server")

# ── CLI args (parsed before FastAPI import so --port reaches uvicorn) ─────────
parser = argparse.ArgumentParser(description="Iron Condor options local server")
parser.add_argument(
    "--port", type=int, default=8001,
    help="TCP port (default: 8001)",
)
parser.add_argument(
    "--run-now", action="store_true",
    help="Trigger Monday-open (fetch symbols + optsp) AND daily prefetch "
         "immediately in a background thread. Use to catch up if the "
         "scheduler was not running at market open/close.",
)
args = parser.parse_args()

# ── FastAPI app ────────────────────────────────────────────────────────────────
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.routers.options import router as options_router
from app.services.options_scheduler import (
    _job_daily_prefetch,
    _job_monday_open,
    start_options_scheduler,
    stop_options_scheduler,
)
from app.services.options_service import warm_ticker_summaries_cache


@asynccontextmanager
async def lifespan(app_instance: FastAPI):
    data_dir = os.environ.get("STOCK_BASE_DIR", _DEFAULT_DATA_DIR)
    logger.info("=" * 60)
    logger.info(f"  Options server  –  port {args.port}")
    logger.info(f"  STOCK_BASE_DIR  = {data_dir}")
    logger.info("=" * 60)

    # Start the APScheduler (Monday 09:30 + weekday 16:30 ET)
    try:
        start_options_scheduler()
        logger.info("Scheduler started.")
    except Exception as exc:
        logger.warning(f"Scheduler failed to start: {exc}")

    # Pre-compute P&L summaries in background so first AI chat is fast
    try:
        warm_ticker_summaries_cache(start_year=2023)
    except Exception as exc:
        logger.warning(f"P&L cache warm-up failed: {exc}")

    # --run-now: catch up on missed scheduled jobs
    if args.run_now:
        def _catchup():
            logger.info("--run-now ▶ Monday open job (fetch S&P 500 symbols)...")
            try:
                _job_monday_open()
                logger.info("--run-now ✓ SP500 symbols updated.")
            except Exception as exc:
                logger.error(f"--run-now ✗ SP500 update failed: {exc}")
            logger.info("--run-now ▶ Daily prefetch job...")
            try:
                _job_daily_prefetch()
                logger.info("--run-now ✓ Daily prefetch job done.")
            except Exception as exc:
                logger.error(f"--run-now ✗ Daily prefetch failed: {exc}")

        t = threading.Thread(target=_catchup, daemon=True, name="catchup-jobs")
        t.start()
        logger.info("--run-now: catch-up jobs started in background.")

    yield  # ── server is running ──

    # Shutdown
    try:
        stop_options_scheduler()
    except Exception:
        pass
    logger.info("Options server stopped.")


app = FastAPI(
    lifespan=lifespan,
    title="Options Trading API (local)",
    description=(
        "Iron Condor options system — local server with direct access "
        "to the OptionSys data directory."
    ),
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(options_router)


@app.get("/health", tags=["Health"])
def health():
    """Quick health check — also confirms the data directory is accessible."""
    data_dir = os.environ.get("STOCK_BASE_DIR", _DEFAULT_DATA_DIR)
    return {
        "status": "ok",
        "service": "options-local-server",
        "port": args.port,
        "data_dir": data_dir,
        "data_dir_exists": os.path.isdir(data_dir),
    }


# ── Entry point ────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=args.port,
        log_level="info",
        access_log=True,
    )
