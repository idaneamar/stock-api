"""
options.py  –  FastAPI router for the Iron Condor options trading system
------------------------------------------------------------------------

Endpoints
---------
GET  /options/recommendations              – today's (or most recent) iron condor recs
GET  /options/recommendations/dates       – list of dates that have recommendation files
GET  /options/recommendations/{date}      – recs for a specific YYYY-MM-DD
GET  /options/symbols                     – current S&P 500 symbol list
GET  /options/status                      – system status + scheduled job info
GET  /options/ai/ticker-history/{ticker}  – full P&L simulation + chain summary for a ticker
GET  /options/ai/data-coverage            – summary of all prefetched data in OptionSys DB
POST /options/execute                     – execute selected recs via exeopt → IBKR
POST /options/ai/chat                     – AI chat with full historical backtest context
POST /options/trigger/fetch-symbols       – manual: run fetch_sp500_symbols.py
POST /options/trigger/prefetch            – manual: run prefetch_options_datasp.py
POST /options/trigger/run-optsp           – manual: run optsp.py for a given date
"""

from __future__ import annotations

import logging
import os
from datetime import date
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, BackgroundTasks, Header, HTTPException, Request
from pydantic import BaseModel

from app.services import options_service as svc
from app.services.options_scheduler import get_scheduler_jobs

# scripts that can be cancelled
_CANCELLABLE = {"optsp.py", "opsp.py", "prefetch_options_datasp.py", "fetch_sp500_symbols.py"}

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/options",
    tags=["Options"],
    responses={404: {"description": "Not found"}},
)


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------

class OptionsRecommendation(BaseModel):
    ticker: str
    run_date: Optional[str] = None
    exp: Optional[str] = None
    dte: Optional[int] = None
    short_put: Optional[float] = None
    long_put: Optional[float] = None
    short_call: Optional[float] = None
    long_call: Optional[float] = None
    width: Optional[float] = None
    net_credit: Optional[float] = None
    net_credit_conservative: Optional[float] = None
    sp_bid: Optional[float] = None
    sp_ask: Optional[float] = None
    lp_bid: Optional[float] = None
    lp_ask: Optional[float] = None
    sc_bid: Optional[float] = None
    sc_ask: Optional[float] = None
    lc_bid: Optional[float] = None
    lc_ask: Optional[float] = None
    max_loss_per_share: Optional[float] = None
    pop_est: Optional[float] = None
    pop_method: Optional[str] = None
    spot: Optional[float] = None
    iv_est: Optional[float] = None
    sp_delta: Optional[float] = None
    sc_delta: Optional[float] = None
    score: Optional[float] = None
    contracts: Optional[int] = None
    max_risk_usd: Optional[float] = None
    max_profit_usd: Optional[float] = None
    max_profit_usd_conservative: Optional[float] = None
    margin_per_contract: Optional[float] = None
    total_margin: Optional[float] = None
    net_credit_opti_fill: Optional[float] = None
    min_recommended_price: Optional[float] = None


class ExecuteRequest(BaseModel):
    tickers: Optional[List[str]] = None
    rec_date: Optional[str] = None
    dry_run: bool = False
    port: int = 7497
    client_id: int = 1
    stop_loss_pct: float = 1.0
    take_profit_pct: float = 0.50


class CreatePendingExecutionRequest(BaseModel):
    rec_date: Optional[str] = None
    dry_run: bool = False
    port: int = 7497
    client_id: int = 1
    stop_loss_pct: float = 1.0
    take_profit_pct: float = 0.50


class ExecuteResponse(BaseModel):
    ok: bool
    message: str
    returncode: int
    details: str


class PrefetchRequest(BaseModel):
    years: Optional[List[int]] = None


class RunOptspRequest(BaseModel):
    run_date: Optional[str] = None
    max_trades: Optional[int] = None  # 10–100; default 40 when omitted


class ChatMessage(BaseModel):
    role: str    # "user" | "assistant"
    content: str


class OptionsChatRequest(BaseModel):
    messages: List[ChatMessage]
    recommendations: List[Dict[str, Any]] = []
    rec_date: Optional[str] = None
    portfolio_size: Optional[float] = None
    include_backtest_history: bool = True


class OptionsChatResponse(BaseModel):
    reply: str


class RecsResponse(BaseModel):
    date: Optional[str]
    recommendations: List[Dict[str, Any]]
    count: int


# ---------------------------------------------------------------------------
# GET /options/recommendations
# ---------------------------------------------------------------------------

@router.get("/recommendations", response_model=RecsResponse)
def get_latest_recommendations():
    """Return the most recent iron condor recommendations."""
    recs = svc.get_recommendations()
    latest = svc.get_latest_rec_date()
    return RecsResponse(date=latest, recommendations=recs, count=len(recs))


# ---------------------------------------------------------------------------
# GET /options/recommendations/dates
# ---------------------------------------------------------------------------

@router.get("/recommendations/dates")
def list_recommendation_dates(limit: int = 90):
    """List all dates that have recommendation files (newest first)."""
    dates = svc.get_available_rec_dates(limit=limit)
    return {"dates": dates, "count": len(dates)}


# ---------------------------------------------------------------------------
# GET /options/recommendations/{date}
# ---------------------------------------------------------------------------

@router.get("/recommendations/{rec_date}", response_model=RecsResponse)
def get_recommendations_for_date(rec_date: str):
    """Return recommendations for a specific date (YYYY-MM-DD)."""
    recs = svc.get_recommendations(rec_date)
    if not recs:
        try:
            date.fromisoformat(rec_date)
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid date format: {rec_date}. Use YYYY-MM-DD.",
            )
    return RecsResponse(date=rec_date, recommendations=recs, count=len(recs))


# ---------------------------------------------------------------------------
# DELETE /options/recommendations/{date}
# ---------------------------------------------------------------------------

@router.delete("/recommendations/{rec_date}")
def delete_recommendation_date(rec_date: str):
    """Delete all recommendations for a specific date (removes the CSV file)."""
    ok = svc.delete_rec_date(rec_date)
    if not ok:
        raise HTTPException(
            status_code=404,
            detail=f"No recommendation file found for {rec_date}.",
        )
    return {"ok": True, "message": f"Deleted all recommendations for {rec_date}."}


# ---------------------------------------------------------------------------
# DELETE /options/recommendations/{date}/{ticker}
# ---------------------------------------------------------------------------

@router.delete("/recommendations/{rec_date}/{ticker}")
def delete_recommendation_ticker(rec_date: str, ticker: str):
    """Remove a single ticker from a date's recommendation file."""
    ok = svc.delete_rec_ticker(rec_date, ticker.upper())
    if not ok:
        raise HTTPException(
            status_code=404,
            detail=f"Ticker {ticker.upper()} not found in recommendations for {rec_date}.",
        )
    return {
        "ok": True,
        "message": f"Deleted {ticker.upper()} from {rec_date} recommendations.",
    }


# ---------------------------------------------------------------------------
# GET /options/symbols
# ---------------------------------------------------------------------------

@router.get("/symbols")
def get_symbols():
    """Return the current S&P 500 symbol list."""
    symbols = svc.get_symbols()
    return {"symbols": symbols, "count": len(symbols)}


# ---------------------------------------------------------------------------
# POST /options/symbols/upload
# ---------------------------------------------------------------------------

class SymbolsUploadRequest(BaseModel):
    symbols: List[str]

@router.post("/symbols/upload")
def upload_symbols(payload: SymbolsUploadRequest):
    """Replace the stock universe with a custom symbol list.
    Expects { "symbols": ["AAPL", "MSFT", ...] }.
    """
    cleaned = [s.strip().upper() for s in payload.symbols if s.strip()]
    if not cleaned:
        raise HTTPException(status_code=400, detail="symbols list is empty after cleaning")
    try:
        svc.save_symbols(cleaned)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    return {"ok": True, "count": len(cleaned), "message": f"Saved {len(cleaned)} symbols."}


# ---------------------------------------------------------------------------
# POST /options/symbols/restore_default
# ---------------------------------------------------------------------------

@router.post("/symbols/restore_default")
def restore_default_symbols():
    """Restore the stock universe to the packaged S&P 500 default list."""
    try:
        count, sample = svc.restore_default_symbols()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    return {
        "ok": True,
        "count": count,
        "sample": sample[:10],
        "message": f"Restored default S&P 500 list ({count} symbols).",
    }


# ---------------------------------------------------------------------------
# POST /options/recommendations/reprice
# ---------------------------------------------------------------------------

class RepriceRequest(BaseModel):
    ticker: str
    expiration: str          # YYYY-MM-DD
    short_put: float
    long_put: float
    short_call: float
    long_call: float
    contracts: int = 1
    width: Optional[float] = None
    strict_validity: bool = False


@router.post("/recommendations/reprice")
def reprice_recommendation(payload: RepriceRequest):
    """
    Fetch fresh quotes for a specific iron-condor set and return updated metrics.

    Returns:
      spot, per-leg bid/ask/mid, debug (legs, spreads, server_timestamp_utc),
      validity_severity (valid|caution|not_ideal), validity_reasons,
      is_valid, response_time_ms
    """
    import time as _time
    start = _time.perf_counter()
    logger.info("reprice request: ticker=%s strict_validity=%s", payload.ticker, payload.strict_validity)
    try:
        result = svc.reprice_iron_condor(
            ticker=payload.ticker,
            expiration=payload.expiration,
            short_put=payload.short_put,
            long_put=payload.long_put,
            short_call=payload.short_call,
            long_call=payload.long_call,
            contracts=payload.contracts,
            width=payload.width,
            strict_validity=payload.strict_validity,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    elapsed_ms = int((_time.perf_counter() - start) * 1000)
    result["response_time_ms"] = elapsed_ms
    if "debug" in result and isinstance(result["debug"], dict):
        result["debug"]["response_time_ms"] = elapsed_ms
    return result


# ---------------------------------------------------------------------------
# Actual fills — per-user persistence (source of truth on backend)
# ---------------------------------------------------------------------------

def _user_id_from_request(request: Request, x_user_id: Optional[str] = Header(None, alias="X-User-Id")) -> str:
    """Resolve user id from X-User-Id header, then username query param, default 'idane'."""
    if x_user_id and x_user_id.strip():
        return x_user_id.strip()
    username = request.query_params.get("username")
    if username and str(username).strip():
        return str(username).strip()
    return "idane"


class ActualFillPutBody(BaseModel):
    rec_key: str
    ticker: str
    expiration: str
    short_put: float
    long_put: float
    short_call: float
    long_call: float
    actual_net_premium_per_contract: float
    actual_contracts: Optional[int] = None
    entry_pop: Optional[float] = None
    is_entered: Optional[bool] = True


def _upsert_actual_fill_payload(
    payload: ActualFillPutBody,
    request: Request,
    x_user_id: Optional[str],
):
    user_id = _user_id_from_request(request, x_user_id)
    try:
        svc.upsert_actual_fill(
            user_id=user_id,
            rec_key=payload.rec_key,
            ticker=payload.ticker,
            expiration=payload.expiration,
            short_put=payload.short_put,
            long_put=payload.long_put,
            short_call=payload.short_call,
            long_call=payload.long_call,
            actual_net_premium_per_contract=payload.actual_net_premium_per_contract,
            actual_contracts=payload.actual_contracts,
            entry_pop=payload.entry_pop,
            is_entered=payload.is_entered if payload.is_entered is not None else True,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    return {"ok": True, "rec_key": payload.rec_key}


@router.put("/actual_fills")
def put_actual_fill(
    payload: ActualFillPutBody,
    request: Request,
    x_user_id: Optional[str] = Header(None, alias="X-User-Id"),
):
    """UPSERT: insert or overwrite actual fill for (user_id, rec_key)."""
    return _upsert_actual_fill_payload(payload, request, x_user_id)


@router.post("/actual_fills")
def post_actual_fill(
    payload: ActualFillPutBody,
    request: Request,
    x_user_id: Optional[str] = Header(None, alias="X-User-Id"),
):
    """POST alias for browsers/proxies that block PUT preflight to localhost."""
    return _upsert_actual_fill_payload(payload, request, x_user_id)


@router.get("/actual_fills")
def get_actual_fills(
    request: Request,
    x_user_id: Optional[str] = Header(None, alias="X-User-Id"),
    date: Optional[str] = None,
    tickers: Optional[str] = None,
):
    """
    Return all actual fills for the user, keyed by rec_key.
    User: X-User-Id header, or username query param (default 'idane').
    Optional: date=YYYY-MM-DD or tickers=AAPL,MSFT to filter.
    """
    user_id = _user_id_from_request(request, x_user_id)
    ticker_list = [t.strip() for t in tickers.split(",")] if tickers else None
    try:
        fills = svc.get_actual_fills(user_id=user_id, date_filter=date, tickers=ticker_list)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    return {"fills": fills}


@router.delete("/actual_fills/all")
def delete_all_actual_fills(
    request: Request,
    x_user_id: Optional[str] = Header(None, alias="X-User-Id"),
):
    """Delete ALL actual fills for the current user. Resets their active-positions state to empty."""
    user_id = _user_id_from_request(request, x_user_id)
    try:
        count = svc.delete_all_actual_fills(user_id=user_id)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    return {"ok": True, "deleted": count, "user_id": user_id}


@router.delete("/actual_fills/{rec_key:path}")
def delete_actual_fill(
    rec_key: str,
    request: Request,
    x_user_id: Optional[str] = Header(None, alias="X-User-Id"),
):
    """Delete the stored actual fill for this user and rec_key."""
    user_id = _user_id_from_request(request, x_user_id)
    try:
        deleted = svc.delete_actual_fill(user_id=user_id, rec_key=rec_key)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    if not deleted:
        raise HTTPException(status_code=404, detail="Actual fill not found")
    return {"ok": True, "rec_key": rec_key}


class ManualExitBody(BaseModel):
    exit_price: float
    exit_date: str  # YYYY-MM-DD
    username: Optional[str] = None
    is_active: Optional[bool] = False


@router.post("/actual_fills/{rec_key:path}/exit")
def manual_exit_position(
    rec_key: str,
    body: ManualExitBody,
    request: Request,
    x_user_id: Optional[str] = Header(None, alias="X-User-Id"),
):
    """
    Manually close a position: set is_active=false, save exit_price and exit_date.
    User: X-User-Id header, username in body, or username query param (default 'idane').
    """
    user_id = (body.username and body.username.strip()) or _user_id_from_request(request, x_user_id)
    try:
        updated = svc.set_actual_fill_exit(
            user_id=user_id,
            rec_key=rec_key,
            exit_price=body.exit_price,
            exit_date=body.exit_date,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    if not updated:
        raise HTTPException(status_code=404, detail="Position not found or already exited")
    return {"ok": True, "rec_key": rec_key, "message": "Position closed"}


# ---------------------------------------------------------------------------
# GET /options/active-positions
# ---------------------------------------------------------------------------

@router.get("/active-positions")
def get_active_positions(
    request: Request,
    x_user_id: Optional[str] = Header(None, alias="X-User-Id"),
):
    """
    Return active positions (is_entered=1, is_active=1) with live P&L, updated POP, pop_warning.
    User: X-User-Id header or username query param (default 'idane').
    """
    user_id = _user_id_from_request(request, x_user_id)
    try:
        positions = svc.get_active_positions_with_stats(user_id=user_id)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    return {"active_positions": positions}


# ---------------------------------------------------------------------------
# GET /options/status
# ---------------------------------------------------------------------------

@router.get("/status")
def get_status():
    """Return system status, scheduler info, and data directory details."""
    status = svc.get_status()
    status["scheduled_jobs"] = get_scheduler_jobs()
    return status


# ---------------------------------------------------------------------------
# GET /options/job-logs
# ---------------------------------------------------------------------------

@router.get("/job-logs")
def get_job_logs(limit: int = 50):
    """
    Return the most recent script execution log entries (newest first).

    Each entry contains:
      script, label, args, started_at, ended_at, duration_s,
      ok, status (running | ok | error | timeout),
      stdout_tail, stderr_tail, summary
    """
    return {"logs": svc.get_job_logs(limit=limit), "count": limit}


# ---------------------------------------------------------------------------
# GET /options/job-logs/download — download logs as JSON file
# ---------------------------------------------------------------------------

@router.get("/job-logs/download")
def download_job_logs(limit: int = 500):
    """
    Return job logs as a downloadable JSON file for debugging.
    Use for uploading elsewhere (e.g. support) to diagnose issues.
    """
    from fastapi.responses import JSONResponse
    logs = svc.get_job_logs(limit=limit)
    return JSONResponse(
        content={"logs": logs, "count": len(logs)},
        headers={
            "Content-Disposition": 'attachment; filename="options_job_logs.json"',
        },
    )


# ---------------------------------------------------------------------------
# GET /options/morning-status
# ---------------------------------------------------------------------------

@router.get("/morning-status")
def get_morning_status():
    """
    One-stop morning briefing:
      - last_prefetch: when it ran, duration, ok/error, summary
      - last_sp500_update: when it ran, added/removed tickers
      - last_optsp: when recommendations were last generated
      - running_jobs: any jobs currently in progress
      - latest_rec_date / latest_rec_count: current recommendation state
    """
    return svc.get_morning_status()


# ---------------------------------------------------------------------------
# GET /options/sp500-diff
# ---------------------------------------------------------------------------

@router.get("/sp500-diff")
def get_sp500_diff():
    """
    Return the most recent S&P 500 symbol change diff (added / removed tickers).
    Populated every Monday when fetch_sp500_symbols.py runs.
    """
    diff = svc.get_sp500_diff()
    if diff is None:
        return {
            "available": False,
            "message": "No SP500 diff recorded yet. Run fetch-symbols first.",
        }
    return {"available": True, **diff}


# ---------------------------------------------------------------------------
# GET /options/ai/data-coverage
# ---------------------------------------------------------------------------

@router.get("/ai/data-coverage")
def get_data_coverage():
    """
    Summary of all prefetched data available in the OptionSys database.
    Shows ticker count, date ranges for options cache, price history, and backtest results.
    """
    return svc.get_data_coverage()


# ---------------------------------------------------------------------------
# GET /options/ai/ticker-history/{ticker}
# ---------------------------------------------------------------------------

@router.get("/ai/ticker-history/{ticker}")
def get_ticker_history(
    ticker: str,
    start_year: int = 2023,
    end_year: Optional[int] = None,
):
    """
    Full P&L simulation for a ticker using actual expiry prices from the
    historical_data_cache (not heuristics).

    Returns:
      - All historical IC trades for the ticker with real P&L at expiry
      - Aggregated statistics (win rate, avg P&L, outcome breakdown)
      - Options chain availability summary
    """
    ticker = ticker.upper()
    trades = svc.simulate_ic_pnl_for_ticker(ticker, start_year=start_year, end_year=end_year)
    stats  = svc.aggregate_pnl_stats(trades)
    stats["ticker"] = ticker
    stats["years"]  = f"{start_year}-{end_year or date.today().year}"

    # Summary of available options cache dates for this ticker
    cache_dates = svc.get_options_cache_dates(ticker)
    chain_coverage = {
        "cached_days":  len(cache_dates),
        "earliest":     cache_dates[0]  if cache_dates else None,
        "latest":       cache_dates[-1] if cache_dates else None,
    }

    return {
        "ticker":         ticker,
        "stats":          stats,
        "trades":         trades,
        "chain_coverage": chain_coverage,
    }


# ---------------------------------------------------------------------------
# GET /options/ibkr-ping
# ---------------------------------------------------------------------------

@router.get("/ibkr-ping")
def ibkr_ping(port: int = 7497, client_id: int = 1):
    """
    Test the IBKR connection without loading recommendations or placing orders.
    Connects to TWS/Gateway, retrieves managed accounts, then disconnects.

    Returns:
      ok            – true if connection succeeded and accounts were found
      accounts      – list of managed account IDs (e.g. ["DU12345"])
      stdout        – full exeopt log (useful for diagnosing failures)
      duration_s    – how long the test took
    """
    result = svc.run_ibkr_ping(port=port, client_id=client_id)
    # Parse the account list from stdout if present
    accounts: list = []
    for line in result.get("stdout", "").splitlines():
        if "Connected to IBKR — accounts:" in line:
            try:
                raw = line.split("accounts:")[1].strip()
                # raw looks like "['DU12345', 'DU99999']"
                import ast
                accounts = ast.literal_eval(raw)
            except Exception:
                pass
    return {
        "ok": result["ok"],
        "accounts": accounts,
        "stdout": result.get("stdout", ""),
        "duration_s": result.get("duration_s"),
        "port": port,
        "client_id": client_id,
    }


# ---------------------------------------------------------------------------
# POST /options/execute
# ---------------------------------------------------------------------------

@router.post("/execute", response_model=ExecuteResponse)
def execute_recommendations(req: ExecuteRequest, background_tasks: BackgroundTasks):
    """
    Execute iron condor recommendations via exeopt.py → IBKR TWS/Gateway.
    If dry_run=True the order logic runs but no real orders are placed.
    """
    result = svc.run_exeopt(
        rec_date=req.rec_date,
        dry_run=req.dry_run,
        tickers=req.tickers,
        port=req.port,
        client_id=req.client_id,
        stop_loss_pct=req.stop_loss_pct,
        take_profit_pct=req.take_profit_pct,
    )
    return ExecuteResponse(
        ok=result["ok"],
        message="Execution completed" if result["ok"] else "Execution failed",
        returncode=result["returncode"],
        details=f"STDOUT:\n{result['stdout']}\nSTDERR:\n{result['stderr']}",
    )


# ---------------------------------------------------------------------------
# POST /options/pending-execution   — create 5-min countdown job
# GET  /options/pending-execution   — poll status + remaining seconds
# POST /options/pending-execution/confirm — execute immediately
# POST /options/pending-execution/cancel  — cancel
# ---------------------------------------------------------------------------

@router.post("/pending-execution")
async def create_pending_execution(req: CreatePendingExecutionRequest):
    """
    Create a pending execution job with a 5-minute countdown (manual use only).
    Do NOT call this after generating recommendations — the app does not
    auto-send to IBKR; the user sends manually via Execute / Send to IBKR.
    """
    from app.services import pending_execution as pe

    rec_date = req.rec_date or date.today().isoformat()
    job = await pe.create_pending_job(
        rec_date=rec_date,
        config={
            "dry_run": req.dry_run,
            "port": req.port,
            "client_id": req.client_id,
            "stop_loss_pct": req.stop_loss_pct,
            "take_profit_pct": req.take_profit_pct,
        },
    )
    return job.to_dict()


@router.get("/pending-execution")
async def get_pending_execution():
    """
    Poll the current pending execution job status.
    Returns {"status": "none"} if no active job.
    """
    from app.services import pending_execution as pe

    job = pe.get_current_job()
    if not job:
        return {"status": "none"}
    return job.to_dict()


@router.post("/pending-execution/confirm")
async def confirm_pending_execution(job_id: str):
    """Execute the pending job immediately (user tapped 'Send Now')."""
    from app.services import pending_execution as pe

    job = await pe.confirm_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="No matching pending job found")
    return job.to_dict()


@router.post("/pending-execution/cancel")
async def cancel_pending_execution(job_id: Optional[str] = None):
    """Cancel the pending job (user tapped 'Cancel')."""
    from app.services import pending_execution as pe

    cancelled = await pe.cancel_job(job_id)
    return {"cancelled": cancelled}


# ---------------------------------------------------------------------------
# POST /options/ai/chat
# ---------------------------------------------------------------------------

def _build_options_system_prompt(
    recommendations: List[Dict[str, Any]],
    rec_date: Optional[str],
    portfolio_size: Optional[float],
    include_history: bool,
) -> str:
    """
    Build a rich system prompt for the Options AI assistant.

    Includes:
      1. Current iron condor recommendations with full details
      2. Data coverage summary (what's in the OptionSys database)
      3. Per-ticker historical P&L from actual expiry price simulation
         (NOT a heuristic — real prices from historical_data_cache)
    """
    date_str      = rec_date or "latest"
    portfolio_str = f"${portfolio_size:,.0f}" if portfolio_size else "not specified"

    lines = [
        "You are a professional options trading assistant specialising in Iron Condor strategies.",
        f"Recommendation date: {date_str} | Portfolio size: {portfolio_str}",
        "",
        "STRATEGY OVERVIEW:",
        "An Iron Condor sells an OTM put spread + OTM call spread simultaneously.",
        "Max profit = net credit collected (if price stays between short strikes at expiry).",
        "Max loss   = width of one spread − net credit (if price blows through long strikes).",
        "Breakevens: lower = short_put − credit, upper = short_call + credit.",
        "",
    ]

    # ── 1. Current recommendations ─────────────────────────────────────────
    lines += ["TODAY'S IRON CONDOR RECOMMENDATIONS:", "─" * 60]

    if not recommendations:
        lines.append("No recommendations available for this date.")
    else:
        for i, r in enumerate(recommendations, 1):
            ticker    = r.get("ticker", "?")
            exp       = r.get("exp", "?")
            dte       = r.get("dte", "?")
            sp        = r.get("short_put", "?")
            lp        = r.get("long_put", "?")
            sc        = r.get("short_call", "?")
            lc        = r.get("long_call", "?")
            credit    = float(r.get("net_credit", 0) or 0)
            max_loss  = float(r.get("max_loss_per_share", 0) or 0)
            pop       = float(r.get("pop_est", 0) or 0)
            spot      = r.get("spot", "?")
            score     = r.get("score", "?")
            contracts = r.get("contracts", "?")
            max_risk  = r.get("max_risk_usd", "?")
            iv_est    = r.get("iv_est", "?")
            width     = r.get("width", "?")

            be_lower = (float(sp) - credit) if isinstance(sp, (int, float)) else "?"
            be_upper = (float(sc) + credit) if isinstance(sc, (int, float)) else "?"

            lines.append(
                f"\n{i}. {ticker}  |  spot: {spot}  |  IV: {iv_est}"
                f"\n   Expiry: {exp}  ({dte} DTE)"
                f"\n   Put spread:   long {lp} / short {sp}"
                f"\n   Call spread:  short {sc} / long {lc}  (width: {width})"
                f"\n   Net credit:   ${credit:.2f}/share"
                f"\n   Max loss:     ${max_loss:.2f}/share"
                f"\n   Breakevens:   {be_lower} ↔ {be_upper}"
                f"\n   POP: {pop:.1%}  |  Score: {score}  |  Contracts: {contracts}  |  Max risk: ${max_risk}"
            )

    lines += ["─" * 60, ""]

    # ── 2. Data coverage ────────────────────────────────────────────────────
    try:
        coverage = svc.get_data_coverage()
        opt_c   = coverage.get("options_cache", {})
        price_c = coverage.get("price_history", {})
        bt_c    = coverage.get("backtest_results", {})

        lines += [
            "OPTIONSYS DATABASE COVERAGE:",
            "─" * 60,
            f"  Options chain cache: {opt_c.get('ticker_count', 0)} tickers",
            f"    Date range: {opt_c.get('date_range', {}).get('earliest', '?')} → "
            f"{opt_c.get('date_range', {}).get('latest', '?')}",
            f"    Contains: full options chain (strikes, bid/ask, delta, gamma, theta, vega, IV) per trading day",
            f"  Price history: {price_c.get('ticker_count', 0)} tickers",
            f"    Date range: {price_c.get('date_range', {}).get('earliest', '?')} → "
            f"{price_c.get('date_range', {}).get('latest', '?')}",
            f"  Backtest results: years {bt_c.get('years', [])}",
            f"    Generated by optsp for every trading day in those years.",
            "─" * 60,
            "",
        ]
    except Exception as exc:
        logger.warning(f"Could not load data coverage: {exc}")

    # ── 3. Historical P&L simulation (actual expiry prices) ─────────────────
    if include_history:
        try:
            summaries = svc.get_all_ticker_summaries(start_year=2023)
            if summaries:
                rec_tickers = {r.get("ticker", "").upper() for r in recommendations}

                lines += [
                    "HISTORICAL P&L PERFORMANCE (2023 – present):",
                    "Computed using ACTUAL closing prices at expiry from historical_data_cache.",
                    "P&L formula: net_credit − put_spread_loss − call_spread_loss at expiry date.",
                    "─" * 60,
                    f"  {'Ticker':<8}  {'Trades':>6}  {'Win%':>6}  {'MaxP':>5}  {'PartP':>5}  "
                    f"{'SmlL':>5}  {'MaxL':>5}  {'AvgPnL/sh':>10}  {'TotPnL/sh':>10}  {'AvgCredit':>9}",
                    "  " + "─" * 78,
                ]

                # Current recs first, then sorted by total_pnl descending
                ordered = [s for s in summaries if s["ticker"].upper() in rec_tickers]
                rest    = sorted(
                    [s for s in summaries if s["ticker"].upper() not in rec_tickers],
                    key=lambda x: x.get("total_pnl_per_share", 0),
                    reverse=True,
                )
                ordered += rest[:30]

                for s in ordered[:40]:
                    ob = s.get("outcome_breakdown", {})
                    lines.append(
                        f"  {s['ticker']:<8}  {s['simulated_trades']:>6}  "
                        f"{s['win_rate_pct']:>5.1f}%  "
                        f"{ob.get('max_profit', 0):>5}  "
                        f"{ob.get('partial_profit', 0):>5}  "
                        f"{ob.get('small_loss', 0):>5}  "
                        f"{ob.get('max_loss', 0):>5}  "
                        f"{s['avg_pnl_per_share']:>+10.3f}  "
                        f"{s['total_pnl_per_share']:>+10.3f}  "
                        f"{s['avg_credit']:>9.3f}"
                    )

                lines += ["─" * 60, ""]
        except Exception as exc:
            logger.warning(f"Could not load historical P&L summaries: {exc}")

    # ── 4. Role instructions ────────────────────────────────────────────────
    lines += [
        "YOUR ROLE:",
        "- Analyse the iron condor recommendations above and answer the user's questions.",
        "- Use the HISTORICAL P&L table to answer 'what would have happened' questions.",
        "  The win%, avg P&L, and outcome breakdown are computed from REAL expiry prices — not estimates.",
        "- The options chain database (options_data_cache) covers every S&P 500 ticker from 2023.",
        "  You can reference IV levels, strike availability, and Greeks for any ticker/date in that range.",
        "- For questions about a specific ticker's full trade-by-trade history, tell the user to use",
        "  the GET /options/ai/ticker-history/{ticker} endpoint for the complete simulation.",
        "- For allocation questions, consider max_risk_usd and portfolio_size.",
        "- Be concise and actionable.",
        "- Do NOT invent data — only use the numbers provided above.",
    ]

    return "\n".join(lines)


@router.post("/ai/chat", response_model=OptionsChatResponse)
async def options_ai_chat(req: OptionsChatRequest):
    """
    AI chat endpoint with full iron condor context:
      - Current recommendations with strikes, Greeks, and breakevens
      - OptionSys database coverage (499 tickers, 2023 → present)
      - Per-ticker P&L simulation using ACTUAL expiry prices
    """
    api_key = os.environ.get("OPENAI_API_KEY", "")
    if not api_key:
        raise HTTPException(
            status_code=503,
            detail="AI service not configured. Set OPENAI_API_KEY on the server.",
        )

    try:
        from openai import OpenAI

        client = OpenAI(api_key=api_key)

        recs = req.recommendations
        if not recs and req.rec_date:
            recs = svc.get_recommendations(req.rec_date)
        elif not recs:
            recs = svc.get_recommendations()

        system_prompt = _build_options_system_prompt(
            recommendations=recs,
            rec_date=req.rec_date or svc.get_latest_rec_date(),
            portfolio_size=req.portfolio_size,
            include_history=req.include_backtest_history,
        )

        openai_messages = [{"role": "system", "content": system_prompt}]
        for msg in req.messages:
            openai_messages.append({"role": msg.role, "content": msg.content})

        response = client.chat.completions.create(
            model="gpt-4o",
            messages=openai_messages,
            max_tokens=1500,
            temperature=0.4,
        )

        reply = response.choices[0].message.content or ""
        return OptionsChatResponse(reply=reply)

    except HTTPException:
        raise
    except Exception as exc:
        logger.error(f"Options AI chat error: {exc}")
        raise HTTPException(status_code=500, detail=f"AI error: {str(exc)}")


# ---------------------------------------------------------------------------
# POST /options/cancel
# ---------------------------------------------------------------------------

@router.post("/cancel")
def cancel_script(script: str = "optsp.py"):
    """
    Kill a currently running script.
    script: one of optsp.py | prefetch_options_datasp.py | fetch_sp500_symbols.py
    """
    if script not in _CANCELLABLE:
        raise HTTPException(status_code=400, detail=f"Unknown script: {script}")
    was_running = svc.cancel_script(script)
    return {
        "ok": was_running,
        "message": f"{script} cancelled" if was_running else f"{script} was not running",
    }


# ---------------------------------------------------------------------------
# POST /options/trigger/fetch-symbols
# ---------------------------------------------------------------------------

@router.post("/trigger/fetch-symbols")
def trigger_fetch_symbols(background_tasks: BackgroundTasks):
    """Manually trigger fetch_sp500_symbols.py (runs in background)."""
    background_tasks.add_task(_bg_fetch_symbols)
    return {"message": "fetch_sp500_symbols started in background"}


def _bg_fetch_symbols() -> None:
    result = svc.run_fetch_sp500()
    if result["ok"]:
        logger.info("[trigger] fetch_sp500 OK")
    else:
        logger.error(f"[trigger] fetch_sp500 FAILED: {result['stderr'][-500:]}")


# ---------------------------------------------------------------------------
# POST /options/trigger/prefetch  (Sync Data: cache check + prefetch only)
# ---------------------------------------------------------------------------

@router.post("/trigger/prefetch")
def trigger_prefetch(req: PrefetchRequest, background_tasks: BackgroundTasks):
    """
    Sync Data only: check today's cache, prefetch missing symbols (v3), then
    optional full prefetch for years. Does NOT run optsp or generate recommendations.
    Runs in background; poll GET /options/status or morning status for completion.
    """
    years = req.years  # None or e.g. [2024, 2025] for full prefetch after sync
    background_tasks.add_task(_bg_sync_data, years)
    return {
        "ok": True,
        "message": (
            "Sync started: checking cache, prefetching missing today. "
            "Check Activity or status for completion."
        ),
        "years": years,
    }


def _bg_sync_data(years: Optional[List[int]]) -> None:
    """Background: prefetch today if missing, optionally prefetch years. Only calls run_sync_data (no optsp)."""
    try:
        svc.run_sync_data(years=years)
        logger.info("[trigger] Sync data completed")
    except Exception as e:
        logger.exception("[trigger] Sync data failed: %s", e)


def _bg_prefetch(years: List[int]) -> None:
    """Legacy: prefetch only (no cache check, no optsp). Use _bg_sync_data for full sync."""
    result = svc.run_prefetch(years=years)
    if result["ok"]:
        logger.info(f"[trigger] ThetaData prefetch OK for {years}")
    else:
        logger.error(f"[trigger] ThetaData prefetch FAILED: {result['stderr'][-500:]}")


# ---------------------------------------------------------------------------
# POST /options/trigger/run-optsp
# ---------------------------------------------------------------------------

@router.post("/trigger/run-optsp")
def trigger_run_optsp(req: RunOptspRequest, background_tasks: BackgroundTasks):
    """Manually trigger opsp.py (ThetaData) for a specific date (runs in background)."""
    run_date = req.run_date or date.today().isoformat()
    max_trades = req.max_trades  # 10–100; None = use script default (40)
    if svc.is_script_running("opsp.py"):
        return {
            "ok": False,
            "already_running": True,
            "message": "opsp.py is already running",
            "run_date": run_date,
        }

    job_id = f"opsp.py-{run_date}-{os.getpid()}"
    background_tasks.add_task(_bg_run_optsp, run_date, job_id, max_trades)
    return {
        "ok": True,
        "already_running": False,
        "job_id": job_id,
        "run_date": run_date,
        "message": f"opsp.py started in background for date {run_date}",
    }


def _bg_run_optsp(run_date: str, job_id: str, max_trades: Optional[int] = None) -> None:
    # If symbols are missing, fetch them first so opsp has a universe.
    try:
        symbols = svc.get_symbols()
        if not symbols:
            logger.info("[trigger] No symbols found – running fetch_sp500_symbols first...")
            svc.run_fetch_sp500()
    except Exception as exc:
        logger.warning(f"[trigger] fetch_sp500_symbols pre-step failed: {exc}")

    result = svc.run_optsp(run_date=run_date, job_id=job_id, max_trades=max_trades)
    if result["ok"]:
        logger.info(f"[trigger] opsp.py OK for {run_date}")
    else:
        logger.error(f"[trigger] opsp.py FAILED: {result['stderr'][-500:]}")
