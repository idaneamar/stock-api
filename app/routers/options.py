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

from fastapi import APIRouter, BackgroundTasks, HTTPException
from pydantic import BaseModel

from app.services import options_service as svc
from app.services.options_scheduler import get_scheduler_jobs

# scripts that can be cancelled
_CANCELLABLE = {"optsp.py", "prefetch_options_datasp.py", "fetch_sp500_symbols.py"}

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


class ExecuteRequest(BaseModel):
    tickers: Optional[List[str]] = None
    rec_date: Optional[str] = None
    dry_run: bool = False


class ExecuteResponse(BaseModel):
    ok: bool
    message: str
    returncode: int
    details: str


class PrefetchRequest(BaseModel):
    years: Optional[List[int]] = None
    workers: int = 4


class RunOptspRequest(BaseModel):
    run_date: Optional[str] = None
    cache_only: bool = True


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
    )
    return ExecuteResponse(
        ok=result["ok"],
        message="Execution completed" if result["ok"] else "Execution failed",
        returncode=result["returncode"],
        details=(result["stdout"] + "\n" + result["stderr"]).strip()[-2000:],
    )


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
# POST /options/trigger/prefetch
# ---------------------------------------------------------------------------

@router.post("/trigger/prefetch")
def trigger_prefetch(req: PrefetchRequest, background_tasks: BackgroundTasks):
    """Manually trigger prefetch_options_datasp.py for given years (runs in background)."""
    years = req.years or [date.today().year]
    background_tasks.add_task(_bg_prefetch, years, req.workers)
    return {"message": f"prefetch started in background for years {years}"}


def _bg_prefetch(years: List[int], workers: int) -> None:
    result = svc.run_prefetch(years=years, workers=workers)
    if result["ok"]:
        logger.info(f"[trigger] prefetch OK for {years}")
    else:
        logger.error(f"[trigger] prefetch FAILED: {result['stderr'][-500:]}")


# ---------------------------------------------------------------------------
# POST /options/trigger/run-optsp
# ---------------------------------------------------------------------------

@router.post("/trigger/run-optsp")
def trigger_run_optsp(req: RunOptspRequest, background_tasks: BackgroundTasks):
    """Manually trigger optsp.py for a specific date (runs in background)."""
    # Always use today as the trade date so the output file is iron_condor_{today}.csv.
    # optsp's fetch_chain_for_ticker will fall back to the most recent available EOD
    # cache if today's data hasn't been downloaded yet (e.g. before market close).
    run_date = req.run_date or date.today().isoformat()
    # If already running, don't start a second copy.
    if svc.is_script_running("optsp.py"):
        return {
            "ok": False,
            "already_running": True,
            "message": "optsp is already running",
            "run_date": run_date,
        }

    job_id = f"optsp.py-{run_date}-{os.getpid()}"
    background_tasks.add_task(_bg_run_optsp, run_date, req.cache_only, job_id)
    return {
        "ok": True,
        "already_running": False,
        "job_id": job_id,
        "run_date": run_date,
        "message": f"optsp started in background for date {run_date}",
    }


def _bg_run_optsp(run_date: str, cache_only: bool, job_id: str) -> None:
    # If symbols are missing, fetch them first so optsp has a universe.
    try:
        symbols = svc.get_symbols()
        if not symbols:
            logger.info("[trigger] No symbols found – running fetch_sp500_symbols first...")
            svc.run_fetch_sp500()
    except Exception as exc:
        logger.warning(f"[trigger] fetch_sp500_symbols pre-step failed: {exc}")

    result = svc.run_optsp(run_date=run_date, cache_only=cache_only, job_id=job_id)
    if result["ok"]:
        logger.info(f"[trigger] optsp OK for {run_date}")
    else:
        logger.error(f"[trigger] optsp FAILED: {result['stderr'][-500:]}")
