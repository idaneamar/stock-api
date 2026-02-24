"""
options_service.py
------------------
Service layer for the Iron Condor options trading system.

Data sources (all in OPTIONSYS_DATA_DIR = /Volumes/Extreme Pro/OptionSys):
  historical_data_cache/{TICKER}.parquet     — daily OHLCV prices
  options_data_cache/unicornbay/options_eod/{TICKER}/{DATE}.json.gz
                                             — full options chain per day
  backtest_optsp_recommendations/{YEAR}/iron_condor_{DATE}.csv
                                             — daily IC recommendations 2023-2025
  options_recommendations/iron_condor_{DATE}.csv
                                             — live daily recommendations
  sp500_symbols.csv                          — current S&P 500 list
"""

from __future__ import annotations

import gzip
import json
import logging
import os
import subprocess
import sys
from datetime import date, datetime, timedelta
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import threading
import time

import numpy as np
import pandas as pd
import pytz

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

SCRIPTS_DIR = str(Path(__file__).resolve().parents[2] / "options")

OPTIONSYS_DATA_DIR = os.environ.get(
    "STOCK_BASE_DIR",
    SCRIPTS_DIR,  # default: same folder as scripts if env var not set
)

RECS_DIR            = os.path.join(OPTIONSYS_DATA_DIR, "options_recommendations")
BACKTEST_DIR        = os.path.join(OPTIONSYS_DATA_DIR, "backtest_optsp_recommendations")
HIST_CACHE_DIR      = os.path.join(OPTIONSYS_DATA_DIR, "historical_data_cache")
OPTIONS_CACHE_DIR   = os.path.join(OPTIONSYS_DATA_DIR, "options_data_cache", "unicornbay", "options_eod")
SYMBOLS_CSV         = os.path.join(OPTIONSYS_DATA_DIR, "sp500_symbols.csv")
STATE_FILE          = os.path.join(OPTIONSYS_DATA_DIR, "backtest_optsp_state.json")
SP500_DIFF_FILE     = os.path.join(OPTIONSYS_DATA_DIR, "sp500_diff.json")

OUTPUT_PREFIX = "iron_condor"
ET = pytz.timezone("America/New_York")

JOB_LOGS_FILE = os.path.join(OPTIONSYS_DATA_DIR, "options_job_logs.json")

# Friendly display names for each script
_SCRIPT_LABELS: Dict[str, str] = {
    "fetch_sp500_symbols.py":   "Fetch S&P 500 Symbols",
    "prefetch_options_datasp.py": "Prefetch Options Data",
    "optsp.py":                 "Generate Recommendations (optsp)",
    "exeopt.py":                "Execute Orders (exeopt)",
    "backtest_optsp_system.py": "Backtest",
    "Summary_optsp.py":         "Build Summary Report",
}

# ---------------------------------------------------------------------------
# Running process registry — allows cancellation
# ---------------------------------------------------------------------------

_RUNNING_PROCS: Dict[str, "subprocess.Popen[str]"] = {}   # script_name → Popen
_PROCS_LOCK = threading.Lock()


def cancel_script(script_name: str) -> bool:
    """Kill a running script subprocess. Returns True if it was running."""
    with _PROCS_LOCK:
        proc = _RUNNING_PROCS.get(script_name)
    if proc is None:
        return False
    try:
        proc.kill()
        logger.info(f"[job] CANCELLED {script_name}")
    except Exception as exc:
        logger.warning(f"[job] cancel {script_name} error: {exc}")
    return True


def is_script_running(script_name: str) -> bool:
    with _PROCS_LOCK:
        proc = _RUNNING_PROCS.get(script_name)
    if proc is None:
        return False
    return proc.poll() is None   # None means still running


# ---------------------------------------------------------------------------
# Running log reconciliation (fix phantom "running" entries)
# ---------------------------------------------------------------------------

def _reconcile_job_logs(max_running_age_s: int = 2 * 60 * 60) -> int:
    """Mark stale/phantom 'running' log entries as interrupted.

    This fixes the UI case where a job shows as running even though the process
    is no longer alive (e.g. server restart, crash, or logs not updated).
    """
    now = datetime.now(ET)
    updated = 0
    with _JOB_LOGS_LOCK:
        for entry in _JOB_LOGS:
            if entry.get("status") != "running":
                continue

            script = entry.get("script")
            started_at = entry.get("started_at")

            # If we can prove the process isn't running, mark as interrupted.
            proc_alive = bool(script) and is_script_running(str(script))

            too_old = False
            if started_at:
                try:
                    started_dt = datetime.fromisoformat(str(started_at))
                    age_s = (now - started_dt).total_seconds()
                    too_old = age_s > max_running_age_s
                except Exception:
                    too_old = False

            if (not proc_alive) or too_old:
                entry["status"] = "interrupted"
                entry["ok"] = False
                entry["ended_at"] = now.isoformat()
                entry["duration_s"] = entry.get("duration_s") or None
                entry["summary"] = (
                    "Interrupted (process not running)"
                    if not too_old
                    else f"Interrupted (stale > {max_running_age_s}s)"
                )
                updated += 1

    if updated:
        _save_job_logs()
    return updated

# ---------------------------------------------------------------------------
# Job log — in-memory + persisted to JSON
# ---------------------------------------------------------------------------

_JOB_LOGS: List[Dict[str, Any]] = []   # newest at front
_JOB_LOGS_LOCK = threading.Lock()
_MAX_LOGS = 200


def _load_job_logs() -> None:
    """Load persisted logs from disk on startup.
    Any entries still marked 'running' were interrupted by the previous server
    shutdown — mark them as 'interrupted' so the UI doesn't show phantom jobs.
    """
    global _JOB_LOGS
    try:
        if os.path.exists(JOB_LOGS_FILE):
            with open(JOB_LOGS_FILE, encoding="utf-8") as f:
                logs = json.load(f)
            interrupted = 0
            for entry in logs:
                if entry.get("status") == "running":
                    entry["status"] = "interrupted"
                    entry["ok"] = False
                    entry["summary"] = "Interrupted (server restarted)"
                    interrupted += 1
            _JOB_LOGS = logs
            logger.info(
                f"[job-log] Loaded {len(_JOB_LOGS)} entries from {JOB_LOGS_FILE}"
                + (f" ({interrupted} stale 'running' → 'interrupted')" if interrupted else "")
            )
            if interrupted:
                _save_job_logs()
    except Exception as exc:
        logger.warning(f"[job-log] Could not load {JOB_LOGS_FILE}: {exc}")


def _save_job_logs() -> None:
    """Persist in-memory logs to disk (best-effort)."""
    try:
        os.makedirs(OPTIONSYS_DATA_DIR, exist_ok=True)
        with open(JOB_LOGS_FILE, "w", encoding="utf-8") as f:
            json.dump(_JOB_LOGS[:_MAX_LOGS], f, indent=2)
    except Exception as exc:
        logger.warning(f"[job-log] Could not save {JOB_LOGS_FILE}: {exc}")


def _append_job_log(entry: Dict[str, Any]) -> None:
    """Thread-safe append of a log entry (newest first) and persist."""
    with _JOB_LOGS_LOCK:
        _JOB_LOGS.insert(0, entry)
        if len(_JOB_LOGS) > _MAX_LOGS:
            del _JOB_LOGS[_MAX_LOGS:]
    _save_job_logs()


def get_job_logs(limit: int = 50) -> List[Dict[str, Any]]:
    """Return the most recent job log entries (newest first)."""
    _reconcile_job_logs()
    with _JOB_LOGS_LOCK:
        return list(_JOB_LOGS[:limit])


# Load persisted logs immediately when module is imported
_load_job_logs()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _script(name: str) -> str:
    return os.path.join(SCRIPTS_DIR, name)


def _run_script(
    script_name: str,
    extra_args: Optional[List[str]] = None,
    extra_env: Optional[Dict[str, str]] = None,
    timeout: int = 3600,
    job_id: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Run a Python script as a subprocess, record the execution in the job log,
    and return stdout/stderr/returncode.
    """
    env = os.environ.copy()
    env["STOCK_BASE_DIR"] = OPTIONSYS_DATA_DIR
    if extra_env:
        env.update(extra_env)

    cmd = [sys.executable, _script(script_name)] + (extra_args or [])
    label = _SCRIPT_LABELS.get(script_name, script_name)
    started_at = datetime.now(ET).isoformat()
    t0 = time.monotonic()

    logger.info(f"[job] START  {label}  args={extra_args or []}")

    # Write a "running" entry so the UI shows it immediately
    running_entry: Dict[str, Any] = {
        "id":          job_id or f"{script_name}-{int(t0 * 1000)}",
        "script":      script_name,
        "label":       label,
        "args":        extra_args or [],
        "started_at":  started_at,
        "ended_at":    None,
        "duration_s":  None,
        "ok":          None,
        "status":      "running",
        "stdout_tail": "",
        "stderr_tail": "",
    }
    _append_job_log(running_entry)

    try:
        proc = subprocess.Popen(
            cmd, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
        )
        with _PROCS_LOCK:
            _RUNNING_PROCS[script_name] = proc

        try:
            stdout_raw, stderr_raw = proc.communicate(timeout=timeout)
        except subprocess.TimeoutExpired:
            proc.kill()
            stdout_raw, stderr_raw = proc.communicate()
            raise subprocess.TimeoutExpired(cmd, timeout)
        finally:
            with _PROCS_LOCK:
                _RUNNING_PROCS.pop(script_name, None)

        duration_s = round(time.monotonic() - t0, 1)
        cancelled = proc.returncode == -9  # SIGKILL
        ok = proc.returncode == 0

        if cancelled:
            logger.info(f"[job] CANCEL {label}  ({duration_s}s)")
        elif ok:
            logger.info(f"[job] OK     {label}  ({duration_s}s)")
        else:
            logger.error(f"[job] FAIL   {label}  rc={proc.returncode}  ({duration_s}s)")

        stdout_tail = stdout_raw[-3000:] if stdout_raw else ""
        stderr_tail = stderr_raw[-1500:] if stderr_raw else ""

        summary = _parse_script_summary(script_name, stdout_tail, stderr_tail)
        if cancelled:
            summary = "Cancelled by user"

        status_str = "cancelled" if cancelled else ("ok" if ok else "error")

        running_entry.update({
            "ended_at":    datetime.now(ET).isoformat(),
            "duration_s":  duration_s,
            "ok":          ok and not cancelled,
            "status":      status_str,
            "stdout_tail": stdout_tail,
            "stderr_tail": stderr_tail,
            "summary":     summary,
        })
        _append_job_log(running_entry)  # re-append updated entry (remove duplicate below)
        # Remove the original "running" entry (now at index 1 since we just prepended)
        with _JOB_LOGS_LOCK:
            # Keep only the latest entry for this id
            _JOB_LOGS[:] = [
                e for e in _JOB_LOGS
                if not (e["id"] == running_entry["id"] and e["status"] == "running")
            ]
        _save_job_logs()

        return {
            "returncode": proc.returncode,
            "stdout": stdout_tail,
            "stderr": stderr_tail,
            "ok": ok and not cancelled,
            "cancelled": cancelled,
            "duration_s": duration_s,
            "summary": summary,
        }
    except subprocess.TimeoutExpired:
        duration_s = round(time.monotonic() - t0, 1)
        logger.error(f"[job] TIMEOUT {label} after {timeout}s")
        running_entry.update({
            "ended_at":   datetime.now(ET).isoformat(),
            "duration_s": duration_s,
            "ok":         False,
            "status":     "timeout",
            "stderr_tail": f"Timed out after {timeout}s",
            "summary":    f"Timed out after {timeout}s",
        })
        _save_job_logs()
        return {"returncode": -1, "stdout": "", "stderr": "Timeout", "ok": False, "duration_s": duration_s}
    except Exception as exc:
        duration_s = round(time.monotonic() - t0, 1)
        logger.error(f"[job] ERROR  {label}: {exc}")
        running_entry.update({
            "ended_at":   datetime.now(ET).isoformat(),
            "duration_s": duration_s,
            "ok":         False,
            "status":     "error",
            "stderr_tail": str(exc),
            "summary":    str(exc),
        })
        _save_job_logs()
        return {"returncode": -1, "stdout": "", "stderr": str(exc), "ok": False, "duration_s": duration_s}


def _parse_script_summary(script_name: str, stdout: str, stderr: str) -> str:
    """Extract a one-line human-readable summary from a script's output."""
    combined = stdout + "\n" + stderr
    lines = [l.strip() for l in combined.splitlines() if l.strip()]

    if script_name == "fetch_sp500_symbols.py":
        for line in reversed(lines):
            if "symbol" in line.lower() or "wrote" in line.lower() or "updated" in line.lower() or "saved" in line.lower():
                return line[:200]
        # Look for count
        for line in reversed(lines):
            if any(k in line.lower() for k in ["new", "removed", "unchanged", "total"]):
                return line[:200]

    elif script_name == "prefetch_options_datasp.py":
        for line in reversed(lines):
            if any(k in line.lower() for k in ["done", "complete", "skipped", "fetched", "saved", "error", "total"]):
                return line[:200]

    elif script_name == "optsp.py":
        for line in reversed(lines):
            if "saved" in line.lower() or "recommendation" in line.lower() or "returned" in line.lower():
                return line[:200]
        for line in reversed(lines):
            if "iron_condor" in line.lower():
                return line[:200]

    elif script_name == "exeopt.py":
        for line in reversed(lines):
            if any(k in line.lower() for k in ["order", "placed", "executed", "ibkr", "done"]):
                return line[:200]

    # Fallback: last non-empty line
    for line in reversed(lines):
        return line[:200]
    return "Completed"


# ---------------------------------------------------------------------------
# Script runners
# ---------------------------------------------------------------------------

def _read_symbols_set() -> set:
    """Read current S&P 500 tickers from CSV (returns empty set if missing)."""
    try:
        if not os.path.exists(SYMBOLS_CSV):
            return set()
        df = pd.read_csv(SYMBOLS_CSV)
        col = next(
            (c for c in df.columns if c.strip().lower() in ("symbol", "ticker")),
            df.columns[0],
        )
        return set(df[col].dropna().str.strip().str.upper().tolist())
    except Exception:
        return set()


def _save_sp500_diff(before: set, after: set) -> Dict[str, Any]:
    """Compute before→after diff and persist to sp500_diff.json."""
    added   = sorted(after - before)
    removed = sorted(before - after)
    diff = {
        "timestamp":       datetime.now(ET).isoformat(),
        "previous_count":  len(before),
        "current_count":   len(after),
        "added":           added,
        "removed":         removed,
        "unchanged_count": len(before & after),
    }
    try:
        with open(SP500_DIFF_FILE, "w", encoding="utf-8") as f:
            json.dump(diff, f, indent=2)
    except Exception as exc:
        logger.warning(f"Could not save sp500 diff: {exc}")
    return diff


def get_sp500_diff() -> Optional[Dict[str, Any]]:
    """Return the most recent S&P 500 symbol change diff (or None if not available)."""
    try:
        if not os.path.exists(SP500_DIFF_FILE):
            return None
        with open(SP500_DIFF_FILE, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def run_fetch_sp500() -> Dict[str, Any]:
    before = _read_symbols_set()
    result = _run_script("fetch_sp500_symbols.py", timeout=120)
    after  = _read_symbols_set()
    diff   = _save_sp500_diff(before, after)
    result["sp500_diff"] = diff
    # Enrich the log summary with the diff
    added_n   = len(diff["added"])
    removed_n = len(diff["removed"])
    if added_n or removed_n:
        diff_str = f"+{added_n} added, -{removed_n} removed"
    else:
        diff_str = f"no changes ({diff['current_count']} symbols)"
    result["summary"] = f"{result.get('summary', 'Updated')} | {diff_str}"
    # Patch the latest job log entry with the diff
    with _JOB_LOGS_LOCK:
        for entry in _JOB_LOGS:
            if entry.get("script") == "fetch_sp500_symbols.py" and entry.get("status") != "running":
                entry["sp500_diff"] = diff
                entry["summary"] = result["summary"]
                break
    _save_job_logs()
    return result


def run_prefetch(years: Optional[List[int]] = None, workers: int = 4) -> Dict[str, Any]:
    if not years:
        years = [date.today().year]
    args = ["--years"] + [str(y) for y in years] + ["--workers", str(workers)]
    return _run_script("prefetch_options_datasp.py", extra_args=args, timeout=7200)


def run_optsp(
    run_date: Optional[str] = None,
    cache_only: bool = True,
    *,
    job_id: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Run optsp.py to generate iron condor recommendations.

    cache_only=True (default): use only the locally prefetched data.
    optsp.py bails out at startup if EODHD_API_TOKEN is unset, even in
    cache mode.  We pass a placeholder so it proceeds past that check;
    the actual API is only called when a cache file is missing — which
    just causes that ticker to be skipped gracefully.
    """
    args: List[str] = ["--use-cache"]
    if run_date:
        args += ["--run-date", run_date]

    extra_env: Dict[str, str] = {}
    if cache_only:
        extra_env["OPTSP_WRITE_CACHE"] = "0"

    # If no real token is available, supply a placeholder so optsp passes its
    # startup token-presence check and proceeds to read from the local cache.
    # Tickers without a cache file will be skipped (no live API call succeeds).
    real_token = os.environ.get("EODHD_API_TOKEN") or os.environ.get("EODHD_API_KEY")
    if not real_token:
        extra_env.setdefault("EODHD_API_TOKEN", "cache-only-placeholder")

    return _run_script("optsp.py", extra_args=args, extra_env=extra_env, timeout=1800, job_id=job_id)


def run_exeopt(
    rec_date: Optional[str] = None,
    dry_run: bool = False,
    tickers: Optional[List[str]] = None,
) -> Dict[str, Any]:
    args: List[str] = []
    if rec_date:
        # exeopt.py uses --file, not --date
        csv_path = os.path.join(RECS_DIR, f"{OUTPUT_PREFIX}_{rec_date}.csv")
        xlsx_path = os.path.join(RECS_DIR, f"{OUTPUT_PREFIX}_{rec_date}.xlsx")
        if os.path.exists(csv_path):
            args += ["--file", csv_path]
        elif os.path.exists(xlsx_path):
            args += ["--file", xlsx_path]
        else:
            # No specific file — let exeopt auto-find the most recent
            args += ["--recommendations-dir", RECS_DIR]
    else:
        args += ["--recommendations-dir", RECS_DIR]
    if dry_run:
        args += ["--dry-run"]
    if tickers:
        args += ["--tickers"] + tickers
    return _run_script("exeopt.py", extra_args=args, timeout=300)


# ---------------------------------------------------------------------------
# Historical price data
# ---------------------------------------------------------------------------

@lru_cache(maxsize=128)
def _load_price_df(ticker: str) -> Optional[pd.DataFrame]:
    """Load the price Parquet for a ticker (cached in memory)."""
    path = os.path.join(HIST_CACHE_DIR, f"{ticker}.parquet")
    if not os.path.exists(path):
        return None
    try:
        df = pd.read_parquet(path)
        df.index = pd.to_datetime(df.index)
        return df
    except Exception as exc:
        logger.warning(f"Could not load price data for {ticker}: {exc}")
        return None


def get_close_on_date(ticker: str, target_date: str) -> Optional[float]:
    """
    Return the closing price of `ticker` on or just before `target_date`.
    Reads from historical_data_cache/{ticker}.parquet.
    """
    df = _load_price_df(ticker)
    if df is None or df.empty:
        return None
    try:
        ts = pd.Timestamp(target_date)
        # Use the last available price on or before the target date
        mask = df.index <= ts
        if not mask.any():
            return None
        row = df[mask].iloc[-1]
        return float(row["adjusted_close"] if "adjusted_close" in row else row["close"])
    except Exception as exc:
        logger.warning(f"get_close_on_date({ticker}, {target_date}): {exc}")
        return None


def get_price_history(ticker: str, start_date: str, end_date: str) -> List[Dict[str, Any]]:
    """Return daily close prices for a ticker between start_date and end_date."""
    df = _load_price_df(ticker)
    if df is None or df.empty:
        return []
    try:
        mask = (df.index >= pd.Timestamp(start_date)) & (df.index <= pd.Timestamp(end_date))
        sub = df[mask][["close", "adjusted_close"]].copy() if "adjusted_close" in df.columns else df[mask][["close"]].copy()
        sub.index = sub.index.strftime("%Y-%m-%d")
        return sub.reset_index().rename(columns={"index": "date"}).to_dict(orient="records")
    except Exception as exc:
        logger.warning(f"get_price_history({ticker}): {exc}")
        return []


# ---------------------------------------------------------------------------
# Options chain data
# ---------------------------------------------------------------------------

def _options_cache_path(ticker: str, trade_date: str) -> str:
    return os.path.join(OPTIONS_CACHE_DIR, ticker, f"{trade_date}.json.gz")


def get_options_chain(ticker: str, trade_date: str) -> Optional[Dict[str, Any]]:
    """
    Load the full options chain for a ticker on a given date from the cache.
    Returns the raw JSON dict with keys: underlying, trade_date, exp_from, exp_to, rows.
    """
    path = _options_cache_path(ticker, trade_date)
    if not os.path.exists(path):
        return None
    try:
        with gzip.open(path, "rt", encoding="utf-8") as f:
            return json.load(f)
    except Exception as exc:
        logger.warning(f"get_options_chain({ticker}, {trade_date}): {exc}")
        return None


def get_options_chain_summary(ticker: str, trade_date: str) -> Dict[str, Any]:
    """
    Return a concise summary of the options chain for a given ticker/date.
    Suitable for including in the AI context.
    """
    chain = get_options_chain(ticker, trade_date)
    if chain is None:
        return {"ticker": ticker, "trade_date": trade_date, "available": False}

    rows = chain.get("rows", [])
    if not rows:
        return {"ticker": ticker, "trade_date": trade_date, "available": True, "row_count": 0}

    # Build a quick DataFrame for analytics
    df = pd.DataFrame(rows)

    # ATM IV — average IV of calls near the money
    spot = get_close_on_date(ticker, trade_date)
    atm_iv = None
    if spot and "volatility" in df.columns and "strike" in df.columns:
        atm_calls = df[
            (df["type"] == "call") &
            (df["strike"].between(spot * 0.97, spot * 1.03)) &
            (df["volatility"].notna())
        ]
        if not atm_calls.empty:
            atm_iv = round(float(atm_calls["volatility"].mean()), 4)

    expirations = sorted(df["exp_date"].dropna().unique().tolist()) if "exp_date" in df.columns else []
    strikes = sorted(df["strike"].dropna().unique().tolist()) if "strike" in df.columns else []
    call_count = int((df["type"] == "call").sum()) if "type" in df.columns else 0
    put_count  = int((df["type"] == "put").sum())  if "type" in df.columns else 0

    return {
        "ticker": ticker,
        "trade_date": trade_date,
        "available": True,
        "spot": spot,
        "atm_iv": atm_iv,
        "row_count": len(rows),
        "expirations": expirations,
        "strike_range": [min(strikes), max(strikes)] if strikes else None,
        "call_count": call_count,
        "put_count": put_count,
    }


def get_options_cache_dates(ticker: str) -> List[str]:
    """Return all cached dates for a ticker (sorted ascending)."""
    ticker_dir = os.path.join(OPTIONS_CACHE_DIR, ticker)
    if not os.path.isdir(ticker_dir):
        return []
    dates = sorted(
        f.replace(".json.gz", "")
        for f in os.listdir(ticker_dir)
        if f.endswith(".json.gz")
    )
    return dates


# ---------------------------------------------------------------------------
# P&L simulation — actual iron condor outcomes at expiry
# ---------------------------------------------------------------------------

def _ic_pnl_at_expiry(
    net_credit: float,
    short_put: float,
    long_put: float,
    short_call: float,
    long_call: float,
    price_at_expiry: float,
) -> float:
    """
    Compute the P&L per share for an iron condor at expiry.

    Sold: short_put + short_call (receive credit)
    Bought: long_put + long_call (pay debit, already netted into net_credit)

    P&L = net_credit
          - max(0, short_put - price)   [put spread loss if price < short_put]
          + max(0, long_put - price)    [long put gains back if price < long_put]
          - max(0, price - short_call)  [call spread loss if price > short_call]
          + max(0, price - long_call)   [long call gains back if price > long_call]
    """
    put_loss  = max(0.0, short_put - price_at_expiry) - max(0.0, long_put - price_at_expiry)
    call_loss = max(0.0, price_at_expiry - short_call) - max(0.0, price_at_expiry - long_call)
    return round(net_credit - put_loss - call_loss, 6)


def simulate_ic_pnl_for_ticker(
    ticker: str,
    start_year: int = 2023,
    end_year: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """
    For every backtest recommendation for `ticker`, compute the actual P&L
    at expiry using the real close price from historical_data_cache.

    Returns a list of trade dicts with added fields:
      price_at_expiry, pnl_per_share, pnl_usd, outcome
    """
    end_year = end_year or date.today().year
    results: List[Dict[str, Any]] = []

    for year in range(start_year, end_year + 1):
        year_dir = os.path.join(BACKTEST_DIR, str(year))
        if not os.path.isdir(year_dir):
            continue
        for fname in sorted(os.listdir(year_dir)):
            if not (fname.endswith(".csv") and fname.startswith(OUTPUT_PREFIX + "_")):
                continue
            try:
                df = pd.read_csv(os.path.join(year_dir, fname))
                df = df[df["ticker"].str.upper() == ticker.upper()]
                if df.empty:
                    continue
                for _, row in df.iterrows():
                    exp = str(row.get("exp", ""))
                    if not exp:
                        continue
                    price_at_exp = get_close_on_date(ticker, exp)
                    if price_at_exp is None:
                        continue

                    sp = float(row["short_put"])
                    lp = float(row["long_put"])
                    sc = float(row["short_call"])
                    lc = float(row["long_call"])
                    credit = float(row["net_credit"])
                    max_loss = float(row["max_loss_per_share"])
                    contracts = int(row.get("contracts", 1) or 1)
                    pop = float(row.get("pop_est", 0) or 0)
                    score = float(row.get("score", 0) or 0)

                    pnl_ps = _ic_pnl_at_expiry(credit, sp, lp, sc, lc, price_at_exp)
                    pnl_usd = round(pnl_ps * 100 * contracts, 2)

                    if pnl_ps >= credit * 0.9:
                        outcome = "max_profit"
                    elif pnl_ps > 0:
                        outcome = "partial_profit"
                    elif pnl_ps >= -max_loss * 0.5:
                        outcome = "small_loss"
                    else:
                        outcome = "max_loss"

                    results.append({
                        "run_date":       str(row["run_date"]),
                        "exp":            exp,
                        "dte":            int(row.get("dte", 0) or 0),
                        "short_put":      sp,
                        "long_put":       lp,
                        "short_call":     sc,
                        "long_call":      lc,
                        "spot_at_entry":  float(row.get("spot", 0) or 0),
                        "price_at_expiry": price_at_exp,
                        "net_credit":     credit,
                        "max_loss_per_share": max_loss,
                        "contracts":      contracts,
                        "pnl_per_share":  pnl_ps,
                        "pnl_usd":        pnl_usd,
                        "outcome":        outcome,
                        "pop_est":        pop,
                        "score":          score,
                    })
            except Exception as exc:
                logger.warning(f"simulate_ic_pnl_for_ticker({ticker}, {fname}): {exc}")

    return results


def aggregate_pnl_stats(trades: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Aggregate P&L simulation results into summary statistics."""
    if not trades:
        return {"total_trades": 0}

    pnl_list   = [t["pnl_per_share"] for t in trades]
    pnl_usd_list = [t["pnl_usd"] for t in trades]
    outcomes   = [t["outcome"] for t in trades]
    credits    = [t["net_credit"] for t in trades]

    wins  = sum(1 for o in outcomes if o in ("max_profit", "partial_profit"))
    total = len(trades)

    return {
        "total_trades":         total,
        "wins":                 wins,
        "win_rate_pct":         round(wins / total * 100, 1),
        "outcome_breakdown": {
            "max_profit":    outcomes.count("max_profit"),
            "partial_profit": outcomes.count("partial_profit"),
            "small_loss":    outcomes.count("small_loss"),
            "max_loss":      outcomes.count("max_loss"),
        },
        "total_pnl_usd":        round(sum(pnl_usd_list), 2),
        "avg_pnl_per_trade_usd": round(np.mean(pnl_usd_list), 2),
        "avg_pnl_per_share":    round(np.mean(pnl_list), 4),
        "avg_credit":           round(np.mean(credits), 4),
        "best_trade_usd":       round(max(pnl_usd_list), 2),
        "worst_trade_usd":      round(min(pnl_usd_list), 2),
        "date_range": {
            "first": trades[0]["run_date"],
            "last":  trades[-1]["run_date"],
        },
    }


# ---------------------------------------------------------------------------
# Backtest summaries (heuristic + real P&L)
# ---------------------------------------------------------------------------

def get_backtest_summary_for_ticker(
    ticker: str,
    start_year: int = 2023,
    end_year: Optional[int] = None,
) -> Dict[str, Any]:
    """Full simulation-based P&L summary for a single ticker."""
    trades = simulate_ic_pnl_for_ticker(ticker, start_year, end_year)
    stats  = aggregate_pnl_stats(trades)
    stats["ticker"] = ticker
    stats["years"]  = f"{start_year}-{end_year or date.today().year}"
    return stats


# ---------------------------------------------------------------------------
# Ticker summary cache — computed once, refreshed every 4 hours
# ---------------------------------------------------------------------------

_SUMMARIES_CACHE: Dict[str, Any] = {"data": None, "ts": 0.0, "lock": None}
_SUMMARIES_TTL = 4 * 3600  # seconds


def _summaries_lock() -> threading.Lock:
    if _SUMMARIES_CACHE["lock"] is None:
        _SUMMARIES_CACHE["lock"] = threading.Lock()
    return _SUMMARIES_CACHE["lock"]  # type: ignore[return-value]


def warm_ticker_summaries_cache(start_year: int = 2023) -> None:
    """Background-safe cache warm-up. Call once at startup."""
    def _run() -> None:
        try:
            logger.info("[options] Pre-computing ticker P&L summaries in background…")
            data = _compute_ticker_summaries(start_year)
            with _summaries_lock():
                _SUMMARIES_CACHE["data"] = data
                _SUMMARIES_CACHE["ts"]   = time.monotonic()
            logger.info(f"[options] Ticker summaries ready ({len(data)} tickers).")
        except Exception as exc:
            logger.error(f"[options] warm_ticker_summaries_cache failed: {exc}")

    t = threading.Thread(target=_run, daemon=True)
    t.start()


def get_all_ticker_summaries(start_year: int = 2023) -> List[Dict[str, Any]]:
    """
    Return per-ticker P&L summaries.  Uses an in-memory cache (TTL 4 h)
    so AI chat requests don't recompute from scratch every time.
    """
    with _summaries_lock():
        cached   = _SUMMARIES_CACHE["data"]
        age      = time.monotonic() - _SUMMARIES_CACHE["ts"]
        is_fresh = cached is not None and age < _SUMMARIES_TTL

    if is_fresh:
        return cached  # type: ignore[return-value]

    logger.info("[options] Computing ticker P&L summaries (cache miss or expired)…")
    data = _compute_ticker_summaries(start_year)
    with _summaries_lock():
        _SUMMARIES_CACHE["data"] = data
        _SUMMARIES_CACHE["ts"]   = time.monotonic()
    return data


def _compute_ticker_summaries(start_year: int = 2023) -> List[Dict[str, Any]]:
    """
    Per-ticker aggregated P&L stats using ACTUAL expiry prices.
    Reads all backtest CSVs and matches each trade's expiry against
    the historical price cache.
    """
    end_year = date.today().year
    all_rows: List[Dict] = []

    for year in range(start_year, end_year + 1):
        year_dir = os.path.join(BACKTEST_DIR, str(year))
        if not os.path.isdir(year_dir):
            continue
        for fname in sorted(os.listdir(year_dir)):
            if not (fname.endswith(".csv") and fname.startswith(OUTPUT_PREFIX + "_")):
                continue
            try:
                df = pd.read_csv(os.path.join(year_dir, fname))
                all_rows.append(df)
            except Exception:
                continue

    if not all_rows:
        return []

    all_df = pd.concat(all_rows, ignore_index=True)
    results: List[Dict[str, Any]] = []

    for ticker, grp in all_df.groupby("ticker"):
        pnl_list: List[float] = []
        outcome_counts: Dict[str, int] = {"max_profit": 0, "partial_profit": 0, "small_loss": 0, "max_loss": 0}

        for _, row in grp.iterrows():
            exp = str(row.get("exp", ""))
            if not exp:
                continue
            price = get_close_on_date(str(ticker), exp)
            if price is None:
                continue
            try:
                pnl = _ic_pnl_at_expiry(
                    float(row["net_credit"]),
                    float(row["short_put"]),
                    float(row["long_put"]),
                    float(row["short_call"]),
                    float(row["long_call"]),
                    price,
                )
                pnl_list.append(pnl)
                max_loss = float(row["max_loss_per_share"])
                credit   = float(row["net_credit"])
                if pnl >= credit * 0.9:
                    outcome_counts["max_profit"] += 1
                elif pnl > 0:
                    outcome_counts["partial_profit"] += 1
                elif pnl >= -max_loss * 0.5:
                    outcome_counts["small_loss"] += 1
                else:
                    outcome_counts["max_loss"] += 1
            except Exception:
                continue

        total_sim = len(pnl_list)
        total_recs = len(grp)
        wins = outcome_counts["max_profit"] + outcome_counts["partial_profit"]

        results.append({
            "ticker":           ticker,
            "total_recs":       total_recs,
            "simulated_trades": total_sim,
            "win_rate_pct":     round(wins / total_sim * 100, 1) if total_sim else 0,
            "outcome_breakdown": outcome_counts,
            "avg_pnl_per_share": round(float(np.mean(pnl_list)), 4) if pnl_list else 0,
            "total_pnl_per_share": round(float(sum(pnl_list)), 4) if pnl_list else 0,
            "avg_credit":       round(float(grp["net_credit"].mean()), 4),
            "avg_pop_est":      round(float(grp["pop_est"].mean()), 3),
            "avg_score":        round(float(grp["score"].mean()), 4),
        })

    return sorted(results, key=lambda x: x["simulated_trades"], reverse=True)


# ---------------------------------------------------------------------------
# Data coverage summary
# ---------------------------------------------------------------------------

def get_data_coverage() -> Dict[str, Any]:
    """
    Return a summary of all prefetched data in the OptionSys database.
    Used by the AI to understand what historical data is available.
    """
    # Options cache coverage
    options_tickers: List[str] = []
    options_date_range: Dict[str, Any] = {}

    if os.path.isdir(OPTIONS_CACHE_DIR):
        options_tickers = sorted(
            t for t in os.listdir(OPTIONS_CACHE_DIR)
            if os.path.isdir(os.path.join(OPTIONS_CACHE_DIR, t))
        )
        all_dates: List[str] = []
        for t in options_tickers[:5]:  # sample a few tickers for date range
            dates = get_options_cache_dates(t)
            all_dates.extend(dates)
        if all_dates:
            all_dates_sorted = sorted(set(all_dates))
            options_date_range = {
                "earliest": all_dates_sorted[0],
                "latest":   all_dates_sorted[-1],
            }

    # Price history coverage
    price_tickers: List[str] = []
    price_date_range: Dict[str, Any] = {}
    if os.path.isdir(HIST_CACHE_DIR):
        price_tickers = [
            f.replace(".parquet", "")
            for f in os.listdir(HIST_CACHE_DIR)
            if f.endswith(".parquet") and "_" not in f
        ]
        if price_tickers:
            # Sample one parquet for date range
            sample_df = _load_price_df(price_tickers[0])
            if sample_df is not None and not sample_df.empty:
                price_date_range = {
                    "earliest": str(sample_df.index.min().date()),
                    "latest":   str(sample_df.index.max().date()),
                }

    # Backtest years
    backtest_years: List[int] = []
    if os.path.isdir(BACKTEST_DIR):
        backtest_years = sorted(
            int(d) for d in os.listdir(BACKTEST_DIR)
            if d.isdigit() and os.path.isdir(os.path.join(BACKTEST_DIR, d))
        )

    return {
        "options_cache": {
            "ticker_count":   len(options_tickers),
            "date_range":     options_date_range,
            "description":    "Full options chain data (strikes, bid/ask, delta, gamma, theta, vega, IV) per ticker per trading day",
        },
        "price_history": {
            "ticker_count":   len(price_tickers),
            "date_range":     price_date_range,
            "description":    "Daily OHLCV price data used to compute actual P&L at expiry",
        },
        "backtest_results": {
            "years":          backtest_years,
            "description":    "Iron condor recommendations generated by optsp for every trading day",
        },
    }


# ---------------------------------------------------------------------------
# Recommendations
# ---------------------------------------------------------------------------

def _rec_csv_path(rec_date: str) -> Optional[str]:
    path = os.path.join(RECS_DIR, f"{OUTPUT_PREFIX}_{rec_date}.csv")
    return path if os.path.exists(path) else None


def get_latest_rec_date() -> Optional[str]:
    if not os.path.isdir(RECS_DIR):
        return None
    valid_dates = []
    for f in os.listdir(RECS_DIR):
        if not (f.startswith(OUTPUT_PREFIX + "_") and f.endswith(".csv")):
            continue
        date_part = f[len(OUTPUT_PREFIX) + 1 : -4]  # strip "iron_condor_" prefix and ".csv"
        try:
            datetime.strptime(date_part, "%Y-%m-%d")
            valid_dates.append(date_part)
        except ValueError:
            continue  # skip files with non-date suffixes (e.g. iron_condor_recs_...)
    if not valid_dates:
        return None
    return sorted(valid_dates)[-1]




def get_available_rec_dates(limit: int = 90) -> List[str]:
    if not os.path.isdir(RECS_DIR):
        return []
    dates: List[str] = []
    for f in sorted(os.listdir(RECS_DIR), reverse=True):
        if f.startswith(OUTPUT_PREFIX + "_") and f.endswith(".csv"):
            date_part = f.replace(OUTPUT_PREFIX + "_", "").replace(".csv", "")
            try:
                datetime.strptime(date_part, "%Y-%m-%d")
                dates.append(date_part)
            except ValueError:
                continue
        if len(dates) >= limit:
            break
    return dates


def delete_rec_date(rec_date: str) -> bool:
    """Delete the entire recommendation file for a given date."""
    path = os.path.join(RECS_DIR, f"{OUTPUT_PREFIX}_{rec_date}.csv")
    if os.path.exists(path):
        os.remove(path)
        return True
    return False


def delete_rec_ticker(rec_date: str, ticker: str) -> bool:
    """Remove a single ticker row from a date's recommendation file.
    Deletes the file entirely if no rows remain.
    """
    path = _rec_csv_path(rec_date)
    if not path:
        return False
    try:
        df = pd.read_csv(path)
        original_len = len(df)
        ticker_col = next(
            (c for c in df.columns if c.strip().lower() == "ticker"), None
        )
        if ticker_col is None:
            return False
        df = df[df[ticker_col].str.upper() != ticker.upper()]
        if len(df) == original_len:
            return False  # ticker not found
        if df.empty:
            os.remove(path)
        else:
            df.to_csv(path, index=False)
        return True
    except Exception as exc:
        logger.error(f"delete_rec_ticker error: {exc}")
        return False


def get_recommendations(rec_date: Optional[str] = None) -> List[Dict[str, Any]]:
    target_date = rec_date or get_latest_rec_date()
    if not target_date:
        return []
    path = _rec_csv_path(target_date)
    if not path:
        return []
    try:
        df = pd.read_csv(path)
        df = df.where(pd.notna(df), None)
        return df.to_dict(orient="records")
    except Exception as exc:
        logger.error(f"Failed to read recs for {target_date}: {exc}")
        return []


def get_symbols() -> List[str]:
    try:
        df = pd.read_csv(SYMBOLS_CSV)
        col = "symbol" if "symbol" in df.columns else df.columns[0]
        return df[col].dropna().tolist()
    except Exception as exc:
        logger.error(f"Failed to read symbols: {exc}")
        return []


# ---------------------------------------------------------------------------
# System status
# ---------------------------------------------------------------------------

def get_status() -> Dict[str, Any]:
    symbols = get_symbols()
    latest_rec = get_latest_rec_date()
    available_dates = get_available_rec_dates(limit=10)

    backtest_state: Dict[str, Any] = {}
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE) as f:
                backtest_state = json.load(f)
        except Exception:
            pass

    now_et = datetime.now(ET)
    days_to_monday = (7 - now_et.weekday()) % 7 or 7
    next_monday = (now_et + timedelta(days=days_to_monday)).replace(
        hour=9, minute=30, second=0, microsecond=0
    )

    next_prefetch = now_et.replace(hour=17, minute=0, second=0, microsecond=0)
    if now_et >= next_prefetch or now_et.weekday() >= 5:
        delta = 1
        while True:
            candidate = (now_et + timedelta(days=delta)).replace(
                hour=16, minute=30, second=0, microsecond=0
            )
            if candidate.weekday() < 5:
                next_prefetch = candidate
                break
            delta += 1

    return {
        "symbol_count":                   len(symbols),
        "latest_recommendation_date":     latest_rec,
        "available_recommendation_dates": available_dates,
        "backtest_state":                 backtest_state,
        "next_fetch_symbols":             next_monday.isoformat(),
        "next_prefetch":                  next_prefetch.isoformat(),
        "scripts_dir":                    SCRIPTS_DIR,
        "data_dir":                       OPTIONSYS_DATA_DIR,
    }


# ---------------------------------------------------------------------------
# Morning status — one-stop summary for "what happened overnight"
# ---------------------------------------------------------------------------

def get_morning_status() -> Dict[str, Any]:
    """
    Return a concise morning briefing:
      - last_prefetch: when it ran, how long, ok/error, ticker count
      - last_sp500_update: when it ran, diff (added/removed tickers)
      - latest_recommendations: date and count
    """
    # Reconcile phantom running entries before building the briefing.
    logs = get_job_logs(limit=200)

    # ── Last prefetch ────────────────────────────────────────────────────────
    last_prefetch: Dict[str, Any] = {}
    for entry in logs:
        if entry.get("script") == "prefetch_options_datasp.py" and entry.get("status") != "running":
            last_prefetch = {
                "ran_at":     entry.get("started_at"),
                "ended_at":   entry.get("ended_at"),
                "duration_s": entry.get("duration_s"),
                "ok":         entry.get("ok"),
                "status":     entry.get("status"),
                "summary":    entry.get("summary", ""),
                "args":       entry.get("args", []),
            }
            break

    # ── Last SP500 update ────────────────────────────────────────────────────
    last_sp500: Dict[str, Any] = {}
    for entry in logs:
        if entry.get("script") == "fetch_sp500_symbols.py" and entry.get("status") != "running":
            diff = entry.get("sp500_diff") or get_sp500_diff() or {}
            last_sp500 = {
                "ran_at":          entry.get("started_at"),
                "ok":              entry.get("ok"),
                "status":          entry.get("status"),
                "summary":         entry.get("summary", ""),
                "added":           diff.get("added", []),
                "removed":         diff.get("removed", []),
                "previous_count":  diff.get("previous_count"),
                "current_count":   diff.get("current_count"),
            }
            break
    else:
        # Fall back to the persisted diff file (survives server restarts)
        diff = get_sp500_diff()
        if diff:
            last_sp500 = {
                "ran_at":         diff.get("timestamp"),
                "ok":             True,
                "status":         "ok",
                "summary":        f"{diff.get('current_count')} symbols",
                "added":          diff.get("added", []),
                "removed":        diff.get("removed", []),
                "previous_count": diff.get("previous_count"),
                "current_count":  diff.get("current_count"),
            }

    # ── Last optsp run ───────────────────────────────────────────────────────
    last_optsp: Dict[str, Any] = {}
    for entry in logs:
        if entry.get("script") == "optsp.py" and entry.get("status") != "running":
            last_optsp = {
                "ran_at":     entry.get("started_at"),
                "duration_s": entry.get("duration_s"),
                "ok":         entry.get("ok"),
                "status":     entry.get("status"),
                "summary":    entry.get("summary", ""),
            }
            break

    # ── Currently running jobs ───────────────────────────────────────────────
    # Only show jobs that are *actually* running according to the process registry.
    running = []
    for e in logs:
        if e.get("status") != "running":
            continue
        script = e.get("script")
        if script and is_script_running(str(script)):
            running.append(e)
        else:
            # If logs contain a running entry but no live process, it will be
            # reconciled on the next call; don't surface it as "running".
            continue

    return {
        "last_prefetch":     last_prefetch,
        "last_sp500_update": last_sp500,
        "last_optsp":        last_optsp,
        "running_jobs":      running,
        "latest_rec_date":   get_latest_rec_date(),
        "latest_rec_count":  len(get_recommendations()),
    }
