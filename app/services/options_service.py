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
from datetime import date, datetime, timedelta, timezone
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
# Live-price in-memory cache (per ticker, 60 s TTL)
# Prevents EODHD rate-limiting when many cards poll /reprice simultaneously.
# ---------------------------------------------------------------------------
_spot_cache: Dict[str, Tuple[float, float]] = {}  # {TICKER: (price, monotonic_ts)}
_spot_cache_lock = threading.Lock()
_SPOT_CACHE_TTL = 60.0  # seconds


def _get_live_spot_cached(ticker: str) -> Optional[float]:
    """Return the cached live price for *ticker* if it is still fresh."""
    key = ticker.upper()
    with _spot_cache_lock:
        entry = _spot_cache.get(key)
        if entry and (time.monotonic() - entry[1]) < _SPOT_CACHE_TTL:
            return entry[0]
    return None


def _set_live_spot_cached(ticker: str, price: float) -> None:
    key = ticker.upper()
    with _spot_cache_lock:
        _spot_cache[key] = (price, time.monotonic())


# ---------------------------------------------------------------------------
# Option-chain in-memory cache (per ticker+date, 45 s TTL)
# Each reprice used to fetch the full chain from ThetaTerminal on every poll
# (use_cache=False).  With N cards polling every 4 s that is N×2-3 HTTP calls
# per poll cycle.  Caching the chain here cuts that down to ~1 call per ticker
# per 45 s while still delivering fresh bid/ask data during live trading hours.
# ---------------------------------------------------------------------------
_chain_cache: Dict[str, Tuple[Any, float]] = {}   # {"TICKER:YYYY-MM-DD": (DataFrame, ts)}
_chain_cache_lock = threading.Lock()
_CHAIN_CACHE_TTL = 45.0  # seconds


def _get_chain_cached(ticker: str, run_date: str):
    key = f"{ticker.upper()}:{run_date}"
    with _chain_cache_lock:
        entry = _chain_cache.get(key)
        if entry and (time.monotonic() - entry[1]) < _CHAIN_CACHE_TTL:
            return entry[0]
    return None


def _set_chain_cached(ticker: str, run_date: str, chain) -> None:
    key = f"{ticker.upper()}:{run_date}"
    with _chain_cache_lock:
        _chain_cache[key] = (chain, time.monotonic())


# ---------------------------------------------------------------------------
# opsp module singleton — avoid re-importing on every reprice call
# ---------------------------------------------------------------------------
_opsp_module = None
_opsp_lock = threading.Lock()


def _get_opsp():
    """Return the cached opsp module, importing it exactly once per process."""
    global _opsp_module
    if _opsp_module is not None:
        return _opsp_module
    with _opsp_lock:
        if _opsp_module is None:
            import importlib.util as _ilu
            _path = os.path.join(_SCRIPTS_DIR, "opsp.py")
            if not os.path.exists(_path):
                raise FileNotFoundError(f"opsp.py not found at {_path}")
            _spec = _ilu.spec_from_file_location("opsp", _path)
            _mod = _ilu.module_from_spec(_spec)  # type: ignore[arg-type]
            _spec.loader.exec_module(_mod)  # type: ignore[union-attr]
            _opsp_module = _mod
    return _opsp_module


def prewarm_spot_cache(tickers: list) -> None:
    """
    Pre-fetch EODHD live prices for *tickers* in parallel so the first
    browser reprice poll gets instant spot values from cache.
    Called once after recommendations are loaded on startup.
    """
    import concurrent.futures

    def _fetch_one(t: str):
        try:
            opsp = _get_opsp()
            if not hasattr(opsp, "fetch_eodhd_live_price"):
                return
            price = opsp.fetch_eodhd_live_price(t)
            if price and price > 0:
                _set_live_spot_cached(t, price)
        except Exception:
            pass

    unique = list(dict.fromkeys(t.upper() for t in tickers if t))
    if not unique:
        return
    logger.info("[spot-prewarm] Pre-warming EODHD spot cache for %d tickers…", len(unique))
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as ex:
        ex.map(_fetch_one, unique)
    cached = sum(1 for t in unique if _get_live_spot_cached(t) is not None)
    logger.info("[spot-prewarm] Done. %d/%d tickers cached.", cached, len(unique))


# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

SCRIPTS_DIR = str(Path(__file__).resolve().parents[2] / "options")

OPTIONSYS_DATA_DIR = os.environ.get(
    "STOCK_BASE_DIR",
    "/Volumes/Extreme Pro/OptionSys",  # default data directory on this machine
)

RECS_DIR            = os.path.join(OPTIONSYS_DATA_DIR, "options_recommendations")
BACKTEST_DIR        = os.path.join(OPTIONSYS_DATA_DIR, "backtest_optsp_recommendations")
HIST_CACHE_DIR      = os.path.join(OPTIONSYS_DATA_DIR, "historical_data_cache")
OPTIONS_CACHE_DIR   = os.path.join(OPTIONSYS_DATA_DIR, "options_data_cache", "unicornbay", "options_eod")
# ThetaData prefetch writes here (prefetch_options_datasp.py v3)
THETADATA_SNAPSHOTS_DIR = os.path.join(OPTIONSYS_DATA_DIR, "options_data_cache", "thetadata", "snapshots")
SYMBOLS_CSV         = os.path.join(OPTIONSYS_DATA_DIR, "sp500_symbols.csv")
STATE_FILE          = os.path.join(OPTIONSYS_DATA_DIR, "backtest_optsp_state.json")
SP500_DIFF_FILE     = os.path.join(OPTIONSYS_DATA_DIR, "sp500_diff.json")

OUTPUT_PREFIX = "iron_condor"
ET = pytz.timezone("America/New_York")

JOB_LOGS_FILE = os.path.join(OPTIONSYS_DATA_DIR, "options_job_logs.json")

ACTIVE_TRADES_DATA_DIR = os.path.join(OPTIONSYS_DATA_DIR, "data")
ACTIVE_TRADES_JSON = os.path.join(ACTIVE_TRADES_DATA_DIR, "active_trades.json")

# Friendly display names for each script
_SCRIPT_LABELS: Dict[str, str] = {
    "fetch_sp500_symbols.py":   "Fetch S&P 500 Symbols",
    "prefetch_options_datasp.py": "Prefetch Options Data",
    "optsp.py":                 "Generate Recommendations (optsp)",
    "opsp.py":                  "Generate Recommendations (ThetaData)",
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
    timeout: Optional[int] = 3600,
    job_id: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Run a Python script as a subprocess, record the execution in the job log,
    and return stdout/stderr/returncode.
    """
    env = os.environ.copy()
    env["STOCK_BASE_DIR"] = OPTIONSYS_DATA_DIR
    env["PYTHONUNBUFFERED"] = "1"  # force unbuffered output so captured even on crash/kill
    if extra_env:
        env.update(extra_env)

    # -u: unbuffered stdout/stderr so every log line is visible even if the process is killed
    cmd = [sys.executable, "-u", _script(script_name)] + (extra_args or [])
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
            cmd,
            cwd=SCRIPTS_DIR,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,  # merge stderr into stdout for live display
            text=True,
        )
        with _PROCS_LOCK:
            _RUNNING_PROCS[script_name] = proc

        # Stream output in a background thread and update the job log live.
        output_lines: list = []
        _LIVE_OUTPUT_INTERVAL = 10  # seconds between live log snapshots

        def _stream_output() -> None:
            assert proc.stdout is not None
            for line in proc.stdout:
                output_lines.append(line.rstrip())

        reader_thread = threading.Thread(target=_stream_output, daemon=True)
        reader_thread.start()

        deadline = (t0 + timeout) if timeout is not None else None
        try:
            while True:
                elapsed = time.monotonic() - t0
                if timeout is not None and elapsed >= timeout:
                    proc.kill()
                    reader_thread.join(timeout=5)
                    raise subprocess.TimeoutExpired(cmd, timeout)
                # Check every second whether the process has finished.
                try:
                    proc.wait(timeout=1)
                    break  # process exited
                except subprocess.TimeoutExpired:
                    pass
                # Periodically flush live output to the job log entry.
                if int(elapsed) % _LIVE_OUTPUT_INTERVAL == 0 and output_lines:
                    live_tail = "\n".join(output_lines[-100:])
                    running_entry["stdout_tail"] = live_tail[-3000:]
                    _save_job_logs()
        finally:
            reader_thread.join(timeout=10)
            with _PROCS_LOCK:
                _RUNNING_PROCS.pop(script_name, None)

        stdout_raw = "\n".join(output_lines)
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
        stderr_tail = ""  # merged into stdout

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
        # Include any output captured so far so user can see where the script got stuck
        try:
            stdout_tail = "\n".join(output_lines[-200:])[-3000:] if output_lines else ""
        except NameError:
            stdout_tail = ""
        running_entry.update({
            "ended_at":    datetime.now(ET).isoformat(),
            "duration_s":  duration_s,
            "ok":          False,
            "status":      "timeout",
            "stdout_tail": stdout_tail,
            "stderr_tail": f"Timed out after {timeout}s",
            "summary":     f"Timed out after {timeout}s",
        })
        _save_job_logs()
        return {
            "returncode": -1,
            "stdout": stdout_tail,
            "stderr": f"Timed out after {timeout}s",
            "ok": False,
            "duration_s": duration_s,
        }
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

    elif script_name in ("optsp.py", "opsp.py"):
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


def get_symbols_missing_today_cache() -> List[str]:
    """
    Return S&P 500 symbols that are missing today's ThetaData options cache file.
    Used by Sync Data to prefetch only what's missing (v3 endpoints).
    """
    today_str = date.today().isoformat()
    symbols = sorted(_read_symbols_set())
    missing: List[str] = []
    for ticker in symbols:
        path = os.path.join(THETADATA_SNAPSHOTS_DIR, ticker, f"{today_str}.json.gz")
        if not os.path.isfile(path):
            missing.append(ticker)
    return missing


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


def run_prefetch(
    years: Optional[List[int]] = None,
    today_only: bool = False,
    tickers: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """
    Run prefetch_options_datasp.py (ThetaData v3). Either today_only + optional tickers,
    or years for full range. EODHD is used by opsp when generating recommendations after sync.
    """
    if today_only:
        args = ["--today"]
        if tickers:
            args += ["--tickers"] + [str(t).upper() for t in tickers]
        return _run_script("prefetch_options_datasp.py", extra_args=args, timeout=None)
    if not years:
        years = [date.today().year]
    args = ["--years"] + [str(y) for y in years]
    return _run_script("prefetch_options_datasp.py", extra_args=args, timeout=None)


def run_optsp(
    run_date: Optional[str] = None,
    *,
    job_id: Optional[str] = None,
    max_trades: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Run opsp.py (ThetaData Terminal) to generate iron condor recommendations.
    Uses EODHD_API_TOKEN for live underlying prices when run_date is today.
    max_trades: 10–100 (default 40 when omitted).
    """
    args: List[str] = []
    if run_date:
        args += ["--run-date", run_date]
    if max_trades is not None:
        n = max(10, min(100, int(max_trades)))
        args += ["--max-trades", str(n)]
    return _run_script("opsp.py", extra_args=args, timeout=1800, job_id=job_id)


def run_sync_data(years: Optional[List[int]] = None) -> None:
    """
    Sync only: (1) Prefetch today's cache for any missing S&P 500 symbols (v3),
    (2) If years provided, run full prefetch for those years. No recommendations
    generation; runs in background; job logs and morning status update when done.

    DO NOT add run_optsp() here — Sync Data must only sync; recommendations
    are generated only when the user explicitly clicks Generate Recommendations.
    """
    missing = get_symbols_missing_today_cache()
    if missing:
        logger.info(f"[sync] Prefetching today for {len(missing)} symbols with missing cache")
        run_prefetch(today_only=True, tickers=missing)
    else:
        logger.info("[sync] Today's cache complete for all symbols, skipping prefetch")
    if years:
        logger.info(f"[sync] Prefetching years {years}")
        run_prefetch(years=years)


def run_ibkr_ping(port: int = 7497, client_id: int = 1) -> Dict[str, Any]:
    """
    Connect to IBKR, print managed accounts, disconnect, and exit.
    Fast (~5 s on success).  Returns the same dict shape as _run_script.
    """
    args = [
        "--test-connection",
        "--port", str(port),
        "--client-id", str(client_id),
    ]
    # Allow up to 130 s: exeopt tries multiple hosts (127.0.0.1, localhost, ::1),
    # each with 60 s connect timeout, so we need >60 s to get the real error back.
    return _run_script("exeopt.py", extra_args=args, timeout=130)


def run_exeopt(
    rec_date: Optional[str] = None,
    dry_run: bool = False,
    tickers: Optional[List[str]] = None,
    port: int = 7497,
    client_id: int = 1,
    stop_loss_pct: float = 1.0,
    take_profit_pct: float = 0.50,
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
    # IBKR connection settings
    args += ["--port", str(port)]
    args += ["--client-id", str(client_id)]
    # Risk management
    args += ["--stop-loss-pct", str(stop_loss_pct)]
    args += ["--take-profit-pct", str(take_profit_pct)]
    # 420 s (7 min): the connect timeout is now 60 s, plus order execution can
    # take up to 120 s waiting for entry fill, so the script can legitimately run
    # ~3 min for a multi-ticker run.  Flutter receiveTimeout is 8 min (480 s),
    # leaving 60 s for FastAPI to serialize and return the response.
    return _run_script("exeopt.py", extra_args=args, timeout=420)


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
        records = df.to_dict(orient="records")
        # Explicit sort by score descending so frontend receives stable order (no client-side re-sort).
        def _score_key(r: Dict[str, Any]) -> float:
            s = r.get("score")
            if s is None or (isinstance(s, float) and pd.isna(s)):
                return float("-inf")
            try:
                return float(s)
            except (TypeError, ValueError):
                return float("-inf")
        records.sort(key=_score_key, reverse=True)
        return records
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


def save_symbols(symbols: List[str]) -> None:
    """Overwrite the symbols CSV with a new list (upload custom stock universe)."""
    os.makedirs(os.path.dirname(SYMBOLS_CSV), exist_ok=True)
    df = pd.DataFrame({"symbol": [s.upper().strip() for s in symbols if s.strip()]})
    df.to_csv(SYMBOLS_CSV, index=False)
    logger.info(f"Saved {len(df)} symbols to {SYMBOLS_CSV}")


# Packaged default: the sp500_symbols.csv bundled alongside opsp.py
_SCRIPTS_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "options")
_DEFAULT_SYMBOLS_CSV = os.path.join(_SCRIPTS_DIR, "sp500_symbols.csv")


def restore_default_symbols() -> Tuple[int, List[str]]:
    """Restore SYMBOLS_CSV from the bundled default SP500 list."""
    if not os.path.exists(_DEFAULT_SYMBOLS_CSV):
        raise FileNotFoundError(
            f"Default symbols file not found at {_DEFAULT_SYMBOLS_CSV}"
        )
    df = pd.read_csv(_DEFAULT_SYMBOLS_CSV)
    col = "symbol" if "symbol" in df.columns else df.columns[0]
    symbols = df[col].dropna().str.strip().str.upper().tolist()
    save_symbols(symbols)
    return len(symbols), symbols[:10]


# ---------------------------------------------------------------------------
# Reprice a specific iron-condor set using fresh ThetaData quotes
# ---------------------------------------------------------------------------

# Validity severity constants (tunable)
VALIDITY_MIN_PREMIUM = 0.05
VALIDITY_SPREAD_CAUTION_ABS = 0.10
VALIDITY_SPREAD_CAUTION_PCT = 0.15
VALIDITY_SPREAD_NOT_IDEAL_ABS = 0.25
VALIDITY_SPREAD_NOT_IDEAL_PCT = 0.30
VALIDITY_DEBIT_CAUTION_MAX = 0.20  # $/contract; debit magnitude <= this -> caution
VALIDITY_SPOT_BUFFER_FALLBACK = 0.50  # $; fallback when spot is None
VALIDITY_SLIPPAGE = 0.95  # 5% slippage: net_premium = total_mid * this
VALIDITY_PACKAGE_SPREAD_MAX_PCT = 0.40  # package spread > this fraction of premium -> not_ideal


def _build_reprice_response(
    *,
    ticker: str,
    spot: Optional[float],
    sp_bid: float, sp_ask: float, sp_mid: float,
    lp_bid: float, lp_ask: float, lp_mid: float,
    sc_bid: float, sc_ask: float, sc_mid: float,
    lc_bid: float, lc_ask: float, lc_mid: float,
    net_conservative: float,
    net_mid: float,
    net_premium: float,
    total_spread_pct: float,
    be_lower: float,
    be_upper: float,
    max_profit_usd: float,
    max_loss_usd: float,
    max_loss_per_share: float,
    debug: Dict[str, Any],
    strict_validity: bool,
) -> Dict[str, Any]:
    """Compute validity with severity (valid/caution/not_ideal) and build response."""
    validity_reasons: List[str] = []
    severity_level = 0  # 0=valid, 1=caution, 2=not_ideal

    # Distance of spot from breakeven range: 0 if inside, positive if outside
    distance_from_be: Optional[float] = None
    if spot is not None:
        if spot < be_lower:
            distance_from_be = be_lower - spot
        elif spot > be_upper:
            distance_from_be = spot - be_upper
        else:
            distance_from_be = 0.0

    # Dynamic breakeven buffer: 0.5% of spot, fallback when spot is None
    spot_buffer = (spot * 0.005) if (spot is not None and spot > 0) else VALIDITY_SPOT_BUFFER_FALLBACK

    if strict_validity:
        # Original strict: any issue = not valid (use net_premium for premium/debit checks)
        if abs(net_premium) < VALIDITY_MIN_PREMIUM:
            validity_reasons.append(
                f"Net premium too low (${net_premium:.2f} < ${VALIDITY_MIN_PREMIUM:.2f})"
            )
        if spot is not None:
            if spot < be_lower:
                validity_reasons.append(f"Spot ${spot:.2f} is below BE Lower {be_lower:.2f}")
            elif spot > be_upper:
                validity_reasons.append(f"Spot {spot:.2f} is above BE Upper {be_upper:.2f}")
        for name, bid, ask in [
            ("ShortPut", sp_bid, sp_ask),
            ("LongPut", lp_bid, lp_ask),
            ("ShortCall", sc_bid, sc_ask),
            ("LongCall", lc_bid, lc_ask),
        ]:
            mid = (bid + ask) / 2.0
            if mid > 0 and abs(ask - bid) / mid > 0.40:
                validity_reasons.append(f"Wide spread on {name}")
        if net_premium < 0:
            validity_reasons.append("Net premium is a debit (not a credit)")
        is_valid = len(validity_reasons) == 0
        validity_severity = "valid" if is_valid else "not_ideal"
    else:
        # Severity-based rules (use net_premium for Rule 1 and 4)
        if abs(net_premium) < VALIDITY_MIN_PREMIUM:
            validity_reasons.append(
                f"Net premium too low (${net_premium:.2f} < ${VALIDITY_MIN_PREMIUM:.2f})"
            )
            severity_level = max(severity_level, 2)

        if spot is not None:
            if spot < be_lower:
                dist = be_lower - spot
                if dist <= spot_buffer:
                    validity_reasons.append(
                        f"Spot ${spot:.2f} is below BE Lower {be_lower:.2f} (within ${dist:.2f})"
                    )
                    severity_level = max(severity_level, 1)
                else:
                    validity_reasons.append(
                        f"Spot {spot:.2f} is below BE Lower {be_lower:.2f} by {dist:.2f}"
                    )
                    severity_level = max(severity_level, 2)
            elif spot > be_upper:
                dist = spot - be_upper
                if dist <= spot_buffer:
                    validity_reasons.append(
                        f"Spot {spot:.2f} is above BE Upper {be_upper:.2f} (within {dist:.2f})"
                    )
                    severity_level = max(severity_level, 1)
                else:
                    validity_reasons.append(
                        f"Spot {spot:.2f} is above BE Upper {be_upper:.2f} by {dist:.2f}"
                    )
                    severity_level = max(severity_level, 2)

        # Per-leg spread: at most caution (severity 1), never not_ideal from per-leg alone
        for name, bid, ask in [
            ("ShortPut", sp_bid, sp_ask),
            ("LongPut", lp_bid, lp_ask),
            ("ShortCall", sc_bid, sc_ask),
            ("LongCall", lc_bid, lc_ask),
        ]:
            spread_abs = ask - bid
            mid = (bid + ask) / 2.0
            if mid <= 0:
                continue
            spread_pct = spread_abs / mid
            thresh_caution_abs = VALIDITY_SPREAD_CAUTION_ABS
            thresh_caution_pct = VALIDITY_SPREAD_CAUTION_PCT
            thresh_not_abs = VALIDITY_SPREAD_NOT_IDEAL_ABS
            thresh_not_pct = VALIDITY_SPREAD_NOT_IDEAL_PCT
            if spread_abs > max(thresh_not_abs, thresh_not_pct * mid):
                validity_reasons.append(
                    f"Wide spread on {name}: ${spread_abs:.2f} ({spread_pct:.1%} of mid)"
                )
                severity_level = max(severity_level, 1)
            elif spread_abs > max(thresh_caution_abs, thresh_caution_pct * mid):
                validity_reasons.append(
                    f"Wider spread on {name}: ${spread_abs:.2f} ({spread_pct:.1%} of mid)"
                )
                severity_level = max(severity_level, 1)

        # Holistic: package spread > 40% of premium -> not_ideal
        if total_spread_pct > VALIDITY_PACKAGE_SPREAD_MAX_PCT:
            validity_reasons.append(
                f"Package spread is >{VALIDITY_PACKAGE_SPREAD_MAX_PCT:.0%} of premium ({total_spread_pct:.1%})"
            )
            severity_level = max(severity_level, 2)

        if net_premium < 0:
            debit_mag = abs(net_premium)
            if debit_mag <= VALIDITY_DEBIT_CAUTION_MAX:
                validity_reasons.append(
                    f"Net premium is a small debit (${debit_mag:.2f}/contract)"
                )
                severity_level = max(severity_level, 1)
            else:
                validity_reasons.append(
                    f"Net premium is a debit (${debit_mag:.2f}/contract)"
                )
                severity_level = max(severity_level, 2)

        validity_severity = "valid" if severity_level == 0 else ("caution" if severity_level == 1 else "not_ideal")
        is_valid = severity_level == 0

    status = "green" if severity_level == 0 else ("yellow" if severity_level == 1 else "red")

    return {
        "ok": True,
        "ticker": ticker,
        "spot": spot,
        "spot_price": spot,
        "sp_bid": sp_bid, "sp_ask": sp_ask, "sp_mid": sp_mid,
        "lp_bid": lp_bid, "lp_ask": lp_ask, "lp_mid": lp_mid,
        "sc_bid": sc_bid, "sc_ask": sc_ask, "sc_mid": sc_mid,
        "lc_bid": lc_bid, "lc_ask": lc_ask, "lc_mid": lc_mid,
        "net_premium_conservative": net_conservative,
        "net_premium_mid": net_mid,
        "net_mid_premium": net_premium,
        "be_lower": be_lower,
        "be_upper": be_upper,
        "max_profit_usd": max_profit_usd,
        "max_loss_usd": max_loss_usd,
        "max_loss_per_share": max_loss_per_share,
        "is_valid": is_valid,
        "validity_severity": validity_severity,
        "validity_reasons": validity_reasons,
        "status": status,
        "total_spread_percent": total_spread_pct,
        "distance_from_be": distance_from_be,
        "debug": debug,
    }


def reprice_iron_condor(
    ticker: str,
    expiration: str,   # YYYY-MM-DD
    short_put: float,
    long_put: float,
    short_call: float,
    long_call: float,
    contracts: int = 1,
    width: Optional[float] = None,
    strict_validity: bool = False,
) -> Dict[str, Any]:
    """Fetch fresh quotes for the four legs and return updated metrics + validity check."""
    opsp = _get_opsp()

    today = datetime.now().strftime("%Y-%m-%d")

    # Use short-lived in-memory chain cache to avoid hammering ThetaTerminal on
    # every card poll (N cards × every 4 s = N fetches/4 s without this cache).
    chain = _get_chain_cached(ticker, today)
    if chain is None:
        chain = opsp.fetch_chain_for_ticker(
            api_token="Not_Needed",
            ticker=ticker,
            run_date=today,
            use_cache=False,
            write_cache=False,
        )
        if chain is not None and not chain.empty:
            _set_chain_cached(ticker, today, chain)

    NAN = float("nan")

    def _get_leg_quotes(strike: float, opt_type: str) -> Dict[str, float]:
        """Find bid/ask for a specific strike+type in the chain."""
        if chain is None or chain.empty:
            return {"bid": NAN, "ask": NAN, "mid": NAN}
        t_col = None
        for c in ["type", "_type", "right"]:
            if c in chain.columns:
                t_col = c
                break
        s_col = None
        for c in ["strike", "_strike"]:
            if c in chain.columns:
                s_col = c
                break
        if t_col is None or s_col is None:
            return {"bid": NAN, "ask": NAN, "mid": NAN}

        import numpy as np
        type_filter = chain[t_col].astype(str).str.lower().str.startswith(opt_type[0].lower())
        strike_filter = np.isclose(pd.to_numeric(chain[s_col], errors="coerce").fillna(-1), strike, atol=0.01)
        mask = type_filter & strike_filter

        # Also try matching against target expiration date
        for exp_col in ["exp_date", "_exp", "expiration"]:
            if exp_col in chain.columns:
                exp_filter = chain[exp_col].astype(str).str.startswith(expiration[:7])  # match YYYY-MM
                if exp_filter.any():
                    mask = mask & exp_filter
                break

        rows = chain[mask]
        if rows.empty:
            return {"bid": NAN, "ask": NAN, "mid": NAN}
        row = rows.iloc[0]
        bid = float(pd.to_numeric(row.get("bid", NAN), errors="coerce") or NAN)
        ask = float(pd.to_numeric(row.get("ask", NAN), errors="coerce") or NAN)
        mid = (bid + ask) / 2.0 if not (pd.isna(bid) or pd.isna(ask)) else NAN
        return {"bid": bid, "ask": ask, "mid": mid}

    sp_q = _get_leg_quotes(short_put,  "put")
    lp_q = _get_leg_quotes(long_put,   "put")
    sc_q = _get_leg_quotes(short_call, "call")
    lc_q = _get_leg_quotes(long_call,  "call")

    def _safe(v: float, fallback: float = 0.0) -> float:
        return fallback if (v is None or pd.isna(v)) else float(v)

    sp_bid  = _safe(sp_q["bid"])
    sp_ask  = _safe(sp_q["ask"])
    lp_bid  = _safe(lp_q["bid"])
    lp_ask  = _safe(lp_q["ask"])
    sc_bid  = _safe(sc_q["bid"])
    sc_ask  = _safe(sc_q["ask"])
    lc_bid  = _safe(lc_q["bid"])
    lc_ask  = _safe(lc_q["ask"])
    sp_mid  = _safe(sp_q["mid"])
    lp_mid  = _safe(lp_q["mid"])
    sc_mid  = _safe(sc_q["mid"])
    lc_mid  = _safe(lc_q["mid"])

    # Conservative: sell@bid, buy@ask (kept for response/debug)
    net_conservative = sp_bid - lp_ask + sc_bid - lc_ask
    # Mid estimate
    net_mid = sp_mid - lp_mid + sc_mid - lc_mid
    # Pro-Fill: aggressive fill weighted 75% mid / 25% conservative
    net_premium = (net_mid * 0.75) + (net_conservative * 0.25)

    w = width if width is not None else abs(short_put - long_put)
    net_abs = abs(net_premium)
    max_loss_per_share = max(0.0, w - net_abs)
    max_profit_usd = net_abs * 100 * contracts
    max_loss_usd = max_loss_per_share * 100 * contracts
    be_lower = short_put - net_abs if net_premium > 0 else short_put + net_abs
    be_upper = short_call + net_abs if net_premium > 0 else short_call - net_abs

    # Spot: prefer EODHD live price (cached 60 s to avoid rate-limiting from
    # concurrent reprice polls), then fall back to most-recent cached close.
    spot: Optional[float] = None
    try:
        spot = _get_live_spot_cached(ticker)
        if spot is None or spot <= 0:
            if hasattr(opsp, "fetch_eodhd_live_price"):
                fetched = opsp.fetch_eodhd_live_price(ticker)
                if fetched and fetched > 0:
                    _set_live_spot_cached(ticker, fetched)
                    spot = fetched
        if spot is None or spot <= 0:
            price_df = opsp.load_price_df_cached(ticker)
            if price_df is not None:
                spot = opsp.get_close_on_date(price_df, today)
    except Exception:
        pass

    # Per-leg spreads for debug
    def _spread(bid: float, ask: float) -> float:
        return ask - bid if (bid and ask) else 0.0

    sp_spread = _spread(sp_bid, sp_ask)
    lp_spread = _spread(lp_bid, lp_ask)
    sc_spread = _spread(sc_bid, sc_ask)
    lc_spread = _spread(lc_bid, lc_ask)
    total_spread = sp_spread + lp_spread + sc_spread + lc_spread
    total_spread_pct = total_spread / max(abs(net_premium), 1e-6)

    # Debug block (always included)
    _now_utc = datetime.now(timezone.utc)
    debug = {
        "spot": spot,
        "legs": {
            "short_put": {"bid": sp_bid, "ask": sp_ask, "mid": sp_mid, "spread": sp_spread},
            "long_put": {"bid": lp_bid, "ask": lp_ask, "mid": lp_mid, "spread": lp_spread},
            "short_call": {"bid": sc_bid, "ask": sc_ask, "mid": sc_mid, "spread": sc_spread},
            "long_call": {"bid": lc_bid, "ask": lc_ask, "mid": lc_mid, "spread": lc_spread},
        },
        "net_premium_conservative": net_conservative,
        "net_premium_mid": net_mid,
        "be_lower": be_lower,
        "be_upper": be_upper,
        "server_timestamp_utc": _now_utc.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
    }

    return _build_reprice_response(
        ticker=ticker,
        spot=spot,
        sp_bid=sp_bid, sp_ask=sp_ask, sp_mid=sp_mid,
        lp_bid=lp_bid, lp_ask=lp_ask, lp_mid=lp_mid,
        sc_bid=sc_bid, sc_ask=sc_ask, sc_mid=sc_mid,
        lc_bid=lc_bid, lc_ask=lc_ask, lc_mid=lc_mid,
        net_conservative=net_conservative,
        net_mid=net_mid,
        net_premium=net_premium,
        total_spread_pct=total_spread_pct,
        be_lower=be_lower,
        be_upper=be_upper,
        max_profit_usd=max_profit_usd,
        max_loss_usd=max_loss_usd,
        max_loss_per_share=max_loss_per_share,
        debug=debug,
        strict_validity=strict_validity,
    )


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


# ---------------------------------------------------------------------------
# Actual fills — per-user persistence (SQLite)
# ---------------------------------------------------------------------------

import sqlite3

ACTUAL_FILLS_DB = os.path.join(OPTIONSYS_DATA_DIR, "actual_fills.db")


def _actual_fills_conn():
    os.makedirs(os.path.dirname(ACTUAL_FILLS_DB) or ".", exist_ok=True)
    conn = sqlite3.connect(ACTUAL_FILLS_DB)
    conn.row_factory = sqlite3.Row
    return conn


def _init_actual_fills_table(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS actual_fills (
            user_id TEXT NOT NULL,
            rec_key TEXT NOT NULL,
            ticker TEXT NOT NULL,
            expiration TEXT NOT NULL,
            short_put REAL NOT NULL,
            long_put REAL NOT NULL,
            short_call REAL NOT NULL,
            long_call REAL NOT NULL,
            actual_net_premium_per_contract REAL NOT NULL,
            actual_contracts INTEGER,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (user_id, rec_key)
        )
    """)
    conn.commit()
    # Add columns for entry/exit and active state (idempotent migration)
    for col_def in [
        "is_entered INTEGER DEFAULT 1",
        "is_active INTEGER DEFAULT 1",
        "entry_date TEXT",
        "exit_price REAL",
        "exit_date TEXT",
    ]:
        try:
            conn.execute(f"ALTER TABLE actual_fills ADD COLUMN {col_def}")
            conn.commit()
        except sqlite3.OperationalError as e:
            if "duplicate column" in str(e).lower():
                pass
            else:
                raise


def _load_active_trades_json() -> Dict[str, List[Dict[str, Any]]]:
    """Load full active_trades file: { user_id: [ {...}, ... ] }."""
    if not os.path.exists(ACTIVE_TRADES_JSON):
        return {}
    try:
        with open(ACTIVE_TRADES_JSON) as f:
            return json.load(f)
    except Exception:
        return {}


def _save_active_trades_json(data: Dict[str, List[Dict[str, Any]]]) -> None:
    os.makedirs(ACTIVE_TRADES_DATA_DIR, exist_ok=True)
    with open(ACTIVE_TRADES_JSON, "w") as f:
        json.dump(data, f, indent=2)


def load_active_trades(user_id: str) -> List[Dict[str, Any]]:
    """Return list of active trade records for the user."""
    data = _load_active_trades_json()
    return data.get(user_id, [])


def save_active_trades(user_id: str, trades: List[Dict[str, Any]]) -> None:
    """Overwrite active trades list for the user."""
    data = _load_active_trades_json()
    data[user_id] = trades
    _save_active_trades_json(data)


def _sync_actual_fill_to_active_trades(
    user_id: str,
    rec_key: str,
    ticker: str,
    expiration: str,
    short_put: float,
    long_put: float,
    short_call: float,
    long_call: float,
    entry_credit: float,
    contracts: Optional[int] = None,
    entry_pop: Optional[float] = None,
) -> None:
    """Add or update one active trade from an actual fill."""
    trades = load_active_trades(user_id)
    n = contracts if contracts is not None else 1
    record = {
        "rec_key": rec_key,
        "ticker": ticker,
        "exp": expiration,
        "short_put": short_put,
        "long_put": long_put,
        "short_call": short_call,
        "long_call": long_call,
        "entry_credit": entry_credit,
        "entry_pop": entry_pop,
        "contracts": n,
    }
    # Upsert by rec_key
    trades = [t for t in trades if t.get("rec_key") != rec_key]
    trades.append(record)
    save_active_trades(user_id, trades)


def upsert_actual_fill(
    user_id: str,
    rec_key: str,
    ticker: str,
    expiration: str,
    short_put: float,
    long_put: float,
    short_call: float,
    long_call: float,
    actual_net_premium_per_contract: float,
    actual_contracts: Optional[int] = None,
    entry_pop: Optional[float] = None,
    is_entered: bool = True,
) -> Dict[str, Any]:
    now = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    entry_date = date.today().isoformat() if is_entered else None
    conn = _actual_fills_conn()
    try:
        _init_actual_fills_table(conn)
        conn.execute(
            """
            INSERT INTO actual_fills (
                user_id, rec_key, ticker, expiration, short_put, long_put,
                short_call, long_call, actual_net_premium_per_contract, actual_contracts,
                created_at, updated_at, is_entered, is_active, entry_date
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?)
            ON CONFLICT (user_id, rec_key) DO UPDATE SET
                ticker = excluded.ticker,
                expiration = excluded.expiration,
                short_put = excluded.short_put,
                long_put = excluded.long_put,
                short_call = excluded.short_call,
                long_call = excluded.long_call,
                actual_net_premium_per_contract = excluded.actual_net_premium_per_contract,
                actual_contracts = excluded.actual_contracts,
                updated_at = excluded.updated_at,
                is_entered = excluded.is_entered,
                entry_date = CASE WHEN excluded.is_entered = 1 THEN excluded.entry_date ELSE actual_fills.entry_date END
            """,
            (
                user_id, rec_key, ticker, expiration, short_put, long_put,
                short_call, long_call, actual_net_premium_per_contract, actual_contracts,
                now, now, 1 if is_entered else 0, entry_date,
            ),
        )
        conn.commit()
        if is_entered:
            _sync_actual_fill_to_active_trades(
                user_id=user_id,
                rec_key=rec_key,
                ticker=ticker,
                expiration=expiration,
                short_put=short_put,
                long_put=long_put,
                short_call=short_call,
                long_call=long_call,
                entry_credit=actual_net_premium_per_contract,
                contracts=actual_contracts,
                entry_pop=entry_pop,
            )
        return {"ok": True, "rec_key": rec_key}
    finally:
        conn.close()


def get_actual_fills(
    user_id: str,
    date_filter: Optional[str] = None,
    tickers: Optional[List[str]] = None,
) -> Dict[str, Dict[str, Any]]:
    """
    Return all actual fills for the user as a dict keyed by rec_key.
    If date_filter (YYYY-MM-DD) is set, only return fills whose expiration matches that date
    (or rec_key contains that date). If tickers is set, only return fills for those tickers.
    """
    conn = _actual_fills_conn()
    try:
        _init_actual_fills_table(conn)
        query = "SELECT * FROM actual_fills WHERE user_id = ?"
        params: List[Any] = [user_id]
        if date_filter:
            query += " AND (expiration = ? OR rec_key LIKE ?)"
            params.extend([date_filter, f"%|{date_filter}|%"])
        if tickers:
            placeholders = ",".join("?" * len(tickers))
            query += f" AND ticker IN ({placeholders})"
            params.extend([t.upper() for t in tickers])
        query += " ORDER BY updated_at DESC"
        cur = conn.execute(query, params)
        rows = cur.fetchall()
        out: Dict[str, Dict[str, Any]] = {}
        for row in rows:
            r = dict(row)
            rec_key = r["rec_key"]
            out[rec_key] = {
                "rec_key": rec_key,
                "ticker": r["ticker"],
                "expiration": r["expiration"],
                "short_put": r["short_put"],
                "long_put": r["long_put"],
                "short_call": r["short_call"],
                "long_call": r["long_call"],
                "actual_net_premium_per_contract": r["actual_net_premium_per_contract"],
                "actual_contracts": r["actual_contracts"],
                "created_at": r["created_at"],
                "updated_at": r["updated_at"],
                "is_entered": r.get("is_entered", 1),
                "is_active": r.get("is_active", 1),
                "entry_date": r.get("entry_date"),
                "exit_price": r.get("exit_price"),
                "exit_date": r.get("exit_date"),
            }
        return out
    finally:
        conn.close()


def delete_actual_fill(user_id: str, rec_key: str) -> bool:
    conn = _actual_fills_conn()
    try:
        _init_actual_fills_table(conn)
        cur = conn.execute(
            "DELETE FROM actual_fills WHERE user_id = ? AND rec_key = ?",
            (user_id, rec_key),
        )
        conn.commit()
        deleted = cur.rowcount > 0
        if deleted:
            trades = [t for t in load_active_trades(user_id) if t.get("rec_key") != rec_key]
            save_active_trades(user_id, trades)
        return deleted
    finally:
        conn.close()


def delete_all_actual_fills(user_id: str) -> int:
    """Delete all actual_fills rows for this user and clear their active_trades JSON. Returns number of rows deleted."""
    conn = _actual_fills_conn()
    try:
        _init_actual_fills_table(conn)
        cur = conn.execute(
            "DELETE FROM actual_fills WHERE user_id = ?",
            (user_id,),
        )
        conn.commit()
        deleted = cur.rowcount
        save_active_trades(user_id, [])
        return deleted
    finally:
        conn.close()


def set_actual_fill_exit(
    user_id: str,
    rec_key: str,
    exit_price: float,
    exit_date: str,
) -> bool:
    """Mark a position as exited: set is_active=0, exit_price, exit_date. Returns True if updated."""
    conn = _actual_fills_conn()
    try:
        _init_actual_fills_table(conn)
        now = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        cur = conn.execute(
            """
            UPDATE actual_fills
            SET is_active = 0, exit_price = ?, exit_date = ?, updated_at = ?
            WHERE user_id = ? AND rec_key = ?
            """,
            (exit_price, exit_date, now, user_id, rec_key),
        )
        conn.commit()
        updated = cur.rowcount > 0
        if updated:
            trades = [t for t in load_active_trades(user_id) if t.get("rec_key") != rec_key]
            save_active_trades(user_id, trades)
        return updated
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Active positions — from actual_fills (is_entered=1, is_active=1) with live P&L
# ---------------------------------------------------------------------------

POP_WARNING_DROP_PCT = 0.10  # flag when updated_pop drops >= 10 pp below entry_pop


def _compute_updated_pop(spot: Optional[float], short_put: float, short_call: float, expiration: str) -> Optional[float]:
    """Estimate POP using opsp lognormal with default IV. Returns None if spot missing or invalid."""
    if spot is None or not (spot > 0):
        return None
    try:
        exp_dt = datetime.strptime(expiration[:10], "%Y-%m-%d")
        today = date.today()
        dte = max(0, (exp_dt.date() - today).days)
        if dte <= 0:
            return None
        try:
            opsp = _get_opsp()
        except Exception:
            return None
        if hasattr(opsp, "_calc_pop_lognormal"):
            iv = 0.25
            return opsp._calc_pop_lognormal(spot, short_put, short_call, iv, dte)
    except Exception:
        pass
    return None


def _get_active_fills_rows(user_id: str) -> List[Dict[str, Any]]:
    """Return rows from actual_fills where is_entered=1 and is_active=1 (or columns missing for back compat)."""
    conn = _actual_fills_conn()
    try:
        _init_actual_fills_table(conn)
        cur = conn.execute(
            """
            SELECT * FROM actual_fills
            WHERE user_id = ?
              AND (COALESCE(is_entered, 1) = 1)
              AND (COALESCE(is_active, 1) = 1)
            ORDER BY updated_at DESC
            """,
            (user_id,),
        )
        rows = cur.fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def get_active_positions_with_stats(user_id: str) -> List[Dict[str, Any]]:
    """
    Load active positions from actual_fills (is_entered=1, is_active=1), compute current P&L and updated POP.
    P&L = (entry_credit - current_combo_mid) * 100 * contracts.
    pop_warning = True when updated_pop drops >= POP_WARNING_DROP_PCT below entry_pop.
    """
    rows = _get_active_fills_rows(user_id)
    out: List[Dict[str, Any]] = []
    for r in rows:
        rec_key = r.get("rec_key", "")
        ticker = r.get("ticker", "")
        exp = r.get("expiration", "") or r.get("exp", "")
        short_put = float(r.get("short_put", 0))
        long_put = float(r.get("long_put", 0))
        short_call = float(r.get("short_call", 0))
        long_call = float(r.get("long_call", 0))
        entry_credit = float(r.get("actual_net_premium_per_contract", 0))
        entry_pop = r.get("entry_pop")
        if entry_pop is not None:
            entry_pop = float(entry_pop)
        contracts = int(r.get("actual_contracts", 1) or 1)
        entry_date = r.get("entry_date")

        current_combo_mid: Optional[float] = None
        spot: Optional[float] = None
        try:
            rep = reprice_iron_condor(
                ticker=ticker,
                expiration=exp,
                short_put=short_put,
                long_put=long_put,
                short_call=short_call,
                long_call=long_call,
                contracts=contracts,
            )
            current_combo_mid = rep.get("net_premium_mid")
            spot = rep.get("spot")
        except Exception:
            pass

        if current_combo_mid is not None:
            current_pl = (entry_credit - current_combo_mid) * 100 * contracts
            current_pl_pct = (current_pl / (entry_credit * 100 * contracts)) if (entry_credit * contracts) else None
        else:
            current_pl = None
            current_pl_pct = None

        updated_pop = _compute_updated_pop(spot, short_put, short_call, exp)
        pop_warning = False
        if entry_pop is not None and updated_pop is not None:
            if (entry_pop - updated_pop) >= POP_WARNING_DROP_PCT:
                pop_warning = True

        width = abs(short_put - long_put) if short_put and long_put else 0
        max_risk_per_share = max(0.0, width - abs(entry_credit)) if width else 0
        max_risk_usd = max_risk_per_share * 100 * contracts if max_risk_per_share else None

        row: Dict[str, Any] = {
            "rec_key": rec_key,
            "ticker": ticker,
            "expiration": exp,
            "short_put": short_put,
            "long_put": long_put,
            "short_call": short_call,
            "long_call": long_call,
            "strategy_name": "Iron Condor",
            "actual_net_premium_per_contract": entry_credit,
            "actual_price": entry_credit,
            "actual_contracts": contracts,
            "entry_date": entry_date,
            "live_price": spot,
            "spot": spot,
            "current_pl": current_pl,
            "current_pl_pct": current_pl_pct,
            "daily_pl": None,
            "daily_pl_pct": None,
            "sl_price": None,
            "tp_price": None,
            "max_risk_usd": max_risk_usd,
            "updated_pop": updated_pop,
            "pop_warning": pop_warning,
            "is_active": True,
            "exit_price": None,
            "exit_date": None,
            "current_combo_mid": current_combo_mid,
        }
        out.append(row)
    return out
