#!/usr/bin/env python3
"""
prefetch_theta.py

Prefetch option chain data from ThetaData Terminal for backtesting with
backtest_optsp_system.py. Populates the same cache used by optsptheta.py.

Strategy:
  Instead of 1 API request per (ticker, date), this script groups by expiration:
  1 request per (ticker, expiration) covers ALL dates in the range at once.
  For 2 years × 499 tickers this means ~12,000 requests instead of ~250,000.

Cache format: options_data_cache/thetadata/snapshots/{TICKER}/{YYYY-MM-DD}.json.gz
              (identical to what optsptheta.py writes and reads)

Examples:
  export THETADATA_USERNAME="jon.idane6@gmail.com"
  export THETADATA_PASSWORD="di5n1xer"

  # Backfill full years
  caffeinate -i python prefetch_theta.py --years 2024 2025

  # Specific date range
  caffeinate -i python prefetch_theta.py --start-date 2024-01-01 --end-date 2024-06-30

  # Today only
  caffeinate -i python prefetch_theta.py --today

  # Subset of tickers
  caffeinate -i python prefetch_theta.py --today --tickers AAPL MSFT NVDA
"""

from __future__ import annotations

import math
import os
import sys
import json
import gzip
import subprocess
import time
import argparse
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, date
from typing import Any, Dict, List, Optional, Tuple

import requests
import pandas as pd
from scipy.stats import norm
from scipy.optimize import brentq

# =========================
# CONFIG  (matched to optsptheta.py)
# =========================
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.environ.get("STOCK_BASE_DIR") or SCRIPT_DIR
LOG_FILE = os.path.join(BASE_DIR, "prefetch_theta.log")

SP500_SYMBOLS_CSV = os.path.join(BASE_DIR, "sp500_symbols.csv")

TARGET_DTE = 45
# Widen tolerance so prefetch doesn't miss weekly expirations (e.g. when backfilling missing days)
DTE_TOLERANCE_DAYS = 25

_THETA_HOST = "127.0.0.1"
# ThetaTerminal default port (override with THETA_PORT env if different)
_THETA_PORT = int(os.environ.get("THETA_PORT", "25503"))

# ThetaTerminal JAR and credentials for automatic launch (override with env THETA_JAR_PATH / THETADATA_*)
# Prefer v3 JAR from Downloads (v1.8.6 does not expose v3 HTTP endpoints; v3 fixes 404 "No context found")
_DEFAULT_THETA_JAR = "/Users/idanamar/Downloads/ThetaTerminalv3.jar"
_THETA_JAR_FALLBACK = "/Volumes/Extreme Pro/App gpt/stock_api-main_updated/options/ThetaTerminal.jar"
THETA_JAR_PATH = os.environ.get("THETA_JAR_PATH") or (
    _DEFAULT_THETA_JAR if os.path.isfile(_DEFAULT_THETA_JAR) else _THETA_JAR_FALLBACK
)
THETA_USERNAME = os.environ.get("THETADATA_USERNAME") or "jon.idane6@gmail.com"
THETA_PASSWORD = os.environ.get("THETADATA_PASSWORD") or "di5n1xer"

# How long to wait for terminal to become ready after launch (seconds; v1.8.6 may need 180s to init v3)
_THETA_CONNECT_TIMEOUT = 180

HISTORICAL_CACHE_DIR = os.path.join(BASE_DIR, "historical_data_cache")
_HIST_CACHE_ALT      = os.path.join(SCRIPT_DIR, "historical_data_cache")   # fallback when BASE_DIR ≠ SCRIPT_DIR
OPTIONS_CACHE_DIR    = os.path.join(BASE_DIR, "options_data_cache", "thetadata", "snapshots")

_RISK_FREE_RATE = 0.045  # approximate risk-free rate for Black-Scholes

DEFAULT_TICKERS = ["AAPL", "MSFT", "GOOG", "AMZN", "NVDA"]

# Small pause between expiration requests to be kind to the terminal
REQUEST_DELAY_SEC = 0.1


def _theta_http_ready(timeout_sec: float = 5.0) -> bool:
    """Return True if the ThetaTerminal HTTP API on port 25503 is responding (v3 expirations endpoint)."""
    base_url = f"http://{_THETA_HOST}:{_THETA_PORT}"
    # v3 standard: /v3/option/list/expirations?symbol=&format=json (terminal returns non-JSON by default otherwise)
    v3_url = f"{base_url}/v3/option/list/expirations"
    try:
        r = requests.get(
            v3_url,
            params={"symbol": "AAPL", "format": "json"},
            timeout=timeout_sec,
        )
        if r.status_code != 200:
            return False
        # Must get JSON back; empty or non-JSON body causes "Expecting value: line 1 column 1"
        ct = (r.headers.get("Content-Type") or "").lower()
        if "json" not in ct and r.text.strip():
            return False
        return True
    except Exception:
        return False


def _launch_theta_terminal() -> None:
    """Launch ThetaTerminal JAR with configured credentials. JAR must listen on HTTP port 25503."""
    if not os.path.isfile(THETA_JAR_PATH):
        raise FileNotFoundError(
            f"ThetaTerminal JAR not found at {THETA_JAR_PATH}. "
            "Set THETA_JAR_PATH or place ThetaTerminal.jar in the options directory."
        )
    config_path = os.path.join(os.path.dirname(THETA_JAR_PATH), "ThetaTerminal_25503.properties")
    try:
        with open(config_path, "w", encoding="utf-8") as f:
            f.write("HTTP_PORT=25503\n")
    except OSError as e:
        logging.warning("Could not write ThetaTerminal config %s: %s. Terminal may use default port.", config_path, e)
    cmd = [
        "java",
        "-jar",
        THETA_JAR_PATH,
        THETA_USERNAME,
        THETA_PASSWORD,
    ]
    if os.path.isfile(config_path):
        cmd.extend(["--config", config_path])
    cwd = os.path.dirname(THETA_JAR_PATH)
    logging.info("Launching ThetaTerminal: %s (cwd=%s)", " ".join(cmd[:4] + ["..."]), cwd)
    subprocess.Popen(
        cmd,
        cwd=cwd,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(),
    ],
)


# =========================
# Black-Scholes (identical to opsp.py)
# =========================

def _bs_price(S: float, K: float, T: float, r: float, sigma: float, right: str) -> float:
    if T <= 0 or sigma <= 0:
        return max(0.0, (S - K) if right == "C" else (K - S))
    d1 = (math.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * math.sqrt(T))
    d2 = d1 - sigma * math.sqrt(T)
    if right == "C":
        return S * norm.cdf(d1) - K * math.exp(-r * T) * norm.cdf(d2)
    return K * math.exp(-r * T) * norm.cdf(-d2) - S * norm.cdf(-d1)


def _bs_delta(S: float, K: float, T: float, r: float, sigma: float, right: str) -> float:
    if T <= 0 or sigma <= 0:
        return 0.0
    d1 = (math.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * math.sqrt(T))
    return float(norm.cdf(d1) if right == "C" else norm.cdf(d1) - 1)


def _implied_vol(S: float, K: float, T: float, r: float, mid: float, right: str) -> Optional[float]:
    if T <= 0 or mid <= 0 or S <= 0 or K <= 0:
        return None
    intrinsic = max(0.0, (S - K) if right == "C" else (K - S))
    if mid <= intrinsic:
        return None
    try:
        iv = brentq(lambda sigma: _bs_price(S, K, T, r, sigma, right) - mid,
                    1e-4, 10.0, xtol=1e-4, maxiter=50)
        return float(iv) if 0.001 < iv < 9.9 else None
    except Exception:
        return None


# =========================
# Price cache (for Black-Scholes underlying)
# =========================

_price_df_cache: Dict[str, Optional[pd.DataFrame]] = {}


def _load_price_df(ticker: str) -> Optional[pd.DataFrame]:
    if ticker in _price_df_cache:
        return _price_df_cache[ticker]
    candidates = [
        os.path.join(HISTORICAL_CACHE_DIR, f"{ticker}.parquet"),
        os.path.join(_HIST_CACHE_ALT, f"{ticker}.parquet"),
    ]
    for path in candidates:
        if os.path.exists(path):
            try:
                df = pd.read_parquet(path)
                if not isinstance(df.index, pd.DatetimeIndex):
                    df.index = pd.to_datetime(df.index)
                _price_df_cache[ticker] = df.sort_index()
                return _price_df_cache[ticker]
            except Exception as e:
                logging.warning("Could not load price data for %s: %s", ticker, e)
    _price_df_cache[ticker] = None
    return None


def _get_spot(price_df: Optional[pd.DataFrame], for_date: date) -> Optional[float]:
    if price_df is None or price_df.empty:
        return None
    ts = pd.Timestamp(for_date)
    try:
        row = price_df.loc[ts]
        if isinstance(row, pd.Series):
            for c in ("adjusted_close", "close", "Close", "adj_close"):
                if c in row.index and pd.notna(row[c]):
                    return float(row[c])
    except KeyError:
        pass
    idx = price_df.index[price_df.index <= ts]
    if len(idx) == 0:
        return None
    r = price_df.loc[idx[-1]]
    if isinstance(r, pd.Series):
        for c in ("adjusted_close", "close", "Close", "adj_close"):
            if c in r.index and pd.notna(r[c]):
                return float(r[c])
    return None


# =========================
# Helpers
# =========================

def _symbol_base(symbol: str) -> str:
    return str(symbol).upper().replace(".US", "").replace(".INDX", "").strip()


def is_business_day(d: date) -> bool:
    return d.weekday() < 5


def generate_business_days(start: date, end: date) -> List[date]:
    days, cur = [], start
    while cur <= end:
        if is_business_day(cur):
            days.append(cur)
        cur += timedelta(days=1)
    return days


def _options_cache_path(ticker: str, trade_date: str) -> str:
    return os.path.join(OPTIONS_CACHE_DIR, _symbol_base(ticker), f"{trade_date}.json.gz")


def _write_gz_json(path: str, obj: Any) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with gzip.open(path, "wt", encoding="utf-8") as f:
        json.dump(obj, f)


def _write_empty_sentinel(ticker: str, trade_date: date) -> None:
    """Write a zero-row cache file so this date is not re-fetched (e.g. market holiday)."""
    run_date = trade_date.isoformat()
    _write_gz_json(
        _options_cache_path(ticker, run_date),
        {"underlying": ticker, "trade_date": run_date, "rows": []},
    )


def _read_tickers_from_csv(path: str) -> List[str]:
    df = pd.read_csv(path)
    if df.empty:
        return []
    cols = {c.lower(): c for c in df.columns}
    for key in ("ticker", "symbol", "sym"):
        if key in cols:
            ser = df[cols[key]]
            break
    else:
        ser = df.iloc[:, 0]
    tickers = []
    for x in ser.astype(str).tolist():
        t = x.strip().upper()
        if t and t != "NAN":
            tickers.append(_symbol_base(t))
    seen, out = set(), []
    for t in tickers:
        if t not in seen:
            seen.add(t)
            out.append(t)
    return out


# =========================
# ThetaTerminal HTTP helpers
# =========================

# Retry backoff (seconds) for 429 Too Many Requests
_THETA_429_RETRIES = 3
_THETA_429_BACKOFF = [5, 15, 45]


def _theta_get(endpoint: str, params: dict, timeout: int = 120) -> Any:
    """Call the local ThetaTerminal REST API (v3) and return parsed JSON.
    Retries on 429 (Too Many Requests) with backoff.
    """
    url = f"http://{_THETA_HOST}:{_THETA_PORT}{endpoint}"
    full_url = f"{url}?{requests.compat.urlencode(params)}" if params else url
    logging.debug("GET %s", full_url)
    last_429: Optional[requests.Response] = None
    for attempt in range(_THETA_429_RETRIES + 1):
        resp = requests.get(url, params=params, timeout=timeout)
        if resp.status_code == 429:
            last_429 = resp
            if attempt < _THETA_429_RETRIES:
                wait = _THETA_429_BACKOFF[min(attempt, len(_THETA_429_BACKOFF) - 1)]
                logging.warning(
                    "ThetaData 429 Too Many Requests for %s; retry %s/%s in %ss",
                    endpoint, attempt + 1, _THETA_429_RETRIES, wait,
                )
                time.sleep(wait)
                continue
            logging.error(
                "ThetaData 429 Too Many Requests for %s after %s retries.",
                endpoint, _THETA_429_RETRIES,
            )
            resp.raise_for_status()
        if resp.status_code == 403:
            logging.error(
                "ThetaData API returned 403 Unauthorized for %s. "
                "Check ThetaData subscription - v3 access might be restricted.",
                endpoint,
            )
            resp.raise_for_status()
        if resp.status_code == 410:
            logging.error(
                "ThetaData API returned 410 Gone for %s. "
                "Endpoint may have been removed or moved. "
                "Ensure ThetaTerminal and this script use the v3 API.",
                endpoint,
            )
            resp.raise_for_status()
        resp.raise_for_status()
        break
    else:
        if last_429 is not None:
            last_429.raise_for_status()
        raise RuntimeError("_theta_get unexpected loop exit")
    text = resp.text.strip()
    if not text:
        logging.warning("ThetaData returned empty body for %s (add format=json?)", endpoint)
        raise RuntimeError(f"ThetaData returned empty body for {endpoint}. Try adding format=json to the request.")
    try:
        data = resp.json()
    except ValueError as e:
        logging.warning("ThetaData response is not JSON for %s: %.200s", endpoint, text[:200])
        raise RuntimeError(
            f"ThetaData response for {endpoint} is not valid JSON (got {len(text)} chars). "
            "Ensure the request includes format=json."
        ) from e
    if isinstance(data, dict):
        hdr = data.get("header", {})
        if hdr and hdr.get("error_type"):
            raise RuntimeError(f"API error [{endpoint}]: {hdr}")
    return data


def _parse_expiration_value(e: Any) -> Optional[date]:
    """Parse one expiration from API (v3 can return YYYYMMDD int/str or YYYY-MM-DD str)."""
    s = str(e).strip()
    if not s:
        return None
    for fmt in ("%Y%m%d", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


def _get_expirations(root: str) -> List[date]:
    """Return all available option expirations (v3). ThetaData v3 returns response as list of dicts with 'expiration' key."""
    params = {"symbol": root.upper(), "format": "json"}
    data = _theta_get("/v3/option/list/expirations", params, timeout=15)
    raw = data.get("response", data.get("expirations", []))
    if not raw and isinstance(data, list):
        raw = data
    exps = []
    for e in raw:
        # v3 returns list of objects: {"expiration": "2026-04-18"} or {"expiration": "20260418"}
        val = e.get("expiration", e) if isinstance(e, dict) else e
        d = _parse_expiration_value(val)
        if d is not None:
            exps.append(d)
    return exps



def _rows_to_standard(rows: List[dict], ticker: str, run_date: str) -> List[dict]:
    """Convert raw rows to the flat cache format that optsptheta.py reads."""
    out = []
    for r in rows:
        out.append({
            "__ticker":           ticker,
            "__run_date":         run_date,
            "type":               "call" if str(r.get("right", "")).upper().startswith("C") else "put",
            "strike":             r["strike"],
            "bid":                r["bid"],
            "ask":                r["ask"],
            "delta":              r["delta"],
            "implied_volatility": r["implied_vol"],
            "exp_date":           r["expiration"],
        })
    return out


def _add_row(
    date_rows: Dict[date, List[dict]],
    trade_date: date,
    exp: date,
    strike: float,
    right: str,
    bid: float,
    ask: float,
    price_df: Optional[pd.DataFrame],
) -> None:
    """Compute Black-Scholes greeks and append one row to date_rows."""
    bid = float(bid or 0)
    ask = float(ask or 0)
    if bid < 0 or ask < 0 or ask < bid:
        return
    mid = (bid + ask) / 2.0
    T = max((exp - trade_date).days, 1) / 365.0
    r_flag = "C" if str(right).upper().startswith("C") else "P"

    spot = _get_spot(price_df, trade_date)
    delta: float = float("nan")
    iv: Optional[float] = None
    if spot and spot > 0 and strike > 0 and mid > 0:
        iv = _implied_vol(spot, strike, T, _RISK_FREE_RATE, mid, r_flag)
        sigma = iv if iv else 0.30
        delta = _bs_delta(spot, strike, T, _RISK_FREE_RATE, sigma, r_flag)

    date_rows.setdefault(trade_date, []).append({
        "expiration": exp.strftime("%Y%m%d"),
        "strike":     strike,
        "right":      right,
        "bid":        bid,
        "ask":        ask,
        "delta":      delta,
        "implied_vol": iv if iv is not None else float("nan"),
    })


# =========================
# Per-ticker prefetch
# =========================

def prefetch_ticker(
    ticker: str,
    bus_days: List[date],
    force: bool = False,
    target_dte: int = TARGET_DTE,
    dte_tol: int = DTE_TOLERANCE_DAYS,
) -> Tuple[int, int]:
    """
    Fetch and cache option chains for one ticker across all business days.

    Data sources (v3 ThetaTerminal only — v2 bulk endpoints removed in v3):
      • Past dates  → /v3/option/history/eod  (one call per expiration covers full date range)
      • Today       → /v3/option/snapshot/quote (live bid/ask)
    Delta and implied-vol are computed locally via Black-Scholes using the
    per-ticker price cache (historical_data_cache/{TICKER}.parquet).

    Returns (files_written, files_skipped).
    """
    root  = _symbol_base(ticker)
    today = datetime.now().date()

    if not bus_days:
        return 0, 0

    # Determine which dates still need caching
    dates_needed: set[date] = set()
    for d in bus_days:
        if d > today:
            continue
        if force or not os.path.exists(_options_cache_path(ticker, d.isoformat())):
            dates_needed.add(d)

    skipped = len(bus_days) - len(dates_needed)
    if not dates_needed:
        return 0, skipped

    # Get all available expirations
    try:
        all_exps = _get_expirations(root)
    except requests.HTTPError as e:
        if e.response is not None and e.response.status_code == 429:
            logging.warning(f"  {root}: skipped (rate limited on expirations)")
        else:
            logging.warning(f"  {root}: cannot get expirations — {e}")
        return 0, skipped
    except Exception as e:
        logging.warning(f"  {root}: cannot get expirations — {e}")
        return 0, skipped

    start_date = min(dates_needed)
    end_date   = max(dates_needed)

    # Keep expirations that fall within the DTE window for at least one date in our range
    exp_lo = start_date + timedelta(days=target_dte - dte_tol)
    exp_hi = end_date   + timedelta(days=target_dte + dte_tol)
    relevant_exps = [e for e in all_exps if exp_lo <= e <= exp_hi]

    if not relevant_exps:
        sample = sorted(all_exps)[:5] if all_exps else []
        logging.info(
            f"  {root}: no expirations in DTE window (exp_lo=%s exp_hi=%s, got %s total; sample=%s)",
            exp_lo, exp_hi, len(all_exps), sample,
        )
        return 0, skipped

    # Load price df for Black-Scholes underlying (optional — greeks will be NaN if missing)
    price_df = _load_price_df(root)

    # Separate today vs historical dates
    hist_dates: set[date] = {d for d in dates_needed if d < today}
    today_needed = today in dates_needed

    # Split historical dates into contiguous clusters to avoid huge EOD ranges that span
    # holidays (e.g. Jan 1 being a weekday but a market holiday can pull the range all the
    # way back to Jan 1 even when only a few recent days are truly missing).
    def _clusters(ds: set[date]) -> List[Tuple[date, date]]:
        """Return (start, end) pairs for contiguous date groups (gap ≤ 7 calendar days)."""
        if not ds:
            return []
        sorted_ds = sorted(ds)
        clusters: List[Tuple[date, date]] = []
        cs = ce = sorted_ds[0]
        for d in sorted_ds[1:]:
            if (d - ce).days <= 7:
                ce = d
            else:
                clusters.append((cs, ce))
                cs = ce = d
        clusters.append((cs, ce))
        return clusters

    hist_clusters = _clusters(hist_dates)

    # Collect rows grouped by trade-date across all expirations
    date_rows: Dict[date, List[dict]] = {}

    for exp in relevant_exps:
        exp_str = exp.strftime("%Y%m%d")

        # ── Historical dates via EOD (one call per contiguous cluster) ─────────
        for cluster_start, cluster_end in hist_clusters:
            cluster_dates = {d for d in hist_dates if cluster_start <= d <= cluster_end}
            try:
                data = _theta_get(
                    "/v3/option/history/eod",
                    {
                        "symbol":     root.upper(),
                        "expiration": exp_str,
                        "start_date": cluster_start.strftime("%Y-%m-%d"),
                        "end_date":   cluster_end.strftime("%Y-%m-%d"),
                        "format":     "json",
                    },
                    timeout=120,
                )
                for obj in data.get("response", []):
                    contract = obj.get("contract", {})
                    strike   = float(contract.get("strike", 0))
                    right    = contract.get("right", "")
                    for entry in obj.get("data", []):
                        created = entry.get("created", "")
                        try:
                            trade_date = datetime.strptime(created[:10], "%Y-%m-%d").date()
                        except (ValueError, TypeError):
                            continue
                        if trade_date not in cluster_dates:
                            continue
                        _add_row(date_rows, trade_date, exp, strike, right,
                                 entry.get("bid", 0), entry.get("ask", 0), price_df)
            except requests.HTTPError as e:
                logging.warning(f"  {root} exp={exp_str} eod: HTTP {e.response.status_code if e.response is not None else '?'}")
            except Exception as e:
                logging.warning(f"  {root} exp={exp_str} eod: {e}")

        # ── Today via live snapshot ────────────────────────────────────────────
        if today_needed:
            try:
                data = _theta_get(
                    "/v3/option/snapshot/quote",
                    {
                        "symbol":     root.upper(),
                        "expiration": exp_str,
                        "format":     "json",
                    },
                    timeout=30,
                )
                for obj in data.get("response", []):
                    contract  = obj.get("contract", {})
                    strike    = float(contract.get("strike", 0))
                    right     = contract.get("right", "")
                    tick_list = obj.get("data", [])
                    if not tick_list:
                        continue
                    tick = tick_list[0]
                    _add_row(date_rows, today, exp, strike, right,
                             tick.get("bid", 0), tick.get("ask", 0), price_df)
            except requests.HTTPError as e:
                logging.warning(f"  {root} exp={exp_str} snapshot: HTTP {e.response.status_code if e.response is not None else '?'}")
            except Exception as e:
                logging.warning(f"  {root} exp={exp_str} snapshot: {e}")

        time.sleep(REQUEST_DELAY_SEC)

    # Write one cache file per date (rows with data)
    written = 0
    for d, rows in date_rows.items():
        if not rows:
            continue
        run_date   = d.isoformat()
        cache_path = _options_cache_path(ticker, run_date)
        std_rows   = _rows_to_standard(rows, root, run_date)
        try:
            _write_gz_json(cache_path, {
                "underlying": root,
                "trade_date": run_date,
                "rows":       std_rows,
            })
            written += 1
        except Exception as e:
            logging.warning(f"  Failed writing {cache_path}: {e}")

    # Write empty sentinel files for historical dates that returned no data at all.
    # This prevents re-fetching them on the next sync (market holidays, etc.).
    dates_with_data = set(date_rows.keys())
    for d in hist_dates:
        if d not in dates_with_data and not os.path.exists(_options_cache_path(ticker, d.isoformat())):
            try:
                _write_empty_sentinel(ticker, d)
            except Exception:
                pass  # best-effort; not critical

    return written, skipped


# =========================
# Main
# =========================

def main() -> None:
    ap = argparse.ArgumentParser(
        description="Prefetch ThetaData option chains for optsptheta.py / backtest"
    )

    date_group = ap.add_mutually_exclusive_group(required=True)
    date_group.add_argument(
        "--years", nargs="+", type=int,
        help="Calendar years to prefetch, e.g. --years 2024 2025"
    )
    date_group.add_argument(
        "--today", action="store_true",
        help="Prefetch today's data only (uses live snapshot)"
    )
    date_group.add_argument(
        "--start-date", type=str, metavar="YYYY-MM-DD",
        help="Start of custom date range (requires --end-date)"
    )

    ap.add_argument(
        "--end-date", type=str, metavar="YYYY-MM-DD",
        help="End of custom date range (used with --start-date)"
    )
    ap.add_argument(
        "--tickers", nargs="+", default=None,
        help="Override ticker list (default: sp500_symbols.csv)"
    )
    ap.add_argument(
        "--force", action="store_true",
        help="Re-fetch and overwrite existing cache files"
    )
    ap.add_argument(
        "--workers",
        type=int,
        default=4,
        help="Parallel ticker workers (default 4; set 1 for sequential)",
    )
    ap.add_argument("--target-dte", type=int, default=TARGET_DTE)
    ap.add_argument("--dte-tol",    type=int, default=DTE_TOLERANCE_DAYS)

    args = ap.parse_args()
    today = datetime.now().date()

    logging.info("Ensure ThetaTerminal is updated to the latest version for v3 compatibility.")

    # Resolve date range
    if args.today:
        start_date = end_date = today
    elif args.start_date:
        if not args.end_date:
            ap.error("--start-date requires --end-date")
        start_date = datetime.strptime(args.start_date, "%Y-%m-%d").date()
        end_date   = datetime.strptime(args.end_date,   "%Y-%m-%d").date()
    else:
        years      = sorted(set(args.years))
        start_date = date(min(years), 1, 1)
        end_date   = date(max(years), 12, 31)

    if end_date > today:
        end_date = today
    if start_date > end_date:
        logging.error("start_date is after end_date.")
        return

    bus_days = generate_business_days(start_date, end_date)
    if not bus_days:
        logging.error("No business days in the requested range.")
        return

    # Load tickers
    if args.tickers:
        tickers = [_symbol_base(t) for t in args.tickers]
    else:
        try:
            tickers = _read_tickers_from_csv(SP500_SYMBOLS_CSV)
            if not tickers:
                raise ValueError("CSV returned no tickers")
            logging.info(f"Loaded {len(tickers)} tickers from {SP500_SYMBOLS_CSV}")
        except Exception as e:
            logging.warning(f"Could not read tickers CSV: {e}. Using defaults.")
            tickers = DEFAULT_TICKERS

    workers = max(1, min(args.workers, len(tickers)))
    logging.info(
        f"Date range : {start_date} → {end_date}  ({len(bus_days)} business days)"
    )
    logging.info(f"Tickers    : {len(tickers)}")
    logging.info(f"Workers    : {workers}")
    logging.info(f"Target DTE : {args.target_dte} ± {args.dte_tol} days")
    logging.info(f"Force      : {args.force}")

    total_written = total_skipped = 0
    completed = 0

    def _run_one(ticker: str) -> Tuple[str, int, int]:
        try:
            w, s = prefetch_ticker(
                ticker     = ticker,
                bus_days   = bus_days,
                force      = args.force,
                target_dte = args.target_dte,
                dte_tol    = args.dte_tol,
            )
            return ticker, w, s
        except Exception as exc:
            logging.warning(f"  {ticker}: {exc}")
            return ticker, 0, 0

    if workers == 1:
        for i, ticker in enumerate(tickers, 1):
            logging.info(f"[{i}/{len(tickers)}] {ticker}")
            _, written, skipped = _run_one(ticker)
            total_written  += written
            total_skipped  += skipped
            if written:
                logging.info(f"  → wrote {written} cache files  (skipped {skipped})")
            else:
                logging.info(f"  → nothing new  (skipped {skipped})")
    else:
        with ThreadPoolExecutor(max_workers=workers) as ex:
            futures = {ex.submit(_run_one, t): t for t in tickers}
            for fut in as_completed(futures):
                completed += 1
                ticker, written, skipped = fut.result()
                total_written  += written
                total_skipped  += skipped
                if written:
                    logging.info(f"[{completed}/{len(tickers)}] {ticker} → wrote {written}  (skipped {skipped})")
                else:
                    logging.info(f"[{completed}/{len(tickers)}] {ticker} → nothing new  (skipped {skipped})")

    logging.info(
        f"Finished.  Written: {total_written}  |  Skipped (already cached): {total_skipped}"
    )

    # Exit non-zero only when nothing was written AND nothing was skipped (real failure).
    # "Written: 0" with Skipped > 0 is success: data was already cached and up to date.
    if total_written == 0 and total_skipped == 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
