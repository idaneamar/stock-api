#!/usr/bin/env python3
"""
prefetch_options_datasp.py

Prefetch script to download underlying historical prices and options EOD data from EODHD UnicornBay API
to populate the cache directories used by optsp.py.

- Fetches S&P 500 tickers from sp500_symbols.csv (as in optsp.py).
- Prefetches historical EOD prices for each ticker (bulk) if not cached.
- Prefetches options EOD snapshots for each business day in specified years, for the expiration range around TARGET_DTE (as in optsp.py).
- Uses parallel workers (threads) to speed up.
- On API rate limit (429 without quick retry), stops gracefully. Rerun later to resume from missing caches.
- Skips existing cache files.

Example: caffeinate -i python prefetch_options_datasp.py --years 2024 2025 --workers 4
"""

from __future__ import annotations

import os
import glob
import math
import time
import json
import gzip
import argparse
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, date
from typing import Any, Dict, List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed, wait, FIRST_COMPLETED

import requests
import numpy as np
import pandas as pd

# =========================
# CONFIG (Matched from optsp.py)
# =========================
EODHD_BASE = os.environ.get("EODHD_BASE") or "https://eodhd.com"
EODHD_API_TOKEN = os.environ.get("EODHD_API_TOKEN") or os.environ.get("EODHD_API_KEY") or "681a29e3b6f558.05762306"

DEFAULT_TICKERS = ["AAPL", "MSFT", "GOOG", "AMZN", "NVDA"]
SP500_SYMBOLS_CSV = "sp500_symbols.csv"

TARGET_DTE = 45
DTE_TOLERANCE_DAYS = 12

# API / networking (matched)
REQUEST_TIMEOUT = 30
MAX_RETRIES = 10
BASE_BACKOFF_SEC = 2.0
MIN_SECONDS_BETWEEN_REQUESTS = 0.8

# Cache dirs (matched)
# BASE_DIR uses the directory where this script is located
# Since the script is in /Volumes/Extreme Pro/OptionSys, it will use that location
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
HISTORICAL_CACHE_DIR = os.path.join(BASE_DIR, "historical_data_cache")
OPTIONS_CACHE_DIR = os.path.join(BASE_DIR, "options_data_cache", "unicornbay", "options_eod")

# Logging to console and file
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("prefetch_options_datasp.logs"),
        logging.StreamHandler()
    ]
)

class LimitReachedError(Exception):
    pass

# =========================
# Utility / Dates (matched)
# =========================
def _symbol_base(symbol: str) -> str:
    return symbol.upper().replace(".US", "").replace(".INDX", "")

def _symbol_api(symbol: str) -> str:
    sym = symbol.upper()
    if "." in sym:
        return sym
    return f"{sym}.US"

def is_business_day(d: date) -> bool:
    return d.weekday() < 5  # Skip weekends; holidays will return empty data, but ok

def generate_business_days(start_date: date, end_date: date) -> List[date]:
    days = []
    current = start_date
    while current <= end_date:
        if is_business_day(current):
            days.append(current)
        current += timedelta(days=1)
    return days

# =========================
# Rate limiter + requests (matched, with LimitReachedError)
# =========================
@dataclass
class RateLimiter:
    min_interval_sec: float
    _last_ts: float = 0.0

    def wait(self) -> None:
        now = time.time()
        dt = now - self._last_ts
        if dt < self.min_interval_sec:
            time.sleep(self.min_interval_sec - dt)
        self._last_ts = time.time()

rate_limiter = RateLimiter(MIN_SECONDS_BETWEEN_REQUESTS)

def _req_get(url: str, params: Dict[str, Any]) -> requests.Response:
    last_status = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            rate_limiter.wait()
            resp = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)

            last_status = resp.status_code
            if resp.status_code == 200:
                return resp

            if resp.status_code in (429, 500, 502, 503, 504):
                retry_after = None
                if resp.status_code == 429:
                    ra = resp.headers.get("Retry-After")
                    if ra:
                        try:
                            retry_after = float(ra)
                        except (ValueError, TypeError):
                            retry_after = None
                backoff = retry_after if retry_after is not None else min(BASE_BACKOFF_SEC * (2 ** (attempt - 1)), 60.0)
                logging.warning(f"HTTP {resp.status_code} (attempt {attempt}/{MAX_RETRIES}) backoff={backoff:.1f}s")
                time.sleep(backoff)
                continue

            logging.error(f"HTTP {resp.status_code}: {resp.text[:500]}")
            resp.raise_for_status()

        except requests.RequestException as e:
            backoff = min(BASE_BACKOFF_SEC * (2 ** (attempt - 1)), 60.0)
            logging.warning(f"Request error ({attempt}/{MAX_RETRIES}): {e} backoff={backoff:.1f}s")
            time.sleep(backoff)

    if last_status == 429:
        raise LimitReachedError("API rate limit reached (likely daily limit). Stopping to resume later.")
    raise RuntimeError(f"Failed after {MAX_RETRIES} attempts: {url}")

# =========================
# Cache IO (matched)
# =========================
def _options_cache_path(ticker: str, trade_date: str) -> str:
    return os.path.join(OPTIONS_CACHE_DIR, _symbol_base(ticker), f"{trade_date}.json.gz")

def _write_gz_json(path: str, obj: Any) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with gzip.open(path, "wt", encoding="utf-8") as f:
        json.dump(obj, f)

# =========================
# UnicornBay Options API (matched)
# =========================
def eodhd_unicornbay_options_eod(
    api_token: str,
    underlying: str,
    tradetime_from: str,
    tradetime_to: str,
    exp_date_from: str,
    exp_date_to: str,
) -> List[Dict[str, Any]]:
    url = f"{EODHD_BASE}/api/mp/unicornbay/options/eod"
    rows: List[Dict[str, Any]] = []
    offset = 0
    limit = 1000

    while True:
        params: Dict[str, Any] = {
            "api_token": api_token,
            "filter[underlying_symbol]": _symbol_base(underlying),
            "filter[tradetime_from]": tradetime_from,
            "filter[tradetime_to]": tradetime_to,
            "filter[exp_date_from]": exp_date_from,
            "filter[exp_date_to]": exp_date_to,
            "page[limit]": limit,
            "page[offset]": offset,
            "sort": "-exp_date",
        }
        resp = _req_get(url, params=params)
        payload = resp.json()

        if not isinstance(payload, dict):
            break

        data = payload.get("data") or []
        for item in data:
            attrs = (item or {}).get("attributes") or {}
            if attrs:
                rows.append(attrs)

        links = payload.get("links") or {}
        nxt = links.get("next")
        if not nxt:
            break
        if len(data) < limit:
            break

        offset += limit
        if offset > 200_000:  # safety
            break

    return rows

# =========================
# Fetch options for one (ticker, date) - matched to fetch_chain_for_ticker but no df, just cache write
# =========================
def fetch_and_cache_options(ticker: str, run_date: str) -> None:
    d0 = datetime.fromisoformat(run_date).date()
    exp_from = (d0 + timedelta(days=max(1, TARGET_DTE - DTE_TOLERANCE_DAYS))).isoformat()
    exp_to = (d0 + timedelta(days=TARGET_DTE + DTE_TOLERANCE_DAYS)).isoformat()

    rows = eodhd_unicornbay_options_eod(
        api_token=EODHD_API_TOKEN,
        underlying=ticker,
        tradetime_from=run_date,
        tradetime_to=run_date,
        exp_date_from=exp_from,
        exp_date_to=exp_to,
    )

    cache_path = _options_cache_path(ticker, run_date)
    try:
        _write_gz_json(cache_path, {"underlying": _symbol_base(ticker), "trade_date": run_date, "exp_from": exp_from, "exp_to": exp_to, "rows": rows})
        logging.info(f"Cached options for {ticker} on {run_date}")
    except Exception as e:
        logging.warning(f"Failed writing cache {cache_path}: {e}")

# =========================
# Fetch historical prices for one ticker
# =========================
def fetch_and_cache_historical_prices(ticker: str, from_date: str, to_date: str) -> None:
    url = f"{EODHD_BASE}/api/eod/{_symbol_api(ticker)}"
    params = {"api_token": EODHD_API_TOKEN, "from": from_date, "to": to_date, "fmt": "json"}
    resp = _req_get(url, params)
    arr = resp.json()
    if not arr:
        logging.warning(f"No historical data for {ticker}")
        return
    df = pd.DataFrame(arr)
    df["date"] = pd.to_datetime(df["date"])
    df = df.set_index("date").sort_index()
    p = os.path.join(HISTORICAL_CACHE_DIR, f"{_symbol_base(ticker)}.parquet")
    os.makedirs(os.path.dirname(p), exist_ok=True)
    df.to_parquet(p)
    logging.info(f"Cached historical prices for {ticker}")

# =========================
# Read tickers from CSV (from optsp.py)
# =========================
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
    # dedupe preserve order
    seen = set()
    out = []
    for t in tickers:
        if t not in seen:
            seen.add(t)
            out.append(t)
    return out

# =========================
# Main
# =========================
def main() -> None:
    ap = argparse.ArgumentParser(description="Prefetch options and prices data for optsp.py")
    ap.add_argument("--years", nargs="*", type=int, required=True, help="Years to prefetch (e.g., 2024 2025)")
    ap.add_argument("--workers", type=int, default=4, help="Number of parallel workers (default: 4)")
    args = ap.parse_args()

    years = sorted(set(args.years))
    if not years:
        logging.error("No years specified.")
        return

    min_year = min(years)
    max_year = max(years)
    start_date = date(min_year, 1, 1)
    end_date = date(max_year, 12, 31)
    today = datetime.now().date()
    if end_date > today:
        end_date = today
    bus_days = generate_business_days(start_date, end_date)
    if not bus_days:
        logging.error("No business days in range.")
        return

    # Fetch tickers from CSV (as in optsp.py)
    try:
        tickers = _read_tickers_from_csv(SP500_SYMBOLS_CSV)
        if not tickers:
            raise ValueError("No tickers in CSV")
        logging.info(f"Fetched {len(tickers)} tickers from {SP500_SYMBOLS_CSV}.")
    except Exception as e:
        logging.warning(f"Failed to read tickers from {SP500_SYMBOLS_CSV}: {e}. Falling back to defaults.")
        tickers = DEFAULT_TICKERS

    # Prefetch prices (sequential, since few)
    price_from = date(min_year - 1, 1, 1).isoformat()  # +1 year back
    price_to = end_date.isoformat()
    for ticker in tickers:
        cache_p = os.path.join(HISTORICAL_CACHE_DIR, f"{_symbol_base(ticker)}.parquet")
        if not os.path.exists(cache_p):
            try:
                fetch_and_cache_historical_prices(ticker, price_from, price_to)
            except LimitReachedError:
                logging.error("Limit reached during prices fetch. Stopping.")
                return
            except Exception as e:
                logging.warning(f"Failed prices for {ticker}: {e}")

    def _cache_is_empty(path: str) -> bool:
        """Return True if the cache file has zero option rows (written during market hours before EOD data was available)."""
        try:
            with gzip.open(path, 'rt', encoding='utf-8') as fh:
                d = json.load(fh)
            return len(d.get('rows', [])) == 0
        except Exception:
            return True  # unreadable → treat as missing

    # Collect missing or empty options tasks
    tasks: List[Tuple[str, str]] = []
    for ticker in tickers:
        for d in bus_days:
            if d > today:
                continue
            run_date = d.isoformat()
            cache_path = _options_cache_path(ticker, run_date)
            if not os.path.exists(cache_path) or _cache_is_empty(cache_path):
                tasks.append((ticker, run_date))
    logging.info(f"Found {len(tasks)} missing/empty options caches to fetch.")

    if not tasks:
        logging.info("All caches present.")
        return

    # Parallel fetch options
    processed = 0
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        # Initial submit
        initial_count = min(args.workers, len(tasks))
        futures = {}
        for _ in range(initial_count):
            tk = tasks.pop(0)
            fut = executor.submit(fetch_and_cache_options, *tk)
            futures[fut] = tk

        while futures:
            done, _ = wait(futures, return_when=FIRST_COMPLETED)
            for fut in list(done):
                tk = futures.pop(fut)
                try:
                    fut.result()
                    processed += 1
                except LimitReachedError:
                    logging.error(f"Limit reached on {tk}. Stopping prefetch.")
                    return
                except Exception as e:
                    logging.warning(f"Error on {tk}: {e}")

                if tasks:
                    new_tk = tasks.pop(0)
                    new_fut = executor.submit(fetch_and_cache_options, *new_tk)
                    futures[new_fut] = new_tk

    logging.info(f"Processed {processed} options caches.")

if __name__ == "__main__":
    main()