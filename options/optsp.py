#!/usr/bin/env python3
"""
optsp.py (Updated) — Iron Condor recommender using EODHD UnicornBay Marketplace Options EOD (cached)

Update (requested):
- Max 10 recommendations per day (Top 10 by score across ALL tickers).

Key behavior:
- For each ticker: build candidate iron condors (within DTE window, width bounds, credit+POP filters).
- Collect top candidates per ticker, then rank globally and keep ONLY Top-N (default N=10, hard-capped at 10).
- Compatible with your backtest_optsp_system.py call style: pick_iron_condors(chain) works (ticker/run_date inferred).

Dependencies:
  pip install requests pandas numpy scipy openpyxl pyarrow

Env:
  export EODHD_API_TOKEN="..."
  export STOCK_BASE_DIR="/path/to/OptionSys"
"""

from __future__ import annotations

import os
import time
import json
import gzip
import math
import argparse
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, date
from typing import Any, Dict, List, Optional, Tuple

import requests
import numpy as np
import pandas as pd
from scipy.stats import lognorm

# =========================
# BASE / PATHS
# =========================
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.environ.get("STOCK_BASE_DIR") or SCRIPT_DIR

# =========================
# CONFIG (Defaults)
# =========================
EODHD_BASE = os.environ.get("EODHD_BASE") or "https://eodhd.com"
EODHD_API_TOKEN = os.environ.get("EODHD_API_TOKEN") or os.environ.get("EODHD_API_KEY") or ""

SP500_SYMBOLS_CSV = os.path.join(BASE_DIR, "sp500_symbols.csv")

# Strategy params
TARGET_DTE = 45
DTE_TOLERANCE_DAYS = 12
DELTA_TARGET = 0.15
WIDTH_BOUNDS = (1, 25)
MIN_NET_CREDIT = 0.15
MIN_POP = 0.60


# --- Optional robustness filters (OFF by default) ---
# These filters are useful to diagnose weak years (like 2024), but they can also reduce performance
# if the thresholds are too aggressive. לכן הם כבויים כברירת מחדל.
ENABLE_MARKET_FILTER_DEFAULT = False
VIX_MAX_DEFAULT = float(os.environ.get("OPTSP_VIX_MAX", "22"))          # if VIX close > this => (optional) skip trading day
SPY_MA_DAYS_DEFAULT = int(os.environ.get("OPTSP_SPY_MA_DAYS", "200"))   # if SPY close < SMA => (optional) skip trading day

# Reward/Risk sanity filter (OFF by default; set >0 to enable)
MIN_CREDIT_TO_WIDTH = float(os.environ.get("OPTSP_MIN_CREDIT_TO_WIDTH", "0.0"))  # net_credit/width must be >= this

# Liquidity filters (NOTE: UnicornBay fields can be sparse; keep these mild)
MIN_OPEN_INTEREST = 0   # was 200
MIN_VOLUME = 0          # was 5
MAX_SPREAD_PCT = 0.40   # was 0.25
MAX_SPREAD_ABS = 1.00   # was 0.50
MIN_MID = 0.03          # was 0.05

# Risk management
ACCOUNT_SIZE_USD = 250_000.0
RISK_PER_TRADE_PCT = 0.0075

# Daily output cap
DAILY_MAX_RECS_HARDCAP = 10  # requested: max 10 per day

# Candidate limits
MAX_CANDIDATES_PER_TICKER = 10   # keep a small pool per ticker
TOP_CANDIDATES_TO_COLLECT_PER_TICKER = 2  # collect top K per ticker then global rank

# API / networking
REQUEST_TIMEOUT = 30
MAX_RETRIES = 10
BASE_BACKOFF_SEC = 2.0
MIN_SECONDS_BETWEEN_REQUESTS = 0.8

# Output
OUTPUT_DIR = os.path.join(BASE_DIR, "options_recommendations")
OUTPUT_PREFIX = "iron_condor"

# Cache dirs (matches your prefetch style)
HISTORICAL_CACHE_DIR = os.path.join(BASE_DIR, "historical_data_cache")
OPTIONS_CACHE_DIR = os.path.join(BASE_DIR, "options_data_cache", "unicornbay", "options_eod")

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


# =========================
# Helpers
# =========================
def _symbol_base(symbol: str) -> str:
    return str(symbol).upper().replace(".US", "").replace(".INDX", "").strip()

def _symbol_api(symbol: str) -> str:
    sym = str(symbol).upper().strip()
    if "." in sym:
        return sym
    return f"{sym}.US"

def previous_business_day(d: date) -> date:
    """Return the most recent business day strictly before d."""
    dd = d - timedelta(days=1)  # always step back at least one day
    while dd.weekday() >= 5:    # skip Saturday (5) and Sunday (6)
        dd -= timedelta(days=1)
    return dd

def determine_run_date() -> str:
    today = datetime.now().date()
    d = previous_business_day(today)
    return d.isoformat()

def _read_tickers_from_csv(path: str) -> List[str]:
    if not os.path.exists(path):
        logging.warning(f"Tickers CSV not found: {path}")
        return []
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
    tickers: List[str] = []
    for x in ser.astype(str).tolist():
        t = x.strip().upper()
        if t and t != "NAN":
            tickers.append(_symbol_base(t))
    # dedupe preserve order
    seen = set()
    out: List[str] = []
    for t in tickers:
        if t not in seen:
            seen.add(t)
            out.append(t)
    return out


# =========================
# Rate limiter + requests
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
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            rate_limiter.wait()
            resp = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
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
    raise RuntimeError(f"Failed after {MAX_RETRIES} attempts: {url}")


# =========================
# Cache IO
# =========================
def _options_cache_path(ticker: str, trade_date: str) -> str:
    return os.path.join(OPTIONS_CACHE_DIR, _symbol_base(ticker), f"{trade_date}.json.gz")


def _find_most_recent_cache(ticker: str, before_date: str, max_lookback_days: int = 7) -> Optional[str]:
    """
    Find the most recent cached options file for a ticker that has real data (rows > 0),
    searching backwards from before_date up to max_lookback_days.
    Returns the date string (YYYY-MM-DD) of the best available cache, or None.
    """
    d0 = datetime.fromisoformat(before_date).date()
    for offset in range(1, max_lookback_days + 1):
        candidate_date = (d0 - timedelta(days=offset)).isoformat()
        path = _options_cache_path(ticker, candidate_date)
        if os.path.exists(path):
            try:
                obj = _read_gz_json(path)
                rows = obj.get("rows") if isinstance(obj, dict) else None
                if rows:
                    return candidate_date
            except Exception:
                continue
    return None

def _read_gz_json(path: str) -> Any:
    with gzip.open(path, "rt", encoding="utf-8") as f:
        return json.load(f)

def _write_gz_json(path: str, obj: Any) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with gzip.open(path, "wt", encoding="utf-8") as f:
        json.dump(obj, f)

def load_price_df_cached(ticker: str) -> Optional[pd.DataFrame]:
    candidates = [
        os.path.join(HISTORICAL_CACHE_DIR, f"{_symbol_base(ticker)}.parquet"),
        os.path.join(HISTORICAL_CACHE_DIR, f"{_symbol_api(ticker)}.parquet"),
    ]
    p = next((x for x in candidates if os.path.exists(x)), None)
    if not p:
        return None
    try:
        df = pd.read_parquet(p)
        if not isinstance(df.index, pd.DatetimeIndex):
            if "date" in df.columns:
                df["date"] = pd.to_datetime(df["date"])
                df = df.set_index("date")
            else:
                df.index = pd.to_datetime(df.index)
        return df.sort_index()
    except Exception as e:
        logging.warning(f"Failed reading cached price for {ticker}: {e}")
        return None

def get_close_on_date(price_df: pd.DataFrame, d: str) -> Optional[float]:
    if price_df is None or price_df.empty:
        return None
    ts = pd.to_datetime(d)

    # exact
    try:
        row = price_df.loc[ts]
        if isinstance(row, pd.Series):
            for c in ("close", "Close", "adj_close", "Adj Close", "adjusted_close"):
                if c in row.index and pd.notna(row[c]):
                    return float(row[c])
    except Exception:
        pass

    # previous available
    try:
        idx = price_df.index
        idx = idx[idx <= ts]
        if len(idx) == 0:
            return None
        r = price_df.loc[idx[-1]]
        if isinstance(r, pd.Series):
            for c in ("close", "Close", "adj_close", "Adj Close"):
                if c in r.index and pd.notna(r[c]):
                    return float(r[c])
    except Exception:
        return None
    return None


def _compute_sma(price_df: pd.DataFrame, asof: str, window: int) -> Optional[float]:
    if price_df is None or price_df.empty:
        return None
    ts = pd.to_datetime(asof)
    df = price_df.copy()
    close_col = None
    for c in ("close", "Close", "adj_close", "Adj Close", "adjusted_close"):
        if c in df.columns:
            close_col = c
            break
    if close_col is None:
        return None
    df = df.sort_index()
    df2 = df[df.index <= ts]
    s = pd.to_numeric(df2[close_col], errors="coerce").dropna()
    if len(s) < max(30, window // 3):
        return None
    window = min(window, len(s))
    if window <= 1:
        return None
    return float(s.tail(window).mean())

def _market_filter_ok(run_date: str, vix_max: float, spy_ma_days: int) -> Tuple[bool, str]:
    """Return (ok, reason). Uses cached historical_data_cache for SPY + VIX."""
    # SPY trend filter
    spy_df = load_price_df_cached("SPY")
    if spy_df is None or spy_df.empty:
        return True, "no_spy_cache"
    spy_close = get_close_on_date(spy_df, run_date)
    spy_sma = _compute_sma(spy_df, run_date, int(spy_ma_days))
    if spy_close is not None and spy_sma is not None and spy_close < spy_sma:
        return False, f"spy_below_sma{spy_ma_days}"

    # VIX filter
    vix_df = load_price_df_cached("VIX.INDX")
    if vix_df is None or vix_df.empty:
        vix_df = load_price_df_cached("VIX")
    if vix_df is None or vix_df.empty:
        return True, "no_vix_cache"
    vix_close = get_close_on_date(vix_df, run_date)
    if vix_close is not None and float(vix_close) > float(vix_max):
        return False, f"vix_above_{vix_max}"
    return True, "ok"


# =========================
# UnicornBay Options API
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
        if not links.get("next"):
            break
        if len(data) < limit:
            break

        offset += limit
        if offset > 200_000:
            break

    return rows

def flatten_options_rows(rows: List[Dict[str, Any]]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows).copy()
    for c in ["strike", "bid", "ask", "last", "volume", "open_interest", "implied_volatility", "delta"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df

def fetch_chain_for_ticker(
    api_token: str,
    ticker: str,
    run_date: str,
    use_cache: bool = True,
    write_cache: bool = True,
    target_dte: int = TARGET_DTE,
    dte_tol: int = DTE_TOLERANCE_DAYS,
) -> pd.DataFrame:
    cache_path = _options_cache_path(ticker, run_date)
    if use_cache:
        # Try today's cache first
        if os.path.exists(cache_path):
            try:
                obj = _read_gz_json(cache_path)
                rows = obj.get("rows") if isinstance(obj, dict) else None
                if isinstance(rows, list) and rows:  # non-empty rows
                    df = flatten_options_rows(rows)
                    if not df.empty:
                        df["__ticker"] = _symbol_base(ticker)
                        df["__run_date"] = run_date
                    return df
            except Exception as e:
                logging.warning(f"Cache read failed: {cache_path} err={e}")

        # Cache missing or empty — fall back to most recent available cache date
        fallback_date = _find_most_recent_cache(ticker, before_date=run_date)
        if fallback_date:
            fallback_path = _options_cache_path(ticker, fallback_date)
            try:
                obj = _read_gz_json(fallback_path)
                rows = obj.get("rows") if isinstance(obj, dict) else None
                if isinstance(rows, list) and rows:
                    df = flatten_options_rows(rows)
                    if not df.empty:
                        df["__ticker"] = _symbol_base(ticker)
                        df["__run_date"] = run_date  # keep the requested trade date for DTE calcs
                    return df
            except Exception as e:
                logging.warning(f"Fallback cache read failed: {fallback_path} err={e}")

    d0 = datetime.fromisoformat(run_date).date()
    exp_from = (d0 + timedelta(days=max(1, target_dte - dte_tol))).isoformat()
    exp_to = (d0 + timedelta(days=target_dte + dte_tol)).isoformat()

    rows = eodhd_unicornbay_options_eod(
        api_token=api_token,
        underlying=ticker,
        tradetime_from=run_date,
        tradetime_to=run_date,
        exp_date_from=exp_from,
        exp_date_to=exp_to,
    )

    if write_cache:
        try:
            _write_gz_json(cache_path, {
                "underlying": _symbol_base(ticker),
                "trade_date": run_date,
                "exp_from": exp_from,
                "exp_to": exp_to,
                "rows": rows
            })
        except Exception as e:
            logging.warning(f"Failed writing cache {cache_path}: {e}")

    df = flatten_options_rows(rows)
    if not df.empty:
        df["__ticker"] = _symbol_base(ticker)
        df["__run_date"] = run_date
    return df


# =========================
# Pricing helpers
# =========================
def _mid(bid: float, ask: float) -> float:
    if bid is None or ask is None or np.isnan(bid) or np.isnan(ask):
        return np.nan
    return 0.5 * (bid + ask)

def _spread_ok(bid: float, ask: float) -> bool:
    if bid is None or ask is None or np.isnan(bid) or np.isnan(ask):
        return False
    mid = _mid(bid, ask)
    if np.isnan(mid) or mid <= 0:
        return False
    spr = ask - bid
    if spr < 0:
        return False
    if spr > MAX_SPREAD_ABS:
        return False
    if spr / mid > MAX_SPREAD_PCT:
        return False
    return True

def _estimate_pop_from_delta(short_put_delta: float, short_call_delta: float) -> float:
    if short_put_delta is None or short_call_delta is None:
        return 0.0
    if np.isnan(short_put_delta) or np.isnan(short_call_delta):
        return 0.0
    return float(max(0.0, min(1.0, 1.0 - (abs(short_put_delta) + abs(short_call_delta)))))

def _calc_pop_lognormal(spot: float, low: float, high: float, iv: float, dte: int) -> Optional[float]:
    try:
        if not (spot > 0 and low > 0 and high > 0 and high > low and iv > 0 and dte > 0):
            return None
        T = dte / 365.0
        sigma = iv * math.sqrt(T)
        if sigma <= 0:
            return None
        mu = math.log(spot)
        cdf_high = lognorm.cdf(high, s=sigma, scale=math.exp(mu))
        cdf_low = lognorm.cdf(low, s=sigma, scale=math.exp(mu))
        p = float(cdf_high - cdf_low)
        return max(0.0, min(1.0, p))
    except Exception:
        return None

def _estimate_chain_iv_atm(chain: pd.DataFrame, spot: float, exp: pd.Timestamp) -> Optional[float]:
    if chain is None or chain.empty or spot is None or not (spot > 0):
        return None
    if "implied_volatility" not in chain.columns:
        return None
    df = chain.copy()
    if "_exp" in df.columns:
        df = df[df["_exp"] == exp].copy()
    iv = pd.to_numeric(df["implied_volatility"], errors="coerce")
    df = df.assign(_iv=iv)
    df = df[df["_iv"].notna() & (df["_iv"] > 0)]
    if df.empty:
        return None

    strike = pd.to_numeric(df.get("strike"), errors="coerce")
    df = df.assign(_strike=strike)
    df = df[df["_strike"].notna() & (df["_strike"] > 0)]
    if df.empty:
        return None

    near_atm = df[(df["_strike"] / spot - 1.0).abs() <= 0.03]
    if not near_atm.empty:
        v = float(near_atm["_iv"].median())
        if 0.01 <= v <= 5.0:
            return v

    v = float(df["_iv"].median())
    if 0.01 <= v <= 5.0:
        return v
    return None


# =========================
# Iron Condor selection
# =========================
def _infer_ticker_and_date(chain: pd.DataFrame) -> Tuple[Optional[str], Optional[str]]:
    if chain is None or chain.empty:
        return None, None
    if "__ticker" in chain.columns:
        t = chain["__ticker"].dropna()
        if not t.empty:
            ticker = str(t.iloc[0])
        else:
            ticker = None
    else:
        ticker = None

    if "__run_date" in chain.columns:
        d = chain["__run_date"].dropna()
        if not d.empty:
            run_date = str(d.iloc[0])
        else:
            run_date = None
    else:
        run_date = None

    return ticker, run_date

def pick_best_expiration(df: pd.DataFrame, run_date: str, target_dte: int, dte_tol: int) -> Optional[pd.Timestamp]:
    if df is None or df.empty or "_exp" not in df.columns:
        return None
    d0 = pd.to_datetime(run_date)
    exps = df["_exp"].dropna().unique()
    if len(exps) == 0:
        return None

    scored = []
    for e in exps:
        e = pd.to_datetime(e)
        dte = int((e - d0).days)
        if dte < 1:
            continue
        if abs(dte - target_dte) > dte_tol:
            continue
        cnt = int((df["_exp"] == e).sum())
        scored.append((abs(dte - target_dte), -cnt, e))
    if not scored:
        return None
    scored.sort()
    return scored[0][2]

def pick_iron_condors(
    chain: pd.DataFrame,
    ticker: Optional[str] = None,
    run_date: Optional[str] = None,
    target_dte: int = TARGET_DTE,
    dte_tol: int = DTE_TOLERANCE_DAYS,
    delta_target: float = DELTA_TARGET,
    min_pop: float = MIN_POP,
    min_credit: float = MIN_NET_CREDIT,
) -> pd.DataFrame:
    """
    Backtest-compatible:
      - pick_iron_condors(chain) works (ticker/run_date inferred if possible).
      - pick_iron_condors(chain, ticker, run_date, ...) also supported.
    """
    if chain is None or chain.empty:
        return pd.DataFrame()

    if ticker is None or run_date is None:
        t2, d2 = _infer_ticker_and_date(chain)
        ticker = ticker or t2
        run_date = run_date or d2

    if not ticker or not run_date:
        # Cannot compute DTE/spot/expiry window reliably
        return pd.DataFrame()

    df = chain.copy()

    # Normalize columns
    colmap = {c.lower(): c for c in df.columns}
    def pick_col(*cands):
        for c in cands:
            if c in colmap:
                return colmap[c]
        return None

    c_type = pick_col("type", "option_type", "right")
    c_strike = pick_col("strike")
    c_bid = pick_col("bid")
    c_ask = pick_col("ask")
    c_delta = pick_col("delta")
    c_exp = pick_col("exp_date", "expiration", "expiry", "exp")

    if not all([c_type, c_strike, c_bid, c_ask, c_delta, c_exp]):
        return pd.DataFrame()

    # normalize type
    tser = df[c_type].astype(str).str.lower()
    tser = tser.replace({"c": "call", "p": "put"})
    df["_type"] = tser

    df["_mid"] = [_mid(float(b) if pd.notna(b) else np.nan, float(a) if pd.notna(a) else np.nan)
                  for b, a in zip(pd.to_numeric(df[c_bid], errors="coerce"), pd.to_numeric(df[c_ask], errors="coerce"))]
    df[c_strike] = pd.to_numeric(df[c_strike], errors="coerce")
    df[c_delta] = pd.to_numeric(df[c_delta], errors="coerce")

    # Liquidity filters (mild)
    c_oi = pick_col("open_interest", "openinterest", "oi")
    c_vol = pick_col("volume", "vol")
    if c_oi and c_oi in df.columns and MIN_OPEN_INTEREST > 0:
        df[c_oi] = pd.to_numeric(df[c_oi], errors="coerce")
        df = df[df[c_oi].fillna(0) >= MIN_OPEN_INTEREST]
    if c_vol and c_vol in df.columns and MIN_VOLUME > 0:
        df[c_vol] = pd.to_numeric(df[c_vol], errors="coerce")
        df = df[df[c_vol].fillna(0) >= MIN_VOLUME]

    df = df[df["_mid"].fillna(0) >= MIN_MID]

    # Spread filter
    ok = []
    bids = pd.to_numeric(df[c_bid], errors="coerce").to_numpy()
    asks = pd.to_numeric(df[c_ask], errors="coerce").to_numpy()
    for b, a in zip(bids, asks):
        ok.append(_spread_ok(float(b) if pd.notna(b) else np.nan, float(a) if pd.notna(a) else np.nan))
    df = df[np.array(ok, dtype=bool)]
    if df.empty:
        return pd.DataFrame()

    df["_exp"] = pd.to_datetime(df[c_exp], errors="coerce")
    df = df.dropna(subset=["_exp"])
    if df.empty:
        return pd.DataFrame()

    best_exp = pick_best_expiration(df, run_date, target_dte, dte_tol)
    if best_exp is None:
        return pd.DataFrame()
    df = df[df["_exp"] == best_exp].copy()

    calls = df[df["_type"] == "call"].copy()
    puts = df[df["_type"] == "put"].copy()
    if calls.empty or puts.empty:
        return pd.DataFrame()

    puts["delta_dist"] = (puts[c_delta] + float(delta_target)).abs()
    calls["delta_dist"] = (calls[c_delta] - float(delta_target)).abs()

    short_puts = puts.sort_values("delta_dist").head(25)
    short_calls = calls.sort_values("delta_dist").head(25)
    if short_puts.empty or short_calls.empty:
        return pd.DataFrame()

    widths = list(range(int(WIDTH_BOUNDS[0]), int(WIDTH_BOUNDS[1]) + 1))

    # Spot + IV estimate for lognormal POP
    spot = None
    price_df = load_price_df_cached(ticker)
    if price_df is not None:
        spot = get_close_on_date(price_df, run_date)

    dte = int((pd.to_datetime(best_exp) - pd.to_datetime(run_date)).days)
    iv_est = _estimate_chain_iv_atm(df, spot, best_exp) if spot is not None else None

    candidates = []
    for _, sp in short_puts.iterrows():
        spk = float(sp[c_strike])
        for w in widths:
            lpk = spk - w
            lp_rows = puts[np.isclose(puts[c_strike].astype(float), lpk)]
            if lp_rows.empty:
                continue
            lp = lp_rows.iloc[0]

            for _, sc in short_calls.iterrows():
                sck = float(sc[c_strike])
                lck = sck + w
                lc_rows = calls[np.isclose(calls[c_strike].astype(float), lck)]
                if lc_rows.empty:
                    continue
                lc = lc_rows.iloc[0]

                credit = float(sp["_mid"]) - float(lp["_mid"]) + float(sc["_mid"]) - float(lc["_mid"])
                if credit < float(min_credit):
                    continue

                # Optional reward/risk sanity filter
                if float(MIN_CREDIT_TO_WIDTH) > 0 and float(w) > 0:
                    if (float(credit) / float(w)) < float(MIN_CREDIT_TO_WIDTH):
                        continue

                pop_ln = None
                if spot is not None and iv_est is not None:
                    pop_ln = _calc_pop_lognormal(spot=spot, low=spk, high=sck, iv=iv_est, dte=dte)
                pop = pop_ln if pop_ln is not None else _estimate_pop_from_delta(float(sp[c_delta]), float(sc[c_delta]))
                if pop < float(min_pop):
                    continue

                max_loss = float(w) - float(credit)  # per share
                if max_loss <= 0:
                    continue

                                # Score: balance POP and credit, normalized by width (safer default)
                score = float(pop) * float(credit) / max(float(w), 1e-6)

                candidates.append({
                    "ticker": _symbol_base(ticker),
                    "run_date": run_date,
                    "exp": pd.to_datetime(best_exp).date().isoformat(),
                    "dte": int(dte),
                    "short_put": float(spk),
                    "long_put": float(lp[c_strike]),
                    "short_call": float(sck),
                    "long_call": float(lc[c_strike]),
                    "width": float(w),
                    "net_credit": float(credit),
                    "max_loss_per_share": float(max_loss),
                    "pop_est": float(pop),
                    "pop_method": "lognormal" if pop_ln is not None else "delta_proxy",
                    "spot": float(spot) if spot is not None else np.nan,
                    "iv_est": float(iv_est) if iv_est is not None else np.nan,
                    "sp_delta": float(sp[c_delta]),
                    "sc_delta": float(sc[c_delta]),
                    "score": float(score),
                })

    if not candidates:
        return pd.DataFrame()

    out = pd.DataFrame(candidates)
    out = out.sort_values(["score", "pop_est", "net_credit"], ascending=[False, False, False]).head(MAX_CANDIDATES_PER_TICKER).reset_index(drop=True)
    return out


def size_contracts(max_loss_per_share: float, account_size: float, risk_pct: float) -> int:
    if max_loss_per_share is None or not (max_loss_per_share > 0):
        return 1
    risk_budget = float(account_size) * float(risk_pct)
    per_contract_risk = float(max_loss_per_share) * 100.0
    if per_contract_risk <= 0:
        return 1
    n = int(math.floor(risk_budget / per_contract_risk))
    return max(1, n)


def main() -> None:
    global MIN_CREDIT_TO_WIDTH
    ap = argparse.ArgumentParser(description="Iron Condor recommender (Top-10 per day across all tickers)")
    ap.add_argument("--csv", type=str, default=SP500_SYMBOLS_CSV, help="Tickers CSV (default: sp500_symbols.csv)")
    ap.add_argument("--tickers", nargs="*", default=None, help="Override tickers list")
    ap.add_argument("--run-date", type=str, default=None, help="Trade date YYYY-MM-DD (default: last business day)")
    ap.add_argument("--outdir", type=str, default=OUTPUT_DIR)
    ap.add_argument("--use-cache", action="store_true", default=True)
    ap.add_argument("--no-cache", dest="use_cache", action="store_false")
    ap.add_argument("--target-dte", type=int, default=TARGET_DTE)
    ap.add_argument("--dte-tol", type=int, default=DTE_TOLERANCE_DAYS)
    ap.add_argument("--delta", type=float, default=DELTA_TARGET)
    ap.add_argument("--min-pop", type=float, default=MIN_POP)
    ap.add_argument("--min-credit", type=float, default=MIN_NET_CREDIT)
    ap.add_argument("--max-trades", type=int, default=10, help="Max recommendations per day (hard-capped at 10).")
    ap.add_argument("--enable-market-filter", action="store_true", help="Enable SPY/VIX regime filter (OFF by default).")
    ap.add_argument("--vix-max", type=float, default=VIX_MAX_DEFAULT, help="If enabled: skip day if VIX close > this (default 22).")
    ap.add_argument("--spy-ma-days", type=int, default=SPY_MA_DAYS_DEFAULT, help="If enabled: skip day if SPY close < SMA (default 200).")
    ap.add_argument("--min-credit-width", type=float, default=MIN_CREDIT_TO_WIDTH, help="Optional: require net_credit/width >= this (0=disabled).")
    ap.add_argument("--account", type=float, default=ACCOUNT_SIZE_USD)
    ap.add_argument("--risk-pct", type=float, default=RISK_PER_TRADE_PCT)
    args = ap.parse_args()

    if not EODHD_API_TOKEN:
        logging.error("Missing EODHD_API_TOKEN (or EODHD_API_KEY). Please export it first.")
        return

    run_date = args.run_date or determine_run_date()
    tickers = args.tickers if args.tickers else _read_tickers_from_csv(args.csv)
    if not tickers:
        logging.error("No tickers.")
        return

    os.makedirs(args.outdir, exist_ok=True)

    # Market regime filter (optional, OFF by default)
    if args.enable_market_filter or ENABLE_MARKET_FILTER_DEFAULT:
        ok, reason = _market_filter_ok(run_date, float(args.vix_max), int(args.spy_ma_days))
        if not ok:
            logging.warning(f"Market filter blocked trading for {run_date}: {reason}. No recommendations produced.")
            return
        logging.info(f"Market filter OK for {run_date}: {reason}")

    # Override optional reward/risk threshold from CLI
    try:
        MIN_CREDIT_TO_WIDTH = float(args.min_credit_width)
    except Exception:
        pass

    # Collect candidates across all tickers, then choose Top-N globally
    all_cands = []

    for i, t in enumerate(tickers, 1):
        logging.info(f"[{i}/{len(tickers)}] {t} date={run_date}")
        try:
            chain = fetch_chain_for_ticker(
                EODHD_API_TOKEN, t, run_date,
                use_cache=args.use_cache, write_cache=True,
                target_dte=int(args.target_dte), dte_tol=int(args.dte_tol)
            )
            if chain is None or chain.empty:
                continue

            condors = pick_iron_condors(
                chain=chain,
                ticker=_symbol_base(t),
                run_date=run_date,
                target_dte=int(args.target_dte),
                dte_tol=int(args.dte_tol),
                delta_target=float(args.delta),
                min_pop=float(args.min_pop),
                min_credit=float(args.min_credit),
            )
            if condors is None or condors.empty:
                continue

            # keep top K from this ticker, then global rank
            all_cands.append(condors.head(TOP_CANDIDATES_TO_COLLECT_PER_TICKER))

        except Exception as e:
            logging.warning(f"{t}: error {e}")

    if not all_cands:
        logging.info("No recommendations.")
        return

    df = pd.concat(all_cands, ignore_index=True)
    df = df.sort_values(["score", "pop_est", "net_credit"], ascending=[False, False, False]).reset_index(drop=True)

    max_trades = min(int(args.max_trades), DAILY_MAX_RECS_HARDCAP)
    df = df.head(max_trades).copy()

    # sizing
    df["contracts"] = df["max_loss_per_share"].apply(lambda x: size_contracts(x, float(args.account), float(args.risk_pct)))
    df["max_risk_usd"] = df["max_loss_per_share"] * 100.0 * df["contracts"]
    df["max_profit_usd"] = df["net_credit"] * 100.0 * df["contracts"]

    out_csv = os.path.join(args.outdir, f"{OUTPUT_PREFIX}_{run_date}.csv")
    df.to_csv(out_csv, index=False)

    out_xlsx = os.path.join(args.outdir, f"{OUTPUT_PREFIX}_{run_date}.xlsx")
    with pd.ExcelWriter(out_xlsx, engine="openpyxl") as w:
        df.to_excel(w, index=False, sheet_name="Recommendations")

    logging.info(f"Saved: {out_csv}")
    logging.info(f"Saved: {out_xlsx}")
    logging.info(f"Returned {len(df)} recommendations (max {DAILY_MAX_RECS_HARDCAP}).")


if __name__ == "__main__":
    main()
