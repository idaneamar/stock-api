#!/usr/bin/env python3
"""
optsptheta.py — Iron Condor recommender (ThetaData edition)

Identical analysis logic to optsp.py; differs only in the data source:
- Uses ThetaData v3 Terminal (local HTTP) instead of EODHD UnicornBay API.
- Cache written to options_data_cache/thetadata/snapshots/ (vs unicornbay/options_eod/).
- Max 10 recommendations per day (Top 10 by score across ALL tickers).

Dependencies:
  pip install pandas numpy scipy openpyxl pyarrow thetadata

Env:
  export THETADATA_USERNAME="..."
  export THETADATA_PASSWORD="..."
  export STOCK_BASE_DIR="/path/to/OptionSys"
"""

from __future__ import annotations

import os
import json
import gzip
import math
import argparse
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, date
from typing import Any, Dict, List, Optional, Tuple

import math
import time
import requests
import numpy as np
import pandas as pd
from scipy.stats import lognorm, norm
from scipy.optimize import brentq
# ThetaTerminal is managed externally; no need to import ThetaClient here.

# =========================
# BASE / PATHS
# =========================
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.environ.get("STOCK_BASE_DIR") or SCRIPT_DIR

SP500_SYMBOLS_CSV = os.path.join(BASE_DIR, "sp500_symbols.csv")

# =========================
# CONFIG (Defaults)
# =========================
# Strategy params
TARGET_DTE = 45
DTE_TOLERANCE_DAYS = 12
DELTA_TARGET = 0.15
WIDTH_BOUNDS = (1, 25)
MIN_NET_CREDIT = 0.15
MIN_POP = 0.60

# --- Optional robustness filters (OFF by default) ---
ENABLE_MARKET_FILTER_DEFAULT = False
VIX_MAX_DEFAULT = float(os.environ.get("OPTSP_VIX_MAX", "22"))
SPY_MA_DAYS_DEFAULT = int(os.environ.get("OPTSP_SPY_MA_DAYS", "200"))

# Reward/Risk sanity filter (OFF by default; set >0 to enable)
MIN_CREDIT_TO_WIDTH = float(os.environ.get("OPTSP_MIN_CREDIT_TO_WIDTH", "0.0"))

# Liquidity filters
MIN_OPEN_INTEREST = 0   
MIN_VOLUME = 0          
MAX_SPREAD_PCT = 0.40   
MAX_SPREAD_ABS = 1.00   
MIN_MID = 0.03          

# Risk management
ACCOUNT_SIZE_USD = 250_000.0
RISK_PER_TRADE_PCT = 0.0075

# Daily output cap
DAILY_MAX_RECS_HARDCAP = 10  

# Candidate limits
MAX_CANDIDATES_PER_TICKER = 10   
TOP_CANDIDATES_TO_COLLECT_PER_TICKER = 2  

# Output
OUTPUT_DIR = os.path.join(BASE_DIR, "options_recommendations")
OUTPUT_PREFIX = "iron_condor"

# Cache dirs
HISTORICAL_CACHE_DIR = os.path.join(BASE_DIR, "historical_data_cache")
OPTIONS_CACHE_DIR = os.path.join(BASE_DIR, "options_data_cache", "thetadata", "snapshots")

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
    dd = d
    while dd.weekday() >= 5:
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
    seen = set()
    out: List[str] = []
    for t in tickers:
        if t not in seen:
            seen.add(t)
            out.append(t)
    return out


# =========================
# Cache IO & Price History
# =========================
def _options_cache_path(ticker: str, trade_date: str) -> str:
    return os.path.join(OPTIONS_CACHE_DIR, _symbol_base(ticker), f"{trade_date}.json.gz")

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

    try:
        row = price_df.loc[ts]
        if isinstance(row, pd.Series):
            for c in ("close", "Close", "adj_close", "Adj Close", "adjusted_close"):
                if c in row.index and pd.notna(row[c]):
                    return float(row[c])
    except Exception:
        pass

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
    spy_df = load_price_df_cached("SPY")
    if spy_df is None or spy_df.empty:
        return True, "no_spy_cache"
    spy_close = get_close_on_date(spy_df, run_date)
    spy_sma = _compute_sma(spy_df, run_date, int(spy_ma_days))
    if spy_close is not None and spy_sma is not None and spy_close < spy_sma:
        return False, f"spy_below_sma{spy_ma_days}"

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
# ThetaData Integration
# =========================

_THETA_HOST = "127.0.0.1"
_THETA_PORT = int(os.environ.get("THETA_PORT", "25503"))


def _wait_for_theta_ready(timeout: int = 60, poll_interval: float = 2.0) -> None:
    """Poll ThetaTerminal until it is connected to ThetaData's servers.

    The terminal's HTTP server on :25510 starts before it authenticates to
    the backend, so requests made too early return 474. We probe a lightweight
    endpoint (/v2/list/roots) and wait until it returns a 200 with data.
    """
    base = f"http://{_THETA_HOST}:{_THETA_PORT}"
    deadline = time.time() + timeout
    logging.info("Waiting for ThetaTerminal to connect to ThetaData servers...")
    while time.time() < deadline:
        try:
            # Use v3 expirations endpoint as readiness probe.
            resp = requests.get(f"{base}/v3/option/list/expirations",
                                params={"symbol": "AAPL", "format": "json"}, timeout=5)
            if resp.status_code == 200:
                data = resp.json()
                if data.get("response"):
                    logging.info("ThetaTerminal is ready.")
                    return
        except Exception:
            pass
        time.sleep(poll_interval)
    logging.warning(f"ThetaTerminal did not become ready within {timeout}s; proceeding anyway.")


_RISK_FREE_RATE = 0.045  # approximate current risk-free rate


def _bs_price(S: float, K: float, T: float, r: float, sigma: float, right: str) -> float:
    """Black-Scholes option price."""
    if T <= 0 or sigma <= 0:
        return max(0.0, (S - K) if right == "C" else (K - S))
    d1 = (math.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * math.sqrt(T))
    d2 = d1 - sigma * math.sqrt(T)
    if right == "C":
        return S * norm.cdf(d1) - K * math.exp(-r * T) * norm.cdf(d2)
    else:
        return K * math.exp(-r * T) * norm.cdf(-d2) - S * norm.cdf(-d1)


def _bs_delta(S: float, K: float, T: float, r: float, sigma: float, right: str) -> float:
    """Black-Scholes delta."""
    if T <= 0 or sigma <= 0:
        return 0.0
    d1 = (math.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * math.sqrt(T))
    return float(norm.cdf(d1) if right == "C" else norm.cdf(d1) - 1)


def _implied_vol(S: float, K: float, T: float, r: float, mid: float, right: str) -> Optional[float]:
    """Compute implied volatility from mid-price using Brent's method."""
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


def _fetch_chain_bulk_http(root: str, run_date_obj: date, target_dte: int, dte_tol: int) -> pd.DataFrame:
    """Fetch option chain via ThetaTerminal v3 HTTP endpoints.

    Uses /v3/option/snapshot/quote (bid/ask) and computes delta + IV via Black-Scholes.
    """
    base = f"http://{_THETA_HOST}:{_THETA_PORT}"

    # 1. Get expirations (v3 format: symbol param, YYYY-MM-DD dates in response)
    resp = requests.get(f"{base}/v3/option/list/expirations",
                        params={"symbol": root.upper(), "format": "json"}, timeout=10)
    resp.raise_for_status()
    exp_data = resp.json()

    lo = run_date_obj + timedelta(days=target_dte - dte_tol)
    hi = run_date_obj + timedelta(days=target_dte + dte_tol)

    target_exps = []
    for e in exp_data.get("response", []):
        try:
            exp_date = datetime.strptime(str(e.get("expiration", "")), "%Y-%m-%d").date()
            if lo <= exp_date <= hi:
                target_exps.append(exp_date)
        except (ValueError, AttributeError):
            pass

    if not target_exps:
        return pd.DataFrame()

    # 2. Get underlying price for Black-Scholes computations
    spot: Optional[float] = None
    price_df = load_price_df_cached(root)
    if price_df is not None and not price_df.empty:
        spot = get_close_on_date(price_df, run_date_obj.isoformat())

    # 3. For each target expiration, fetch quote snapshot (bid/ask for all contracts)
    rows = []
    for exp in target_exps:
        exp_str = exp.strftime("%Y%m%d")
        T = max((exp - run_date_obj).days, 1) / 365.0
        try:
            resp = requests.get(
                f"{base}/v3/option/snapshot/quote",
                params={"symbol": root.upper(), "expiration": exp_str, "format": "json"},
                timeout=30,
            )
            resp.raise_for_status()
            data = resp.json()
            contracts = data.get("response", [])
            for obj in contracts:
                contract = obj.get("contract", {})
                tick_list = obj.get("data", [])
                if not tick_list:
                    continue
                tick = tick_list[0]
                strike = float(contract.get("strike", 0))
                right = contract.get("right", "")  # "CALL" or "PUT"
                bid = float(tick.get("bid", 0))
                ask = float(tick.get("ask", 0))
                if bid < 0 or ask < 0 or ask < bid:
                    continue
                mid = (bid + ask) / 2.0

                # Black-Scholes delta and implied vol
                r_flag = "C" if right.upper().startswith("C") else "P"
                delta = float("nan")
                iv = None
                if spot and spot > 0 and strike > 0:
                    iv = _implied_vol(spot, strike, T, _RISK_FREE_RATE, mid, r_flag)
                    sigma = iv if iv else 0.30  # fallback sigma for delta estimate
                    delta = _bs_delta(spot, strike, T, _RISK_FREE_RATE, sigma, r_flag)

                rows.append({
                    "expiration": exp_str,
                    "strike": strike,
                    "right": right,
                    "bid": bid,
                    "ask": ask,
                    "delta": delta,
                    "implied_vol": iv if iv else float("nan"),
                })
        except Exception as e:
            logging.warning(f"Quote snapshot failed for {root} exp={exp_str}: {e}")

    return pd.DataFrame(rows)


def _process_long_to_standard(df: pd.DataFrame, ticker: str, run_date: str) -> pd.DataFrame:
    """Convert long-format chain (one row per contract) to the flat format used downstream."""
    if df.empty:
        return pd.DataFrame()
    df = df.copy()
    df["__ticker"] = ticker
    df["__run_date"] = run_date
    df["type"] = df["right"].str.upper().map({"C": "call", "P": "put", "CALL": "call", "PUT": "put"})
    df["implied_volatility"] = df["implied_vol"]
    df["exp_date"] = df["expiration"]
    return df[["__ticker", "__run_date", "type", "strike", "bid", "ask", "delta", "implied_volatility", "exp_date"]]


def fetch_chain_for_ticker(
    api_token: str,  # kept for backward compat, unused
    ticker: str,
    run_date: str,
    use_cache: bool = True,
    write_cache: bool = True,
    target_dte: int = TARGET_DTE,
    dte_tol: int = DTE_TOLERANCE_DAYS,
) -> pd.DataFrame:
    """Fetch full option chain via ThetaTerminal HTTP bulk endpoints."""
    cache_path = _options_cache_path(ticker, run_date)
    if use_cache and os.path.exists(cache_path):
        try:
            obj = _read_gz_json(cache_path)
            rows = obj.get("rows") if isinstance(obj, dict) else None
            if isinstance(rows, list):
                return pd.DataFrame(rows)
        except Exception as e:
            logging.warning(f"Cache read failed, fallback to API: {cache_path} err={e}")

    root = _symbol_base(ticker)
    run_date_obj = datetime.strptime(run_date, "%Y-%m-%d").date()

    try:
        raw = _fetch_chain_bulk_http(root, run_date_obj, target_dte, dte_tol)
        if raw is None or raw.empty:
            return pd.DataFrame()

        df = _process_long_to_standard(raw, root, run_date)

        if write_cache and not df.empty:
            try:
                _write_gz_json(cache_path, {
                    "underlying": root,
                    "trade_date": run_date,
                    "rows": df.to_dict("records"),
                })
            except Exception as e:
                logging.warning(f"Failed writing cache {cache_path}: {e}")

        return df

    except Exception as e:
        logging.error(f"ThetaData Request failed for {ticker}: {e}")
        return pd.DataFrame()


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
    
    if chain is None or chain.empty:
        return pd.DataFrame()

    if ticker is None or run_date is None:
        t2, d2 = _infer_ticker_and_date(chain)
        ticker = ticker or t2
        run_date = run_date or d2

    if not ticker or not run_date:
        return pd.DataFrame()

    df = chain.copy()

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

                if float(MIN_CREDIT_TO_WIDTH) > 0 and float(w) > 0:
                    if (float(credit) / float(w)) < float(MIN_CREDIT_TO_WIDTH):
                        continue

                pop_ln = None
                if spot is not None and iv_est is not None:
                    pop_ln = _calc_pop_lognormal(spot=spot, low=spk, high=sck, iv=iv_est, dte=dte)
                pop = pop_ln if pop_ln is not None else _estimate_pop_from_delta(float(sp[c_delta]), float(sc[c_delta]))
                if pop < float(min_pop):
                    continue

                max_loss = float(w) - float(credit) 
                if max_loss <= 0:
                    continue

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
    ap = argparse.ArgumentParser(description="Iron Condor recommender (ThetaData Edition)")
    ap.add_argument("--csv", type=str, default=SP500_SYMBOLS_CSV, help="Tickers CSV")
    ap.add_argument("--tickers", nargs="*", default=None, help="Override tickers list")
    ap.add_argument("--run-date", type=str, default=None, help="Trade date YYYY-MM-DD")
    ap.add_argument("--outdir", type=str, default=OUTPUT_DIR)
    ap.add_argument("--use-cache", action="store_true", default=True)
    ap.add_argument("--no-cache", dest="use_cache", action="store_false")
    ap.add_argument("--target-dte", type=int, default=TARGET_DTE)
    ap.add_argument("--dte-tol", type=int, default=DTE_TOLERANCE_DAYS)
    ap.add_argument("--delta", type=float, default=DELTA_TARGET)
    ap.add_argument("--min-pop", type=float, default=MIN_POP)
    ap.add_argument("--min-credit", type=float, default=MIN_NET_CREDIT)
    ap.add_argument("--max-trades", type=int, default=10, help="Max recommendations per day")
    ap.add_argument("--enable-market-filter", action="store_true", help="Enable SPY/VIX regime filter")
    ap.add_argument("--vix-max", type=float, default=VIX_MAX_DEFAULT)
    ap.add_argument("--spy-ma-days", type=int, default=SPY_MA_DAYS_DEFAULT)
    ap.add_argument("--min-credit-width", type=float, default=MIN_CREDIT_TO_WIDTH)
    ap.add_argument("--account", type=float, default=ACCOUNT_SIZE_USD)
    ap.add_argument("--risk-pct", type=float, default=RISK_PER_TRADE_PCT)
    args = ap.parse_args()

    run_date = args.run_date or determine_run_date()
    tickers = args.tickers if args.tickers else _read_tickers_from_csv(args.csv)
    if not tickers:
        logging.error("No tickers.")
        return

    os.makedirs(args.outdir, exist_ok=True)

    if args.enable_market_filter or ENABLE_MARKET_FILTER_DEFAULT:
        ok, reason = _market_filter_ok(run_date, float(args.vix_max), int(args.spy_ma_days))
        if not ok:
            logging.warning(f"Market filter blocked trading for {run_date}: {reason}. No recommendations produced.")
            return
        logging.info(f"Market filter OK for {run_date}: {reason}")

    try:
        MIN_CREDIT_TO_WIDTH = float(args.min_credit_width)
    except Exception:
        pass

    all_cands = []

    # ThetaTerminal is expected to already be running on localhost:25510.
    # We do NOT start it here — starting a second instance hangs forever.
    _wait_for_theta_ready(timeout=60, poll_interval=2.0)

    for i, t in enumerate(tickers, 1):
        logging.info(f"[{i}/{len(tickers)}] {t} date={run_date}")
        try:
            chain = fetch_chain_for_ticker(
                api_token="Not_Needed_For_ThetaData",
                ticker=t,
                run_date=run_date,
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