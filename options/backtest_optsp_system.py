#!/usr/bin/env python3
"""
backtest_optsp_system.py

Goal:
- Run optsp.py logic for EVERY weekday in the requested years (e.g. 2024 2025)
- Use ONLY the local caches populated by prefetch_options_datasp.py
- Universe comes ONLY from sp500_symbols.csv in the same directory
- Save daily recommendation files under:
    backtest_optsp_recommendations/{YEAR}/iron_condor_{YYYY-MM-DD}.csv/.xlsx
- Produce a simple annual summary under:
    backtest_optsp_summaries/summary_{YEAR}.xlsx

Usage examples:
  caffeinate -i python3 backtest_optsp_system.py --years 2024 2025 --step analysis --workers 6 --skip-existing
  caffeinate -i python3 backtest_optsp_system.py --years 2024 2025 --step virtual --analysis-type full
  python3 backtest_optsp_system.py --years 2024 --step summary

Notes:
- This script intentionally DISABLES API calls. If a cache file is missing, that ticker/date is skipped.
"""

from __future__ import annotations

import os
import json
import logging
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Tuple

import pandas as pd
from tqdm import tqdm

# Import optsp module (must be provided as optsp.py on PYTHONPATH or in this directory)
import optsp

# -------------------------
# Directories (relative to cwd)
# -------------------------
BASE_DIR = os.getcwd()
RECS_DIR = os.path.join(BASE_DIR, "backtest_optsp_recommendations")
SUMMARIES_DIR = os.path.join(BASE_DIR, "backtest_optsp_summaries")
VIRTUAL_DIR = os.path.join(BASE_DIR, "backtest_optsp_virtual")
STATE_FILE = os.path.join(BASE_DIR, "backtest_optsp_state.json")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger(__name__)


class APIBlocked(Exception):
    pass


def _disable_api_calls() -> None:
    """Monkeypatch optsp to ensure we never hit the network."""
    def _blocked(*args, **kwargs):
        raise APIBlocked("API calls are disabled in backtest_optsp_system.py (cache-only mode).")
    optsp.eodhd_unicornbay_options_eod = _blocked  # type: ignore[attr-defined]


def _read_sp500_tickers(csv_path: str) -> List[str]:
    return optsp._read_tickers_from_csv(csv_path)  # type: ignore[attr-defined]


def _iter_weekdays(year: int) -> List[date]:
    d0 = date(year, 1, 1)
    d1 = date(year, 12, 31)
    out: List[date] = []
    d = d0
    while d <= d1:
        if d.weekday() < 5:
            out.append(d)
        d += timedelta(days=1)
    return out


def _load_state() -> Optional[Dict]:
    if not os.path.exists(STATE_FILE):
        return None
    try:
        with open(STATE_FILE, "r") as f:
            return json.load(f)
    except Exception:
        return None


def _save_state(payload: Dict) -> None:
    try:
        with open(STATE_FILE, "w") as f:
            json.dump(payload, f, indent=2)
    except Exception:
        pass


def _daily_out_paths(day: date) -> Tuple[str, str]:
    year_dir = os.path.join(RECS_DIR, str(day.year))
    os.makedirs(year_dir, exist_ok=True)
    ds = day.isoformat()
    out_csv = os.path.join(year_dir, f"{optsp.OUTPUT_PREFIX}_{ds}.csv")  # type: ignore[attr-defined]
    out_xlsx = os.path.join(year_dir, f"{optsp.OUTPUT_PREFIX}_{ds}.xlsx")  # type: ignore[attr-defined]
    return out_csv, out_xlsx


def _cache_exists(ticker: str, run_date: str) -> bool:
    p = optsp._options_cache_path(ticker, run_date)  # type: ignore[attr-defined]
    return os.path.exists(p)


def _run_analysis_one_day(
    year: int, csv_path: str, day: date, skip_existing: bool, tickers: List[str]
) -> Tuple[date, Optional[Dict[str, int]]]:
    """Process a single day; returns (day, stats_dict) or (day, None) if skipped. Safe to run in a worker process."""
    _disable_api_calls()
    run_date = day.isoformat()
    out_csv, out_xlsx = _daily_out_paths(day)

    if skip_existing and os.path.exists(out_xlsx) and os.path.exists(out_csv):
        return (day, {"skipped": 1})

    recs = []
    stats = {
        "days_total": 1,
        "days_done": 0,
        "days_skipped_existing": 0,
        "days_no_recs": 0,
        "ticker_days_missing_cache": 0,
        "ticker_days_processed": 0,
        "recs_total": 0,
    }
    for t in tickers:
        if not _cache_exists(t, run_date):
            stats["ticker_days_missing_cache"] += 1
            continue
        try:
            chain = optsp.fetch_chain_for_ticker(
                optsp.EODHD_API_TOKEN, t, run_date, use_cache=True, write_cache=False  # type: ignore[attr-defined]
            )
            if chain is None or chain.empty:
                continue
            condors = optsp.pick_iron_condors(chain)  # type: ignore[attr-defined]
            if condors is None or condors.empty:
                continue
            best = condors.iloc[0].to_dict()
            best["ticker"] = t
            best["run_date"] = run_date
            recs.append(best)
            stats["ticker_days_processed"] += 1
        except APIBlocked:
            stats["ticker_days_missing_cache"] += 1
            continue
        except Exception:
            continue

    if not recs:
        stats["days_no_recs"] = 1
        stats["days_done"] = 1
        return (day, stats)

    df = pd.DataFrame(recs).sort_values(["pop_est", "net_credit"], ascending=[False, False])
    df.to_csv(out_csv, index=False)
    with pd.ExcelWriter(out_xlsx, engine="openpyxl") as w:
        df.to_excel(w, index=False, sheet_name="Recommendations")
    stats["recs_total"] = len(df)
    stats["days_done"] = 1
    return (day, stats)


def run_analysis_year(
    year: int, csv_path: str, skip_existing: bool = True, workers: int = 1
) -> Dict[str, int]:
    tickers = _read_sp500_tickers(csv_path)
    if not tickers:
        raise RuntimeError(f"No tickers found in {csv_path}")

    days = _iter_weekdays(year)

    stats = {
        "days_total": len(days),
        "days_done": 0,
        "days_skipped_existing": 0,
        "days_no_recs": 0,
        "tickers_total": len(tickers),
        "ticker_days_missing_cache": 0,
        "ticker_days_processed": 0,
        "recs_total": 0,
    }

    def _merge_day_stats(s: Dict[str, int], day_result: Optional[Dict[str, int]]) -> None:
        if not day_result:
            return
        if day_result.get("skipped"):
            s["days_skipped_existing"] += 1
            s["days_done"] += 1
            return
        s["days_done"] += day_result.get("days_done", 0)
        s["days_no_recs"] += day_result.get("days_no_recs", 0)
        s["ticker_days_missing_cache"] += day_result.get("ticker_days_missing_cache", 0)
        s["ticker_days_processed"] += day_result.get("ticker_days_processed", 0)
        s["recs_total"] += day_result.get("recs_total", 0)

    if workers <= 1:
        for day in tqdm(days, desc=f"optsp backtest {year}"):
            _, day_stats = _run_analysis_one_day(year, csv_path, day, skip_existing, tickers)
            if day_stats and day_stats.get("skipped"):
                _merge_day_stats(stats, day_stats)
                continue
            _merge_day_stats(stats, day_stats)
            if day_stats:
                run_date = day.isoformat()
                _save_state({
                    "last_completed": run_date,
                    "year": year,
                    "status": "no_recs" if day_stats.get("days_no_recs") else "ok",
                    "recs": day_stats.get("recs_total", 0),
                })
    else:
        with ProcessPoolExecutor(max_workers=workers) as pool:
            futures = {
                pool.submit(_run_analysis_one_day, year, csv_path, day, skip_existing, tickers): day
                for day in days
            }
            with tqdm(total=len(days), desc=f"optsp backtest {year}") as pbar:
                for fut in as_completed(futures):
                    day = futures[fut]
                    try:
                        _, day_stats = fut.result()
                        _merge_day_stats(stats, day_stats)
                        if day_stats and not day_stats.get("skipped"):
                            _save_state({
                                "last_completed": day.isoformat(),
                                "year": year,
                                "status": "no_recs" if day_stats.get("days_no_recs") else "ok",
                                "recs": day_stats.get("recs_total", 0),
                            })
                    except Exception as e:
                        logger.exception("Worker failed for %s: %s", day.isoformat(), e)
                    pbar.update(1)

    return stats


def build_summary_year(year: int) -> Optional[str]:
    year_dir = os.path.join(RECS_DIR, str(year))
    if not os.path.isdir(year_dir):
        logger.warning(f"No recs directory for {year}: {year_dir}")
        return None

    files = sorted([f for f in os.listdir(year_dir) if f.endswith(".csv") and f.startswith(optsp.OUTPUT_PREFIX + "_")])  # type: ignore[attr-defined]
    if not files:
        logger.warning(f"No daily CSVs for {year} in {year_dir}")
        return None

    rows = []
    for fn in files:
        ds = fn.replace(optsp.OUTPUT_PREFIX + "_", "").replace(".csv", "")  # type: ignore[attr-defined]
        path = os.path.join(year_dir, fn)
        try:
            df = pd.read_csv(path)
        except Exception:
            continue
        if df.empty:
            continue
        rows.append({
            "date": ds,
            "count": int(len(df)),
            "avg_pop": float(pd.to_numeric(df.get("pop_est"), errors="coerce").dropna().mean()) if "pop_est" in df.columns else None,
            "avg_credit": float(pd.to_numeric(df.get("net_credit"), errors="coerce").dropna().mean()) if "net_credit" in df.columns else None,
            "max_pop": float(pd.to_numeric(df.get("pop_est"), errors="coerce").dropna().max()) if "pop_est" in df.columns else None,
            "max_credit": float(pd.to_numeric(df.get("net_credit"), errors="coerce").dropna().max()) if "net_credit" in df.columns else None,
        })

    if not rows:
        logger.warning(f"No usable daily files for {year}")
        return None

    os.makedirs(SUMMARIES_DIR, exist_ok=True)
    out_xlsx = os.path.join(SUMMARIES_DIR, f"summary_{year}.xlsx")
    summary = pd.DataFrame(rows)
    summary["date"] = pd.to_datetime(summary["date"], errors="coerce")
    summary = summary.dropna(subset=["date"]).sort_values("date")
    summary["weekday"] = summary["date"].dt.day_name()
    summary["month"] = summary["date"].dt.to_period("M").astype(str)

    # Monthly breakdown
    monthly = summary.groupby("month").agg(
        days=("date", "count"),
        total_recs=("count", "sum"),
        avg_daily_recs=("count", "mean"),
        avg_pop=("avg_pop", "mean"),
        avg_credit=("avg_credit", "mean"),
    ).reset_index()

    with pd.ExcelWriter(out_xlsx, engine="openpyxl") as w:
        summary.to_excel(w, index=False, sheet_name="Daily")
        monthly.to_excel(w, index=False, sheet_name="Monthly")

    logger.info(f"Saved summary: {out_xlsx}")
    return out_xlsx


def run_virtual_years(years: List[int], analysis_type: str = "full") -> Dict[str, str]:
    """
    Build virtual results from analysis output: read all daily recommendation CSVs
    for the given years, concatenate into virtual result tables (one per year + combined).
    Saves to backtest_optsp_virtual/ (virtual_{year}.csv and virtual_all.xlsx).
    """
    os.makedirs(VIRTUAL_DIR, exist_ok=True)
    prefix = optsp.OUTPUT_PREFIX + "_"  # type: ignore[attr-defined]
    out_paths: Dict[str, str] = {}

    all_dfs: List[pd.DataFrame] = []

    for year in sorted(set(years)):
        year_dir = os.path.join(RECS_DIR, str(year))
        if not os.path.isdir(year_dir):
            logger.warning(f"No recs directory for {year}: {year_dir}")
            continue

        files = sorted(
            [f for f in os.listdir(year_dir) if f.endswith(".csv") and f.startswith(prefix)]
        )
        if not files:
            logger.warning(f"No daily CSVs for {year} in {year_dir}")
            continue

        rows: List[pd.DataFrame] = []
        for fn in files:
            path = os.path.join(year_dir, fn)
            try:
                df = pd.read_csv(path)
            except Exception:
                continue
            if df.empty:
                continue
            rows.append(df)

        if not rows:
            continue

        combined = pd.concat(rows, ignore_index=True)
        combined["run_date"] = pd.to_datetime(combined["run_date"], errors="coerce")
        combined = combined.sort_values(["run_date", "pop_est", "net_credit"], ascending=[True, False, False])

        out_csv = os.path.join(VIRTUAL_DIR, f"virtual_{year}.csv")
        combined.to_csv(out_csv, index=False)
        out_paths[f"virtual_{year}"] = out_csv
        all_dfs.append(combined)
        logger.info(f"Virtual {year}: {len(combined)} recommendations -> {out_csv}")

    if all_dfs:
        combined_all = pd.concat(all_dfs, ignore_index=True)
        combined_all["run_date"] = pd.to_datetime(combined_all["run_date"], errors="coerce")
        combined_all = combined_all.sort_values(["run_date", "pop_est", "net_credit"], ascending=[True, False, False])
        out_xlsx = os.path.join(VIRTUAL_DIR, "virtual_all.xlsx")
        with pd.ExcelWriter(out_xlsx, engine="openpyxl") as w:
            combined_all.to_excel(w, index=False, sheet_name="All")
            for year in sorted(set(years)):
                yd = combined_all[combined_all["run_date"].dt.year == year]
                if not yd.empty:
                    yd.to_excel(w, index=False, sheet_name=str(year))
        out_paths["virtual_all"] = out_xlsx
        logger.info(f"Virtual all: {len(combined_all)} recommendations -> {out_xlsx}")

    return out_paths


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Backtest optsp daily (cache-only) using sp500_symbols.csv")
    parser.add_argument("--years", nargs="+", type=int, required=True, help="Years to process (e.g., 2024 2025)")
    parser.add_argument(
        "--step",
        choices=["analysis", "summary", "virtual", "all"],
        default="all",
        help="Which step to run (virtual = build virtual results from analysis output)",
    )
    parser.add_argument(
        "--analysis-type",
        type=str,
        default="full",
        choices=["full", "quick"],
        help="Analysis type for virtual step (default: full)",
    )
    parser.add_argument("--csv", type=str, default="sp500_symbols.csv", help="Universe CSV (default: sp500_symbols.csv)")
    parser.add_argument("--skip-existing", action="store_true", help="Skip dates that already have outputs")
    parser.add_argument("--workers", type=int, default=1, help="Number of parallel workers for analysis step (default: 1)")
    args = parser.parse_args()

    csv_path = os.path.join(BASE_DIR, args.csv) if not os.path.isabs(args.csv) else args.csv
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV not found: {csv_path}")

    # Force cache-only mode
    _disable_api_calls()

    if args.step in ("analysis", "all"):
        os.makedirs(RECS_DIR, exist_ok=True)
        for y in sorted(set(args.years)):
            logger.info(f"=== Running optsp daily analysis for {y} (workers={args.workers}) ===")
            st = run_analysis_year(y, csv_path, skip_existing=args.skip_existing, workers=args.workers)
            logger.info(f"Year {y} done. stats={st}")

    if args.step in ("summary", "all"):
        for y in sorted(set(args.years)):
            build_summary_year(y)

    if args.step in ("virtual", "all"):
        run_virtual_years(args.years, analysis_type=args.analysis_type)


if __name__ == "__main__":
    main()
