#!/usr/bin/env python3
"""
fetch_sp500_symbols.py

Fetch current S&P 500 constituents from EODHD Fundamentals API (GSPC.INDX)
and write sp500_symbols.csv in the same directory as this script.

Usage:
  export EODHD_API_TOKEN="YOUR_TOKEN"
  python3 fetch_sp500_symbols.py
"""

from __future__ import annotations

import csv
import json
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, List, Optional, Tuple

import requests


API_URL = "https://eodhd.com/api/fundamentals/GSPC.INDX"


@dataclass(frozen=True)
class Component:
    code: str
    name: str = ""
    exchange: str = ""
    sector: str = ""
    industry: str = ""


def _normalize_code(code: str) -> str:
    code = (code or "").strip()
    # Most EODHD US equity symbols are like AAPL.US
    return code


def _extract_components(payload: Any) -> List[Component]:
    """
    EODHD may return components in several shapes:
      1. Full response:     {"Components": list | dict, ...}
      2. Filtered response: the components value directly as list | dict
         (when filter=Components is used, EODHD now returns the data directly)
      3. Alternative keys:  "Holdings" or "holdings" (seen in some index responses)
    """
    comps: Any = None

    if isinstance(payload, dict):
        # Try all known wrapper keys
        for key in ("Components", "components", "Holdings", "holdings"):
            if key in payload:
                comps = payload[key]
                break
        # If none of those keys exist, the dict itself might be the components
        # (e.g. {"AAPL.US": {...}, "MSFT.US": {...}} returned directly)
        if comps is None and payload:
            first_val = next(iter(payload.values()), None)
            if isinstance(first_val, dict) and ("Code" in first_val or "code" in first_val):
                comps = payload
    elif isinstance(payload, list):
        # Returned directly as a list of component objects
        comps = payload

    if comps is None:
        raise ValueError("No 'Components' field found in API response.")

    out: List[Component] = []

    def from_obj(obj: Any, key_fallback: str = "") -> Optional[Component]:
        if not isinstance(obj, dict):
            return None
        code = obj.get("Code") or obj.get("code") or key_fallback
        code = _normalize_code(str(code))
        if not code:
            return None
        return Component(
            code=code,
            name=str(obj.get("Name") or obj.get("name") or ""),
            exchange=str(obj.get("Exchange") or obj.get("exchange") or ""),
            sector=str(obj.get("Sector") or obj.get("sector") or ""),
            industry=str(obj.get("Industry") or obj.get("industry") or ""),
        )

    if isinstance(comps, list):
        for item in comps:
            c = from_obj(item)
            if c:
                out.append(c)
    elif isinstance(comps, dict):
        for k, v in comps.items():
            c = from_obj(v, key_fallback=str(k))
            if c:
                out.append(c)
    else:
        raise ValueError(f"Unexpected Components type: {type(comps)}")

    # Deduplicate by code
    uniq = {}
    for c in out:
        uniq[c.code] = c
    out = list(uniq.values())

    # Sort
    out.sort(key=lambda x: x.code)
    return out


def _read_existing_symbols(csv_path: Path) -> List[str]:
    if not csv_path.exists():
        return []
    try:
        with csv_path.open("r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            syms = []
            for row in reader:
                s = (row.get("symbol") or row.get("code") or row.get("Symbol") or "").strip()
                if s:
                    syms.append(s)
            return syms
    except Exception:
        return []


def main() -> int:
    token = os.getenv("EODHD_API_TOKEN") or os.getenv("EODHD_API_KEY") or ""
    token = token.strip()
    if not token:
        print('ERROR: Missing token. Run: export EODHD_API_TOKEN="YOUR_TOKEN"', file=sys.stderr)
        return 2

    script_dir = Path(__file__).resolve().parent
    out_path = script_dir / "sp500_symbols.csv"
    tmp_path = script_dir / "sp500_symbols.csv.tmp"

    # Use filter=Components to keep response smaller (if supported),
    # but still handle if server ignores/changes it.
    params = {
        "api_token": token,
        "fmt": "json",
        "filter": "Components",
    }

    try:
        r = requests.get(API_URL, params=params, timeout=60)
        r.raise_for_status()
        payload = r.json()
    except json.JSONDecodeError as e:
        print(f"ERROR: API did not return valid JSON: {e}", file=sys.stderr)
        return 3
    except requests.RequestException as e:
        print(f"ERROR: Request failed: {e}", file=sys.stderr)
        return 4

    try:
        components = _extract_components(payload)
    except Exception as e:
        top_keys = list(payload.keys())[:10] if isinstance(payload, dict) else type(payload).__name__
        print(f"ERROR: Failed parsing components: {e}", file=sys.stderr)
        print(f"DEBUG: API response type={type(payload).__name__}, top-level keys={top_keys}", file=sys.stderr)
        return 5

    existing = set(_read_existing_symbols(out_path))
    new_set = {c.code for c in components}
    added = sorted(new_set - existing)
    removed = sorted(existing - new_set)

    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    # Atomic write
    with tmp_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["symbol", "name", "exchange", "sector", "industry", "updated_utc"])
        for c in components:
            writer.writerow([c.code, c.name, c.exchange, c.sector, c.industry, now_utc])

    tmp_path.replace(out_path)

    print(f"OK: wrote {len(components)} symbols -> {out_path}")
    if added:
        print(f"Added ({len(added)}): {', '.join(added[:30])}" + (" ..." if len(added) > 30 else ""))
    if removed:
        print(f"Removed ({len(removed)}): {', '.join(removed[:30])}" + (" ..." if len(removed) > 30 else ""))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

