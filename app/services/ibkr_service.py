"""
ibkr_service.py
---------------
Service for sending Iron Condor draft orders to IBKR TWS, and for fetching
live portfolio positions to power the IBKR Live Portfolio Dashboard.
Uses ib_insync to connect to a running TWS or IB Gateway instance.

Port policy (hybrid model):
  • 7496 — Live TWS.  Used by Recommendations / order-execution flow.
  • 4001 — IB Gateway (live).  Used by IBKR Portfolio Dashboard reads.
  • 7497 (Paper TWS) and all other ports are explicitly rejected.
No port fallback logic exists.
"""

from __future__ import annotations

import asyncio
import json as _json
import logging
import math
import os as _os
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# Allowed ports — any other value is rejected before a connection is attempted.
TWS_LIVE_PORT     = 7496   # Live TWS  – Recommendations / order execution
GATEWAY_LIVE_PORT = 4001   # IB Gateway – Portfolio Dashboard reads
_ALLOWED_PORTS: frozenset[int] = frozenset({TWS_LIVE_PORT, GATEWAY_LIVE_PORT})

# Client ID 0 = Master Client — order appears in main TWS Mosaic/Classic view.
DEFAULT_HOST      = "127.0.0.1"
DEFAULT_PORT      = TWS_LIVE_PORT   # Default for draft-order endpoints
DEFAULT_CLIENT_ID = 0
PORTFOLIO_CLIENT_ID = 3             # Dedicated clientId for portfolio reads

# Shorter handshake timeout: TCP connects fast; IB handshake takes 1-3 s when
# TWS/Gateway is healthy. 10 s is generous and keeps stuck-connection fast.
CONNECT_TIMEOUT = 10


def _ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] + " UTC"


def _safe_float(value: Any, default: float = 0.0) -> float:
    """Return finite float; map NaN/Inf/invalid values to default."""
    try:
        num = float(value)
    except (TypeError, ValueError):
        return default
    return num if math.isfinite(num) else default


def _format_price_for_log(value: Any, decimals: int = 4) -> str:
    """Safe for logs — never applies float format specs to None or invalid values."""
    x = _safe_float(value, float("nan"))
    if not math.isfinite(x):
        return "N/A"
    try:
        return f"{x:.{decimals}f}"
    except (TypeError, ValueError):
        return "N/A"


# ---------------------------------------------------------------------------
# Portfolio grouping helpers
# ---------------------------------------------------------------------------

# Max seconds between consecutive fills to consider them the same trade
# when we have no orderId or rec_key match.
_EXEC_CLUSTER_SECS = 90


def _active_trades_path() -> str:
    base = _os.environ.get("STOCK_BASE_DIR", "/Volumes/Extreme Pro/OptionSys")
    return _os.path.join(base, "data", "active_trades.json")


def _load_active_trades_index() -> Dict[Tuple, Dict]:
    """
    Load active_trades.json and return a lookup dict:
      key  : (ticker, YYYYMMDD_expiry, frozenset_of_up_to_4_strikes)
      value: trade dict (rec_key, entry_credit, entry_pop, contracts, …)

    All users' trades are merged into one index so any leg can be matched
    regardless of which user submitted the recommendation.
    Returns empty dict on any IO or parse failure.
    """
    try:
        with open(_active_trades_path(), "r") as fh:
            raw = _json.load(fh)
    except Exception:
        return {}

    idx: Dict[Tuple, Dict] = {}
    for user_trades in raw.values():
        if not isinstance(user_trades, list):
            continue
        for trade in user_trades:
            ticker = trade.get("ticker", "")
            exp = (trade.get("exp") or "").replace("-", "")   # → YYYYMMDD
            strikes = frozenset(
                float(s)
                for s in [
                    trade.get("short_put"),
                    trade.get("long_put"),
                    trade.get("short_call"),
                    trade.get("long_call"),
                ]
                if s is not None
            )
            if ticker and exp and strikes:
                idx[(ticker, exp, strikes)] = trade
    return idx


def _parse_exec_time(time_str: str) -> Optional[datetime]:
    """
    Parse a TWS execution timestamp string to a naive datetime.
    Handles formats like '20260331 22:30:00 US/Eastern' or '20260331  22:30:00'.
    Returns None on any parse failure.
    """
    try:
        parts = time_str.strip().split()
        return datetime.strptime(f"{parts[0]} {parts[1]}", "%Y%m%d %H:%M:%S")
    except Exception:
        return None


async def _fetch_exec_maps(ib: Any, log_cb: Any) -> Tuple[Dict, Dict]:
    """
    Request current-session execution history from the connected IB instance.

    Returns a 2-tuple:
      order_fills   : Dict[int, List[dict]]
                        orderId → list of fill records, each:
                        {conId, side ("BOT"/"SLD"), shares (float), signed_qty, time (datetime)}
      conid_to_time : Dict[int, datetime]
                        conId → datetime of its earliest (entry) fill

    Both dicts are empty if reqExecutions fails or returns no data —
    this is non-fatal: grouping falls through to rec_key / time-window / fallback.
    """
    order_fills: Dict[int, List[dict]] = {}
    conid_to_time: Dict[int, datetime] = {}

    try:
        from ib_insync import ExecutionFilter
        fills = await ib.reqExecutionsAsync(ExecutionFilter())
        log_cb(f"[exec] Fetched {len(fills)} execution fill(s) for trade grouping.")
        for fill in fills:
            cid = fill.contract.conId
            oid = fill.execution.orderId
            side = getattr(fill.execution, "side", "")
            shares = _safe_float(getattr(fill.execution, "shares", 0), 0.0)
            dt = _parse_exec_time(getattr(fill.execution, "time", "") or "")
            if dt is None or shares <= 0:
                continue
            signed_qty = shares if side == "BOT" else -shares
            order_fills.setdefault(oid, []).append(
                {"conId": cid, "side": side, "shares": shares,
                 "signed_qty": signed_qty, "time": dt}
            )
            if cid not in conid_to_time or dt < conid_to_time[cid]:
                conid_to_time[cid] = dt
    except Exception as exc:
        log_cb(f"[exec] reqExecutions unavailable (non-fatal): {exc}")

    return order_fills, conid_to_time


class _PartialItem:
    """
    Lightweight proxy around an ib_insync portfolio item that represents
    a partial quantity of that leg.

    Used during TWS-style partial quantity matching: when an Iron Condor is
    formed from spreads of unequal size, the larger spread's legs are wrapped
    in _PartialItem at the IC portion quantity, and its remainder is wrapped
    at (original_qty − IC_qty) for the next matching round.

    Per-unit fields (marketPrice, averageCost) are passed through unchanged.
    Quantity-proportional fields (position, unrealizedPNL, marketValue) are
    scaled by (partial_qty / original_qty).

    The ``_root`` attribute always points to the original ib_insync item so
    the grouping algorithm can always build fresh wrappers from the true data.
    """

    __slots__ = ("_root", "_sign", "_qty", "_frac")

    def __init__(self, original: Any, partial_qty: int) -> None:
        # Unwrap any existing _PartialItem so _root is always the real item.
        self._root: Any = (
            original._root if isinstance(original, _PartialItem) else original
        )
        orig_qty = abs(_safe_float(getattr(self._root, "position", 0), 0.0))
        self._sign: int = -1 if (getattr(self._root, "position", 0) or 0) < 0 else 1
        self._qty: int = partial_qty
        self._frac: float = partial_qty / orig_qty if orig_qty > 0 else 1.0

    # ── Pass-through: structural / per-unit fields ────────────────────────

    @property
    def contract(self) -> Any:
        return self._root.contract

    @property
    def marketPrice(self) -> float:
        return _safe_float(getattr(self._root, "marketPrice", 0), 0.0)

    @property
    def averageCost(self) -> float:
        return _safe_float(getattr(self._root, "averageCost", 0), 0.0)

    # ── Scaled: quantity-proportional fields ──────────────────────────────

    @property
    def position(self) -> float:
        return float(self._sign * self._qty)

    @property
    def unrealizedPNL(self) -> float:
        return _safe_float(getattr(self._root, "unrealizedPNL", 0), 0.0) * self._frac

    @property
    def marketValue(self) -> float:
        return _safe_float(getattr(self._root, "marketValue", 0), 0.0) * self._frac


def _split_group_by_exec(
    items: List[Any],
    order_fills: Dict[int, List[dict]],
    conid_to_time: Dict[int, datetime],
    trade_idx: Dict[Tuple, Dict],
    symbol: str,
    expiry_norm: str,    # YYYYMMDD
    log_cb: Any,
) -> List[Tuple[List[Any], Optional[Dict]]]:
    """
    Split a flat list of portfolio items (all same symbol + expiry) into
    individual trade groups.

    Returns list of (group_items, matched_trade_or_None) tuples.
    group_items are the raw portfolio items; downstream code reads their
    .contract / position / marketPrice etc. as usual.

    Priority order:
      1. orderId clustering  — legs placed as one combo order this session
      2. rec_key matching    — cross-reference with active_trades.json
      3. Time-window clustering — fills within _EXEC_CLUSTER_SECS of each other
      4. Smart quantity pairing — match balanced long/short spread pairs
      5. Fallback            — remaining legs as one group (existing behaviour)
    """
    claimed: set = set()          # id(item) values already assigned
    result: List[Tuple] = []

    def remaining() -> List[Any]:
        return [it for it in items if id(it) not in claimed]

    def claim(group: List[Any], trade_info: Optional[Dict] = None) -> None:
        result.append((group, trade_info))
        for it in group:
            claimed.add(id(it))

    # Build conId → portfolio item map (first occurrence wins for duplicates)
    conid_to_item: Dict[int, Any] = {}
    for it in items:
        cid = it.contract.conId
        if cid not in conid_to_item:
            conid_to_item[cid] = it

    # ── 1. orderId clustering ─────────────────────────────────────────────
    for oid, fills in order_fills.items():
        # Collect portfolio items whose conId appears in this order's fills
        seen_cids: set = set()
        group: List[Any] = []
        for f in fills:
            cid = f["conId"]
            if cid in conid_to_item and cid not in seen_cids:
                it = conid_to_item[cid]
                if id(it) not in claimed:
                    group.append(it)
                    seen_cids.add(cid)

        if len(group) < 2:
            continue

        strikes = frozenset(_safe_float(it.contract.strike, 0.0) for it in group)
        trade_info = trade_idx.get((symbol, expiry_norm, strikes))
        log_cb(
            f"[split] orderId={oid}: {len(group)} legs"
            + (f" → rec={trade_info['rec_key']}" if trade_info else " (no rec_key match)")
        )
        claim(group, trade_info)

    # ── 2. rec_key matching ───────────────────────────────────────────────
    rem = remaining()
    if rem:
        rem_strike_map: Dict[float, Any] = {
            _safe_float(it.contract.strike, 0.0): it
            for it in rem
            if id(it) not in claimed
        }
        for (t_sym, t_exp, t_strikes), trade_info in trade_idx.items():
            if t_sym != symbol or t_exp != expiry_norm:
                continue
            matched = [
                rem_strike_map[s]
                for s in t_strikes
                if s in rem_strike_map and id(rem_strike_map[s]) not in claimed
            ]
            if len(matched) == len(t_strikes):
                log_cb(
                    f"[split] rec_key={trade_info['rec_key']}: "
                    f"{len(matched)} legs matched from active_trades"
                )
                claim(matched, trade_info)
                rem = remaining()
                rem_strike_map = {
                    _safe_float(it.contract.strike, 0.0): it
                    for it in rem
                }

    # ── 3. Time-window clustering ─────────────────────────────────────────
    rem = remaining()
    if rem and conid_to_time:
        timed = sorted(
            [
                (it, conid_to_time[it.contract.conId])
                for it in rem
                if it.contract.conId in conid_to_time
            ],
            key=lambda x: x[1],
        )
        no_time = [it for it in rem if it.contract.conId not in conid_to_time]

        if timed:
            cluster = [timed[0][0]]
            window_start = timed[0][1]
            for it, t in timed[1:]:
                gap = (t - window_start).total_seconds()
                if gap <= _EXEC_CLUSTER_SECS:
                    cluster.append(it)
                else:
                    if cluster:
                        log_cb(
                            f"[split] time-window: {len(cluster)} legs "
                            f"near {window_start.strftime('%H:%M:%S')}"
                        )
                        claim(cluster)
                    cluster = [it]
                    window_start = t
            if cluster:
                log_cb(
                    f"[split] time-window: {len(cluster)} legs "
                    f"near {window_start.strftime('%H:%M:%S')}"
                )
                claim(cluster)

        # Items with no timestamp feed into step 4
        if no_time:
            pass  # picked up by remaining() below

    # ── 4. Smart quantity pairing — TWS-style partial matching ───────────
    #
    # Mirrors TWS "Partial Quantity Matching": form the most complex combo
    # possible, then recursively return any remainder to the pool for
    # further matching.
    #
    # Pool entry: (working_lg, working_sh, qty, root_lg, root_sh)
    #   working_lg/sh — _PartialItem wrappers (raw items on the first pass)
    #   qty           — effective contracts in this slice
    #   root_lg/sh    — original ib_insync items; used to claim the
    #                   underlying position and build fresh wrappers
    #
    # Loop invariant:
    #   1. Prefer an exact-qty IC match (no splitting needed).
    #   2. Else take the first put+call pair → IC(min_qty) + remainder
    #      re-enters the pool at abs(p_qty − c_qty) for the next round.
    #   3. After the IC loop, leftover pool entries → standalone spreads.
    #   4. Truly lone legs fall through to the step-5 fallback.
    rem = remaining()
    if rem:
        puts  = [it for it in rem if it.contract.right == "P"]
        calls = [it for it in rem if it.contract.right == "C"]

        def balanced_pairs(legs: List[Any]) -> List[Tuple[Any, Any, float]]:
            """Pair long+short legs that share the same absolute position size."""
            shorts = [l for l in legs if (l.position or 0) < 0]
            longs  = [l for l in legs if (l.position or 0) > 0]
            used_longs: set = set()
            pairs = []
            for sh in shorts:
                sq = abs(_safe_float(getattr(sh, "position", 0), 0.0))
                for lg in longs:
                    if id(lg) in used_longs:
                        continue
                    lq = abs(_safe_float(getattr(lg, "position", 0), 0.0))
                    if abs(sq - lq) < 0.01:
                        pairs.append((lg, sh, sq))
                        used_longs.add(id(lg))
                        break
            return pairs

        # Build the initial pools from balanced spread pairs.
        # Each entry: (working_lg, working_sh, qty, root_lg, root_sh)
        put_pool: List[Tuple[Any, Any, float, Any, Any]] = [
            (lg, sh, qty, lg, sh) for lg, sh, qty in balanced_pairs(puts)
        ]
        call_pool: List[Tuple[Any, Any, float, Any, Any]] = [
            (lg, sh, qty, lg, sh) for lg, sh, qty in balanced_pairs(calls)
        ]

        # ── Iterative IC formation ────────────────────────────────────────
        while put_pool and call_pool:
            # Pass 1: look for an exact-qty match (no splitting needed).
            best_pi: Optional[int] = None
            best_ci: Optional[int] = None
            for pi, p_entry in enumerate(put_pool):
                p_qty = p_entry[2]
                for ci, c_entry in enumerate(call_pool):
                    c_qty = c_entry[2]
                    if abs(p_qty - c_qty) < 0.01:
                        best_pi, best_ci = pi, ci
                        break
                if best_pi is not None:
                    break

            # Pass 2: no exact match → partial IC with the first pair.
            if best_pi is None:
                best_pi, best_ci = 0, 0

            p_lg_w, p_sh_w, p_qty, p_root_lg, p_root_sh = put_pool[best_pi]
            c_lg_w, c_sh_w, c_qty, c_root_lg, c_root_sh = call_pool[best_ci]

            min_qty = int(min(p_qty, c_qty))
            rem_qty = int(round(abs(p_qty - c_qty)))
            is_partial = rem_qty > 0

            # Claim all four root items — prevents step-5 re-absorption.
            for _root in (p_root_lg, p_root_sh, c_root_lg, c_root_sh):
                claimed.add(id(_root))

            # Build IC legs as fresh _PartialItem wrappers at min_qty.
            ic_legs: List[Any] = [
                _PartialItem(p_root_lg, min_qty),
                _PartialItem(p_root_sh, min_qty),
                _PartialItem(c_root_lg, min_qty),
                _PartialItem(c_root_sh, min_qty),
            ]
            ic_strikes = frozenset(
                _safe_float(leg.contract.strike, 0.0) for leg in ic_legs
            )
            ic_trade_info = trade_idx.get((symbol, expiry_norm, ic_strikes))
            log_cb(
                f"[split] IC {min_qty}x {symbol}"
                + (" (partial-qty)" if is_partial else "")
                + (f" → rec={ic_trade_info['rec_key']}" if ic_trade_info else "")
            )
            result.append((ic_legs, ic_trade_info))

            # Handle the remainder:
            #   • The fully-consumed spread is removed from its pool.
            #   • The surplus spread re-enters its pool at rem_qty so the
            #     next iteration can match it against another spread.
            if is_partial:
                if p_qty > c_qty:
                    put_pool[best_pi] = (
                        _PartialItem(p_root_lg, rem_qty),
                        _PartialItem(p_root_sh, rem_qty),
                        float(rem_qty),
                        p_root_lg,
                        p_root_sh,
                    )
                    call_pool.pop(best_ci)
                else:
                    call_pool[best_ci] = (
                        _PartialItem(c_root_lg, rem_qty),
                        _PartialItem(c_root_sh, rem_qty),
                        float(rem_qty),
                        c_root_lg,
                        c_root_sh,
                    )
                    put_pool.pop(best_pi)
            else:
                put_pool.pop(best_pi)
                call_pool.pop(best_ci)

        # ── Remaining pool entries → standalone spread cards ──────────────
        for p_lg_w, p_sh_w, p_qty, p_root_lg, p_root_sh in put_pool:
            for _root in (p_root_lg, p_root_sh):
                claimed.add(id(_root))
            log_cb(f"[split] Put Spread {int(p_qty)}x {symbol}")
            result.append(([p_lg_w, p_sh_w], None))

        for c_lg_w, c_sh_w, c_qty, c_root_lg, c_root_sh in call_pool:
            for _root in (c_root_lg, c_root_sh):
                claimed.add(id(_root))
            log_cb(f"[split] Call Spread {int(c_qty)}x {symbol}")
            result.append(([c_lg_w, c_sh_w], None))

    # ── 5. Fallback ───────────────────────────────────────────────────────
    rem = remaining()
    if rem:
        log_cb(f"[split] fallback: {len(rem)} unmatched leg(s) as one group")
        claim(rem)

    return result


def _build_strategy_group(
    ticker: str,
    expiry_str: str,
    legs: List[Any],
    spot: float,
    trade_info: Optional[Dict],
) -> Dict[str, Any]:
    """
    Build a single strategy-group dict from a list of portfolio items.
    Encapsulates all P&L, strike, DTE, pin-risk, and serialisation logic.
    """
    today = date.today()

    try:
        es = expiry_str
        if len(es) == 8 and es.isdigit():
            exp_date = date(int(es[:4]), int(es[4:6]), int(es[6:8]))
        else:
            exp_date = datetime.strptime(es, "%Y-%m-%d").date()
        dte = (exp_date - today).days
    except Exception:
        exp_date = None
        dte = None

    put_legs  = [l for l in legs if l.contract.right == "P"]
    call_legs = [l for l in legs if l.contract.right == "C"]
    num_legs  = len(legs)

    if num_legs == 4 and len(put_legs) == 2 and len(call_legs) == 2:
        strategy_name = "Iron Condor"
    elif num_legs == 2 and len(put_legs) == 2:
        strategy_name = "Put Spread"
    elif num_legs == 2 and len(call_legs) == 2:
        strategy_name = "Call Spread"
    elif num_legs == 2:
        strategy_name = "Strangle / Straddle"
    elif num_legs == 1:
        right    = legs[0].contract.right
        pos_sign = "Short" if legs[0].position < 0 else "Long"
        strategy_name = f"{pos_sign} {'Put' if right == 'P' else 'Call'}"
    else:
        strategy_name = f"{num_legs}-Leg Strategy"

    total_unrealized_pnl = sum(
        _safe_float(getattr(l, "unrealizedPNL", 0), 0.0) for l in legs
    )
    total_avg_cost = sum(
        _safe_float(getattr(l, "averageCost", 0), 0.0)
        * _safe_float(getattr(l, "position", 0), 0.0)
        for l in legs
    )
    initial_credit = max(-total_avg_cost, 0.0)

    short_legs      = [l for l in legs if (l.position or 0) < 0]
    long_legs_list  = [l for l in legs if (l.position or 0) > 0]
    short_put_legs  = [l for l in short_legs if l.contract.right == "P"]
    short_call_legs = [l for l in short_legs if l.contract.right == "C"]
    long_put_legs   = [l for l in long_legs_list if l.contract.right == "P"]
    long_call_legs  = [l for l in long_legs_list if l.contract.right == "C"]

    short_put_leg  = max(short_put_legs,  key=lambda x: x.contract.strike, default=None)
    short_call_leg = min(short_call_legs, key=lambda x: x.contract.strike, default=None)
    long_put_leg   = max(long_put_legs,   key=lambda x: x.contract.strike, default=None)
    long_call_leg  = min(long_call_legs,  key=lambda x: x.contract.strike, default=None)

    short_put_strike  = _safe_float(short_put_leg.contract.strike,  0.0) if short_put_leg  else None
    short_call_strike = _safe_float(short_call_leg.contract.strike, 0.0) if short_call_leg else None

    abs_quantity = max(
        1,
        max(abs(int(_safe_float(getattr(l, "position", 0), 0.0))) for l in legs),
    )

    max_loss: Optional[float] = None
    if strategy_name == "Iron Condor" and short_put_leg and long_put_leg and short_call_leg and long_call_leg:
        put_width  = _safe_float(short_put_leg.contract.strike,  0.0) - _safe_float(long_put_leg.contract.strike,  0.0)
        call_width = _safe_float(long_call_leg.contract.strike,  0.0) - _safe_float(short_call_leg.contract.strike, 0.0)
        max_spread = max(put_width, call_width)
        if max_spread > 0:
            max_loss = (max_spread * 100 * abs_quantity) - initial_credit

    pct_max_profit = (total_unrealized_pnl / initial_credit * 100) if initial_credit > 0 else None
    pct_max_loss   = (total_unrealized_pnl / max_loss * 100) if (max_loss and max_loss > 0) else None

    pin_risk = False
    if dte is not None and dte <= 2 and spot > 0:
        if short_put_strike  and abs(spot - short_put_strike)  / spot <= 0.01:
            pin_risk = True
        if short_call_strike and abs(spot - short_call_strike) / spot <= 0.01:
            pin_risk = True

    # Enrich with known trade metadata from active_trades.json
    entry_credit_known = round(_safe_float(trade_info.get("entry_credit"), 0.0), 2) if trade_info else None
    entry_pop_known    = round(_safe_float(trade_info.get("entry_pop"),    0.0), 4) if trade_info else None
    rec_key            = trade_info.get("rec_key") if trade_info else None

    serialized_legs = [
        {
            "con_id":         int(l.contract.conId or 0),
            "symbol":         l.contract.symbol,
            "strike":         _safe_float(l.contract.strike, 0.0),
            "right":          l.contract.right,
            "expiry":         l.contract.lastTradeDateOrContractMonth,
            "position":       _safe_float(getattr(l, "position",     0), 0.0),
            "market_value":   _safe_float(getattr(l, "marketValue",  0), 0.0),
            "avg_cost":       _safe_float(getattr(l, "averageCost",  0), 0.0),
            "unrealized_pnl": _safe_float(getattr(l, "unrealizedPNL",0), 0.0),
            "market_price":   _safe_float(getattr(l, "marketPrice",  0), 0.0),
            "action_to_close": "BUY" if (l.position or 0) < 0 else "SELL",
        }
        for l in legs
    ]

    return {
        "ticker":            ticker,
        "strategy_name":     strategy_name,
        "legs":              serialized_legs,
        "unrealized_pnl":    round(total_unrealized_pnl, 2),
        "initial_credit":    round(initial_credit, 2),
        "max_loss":          round(max_loss, 2) if max_loss is not None else None,
        "pct_max_profit":    round(pct_max_profit, 1) if pct_max_profit is not None else None,
        "pct_max_loss":      round(pct_max_loss,   1) if pct_max_loss   is not None else None,
        "expiration":        expiry_str,
        "dte":               dte,
        "short_put_strike":  short_put_strike,
        "short_call_strike": short_call_strike,
        "spot_price":        round(_safe_float(spot, 0.0), 2),
        "quantity":          abs_quantity,
        "pin_risk":          pin_risk,
        # Enrichment from active_trades.json (None when trade was placed manually)
        "rec_key":           rec_key,
        "entry_credit_known": entry_credit_known,
        "entry_pop_known":   entry_pop_known,
    }


async def _connect_to_tws(
    *,
    host: str,
    port: int,
    client_id: int,
    log_cb: Any,
    max_attempts: int = 2,
) -> Any:
    """
    Connect to TWS or IB Gateway on *port* using *client_id* and return a
    connected IB instance.

    Allowed ports:
      • 7496 — Live TWS  (Recommendations / order-execution flow)
      • 4001 — IB Gateway live  (IBKR Portfolio Dashboard)

    Any other port (including 7497 Paper) is rejected immediately.

    A **fresh** IB object is created for every attempt so that a previous
    timed-out socket can never pollute the next attempt. No clientId cycling
    occurs — the provided client_id is used exclusively.

    Raises RuntimeError after *max_attempts* failures.
    """
    if port not in _ALLOWED_PORTS:
        raise RuntimeError(
            f"IBKR service only connects to port {TWS_LIVE_PORT} (TWS Live) "
            f"or port {GATEWAY_LIVE_PORT} (IB Gateway Live). "
            f"Requested port {port} is not allowed."
        )

    try:
        from ib_insync import IB
    except ImportError as exc:
        raise RuntimeError(
            "ib_insync is not installed. Install with: pip install ib_insync"
        ) from exc

    last_error: Optional[Exception] = None

    for attempt in range(1, max_attempts + 1):
        ib = IB()
        try:
            log_cb(
                f"Connecting to {host}:{port} clientId={client_id} "
                f"(attempt {attempt}/{max_attempts})…"
            )
            await ib.connectAsync(host, port, clientId=client_id, timeout=CONNECT_TIMEOUT)
            if ib.isConnected():
                log_cb(f"Connected to TWS clientId={client_id}.")
                return ib
            raise RuntimeError("Not connected after connectAsync returned")
        except Exception as ex:
            last_error = ex
            log_cb(f"Attempt {attempt}/{max_attempts} failed: {ex!s}")
            # Always disconnect the failed IB instance before discarding it so
            # the underlying socket is closed and TWS releases the slot.
            try:
                ib.disconnect()
            except Exception:
                pass
            if attempt < max_attempts:
                await asyncio.sleep(2.0)

    last_msg = str(last_error).strip() if last_error else ""
    if not last_msg:
        last_msg = last_error.__class__.__name__ if last_error else "Unknown TWS connection failure"
    raise RuntimeError(f"IBKR connection failed after {max_attempts} attempt(s): {last_msg}")


# ---------------------------------------------------------------------------
# Bracket price helper
# ---------------------------------------------------------------------------

def _compute_bracket_prices(
    net_premium: float,
    short_put: float,
    long_put: float,
    short_call: float,
    long_call: float,
    take_profit_pct: Optional[float] = None,
) -> Tuple[float, float]:
    """
    Compute the closing-direction limit prices for TP and SL bracket child orders.

    All prices are positive debits — the amount paid when buying the combo back
    to close the position.

    Take-profit uses ``HOLD_TP_STRONG_PCT`` (percent of credit to bank) when
    ``take_profit_pct`` is omitted; otherwise ``take_profit_pct`` is the fraction
    of credit to capture (0–1), i.e. aligned with ``HOLD_TP_STRONG_PCT / 100``.

    Stop-loss debit is derived from ``HOLD_SL_STRONG_CLOSE_PCT`` (magnitude =
    percent of collected credit lost vs entry), capped at structural max loss
    per share (``net_premium + max_loss_per_share``).

    Returns:
        (tp_debit, sl_debit) — both positive, suitable for LimitOrder("BUY", qty, price).
    """
    spread_width = max(short_put - long_put, long_call - short_call)
    max_loss_per_share = max(0.0, spread_width - net_premium)

    tp_frac = (
        take_profit_pct
        if take_profit_pct is not None
        else (HOLD_TP_STRONG_PCT / 100.0)
    )
    tp_frac = max(0.0, min(1.0, tp_frac))
    tp_debit = round(max(0.01, net_premium * (1.0 - tp_frac)), 2)

    loss_vs_credit_pct = abs(HOLD_SL_STRONG_CLOSE_PCT)
    sl_debit_uncapped = net_premium * (1.0 + loss_vs_credit_pct / 100.0)
    sl_cap = net_premium + max_loss_per_share
    sl_debit = round(max(0.01, min(sl_debit_uncapped, sl_cap)), 2)

    return tp_debit, sl_debit


async def send_iron_condor_draft(
    ticker: str,
    exp_date: str,
    short_put: float,
    long_put: float,
    short_call: float,
    long_call: float,
    net_premium: float,
    quantity: int,
    take_profit_pct: Optional[float] = None,
    stop_loss_pct: float = 1.0,
    host: str = DEFAULT_HOST,
    port: int = DEFAULT_PORT,
    client_id: int = DEFAULT_CLIENT_ID,
) -> Dict[str, Any]:
    """
    Connect to TWS, build a 4-leg Iron Condor bracket order, and queue all
    three orders as a **draft** (all ``transmit=False``) for manual review.

    Order structure:
      • Parent  — entry LimitOrder("BUY") on the opening BAG at −net_premium
                  (negative limit = credit received).  transmit=False.
      • TP child — LimitOrder("BUY") on the closing BAG at tp_debit.
                  transmit=False.
      • SL child — LimitOrder("BUY") on the closing BAG at sl_debit.
                  transmit=False — stays queued until you Transmit in TWS.

    TP/SL child limits are derived from ``HOLD_TP_STRONG_PCT`` /
    ``HOLD_SL_STRONG_CLOSE_PCT`` (see ``_compute_bracket_prices``).
    ``stop_loss_pct`` is retained for API compatibility and ignored for pricing.

    For live submission without TWS review, use ``send_gateway_live_order``
    (parent/TP False, SL True — standard ib_insync bracket transmit).

    All three orders are placed immediately and linked via parentId.  IBKR
    holds the children until the parent fills, then activates both; when one
    child fills the other is automatically cancelled (bracket behaviour).

    Returns:
        Dict with "ok", "order_id", "tp_order_id", "sl_order_id",
                  "status", "message", "logs".
    """
    execution_logs: List[str] = []

    def log(msg: str) -> None:
        execution_logs.append(f"[{_ts()}] {msg}")

    try:
        from ib_insync import IB, Option, Contract, LimitOrder, ComboLeg
    except ImportError as e:
        logger.error("ib_insync not installed: %s", e)
        log(f"Import error: ib_insync not installed — {e}")
        raise RuntimeError("ib_insync is not installed. Install with: pip install ib_insync") from e

    ib = IB()
    exp_ibkr = exp_date.replace("-", "") if "-" in exp_date else exp_date

    def on_error(*args: Any, **kwargs: Any) -> None:
        parts = [str(a) for a in args] + [f"{k}={v}" for k, v in kwargs.items()]
        log(f"TWS Error — {', '.join(parts)}")

    try:
        if hasattr(ib, "errorEvent"):
            ib.errorEvent += on_error
        log(f"Attempting connection to {host}:{port} clientId={client_id}...")
        logger.info("Connecting to TWS %s:%s clientId=%s", host, port, client_id)
        await ib.connectAsync(host, port, clientId=client_id, timeout=CONNECT_TIMEOUT)
        if not ib.isConnected():
            log("TWS connection failed (not connected after connect()).")
            raise RuntimeError("TWS connection failed (not connected after connect())")
        log("Connected to TWS.")

        async def qualify(symbol: str, strike: float, right: str, leg_name: str) -> Contract:
            opt = Option(symbol, exp_ibkr, strike, right, "SMART")
            log(f"Qualifying {leg_name} ({symbol} {exp_ibkr} {strike} {right})...")
            res = await ib.qualifyContractsAsync(opt)
            if asyncio.iscoroutine(res):
                res = await res
            if not res:
                log(f"Could not qualify {leg_name}.")
                raise RuntimeError(
                    f"Could not qualify option: {symbol} {exp_ibkr} strike={strike} {right}"
                )
            c = res[0] if isinstance(res, list) else res
            log(f"{leg_name} qualified with conId={getattr(c, 'conId', '?')}.")
            return c

        short_put_c  = await qualify(ticker, short_put,  "P", "Leg 1 (short put)")
        long_put_c   = await qualify(ticker, long_put,   "P", "Leg 2 (long put)")
        short_call_c = await qualify(ticker, short_call, "C", "Leg 3 (short call)")
        long_call_c  = await qualify(ticker, long_call,  "C", "Leg 4 (long call)")

        leg_exchange = getattr(short_put_c, "exchange", None) or "SMART"
        for c in (long_put_c, short_call_c, long_call_c):
            ex = getattr(c, "exchange", None)
            if ex:
                leg_exchange = ex
                break

        # ── Opening BAG: legs in their natural open-position direction ─────
        log("Building opening BAG contract (4 legs)...")
        open_combo = Contract()
        open_combo.symbol   = ticker
        open_combo.secType  = "BAG"
        open_combo.currency = "USD"
        open_combo.exchange = "SMART"
        open_combo.comboLegs = [
            ComboLeg(conId=short_put_c.conId,  ratio=1, action="SELL", exchange=leg_exchange),
            ComboLeg(conId=long_put_c.conId,   ratio=1, action="BUY",  exchange=leg_exchange),
            ComboLeg(conId=short_call_c.conId, ratio=1, action="SELL", exchange=leg_exchange),
            ComboLeg(conId=long_call_c.conId,  ratio=1, action="BUY",  exchange=leg_exchange),
        ]

        # ── Closing BAG: reversed leg actions, used by TP and SL children ─
        close_combo = Contract()
        close_combo.symbol   = ticker
        close_combo.secType  = "BAG"
        close_combo.currency = "USD"
        close_combo.exchange = "SMART"
        close_combo.comboLegs = [
            ComboLeg(conId=short_put_c.conId,  ratio=1, action="BUY",  exchange=leg_exchange),
            ComboLeg(conId=long_put_c.conId,   ratio=1, action="SELL", exchange=leg_exchange),
            ComboLeg(conId=short_call_c.conId, ratio=1, action="BUY",  exchange=leg_exchange),
            ComboLeg(conId=long_call_c.conId,  ratio=1, action="SELL", exchange=leg_exchange),
        ]

        # ── Bracket prices ────────────────────────────────────────────────
        tp_price, sl_price = _compute_bracket_prices(
            net_premium=net_premium,
            short_put=short_put, long_put=long_put,
            short_call=short_call, long_call=long_call,
            take_profit_pct=take_profit_pct,
        )
        _tp_frac = (
            take_profit_pct
            if take_profit_pct is not None
            else (HOLD_TP_STRONG_PCT / 100.0)
        )
        log(
            f"Bracket prices — entry credit: ${net_premium:.2f} | "
            f"TP close debit: ${tp_price:.2f} ({_tp_frac * 100:.0f}% credit capture) | "
            f"SL close debit: ${sl_price:.2f} "
            f"(|HOLD_SL_STRONG_CLOSE_PCT|={abs(HOLD_SL_STRONG_CLOSE_PCT):.0f}% vs credit, capped)"
        )

        # ── Parent order (entry) ───────────────────────────────────────────
        entry_limit = -round(net_premium, 2)  # negative = receive credit
        parent_order = LimitOrder("BUY", quantity, entry_limit)
        parent_order.transmit = False
        log(
            f"Placing parent order: BUY qty={quantity} limit={entry_limit} "
            f"[credit ${net_premium:.2f}] transmit=False"
        )
        parent_trade = ib.placeOrder(open_combo, parent_order)
        parent_id = parent_trade.order.orderId
        log(f"Parent order placed — orderId={parent_id}")

        # ── TP child order ────────────────────────────────────────────────
        tp_order = LimitOrder("BUY", quantity, tp_price)
        tp_order.parentId = parent_id
        tp_order.transmit = False
        log(
            f"Placing TP child: BUY qty={quantity} limit={tp_price:.2f} "
            f"parentId={parent_id} transmit=False"
        )
        tp_trade = ib.placeOrder(close_combo, tp_order)
        tp_order_id = tp_trade.order.orderId

        # ── SL child order ────────────────────────────────────────────────
        sl_order = LimitOrder("BUY", quantity, sl_price)
        sl_order.parentId = parent_id
        sl_order.transmit = False  # Draft: remain in TWS until user Transmit
        log(
            f"Placing SL child: BUY qty={quantity} limit={sl_price:.2f} "
            f"parentId={parent_id} transmit=False"
        )
        sl_trade = ib.placeOrder(close_combo, sl_order)
        sl_order_id = sl_trade.order.orderId

        await asyncio.sleep(1)  # Allow TWS to process and display before disconnect
        status = getattr(parent_trade.orderStatus, "status", None) if parent_trade.orderStatus else None
        log(
            f"Bracket draft complete — parent={parent_id} TP={tp_order_id} "
            f"SL={sl_order_id} status={status}"
        )
        logger.info(
            "Bracket draft placed: ticker=%s exp=%s parent=%s tp=%s sl=%s transmit=False",
            ticker, exp_ibkr, parent_id, tp_order_id, sl_order_id,
        )
        return {
            "ok":          True,
            "order_id":    parent_id,
            "tp_order_id": tp_order_id,
            "sl_order_id": sl_order_id,
            "status":      status,
            "message":     "Bracket draft sent to TWS (3 orders: entry + TP + SL)",
            "logs":        execution_logs,
        }

    except Exception as e:
        log(f"Exception: {e!s}")
        logger.exception("send_iron_condor_draft failed: %s", e)
        raise RuntimeError(str(e)) from e
    finally:
        try:
            if hasattr(ib, "errorEvent"):
                ib.errorEvent -= on_error
        except Exception:
            pass
        try:
            if ib.isConnected():
                ib.disconnect()
                log("Disconnected from TWS.")
                logger.debug("Disconnected from TWS")
        except Exception as disc_ex:
            logger.warning("Disconnect warning: %s", disc_ex)
            log(f"Disconnect warning: {disc_ex}")


async def fetch_ibkr_portfolio(
    host: str = DEFAULT_HOST,
    port: int = DEFAULT_PORT,
    client_id: int = PORTFOLIO_CLIENT_ID,
) -> Dict[str, Any]:
    """
    Connect to IB Gateway (port 4001) or TWS Live (port 7496) and fetch all
    open option positions. Groups individual option legs into strategy groups
    (Iron Condor, Spread, etc.), calculates P&L metrics, DTE, and Pin Risk.

    Port 7497 (Paper) is never used. No fallback port logic exists here.

    Returns:
        Dict with "ok", "groups" (list of strategy group dicts), "logs".
    """
    execution_logs: List[str] = []

    def log(msg: str) -> None:
        execution_logs.append(f"[{_ts()}] {msg}")

    try:
        from ib_insync import Stock
    except ImportError as e:
        logger.error("ib_insync not installed: %s", e)
        raise RuntimeError("ib_insync is not installed. Install with: pip install ib_insync") from e

    ib: Any = None

    def on_error(*args: Any, **kwargs: Any) -> None:
        parts = [str(a) for a in args] + [f"{k}={v}" for k, v in kwargs.items()]
        log(f"TWS Error — {', '.join(parts)}")

    try:
        logger.info("Connecting to TWS %s:%s clientId=%s for portfolio", host, port, client_id)
        ib = await _connect_to_tws(
            host=host,
            port=port,
            client_id=client_id,
            log_cb=log,
            max_attempts=2,
        )
        if hasattr(ib, "errorEvent"):
            ib.errorEvent += on_error
        log(f"Using clientId={client_id} for portfolio fetch.")

        # Wait briefly for account data to populate
        await asyncio.sleep(2)

        portfolio_items = ib.portfolio()
        log(f"Fetched {len(portfolio_items)} total portfolio items.")

        opt_items = [item for item in portfolio_items if item.contract.secType == "OPT"]
        log(f"Found {len(opt_items)} open option leg(s).")

        if not opt_items:
            return {"ok": True, "groups": [], "logs": execution_logs}

        # ── Fetch execution history for timestamp/orderId-based grouping ──
        order_fills, conid_to_time = await _fetch_exec_maps(ib, log)

        # ── Load active_trades.json for rec_key cross-referencing ─────────
        trade_idx = _load_active_trades_index()
        log(f"[trades] Loaded {len(trade_idx)} known trade signature(s) from active_trades.")

        # ── First-pass grouping: bucket by (symbol, expiry) ───────────────
        expiry_buckets: Dict[tuple, list] = defaultdict(list)
        for item in opt_items:
            key = (item.contract.symbol, item.contract.lastTradeDateOrContractMonth)
            expiry_buckets[key].append(item)

        # ── Fetch spot prices ─────────────────────────────────────────────
        unique_tickers = list({k[0] for k in expiry_buckets})
        spot_map: Dict[str, float] = {}
        spot_source_map: Dict[str, str] = {}
        for ticker in unique_tickers:
            try:
                stock = Stock(ticker, "SMART", "USD")
                await ib.qualifyContractsAsync(stock)
                ticker_data = ib.reqMktData(stock, "", False, False)
                await asyncio.sleep(2)
                price = _safe_float(ticker_data.marketPrice(), 0.0)
                if price <= 0:
                    price = _safe_float(getattr(ticker_data, "close", 0), 0.0)
                if price <= 0:
                    bid = _safe_float(getattr(ticker_data, "bid", 0), 0.0)
                    ask = _safe_float(getattr(ticker_data, "ask", 0), 0.0)
                    if bid > 0 and ask > 0:
                        price = (bid + ask) / 2.0
                spot_map[ticker] = price
                if price > 0:
                    spot_source_map[ticker] = "ibkr"
                ib.cancelMktData(stock)
                log(f"Spot price for {ticker}: {spot_map[ticker]}")
            except Exception as spot_ex:
                log(f"Could not fetch spot for {ticker}: {spot_ex}")
                spot_map[ticker] = 0.0

        # EODHD fallback when IB returns no usable quote (weekend / after-hours / no data)
        _loop = asyncio.get_event_loop()
        for _ticker in unique_tickers:
            if _safe_float(spot_map.get(_ticker), 0.0) > 0:
                continue
            alt = await _loop.run_in_executor(
                None, _fetch_eodhd_underlying_price_sync, _ticker
            )
            if alt is not None and alt > 0:
                spot_map[_ticker] = round(alt, 2)
                spot_source_map[_ticker] = "eodhd"
                log(f"Spot for {_ticker} via EODHD fallback: {spot_map[_ticker]}")

        # ── Second-pass: split each (symbol, expiry) bucket into individual
        #    trades using execution timestamps, rec_key matching, and smart
        #    quantity pairing. ───────────────────────────────────────────────
        result_groups: List[Dict[str, Any]] = []

        for (ticker, expiry_str), bucket_items in expiry_buckets.items():
            expiry_norm = expiry_str.replace("-", "")  # → YYYYMMDD

            # Fast path is valid only when every leg shares the same absolute
            # quantity.  Mixed quantities (e.g. a put spread qty 6 alongside
            # a call spread qty 11 in the same expiry bucket) must run through
            # the full split logic so that partial IC matching can fire.
            leg_qtys = {
                abs(_safe_float(getattr(it, "position", 0), 0.0))
                for it in bucket_items
            }
            use_fast_path = len(bucket_items) <= 4 and len(leg_qtys) <= 1

            if use_fast_path:
                strikes = frozenset(
                    _safe_float(it.contract.strike, 0.0) for it in bucket_items
                )
                trade_info = trade_idx.get((ticker, expiry_norm, strikes))
                sub_groups = [(bucket_items, trade_info)]
                log(
                    f"[group] {ticker} {expiry_str}: {len(bucket_items)} legs "
                    f"(single-group fast path)"
                )
            else:
                log(
                    f"[group] {ticker} {expiry_str}: {len(bucket_items)} legs — "
                    f"running split logic"
                )
                sub_groups = _split_group_by_exec(
                    items=bucket_items,
                    order_fills=order_fills,
                    conid_to_time=conid_to_time,
                    trade_idx=trade_idx,
                    symbol=ticker,
                    expiry_norm=expiry_norm,
                    log_cb=log,
                )

            spot = spot_map.get(ticker, 0.0)
            for group_legs, trade_info in sub_groups:
                result_groups.append(
                    _build_strategy_group(ticker, expiry_str, group_legs, spot, trade_info)
                )

        for grp in result_groups:
            tkr = grp["ticker"]
            spv = _safe_float(grp.get("spot_price"), 0.0)
            if spv > 0:
                grp["spot_price_source"] = spot_source_map.get(tkr, "ibkr")
            else:
                grp["spot_price_source"] = None

        log(f"Built {len(result_groups)} strategy group(s).")
        return {"ok": True, "groups": result_groups, "logs": execution_logs}

    except Exception as e:
        msg = str(e).strip() or e.__class__.__name__
        if isinstance(e, asyncio.TimeoutError) or "TimeoutError" in msg or "timed out" in msg.lower():
            port_hint = (
                f"port {GATEWAY_LIVE_PORT} (IB Gateway)"
                if port == GATEWAY_LIVE_PORT
                else f"port {TWS_LIVE_PORT} (TWS Live)"
            )
            msg = (
                f"IBKR API handshake timed out on {port_hint}. "
                "Verify settings: Enable ActiveX and Socket Clients, "
                "add 127.0.0.1 and ::1 to trusted IPs, Read-Only API is OFF, "
                "and dismiss any permission popup."
            )
        log(f"Exception: {msg}")
        logger.exception("fetch_ibkr_portfolio failed: %s", e)
        raise RuntimeError(msg) from e
    finally:
        if ib is not None:
            try:
                if hasattr(ib, "errorEvent"):
                    ib.errorEvent -= on_error
            except Exception:
                pass
            try:
                if ib.isConnected():
                    ib.disconnect()
                    log("Disconnected from TWS.")
                    logger.debug("Disconnected from TWS (portfolio fetch)")
            except Exception as disc_ex:
                logger.warning("Disconnect warning: %s", disc_ex)
                log(f"Disconnect warning: {disc_ex}")


# Dedicated clientIds for the new Gateway flows (avoid conflicts with existing clients)
GATEWAY_MKTDATA_CLIENT_ID = 5   # Market-data snapshot (read-only, short-lived)
GATEWAY_ORDER_CLIENT_ID   = 6   # Live order placement via IB Gateway


async def fetch_combo_market_data(
    ticker: str,
    exp_date: str,
    short_put: float,
    long_put: float,
    short_call: float,
    long_call: float,
    host: str = DEFAULT_HOST,
    port: int = GATEWAY_LIVE_PORT,
    client_id: int = GATEWAY_MKTDATA_CLIENT_ID,
) -> Dict[str, Any]:
    """
    Connect to IB Gateway (port 4001), qualify all four Iron Condor option legs,
    and fetch real-time Bid / Ask / Mid for the full combo by summing individual
    leg prices.

    Combo pricing convention (credit strategy — BUY order model):
      combo_bid  = sp_bid + sc_bid − lp_ask − lc_ask   (worst-case fill for seller)
      combo_ask  = sp_ask + sc_ask − lp_bid − lc_bid   (best-case fill for seller)
      combo_mid  = (combo_bid + combo_ask) / 2

    Returns dict: {ok, bid, ask, mid, logs}
    bid/ask/mid are None when market data is unavailable.
    """
    execution_logs: List[str] = []

    def log(msg: str) -> None:
        execution_logs.append(f"[{_ts()}] {msg}")

    try:
        from ib_insync import Option
    except ImportError as e:
        raise RuntimeError("ib_insync is not installed. Install with: pip install ib_insync") from e

    ib: Any = None
    exp_ibkr = exp_date.replace("-", "") if "-" in exp_date else exp_date

    def on_error(*args: Any, **kwargs: Any) -> None:
        parts = [str(a) for a in args] + [f"{k}={v}" for k, v in kwargs.items()]
        log(f"TWS Error — {', '.join(parts)}")

    try:
        ib = await _connect_to_tws(
            host=host,
            port=port,
            client_id=client_id,
            log_cb=log,
            max_attempts=2,
        )
        if hasattr(ib, "errorEvent"):
            ib.errorEvent += on_error
        log(f"Connected to Gateway {host}:{port} for combo market data.")

        async def qualify(strike: float, right: str, leg_name: str) -> Any:
            opt = Option(ticker, exp_ibkr, strike, right, "SMART")
            log(f"Qualifying {leg_name} ({ticker} {exp_ibkr} {strike} {right})…")
            res = await ib.qualifyContractsAsync(opt)
            if asyncio.iscoroutine(res):
                res = await res
            if not res:
                raise RuntimeError(
                    f"Could not qualify option: {ticker} {exp_ibkr} strike={strike} {right}"
                )
            c = res[0] if isinstance(res, list) else res
            log(f"{leg_name} qualified conId={getattr(c, 'conId', '?')}")
            return c

        sp_c = await qualify(short_put,  "P", "short_put")
        lp_c = await qualify(long_put,   "P", "long_put")
        sc_c = await qualify(short_call, "C", "short_call")
        lc_c = await qualify(long_call,  "C", "long_call")

        # Subscribe to live market data for all four legs simultaneously.
        td_map = {
            "sp": ib.reqMktData(sp_c, "", False, False),
            "lp": ib.reqMktData(lp_c, "", False, False),
            "sc": ib.reqMktData(sc_c, "", False, False),
            "lc": ib.reqMktData(lc_c, "", False, False),
        }

        # Allow time for the first ticks to arrive (3 s is ample on a healthy connection).
        await asyncio.sleep(3)

        def _tick(td: Any, field: str) -> float:
            """Return the tick value; treat ib_insync sentinel −1 / NaN / ≤0 as NaN."""
            raw = getattr(td, field, None)
            v = _safe_float(raw, float("nan"))
            if not math.isfinite(v) or v <= 0:
                # Try marketPrice() as a last resort
                mp = _safe_float(td.marketPrice(), float("nan"))
                return mp if math.isfinite(mp) and mp > 0 else float("nan")
            return v

        prices: Dict[str, Tuple[float, float]] = {}
        for key, td in td_map.items():
            bid = _tick(td, "bid")
            ask = _tick(td, "ask")
            log(
                f"  {key}: bid={_format_price_for_log(bid)} "
                f"ask={_format_price_for_log(ask)}"
            )
            prices[key] = (bid, ask)

        # Cancel subscriptions
        for c in [sp_c, lp_c, sc_c, lc_c]:
            try:
                ib.cancelMktData(c)
            except Exception:
                pass

        sp_bid, sp_ask = prices["sp"]
        lp_bid, lp_ask = prices["lp"]
        sc_bid, sc_ask = prices["sc"]
        lc_bid, lc_ask = prices["lc"]

        combo_bid = sp_bid + sc_bid - lp_ask - lc_ask
        combo_ask = sp_ask + sc_ask - lp_bid - lc_bid

        if not (math.isfinite(combo_bid) and math.isfinite(combo_ask)):
            log("WARNING: Some leg prices unavailable; bid/ask/mid may be None.")
            combo_bid = combo_ask = float("nan")

        combo_mid = (combo_bid + combo_ask) / 2 if math.isfinite(combo_bid) and math.isfinite(combo_ask) else float("nan")

        def _round_or_none(v: float) -> Optional[float]:
            return round(v, 2) if math.isfinite(v) else None

        result = {
            "ok":   True,
            "bid":  _round_or_none(combo_bid),
            "ask":  _round_or_none(combo_ask),
            "mid":  _round_or_none(combo_mid),
            "logs": execution_logs,
        }
        log(f"Combo result: bid={result['bid']} ask={result['ask']} mid={result['mid']}")
        return result

    except Exception as e:
        log(f"Exception: {e!s}")
        logger.exception("fetch_combo_market_data failed: %s", e)
        raise RuntimeError(str(e)) from e
    finally:
        if ib is not None:
            try:
                if hasattr(ib, "errorEvent"):
                    ib.errorEvent -= on_error
            except Exception:
                pass
            try:
                if ib.isConnected():
                    ib.disconnect()
                    log("Disconnected from Gateway (market data).")
            except Exception as disc_ex:
                logger.warning("Disconnect warning: %s", disc_ex)
                log(f"Disconnect warning: {disc_ex}")


async def send_gateway_live_order(
    ticker: str,
    exp_date: str,
    short_put: float,
    long_put: float,
    short_call: float,
    long_call: float,
    limit_price: float,
    quantity: int,
    take_profit_pct: Optional[float] = None,
    stop_loss_pct: float = 1.0,
    host: str = DEFAULT_HOST,
    port: int = GATEWAY_LIVE_PORT,
    client_id: int = GATEWAY_ORDER_CLIENT_ID,
) -> Dict[str, Any]:
    """
    Place a live Iron Condor bracket order via IB Gateway (port 4001).

    Builds a true bracket (parentId-linked orders) so that TP and SL
    are activated atomically by IBKR as soon as the entry fills.

    TP/SL limits use ``HOLD_TP_STRONG_PCT`` / ``HOLD_SL_STRONG_CLOSE_PCT``;
    ``stop_loss_pct`` is retained for API compatibility and ignored for pricing.

    Order structure:
      • Parent  — entry LimitOrder("BUY") on the opening BAG at −limit_price.
                  transmit=False (must be False so children are submitted first).
      • TP child — LimitOrder("BUY") on the closing BAG at tp_debit.
                  transmit=False.
      • SL child — LimitOrder("BUY") on the closing BAG at sl_debit.
                  transmit=True — this fires the entire bracket to the exchange
                  atomically (standard ib_insync bracket pattern).

    IBKR holds the children until the parent fills, then activates both; when
    one child fills the other is automatically cancelled.

    limit_price: credit to receive (positive value, e.g. 1.50 → order limit −1.50).

    Returns dict: {ok, order_id, tp_order_id, sl_order_id, status, message, logs}
    """
    execution_logs: List[str] = []

    def log(msg: str) -> None:
        execution_logs.append(f"[{_ts()}] {msg}")

    try:
        from ib_insync import IB, Option, Contract, LimitOrder, ComboLeg
    except ImportError as e:
        raise RuntimeError("ib_insync is not installed. Install with: pip install ib_insync") from e

    ib = IB()
    exp_ibkr = exp_date.replace("-", "") if "-" in exp_date else exp_date

    def on_error(*args: Any, **kwargs: Any) -> None:
        parts = [str(a) for a in args] + [f"{k}={v}" for k, v in kwargs.items()]
        log(f"TWS Error — {', '.join(parts)}")

    try:
        if hasattr(ib, "errorEvent"):
            ib.errorEvent += on_error
        log(f"Connecting to IB Gateway {host}:{port} clientId={client_id} for live bracket order…")
        await ib.connectAsync(host, port, clientId=client_id, timeout=CONNECT_TIMEOUT)
        if not ib.isConnected():
            raise RuntimeError("IB Gateway connection failed (not connected after connect())")
        log("Connected to IB Gateway.")

        async def qualify(strike: float, right: str, leg_name: str) -> Any:
            opt = Option(ticker, exp_ibkr, strike, right, "SMART")
            log(f"Qualifying {leg_name} ({ticker} {exp_ibkr} {strike} {right})…")
            res = await ib.qualifyContractsAsync(opt)
            if asyncio.iscoroutine(res):
                res = await res
            if not res:
                raise RuntimeError(
                    f"Could not qualify option: {ticker} {exp_ibkr} strike={strike} {right}"
                )
            c = res[0] if isinstance(res, list) else res
            log(f"{leg_name} qualified conId={getattr(c, 'conId', '?')}")
            return c

        sp_c = await qualify(short_put,  "P", "Leg 1 (short put)")
        lp_c = await qualify(long_put,   "P", "Leg 2 (long put)")
        sc_c = await qualify(short_call, "C", "Leg 3 (short call)")
        lc_c = await qualify(long_call,  "C", "Leg 4 (long call)")

        leg_exchange = getattr(sp_c, "exchange", None) or "SMART"
        for c in (lp_c, sc_c, lc_c):
            ex = getattr(c, "exchange", None)
            if ex:
                leg_exchange = ex
                break

        # ── Opening BAG: natural open-position leg direction ───────────────
        log("Building opening BAG contract (4 legs) for live bracket…")
        open_combo = Contract()
        open_combo.symbol   = ticker
        open_combo.secType  = "BAG"
        open_combo.currency = "USD"
        open_combo.exchange = "SMART"
        open_combo.comboLegs = [
            ComboLeg(conId=sp_c.conId, ratio=1, action="SELL", exchange=leg_exchange),
            ComboLeg(conId=lp_c.conId, ratio=1, action="BUY",  exchange=leg_exchange),
            ComboLeg(conId=sc_c.conId, ratio=1, action="SELL", exchange=leg_exchange),
            ComboLeg(conId=lc_c.conId, ratio=1, action="BUY",  exchange=leg_exchange),
        ]

        # ── Closing BAG: reversed leg actions for TP / SL children ────────
        close_combo = Contract()
        close_combo.symbol   = ticker
        close_combo.secType  = "BAG"
        close_combo.currency = "USD"
        close_combo.exchange = "SMART"
        close_combo.comboLegs = [
            ComboLeg(conId=sp_c.conId, ratio=1, action="BUY",  exchange=leg_exchange),
            ComboLeg(conId=lp_c.conId, ratio=1, action="SELL", exchange=leg_exchange),
            ComboLeg(conId=sc_c.conId, ratio=1, action="BUY",  exchange=leg_exchange),
            ComboLeg(conId=lc_c.conId, ratio=1, action="SELL", exchange=leg_exchange),
        ]

        # ── Bracket prices ─────────────────────────────────────────────────
        credit = abs(limit_price)
        tp_price, sl_price = _compute_bracket_prices(
            net_premium=credit,
            short_put=short_put, long_put=long_put,
            short_call=short_call, long_call=long_call,
            take_profit_pct=take_profit_pct,
        )
        _tp_frac_gw = (
            take_profit_pct
            if take_profit_pct is not None
            else (HOLD_TP_STRONG_PCT / 100.0)
        )
        log(
            f"Bracket prices — entry credit: ${credit:.2f} | "
            f"TP close debit: ${tp_price:.2f} ({_tp_frac_gw * 100:.0f}% credit capture) | "
            f"SL close debit: ${sl_price:.2f} "
            f"(|HOLD_SL_STRONG_CLOSE_PCT|={abs(HOLD_SL_STRONG_CLOSE_PCT):.0f}% vs credit, capped)"
        )

        # ── Parent order (entry) ───────────────────────────────────────────
        order_limit = -round(credit, 2)  # negative = receive credit
        parent_order = LimitOrder("BUY", quantity, order_limit)
        # transmit=False: children must be submitted before transmission so that
        # IBKR receives the full bracket atomically (standard ib_insync pattern).
        parent_order.transmit = False
        log(
            f"Placing parent order: BUY qty={quantity} limit={order_limit} "
            f"(credit ${credit:.2f}) ticker={ticker} exp={exp_ibkr} transmit=False"
        )
        parent_trade = ib.placeOrder(open_combo, parent_order)
        parent_id = parent_trade.order.orderId
        log(f"Parent order placed — orderId={parent_id}")

        # ── TP child order ─────────────────────────────────────────────────
        tp_order = LimitOrder("BUY", quantity, tp_price)
        tp_order.parentId = parent_id
        tp_order.transmit = False
        log(
            f"Placing TP child: BUY qty={quantity} limit={tp_price:.2f} "
            f"parentId={parent_id} transmit=False"
        )
        tp_trade = ib.placeOrder(close_combo, tp_order)
        tp_order_id = tp_trade.order.orderId

        # ── SL child order ─────────────────────────────────────────────────
        # transmit=True on the final child fires the entire bracket to the exchange.
        sl_order = LimitOrder("BUY", quantity, sl_price)
        sl_order.parentId = parent_id
        sl_order.transmit = True  # Transmits the full bracket (parent + TP + SL) to the exchange
        log(
            f"Placing SL child: BUY qty={quantity} limit={sl_price:.2f} "
            f"parentId={parent_id} transmit=True [fires full bracket]"
        )
        sl_trade = ib.placeOrder(close_combo, sl_order)
        sl_order_id = sl_trade.order.orderId

        await asyncio.sleep(1)
        status = getattr(parent_trade.orderStatus, "status", None) if parent_trade.orderStatus else None
        log(
            f"Live bracket placed — parent={parent_id} TP={tp_order_id} "
            f"SL={sl_order_id} status={status}"
        )
        logger.info(
            "Gateway live bracket placed: ticker=%s exp=%s parent=%s tp=%s sl=%s transmit=True",
            ticker, exp_ibkr, parent_id, tp_order_id, sl_order_id,
        )

        return {
            "ok":          True,
            "order_id":    parent_id,
            "tp_order_id": tp_order_id,
            "sl_order_id": sl_order_id,
            "status":      status,
            "message":     "Live bracket order sent to IB Gateway (entry + TP + SL)",
            "logs":        execution_logs,
        }

    except Exception as e:
        log(f"Exception: {e!s}")
        logger.exception("send_gateway_live_order failed: %s", e)
        raise RuntimeError(str(e)) from e
    finally:
        if ib is not None:
            try:
                if hasattr(ib, "errorEvent"):
                    ib.errorEvent -= on_error
            except Exception:
                pass
            try:
                if ib.isConnected():
                    ib.disconnect()
                    log("Disconnected from Gateway (live bracket order).")
            except Exception as disc_ex:
                logger.warning("Disconnect warning: %s", disc_ex)
                log(f"Disconnect warning: {disc_ex}")


async def fetch_close_combo_market_data(
    ticker: str,
    legs: List[Dict[str, Any]],
    host: str = DEFAULT_HOST,
    port: int = GATEWAY_LIVE_PORT,
    client_id: int = GATEWAY_MKTDATA_CLIENT_ID,
) -> Dict[str, Any]:
    """
    Fetch real-time Bid / Ask / Mid for a closing combo using pre-known con_ids.

    Each leg dict must contain: con_id (int), action_to_close ("BUY"|"SELL").

    Pricing convention (debit to close):
        close_bid = sum(BUY_leg.bid) - sum(SELL_leg.ask)   best-case debit
        close_ask = sum(BUY_leg.ask) - sum(SELL_leg.bid)   worst-case debit
        close_mid = (close_bid + close_ask) / 2

    Returns: {ok, bid, ask, mid, logs}. bid/ask/mid are None when unavailable.
    Connects to IB Gateway (port 4001) only — no TWS fallback.
    """
    execution_logs: List[str] = []

    def log(msg: str) -> None:
        execution_logs.append(f"[{_ts()}] {msg}")

    try:
        from ib_insync import Contract as IBContract
    except ImportError as e:
        raise RuntimeError("ib_insync is not installed. Install with: pip install ib_insync") from e

    ib: Any = None

    def on_error(*args: Any, **kwargs: Any) -> None:
        parts = [str(a) for a in args] + [f"{k}={v}" for k, v in kwargs.items()]
        log(f"TWS Error — {', '.join(parts)}")

    try:
        ib = await _connect_to_tws(
            host=host,
            port=port,
            client_id=client_id,
            log_cb=log,
            max_attempts=2,
        )
        if hasattr(ib, "errorEvent"):
            ib.errorEvent += on_error
        log(f"Connected to Gateway {host}:{port} for close combo market data.")

        leg_tickers: List[tuple[str, Any]] = []
        for leg in legs:
            c = IBContract()
            c.conId = int(leg["con_id"])
            c.exchange = "SMART"
            res = await ib.qualifyContractsAsync(c)
            if asyncio.iscoroutine(res):
                res = await res
            if not res:
                raise RuntimeError(f"Could not qualify conId={leg['con_id']}")
            qualified = res[0] if isinstance(res, list) else res
            action = leg.get("action_to_close", "BUY").upper()
            td = ib.reqMktData(qualified, "", False, False)
            log(f"  Subscribed market data for conId={leg['con_id']} action={action}")
            leg_tickers.append((action, td))

        await asyncio.sleep(3)

        try:

            def _tick(td: Any, field: str) -> float:
                try:
                    raw = getattr(td, field, None)
                    v = _safe_float(raw, float("nan"))
                    if not math.isfinite(v) or v <= 0:
                        mp_fn = getattr(td, "marketPrice", None)
                        mp_raw = mp_fn() if callable(mp_fn) else None
                        mp = _safe_float(mp_raw, float("nan"))
                        return mp if math.isfinite(mp) and mp > 0 else float("nan")
                    return v
                except Exception:
                    return float("nan")

            close_bid = 0.0
            close_ask = 0.0
            for action, td in leg_tickers:
                bid = _tick(td, "bid")
                ask = _tick(td, "ask")
                log(
                    f"  {action}: bid={_format_price_for_log(bid)} "
                    f"ask={_format_price_for_log(ask)}"
                )
                if action == "BUY":
                    close_bid += bid
                    close_ask += ask
                else:
                    close_bid -= ask
                    close_ask -= bid

            if not (math.isfinite(close_bid) and math.isfinite(close_ask)):
                log("WARNING: Some leg prices unavailable; bid/ask/mid may be None.")
                close_bid = close_ask = float("nan")

            close_mid = (
                (close_bid + close_ask) / 2
                if math.isfinite(close_bid) and math.isfinite(close_ask)
                else float("nan")
            )

            def _round_or_none(v: float) -> Optional[float]:
                return round(v, 2) if math.isfinite(v) else None

            result = {
                "ok":   True,
                "bid":  _round_or_none(close_bid),
                "ask":  _round_or_none(close_ask),
                "mid":  _round_or_none(close_mid),
                "logs": execution_logs,
            }
            log(
                f"Close combo result: bid={result['bid']} ask={result['ask']} mid={result['mid']}"
            )
            return result
        except Exception as ex:
            log(f"Close combo pricing failed (returning null prices): {ex!s}")
            logger.warning(
                "fetch_close_combo_market_data soft fail: %s", ex, exc_info=True
            )
            return {
                "ok":   True,
                "bid":  None,
                "ask":  None,
                "mid":  None,
                "logs": execution_logs,
            }

    except Exception as e:
        log(f"Exception: {e!s}")
        logger.exception("fetch_close_combo_market_data failed: %s", e)
        raise RuntimeError(str(e)) from e
    finally:
        if ib is not None:
            try:
                if hasattr(ib, "errorEvent"):
                    ib.errorEvent -= on_error
            except Exception:
                pass
            try:
                if ib.isConnected():
                    ib.disconnect()
                    log("Disconnected from Gateway (close market data).")
            except Exception as disc_ex:
                logger.warning("Disconnect warning: %s", disc_ex)
                log(f"Disconnect warning: {disc_ex}")


async def send_close_draft(
    ticker: str,
    expiry: str,
    legs: List[Dict[str, Any]],
    mid_price: float,
    quantity: int,
    host: str = DEFAULT_HOST,
    port: int = DEFAULT_PORT,
    client_id: int = PORTFOLIO_CLIENT_ID,
    transmit: bool = False,
) -> Dict[str, Any]:
    """
    Place a closing order for an existing option strategy as a unified BAG combo.

    When transmit=False (default): non-transmitting draft queued in TWS (port 7496).
    When transmit=True: live execution sent directly to the exchange via IB Gateway (port 4001).

    Builds a BAG combo with each leg's close action (reversed from the original position),
    then places a BUY limit order at mid_price.

    Connects to port 7496 (TWS Live) or 4001 (IB Gateway). No fallback to 7497.
    Each leg dict must contain: con_id (int), action_to_close ("BUY"/"SELL").

    Returns:
        Dict with "ok", "order_id", "status", "message", "logs".
    """
    execution_logs: List[str] = []

    def log(msg: str) -> None:
        execution_logs.append(f"[{_ts()}] {msg}")

    try:
        from ib_insync import Contract, LimitOrder, ComboLeg
    except ImportError as e:
        raise RuntimeError("ib_insync is not installed. Install with: pip install ib_insync") from e

    ib: Any = None
    exp_ibkr = expiry.replace("-", "") if "-" in expiry else expiry

    def on_error(*args: Any, **kwargs: Any) -> None:
        parts = [str(a) for a in args] + [f"{k}={v}" for k, v in kwargs.items()]
        log(f"TWS Error — {', '.join(parts)}")

    try:
        ib = await _connect_to_tws(
            host=host,
            port=port,
            client_id=client_id,
            log_cb=log,
            max_attempts=2,
        )
        if hasattr(ib, "errorEvent"):
            ib.errorEvent += on_error
        log(f"Using clientId={client_id} for close draft.")

        combo = Contract()
        combo.symbol = ticker
        combo.secType = "BAG"
        combo.currency = "USD"
        combo.exchange = "SMART"
        combo.comboLegs = [
            ComboLeg(
                conId=int(leg["con_id"]),
                ratio=1,
                action=leg["action_to_close"],
                exchange="SMART",
            )
            for leg in legs
        ]

        # BUY combo with reversed-action legs at positive mid_price (debit to close)
        limit_price = round(abs(mid_price), 2)
        order = LimitOrder("BUY", quantity, limit_price)
        order.transmit = transmit
        mode_label = "live (transmit=True)" if transmit else "draft (transmit=False)"
        log(f"Placing close order [{mode_label}]: BUY qty={quantity} limit={limit_price} ticker={ticker} exp={exp_ibkr}...")

        trade = ib.placeOrder(combo, order)
        await asyncio.sleep(1)
        order_id = getattr(trade.order, "orderId", None)
        status = getattr(trade.orderStatus, "status", None) if trade.orderStatus else None
        log(f"Close order placed — orderId={order_id}, status={status}, transmit={transmit}.")
        logger.info(
            "Close order placed: ticker=%s exp=%s orderId=%s transmit=%s",
            ticker, exp_ibkr, order_id, transmit,
        )

        return {
            "ok": True,
            "order_id": order_id,
            "status": status,
            "message": "Close order transmitted to exchange" if transmit else "Close draft sent to TWS",
            "logs": execution_logs,
        }

    except Exception as e:
        log(f"Exception: {e!s}")
        logger.exception("send_close_draft failed: %s", e)
        raise RuntimeError(str(e)) from e
    finally:
        if ib is not None:
            try:
                if hasattr(ib, "errorEvent"):
                    ib.errorEvent -= on_error
            except Exception:
                pass
            try:
                if ib.isConnected():
                    ib.disconnect()
                    log("Disconnected from TWS.")
                    logger.debug("Disconnected from TWS (close draft)")
            except Exception as disc_ex:
                logger.warning("Disconnect warning: %s", disc_ex)
                log(f"Disconnect warning: {disc_ex}")


# ---------------------------------------------------------------------------
# Enriched portfolio — new Trade Management Command Center functions
# ---------------------------------------------------------------------------

# Dedicated clientId for fetching open orders (separate from portfolio clientId=3)
ORDERS_CLIENT_ID = 7

# EODHD API token — same env var pattern used by trading_analysis.py and stock_scanner.py
EODHD_API_TOKEN: str = (
    _os.environ.get("EODHD_API_TOKEN")
    or _os.environ.get("EODHD_API_KEY")
    or "699ae161e60555.92486250"
)


# ---------------------------------------------------------------------------
# Hold score — tunable thresholds (Iron Condors & credit spreads)
# ---------------------------------------------------------------------------
# pct_max_profit = unrealized_pnl / initial_credit * 100 (percent of premium collected)

# --- Grace period (flat P&L + safe distance to BE → neutral 3–4, no DTE/earnings hit) ---
HOLD_GRACE_PNL_LOW: float = -15.0          # % of credit; below → not in grace
HOLD_GRACE_PNL_HIGH: float = 15.0          # % of credit; above → not in grace
HOLD_GRACE_MIN_BE_DIST_FRAC: float = 0.15  # min_dist ≥ this (fraction of BE width)
HOLD_GRACE_BASE_SCORE: float = 3.5         # center before micro-tilt
HOLD_GRACE_PNL_TILT_THRESH: float = 5.0    # |pct| above this adds ± tilt inside grace
HOLD_GRACE_PNL_TILT: float = 0.25          # nudge inside grace when |pct| > tilt thresh
HOLD_GRACE_SCORE_MIN: int = 3
HOLD_GRACE_SCORE_MAX: int = 4

# --- Take profit vs initial credit (industry: ~50% of max profit ≈ 50% of credit on many ICs) ---
HOLD_TP_EXCEPTIONAL_PCT: float = 65.0   # strong lift toward 5
HOLD_TP_EXCEPTIONAL_BONUS: float = 1.5
HOLD_TP_STRONG_PCT: float = 50.0        # canonical TP zone + entry bracket TP (see _compute_bracket_prices)
HOLD_TP_STRONG_BONUS: float = 1.25
HOLD_TP_GOOD_PCT: float = 35.0
HOLD_TP_GOOD_BONUS: float = 0.75
HOLD_TP_MODERATE_PCT: float = 15.0
HOLD_TP_MODERATE_BONUS: float = 0.35

# --- Stop loss vs initial credit (defined-risk: 1× credit lost = -100%; 2× = -200%) ---
HOLD_SL_STRONG_CLOSE_PCT: float = -100.0   # exit signal + entry bracket SL vs credit (see _compute_bracket_prices)
HOLD_SL_SEVERE_PCT: float = -75.0
HOLD_SL_SEVERE_PENALTY: float = 2.25
HOLD_SL_WARN_PCT: float = -50.0
HOLD_SL_WARN_PENALTY: float = 1.5
HOLD_SL_SOFT_PCT: float = -25.0
HOLD_SL_SOFT_PENALTY: float = 0.75
HOLD_SL_MINOR_PCT: float = 0.0
HOLD_SL_MINOR_PENALTY: float = 0.35

# --- Breakeven distance (normalized: dist to nearest BE / full BE width) ---
HOLD_BE_WIDE_BONUS_FRAC: float = 0.40
HOLD_BE_WIDE_BONUS: float = 0.5
HOLD_BE_TIGHT_PENALTY_FRAC: float = 0.20
HOLD_BE_TIGHT_PENALTY: float = 0.5
HOLD_BE_VERY_TIGHT_FRAC: float = 0.10
HOLD_BE_VERY_TIGHT_PENALTY: float = 1.5

# --- DTE (calendar risk on short premium) ---
HOLD_DTE_PIN_RISK: int = 2
HOLD_DTE_PIN_PENALTY: float = 1.5
HOLD_DTE_ELEVATED_MAX: int = 7
HOLD_DTE_ELEVATED_PENALTY: float = 0.5
HOLD_DTE_COMFORT_MIN: int = 21
HOLD_DTE_COMFORT_BONUS: float = 0.25

# --- Earnings (entry may hold through event; mild caution outside grace only) ---
HOLD_EARNINGS_PENALTY: float = 0.5  # was -1.5; avoids instant “2” on earnings alone


def _hold_score_min_dist_normalized(group: Dict[str, Any]) -> Optional[float]:
    """
    Distance from spot to nearest breakeven, as a fraction of (upper - lower).
    Negative if spot is outside the breakeven band. None if not computable.
    """
    spot = _safe_float(group.get("spot_price"), 0.0)
    be_lower = group.get("breakeven_lower")
    be_upper = group.get("breakeven_upper")
    if spot <= 0 or be_lower is None or be_upper is None:
        return None
    lo = _safe_float(be_lower, float("nan"))
    hi = _safe_float(be_upper, float("nan"))
    if not math.isfinite(lo) or not math.isfinite(hi):
        return None
    be_range = hi - lo
    if be_range <= 0:
        return None
    return min(spot - lo, hi - spot) / be_range


def _in_hold_grace_period(group: Dict[str, Any]) -> bool:
    """
    True when P&L is flat vs collected credit and spot is comfortably inside BE.

    In this window we bypass DTE and earnings penalties so new condors/spreads
    are not immediately scored as exits.
    """
    pct_raw = group.get("pct_max_profit")
    if pct_raw is None:
        return False
    pct = _safe_float(pct_raw, float("nan"))
    if not math.isfinite(pct):
        return False
    if pct < HOLD_GRACE_PNL_LOW or pct > HOLD_GRACE_PNL_HIGH:
        return False

    min_dist = _hold_score_min_dist_normalized(group)
    if min_dist is None:
        return False
    if min_dist < 0.0:
        return False
    if min_dist < HOLD_GRACE_MIN_BE_DIST_FRAC:
        return False

    return True


def _compute_hold_score(group: Dict[str, Any]) -> int:
    """
    1–5 Hold/Sell score for Iron Condors and credit spreads (enriched group dict).

    5 = Strong Hold   3 = Neutral   1 = Strong Close

    Uses pct_max_profit = unrealized_pnl / initial_credit * 100.
    Grace period: flat P&L band + safe BE → score clamped to 3–4; no DTE/earnings penalty.
    Hard risk: loss ≥ 100% of credit or spot outside BE → Strong Close (1).
    """
    pct_val = group.get("pct_max_profit")
    pct = _safe_float(pct_val, float("nan")) if pct_val is not None else float("nan")
    min_dist = _hold_score_min_dist_normalized(group)

    # --- Hard overrides (never masked by grace) ---
    if math.isfinite(pct) and pct <= HOLD_SL_STRONG_CLOSE_PCT:
        return 1

    if min_dist is not None and min_dist < 0.0:
        return 1

    if _in_hold_grace_period(group):
        s = HOLD_GRACE_BASE_SCORE
        if math.isfinite(pct):
            if pct > HOLD_GRACE_PNL_TILT_THRESH:
                s += HOLD_GRACE_PNL_TILT
            elif pct < -HOLD_GRACE_PNL_TILT_THRESH:
                s -= HOLD_GRACE_PNL_TILT
        out = int(round(s))
        return max(HOLD_GRACE_SCORE_MIN, min(HOLD_GRACE_SCORE_MAX, out))

    score = 3.0

    # --- Factor 1: P&L vs initial credit (take-profit / stop-loss ladder) ---
    if math.isfinite(pct):
        if pct >= HOLD_TP_EXCEPTIONAL_PCT:
            score += HOLD_TP_EXCEPTIONAL_BONUS
        elif pct >= HOLD_TP_STRONG_PCT:
            score += HOLD_TP_STRONG_BONUS
        elif pct >= HOLD_TP_GOOD_PCT:
            score += HOLD_TP_GOOD_BONUS
        elif pct >= HOLD_TP_MODERATE_PCT:
            score += HOLD_TP_MODERATE_BONUS
        elif pct <= HOLD_SL_SEVERE_PCT:
            score -= HOLD_SL_SEVERE_PENALTY
        elif pct <= HOLD_SL_WARN_PCT:
            score -= HOLD_SL_WARN_PENALTY
        elif pct <= HOLD_SL_SOFT_PCT:
            score -= HOLD_SL_SOFT_PENALTY
        elif pct < HOLD_SL_MINOR_PCT:
            score -= HOLD_SL_MINOR_PENALTY

    # --- Factor 2: Breakeven distance (normalized; outside BE handled above) ---
    if min_dist is not None:
        if min_dist > HOLD_BE_WIDE_BONUS_FRAC:
            score += HOLD_BE_WIDE_BONUS
        elif min_dist < HOLD_BE_VERY_TIGHT_FRAC:
            score -= HOLD_BE_VERY_TIGHT_PENALTY
        elif min_dist < HOLD_BE_TIGHT_PENALTY_FRAC:
            score -= HOLD_BE_TIGHT_PENALTY

    # --- Factor 3: DTE ---
    dte = group.get("dte")
    if dte is not None:
        di = int(dte)
        if di <= HOLD_DTE_PIN_RISK:
            score -= HOLD_DTE_PIN_PENALTY
        elif di <= HOLD_DTE_ELEVATED_MAX:
            score -= HOLD_DTE_ELEVATED_PENALTY
        elif di >= HOLD_DTE_COMFORT_MIN:
            score += HOLD_DTE_COMFORT_BONUS

    # --- Factor 4: Earnings (mild; bypassed entirely in grace via early return) ---
    if group.get("earnings_risk"):
        score -= HOLD_EARNINGS_PENALTY

    return int(max(1, min(5, round(score))))


async def fetch_earnings_date(symbol: str, expiry_str: str) -> Optional[str]:
    """
    Query the EODHD earnings calendar for the next earnings date between today
    and the position's expiry date.

    Returns the earliest earnings date as an ISO string (YYYY-MM-DD) if one
    falls within [today, expiry], else None.

    Uses EODHD_API_TOKEN (same env var as trading_analysis.py / stock_scanner.py).
    Runs the HTTP call in a thread executor to avoid blocking the event loop
    — no new dependencies required (uses the `requests` library already in use).
    """
    if not EODHD_API_TOKEN:
        return None

    try:
        es = expiry_str
        if len(es) == 8 and es.isdigit():
            exp_date_obj = date(int(es[:4]), int(es[4:6]), int(es[6:8]))
        else:
            exp_date_obj = datetime.strptime(es[:10], "%Y-%m-%d").date()
    except Exception:
        return None

    today = date.today()
    if exp_date_obj <= today:
        return None  # position already expired

    today_str  = today.isoformat()
    expiry_iso = exp_date_obj.isoformat()

    url = (
        f"https://eodhd.com/api/calendar/earnings"
        f"?api_token={EODHD_API_TOKEN}"
        f"&symbols={symbol}.US"
        f"&from={today_str}"
        f"&to={expiry_iso}"
        f"&fmt=json"
    )

    def _sync_fetch() -> Optional[str]:
        try:
            import requests as _req
            resp = _req.get(url, timeout=8)
            if resp.status_code != 200:
                return None
            data = resp.json()
            # EODHD returns {"earnings": [...]} or a bare list
            if isinstance(data, list):
                earnings_list = data
            elif isinstance(data, dict):
                earnings_list = data.get("earnings", [])
            else:
                return None
            dates = []
            for item in earnings_list:
                report_date = (item.get("report_date") or item.get("date") or "")[:10]
                if report_date:
                    try:
                        d = datetime.strptime(report_date, "%Y-%m-%d").date()
                        if today <= d <= exp_date_obj:
                            dates.append(d)
                    except Exception:
                        pass
            return min(dates).isoformat() if dates else None
        except Exception as exc:
            logger.debug("EODHD earnings fetch failed for %s: %s", symbol, exc)
            return None

    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _sync_fetch)


def _fetch_eodhd_underlying_price_sync(symbol: str) -> Optional[float]:
    """
    Last-known US equity price from EODHD when IB Gateway has no live quote
    (weekends, after-hours, or missing market data subscription).

    Order: real-time endpoint (often returns last close when the market is closed),
    then daily EOD bars for the most recent trading day.
    """
    if not EODHD_API_TOKEN:
        return None

    sym = symbol.upper().strip()
    candidates: List[str] = []
    if "." in sym:
        candidates.append(sym)
    else:
        candidates.extend([f"{sym}.US", sym])

    try:
        import requests as _req
    except ImportError:
        logger.warning("requests not available for EODHD spot fallback")
        return None

    for c in candidates:
        try:
            url = (
                f"https://eodhd.com/api/real-time/{c}"
                f"?api_token={EODHD_API_TOKEN}&fmt=json"
            )
            resp = _req.get(url, timeout=12)
            if resp.status_code != 200:
                continue
            data = resp.json()
            if not isinstance(data, dict):
                continue
            for key in ("close", "price", "last"):
                val = data.get(key)
                if val is not None:
                    p = _safe_float(val, 0.0)
                    if math.isfinite(p) and p > 0:
                        return p
        except Exception as exc:
            logger.debug("EODHD real-time failed for %s: %s", c, exc)
            continue

    to_d   = date.today()
    from_d = to_d - timedelta(days=21)
    for c in candidates:
        try:
            url = (
                f"https://eodhd.com/api/eod/{c}"
                f"?api_token={EODHD_API_TOKEN}"
                f"&from={from_d.isoformat()}&to={to_d.isoformat()}"
                f"&period=d&fmt=json"
            )
            resp = _req.get(url, timeout=12)
            if resp.status_code != 200:
                continue
            data = resp.json()
            if not isinstance(data, list) or not data:
                continue
            row = data[-1]
            val = row.get("close")
            if val is None:
                val = row.get("adjusted_close")
            if val is not None:
                p = _safe_float(val, 0.0)
                if math.isfinite(p) and p > 0:
                    return p
        except Exception as exc:
            logger.debug("EODHD eod failed for %s: %s", c, exc)
            continue

    return None


async def fetch_open_orders(
    host: str = DEFAULT_HOST,
    port: int = GATEWAY_LIVE_PORT,
    client_id: int = ORDERS_CLIENT_ID,
) -> Dict[str, Any]:
    """
    Connect to IB Gateway (port 4001) and fetch all currently open orders via
    reqAllOpenOrders().

    Returns a list of order dicts containing all fields needed for:
      - Orphan detection (symbol cross-reference against active positions)
      - TP/SL display (lmt_price / aux_price / order_type)
      - Manual cancellation (order_id)
    """
    execution_logs: List[str] = []

    def log(msg: str) -> None:
        execution_logs.append(f"[{_ts()}] {msg}")

    ib: Any = None

    def on_error(*args: Any, **kwargs: Any) -> None:
        parts = [str(a) for a in args] + [f"{k}={v}" for k, v in kwargs.items()]
        log(f"TWS Error — {', '.join(parts)}")

    try:
        ib = await _connect_to_tws(
            host=host,
            port=port,
            client_id=client_id,
            log_cb=log,
            max_attempts=2,
        )
        if hasattr(ib, "errorEvent"):
            ib.errorEvent += on_error

        log("Requesting all open orders…")
        ib.reqAllOpenOrders()
        await asyncio.sleep(2)  # Allow order data to arrive

        open_trades = ib.openTrades()
        log(f"Found {len(open_trades)} open trade(s).")

        # ib_insync uses this sentinel value when a price field is unset
        _IB_UNSET = 1.7976931348623157e+308

        def _price_or_none(val: Any) -> Optional[float]:
            v = _safe_float(val, float("nan"))
            if not math.isfinite(v) or v >= _IB_UNSET * 0.9:
                return None
            return round(v, 4)

        orders: List[Dict[str, Any]] = []
        for trade in open_trades:
            contract    = trade.contract
            order       = trade.order
            order_state = trade.orderStatus
            orders.append({
                "order_id":   order.orderId,
                "perm_id":    getattr(order, "permId", 0),
                "client_id":  getattr(order, "clientId", 0),
                "symbol":     contract.symbol,
                "sec_type":   contract.secType,
                "expiry":     getattr(contract, "lastTradeDateOrContractMonth", ""),
                "strike":     _price_or_none(getattr(contract, "strike", 0)),
                "right":      getattr(contract, "right", ""),
                "action":     order.action,
                "total_qty":  _safe_float(getattr(order, "totalQuantity", 0), 0.0),
                "order_type": order.orderType,
                "lmt_price":  _price_or_none(getattr(order, "lmtPrice", None)),
                "aux_price":  _price_or_none(getattr(order, "auxPrice", None)),
                "status":     getattr(order_state, "status", "Unknown"),
                "parent_id":  getattr(order, "parentId", 0),
                "tif":        getattr(order, "tif", ""),
            })

        log(f"Serialized {len(orders)} order(s).")
        return {"ok": True, "orders": orders, "logs": execution_logs}

    except Exception as e:
        msg = str(e).strip() or e.__class__.__name__
        log(f"Exception: {msg}")
        logger.exception("fetch_open_orders failed: %s", e)
        raise RuntimeError(msg) from e
    finally:
        if ib is not None:
            try:
                if hasattr(ib, "errorEvent"):
                    ib.errorEvent -= on_error
            except Exception:
                pass
            try:
                if ib.isConnected():
                    ib.disconnect()
                    log("Disconnected from Gateway (open orders).")
            except Exception as disc_ex:
                logger.warning("Disconnect warning: %s", disc_ex)
                log(f"Disconnect warning: {disc_ex}")


async def cancel_order(
    order_id: int,
    host: str = DEFAULT_HOST,
    port: int = GATEWAY_LIVE_PORT,
    client_id: int = ORDERS_CLIENT_ID,
) -> Dict[str, Any]:
    """
    Cancel an open order by orderId via IB Gateway.

    Connects to port 4001, locates the order among openTrades(), and calls
    ib.cancelOrder().  Returns an ack dict with ok, order_id, message, logs.
    """
    execution_logs: List[str] = []

    def log(msg: str) -> None:
        execution_logs.append(f"[{_ts()}] {msg}")

    ib: Any = None

    def on_error(*args: Any, **kwargs: Any) -> None:
        parts = [str(a) for a in args] + [f"{k}={v}" for k, v in kwargs.items()]
        log(f"TWS Error — {', '.join(parts)}")

    try:
        ib = await _connect_to_tws(
            host=host,
            port=port,
            client_id=client_id,
            log_cb=log,
            max_attempts=2,
        )
        if hasattr(ib, "errorEvent"):
            ib.errorEvent += on_error

        log(f"Locating orderId={order_id} among open orders…")
        ib.reqAllOpenOrders()
        await asyncio.sleep(2)

        open_trades  = ib.openTrades()
        target_trade = next(
            (t for t in open_trades if t.order.orderId == order_id), None
        )

        if target_trade is None:
            raise RuntimeError(
                f"Order {order_id} not found among {len(open_trades)} open order(s). "
                "It may have already been filled or cancelled."
            )

        log(f"Sending cancel for orderId={order_id}…")
        ib.cancelOrder(target_trade.order)
        await asyncio.sleep(1)

        log(f"Cancel request submitted for orderId={order_id}.")
        logger.info("Cancel order submitted: orderId=%s", order_id)
        return {
            "ok":       True,
            "order_id": order_id,
            "message":  f"Cancel request sent for order {order_id}",
            "logs":     execution_logs,
        }

    except Exception as e:
        msg = str(e).strip() or e.__class__.__name__
        log(f"Exception: {msg}")
        logger.exception("cancel_order failed: %s", e)
        raise RuntimeError(msg) from e
    finally:
        if ib is not None:
            try:
                if hasattr(ib, "errorEvent"):
                    ib.errorEvent -= on_error
            except Exception:
                pass
            try:
                if ib.isConnected():
                    ib.disconnect()
                    log("Disconnected from Gateway (cancel order).")
            except Exception as disc_ex:
                logger.warning("Disconnect warning: %s", disc_ex)
                log(f"Disconnect warning: {disc_ex}")


async def modify_order(
    order_id: int,
    new_price: float,
    host: str = DEFAULT_HOST,
    port: int = GATEWAY_LIVE_PORT,
    client_id: int = ORDERS_CLIENT_ID,
) -> Dict[str, Any]:
    """
    Modify the limit price of an existing open order via IB Gateway.

    Connects to port 4001, locates the order among openTrades(), updates its
    lmtPrice, and resubmits via ib.placeOrder().  Returns an ack dict with
    ok, order_id, new_price, message, logs.
    """
    execution_logs: List[str] = []

    def log(msg: str) -> None:
        execution_logs.append(f"[{_ts()}] {msg}")

    ib: Any = None

    def on_error(*args: Any, **kwargs: Any) -> None:
        parts = [str(a) for a in args] + [f"{k}={v}" for k, v in kwargs.items()]
        log(f"TWS Error — {', '.join(parts)}")

    try:
        ib = await _connect_to_tws(
            host=host,
            port=port,
            client_id=client_id,
            log_cb=log,
            max_attempts=2,
        )
        if hasattr(ib, "errorEvent"):
            ib.errorEvent += on_error

        log(f"Locating orderId={order_id} to modify (new_price={new_price})…")
        ib.reqAllOpenOrders()
        await asyncio.sleep(2)

        open_trades  = ib.openTrades()
        target_trade = next(
            (t for t in open_trades if t.order.orderId == order_id), None
        )

        if target_trade is None:
            raise RuntimeError(
                f"Order {order_id} not found among {len(open_trades)} open order(s). "
                "It may have already been filled or cancelled."
            )

        old_price = _safe_float(getattr(target_trade.order, "lmtPrice", 0.0), 0.0)
        log(f"Found orderId={order_id}  old_lmtPrice={old_price}  → new_lmtPrice={new_price}")

        target_trade.order.lmtPrice = round(new_price, 4)
        ib.placeOrder(target_trade.contract, target_trade.order)
        await asyncio.sleep(1)

        log(f"Modify request submitted for orderId={order_id}.")
        logger.info("Modify order submitted: orderId=%s new_price=%s", order_id, new_price)
        return {
            "ok":        True,
            "order_id":  order_id,
            "new_price": new_price,
            "old_price": old_price,
            "message":   f"Order {order_id} limit price updated to {_format_price_for_log(new_price, 4)}",
            "logs":      execution_logs,
        }

    except Exception as e:
        msg = str(e).strip() or e.__class__.__name__
        log(f"Exception: {msg}")
        logger.exception("modify_order failed: %s", e)
        raise RuntimeError(msg) from e
    finally:
        if ib is not None:
            try:
                if hasattr(ib, "errorEvent"):
                    ib.errorEvent -= on_error
            except Exception:
                pass
            try:
                if ib.isConnected():
                    ib.disconnect()
                    log("Disconnected from Gateway (modify order).")
            except Exception as disc_ex:
                logger.warning("Disconnect warning: %s", disc_ex)
                log(f"Disconnect warning: {disc_ex}")


async def compute_enriched_portfolio(
    host: str = DEFAULT_HOST,
    port: int = GATEWAY_LIVE_PORT,
    client_id: int = PORTFOLIO_CLIENT_ID,
) -> Dict[str, Any]:
    """
    Extended portfolio fetch (Trade Management Command Center).

    Calls fetch_ibkr_portfolio for base data, then enriches every strategy
    group with:
      • breakeven_lower / breakeven_upper   — from short strikes + net credit
      • integrity_warning                    — leg count / qty mismatch
      • active_tp_price / active_sl_price   — cross-referenced from open orders
      • recommendation_score                 — 1-5 Hold/Sell score
      • earnings_risk / earnings_date        — via EODHD earnings calendar

    Also fetches all open orders (clientId=7) and returns them alongside the
    enriched groups so the Flutter client can perform orphan detection.
    """
    execution_logs: List[str] = []

    def log(msg: str) -> None:
        execution_logs.append(f"[{_ts()}] {msg}")

    # ── 1. Base portfolio ──────────────────────────────────────────────────
    log("Fetching base portfolio…")
    portfolio_result = await fetch_ibkr_portfolio(
        host=host, port=port, client_id=client_id
    )
    base_groups: List[Dict[str, Any]] = portfolio_result.get("groups", [])
    execution_logs.extend(portfolio_result.get("logs", []))
    log(f"Base portfolio: {len(base_groups)} group(s).")

    # ── 2. Open orders ─────────────────────────────────────────────────────
    orders: List[Dict[str, Any]] = []
    try:
        orders_result = await fetch_open_orders(
            host=host, port=port, client_id=ORDERS_CLIENT_ID
        )
        orders = orders_result.get("orders", [])
        execution_logs.extend(orders_result.get("logs", []))
        log(f"Open orders: {len(orders)} order(s).")
    except Exception as exc:
        log(f"[orders] Non-fatal: could not fetch open orders — {exc}")

    # Build symbol → orders lookup
    symbol_orders: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for o in orders:
        symbol_orders[o["symbol"]].append(o)

    # ── 3. Enrich each group ───────────────────────────────────────────────
    enriched_groups: List[Dict[str, Any]] = []

    for group in base_groups:
        g        = dict(group)   # mutable copy
        ticker   = g["ticker"]
        quantity = g.get("quantity", 1) or 1
        legs     = g.get("legs", [])

        # ─ Breakevens ─────────────────────────────────────────────────────
        sp_strike = g.get("short_put_strike")
        sc_strike = g.get("short_call_strike")
        credit_per_share = (
            g.get("initial_credit", 0.0) / (quantity * 100)
            if quantity > 0 else 0.0
        )
        if sp_strike and sc_strike and credit_per_share > 0:
            g["breakeven_lower"] = round(sp_strike - credit_per_share, 2)
            g["breakeven_upper"] = round(sc_strike + credit_per_share, 2)
        else:
            g["breakeven_lower"] = None
            g["breakeven_upper"] = None

        # ─ Spot price: EODHD fallback if IB still has no quote ───────────────
        sp0 = _safe_float(g.get("spot_price"), 0.0)
        if sp0 <= 0:
            alt_sp = await asyncio.get_event_loop().run_in_executor(
                None, _fetch_eodhd_underlying_price_sync, ticker
            )
            if alt_sp is not None and alt_sp > 0:
                g["spot_price"] = round(alt_sp, 2)
                g["spot_price_source"] = "eodhd"
                log(f"[{ticker}] spot via EODHD enrichment fallback: {g['spot_price']}")
                # Pin risk uses spot — recompute if we only got spot here
                _dte = g.get("dte")
                _spot = g["spot_price"]
                _spk = g.get("short_put_strike")
                _sck = g.get("short_call_strike")
                _pin = False
                if _dte is not None and _dte <= 2 and _spot > 0:
                    if _spk and abs(_spot - _spk) / _spot <= 0.01:
                        _pin = True
                    if _sck and abs(_spot - _sck) / _spot <= 0.01:
                        _pin = True
                g["pin_risk"] = _pin
            else:
                g["spot_price"] = 0.0
                g["spot_price_source"] = None
        elif not g.get("spot_price_source"):
            g["spot_price_source"] = "ibkr"

        # ─ Integrity check ─────────────────────────────────────────────────
        num_legs   = len(legs)
        strategy   = g.get("strategy_name", "")
        put_legs   = [l for l in legs if l.get("right") == "P"]
        call_legs  = [l for l in legs if l.get("right") == "C"]
        short_legs = [l for l in legs if (l.get("position") or 0) < 0]
        long_legs  = [l for l in legs if (l.get("position") or 0) > 0]

        integrity_warning: Optional[str] = None

        if strategy == "Iron Condor":
            if num_legs != 4 or len(put_legs) != 2 or len(call_legs) != 2:
                integrity_warning = (
                    f"Incomplete Iron Condor: expected 4 legs "
                    f"(2P + 2C), found {num_legs}"
                )
            elif len(short_legs) != 2 or len(long_legs) != 2:
                integrity_warning = (
                    "Iron Condor direction mismatch "
                    "(expected 2 short + 2 long)"
                )
        elif strategy in ("Put Spread", "Call Spread"):
            if num_legs != 2:
                integrity_warning = (
                    f"Incomplete {strategy}: expected 2 legs, found {num_legs}"
                )
            elif len(short_legs) != 1 or len(long_legs) != 1:
                integrity_warning = (
                    f"{strategy} direction mismatch "
                    "(expected 1 short + 1 long)"
                )
        elif strategy == "Strangle / Straddle":
            if num_legs != 2:
                integrity_warning = (
                    f"Incomplete Straddle/Strangle: expected 2 legs, "
                    f"found {num_legs}"
                )

        # Quantity consistency check (all legs should have the same absolute qty)
        if not integrity_warning and legs:
            qtys = [
                abs(_safe_float(l.get("position", 0), 0.0))
                for l in legs if l.get("position")
            ]
            if qtys and (max(qtys) - min(qtys)) > 0.01:
                integrity_warning = (
                    f"Quantity mismatch: legs have unequal sizes "
                    f"({', '.join(str(int(q)) for q in qtys)})"
                )

        g["integrity_warning"] = integrity_warning

        # ─ TP / SL from open orders ────────────────────────────────────────
        ticker_orders = symbol_orders.get(ticker, [])
        tp_price: Optional[float] = None
        sl_price: Optional[float] = None

        for o in ticker_orders:
            otype = (o.get("order_type") or "").upper().replace(" ", "")
            lmt   = o.get("lmt_price")
            aux   = o.get("aux_price")

            if otype in ("LMT", "LIM") and lmt is not None and lmt > 0:
                # TP is a limit buy-to-close; keep the smallest (most conservative)
                if tp_price is None or lmt < tp_price:
                    tp_price = lmt
            elif otype in ("STP", "STPLMT", "TRAIL", "TRAILLMT"):
                stop_val = aux if (aux is not None and aux > 0) else lmt
                if stop_val is not None and stop_val > 0:
                    if sl_price is None or stop_val > sl_price:
                        sl_price = stop_val

        g["active_tp_price"] = round(tp_price, 4) if tp_price is not None else None
        g["active_sl_price"] = round(sl_price, 4) if sl_price is not None else None

        # ─ Earnings risk (EODHD) ───────────────────────────────────────────
        expiry = g.get("expiration", "")
        earnings_date = await fetch_earnings_date(ticker, expiry)
        g["earnings_risk"]  = earnings_date is not None
        g["earnings_date"]  = earnings_date

        # ─ Recommendation score ────────────────────────────────────────────
        g["recommendation_score"] = _compute_hold_score(g)

        enriched_groups.append(g)

    log(f"Enrichment complete: {len(enriched_groups)} group(s).")
    return {
        "ok":     True,
        "groups": enriched_groups,
        "orders": orders,
        "logs":   execution_logs,
    }
