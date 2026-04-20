"""
ibkr.py – FastAPI router for IBKR TWS draft orders and portfolio fetch
-----------------------------------------------------------------------
POST /ibkr/draft_condor         – Send an Iron Condor as a non-transmitting draft to TWS.
POST /ibkr/portfolio            – Fetch all open option positions grouped into strategy groups.
POST /ibkr/portfolio-enriched   – Enriched portfolio: breakevens, integrity, TP/SL, score, earnings.
POST /ibkr/draft_close          – Send a draft close order for an existing strategy (transmit=False, port 7496).
POST /ibkr/send-to-tws          – Send an immediate live close order to TWS (transmit=True, port 7496).
GET  /ibkr/combo-market-data    – Fetch real-time Bid/Ask/Mid for an Iron Condor combo via Gateway.
GET  /ibkr/close-market-data    – Fetch real-time Bid/Ask/Mid for a closing combo by con_id legs via Gateway.
GET  /ibkr/open-orders          – Fetch all open orders from IB Gateway (for orphan detection + TP/SL display).
POST /ibkr/cancel-order         – Cancel a specific open order by orderId via IB Gateway.
POST /ibkr/gateway-execute      – Place a live Iron Condor order via IB Gateway (transmit=True).
POST /ibkr/gateway-close        – Place a live closing order for an existing position via IB Gateway (transmit=True, port 4001).

Hybrid port model:
  • 7496 — Live TWS       (Recommendations / order-execution endpoints, draft_close, send-to-tws)
  • 4001 — IB Gateway     (Portfolio Dashboard reads, open-orders, cancel-order, gateway-close)
  • 7497 (Paper) and all other ports are rejected.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from app.services import ibkr_service

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/ibkr",
    tags=["IBKR"],
    responses={404: {"description": "Not found"}},
)


# ---------------------------------------------------------------------------
# Existing: Draft Iron Condor
# ---------------------------------------------------------------------------

class DraftCondorRequest(BaseModel):
    ticker: str
    exp_date: str  # YYYY-MM-DD or YYYYMMDD
    short_put: float
    long_put: float
    short_call: float
    long_call: float
    net_premium: float
    quantity: int = 1
    take_profit_pct: Optional[float] = None  # Fraction 0–1 of credit to capture; None → HOLD_TP_STRONG_PCT
    stop_loss_pct: float = 1.0               # Ignored; SL child limit uses HOLD_SL_STRONG_CLOSE_PCT in service
    host: Optional[str] = None
    port: Optional[int] = None  # 7496 (TWS Live) or 4001 (IB Gateway); defaults to 7496
    client_id: Optional[int] = None


@router.post("/draft_condor")
async def draft_condor(payload: DraftCondorRequest):
    """
    Place an Iron Condor bracket order as a draft in TWS (parent + TP + SL, all transmit=False).
    TP/SL limits follow ``HOLD_TP_STRONG_PCT`` / ``HOLD_SL_STRONG_CLOSE_PCT`` in ``ibkr_service``.
    The full bracket is queued in TWS and does not execute until the user transmits.
    """
    try:
        result = await ibkr_service.send_iron_condor_draft(
            ticker=payload.ticker,
            exp_date=payload.exp_date,
            short_put=payload.short_put,
            long_put=payload.long_put,
            short_call=payload.short_call,
            long_call=payload.long_call,
            net_premium=payload.net_premium,
            quantity=payload.quantity,
            take_profit_pct=payload.take_profit_pct,
            stop_loss_pct=payload.stop_loss_pct,
            host=payload.host or ibkr_service.DEFAULT_HOST,
            port=payload.port or ibkr_service.DEFAULT_PORT,
            client_id=payload.client_id or ibkr_service.DEFAULT_CLIENT_ID,
        )
        return result
    except RuntimeError as e:
        err_msg = str(e)
        if "connection" in err_msg.lower() or "connect" in err_msg.lower() or "timeout" in err_msg.lower():
            raise HTTPException(status_code=503, detail=err_msg)
        if "qualify" in err_msg.lower():
            raise HTTPException(status_code=400, detail=err_msg)
        raise HTTPException(status_code=500, detail=err_msg)
    except Exception as e:
        logger.exception("draft_condor failed: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


# ---------------------------------------------------------------------------
# New: Portfolio fetch (IBKR Live Portfolio Dashboard)
# ---------------------------------------------------------------------------

class PortfolioRequest(BaseModel):
    host: Optional[str] = None
    port: Optional[int] = None   # 4001 (IB Gateway) or 7496 (TWS Live); defaults to 7496
    client_id: Optional[int] = None


class IBKRLeg(BaseModel):
    con_id: int
    symbol: str
    strike: float
    right: str          # "P" or "C"
    expiry: str
    position: float     # negative = short, positive = long
    market_value: float
    avg_cost: float
    unrealized_pnl: float
    market_price: float
    action_to_close: str  # "BUY" (to close a short) or "SELL" (to close a long)


class IBKRStrategyGroup(BaseModel):
    ticker: str
    strategy_name: str
    legs: List[IBKRLeg]
    unrealized_pnl: float
    initial_credit: float
    max_loss: Optional[float] = None
    pct_max_profit: Optional[float] = None
    pct_max_loss: Optional[float] = None
    expiration: str
    dte: Optional[int] = None
    short_put_strike: Optional[float] = None
    short_call_strike: Optional[float] = None
    spot_price: float
    spot_price_source: Optional[str] = None  # "ibkr" | "eodhd" when spot_price > 0
    quantity: int
    pin_risk: bool


class PortfolioResponse(BaseModel):
    ok: bool
    groups: List[IBKRStrategyGroup]
    logs: List[str] = []


@router.post("/portfolio", response_model=PortfolioResponse)
async def get_portfolio(payload: PortfolioRequest):
    """
    Fetch all open option positions from TWS, grouped into strategy groups.
    Read-only: no orders are placed. Connection is initiated only on this call.
    """
    try:
        result = await ibkr_service.fetch_ibkr_portfolio(
            host=payload.host or ibkr_service.DEFAULT_HOST,
            port=payload.port or ibkr_service.DEFAULT_PORT,
            client_id=payload.client_id or ibkr_service.PORTFOLIO_CLIENT_ID,
        )
        return result
    except RuntimeError as e:
        err_msg = str(e)
        low = err_msg.lower()
        if "timeout" in low or "timed out" in low or "errno 60" in low:
            raise HTTPException(
                status_code=503,
                detail=(
                    "IBKR API timed out. Verify TWS API settings: "
                    "Enable ActiveX/Socket Clients, trusted IP includes 127.0.0.1 and ::1, "
                    "Read-Only API is OFF, and accept API popup in TWS."
                ),
            )
        if "connection" in low or "connect" in low or "refused" in low:
            raise HTTPException(status_code=503, detail=err_msg)
        raise HTTPException(status_code=500, detail=err_msg)
    except Exception as e:
        logger.exception("get_portfolio failed: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


# ---------------------------------------------------------------------------
# New: Draft close order (IBKR Live Portfolio Dashboard)
# ---------------------------------------------------------------------------

class CloseLeg(BaseModel):
    con_id: int
    action_to_close: str  # "BUY" (to close a short leg) or "SELL" (to close a long leg)


class DraftCloseRequest(BaseModel):
    ticker: str
    expiry: str           # YYYYMMDD or YYYY-MM-DD
    legs: List[CloseLeg]
    mid_price: float      # Current mid price of the combo (positive debit to close)
    quantity: int = 1
    host: Optional[str] = None
    port: Optional[int] = None
    client_id: Optional[int] = None


# ---------------------------------------------------------------------------
# New: Combo market-data fetch via IB Gateway (port 4001)
# ---------------------------------------------------------------------------

class ComboMarketDataResponse(BaseModel):
    ok: bool
    bid: Optional[float] = None
    ask: Optional[float] = None
    mid: Optional[float] = None
    logs: List[str] = []


@router.get("/combo-market-data", response_model=ComboMarketDataResponse)
async def combo_market_data(
    ticker: str,
    exp_date: str,
    short_put: float,
    long_put: float,
    short_call: float,
    long_call: float,
    host: Optional[str] = None,
    port: Optional[int] = None,
    client_id: Optional[int] = None,
):
    """
    Fetch real-time Bid / Ask / Mid for an Iron Condor combo directly from
    IB Gateway (port 4001).  Prices are computed by summing individual leg
    market data (short legs at bid/ask, long legs at ask/bid).

    Returns: {ok, bid, ask, mid, logs}
    bid/ask/mid are None when market data is not yet available.
    """
    try:
        result = await ibkr_service.fetch_combo_market_data(
            ticker=ticker,
            exp_date=exp_date,
            short_put=short_put,
            long_put=long_put,
            short_call=short_call,
            long_call=long_call,
            host=host or ibkr_service.DEFAULT_HOST,
            port=port or ibkr_service.GATEWAY_LIVE_PORT,
            client_id=client_id or ibkr_service.GATEWAY_MKTDATA_CLIENT_ID,
        )
        return result
    except RuntimeError as e:
        err_msg = str(e)
        low = err_msg.lower()
        if "connection" in low or "connect" in low or "timeout" in low or "timed out" in low:
            raise HTTPException(status_code=503, detail=err_msg)
        if "qualify" in low:
            raise HTTPException(status_code=400, detail=err_msg)
        raise HTTPException(status_code=500, detail=err_msg)
    except Exception as e:
        logger.exception("combo_market_data failed: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


# ---------------------------------------------------------------------------
# New: Live order placement via IB Gateway (port 4001, transmit=True)
# ---------------------------------------------------------------------------

class GatewayExecuteRequest(BaseModel):
    ticker: str
    exp_date: str           # YYYY-MM-DD or YYYYMMDD
    short_put: float
    long_put: float
    short_call: float
    long_call: float
    limit_price: float      # Credit to receive (positive value, e.g. 1.50)
    quantity: int = 1
    take_profit_pct: Optional[float] = None  # Fraction 0–1 of credit to capture; None → HOLD_TP_STRONG_PCT
    stop_loss_pct: float = 1.0               # Ignored; SL child limit uses HOLD_SL_STRONG_CLOSE_PCT in service
    host: Optional[str] = None
    port: Optional[int] = None   # defaults to GATEWAY_LIVE_PORT (4001)
    client_id: Optional[int] = None


@router.post("/gateway-execute")
async def gateway_execute(payload: GatewayExecuteRequest):
    """
    Place a live Iron Condor bracket order via IB Gateway (transmit=True).
    Sends parent entry + TP child + SL child (standard bracket: SL child transmit=True).
    TP/SL limits follow ``HOLD_TP_STRONG_PCT`` / ``HOLD_SL_STRONG_CLOSE_PCT`` in ``ibkr_service``.
    Uses GATEWAY_LIVE_PORT (4001) exclusively.
    """
    try:
        result = await ibkr_service.send_gateway_live_order(
            ticker=payload.ticker,
            exp_date=payload.exp_date,
            short_put=payload.short_put,
            long_put=payload.long_put,
            short_call=payload.short_call,
            long_call=payload.long_call,
            limit_price=payload.limit_price,
            quantity=payload.quantity,
            take_profit_pct=payload.take_profit_pct,
            stop_loss_pct=payload.stop_loss_pct,
            host=payload.host or ibkr_service.DEFAULT_HOST,
            port=payload.port or ibkr_service.GATEWAY_LIVE_PORT,
            client_id=payload.client_id or ibkr_service.GATEWAY_ORDER_CLIENT_ID,
        )
        return result
    except RuntimeError as e:
        err_msg = str(e)
        low = err_msg.lower()
        if "connection" in low or "connect" in low or "timeout" in low or "timed out" in low:
            raise HTTPException(status_code=503, detail=err_msg)
        if "qualify" in low:
            raise HTTPException(status_code=400, detail=err_msg)
        raise HTTPException(status_code=500, detail=err_msg)
    except Exception as e:
        logger.exception("gateway_execute failed: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/draft_close")
async def draft_close(payload: DraftCloseRequest):
    """
    Place a draft close order for an existing option strategy in TWS (transmit=False).
    Always connects to TWS_LIVE_PORT (7496) — the payload port field is ignored to
    prevent accidental routing to IB Gateway (4001).
    The order is queued in TWS and does not execute until the user manually transmits it.
    """
    try:
        legs_dicts = [{"con_id": leg.con_id, "action_to_close": leg.action_to_close} for leg in payload.legs]
        result = await ibkr_service.send_close_draft(
            ticker=payload.ticker,
            expiry=payload.expiry,
            legs=legs_dicts,
            mid_price=payload.mid_price,
            quantity=payload.quantity,
            host=payload.host or ibkr_service.DEFAULT_HOST,
            port=ibkr_service.TWS_LIVE_PORT,   # always 7496 — never route drafts to Gateway
            client_id=payload.client_id or ibkr_service.PORTFOLIO_CLIENT_ID,
            transmit=False,
        )
        return result
    except RuntimeError as e:
        err_msg = str(e)
        if "connection" in err_msg.lower() or "connect" in err_msg.lower() or "timeout" in err_msg.lower():
            raise HTTPException(status_code=503, detail=err_msg)
        raise HTTPException(status_code=500, detail=err_msg)
    except Exception as e:
        logger.exception("draft_close failed: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


# ---------------------------------------------------------------------------
# Close combo market-data via IB Gateway (for the "Close via Gateway" review dialog)
# ---------------------------------------------------------------------------

@router.get("/close-market-data")
async def close_market_data(
    ticker: str,
    legs: str,  # JSON-encoded list of {con_id, action_to_close}
    host: Optional[str] = None,
    port: Optional[int] = None,
    client_id: Optional[int] = None,
):
    """
    Fetch real-time Bid / Ask / Mid for a closing combo by con_id legs via IB Gateway (port 4001).

    `legs` must be a JSON string: '[{"con_id": 123, "action_to_close": "BUY"}, ...]'
    Always uses GATEWAY_LIVE_PORT (4001) unless overridden.
    Returns: {ok, bid, ask, mid, logs}
    """
    print("DEBUG: Received request at /ibkr/close-market-data")
    import json as _json_mod
    try:
        legs_list = _json_mod.loads(legs)
    except Exception:
        raise HTTPException(status_code=400, detail="legs must be a valid JSON array string")
    try:
        result = await ibkr_service.fetch_close_combo_market_data(
            ticker=ticker,
            legs=legs_list,
            host=host or ibkr_service.DEFAULT_HOST,
            port=port or ibkr_service.GATEWAY_LIVE_PORT,
            client_id=client_id or ibkr_service.GATEWAY_MKTDATA_CLIENT_ID,
        )
        return result
    except RuntimeError as e:
        err_msg = str(e)
        low = err_msg.lower()
        if "connection" in low or "connect" in low or "timeout" in low or "timed out" in low:
            raise HTTPException(status_code=503, detail=err_msg)
        raise HTTPException(status_code=500, detail=err_msg)
    except Exception as e:
        logger.exception("close_market_data failed: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


# ---------------------------------------------------------------------------
# Live gateway close — close an existing position via IB Gateway (transmit=True)
# ---------------------------------------------------------------------------

class GatewayCloseRequest(BaseModel):
    ticker: str
    expiry: str           # YYYYMMDD or YYYY-MM-DD
    legs: List[CloseLeg]
    limit_price: float    # Debit to pay to close (positive value, e.g. 0.85)
    quantity: int = 1
    host: Optional[str] = None
    client_id: Optional[int] = None


@router.post("/gateway-close")
async def gateway_close(payload: GatewayCloseRequest):
    """
    Place a live closing order for an existing option strategy via IB Gateway (transmit=True).
    Uses GATEWAY_LIVE_PORT (4001) exclusively — no TWS fallback.
    The order fires immediately to the exchange — no manual TWS approval required.
    No bracket children (TP/SL) are attached; this is a simple flat-close order.
    """
    try:
        legs_dicts = [{"con_id": leg.con_id, "action_to_close": leg.action_to_close} for leg in payload.legs]
        result = await ibkr_service.send_close_draft(
            ticker=payload.ticker,
            expiry=payload.expiry,
            legs=legs_dicts,
            mid_price=payload.limit_price,
            quantity=payload.quantity,
            host=payload.host or ibkr_service.DEFAULT_HOST,
            port=ibkr_service.GATEWAY_LIVE_PORT,   # always 4001 for live gateway close
            client_id=payload.client_id or ibkr_service.GATEWAY_ORDER_CLIENT_ID,
            transmit=True,
        )
        return result
    except RuntimeError as e:
        err_msg = str(e)
        low = err_msg.lower()
        if "connection" in low or "connect" in low or "timeout" in low or "timed out" in low:
            raise HTTPException(status_code=503, detail=err_msg)
        raise HTTPException(status_code=500, detail=err_msg)
    except Exception as e:
        logger.exception("gateway_close failed: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


# ---------------------------------------------------------------------------
# Trade Management Command Center — new endpoints
# ---------------------------------------------------------------------------

# ── Enriched portfolio ────────────────────────────────────────────────────────

class IBKRStrategyGroupEnriched(IBKRStrategyGroup):
    """Strategy group extended with enrichment data for the Command Center."""
    breakeven_lower: Optional[float] = None
    breakeven_upper: Optional[float] = None
    integrity_warning: Optional[str] = None
    active_tp_price: Optional[float] = None
    active_sl_price: Optional[float] = None
    recommendation_score: Optional[int] = None
    earnings_risk: bool = False
    earnings_date: Optional[str] = None
    rec_key: Optional[str] = None
    entry_credit_known: Optional[float] = None
    entry_pop_known: Optional[float] = None


class IBKROpenOrderResponse(BaseModel):
    order_id: int
    perm_id: Optional[int] = None
    client_id: Optional[int] = None
    symbol: str
    sec_type: str = ""
    expiry: str = ""
    strike: Optional[float] = None
    right: str = ""
    action: str
    total_qty: float
    order_type: str
    lmt_price: Optional[float] = None
    aux_price: Optional[float] = None
    status: str
    parent_id: int = 0
    tif: str = ""


class EnrichedPortfolioResponse(BaseModel):
    ok: bool
    groups: List[IBKRStrategyGroupEnriched]
    orders: List[IBKROpenOrderResponse] = []
    logs: List[str] = []


@router.post("/portfolio-enriched", response_model=EnrichedPortfolioResponse)
async def get_portfolio_enriched(payload: PortfolioRequest):
    """
    Enriched portfolio fetch for the Trade Management Command Center.

    Extends the base /portfolio response with:
      • Breakeven lower/upper (from short strikes and net credit per share)
      • Integrity warning   (validates leg count and quantity balance per strategy)
      • Active TP/SL prices (cross-referenced from open orders on the same symbol)
      • Recommendation score 1–5 (Hold/Sell composite based on P&L, DTE, BE distance, earnings)
      • Earnings risk flag + date (EODHD earnings calendar, today → expiry window)

    Also returns the full open-orders list so the Flutter client can perform
    orphan detection without a separate HTTP round-trip.
    """
    try:
        result = await ibkr_service.compute_enriched_portfolio(
            host=payload.host or ibkr_service.DEFAULT_HOST,
            port=payload.port or ibkr_service.GATEWAY_LIVE_PORT,
            client_id=payload.client_id or ibkr_service.PORTFOLIO_CLIENT_ID,
        )
        return result
    except RuntimeError as e:
        err_msg = str(e)
        low = err_msg.lower()
        if "timeout" in low or "timed out" in low or "errno 60" in low:
            raise HTTPException(
                status_code=503,
                detail=(
                    "IBKR API timed out. Verify IB Gateway settings: "
                    "Enable ActiveX/Socket Clients, trusted IP 127.0.0.1 and ::1, "
                    "Read-Only API is OFF, and dismiss any permission popup."
                ),
            )
        if "connection" in low or "connect" in low or "refused" in low:
            raise HTTPException(status_code=503, detail=err_msg)
        raise HTTPException(status_code=500, detail=err_msg)
    except Exception as e:
        logger.exception("get_portfolio_enriched failed: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


# ── Open orders ───────────────────────────────────────────────────────────────

class OpenOrdersResponse(BaseModel):
    ok: bool
    orders: List[IBKROpenOrderResponse] = []
    logs: List[str] = []


@router.get("/open-orders", response_model=OpenOrdersResponse)
async def get_open_orders(
    port: Optional[int] = None,
    host: Optional[str] = None,
    client_id: Optional[int] = None,
):
    """
    Fetch all open orders from IB Gateway (port 4001).

    Returns every pending order regardless of which client placed it.
    Use the result to:
      - Display TP/SL orders alongside positions
      - Detect orphaned orders (order exists but no matching open position)
    """
    try:
        result = await ibkr_service.fetch_open_orders(
            host=host or ibkr_service.DEFAULT_HOST,
            port=port or ibkr_service.GATEWAY_LIVE_PORT,
            client_id=client_id or ibkr_service.ORDERS_CLIENT_ID,
        )
        return result
    except RuntimeError as e:
        err_msg = str(e)
        low = err_msg.lower()
        if "connection" in low or "connect" in low or "timeout" in low or "timed out" in low:
            raise HTTPException(status_code=503, detail=err_msg)
        raise HTTPException(status_code=500, detail=err_msg)
    except Exception as e:
        logger.exception("get_open_orders failed: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


# ── Cancel order ──────────────────────────────────────────────────────────────

class CancelOrderRequest(BaseModel):
    order_id: int
    host: Optional[str] = None
    port: Optional[int] = None
    client_id: Optional[int] = None


@router.post("/cancel-order")
async def cancel_order(payload: CancelOrderRequest):
    """
    Cancel an open order by orderId via IB Gateway.

    Connects to port 4001, locates the order by orderId, and sends a cancel
    request.  Returns an ack immediately; actual cancellation confirmation
    comes from the exchange asynchronously.
    """
    try:
        result = await ibkr_service.cancel_order(
            order_id=payload.order_id,
            host=payload.host or ibkr_service.DEFAULT_HOST,
            port=payload.port or ibkr_service.GATEWAY_LIVE_PORT,
            client_id=payload.client_id or ibkr_service.ORDERS_CLIENT_ID,
        )
        return result
    except RuntimeError as e:
        err_msg = str(e)
        low = err_msg.lower()
        if "connection" in low or "connect" in low or "timeout" in low or "timed out" in low:
            raise HTTPException(status_code=503, detail=err_msg)
        if "not found" in low:
            raise HTTPException(status_code=404, detail=err_msg)
        raise HTTPException(status_code=500, detail=err_msg)
    except Exception as e:
        logger.exception("cancel_order failed: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


# ── Send-to-TWS (immediate, transmit=True, port 7496) ────────────────────────

class ModifyOrderRequest(BaseModel):
    order_id: int
    new_price: float    # New limit price (positive value)
    host: Optional[str] = None
    port: Optional[int] = None
    client_id: Optional[int] = None


@router.post("/modify-order")
async def modify_order(payload: ModifyOrderRequest):
    """
    Modify the limit price of an existing open order via IB Gateway (port 4001).

    Locates the order by orderId among openTrades(), updates its lmtPrice,
    and resubmits via placeOrder(). Returns {ok, order_id, new_price, old_price, message, logs}.
    """
    try:
        result = await ibkr_service.modify_order(
            order_id=payload.order_id,
            new_price=payload.new_price,
            host=payload.host or ibkr_service.DEFAULT_HOST,
            port=payload.port or ibkr_service.GATEWAY_LIVE_PORT,
            client_id=payload.client_id or ibkr_service.ORDERS_CLIENT_ID,
        )
        return result
    except RuntimeError as e:
        err_msg = str(e)
        low = err_msg.lower()
        if "connection" in low or "connect" in low or "timeout" in low or "timed out" in low:
            raise HTTPException(status_code=503, detail=err_msg)
        if "not found" in low:
            raise HTTPException(status_code=404, detail=err_msg)
        raise HTTPException(status_code=500, detail=err_msg)
    except Exception as e:
        logger.exception("modify_order failed: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/send-to-tws")
async def send_to_tws(payload: DraftCloseRequest):
    """
    Send an immediate live close order to TWS (port 7496, transmit=True).

    Unlike /draft_close (which queues transmit=False in TWS), this endpoint
    transmits the order directly to the exchange via TWS Live.
    Uses TWS_LIVE_PORT (7496) exclusively — no Gateway (4001) fallback.
    The order executes immediately without requiring manual TWS approval.
    """
    try:
        legs_dicts = [{"con_id": leg.con_id, "action_to_close": leg.action_to_close} for leg in payload.legs]
        result = await ibkr_service.send_close_draft(
            ticker=payload.ticker,
            expiry=payload.expiry,
            legs=legs_dicts,
            mid_price=payload.mid_price,
            quantity=payload.quantity,
            host=payload.host or ibkr_service.DEFAULT_HOST,
            port=ibkr_service.TWS_LIVE_PORT,   # always 7496 — TWS Live
            client_id=payload.client_id or ibkr_service.PORTFOLIO_CLIENT_ID,
            transmit=True,
        )
        return result
    except RuntimeError as e:
        err_msg = str(e)
        low = err_msg.lower()
        if "connection" in low or "connect" in low or "timeout" in low or "timed out" in low:
            raise HTTPException(status_code=503, detail=err_msg)
        raise HTTPException(status_code=500, detail=err_msg)
    except Exception as e:
        logger.exception("send_to_tws failed: %s", e)
        raise HTTPException(status_code=500, detail=str(e))
