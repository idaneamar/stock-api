import json
import logging
from typing import Dict, List

from fastapi import APIRouter, HTTPException, Query, status
from fastapi.responses import Response
from app.schemas.response import StandardResponse
from app.schemas.trade import ClosedTradeCreate, OpenTradeCreate, OpenTradeUpdateRequest
from app.services.trade_service import TradeService

router = APIRouter(
    prefix="/trades",
    tags=["Trades"],
    responses={404: {"description": "Not found"}},
)

trade_service = TradeService()


@router.get("/open", response_model=StandardResponse)
async def get_open_trades(
    page: int = Query(1, ge=1, description="Page number (1-indexed)"),
    page_size: int = Query(10, ge=1, le=100, description="Number of trades per page"),
):
    """Get paginated open trades."""
    try:
        paginated_trades = trade_service.get_open_trades(page=page, page_size=page_size)

        retrieved_count = len(paginated_trades.get("items", []))
        total_records = paginated_trades.get("total", 0)

        if total_records == 0:
            message = "No open trades found"
        else:
            total_pages = paginated_trades.get("total_pages", 1) or 1
            message = (
                f"Retrieved {retrieved_count} open trades "
                f"(page {paginated_trades.get('page', page)} of {total_pages})"
            )

        return StandardResponse(
            success=True,
            status=200,
            message=message,
            data=paginated_trades,
        )
    except HTTPException:
        raise
    except Exception as exc:
        logging.error(f"Error retrieving open trades: {exc}")
        raise HTTPException(
            status_code=500,
            detail=f"Error retrieving open trades: {exc}",
        ) from exc


@router.get("/closed", response_model=StandardResponse)
async def get_closed_trades(
    page: int = Query(1, ge=1, description="Page number (1-indexed)"),
    page_size: int = Query(10, ge=1, le=100, description="Number of trades per page"),
):
    """Get paginated closed trades."""
    try:
        paginated_trades = trade_service.get_closed_trades(page=page, page_size=page_size)

        retrieved_count = len(paginated_trades.get("items", []))
        total_records = paginated_trades.get("total", 0)

        if total_records == 0:
            message = "No closed trades found"
        else:
            total_pages = paginated_trades.get("total_pages", 1) or 1
            message = (
                f"Retrieved {retrieved_count} closed trades "
                f"(page {paginated_trades.get('page', page)} of {total_pages})"
            )

        return StandardResponse(
            success=True,
            status=200,
            message=message,
            data=paginated_trades,
        )
    except HTTPException:
        raise
    except Exception as exc:
        logging.error(f"Error retrieving closed trades: {exc}")
        raise HTTPException(
            status_code=500,
            detail=f"Error retrieving closed trades: {exc}",
        ) from exc


@router.get("/open/export")
async def export_open_trades():
    """Export all open trades as JSON matching the import payload structure."""

    try:
        export_payload = trade_service.export_open_trades()
        return Response(
            content=json.dumps(export_payload, indent=2),
            media_type="application/json",
            headers={"Content-Disposition": 'attachment; filename="open_trades_export.json"'},
        )
    except Exception as exc:
        logging.error(f"Error exporting open trades: {exc}")
        raise HTTPException(
            status_code=500,
            detail=f"Error exporting open trades: {exc}",
        ) from exc


@router.get("/closed/export")
async def export_closed_trades():
    """Export all closed trades as JSON matching the import payload structure."""

    try:
        export_payload = trade_service.export_closed_trades()
        return Response(
            content=json.dumps(export_payload, indent=2),
            media_type="application/json",
            headers={"Content-Disposition": 'attachment; filename="closed_trades_export.json"'},
        )
    except Exception as exc:
        logging.error(f"Error exporting closed trades: {exc}")
        raise HTTPException(
            status_code=500,
            detail=f"Error exporting closed trades: {exc}",
        ) from exc


@router.post("/open", response_model=StandardResponse, status_code=status.HTTP_201_CREATED)
async def create_open_trades(payload: Dict[str, OpenTradeCreate]):
    """Create open trades in bulk from the provided payload."""

    if not payload:
        raise HTTPException(status_code=400, detail="Request body must include at least one open trade")

    try:
        normalized_payload = []
        for symbol, trade in payload.items():
            trade_data = trade.model_dump(mode="python")
            trade_data["symbol"] = trade_data.get("symbol") or symbol
            normalized_payload.append(trade_data)

        result = trade_service.create_open_trades(normalized_payload)
        created_trades = result.get("created", [])
        skipped_symbols = result.get("skipped", [])

        message_suffix = f" (skipped {len(skipped_symbols)} existing symbols)" if skipped_symbols else ""

        return StandardResponse(
            success=True,
            status=status.HTTP_201_CREATED,
            message=f"Inserted {len(created_trades)} open trades{message_suffix}",
            data={
                "items": created_trades,
                "count": len(created_trades),
                "skipped_symbols": skipped_symbols,
            },
        )
    except HTTPException:
        raise
    except Exception as exc:
        logging.error(f"Error creating open trades: {exc}")
        raise HTTPException(
            status_code=500,
            detail=f"Error creating open trades: {exc}",
        ) from exc


@router.post("/closed", response_model=StandardResponse, status_code=status.HTTP_201_CREATED)
async def create_closed_trades(payload: List[ClosedTradeCreate]):
    """Create closed trades in bulk from the provided payload."""

    if not payload:
        raise HTTPException(status_code=400, detail="Request body must include at least one closed trade")

    try:
        serialized_payload = [trade.model_dump(mode="python") for trade in payload]
        created_trades = trade_service.create_closed_trades(serialized_payload)

        return StandardResponse(
            success=True,
            status=status.HTTP_201_CREATED,
            message=f"Inserted {len(created_trades)} closed trades",
            data={"items": created_trades, "count": len(created_trades)},
        )
    except HTTPException:
        raise
    except Exception as exc:
        logging.error(f"Error creating closed trades: {exc}")
        raise HTTPException(
            status_code=500,
            detail=f"Error creating closed trades: {exc}",
        ) from exc


@router.delete("/open/{trade_id}", response_model=StandardResponse)
async def delete_open_trade(trade_id: int):
    """Delete an open trade by ID."""
    try:
        deleted = trade_service.delete_open_trade(trade_id)
        if not deleted:
            raise HTTPException(status_code=404, detail=f"Open trade with ID {trade_id} not found")

        return StandardResponse(
            success=True,
            status=200,
            message=f"Deleted open trade with ID {trade_id}",
            data={"deleted_trade_id": trade_id},
        )
    except HTTPException:
        raise
    except Exception as exc:
        logging.error(f"Error deleting open trade {trade_id}: {exc}")
        raise HTTPException(
            status_code=500,
            detail=f"Error deleting open trade: {exc}",
        ) from exc


@router.patch("/open/{trade_id}", response_model=StandardResponse)
async def update_open_trade(trade_id: int, payload: OpenTradeUpdateRequest):
    """Update the target date on an open trade by ID."""

    updates = payload.model_dump(exclude_unset=True)
    if "target_date" not in updates:
        raise HTTPException(status_code=400, detail="target_date must be provided")

    try:
        updated_trade = trade_service.update_open_trade(trade_id, updates)
        if updated_trade is None:
            raise HTTPException(status_code=404, detail=f"Open trade with ID {trade_id} not found")

        return StandardResponse(
            success=True,
            status=200,
            message=f"Updated target date for open trade {trade_id}",
            data=updated_trade,
        )
    except HTTPException:
        raise
    except Exception as exc:
        logging.error(f"Error updating open trade {trade_id}: {exc}")
        raise HTTPException(
            status_code=500,
            detail=f"Error updating open trade: {exc}",
        ) from exc
