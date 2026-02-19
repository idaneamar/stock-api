import logging
import io
import json
import base64
from decimal import Decimal, InvalidOperation
import pandas as pd
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse
from typing import Dict, List, Optional, Any
from datetime import datetime, timezone
from app.schemas.response import StandardResponse, PlaceModifiedOrdersRequest
from app.schemas.analysis import ActiveTradesUpdateRequest
from app.services.trading_analysis import TradingAnalysisService
from app.services.websocket_manager import trading_websocket_manager
from app.models.database import SessionLocal
from app.models.scan import ScanRecord
from app.models.trade import OpenTrade, ClosedTrade

router = APIRouter(
    prefix="/analysis",
    tags=["Analysis"],
    responses={404: {"description": "Not found"}},
)

# Initialize services
trading_analysis_service = TradingAnalysisService()

# Safety cap for order quantities (shares).
MAX_SHARES_PER_ORDER = 500


def _is_missing_protection(value: Optional[Decimal]) -> bool:
    """Return True if stop-loss/take-profit value is effectively missing."""
    if value is None:
        return True
    try:
        return Decimal(value) <= 0
    except (InvalidOperation, TypeError):
        return True


def _archive_open_trade(
    db,
    open_trade: OpenTrade,
    *,
    close_reason: str,
    exit_price: Optional[Decimal] = None,
    exit_time: Optional[datetime] = None,
    remove_open_trade: bool = True,
    fallback_scan_id: Optional[int] = None,
    fallback_analysis_type: Optional[str] = None,
):
    """Persist a ClosedTrade entry derived from an existing OpenTrade."""

    exit_timestamp = exit_time or datetime.now(timezone.utc)
    resolved_exit_price = exit_price if exit_price is not None else open_trade.entry_price

    closed_trade = ClosedTrade(
        symbol=open_trade.symbol,
        action=open_trade.action,
        quantity=open_trade.quantity,
        entry_price=open_trade.entry_price,
        exit_price=resolved_exit_price,
        entry_date=open_trade.entry_date,
        exit_date=exit_timestamp,
        close_reason=close_reason,
        strategy=open_trade.strategy,
        scan_id=open_trade.scan_id or fallback_scan_id,
        analysis_type=open_trade.analysis_type or fallback_analysis_type,
        stop_loss=open_trade.stop_loss,
        take_profit=open_trade.take_profit,
        target_date=open_trade.target_date,
    )

    db.add(closed_trade)
    if remove_open_trade:
        db.delete(open_trade)

    return closed_trade


async def _send_close_position_command(positions_to_close: List[Dict[str, Any]]):
    """Send close position commands to the local trading client via WebSocket"""
    if not positions_to_close:
        return

    try:
        close_data = {
            'positions': positions_to_close
        }

        recipients = await trading_websocket_manager.send_close_positions_event(close_data)

        if recipients > 0:
            logging.info(f"✅ Sent close commands for {len(positions_to_close)} positions to {recipients} client(s)")
        else:
            logging.warning(f"⚠️ No trading clients available to receive close commands for {len(positions_to_close)} positions")

    except Exception as e:
        logging.error(f"❌ Failed to send close position commands: {e}")


def _apply_trade_maintenance_rules(db) -> Dict[str, List[str]]:
    """Apply automated closure rules before generating new trading orders."""

    now_utc = datetime.now(timezone.utc)
    today = now_utc.date()
    closed_by_reason: Dict[str, List[str]] = {"target_date": [], "missing_protection": []}
    positions_to_close: List[Dict[str, Any]] = []

    open_positions: List[OpenTrade] = list(db.query(OpenTrade).all())

    for trade in open_positions:
        if trade.target_date and today >= trade.target_date:
            _archive_open_trade(
                db,
                trade,
                close_reason="Target date reached",
                exit_time=now_utc,
            )
            closed_by_reason["target_date"].append(trade.symbol)

            # Add to close command list for IBKR
            positions_to_close.append({
                'symbol': trade.symbol,
                'quantity': trade.quantity,
                'action': trade.action,
                'reason': 'Target date reached'
            })
            continue

        if _is_missing_protection(trade.stop_loss) or _is_missing_protection(trade.take_profit):
            _archive_open_trade(
                db,
                trade,
                close_reason="Missing stop loss or take profit",
                exit_time=now_utc,
            )
            closed_by_reason["missing_protection"].append(trade.symbol)

            # Add to close command list for IBKR
            positions_to_close.append({
                'symbol': trade.symbol,
                'quantity': trade.quantity,
                'action': trade.action,
                'reason': 'Missing stop loss or take profit'
            })

    if closed_by_reason["target_date"] or closed_by_reason["missing_protection"]:
        logging.info(
            "⚠️ Applied maintenance rules: closed %s due to target date, %s due to missing protection",
            len(closed_by_reason["target_date"]),
            len(closed_by_reason["missing_protection"]),
        )
        db.flush()

        # Send close commands to IBKR via WebSocket (run in background)
        if positions_to_close:
            import asyncio
            try:
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    asyncio.create_task(_send_close_position_command(positions_to_close))
                else:
                    loop.run_until_complete(_send_close_position_command(positions_to_close))
            except Exception as e:
                logging.error(f"Failed to send close commands: {e}")

    return closed_by_reason

@router.get("/excel/{scan_id}")
async def get_analysis_excel(scan_id: int):
    """Download an Excel file for a scan containing a single `analysis` sheet."""
    try:
        logging.info(f"📥 Analysis Excel request for scan_id: {scan_id}")

        def _to_rows(trades: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
            rows: List[Dict[str, Any]] = []
            for trade in trades or []:
                if not isinstance(trade, dict):
                    continue
                recommendation = trade.get("recommendation")
                exit_date = trade.get("exit_date", "-")
                rows.append(
                    {
                        "symbol": trade.get("symbol"),
                        "price": trade.get("entry_price"),
                        "recommendation": recommendation,
                        "position_size": trade.get("position_size"),
                        "stop_loss": trade.get("stop_loss"),
                        "take_profit": trade.get("take_profit"),
                        "sell_time_after_buy": exit_date if recommendation == "Buy" else "-",
                        "buy_time_after_sell": exit_date if recommendation == "Sell" else "-",
                        "strategy": trade.get("strategy"),
                        "backtest_win_rate": trade.get("backtest_win_rate", 0.0),
                        "adx": trade.get("adx", 0.0),
                        "rsi": trade.get("rsi", 0.0),
                        "macd": trade.get("macd", 0.0),
                        "volume_spike": trade.get("volume_spike", False),
                        "score": trade.get("score", 0),
                        "score_notes": trade.get("score_notes", ""),
                    }
                )
            return rows

        db = SessionLocal()
        try:
            scan_record = db.query(ScanRecord).filter(ScanRecord.id == scan_id).first()
            if not scan_record:
                raise HTTPException(status_code=404, detail=f"Scan with ID {scan_id} not found")

            analysis = scan_record.analysis_results or []

            if scan_record.analysis_status != "completed":
                raise HTTPException(
                    status_code=400,
                    detail=f"Analysis not completed for scan {scan_id}. Current status: {scan_record.analysis_status or 'not_started'}",
                )

            if not analysis:
                raise HTTPException(status_code=404, detail=f"Analysis results not found for scan {scan_id}")

            output = io.BytesIO()

            with pd.ExcelWriter(output, engine="openpyxl") as writer:
                df_analysis = trading_analysis_service.generate_excel_data(_to_rows(analysis))
                if df_analysis is None:
                    raise HTTPException(status_code=404, detail=f"No trade data found for scan {scan_id}")
                df_analysis.to_excel(writer, sheet_name="analysis", index=False)

                # --- Metadata sheet (engine + active program + effective rules) ---
                try:
                    from app.models.settings import Settings
                    active = Settings.get_active_program(db)
                    criteria = scan_record.criteria or {}
                    meta_rows = [
                        {"key": "scan_id", "value": str(scan_id)},
                        {"key": "created_at", "value": scan_record.created_at.isoformat() if scan_record.created_at else ""},
                        {"key": "analyzed_at", "value": scan_record.analyzed_at.isoformat() if scan_record.analyzed_at else ""},
                        {"key": "portfolio_size", "value": str(scan_record.portfolio_size or "")},
                        {"key": "program_id", "value": str(criteria.get("program_id") or active.get("active_program_id") or "")},
                        {"key": "ignore_vix", "value": str(bool(criteria.get("ignore_vix", False)))},
                        {"key": "strict_rules", "value": str(bool(criteria.get("strict_rules", True)))},
                        {"key": "adx_min", "value": str(criteria.get("adx_min", ""))},
                        {"key": "volume_spike_required", "value": str(criteria.get("volume_spike_required", ""))},
                        {"key": "daily_loss_limit_pct", "value": str(criteria.get("daily_loss_limit_pct", ""))},
                        {"key": "allow_intraday_prices", "value": str(bool(criteria.get("allow_intraday_prices", False)))},
                    ]
                    pd.DataFrame(meta_rows).to_excel(writer, sheet_name="metadata", index=False)
                except Exception:
                    pass

            output.seek(0)
            filename = f"analysis_scan_{scan_id}.xlsx"
            logging.info(f"✅ Generated analysis Excel file for scan {scan_id}")

            return StreamingResponse(
                io.BytesIO(output.read()),
                media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                headers={"Content-Disposition": f"attachment; filename={filename}"},
            )
        finally:
            db.close()
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Error generating analysis Excel for scan {scan_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error generating analysis Excel file: {str(e)}")


@router.get("/active-trades/{scan_id}", response_model=StandardResponse)
async def get_analysis_active_trades(scan_id: int):
    """Fetch analysis results for a scan (single unified list)."""
    try:
        db = SessionLocal()
        try:
            scan_record = db.query(ScanRecord).filter(ScanRecord.id == scan_id).first()
            if not scan_record:
                raise HTTPException(status_code=404, detail=f"Scan with ID {scan_id} not found")

            return StandardResponse(
                success=True,
                status=200,
                message="Analysis retrieved successfully",
                data={
                    "scan_id": scan_record.id,
                    "status": scan_record.analysis_status or "not_started",
                    "progress": scan_record.analysis_progress or 0,
                    "analyzed_at": scan_record.analyzed_at.isoformat() if scan_record.analyzed_at else None,
                    "portfolio_size": float(scan_record.portfolio_size) if scan_record.portfolio_size else 350000.0,
                    "analysis": scan_record.analysis_results or [],
                },
            )
        finally:
            db.close()
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Error retrieving analysis for scan {scan_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error retrieving analysis: {str(e)}")


@router.get("/active-trades", response_model=StandardResponse)
@router.get("/active_trades", response_model=StandardResponse, include_in_schema=False)  # Legacy endpoint for backward compatibility  
async def get_all_active_trades(
    start_date: Optional[str] = Query(None, description="Start date filter in YYYY-MM-DD format (optional)"),
    end_date: Optional[str] = Query(None, description="End date filter in YYYY-MM-DD format (optional)")
):
    """
    Get combined analysis trades from all scans with optional date filtering.
    
    Args:
        start_date: Optional start date filter in YYYY-MM-DD format
        end_date: Optional end date filter in YYYY-MM-DD format
    
    Returns all active trades from scans, filtered by:
    - Date range (if provided) - filters by scan creation date
    
    Each trade object includes:
    - scan_id: The ID of the scan this trade belongs to
    - All original trade fields (symbol, recommendation, entry_price, etc.)
    """
    try:
        from datetime import datetime
        
        db = SessionLocal()
        try:
            # Start with base query
            query = db.query(ScanRecord)
            
            # Apply date filtering if provided
            if start_date or end_date:
                if start_date:
                    try:
                        start_datetime = datetime.strptime(start_date, "%Y-%m-%d")
                        query = query.filter(ScanRecord.created_at >= start_datetime)
                    except ValueError:
                        raise HTTPException(
                            status_code=400,
                            detail="Invalid start_date format. Use YYYY-MM-DD format."
                        )
                
                if end_date:
                    try:
                        end_datetime = datetime.strptime(end_date, "%Y-%m-%d").replace(hour=23, minute=59, second=59)
                        query = query.filter(ScanRecord.created_at <= end_datetime)
                    except ValueError:
                        raise HTTPException(
                            status_code=400,
                            detail="Invalid end_date format. Use YYYY-MM-DD format."
                        )
                
                # Validate date range if both provided
                if start_date and end_date:
                    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
                    end_dt = datetime.strptime(end_date, "%Y-%m-%d")
                    if start_dt > end_dt:
                        raise HTTPException(
                            status_code=400,
                            detail="Start date cannot be after end date."
                        )
            
            # Only include completed analyses
            query = query.filter(ScanRecord.analysis_status == "completed")
            
            # Order by creation date (newest first)
            scan_records = query.order_by(ScanRecord.created_at.desc()).all()
            
            analysis_trades: List[Dict[str, Any]] = []

            for scan_record in scan_records:
                analyzed_at = scan_record.analyzed_at.isoformat() if scan_record.analyzed_at else None
                scan_created_at = scan_record.created_at.isoformat() if scan_record.created_at else None

                for trade in (scan_record.analysis_results or []):
                    if isinstance(trade, dict):
                        t = trade.copy()
                        t["scan_id"] = scan_record.id
                        t["scan_created_at"] = scan_created_at
                        t["analyzed_at"] = analyzed_at
                        analysis_trades.append(t)
            
            # Create descriptive message
            filter_desc = []
            if start_date or end_date:
                if start_date and end_date:
                    filter_desc.append(f"between {start_date} and {end_date}")
                elif start_date:
                    filter_desc.append(f"from {start_date}")
                elif end_date:
                    filter_desc.append(f"until {end_date}")
            
            filter_text = f" {' '.join(filter_desc)}" if filter_desc else ""
            
            message = (
                f"Retrieved {len(analysis_trades)} analysis trades "
                f"from {len(scan_records)} scans{filter_text}"
            )
            
            return StandardResponse(
                success=True,
                status=200,
                message=message,
                data={
                    "total_scans": len(scan_records),
                    "date_filter": {"start_date": start_date, "end_date": end_date},
                    "analysis": analysis_trades,
                },
            )
            
        finally:
            db.close()
            
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Error retrieving combined active trades: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error retrieving combined active trades: {str(e)}"
        )


@router.patch("/active-trades/{scan_id}", response_model=StandardResponse)
async def update_analysis_active_trades(
    scan_id: int,
    payload: ActiveTradesUpdateRequest,
):
    """Update one or more trades for the provided scan."""

    if not payload.updates:
        raise HTTPException(status_code=400, detail="No updates provided")

    db = SessionLocal()
    try:
        scan_record = db.query(ScanRecord).filter(ScanRecord.id == scan_id).first()
        if not scan_record:
            raise HTTPException(status_code=404, detail=f"Scan with ID {scan_id} not found")

        trades_list = [trade.copy() for trade in (scan_record.analysis_results or []) if isinstance(trade, dict)]

        updated_symbols = []
        missing_symbols = []

        for update in payload.updates:
            update_data = update.model_dump(exclude_unset=True)
            symbol = update_data.pop("symbol", None)
            if not symbol:
                continue
            updated_any = False

            target_trade = next((t for t in trades_list if t.get("symbol") == symbol), None)
            if target_trade:
                for field, value in update_data.items():
                    target_trade[field] = value
                updated_any = True

            if updated_any:
                updated_symbols.append(symbol)
            else:
                missing_symbols.append(symbol)

        if not updated_symbols:
            raise HTTPException(
                status_code=404,
                detail="No matching trades found for provided symbols",
            )

        scan_record.analysis_results = trades_list

        try:
            db.commit()
        except Exception as commit_error:
            db.rollback()
            logging.error(
                f"Failed to persist trade updates for scan {scan_id}: {commit_error}"
            )
            raise HTTPException(status_code=500, detail="Failed to persist trade updates") from commit_error

        response_data = {
            "updated_symbols": updated_symbols,
            "not_found_symbols": missing_symbols or None,
        }

        return StandardResponse(
            success=True,
            status=200,
            message=f"Updated {len(updated_symbols)} trade(s) successfully",
            data=response_data,
        )
    finally:
        db.close()


@router.delete("/active-trades/{scan_id}", response_model=StandardResponse)
async def delete_analysis_active_trade(
    scan_id: int,
    symbol: str = Query(..., description="Symbol of the trade to delete"),
):
    """Delete a single trade from the specified scan."""

    db = SessionLocal()
    try:
        scan_record = db.query(ScanRecord).filter(ScanRecord.id == scan_id).first()
        if not scan_record:
            raise HTTPException(status_code=404, detail=f"Scan with ID {scan_id} not found")

        trades_list = [trade.copy() for trade in (scan_record.analysis_results or []) if isinstance(trade, dict)]
        initial_len = len(trades_list)

        trades_list = [t for t in trades_list if t.get("symbol") != symbol]

        if len(trades_list) == initial_len:
            raise HTTPException(status_code=404, detail=f"Trade with symbol {symbol} not found in analysis")

        scan_record.analysis_results = trades_list

        try:
            db.commit()
        except Exception as commit_error:
            db.rollback()
            logging.error(
                f"Failed to delete trade for scan {scan_id}: {commit_error}"
            )
            raise HTTPException(status_code=500, detail="Failed to delete trade") from commit_error

        response_data = {
            "deleted_symbol": symbol,
            "remaining_analysis": len(trades_list),
        }

        return StandardResponse(
            success=True,
            status=200,
            message=f"Deleted trade {symbol} from analysis",
            data=response_data,
        )
    finally:
        db.close()


@router.get("/combined/excel")
async def get_combined_analysis_excel(
    start_date: str = Query(..., description="Start date in YYYY-MM-DD format"),
    end_date: str = Query(..., description="End date in YYYY-MM-DD format")
):
    """
    Generate and download combined Excel file with analysis results from multiple scans within a date range.
    
    This endpoint combines analysis data from all scans between the specified dates into a single Excel file.
    
    Args:
        start_date: Start date for scan filtering in YYYY-MM-DD format
        end_date: End date for scan filtering in YYYY-MM-DD format
    
    Returns:
        Excel file with combined analysis results from all scans in the date range
    """
    return await _get_combined_analysis_excel_internal(start_date, end_date)


@router.get("/combined/active-trades/excel")
async def get_combined_active_trades_excel(
    start_date: str = Query(..., description="Start date in YYYY-MM-DD format"),
    end_date: str = Query(..., description="End date in YYYY-MM-DD format")
):
    """
    Generate and download combined Excel file with active trade data from multiple scans within a date range.
    
    This endpoint combines active trade data from all scans between the specified dates into a single Excel file,
    providing the same functionality as /combined/excel but with a clearer name for active trades.
    
    Args:
        start_date: Start date for scan filtering in YYYY-MM-DD format
        end_date: End date for scan filtering in YYYY-MM-DD format
    
    Returns:
        Excel file with combined active trade data from all scans in the date range
    """
    return await _get_combined_analysis_excel_internal(start_date, end_date)


@router.get("/active-trades/excel/{scan_id}")
async def get_scan_active_trades_excel(
    scan_id: int
):
    """
    Generate and download Excel file with active trade data from a specific scan.
    
    This endpoint generates the same kind of Excel file as the combined endpoint but for a single scan,
    using the same data format and structure.
    
    Args:
        scan_id: The ID of the scan to generate Excel for
    
    Returns:
        Excel file with active trade data from the specified scan
    """
    return await get_analysis_excel(scan_id)


async def _get_combined_analysis_excel_internal(start_date: str, end_date: str):
    """
    Internal function to generate and download combined Excel file with analysis results.
    
    Args:
        start_date: Start date for scan filtering in YYYY-MM-DD format
        end_date: End date for scan filtering in YYYY-MM-DD format
    
    Returns:
        Excel file with combined analysis results from all scans in the date range
    """
    try:
        logging.info(f"📥 Combined analysis Excel request for {start_date} to {end_date}")
        
        # Validate date format and range
        try:
            start_datetime = datetime.strptime(start_date, "%Y-%m-%d")
            end_datetime = datetime.strptime(end_date, "%Y-%m-%d").replace(hour=23, minute=59, second=59)
            
            if start_datetime > end_datetime:
                raise HTTPException(
                    status_code=400,
                    detail="Start date cannot be after end date."
                )
        except ValueError as e:
            raise HTTPException(
                status_code=400,
                detail="Invalid date format. Use YYYY-MM-DD format."
            )
        
        db = SessionLocal()
        try:
            # Query scans within date range with completed analysis
            query = db.query(ScanRecord).filter(
                ScanRecord.created_at >= start_datetime,
                ScanRecord.created_at <= end_datetime,
                ScanRecord.analysis_status == "completed",
            )
            
            # Order by creation date (newest first)
            scan_records = query.order_by(ScanRecord.created_at.desc()).all()
            
            if not scan_records:
                raise HTTPException(
                    status_code=404,
                    detail=f"No completed analysis scans found between {start_date} and {end_date}",
                )

            analysis_rows: List[Dict[str, Any]] = []

            for scan_record in scan_records:
                scan_date = scan_record.created_at.strftime("%Y-%m-%d %H:%M") if scan_record.created_at else None

                for trade in (scan_record.analysis_results or []):
                    if isinstance(trade, dict) and trade.get("recommendation") in ["Buy", "Sell"]:
                        row = trade.copy()
                        row["scan_id"] = scan_record.id
                        row["scan_date"] = scan_date
                        analysis_rows.append(row)

            if not analysis_rows:
                raise HTTPException(
                    status_code=404,
                    detail=f"No trade data found in analysis scans between {start_date} and {end_date}",
                )

            output = io.BytesIO()
            with pd.ExcelWriter(output, engine="openpyxl") as writer:
                pd.DataFrame(analysis_rows).to_excel(writer, sheet_name="analysis", index=False)

            output.seek(0)
            filename = f"combined_analysis_{start_date}_{end_date}.xlsx"
            logging.info("✅ Generated combined analysis Excel file")

            return StreamingResponse(
                io.BytesIO(output.read()),
                media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                headers={"Content-Disposition": f"attachment; filename={filename}"},
            )
            
        finally:
            db.close()
            
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Error generating combined Excel file: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error generating combined Excel file: {str(e)}"
        )


@router.get("/orders/preview/{scan_id}", response_model=StandardResponse)
async def get_orders_preview(
    scan_id: int
):
    """
    Get order preview data for modification before placing orders.
    
    Returns the order data that would be sent to the trading client,
    allowing the frontend to display and modify quantities.
    
    Args:
        scan_id: The ID of the scan to preview orders for
    
    Returns:
        Order preview data with modifiable fields
    """
    try:
        logging.info(f"📋 Getting order preview for scan {scan_id}")
        
        db = SessionLocal()
        try:
            # Check if scan exists
            scan_record = db.query(ScanRecord).filter(ScanRecord.id == scan_id).first()
            if not scan_record:
                raise HTTPException(
                    status_code=404,
                    detail=f"Scan with ID {scan_id} not found"
                )

            if scan_record.analysis_status != "completed":
                raise HTTPException(
                    status_code=400,
                    detail=f"Analysis not completed for scan {scan_id}. Current status: {scan_record.analysis_status or 'not_started'}",
                )
            
            # Get portfolio size amount for recalculating position sizes
            portfolio_size = float(scan_record.portfolio_size) if scan_record.portfolio_size else 350000.0

            risk_per_trade = 0.015

            def _build_preview(trades_list: List[Dict[str, Any]]) -> Dict[str, Any]:
                preview_orders: List[Dict[str, Any]] = []
                total_investment = 0.0

                for trade in trades_list or []:
                    if not (isinstance(trade, dict) and trade.get("recommendation") in ["Buy", "Sell"]):
                        continue
                    entry_price = float(trade.get("entry_price", 0) or 0)
                    stop_loss = float(trade.get("stop_loss", 0) or 0)

                    risk_per_share = abs(entry_price - stop_loss) if stop_loss > 0 else entry_price * 0.02
                    calculated_position_size = (
                        int((portfolio_size * risk_per_trade) / risk_per_share) if risk_per_share > 0 else 1
                    )
                    calculated_position_size = max(1, calculated_position_size)
                    calculated_position_size = min(MAX_SHARES_PER_ORDER, calculated_position_size)

                    investment = entry_price * calculated_position_size
                    if trade.get("recommendation") == "Sell":
                        investment *= 0.65
                    total_investment += investment

                    preview_orders.append(
                        {
                            "symbol": trade.get("symbol", ""),
                            "entry_price": entry_price,
                            "recommendation": trade.get("recommendation", ""),
                            "original_position_size": trade.get("position_size", 0),
                            "calculated_position_size": calculated_position_size,
                            "current_position_size": calculated_position_size,
                            "stop_loss": stop_loss,
                            "take_profit": trade.get("take_profit", 0),
                            "exit_date": trade.get("exit_date", "-"),
                            "strategy": trade.get("strategy", ""),
                            "risk_per_share": round(risk_per_share, 4),
                            "original_investment": round(entry_price * float(trade.get("position_size", 0) or 0), 2),
                            "calculated_investment": round(investment, 2),
                            "current_investment": round(investment, 2),
                        }
                    )

                return {
                    "total_orders": len(preview_orders),
                    "original_total_investment": round(total_investment, 2),
                    "orders": preview_orders,
                }

            analysis_preview = _build_preview(scan_record.analysis_results or [])

            if analysis_preview["total_orders"] == 0:
                raise HTTPException(status_code=404, detail=f"No valid trade orders found in analysis for scan {scan_id}")

            return StandardResponse(
                success=True,
                status=200,
                message="Order preview retrieved successfully",
                data={
                    "scan_id": scan_id,
                    "portfolio_size": portfolio_size,
                    "risk_per_trade": risk_per_trade,
                    "max_shares_per_order": MAX_SHARES_PER_ORDER,
                    "analysis": analysis_preview,
                },
            )
            
        finally:
            db.close()
            
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Error getting order preview for scan {scan_id}: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error getting order preview: {str(e)}"
        )


@router.get("/orders/trading-client-status", response_model=StandardResponse)
async def get_trading_client_status():
    """Report whether any local trading clients are currently connected."""

    connected = trading_websocket_manager.has_active_connections()
    connection_count = trading_websocket_manager.active_connection_count()
    message = (
        "Trading client connected"
        if connected
        else "No trading client connected"
    )

    return StandardResponse(
        success=True,
        status=200,
        message=message,
        data={
            "connected": connected,
            "active_connections": connection_count,
        },
    )


@router.post("/sync/positions", response_model=StandardResponse)
async def sync_positions_from_client(sync_data: Dict[str, Any]):
    """
    Receive position sync data from local trading client and reconcile with database.

    This endpoint:
    1. Receives current IBKR positions and orders from the local client
    2. Compares with OpenTrade table in the database
    3. Archives positions that were closed in IBKR (e.g., SL/TP hit)
    4. Identifies positions missing SL or TP protection
    5. Sends close commands for unprotected positions

    Args:
        sync_data: Position and order data from IBKR

    Returns:
        Reconciliation results and actions taken
    """
    try:
        ibkr_positions = sync_data.get('positions', {})
        ibkr_orders = sync_data.get('orders', {})
        synced_at = sync_data.get('synced_at', datetime.now().isoformat())

        logging.info(f"📊 Processing position sync: {len(ibkr_positions)} IBKR positions, {len(ibkr_orders)} order groups")

        db = SessionLocal()
        try:
            now_utc = datetime.now(timezone.utc)
            closed_positions = []
            missing_protection = []
            positions_to_close = []

            # Get all open trades from database
            open_trades = db.query(OpenTrade).all()

            for trade in open_trades:
                symbol = trade.symbol

                # Check if position still exists in IBKR
                if symbol not in ibkr_positions:
                    # Position was closed in IBKR (likely SL or TP hit)
                    logging.info(f"🔴 Position {symbol} closed in IBKR - archiving")

                    _archive_open_trade(
                        db,
                        trade,
                        close_reason="Position closed in IBKR (SL/TP hit or manual close)",
                        exit_time=now_utc,
                    )
                    closed_positions.append(symbol)
                    continue

                # Position exists - check if it has both SL and TP protection
                if symbol in ibkr_orders:
                    orders = ibkr_orders[symbol]
                    has_sl = orders.get('stop_loss') is not None
                    has_tp = orders.get('take_profit') is not None

                    if not has_sl or not has_tp:
                        logging.warning(f"⚠️ Position {symbol} missing protection in IBKR (SL: {has_sl}, TP: {has_tp})")

                        # Archive in database
                        _archive_open_trade(
                            db,
                            trade,
                            close_reason=f"Missing protection in IBKR (SL: {has_sl}, TP: {has_tp})",
                            exit_time=now_utc,
                        )
                        missing_protection.append(symbol)

                        # Add to close command list
                        ibkr_position = ibkr_positions[symbol]
                        positions_to_close.append({
                            'symbol': symbol,
                            'quantity': ibkr_position['quantity'],
                            'action': ibkr_position['action'],
                            'reason': f'Missing protection (SL: {has_sl}, TP: {has_tp})'
                        })
                else:
                    # Position exists but no orders found - critical issue
                    logging.error(f"❌ CRITICAL: Position {symbol} exists in IBKR but has NO protective orders")

                    _archive_open_trade(
                        db,
                        trade,
                        close_reason="No protective orders found in IBKR",
                        exit_time=now_utc,
                    )
                    missing_protection.append(symbol)

                    ibkr_position = ibkr_positions[symbol]
                    positions_to_close.append({
                        'symbol': symbol,
                        'quantity': ibkr_position['quantity'],
                        'action': ibkr_position['action'],
                        'reason': 'No protective orders found'
                    })

            db.commit()

            # Send close commands for positions missing protection
            if positions_to_close:
                await _send_close_position_command(positions_to_close)

            result_data = {
                'synced_at': synced_at,
                'ibkr_positions_count': len(ibkr_positions),
                'database_positions_count': len(open_trades),
                'closed_positions': closed_positions,
                'missing_protection': missing_protection,
                'close_commands_sent': len(positions_to_close)
            }

            message_parts = [f"Position sync completed"]
            if closed_positions:
                message_parts.append(f"{len(closed_positions)} positions closed")
            if missing_protection:
                message_parts.append(f"{len(missing_protection)} positions missing protection")
            if positions_to_close:
                message_parts.append(f"{len(positions_to_close)} close commands sent")

            return StandardResponse(
                success=True,
                status=200,
                message="; ".join(message_parts),
                data=result_data
            )

        finally:
            db.close()

    except Exception as e:
        logging.error(f"Error processing position sync: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error processing position sync: {str(e)}"
        )


@router.post("/orders/place-modified-orders", response_model=StandardResponse)
async def place_modified_trading_orders(request: PlaceModifiedOrdersRequest):
    """
    Place trading orders with modified quantities.
    
    This endpoint:
    1. Gets the original analysis data for the scan
    2. Applies the modified quantities to specific symbols
    3. Generates Excel file and sends orders via WebSocket to local trading client
    
    Args:
        request: Contains scan_id and list of modified orders
    
    Returns:
        Success response with Excel file path and order count
    """
    try:
        scan_id = request.scan_id
        modified_orders_dict = {mod.symbol: mod.position_size for mod in request.modified_orders}

        logging.info(f"📊 Placing modified trading orders for scan {scan_id}")
        logging.info(f"📝 Modified orders: {modified_orders_dict}")

        db = SessionLocal()
        try:
            maintenance_summary = _apply_trade_maintenance_rules(db)

            # Check if scan exists
            scan_record = db.query(ScanRecord).filter(ScanRecord.id == scan_id).first()
            if not scan_record:
                raise HTTPException(
                    status_code=404,
                    detail=f"Scan with ID {scan_id} not found"
                )
            
            if scan_record.analysis_status != "completed":
                raise HTTPException(
                    status_code=400,
                    detail=f"Analysis not completed for scan {scan_id}. Current status: {scan_record.analysis_status or 'not_started'}",
                )

            trades_list = scan_record.analysis_results or []

            if not trades_list:
                raise HTTPException(status_code=404, detail=f"Analysis results not found for scan {scan_id}")
            
            # Only process symbols that are specified in modified_orders
            valid_trades = []
            for trade in trades_list:
                if isinstance(trade, dict) and trade.get("recommendation") in ["Buy", "Sell"]:
                    symbol = trade.get("symbol", "")
                    
                    # Only include this trade if it's in the modified_orders list
                    if symbol in modified_orders_dict:
                        requested_quantity = modified_orders_dict[symbol]
                        final_quantity = min(MAX_SHARES_PER_ORDER, requested_quantity)
                        trade_copy = trade.copy()  # Don't modify original data
                        trade_copy["position_size"] = final_quantity
                        valid_trades.append(trade_copy)
                        logging.info(
                            "📝 Including %s with modified quantity: %s -> %s shares",
                            symbol,
                            trade.get("position_size"),
                            final_quantity,
                        )
                    # Skip symbols not in modified_orders_dict
            
            if not valid_trades:
                raise HTTPException(
                    status_code=404,
                    detail=f"No valid trade orders found in analysis for scan {scan_id}"
                )

            if not trading_websocket_manager.has_active_connections():
                raise HTTPException(
                    status_code=503,
                    detail="No local trading clients are connected. Connect the trading client before placing orders."
                )

            def _to_decimal(raw_value) -> Optional[Decimal]:
                if raw_value in (None, "", "-", 0):
                    return None
                try:
                    return Decimal(str(raw_value))
                except (InvalidOperation, ValueError, TypeError):
                    logging.warning(f"Unable to convert value '{raw_value}' to Decimal")
                    return None

            def _parse_target_date(raw_value):
                if not raw_value or raw_value in ("-", ""):
                    return None
                try:
                    return datetime.fromisoformat(str(raw_value)).date()
                except ValueError:
                    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y"):
                        try:
                            return datetime.strptime(str(raw_value), fmt).date()
                        except ValueError:
                            continue
                    logging.warning(f"Unable to parse target date '{raw_value}'")
                    return None

            # Persist trade state in database before returning response
            skipped_payload: List[str] = []
            try:
                skipped_symbols: List[str] = []

                for trade in valid_trades:
                    symbol = trade.get("symbol")
                    action = trade.get("recommendation")
                    if not symbol or action not in ("Buy", "Sell"):
                        logging.info(f"Skipping trade persistence for symbol '{symbol}' with action '{action}'")
                        continue

                    try:
                        quantity = int(trade.get("position_size", 0))
                    except (TypeError, ValueError):
                        quantity = 0

                    if quantity <= 0:
                        logging.info(f"Skipping trade persistence for {symbol}: quantity <= 0")
                        continue

                    entry_price_decimal = _to_decimal(trade.get("entry_price"))
                    if entry_price_decimal is None:
                        logging.warning(f"Skipping trade persistence for {symbol}: entry price missing")
                        continue

                    stop_loss_decimal = _to_decimal(trade.get("stop_loss"))
                    take_profit_decimal = _to_decimal(trade.get("take_profit"))

                    if _is_missing_protection(stop_loss_decimal) or _is_missing_protection(take_profit_decimal):
                        logging.warning(
                            f"Skipping {symbol} due to missing protective levels (SL: {trade.get('stop_loss')}, TP: {trade.get('take_profit')})"
                        )
                        skipped_symbols.append(symbol)
                        continue

                    target_date = _parse_target_date(trade.get("exit_date"))
                    if target_date is None:
                        target_date = _parse_target_date(trade.get("target_date"))

                    now_utc = datetime.now(timezone.utc)
                    open_trade = db.query(OpenTrade).filter(OpenTrade.symbol == symbol).first()

                    if open_trade:
                        if (open_trade.action or "").lower() == action.lower():
                            open_trade.quantity = quantity
                            open_trade.entry_price = entry_price_decimal
                            open_trade.entry_date = now_utc
                            open_trade.stop_loss = stop_loss_decimal
                            open_trade.take_profit = take_profit_decimal
                            open_trade.target_date = target_date
                            open_trade.strategy = trade.get("strategy")
                            open_trade.scan_id = scan_id
                            open_trade.analysis_type = "analysis"
                        else:
                            _archive_open_trade(
                                db,
                                open_trade,
                                close_reason="Opposite recommendation",
                                exit_price=entry_price_decimal,
                                exit_time=now_utc,
                                remove_open_trade=False,
                                fallback_scan_id=scan_id,
                                fallback_analysis_type="analysis",
                            )

                            open_trade.action = action
                            open_trade.quantity = quantity
                            open_trade.entry_price = entry_price_decimal
                            open_trade.entry_date = now_utc
                            open_trade.stop_loss = stop_loss_decimal
                            open_trade.take_profit = take_profit_decimal
                            open_trade.target_date = target_date
                            open_trade.strategy = trade.get("strategy")
                            open_trade.scan_id = scan_id
                            open_trade.analysis_type = "analysis"
                    else:
                        new_trade = OpenTrade(
                            symbol=symbol,
                            action=action,
                            quantity=quantity,
                            entry_price=entry_price_decimal,
                            entry_date=now_utc,
                            stop_loss=stop_loss_decimal,
                            take_profit=take_profit_decimal,
                            target_date=target_date,
                            strategy=trade.get("strategy"),
                            scan_id=scan_id,
                            analysis_type="analysis",
                        )
                        db.add(new_trade)

                if skipped_symbols:
                    skipped_set = set(skipped_symbols)
                    original_count = len(valid_trades)
                    valid_trades[:] = [trade for trade in valid_trades if trade.get("symbol") not in skipped_set]
                    skipped_payload = sorted(skipped_set)
                    logging.info(
                        "⚠️ Removed %s trades lacking protective levels from outbound payload: %s",
                        original_count - len(valid_trades),
                        ", ".join(skipped_payload),
                    )

                db.flush()
            except Exception as trade_db_error:
                db.rollback()
                logging.error(f"Failed to persist trade updates for scan {scan_id}: {trade_db_error}")
                raise HTTPException(status_code=500, detail="Failed to persist trade updates") from trade_db_error

            # Create Excel data in trading bot format with modified quantities
            excel_data = []
            total_investment = 0

            for trade in valid_trades:
                # Calculate investment amount with modified quantity
                try:
                    quantity = int(trade.get("position_size", 0))
                except (TypeError, ValueError):
                    quantity = 0

                price_value = trade.get("entry_price", 0)
                try:
                    price_float = float(price_value)
                except (TypeError, ValueError):
                    price_float = 0.0

                investment = price_float * quantity
                if trade.get("recommendation") == "Sell":
                    investment *= 0.65  # Apply safety margin for short trades

                total_investment += investment

                excel_data.append({
                    "Symbol": trade.get("symbol", ""),
                    "Current price": round(price_float, 4),
                    "investment in $": round(investment, 2),
                    "Transaction type": trade.get("recommendation", ""),
                    "Order Type": "Market",
                    "Position": quantity,
                    "Stop Loss": trade.get("stop_loss", 0),
                    "Take Profit": trade.get("take_profit", 0),
                    "Target Date": trade.get("exit_date", "-"),
                    "Win Rate": trade.get("backtest_win_rate", 0),
                    "ADX": trade.get("adx", 0),
                    "RSI": trade.get("rsi", 0),
                    "MACD": trade.get("macd", 0),
                    "Volume Spike": trade.get("volume_spike", False),
                    "Score": trade.get("score", 0),
                    "Score Notes": trade.get("score_notes", ""),
                    "Strategy": trade.get("strategy", "")
                })
            
            # Add total investment row
            excel_data.append({
                "Symbol": "Total Investment",
                "Current price": "",
                "investment in $": round(total_investment, 2),
                "Transaction type": "",
                "Order Type": "",
                "Position": "",
                "Stop Loss": "",
                "Take Profit": "",
                "Target Date": "",
                "Win Rate": "",
                "ADX": "",
                "RSI": "",
                "MACD": "",
                "Volume Spike": "",
                "Score": "",
                "Score Notes": "",
                "Strategy": ""
            })
            
            # Create DataFrame and Excel file (in-memory)
            df = pd.DataFrame(excel_data)
            
            # Define filename for downstream clients
            scan_date = scan_record.created_at.strftime('%Y%m%d_%H%M') if scan_record.created_at else 'unknown'
            filename = f"analysis_modified_trading_analysis_scan_{scan_id}_{scan_date}.xlsx"
            buffer = io.BytesIO()
            df.to_excel(buffer, index=False)
            buffer.seek(0)
            excel_base64 = base64.b64encode(buffer.read()).decode("utf-8")
            
            # Prepare order data for WebSocket notification (with modified quantities)
            order_data = {
                "scan_id": scan_id,
                "analysis_type": "analysis",
                "excel_file_base64": excel_base64,
                "excel_filename": filename,
                "total_orders": len(valid_trades),
                "total_investment": total_investment,
                "orders": valid_trades,  # Contains modified position_size values
                "modified_orders": modified_orders_dict,  # Track which orders were modified
                "maintenance_closures": maintenance_summary,
                "skipped_symbols": skipped_payload,
                "generated_at": datetime.now().isoformat()
            }
            
            # Send WebSocket notification to local trading client
            try:
                recipients = await trading_websocket_manager.send_trading_orders_event(order_data)
            except Exception as ws_error:
                db.rollback()
                logging.error(f"Failed to send WebSocket modified order notification for scan {scan_id}: {ws_error}")
                raise HTTPException(status_code=503, detail="Unable to deliver orders to the trading client") from ws_error

            if recipients == 0:
                db.rollback()
                logging.error(f"No trading clients received the order payload for scan {scan_id}")
                raise HTTPException(
                    status_code=503,
                    detail="Trading client not connected. Orders were not placed."
                )

            db.commit()
            
            logging.info(f"✅ Generated modified trading orders payload for scan {scan_id} with {len(valid_trades)} orders")

            message_fragments = [
                f"Modified trading orders prepared successfully with {len(valid_trades)} orders"
            ]
            if maintenance_summary.get("target_date"):
                message_fragments.append(
                    f"Closed {len(maintenance_summary['target_date'])} open trades due to target date"
                )
            if maintenance_summary.get("missing_protection"):
                message_fragments.append(
                    f"Closed {len(maintenance_summary['missing_protection'])} open trades lacking protective levels"
                )
            if skipped_payload:
                message_fragments.append(
                    f"Skipped {len(skipped_payload)} symbols without stop loss or take profit"
                )

            return StandardResponse(
                success=True,
                status=200,
                message="; ".join(message_fragments),
                data={
                    "scan_id": scan_id,
                    "total_orders": len(valid_trades),
                    "total_investment": round(total_investment, 2),
                    "modified_orders": modified_orders_dict,
                    "modifications_applied": len(modified_orders_dict),
                    "maintenance_closures": maintenance_summary,
                    "skipped_symbols": skipped_payload,
                }
            )
            
        finally:
            db.close()
            
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Error generating modified trading orders Excel for scan {scan_id}: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error generating modified trading orders Excel: {str(e)}"
        )
