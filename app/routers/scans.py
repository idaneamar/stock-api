import logging
import io
import json
import pandas as pd
from typing import Optional
from fastapi import APIRouter, HTTPException, Query, Body
from fastapi.responses import StreamingResponse
from datetime import datetime
from app.schemas.stock_scanner import StockScannerRequest
from app.schemas.response import StandardResponse
from app.services.stock_scanner import StockScanner
from app.models.database import SessionLocal
from app.models.scan import ScanRecord

router = APIRouter(
    prefix="/scans",
    tags=["Scans"],
    responses={404: {"description": "Not found"}},
)

# Initialize services
stock_scanner = StockScanner()

@router.post("/", response_model=StandardResponse)
@router.post("/scan-stocks", response_model=StandardResponse, include_in_schema=False)  # Legacy endpoint for backward compatibility
async def scan_stocks(criteria: StockScannerRequest):
    """
    Start a background stock scan based on provided criteria.
    
    This endpoint starts a background scan of US stocks based on market cap, 
    volume, volatility, and other criteria. Returns immediately with scan ID.
    """
    try:
        scan_criteria = criteria.dict()
        received_program_id = scan_criteria.get("program_id") if scan_criteria else None
        if received_program_id is not None and str(received_program_id).strip() == "":
            received_program_id = None
        logging.info("="*60)
        logging.info("🚀 NEW SCAN REQUEST RECEIVED")
        logging.info("📋 Scan criteria: %s", scan_criteria)
        logging.info("📌 POST /scans received program_id: %s", received_program_id if received_program_id else "(absent/empty - backend will use only globally enabled strategies)")
        logging.info("="*60)

        # Start background scan and get scan ID
        scan_id = stock_scanner.start_background_scan(scan_criteria)
        
        # Create immediate response
        response_data = {
            "scan_id": scan_id,
            "status": "in_progress",
            "message": "Stock scan started successfully",
            "criteria_used": criteria.dict(),
            "scan_timestamp": datetime.now().isoformat()
        }
        
        logging.info(f"✅ Background stock scan started successfully with ID: {scan_id}")
        logging.info(f"🔄 Scan is now running in background. Check /scans/{scan_id} for progress")
        logging.info("="*60)
        
        return StandardResponse(
            success=True,
            status=200,
            message="Stock scan started successfully",
            data=response_data
        )
        
    except Exception as e:
        logging.error(f"Error starting stock scan: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error starting stock scan: {str(e)}"
        )


@router.get("/", response_model=StandardResponse)
async def get_all_scans(
    page: int = Query(1, ge=1, description="Page number (1-indexed)"),
    page_size: int = Query(10, ge=1, le=100, description="Number of scans per page"),
):
    """
    Get scan records with their status, progress tracking, and pagination metadata.

    Args:
        page: Page number to retrieve (1-indexed)
        page_size: Number of scans to return per page

    Returns paginated scan records including per-record progress fields:
    - scan_progress: Progress of initial stock scanning (0-100%)
    - analysis_progress: Progress of analysis (0-100%)

    Response payload contains `items` along with `total`, `page`, `page_size`, and navigation flags.
    """
    try:
        all_scans = stock_scanner.get_all_scans(page=page, page_size=page_size)

        retrieved_count = len(all_scans.get("items", []))
        total_records = all_scans.get("total", 0)

        if total_records == 0:
            message = "No scan records found"
        else:
            total_pages = all_scans.get("total_pages", 1) or 1
            message = (
                f"Retrieved {retrieved_count} scan records "
                f"(page {all_scans.get('page', page)} of {total_pages})"
            )
        
        return StandardResponse(
            success=True,
            status=200,
            message=message,
            data=all_scans
        )
        
    except Exception as e:
        logging.error(f"Error retrieving all scans: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error retrieving all scans: {str(e)}"
        )


@router.get("/{scan_id}", response_model=StandardResponse)
async def get_scan_by_id(scan_id: int):
    """
    Get a specific scan record by ID with detailed progress tracking.
    
    Args:
        scan_id: The ID of the scan to retrieve
    
    Returns the scan record with comprehensive progress information:
    - scan_progress: Current progress of stock scanning (0-100%)
    - analysis_progress: Current progress of analysis (0-100%)
    
    Use this endpoint to monitor the real-time progress of any ongoing processes.
    """
    try:
        scan = stock_scanner.get_scan_by_id(scan_id)
        
        return StandardResponse(
            success=True,
            status=200,
            message=f"Retrieved scan record for ID {scan_id}",
            data=scan
        )
        
    except ValueError as e:
        if "not found" in str(e):
            raise HTTPException(
                status_code=404,
                detail=str(e)
            )
        raise HTTPException(
            status_code=400,
            detail=str(e)
        )
    except Exception as e:
        logging.error(f"Error retrieving scan {scan_id}: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error retrieving scan {scan_id}: {str(e)}"
        )


@router.get("/completed/filtered", response_model=StandardResponse)
async def get_completed_scans_filtered(
    start_date: str = Query(..., description="Start date in YYYY-MM-DD format"),
    end_date: str = Query(..., description="End date in YYYY-MM-DD format")
):
    """
    Get filtered scans that are fully completed (scan + analysis).
    
    Args:
        start_date: Start date filter in YYYY-MM-DD format
        end_date: End date filter in YYYY-MM-DD format
    
    Returns only scans where:
    - Scan status is "completed"
    - Analysis status is "completed"
    - Created date is within the specified range
    
    Response includes only id and created_at fields for each scan.
    """
    try:
        from datetime import datetime
        
        # Validate date format
        try:
            start_datetime = datetime.strptime(start_date, "%Y-%m-%d")
            end_datetime = datetime.strptime(end_date, "%Y-%m-%d").replace(hour=23, minute=59, second=59)
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail="Invalid date format. Use YYYY-MM-DD format."
            )
        
        # Validate date range
        if start_datetime > end_datetime:
            raise HTTPException(
                status_code=400,
                detail="Start date cannot be after end date."
            )
        
        db = SessionLocal()
        try:
            # Query for completed scans within date range
            completed_scans = db.query(ScanRecord).filter(
                ScanRecord.status == "completed",
                ScanRecord.analysis_status == "completed",
                ScanRecord.created_at >= start_datetime,
                ScanRecord.created_at <= end_datetime
            ).order_by(ScanRecord.created_at.desc()).all()
            
            # Return only id and created_at fields
            filtered_data = [
                {
                    "id": scan.id,
                    "created_at": scan.created_at.isoformat()
                }
                for scan in completed_scans
            ]
            
            return StandardResponse(
                success=True,
                status=200,
                message=f"Retrieved {len(filtered_data)} completed scans between {start_date} and {end_date}",
                data=filtered_data
            )
            
        finally:
            db.close()
            
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Error retrieving filtered scans: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error retrieving filtered scans: {str(e)}"
        )



@router.get("/dates/range", response_model=StandardResponse)
async def get_scan_date_range():
    """
    Get the first and last scan dates from the database for filtering purposes.
    
    Returns:
        - first_scan_date: Date of the oldest scan in the database
        - last_scan_date: Date of the most recent scan in the database
    """
    try:
        db = SessionLocal()
        try:
            # Get the first (oldest) scan date
            first_scan = db.query(ScanRecord.created_at).order_by(ScanRecord.created_at.asc()).first()
            
            # Get the last (newest) scan date  
            last_scan = db.query(ScanRecord.created_at).order_by(ScanRecord.created_at.desc()).first()
            
            if not first_scan or not last_scan:
                return StandardResponse(
                    success=True,
                    status=200,
                    message="No scans found in database",
                    data={
                        "first_scan_date": None,
                        "last_scan_date": None
                    }
                )
            
            return StandardResponse(
                success=True,
                status=200,
                message="Retrieved scan date range successfully",
                data={
                    "first_scan_date": first_scan[0].isoformat(),
                    "last_scan_date": last_scan[0].isoformat()
                }
            )
            
        finally:
            db.close()
            
    except Exception as e:
        logging.error(f"Error retrieving scan date range: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error retrieving scan date range: {str(e)}"
        )


@router.post("/{scan_id}/restart-analysis", response_model=StandardResponse)
async def restart_analysis(
    scan_id: int,
    program_id: Optional[str] = Body(None, embed=True),
):
    """
    Restart analysis by creating a duplicate scan without analysis data.

    Accepts an optional ``program_id`` body field to override the strategy set
    used for the re-analysis (pass ``null`` / omit for "No Program – use
    globally enabled strategies").
    """
    try:
        logging.info(f"🔄 Restart analysis request for scan_id: {scan_id}, program_id={program_id!r}")

        db = SessionLocal()
        try:
            original_scan = db.query(ScanRecord).filter(ScanRecord.id == scan_id).first()
            if not original_scan:
                raise HTTPException(
                    status_code=404,
                    detail=f"Scan with ID {scan_id} not found"
                )

            # Build new criteria – copy original, then override program_id
            new_criteria = dict(original_scan.criteria or {})
            if program_id is not None and str(program_id).strip():
                new_criteria["program_id"] = str(program_id).strip()
            else:
                new_criteria.pop("program_id", None)

            logging.info(f"🔀 Restart criteria program_id resolved to: {new_criteria.get('program_id', '(no program)')!r}")

            # Create duplicate scan without analysis data
            new_scan = ScanRecord(
                status="completed",
                scan_progress=original_scan.scan_progress,
                criteria=new_criteria,
                stock_symbols=original_scan.stock_symbols,
                total_found=original_scan.total_found,
                portfolio_size=original_scan.portfolio_size,
                analysis_results=None,
                analysis_status=None,
                analysis_progress=0,
                analyzed_at=None,
                error_message=None,
                completed_at=datetime.now()
            )
            
            db.add(new_scan)
            db.commit()
            db.refresh(new_scan)
            new_scan_id = new_scan.id
            
            logging.info(f"✅ Created duplicate scan with ID: {new_scan_id}")
            
            # Start analysis for the new scan
            try:
                from app.services.trading_analysis import TradingAnalysisService
                analysis_service = TradingAnalysisService()

                # Update status to analyzing
                new_scan.analysis_status = "analyzing"
                new_scan.analysis_progress = 0
                db.commit()

                # Start background analysis
                analysis_service.start_background_analysis(new_scan_id)
                logging.info(f"🚀 Started analysis for new scan {new_scan_id}")
            except Exception as e:
                logging.error(f"Error starting analysis: {e}")
            
            response_data = {
                "original_scan_id": scan_id,
                "new_scan_id": new_scan_id,
                "status": "analysis_started",
                "message": "Duplicate scan created and analysis restarted",
                "restart_timestamp": datetime.now().isoformat()
            }
            
            logging.info(f"✅ Analysis restart completed for scan {scan_id}, new scan: {new_scan_id}")
            
            return StandardResponse(
                success=True,
                status=200,
                message=f"Analysis restarted successfully. New scan created with ID {new_scan_id}",
                data=response_data
            )
            
        finally:
            db.close()
            
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Error restarting analysis for scan {scan_id}: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error restarting analysis: {str(e)}"
        )


@router.delete("/{scan_id}", response_model=StandardResponse)
async def delete_scan_by_id(scan_id: int):
    """
    Delete a specific scan record by ID.
    
    Args:
        scan_id: The ID of the scan to delete
    
    Returns success message if scan was deleted successfully.
    """
    try:
        result = stock_scanner.delete_scan_by_id(scan_id)
        
        return StandardResponse(
            success=True,
            status=200,
            message=f"Scan with ID {scan_id} deleted successfully",
            data={"scan_id": scan_id, "deleted": True}
        )
        
    except ValueError as e:
        if "not found" in str(e):
            raise HTTPException(
                status_code=404,
                detail=str(e)
            )
        raise HTTPException(
            status_code=400,
            detail=str(e)
        )
    except Exception as e:
        logging.error(f"Error deleting scan {scan_id}: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error deleting scan {scan_id}: {str(e)}"
        )


@router.delete("/", response_model=StandardResponse)
async def delete_all_scans():
    """
    Delete all scan records in the system.
    
    Returns total number of deleted scans.
    """
    try:
        deleted_count = stock_scanner.delete_all_scans()

        message = (
            "No scans found to delete"
            if deleted_count == 0
            else f"Deleted {deleted_count} scan records successfully"
        )

        return StandardResponse(
            success=True,
            status=200,
            message=message,
            data={"deleted": deleted_count}
        )
    except Exception as e:
        logging.error(f"Error deleting all scans: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error deleting all scans: {str(e)}"
        )


@router.get("/{scan_id}/stocks/excel")
async def download_scanned_stocks_excel(scan_id: int):
    """
    Download all scanned stock data as Excel for a particular scan.
    
    Args:
        scan_id: The ID of the scan to download stock data for
    
    Returns:
        Excel file containing all stock symbols and their details from the scan
    """
    try:
        logging.info(f"📥 Scanned stocks Excel download request for scan_id: {scan_id}")
        
        db = SessionLocal()
        try:
            # Check if scan exists
            scan_record = db.query(ScanRecord).filter(ScanRecord.id == scan_id).first()
            if not scan_record:
                raise HTTPException(
                    status_code=404,
                    detail=f"Scan with ID {scan_id} not found"
                )
            
            # Check if scan has stock symbols
            if not scan_record.stock_symbols:
                raise HTTPException(
                    status_code=404,
                    detail=f"No stock data found for scan {scan_id}. Scan may still be in progress."
                )
            
            # Process stock symbols data
            stocks_data = scan_record.stock_symbols
            if isinstance(stocks_data, str):
                stocks_data = json.loads(stocks_data)
            
            # Convert to DataFrame
            if isinstance(stocks_data, list) and len(stocks_data) > 0:
                # Check if the data contains detailed stock information or just ticker symbols
                if isinstance(stocks_data[0], dict):
                    # Detailed stock data - convert to DataFrame directly
                    df = pd.DataFrame(stocks_data)
                else:
                    # Simple ticker list - create DataFrame with ticker column
                    df = pd.DataFrame(stocks_data, columns=['ticker'])
            else:
                raise HTTPException(
                    status_code=404,
                    detail=f"No valid stock data found for scan {scan_id}"
                )
            
            # Create Excel file in memory
            output = io.BytesIO()
            df.to_excel(output, index=False)
            output.seek(0)
            
            # Generate filename for download
            scan_date = scan_record.created_at.strftime('%Y%m%d') if scan_record.created_at else 'unknown'
            filename = f"scanned_stocks_scan_{scan_id}_{scan_date}.xlsx"
            
            logging.info(f"✅ Generated Excel file for scan {scan_id} with {len(df)} stocks")
            
            return StreamingResponse(
                io.BytesIO(output.read()),
                media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                headers={"Content-Disposition": f"attachment; filename={filename}"}
            )
            
        finally:
            db.close()
            
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Error generating scanned stocks Excel file for scan {scan_id}: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error generating Excel file: {str(e)}"
        )
