import requests
import pandas as pd
import time
from datetime import datetime, timedelta
import pytz
import logging
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import json
from typing import List, Dict, Optional, Any
import math
from sqlalchemy.orm import Session
from sqlalchemy.sql import func
from app.models.database import SessionLocal
from app.models.scan import ScanRecord
from app.models.settings import Settings

# API Configuration – reads from environment variable first, falls back to hardcoded value
import os as _os
API_KEY = _os.environ.get("EODHD_API_TOKEN", "699ae161e60555.92486250")
BASE_URL = "https://eodhd.com/api"

class StockScanner:
    def __init__(self):
        self.api_key = API_KEY
        self.base_url = BASE_URL
        self.session = self._create_session()
        self.logger = logging.getLogger(__name__)
        
    def _create_session(self):
        """Create HTTP session with retry strategy"""
        session = requests.Session()
        retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
        session.mount('https://', HTTPAdapter(max_retries=retries))
        return session
    
    def _get_tickers_in_range(self, min_market_cap: float, max_market_cap: float) -> List[Dict]:
        """Fetch US stocks within a specific market cap range."""
        all_tickers = []
        offset = 0
        limit = 100
        backoff = 1.0

        while True:
            filters = [
                ["exchange", "=", "us"],
                ["market_capitalization", ">", int(min_market_cap)],
                ["market_capitalization", "<", int(max_market_cap)]
            ]
            params = {
                "api_token": self.api_key,
                "filters": json.dumps(filters),
                "offset": offset,
                "limit": limit,
            }

            try:
                resp = self.session.get(f"{self.base_url}/screener", params=params, timeout=15)
                resp.raise_for_status()
                payload = resp.json() or {}
                data = payload.get("data", [])
                total = payload.get("total")

                all_tickers.extend(data)
                logging.info(f"Fetched {len(data)} at offset {offset} (accumulated {len(all_tickers)})")

                if not data or len(data) < limit:
                    if total is None or len(all_tickers) >= total:
                        break

                if total is not None and len(all_tickers) >= total:
                    break

                offset += limit
                time.sleep(0.5)
                backoff = 1.0

            except requests.HTTPError as e:
                status = getattr(e.response, "status_code", None)
                logging.warning(f"/screener failed (status={status}) at offset {offset}: {e}")

                if status in (429, 500, 502, 503, 504):
                    if backoff > 8:
                        break
                    time.sleep(backoff)
                    backoff *= 2
                    continue

                if status == 422:
                    break
                break

            except requests.RequestException as e:
                logging.error(f"Network error at offset {offset}: {e}")
                break

        return all_tickers

    def _get_us_tickers(self, min_market_cap: float, max_market_cap: float) -> List[Dict]:
        """Get all US tickers within market cap range using multiple smaller ranges"""
        self.logger.info(f"🔍 Scanning US market for stocks with market cap ${min_market_cap/1e6:.0f}M - ${max_market_cap/1e6:.0f}M")
        
        all_tickers = []
        range_size = (max_market_cap - min_market_cap) // 8
        market_cap_ranges = [
            (min_market_cap + i * range_size, min_market_cap + (i + 1) * range_size) for i in range(7)
        ]
        market_cap_ranges.append((min_market_cap + 7 * range_size, max_market_cap))

        for i, (min_cap, max_cap) in enumerate(market_cap_ranges, 1):
            self.logger.info(f"📊 Fetching range {i}/8: ${min_cap/1e6:.0f}M to ${max_cap/1e6:.0f}M")
            tickers_in_range = self._get_tickers_in_range(min_cap, max_cap)
            self.logger.info(f"   └─ Found {len(tickers_in_range)} stocks in this range")
            all_tickers.extend(tickers_in_range)
            time.sleep(1)

        unique_tickers = list({ticker['code']: ticker for ticker in all_tickers}.values())
        self.logger.info(f"📈 Total unique candidate stocks found: {len(unique_tickers)}")
        return unique_tickers

    def _get_stock_data(self, ticker: str, market_cap: float, exchange: str, 
                       min_avg_volume: float, min_avg_transaction_value: float, 
                       min_volatility: float, min_price: float) -> Optional[Dict]:
        """Analyze individual stock and return data if it meets criteria"""
        to_date = datetime.now(pytz.UTC).date().strftime("%Y-%m-%d")
        from_date = (datetime.now(pytz.UTC).date() - timedelta(days=90)).strftime("%Y-%m-%d")

        url = f"{self.base_url}/eod/{ticker}.{exchange}?api_token={self.api_key}&fmt=json&period=d&order=d&from={from_date}&to={to_date}"
        try:
            response = self.session.get(url, timeout=15)
            response.raise_for_status()
            data = response.json()
            
            if len(data) < 20:
                return None

            full_df = pd.DataFrame(data)
            if 'date' not in full_df.columns or 'close' not in full_df.columns:
                return None

            full_df['date'] = pd.to_datetime(full_df['date'])
            latest_date = full_df['date'].max()
            if latest_date.year < 2025:
                return None

            df = pd.DataFrame(full_df[-20:])
            if 'close' not in df.columns:
                return None

            current_price = df['close'].iloc[-1]
            if current_price < min_price:
                return None

            avg_volume = df['volume'].mean()
            if avg_volume < min_avg_volume:
                return None

            df['Transaction_Value'] = df['volume'] * df['close']
            avg_transaction_value_calc = df['Transaction_Value'].mean()
            if avg_transaction_value_calc < min_avg_transaction_value:
                return None

            df['Returns'] = df['close'].pct_change()
            volatility = df['Returns'].std() * (252 ** 0.5)
            
            # Check for NaN volatility (insufficient data) or below threshold
            if pd.isna(volatility) or volatility < min_volatility:
                return None

            # Get company name (optional)
            company_name = ticker
            try:
                fund_url = f"{self.base_url}/fundamentals/{ticker}.{exchange}?api_token={self.api_key}&fmt=json"
                fund_response = self.session.get(fund_url, timeout=15)
                if fund_response.status_code == 200:
                    fund_data = fund_response.json()
                    company_name = fund_data.get('General', {}).get('Name', ticker)
            except Exception:
                pass

            return {
                'ticker': ticker,
                'company_name': company_name,
                'market_cap': market_cap,
                'volatility': volatility * 100,  # Convert to percentage for frontend display
                'price': current_price,
                'avg_volume': avg_volume,
                'avg_transaction_value': avg_transaction_value_calc
            }
        except (requests.RequestException, ValueError, KeyError) as e:
            logging.warning(f"Error fetching data for {ticker}: {e}")
            return None

    async def scan_stocks(self, criteria: Dict) -> List[Dict]:
        """Scan stocks based on criteria and return top stocks"""
        min_market_cap = criteria.get("min_market_cap", 150e6)
        max_market_cap = criteria.get("max_market_cap", 160e6)
        min_avg_volume = criteria.get("min_avg_volume", 15000)
        min_avg_transaction_value = criteria.get("min_avg_transaction_value", 150000)
        min_volatility = criteria.get("min_volatility", 0.4)
        min_price = criteria.get("min_price", 2.0)
        top_n_stocks = criteria.get("top_n_stocks", 20)

        tickers = self._get_us_tickers(min_market_cap, max_market_cap)
        if not tickers:
            logging.error("No stocks found for scanning")
            return []

        stock_data = []
        total = len(tickers)
        logging.info(f"Starting scan. Candidates: {total}")

        for item in tickers:
            ticker = item['code']
            market_cap = item.get('market_capitalization')
            exchange = 'US'

            data = self._get_stock_data(
                ticker, market_cap, exchange,
                min_avg_volume, min_avg_transaction_value, min_volatility, min_price
            )
            if data:
                stock_data.append(data)

            time.sleep(0.2)  # Rate limiting

        if not stock_data:
            logging.error("No stocks passed the filtering criteria")
            return []

        df_all = pd.DataFrame(stock_data)
        df_top = (df_all
                  .sort_values('volatility', ascending=False)
                  .head(top_n_stocks)
                  .reset_index(drop=True))

        logging.info(f"Scan complete. Qualified: {len(df_all)}. Returning top {len(df_top)} by volatility.")
        return df_top.to_dict('records')

    def start_background_scan(self, criteria: Dict) -> int:
        """Start a background scan and return scan ID"""
        db = SessionLocal()
        try:
            try:
                portfolio_size = float(Settings.get_portfolio_size(db))
            except Exception:
                portfolio_size = 350000.0

            # Persist strategy selection (Strategy IDs) for the analysis phase. If the client sends an empty
            # list, normalize it to null so the backend runs all enabled strategies.
            selected_strategies = criteria.get("selected_strategies")
            if isinstance(selected_strategies, list) and len(selected_strategies) == 0:
                selected_strategies = None

            # Create scan record
            scan_record = ScanRecord(
                status="in_progress",
                criteria=criteria,
                portfolio_size=portfolio_size,
                selected_strategies=selected_strategies,
            )
            db.add(scan_record)
            db.commit()
            db.refresh(scan_record)
            
            self.logger.info(f"📝 Created scan record with ID: {scan_record.id}")
            
            # Start background task (in a real application, use Celery or similar)
            import threading
            thread = threading.Thread(
                target=self._background_scan_task, 
                args=(scan_record.id, criteria)
            )
            thread.daemon = True
            thread.start()
            
            self.logger.info(f"🎯 Background thread started for scan {scan_record.id}")
            return scan_record.id
        except Exception as e:
            db.rollback()
            self.logger.error(f"Error starting background scan: {e}")
            raise
        finally:
            db.close()
    
    def _update_scan_progress(self, scan_id: int, progress: int):
        """Update scan progress in database and send WebSocket event"""
        db = SessionLocal()
        try:
            scan_record = db.query(ScanRecord).filter(ScanRecord.id == scan_id).first()
            if scan_record:
                scan_record.scan_progress = progress
                db.commit()
                
                # Send WebSocket event for progress update
                try:
                    import asyncio
                    from app.services.websocket_manager import websocket_manager
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    loop.run_until_complete(websocket_manager.send_progress_event(scan_id, progress))
                    loop.close()
                except Exception as ws_error:
                    self.logger.warning(f"Failed to send progress WebSocket event: {ws_error}")
        except Exception as e:
            self.logger.error(f"Error updating scan progress: {e}")
        finally:
            db.close()

    def _background_scan_task(self, scan_id: int, criteria: Dict):
        """Background task that performs the actual scan"""
        # Configure logging for this thread
        import logging
        logger = logging.getLogger(f"{__name__}.background_task")
        logger.info(f"🔥 Background task thread started for scan {scan_id}")
        
        db = SessionLocal()
        try:
            scan_record = db.query(ScanRecord).filter(ScanRecord.id == scan_id).first()
            if not scan_record:
                logger.error(f"Scan record {scan_id} not found")
                return

            logger.info(f"🚀 Starting background scan {scan_id} with criteria: {criteria}")
            logger.info(f"📊 Market cap range: ${criteria.get('min_market_cap', 150e6)/1e6:.0f}M - ${criteria.get('max_market_cap', 160e6)/1e6:.0f}M")
            
            # Perform the scan (synchronous version)
            stocks = self._sync_scan_stocks(criteria, scan_id, logger)
            
            # Store detailed symbol data in stock_symbols JSON field
            logger.info(f"💾 Saving detailed symbol data for {len(stocks)} stocks")
            detailed_symbols = []
            for stock in stocks:
                detailed_symbols.append({
                    'ticker': stock['ticker'],
                    'company_name': stock.get('company_name'),
                    'market_cap': stock.get('market_cap'),
                    'price': stock['price'],
                    'volatility': stock['volatility'],
                    'avg_volume': stock['avg_volume'],
                    'avg_transaction_value': stock['avg_transaction_value']
                })
            
            # Update scan record
            scan_record.status = "completed"
            scan_record.stock_symbols = detailed_symbols  # Now stores detailed data as JSON
            scan_record.total_found = len(stocks)
            scan_record.completed_at = func.now()
            
            db.commit()
            logger.info(f"✅ Background scan {scan_id} completed successfully!")
            logger.info(f"🎯 Final Results: Found {len(stocks)} qualified stocks")
            
            # Send WebSocket event for scan completion
            try:
                import asyncio
                from app.services.websocket_manager import websocket_manager
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                loop.run_until_complete(websocket_manager.broadcast_scan_completed(scan_id))
                loop.close()
                logger.info(f"📡 WebSocket event sent for scan completion: {scan_id}")
            except Exception as ws_error:
                logger.error(f"Failed to send WebSocket event for scan {scan_id}: {ws_error}")
            if detailed_symbols:
                top_tickers = [symbol['ticker'] for symbol in detailed_symbols[:5]]
                logger.info(f"📈 Top stock symbols: {top_tickers}{'...' if len(detailed_symbols) > 5 else ''}")
            
            # Auto-start analysis if stocks were found
            if detailed_symbols:
                try:
                    logger.info(f"🔄 Auto-starting analysis for scan {scan_id}")
                    from app.services.trading_analysis import TradingAnalysisService
                    analysis_service = TradingAnalysisService()

                    scan_record.analysis_status = "analyzing"
                    scan_record.analysis_progress = 0
                    db.commit()

                    analysis_service.start_background_analysis(scan_id)
                    logger.info(f"✅ Auto-started analysis for scan {scan_id}")
                except Exception as auto_start_error:
                    logger.error(f"Failed to auto-start analysis for scan {scan_id}: {auto_start_error}")
            else:
                logger.info(f"⚠️ No stocks found in scan {scan_id}, skipping analysis")
            
        except Exception as e:
            logger.error(f"❌ Error in background scan {scan_id}: {e}")
            try:
                scan_record = db.query(ScanRecord).filter(ScanRecord.id == scan_id).first()
                if scan_record:
                    scan_record.status = "failed"
                    scan_record.error_message = str(e)
                    scan_record.completed_at = func.now()
                    db.commit()
            except Exception as db_error:
                logger.error(f"Error updating failed scan record: {db_error}")
        finally:
            db.close()

    def _sync_scan_stocks(self, criteria: Dict, scan_id: int = None, logger=None) -> List[Dict]:
        """Synchronous version of scan_stocks for background tasks"""
        min_market_cap = criteria.get("min_market_cap", 150e6)
        max_market_cap = criteria.get("max_market_cap", 160e6)
        min_avg_volume = criteria.get("min_avg_volume", 15000)
        min_avg_transaction_value = criteria.get("min_avg_transaction_value", 150000)
        min_volatility = criteria.get("min_volatility", 0.4)
        min_price = criteria.get("min_price", 2.0)
        top_n_stocks = criteria.get("top_n_stocks", 20)

        # Use provided logger or fall back to instance logger
        if logger is None:
            logger = self.logger
            
        scan_prefix = f"[Scan {scan_id}] " if scan_id else ""
        
        logger.info(f"{scan_prefix}🔍 Phase 1: Fetching candidate tickers from market...")
        if scan_id:
            self._update_scan_progress(scan_id, 10)  # 10% - Starting to fetch tickers
        
        tickers = self._get_us_tickers(min_market_cap, max_market_cap)
        if not tickers:
            logger.error(f"{scan_prefix}❌ No stocks found for scanning")
            return []

        if scan_id:
            self._update_scan_progress(scan_id, 25)  # 25% - Tickers fetched, starting analysis

        stock_data = []
        total = len(tickers)
        logger.info(f"{scan_prefix}📋 Phase 2: Analyzing {total} candidate stocks...")
        logger.info(f"{scan_prefix}🎯 Filtering criteria:")
        logger.info(f"{scan_prefix}   • Min volume: {min_avg_volume:,}")
        logger.info(f"{scan_prefix}   • Min transaction value: ${min_avg_transaction_value:,}")
        logger.info(f"{scan_prefix}   • Min volatility: {min_volatility}")
        logger.info(f"{scan_prefix}   • Min price: ${min_price}")

        processed_count = 0
        qualified_count = 0
        
        for item in tickers:
            ticker = item['code']
            market_cap = item.get('market_capitalization')
            exchange = 'US'
            processed_count += 1

            # Update progress in database and WebSocket (25% to 85% during stock analysis)
            analysis_progress = 25 + int((processed_count / total) * 60)  # 25% to 85%
            if scan_id and (processed_count % 10 == 0 or processed_count <= 10):
                self._update_scan_progress(scan_id, analysis_progress)

            # Log progress every 50 stocks
            if processed_count % 50 == 0 or processed_count <= 10:
                logger.info(f"{scan_prefix}⏳ Progress: {processed_count}/{total} stocks processed ({processed_count/total*100:.1f}%) - {qualified_count} qualified so far")

            data = self._get_stock_data(
                ticker, market_cap, exchange,
                min_avg_volume, min_avg_transaction_value, min_volatility, min_price
            )
            if data:
                qualified_count += 1
                stock_data.append(data)
                logger.info(f"{scan_prefix}✅ {ticker} qualified (volatility: {data['volatility']:.3f}, price: ${data['price']:.2f})")

            time.sleep(0.2)  # Rate limiting

        if not stock_data:
            logger.error(f"{scan_prefix}❌ No stocks passed the filtering criteria out of {total} candidates")
            return []

        logger.info(f"{scan_prefix}📊 Phase 3: Ranking qualified stocks by volatility...")
        if scan_id:
            self._update_scan_progress(scan_id, 90)  # 90% - Ranking stocks
        
        df_all = pd.DataFrame(stock_data)
        df_top = (df_all
                  .sort_values('volatility', ascending=False)
                  .head(top_n_stocks)
                  .reset_index(drop=True))

        if scan_id:
            self._update_scan_progress(scan_id, 100)  # 100% - Scan complete

        logger.info(f"{scan_prefix}🏆 Scan complete! Qualified: {len(df_all)} stocks, returning top {len(df_top)} by volatility")
        
        # Log top results
        for i, stock in enumerate(df_top.to_dict('records')[:5], 1):
            logger.info(f"{scan_prefix}#{i} {stock['ticker']}: volatility={stock['volatility']:.3f}, price=${stock['price']:.2f}")
            
        return df_top.to_dict('records')


    def get_all_scans(self, page: int = 1, page_size: int = 10) -> Dict[str, Any]:
        """Return paginated scan records ordered by most recent first."""
        db = SessionLocal()
        try:
            total_records = db.query(func.count(ScanRecord.id)).scalar() or 0
            total_pages = math.ceil(total_records / page_size) if total_records > 0 else 0
            offset = max((page - 1) * page_size, 0)

            scan_records = (
                db.query(ScanRecord)
                .order_by(ScanRecord.created_at.desc())
                .offset(offset)
                .limit(page_size)
                .all()
            )

            items = [
                {
                    "id": record.id,
                    "status": record.status,
                    "scan_progress": record.scan_progress,
                    "analysis_status": record.analysis_status,
                    "analysis_progress": record.analysis_progress,
                    "criteria": record.criteria,
                    "stock_symbols": record.stock_symbols,
                    "total_found": record.total_found,
                    "error_message": record.error_message,
                    "portfolio_size": float(record.portfolio_size) if record.portfolio_size is not None else 350000.00,
                    "created_at": record.created_at.isoformat() if record.created_at else None,
                    "completed_at": record.completed_at.isoformat() if record.completed_at else None,
                    "analyzed_at": record.analyzed_at.isoformat() if record.analyzed_at else None,
                }
                for record in scan_records
            ]

            return {
                "items": items,
                "total": total_records,
                "page": page,
                "page_size": page_size,
                "total_pages": total_pages,
                "has_next": page < total_pages,
                "has_previous": page > 1,
            }
        finally:
            db.close()

    def get_scan_by_id(self, scan_id: int) -> Dict:
        """Get a specific scan record by ID"""
        db = SessionLocal()
        try:
            scan_record = db.query(ScanRecord).filter(ScanRecord.id == scan_id).first()
            
            if not scan_record:
                raise ValueError(f"Scan with ID {scan_id} not found")
            
            return {
                "id": scan_record.id,
                "status": scan_record.status,
                "scan_progress": scan_record.scan_progress,
                "analysis_status": scan_record.analysis_status,
                "analysis_progress": scan_record.analysis_progress,
                "criteria": scan_record.criteria,
                "stock_symbols": scan_record.stock_symbols,
                "total_found": scan_record.total_found,
                "error_message": scan_record.error_message,
                "portfolio_size": float(scan_record.portfolio_size) if scan_record.portfolio_size is not None else 350000.00,
                "created_at": scan_record.created_at.isoformat() if scan_record.created_at else None,
                "completed_at": scan_record.completed_at.isoformat() if scan_record.completed_at else None,
                "analyzed_at": scan_record.analyzed_at.isoformat() if scan_record.analyzed_at else None,
                "analysis_counts": {
                    "analysis": len(scan_record.analysis_results or []),
                },
                "selected_strategies": scan_record.selected_strategies,
            }
        finally:
            db.close()
    
    def delete_scan_by_id(self, scan_id: int) -> bool:
        """Delete a specific scan record by ID"""
        db = SessionLocal()
        try:
            scan_record = db.query(ScanRecord).filter(ScanRecord.id == scan_id).first()
            
            if not scan_record:
                raise ValueError(f"Scan with ID {scan_id} not found")
            
            db.delete(scan_record)
            db.commit()
            return True
        finally:
            db.close()

    def delete_all_scans(self) -> int:
        """Delete all scan records and return the number of rows removed"""
        db = SessionLocal()
        try:
            deleted_count = db.query(ScanRecord).delete(synchronize_session=False)
            db.commit()
            return deleted_count
        finally:
            db.close()
    
    def get_symbols_by_scan_id(self, scan_id: int) -> List[Dict]:
        """Get detailed symbol data for a scan"""
        db = SessionLocal()
        try:
            scan_record = db.query(ScanRecord).filter(ScanRecord.id == scan_id).first()
            if not scan_record:
                raise ValueError(f"Scan with ID {scan_id} not found")
            
            if not scan_record.stock_symbols:
                return []
            
            # Handle both old format (list of strings) and new format (list of objects)
            if isinstance(scan_record.stock_symbols[0], dict):
                # New format: already detailed objects
                return scan_record.stock_symbols
            else:
                # Old format: convert ticker strings to simple objects
                return [{"ticker": ticker} for ticker in scan_record.stock_symbols]
                
        finally:
            db.close()
    
    def get_symbol_tickers_by_scan_id(self, scan_id: int) -> List[str]:
        """Get just the ticker symbols for a scan (for backward compatibility)"""
        db = SessionLocal()
        try:
            scan_record = db.query(ScanRecord).filter(ScanRecord.id == scan_id).first()
            if not scan_record:
                raise ValueError(f"Scan with ID {scan_id} not found")
                
            if not scan_record.stock_symbols:
                return []
            
            # Handle both old format (list of strings) and new format (list of objects)
            if isinstance(scan_record.stock_symbols[0], dict):
                # New format: extract ticker from objects
                return [symbol['ticker'] for symbol in scan_record.stock_symbols]
            else:
                # Old format: already ticker strings
                return scan_record.stock_symbols
                
        finally:
            db.close()
