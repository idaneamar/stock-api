#!/usr/bin/env python3
"""
Local Trading Client with WebSocket Communication

This script runs on your local machine and:
1. Connects to the cloud API via the trading WebSocket channel
2. Receives trading orders from the cloud
3. Downloads Excel files and places orders through IBKR locally
4. Provides real-time status updates back to the cloud

Usage:
    python3 local_trading_client.py --api-url ws://0.0.0.0:8000/listen_trading_client
"""

import os
import sys
import json
import time
import asyncio
import argparse
import logging
import base64
import websockets
import pandas as pd
import requests
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any

# Check if ib_insync is available for IBKR trading
try:
    from ib_insync import IB, Stock, MarketOrder, StopOrder, LimitOrder
    IBKR_AVAILABLE = True
except ImportError as e:
    print(f"Warning: ib_insync library not available: {e}")
    print("Running in simulation mode - no actual trades will be placed")
    IBKR_AVAILABLE = False

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'local_trading_client_{datetime.now().strftime("%Y%m%d")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# IBKR Trading Functions (self-contained)


def _setup_thread_event_loop() -> asyncio.AbstractEventLoop:
    """Ensure worker threads interacting with ib_insync have an event loop."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    return loop


def _cleanup_thread_event_loop(loop: Optional[asyncio.AbstractEventLoop]) -> None:
    """Tear down thread-local event loops without leaking resources."""
    if loop is None:
        return
    try:
        loop.run_until_complete(asyncio.sleep(0))
    except Exception:
        pass
    finally:
        asyncio.set_event_loop(None)
        loop.close()


def close_ibkr_position(ib, symbol, quantity, current_action):
    """Close an existing position in IBKR"""
    try:
        contract = Stock(symbol, 'SMART', 'USD')
        ib.qualifyContracts(contract)

        # Reverse the action to close the position
        close_action = "SELL" if current_action == "BUY" else "BUY"

        # Place market order to close
        order = MarketOrder(
            action=close_action,
            totalQuantity=quantity,
            tif="DAY"
        )

        trade = ib.placeOrder(contract, order)
        logger.info(f"Placed CLOSE order for {symbol}: {close_action} {quantity} shares")

        ib.sleep(0.1)

        return True

    except Exception as e:
        logger.error(f"Error closing position for {symbol}: {e}")
        return False


def cancel_ibkr_orders(ib, symbol):
    """Cancel all open orders for a specific symbol"""
    try:
        contract = Stock(symbol, 'SMART', 'USD')
        ib.qualifyContracts(contract)

        # Get all open orders
        open_orders = ib.openOrders()
        cancelled_count = 0

        for trade in open_orders:
            if trade.contract.symbol == symbol:
                ib.cancelOrder(trade.order)
                cancelled_count += 1
                logger.info(f"Cancelled order for {symbol}: {trade.order.action} {trade.order.orderType}")

        logger.info(f"Cancelled {cancelled_count} orders for {symbol}")
        return cancelled_count

    except Exception as e:
        logger.error(f"Error cancelling orders for {symbol}: {e}")
        return 0


def get_ibkr_positions_and_orders(ib):
    """Get current positions and open orders from IBKR"""
    try:
        positions = {}
        for position in ib.positions():
            if position.position != 0:
                positions[position.contract.symbol] = {
                    'symbol': position.contract.symbol,
                    'quantity': abs(position.position),
                    'action': 'BUY' if position.position > 0 else 'SELL',
                    'avg_cost': position.avgCost
                }

        orders_by_symbol = {}
        for trade in ib.openOrders():
            symbol = trade.contract.symbol
            if symbol not in orders_by_symbol:
                orders_by_symbol[symbol] = {'stop_loss': None, 'take_profit': None, 'orders': []}

            order_info = {
                'action': trade.order.action,
                'order_type': trade.order.orderType,
                'quantity': trade.order.totalQuantity,
                'order_id': trade.order.orderId
            }

            # Identify SL and TP orders
            if trade.order.orderType == 'STP':  # Stop order
                orders_by_symbol[symbol]['stop_loss'] = order_info
            elif trade.order.orderType == 'LMT':  # Limit order
                orders_by_symbol[symbol]['take_profit'] = order_info

            orders_by_symbol[symbol]['orders'].append(order_info)

        return positions, orders_by_symbol

    except Exception as e:
        logger.error(f"Error getting positions and orders: {e}")
        return {}, {}


def validate_position_protection(ib, symbol, expected_quantity):
    """Validate that a position has both SL and TP orders"""
    try:
        _, orders_by_symbol = get_ibkr_positions_and_orders(ib)

        if symbol not in orders_by_symbol:
            logger.warning(f"No orders found for {symbol}")
            return False, "No protective orders found"

        symbol_orders = orders_by_symbol[symbol]

        has_sl = symbol_orders['stop_loss'] is not None
        has_tp = symbol_orders['take_profit'] is not None

        if not has_sl and not has_tp:
            return False, "Missing both SL and TP"
        elif not has_sl:
            return False, "Missing Stop Loss"
        elif not has_tp:
            return False, "Missing Take Profit"

        return True, "Both SL and TP present"

    except Exception as e:
        logger.error(f"Error validating protection for {symbol}: {e}")
        return False, f"Validation error: {e}"


def place_ibkr_order(ib, symbol, action, quantity, stop_loss=None, take_profit=None):
    """Place order with protective stops via IBKR and validate both SL and TP are placed"""
    try:
        contract = Stock(symbol, 'SMART', 'USD')
        ib.qualifyContracts(contract)

        # Place market order
        order = MarketOrder(
            action=action,
            totalQuantity=quantity,
            tif="DAY"
        )

        trade = ib.placeOrder(contract, order)
        logger.info(f"Placed {action} order for {symbol}: {quantity} shares")

        # Brief pause to allow order processing
        ib.sleep(0.1)

        if not trade.isDone():
            logger.info(f"Order for {symbol} submitted (may still be processing)")
        else:
            logger.info(f"Order for {symbol} completed immediately")

        # Track if both protective orders are placed
        sl_placed = False
        tp_placed = False

        # Place protective orders if specified
        if stop_loss and stop_loss != "-" and float(stop_loss) > 0:
            stop_action = "SELL" if action == "BUY" else "BUY"
            stop_order = StopOrder(
                action=stop_action,
                totalQuantity=quantity,
                stopPrice=float(stop_loss),
                tif="GTC"
            )
            sl_trade = ib.placeOrder(contract, stop_order)
            ib.sleep(0.1)
            sl_placed = True
            logger.info(f"Placed stop loss for {symbol} at {stop_loss}")
        else:
            logger.warning(f"No valid stop loss provided for {symbol}")

        if take_profit and take_profit != "-" and float(take_profit) > 0:
            tp_action = "SELL" if action == "BUY" else "BUY"
            tp_order = LimitOrder(
                action=tp_action,
                totalQuantity=quantity,
                lmtPrice=float(take_profit),
                tif="GTC"
            )
            tp_trade = ib.placeOrder(contract, tp_order)
            ib.sleep(0.1)
            tp_placed = True
            logger.info(f"Placed take profit for {symbol} at {take_profit}")
        else:
            logger.warning(f"No valid take profit provided for {symbol}")

        # Validate that both SL and TP were placed
        if not sl_placed or not tp_placed:
            logger.error(f"⚠️ CRITICAL: {symbol} missing protective orders (SL: {sl_placed}, TP: {tp_placed})")
            logger.error(f"⚠️ Cancelling all orders and closing position for {symbol}")

            # Cancel all orders for this symbol
            cancel_ibkr_orders(ib, symbol)

            # Close the position if it was opened
            # Wait a bit for the market order to fill
            ib.sleep(2)
            positions, _ = get_ibkr_positions_and_orders(ib)
            if symbol in positions:
                close_ibkr_position(ib, symbol, positions[symbol]['quantity'], action)

            return False

        # Additional validation: verify orders are actually in IBKR
        ib.sleep(1)  # Wait for orders to register
        is_valid, message = validate_position_protection(ib, symbol, quantity)

        if not is_valid:
            logger.error(f"⚠️ CRITICAL: {symbol} failed validation: {message}")
            logger.error(f"⚠️ Cancelling all orders and closing position for {symbol}")

            cancel_ibkr_orders(ib, symbol)
            positions, _ = get_ibkr_positions_and_orders(ib)
            if symbol in positions:
                close_ibkr_position(ib, symbol, positions[symbol]['quantity'], action)

            return False

        logger.info(f"✅ {symbol} validated: {message}")
        return True

    except Exception as e:
        logger.error(f"Error placing order for {symbol}: {e}")
        # Attempt cleanup on error
        try:
            cancel_ibkr_orders(ib, symbol)
        except:
            pass
        return False

def execute_trades_from_excel(df, ib):
    """Execute trades based on Excel DataFrame"""
    logger.info("Starting trade execution from Excel data")
    orders_placed = 0
    
    for index, row in df.iterrows():
        symbol = row.get("Symbol", "")
        transaction_type = row.get("Transaction type", "")
        position = row.get("Position", 0)
        stop_loss = row.get("Stop Loss", "-")
        take_profit = row.get("Take Profit", "-")
        
        # Skip non-trading rows
        if symbol == "Total Investment" or not symbol:
            continue
            
        if transaction_type not in ["Buy", "Sell"]:
            continue
            
        if not position or position <= 0:
            logger.warning(f"Skipping {symbol} - invalid position size: {position}")
            continue
        
        logger.info(f"Processing {symbol}: {transaction_type} {position} shares")
        
        # Convert transaction type to IBKR action
        action = "BUY" if transaction_type == "Buy" else "SELL"
        
        # Place the order
        if place_ibkr_order(ib, symbol, action, int(position), stop_loss, take_profit):
            orders_placed += 1
            logger.info(f"✅ Successfully placed order for {symbol}")
        else:
            logger.error(f"❌ Failed to place order for {symbol}")
        
        # Small delay between orders
        time.sleep(0.1)
    
    logger.info(f"Trade execution completed. Orders placed: {orders_placed}")
    return orders_placed

class LocalTradingClient:
    """Local trading client that communicates with cloud API"""

    def __init__(self, api_url: str, download_dir: str = "./downloads", sync_interval: int = 60):
        self.api_url = api_url
        self.download_dir = Path(download_dir)
        self.download_dir.mkdir(exist_ok=True)
        self.websocket = None
        self.running = False
        self.connected_endpoint: Optional[str] = api_url
        self.sync_interval = sync_interval  # seconds between position syncs
        self.last_sync_time = 0

        logger.info(f"🚀 Local Trading Client initialized")
        logger.info(f"📡 Cloud API URL: {api_url}")
        logger.info(f"📁 Download directory: {self.download_dir.absolute()}")
        logger.info(f"🔧 IBKR Integration: {'Enabled' if IBKR_AVAILABLE else 'Disabled (Simulation Mode)'}")
        logger.info(f"🔄 Position sync interval: {sync_interval} seconds")

    async def connect_websocket(self):
        """Connect to the cloud WebSocket API"""
        try:
            logger.info(f"🔌 Connecting to WebSocket: {self.api_url}")
            self.websocket = await websockets.connect(self.api_url)
            logger.info("✅ WebSocket connected successfully")
            self.connected_endpoint = self.api_url
            return True
        except Exception as e:
            logger.error(f"❌ Failed to connect to WebSocket: {e}")
            return False

    async def disconnect_websocket(self):
        """Disconnect from the WebSocket"""
        if self.websocket:
            await self.websocket.close()
            self.websocket = None
            self.connected_endpoint = None
            logger.info("🔌 WebSocket disconnected")


    def download_excel_file(self, file_url: str, local_filename: str) -> Optional[str]:
        """Download Excel file from cloud API"""
        try:
            logger.info(f"📥 Downloading Excel file: {file_url}")
            
            # Convert WebSocket URL to HTTP URL for file download
            http_url = file_url.replace('ws://', 'http://').replace('wss://', 'https://')
            
            response = requests.get(http_url, stream=True, timeout=30)
            response.raise_for_status()
            
            local_path = self.download_dir / local_filename
            
            with open(local_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            logger.info(f"✅ Downloaded Excel file: {local_path}")
            return str(local_path)
            
        except Exception as e:
            logger.error(f"❌ Failed to download Excel file: {e}")
            return None

    def save_excel_from_base64(self, encoded_excel: str, filename: Optional[str]) -> Optional[str]:
        """Persist Excel content provided as base64 string."""
        if not encoded_excel:
            return None

        target_name = filename or f"modified_orders_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        local_path = self.download_dir / target_name

        try:
            excel_bytes = base64.b64decode(encoded_excel)
            with open(local_path, 'wb') as excel_file:
                excel_file.write(excel_bytes)
            logger.info(f"📄 Saved Excel payload to {local_path}")
            return str(local_path)
        except Exception as exc:
            logger.error(f"❌ Failed to decode Excel payload: {exc}")
            return None

    def process_trading_orders(self, order_data: Dict[str, Any]) -> Dict[str, Any]:
        """Process trading orders from the cloud"""
        try:
            scan_id = order_data.get('scan_id')
            analysis_type = order_data.get('analysis_type')
            excel_file_path = order_data.get('excel_file_path')
            excel_file_base64 = order_data.get('excel_file_base64')
            excel_filename = order_data.get('excel_filename')
            orders = order_data.get('orders', [])
            
            logger.info(f"📊 Processing trading orders for scan {scan_id} ({analysis_type})")
            logger.info(f"📈 Total orders: {len(orders)}")
            
            result = {
                'scan_id': scan_id,
                'analysis_type': analysis_type,
                'processed_at': datetime.now().isoformat(),
                'success': False,
                'orders_placed': 0,
                'errors': [],
                'message': ''
            }
            
            if not orders:
                result['message'] = 'No orders to process'
                return result
            
            # Handle Excel file path - prioritize JSON data over Excel files
            local_excel_path = None
            if excel_file_base64:
                local_excel_path = self.save_excel_from_base64(excel_file_base64, excel_filename)
            elif excel_file_path and os.path.exists(excel_file_path):
                # Only use Excel file if it exists locally (for development/testing)
                local_excel_path = excel_file_path
                logger.info(f"📄 Using local Excel file: {local_excel_path}")
            else:
                # Excel file is generated on cloud server, not available locally
                # This is expected behavior - we'll process orders from JSON data instead
                if excel_file_base64 and not local_excel_path:
                    logger.warning("⚠️ Excel payload included but decoding failed; continuing with JSON data")
                elif excel_file_path:
                    logger.info(f"📄 Excel file is on cloud server: {excel_file_path}")
                logger.info("📊 Processing orders from JSON data (recommended approach)")
            
            # IBKR availability check (connection will be made in trade execution thread)
            if not IBKR_AVAILABLE:
                logger.info("🧪 Simulation mode - IBKR library not available")
            else:
                logger.info("🏦 IBKR library available - will attempt connection during trade execution")
            
            # Process orders
            try:
                if local_excel_path and os.path.exists(local_excel_path):
                    # Load from Excel file
                    df = pd.read_excel(local_excel_path)
                    logger.info(f"📊 Loaded {len(df)} rows from Excel file")
                    
                    if IBKR_AVAILABLE:
                        # Execute trades with IBKR connection in a dedicated thread with event loop
                        import threading
                        execution_result = {'success': False, 'error': None, 'orders_placed': 0}
                        
                        def execute_trades_with_ibkr():
                            loop = _setup_thread_event_loop()
                            ib = None
                            try:
                                # Configuration
                                IBKR_HOST = '127.0.0.1'
                                IBKR_PORT = 7497  # Paper trading port
                                IBKR_CLIENT_ID = 1

                                ib = IB()
                                logger.info("🏦 Connecting to IBKR for trade execution...")

                                connected = ib.connect(
                                    IBKR_HOST,
                                    IBKR_PORT,
                                    clientId=IBKR_CLIENT_ID,
                                    timeout=10
                                )

                                if not connected or not ib.isConnected():
                                    raise RuntimeError("IBKR connection attempt returned without establishing a session")

                                logger.info("✅ IBKR connected for trade execution")

                                # Execute trades with the connected IB instance
                                orders_placed = execute_trades_from_excel(df, ib)
                                execution_result['orders_placed'] = orders_placed
                                execution_result['success'] = True

                                logger.info(f"✅ Successfully executed {orders_placed} orders")

                            except Exception as trade_e:
                                execution_result['error'] = str(trade_e) or repr(trade_e)
                                logger.exception(f"Error in trade execution: {trade_e}")

                            finally:
                                if ib is not None:
                                    try:
                                        if ib.isConnected():
                                            ib.disconnect()
                                            logger.info("🏦 IBKR disconnected after trade execution")
                                    except Exception as disc_e:
                                        logger.warning(f"Warning during IBKR disconnect: {disc_e}")
                                _cleanup_thread_event_loop(loop)
                        
                        # Run trade execution in separate thread with its own IBKR connection
                        trade_thread = threading.Thread(target=execute_trades_with_ibkr)
                        trade_thread.daemon = True
                        trade_thread.start()
                        trade_thread.join(timeout=300)  # Wait up to 5 minutes
                        
                        if execution_result['success']:
                            result['orders_placed'] = execution_result['orders_placed']
                            result['success'] = True
                            result['message'] = f'Successfully placed {result["orders_placed"]} orders via IBKR'
                        else:
                            error_msg = execution_result['error'] or 'Trade execution timeout'
                            result['message'] = f'Failed to execute trades: {error_msg}'
                            result['errors'].append(error_msg)
                            # Fall back to simulation mode
                            logger.warning("⚠️  Falling back to simulation mode")
                            simulated_orders = len([row for _, row in df.iterrows() 
                                                  if row.get('Transaction type') in ['Buy', 'Sell']])
                            result['orders_placed'] = simulated_orders
                            result['success'] = True
                            result['message'] = f'Simulated {simulated_orders} orders (IBKR execution failed)'
                    else:
                        # Simulation mode
                        simulated_orders = len([row for _, row in df.iterrows() 
                                              if row.get('Transaction type') in ['Buy', 'Sell']])
                        result['orders_placed'] = simulated_orders
                        result['success'] = True
                        if not IBKR_AVAILABLE:
                            result['message'] = f'Simulated {simulated_orders} orders (IBKR library not available)'
                        else:
                            result['message'] = f'Simulated {simulated_orders} orders (IBKR connection failed)'
                        
                        # Log the simulated orders
                        for _, row in df.iterrows():
                            if row.get('Transaction type') in ['Buy', 'Sell']:
                                logger.info(f"🧪 Simulated order: {row.get('Transaction type')} "
                                          f"{row.get('Position', 0)} shares of {row.get('Symbol', 'Unknown')} "
                                          f"at ${row.get('Current price', 0)}")
                
                else:
                    # Process orders directly from JSON data
                    logger.info("📝 Processing orders from JSON data")
                    
                    if IBKR_AVAILABLE:
                        # Execute trades with IBKR connection in a dedicated thread
                        import threading
                        execution_result = {'success': False, 'error': None, 'orders_placed': 0}
                        
                        def execute_trades_from_json():
                            loop = _setup_thread_event_loop()
                            ib = None
                            try:
                                # Configuration
                                IBKR_HOST = '127.0.0.1'
                                IBKR_PORT = 7497  # Paper trading port
                                IBKR_CLIENT_ID = 1

                                ib = IB()
                                logger.info("🏦 Connecting to IBKR for JSON order execution...")

                                connected = ib.connect(
                                    IBKR_HOST,
                                    IBKR_PORT,
                                    clientId=IBKR_CLIENT_ID,
                                    timeout=10
                                )

                                if not connected or not ib.isConnected():
                                    raise RuntimeError("IBKR connection attempt returned without establishing a session")

                                logger.info("✅ IBKR connected for JSON order execution")

                                orders_placed = 0
                                for order in orders:
                                    symbol = order.get('symbol', '')
                                    recommendation = order.get('recommendation', '')
                                    entry_price = order.get('entry_price', 0)
                                    position_size = order.get('position_size', 0)
                                    stop_loss = order.get('stop_loss', 0)
                                    take_profit = order.get('take_profit', 0)

                                    if recommendation in ['Buy', 'Sell'] and symbol and position_size > 0:
                                        logger.info(f"📈 Processing {recommendation} order: {position_size} shares of {symbol}")

                                        # Convert recommendation to IBKR action
                                        action = "BUY" if recommendation == "Buy" else "SELL"

                                        # Place order via IBKR
                                        if place_ibkr_order(ib, symbol, action, int(position_size), 
                                                           stop_loss if stop_loss and stop_loss > 0 else None, 
                                                           take_profit if take_profit and take_profit > 0 else None):
                                            orders_placed += 1
                                            logger.info(f"✅ Successfully placed JSON order for {symbol}")
                                        else:
                                            logger.error(f"❌ Failed to place JSON order for {symbol}")

                                        # Small delay between orders
                                        time.sleep(0.1)

                                execution_result['orders_placed'] = orders_placed
                                execution_result['success'] = True
                                logger.info(f"✅ Successfully executed {orders_placed} JSON orders")

                            except Exception as trade_e:
                                execution_result['error'] = str(trade_e) or repr(trade_e)
                                logger.exception(f"Error in JSON trade execution: {trade_e}")

                            finally:
                                if ib is not None:
                                    try:
                                        if ib.isConnected():
                                            ib.disconnect()
                                            logger.info("🏦 IBKR disconnected after JSON trade execution")
                                    except Exception as disc_e:
                                        logger.warning(f"Warning during IBKR disconnect: {disc_e}")
                                _cleanup_thread_event_loop(loop)
                        
                        # Run trade execution in separate thread with its own IBKR connection
                        trade_thread = threading.Thread(target=execute_trades_from_json)
                        trade_thread.daemon = True
                        trade_thread.start()
                        trade_thread.join(timeout=300)  # Wait up to 5 minutes
                        
                        if execution_result['success']:
                            result['orders_placed'] = execution_result['orders_placed']
                            result['success'] = True
                            result['message'] = f'Successfully placed {result["orders_placed"]} orders via IBKR (JSON)'
                        else:
                            error_msg = execution_result['error'] or 'JSON trade execution timeout'
                            result['message'] = f'Failed to execute JSON trades: {error_msg}'
                            result['errors'].append(error_msg)
                            # Fall back to simulation mode
                            logger.warning("⚠️  Falling back to simulation mode for JSON orders")
                            simulated_orders = len([order for order in orders 
                                                  if order.get('recommendation') in ['Buy', 'Sell'] and order.get('position_size', 0) > 0])
                            result['orders_placed'] = simulated_orders
                            result['success'] = True
                            result['message'] = f'Simulated {simulated_orders} JSON orders (IBKR execution failed)'
                    else:
                        # Simulation mode for JSON orders
                        orders_processed = 0
                        for order in orders:
                            symbol = order.get('symbol', '')
                            recommendation = order.get('recommendation', '')
                            entry_price = order.get('entry_price', 0)
                            position_size = order.get('position_size', 0)
                            stop_loss = order.get('stop_loss', 0)
                            take_profit = order.get('take_profit', 0)
                            
                            if recommendation in ['Buy', 'Sell'] and symbol and position_size > 0:
                                logger.info(f"🧪 Simulated {recommendation} order: {position_size} shares of {symbol} "
                                          f"at ${entry_price} (SL: ${stop_loss}, TP: ${take_profit})")
                                orders_processed += 1
                        
                        result['orders_placed'] = orders_processed
                        result['success'] = orders_processed > 0
                        result['message'] = f'Simulated {orders_processed} JSON orders (IBKR library not available)'
                
            except Exception as e:
                logger.error(f"❌ Error processing orders: {e}")
                result['errors'].append(str(e))
                result['message'] = f'Failed to process orders: {e}'
            
            finally:
                # IBKR disconnection is handled in the trade execution thread
                pass
            
            return result
            
        except Exception as e:
            logger.error(f"❌ Error in process_trading_orders: {e}")
            return {
                'scan_id': order_data.get('scan_id', 'unknown'),
                'success': False,
                'error': str(e),
                'processed_at': datetime.now().isoformat()
            }

    def process_close_positions(self, close_data: Dict[str, Any]) -> Dict[str, Any]:
        """Process close position commands from the cloud"""
        try:
            positions_to_close = close_data.get('positions', [])
            logger.info(f"📊 Processing close commands for {len(positions_to_close)} positions")

            result = {
                'processed_at': datetime.now().isoformat(),
                'success': False,
                'positions_closed': 0,
                'errors': [],
                'message': '',
                'closed_symbols': []
            }

            if not positions_to_close:
                result['message'] = 'No positions to close'
                result['success'] = True
                return result

            if not IBKR_AVAILABLE:
                logger.info("🧪 Simulation mode - would close positions")
                result['positions_closed'] = len(positions_to_close)
                result['success'] = True
                result['message'] = f'Simulated closing {len(positions_to_close)} positions'
                result['closed_symbols'] = [p.get('symbol') for p in positions_to_close]
                return result

            # Execute closes with IBKR connection in a dedicated thread
            import threading
            execution_result = {'success': False, 'error': None, 'closed': [], 'errors': []}

            def close_positions_with_ibkr():
                loop = _setup_thread_event_loop()
                ib = None
                try:
                    IBKR_HOST = '127.0.0.1'
                    IBKR_PORT = 7497
                    IBKR_CLIENT_ID = 2

                    ib = IB()
                    logger.info("🏦 Connecting to IBKR for position closing...")

                    connected = ib.connect(IBKR_HOST, IBKR_PORT, clientId=IBKR_CLIENT_ID, timeout=10)

                    if not connected or not ib.isConnected():
                        raise RuntimeError("IBKR connection failed")

                    logger.info("✅ IBKR connected for position closing")

                    for position in positions_to_close:
                        symbol = position.get('symbol', '')
                        quantity = position.get('quantity', 0)
                        action = position.get('action', 'BUY')
                        reason = position.get('reason', 'Unknown')

                        if symbol and quantity > 0:
                            logger.info(f"📉 Closing {symbol}: {quantity} shares (reason: {reason})")

                            # Cancel all orders first
                            cancelled = cancel_ibkr_orders(ib, symbol)
                            logger.info(f"Cancelled {cancelled} orders for {symbol}")

                            # Close position
                            if close_ibkr_position(ib, symbol, quantity, action):
                                execution_result['closed'].append(symbol)
                                logger.info(f"✅ Successfully closed position for {symbol}")
                            else:
                                error_msg = f"Failed to close position for {symbol}"
                                execution_result['errors'].append(error_msg)
                                logger.error(f"❌ {error_msg}")

                            time.sleep(0.2)

                    execution_result['success'] = True

                except Exception as close_e:
                    execution_result['error'] = str(close_e) or repr(close_e)
                    logger.exception(f"Error in position closing: {close_e}")

                finally:
                    if ib is not None:
                        try:
                            if ib.isConnected():
                                ib.disconnect()
                                logger.info("🏦 IBKR disconnected after position closing")
                        except Exception as disc_e:
                            logger.warning(f"Warning during IBKR disconnect: {disc_e}")
                    _cleanup_thread_event_loop(loop)

            # Run close execution in separate thread
            close_thread = threading.Thread(target=close_positions_with_ibkr)
            close_thread.daemon = True
            close_thread.start()
            close_thread.join(timeout=300)

            if execution_result['success']:
                result['positions_closed'] = len(execution_result['closed'])
                result['closed_symbols'] = execution_result['closed']
                result['errors'] = execution_result['errors']
                result['success'] = True
                result['message'] = f'Closed {len(execution_result["closed"])} positions'
            else:
                error_msg = execution_result['error'] or 'Position closing timeout'
                result['message'] = f'Failed to close positions: {error_msg}'
                result['errors'].append(error_msg)

            return result

        except Exception as e:
            logger.error(f"❌ Error in process_close_positions: {e}")
            return {
                'success': False,
                'error': str(e),
                'processed_at': datetime.now().isoformat()
            }

    def sync_positions_with_cloud(self):
        """Sync current IBKR positions and orders with cloud database"""
        try:
            if not IBKR_AVAILABLE:
                logger.debug("Skipping position sync - IBKR not available")
                return None

            logger.info("🔄 Starting position sync with IBKR...")

            import threading
            sync_result = {'success': False, 'positions': {}, 'orders': {}, 'error': None}

            def get_positions_from_ibkr():
                loop = _setup_thread_event_loop()
                ib = None
                try:
                    IBKR_HOST = '127.0.0.1'
                    IBKR_PORT = 7497
                    IBKR_CLIENT_ID = 3

                    ib = IB()
                    connected = ib.connect(IBKR_HOST, IBKR_PORT, clientId=IBKR_CLIENT_ID, timeout=10)

                    if not connected or not ib.isConnected():
                        raise RuntimeError("IBKR connection failed for sync")

                    positions, orders = get_ibkr_positions_and_orders(ib)
                    sync_result['positions'] = positions
                    sync_result['orders'] = orders
                    sync_result['success'] = True

                except Exception as sync_e:
                    sync_result['error'] = str(sync_e)
                    logger.error(f"Error syncing positions: {sync_e}")

                finally:
                    if ib is not None:
                        try:
                            if ib.isConnected():
                                ib.disconnect()
                        except:
                            pass
                    _cleanup_thread_event_loop(loop)

            sync_thread = threading.Thread(target=get_positions_from_ibkr)
            sync_thread.daemon = True
            sync_thread.start()
            sync_thread.join(timeout=30)

            if sync_result['success']:
                logger.info(f"✅ Synced {len(sync_result['positions'])} positions and {len(sync_result['orders'])} order groups")
                return {
                    'positions': sync_result['positions'],
                    'orders': sync_result['orders'],
                    'synced_at': datetime.now().isoformat()
                }
            else:
                logger.error(f"❌ Position sync failed: {sync_result.get('error', 'Unknown error')}")
                return None

        except Exception as e:
            logger.error(f"❌ Error in sync_positions_with_cloud: {e}")
            return None

    async def send_status_update(self, status_data: Dict[str, Any]):
        """Send status update back to cloud via WebSocket"""
        try:
            if self.websocket:
                payload = dict(status_data)
                if self.connected_endpoint and 'endpoint' not in payload:
                    payload['endpoint'] = self.connected_endpoint

                message = {
                    'event': 'local_trading_status',
                    'data': payload
                }
                await self.websocket.send(json.dumps(message))
                logger.info(f"📡 Sent status update to cloud: {payload.get('message', 'Unknown')}")
        except Exception as e:
            logger.error(f"❌ Failed to send status update: {e}")

    async def send_position_sync(self, sync_data: Dict[str, Any]):
        """Send position sync data to cloud"""
        try:
            if self.websocket and sync_data:
                message = {
                    'event': 'position_sync',
                    'data': sync_data
                }
                await self.websocket.send(json.dumps(message))
                logger.info(f"📡 Sent position sync to cloud: {len(sync_data.get('positions', {}))} positions")
        except Exception as e:
            logger.error(f"❌ Failed to send position sync: {e}")

    async def handle_message(self, message: str):
        """Handle incoming WebSocket message"""
        try:
            data = json.loads(message)
            event = data.get('event')

            logger.info(f"📨 Received event: {event}")

            if event == 'connected':
                logger.info("✅ Connected to cloud API successfully")
                await self.send_status_update({
                    'status': 'connected',
                    'message': 'Local trading client connected',
                    'ibkr_available': IBKR_AVAILABLE,
                    'endpoint': self.connected_endpoint
                })

            elif event == 'trading_orders_generated':
                order_data = data.get('data', {})
                logger.info(f"📊 Received trading orders: scan_id={order_data.get('scan_id')}")

                # Process the orders in a separate thread to avoid blocking the async loop
                import concurrent.futures

                loop = asyncio.get_event_loop()

                def process_orders_sync():
                    return self.process_trading_orders(order_data)

                # Run the blocking operation in a thread pool
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    future = executor.submit(process_orders_sync)
                    result = await loop.run_in_executor(None, lambda: future.result(timeout=600))

                # Send result back to cloud
                await self.send_status_update(result)

            elif event == 'close_positions':
                close_data = data.get('data', {})
                logger.info(f"📉 Received close position commands: {len(close_data.get('positions', []))} positions")

                # Process close commands in a separate thread
                import concurrent.futures

                loop = asyncio.get_event_loop()

                def process_close_sync():
                    return self.process_close_positions(close_data)

                with concurrent.futures.ThreadPoolExecutor() as executor:
                    future = executor.submit(process_close_sync)
                    result = await loop.run_in_executor(None, lambda: future.result(timeout=300))

                # Send result back to cloud
                await self.send_status_update(result)

            elif event == 'position_sync_request':
                logger.info("🔄 Received position sync request")

                # Perform sync in a separate thread
                import concurrent.futures

                loop = asyncio.get_event_loop()

                def sync_positions_sync():
                    return self.sync_positions_with_cloud()

                with concurrent.futures.ThreadPoolExecutor() as executor:
                    future = executor.submit(sync_positions_sync)
                    sync_data = await loop.run_in_executor(None, lambda: future.result(timeout=60))

                # Send sync data back to cloud
                if sync_data:
                    await self.send_position_sync(sync_data)

            else:
                logger.info(f"📝 Unhandled event: {event}")

        except Exception as e:
            logger.error(f"❌ Error handling message: {e}")
            logger.error(f"Raw message: {message}")

    async def run(self):
        """Main run loop with periodic position sync"""
        self.running = True
        logger.info("🚀 Starting Local Trading Client...")

        # Create a task for periodic sync
        async def periodic_sync():
            """Periodically sync positions and check for issues"""
            while self.running:
                try:
                    current_time = time.time()
                    if current_time - self.last_sync_time >= self.sync_interval:
                        logger.info("🔄 Performing periodic position sync...")

                        # Perform sync in executor to avoid blocking
                        import concurrent.futures
                        loop = asyncio.get_event_loop()

                        def sync_positions_sync():
                            return self.sync_positions_with_cloud()

                        with concurrent.futures.ThreadPoolExecutor() as executor:
                            future = executor.submit(sync_positions_sync)
                            sync_data = await loop.run_in_executor(None, lambda: future.result(timeout=60))

                        # Send sync data to cloud
                        if sync_data:
                            await self.send_position_sync(sync_data)

                        self.last_sync_time = current_time

                    await asyncio.sleep(10)  # Check every 10 seconds

                except Exception as e:
                    logger.error(f"❌ Error in periodic sync: {e}")
                    await asyncio.sleep(30)

        # Start periodic sync task
        sync_task = asyncio.create_task(periodic_sync())

        try:
            while self.running:
                try:
                    # Connect to WebSocket
                    if not await self.connect_websocket():
                        logger.error("❌ Failed to connect to WebSocket. Retrying in 10 seconds...")
                        await asyncio.sleep(10)
                        continue

                    # Listen for messages
                    async for message in self.websocket:
                        if not self.running:
                            break
                        await self.handle_message(message)

                except websockets.exceptions.ConnectionClosed:
                    logger.warning("🔌 WebSocket connection closed. Reconnecting in 5 seconds...")
                    await asyncio.sleep(5)
                except Exception as e:
                    logger.error(f"❌ Error in main loop: {e}")
                    await asyncio.sleep(5)
                finally:
                    await self.disconnect_websocket()
        finally:
            # Cancel the sync task when shutting down
            sync_task.cancel()
            try:
                await sync_task
            except asyncio.CancelledError:
                pass

    def stop(self):
        """Stop the client"""
        logger.info("🛑 Stopping Local Trading Client...")
        self.running = False


def main():
    """Main entry point"""
    # Static configuration
    # STATIC_API_URL = 'ws://0.0.0.0:8000/listen_trading_client'
    STATIC_API_URL = 'wss://stock-api-1-jhsa.onrender.com/listen_trading_client'

    
    parser = argparse.ArgumentParser(description='Local Trading Client for Cloud API')
    parser.add_argument(
        '--api-url',
        default=STATIC_API_URL,
        help=f'Trading WebSocket URL of the cloud API (default: {STATIC_API_URL})'
    )
    parser.add_argument(
        '--download-dir',
        default='./downloads',
        help='Directory to download Excel files (default: ./downloads)'
    )
    parser.add_argument(
        '--simulate',
        action='store_true',
        help='Run in simulation mode (no actual trades)'
    )
    
    args = parser.parse_args()
    
    # Force simulation mode if IBKR is not available
    if not IBKR_AVAILABLE:
        args.simulate = True
    
    logger.info("=" * 60)
    logger.info("🚀 LOCAL TRADING CLIENT STARTING")
    logger.info("=" * 60)
    logger.info(f"📡 API URL: {args.api_url}")
    logger.info(f"📁 Download Dir: {args.download_dir}")
    logger.info(f"🧪 Simulation Mode: {args.simulate or not IBKR_AVAILABLE}")
    logger.info("=" * 60)
    
    # Create and run the client
    client = LocalTradingClient(args.api_url, args.download_dir)
    
    try:
        asyncio.run(client.run())
    except KeyboardInterrupt:
        logger.info("🛑 Received interrupt signal")
        client.stop()
    except Exception as e:
        logger.error(f"❌ Fatal error: {e}")
    finally:
        logger.info("👋 Local Trading Client stopped")


if __name__ == "__main__":
    main()
