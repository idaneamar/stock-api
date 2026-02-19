# Trading System Integration

This document describes the enhanced trading system that enables automated order placement through Interactive Brokers (IBKR) with cloud-to-local communication.

## Architecture Overview

```
[Cloud API] ←→ WebSocket ←→ [Local Trading Client] ←→ [IBKR TWS/Gateway]
```

### Components:

1. **Cloud API** - FastAPI application running on cloud server
2. **Local Trading Client** - Python script running on your local machine
3. **IBKR Connection** - Direct connection to Interactive Brokers TWS/Gateway

## Features

### 1. API Endpoint for Order Generation

**Endpoints:**
- `GET /analysis/orders/preview/{scan_id}` (inspect/adjust order quantities)
- `POST /analysis/orders/place-modified-orders` (generate Excel + send orders to local client)

**Notes:**
- Orders are generated from a single unified analysis list (no "full/limited" modes).

**Functionality:**
- Generates Excel file in trading bot format
- Saves file locally on the server
- Sends order data via WebSocket to local client
- Returns order summary and file information

**Example Usage:**
```bash
# Preview orders for a completed scan
curl "http://your-server:8000/analysis/orders/preview/123"

# Place orders (optionally with modified quantities)
curl -X POST "http://your-server:8000/analysis/orders/place-modified-orders" \
  -H "Content-Type: application/json" \
  -d '{"scan_id":123,"modified_orders":[{"symbol":"AAPL","position_size":150}]}'
```

### 2. WebSocket Communication System

**WebSocket Endpoint:** `ws://your-server:8000/listen_events`

**Events:**
- `trading_orders_generated` - New orders available for placement
- `local_trading_status` - Status updates from local client
- `connected` - Connection established
- `status_acknowledged` - Acknowledgment of status updates

### 3. Local Trading Client

**Script:** `local_trading_client.py`

**Features:**
- Connects to cloud API via WebSocket
- Receives trading orders in real-time
- Places orders through IBKR locally
- Sends status updates back to cloud
- Supports simulation mode for testing

## Setup Instructions

### Prerequisites

1. **Interactive Brokers Account**
   - TWS (Trader Workstation) or IB Gateway installed
   - API access enabled
   - Paper trading account recommended for testing

2. **Python Dependencies**
   ```bash
   pip install websockets pandas requests ib_insync
   ```

### Cloud Server Setup

1. The API endpoints are already integrated into your FastAPI application
2. Ensure WebSocket support is enabled
3. The server will generate Excel files in the project root directory

### Local Client Setup

1. **Copy the trading client to your local machine:**
   ```bash
   # Copy the self-contained trading client
   cp local_trading_client.py /path/to/local/directory/
   ```

2. **Start Interactive Brokers TWS/Gateway locally**
   - Enable API connections (Configure → API → Settings)
   - Set Socket Port to 7497 (paper) or 7496 (live)
   - Enable "Accept incoming connection"

3. **Run the local trading client:**
   ```bash
   # Basic usage (connects to localhost:8000)
   python local_trading_client.py
   
   # Connect to cloud server
   python local_trading_client.py --api-url ws://your-cloud-server:8000/listen_events
   
   # Simulation mode (no actual trades)
   python local_trading_client.py --simulate
   ```

## Usage Workflow

### Step 1: Generate Orders on Cloud

```python
import requests

# Preview orders for a completed scan
preview = requests.get("http://your-server:8000/analysis/orders/preview/123").json()
print(preview)

# Place orders (with optional per-symbol quantity overrides)
place = requests.post(
    "http://your-server:8000/analysis/orders/place-modified-orders",
    json={"scan_id": 123, "modified_orders": [{"symbol": "AAPL", "position_size": 150}]},
).json()
print(place)
```

### Step 2: Local Client Receives Orders

The local client automatically receives the orders via WebSocket and processes them:

1. **Receives WebSocket event:** `trading_orders_generated`
2. **Connects to IBKR:** Establishes connection to TWS/Gateway
3. **Places Orders:** Executes buy/sell orders with stop loss and take profit
4. **Sends Status:** Reports success/failure back to cloud

### Step 3: Monitor Results

Monitor the local client logs and cloud API logs for order execution status.

## Configuration

### Local Trading Client Configuration

**Command Line Options:**
- `--api-url`: WebSocket URL of cloud API
- `--download-dir`: Directory for downloaded files
- `--simulate`: Run in simulation mode

**IBKR Settings** (in `local_trading_client.py`):
```python
IBKR_HOST = '127.0.0.1'
IBKR_PORT = 7497  # Paper trading port (7496 for live)
IBKR_CLIENT_ID = 1
```

### Excel File Format

The generated Excel files contain the following columns:
- `Symbol`: Stock ticker
- `Current price`: Entry price
- `investment in $`: Dollar amount to invest
- `Transaction type`: "Buy" or "Sell"
- `Order Type`: Always "Market"
- `Position`: Number of shares
- `Stop Loss`: Stop loss price
- `Take Profit`: Take profit price
- `Target Date`: Expected exit date
- `Win Rate`: Backtest win rate (optional)
- `ADX`: ADX indicator value (optional)
- `Strategy`: Trading strategy used

## Error Handling

### Common Issues

1. **IBKR Connection Failed**
   - Ensure TWS/Gateway is running
   - Check API settings are enabled
   - Verify port numbers match

2. **WebSocket Connection Failed**
   - Check cloud server is accessible
   - Verify WebSocket endpoint URL
   - Check firewall settings

3. **Order Placement Failed**
   - Check account permissions
   - Verify sufficient buying power
   - Check market hours

### Logs

**Local Client Logs:**
- File: `local_trading_client_YYYYMMDD.log`
- Console output with real-time status

**Cloud API Logs:**
- FastAPI application logs
- WebSocket connection events
- Order generation events

## Security Considerations

1. **Network Security**
   - Use WSS (secure WebSocket) in production
   - Implement authentication for WebSocket connections
   - Firewall configuration for IBKR ports

2. **API Security**
   - Secure the order generation endpoint
   - Implement rate limiting
   - Input validation and sanitization

3. **IBKR Security**
   - Use paper trading account for testing
   - Set position size limits
   - Implement daily loss limits

## Testing

### Simulation Mode

Run the local client in simulation mode to test without placing real orders:

```bash
python local_trading_client.py --simulate
```

This will:
- Connect to the WebSocket
- Receive orders from cloud
- Log simulated trades
- Send status updates back

### Paper Trading

Use IBKR paper trading account (port 7497) for safe testing with simulated money.

## Monitoring and Maintenance

### Health Checks

1. **WebSocket Connection**: Monitor connection status
2. **IBKR Connection**: Check TWS/Gateway availability
3. **Order Execution**: Track success/failure rates
4. **File Operations**: Monitor Excel file generation

### Maintenance Tasks

1. **Log Rotation**: Clean up old log files
2. **File Cleanup**: Remove old Excel files
3. **Connection Monitoring**: Restart clients if disconnected
4. **Performance Monitoring**: Track order execution times

## Support

For issues or questions:
1. Check the logs for error messages
2. Verify all prerequisites are met
3. Test with simulation mode first
4. Ensure IBKR account has proper permissions
