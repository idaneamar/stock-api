import logging
import json
from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from datetime import datetime
from app.services.websocket_manager import websocket_manager, trading_websocket_manager

router = APIRouter(
    tags=["WebSocket"],
)

@router.websocket("/listen_events")
async def listen_events_endpoint(websocket: WebSocket):
    """
    WebSocket endpoint for real-time event notifications.
    
    Clients can connect to this endpoint to receive real-time events:
    
    Completion Events:
    - scan_completed: When a stock scan is completed
    - analysis_completed: When analysis is completed
    
    Progress Events (with progress percentage 0-100):
    - scan_progress: Real-time progress updates during stock scanning
    - analysis_progress: Real-time progress updates during analysis
    
    Event format: {"event": "event_type", "id": "scan_id"}
    Progress format: {"event": "event_type", "id": "scan_id", "progress": percentage}
    """
    await websocket_manager.connect(websocket)
    
    # Send welcome message when client connects
    welcome_message = {
        "event": "connected",
        "message": "Successfully connected to Stock API events",
        "timestamp": datetime.now().isoformat()
    }
    await websocket.send_text(json.dumps(welcome_message))
    
    try:
        while True:
            # Keep connection alive and listen for client messages
            data = await websocket.receive_text()
            logging.info(f"Received WebSocket message: {data}")
            
            try:
                message = json.loads(data)
                event = message.get('event')
                
                # Handle client status updates
                if event == 'local_trading_status':
                    status_data = message.get('data', {})
                    logging.info(f"📊 Received trading status from local client: {status_data.get('message', 'No message')}")
                    
                    # You can broadcast this status to other connected clients or store it
                    # For now, just log it
                    if status_data.get('success'):
                        logging.info(f"✅ Trading operation successful: {status_data.get('orders_placed', 0)} orders placed")
                    else:
                        logging.error(f"❌ Trading operation failed: {status_data.get('message', 'Unknown error')}")
                    
                    # Send acknowledgment back to client
                    ack_message = {
                        "event": "status_acknowledged",
                        "message": "Status update received",
                        "timestamp": datetime.now().isoformat()
                    }
                    await websocket.send_text(json.dumps(ack_message))
                
                elif event == 'ping':
                    # Handle ping/keepalive messages
                    pong_message = {
                        "event": "pong",
                        "timestamp": datetime.now().isoformat()
                    }
                    await websocket.send_text(json.dumps(pong_message))
                
                else:
                    # Echo back for other message types
                    echo_message = {
                        "event": "echo",
                        "original_message": message,
                        "timestamp": datetime.now().isoformat()
                    }
                    await websocket.send_text(json.dumps(echo_message))
                    
            except json.JSONDecodeError:
                # Handle non-JSON messages
                error_message = {
                    "event": "error",
                    "message": "Invalid JSON format",
                    "timestamp": datetime.now().isoformat()
                }
                await websocket.send_text(json.dumps(error_message))
                
    except WebSocketDisconnect:
        websocket_manager.disconnect(websocket)
        logging.info("WebSocket client disconnected")
    except Exception as e:
        logging.error(f"WebSocket error: {e}")
        websocket_manager.disconnect(websocket)


@router.websocket("/listen_trading_client")
async def listen_trading_client_endpoint(websocket: WebSocket):
    """Dedicated WebSocket endpoint for local trading clients."""

    await trading_websocket_manager.connect(websocket)

    welcome_message = {
        "event": "connected",
        "message": "Successfully connected to trading order stream",
        "timestamp": datetime.now().isoformat()
    }
    await websocket.send_text(json.dumps(welcome_message))

    try:
        while True:
            data = await websocket.receive_text()
            logging.info(f"[Trading WS] Received message: {data}")

            try:
                message = json.loads(data)
            except json.JSONDecodeError:
                error_message = {
                    "event": "error",
                    "message": "Invalid JSON format",
                    "timestamp": datetime.now().isoformat()
                }
                await websocket.send_text(json.dumps(error_message))
                continue

            event = message.get("event")

            if event == "ping":
                pong_message = {
                    "event": "pong",
                    "timestamp": datetime.now().isoformat()
                }
                await websocket.send_text(json.dumps(pong_message))
                continue

            if event == "local_trading_status":
                status_data = message.get("data", {})
                logging.info(
                    "📊 Trading status update: %s",
                    status_data.get("message", "No message"),
                )

                if status_data.get("success"):
                    logging.info(
                        "✅ Trading operation successful: %s orders placed",
                        status_data.get("orders_placed", 0),
                    )
                else:
                    logging.error(
                        "❌ Trading operation failed: %s",
                        status_data.get("message", "Unknown error"),
                    )

                ack_message = {
                    "event": "status_acknowledged",
                    "message": "Status update received",
                    "timestamp": datetime.now().isoformat()
                }
                await websocket.send_text(json.dumps(ack_message))
                continue

            if event == "position_sync":
                sync_data = message.get("data", {})
                logging.info(
                    "🔄 Position sync received: %s IBKR positions",
                    len(sync_data.get("positions", {})),
                )

                # Process position sync via the analysis endpoint
                try:
                    from app.routers.analysis import sync_positions_from_client
                    result = await sync_positions_from_client(sync_data)

                    ack_message = {
                        "event": "sync_acknowledged",
                        "message": "Position sync processed successfully",
                        "data": result.data if hasattr(result, 'data') else {},
                        "timestamp": datetime.now().isoformat()
                    }
                    await websocket.send_text(json.dumps(ack_message))
                except Exception as sync_error:
                    logging.error(f"Error processing position sync: {sync_error}")
                    error_message = {
                        "event": "sync_error",
                        "message": f"Failed to process position sync: {str(sync_error)}",
                        "timestamp": datetime.now().isoformat()
                    }
                    await websocket.send_text(json.dumps(error_message))

                continue

            # Default response for unhandled events
            info_message = {
                "event": "received",
                "message": "Event processed",
                "timestamp": datetime.now().isoformat()
            }
            await websocket.send_text(json.dumps(info_message))

    except WebSocketDisconnect:
        trading_websocket_manager.disconnect(websocket)
        logging.info("Trading WebSocket client disconnected")
    except Exception as e:
        logging.error(f"Trading WebSocket error: {e}")
        trading_websocket_manager.disconnect(websocket)
