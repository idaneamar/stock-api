import json
import logging
from typing import List, Dict, Any
from fastapi import WebSocket, WebSocketDisconnect
from contextlib import asynccontextmanager

logger = logging.getLogger(__name__)

class WebSocketManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        """Accept a new WebSocket connection and add it to active connections"""
        await websocket.accept()
        self.active_connections.append(websocket)
        logger.info(f"WebSocket connected. Total active connections: {len(self.active_connections)}")

    def disconnect(self, websocket: WebSocket):
        """Remove a WebSocket connection from active connections"""
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
        logger.info(f"WebSocket disconnected. Total active connections: {len(self.active_connections)}")

    def active_connection_count(self) -> int:
        """Number of currently connected clients."""
        return len(self.active_connections)

    def has_active_connections(self) -> bool:
        """Return True when at least one client is connected."""
        return self.active_connection_count() > 0

    async def send_event(self, event_type: str, scan_id: str, progress: int = None):
        """Send an event to all connected WebSocket clients"""
        message = {
            "event": event_type,
            "id": str(scan_id)
        }
        
        if progress is not None:
            message["progress"] = progress
        
        message_json = json.dumps(message)
        logger.info(f"Broadcasting event: {message_json} to {len(self.active_connections)} clients")
        
        # Send to all active connections
        disconnected_connections = []
        
        for connection in self.active_connections:
            try:
                await connection.send_text(message_json)
            except Exception as e:
                logger.warning(f"Failed to send message to WebSocket: {e}")
                disconnected_connections.append(connection)
        
        # Remove disconnected connections
        for connection in disconnected_connections:
            self.disconnect(connection)

    async def broadcast_scan_completed(self, scan_id: int):
        """Broadcast scan completed event"""
        await self.send_event("scan_completed", scan_id)

    async def broadcast_analysis_completed(self, scan_id: int):
        """Broadcast analysis completed event."""
        await self.send_event("analysis_completed", scan_id)

    async def send_progress_event(self, scan_id: int, progress: int):
        """Send scan progress update event"""
        await self.send_event("scan_progress", scan_id, progress)

    async def send_analysis_progress_event(self, scan_id: int, progress: int):
        """Send analysis progress update event."""
        await self.send_event("analysis_progress", scan_id, progress)

    async def send_trading_orders_event(self, order_data: Dict[str, Any]) -> int:
        """Send trading orders generated event to local trading clients.

        Returns the number of clients that successfully received the payload.
        """
        message = {
            "event": "trading_orders_generated",
            "data": order_data
        }

        message_json = json.dumps(message)
        logger.info(f"Broadcasting trading orders event: scan_id={order_data.get('scan_id')} to {len(self.active_connections)} clients")

        # Send to all active connections
        disconnected_connections = []
        recipients = 0

        for connection in self.active_connections:
            try:
                await connection.send_text(message_json)
                recipients += 1
            except Exception as e:
                logger.warning(f"Failed to send trading orders to WebSocket: {e}")
                disconnected_connections.append(connection)

        # Remove disconnected connections
        for connection in disconnected_connections:
            self.disconnect(connection)

        return recipients

    async def send_close_positions_event(self, close_data: Dict[str, Any]) -> int:
        """Send close positions command event to local trading clients.

        Returns the number of clients that successfully received the payload.
        """
        message = {
            "event": "close_positions",
            "data": close_data
        }

        message_json = json.dumps(message)
        logger.info(f"Broadcasting close positions event: {len(close_data.get('positions', []))} positions to {len(self.active_connections)} clients")

        # Send to all active connections
        disconnected_connections = []
        recipients = 0

        for connection in self.active_connections:
            try:
                await connection.send_text(message_json)
                recipients += 1
            except Exception as e:
                logger.warning(f"Failed to send close positions command to WebSocket: {e}")
                disconnected_connections.append(connection)

        # Remove disconnected connections
        for connection in disconnected_connections:
            self.disconnect(connection)

        return recipients

    async def send_position_sync_request(self) -> int:
        """Request position sync from local trading clients.

        Returns the number of clients that successfully received the request.
        """
        message = {
            "event": "position_sync_request",
            "data": {}
        }

        message_json = json.dumps(message)
        logger.info(f"Broadcasting position sync request to {len(self.active_connections)} clients")

        # Send to all active connections
        disconnected_connections = []
        recipients = 0

        for connection in self.active_connections:
            try:
                await connection.send_text(message_json)
                recipients += 1
            except Exception as e:
                logger.warning(f"Failed to send position sync request to WebSocket: {e}")
                disconnected_connections.append(connection)

        # Remove disconnected connections
        for connection in disconnected_connections:
            self.disconnect(connection)

        return recipients

# Global WebSocket manager instances
# `websocket_manager` handles general event subscribers (dashboards, monitors, etc.)
# `trading_websocket_manager` is dedicated to local trading clients that should receive
# order placement payloads.
websocket_manager = WebSocketManager()
trading_websocket_manager = WebSocketManager()
