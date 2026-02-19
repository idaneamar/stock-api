import logging
import math
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any, Callable, Dict, List, Optional, Set, Type, Union
from sqlalchemy.sql import func

from app.models.database import SessionLocal
from app.models.trade import ClosedTrade, OpenTrade


def _decimal_to_float(value: Optional[Decimal]) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _paginate_query(
    model: Type,
    serializer: Callable[[Any], Dict[str, Any]],
    order_by,
    page: int,
    page_size: int,
) -> Dict[str, Any]:
    db = SessionLocal()
    try:
        total_records = db.query(func.count(model.id)).scalar() or 0
        total_pages = math.ceil(total_records / page_size) if total_records > 0 else 0
        offset = max((page - 1) * page_size, 0)

        query = db.query(model)
        if order_by is not None:
            query = query.order_by(order_by)

        records: List[Any] = query.offset(offset).limit(page_size).all()
        items = [serializer(record) for record in records]

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


class TradeService:
    """Provides paginated access to open and closed trades."""

    def __init__(self) -> None:
        self.logger = logging.getLogger(__name__)

    @staticmethod
    def _serialize_open(trade: OpenTrade) -> Dict[str, Any]:
        return {
            "id": trade.id,
            "symbol": trade.symbol,
            "action": trade.action,
            "quantity": trade.quantity,
            "entry_price": _decimal_to_float(trade.entry_price),
            "entry_date": trade.entry_date.isoformat() if trade.entry_date else None,
            "stop_loss": _decimal_to_float(trade.stop_loss),
            "take_profit": _decimal_to_float(trade.take_profit),
            "target_date": trade.target_date.isoformat() if trade.target_date else None,
            "scan_id": trade.scan_id,
            "analysis_type": trade.analysis_type,
            "strategy": trade.strategy,
            "updated_at": trade.updated_at.isoformat() if trade.updated_at else None,
        }

    @staticmethod
    def _serialize_closed(trade: ClosedTrade) -> Dict[str, Any]:
        return {
            "id": trade.id,
            "symbol": trade.symbol,
            "action": trade.action,
            "quantity": trade.quantity,
            "entry_price": _decimal_to_float(trade.entry_price),
            "exit_price": _decimal_to_float(trade.exit_price),
            "entry_date": trade.entry_date.isoformat() if trade.entry_date else None,
            "exit_date": trade.exit_date.isoformat() if trade.exit_date else None,
            "close_reason": trade.close_reason,
            "strategy": trade.strategy,
            "scan_id": trade.scan_id,
            "analysis_type": trade.analysis_type,
            "stop_loss": _decimal_to_float(trade.stop_loss),
            "take_profit": _decimal_to_float(trade.take_profit),
            "target_date": trade.target_date.isoformat() if trade.target_date else None,
            "created_at": trade.created_at.isoformat() if trade.created_at else None,
        }

    def get_open_trades(self, page: int = 1, page_size: int = 10) -> Dict[str, Any]:
        """Return paginated open trades ordered by most recent updates."""

        return _paginate_query(
            model=OpenTrade,
            serializer=self._serialize_open,
            order_by=OpenTrade.updated_at.desc(),
            page=page,
            page_size=page_size,
        )

    def get_closed_trades(self, page: int = 1, page_size: int = 10) -> Dict[str, Any]:
        """Return paginated closed trades ordered by most recent exit date."""

        return _paginate_query(
            model=ClosedTrade,
            serializer=self._serialize_closed,
            order_by=ClosedTrade.exit_date.desc(),
            page=page,
            page_size=page_size,
        )

    def delete_open_trade(self, trade_id: int) -> bool:
        """Delete an open trade by ID.

        Returns True if the trade existed and was deleted, False otherwise.
        """
        db = SessionLocal()
        try:
            trade = db.query(OpenTrade).filter(OpenTrade.id == trade_id).first()
            if trade is None:
                return False

            db.delete(trade)
            db.commit()
            return True
        except Exception as exc:
            db.rollback()
            self.logger.error(f"Failed to delete open trade {trade_id}: {exc}")
            raise
        finally:
            db.close()

    def update_open_trade(self, trade_id: int, updates: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Update the target date on an open trade and return the updated record."""

        db = SessionLocal()
        try:
            trade = db.query(OpenTrade).filter(OpenTrade.id == trade_id).first()
            if trade is None:
                return None

            sanitized_updates = {}
            if "target_date" in updates:
                sanitized_updates["target_date"] = updates.get("target_date")
            if not sanitized_updates:
                return self._serialize_open(trade)

            for field, value in sanitized_updates.items():
                setattr(trade, field, value)

            db.commit()
            db.refresh(trade)
            return self._serialize_open(trade)
        except Exception as exc:
            db.rollback()
            self.logger.error(f"Failed to update open trade {trade_id}: {exc}")
            raise
        finally:
            db.close()

    def delete_all_open_trades(self) -> int:
        """Delete all open trades and return how many rows were removed."""
        db = SessionLocal()
        try:
            deleted_count = db.query(OpenTrade).delete(synchronize_session=False)
            db.commit()
            return deleted_count
        except Exception as exc:
            db.rollback()
            self.logger.error(f"Failed to delete all open trades: {exc}")
            raise
        finally:
            db.close()

    def delete_all_closed_trades(self) -> int:
        """Delete all closed trades and return how many rows were removed."""
        db = SessionLocal()
        try:
            deleted_count = db.query(ClosedTrade).delete(synchronize_session=False)
            db.commit()
            return deleted_count
        except Exception as exc:
            db.rollback()
            self.logger.error(f"Failed to delete all closed trades: {exc}")
            raise
        finally:
            db.close()

    @staticmethod
    def _coerce_date(value: Optional[Union[date, datetime]]) -> Optional[date]:
        if value is None:
            return None
        if isinstance(value, date) and not isinstance(value, datetime):
            return value
        if isinstance(value, datetime):
            return value.date()
        raise TypeError(f"Unsupported date value type: {type(value)}")

    @staticmethod
    def _format_date(value: Optional[Union[date, datetime]]) -> Optional[str]:
        if value is None:
            return None
        if isinstance(value, datetime):
            return value.date().isoformat()
        if isinstance(value, date):
            return value.isoformat()
        return None

    def _export_open_payload(self, trade: OpenTrade) -> Dict[str, Any]:
        return {
            "quantity": trade.quantity,
            "entry_price": _decimal_to_float(trade.entry_price),
            "entry_date": self._format_date(trade.entry_date),
            "action": trade.action,
            "stop_loss": _decimal_to_float(trade.stop_loss),
            "take_profit": _decimal_to_float(trade.take_profit),
            "target_date": self._format_date(trade.target_date),
            "strategy": trade.strategy,
            "scan_id": trade.scan_id,
            "analysis_type": trade.analysis_type,
        }

    def _export_closed_payload(self, trade: ClosedTrade) -> Dict[str, Any]:
        return {
            "symbol": trade.symbol,
            "entry_price": _decimal_to_float(trade.entry_price),
            "exit_price": _decimal_to_float(trade.exit_price),
            "entry_date": self._format_date(trade.entry_date),
            "exit_date": self._format_date(trade.exit_date),
            "close_reason": trade.close_reason,
            "quantity": trade.quantity,
            "action": trade.action,
            "strategy": trade.strategy,
            "scan_id": trade.scan_id,
            "analysis_type": trade.analysis_type,
            "stop_loss": _decimal_to_float(trade.stop_loss),
            "take_profit": _decimal_to_float(trade.take_profit),
            "target_date": self._format_date(trade.target_date),
        }

    def export_open_trades(self) -> Dict[str, Dict[str, Any]]:
        """Return open trades keyed by symbol, matching the creation payload structure."""

        db = SessionLocal()
        try:
            trades: List[OpenTrade] = db.query(OpenTrade).order_by(OpenTrade.symbol.asc()).all()
            return {trade.symbol: self._export_open_payload(trade) for trade in trades}
        finally:
            db.close()

    def export_closed_trades(self) -> List[Dict[str, Any]]:
        """Return closed trades matching the creation payload structure."""

        db = SessionLocal()
        try:
            trades: List[ClosedTrade] = db.query(ClosedTrade).order_by(ClosedTrade.exit_date.desc()).all()
            return [self._export_closed_payload(trade) for trade in trades]
        finally:
            db.close()

    def create_open_trades(self, trades: List[Dict[str, Any]]) -> Dict[str, List[Any]]:
        """Persist open trades and return created entries alongside skipped symbols."""

        if not trades:
            return {"created": [], "skipped": []}

        db = SessionLocal()
        created_records: List[OpenTrade] = []
        skipped_symbols: List[str] = []

        symbols = [trade["symbol"] for trade in trades if trade.get("symbol")]
        existing_symbols: Set[str] = set()
        if symbols:
            existing_rows = db.query(OpenTrade.symbol).filter(OpenTrade.symbol.in_(symbols)).all()
            existing_symbols = {row[0] for row in existing_rows}

        seen_symbols: Set[str] = set()
        try:
            for trade_data in trades:
                symbol = trade_data["symbol"]
                if symbol in existing_symbols or symbol in seen_symbols:
                    skipped_symbols.append(symbol)
                    continue

                entry_date = self._coerce_datetime(trade_data.get("entry_date")) or datetime.now(timezone.utc)
                target_date = self._coerce_date(trade_data.get("target_date"))

                open_trade = OpenTrade(
                    symbol=symbol,
                    action=trade_data["action"],
                    quantity=trade_data["quantity"],
                    entry_price=trade_data["entry_price"],
                    entry_date=entry_date,
                    stop_loss=trade_data.get("stop_loss"),
                    take_profit=trade_data.get("take_profit"),
                    target_date=target_date,
                    scan_id=trade_data.get("scan_id"),
                    analysis_type=trade_data.get("analysis_type"),
                    strategy=trade_data.get("strategy"),
                )
                db.add(open_trade)
                created_records.append(open_trade)
                seen_symbols.add(symbol)

            db.commit()

            for record in created_records:
                db.refresh(record)

            return {
                "created": [self._serialize_open(record) for record in created_records],
                "skipped": skipped_symbols,
            }
        except Exception as exc:
            db.rollback()
            self.logger.error(f"Failed to create open trades: {exc}")
            raise
        finally:
            db.close()

    @staticmethod
    def _coerce_datetime(value: Optional[Union[date, datetime]]) -> Optional[datetime]:
        if value is None:
            return None
        if isinstance(value, date) and not isinstance(value, datetime):
            value = datetime.combine(value, datetime.min.time())
        if isinstance(value, datetime):
            if value.tzinfo is None:
                return value.replace(tzinfo=timezone.utc)
            return value.astimezone(timezone.utc)
        raise TypeError(f"Unsupported date value type: {type(value)}")

    def create_closed_trades(self, trades: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Persist closed trades and return their serialized representation."""

        if not trades:
            return []

        db = SessionLocal()
        created_records: List[ClosedTrade] = []
        try:
            for trade_data in trades:
                entry_date = self._coerce_datetime(trade_data.get("entry_date"))
                exit_date = self._coerce_datetime(trade_data.get("exit_date")) or datetime.now(timezone.utc)
                closed_trade = ClosedTrade(
                    symbol=trade_data["symbol"],
                    action=trade_data["action"],
                    quantity=trade_data["quantity"],
                    entry_price=trade_data["entry_price"],
                    exit_price=trade_data.get("exit_price"),
                    entry_date=entry_date,
                    exit_date=exit_date,
                    close_reason=trade_data.get("close_reason"),
                    strategy=trade_data.get("strategy"),
                    scan_id=trade_data.get("scan_id"),
                    analysis_type=trade_data.get("analysis_type"),
                    stop_loss=trade_data.get("stop_loss"),
                    take_profit=trade_data.get("take_profit"),
                    target_date=trade_data.get("target_date"),
                )
                db.add(closed_trade)
                created_records.append(closed_trade)

            db.commit()

            for record in created_records:
                db.refresh(record)

            return [self._serialize_closed(record) for record in created_records]
        except Exception as exc:
            db.rollback()
            self.logger.error(f"Failed to create closed trades: {exc}")
            raise
        finally:
            db.close()
