from sqlalchemy import Column, Integer, Float, String, JSON
from typing import Optional, Any, Dict
from app.models.database import Base

class Settings(Base):
    __tablename__ = "settings"

    id = Column(Integer, primary_key=True, index=True)
    portfolio_size = Column(Float, nullable=False, default=350000.0)

    # "Plans / Programs" support
    active_program_id = Column(String(100), nullable=True)
    active_config = Column(JSON, nullable=False, default=dict)

    @classmethod
    def _get_or_create(cls, db):
        """Fetch the singleton settings row, creating it with defaults if missing."""
        setting = db.query(cls).first()
        if not setting:
            # Create default record if none exists
            setting = cls()  # Will use default value from column definition
            db.add(setting)
            db.commit()
            db.refresh(setting)
        return setting

    @classmethod
    def get_settings(cls, db):
        """Retrieve the current settings row, creating defaults when needed."""
        return cls._get_or_create(db)

    @classmethod
    def get_daily_investment(cls, db):
        """Backward-compatible alias for older code paths (now portfolio_size)."""
        return cls._get_or_create(db).portfolio_size

    @classmethod
    def get_portfolio_size(cls, db):
        """Get portfolio size setting, create default record if none exists."""
        return cls._get_or_create(db).portfolio_size

    @classmethod
    def update_settings(
        cls,
        db,
        *,
        portfolio_size: Optional[float] = None,
        active_program_id: Optional[str] = None,
        active_config: Optional[Dict[str, Any]] = None,
    ):
        """Persist provided settings values, creating defaults if necessary."""
        setting = cls._get_or_create(db)

        if portfolio_size is not None:
            setting.portfolio_size = portfolio_size

        if active_program_id is not None:
            setting.active_program_id = active_program_id

        if active_config is not None:
            setting.active_config = active_config

        db.commit()
        db.refresh(setting)
        return setting

    @classmethod
    def set_active_program(cls, db, program_id: str, config: Dict[str, Any]):
        """Set the globally active program and persist its config snapshot."""
        return cls.update_settings(db, active_program_id=program_id, active_config=config or {})

    @classmethod
    def get_active_program(cls, db):
        s = cls._get_or_create(db)
        return {
            "active_program_id": s.active_program_id,
            "active_config": s.active_config or {},
        }


    @classmethod
    def set_daily_investment(cls, db, value: float):
        """Backward-compatible alias for older code paths (now portfolio_size)."""
        return cls.update_settings(db, portfolio_size=value)

    @classmethod
    def set_portfolio_size(cls, db, value: float):
        """Set portfolio size setting."""
        return cls.update_settings(db, portfolio_size=value)
