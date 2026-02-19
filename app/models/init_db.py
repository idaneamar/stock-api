from sqlalchemy import create_engine
from sqlalchemy import inspect, text
from app.models.database import Base, SessionLocal
from app.models.scan import ScanRecord  # Import to register the model
from app.models.settings import Settings  # Import to register the settings model
from app.models.trade import OpenTrade, ClosedTrade  # Import trade tracking models
from app.models.strategy import Strategy  # Import to register the strategy model
from app.models.program import Program  # Import to register the program model
from app.core.config import settings
import logging

def _ensure_unified_analysis_schema(engine) -> None:
    """
    Ensure the unified analysis columns exist on live databases.

    Alembic is the preferred mechanism, but this keeps the API functional when the DB schema is behind.
    """
    inspector = inspect(engine)
    tables = set()
    try:
        tables = set(inspector.get_table_names())
    except Exception:
        return

    if "scans" in tables:
        scan_cols = {c["name"] for c in inspector.get_columns("scans")}
        statements: list[str] = []

        if "analysis_results" not in scan_cols:
            statements.append("ALTER TABLE scans ADD COLUMN IF NOT EXISTS analysis_results JSON")
        if "analysis_status" not in scan_cols:
            statements.append("ALTER TABLE scans ADD COLUMN IF NOT EXISTS analysis_status VARCHAR(50)")
        if "analysis_progress" not in scan_cols:
            statements.append(
                "ALTER TABLE scans ADD COLUMN IF NOT EXISTS analysis_progress INTEGER NOT NULL DEFAULT 0"
            )
        if "analyzed_at" not in scan_cols:
            statements.append("ALTER TABLE scans ADD COLUMN IF NOT EXISTS analyzed_at TIMESTAMPTZ")
        if "portfolio_size" not in scan_cols:
            statements.append(
                "ALTER TABLE scans ADD COLUMN IF NOT EXISTS portfolio_size NUMERIC(12, 2) NOT NULL DEFAULT 350000.00"
            )
        if "selected_strategies" not in scan_cols:
            statements.append("ALTER TABLE scans ADD COLUMN IF NOT EXISTS selected_strategies JSON")

        if statements:
            with engine.begin() as conn:
                for stmt in statements:
                    conn.execute(text(stmt))
                # Optional: align with Alembic by removing server default if we created it.
                if "analysis_progress" not in scan_cols:
                    try:
                        conn.execute(text("ALTER TABLE scans ALTER COLUMN analysis_progress DROP DEFAULT"))
                    except Exception:
                        pass
                if "portfolio_size" not in scan_cols:
                    try:
                        conn.execute(text("ALTER TABLE scans ALTER COLUMN portfolio_size DROP DEFAULT"))
                    except Exception:
                        pass

    if "settings" in tables:
        settings_cols = {c["name"] for c in inspector.get_columns("settings")}
        with engine.begin() as conn:
            if "portfolio_size" not in settings_cols:
                conn.execute(
                    text(
                        "ALTER TABLE settings "
                        "ADD COLUMN IF NOT EXISTS portfolio_size DOUBLE PRECISION NOT NULL DEFAULT 350000.0"
                    )
                )
                # If an older daily_investment column exists, do not assume it's portfolio size; keep default unless set.
                try:
                    conn.execute(
                        text(
                            "UPDATE settings SET portfolio_size = 350000.0 "
                            "WHERE portfolio_size IS NULL"
                        )
                    )
                except Exception:
                    pass
                try:
                    conn.execute(text("ALTER TABLE settings ALTER COLUMN portfolio_size DROP DEFAULT"))
                except Exception:
                    pass

            # Programs / Plans support
            if "active_program_id" not in settings_cols:
                try:
                    conn.execute(text("ALTER TABLE settings ADD COLUMN IF NOT EXISTS active_program_id VARCHAR(100)"))
                except Exception:
                    pass
            if "active_config" not in settings_cols:
                try:
                    conn.execute(text("ALTER TABLE settings ADD COLUMN IF NOT EXISTS active_config JSON"))
                except Exception:
                    pass

    # Programs table (SQLite may not support IF NOT EXISTS in the same way across versions; create_all handles it.)

def _verify_expected_schema(engine) -> None:
    """Fail fast when the DB schema is behind the code model."""
    inspector = inspect(engine)

    required_scan_columns = {
        "analysis_results",
        "analysis_status",
        "analysis_progress",
        "analyzed_at",
        "portfolio_size",
    }
    required_settings_columns = {
        "portfolio_size",
        "active_program_id",
        "active_config",
    }

    missing: list[str] = []

    try:
        scan_cols = {c["name"] for c in inspector.get_columns("scans")}
        for col in sorted(required_scan_columns - scan_cols):
            missing.append(f"scans.{col}")
    except Exception:
        # If the table doesn't exist yet, create_all() will handle it.
        pass

    try:
        settings_cols = {c["name"] for c in inspector.get_columns("settings")}
        for col in sorted(required_settings_columns - settings_cols):
            missing.append(f"settings.{col}")
    except Exception:
        pass

    if missing:
        message = (
            "Database schema is missing required columns: "
            + ", ".join(missing)
            + ". Run `alembic upgrade head` against the same DATABASE_URL, then restart the API."
        )
        logging.error(message)
        raise RuntimeError(message)

def create_tables():
    """Create all database tables"""
    try:
        engine = create_engine(settings.DATABASE_URL)
        Base.metadata.create_all(bind=engine)
        _ensure_unified_analysis_schema(engine)
        _verify_expected_schema(engine)

        # Seed default strategies (if table is empty)
        try:
            db = SessionLocal()
            try:
                existing = db.query(Strategy).count()
                if existing == 0:
                    defaults = [
                        Strategy(name="Trend", enabled=True),
                        Strategy(name="Momentum", enabled=True),
                        Strategy(name="VWAP", enabled=True),
                        Strategy(name="Breakout", enabled=False),
                        Strategy(name="Mean Reversion", enabled=False),
                    ]
                    db.add_all(defaults)
                    db.commit()
            finally:
                db.close()
        except Exception as seed_strat_error:
            logging.warning(f"Could not seed default strategies: {seed_strat_error}")

        # Seed baseline program (STR CODE 9)
        try:
            db = SessionLocal()
            try:
                baseline = db.query(Program).filter(Program.program_id == "str_code_9").first()
                if baseline is None:
                    baseline = Program(
                        program_id="str_code_9",
                        name="Baseline – STR CODE 9",
                        is_baseline=True,
                        config={
                            "enabled_strategy_names": ["Trend", "Momentum", "VWAP"],
                            "rules": {
                                "strict_rules": True,
                                "adx_min": 30,
                                "volume_spike_required": True,
                            },
                            "market": {"use_vix_filter": True},
                            "risk": {"daily_loss_limit_pct": 0.02},
                        },
                    )
                    db.add(baseline)
                    db.commit()

                # If nothing is active yet, set baseline as active.
                active = Settings.get_settings(db)
                if not getattr(active, "active_program_id", None):
                    Settings.set_active_program(db, "str_code_9", baseline.config or {})
                    db.commit()
            finally:
                db.close()
        except Exception as seed_error:
            logging.warning(f"Could not seed baseline program: {seed_error}")

        logging.info("Database tables created successfully")
    except Exception as e:
        logging.error(f"Error creating database tables: {e}")
        raise

if __name__ == "__main__":
    create_tables()
