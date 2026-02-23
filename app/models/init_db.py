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

            # Global engine toggles (adx_min column kept for DB backward compat but no longer used)
            _engine_cols = {
                "strict_rules": "BOOLEAN NOT NULL DEFAULT TRUE",
                "volume_spike_required": "BOOLEAN NOT NULL DEFAULT FALSE",
                "use_intraday": "BOOLEAN NOT NULL DEFAULT FALSE",
                "daily_loss_limit_pct": "DOUBLE PRECISION NOT NULL DEFAULT 0.02",
            }
            for col_name, col_def in _engine_cols.items():
                if col_name not in settings_cols:
                    try:
                        conn.execute(text(f"ALTER TABLE settings ADD COLUMN IF NOT EXISTS {col_name} {col_def}"))
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

        # Strategy configs matching str code 9 exactly:
        # - Pre-filters: volume_spike required + ADX > 30
        # - Trend/Momentum: SL=2×ATR, TP=4×ATR
        # - VWAP: SL=1.5×ATR, TP=3×ATR (≥2.0 R:R), dynamic threshold vs VWAP±0.5×ATR
        # - All with MACD confirmation and min R:R = 2.0
        _STR9_PRE_FILTERS = [
            {"indicator": "volume_spike", "operator": "==", "value": 1},
            {"indicator": "adx", "operator": ">", "value": 30},
        ]

        _STRATEGY_CONFIGS = {
            "Trend": {
                "pre_filters": _STR9_PRE_FILTERS,
                "buy_rules": [
                    {"indicator": "ema20", "operator": ">", "compare_to": "ema50"},
                    {"indicator": "rsi", "operator": "<", "value": 50},
                    {"indicator": "macd", "operator": ">", "compare_to": "macd_signal"},
                ],
                "sell_rules": [
                    {"indicator": "ema20", "operator": "<", "compare_to": "ema50"},
                    {"indicator": "rsi", "operator": ">", "value": 50},
                    {"indicator": "macd", "operator": "<", "compare_to": "macd_signal"},
                ],
                "risk": {"stop_loss_atr_mult": 2.0, "take_profit_atr_mult": 4.0, "min_risk_reward": 2.0},
            },
            "Momentum": {
                # Buy-only (no sell_rules). Requires 3-day price surge >7%, volume
                # >1.5× 20-day average, MACD bullish, and RSI <50 – mirroring str code 9.
                "pre_filters": _STR9_PRE_FILTERS,
                "buy_rules": [
                    {"indicator": "price_change_3d", "operator": ">", "value": 7},
                    {"indicator": "volume_ratio_20d", "operator": ">", "value": 1.5},
                    {"indicator": "macd", "operator": ">", "compare_to": "macd_signal"},
                    {"indicator": "rsi", "operator": "<", "value": 50},
                ],
                "sell_rules": [],
                "risk": {"stop_loss_atr_mult": 2.0, "take_profit_atr_mult": 4.0, "min_risk_reward": 2.0},
            },
            "VWAP": {
                # close < vwap - 0.5*atr for buy; close > vwap + 0.5*atr for sell
                "pre_filters": _STR9_PRE_FILTERS,
                "buy_rules": [
                    {"indicator": "close", "operator": "<", "expression": "vwap - 0.5 * atr"},
                    {"indicator": "rsi", "operator": "<", "value": 50},
                    {"indicator": "macd", "operator": ">", "compare_to": "macd_signal"},
                ],
                "sell_rules": [
                    {"indicator": "close", "operator": ">", "expression": "vwap + 0.5 * atr"},
                    {"indicator": "rsi", "operator": ">", "value": 50},
                    {"indicator": "macd", "operator": "<", "compare_to": "macd_signal"},
                ],
                "risk": {"stop_loss_atr_mult": 1.5, "take_profit_atr_mult": 3.0, "min_risk_reward": 2.0},
            },
            "Breakout": {
                "pre_filters": _STR9_PRE_FILTERS,
                "buy_rules": [
                    {"indicator": "close", "operator": ">", "compare_to": "bb_upper"},
                ],
                "sell_rules": [
                    {"indicator": "close", "operator": "<", "compare_to": "bb_lower"},
                ],
                "risk": {"stop_loss_atr_mult": 2.0, "take_profit_atr_mult": 4.0, "min_risk_reward": 2.0},
            },
            "Mean Reversion": {
                "pre_filters": [],
                "buy_rules": [
                    {"indicator": "rsi", "operator": "<", "value": 30},
                    {"indicator": "close", "operator": "<", "compare_to": "bb_lower"},
                ],
                "sell_rules": [
                    {"indicator": "rsi", "operator": ">", "value": 70},
                    {"indicator": "close", "operator": ">", "compare_to": "bb_upper"},
                ],
                "risk": {"stop_loss_atr_mult": 1.0, "take_profit_atr_mult": 2.0, "min_risk_reward": 2.0},
            },
        }

        _STRATEGY_ENABLED = {
            "Trend": True, "Momentum": True, "VWAP": True,
            "Breakout": False, "Mean Reversion": False,
        }

        # Seed default strategies (if table is empty) or fix configs on existing strategies
        try:
            db = SessionLocal()
            try:
                existing_strategies = db.query(Strategy).all()
                if not existing_strategies:
                    defaults = [
                        Strategy(
                            name=name,
                            enabled=_STRATEGY_ENABLED.get(name, False),
                            config=cfg,
                        )
                        for name, cfg in _STRATEGY_CONFIGS.items()
                    ]
                    db.add_all(defaults)
                    db.commit()
                    logging.info("Seeded default strategies with buy/sell rule configs.")
                else:
                    # Always sync every strategy to the canonical str code 9 config so
                    # thresholds, ATR multipliers and sell_rules stay consistent.
                    updated = 0
                    for strat in existing_strategies:
                        new_cfg = _STRATEGY_CONFIGS.get(strat.name)
                        if new_cfg and strat.config != new_cfg:
                            strat.config = new_cfg
                            updated += 1
                    if updated:
                        db.commit()
                        logging.info(f"Updated {updated} strategies to match str code 9 canonical configs.")
            finally:
                db.close()
        except Exception as seed_strat_error:
            logging.warning(f"Could not seed default strategies: {seed_strat_error}")

        # Baseline program config matching str code 9:
        # volume_spike + adx>30 are enforced inside each strategy's pre_filters,
        # so the global rules do NOT repeat them (avoids double-filtering).
        _BASELINE_CONFIG = {
            "enabled_strategy_names": ["Trend", "Momentum", "VWAP"],
            "rules": {
                "strict_rules": True,
                "volume_spike_required": False,
            },
            "market": {"use_vix_filter": True},
            "risk": {
                "daily_loss_limit_pct": 0.02,
                "min_risk_reward": 2.0,
            },
        }

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
                        config=_BASELINE_CONFIG,
                    )
                    db.add(baseline)
                    db.commit()
                else:
                    # Migrate existing baseline: remove legacy adx_min global rule (ADX now lives
                    # in each strategy's pre_filters) and ensure min_risk_reward=2.0.
                    existing_rules = (baseline.config or {}).get("rules", {})
                    existing_risk = (baseline.config or {}).get("risk", {})
                    needs_update = (
                        "adx_min" in existing_rules
                        or existing_rules.get("volume_spike_required") is True
                        or existing_risk.get("min_risk_reward", 0) != 2.0
                    )
                    if needs_update:
                        updated_config = dict(baseline.config or {})
                        updated_config["rules"] = {
                            "strict_rules": existing_rules.get("strict_rules", True),
                            "volume_spike_required": False,
                        }
                        updated_config["risk"] = {
                            "daily_loss_limit_pct": existing_risk.get("daily_loss_limit_pct", 0.02),
                            "min_risk_reward": 2.0,
                        }
                        baseline.config = updated_config
                        db.commit()
                        active = Settings.get_settings(db)
                        if getattr(active, "active_program_id", None) == "str_code_9":
                            Settings.set_active_program(db, "str_code_9", updated_config)
                            db.commit()
                        logging.info("Migrated STR_CODE_9 baseline: removed global adx_min, volume_spike_required=False, min_risk_reward=2.0.")

                # If nothing is active yet, set baseline as active.
                active = Settings.get_settings(db)
                if not getattr(active, "active_program_id", None):
                    Settings.set_active_program(db, "str_code_9", baseline.config or {})
                    db.commit()
            finally:
                db.close()
        except Exception as seed_error:
            logging.warning(f"Could not seed baseline program: {seed_error}")

        # STR CODE 18 – same strategies as str9 plus per-symbol backtest-based
        # strategy filtering (min 10 trades, expectancy > 0, profit_factor > 1.2).
        # Sentiment analysis (DistilBERT) is noted in the config but intentionally
        # not run inside the API service to avoid heavy ML dependencies at runtime.
        _STR18_CONFIG = {
            "enabled_strategy_names": ["Trend", "Momentum", "VWAP"],
            "rules": {
                "strict_rules": True,
                "volume_spike_required": False,
                "backtest_filter": {
                    "min_trades": 10,
                    "min_expectancy": 0.0,
                    "min_profit_factor": 1.2,
                },
            },
            "market": {"use_vix_filter": True},
            "risk": {
                "daily_loss_limit_pct": 0.02,
                "min_risk_reward": 2.0,
            },
        }

        try:
            db = SessionLocal()
            try:
                str18 = db.query(Program).filter(Program.program_id == "str_code_18").first()
                if str18 is None:
                    str18 = Program(
                        program_id="str_code_18",
                        name="STR CODE 18",
                        is_baseline=False,
                        config=_STR18_CONFIG,
                    )
                    db.add(str18)
                    db.commit()
                    logging.info("Seeded STR CODE 18 program.")
                else:
                    # Sync config to keep thresholds current.
                    existing_bt_filter = (str18.config or {}).get("rules", {}).get("backtest_filter")
                    if existing_bt_filter != _STR18_CONFIG["rules"]["backtest_filter"]:
                        updated = dict(str18.config or {})
                        updated["rules"] = _STR18_CONFIG["rules"]
                        str18.config = updated
                        db.commit()
                        logging.info("Updated STR CODE 18 backtest_filter config.")
            finally:
                db.close()
        except Exception as e:
            logging.warning(f"Could not seed STR CODE 18 program: {e}")

        logging.info("Database tables created successfully")
    except Exception as e:
        logging.error(f"Error creating database tables: {e}")
        raise

if __name__ == "__main__":
    create_tables()
