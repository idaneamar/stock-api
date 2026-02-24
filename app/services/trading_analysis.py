import os
import time
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
import random
from ta.trend import EMAIndicator, MACD, ADXIndicator
from ta.momentum import RSIIndicator
from ta.volatility import BollingerBands, AverageTrueRange
from threading import Thread, Event
from concurrent.futures import ThreadPoolExecutor
import backoff
import logging
from tqdm import tqdm
import json
from typing import List, Dict, Optional
from sqlalchemy.orm import Session
from sqlalchemy.sql import func
from app.models.database import SessionLocal
from app.models.scan import ScanRecord
from app.models.settings import Settings
from app.models.strategy import Strategy
from app.models.program import Program
import ast
import operator as op_operator

API_KEY = os.environ.get("EODHD_API_TOKEN", "699ae161e60555.92486250")

holidays_cache = {}

def fetch_holidays(year, country_code="US"):
    """Fetch holidays for a given year"""
    if year in holidays_cache:
        return holidays_cache[year]
    url = f"https://date.nager.at/api/v3/PublicHolidays/{year}/{country_code}"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        holidays = response.json()
        holiday_dates = [holiday["date"] for holiday in holidays]
        holidays_cache[year] = holiday_dates
        return holiday_dates
    except Exception as e:
        print(f"Error fetching holidays from API for year {year}: {e}")
        fallback_holidays = [f"{year}-01-01", f"{year}-07-04", f"{year}-12-25"]
        holidays_cache[year] = fallback_holidays
        return fallback_holidays

class TradingAnalysisService:
    """Trading Analysis Service adapted for FastAPI integration"""
    
    def __init__(self):
        self.running = Event()
        self.last_results = []
        self.backtest_results = {}
        self.daily_loss_limit = 0.02
        self.capital = 100000
        self.daily_loss = 0
        self.last_reset_date = datetime.now().date()
        self.risk_per_trade = 0.015  # 1.5%
        self.min_risk_reward_ratio = 2.0
        self.active_trades = {}
        self.logger = logging.getLogger(__name__)
        self._market_conditions_cache: Dict[str, tuple[bool, str, str]] = {}

    def _update_analysis_progress(self, scan_id: int, progress: int):
        """Update analysis progress in database and send WebSocket event."""
        db = SessionLocal()
        try:
            scan_record = db.query(ScanRecord).filter(ScanRecord.id == scan_id).first()
            if scan_record:
                scan_record.analysis_progress = progress
                db.commit()
                
                # Send WebSocket event for progress update
                try:
                    import asyncio
                    from app.services.websocket_manager import websocket_manager
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    loop.run_until_complete(websocket_manager.send_event("analysis_progress", scan_id, progress))
                    loop.close()
                except Exception as ws_error:
                    self.logger.warning(f"Failed to send analysis progress WebSocket event: {ws_error}")
        except Exception as e:
            self.logger.error(f"Error updating analysis progress: {e}")
        finally:
            db.close()

    _OPS = {
        ">": op_operator.gt,
        "<": op_operator.lt,
        ">=": op_operator.ge,
        "<=": op_operator.le,
        "==": op_operator.eq,
        "!=": op_operator.ne,
    }

    @staticmethod
    def _safe_eval_expression(expr: str, values: Dict[str, float]) -> Optional[float]:
        """Safely evaluate a basic arithmetic expression using indicator values."""
        try:
            tree = ast.parse(expr, mode="eval")
        except Exception:
            return None

        def _eval(node):
            if isinstance(node, ast.Expression):
                return _eval(node.body)
            if isinstance(node, ast.Constant) and isinstance(node.value, (int, float)):
                return float(node.value)
            if isinstance(node, ast.Name):
                if node.id not in values:
                    raise KeyError(node.id)
                return float(values[node.id])
            if isinstance(node, ast.UnaryOp) and isinstance(node.op, (ast.UAdd, ast.USub)):
                val = _eval(node.operand)
                return val if isinstance(node.op, ast.UAdd) else -val
            if isinstance(node, ast.BinOp) and isinstance(node.op, (ast.Add, ast.Sub, ast.Mult, ast.Div)):
                left = _eval(node.left)
                right = _eval(node.right)
                if isinstance(node.op, ast.Add):
                    return left + right
                if isinstance(node.op, ast.Sub):
                    return left - right
                if isinstance(node.op, ast.Mult):
                    return left * right
                if isinstance(node.op, ast.Div):
                    return left / right
            raise ValueError("unsupported_expression")

        try:
            return float(_eval(tree))
        except Exception:
            return None

    def _rules_pass(self, rules: List[Dict], values: Dict[str, float]) -> bool:
        for rule in rules or []:
            try:
                indicator = rule.get("indicator")
                op_symbol = rule.get("operator")
                if not indicator or not op_symbol or op_symbol not in self._OPS:
                    return False

                left = values.get(indicator)
                if left is None:
                    return False

                if rule.get("value") is not None:
                    right = float(rule.get("value"))
                elif rule.get("compare_to"):
                    right = values.get(rule.get("compare_to"))
                    if right is None:
                        return False
                elif rule.get("expression"):
                    right = self._safe_eval_expression(str(rule.get("expression")), values)
                    if right is None:
                        return False
                else:
                    return False

                if not self._OPS[op_symbol](float(left), float(right)):
                    return False
            except Exception:
                return False
        return True

    @staticmethod
    def _extract_latest_values(last_row: pd.Series) -> Dict[str, float]:
        keys = [
            "close",
            "open",
            "high",
            "low",
            "volume",
            "ema20",
            "ema50",
            "rsi",
            "vwap",
            "bb_upper",
            "bb_lower",
            "atr",
            "adx",
            "macd",
            "macd_signal",
            "price_change_3d",
            "volume_ratio_20d",
        ]
        out: Dict[str, float] = {}
        for key in keys:
            try:
                out[key] = float(last_row.get(key))
            except Exception:
                continue
        try:
            out["volume_spike"] = 1.0 if bool(last_row.get("volume_spike", False)) else 0.0
        except Exception:
            out["volume_spike"] = 0.0
        return out

    def _evaluate_strategy(self, strategy: Strategy, df: pd.DataFrame, last_row: pd.Series) -> List[Dict]:
        """Evaluate a DB-backed strategy against the latest candle, returning 0..2 signals."""
        cfg = strategy.config or {}
        if not isinstance(cfg, dict):
            return []

        # STR9 invariant: Momentum strategy is BUY-only (never short).
        # Even if a DB config accidentally defines sell_rules, we hard-disable them.
        try:
            strat_name_norm = str(getattr(strategy, "name", "") or "").strip().lower()
        except Exception:
            strat_name_norm = ""

        risk_cfg = cfg.get("risk") if isinstance(cfg.get("risk"), dict) else {}
        stop_mult = float((risk_cfg or {}).get("stop_loss_atr_mult", 1.5) or 1.5)
        take_mult = float((risk_cfg or {}).get("take_profit_atr_mult", 3.0) or 3.0)
        take_mode = str((risk_cfg or {}).get("take_profit_mode", "atr_mult") or "atr_mult").strip().lower()
        take_vwap_mult_buy = float((risk_cfg or {}).get("take_profit_vwap_mult_buy", 1.04) or 1.04)
        take_vwap_mult_sell = float((risk_cfg or {}).get("take_profit_vwap_mult_sell", 0.96) or 0.96)
        min_rr = float((risk_cfg or {}).get("min_risk_reward", self.min_risk_reward_ratio) or self.min_risk_reward_ratio)

        values = self._extract_latest_values(last_row)
        entry = float(values.get("close", 0.0) or 0.0)
        atr = float(values.get("atr", 0.0) or 0.0)
        if entry <= 0 or atr <= 0:
            return []

        pre_filters = cfg.get("pre_filters")
        if isinstance(pre_filters, list) and not self._rules_pass(pre_filters, values):
            return []

        buy_rules = cfg.get("buy_rules")
        sell_rules = cfg.get("sell_rules")
        if buy_rules is None and cfg.get("rules") is not None:
            buy_rules = cfg.get("rules")
        if strat_name_norm == "momentum":
            sell_rules = []

        candidates: List[Dict] = []

        if buy_rules and isinstance(buy_rules, list) and self._rules_pass(buy_rules, values):
            stop_loss = entry - stop_mult * atr
            if take_mode == "vwap_mult":
                vwap = float(values.get("vwap", 0.0) or 0.0)
                if vwap <= 0:
                    return []
                take_profit = vwap * take_vwap_mult_buy
            else:
                take_profit = entry + take_mult * atr
            risk = abs(entry - stop_loss)
            reward = abs(take_profit - entry)
            rr = (reward / risk) if risk > 0 else 0.0
            if rr >= min_rr:
                candidates.append(
                    {
                        "signal": "Buy",
                        "stop_loss": round(stop_loss, 2),
                        "take_profit": round(take_profit, 2),
                        "strategy_id": strategy.id,
                        "strategy_name": strategy.name,
                        "risk_reward_ratio": rr,
                    }
                )

        if sell_rules and isinstance(sell_rules, list) and self._rules_pass(sell_rules, values):
            stop_loss = entry + stop_mult * atr
            if take_mode == "vwap_mult":
                vwap = float(values.get("vwap", 0.0) or 0.0)
                if vwap <= 0:
                    return []
                take_profit = vwap * take_vwap_mult_sell
            else:
                take_profit = entry - take_mult * atr
            risk = abs(stop_loss - entry)
            reward = abs(entry - take_profit)
            rr = (reward / risk) if risk > 0 else 0.0
            if rr >= min_rr:
                candidates.append(
                    {
                        "signal": "Sell",
                        "stop_loss": round(stop_loss, 2),
                        "take_profit": round(take_profit, 2),
                        "strategy_id": strategy.id,
                        "strategy_name": strategy.name,
                        "risk_reward_ratio": rr,
                    }
                )

        return candidates

    @backoff.on_exception(backoff.expo, requests.exceptions.RequestException, max_tries=3)
    def fetch_daily(self, symbol, period="90d", end_date=None):
        """Fetch daily stock data"""
        if end_date is None:
            end_date = datetime.now().date()
        else:
            end_date = datetime.strptime(end_date, "%Y-%m-%d").date() if isinstance(end_date, str) else end_date
        start_date = (end_date - timedelta(days=90)).strftime("%Y-%m-%d")
        end_date_str = end_date.strftime("%Y-%m-%d")
        url = f"https://eodhd.com/api/eod/{symbol}?api_token={API_KEY}&from={start_date}&to={end_date_str}&fmt=json"
        try:
            res = requests.get(url, timeout=10)
            if res.status_code == 429:
                self.logger.error("EODHD rate limit reached")
                return None
            res.raise_for_status()
            data = res.json()
            self.logger.info(f"Fetched {len(data)} rows for {symbol} from {start_date} to {end_date_str}")
            if not data or 'date' not in data[0] or 'close' not in data[0]:
                self.logger.warning(f"Missing required columns for {symbol}")
                return None
            df = pd.DataFrame(data)
            df['date'] = pd.to_datetime(df['date']).dt.date
            df = df[df['date'] <= end_date]
            df.set_index('date', inplace=True)
            return df
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to fetch {symbol}: {e}")
            return None

    def fetch_multiple_daily(self, symbols, period="90d", end_date=None):
        """Fetch daily data for multiple symbols"""
        with ThreadPoolExecutor(max_workers=4) as executor:
            results = list(executor.map(lambda s: self.fetch_daily(s, period, end_date), symbols))
        return {sym: res for sym, res in zip(symbols, results) if res is not None}

    def fetch_realtime_price(self, symbol: str) -> Optional[float]:
        """Best-effort real-time last price (used for intraday mode).

        Notes:
        - EODHD real-time endpoint conventions vary by symbol (often requires exchange suffix).
        - We try the symbol as-is, and if it has no '.', we also try '.US'.
        """
        candidates = [symbol]
        if "." not in symbol:
            candidates.append(f"{symbol}.US")

        for sym in candidates:
            url = f"https://eodhd.com/api/real-time/{sym}?api_token={API_KEY}&fmt=json"
            try:
                res = requests.get(url, timeout=8)
                if res.status_code != 200:
                    continue
                payload = res.json() or {}
                # Common keys: "close" or "price" or "last"
                for k in ("close", "price", "last"):
                    if k in payload and payload[k] is not None:
                        return float(payload[k])
            except Exception:
                continue
        return None

    def check_market_conditions(self, end_date=None, ignore_vix: bool = False):
        """Check market conditions using VIX and SPY trend (matching str code 9).

        Returns:
            (ok, note, market_trend) where ok gates analysis, note is for logging/UI,
            and market_trend is 'bull', 'bear', or 'neutral' (used to filter signal direction).
        """
        cache_date = end_date.strftime("%Y-%m-%d") if hasattr(end_date, "strftime") else (str(end_date) if end_date else "latest")
        cache_key = f"{cache_date}:ignore_vix={bool(ignore_vix)}"
        if cache_key in self._market_conditions_cache:
            return self._market_conditions_cache[cache_key]

        if ignore_vix:
            out = (True, "VIX check disabled", "neutral")
            self._market_conditions_cache[cache_key] = out
            return out

        vix_symbol = "VIX.INDX"
        vix_data = self.fetch_daily(vix_symbol, period="5d", end_date=end_date)
        if vix_data is None:
            self.logger.warning("Failed to fetch VIX data. Proceeding with analysis (fail-open).")
            out = (True, "VIX data unavailable – proceeding without VIX filter", "neutral")
            self._market_conditions_cache[cache_key] = out
            return out

        try:
            latest_vix = float(vix_data["close"].iloc[-1])
        except Exception as e:
            self.logger.error(f"Error accessing VIX data: {e}")
            out = (True, "Error reading VIX data – proceeding without VIX filter", "neutral")
            self._market_conditions_cache[cache_key] = out
            return out

        self.logger.info(f"VIX value: {latest_vix:.2f}")
        if latest_vix > 30:
            out = (False, f"High market volatility (VIX = {latest_vix:.2f})", None)
            self._market_conditions_cache[cache_key] = out
            return out

        # Derive broad SPY trend via EMA200 (mirrors str code 9 logic)
        market_trend = "neutral"
        try:
            spy_data = self.fetch_daily("SPY.US", period="250d", end_date=end_date)
            if spy_data is not None and len(spy_data) >= 200 and "close" in spy_data.columns:
                spy_close = spy_data["close"].astype(float)
                ema200 = EMAIndicator(spy_close, window=200).ema_indicator()
                latest_close = float(spy_close.iloc[-1])
                latest_ema200 = float(ema200.iloc[-1])
                if latest_close > latest_ema200:
                    market_trend = "bull"
                elif latest_close < latest_ema200:
                    market_trend = "bear"
            else:
                self.logger.warning("Failed to fetch SPY data. Assuming neutral market.")
        except Exception as e:
            self.logger.warning(f"Failed to derive SPY trend: {e}")

        out = (True, f"Normal market conditions (VIX = {latest_vix:.2f}, trend={market_trend})", market_trend)
        self._market_conditions_cache[cache_key] = out
        return out

    def is_market_day(self, date):
        """Check if given date is a market day"""
        if date.weekday() >= 5:
            return False
        year = date.year
        holidays = fetch_holidays(year)
        date_str = date.strftime("%Y-%m-%d")
        if date_str in holidays:
            return False
        return True

    def get_next_market_day(self, date):
        """Get next market day from given date"""
        next_day = date
        while not self.is_market_day(next_day):
            next_day += timedelta(days=1)
        return next_day

    def adjust_time_to_market_days(self, target_date, current_date):
        """Adjust time estimation to market days"""
        market_days = min(max((target_date - current_date).days, 1), 25)
        adjusted_date = current_date
        for _ in range(market_days):
            adjusted_date = self.get_next_market_day(adjusted_date + timedelta(days=1))
        return adjusted_date, None

    def estimate_time_to_target(self, df, current_price, target_price, direction="up"):
        """Estimate time to reach target price"""
        try:
            df = df.copy()
            df['price_change'] = df['close'].diff()
            price_changes = df['price_change'].dropna()
            if len(price_changes) < 10:
                return None, "Insufficient data"
            if direction == "up":
                relevant_changes = price_changes[price_changes > 0]
            else:
                relevant_changes = price_changes[price_changes < 0]
            if len(relevant_changes) < 5:
                return None, "Not enough relevant price movements"
            avg_change = relevant_changes.mean()
            std_change = relevant_changes.std()
            price_diff = abs(target_price - current_price)
            if avg_change == 0:
                return None, "Average price change is zero"
            days_needed = price_diff / abs(avg_change)
            variability = random.uniform(-std_change, std_change) / abs(avg_change) if std_change != 0 else 0
            days_needed = days_needed * (1 + variability * 0.5)
            days_needed = min(max(int(days_needed), 1), 20)
            return days_needed, None
        except Exception as e:
            self.logger.error(f"Error in estimate_time_to_target: {e}")
            return None, f"Error: {e}"

    def precompute_indicators(self, symbol, df):
        """Precompute technical indicators"""
        try:
            df = df.copy()
            df['ema20'] = EMAIndicator(df['close'], window=20).ema_indicator()
            df['ema50'] = EMAIndicator(df['close'], window=50).ema_indicator()
            df['rsi'] = RSIIndicator(df['close'], window=14).rsi()
            df['vwap'] = (df['close'] * df['volume']).cumsum() / df['volume'].cumsum()
            df['volume_spike'] = df['volume'] > (df['volume'].rolling(window=20).mean() * 2)
            bb = BollingerBands(df['close'], window=20, window_dev=2)
            df['bb_upper'] = bb.bollinger_hband()
            df['bb_lower'] = bb.bollinger_lband()
            macd = MACD(df['close'], window_slow=26, window_fast=12, window_sign=9)
            df['macd'] = macd.macd()
            df['macd_signal'] = macd.macd_signal()
            atr = AverageTrueRange(df['high'], df['low'], df['close'], window=14).average_true_range()
            df['atr'] = atr.where(atr > 0, (df['close'].pct_change().fillna(0).std() * df['close'] * 14))
            df['atr'] = df['atr'].where(df['atr'] > 0, 1.0)
            df['adx'] = ADXIndicator(df['high'], df['low'], df['close'], window=14).adx()
            # Derived indicators used by momentum strategy (str code 9)
            df['price_change_3d'] = df['close'].pct_change(3) * 100
            df['volume_ratio_20d'] = df['volume'] / df['volume'].rolling(window=20).mean()
            return df
        except Exception as e:
            self.logger.error(f"Error in precompute_indicators for {symbol}: {e}")
            return df

    def trend_strategy(self, df, last):
        """Trend-based trading with EMA, RSI, ADX, and MACD confirmation, matching str code 9."""
        try:
            if not last['volume_spike']:
                return "Hold", None, None, None
            if last['adx'] <= 30:
                return "Hold", None, None, None
            if (last['ema20'] > last['ema50']
                    and last['rsi'] < 50
                    and last['macd'] > last['macd_signal']):
                signal = "Buy"
                stop_loss = round(last['close'] - 2 * last['atr'], 2)
                take_profit = round(last['close'] + 4 * last['atr'], 2)
                return signal, stop_loss, take_profit, "Trend"
            elif (last['ema20'] < last['ema50']
                  and last['rsi'] > 50
                  and last['macd'] < last['macd_signal']):
                signal = "Sell"
                stop_loss = round(last['close'] + 2 * last['atr'], 2)
                take_profit = round(last['close'] - 4 * last['atr'], 2)
                return signal, stop_loss, take_profit, "Trend"
            return "Hold", None, None, None
        except Exception as e:
            self.logger.error(f"Error in trend_strategy: {e}")
            return "Hold", None, None, None

    def momentum_strategy(self, df, last):
        """Momentum strategy: Buy-only (no short selling), matching str code 9."""
        try:
            if not last['volume_spike']:
                return "Hold", None, None, None
            if last['adx'] <= 30:
                return "Hold", None, None, None
            if len(df) < 4:
                return "Hold", None, None, None
            price_3ago = df['close'].shift(3).iloc[-1]
            if pd.isna(price_3ago) or price_3ago == 0:
                return "Hold", None, None, None
            try:
                price_change = (float(last['close']) - float(price_3ago)) / float(price_3ago) * 100
            except ZeroDivisionError:
                return "Hold", None, None, None
            volume_mean_20 = df['volume'].rolling(window=20).mean().iloc[-1]
            if pd.isna(volume_mean_20):
                return "Hold", None, None, None
            volume_condition = float(last['volume']) > float(volume_mean_20) * 1.5
            if price_change > 7 and volume_condition and last['macd'] > last['macd_signal']:
                if last['rsi'] >= 50:
                    return "Hold", None, None, None
                signal = "Buy"
                stop_loss = round(float(last['close']) - 2 * float(last['atr']), 2)
                take_profit = round(float(last['close']) + 4 * float(last['atr']), 2)
                return signal, stop_loss, take_profit, "Momentum"
            return "Hold", None, None, None
        except Exception as e:
            self.logger.error(f"Error in momentum_strategy: {e}")
            return "Hold", None, None, None

    def breakout_strategy(self, df, last):
        """Breakout trading strategy"""
        try:
            if len(df) < 21:
                return "Hold", None, None, None
            high_20 = df['high'].rolling(window=20).max()
            low_20 = df['low'].rolling(window=20).min()
            volume_mean_20 = df['volume'].rolling(window=20).mean().iloc[-1]
            if pd.isna(volume_mean_20):
                return "Hold", None, None, None
            volume_condition = float(last['volume']) > float(volume_mean_20) * 1.5
            if float(last['close']) > float(high_20.iloc[-2]) and volume_condition:
                signal = "Buy"
                stop_loss = round(float(last['close']) - 1.5 * float(last['atr']), 2)
                take_profit = round(float(last['close']) + 3.0 * float(last['atr']), 2)
                return signal, stop_loss, take_profit, "Breakout"
            elif float(last['close']) < float(low_20.iloc[-2]) and volume_condition:
                signal = "Sell"
                stop_loss = round(float(last['close']) + 1.5 * float(last['atr']), 2)
                take_profit = round(float(last['close']) - 3.0 * float(last['atr']), 2)
                return signal, stop_loss, take_profit, "Breakout"
            return "Hold", None, None, None
        except Exception as e:
            self.logger.error(f"Error in breakout_strategy: {e}")
            return "Hold", None, None, None

    def mean_reversion_strategy(self, df, last):
        """Mean reversion trading strategy"""
        try:
            if last['rsi'] < 30 and last['close'] < last['bb_lower']:
                signal = "Buy"
                stop_loss = round(last['close'] - 1.0 * last['atr'], 2)
                take_profit = round(last['close'] + 2.0 * last['atr'], 2)
                return signal, stop_loss, take_profit, "Mean Reversion"
            elif last['rsi'] > 70 and last['close'] > last['bb_upper']:
                signal = "Sell"
                stop_loss = round(last['close'] + 1.0 * last['atr'], 2)
                take_profit = round(last['close'] - 2.0 * last['atr'], 2)
                return signal, stop_loss, take_profit, "Mean Reversion"
            return "Hold", None, None, None
        except Exception as e:
            self.logger.error(f"Error in mean_reversion_strategy: {e}")
            return "Hold", None, None, None

    def vwap_strategy(self, df, last):
        """VWAP strategy with dynamic threshold (VWAP±0.5×ATR), matching str code 9."""
        try:
            if not last['volume_spike']:
                return "Hold", None, None, None
            if last['adx'] <= 30:
                return "Hold", None, None, None
            buy_threshold = last['vwap'] - 0.5 * last['atr']
            sell_threshold = last['vwap'] + 0.5 * last['atr']
            if last['close'] < buy_threshold and last['rsi'] < 50 and last['macd'] > last['macd_signal']:
                signal = "Buy"
                stop_loss = round(last['close'] - 1.5 * last['atr'], 2)
                take_profit = round(last['vwap'] * 1.04, 2)
                return signal, stop_loss, take_profit, "VWAP"
            elif last['close'] > sell_threshold and last['rsi'] > 50 and last['macd'] < last['macd_signal']:
                signal = "Sell"
                stop_loss = round(last['close'] + 1.5 * last['atr'], 2)
                take_profit = round(last['vwap'] * 0.96, 2)
                return signal, stop_loss, take_profit, "VWAP"
            return "Hold", None, None, None
        except Exception as e:
            self.logger.error(f"Error in vwap_strategy: {e}")
            return "Hold", None, None, None

    def backtest_symbol(self, symbol, historical_data):
        """Backtest trading strategies on a symbol"""
        try:
            start_time = datetime.now()
            self.logger.info(f"Starting backtest for {symbol}")
            df = historical_data.get(symbol)
            if df is None or len(df) < 50:
                self.logger.info(f"Skipping backtest for {symbol}: Not enough data")
                return None

            df = self.precompute_indicators(symbol, df)
            def _simulate_one_strategy(strat_func, strat_name: str):
                """Simulate one strategy independently (no cross-strategy interference, no lookahead)."""
                trades_local = []
                profits_local = []
                position = None  # "long" | "short" | None
                entry_price = 0.0
                entry_time = None
                entry_stop_loss = None
                entry_take_profit = None
                entry_atr = 0.0
                position_size = 0

                for i in range(50, len(df)):
                    subset = df.iloc[: i + 1]
                    last = subset.iloc[-1]

                    try:
                        current_price = float(last["close"])
                        atr_val = float(last.get("atr", 0.0) or 0.0)
                    except Exception:
                        continue

                    signal, stop_loss, take_profit, _ = strat_func(subset, last)

                    if position is None:
                        if signal == "Buy":
                            position = "long"
                            entry_price = current_price
                            entry_time = subset.index[-1]
                            entry_stop_loss = float(stop_loss) if stop_loss is not None else None
                            entry_take_profit = float(take_profit) if take_profit is not None else None
                            entry_atr = atr_val
                            risk_per_share = abs(entry_price - entry_stop_loss) if entry_stop_loss is not None else 0.0
                            risk_amount = float(self.capital) * float(self.risk_per_trade)
                            position_size = int(risk_amount / risk_per_share) if risk_per_share > 0 else 1
                            position_size = max(1, position_size)
                        elif signal == "Sell":
                            position = "short"
                            entry_price = current_price
                            entry_time = subset.index[-1]
                            entry_stop_loss = float(stop_loss) if stop_loss is not None else None
                            entry_take_profit = float(take_profit) if take_profit is not None else None
                            entry_atr = atr_val
                            risk_per_share = abs(entry_stop_loss - entry_price) if entry_stop_loss is not None else 0.0
                            risk_amount = float(self.capital) * float(self.risk_per_trade)
                            position_size = int(risk_amount / risk_per_share) if risk_per_share > 0 else 1
                            position_size = max(1, position_size)
                        continue

                    # Manage open position
                    profit_so_far = (current_price - entry_price) if position == "long" else (entry_price - current_price)
                    if entry_atr > 0 and profit_so_far > 2 * entry_atr and entry_stop_loss is not None:
                        if position == "long":
                            entry_stop_loss = max(entry_stop_loss, entry_price + 1 * entry_atr)
                        else:
                            entry_stop_loss = min(entry_stop_loss, entry_price - 1 * entry_atr)

                    if entry_take_profit is None or entry_stop_loss is None:
                        continue

                    if position == "long":
                        hit = current_price >= entry_take_profit or current_price <= entry_stop_loss
                        if hit:
                            profit = (current_price - entry_price) * position_size
                            trades_local.append(
                                {
                                    "symbol": symbol,
                                    "entry_time": entry_time,
                                    "exit_time": subset.index[-1],
                                    "entry_price": entry_price,
                                    "exit_price": current_price,
                                    "profit": profit,
                                    "type": "long",
                                    "strategy": strat_name,
                                }
                            )
                            profits_local.append(profit)
                            position = None
                    else:
                        hit = current_price <= entry_take_profit or current_price >= entry_stop_loss
                        if hit:
                            profit = (entry_price - current_price) * position_size
                            trades_local.append(
                                {
                                    "symbol": symbol,
                                    "entry_time": entry_time,
                                    "exit_time": subset.index[-1],
                                    "entry_price": entry_price,
                                    "exit_price": current_price,
                                    "profit": profit,
                                    "type": "short",
                                    "strategy": strat_name,
                                }
                            )
                            profits_local.append(profit)
                            position = None

                return trades_local, profits_local

            # Backtest each STR9 strategy independently (matches how we later gate by per-strategy stats)
            strategy_funcs = {
                "Trend": self.trend_strategy,
                "Momentum": self.momentum_strategy,
                "VWAP": self.vwap_strategy,
            }
            strategy_results = {"Trend": [], "Momentum": [], "VWAP": []}
            trades = []
            for name, fn in strategy_funcs.items():
                t, p = _simulate_one_strategy(fn, name)
                trades.extend(t)
                strategy_results[name] = list(p)

            total_trades = len(trades)
            winning_trades = len([t for t in trades if t['profit'] > 0])
            win_rate = (winning_trades / total_trades * 100) if total_trades > 0 else 0
            total_profit = sum(t['profit'] for t in trades)
            max_drawdown = min(0, min([sum(t['profit'] for t in trades[:i+1]) for i in range(len(trades))], default=0))
            returns = [t['profit'] / self.capital for t in trades]
            if returns and len(returns) > 1 and np.std(returns) != 0 and not any(np.isnan(returns)) and not any(np.isinf(returns)):
                sharpe_ratio = (np.mean(returns) / np.std(returns) * np.sqrt(252))
            else:
                sharpe_ratio = 0

            strategy_summary = {}
            for strat, profits in strategy_results.items():
                strat_trades = len(profits)
                strat_win_rate = (len([p for p in profits if p > 0]) / strat_trades * 100) if strat_trades > 0 else 0
                strat_profit = sum(profits)
                expectancy = strat_profit / strat_trades if strat_trades > 0 else 0.0
                gross_profit = sum(p for p in profits if p > 0)
                gross_loss = sum(p for p in profits if p < 0)
                if gross_loss < 0:
                    profit_factor = gross_profit / abs(gross_loss) if abs(gross_loss) > 0 else float('inf')
                else:
                    profit_factor = float('inf') if gross_profit > 0 else 0.0
                strategy_summary[strat] = {
                    "total_trades": strat_trades,
                    "win_rate": strat_win_rate,
                    "total_profit": strat_profit,
                    "expectancy": expectancy,
                    "profit_factor": profit_factor,
                }

            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            self.logger.info(f"Finished backtest for {symbol}, took {duration:.2f} seconds, Win Rate: {win_rate:.2f}%")

            return {
                "symbol": symbol,
                "total_trades": total_trades,
                "win_rate": win_rate,
                "total_profit": total_profit,
                "max_drawdown": max_drawdown,
                "sharpe_ratio": sharpe_ratio,
                "strategy_summary": strategy_summary
            }
        except Exception as e:
            self.logger.error(f"Error in backtest_symbol for {symbol}: {e}")
            return None

    def run_initial_backtest(self, symbols, end_date=None):
        """Run initial backtest for all symbols"""
        try:
            self.logger.info("Running initial backtest for all symbols...")
            historical_data = self.fetch_multiple_daily(symbols, period="90d", end_date=end_date)
            backtest_results = []
            with ThreadPoolExecutor(max_workers=4) as executor:
                futures = [executor.submit(self.backtest_symbol, symbol, historical_data) for symbol in symbols]
                for future in futures:
                    result = future.result()
                    if result is not None:
                        backtest_results.append(result)
            
            self.logger.info(f"Backtest results count: {len(backtest_results)}")
            for res in backtest_results:
                self.backtest_results[res['symbol']] = res
            self.logger.info("Initial backtest completed.")
        except Exception as e:
            self.logger.error(f"Error in run_initial_backtest: {e}")

    def analyze_symbol(
        self,
        symbol,
        strategies: List[Strategy],
        df=None,
        analysis_date=None,
        ignore_vix: bool = False,
        rules_config: Optional[Dict] = None,
        allow_intraday_prices: bool = False,
        ignore_strategy_enabled: bool = False,
    ):
        """Analyze a single symbol and generate trading recommendations"""
        try:
            if df is None:
                df = self.fetch_daily(symbol, end_date=analysis_date)
            if df is None or len(df) < 50:
                self.logger.info(f"Skipping {symbol}: Not enough data")
                return []

            df = self.precompute_indicators(symbol, df)
            last = df.iloc[-1]

            # Optional: intraday mode (best-effort). If available, use real-time last price
            # as the reference "close" for the latest row.
            if allow_intraday_prices:
                try:
                    rt = self.fetch_realtime_price(symbol)
                    if rt is not None and not pd.isna(rt) and float(rt) > 0:
                        df = df.copy()
                        df.loc[df.index[-1], "close"] = float(rt)
                        last = df.iloc[-1]
                except Exception:
                    pass
            results = []

            current_date = analysis_date if analysis_date else datetime.now().date()
            if current_date != self.last_reset_date:
                self.daily_loss = 0
                self.last_reset_date = current_date

            market_ok, market_note, market_trend = self.check_market_conditions(end_date=analysis_date, ignore_vix=ignore_vix)
            if not market_ok:
                self.logger.info(f"Skipping {symbol}: {market_note}")
                return []

            if self.daily_loss >= self.capital * self.daily_loss_limit:
                self.logger.info(f"Skipping {symbol}: Daily loss limit reached")
                return []

            # str18: per-symbol backtest-based strategy filtering
            active_strategies = list(strategies or [])
            backtest_filter = (rules_config or {}).get("backtest_filter")
            if backtest_filter and active_strategies:
                min_trades = int(backtest_filter.get("min_trades", 10))
                min_expectancy = float(backtest_filter.get("min_expectancy", 0.0))
                min_pf = float(backtest_filter.get("min_profit_factor", 1.2))
                sym_bt = self.backtest_results.get(symbol, {}).get("strategy_summary", {})
                filtered = [
                    s for s in active_strategies
                    if (
                        sym_bt.get(s.name, {}).get("total_trades", 0) >= min_trades
                        and sym_bt.get(s.name, {}).get("expectancy", 0.0) > min_expectancy
                        and sym_bt.get(s.name, {}).get("profit_factor", 0.0) > min_pf
                    )
                ]
                if filtered:
                    active_strategies = filtered
                else:
                    # Fallback order when no strategies qualify by backtest (str code 9).
                    fallback_order = (rules_config or {}).get("fallback_order") or ["VWAP", "Trend", "Momentum"]
                    ordered = []
                    for name in fallback_order:
                        match = next((s for s in active_strategies if s.name == name), None)
                        if match is not None:
                            ordered.append(match)
                    if ordered:
                        active_strategies = ordered

            valid_signals: List[Dict] = []
            use_canonical_str9 = bool((rules_config or {}).get("use_canonical_str9"))
            if use_canonical_str9:
                # Canonical STR9 evaluation path: guarantees Momentum is BUY-only and
                # uses the exact hardcoded Trend/Momentum/VWAP rules.
                fn_by_name = {
                    "Trend": self.trend_strategy,
                    "Momentum": self.momentum_strategy,
                    "VWAP": self.vwap_strategy,
                }
                entry = float(last.get("close", 0.0) or 0.0)
                for strat in active_strategies:
                    if not ignore_strategy_enabled and not bool(getattr(strat, "enabled", False)):
                        continue
                    fn = fn_by_name.get(getattr(strat, "name", None))
                    if fn is None:
                        continue
                    signal, stop_loss, take_profit, strat_name = fn(df, last)
                    if signal not in ("Buy", "Sell") or stop_loss is None or take_profit is None or entry <= 0:
                        continue
                    try:
                        sl = float(stop_loss)
                        tp = float(take_profit)
                    except Exception:
                        continue
                    risk = abs(entry - sl)
                    reward = abs(tp - entry) if signal == "Buy" else abs(entry - tp)
                    rr = (reward / risk) if risk > 0 else 0.0
                    if rr < float(self.min_risk_reward_ratio):
                        continue
                    valid_signals.append(
                        {
                            "signal": signal,
                            "stop_loss": round(sl, 2),
                            "take_profit": round(tp, 2),
                            "strategy_id": getattr(strat, "id", None),
                            "strategy_name": strat_name or getattr(strat, "name", "-"),
                            "risk_reward_ratio": rr,
                        }
                    )
            else:
                for strat in active_strategies:
                    if not ignore_strategy_enabled and not bool(getattr(strat, "enabled", False)):
                        continue
                    valid_signals.extend(self._evaluate_strategy(strat, df, last))

            if not valid_signals:
                return []

            # Prefer non-VWAP signals when available (mirrors str code 9 preference)
            non_vwap = [s for s in valid_signals if s.get("strategy_name") != "VWAP"]
            if non_vwap:
                valid_signals = non_vwap

            # Filter by market trend: bull → Buy only; bear → Sell only (str code 9 logic)
            if market_trend == "bull":
                bullish = [s for s in valid_signals if s.get("signal") == "Buy"]
                if bullish:
                    valid_signals = bullish
            elif market_trend == "bear":
                bearish = [s for s in valid_signals if s.get("signal") == "Sell"]
                if bearish:
                    valid_signals = bearish

            if not valid_signals:
                self.logger.info(f"No signals aligned with market trend ({market_trend}) for {symbol}")
                return []

            best_signal = max(valid_signals, key=lambda x: float(x.get("risk_reward_ratio", 0.0) or 0.0))
            signal = best_signal["signal"]
            stop_loss = best_signal["stop_loss"]
            take_profit = best_signal["take_profit"]
            risk_reward_ratio = float(best_signal.get("risk_reward_ratio", 0.0) or 0.0)

            primary_strategy = best_signal.get("strategy_name") or "-"
            supporting_strategies = [
                (s.get("strategy_name") or "-")
                for s in valid_signals
                if s.get("signal") == signal
            ]
            strategy_display = ", ".join(sorted(set(supporting_strategies)))

            risk = abs(stop_loss - last['close'])
            risk_amount = self.capital * self.risk_per_trade
            position_size = int(risk_amount / risk) if risk > 0 else 1
            position_size = max(1, position_size)

            current_time = datetime.now() if not analysis_date else datetime.combine(analysis_date, datetime.min.time())
            last_time = datetime.combine(last.name, datetime.min.time()) if not pd.isna(last.name) else current_time

            sell_time_after_buy = "-"
            sell_price_after_buy = "-"
            buy_time_after_sell = "-"
            buy_price_after_sell = "-"
            time_estimate_note = "-"

            if signal == "Buy" and take_profit is not None:
                days_needed, note = self.estimate_time_to_target(df, last['close'], take_profit, direction="up")
                if days_needed is not None:
                    target_time = current_time + timedelta(days=days_needed)
                    target_time, adjustment_note = self.adjust_time_to_market_days(target_time, current_time)
                    sell_time_after_buy = target_time.strftime("%Y-%m-%d")
                    sell_price_after_buy = round(take_profit, 2)
                    time_estimate_note = note or adjustment_note or "-"
                else:
                    sell_price_after_buy = round(last['close'] * 1.03, 2)
                    target_time = current_time + timedelta(days=3)
                    target_time, adjustment_note = self.adjust_time_to_market_days(target_time, current_time)
                    sell_time_after_buy = target_time.strftime("%Y-%m-%d")
                    time_estimate_note = note or "Fallback used"
            elif signal == "Sell" and take_profit is not None:
                days_needed, note = self.estimate_time_to_target(df, last['close'], take_profit, direction="down")
                if days_needed is not None:
                    target_time = current_time + timedelta(days=days_needed)
                    target_time, adjustment_note = self.adjust_time_to_market_days(target_time, current_time)
                    buy_time_after_sell = target_time.strftime("%Y-%m-%d")
                    buy_price_after_sell = round(take_profit, 2)
                    time_estimate_note = note or adjustment_note or "-"
                else:
                    buy_price_after_sell = round(last['close'] * 0.97, 2)
                    target_time = current_time + timedelta(days=3)
                    target_time, adjustment_note = self.adjust_time_to_market_days(target_time, current_time)
                    buy_time_after_sell = target_time.strftime("%Y-%m-%d")
                    time_estimate_note = note or "Fallback used"

            backtest_win_rate = self.backtest_results.get(symbol, {}).get("win_rate", 0.0)

            results.append({
                "symbol": symbol,
                "price": round(last['close'], 2),
                "ema20": round(last['ema20'], 2),
                "ema50": round(last['ema50'], 2),
                "rsi": round(last['rsi'], 2),
                "vwap": round(last['vwap'], 2),
                "volume_spike": bool(last['volume_spike']),
                "macd": round(last['macd'], 2),
                "macd_signal": round(last['macd_signal'], 2),
                "bb_upper": round(last['bb_upper'], 2),
                "bb_lower": round(last['bb_lower'], 2),
                "atr": round(last['atr'], 2),
                "adx": round(last['adx'], 2),
                "stop_loss": stop_loss or "-",
                "take_profit": take_profit or "-",
                "position_size": position_size,
                "risk_reward_ratio": round(risk_reward_ratio, 2) if risk_reward_ratio > 0 else "-",
                "recommendation": signal,
                "strategy": strategy_display or "-",
                "primary_strategy": primary_strategy,  # NEW
                "date": last.name.strftime("%Y-%m-%d") if not pd.isna(last.name) else "N/A",
                "timestamp": last.name.strftime("%H:%M") if not pd.isna(last.name) else "N/A",
                "sell_time_after_buy": sell_time_after_buy,
                "sell_price_after_buy": sell_price_after_buy,
                "buy_time_after_sell": buy_time_after_sell,
                "buy_price_after_sell": buy_price_after_sell,
                "time_estimate_note": time_estimate_note,
                "last_update_time": current_time.strftime("%Y-%m-%d %H:%M"),
                "backtest_win_rate": backtest_win_rate,
                "data": df
            })

            # --- Strict rules / mandatory filters (single source: UI / Program config) ---
            # Note: ADX is no longer a global filter – it is configured per-strategy
            # via each strategy's pre_filters (e.g. adx > 30 in STR9 strategies).
            cfg = rules_config or {}
            strict = bool(cfg.get("strict_rules", True))
            reasons = []

            # Enforce only if the recommendation is actionable
            if results[-1].get("recommendation") in ("Buy", "Sell"):
                # Volume Spike mandatory (global toggle)
                volume_required = bool(cfg.get("volume_spike_required", False))
                if volume_required and not bool(results[-1].get("volume_spike", False)):
                    reasons.append("VolumeSpike=FALSE")

                if strict and reasons:
                    # Hard filter: do not output Buy/Sell
                    return []

            # Compute a lightweight score for ranking/selection of suggestions.
            score = 0
            notes = []

            strategy_text = strategy_display or ""
            if "Trend" in strategy_text:
                score += 10
                notes.append("Trend: +10")
            if bool(last.get("volume_spike", False)):
                score += 5
                notes.append("Volume Spike: +5")
            try:
                rsi_value = float(last.get("rsi", 0.0))
            except Exception:
                rsi_value = 0.0
            if 30 <= rsi_value <= 50:
                score += 5
                notes.append("RSI 30-50: +5")
            try:
                adx_value = float(last.get("adx", 0.0))
            except Exception:
                adx_value = 0.0
            if adx_value > 22:
                score += 4
                notes.append("ADX >22: +4")
            try:
                rr_value = float(risk_reward_ratio) if risk_reward_ratio is not None else 0.0
            except Exception:
                rr_value = 0.0
            if 1.6 <= rr_value <= 3:
                score += 3
                notes.append("Risk Reward 1.6-3: +3")
            try:
                macd_value = float(last.get("macd", 0.0))
            except Exception:
                macd_value = 0.0
            if macd_value > 0:
                score += 1
                notes.append("MACD >0: +1")

            results[-1]["score"] = score
            results[-1]["score_notes"] = ", ".join(notes)

            return results
        except Exception as e:
            self.logger.error(f"Error in analyze_symbol for {symbol}: {e}")
            return []

    def generate_excel_data(self, results):
        """Generate Excel data from analysis results without saving to file."""
        try:
            if not results:
                self.logger.warning("No data available to export.")
                return None

            unique_results = {}
            for result in results:
                if result["recommendation"] in ["Buy", "Sell"]:
                    symbol = result["symbol"]
                    if symbol not in unique_results:
                        unique_results[symbol] = result

            export_data = []
            total_investment = 0
            for result in unique_results.values():
                exit_date = result["sell_time_after_buy"] if result["recommendation"] == "Buy" else result["buy_time_after_sell"]
                try:
                    if isinstance(result["backtest_win_rate"], str) and "%" in result["backtest_win_rate"]:
                        win_rate = float(result["backtest_win_rate"].replace("%", ""))
                    else:
                        win_rate = float(result["backtest_win_rate"]) if result["backtest_win_rate"] is not None else 0.0
                except (ValueError, TypeError) as e:
                    self.logger.error(f"Error converting Win Rate for {result['symbol']}: {result['backtest_win_rate']}")
                    win_rate = 0.0

                investment = result["price"] * result["position_size"]
                if result["recommendation"] == "Sell":
                    investment *= 0.5  # 50% safety margin for Sell trades
                total_investment += investment

                export_data.append({
                    "Symbol": result["symbol"],
                    "Current price": result["price"],
                    "investment in $": round(investment, 2),
                    "Transaction type": result["recommendation"],
                    "Order Type": "Market",
                    "Position": result["position_size"],
                    "Stop Loss": result["stop_loss"],
                    "Take Profit": result["take_profit"],
                    "Target Date": exit_date,
                    "Win Rate": win_rate,
                    "ADX": result.get("adx", 0),
                    "RSI": result.get("rsi", 0),
                    "MACD": result.get("macd", 0),
                    "Volume Spike": result.get("volume_spike", False),
                    "Score": result.get("score", 0),
                    "Score Notes": result.get("score_notes", ""),
                    "Strategy": result["strategy"]
                })

            if not export_data:
                self.logger.warning("No Buy or Sell recommendations to export.")
                return None

            export_data.append({
                "Symbol": "Total Investment",
                "Current price": "",
                "investment in $": round(total_investment, 2),
                "Transaction type": "",
                "Order Type": "",
                "Position": "",
                "Stop Loss": "",
                "Take Profit": "",
                "Target Date": "",
                "Win Rate": "",
                "ADX": "",
                "RSI": "",
                "MACD": "",
                "Volume Spike": "",
                "Score": "",
                "Score Notes": "",
                "Strategy": ""
            })

            # Return DataFrame instead of saving to file
            df = pd.DataFrame(export_data)
            self.logger.info(f"Generated Excel data with {len(export_data)} rows")
            return df
        except Exception as e:
            self.logger.error(f"Error in generate_excel_data: {e}")
            return None

    def _summarize_trades_for_scan(self, results, entry_timestamp: Optional[datetime] = None):
        """
        Turn analyze_symbol outputs into a per-symbol trade JSON list suitable for analytics/storage.
        Example item:
        {
          "symbol": "DAIC",
          "recommendation": "Buy",
          "entry_price": 4.35,
          "stop_loss": 3.36,
          "take_profit": 7.84,
          "position_size": 1515,
          "risk_reward_ratio": 3.5252,
          "entry_date": "2025-08-25",
          "exit_date": "2025-08-28",
          "strategy": "VWAP"
        }
        """
        if entry_timestamp is not None:
            if entry_timestamp.tzinfo is None:
                entry_timestamp = entry_timestamp.replace(tzinfo=timezone.utc)
            else:
                entry_timestamp = entry_timestamp.astimezone(timezone.utc)
            entry_timestamp_iso = entry_timestamp.isoformat()
        else:
            entry_timestamp_iso = datetime.now(timezone.utc).isoformat()

        out = []
        for r in results:
            if r.get("recommendation") not in ("Buy", "Sell"):
                continue

            exit_date = r.get("sell_time_after_buy") if r["recommendation"] == "Buy" else r.get("buy_time_after_sell")

            rr = r.get("risk_reward_ratio")
            try:
                rr = float(rr) if rr not in (None, "-", "") else None
            except Exception:
                rr = None

            out.append({
                "symbol": r["symbol"],
                "recommendation": r["recommendation"],
                "entry_price": r["price"],
                "stop_loss": r["stop_loss"],
                "take_profit": r["take_profit"],
                "position_size": r["position_size"],
                "risk_reward_ratio": rr,
                "entry_date": entry_timestamp_iso,
                "exit_date": exit_date,
                "strategy": r.get("primary_strategy") or r.get("strategy") or "-",
                "adx": r.get("adx", 0.0),
                "rsi": r.get("rsi", 0.0),
                "macd": r.get("macd", 0.0),
                "volume_spike": bool(r.get("volume_spike", False)),
                "score": r.get("score", 0),
                "score_notes": r.get("score_notes", ""),
            })
        return out

    def analyze_stocks_for_scan(self, scan_id: int):
        """Run analysis for a scan ID and persist results."""
        db = SessionLocal()
        try:
            # Get scan record
            scan_record = db.query(ScanRecord).filter(ScanRecord.id == scan_id).first()
            if not scan_record:
                raise ValueError(f"Scan record {scan_id} not found")

            # Refresh portfolio size from settings (single source of truth for position sizing)
            try:
                portfolio_size = float(Settings.get_portfolio_size(db))
            except Exception:
                portfolio_size = 350000.0
            self.capital = max(0.0, portfolio_size)
            scan_record.portfolio_size = portfolio_size

            criteria = scan_record.criteria or {}

            # Rule: if program_id is provided => use ONLY that program's strategies (ignore global enabled).
            #       if program_id is absent/empty => use ONLY globally enabled strategies.
            received_program_id = criteria.get("program_id")
            if received_program_id is not None and str(received_program_id).strip() == "":
                received_program_id = None

            all_strategies = db.query(Strategy).all()
            enabled_strategies = [s for s in all_strategies if bool(getattr(s, "enabled", False))]

            if received_program_id:
                # Use ONLY this program's strategies (ignore global enabled/disabled).
                effective_program_id = str(received_program_id).strip()
                effective_config = {}
                try:
                    p = db.query(Program).filter(Program.program_id == effective_program_id).first()
                    if p and isinstance(p.config, dict):
                        effective_config = dict(p.config)
                except Exception:
                    pass
                program_enabled_names = set((effective_config.get("enabled_strategy_names") or []))
                strategies_to_run = [s for s in all_strategies if s.name in program_enabled_names] if program_enabled_names else []
                strategy_source = "program"
            else:
                # No program_id: use ONLY globally enabled strategies.
                effective_program_id = None
                effective_config = dict(Settings.get_active_program(db).get("active_config") or {})
                strategies_to_run = list(enabled_strategies)
                strategy_source = "globally_enabled"

            strategy_ids_used = [int(s.id) for s in strategies_to_run]
            strategy_names_used = [s.name for s in strategies_to_run]
            self.logger.info(
                "SCAN_STRATEGIES scan_id=%s received program_id=%s source=%s strategy_ids=%s strategy_names=%s",
                scan_id, received_program_id, strategy_source, strategy_ids_used, strategy_names_used,
            )

            # Resolve engine toggles: criteria > Settings DB > program config > hardcoded default
            global_settings = Settings.get_settings(db)

            # adx_min has been removed from global settings – ADX is now configured
            # per-strategy via each strategy's pre_filters.
            rules_cfg = dict((effective_config.get("rules") or {}))
            for k in ("strict_rules", "volume_spike_required"):
                if k in criteria and criteria.get(k) is not None:
                    rules_cfg[k] = criteria.get(k)
                elif not rules_cfg.get(k):
                    gs_val = getattr(global_settings, k, None)
                    if gs_val is not None:
                        rules_cfg[k] = gs_val
            # Drop any stale adx_min that may linger in stored program configs
            rules_cfg.pop("adx_min", None)
            effective_config["rules"] = rules_cfg

            # allow_intraday_prices: criteria > Settings
            if "allow_intraday_prices" not in criteria:
                criteria = dict(criteria)
                gs_intraday = getattr(global_settings, "use_intraday", False)
                criteria["allow_intraday_prices"] = bool(gs_intraday)

            # Daily loss limit: criteria > Settings > program config > default
            try:
                dll = criteria.get("daily_loss_limit_pct")
                if dll is None:
                    dll_gs = getattr(global_settings, "daily_loss_limit_pct", None)
                    dll = dll_gs if dll_gs is not None else (effective_config.get("risk") or {}).get("daily_loss_limit_pct")
                if dll is not None:
                    self.daily_loss_limit = float(dll)
            except Exception:
                self.daily_loss_limit = 0.02

            if not scan_record.stock_symbols:
                raise ValueError(f"No stock symbols found for scan {scan_id}")

            # Handle both old format (list of strings) and new format (list of objects)
            if scan_record.stock_symbols and isinstance(scan_record.stock_symbols[0], dict):
                # New format: list of objects with detailed data
                symbols_to_analyze = [symbol['ticker'] for symbol in scan_record.stock_symbols]
            else:
                # Old format: list of ticker strings (backward compatibility)
                symbols_to_analyze = scan_record.stock_symbols

            # Update status to analyzing
            scan_record.analysis_status = "analyzing"
            scan_record.analysis_progress = 0
            db.commit()

            self.logger.info(f"Starting analysis for scan {scan_id} with {len(symbols_to_analyze)} symbols")
            self._update_analysis_progress(scan_id, 10)  # 10% - Starting analysis
            
            # Run backtest
            self.logger.info(f"Running comprehensive backtest for {len(symbols_to_analyze)} symbols...")
            self._update_analysis_progress(scan_id, 15)  # 15% - Starting backtest
            
            self.run_initial_backtest(symbols_to_analyze)
            self._update_analysis_progress(scan_id, 35)  # 35% - Backtest complete
            
            # Analyze each symbol
            self.logger.info(f"Analyzing individual symbols with detailed strategies...")
            all_results = []
            ignore_vix = bool(criteria.get("ignore_vix", False))
            allow_intraday_prices = bool(criteria.get("allow_intraday_prices", False))  # resolved above
            rules_for_scan = dict(effective_config.get("rules") or {})
            # For STR9/STR18 programs, ensure canonical rules are enforced even if the stored
            # program config is stale/missing some keys.
            if effective_program_id in ("str_code_9", "str_code_18"):
                rules_for_scan.setdefault(
                    "backtest_filter",
                    {"min_trades": 10, "min_expectancy": 0.0, "min_profit_factor": 1.2},
                )
                rules_for_scan.setdefault("fallback_order", ["VWAP", "Trend", "Momentum"])
                rules_for_scan["use_canonical_str9"] = True
            ignore_strategy_enabled = (strategy_source == "program")
            for i, symbol in enumerate(symbols_to_analyze):
                self.logger.info(f"Analyzing symbol {i+1}/{len(symbols_to_analyze)}: {symbol}")
                
                # Update progress during symbol analysis (35% to 80%)
                symbol_progress = 35 + int((i / len(symbols_to_analyze)) * 45)
                self._update_analysis_progress(scan_id, symbol_progress)
                
                results = self.analyze_symbol(
                    symbol,
                    strategies_to_run,
                    ignore_vix=ignore_vix,
                    rules_config=rules_for_scan,
                    allow_intraday_prices=allow_intraday_prices,
                    ignore_strategy_enabled=ignore_strategy_enabled,
                )
                all_results.extend(results)
            
            self._update_analysis_progress(scan_id, 85)  # 85% - Symbol analysis complete

            # Excel generation now happens on-demand via API endpoints

            # Build compact JSON summary for analytics/storage
            self.logger.info(f"Processing and summarizing results...")
            self._update_analysis_progress(scan_id, 90)  # 90% - Processing results
            
            analysis_completed_at = datetime.now(timezone.utc)
            trade_summary = self._summarize_trades_for_scan(
                all_results,
                entry_timestamp=analysis_completed_at,
            )
            
            # Update scan record with summarized results
            scan_record.analysis_results = trade_summary
            scan_record.analysis_status = "completed"
            scan_record.analyzed_at = analysis_completed_at
            
            db.commit()
            
            self._update_analysis_progress(scan_id, 100)  # 100% - Complete
            
            self.logger.info(
                f"Analysis for scan {scan_id} completed with {len(trade_summary)} trades"
            )
            
            # Send WebSocket event for analysis completion
            try:
                import asyncio
                from app.services.websocket_manager import websocket_manager
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                loop.run_until_complete(websocket_manager.send_event("analysis_completed", scan_id))
                loop.close()
                self.logger.info(f"📡 WebSocket event sent for analysis completion: {scan_id}")
            except Exception as ws_error:
                self.logger.error(f"Failed to send WebSocket event for analysis {scan_id}: {ws_error}")
            
            
            return all_results
            
        except Exception as e:
            self.logger.error(f"Error analyzing stocks for scan {scan_id}: {e}")
            # Update scan record with error
            scan_record = db.query(ScanRecord).filter(ScanRecord.id == scan_id).first()
            if scan_record:
                scan_record.analysis_status = "failed"
                scan_record.error_message = str(e)
                db.commit()
            raise
        finally:
            db.close()

    def start_background_analysis(self, scan_id: int):
        """Start background analysis for a scan"""
        import threading
        thread = threading.Thread(
            target=self.analyze_stocks_for_scan,
            args=(scan_id,)
        )
        thread.daemon = True
        thread.start()
        self.logger.info(f"Background analysis started for scan {scan_id}")
