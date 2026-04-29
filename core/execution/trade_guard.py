import logging
import time
import pandas as pd
from typing import Dict, Any, Optional, Tuple
from config.settings import settings

logger = logging.getLogger(__name__)

class TradeGuard:
    """
    Logic for trade lifecycle management:
    - Break-even arming
    - Partial reduction
    - Time-based exits
    - Setup invalidation checks
    - Trailing stop calculation
    """

    def evaluate_management(
        self, 
        trade: Dict[str, Any], 
        current_price: float, 
        current_r: float, 
        adx: Optional[float], 
        df_tf: Optional[pd.DataFrame], 
        cvd_val: float = 0.0
    ) -> Dict[str, Any]:
        """
        Evaluates current trade state and returns desired actions.
        Returns a dict: {
            "action": "CLOSE" | "PARTIAL" | "NONE",
            "reason": str,
            "be_armed": bool,
            "desired_stop": float | None
        }
        """
        symbol = trade.get("symbol", "?")
        side = trade.get("signal_type")
        entry = float(trade.get("entry", 0.0))
        
        result = {
            "action": "NONE",
            "reason": None,
            "be_armed": trade.get("be_armed", False),
            "desired_stop": None
        }

        # 1. Update R-multiples in-place (since trade is a dict ref)
        if current_r > trade.get("max_favorable_r", 0.0):
            trade["max_favorable_r"] = current_r
        
        inv_r = -current_r
        if inv_r > trade.get("max_adverse_r", 0.0):
            trade["max_adverse_r"] = inv_r

        # 2. Check Invalidation
        if self._should_invalidation_exit(trade, current_price, df_tf):
            result.update({"action": "CLOSE", "reason": "INVALIDATION"})
            return result

        # 3. Check Time Stop
        if self._should_time_stop(trade, current_price):
            result.update({"action": "CLOSE", "reason": "TIME_MGMT"})
            return result

        # 4. Check Volspike (Simplified)
        if df_tf is not None and not df_tf.empty and current_r >= 1.0:
            last_vol = float(df_tf.iloc[-1].get("volume", 0))
            avg_vol = float(df_tf["volume"].tail(20).mean())
            if avg_vol > 0 and last_vol > avg_vol * 5.0:
                result.update({"action": "CLOSE", "reason": "VOLSPIKE"})
                return result

        # 5. Arm Break-Even
        if self._should_arm_break_even(trade, current_r, adx):
            if not result["be_armed"]:
                result["be_armed"] = True
                logger.info(f"🔰 [TM-BE-ARM] {symbol}: armed BE at {current_r:.2f}R")

        # 5.1 CVD-based BE
        is_in_profit = (current_price > entry) if side == "LONG" else (current_price < entry)
        if not result["be_armed"] and is_in_profit:
            cvd_threshold = 0.7
            should_cvd_be = (side == "LONG" and cvd_val < -cvd_threshold) or (side == "SHORT" and cvd_val > cvd_threshold)
            if should_cvd_be:
                result["be_armed"] = True
                logger.info(f"🔰 [TM-BE-CVD] {symbol}: armed BE due to CVD reversal ({cvd_val:.2f})")

        # 6. Partial reduction check
        if not trade.get("partial_done") and current_r >= settings.partial_trigger_r:
            result["action"] = "PARTIAL"
            result["reason"] = f"Partial at {current_r:.2f}R"

        return result

    def compute_desired_stop(
        self,
        trade: Dict[str, Any],
        current_price: float,
        atr: float,
        adx: Optional[float] = None
    ) -> Optional[float]:
        """Calculates the optimal stop-loss level based on current market and trade state."""
        side = trade.get("signal_type")
        is_long = (side == "LONG")
        entry = float(trade.get("entry", 0.0))
        
        # 1. ATR Trailing (Aggressive if ADX > 25)
        # Trail multiple: 2.5x ATR default, 2.0x if trending
        trail_mult = 2.0 if (adx and adx > 25) else 2.5
        atr_trail = current_price - (atr * trail_mult) if is_long else current_price + (atr * trail_mult)
        
        # 2. Break-Even logic
        be_level = entry if is_long else entry
        
        # 3. Combine
        current_stop = float(trade.get("stop", 0.0))
        desired = current_stop
        
        # Trail only in our favor
        if is_long:
            desired = max(desired, atr_trail)
            if trade.get("be_armed"):
                desired = max(desired, be_level)
        else:
            if desired <= 0: desired = atr_trail # Init if missing
            else: desired = min(desired, atr_trail)
            
            if trade.get("be_armed"):
                desired = min(desired, be_level)
                
        # 4. Final safety: Never move stop AGAINST us
        if is_long:
            return max(current_stop, desired)
        else:
            return min(current_stop, desired) if current_stop > 0 else desired

    # ---- Internal helpers ----

    def _should_time_stop(self, trade: dict, current_price: float) -> bool:
        """Exit if trade is stagnant (Time Stop)."""
        if not settings.time_exit_enabled: return False
        
        # Calculate bars since entry
        bars = self._bars_since_entry(trade)
        trade["bars_since_entry"] = bars
        
        if bars < settings.time_exit_bars: return False
        
        # Only exit if not in significant profit
        entry = float(trade.get("entry", 0))
        side = trade.get("signal_type")
        pnl_pct = (current_price - entry) / entry if side == "LONG" else (entry - current_price) / entry
        
        if pnl_pct < 0.005: # less than 0.5% profit
            return True
        return False

    def _bars_since_entry(self, trade: dict) -> int:
        opened_at = trade.get("opened_at", 0)
        if not opened_at: return 0
        
        tf = trade.get("timeframe", "1h")
        tf_seconds = self._tf_to_seconds(tf)
        if tf_seconds <= 0: return 0
        
        return int((time.time() - opened_at) / tf_seconds)

    def _tf_to_seconds(self, tf: str) -> int:
        mult = 60
        if tf.endswith("m"): mult = 60
        elif tf.endswith("h"): mult = 3600
        elif tf.endswith("d"): mult = 86400
        
        try:
            num = int(''.join(filter(str.isdigit, tf)))
            return num * mult
        except:
            return 3600

    def _should_invalidation_exit(self, trade: dict, current_price: float, df: Optional[pd.DataFrame]) -> bool:
        """Exit if technical setup is invalidated (e.g. price crosses key EMA)."""
        setup = trade.get("setup_group")
        if setup == "trend" and df is not None and not df.empty:
            # If price crosses EMA50 against us
            ema50 = df.iloc[-1].get("ema50")
            if ema50:
                side = trade.get("signal_type")
                if side == "LONG" and current_price < ema50: return True
                if side == "SHORT" and current_price > ema50: return True
        
        # Breakout invalidation
        inv_level = trade.get("invalidation_level")
        if inv_level:
            side = trade.get("signal_type")
            if side == "LONG" and current_price < inv_level: return True
            if side == "SHORT" and current_price > inv_level: return True
            
        return False

    def _should_arm_break_even(self, trade: dict, current_r: float, adx: Optional[float]) -> bool:
        """Determine if BE should be armed based on R-multiple and trend strength."""
        trigger = settings.be_trigger_r
        # If trend is weak, be more aggressive with BE
        if adx and adx < 20:
            trigger *= 0.8
        return current_r >= trigger
