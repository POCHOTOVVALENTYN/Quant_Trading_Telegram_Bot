import logging
from typing import Dict, Any, Optional
import pandas as pd
from config.settings import settings

logger = logging.getLogger("trade_guard")

class TradeGuard:
    """
    TradeGuard инкапсулирует логику сопровождения позиции (Trade Management).
    Оценивает необходимость экстренного выхода, перевода в безубыток или частичной фиксации.
    Не взаимодействует напрямую с биржей. Возвращает команды (actions) для ExecutionEngine.
    """

    @staticmethod
    def should_invalidation_exit(trade: Dict[str, Any], current_price: float, df: Optional[pd.DataFrame], risk_manager: Any) -> bool:
        if not getattr(settings, "invalidation_exit_enabled", True):
            return False
            
        inv_level = trade.get("invalidation_level")
        if inv_level is None or float(inv_level) <= 0:
            return False
            
        inv = float(inv_level)
        side = str(trade.get("signal_type", "")).upper()
        setup_group = trade.get("setup_group", "trend")

        if setup_group == "breakout":
            if side == "LONG" and current_price < inv:
                return True
            if side == "SHORT" and current_price > inv:
                return True

        elif setup_group == "trend":
            if df is not None and not df.empty and 'ema50' in df.columns:
                current_ma50 = float(df.iloc[-1].get('ema50', 0) or 0)
                if current_ma50 > 0:
                    # Смягчение: даем буфер 0.5% ниже EMA50, чтобы избежать выбивания на шуме
                    buffer = 0.005 # 0.5%
                    if side == "LONG" and current_price < current_ma50 * (1 - buffer):
                        return True
                    if side == "SHORT" and current_price > current_ma50 * (1 + buffer):
                        return True
            elif inv > 0:
                if side == "LONG" and current_price < inv:
                    return True
                if side == "SHORT" and current_price > inv:
                    return True

        elif setup_group == "mean_reversion":
            r = risk_manager.current_r_multiple(
                float(trade.get("entry", 0)),
                float(trade.get("initial_stop", trade.get("stop", 0))),
                current_price,
                side,
            )
            bars = trade.get("bars_since_entry", 0)
            if bars > 8 and r < -0.5:
                return True

        return False

    @staticmethod
    def should_time_stop(trade: Dict[str, Any], current_price: float) -> bool:
        if not getattr(settings, "trade_mgmt_enabled", True):
            return False
            
        bars = trade.get("bars_since_entry", 0)
        setup_group = trade.get("setup_group", "trend")
        
        # Используем правильные настройки в зависимости от типа сетапа
        if setup_group == "breakout":
            threshold = getattr(settings, "time_stop_max_bars_breakout", 24)
        elif setup_group == "mean_reversion":
            threshold = getattr(settings, "time_stop_max_bars_mean_reversion", 18)
        else: # trend
            threshold = getattr(settings, "time_stop_max_bars_trend", 36)
            
        if bars >= threshold:
            # Если сделка в хорошем плюсе (>0.5R), не закрываем по времени (даем тренду идти)
            entry = float(trade.get("entry", current_price))
            stop = float(trade.get("initial_stop", entry * 0.99))
            risk = abs(entry - stop)
            side = str(trade.get("signal_type", "LONG")).upper()
            profit = (current_price - entry) if side == "LONG" else (entry - current_price)
            current_r = profit / risk if risk > 0 else 0
            
            if current_r < 0.5:
                return True
        return False

    @staticmethod
    def should_arm_break_even(trade: Dict[str, Any], current_r: float, adx: Optional[float]) -> bool:
        if trade.get("be_armed") or trade.get("be_moved"):
            return False
        if current_r < getattr(settings, "be_trigger_r", 1.0):
            return False
        if not getattr(settings, "be_require_confirmation", True):
            return True
        adx_ok = adx is not None and float(adx) >= 20.0
        return adx_ok
        
    @staticmethod
    def should_partial_reduce(trade: Dict[str, Any], current_r: float) -> bool:
        if not getattr(settings, "partial_enabled", True):
            return False
        if trade.get("partial_done"):
            return False
        if current_r < getattr(settings, "partial_trigger_r", 2.0):
            return False
        return True
