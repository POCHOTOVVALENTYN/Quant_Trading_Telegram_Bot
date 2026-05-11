import time
import logging
from typing import Dict, Any, Optional
import redis.asyncio as aioredis
from config.settings import settings

_rm_logger = logging.getLogger("risk_manager")

class TimeExitSystem:
    """Управляет выходами по времени (time stops)."""
    def __init__(self):
        pass

    def check_exit(self, position: Any, current_bars: int) -> bool:
        # Заглушка, логика будет в TM (Trade Management)
        return False

class PyramidingSystem:
    """Управляет доливками к позициям."""
    def __init__(self):
        pass

    def check_add(self, position: Any) -> bool:
        # Заглушка
        return False

class RiskManager:
    """
    Управляет рисками системы:
    - Максимальный риск на сделку (например, 2%)
    - Защита от максимальной просадки (Max Drawdown)
    - Адаптивный риск на основе серий сделок (Schwager-style)
    - Лимиты по корреляционным группам
    """
    def __init__(self, max_risk_pct: Optional[float] = None, max_drawdown_pct: Optional[float] = None, max_open_trades: Optional[int] = None):
        self.max_risk_pct = max_risk_pct if max_risk_pct is not None else settings.max_risk_per_trade_pct
        self.max_drawdown_pct = max_drawdown_pct if max_drawdown_pct is not None else settings.max_drawdown_pct
        self.max_open_trades = max_open_trades if max_open_trades is not None else settings.max_open_positions

        # Daily PnL tracking
        self.max_daily_drawdown_pct = settings.max_daily_drawdown_pct
        self.redis: Optional[aioredis.Redis] = None

        self._daily_pnl_usd = 0.0
        self._daily_start_balance = 0.0
        self._daily_reset_ts = 0.0
        self._daily_halted = False
        
        # Streak-based adaptive risk
        self._consecutive_losses = 0
        self._consecutive_wins = 0
        self._streak_risk_mult = 1.0

        # Correlation groups
        self.correlation_groups = {
            "BTC_cluster": ["BTC/USDT", "ETH/USDT", "SOL/USDT", "LTC/USDT", "BCH/USDT"],
            "ALT_cluster": ["ADA/USDT", "DOT/USDT", "LINK/USDT", "NEAR/USDT"],
            "MEME_cluster": ["DOGE/USDT", "SHIB/USDT"]
        }
        self.max_correlated_same_direction = settings.max_correlated_same_direction

    async def initialize_redis(self):
        try:
            self.redis = aioredis.from_url(settings.redis_url, decode_responses=True)
            await self.redis.ping()
            await self._load_from_redis()
        except Exception as e:
            _rm_logger.warning(f"Could not connect to Redis for RiskManager: {e}")

    async def _load_from_redis(self):
        if not self.redis: return
        try:
            pnl = await self.redis.get("rm_daily_pnl")
            if pnl: self._daily_pnl_usd = float(pnl)
            halt = await self.redis.get("rm_daily_halt")
            if halt: self._daily_halted = (halt == "1")
            l = await self.redis.get("rm_cons_loss")
            if l: self._consecutive_losses = int(l)
        except Exception as e:
            _rm_logger.warning(f"Failed to load RiskManager state: {e}")

    def calculate_position_size(
        self, 
        account_balance: float, 
        entry_price: float, 
        stop_loss_price: float,
        ml_prob: float = 0.5
    ) -> float:
        """
        Рассчитывает размер позиции на основе риска:
        size = (balance * risk_pct * streak_mult) / |entry - stop|
        """
        if account_balance <= 0 or entry_price <= 0 or stop_loss_price <= 0:
            return 0.0
            
        risk_per_unit = abs(entry_price - stop_loss_price)
        if risk_per_unit <= 1e-12:
            return 0.0
            
        # Базовая сумма риска в USD
        risk_amount_usd = account_balance * self.max_risk_pct * self._streak_risk_mult
        
        # Корректировка на основе уверенности ML модели (опционально)
        if ml_prob > 0.7:
            risk_amount_usd *= 1.2
        elif ml_prob < 0.4:
            risk_amount_usd *= 0.8
            
        return risk_amount_usd / risk_per_unit

    def check_trade_allowed(self, current_open_trades: int, current_drawdown_pct: float) -> bool:
        """Проверка общих лимитов перед входом."""
        if self._daily_halted:
            _rm_logger.info("Trade blocked: daily drawdown halt active")
            return False
        if current_open_trades >= self.max_open_trades:
            _rm_logger.info(f"Trade blocked: max open trades reached ({self.max_open_trades})")
            return False
        if current_drawdown_pct >= self.max_drawdown_pct:
            _rm_logger.info(f"Trade blocked: total drawdown limit reached ({current_drawdown_pct:.2%})")
            return False
        return True

    def calculate_atr_stop(self, entry_price: float, atr: float, direction: str, multiplier: float = 2.0) -> float:
        """Рассчитывает стоп-лосс на основе ATR."""
        if direction.upper() == "LONG":
            return entry_price - (atr * multiplier)
        return entry_price + (atr * multiplier)

    def max_risk_amount(self, account_balance: float) -> float:
        """Максимальная сумма риска в USD для данной сделки."""
        return account_balance * self.max_risk_pct * self._streak_risk_mult

    def assess_trade_feasibility(
        self, 
        account_balance: float, 
        entry_price: float, 
        stop_loss_price: float,
        market_info: Optional[Dict[str, Any]] = None,
        market_context: Optional[Dict[str, Any]] = None,
        ml_prob: float = 0.5
    ) -> Dict[str, Any]:
        """
        Проверяет, возможна ли сделка с учетом минимальных лотов и маржи.
        """
        pos_size = self.calculate_position_size(account_balance, entry_price, stop_loss_price, ml_prob)
        if pos_size <= 0:
            return {"feasible": False, "reason": "invalid_size", "position_size": 0}

        # Проверка лимитов биржи
        if market_info:
            limits = market_info.get("limits", {})
            
            # Минимальное количество
            min_qty = limits.get("amount", {}).get("min", 0)
            if pos_size < min_qty:
                return {"feasible": False, "reason": "below_min_amount", "position_size": pos_size}
                
            # Минимальный объем (notional)
            min_cost = limits.get("cost", {}).get("min", 0)
            if (pos_size * entry_price) < min_cost:
                return {"feasible": False, "reason": "below_min_notional", "position_size": pos_size}

        return {"feasible": True, "reason": "ok", "position_size": pos_size}

    async def record_trade_result(self, pnl_usd: float):
        """Обновляет статистику после закрытия сделки (адаптивный риск)."""
        if pnl_usd < 0:
            self._consecutive_losses += 1
            self._consecutive_wins = 0
        else:
            self._consecutive_wins += 1
            self._consecutive_losses = 0

        # Срезаем риск в 2 раза после 3 поражений подряд (Schwager rule)
        if self._consecutive_losses >= settings.streak_loss_threshold_hard:
            self._streak_risk_mult = 0.25
        elif self._consecutive_losses >= settings.streak_loss_threshold_soft:
            self._streak_risk_mult = 0.5
        else:
            self._streak_risk_mult = 1.0
            
        _rm_logger.info(f"Risk Update: streak_mult={self._streak_risk_mult}, losses={self._consecutive_losses}")
    def current_r_multiple(self, entry: float, initial_stop: float, current_price: float, side: str) -> float:
        """Рассчитывает текущую кратность R (профит/риск)."""
        risk = abs(entry - initial_stop)
        if risk < 1e-12:
            return 0.0
        
        profit = (current_price - entry) if side.upper() == "LONG" else (entry - current_price)
        return profit / risk
