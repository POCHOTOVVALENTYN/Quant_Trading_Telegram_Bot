import time
import logging
from typing import Dict, Any, Optional
import redis.asyncio as aioredis
from config.settings import settings

_rm_logger = logging.getLogger("risk_manager")


class RiskManager:
    def __init__(self, max_risk_pct: float = 0.02, max_drawdown_pct: float = 0.20, max_open_trades: int = 5):
        self.max_risk_pct = max_risk_pct
        self.max_drawdown_pct = max_drawdown_pct
        self.max_open_trades = max_open_trades

        # Daily PnL tracking — auto-stop at 5% daily drawdown
        self.max_daily_drawdown_pct = 0.05
        self.redis: Optional[aioredis.Redis] = None

        self._daily_pnl_usd = 0.0
        self._daily_start_balance = 0.0
        self._daily_reset_ts = 0.0
        self._daily_halted = False
        
        # Streak-based adaptive risk
        self._consecutive_losses = 0
        self._consecutive_wins = 0
        self._streak_risk_mult = 1.0

        # Correlation groups — max 2 positions in same direction per cluster
        self.correlation_groups = {
            "BTC_cluster": ["BTC/USDT", "ETH/USDT", "SOL/USDT", "LTC/USDT", "BCH/USDT"],
            "ALT_cluster": ["ADA/USDT", "DOT/USDT", "LINK/USDT", "NEAR/USDT"],
            "MEME_cluster": ["DOGE/USDT", "SHIB/USDT"]
        }
        self.max_correlated_same_direction = 2

    async def initialize_redis(self):
        """Asynchronous initialization of Redis connection."""
        try:
            self.redis = aioredis.from_url(settings.redis_url, decode_responses=True)
            await self.redis.ping()
            await self._load_from_redis()
        except Exception as e:
            _rm_logger.warning(f"Could not connect to Redis for RiskManager Daily PnL: {e}")
            self.redis = None

    async def _load_from_redis(self):
        if not self.redis: return
        try:
            pnl = await self.redis.get("rm_daily_pnl")
            if pnl is not None: self._daily_pnl_usd = float(pnl)
            
            bal = await self.redis.get("rm_daily_bal")
            if bal is not None: self._daily_start_balance = float(bal)
            
            ts = await self.redis.get("rm_daily_ts")
            if ts is not None: self._daily_reset_ts = float(ts)
            
            halt = await self.redis.get("rm_daily_halt")
            if halt is not None: self._daily_halted = (halt == "1")
            
            w = await self.redis.get("rm_cons_wins")
            if w is not None: self._consecutive_wins = int(w)
            
            l = await self.redis.get("rm_cons_loss")
            if l is not None: self._consecutive_losses = int(l)
        except Exception as e:
            _rm_logger.warning(f"Failed to load RiskManager state from Redis: {e}")

    async def _save_to_redis(self):
        if not self.redis: return
        try:
            await self.redis.set("rm_daily_pnl", self._daily_pnl_usd)
            await self.redis.set("rm_daily_bal", self._daily_start_balance)
            await self.redis.set("rm_daily_ts", self._daily_reset_ts)
            await self.redis.set("rm_daily_halt", "1" if self._daily_halted else "0")
            await self.redis.set("rm_cons_wins", self._consecutive_wins)
            await self.redis.set("rm_cons_loss", self._consecutive_losses)
        except Exception as e:
            _rm_logger.warning(f"Failed to save RiskManager state to Redis: {e}")

    async def record_closed_pnl(self, pnl_usd: float, account_balance: float) -> None:
        """Call on every closed trade. Tracks cumulative daily PnL, streaks, and halts if limit breached."""
        await self._ensure_daily_reset(account_balance)
        self._daily_pnl_usd += pnl_usd

        # Streak tracking (Schwager-style adaptive risk)
        if pnl_usd > 0:
            self._consecutive_wins += 1
            self._consecutive_losses = 0
        elif pnl_usd < 0:
            self._consecutive_losses += 1
            self._consecutive_wins = 0

        # Risk multiplier: reduce after 3+ losses, cautiously increase after 3+ wins
        if self._consecutive_losses >= 5:
            self._streak_risk_mult = 0.25
        elif self._consecutive_losses >= 3:
            self._streak_risk_mult = 0.5
        elif self._consecutive_wins >= 5:
            self._streak_risk_mult = 1.25
        elif self._consecutive_wins >= 3:
            self._streak_risk_mult = 1.1
        else:
            self._streak_risk_mult = 1.0

        if self._streak_risk_mult != 1.0:
            _rm_logger.info(
                f"STREAK: W={self._consecutive_wins} L={self._consecutive_losses} "
                f"-> risk_mult={self._streak_risk_mult:.2f}"
            )

        if self._daily_start_balance > 0:
            dd_pct = abs(min(0.0, self._daily_pnl_usd)) / self._daily_start_balance
            if dd_pct >= self.max_daily_drawdown_pct:
                self._daily_halted = True
                _rm_logger.warning(
                    f"DAILY DRAWDOWN HALT: PnL={self._daily_pnl_usd:.2f} USDT "
                    f"({dd_pct*100:.1f}% >= {self.max_daily_drawdown_pct*100:.0f}%)"
                )
                
        await self._save_to_redis()

    async def is_daily_halted(self) -> bool:
        await self._ensure_daily_reset(0)
        return self._daily_halted

    def get_daily_stats(self) -> dict:
        bal = self._daily_start_balance or 1
        dd = abs(min(0.0, self._daily_pnl_usd)) / bal if bal > 0 else 0
        return {
            "daily_pnl_usd": round(self._daily_pnl_usd, 2),
            "daily_drawdown_pct": round(dd * 100, 2),
            "halted": self._daily_halted,
            "consecutive_wins": self._consecutive_wins,
            "consecutive_losses": self._consecutive_losses,
            "streak_risk_mult": self._streak_risk_mult,
        }

    async def _ensure_daily_reset(self, account_balance: float) -> None:
        now = time.time()
        if now - self._daily_reset_ts > 86400:
            self._daily_pnl_usd = 0.0
            self._daily_halted = False
            self._daily_reset_ts = now
            self._consecutive_wins = 0
            self._consecutive_losses = 0
            if account_balance > 0:
                self._daily_start_balance = account_balance
            await self._save_to_redis()

    def check_listing_days(self, listing_date_str: str, min_days: int) -> bool:
        """Проверка даты листинга (Защита от новых монет)"""
        from datetime import datetime, timezone
        try:
            listing_date = datetime.fromisoformat(listing_date_str.replace('Z', '+00:00'))
            days_since = (datetime.now(timezone.utc) - listing_date).days
            return days_since >= min_days
        except Exception:
            return True # Если не смогли определить, пропускаем (или наоборот блокируем - на выбор)

    def check_trade_allowed(self, current_open_trades: int, current_drawdown_pct: float) -> bool:
        if self._daily_halted:
            return False
        if current_open_trades >= self.max_open_trades:
            return False
        if current_drawdown_pct >= self.max_drawdown_pct:
            return False
        return True

    def max_risk_amount(self, account_balance: float) -> float:
        """Maximum USD loss allowed for a new trade."""
        if account_balance <= 0:
            return 0.0
        base_risk = account_balance * float(self.max_risk_pct)
        return max(0.0, base_risk * float(self._streak_risk_mult))

    @staticmethod
    def drawdown_pct(account_balance: float, equity_peak: float) -> float:
        if account_balance <= 0 or equity_peak <= 0:
            return 0.0
        if account_balance >= equity_peak:
            return 0.0
        return (equity_peak - account_balance) / equity_peak

    def max_drawdown_exceeded(self, current_drawdown_pct: float) -> bool:
        return float(current_drawdown_pct or 0.0) >= float(self.max_drawdown_pct)

    def calculate_position_size_by_risk(
        self,
        account_balance: float,
        entry_price: float,
        stop_loss_price: float,
    ) -> float:
        """
        Risk-based position sizing:
        size = max_risk_amount / |entry - stop|
        """
        if account_balance <= 0 or entry_price <= 0 or stop_loss_price <= 0:
            return 0.0
        risk_per_unit = abs(entry_price - stop_loss_price)
        if risk_per_unit <= 1e-12:
            return 0.0
        return self.max_risk_amount(account_balance) / risk_per_unit

    def check_correlation_limit(self, symbol: str, direction: str, active_trades: dict) -> bool:
        """
        R3: Корреляционный фильтр.
        Не открывать больше max_correlated_same_direction позиций в одном направлении
        для монет из одной корреляционной группы.
        """
        for group_name, group_symbols in self.correlation_groups.items():
            if symbol in group_symbols:
                same_dir_count = 0
                for sym, trade_info in active_trades.items():
                    if sym in group_symbols and trade_info.get('signal_type') == direction:
                        same_dir_count += 1
                if same_dir_count >= self.max_correlated_same_direction:
                    return False
        return True

    @staticmethod
    def context_risk_multiplier(
        session: str = "US",
        volatility: str = "NORMAL",
        funding: str = "NORMAL",
        symbol: str = "",
    ) -> float:
        """
        Multiplicative risk scaler based on market context.
        Returns a factor in (0, 1] to apply to position size.
        """
        from config.settings import settings
        mult = 1.0
        if session == "ASIA" and "BTC" not in symbol:
            mult *= float(getattr(settings, "session_asia_risk_mult", 0.5))
        if volatility == "HIGH":
            mult *= float(getattr(settings, "session_volatility_high_risk_mult", 0.7))
        if funding in ("EXTREME_LONG", "EXTREME_SHORT"):
            mult *= float(getattr(settings, "session_funding_extreme_risk_mult", 0.8))
        return max(0.1, mult)

    @staticmethod
    def kelly_position_size(account_balance: float, win_prob: float, avg_win_pct: float = 1.5, avg_loss_pct: float = 1.0) -> float:
        """
        R1: Kelly Criterion для оптимального размера позиции.
        f* = (b*p - q) / b, где:
        - p = вероятность выигрыша (из AI)
        - q = 1 - p (вероятность проигрыша)
        - b = средний выигрыш / средний проигрыш
        
        Используем Half-Kelly (f*/2) для снижения дисперсии.
        """
        if win_prob <= 0 or win_prob >= 1 or avg_loss_pct <= 0:
            return account_balance * 0.01  # Fallback 1%
        
        b = avg_win_pct / avg_loss_pct
        q = 1 - win_prob
        kelly_f = (b * win_prob - q) / b
        
        # Half-Kelly для безопасности
        kelly_f = max(0.005, min(0.05, kelly_f / 2))  # Лимит: 0.5%-5% от депозита
        
        return account_balance * kelly_f

    @staticmethod
    def is_volatility_sufficient(df, threshold: float = 0.002) -> bool:
        """
        Проверка 'Regime Detection' (Этап 6).
        Достаточно ли волатильности для трендового входа?
        Используем отношение ATR к цене закрытия.
        """
        if df.empty or 'atr' not in df.columns:
            return True # Пропускаем если нет данных
            
        last_row = df.iloc[-1]
        volatility_ratio = last_row['atr'] / last_row['close']
        return volatility_ratio >= threshold

    def assess_trade_feasibility(
        self,
        account_balance: float,
        entry_price: float,
        stop_loss_price: float,
        market_info: dict = None,
        market_context: dict = None,
        ml_prob: float = 0.70,
    ) -> Dict[str, Any]:
        """
        Returns sizing feasibility details before the execution stage.
        Applies Dynamic Scaling based on ML confidence.
        """
        from config.settings import settings

        result: Dict[str, Any] = {
            "feasible": False,
            "reason": "unknown",
            "risk_amount_usd": 0.0,
            "risk_per_unit_usd": 0.0,
            "margin_usd": 0.0,
            "notional_usd": 0.0,
            "position_size": 0.0,
            "min_notional": 0.0,
            "min_amount": 0.0,
            "ml_scale": 1.0,
        }
        if account_balance <= 0 or entry_price <= 0:
            result["reason"] = "invalid_account_or_entry"
            return result

        risk_amount_usd = self.max_risk_amount(account_balance)
        risk_per_unit = abs(entry_price - stop_loss_price) if stop_loss_price > 0 else 0.0

        fixed_usdt = float(getattr(settings, "position_size_usdt", 0.0) or 0.0)
        margin_usd = min(fixed_usdt, account_balance) if fixed_usdt > 0 else account_balance * settings.per_trade_margin_pct
        notional_usd = margin_usd * max(1, int(settings.leverage))
        margin_position_size = notional_usd / entry_price if entry_price > 0 else 0.0
        risk_position_size = self.calculate_position_size_by_risk(account_balance, entry_price, stop_loss_price)
        position_size = risk_position_size if risk_position_size > 0 else margin_position_size

        # ML Dynamic Scaling (Phase 4A)
        # 0.50 neutral -> scaling reduces
        # 0.70+ strong -> full size
        ml_scale = 1.0
        if ml_prob >= 0.75:   ml_scale = 1.2  # Aggressive for high confidence
        elif ml_prob >= 0.70: ml_scale = 1.0  # Full base size
        elif ml_prob >= 0.65: ml_scale = 0.75 # Conservative
        elif ml_prob >= 0.60: ml_scale = 0.5  # Half size
        elif ml_prob >= 0.55: ml_scale = 0.25 # Quarter size
        else:                ml_scale = 0.1  # Minimal (shadow/test)
        
        # Kelly Criterion integration (Phase 4C)
        # If ml_prob is high, we can use Kelly to refine the margin
        kelly_margin = self.kelly_position_size(account_balance, ml_prob)
        # We take the minimum between our fixed strategy risk and Kelly for stability
        margin_usd = min(margin_usd, kelly_margin)
        notional_usd = margin_usd * max(1, int(settings.leverage))
        margin_position_size = (notional_usd / entry_price) if entry_price > 0 else 0.0
        risk_position_size = risk_position_size * ml_scale
        margin_position_size = margin_position_size * ml_scale
        if risk_position_size > 0:
            position_size = min(risk_position_size, margin_position_size)
        else:
            position_size = margin_position_size
        
        result["ml_scale"] = ml_scale
        result["kelly_limit_usd"] = kelly_margin

        if market_context:
            ctx_mult = self.context_risk_multiplier(
                session=market_context.get("session", "US"),
                volatility=market_context.get("volatility", "NORMAL"),
                funding=market_context.get("funding", "NORMAL"),
                symbol=market_context.get("symbol", ""),
            )
            position_size *= ctx_mult

        if self._streak_risk_mult != 1.0:
            position_size *= self._streak_risk_mult

        result.update(
            {
                "risk_amount_usd": float(risk_amount_usd),
                "risk_per_unit_usd": float(risk_per_unit),
                "margin_usd": float(margin_usd),
                "notional_usd": float(notional_usd),
                "position_size": float(max(0.0, position_size)),
            }
        )

        limits = (market_info or {}).get("limits", {}) if market_info else {}
        try:
            result["min_notional"] = float(limits.get("cost", {}).get("min", 0) or 0)
        except Exception:
            result["min_notional"] = 0.0
        try:
            result["min_amount"] = float(limits.get("amount", {}).get("min", 0) or 0)
        except Exception:
            result["min_amount"] = 0.0

        if result["min_notional"] > 0 and notional_usd < result["min_notional"]:
            result["reason"] = "below_min_notional"
            return result
        if result["min_amount"] > 0 and position_size < result["min_amount"]:
            result["reason"] = "below_min_amount"
            return result

        if stop_loss_price > 0 and entry_price > 0:
            sl_distance_pct = abs(entry_price - stop_loss_price) / entry_price
            if sl_distance_pct > 0:
                round_trip_fee = (settings.maker_fee_pct + settings.taker_fee_pct) * 2
                tp_distance_pct = sl_distance_pct * 2.0
                net_r = (tp_distance_pct - round_trip_fee) / (sl_distance_pct + round_trip_fee)
                if net_r < settings.min_r_multiple_after_fees:
                    result["reason"] = "below_min_r_after_fees"
                    result["net_r"] = float(net_r)
                    return result

        result["feasible"] = result["position_size"] > 0
        result["reason"] = "ok" if result["feasible"] else "zero_position_size"
        return result

    def calculate_usd_stop(self, entry_price: float, risk_usd: float, amount: float, signal_type: str = "LONG") -> float:
        """Расчет цены стопа исходя из допустимого убытка в USD для заданного объема"""
        if amount <= 0: return entry_price * 0.9 if signal_type == "LONG" else entry_price * 1.1
        
        price_diff = risk_usd / amount
        if signal_type == "LONG":
            return entry_price - price_diff
        else:
            return entry_price + price_diff

    def calculate_position_size(
        self,
        account_balance: float,
        entry_price: float,
        stop_loss_price: float,
        market_info: dict = None,
        market_context: dict = None,
    ) -> float:
        """
        Position sizing with micro-account adaptation:
        - Checks minimum notional for the symbol
        - Commission-aware R-multiple check
        - Forces max_open_trades=1 when below micro threshold
        """
        from config.settings import settings

        if account_balance <= 0 or entry_price <= 0:
            return 0.0

        # Micro-account auto-adaptation
        if settings.micro_account_mode and account_balance < settings.micro_account_threshold:
            if self.max_open_trades > 1:
                self.max_open_trades = 1
                _rm_logger.info(f"MICRO MODE: balance={account_balance:.2f} < {settings.micro_account_threshold}, forcing max_open_trades=1")

        feasibility = self.assess_trade_feasibility(
            account_balance=account_balance,
            entry_price=entry_price,
            stop_loss_price=stop_loss_price,
            market_info=market_info,
            market_context=market_context,
        )
        if not feasibility["feasible"]:
            reason = feasibility.get("reason", "unknown")
            if reason == "below_min_notional":
                _rm_logger.warning(
                    f"SKIP: notional {feasibility['notional_usd']:.2f} < min_notional {feasibility['min_notional']:.2f}"
                )
            elif reason == "below_min_amount":
                _rm_logger.warning(
                    f"SKIP: size {feasibility['position_size']:.6f} < min_amount {feasibility['min_amount']:.6f}"
                )
            elif reason == "below_min_r_after_fees":
                _rm_logger.info(
                    f"SKIP: net R-multiple {feasibility.get('net_r', 0.0):.2f} < {settings.min_r_multiple_after_fees} "
                    f"(fees eat too much at this size)"
                )
            return 0.0
        return max(0.0, float(feasibility["position_size"]))

    @staticmethod
    def break_even_price(entry_price: float, signal_type: str, buffer_pct: float = 0.0004) -> float:
        """BE price with a small buffer to cover commissions/slippage."""
        buf = entry_price * abs(buffer_pct)
        return (entry_price + buf) if signal_type.upper() == "LONG" else (entry_price - buf)

    @staticmethod
    def risk_unit(entry_price: float, initial_stop: float) -> float:
        """1R distance (always positive)."""
        return abs(entry_price - initial_stop) if entry_price > 0 and initial_stop > 0 else 0.0

    @staticmethod
    def current_r_multiple(entry_price: float, initial_stop: float, current_price: float, signal_type: str) -> float:
        """Current R-multiple (positive = favorable, negative = adverse)."""
        r = RiskManager.risk_unit(entry_price, initial_stop)
        if r <= 0:
            return 0.0
        if signal_type.upper() == "LONG":
            return (current_price - entry_price) / r
        return (entry_price - current_price) / r

    @staticmethod
    def favorable_bar_confirmed(bar: dict, signal_type: str) -> bool:
        """A bar is 'confirmed favorable' if it closes in the trade direction with body > 50% of range."""
        o, c, h, l = float(bar.get("open", 0)), float(bar.get("close", 0)), float(bar.get("high", 0)), float(bar.get("low", 0))
        bar_range = h - l
        if bar_range <= 0:
            return False
        body = abs(c - o)
        if body / bar_range < 0.5:
            return False
        return (c > o) if signal_type.upper() == "LONG" else (c < o)

    @staticmethod
    def adverse_bar_is_strong(bar: dict, signal_type: str, atr: float) -> bool:
        """A bar is 'strongly adverse' if it closes against trade with body > 0.8 ATR."""
        o, c = float(bar.get("open", 0)), float(bar.get("close", 0))
        if atr <= 0:
            return False
        body = abs(c - o)
        direction_ok = (c < o) if signal_type.upper() == "LONG" else (c > o)
        return direction_ok and body > 0.8 * atr

    @staticmethod
    def calculate_atr_stop(entry_price: float, atr: float, signal_type: str = "LONG", multiplier: float = 2.0) -> float:
        """
        ATR-based stop (Schwager: primary stop method).
        multiplier=2.0 означает стоп на расстоянии 2 ATR от входа.
        Fallback на процент только если ATR недоступен.
        """
        from config.settings import settings

        if atr > 0:
            raw_stop = entry_price - (multiplier * atr) if signal_type == "LONG" else entry_price + (multiplier * atr)
        else:
            pct = settings.sl_long_pct if signal_type == "LONG" else settings.sl_short_pct
            raw_stop = entry_price * (1 - pct) if signal_type == "LONG" else entry_price * (1 + pct)

        # Минимальная дистанция: не менее 0.5% от цены входа (защита от слишком тесных стопов)
        min_distance = entry_price * 0.005
        if signal_type == "LONG":
            raw_stop = min(raw_stop, entry_price - min_distance)
        else:
            raw_stop = max(raw_stop, entry_price + min_distance)

        if settings.sl_correction_enabled:
            correction = entry_price * 0.001
            return raw_stop - correction if signal_type == "LONG" else raw_stop + correction

        return raw_stop

    @staticmethod
    def calculate_trailing_stop(current_stop: float, current_price: float, atr: float, signal_type: str = "LONG", multiplier: float = 2.5) -> float:
        """
        Динамический ATR Trailing Stop (Швагер):
        Для LONG: Стоп только поднимается вверх.
        Stop = Max(OldStop, Price - 2.5 * ATR)

        Minimum distance: 0.3% from current price to avoid premature stop-outs
        on low-ATR coins (ADA, TRX, etc.) where 2.5×ATR can be < 0.1%.
        """
        min_distance = current_price * 0.003
        effective_distance = max(multiplier * atr, min_distance)
        new_potential_stop = current_price - effective_distance if signal_type == "LONG" else current_price + effective_distance

        if signal_type == "LONG":
            return max(current_stop, new_potential_stop)
        else:
            return min(current_stop, new_potential_stop)

class TimeExitSystem:
    """
    Time Exit Strategy 2.0 (Best Practice: Bar-based Exit).
    Для таймфреймов 1м-1ч выход через 5 дней неэффективен.
    
    Используем правило 'No-Progress after N Bars':
    - Если за 48 свечей (2 полных цикла 24ч для часовика или 48 мин для 1м) 
      цена не ушла в профит или стоит на месте — выходим.
    """
    @staticmethod
    def should_exit(opened_at_ts: float, current_ts: float, timeframe: str, 
                    current_price: float, entry_price: float, signal_type: str = "LONG") -> bool:
        from core.strategies.strategies import get_timeframe_seconds
        
        # 1. Расчет количества прошедших свечей
        tf_seconds = get_timeframe_seconds(timeframe)
        duration_sec = current_ts - opened_at_ts
        bars_passed = duration_sec / tf_seconds
        
        # 2. Лимит: 48 свечей без прогресса (Стандарт для внутридневной торговли)
        MAX_BARS = 48 
        
        if bars_passed >= MAX_BARS:
            # Если цена все еще около входа или в убытке — momentum потерян
            if signal_type == "LONG" and current_price < (entry_price * 1.005): # меньше 0.5% профита
                return True
            if signal_type == "SHORT" and current_price > (entry_price * 0.995):
                return True
                
        # 3. Хард-стоп: 120 свечей (в любом случае выход, так как рыночные условия сменились)
        if bars_passed >= 120:
            return True
            
        return False

class PyramidingSystem:
    def __init__(self):
        """
        Пирамидинг 2.0 (Обновлено по запросу пользователя - Баг 3.1).
        Вход в позицию частями от общего ДЕПОЗИТА:
        Stage 0 (Entry) = 5%
        Stage 1 (Confirmation) = 3%
        """
        self.allocation_pct = [0.05, 0.03]  # 5% вход, 3% доливка

    def check_next_entry_allowed(self, current_price: float, initial_entry: float, atr: float, signal_type: str = "LONG") -> bool:
        """
        Условие добавления: Price > Entry + 1.5 ATR (для лонгов)
        """
        if signal_type == "LONG":
            return current_price > (initial_entry + 1.5 * atr)
        else:
            return current_price < (initial_entry - 1.5 * atr)
            
    def get_allocation_amount(self, account_balance: float, current_stage: int, entry_price: float) -> float:
        """Возвращает количество контрактов для данного этапа (Баг 3.1)."""
        if current_stage >= len(self.allocation_pct) or entry_price <= 0:
            return 0.0
        usdt_amount = account_balance * self.allocation_pct[current_stage]
        return usdt_amount / entry_price

    def get_allocation_usdt(self, account_balance: float, current_stage: int) -> float:
        """Вспомогательный метод — возвращает USD-сумму (для совместимости с тестами)."""
        if current_stage >= len(self.allocation_pct):
            return 0.0
        return account_balance * self.allocation_pct[current_stage]
