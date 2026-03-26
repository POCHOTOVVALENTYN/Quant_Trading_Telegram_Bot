import time
import logging

_rm_logger = logging.getLogger("risk_manager")


class RiskManager:
    def __init__(self, max_risk_pct: float = 0.02, max_drawdown_pct: float = 0.20, max_open_trades: int = 5):
        self.max_risk_pct = max_risk_pct
        self.max_drawdown_pct = max_drawdown_pct
        self.max_open_trades = max_open_trades

        # Daily PnL tracking — auto-stop at 5% daily drawdown
        self.max_daily_drawdown_pct = 0.05
        self._daily_pnl_usd = 0.0
        self._daily_start_balance = 0.0
        self._daily_reset_ts = 0.0
        self._daily_halted = False

        # Correlation groups — max 2 positions in same direction per cluster
        self.correlation_groups = {
            "BTC_cluster": ["BTC/USDT", "ETH/USDT", "SOL/USDT", "LTC/USDT", "BCH/USDT"],
            "ALT_cluster": ["ADA/USDT", "DOT/USDT", "LINK/USDT", "NEAR/USDT"],
            "MEME_cluster": ["DOGE/USDT", "SHIB/USDT"]
        }
        self.max_correlated_same_direction = 2

    def record_closed_pnl(self, pnl_usd: float, account_balance: float) -> None:
        """Call on every closed trade. Tracks cumulative daily PnL and halts if limit breached."""
        self._ensure_daily_reset(account_balance)
        self._daily_pnl_usd += pnl_usd
        if self._daily_start_balance > 0:
            dd_pct = abs(min(0.0, self._daily_pnl_usd)) / self._daily_start_balance
            if dd_pct >= self.max_daily_drawdown_pct:
                self._daily_halted = True
                _rm_logger.warning(
                    f"DAILY DRAWDOWN HALT: PnL={self._daily_pnl_usd:.2f} USDT "
                    f"({dd_pct*100:.1f}% >= {self.max_daily_drawdown_pct*100:.0f}%)"
                )

    def is_daily_halted(self) -> bool:
        self._ensure_daily_reset(0)
        return self._daily_halted

    def get_daily_stats(self) -> dict:
        bal = self._daily_start_balance or 1
        dd = abs(min(0.0, self._daily_pnl_usd)) / bal if bal > 0 else 0
        return {
            "daily_pnl_usd": round(self._daily_pnl_usd, 2),
            "daily_drawdown_pct": round(dd * 100, 2),
            "halted": self._daily_halted,
        }

    def _ensure_daily_reset(self, account_balance: float) -> None:
        now = time.time()
        if now - self._daily_reset_ts > 86400:
            self._daily_pnl_usd = 0.0
            self._daily_halted = False
            self._daily_reset_ts = now
            if account_balance > 0:
                self._daily_start_balance = account_balance

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
        if current_open_trades >= self.max_open_trades:
            return False
            
        if current_drawdown_pct >= self.max_drawdown_pct:
            return False
            
        return True

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

    def calculate_usd_stop(self, entry_price: float, risk_usd: float, amount: float, signal_type: str = "LONG") -> float:
        """Расчет цены стопа исходя из допустимого убытка в USD для заданного объема"""
        if amount <= 0: return entry_price * 0.9 if signal_type == "LONG" else entry_price * 1.1
        
        price_diff = risk_usd / amount
        if signal_type == "LONG":
            return entry_price - price_diff
        else:
            return entry_price + price_diff

    def calculate_position_size(self, account_balance: float, entry_price: float, stop_loss_price: float) -> float:
        """
        Новый режим размера позиции:
        - на одну сделку выделяем фиксированную долю маржи (settings.per_trade_margin_pct)
        - размер позиции считается из выделенной маржи и плеча
        """
        from config.settings import settings

        if account_balance <= 0 or entry_price <= 0:
            return 0.0

        # Приоритет 1: ручной фиксированный объём (USDT) из Telegram/runtime.
        fixed_usdt = float(getattr(settings, "position_size_usdt", 0.0) or 0.0)
        if fixed_usdt > 0:
            margin_usd = min(fixed_usdt, account_balance)
        else:
            margin_usd = account_balance * settings.per_trade_margin_pct
        notional_usd = margin_usd * max(1, int(settings.leverage))
        position_size = notional_usd / entry_price
        return max(0.0, position_size)

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
