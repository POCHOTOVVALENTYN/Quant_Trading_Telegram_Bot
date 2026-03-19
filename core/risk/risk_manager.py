class RiskManager:
    def __init__(self, max_risk_pct: float = 0.02, max_drawdown_pct: float = 0.20, max_open_trades: int = 5):
        """
        Менеджер рисков (Этап 9).
        max risk per trade = 2%
        max drawdown = 20%
        max open trades = 5
        """
        self.max_risk_pct = max_risk_pct
        self.max_drawdown_pct = max_drawdown_pct
        self.max_open_trades = max_open_trades

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
        Position size = Account risk / (Entry - Stop)
        """
        account_risk_usd = account_balance * self.max_risk_pct
        risk_per_coin = abs(entry_price - stop_loss_price)
        
        if risk_per_coin <= 0:
            return 0.0
            
        position_size = account_risk_usd / risk_per_coin
        return position_size

    @staticmethod
    def calculate_atr_stop(entry_price: float, atr: float, signal_type: str = "LONG", multiplier: float = 2.0) -> float:
        """
        Начальный стоп. Теперь учитываем раздельные настройки из Settings.
        """
        from config.settings import settings
        
        # Если в сигнале нет ATR, используем фиксированный процент из новых настроек
        # (это гибридный подход: если есть ATR — по нему, если нет — по % из референса)
        if atr > 0:
            raw_stop = entry_price - (multiplier * atr) if signal_type == "LONG" else entry_price + (multiplier * atr)
        else:
            pct = settings.sl_long_pct if signal_type == "LONG" else settings.sl_short_pct
            raw_stop = entry_price * (1 - pct) if signal_type == "LONG" else entry_price * (1 + pct)
            
        # Логика коррекции (Этап 2 плана)
        if settings.sl_correction_enabled:
            # Сдвигаем на 0.1% для гарантии срабатывания биржевого ордера при резком движении
            correction = entry_price * 0.001
            return raw_stop - correction if signal_type == "LONG" else raw_stop + correction
            
        return raw_stop

    @staticmethod
    def calculate_trailing_stop(current_stop: float, current_price: float, atr: float, signal_type: str = "LONG", multiplier: float = 2.5) -> float:
        """
        Динамический ATR Trailing Stop (Швагер):
        Для LONG: Стоп только поднимается вверх.
        Stop = Max(OldStop, Price - 2.5 * ATR)
        """
        new_potential_stop = current_price - (multiplier * atr) if signal_type == "LONG" else current_price + (multiplier * atr)
        
        if signal_type == "LONG":
            return max(current_stop, new_potential_stop)
        else:
            # Для SHORT: Стоп только опускается вниз
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
        Пирамидинг (Этап 11).
        Вход в позицию частями:
        Entry1 = 50%
        Entry2 = 30%
        Entry3 = 20%
        Условие: Price > Entry + 1.5 ATR
        """
        # Теперь вход 15% (Stage 0), затем 5% (Stage 1), 5% (Stage 2) и т.д.
        # Можем расширить список для более плавного входа
        self.allocation = [0.15, 0.05, 0.05, 0.05, 0.05]

    def check_next_entry_allowed(self, current_price: float, initial_entry: float, atr: float, signal_type: str = "LONG") -> bool:
        """
        Условие добавления: Price > Entry + 1.5 ATR (для лонгов)
        """
        if signal_type == "LONG":
            return current_price > (initial_entry + 1.5 * atr)
        else:
            return current_price < (initial_entry - 1.5 * atr)
            
    def get_allocation_amount(self, total_size: float, current_stage: int) -> float:
        """
        stage: 0, 1, 2
        """
        if current_stage < len(self.allocation):
            return total_size * self.allocation[current_stage]
        return 0.0
